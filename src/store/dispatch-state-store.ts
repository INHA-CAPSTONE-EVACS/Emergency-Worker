import type Redis from 'ioredis';
import {loadConfig, type WorkerConfig} from '../config/config';
import {dispatchConeKey, dispatchDecisionsKey, dispatchStateKey} from '../domain/contracts';
import type {Cone, DispatchState, YieldDecision} from '../domain/types';
import {loadLuaScript} from './lua-script';

const completionScript = loadLuaScript('complete-and-enqueue-clears.lua');
const saveScript = `
local current = tonumber(redis.call('HGET', KEYS[1], 'decisionVersion') or '0')
local fields = cjson.decode(ARGV[1])
fields.decisionVersion = tostring(math.max(current, tonumber(fields.decisionVersion)))
for field, value in pairs(fields) do redis.call('HSET', KEYS[1], field, value) end
return 1`;
const saveConeScript = `
local status = redis.call('HGET', KEYS[1], 'status')
if not status or status == 'COMPLETED' or status == 'CANCELED' then return 0 end
local previous = redis.call('GET', KEYS[2])
local current_version = previous and cjson.decode(previous).sourceEmergencyStateVersion or 0
if tonumber(ARGV[2]) < current_version then return 0 end
redis.call('SET', KEYS[2], ARGV[1])
return 1`;

export class DispatchStateStore {
  constructor(private readonly redis: Redis, private readonly config: Pick<WorkerConfig, 'alertOutboxStream'> = loadConfig()) {}

  async save(state: DispatchState): Promise<void> {
    await this.redis.eval(saveScript, 1, dispatchStateKey(state.dispatchId), JSON.stringify({...state,
      origin: JSON.stringify(state.origin), destination: JSON.stringify(state.destination)}));
  }

  async findById(dispatchId: string): Promise<DispatchState | null> {
    const value = await this.redis.hgetall(dispatchStateKey(dispatchId));
    if (!Object.keys(value).length) return null;
    return {...value, origin: JSON.parse(value.origin!), destination: JSON.parse(value.destination!),
      decisionVersion: Number(value.decisionVersion), routeVersion: Number(value.routeVersion)} as unknown as DispatchState;
  }

  async saveCone(dispatchId: string, cone: Cone, sourceEmergencyStateVersion?: number): Promise<boolean> {
    // Source state order is independent of decisionVersion: a cone move can be a decision no-op.
    const value = sourceEmergencyStateVersion === undefined ? cone : {...cone, sourceEmergencyStateVersion};
    return await this.redis.eval(saveConeScript, 2, dispatchStateKey(dispatchId), dispatchConeKey(dispatchId),
      JSON.stringify(value), sourceEmergencyStateVersion ?? 0) === 1;
  }

  async findCone(dispatchId: string): Promise<Cone | null> {
    const value = await this.redis.get(dispatchConeKey(dispatchId));
    return value === null ? null : JSON.parse(value) as Cone;
  }

  async findDecisions(dispatchId: string): Promise<YieldDecision[]> {
    const value = await this.redis.get(dispatchDecisionsKey(dispatchId));
    return value === null ? [] : Object.values(JSON.parse(value) as Record<string, YieldDecision>);
  }

  async completeAndEnqueueClears(dispatchId: string, occurredAt = new Date().toISOString()): Promise<string[]> {
    return await this.redis.eval(completionScript, 4, dispatchStateKey(dispatchId), dispatchConeKey(dispatchId),
      dispatchDecisionsKey(dispatchId), this.config.alertOutboxStream, dispatchId, occurredAt) as string[];
  }
}
