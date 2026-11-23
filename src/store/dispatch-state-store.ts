import type Redis from 'ioredis';
import {loadConfig, type WorkerConfig} from '../config/config';
import {CONE_PROJECTION_TTL_MS, dispatchConeKey, dispatchDecisionsKey, dispatchStateKey, type Coordinates} from '../domain/contracts';
import type {Cone, DispatchState, RouteResult, YieldDecision} from '../domain/types';
import {loadLuaScript} from './lua-script';

export interface AcceptedRoute {
  destination: Coordinates;
  route: Extract<RouteResult, {outcome: 'ROUTE_FOUND'}>;
}
type CachedDispatch = DispatchState & {acceptedRoute?: AcceptedRoute};

const completionScript = loadLuaScript('complete-and-enqueue-clears.lua');
const saveScript = `
local current = tonumber(redis.call('HGET', KEYS[1], 'decisionVersion') or '0')
local fields = cjson.decode(ARGV[1])
fields.decisionVersion = tostring(math.max(current, tonumber(fields.decisionVersion)))
local route_version = tonumber(redis.call('HGET', KEYS[1], 'routeVersion') or '0')
fields.routeVersion = tostring(math.max(route_version, tonumber(fields.routeVersion)))
for field, value in pairs(fields) do redis.call('HSET', KEYS[1], field, value) end
return 1`;
// Only committed MySQL route versions enter the cache. Late hydration cannot replace a newer route.
const cacheRouteScript = `
local status = redis.call('HGET', KEYS[1], 'status')
if not status or status == 'COMPLETED' or status == 'CANCELED' then return 0 end
local current = tonumber(redis.call('HGET', KEYS[1], 'routeVersion') or '0')
local incoming = tonumber(ARGV[1])
if incoming < current or (incoming == current and redis.call('HEXISTS', KEYS[1], 'acceptedRoute') == 1) then return 0 end
local destination = cjson.decode(redis.call('HGET', KEYS[1], 'destination'))
local route = cjson.decode(ARGV[2])
if destination.latitude ~= route.destination.latitude or destination.longitude ~= route.destination.longitude then return 0 end
redis.call('HSET', KEYS[1], 'routeVersion', ARGV[1], 'acceptedRoute', ARGV[2])
return 1`;
const saveConeScript = `
local status = redis.call('HGET', KEYS[1], 'status')
if not status or status == 'COMPLETED' or status == 'CANCELED' then return 0 end
if ARGV[3] ~= '' then
  local expected = cjson.decode(ARGV[3])
  local route_version = tonumber(redis.call('HGET', KEYS[1], 'routeVersion') or '0')
  local destination = cjson.decode(redis.call('HGET', KEYS[1], 'destination'))
  if route_version ~= expected.routeVersion or destination.latitude ~= expected.destination.latitude
    or destination.longitude ~= expected.destination.longitude then return 0 end
end
local previous = redis.call('GET', KEYS[2])
local current_version = previous and cjson.decode(previous).sourceEmergencyStateVersion or 0
if tonumber(ARGV[2]) < current_version then return 0 end
redis.call('SET', KEYS[2], ARGV[1])
return 1`;

// Control API Server projection (ConeSnapshot). version increases on every write so the dashboard can drop
// out-of-order polls. An empty cell list is not projected: Lua cjson would encode it as an object.
const projectConeScript = `
local previous = redis.call('GET', KEYS[1])
local version = 1
if previous then
  local ok, decoded = pcall(cjson.decode, previous)
  if ok and type(decoded) == 'table' and tonumber(decoded.version) then version = tonumber(decoded.version) + 1 end
end
local value = cjson.decode(ARGV[1])
value.version = version
redis.call('SET', KEYS[1], cjson.encode(value), 'PX', ARGV[2])
return version`;

type StoreConfig = Pick<WorkerConfig, 'alertOutboxStream'> & Partial<Pick<WorkerConfig, 'coneKeyPattern' | 'h3Resolution'>>;

export class DispatchStateStore {
  constructor(private readonly redis: Redis, private readonly config: StoreConfig = loadConfig()) {}

  async save(state: CachedDispatch): Promise<void> {
    const {acceptedRoute: _cached, ...fields} = state;
    await this.redis.eval(saveScript, 1, dispatchStateKey(state.dispatchId), JSON.stringify({...fields,
      origin: JSON.stringify(state.origin), destination: JSON.stringify(state.destination)}));
  }

  async findById(dispatchId: string): Promise<CachedDispatch | null> {
    const value = await this.redis.hgetall(dispatchStateKey(dispatchId));
    if (!Object.keys(value).length) return null;
    const {acceptedRoute, ...fields} = value;
    return {...fields, origin: JSON.parse(value.origin!), destination: JSON.parse(value.destination!),
      decisionVersion: Number(value.decisionVersion), routeVersion: Number(value.routeVersion ?? 0),
      ...(acceptedRoute ? {acceptedRoute: JSON.parse(acceptedRoute)} : {})} as unknown as CachedDispatch;
  }

  async cacheAcceptedRoute(dispatchId: string, routeVersion: number, route: AcceptedRoute): Promise<void> {
    await this.redis.eval(cacheRouteScript, 1, dispatchStateKey(dispatchId), routeVersion, JSON.stringify(route));
  }

  async saveCone(dispatchId: string, cone: Cone, sourceEmergencyStateVersion?: number,
    dispatch?: Pick<DispatchState, 'routeVersion' | 'destination'>): Promise<boolean> {
    // Source state order is independent of decisionVersion: a cone move can be a decision no-op.
    const value = sourceEmergencyStateVersion === undefined ? cone : {...cone, sourceEmergencyStateVersion};
    const saved = await this.redis.eval(saveConeScript, 2, dispatchStateKey(dispatchId), dispatchConeKey(dispatchId),
      JSON.stringify(value), sourceEmergencyStateVersion ?? 0,
      dispatch ? JSON.stringify({routeVersion: dispatch.routeVersion, destination: dispatch.destination}) : '') === 1;
    if (saved) await this.projectCone(dispatchId, cone);
    return saved;
  }

  /** The API projection is a read model: it follows the accepted cone and is not part of the decision state. */
  private async projectCone(dispatchId: string, cone: Cone): Promise<void> {
    if (!this.config.coneKeyPattern) return;
    const [emergencyId, vehicleId] = await this.redis.hmget(dispatchStateKey(dispatchId), 'emergencyId', 'emergencyVehicleId');
    if (!emergencyId) return;
    const key = this.coneProjectionKey(emergencyId);
    if (!cone.cells.length) { await this.redis.del(key); return; }
    await this.redis.eval(projectConeScript, 1, key, JSON.stringify({emergencyId, vehicleId,
      ...(this.config.h3Resolution === undefined ? {} : {resolution: this.config.h3Resolution}),
      cells: cone.cells, updatedAt: new Date().toISOString()}), CONE_PROJECTION_TTL_MS);
  }

  private coneProjectionKey(emergencyId: string): string {
    return this.config.coneKeyPattern!.replace('%s', emergencyId);
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
    const emergencyId = this.config.coneKeyPattern ? await this.redis.hget(dispatchStateKey(dispatchId), 'emergencyId') : null;
    const ids = await this.redis.eval(completionScript, 4, dispatchStateKey(dispatchId), dispatchConeKey(dispatchId),
      dispatchDecisionsKey(dispatchId), this.config.alertOutboxStream, dispatchId, occurredAt) as string[];
    // The dashboard clears the cone display once the emergency ends (design p.95).
    if (emergencyId) await this.redis.del(this.coneProjectionKey(emergencyId));
    return ids;
  }
}
