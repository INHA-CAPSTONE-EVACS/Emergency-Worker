import type Redis from 'ioredis';
import {loadConfig, type WorkerConfig} from '../config/config';
import {dispatchConeKey, dispatchDecisionsKey, dispatchStateKey, parseEventEnvelope, YIELD_ALERT_CLEAR_REQUESTED,
  YIELD_ALERT_REQUESTED, vehicleStateKey} from '../domain/contracts';
import type {Cone, DecisionDiff, DispatchState, OutboxRecord, YieldDecision} from '../domain/types';
import {loadLuaScript} from './lua-script';

const script = loadLuaScript('compare-save-and-enqueue.lua');
interface DecisionFence {
  sourceVersions: ReadonlyMap<string, string | null>;
  dispatch: Pick<DispatchState, 'routeVersion' | 'destination'>;
}

export function decisionEventKey(dispatchId: string, version: number, eventType: string, targetVehicleId: string): string {
  return `${dispatchId}:${version}:${eventType}:${targetVehicleId}`;
}

export class DecisionStore {
  constructor(private readonly redis: Redis, private readonly config: Pick<WorkerConfig, 'alertOutboxStream'> = loadConfig()) {}

  async compareSaveAndEnqueue(dispatchId: string, expectedVersion: number, next: readonly YieldDecision[],
    events: readonly OutboxRecord[], expectedCone?: Cone,
    fence?: DecisionFence): Promise<DecisionDiff> {
    if (!Number.isSafeInteger(expectedVersion) || expectedVersion < 0) throw new Error('Invalid expected decision version');
    const map: Record<string, YieldDecision> = Object.create(null) as Record<string, YieldDecision>;
    for (const decision of next) {
      if (decision.dispatchId !== dispatchId || Object.hasOwn(map, decision.targetVehicleId)) throw new Error('Invalid decision identity');
      const {issuedAt, ...payload} = decision;
      parseEventEnvelope({eventId: 'validate-decision', eventType: YIELD_ALERT_REQUESTED, schemaVersion: 1,
        occurredAt: issuedAt, producer: 'emergency-worker', payload});
      map[decision.targetVehicleId] = decision;
    }
    for (const record of events) {
      const event = parseEventEnvelope(record.event);
      if (event.eventType !== YIELD_ALERT_REQUESTED && event.eventType !== YIELD_ALERT_CLEAR_REQUESTED) throw new Error('Invalid decision event type');
      if (event.payload.dispatchId !== dispatchId || record.eventKey !== decisionEventKey(dispatchId, expectedVersion + 1,
        event.eventType, event.payload.targetVehicleId)) throw new Error('Invalid deterministic event key');
    }
    const sources = fence ? [...fence.sourceVersions] : [];
    const sourceKeys = sources.map(([id]) => vehicleStateKey(id));
    const value = await this.redis.eval(script, 4 + sourceKeys.length, dispatchStateKey(dispatchId), dispatchDecisionsKey(dispatchId),
      this.config.alertOutboxStream, dispatchConeKey(dispatchId), ...sourceKeys, expectedVersion, JSON.stringify(map), JSON.stringify(events),
      expectedCone ? JSON.stringify(expectedCone) : '', JSON.stringify(sources.map(([, version]) => version)),
      fence ? JSON.stringify({routeVersion: fence.dispatch.routeVersion, destination: fence.dispatch.destination}) : '') as string;
    const result = JSON.parse(value) as DecisionDiff;
    // Redis Lua cjson encodes an empty table as {}; keep the public array contract.
    for (const field of ['added', 'changed', 'removed', 'outboxIds'] as const) {
      if (!Array.isArray(result[field])) Object.assign(result, {[field]: []});
    }
    return result;
  }
}
