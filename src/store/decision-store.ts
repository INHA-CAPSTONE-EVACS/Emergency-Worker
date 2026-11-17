import type Redis from 'ioredis';
import {loadConfig, type WorkerConfig} from '../config/config';
import {dispatchConeKey, dispatchDecisionsKey, dispatchStateKey, parseEventEnvelope, YIELD_ALERT_CLEAR_REQUESTED,
  YIELD_ALERT_REQUESTED} from '../domain/contracts';
import type {Cone, DecisionDiff, OutboxRecord, YieldDecision} from '../domain/types';
import {loadLuaScript} from './lua-script';

const script = loadLuaScript('compare-save-and-enqueue.lua');

export function decisionEventKey(dispatchId: string, version: number, eventType: string, targetVehicleId: string): string {
  return `${dispatchId}:${version}:${eventType}:${targetVehicleId}`;
}

export class DecisionStore {
  constructor(private readonly redis: Redis, private readonly config: Pick<WorkerConfig, 'alertOutboxStream'> = loadConfig()) {}

  async compareSaveAndEnqueue(dispatchId: string, expectedVersion: number, next: readonly YieldDecision[],
    events: readonly OutboxRecord[], expectedCone?: Cone): Promise<DecisionDiff> {
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
    const value = await this.redis.eval(script, 4, dispatchStateKey(dispatchId), dispatchDecisionsKey(dispatchId),
      this.config.alertOutboxStream, dispatchConeKey(dispatchId), expectedVersion, JSON.stringify(map), JSON.stringify(events),
      expectedCone ? JSON.stringify(expectedCone) : '') as string;
    const result = JSON.parse(value) as DecisionDiff;
    // Redis Lua cjson encodes an empty table as {}; keep the public array contract.
    for (const field of ['added', 'changed', 'removed', 'outboxIds'] as const) {
      if (!Array.isArray(result[field])) Object.assign(result, {[field]: []});
    }
    return result;
  }
}
