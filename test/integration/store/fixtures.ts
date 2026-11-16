import {randomUUID} from 'node:crypto';
import Redis from 'ioredis';
import {loadConfig} from '../../../src/config/config';
import type {Cone, DispatchState, OutboxRecord, YieldDecision} from '../../../src/domain/types';

export const config = loadConfig({REDIS_URL: process.env.REDIS_URL ?? 'redis://127.0.0.1:6401'});
export const state: DispatchState = {dispatchId: 'dispatch-1', emergencyId: 'emergency-1',
  emergencyVehicleId: 'ambulance-1', status: 'DISPATCHED', origin: {latitude: 37.45, longitude: 126.65},
  destination: {latitude: 37.46, longitude: 126.66}, decisionVersion: 0, routeVersion: 0,
  updatedAt: '2026-11-16T12:00:00.000Z'};
export const cone: Cone = {cells: ['8930e0a845bffff'], origin: state.origin,
  headingDegrees: 0, distanceM: 100, halfAngleDegrees: 30};
export const decision = (targetVehicleId = 'target-1', version = 1): YieldDecision => ({
  alertId: `alert-${targetVehicleId}`, emergencyId: state.emergencyId, dispatchId: state.dispatchId,
  emergencyVehicleId: state.emergencyVehicleId, targetVehicleId, approachDirection: 'REAR',
  estimatedArrivalSeconds: 15, recommendedAction: 'YIELD_WHEN_SAFE', reason: 'SAME_ROUTE_AHEAD',
  decisionVersion: version, issuedAt: state.updatedAt, expiresAt: '2026-11-16T12:00:20.000Z',
});
export const request = (value: YieldDecision): OutboxRecord => {
  const {issuedAt, ...payload} = value;
  const eventKey = `${value.dispatchId}:${value.decisionVersion}:YIELD_ALERT_REQUESTED:${value.targetVehicleId}`;
  return {eventKey, event: {eventId: eventKey, eventType: 'YIELD_ALERT_REQUESTED', schemaVersion: 1,
    occurredAt: issuedAt, producer: 'emergency-worker', payload}};
};
export function redisFixture() {
  const prefix = `emergency-worker:test:${randomUUID()}:`;
  const redis = new Redis(config.redisUrl, {keyPrefix: prefix, maxRetriesPerRequest: 1, retryStrategy: null});
  const close = async () => {
    const raw = new Redis(config.redisUrl, {maxRetriesPerRequest: 1, retryStrategy: null});
    try {
      const keys = await raw.keys(`${prefix}*`);
      if (keys.length) await raw.del(...keys);
    } finally { await raw.quit(); await redis.quit(); }
  };
  return {redis, close};
}
