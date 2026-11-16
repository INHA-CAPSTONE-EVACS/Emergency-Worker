import type Redis from 'ioredis';
import {AwaitingPredecessorError} from '../consumer/errors';
import {ACTIVE_DISPATCHES_KEY, dispatchStateKey, dispatchVehicleKey, type DispatchStatus, type EmergencyEvent,
  type InputDispatchStatus} from '../domain/contracts';
import type {DispatchState} from '../domain/types';
import type {DispatchStateStore} from '../store/dispatch-state-store';

export interface ImmediateRouteScheduler {requestImmediate(dispatchId: string): Promise<void> | void}
const normalize = (status: InputDispatchStatus): DispatchStatus => status === 'ARRIVED' ? 'ARRIVED_ON_SCENE' : status;
const terminalKey = (id: string) => `dispatch:${id}:completed`;
const activateScript = `
if redis.call('EXISTS', KEYS[4]) == 1 then return 0 end
local fields = cjson.decode(ARGV[2])
if redis.call('EXISTS', KEYS[1]) == 1 then
  if redis.call('HGET', KEYS[1], 'emergencyVehicleId') ~= fields.emergencyVehicleId
    or redis.call('HGET', KEYS[1], 'emergencyId') ~= fields.emergencyId then return 0 end
end
local vehicle_dispatch = redis.call('GET', KEYS[3])
if vehicle_dispatch and vehicle_dispatch ~= ARGV[1] then return 0 end
if redis.call('EXISTS', KEYS[1]) == 0 then
  for field, value in pairs(fields) do redis.call('HSET', KEYS[1], field, value) end
end
redis.call('SADD', KEYS[2], ARGV[1])
redis.call('SET', KEYS[3], ARGV[1])
return 1`;
const transitionScript = `
if redis.call('EXISTS', KEYS[2]) == 1 then return 0 end
if redis.call('HGET', KEYS[1], 'status') ~= ARGV[1] then return 0 end
redis.call('HSET', KEYS[1], 'status', ARGV[2], 'destination', ARGV[3], 'updatedAt', ARGV[4])
return 1`;
const removeIndexScript = `
redis.call('SREM', KEYS[1], ARGV[1])
if redis.call('GET', KEYS[2]) == ARGV[1] then redis.call('DEL', KEYS[2]) end
return 1`;
const transitions: Record<DispatchStatus, readonly DispatchStatus[]> = {
  REGISTERED: ['DISPATCHED', 'CANCELED'], DISPATCHED: ['ARRIVED_ON_SCENE', 'COMPLETED', 'CANCELED'],
  ARRIVED_ON_SCENE: ['TRANSPORTING', 'COMPLETED', 'CANCELED'], TRANSPORTING: ['COMPLETED', 'CANCELED'],
  COMPLETED: [], CANCELED: [],
};

const canReach = (from: DispatchStatus, required: DispatchStatus): boolean =>
  transitions[from].some(next => next === required || canReach(next, required));

export class DispatchLifecycleService {
  constructor(private readonly redis: Redis, private readonly store: DispatchStateStore,
    private readonly routes: ImmediateRouteScheduler) {}

  async onEmergencyCreated(_event: Extract<EmergencyEvent, {eventType: 'EMERGENCY_CREATED'}>): Promise<void> {
    // An emergency registration has no assigned vehicle/dispatch and creates no active dispatch.
  }

  async onDispatched(event: Extract<EmergencyEvent, {eventType: 'EMERGENCY_DISPATCHED'}>): Promise<void> {
    const p = event.payload;
    await this.findActiveByVehicleId(p.vehicleId); // Repair a stale index left by a completion crash.
    const state: DispatchState = {dispatchId: p.dispatchId, emergencyId: p.emergencyId, emergencyVehicleId: p.vehicleId,
      origin: p.origin, destination: p.destination, status: 'DISPATCHED', decisionVersion: 0, routeVersion: 0, updatedAt: event.occurredAt};
    const existing = await this.store.findById(p.dispatchId);
    if (existing && (existing.emergencyVehicleId !== p.vehicleId || existing.emergencyId !== p.emergencyId)) return;
    const accepted = await this.redis.eval(activateScript, 4, dispatchStateKey(p.dispatchId), ACTIVE_DISPATCHES_KEY,
      dispatchVehicleKey(p.vehicleId), terminalKey(p.dispatchId), p.dispatchId,
      JSON.stringify({...state, origin: JSON.stringify(state.origin), destination: JSON.stringify(state.destination)}));
    if (accepted === 1 && (!existing || existing.status === 'DISPATCHED')) await this.routes.requestImmediate(p.dispatchId);
  }

  async onStatusChanged(event: Extract<EmergencyEvent, {eventType: 'EMERGENCY_STATUS_CHANGED'}>): Promise<void> {
    const p = event.payload;
    const current = await this.store.findById(p.dispatchId);
    const next = normalize(p.currentStatus);
    if (current && current.emergencyId !== p.emergencyId) return;
    const previous = normalize(p.previousStatus);
    if (!transitions[previous].includes(next)) return;
    // A terminal event is authoritative even when activation or an intermediate status is still pending.
    // Persisting the completion intent before ACK prevents its predecessor from activating on reclaim.
    if (next === 'COMPLETED' || next === 'CANCELED') {
      await this.completeDispatch(p.dispatchId, event.occurredAt); return;
    }
    if (!current) {
      if (await this.redis.exists(terminalKey(p.dispatchId))) return;
      throw new AwaitingPredecessorError(`Dispatch ${p.dispatchId} is awaiting activation`);
    }
    if (current.status === next) {
      // Replays must retry routing if the original scheduler call failed after the state commit.
      if (next === 'TRANSPORTING' && p.destination && current.destination.latitude === p.destination.latitude
        && current.destination.longitude === p.destination.longitude) await this.routes.requestImmediate(p.dispatchId);
      return;
    }
    if (current.status !== previous) {
      if (canReach(current.status, previous)) {
        throw new AwaitingPredecessorError(`Dispatch ${p.dispatchId} is awaiting ${previous}`);
      }
      return; // A replay of an already surpassed transition.
    }
    const changed = await this.redis.eval(transitionScript, 2, dispatchStateKey(p.dispatchId), terminalKey(p.dispatchId),
      current.status, next, JSON.stringify(p.destination ?? current.destination), event.occurredAt);
    if (changed === 1 && next === 'TRANSPORTING' && p.destination) await this.routes.requestImmediate(p.dispatchId);
  }

  async completeDispatch(dispatchId: string, occurredAt = new Date().toISOString()): Promise<void> {
    const current = await this.store.findById(dispatchId);
    // Durable completion intent prevents a replay from resurrecting an already-cleared dispatch.
    await this.redis.set(terminalKey(dispatchId), occurredAt, 'NX');
    await this.store.completeAndEnqueueClears(dispatchId, occurredAt);
    if (current) await this.removeIndex(dispatchId, current.emergencyVehicleId);
    else await this.redis.srem(ACTIVE_DISPATCHES_KEY, dispatchId);
  }

  async findActiveByVehicleId(vehicleId: string): Promise<DispatchState | null> {
    const id = await this.redis.get(dispatchVehicleKey(vehicleId));
    if (!id) return null;
    const state = await this.store.findById(id);
    if (!state) { await this.removeIndex(id, vehicleId); return null; }
    return state;
  }

  async findActiveDispatches(): Promise<DispatchState[]> {
    const ids = await this.redis.smembers(ACTIVE_DISPATCHES_KEY);
    const states = await Promise.all(ids.map(id => this.store.findById(id)));
    return states.filter((state): state is DispatchState => state !== null);
  }
  private async removeIndex(id: string, vehicleId: string): Promise<void> {
    await this.redis.eval(removeIndexScript, 2, ACTIVE_DISPATCHES_KEY, dispatchVehicleKey(vehicleId), id);
  }
}
