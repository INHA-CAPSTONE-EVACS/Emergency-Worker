import {DispatchLifecycleService} from '../../../src/dispatch/dispatch-lifecycle-service';
import {EmergencyEventSubscriber} from '../../../src/dispatch/emergency-event-subscriber';
import {DispatchStateStore} from '../../../src/store/dispatch-state-store';
import {DecisionStore} from '../../../src/store/decision-store';
import {parseStreamEvent, type EmergencyEvent} from '../../../src/domain/contracts';
import {redisFixture, config, state, decision, request} from '../store/fixtures';

const dispatched: EmergencyEvent = {eventId: 'dispatch-event', eventType: 'EMERGENCY_DISPATCHED', schemaVersion: 1,
  occurredAt: state.updatedAt, producer: 'dispatch-service', payload: {emergencyId: state.emergencyId,
    dispatchId: state.dispatchId, vehicleId: state.emergencyVehicleId, origin: state.origin, destination: state.destination}};
const status = (previousStatus: string, currentStatus: string, destination?: typeof state.destination) => ({
  ...dispatched, eventId: `status-${currentStatus}`, eventType: 'EMERGENCY_STATUS_CHANGED',
  payload: {emergencyId: state.emergencyId, dispatchId: state.dispatchId, previousStatus, currentStatus, ...(destination ? {destination} : {})},
}) as EmergencyEvent;

describe('dispatch lifecycle', () => {
  const {redis, close} = redisFixture();
  const store = new DispatchStateStore(redis, config);
  const routes: string[] = [];
  const service = new DispatchLifecycleService(redis, store, {requestImmediate: async dispatchId => { routes.push(dispatchId); }});
  const subscriber = new EmergencyEventSubscriber(service);
  beforeEach(async () => {
    const keys = ['dispatch:active', 'dispatch:vehicle:ambulance-1', 'dispatch:dispatch-1:state',
      'dispatch:dispatch-1:cone', 'dispatch:dispatch-1:decisions', 'dispatch:dispatch-1:completed', config.alertOutboxStream];
    await redis.del(...keys); routes.length = 0;
  });
  afterAll(close);

  test('dispatch activation survives replay without resetting versions and requests initial routing', async () => {
    await subscriber.handle(dispatched);
    expect(await service.findActiveByVehicleId('ambulance-1')).toEqual(state);
    expect(routes).toEqual(['dispatch-1']);
    await store.save({...state, decisionVersion: 3, routeVersion: 2});
    await subscriber.handle(dispatched);
    expect(await service.findActiveDispatches()).toEqual([{...state, decisionVersion: 3, routeVersion: 2}]);
  });

  test('concurrent activations keep one dispatch per vehicle and reject conflicting dispatch identity', async () => {
    const second = {...dispatched, eventId: 'second-dispatch', payload: {...dispatched.payload,
      dispatchId: 'dispatch-2'}} as typeof dispatched;
    await Promise.all([subscriber.handle(dispatched), subscriber.handle(second)]);
    expect(await service.findActiveDispatches()).toHaveLength(1);
    const active = await service.findActiveByVehicleId('ambulance-1');
    expect(active?.dispatchId).toBe('dispatch-1');
    await service.completeDispatch('dispatch-1');
    await redis.del('dispatch:dispatch-1:completed');
    const conflicting = {...dispatched, eventId: 'conflicting-dispatch', payload: {...dispatched.payload,
      vehicleId: 'ambulance-2'}} as typeof dispatched;
    await Promise.all([subscriber.handle(dispatched), subscriber.handle(conflicting)]);
    expect(await service.findActiveByVehicleId('ambulance-2')).toBeNull();
    await redis.del('dispatch:vehicle:ambulance-2', 'dispatch:dispatch-2:state');
  });

  test('route scheduler failures remain retryable after initial and next-leg state commits', async () => {
    let failing = true;
    const scheduled: string[] = [];
    const reliable = new EmergencyEventSubscriber(new DispatchLifecycleService(redis, store, {
      requestImmediate: async id => { if (failing) throw new Error('scheduler unavailable'); scheduled.push(id); },
    }));
    await expect(reliable.handle(dispatched)).rejects.toThrow('scheduler unavailable');
    expect(await service.findActiveByVehicleId('ambulance-1')).toMatchObject({status: 'DISPATCHED'});
    failing = false;
    await reliable.handle(dispatched);
    await reliable.handle(status('DISPATCHED', 'ARRIVED'));
    failing = true;
    const transport = status('ARRIVED', 'TRANSPORTING', {latitude: 37.5, longitude: 126.7});
    await expect(reliable.handle(transport)).rejects.toThrow('scheduler unavailable');
    expect(await service.findActiveByVehicleId('ambulance-1')).toMatchObject({status: 'TRANSPORTING'});
    failing = false;
    await reliable.handle(transport);
    expect(scheduled).toEqual(['dispatch-1', 'dispatch-1']);
  });

  test.each(['COMPLETED', 'CANCELED'])('%s atomically clears every target and removes dispatch indexes', async terminal => {
    await subscriber.handle(dispatched);
    const decisions = ['a', 'b'].map(id => decision(id));
    await new DecisionStore(redis, config).compareSaveAndEnqueue(state.dispatchId, 0, decisions, decisions.map(request));
    await redis.hset('vehicle:ambulance-1:state', 'status', 'ACTIVE');
    await subscriber.handle(status('DISPATCHED', terminal));
    expect(await service.findActiveDispatches()).toEqual([]);
    expect(await service.findActiveByVehicleId('ambulance-1')).toBeNull();
    expect(await store.findById('dispatch-1')).toBeNull();
    const clearRecords = (await redis.xrange(config.alertOutboxStream, '-', '+')).slice(2).map(([, fields]) => parseStreamEvent(fields));
    expect(clearRecords.map(record => record.payload)).toEqual(['a', 'b'].map(targetVehicleId => ({
      dispatchId: 'dispatch-1', targetVehicleId, alertId: `alert-${targetVehicleId}`, reason: 'EMERGENCY_COMPLETED'})));
    await subscriber.handle(status('DISPATCHED', terminal));
    await subscriber.handle(dispatched);
    expect(await store.findById('dispatch-1')).toBeNull();
    expect(await redis.xlen(config.alertOutboxStream)).toBe(4);
    expect(await redis.hget('vehicle:ambulance-1:state', 'status')).toBe('ACTIVE');
  });

  test('failed completion retains active dispatch and decisions for retry', async () => {
    await subscriber.handle(dispatched);
    await new DecisionStore(redis, config).compareSaveAndEnqueue(state.dispatchId, 0, [decision()], [request(decision())]);
    await redis.del(config.alertOutboxStream);
    await redis.set(config.alertOutboxStream, 'wrong-type');
    await expect(subscriber.handle(status('DISPATCHED', 'CANCELED'))).rejects.toThrow();
    expect(await service.findActiveByVehicleId('ambulance-1')).toMatchObject({status: 'DISPATCHED'});
    expect(await store.findDecisions('dispatch-1')).toHaveLength(1);
  });

  test('normalizes ARRIVED then retains dispatch and requests next-leg routing on TRANSPORTING', async () => {
    await subscriber.handle(dispatched);
    await subscriber.handle(status('DISPATCHED', 'ARRIVED'));
    expect(await service.findActiveByVehicleId('ambulance-1')).toMatchObject({status: 'ARRIVED_ON_SCENE'});
    const destination = {latitude: 37.5, longitude: 126.7};
    await subscriber.handle(status('ARRIVED', 'TRANSPORTING', destination));
    expect(await service.findActiveByVehicleId('ambulance-1')).toMatchObject({status: 'TRANSPORTING', destination});
    expect(routes).toEqual(['dispatch-1', 'dispatch-1']);
  });

  test('ignores illegal backwards status changes without losing active state', async () => {
    await subscriber.handle(dispatched);
    await subscriber.handle(status('DISPATCHED', 'TRANSPORTING'));
    expect(await service.findActiveByVehicleId('ambulance-1')).toMatchObject({status: 'DISPATCHED'});
  });
});
