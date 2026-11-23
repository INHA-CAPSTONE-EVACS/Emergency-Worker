import {DispatchLifecycleService} from '../../../src/dispatch/dispatch-lifecycle-service';
import {EmergencyEventSubscriber} from '../../../src/dispatch/emergency-event-subscriber';
import {DispatchStateStore} from '../../../src/store/dispatch-state-store';
import {DecisionStore} from '../../../src/store/decision-store';
import {parseStreamEvent, type EmergencyEvent} from '../../../src/domain/contracts';
import {redisFixture, config, cone, decision, request} from '../store/fixtures';

/** EVACS-API-Server contract: one vehicle per emergency, dispatch identified by the emergency ID. */
const emergencyId = '5ef3cd03-9313-41e9-93f9-6365625e90a1';
const origin = {latitude: 37.4462, longitude: 126.6504}, destination = {latitude: 37.451, longitude: 126.655};
const created = (payload: Record<string, unknown> = {}): EmergencyEvent => ({eventId: 'created-1', eventType: 'EMERGENCY_CREATED',
  schemaVersion: 1, occurredAt: '2026-11-23T06:43:25.308Z', producer: 'control-api', payload: {emergencyId,
    vehicleId: 'EVACS_EMG_1062', emergencyType: 'MEDICAL', status: 'ACTIVE', origin, destination, ...payload}} as EmergencyEvent);
const ended: EmergencyEvent = {eventId: 'ended-1', eventType: 'EMERGENCY_ENDED', schemaVersion: 1,
  occurredAt: '2026-11-23T06:45:00.000Z', producer: 'control-api',
  payload: {emergencyId, vehicleId: 'EVACS_EMG_1062', status: 'ENDED', endedAt: '2026-11-23T06:45:00.000Z'}};

describe('Control API Server emergency lifecycle', () => {
  const {redis, close} = redisFixture();
  const store = new DispatchStateStore(redis, config);
  const routes: string[] = [];
  const service = new DispatchLifecycleService(redis, store, {requestImmediate: async id => { routes.push(id); }});
  const subscriber = new EmergencyEventSubscriber(service);
  const coneKey = `evacs:emergency:${emergencyId}:cone`;
  beforeEach(async () => {
    await redis.del('dispatch:active', 'dispatch:vehicle:EVACS_EMG_1062', `dispatch:${emergencyId}:state`,
      `dispatch:${emergencyId}:cone`, `dispatch:${emergencyId}:decisions`, `dispatch:${emergencyId}:completed`,
      coneKey, config.alertOutboxStream);
    routes.length = 0;
  });
  afterAll(close);

  test('EMERGENCY_CREATED with a vehicle starts a dispatch keyed by the emergency ID and requests routing', async () => {
    await subscriber.handle(created());
    expect(await service.findActiveByVehicleId('EVACS_EMG_1062')).toEqual({dispatchId: emergencyId, emergencyId,
      emergencyVehicleId: 'EVACS_EMG_1062', status: 'DISPATCHED', origin, destination, decisionVersion: 0,
      routeVersion: 0, updatedAt: '2026-11-23T06:43:25.308Z'});
    expect(routes).toEqual([emergencyId]);
  });

  test('missing origin/destination fall back to the registered route ends', async () => {
    await subscriber.handle(created({origin: undefined, destination: undefined,
      route: [{sequence: 0, ...origin}, {sequence: 1, latitude: 37.449, longitude: 126.652}, {sequence: 2, ...destination}]}));
    expect(await store.findById(emergencyId)).toMatchObject({origin, destination});
  });

  test('a registration without a vehicle starts nothing', async () => {
    await subscriber.handle(created({vehicleId: undefined}));
    expect(await service.findActiveDispatches()).toEqual([]);
    expect(routes).toEqual([]);
  });

  test('EMERGENCY_ENDED clears every alert, removes the dispatch and the dashboard cone', async () => {
    await subscriber.handle(created());
    expect(await store.saveCone(emergencyId, cone)).toBe(true);
    const decisions = ['a', 'b'].map(id => ({...decision(id), dispatchId: emergencyId, emergencyId}));
    await new DecisionStore(redis, config).compareSaveAndEnqueue(emergencyId, 0, decisions, decisions.map(request));
    await subscriber.handle(ended);
    const clears = (await redis.xrange(config.alertOutboxStream, '-', '+')).slice(2).map(([, fields]) => parseStreamEvent(fields));
    expect(clears.map(event => [event.eventType, (event.payload as {reason: string}).reason])).toEqual([
      ['YIELD_ALERT_CLEAR_REQUESTED', 'EMERGENCY_COMPLETED'], ['YIELD_ALERT_CLEAR_REQUESTED', 'EMERGENCY_COMPLETED']]);
    expect(await service.findActiveDispatches()).toEqual([]);
    expect(await redis.exists(coneKey)).toBe(0);
  });

  test('an end processed before its creation (redelivery) keeps the emergency from activating', async () => {
    await subscriber.handle(ended);
    await subscriber.handle(created());
    expect(await service.findActiveDispatches()).toEqual([]);
  });

  test('the accepted cone is projected to the API key with an increasing version', async () => {
    await subscriber.handle(created());
    await store.saveCone(emergencyId, cone);
    const first = JSON.parse((await redis.get(coneKey))!);
    expect(first).toMatchObject({emergencyId, vehicleId: 'EVACS_EMG_1062', version: 1, resolution: 9, cells: cone.cells});
    expect(Date.parse(first.updatedAt)).not.toBeNaN();
    expect(await redis.pttl(coneKey)).toBeGreaterThan(0);
    await store.saveCone(emergencyId, {...cone, cells: ['8930e0a8693ffff', ...cone.cells]});
    expect(JSON.parse((await redis.get(coneKey))!)).toMatchObject({version: 2, cells: ['8930e0a8693ffff', ...cone.cells]});
  });

  test('a cone rejected by the dispatch state is not projected', async () => {
    expect(await store.saveCone(emergencyId, cone)).toBe(false);
    expect(await redis.exists(coneKey)).toBe(0);
  });
});
