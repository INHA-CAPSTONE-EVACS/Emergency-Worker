import {latLngToCell} from 'h3-js';
import {loadConfig} from '../../src/config/config';
import {Worker} from '../../src/worker';
import {cellVehiclesKey, vehicleStateKey, dispatchStateKey, parseStreamEvent, DENSITY_META_KEY,
  DENSITY_CELLS_KEY, DENSITY_EDGES_KEY} from '../../src/domain/contracts';
import type {VehicleState} from '../../src/domain/types';
import {RedisAlertOutboxDispatcher} from '../../src/publishing/redis-alert-outbox-dispatcher';
import {YieldDecisionService} from '../../src/decision/yield-decision-service';
import {DispatchRouteRepository} from '../../src/route/dispatch-route-repository';
import {RouteScheduler} from '../../src/route/route-scheduler';
import {DispatchStateStore} from '../../src/store/dispatch-state-store';
import {redisFixture, decision} from './store/fixtures';
import {postgresFixture, dispatch, input} from './route/fixtures';
import {target, vehicle} from '../unit/decision/fixtures';

const eventually = async (check: () => Promise<boolean>) => {
  const end = Date.now() + 4000;
  while (Date.now() < end) {if (await check()) return; await new Promise(r => setTimeout(r, 25));}
  throw new Error('Condition did not become true');
};
describe('composed worker', () => {
  const pool = postgresFixture();
  let fixture: ReturnType<typeof redisFixture>, worker: Worker;
  const config = {...loadConfig({REDIS_URL: process.env.REDIS_URL ?? 'redis://127.0.0.1:6401',
    ROUTE_PERSISTENCE_ENABLED: 'false', BLOCK_MS: '20', PENDING_MIN_IDLE_MS: '20', INACTIVE_TTL_MS: '60000'}), healthPort: 0};
  const seed = async (value: VehicleState) => {
    await fixture.redis.hset(vehicleStateKey(value.vehicleId), value as unknown as Record<string, string | number>);
    await fixture.redis.zadd(cellVehiclesKey(value.h3CellId), value.lastMeasuredAt, value.vehicleId);
  };
  const notify = async (value: VehicleState, h3CellId = value.h3CellId) => fixture.redis.xadd(config.stateStream, '*', 'event',
    JSON.stringify({eventId: `state-${value.vehicleId}-${value.stateVersion}`, eventType: 'VEHICLE_STATE_UPDATED',
      schemaVersion: 1, producer: 'location-worker', occurredAt: new Date().toISOString(), payload: {
        vehicleId: value.vehicleId, vehicleType: value.vehicleType, h3CellId, previousH3CellId: null,
        stateVersion: value.stateVersion, measuredAt: new Date(value.lastMeasuredAt).toISOString()}}));
  const status = async (path = '/readyz') => (await fetch(`${worker.healthUrl}${path}`)).status;
  const quiet = {info() {}, error() {}};
  beforeEach(() => {fixture = redisFixture(); worker = new Worker(config, {redis: fixture.redis, logger: quiet});});
  afterEach(async () => {jest.restoreAllMocks(); await worker.stop(); await fixture.close();});

  test('recovers committed alert intent on restart, projects Gateway payload and emits one canonical clear', async () => {
    const logs: Record<string, unknown>[] = [];
    const unavailable = jest.spyOn(RedisAlertOutboxDispatcher.prototype, 'dispatchPending')
      .mockRejectedValue(new Error('publisher stopped'));
    await worker.start();
    const emergency = vehicle({lastMeasuredAt: Date.now()}), normal = target({lastMeasuredAt: Date.now()});
    await seed(emergency); await seed(normal);
    await fixture.redis.xadd(config.emergencyStream, '*', 'event', JSON.stringify({eventId: 'dispatch',
      eventType: 'EMERGENCY_DISPATCHED', schemaVersion: 1, producer: 'emergency-service', occurredAt: new Date().toISOString(),
      payload: {dispatchId: dispatch.dispatchId, emergencyId: dispatch.emergencyId, vehicleId: emergency.vehicleId,
        origin: dispatch.origin, destination: dispatch.destination}}));
    await eventually(async () => await fixture.redis.exists(dispatchStateKey(dispatch.dispatchId)) === 1);
    await notify(emergency);
    await eventually(async () => await fixture.redis.xlen(config.alertOutboxStream) === 1);
    expect(await fixture.redis.xlen(config.gatewayAlertStream)).toBe(0);
    expect(await worker.stop()).toEqual({drained: true});
    unavailable.mockRestore();
    worker = new Worker(config, {redis: fixture.redis, logger: {error() {}, info(_message, fields) {logs.push(fields ?? {});}}});
    await worker.start();
    await eventually(async () => await fixture.redis.xlen(config.gatewayAlertStream) === 1);
    const rows = await fixture.redis.xrange(config.gatewayAlertStream, '-', '+');
    expect(JSON.parse(rows[0]![1][rows[0]![1].indexOf('payload') + 1]!)).toMatchObject({
      vehicleId: 'target', emergencyVehicleId: 'ambulance', alertVersion: 1});
    const moved = target({latitude: -0.001, stateVersion: 2, lastMeasuredAt: Date.now()});
    await seed(moved); await notify(moved); await notify(moved);
    await eventually(async () => await fixture.redis.xlen(config.alertStream) === 2);
    expect((await fixture.redis.xrange(config.alertStream, '-', '+')).map(([, fields]) => parseStreamEvent(fields).eventType))
      .toEqual(['YIELD_ALERT_REQUESTED', 'YIELD_ALERT_CLEAR_REQUESTED']);
    expect(await fixture.redis.xlen(config.gatewayAlertStream)).toBe(1);
    expect(logs).toContainEqual(expect.objectContaining({eventType: 'YIELD_ALERT_CLEAR_REQUESTED',
      targetVehicleId: 'target', reason: 'EMERGENCY_PASSED'}));
    expect(await status()).toBe(200);
  });

  test('bounds shutdown and leaves an unfinished decision pending for restart', async () => {
    let release!: () => void, entered = false;
    const gate = new Promise<void>(resolve => {release = resolve;});
    // Delay the real decision entry point to model an in-flight backing operation.
    jest.spyOn(YieldDecisionService.prototype, 'onEmergencyState').mockImplementation(async () => {entered = true; await gate;});
    worker = new Worker({...config, shutdownDrainTimeoutMs: 30}, {redis: fixture.redis, logger: quiet});
    await new DispatchStateStore(fixture.redis, config).save(dispatch);
    await fixture.redis.set('dispatch:vehicle:ambulance', dispatch.dispatchId);
    await worker.start();
    await notify(vehicle({lastMeasuredAt: Date.now()}));
    await eventually(async () => entered);
    try {
      expect(await worker.stop()).toEqual({drained: false});
      expect((await fixture.redis.xpending(config.stateStream, config.vehicleConsumerGroup))[0]).toBe(1);
    } finally {release();}
  });

  test('closes resources and reports undrained when scheduler draining fails', async () => {
    const stopScheduler = RouteScheduler.prototype.stop;
    jest.spyOn(RouteScheduler.prototype, 'stop').mockImplementation(async function(this: RouteScheduler) {
      await stopScheduler.call(this);
      throw new Error('route backing dependency failed during drain');
    });
    worker = new Worker({...config, routePersistenceEnabled: true}, {redis: fixture.redis, pool, logger: quiet});
    await worker.start();
    await expect(worker.stop()).resolves.toEqual({drained: false});
    await expect(fetch(`${worker.healthUrl}/livez`)).rejects.toThrow();
  });

  test('selects only targets excluded by the new route for PostgreSQL route-change clears', async () => {
    const repository = new DispatchRouteRepository(pool);
    await repository.saveNewVersionWithOutbox(input());
    const store = new DispatchStateStore(fixture.redis, config);
    await store.save({...dispatch, destination: {latitude: 0, longitude: 0.001}});
    await fixture.redis.sadd('dispatch:active', dispatch.dispatchId);
    const emergency = vehicle({headingDegrees: 45, lastMeasuredAt: Date.now()});
    const excluded = target({vehicleId: 'excluded', latitude: 0.0008, lastMeasuredAt: Date.now()});
    const retained = target({vehicleId: 'retained', latitude: 0, longitude: 0.0008, headingDegrees: 90,
      lastMeasuredAt: Date.now()});
    await seed(emergency); await seed(excluded); await seed(retained);
    await fixture.redis.set(`dispatch:${dispatch.dispatchId}:decisions`, JSON.stringify(
      ['excluded', 'retained'].map(id => ({...decision(id), dispatchId: dispatch.dispatchId,
        emergencyId: dispatch.emergencyId, emergencyVehicleId: 'ambulance',
        issuedAt: new Date().toISOString(), expiresAt: new Date(Date.now() + 20000).toISOString()}))));
    await fixture.redis.hset(DENSITY_META_KEY, {generation: 'selector', generatedAt: new Date().toISOString(),
      h3Resolution: '9', edgeDensity: 'disabled:no-table'});
    await fixture.redis.hset(DENSITY_CELLS_KEY, '_generation', 'selector');
    worker = new Worker({...config, routePersistenceEnabled: true, graphPath: 'test/fixtures/road-graph.json', routeIntervalMs: 1000},
      {redis: fixture.redis, pool, logger: quiet});
    await worker.start();
    await eventually(async () => (await repository.findLatest(dispatch.dispatchId))?.routeVersion === 2);
    const clears = (await pool.query("SELECT event_key, event FROM emergency_route_outbox WHERE event->>'eventType' = 'YIELD_ALERT_CLEAR_REQUESTED'")).rows;
    expect(clears).toEqual([{event_key: `${dispatch.dispatchId}:route:2:YIELD_ALERT_CLEAR_REQUESTED:excluded`,
      event: expect.objectContaining({eventId: `${dispatch.dispatchId}:route:2:YIELD_ALERT_CLEAR_REQUESTED:excluded`,
        payload: {dispatchId: dispatch.dispatchId, alertId: 'alert-excluded', targetVehicleId: 'excluded', reason: 'ROUTE_CHANGED'}})}]);
    await eventually(async () => (await fixture.redis.xrange(config.alertStream, '-', '+'))
      .some(([, fields]) => parseStreamEvent(fields).eventType === 'YIELD_ALERT_CLEAR_REQUESTED'));
    expect(await fixture.redis.xlen(config.gatewayAlertStream)).toBe(0);
  });

  test('keeps H3 mismatch pending and reports blocked readiness while liveness stays healthy', async () => {
    await worker.start();
    const value = vehicle({lastMeasuredAt: Date.now()});
    await notify(value, latLngToCell(0, 0, 8));
    await eventually(async () => await status() === 503);
    expect((await fixture.redis.xpending(config.stateStream, config.vehicleConsumerGroup))[0]).toBe(1);
    expect(await fixture.redis.xlen(config.dlqStream)).toBe(0);
    expect(await status('/livez')).toBe(200);
  });

  test('becomes unready after an owned Redis connection disconnects', async () => {
    await worker.start();
    expect(await status()).toBe(200);
    const clients = String(await fixture.redis.client('LIST')).split('\n');
    const density = clients.find(line => line.includes(`name=${config.instanceId}:density `));
    expect(density).toBeDefined();
    const id = /id=(\d+)/.exec(density!)![1]!;
    await fixture.redis.client('KILL', 'ID', id);
    await eventually(async () => await status() === 503);
    expect(await status('/livez')).toBe(200);
  });

  test('schedules and publishes a durable route using the graph and density connection', async () => {
    worker = new Worker({...config, routePersistenceEnabled: true, graphPath: 'test/fixtures/road-graph.json'},
      {redis: fixture.redis, pool, logger: quiet});
    await fixture.redis.hset(DENSITY_META_KEY, {generation: 'route', generatedAt: new Date().toISOString(),
      h3Resolution: '9', edgeDensity: 'disabled:no-table'});
    await fixture.redis.hset(DENSITY_CELLS_KEY, '_generation', 'route');
    await seed(vehicle({lastMeasuredAt: Date.now()}));
    await worker.start();
    await fixture.redis.xadd(config.emergencyStream, '*', 'event', JSON.stringify({eventId: 'route-dispatch',
      eventType: 'EMERGENCY_DISPATCHED', schemaVersion: 1, producer: 'emergency-service', occurredAt: new Date().toISOString(),
      payload: {dispatchId: dispatch.dispatchId, emergencyId: dispatch.emergencyId, vehicleId: 'ambulance',
        origin: dispatch.origin, destination: dispatch.destination}}));
    await eventually(async () => await fixture.redis.xlen(config.alertStream) === 1);
    const [row] = await fixture.redis.xrange(config.alertStream, '-', '+');
    expect(parseStreamEvent(row![1])).toMatchObject({eventType: 'ROUTE_UPDATE_REQUESTED',
      payload: {routeVersion: 1, targetVehicleId: 'ambulance', estimatedTravelSeconds: 20}});
    expect((await pool.query('SELECT road_segments FROM dispatch_route')).rows[0].road_segments).toEqual(['ac', 'cd']);
    expect(await status()).toBe(200);
  });

  test('reports a required PostgreSQL failure and density configuration mismatch', async () => {
    worker = new Worker({...config, routePersistenceEnabled: true}, {redis: fixture.redis, pool, logger: quiet});
    await fixture.redis.hset(DENSITY_META_KEY, {generation: '1', generatedAt: new Date().toISOString(), h3Resolution: '9', edgeDensity: 'disabled:no-table'});
    await fixture.redis.hset(DENSITY_CELLS_KEY, '_generation', '1');
    await fixture.redis.hset(DENSITY_EDGES_KEY, '_generation', '1');
    await worker.start();
    expect(await status()).toBe(200);
    await fixture.redis.hset(DENSITY_META_KEY, 'h3Resolution', '8');
    expect(await status()).toBe(503);
    await fixture.redis.hset(DENSITY_META_KEY, 'h3Resolution', '9');
    expect(await status()).toBe(200);
    await pool.query('ALTER TABLE emergency_route_outbox RENAME TO temporarily_unavailable');
    try {expect(await status()).toBe(503); expect(await status('/livez')).toBe(200);}
    finally {await pool.query('ALTER TABLE temporarily_unavailable RENAME TO emergency_route_outbox');}
  });
});
