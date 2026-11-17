import {RouteScheduler, type SchedulerPlanner, type RouteSchedulerDependencies} from '../../../src/route/route-scheduler';
import {DispatchRouteRepository} from '../../../src/route/dispatch-route-repository';
import {RouteStabilizer} from '../../../src/route/route-stabilizer';
import {RouteOutboxDispatcher} from '../../../src/route/route-outbox-dispatcher';
import {EdgeWeightCalculator} from '../../../src/route/edge-weight-calculator';
import {RoutePlanner} from '../../../src/route/route-planner';
import {RoadGraph} from '../../../src/route/road-graph';
import {DispatchStateStore} from '../../../src/store/dispatch-state-store';
import {VehicleIndexReader} from '../../../src/store/vehicle-index-reader';
import {RedisStreamEventPublisher} from '../../../src/publishing/redis-stream-event-publisher';
import {DENSITY_CELLS_KEY, DENSITY_EDGES_KEY, DENSITY_META_KEY, dispatchDecisionsKey, vehicleStateKey, parseStreamEvent} from '../../../src/domain/contracts';
import {loadConfig} from '../../../src/config/config';
import {redisFixture, decision} from '../store/fixtures';
import {postgresFixture, dispatch, input} from './fixtures';
import graphDocument from '../../fixtures/road-graph.json';

describe('Slow-Path scheduler', () => {
  const pool = postgresFixture(), {redis, close} = redisFixture();
  const config = loadConfig({BPR_ALPHA: '0.5', BPR_BETA: '2', ROAD_CAPACITY: '10', BLOCKED_MULTIPLIER: '3'});
  const repository = new DispatchRouteRepository(pool), dispatches = new DispatchStateStore(redis, config);
  const vehicles = new VehicleIndexReader(redis), weights = new EdgeWeightCalculator(redis, config);
  const planner = new RoutePlanner(RoadGraph.load(graphDocument), weights);
  let now = Date.parse('2026-11-16T12:00:06.000Z');
  const makeScheduler = (routePlanner: SchedulerPlanner = planner, overrides: Partial<RouteSchedulerDependencies> = {}) => new RouteScheduler({
    lifecycle: {findActiveDispatches: async () => [dispatch]}, dispatches, vehicles, weights,
    planner: routePlanner, repository, stabilizer: new RouteStabilizer(), config, now: () => now,
    selectRouteChangeClears: (_dispatch, _previous, _next, decisions) => decisions.map(value => ({alertId: value.alertId, targetVehicleId: value.targetVehicleId})),
    ...overrides,
  });
  beforeEach(async () => {
    now = Date.parse('2026-11-16T12:00:06.000Z');
    await dispatches.save(dispatch);
    await redis.del(dispatchDecisionsKey(dispatch.dispatchId), DENSITY_CELLS_KEY, DENSITY_EDGES_KEY, DENSITY_META_KEY);
    await redis.hset(DENSITY_CELLS_KEY, '_generation', 'g1');
    await redis.hset(DENSITY_EDGES_KEY, '_generation', 'g1', 'ac', '20');
    await redis.hset(DENSITY_META_KEY, 'generation', 'g1', 'generatedAt', dispatch.updatedAt, 'h3Resolution', '9', 'edgeDensity', 'enabled');
    await redis.hset(vehicleStateKey(dispatch.emergencyVehicleId), {
      vehicleId: dispatch.emergencyVehicleId, vehicleType: 'EMERGENCY', latitude: '0', longitude: '0',
      headingDegrees: '0', speedMps: '10', h3CellId: '8930e0a845bffff', lastSequence: '1',
      lastMeasuredAt: String(now), lastUpdatedAt: dispatch.updatedAt, stateVersion: '1', status: 'ACTIVE',
    });
  });
  afterAll(close);
  test('matching snapshot emits one versioned route request with congested segments and current vehicle origin', async () => {
    await redis.hset(vehicleStateKey(dispatch.emergencyVehicleId), 'longitude', '0.001');
    const scheduler = makeScheduler();
    expect(await scheduler.tick()).toMatchObject({outcome: 'APPLIED', routesCommitted: 1, density: {generation: 'g1', degraded: false}});
    await scheduler.tick();
    const delivery = new RouteOutboxDispatcher(repository, new RedisStreamEventPublisher(redis, config), 'scheduler-route');
    await delivery.dispatchPending();
    const events = (await redis.xrange('scheduler-route', '-', '+')).map(([, fields]) => parseStreamEvent(fields));
    expect(events).toHaveLength(1);
    expect(events[0]).toMatchObject({eventType: 'ROUTE_UPDATE_REQUESTED', payload: {routeVersion: 1,
      waypoints: [{latitude: 0, longitude: 0.001}, {latitude: 0.001, longitude: 0.001}], congestedSegments: [], estimatedTravelSeconds: 10}});
  });
  test('generation mismatch skips routing; H3 mismatch stays configuration-blocked; disabled density is degraded', async () => {
    const scheduler = makeScheduler();
    await redis.hset(DENSITY_EDGES_KEY, '_generation', 'g2');
    expect(await scheduler.tick()).toMatchObject({outcome: 'SKIPPED', error: {code: 'GENERATION_MISMATCH'}, routesCommitted: 0});
    expect(await repository.findLatest(dispatch.dispatchId)).toBeNull();
    await redis.hset(DENSITY_META_KEY, 'h3Resolution', '8');
    expect(await scheduler.tick()).toMatchObject({outcome: 'CONFIGURATION_BLOCKED', error: {code: 'H3_RESOLUTION_MISMATCH', configurationBlocked: true}});
    await redis.hset(DENSITY_META_KEY, 'h3Resolution', '9', 'edgeDensity', 'disabled:no-table');
    expect(await scheduler.tick()).toMatchObject({outcome: 'APPLIED', density: {degraded: true, edgeDensity: 'disabled:no-table'}, routesCommitted: 1});
    expect((await repository.findLatest(dispatch.dispatchId))?.estimatedDuration).toBe(20);
  });
  test('a replacement creates a known affected target clear in the same route transaction', async () => {
    await repository.saveNewVersionWithOutbox(input());
    await redis.set(dispatchDecisionsKey(dispatch.dispatchId), JSON.stringify({target: {...decision('target'), dispatchId: dispatch.dispatchId}}));
    expect(await makeScheduler().tick()).toMatchObject({routesCommitted: 1});
    expect((await repository.findLatest(dispatch.dispatchId))?.routeVersion).toBe(2);
    const records = await repository.findPendingOutbox();
    expect(records).toHaveLength(3);
    expect(records[2]?.event).toMatchObject({eventType: 'YIELD_ALERT_CLEAR_REQUESTED', payload: {targetVehicleId: 'target', reason: 'ROUTE_CHANGED'}});
  });
  test('immediate bursts and timer ticks never overlap a search and coalesce to one follow-up', async () => {
    let release!: () => void, entered!: () => void, searches = 0, concurrent = 0, maximum = 0;
    const gate = new Promise<void>(resolve => {release = resolve;});
    const started = new Promise<void>(resolve => {entered = resolve;});
    const scheduler = makeScheduler({findRoute: async (origin, destination, snapshot) => {
      searches++; concurrent++; maximum = Math.max(maximum, concurrent); entered();
      await gate; concurrent--;
      return planner.findRoute(origin, destination, snapshot);
    }});
    const tick = scheduler.tick();
    await started;
    for (let i = 0; i < 10; i++) scheduler.requestImmediate(dispatch.dispatchId);
    const duplicate = scheduler.tick();
    release();
    await Promise.all([tick, duplicate]);
    await scheduler.stop();
    expect(maximum).toBe(1);
    expect(searches).toBe(2);
    expect((await repository.findPendingOutbox()).filter(record => record.event.eventType === 'ROUTE_UPDATE_REQUESTED')).toHaveLength(1);
  });
  test('one density snapshot is shared by all active dispatches in a tick', async () => {
    const second = {...dispatch, dispatchId: 'd9e967a7-0697-42ec-9118-fb66c7c5bad2'};
    await dispatches.save(second);
    const load = jest.spyOn(weights, 'loadDensitySnapshot');
    const scheduler = makeScheduler(planner, {lifecycle: {findActiveDispatches: async () => [dispatch, second]}});
    expect(await scheduler.tick()).toMatchObject({routesCommitted: 2});
    expect(load).toHaveBeenCalledTimes(1);
    const records = await repository.findPendingOutbox();
    expect(records.map(record => record.event.payload)).toEqual([dispatch, second].map(value => ({
      dispatchId: value.dispatchId, targetVehicleId: 'ambulance', routeVersion: 1, destination: value.destination,
      waypoints: [{latitude: 0, longitude: 0}, {latitude: 0, longitude: 0.001}, {latitude: 0.001, longitude: 0.001}],
      congestedSegments: ['ac'], estimatedTravelSeconds: 25,
    })));
    load.mockRestore();
  });
  test('start uses its own interval, is repeatable, and stop cancels future ticks', async () => {
    jest.useFakeTimers({doNotFake: ['nextTick', 'setImmediate', 'queueMicrotask']});
    const load = jest.spyOn(weights, 'loadDensitySnapshot');
    const scheduler = makeScheduler();
    try {
      scheduler.start(); scheduler.start();
      await jest.advanceTimersByTimeAsync(2000);
      await scheduler.stop();
      expect((await repository.findLatest(dispatch.dispatchId))?.routeVersion).toBe(1);
      await jest.advanceTimersByTimeAsync(4000);
      expect(load).toHaveBeenCalledTimes(1);
    } finally {
      await scheduler.stop(); load.mockRestore(); jest.useRealTimers();
    }
  });
  test('stale emergency state and disconnected routes leave the database unchanged', async () => {
    await redis.hset(vehicleStateKey(dispatch.emergencyVehicleId), 'lastMeasuredAt', String(now - 5000));
    expect(await makeScheduler().tick()).toMatchObject({outcome: 'SKIPPED', routesCommitted: 0});
    await redis.hset(vehicleStateKey(dispatch.emergencyVehicleId), 'lastMeasuredAt', String(now), 'latitude', '0.001', 'longitude', '0.001');
    await dispatches.save({...dispatch, destination: {latitude: 0, longitude: 0}});
    expect(await makeScheduler().tick()).toMatchObject({outcome: 'SKIPPED', routesCommitted: 0});
    expect(await repository.findLatest(dispatch.dispatchId)).toBeNull();
  });
  test('a dispatch completing during planning is not given a route', async () => {
    const scheduler = makeScheduler({findRoute: async (origin, destination, snapshot) => {
      await dispatches.completeAndEnqueueClears(dispatch.dispatchId);
      return planner.findRoute(origin, destination, snapshot);
    }});
    expect(await scheduler.tick()).toMatchObject({routesCommitted: 0});
    expect(await repository.findPendingOutbox()).toHaveLength(0);
  });
});
