import {cellVehiclesKey, parseStreamEvent, vehicleStateKey, YIELD_ALERT_REQUEST_FIELDS, type CanonicalRequestEvent} from '../../../src/domain/contracts';
import type {VehicleState} from '../../../src/domain/types';
import {DispatchLifecycleService} from '../../../src/dispatch/dispatch-lifecycle-service';
import {VehicleStateSubscriber} from '../../../src/dispatch/vehicle-state-subscriber';
import {YieldDecisionService} from '../../../src/decision/yield-decision-service';
import {MovingConeCalculator} from '../../../src/decision/moving-cone-calculator';
import {YieldTargetEvaluator} from '../../../src/decision/yield-target-evaluator';
import {YieldActionResolver} from '../../../src/decision/yield-action-resolver';
import {RedisAlertOutboxDispatcher} from '../../../src/publishing/redis-alert-outbox-dispatcher';
import {DecisionStore} from '../../../src/store/decision-store';
import {DispatchStateStore} from '../../../src/store/dispatch-state-store';
import {VehicleIndexReader} from '../../../src/store/vehicle-index-reader';
import {config, redisFixture, state} from '../store/fixtures';
import {graphDocument, target, turnRoute, vehicle} from '../../unit/decision/fixtures';
import {RoadGraph} from '../../../src/route/road-graph';

describe('Fast-Path decision deltas', () => {
  let fixture: ReturnType<typeof redisFixture>;
  let dispatches: DispatchStateStore;
  let service: YieldDecisionService;
  let decisions: DecisionStore;
  let lifecycle: DispatchLifecycleService;
  const now = Date.parse('2026-11-16T12:00:00.000Z');
  const emergency = vehicle({vehicleId: state.emergencyVehicleId, lastMeasuredAt: now});
  const normal = (overrides: Partial<VehicleState> = {}) => target({lastMeasuredAt: now, ...overrides});
  const seed = async (...states: VehicleState[]) => {
    for (const value of states) {
      await fixture.redis.hset(vehicleStateKey(value.vehicleId), value as unknown as Record<string, string | number>);
      await fixture.redis.zadd(cellVehiclesKey(value.h3CellId), value.lastMeasuredAt, value.vehicleId);
    }
  };
  const events = async () => (await fixture.redis.xrange(config.alertOutboxStream, '-', '+'))
    .map(([, fields]) => parseStreamEvent(fields) as CanonicalRequestEvent);
  beforeEach(async () => {
    fixture = redisFixture();
    dispatches = new DispatchStateStore(fixture.redis, config);
    lifecycle = new DispatchLifecycleService(fixture.redis, dispatches, {requestImmediate: () => {}});
    await lifecycle.onDispatched({eventId: 'dispatch', eventType: 'EMERGENCY_DISPATCHED', schemaVersion: 1,
      producer: 'emergency-service', occurredAt: state.updatedAt, payload: {dispatchId: state.dispatchId,
        emergencyId: state.emergencyId, vehicleId: state.emergencyVehicleId, origin: state.origin, destination: state.destination}});
    decisions = new DecisionStore(fixture.redis, config);
    service = new YieldDecisionService(lifecycle, dispatches, decisions,
      new VehicleIndexReader(fixture.redis), new MovingConeCalculator(), new YieldTargetEvaluator(),
      new YieldActionResolver(), config, () => now);
    await seed(emergency);
  });
  afterEach(async () => { jest.restoreAllMocks(); await fixture.close(); });

  test('publishes only cone/ETA eligible targets with the complete canonical request and exact TTL', async () => {
    await seed(normal(), normal({vehicleId: 'behind', latitude: -0.001}),
      normal({vehicleId: 'side', longitude: 0.002}), normal({vehicleId: 'opposing', headingDegrees: 180}),
      normal({vehicleId: 'too-close', latitude: 0.0004}), normal({vehicleId: 'too-late', latitude: 0.0027, speedMps: 10}),
      normal({vehicleId: 'stale', lastMeasuredAt: now - config.inactiveTtlMs}));
    await service.onEmergencyVehicleMoved(emergency.vehicleId);
    const [event] = await events();
    expect(await events()).toHaveLength(1);
    expect(event).toMatchObject({eventType: 'YIELD_ALERT_REQUESTED', occurredAt: '2026-11-16T12:00:00.000Z',
      payload: {targetVehicleId: 'target', approachDirection: 'REAR', estimatedArrivalSeconds: 13,
        recommendedAction: 'YIELD_WHEN_SAFE', reason: 'BLOCK_SECTION', decisionVersion: 1,
        expiresAt: '2026-11-16T12:00:20.000Z'}});
    expect(Object.keys(event!.payload).sort()).toEqual([...YIELD_ALERT_REQUEST_FIELDS].sort());
    expect(await fixture.redis.xlen(config.alertStream)).toBe(0);
    expect(await dispatches.findCone(state.dispatchId)).toMatchObject({distanceM: 400, headingDegrees: 0});
  });

  test('ignores identical semantic updates even after the clock advances and increments changed decisions', async () => {
    await seed(normal());
    await service.onEmergencyVehicleMoved(emergency.vehicleId);
    const first = (await dispatches.findDecisions(state.dispatchId))[0]!;
    const later = new YieldDecisionService(lifecycle, dispatches, new DecisionStore(fixture.redis, config),
      new VehicleIndexReader(fixture.redis), new MovingConeCalculator(), new YieldTargetEvaluator(),
      new YieldActionResolver(), config, () => now + 1000);
    await later.onEmergencyVehicleMoved(emergency.vehicleId);
    expect(await events()).toHaveLength(1);
    expect(await dispatches.findDecisions(state.dispatchId)).toEqual([first]);
    await seed(normal({speedMps: 10}));
    await later.onNormalVehicleMoved('target');
    expect((await dispatches.findDecisions(state.dispatchId))[0]).toMatchObject({alertId: first.alertId,
      decisionVersion: 2, estimatedArrivalSeconds: 20, issuedAt: '2026-11-16T12:00:01.000Z'});
    await new RedisAlertOutboxDispatcher(fixture.redis, config).dispatchPending();
    const projected = await fixture.redis.xrange(config.gatewayAlertStream, '-', '+');
    expect(projected.map(([, fields]) => JSON.parse(fields[fields.indexOf('payload') + 1]!).alertVersion)).toEqual([1, 2]);
  });

  test('normal updates reevaluate only that target and clear an exit exactly once', async () => {
    await seed(normal(), normal({vehicleId: 'other'}));
    await service.onEmergencyVehicleMoved(emergency.vehicleId);
    // This other state changes in Redis without its notification being processed yet.
    await seed(normal({vehicleId: 'other', speedMps: 10}), normal({latitude: 0, longitude: 0.004}));
    await service.onNormalVehicleMoved('target');
    await service.onNormalVehicleMoved('target');
    expect(await dispatches.findDecisions(state.dispatchId)).toEqual([expect.objectContaining({targetVehicleId: 'other',
      estimatedArrivalSeconds: 13, decisionVersion: 1})]);
    expect((await events()).filter(event => event.eventType === 'YIELD_ALERT_CLEAR_REQUESTED'))
      .toEqual([expect.objectContaining({payload: expect.objectContaining({targetVehicleId: 'target', reason: 'NO_LONGER_RELEVANT'})})]);
  });

  test('emergency passing a target emits one passed clear, including targets outside the rebuilt cone', async () => {
    await seed(normal());
    await service.onEmergencyVehicleMoved(emergency.vehicleId);
    await seed(vehicle({...emergency, latitude: 0.006}));
    await service.onEmergencyVehicleMoved(emergency.vehicleId);
    await service.onEmergencyVehicleMoved(emergency.vehicleId);
    expect(await events()).toHaveLength(2);
    expect((await events())[1]).toMatchObject({eventType: 'YIELD_ALERT_CLEAR_REQUESTED',
      payload: {targetVehicleId: 'target', reason: 'EMERGENCY_PASSED'}});
  });

  test('subscriber Fast-Path ports and concurrent evaluations preserve monotonic versions', async () => {
    await seed(normal());
    const subscriber = new VehicleStateSubscriber(lifecycle, service);
    await subscriber.handle({eventId: 'move', eventType: 'VEHICLE_STATE_UPDATED', schemaVersion: 1,
      producer: 'location-worker', occurredAt: state.updatedAt, payload: {vehicleId: emergency.vehicleId,
        vehicleType: 'EMERGENCY', h3CellId: emergency.h3CellId, previousH3CellId: null, stateVersion: 1, measuredAt: state.updatedAt}});
    await seed(normal({speedMps: 10}));
    await Promise.all(Array.from({length: 5}, () => service.onNormalVehicleMoved('target')));
    expect((await dispatches.findById(state.dispatchId))?.decisionVersion).toBe(2);
    expect(await events()).toHaveLength(2);
  });

  test('concurrent normal updates retry CAS without losing another target delta', async () => {
    await seed(normal(), normal({vehicleId: 'other'}));
    await service.onEmergencyVehicleMoved(emergency.vehicleId);
    await seed(normal({speedMps: 10}), normal({vehicleId: 'other', speedMps: 10}));
    await Promise.all([service.onNormalVehicleMoved('target'), service.onNormalVehicleMoved('other')]);
    expect(await dispatches.findDecisions(state.dispatchId)).toEqual(expect.arrayContaining([
      expect.objectContaining({targetVehicleId: 'target', estimatedArrivalSeconds: 20}),
      expect.objectContaining({targetVehicleId: 'other', estimatedArrivalSeconds: 20}),
    ]));
    expect((await dispatches.findById(state.dispatchId))?.decisionVersion).toBe(3);
    expect(await events()).toHaveLength(4);
  });

  test('a target move with no decision delta fences an older alert before it reaches the outbox', async () => {
    await service.onEmergencyVehicleMoved(emergency.vehicleId);
    await seed(normal());
    const {promise: paused, resolve: pauseReached} = Promise.withResolvers<void>();
    const {promise: resume, resolve: resumeOlder} = Promise.withResolvers<void>();
    const commit = decisions.compareSaveAndEnqueue.bind(decisions);
    jest.spyOn(decisions, 'compareSaveAndEnqueue').mockImplementationOnce(async (...args) => {
      pauseReached();
      await resume;
      return commit(...args);
    });
    const older = service.onNormalVehicleMoved('target');
    try {
      await paused;
      const behind = normal({latitude: -0.001, stateVersion: 2});
      await seed(behind);
      // The index still picks the target up, but the newer position is ineligible.
      expect((await dispatches.findCone(state.dispatchId))?.cells).toContain(behind.h3CellId);
      await service.onNormalVehicleMoved('target');
      expect((await dispatches.findById(state.dispatchId))?.decisionVersion).toBe(0);
    } finally {
      resumeOlder();
      await older;
    }
    expect(await events()).toEqual([]);
    expect(await dispatches.findDecisions(state.dispatchId)).toEqual([]);
  });

  test('a target reappearing with the same eligibility prevents a stale clear after state expiry', async () => {
    await seed(normal());
    await service.onEmergencyVehicleMoved(emergency.vehicleId);
    await fixture.redis.del(vehicleStateKey('target'));
    const {promise: paused, resolve: pauseReached} = Promise.withResolvers<void>();
    const {promise: resume, resolve: resumeOlder} = Promise.withResolvers<void>();
    const commit = decisions.compareSaveAndEnqueue.bind(decisions);
    jest.spyOn(decisions, 'compareSaveAndEnqueue').mockImplementationOnce(async (...args) => {
      pauseReached();
      await resume;
      return commit(...args);
    });
    const older = service.onNormalVehicleMoved('target');
    try {
      await paused;
      await seed(normal({stateVersion: 2}));
      await service.onNormalVehicleMoved('target');
    } finally {
      resumeOlder();
      await older;
    }
    expect((await events()).map(event => event.eventType)).toEqual(['YIELD_ALERT_REQUESTED']);
    expect(await dispatches.findDecisions(state.dispatchId)).toHaveLength(1);
  });

  test('an older no-op emergency evaluation cannot replace a newer cone or hide a normal update', async () => {
    let pauseReached!: () => void, resumeOlder!: () => void;
    const paused = new Promise<void>(resolve => { pauseReached = resolve; });
    const resume = new Promise<void>(resolve => { resumeOlder = resolve; });
    const saveCone = dispatches.saveCone.bind(dispatches);
    jest.spyOn(dispatches, 'saveCone').mockImplementationOnce(async (...args) => {
      pauseReached();
      await resume;
      return saveCone(...args);
    });
    const older = service.onEmergencyVehicleMoved(emergency.vehicleId);
    try {
      await paused;
      await seed(vehicle({...emergency, latitude: 0.02, stateVersion: 2}));
      await service.onEmergencyVehicleMoved(emergency.vehicleId);
    } finally {
      resumeOlder();
      await older;
    }
    const persistedCone = await dispatches.findCone(state.dispatchId);
    const versionAfterNoops = (await dispatches.findById(state.dispatchId))?.decisionVersion;
    const eventsAfterNoops = (await events()).length;
    await seed(normal({vehicleId: 'new-cone-target', latitude: 0.0218}));
    await service.onNormalVehicleMoved('new-cone-target');
    expect({origin: persistedCone?.origin, versionAfterNoops, eventsAfterNoops,
      alertedTargets: (await events()).map(event => event.payload.targetVehicleId)})
      .toEqual({origin: {latitude: 0.02, longitude: 0}, versionAfterNoops: 0, eventsAfterNoops: 0,
        alertedTargets: ['new-cone-target']});
  });

  test('a newer cone fences stale decisions after the older emergency cone has already been saved', async () => {
    await seed(normal({vehicleId: 'old-cone-target'}));
    let pauseReached!: () => void, resumeOlder!: () => void;
    const paused = new Promise<void>(resolve => { pauseReached = resolve; });
    const resume = new Promise<void>(resolve => { resumeOlder = resolve; });
    const saveCone = dispatches.saveCone.bind(dispatches);
    jest.spyOn(dispatches, 'saveCone').mockImplementationOnce(async (...args) => {
      const accepted = await saveCone(...args);
      pauseReached();
      await resume;
      return accepted;
    });
    const older = service.onEmergencyVehicleMoved(emergency.vehicleId);
    try {
      await paused;
      await seed(vehicle({...emergency, latitude: 0.02, stateVersion: 2}));
      await service.onEmergencyVehicleMoved(emergency.vehicleId);
      expect((await dispatches.findById(state.dispatchId))?.decisionVersion).toBe(0);
    } finally {
      resumeOlder();
      await older;
    }
    expect({coneOrigin: (await dispatches.findCone(state.dispatchId))?.origin,
      decisionVersion: (await dispatches.findById(state.dispatchId))?.decisionVersion,
      decisions: await dispatches.findDecisions(state.dispatchId), events: await events()})
      .toEqual({coneOrigin: {latitude: 0.02, longitude: 0}, decisionVersion: 0, decisions: [], events: []});
  });

  test('completion during emergency evaluation cannot recreate cone or decision state', async () => {
    await seed(normal());
    const saveCone = dispatches.saveCone.bind(dispatches);
    jest.spyOn(dispatches, 'saveCone').mockImplementationOnce(async (id, cone) => {
      await lifecycle.completeDispatch(id, state.updatedAt);
      return saveCone(id, cone);
    });
    await service.onEmergencyVehicleMoved(emergency.vehicleId);
    expect(await dispatches.findById(state.dispatchId)).toBeNull();
    expect(await dispatches.findCone(state.dispatchId)).toBeNull();
    expect(await dispatches.findDecisions(state.dispatchId)).toEqual([]);
    expect(await events()).toHaveLength(0);
  });

  test('recovers committed decisions after a crash before delivery with stable external idempotency keys', async () => {
    await seed(normal());
    await service.onEmergencyVehicleMoved(emergency.vehicleId);
    const original = (await events())[0]!;
    // Recreate the service after the transaction and replay the notification before restarting delivery.
    await service.onEmergencyVehicleMoved(emergency.vehicleId);
    const fault = jest.spyOn(fixture.redis, 'eval').mockRejectedValueOnce(new Error('crash before receipt'));
    await expect(new RedisAlertOutboxDispatcher(fixture.redis, config).dispatchPending()).rejects.toThrow('crash before receipt');
    fault.mockRestore();
    await new RedisAlertOutboxDispatcher(fixture.redis, config).dispatchPending();
    await new RedisAlertOutboxDispatcher(fixture.redis, config).dispatchPending();
    const delivered = await fixture.redis.xrange(config.alertStream, '-', '+');
    expect(delivered).toHaveLength(2);
    expect(new Set(delivered.map(([, fields]) => fields[fields.indexOf('eventKey') + 1]))).toEqual(new Set([original.eventId]));
    expect(new Set(delivered.map(([, fields]) => parseStreamEvent(fields).eventId)).size).toBe(1);
    const projections = await fixture.redis.xrange(config.gatewayAlertStream, '-', '+');
    expect(projections).toHaveLength(2);
    expect(new Set(projections.map(([, fields]) => {
      const payload = JSON.parse(fields[fields.indexOf('payload') + 1]!);
      return `${payload.alertId}:${payload.alertVersion}`;
    })).size).toBe(1);
    expect(await events()).toHaveLength(1);
  });

  test('route cone preserves connected incoming-road eligibility and rejects a nearby disconnected road', async () => {
    const graph = RoadGraph.load(graphDocument());
    service = new YieldDecisionService(lifecycle, dispatches, new DecisionStore(fixture.redis, config),
      new VehicleIndexReader(fixture.redis), new MovingConeCalculator(), new YieldTargetEvaluator(graph),
      new YieldActionResolver(), config, () => now, graph);
    await seed(normal({vehicleId: 'incoming', latitude: 0.002, longitude: -0.0018, headingDegrees: 90}),
      normal({vehicleId: 'disconnected', latitude: 0.0021, longitude: -0.0018, headingDegrees: 90}));
    await service.evaluate(state.dispatchId, {route: turnRoute});
    expect((await events()).map(event => event.payload.targetVehicleId)).toEqual(['incoming']);
    expect((await events())[0]).toMatchObject({payload: {recommendedAction: 'CLEAR_INTERSECTION_FIRST'}});
    await service.onNormalVehicleMoved('incoming');
    await service.onEmergencyVehicleMoved(emergency.vehicleId);
    expect(await events()).toHaveLength(1);
    expect(await dispatches.findDecisions(state.dispatchId)).toHaveLength(1);
  });
});
