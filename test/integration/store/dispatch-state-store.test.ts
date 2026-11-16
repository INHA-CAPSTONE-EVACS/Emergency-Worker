import {DispatchStateStore} from '../../../src/store/dispatch-state-store';
import {DecisionStore} from '../../../src/store/decision-store';
import {parseStreamEvent} from '../../../src/domain/contracts';
import {redisFixture, config, state, cone, decision, request} from './fixtures';

describe('dispatch persistence and completion', () => {
  const {redis, close} = redisFixture();
  const store = new DispatchStateStore(redis, config);
  afterAll(close);
  test('round trips state and cone without changing unrelated dispatch fields', async () => {
    expect(await store.findById('missing')).toBeNull();
    expect(await store.findCone('missing')).toBeNull();
    expect(await store.findDecisions('missing')).toEqual([]);
    await store.save(state);
    await store.saveCone(state.dispatchId, cone);
    expect(await store.findById(state.dispatchId)).toEqual(state);
    expect(await store.findCone(state.dispatchId)).toEqual(cone);
  });
  test('atomically queues all three completion clears before removing dispatch data, and is repeatable', async () => {
    await store.save(state);
    const decisions = ['a', 'b', 'c'].map(id => decision(id));
    await new DecisionStore(redis, config).compareSaveAndEnqueue(state.dispatchId, 0, decisions, decisions.map(request));
    await redis.hset('vehicle:ambulance-1:state', 'status', 'ACTIVE');
    const ids = await store.completeAndEnqueueClears(state.dispatchId, '2026-11-16T12:01:00.000Z');
    expect(ids).toHaveLength(3);
    const outbox = await redis.xrange(config.alertOutboxStream, '-', '+');
    const clears = outbox.slice(3).map(([, fields]) => parseStreamEvent(fields));
    expect(clears).toHaveLength(3);
    expect(clears.map(event => event.payload)).toEqual(['a', 'b', 'c'].map(targetVehicleId => ({
      alertId: `alert-${targetVehicleId}`, dispatchId: 'dispatch-1', targetVehicleId, reason: 'EMERGENCY_COMPLETED'})));
    expect(await redis.exists('dispatch:dispatch-1:state', 'dispatch:dispatch-1:cone', 'dispatch:dispatch-1:decisions')).toBe(0);
    expect(await redis.hget('vehicle:ambulance-1:state', 'status')).toBe('ACTIVE');
    expect(await store.completeAndEnqueueClears(state.dispatchId)).toEqual([]);
    expect(await redis.xlen(config.alertOutboxStream)).toBe(6);
    expect((await new DecisionStore(redis, config).compareSaveAndEnqueue(state.dispatchId, 0, [decision()], [request(decision())])).accepted).toBe(false);
  });
  test('completion retains all dispatch data when clear enqueueing fails', async () => {
    await store.save(state);
    await store.saveCone(state.dispatchId, cone);
    await new DecisionStore(redis, config).compareSaveAndEnqueue(state.dispatchId, 0, [decision()], [request(decision())]);
    await redis.del(config.alertOutboxStream);
    await redis.set(config.alertOutboxStream, 'wrong-type');
    await expect(store.completeAndEnqueueClears(state.dispatchId)).rejects.toThrow();
    expect(await store.findById(state.dispatchId)).toMatchObject({decisionVersion: 1});
    expect(await store.findCone(state.dispatchId)).toEqual(cone);
    expect(await store.findDecisions(state.dispatchId)).toEqual([decision()]);
  });
});
