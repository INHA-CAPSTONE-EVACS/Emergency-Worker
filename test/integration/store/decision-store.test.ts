import {DispatchStateStore} from '../../../src/store/dispatch-state-store';
import {DecisionStore} from '../../../src/store/decision-store';
import {RedisAlertOutboxDispatcher} from '../../../src/publishing/redis-alert-outbox-dispatcher';
import {OUTBOX_RECEIPT_TTL_MS, OUTBOX_RETENTION_MS, parseStreamEvent} from '../../../src/domain/contracts';
import {redisFixture, config, state, cone, decision, request} from './fixtures';

describe('atomic decision outbox', () => {
  const {redis, close} = redisFixture();
  const store = new DecisionStore(redis, config);
  const dispatches = new DispatchStateStore(redis, config);
  beforeEach(async () => {
    await redis.del('dispatch:dispatch-1:state', 'dispatch:dispatch-1:decisions', config.alertOutboxStream, config.alertStream);
    await redis.del(`${config.alertOutboxStream}:delivery-cursor`);
    await dispatches.save(state);
  });
  afterAll(close);
  test('concurrent compare-and-save accepts one version and publishes one delta', async () => {
    const next = [decision()];
    const results = await Promise.all([store.compareSaveAndEnqueue(state.dispatchId, 0, next, next.map(request)),
      store.compareSaveAndEnqueue(state.dispatchId, 0, next, next.map(request))]);
    expect(results.map(result => result.accepted).sort()).toEqual([false, true]);
    expect(results.find(result => result.accepted)).toMatchObject({decisionVersion: 1, added: next, changed: [], removed: []});
    expect(results.find(result => !result.accepted)).toMatchObject({decisionVersion: 1, added: [], changed: [], removed: [], outboxIds: []});
    expect(await redis.xlen(config.alertOutboxStream)).toBe(1);
    expect(await redis.xlen(config.alertStream)).toBe(0);
    expect(await dispatches.findDecisions(state.dispatchId)).toEqual(next);
    expect((await dispatches.findById(state.dispatchId))?.decisionVersion).toBe(1);
  });
  test('does not bump the version for metadata-only changes', async () => {
    await store.compareSaveAndEnqueue(state.dispatchId, 0, [decision()], [request(decision())]);
    const same = {...decision('target-1', 2), issuedAt: '2026-11-16T12:00:01.000Z', expiresAt: '2026-11-16T12:00:21.000Z'};
    expect(await store.compareSaveAndEnqueue(state.dispatchId, 1, [same], [])).toMatchObject({accepted: true, decisionVersion: 1, added: [], changed: [], removed: [], outboxIds: []});
    expect(await dispatches.findDecisions(state.dispatchId)).toEqual([decision()]);
    expect(await redis.xlen(config.alertOutboxStream)).toBe(1);
  });
  test('atomically fences the expected cone snapshot even when decision and source versions are unchanged', async () => {
    const older = {...cone, sourceEmergencyStateVersion: 1};
    const newer = {...older, headingDegrees: 90};
    await dispatches.saveCone(state.dispatchId, newer, 1);
    expect(await store.compareSaveAndEnqueue(state.dispatchId, 0, [decision()], [request(decision())], older))
      .toMatchObject({accepted: false, decisionVersion: 0, outboxIds: []});
    expect(await dispatches.findDecisions(state.dispatchId)).toEqual([]);
    expect(await redis.xlen(config.alertOutboxStream)).toBe(0);
    expect(await store.compareSaveAndEnqueue(state.dispatchId, 0, [decision()], [request(decision())], newer))
      .toMatchObject({accepted: true, decisionVersion: 1});
    expect(await store.compareSaveAndEnqueue(state.dispatchId, 1, [decision()], [], newer))
      .toMatchObject({accepted: true, decisionVersion: 1, outboxIds: []});
    expect(await redis.xlen(config.alertOutboxStream)).toBe(1);
  });
  test('returns changed and removed deltas with canonical records', async () => {
    await store.compareSaveAndEnqueue(state.dispatchId, 0, [decision()], [request(decision())]);
    const changed = {...decision('target-1', 2), estimatedArrivalSeconds: 10};
    expect(await store.compareSaveAndEnqueue(state.dispatchId, 1, [changed], [request(changed)]))
      .toMatchObject({accepted: true, decisionVersion: 2, added: [], changed: [changed], removed: []});
    const eventKey = 'dispatch-1:3:YIELD_ALERT_CLEAR_REQUESTED:target-1';
    expect(await store.compareSaveAndEnqueue(state.dispatchId, 2, [], [{eventKey, event: {eventId: eventKey,
      eventType: 'YIELD_ALERT_CLEAR_REQUESTED', schemaVersion: 1, occurredAt: state.updatedAt, producer: 'emergency-worker',
      payload: {alertId: changed.alertId, dispatchId: state.dispatchId, targetVehicleId: 'target-1', reason: 'EMERGENCY_PASSED'}}}]))
      .toMatchObject({accepted: true, decisionVersion: 3, removed: [{decision: changed, reason: 'EMERGENCY_PASSED'}]});
    expect(await dispatches.findDecisions(state.dispatchId)).toEqual([]);
    expect(await redis.xlen(config.alertOutboxStream)).toBe(3);
  });
  test('rejects a missing delta event without persisting decision state', async () => {
    await expect(store.compareSaveAndEnqueue(state.dispatchId, 0, [decision()], [])).rejects.toThrow();
    expect((await dispatches.findById(state.dispatchId))?.decisionVersion).toBe(0);
    expect(await dispatches.findDecisions(state.dispatchId)).toEqual([]);
    expect(await redis.xlen(config.alertOutboxStream)).toBe(0);
  });
  test('restarts after destination XADD failure without losing the durable request', async () => {
    const d = decision('retry');
    await store.compareSaveAndEnqueue(state.dispatchId, 0, [d], [request(d)]);
    await redis.set(config.alertStream, 'wrong-type-injects-XADD-failure');
    await expect(new RedisAlertOutboxDispatcher(redis, config).dispatchPending()).rejects.toThrow();
    expect(await redis.xlen(config.alertOutboxStream)).toBe(1);
    expect(await redis.exists(`emergency-worker:outbox:delivered:${request(d).eventKey}`)).toBe(0);
    await redis.del(config.alertStream);
    await new RedisAlertOutboxDispatcher(redis, config).dispatchPending();
    await new RedisAlertOutboxDispatcher(redis, config).dispatchPending();
    expect(await redis.xlen(config.alertStream)).toBe(1);
    const [[, fields]] = await redis.xrange(config.alertStream, '-', '+') as [[string, string[]]];
    expect(fields[fields.indexOf('eventKey') + 1]).toBe(request(d).eventKey);
    expect(parseStreamEvent(fields)).toEqual(request(d).event);
    expect(await redis.exists(`emergency-worker:outbox:delivered:${request(d).eventKey}`)).toBe(1);
    const ttl = await redis.pttl(`emergency-worker:outbox:delivered:${request(d).eventKey}`);
    expect(ttl).toBeGreaterThan(0);
    expect(ttl).toBeLessThanOrEqual(OUTBOX_RECEIPT_TTL_MS);
    expect(await redis.xlen(config.alertOutboxStream)).toBe(1);
  });
  test('trims delivered outbox entries past retention but never undelivered ones', async () => {
    const later = () => Date.now() + OUTBOX_RETENTION_MS + 60_000;
    const first = decision('trim-1'), second = decision('trim-2', 2);
    await store.compareSaveAndEnqueue(state.dispatchId, 0, [first], [request(first)]);
    await new RedisAlertOutboxDispatcher(redis, config, undefined, later).dispatchPending();
    expect(await redis.xlen(config.alertOutboxStream)).toBe(1);
    await store.compareSaveAndEnqueue(state.dispatchId, 1, [first, second], [request(second)]);
    await redis.set(config.alertStream, 'wrong-type-injects-XADD-failure');
    await expect(new RedisAlertOutboxDispatcher(redis, config, undefined, later).dispatchPending()).rejects.toThrow();
    expect(await redis.xlen(config.alertOutboxStream)).toBe(2);
    await redis.del(config.alertStream);
    await new RedisAlertOutboxDispatcher(redis, config, undefined, later).dispatchPending();
    const remaining = await redis.xrange(config.alertOutboxStream, '-', '+');
    expect(remaining.map(([, fields]) => fields[fields.indexOf('eventKey') + 1])).toEqual([request(second).eventKey]);
    expect(await redis.xlen(config.alertStream)).toBe(1);
  });
  test('keeps delivered outbox entries within retention', async () => {
    const d = decision('fresh');
    await store.compareSaveAndEnqueue(state.dispatchId, 0, [d], [request(d)]);
    await store.compareSaveAndEnqueue(state.dispatchId, 1, [d, decision('fresh-2', 2)], [request(decision('fresh-2', 2))]);
    await new RedisAlertOutboxDispatcher(redis, config).dispatchPending();
    expect(await redis.xlen(config.alertOutboxStream)).toBe(2);
  });
  test('a crash after XADD retries with the same idempotency key', async () => {
    const d = decision('crash');
    await store.compareSaveAndEnqueue(state.dispatchId, 0, [d], [request(d)]);
    const fault = jest.spyOn(redis, 'eval').mockRejectedValueOnce(new Error('crash-before-delivery-receipt'));
    try { await expect(new RedisAlertOutboxDispatcher(redis, config).dispatchPending()).rejects.toThrow('crash-before-delivery-receipt'); }
    finally { fault.mockRestore(); }
    expect(await redis.xlen(config.alertStream)).toBe(1);
    expect(await redis.xlen(config.alertOutboxStream)).toBe(1);
    await new RedisAlertOutboxDispatcher(redis, config).dispatchPending();
    expect(await redis.xlen(config.alertStream)).toBe(2);
    const records = await redis.xrange(config.alertStream, '-', '+');
    expect(records.map(([, fields]) => fields[fields.indexOf('eventKey') + 1])).toEqual([request(d).eventKey, request(d).eventKey]);
    await new RedisAlertOutboxDispatcher(redis, config).dispatchPending();
    expect(await redis.xlen(config.alertStream)).toBe(2);
  });
  test('an incompatible outbox key cannot partially save decision state', async () => {
    await redis.set(config.alertOutboxStream, 'wrong-type');
    await expect(store.compareSaveAndEnqueue(state.dispatchId, 0, [decision()], [request(decision())])).rejects.toThrow();
    expect((await dispatches.findById(state.dispatchId))?.decisionVersion).toBe(0);
    expect(await dispatches.findDecisions(state.dispatchId)).toEqual([]);
  });
  test('a lifecycle state save cannot roll back an accepted decision version', async () => {
    await store.compareSaveAndEnqueue(state.dispatchId, 0, [decision()], [request(decision())]);
    await dispatches.save({...state, status: 'TRANSPORTING'});
    expect(await dispatches.findById(state.dispatchId)).toMatchObject({status: 'TRANSPORTING', decisionVersion: 1});
  });
});
