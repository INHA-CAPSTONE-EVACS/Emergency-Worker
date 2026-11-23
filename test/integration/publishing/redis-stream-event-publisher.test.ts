import {parseStreamEvent} from '../../../src/domain/contracts';
import {RedisStreamEventPublisher} from '../../../src/publishing/redis-stream-event-publisher';
import {config, decision, redisFixture, request} from '../store/fixtures';

// The retired temporary Gateway projection stream. The worker must never write it.
const LEGACY_GATEWAY_STREAM = 'evacs:stream:yield-alert';

describe('Redis canonical publisher', () => {
  let fixture: ReturnType<typeof redisFixture>;
  beforeEach(() => { fixture = redisFixture(); });
  afterEach(async () => { await fixture.close(); });

  test('writes a yield issue only to the canonical stream with one stable idempotency key', async () => {
    const event = request(decision()).event;
    const receipt = await new RedisStreamEventPublisher(fixture.redis, config).publish(config.alertStream, event);
    const [[id, canonical]] = await fixture.redis.xrange(config.alertStream, '-', '+') as [[string, string[]]];
    expect(receipt).toMatchObject({messageId: id, publishedAt: expect.any(String)});
    expect(parseStreamEvent(canonical)).toEqual(event);
    expect(canonical[canonical.indexOf('eventKey') + 1]).toBe(event.eventId);
    expect(await fixture.redis.exists(LEGACY_GATEWAY_STREAM)).toBe(0);
  });

  test('publishes clear requests canonically', async () => {
    await new RedisStreamEventPublisher(fixture.redis, config).publish(config.alertStream, {eventId: 'clear',
      eventType: 'YIELD_ALERT_CLEAR_REQUESTED', schemaVersion: 1, producer: 'emergency-worker',
      occurredAt: '2026-11-16T12:00:00.000Z', payload: {alertId: 'alert', dispatchId: 'dispatch',
        targetVehicleId: 'target', reason: 'EMERGENCY_PASSED'}});
    expect(await fixture.redis.xlen(config.alertStream)).toBe(1);
    expect(await fixture.redis.exists(LEGACY_GATEWAY_STREAM)).toBe(0);
  });

  test('a leftover legacy Gateway key of the wrong type does not affect publishing', async () => {
    await fixture.redis.set(LEGACY_GATEWAY_STREAM, 'wrong-type');
    await new RedisStreamEventPublisher(fixture.redis, config).publish(config.alertStream, request(decision()).event);
    expect(await fixture.redis.xlen(config.alertStream)).toBe(1);
    expect(await fixture.redis.get(LEGACY_GATEWAY_STREAM)).toBe('wrong-type');
  });

  test('an invalid canonical destination rejects without writing', async () => {
    await fixture.redis.set(config.alertStream, 'wrong-type');
    await expect(new RedisStreamEventPublisher(fixture.redis, config).publish(config.alertStream, request(decision()).event))
      .rejects.toThrow();
    expect(await fixture.redis.get(config.alertStream)).toBe('wrong-type');
  });
});
