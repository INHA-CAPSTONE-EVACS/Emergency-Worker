import {parseStreamEvent} from '../../../src/domain/contracts';
import {RedisStreamEventPublisher} from '../../../src/publishing/redis-stream-event-publisher';
import {config, decision, redisFixture, request} from '../store/fixtures';

describe('Redis canonical publisher and Gateway projection', () => {
  let fixture: ReturnType<typeof redisFixture>;
  beforeEach(() => { fixture = redisFixture(); });
  afterEach(async () => { await fixture.close(); });

  test('writes canonical event and full Gateway issue projection using one stable idempotency key', async () => {
    const event = request(decision()).event;
    const receipt = await new RedisStreamEventPublisher(fixture.redis, config).publish(config.alertStream, event);
    const [[id, canonical]] = await fixture.redis.xrange(config.alertStream, '-', '+') as [[string, string[]]];
    expect(receipt).toMatchObject({messageId: id, publishedAt: expect.any(String)});
    expect(parseStreamEvent(canonical)).toEqual(event);
    expect(canonical[canonical.indexOf('eventKey') + 1]).toBe(event.eventId);
    const [[, projected]] = await fixture.redis.xrange(config.gatewayAlertStream, '-', '+') as [[string, string[]]];
    expect(JSON.parse(projected[projected.indexOf('payload') + 1]!)).toEqual({alertId: 'alert-target-1', vehicleId: 'target-1',
      emergencyVehicleId: 'ambulance-1', alertVersion: 1, issuedAt: '2026-11-16T12:00:00.000Z',
      expiresAt: '2026-11-16T12:00:20.000Z', details: event});
  });

  test('publishes clear requests canonically without inventing a Gateway clear consumer', async () => {
    await new RedisStreamEventPublisher(fixture.redis, config).publish(config.alertStream, {eventId: 'clear',
      eventType: 'YIELD_ALERT_CLEAR_REQUESTED', schemaVersion: 1, producer: 'emergency-worker',
      occurredAt: '2026-11-16T12:00:00.000Z', payload: {alertId: 'alert', dispatchId: 'dispatch',
        targetVehicleId: 'target', reason: 'EMERGENCY_PASSED'}});
    expect(await fixture.redis.xlen(config.alertStream)).toBe(1);
    expect(await fixture.redis.exists(config.gatewayAlertStream)).toBe(0);
  });

  test('invalid projection destination cannot partially publish a canonical issue', async () => {
    await fixture.redis.set(config.gatewayAlertStream, 'wrong-type');
    await expect(new RedisStreamEventPublisher(fixture.redis, config).publish(config.alertStream, request(decision()).event))
      .rejects.toThrow();
    expect(await fixture.redis.exists(config.alertStream)).toBe(0);
  });
});
