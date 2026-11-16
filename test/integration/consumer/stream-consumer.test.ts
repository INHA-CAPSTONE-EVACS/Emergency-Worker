import {latLngToCell} from 'h3-js';
import {StreamConsumer} from '../../../src/consumer/stream-consumer';
import {VehicleStateSubscriber} from '../../../src/dispatch/vehicle-state-subscriber';
import {EMERGENCY_CONSUMER_GROUP, VEHICLE_CONSUMER_GROUP} from '../../../src/domain/contracts';
import {redisFixture, config} from '../store/fixtures';

const waitUntil = async (predicate: () => Promise<boolean>) => {
  const end = Date.now() + 3000;
  while (!await predicate()) {
    if (Date.now() > end) throw new Error('Timed out awaiting consumer outcome');
    await new Promise(resolve => setTimeout(resolve, 10));
  }
};
const event = (resolution = 9) => ({eventId: 'state-1', eventType: 'VEHICLE_STATE_UPDATED', schemaVersion: 1,
  occurredAt: new Date().toISOString(), producer: 'location-worker', payload: {vehicleId: 'ambulance-1',
    vehicleType: 'EMERGENCY', h3CellId: latLngToCell(37.45, 126.65, resolution), previousH3CellId: null,
    stateVersion: 1, measuredAt: new Date().toISOString()}});

describe('reliable stream consumption', () => {
  const {redis, close} = redisFixture();
  let consumer: StreamConsumer | undefined;
  beforeEach(async () => { await redis.del(config.stateStream, config.emergencyStream, config.dlqStream); });
  afterEach(async () => { await consumer?.stop(); consumer = undefined; });
  afterAll(close);
  const settings = () => ({...config, blockMs: 20, pendingMinIdleMs: 20, maxDeliveries: 2, inactiveTtlMs: 60_000});
  const pending = async (stream = config.stateStream, group = VEHICLE_CONSUMER_GROUP) =>
    Number((await redis.xpending(stream, group) as [number, ...unknown[]])[0]);

  test('ACKs only after the handler commits and stop drains in-flight work', async () => {
    let release!: () => void;
    let entered = false;
    const gate = new Promise<void>(resolve => { release = resolve; });
    consumer = new StreamConsumer(redis, config.stateStream, VEHICLE_CONSUMER_GROUP, settings(), async () => {
      entered = true; await gate; await redis.set('committed', 'yes');
    });
    await redis.xadd(config.stateStream, '*', 'event', JSON.stringify(event()));
    await consumer.start();
    await waitUntil(async () => entered);
    expect(await pending()).toBe(1);
    const stopping = consumer.stop();
    release();
    await stopping;
    expect(await redis.get('committed')).toBe('yes');
    expect(await pending()).toBe(0);
  });

  test('malformed and expired input reaches DLQ before ACK', async () => {
    const stale = event(); stale.payload.measuredAt = '2000-01-01T00:00:00.000Z';
    await redis.xadd(config.stateStream, '*', 'event', '{');
    await redis.xadd(config.stateStream, '*', 'event', JSON.stringify(stale));
    consumer = new StreamConsumer(redis, config.stateStream, VEHICLE_CONSUMER_GROUP, settings(), async () => {
      throw new Error('invalid input reached handler');
    });
    await consumer.start();
    await waitUntil(async () => await redis.xlen(config.dlqStream) === 2);
    await waitUntil(async () => await pending() === 0);
    const records = await redis.xrange(config.dlqStream, '-', '+');
    expect(records.map(([, fields]) => fields[fields.indexOf('reason') + 1])).toEqual(['INVALID_EVENT', 'EXPIRED_EVENT']);
  });

  test('transient failures remain pending and are reclaimed by a new consumer', async () => {
    let attempts = 0;
    const initial = {...settings(), pendingMinIdleMs: 60_000};
    consumer = new StreamConsumer(redis, config.stateStream, VEHICLE_CONSUMER_GROUP, initial, async () => {
      attempts++; throw new Error('temporary failure');
    });
    await redis.xadd(config.stateStream, '*', 'event', JSON.stringify(event()));
    await consumer.start();
    await waitUntil(async () => attempts === 1);
    expect(await pending()).toBe(1);
    expect(await redis.xlen(config.dlqStream)).toBe(0);
    await consumer.stop();
    consumer = new StreamConsumer(redis, config.stateStream, VEHICLE_CONSUMER_GROUP,
      {...settings(), consumerName: 'replacement'}, async () => { await redis.set('recovered', 'yes'); });
    await consumer.start();
    await waitUntil(async () => await redis.get('recovered') === 'yes');
    await waitUntil(async () => await pending() === 0);
  });

  test('caps ordinary transient deliveries without losing the failed event', async () => {
    await redis.xadd(config.stateStream, '*', 'event', JSON.stringify(event()));
    consumer = new StreamConsumer(redis, config.stateStream, VEHICLE_CONSUMER_GROUP, settings(), async () => {
      throw new Error('permanent outage');
    });
    await consumer.start();
    await waitUntil(async () => await redis.xlen(config.dlqStream) === 1);
    await waitUntil(async () => await pending() === 0);
    const records = await redis.xrange(config.dlqStream, '-', '+');
    const fields = records[0]![1];
    expect(fields[fields.indexOf('reason') + 1]).toBe('MAX_DELIVERIES');
    expect(JSON.parse(fields[fields.indexOf('fields') + 1]!)[0]).toBe('event');
  });

  test('incompatible valid H3 blocks readiness beyond delivery cap and recovers after correction', async () => {
    const mutableConfig = settings();
    let applied = 0;
    await redis.xadd(config.stateStream, '*', 'event', JSON.stringify(event(8)));
    consumer = new StreamConsumer(redis, config.stateStream, VEHICLE_CONSUMER_GROUP, mutableConfig, async () => { applied++; });
    await consumer.start();
    await waitUntil(async () => consumer!.configurationBlocked);
    await waitUntil(async () => {
      const deliveries = await redis.xpending(config.stateStream, VEHICLE_CONSUMER_GROUP, '-', '+', 1) as [string, string, number, number][];
      return (deliveries[0]?.[3] ?? 0) > mutableConfig.maxDeliveries;
    });
    expect(consumer.ready).toBe(false);
    expect(await pending()).toBe(1);
    expect(await redis.xlen(config.dlqStream)).toBe(0);
    mutableConfig.h3Resolution = 8;
    await waitUntil(async () => applied === 1);
    await waitUntil(async () => consumer!.ready && !consumer!.configurationBlocked);
    expect(await pending()).toBe(0);
  });

  test('a state update without an active emergency dispatch has no Fast-Path effect', async () => {
    const effects: string[] = [];
    const subscriber = new VehicleStateSubscriber({findActiveByVehicleId: async () => null}, {
      onEmergencyState: async () => { effects.push('emergency'); }, onNormalState: async () => { effects.push('normal'); },
    });
    consumer = new StreamConsumer(redis, config.stateStream, VEHICLE_CONSUMER_GROUP, settings(), subscriber.handle);
    await consumer.start();
    await redis.xadd(config.stateStream, '*', 'event', JSON.stringify(event()));
    await waitUntil(async () => {
      const groups = await redis.xinfo('GROUPS', config.stateStream) as unknown[][];
      const group = groups[0]!;
      return group[group.indexOf('last-delivered-id') + 1] !== '0-0' && await pending() === 0;
    });
    await consumer.stop();
    expect(effects).toEqual([]);
    expect(EMERGENCY_CONSUMER_GROUP).not.toBe(VEHICLE_CONSUMER_GROUP);
  });
});
