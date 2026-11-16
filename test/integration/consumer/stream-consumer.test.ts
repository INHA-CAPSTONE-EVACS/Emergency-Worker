import {latLngToCell} from 'h3-js';
import {StreamConsumer} from '../../../src/consumer/stream-consumer';
import {DispatchLifecycleService} from '../../../src/dispatch/dispatch-lifecycle-service';
import {EmergencyEventSubscriber} from '../../../src/dispatch/emergency-event-subscriber';
import {DispatchStateStore} from '../../../src/store/dispatch-state-store';
import {VehicleStateSubscriber} from '../../../src/dispatch/vehicle-state-subscriber';
import {EMERGENCY_CONSUMER_GROUP, VEHICLE_CONSUMER_GROUP, type EmergencyEvent} from '../../../src/domain/contracts';
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
  const store = new DispatchStateStore(redis, config);
  let consumer: StreamConsumer | undefined;
  beforeEach(async () => { await redis.del(config.stateStream, config.emergencyStream, config.dlqStream,
    'dispatch:active', 'dispatch:vehicle:ambulance-1', 'dispatch:ordering:state', 'dispatch:ordering:completed'); });
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


  test.each(['COMPLETED', 'CANCELED'] as const)('%s received during a pending activation prevents late activation after reclaim', async terminal => {
    const routes: string[] = [];
    const subscriber = new EmergencyEventSubscriber(new DispatchLifecycleService(redis, store, {
      requestImmediate: async id => { routes.push(id); },
    }));
    const activation: EmergencyEvent = {eventId: 'activate', eventType: 'EMERGENCY_DISPATCHED', schemaVersion: 1,
      occurredAt: new Date().toISOString(), producer: 'dispatch-service', payload: {emergencyId: 'emergency-1',
        dispatchId: 'ordering', vehicleId: 'ambulance-1', origin: {latitude: 37, longitude: 126},
        destination: {latitude: 38, longitude: 127}}};
    const completed: EmergencyEvent = {...activation, eventId: 'complete', eventType: 'EMERGENCY_STATUS_CHANGED',
      payload: {emergencyId: 'emergency-1', dispatchId: 'ordering', previousStatus: 'DISPATCHED', currentStatus: terminal}};
    let failed = false;
    let attempts = 0;
    consumer = new StreamConsumer(redis, config.emergencyStream, EMERGENCY_CONSUMER_GROUP, settings(), async input => {
      attempts++;
      if (input.eventType === 'EMERGENCY_DISPATCHED' && !failed) { failed = true; throw new Error('activation temporarily unavailable'); }
      await subscriber.handle(input);
    });
    await redis.xadd(config.emergencyStream, '*', 'event', JSON.stringify(activation));
    await redis.xadd(config.emergencyStream, '*', 'event', JSON.stringify(completed));
    await consumer.start();
    await waitUntil(async () => attempts >= 3 && await pending(config.emergencyStream, EMERGENCY_CONSUMER_GROUP) === 0);
    expect(await store.findById('ordering')).toBeNull();
    expect(routes).toEqual([]);
    expect(await redis.xlen(config.dlqStream)).toBe(0);
  });

  test('a later valid status remains pending until its predecessor status succeeds', async () => {
    const subscriber = new EmergencyEventSubscriber(new DispatchLifecycleService(redis, store, {requestImmediate: async () => {}}));
    const activation: EmergencyEvent = {eventId: 'activate', eventType: 'EMERGENCY_DISPATCHED', schemaVersion: 1,
      occurredAt: new Date().toISOString(), producer: 'dispatch-service', payload: {emergencyId: 'emergency-1',
        dispatchId: 'ordering', vehicleId: 'ambulance-1', origin: {latitude: 37, longitude: 126},
        destination: {latitude: 38, longitude: 127}}};
    await subscriber.handle(activation);
    const arrived: EmergencyEvent = {...activation, eventId: 'arrive', eventType: 'EMERGENCY_STATUS_CHANGED',
      payload: {emergencyId: 'emergency-1', dispatchId: 'ordering', previousStatus: 'DISPATCHED', currentStatus: 'ARRIVED'}};
    const transporting: EmergencyEvent = {...arrived, eventId: 'transport', payload: {emergencyId: 'emergency-1',
      dispatchId: 'ordering', previousStatus: 'ARRIVED', currentStatus: 'TRANSPORTING', destination: {latitude: 39, longitude: 128}}};
    let allowArrived = false;
    let seenTransport = false;
    consumer = new StreamConsumer(redis, config.emergencyStream, EMERGENCY_CONSUMER_GROUP,
      {...settings(), pendingMinIdleMs: 60_000}, async input => {
        if (input.eventId === 'arrive' && !allowArrived) throw new Error('arrival temporarily unavailable');
        if (input.eventId === 'transport') seenTransport = true;
        await subscriber.handle(input);
      });
    await redis.xadd(config.emergencyStream, '*', 'event', JSON.stringify(arrived));
    await redis.xadd(config.emergencyStream, '*', 'event', JSON.stringify(transporting));
    await consumer.start();
    await waitUntil(async () => seenTransport);
    expect(await pending(config.emergencyStream, EMERGENCY_CONSUMER_GROUP)).toBe(2);
    await consumer.stop();
    allowArrived = true;
    consumer = new StreamConsumer(redis, config.emergencyStream, EMERGENCY_CONSUMER_GROUP,
      {...settings(), consumerName: 'ordered-recovery'}, subscriber.handle);
    await consumer.start();
    await waitUntil(async () => await pending(config.emergencyStream, EMERGENCY_CONSUMER_GROUP) === 0);
    expect(await store.findById('ordering')).toMatchObject({status: 'TRANSPORTING', destination: {latitude: 39, longitude: 128}});
    expect(await redis.xlen(config.dlqStream)).toBe(0);
  });

  test('valid status awaiting a predecessor bypasses the ordinary delivery cap', async () => {
    const subscriber = new EmergencyEventSubscriber(new DispatchLifecycleService(redis, store, {requestImmediate: async () => {}}));
    const activation: EmergencyEvent = {eventId: 'activate', eventType: 'EMERGENCY_DISPATCHED', schemaVersion: 1,
      occurredAt: new Date().toISOString(), producer: 'dispatch-service', payload: {emergencyId: 'emergency-1',
        dispatchId: 'ordering', vehicleId: 'ambulance-1', origin: {latitude: 37, longitude: 126},
        destination: {latitude: 38, longitude: 127}}};
    await subscriber.handle(activation);
    const transporting: EmergencyEvent = {...activation, eventId: 'transport', eventType: 'EMERGENCY_STATUS_CHANGED',
      payload: {emergencyId: 'emergency-1', dispatchId: 'ordering', previousStatus: 'ARRIVED', currentStatus: 'TRANSPORTING',
        destination: {latitude: 39, longitude: 128}}};
    await redis.xadd(config.emergencyStream, '*', 'event', JSON.stringify(transporting));
    consumer = new StreamConsumer(redis, config.emergencyStream, EMERGENCY_CONSUMER_GROUP,
      {...settings(), maxDeliveries: 1}, subscriber.handle);
    await consumer.start();
    await waitUntil(async () => {
      const deliveries = await redis.xpending(config.emergencyStream, EMERGENCY_CONSUMER_GROUP, '-', '+', 1) as [string, string, number, number][];
      return (deliveries[0]?.[3] ?? 0) > 2;
    });
    expect(await redis.xlen(config.dlqStream)).toBe(0);
    await subscriber.handle({...transporting, eventId: 'arrive', payload: {emergencyId: 'emergency-1', dispatchId: 'ordering',
      previousStatus: 'DISPATCHED', currentStatus: 'ARRIVED'}});
    await waitUntil(async () => await pending(config.emergencyStream, EMERGENCY_CONSUMER_GROUP) === 0);
    expect(await store.findById('ordering')).toMatchObject({status: 'TRANSPORTING'});
    expect(await redis.xlen(config.dlqStream)).toBe(0);
  });

  test('predecessor waits preserve the ordinary scheduler retry budget across consumer replacement', async () => {
    let failNextLeg = false;
    let schedulerFailures = 0;
    const routed: string[] = [];
    const subscriber = new EmergencyEventSubscriber(new DispatchLifecycleService(redis, store, {
      requestImmediate: async id => {
        if (failNextLeg) { schedulerFailures++; throw new Error('scheduler temporarily unavailable'); }
        routed.push(id);
      },
    }));
    const activation: EmergencyEvent = {eventId: 'activate', eventType: 'EMERGENCY_DISPATCHED', schemaVersion: 1,
      occurredAt: new Date().toISOString(), producer: 'dispatch-service', payload: {emergencyId: 'emergency-1',
        dispatchId: 'ordering', vehicleId: 'ambulance-1', origin: {latitude: 37, longitude: 126},
        destination: {latitude: 38, longitude: 127}}};
    await subscriber.handle(activation);
    const transporting: EmergencyEvent = {...activation, eventId: 'transport', eventType: 'EMERGENCY_STATUS_CHANGED',
      payload: {emergencyId: 'emergency-1', dispatchId: 'ordering', previousStatus: 'ARRIVED', currentStatus: 'TRANSPORTING',
        destination: {latitude: 39, longitude: 128}}};
    await redis.xadd(config.emergencyStream, '*', 'event', JSON.stringify(transporting));
    consumer = new StreamConsumer(redis, config.emergencyStream, EMERGENCY_CONSUMER_GROUP, settings(), subscriber.handle);
    await consumer.start();
    await waitUntil(async () => {
      const entries = await redis.xpending(config.emergencyStream, EMERGENCY_CONSUMER_GROUP, '-', '+', 1) as [string, string, number, number][];
      return (entries[0]?.[3] ?? 0) > settings().maxDeliveries;
    });
    await consumer.stop();
    await subscriber.handle({...transporting, eventId: 'arrive', payload: {emergencyId: 'emergency-1', dispatchId: 'ordering',
      previousStatus: 'DISPATCHED', currentStatus: 'ARRIVED'}});
    failNextLeg = true;
    consumer = new StreamConsumer(redis, config.emergencyStream, EMERGENCY_CONSUMER_GROUP,
      {...settings(), consumerName: 'first-scheduler-attempt'}, subscriber.handle);
    await consumer.start();
    await waitUntil(async () => schedulerFailures > 0);
    await consumer.stop();
    expect(schedulerFailures).toBe(1);
    expect(await pending(config.emergencyStream, EMERGENCY_CONSUMER_GROUP)).toBe(1);
    expect(await redis.xlen(config.dlqStream)).toBe(0);
    failNextLeg = false;
    consumer = new StreamConsumer(redis, config.emergencyStream, EMERGENCY_CONSUMER_GROUP,
      {...settings(), consumerName: 'scheduler-recovery'}, subscriber.handle);
    await consumer.start();
    await waitUntil(async () => await pending(config.emergencyStream, EMERGENCY_CONSUMER_GROUP) === 0);
    expect(routed).toEqual(['ordering', 'ordering']);
    expect(await store.findById('ordering')).toMatchObject({status: 'TRANSPORTING'});
    expect(await redis.xlen(config.dlqStream)).toBe(0);
  });

  test('ordinary failure counts survive reclaim by a replacement consumer', async () => {
    let failures = 0;
    const failingHandler = async () => { failures++; throw new Error('persistent handler outage'); };
    await redis.xadd(config.stateStream, '*', 'event', JSON.stringify(event()));
    consumer = new StreamConsumer(redis, config.stateStream, VEHICLE_CONSUMER_GROUP,
      {...settings(), pendingMinIdleMs: 60_000}, failingHandler);
    await consumer.start();
    await waitUntil(async () => failures === 1);
    await consumer.stop();
    expect(await pending()).toBe(1);
    consumer = new StreamConsumer(redis, config.stateStream, VEHICLE_CONSUMER_GROUP,
      {...settings(), consumerName: 'failure-budget-recovery'}, failingHandler);
    await consumer.start();
    await waitUntil(async () => await redis.xlen(config.dlqStream) === 1 && await pending() === 0);
    expect(failures).toBe(2);
  });

  test('configuration readiness recovers when another consumer reclaims and ACKs the blocked entry', async () => {
    const mutableConfig = {...settings(), pendingMinIdleMs: 60_000};
    await redis.xadd(config.stateStream, '*', 'event', JSON.stringify(event(8)));
    consumer = new StreamConsumer(redis, config.stateStream, VEHICLE_CONSUMER_GROUP, mutableConfig, async () => {});
    await consumer.start();
    await waitUntil(async () => consumer!.configurationBlocked);
    mutableConfig.h3Resolution = 8;
    const replacement = new StreamConsumer(redis, config.stateStream, VEHICLE_CONSUMER_GROUP,
      {...settings(), h3Resolution: 8, consumerName: 'other-worker'}, async () => {});
    try {
      await replacement.start();
      await waitUntil(async () => await pending() === 0);
      expect(replacement.ready).toBe(true);
      await waitUntil(async () => consumer!.ready && !consumer!.configurationBlocked);
    } finally { await replacement.stop(); }
    expect(await redis.xlen(config.dlqStream)).toBe(0);
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
