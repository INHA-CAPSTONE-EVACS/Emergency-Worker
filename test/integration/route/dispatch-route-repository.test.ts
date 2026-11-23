import {DispatchRouteRepository} from '../../../src/route/dispatch-route-repository';
import {RouteOutboxDispatcher} from '../../../src/route/route-outbox-dispatcher';
import {RedisStreamEventPublisher} from '../../../src/publishing/redis-stream-event-publisher';
import {parseStreamEvent} from '../../../src/domain/contracts';
import {redisFixture, config} from '../store/fixtures';
import {mysqlFixture, dispatch, input} from './fixtures';

describe('append-only route persistence and durable outbox', () => {
  const pool = mysqlFixture();
  const {redis, close} = redisFixture();
  const repository = new DispatchRouteRepository(pool);
  afterAll(close);
  test('migration owns exact route columns and rejects updates/deletes while versions append', async () => {
    const [columns] = await pool.query(`SELECT column_name AS column_name, column_type AS column_type,
        is_nullable AS is_nullable FROM information_schema.columns
      WHERE table_schema = DATABASE() AND table_name = 'dispatch_route' ORDER BY ordinal_position`);
    expect(columns).toEqual([
      ['route_id', 'char(36)', 'NO'], ['dispatch_id', 'varchar(64)', 'NO'], ['route_version', 'int', 'NO'],
      ['route_coordinates', 'json', 'NO'], ['road_segments', 'json', 'NO'], ['estimated_duration', 'int', 'NO'],
      ['change_reason', 'varchar(64)', 'YES'], ['created_at', 'datetime(3)', 'NO'],
    ].map(([column_name, column_type, is_nullable]) => ({column_name, column_type, is_nullable})));
    expect(await repository.findLatest(dispatch.dispatchId)).toBeNull();
    const first = await repository.saveNewVersionWithOutbox(input());
    const second = await repository.saveNewVersionWithOutbox({...input(), expectedVersion: 1,
      route: {...input().route, estimatedTravelSeconds: 80}, createdAt: '2026-11-16T12:00:05.000Z'});
    expect(first?.routeVersion).toBe(1);
    expect(second?.routeVersion).toBe(2);
    expect(await repository.findLatest(dispatch.dispatchId)).toEqual(second);
    expect((await pool.query('SELECT route_version FROM dispatch_route ORDER BY route_version'))[0]).toEqual([{route_version: 1}, {route_version: 2}]);
    await expect(pool.query('UPDATE dispatch_route SET estimated_duration = 1')).rejects.toThrow(/append-only/);
    await expect(pool.query('DELETE FROM dispatch_route')).rejects.toThrow(/append-only/);
    await expect(pool.query(`INSERT INTO dispatch_route SELECT * FROM (SELECT UUID(), dispatch_id, route_version,
      route_coordinates, road_segments, estimated_duration, change_reason, created_at FROM dispatch_route LIMIT 1) AS copy`))
      .rejects.toThrow(/Duplicate entry/);
  });
  test('concurrent writers with the same expected version produce exactly one version and event', async () => {
    const results = await Promise.all([repository.saveNewVersionWithOutbox(input()), repository.saveNewVersionWithOutbox(input())]);
    expect(results.filter(Boolean)).toHaveLength(1);
    expect((await pool.query('SELECT * FROM emergency_route_outbox'))[0]).toHaveLength(1);
  });
  test('replacement and route-change clear commit together; failing outbox insert rolls back route', async () => {
    await repository.saveNewVersionWithOutbox(input());
    await repository.saveNewVersionWithOutbox({...input(), expectedVersion: 1, clears: [{alertId: 'old-alert', targetVehicleId: 'target'}]});
    const records = await repository.findPendingOutbox();
    expect(records.map(record => record.event.eventType)).toEqual(['ROUTE_UPDATE_REQUESTED', 'ROUTE_UPDATE_REQUESTED', 'YIELD_ALERT_CLEAR_REQUESTED']);
    expect(records[2]?.event.payload).toEqual({dispatchId: dispatch.dispatchId, targetVehicleId: 'target', alertId: 'old-alert', reason: 'ROUTE_CHANGED'});
    expect(new Set(records.map(record => record.eventKey)).size).toBe(3);
    await pool.query(`ALTER TABLE emergency_route_outbox ADD CONSTRAINT reject_insert CHECK (event_key NOT LIKE '%:3:%')`);
    await expect(repository.saveNewVersionWithOutbox({...input(), expectedVersion: 2})).rejects.toThrow();
    expect((await repository.findLatest(dispatch.dispatchId))?.routeVersion).toBe(2);
    expect(await repository.findPendingOutbox()).toHaveLength(3);
    await pool.query('ALTER TABLE emergency_route_outbox DROP CHECK reject_insert');
  });
  test('receipt-write failure replays the durable event with the same idempotency key after restart', async () => {
    const stream = 'route-receipt-retry';
    await repository.saveNewVersionWithOutbox(input());
    const publisher = new RedisStreamEventPublisher(redis, config);
    const failReceipt = jest.spyOn(repository, 'markOutboxDelivered').mockRejectedValueOnce(new Error('database unavailable'));
    await expect(new RouteOutboxDispatcher(repository, publisher, stream).dispatchPending()).rejects.toThrow('database unavailable');
    expect(await repository.findPendingOutbox()).toHaveLength(1);
    failReceipt.mockRestore();
    await new RouteOutboxDispatcher(new DispatchRouteRepository(pool), publisher, stream).dispatchPending();
    const events = (await redis.xrange(stream, '-', '+')).map(([, fields]) => parseStreamEvent(fields));
    expect(events).toHaveLength(2);
    expect(events[0]?.eventId).toBe(events[1]?.eventId);
    expect(await repository.findPendingOutbox()).toHaveLength(0);
  });
  test('a Redis failure after database commit is retried and marked only after receipt', async () => {
    const stream = 'route-retry';
    await repository.saveNewVersionWithOutbox(input());
    const delivery = new RouteOutboxDispatcher(repository, new RedisStreamEventPublisher(redis, config), stream);
    await redis.set(stream, 'wrong-type');
    await expect(delivery.dispatchPending()).rejects.toThrow();
    expect(await repository.findPendingOutbox()).toHaveLength(1);
    await redis.del(stream);
    expect(await delivery.dispatchPending()).toBe(1);
    expect(await delivery.dispatchPending()).toBe(0);
    const events = (await redis.xrange(stream, '-', '+')).map(([, fields]) => parseStreamEvent(fields));
    expect(events).toHaveLength(1);
    expect(events[0]).toMatchObject({eventType: 'ROUTE_UPDATE_REQUESTED', payload: {dispatchId: dispatch.dispatchId,
      routeVersion: 1, waypoints: input().route.waypoints, congestedSegments: [], estimatedTravelSeconds: 100}});
    expect(await repository.findPendingOutbox()).toEqual([]);
  });
});
