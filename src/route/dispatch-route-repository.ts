import {createHash, randomUUID} from 'node:crypto';
import type {Pool, RowDataPacket} from 'mysql2/promise';
import {parseEventEnvelope, ROUTE_UPDATE_REQUESTED, WORKER_EVENT_PRODUCER, YIELD_ALERT_CLEAR_REQUESTED,
  type CanonicalRequestEvent, type Coordinates} from '../domain/contracts';
import type {DispatchState, OutboxRecord, PublishReceipt, RouteResult} from '../domain/types';

export type FoundRoute = Extract<RouteResult, {outcome: 'ROUTE_FOUND'}>;
export interface DispatchRoute {
  routeId: string; dispatchId: string; routeVersion: number; routeCoordinates: Coordinates[]; destination: Coordinates;
  roadSegments: string[]; estimatedDuration: number; changeReason: string | null; createdAt: string;
}
export interface RouteChangeClear {alertId: string; targetVehicleId: string}
export interface SaveRouteInput {
  dispatch: DispatchState; route: FoundRoute; expectedVersion: number; changeReason: string | null;
  createdAt: string; clears: readonly RouteChangeClear[];
}
export interface RouteOutboxRecord extends OutboxRecord {outboxId: string}
const LOCK_TIMEOUT_SECONDS = 5;
const fromRow = (row: RowDataPacket): DispatchRoute => ({routeId: row.route_id, dispatchId: row.dispatch_id,
  routeVersion: row.route_version, routeCoordinates: row.route_coordinates, destination: row.destination, roadSegments: row.road_segments,
  estimatedDuration: row.estimated_duration, changeReason: row.change_reason, createdAt: new Date(row.created_at).toISOString()});

export class DispatchRouteRepository {
  constructor(private readonly pool: Pool) {}

  async findLatest(dispatchId: string): Promise<DispatchRoute | null> {
    // The transactional route event retains the requested destination, including after delivery.
    // Its coordinates may differ from the planner's snapped final waypoint.
    const [rows] = await this.pool.query<RowDataPacket[]>(`SELECT route.*,
        JSON_EXTRACT(outbox.event, '$.payload.destination') AS destination
      FROM dispatch_route AS route JOIN emergency_route_outbox AS outbox ON outbox.route_id = route.route_id
      WHERE route.dispatch_id = ? AND outbox.event->>'$.eventType' = ?
      ORDER BY route.route_version DESC LIMIT 1`, [dispatchId, ROUTE_UPDATE_REQUESTED]);
    return rows[0] ? fromRow(rows[0]) : null;
  }

  /** Returns null for a stale plan; the caller must re-read state and replan instead of overwriting. */
  async saveNewVersionWithOutbox(input: SaveRouteInput): Promise<DispatchRoute | null> {
    if (!Number.isSafeInteger(input.expectedVersion) || input.expectedVersion < 0) throw new Error('Invalid expected route version');
    const {dispatch, route} = input, version = input.expectedVersion + 1;
    const seconds = Math.ceil(route.estimatedTravelSeconds);
    const base = `${dispatch.dispatchId}:route:${version}`;
    const routeKey = `${base}:${ROUTE_UPDATE_REQUESTED}:${dispatch.emergencyVehicleId}`;
    const events: OutboxRecord[] = [{eventKey: routeKey, event: {eventId: routeKey, eventType: ROUTE_UPDATE_REQUESTED,
      schemaVersion: 1, occurredAt: input.createdAt, producer: WORKER_EVENT_PRODUCER,
      payload: {dispatchId: dispatch.dispatchId, targetVehicleId: dispatch.emergencyVehicleId, routeVersion: version,
        destination: dispatch.destination, waypoints: route.waypoints, congestedSegments: route.congestedSegments,
        estimatedTravelSeconds: seconds}}}];
    for (const clear of input.clears) {
      const key = `${base}:${YIELD_ALERT_CLEAR_REQUESTED}:${clear.targetVehicleId}`;
      events.push({eventKey: key, event: {eventId: key, eventType: YIELD_ALERT_CLEAR_REQUESTED, schemaVersion: 1,
        occurredAt: input.createdAt, producer: WORKER_EVENT_PRODUCER, payload: {dispatchId: dispatch.dispatchId,
          alertId: clear.alertId, targetVehicleId: clear.targetVehicleId, reason: 'ROUTE_CHANGED'}}});
    }
    for (const record of events) parseEventEnvelope(record.event);
    if (new Set(events.map(record => record.eventKey)).size !== events.length) throw new Error('Duplicate route clear target');
    const connection = await this.pool.getConnection();
    // A named lock serializes the first version too, when no route row exists to lock.
    // GET_LOCK is session-scoped in MySQL, so it is released explicitly after the transaction ends.
    const lock = `dispatch_route:${createHash('sha256').update(dispatch.dispatchId).digest('hex').slice(0, 40)}`;
    try {
      const [[acquired]] = await connection.query<RowDataPacket[]>('SELECT GET_LOCK(?, ?) AS acquired', [lock, LOCK_TIMEOUT_SECONDS]);
      if (Number(acquired?.acquired) !== 1) throw new Error(`Route lock unavailable for dispatch ${dispatch.dispatchId}`);
      try {
        await connection.beginTransaction();
        try {
          const [latest] = await connection.query<RowDataPacket[]>(
            'SELECT route_version FROM dispatch_route WHERE dispatch_id = ? ORDER BY route_version DESC LIMIT 1', [dispatch.dispatchId]);
          if ((latest[0]?.route_version ?? 0) !== input.expectedVersion) {
            await connection.rollback(); return null;
          }
          const routeId = randomUUID(), createdAt = new Date(input.createdAt);
          await connection.query(`INSERT INTO dispatch_route
            (route_id, dispatch_id, route_version, route_coordinates, road_segments, estimated_duration, change_reason, created_at)
            VALUES (?,?,?,?,?,?,?,?)`, [routeId, dispatch.dispatchId, version,
            JSON.stringify(route.waypoints), JSON.stringify(route.roadSegments), seconds, input.changeReason, createdAt]);
          for (const record of events) {
            await connection.query('INSERT INTO emergency_route_outbox (route_id, event_key, event, created_at) VALUES (?,?,?,?)',
              [routeId, record.eventKey, JSON.stringify(record.event), createdAt]);
          }
          await connection.commit();
          // MySQL has no RETURNING; the inserted row is exactly the values written above.
          return fromRow({route_id: routeId, dispatch_id: dispatch.dispatchId, route_version: version,
            route_coordinates: route.waypoints, road_segments: route.roadSegments, estimated_duration: seconds,
            change_reason: input.changeReason, created_at: createdAt, destination: dispatch.destination} as RowDataPacket);
        } catch (error) {
          await connection.rollback(); throw error;
        }
      } finally {
        await connection.query('SELECT RELEASE_LOCK(?)', [lock]);
      }
    } finally {connection.release();}
  }

  async findPendingOutbox(limit = 100): Promise<RouteOutboxRecord[]> {
    const [rows] = await this.pool.query<RowDataPacket[]>(
      'SELECT outbox_id, event_key, event FROM emergency_route_outbox WHERE delivered_at IS NULL ORDER BY outbox_id LIMIT ?', [limit]);
    return rows.map(row => ({outboxId: String(row.outbox_id), eventKey: row.event_key, event: row.event as CanonicalRequestEvent}));
  }

  async markOutboxDelivered(outboxId: string, receipt: PublishReceipt): Promise<void> {
    await this.pool.query(`UPDATE emergency_route_outbox SET delivered_at = ?, message_id = ?
      WHERE outbox_id = ? AND delivered_at IS NULL`, [new Date(receipt.publishedAt), receipt.messageId, outboxId]);
  }
}
