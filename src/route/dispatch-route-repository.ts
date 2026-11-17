import {randomUUID} from 'node:crypto';
import type {Pool, QueryResultRow} from 'pg';
import {parseEventEnvelope, ROUTE_UPDATE_REQUESTED, WORKER_EVENT_PRODUCER, YIELD_ALERT_CLEAR_REQUESTED,
  type CanonicalRequestEvent, type Coordinates} from '../domain/contracts';
import type {DispatchState, OutboxRecord, PublishReceipt, RouteResult} from '../domain/types';

export type FoundRoute = Extract<RouteResult, {outcome: 'ROUTE_FOUND'}>;
export interface DispatchRoute {
  routeId: string; dispatchId: string; routeVersion: number; routeCoordinates: Coordinates[];
  roadSegments: string[]; estimatedDuration: number; changeReason: string | null; createdAt: string;
}
export interface RouteChangeClear {alertId: string; targetVehicleId: string}
export interface SaveRouteInput {
  dispatch: DispatchState; route: FoundRoute; expectedVersion: number; changeReason: string | null;
  createdAt: string; clears: readonly RouteChangeClear[];
}
export interface RouteOutboxRecord extends OutboxRecord {outboxId: string}
const fromRow = (row: QueryResultRow): DispatchRoute => ({routeId: row.route_id, dispatchId: row.dispatch_id,
  routeVersion: row.route_version, routeCoordinates: row.route_coordinates, roadSegments: row.road_segments,
  estimatedDuration: row.estimated_duration, changeReason: row.change_reason, createdAt: new Date(row.created_at).toISOString()});

export class DispatchRouteRepository {
  constructor(private readonly pool: Pool) {}

  async findLatest(dispatchId: string): Promise<DispatchRoute | null> {
    const result = await this.pool.query('SELECT * FROM dispatch_route WHERE dispatch_id = $1 ORDER BY route_version DESC LIMIT 1', [dispatchId]);
    return result.rows[0] ? fromRow(result.rows[0]) : null;
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
    const client = await this.pool.connect();
    try {
      await client.query('BEGIN');
      // A transaction lock serializes the first version too, when no route row exists to lock.
      await client.query('SELECT pg_advisory_xact_lock(hashtextextended($1, 0))', [dispatch.dispatchId]);
      const latest = await client.query('SELECT route_version FROM dispatch_route WHERE dispatch_id = $1 ORDER BY route_version DESC LIMIT 1', [dispatch.dispatchId]);
      if ((latest.rows[0]?.route_version ?? 0) !== input.expectedVersion) {
        await client.query('ROLLBACK'); return null;
      }
      const routeId = randomUUID();
      const result = await client.query(`INSERT INTO dispatch_route
        (route_id, dispatch_id, route_version, route_coordinates, road_segments, estimated_duration, change_reason, created_at)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8) RETURNING *`, [routeId, dispatch.dispatchId, version,
        JSON.stringify(route.waypoints), JSON.stringify(route.roadSegments), seconds, input.changeReason, input.createdAt]);
      for (const record of events) {
        await client.query('INSERT INTO emergency_route_outbox (route_id, event_key, event, created_at) VALUES ($1,$2,$3,$4)',
          [routeId, record.eventKey, JSON.stringify(record.event), input.createdAt]);
      }
      await client.query('COMMIT');
      return fromRow(result.rows[0]!);
    } catch (error) {
      await client.query('ROLLBACK'); throw error;
    } finally {client.release();}
  }

  async findPendingOutbox(limit = 100): Promise<RouteOutboxRecord[]> {
    const result = await this.pool.query('SELECT outbox_id, event_key, event FROM emergency_route_outbox WHERE delivered_at IS NULL ORDER BY outbox_id LIMIT $1', [limit]);
    return result.rows.map(row => ({outboxId: String(row.outbox_id), eventKey: row.event_key, event: row.event as CanonicalRequestEvent}));
  }

  async markOutboxDelivered(outboxId: string, receipt: PublishReceipt): Promise<void> {
    await this.pool.query(`UPDATE emergency_route_outbox SET delivered_at = $2, message_id = $3
      WHERE outbox_id = $1 AND delivered_at IS NULL`, [outboxId, receipt.publishedAt, receipt.messageId]);
  }
}
