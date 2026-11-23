import {isValidCell} from 'h3-js';

// Redis protocol names shared with Location Worker and the current Gateway.
export const EVENT_FIELD = 'event';
export const GATEWAY_PAYLOAD_FIELD = 'payload';
export const STATE_STREAM = 'stream:vehicle:state';
/** Control API Server Outbox stream (EVACS-API-Server `evacs.redis.emergency-stream`). */
export const EMERGENCY_STREAM = 'evacs:stream:emergency-event';
export const ALERT_STREAM = 'stream:alert:event';
export const GATEWAY_ALERT_STREAM = 'evacs:stream:yield-alert';
export const DLQ_STREAM = 'stream:emergency-worker:dlq';
export const ALERT_OUTBOX_STREAM = 'stream:emergency-worker:alert-outbox';
export const VEHICLE_CONSUMER_GROUP = 'emergency-worker:vehicle-state';
export const EMERGENCY_CONSUMER_GROUP = 'emergency-worker:emergency';
export const ALERT_OUTBOX_CONSUMER_GROUP = 'emergency-worker:alert-outbox';
export const ACTIVE_DISPATCHES_KEY = 'dispatch:active';
export const ACTIVE_CELLS_KEY = 'h3:cells:active';
export const CELL_KEY_PREFIX = 'h3:vehicles:';
export const DENSITY_CELLS_KEY = 'density:cells';
export const DENSITY_EDGES_KEY = 'density:edges';
export const DENSITY_META_KEY = 'density:meta';
export const DENSITY_GENERATION_FIELD = '_generation';
export const ROUTE_OUTBOX_TABLE = 'emergency_route_outbox';
export const DISPATCH_ROUTE_TABLE = 'dispatch_route';
/** Read by the Control API Server (`EVACS_CONE_KEY_PATTERN`); %s is the emergency ID. */
export const CONE_PROJECTION_KEY_PATTERN = 'evacs:emergency:%s:cone';
export const CONE_PROJECTION_TTL_MS = 600_000;
export const vehicleStateKey = (vehicleId: string) => `vehicle:${vehicleId}:state`;
export const vehicleVersionKey = (vehicleId: string) => `vehicle:${vehicleId}:version`;
export const cellVehiclesKey = (cellId: string) => `${CELL_KEY_PREFIX}${cellId}`;
export const dispatchStateKey = (dispatchId: string) => `dispatch:${dispatchId}:state`;
export const dispatchConeKey = (dispatchId: string) => `dispatch:${dispatchId}:cone`;
export const dispatchDecisionsKey = (dispatchId: string) => `dispatch:${dispatchId}:decisions`;
export const dispatchVehicleKey = (vehicleId: string) => `dispatch:vehicle:${vehicleId}`;
export const outboxDeliveredKey = (eventKey: string) => `emergency-worker:outbox:delivered:${eventKey}`;
export const outboxDeduplicationKey = (eventKey: string) => `emergency-worker:outbox:dedup:${eventKey}`;

export const VEHICLE_STATE_FIELDS = ['vehicleId', 'vehicleType', 'latitude', 'longitude', 'speedMps',
  'headingDegrees', 'h3CellId', 'lastSequence', 'lastMeasuredAt', 'lastUpdatedAt', 'stateVersion', 'status'] as const;
export const DENSITY_META_FIELDS = ['generation', 'generatedAt', 'intervalMs', 'ttlMs', 'h3Resolution', 'tableVersion', 'edgeDensity'] as const;
export const ENV = {
  redisUrl: 'REDIS_URL', databaseUrl: 'DATABASE_URL', h3Resolution: 'H3_RESOLUTION',
  inactiveTtlMs: 'INACTIVE_TTL_MS', stateStream: 'STATE_STREAM', emergencyStream: 'EMERGENCY_STREAM',
  alertStream: 'ALERT_STREAM', gatewayAlertStream: 'EVACS_ALERT_STREAM', dlqStream: 'DLQ_STREAM',
  alertOutboxStream: 'ALERT_OUTBOX_STREAM', vehicleConsumerGroup: 'VEHICLE_CONSUMER_GROUP',
  emergencyConsumerGroup: 'EMERGENCY_CONSUMER_GROUP', consumerName: 'CONSUMER_NAME', instanceId: 'INSTANCE_ID',
  routeIntervalMs: 'ROUTE_INTERVAL_MS', alertTtlMs: 'ALERT_TTL_MS', graphPath: 'GRAPH_PATH',
  routePersistenceEnabled: 'ROUTE_PERSISTENCE_ENABLED', readCount: 'READ_COUNT', blockMs: 'BLOCK_MS',
  pendingMinIdleMs: 'PENDING_MIN_IDLE_MS', maxDeliveries: 'MAX_DELIVERIES', dlqMaxlen: 'DLQ_MAXLEN',
  alertStreamMaxlen: 'ALERT_STREAM_MAXLEN', shutdownDrainTimeoutMs: 'SHUTDOWN_DRAIN_TIMEOUT_MS',
  healthPort: 'HEALTH_PORT', bprAlpha: 'BPR_ALPHA', bprBeta: 'BPR_BETA', coneKeyPattern: 'EVACS_CONE_KEY_PATTERN',
  roadCapacity: 'ROAD_CAPACITY', blockedMultiplier: 'BLOCKED_MULTIPLIER',
} as const;
export const ALERT_TTL_MS = 20_000;
export const ROUTE_INTERVAL_MS = 2_000;
export const STATE_SCHEMA_VERSION = 1;
export const REQUEST_SCHEMA_VERSION = 1;
export const STATE_EVENT_PRODUCER = 'location-worker';
export const WORKER_EVENT_PRODUCER = 'emergency-worker';
export const STATE_EVENT_TYPE = 'VEHICLE_STATE_UPDATED';
export const EMERGENCY_CREATED = 'EMERGENCY_CREATED';
/** Control API Server termination event; not in the design event table, which ends a dispatch by status. */
export const EMERGENCY_ENDED = 'EMERGENCY_ENDED';
export const EMERGENCY_DISPATCHED = 'EMERGENCY_DISPATCHED';
export const EMERGENCY_STATUS_CHANGED = 'EMERGENCY_STATUS_CHANGED';
export const YIELD_ALERT_REQUESTED = 'YIELD_ALERT_REQUESTED';
export const YIELD_ALERT_CLEAR_REQUESTED = 'YIELD_ALERT_CLEAR_REQUESTED';
export const ROUTE_UPDATE_REQUESTED = 'ROUTE_UPDATE_REQUESTED';
export const VEHICLE_TYPES = ['NORMAL', 'EMERGENCY'] as const;
export type VehicleType = typeof VEHICLE_TYPES[number];
export const APPROACH_DIRECTIONS = ['REAR', 'REAR_LEFT', 'REAR_RIGHT', 'LEFT', 'RIGHT'] as const;
export type ApproachDirection = typeof APPROACH_DIRECTIONS[number];
export const RECOMMENDED_ACTIONS = ['YIELD_WHEN_SAFE', 'MOVE_RIGHT_WHEN_SAFE', 'CLEAR_INTERSECTION_FIRST'] as const;
export type RecommendedAction = typeof RECOMMENDED_ACTIONS[number];
export const YIELD_REASONS = ['SAME_ROUTE_AHEAD', 'MOVING_SLOW', 'BLOCK_SECTION'] as const;
export type YieldReason = typeof YIELD_REASONS[number];
export const CLEAR_REASONS = ['EMERGENCY_PASSED', 'ROUTE_CHANGED', 'NO_LONGER_RELEVANT', 'EMERGENCY_COMPLETED'] as const;
export type ClearReason = typeof CLEAR_REASONS[number];
export const DISPATCH_STATUSES = ['REGISTERED', 'DISPATCHED', 'ARRIVED_ON_SCENE', 'TRANSPORTING', 'COMPLETED', 'CANCELED'] as const;
export type DispatchStatus = typeof DISPATCH_STATUSES[number];
export type InputDispatchStatus = DispatchStatus | 'ARRIVED';
export interface Coordinates {latitude: number; longitude: number}
export interface EventEnvelope<TPayload, TType extends string = string> {
  eventId: string; eventType: TType; schemaVersion: 1; occurredAt: string; producer: string; payload: TPayload;
}
export interface VehicleStateUpdatedPayload {
  vehicleId: string; vehicleType: VehicleType; h3CellId: string; previousH3CellId: string | null;
  stateVersion: number; measuredAt: string;
}
export type VehicleStateUpdatedEvent = EventEnvelope<VehicleStateUpdatedPayload, typeof STATE_EVENT_TYPE>;
/** One point of the route registered with the emergency by the Control API Server. */
export interface EmergencyRoutePoint extends Coordinates {sequence: number; h3Index?: string; etaSeconds?: number}
/**
 * Control API Server EMERGENCY_CREATED. The API has no separate dispatch: an emergency names its vehicle,
 * so creation also starts the dispatch, identified by the emergency ID. A registration without vehicleId
 * (design p.100 form, `location` only) starts nothing.
 */
export interface EmergencyCreatedPayload {
  emergencyId: string; emergencyType: string; status: string; vehicleId?: string; startedAt?: string;
  origin?: Coordinates; destination?: Coordinates; location?: Coordinates; route?: EmergencyRoutePoint[];
}
/** Control API Server EMERGENCY_ENDED: completes the emergency's dispatches and clears their alerts. */
export interface EmergencyEndedPayload {emergencyId: string; vehicleId?: string; status: string; endedAt?: string}
export interface EmergencyDispatchedPayload {
  emergencyId: string; dispatchId: string; vehicleId: string; origin: Coordinates; destination: Coordinates;
}
export interface EmergencyStatusChangedPayload {
  emergencyId: string; dispatchId: string; previousStatus: InputDispatchStatus;
  currentStatus: InputDispatchStatus; destination?: Coordinates;
}
export type EmergencyEvent = EventEnvelope<EmergencyCreatedPayload, typeof EMERGENCY_CREATED>
  | EventEnvelope<EmergencyEndedPayload, typeof EMERGENCY_ENDED>
  | EventEnvelope<EmergencyDispatchedPayload, typeof EMERGENCY_DISPATCHED>
  | EventEnvelope<EmergencyStatusChangedPayload, typeof EMERGENCY_STATUS_CHANGED>;
export interface YieldAlertRequestedPayload {
  alertId: string; emergencyId: string; dispatchId: string; emergencyVehicleId: string; targetVehicleId: string;
  approachDirection: ApproachDirection; estimatedArrivalSeconds: number; recommendedAction: RecommendedAction;
  reason: YieldReason; decisionVersion: number; expiresAt: string;
}
export interface YieldAlertClearRequestedPayload {
  alertId: string; dispatchId: string; targetVehicleId: string; reason: ClearReason;
}
export interface RouteUpdateRequestedPayload {
  dispatchId: string; targetVehicleId: string; routeVersion: number; destination: Coordinates;
  waypoints: Coordinates[]; congestedSegments: string[]; estimatedTravelSeconds: number;
}
export type YieldAlertRequestedEvent = EventEnvelope<YieldAlertRequestedPayload, typeof YIELD_ALERT_REQUESTED>;
export type YieldAlertClearRequestedEvent = EventEnvelope<YieldAlertClearRequestedPayload, typeof YIELD_ALERT_CLEAR_REQUESTED>;
export type RouteUpdateRequestedEvent = EventEnvelope<RouteUpdateRequestedPayload, typeof ROUTE_UPDATE_REQUESTED>;
export type CanonicalRequestEvent = YieldAlertRequestedEvent | YieldAlertClearRequestedEvent | RouteUpdateRequestedEvent;
export type WorkerEvent = VehicleStateUpdatedEvent | EmergencyEvent | CanonicalRequestEvent;
export const YIELD_ALERT_REQUEST_FIELDS = ['alertId', 'emergencyId', 'dispatchId', 'emergencyVehicleId',
  'targetVehicleId', 'approachDirection', 'estimatedArrivalSeconds', 'recommendedAction', 'reason', 'decisionVersion', 'expiresAt'] as const;
export const YIELD_ALERT_CLEAR_FIELDS = ['alertId', 'dispatchId', 'targetVehicleId', 'reason'] as const;
export const ROUTE_UPDATE_FIELDS = ['dispatchId', 'targetVehicleId', 'routeVersion', 'destination', 'waypoints', 'congestedSegments', 'estimatedTravelSeconds'] as const;
export const GATEWAY_ALERT_FIELDS = ['alertId', 'vehicleId', 'emergencyVehicleId', 'alertVersion', 'issuedAt', 'expiresAt', 'details'] as const;
/** Gateway projection only; deliveryId is assigned by the receiving Gateway. */
export interface GatewayYieldAlertPayload {
  alertId: string; vehicleId: string; emergencyVehicleId: string; alertVersion: number;
  issuedAt: string; expiresAt: string; details: YieldAlertRequestedEvent;
}

export class ContractError extends Error {
  constructor(message: string) { super(message); this.name = 'ContractError'; }
}
const object = (value: unknown): value is Record<string, unknown> =>
  typeof value === 'object' && value !== null && !Array.isArray(value);
const text = (value: unknown): value is string => typeof value === 'string' && value.trim().length > 0;
const integer = (value: unknown, min = 0): value is number => Number.isSafeInteger(value) && (value as number) >= min;
const member = (value: unknown, values: readonly string[]) => typeof value === 'string' && values.includes(value);
const timestamp = (value: unknown): boolean => {
  if (typeof value !== 'string' || !/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,3})?Z$/.test(value)) return false;
  const ms = Date.parse(value);
  return Number.isFinite(ms) && new Date(ms).toISOString().slice(0, 19) === value.slice(0, 19);
};
const point = (value: unknown): boolean => object(value) && typeof value.latitude === 'number'
  && Number.isFinite(value.latitude) && Math.abs(value.latitude) <= 90 && typeof value.longitude === 'number'
  && Number.isFinite(value.longitude) && Math.abs(value.longitude) <= 180;
const exact = (value: Record<string, unknown>, fields: readonly string[]) =>
  Object.keys(value).length === fields.length && fields.every(field => Object.hasOwn(value, field));
const strings = (value: Record<string, unknown>, fields: readonly string[]) => fields.every(field => text(value[field]));
const optional = (value: Record<string, unknown>, field: string, check: (value: unknown) => boolean) =>
  value[field] === undefined || value[field] === null || check(value[field]);

/** Schema validation only: expiry and configured H3 resolution belong to consumer policy. */
export function parseEventEnvelope(value: unknown): WorkerEvent {
  if (!object(value) || !strings(value, ['eventId', 'eventType', 'producer']) || value.schemaVersion !== 1
    || !timestamp(value.occurredAt) || !object(value.payload)) throw new ContractError('Invalid event envelope');
  const p = value.payload;
  let valid = false;
  switch (value.eventType) {
    case STATE_EVENT_TYPE:
      valid = strings(p, ['vehicleId', 'h3CellId']) && member(p.vehicleType, VEHICLE_TYPES)
        && isValidCell(p.h3CellId as string) && (p.previousH3CellId === null
          || (text(p.previousH3CellId) && isValidCell(p.previousH3CellId)))
        && integer(p.stateVersion, 1) && timestamp(p.measuredAt);
      break;
    case EMERGENCY_CREATED:
      valid = strings(p, ['emergencyId', 'emergencyType', 'status']) && optional(p, 'vehicleId', text)
        && optional(p, 'origin', point) && optional(p, 'destination', point) && optional(p, 'location', point)
        && optional(p, 'route', value => Array.isArray(value) && value.every(point))
        // A dispatched vehicle needs somewhere to go: destination, the route's last point, or the location.
        && (p.vehicleId === undefined || createdDestination(p as unknown as EmergencyCreatedPayload) !== undefined);
      break;
    case EMERGENCY_ENDED:
      valid = strings(p, ['emergencyId', 'status']) && optional(p, 'vehicleId', text);
      break;
    case EMERGENCY_DISPATCHED:
      valid = strings(p, ['emergencyId', 'dispatchId', 'vehicleId']) && point(p.origin) && point(p.destination);
      break;
    case EMERGENCY_STATUS_CHANGED:
      valid = strings(p, ['emergencyId', 'dispatchId']) && member(p.previousStatus, [...DISPATCH_STATUSES, 'ARRIVED'])
        && member(p.currentStatus, [...DISPATCH_STATUSES, 'ARRIVED'])
        && (p.destination === undefined || point(p.destination));
      break;
    case YIELD_ALERT_REQUESTED:
      valid = exact(p, YIELD_ALERT_REQUEST_FIELDS)
        && strings(p, ['alertId', 'emergencyId', 'dispatchId', 'emergencyVehicleId', 'targetVehicleId'])
        && member(p.approachDirection, APPROACH_DIRECTIONS) && member(p.recommendedAction, RECOMMENDED_ACTIONS)
        && member(p.reason, YIELD_REASONS) && integer(p.estimatedArrivalSeconds) && integer(p.decisionVersion, 1) && timestamp(p.expiresAt);
      break;
    case YIELD_ALERT_CLEAR_REQUESTED:
      valid = exact(p, YIELD_ALERT_CLEAR_FIELDS) && strings(p, ['alertId', 'dispatchId', 'targetVehicleId']) && member(p.reason, CLEAR_REASONS);
      break;
    case ROUTE_UPDATE_REQUESTED:
      valid = exact(p, ROUTE_UPDATE_FIELDS) && strings(p, ['dispatchId', 'targetVehicleId']) && integer(p.routeVersion, 1)
        && point(p.destination) && Array.isArray(p.waypoints) && p.waypoints.length > 0 && p.waypoints.every(point)
        && Array.isArray(p.congestedSegments) && p.congestedSegments.every(text) && integer(p.estimatedTravelSeconds);
      break;
  }
  if (!valid) throw new ContractError(`Invalid ${String(value.eventType)} payload`);
  return value as unknown as WorkerEvent;
}
export function validateEventEnvelope(value: unknown): value is WorkerEvent {
  try { parseEventEnvelope(value); return true; } catch (error) {
    if (error instanceof ContractError) return false;
    throw error;
  }
}
/** Route origin/destination of a dispatch started by EMERGENCY_CREATED; route points fill missing ends. */
export function createdDestination(p: EmergencyCreatedPayload): Coordinates | undefined {
  const last = p.route?.length ? p.route[p.route.length - 1] : undefined;
  const value = p.destination ?? last ?? p.location;
  return value ? {latitude: value.latitude, longitude: value.longitude} : undefined;
}
export function createdOrigin(p: EmergencyCreatedPayload): Coordinates | undefined {
  const value = p.origin ?? p.route?.[0] ?? createdDestination(p);
  return value ? {latitude: value.latitude, longitude: value.longitude} : undefined;
}

/** Control API Server field names (BrokerEventPublisher): the envelope is split into stream fields. */
export const FLAT_ENVELOPE_FIELDS = ['eventId', 'eventType', 'schemaVersion', 'occurredAt', 'producer', 'payload'] as const;
const toMillisecondIso = (value: string | undefined): string | undefined => {
  // Java Instant.toString() emits 0, 3, 6 or 9 fraction digits; the canonical envelope keeps milliseconds.
  if (value === undefined || !/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,9})?Z$/.test(value)) return value;
  const ms = Date.parse(value);
  return Number.isFinite(ms) ? new Date(ms).toISOString() : value;
};

/**
 * Two stream layouts are accepted. Worker-to-worker streams carry one JSON envelope under `event`.
 * The Control API Server writes the same envelope as separate fields with a JSON `payload` (design p.97
 * common structure); `aggregateType`/`aggregateId` routing hints are ignored.
 */
export function parseStreamEvent(fields: readonly string[]): WorkerEvent {
  if (fields.length % 2 !== 0) throw new ContractError('Unpaired stream fields');
  const values = new Map<string, string>();
  for (let i = 0; i < fields.length; i += 2) {
    if (values.has(fields[i]!)) throw new ContractError(`Duplicate ${fields[i]} field`);
    values.set(fields[i]!, fields[i + 1]!);
  }
  const raw = values.get(EVENT_FIELD);
  if (raw !== undefined) {
    let value: unknown;
    try { value = JSON.parse(raw); } catch { throw new ContractError('Invalid event JSON'); }
    return parseEventEnvelope(value);
  }
  if (!values.has('eventType') || !values.has('payload')) throw new ContractError('Missing event field');
  let payload: unknown;
  try { payload = JSON.parse(values.get('payload')!); } catch { throw new ContractError('Invalid payload JSON'); }
  const schemaVersion = values.get('schemaVersion');
  return parseEventEnvelope({eventId: values.get('eventId'), eventType: values.get('eventType'),
    schemaVersion: schemaVersion !== undefined && /^\d+$/.test(schemaVersion) ? Number(schemaVersion) : schemaVersion,
    occurredAt: toMillisecondIso(values.get('occurredAt')), producer: values.get('producer'), payload});
}
