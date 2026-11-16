import type {ApproachDirection, ClearReason, Coordinates, DispatchStatus, RecommendedAction,
  VehicleType, YieldReason, CanonicalRequestEvent} from './contracts';

/** Mirrors Location Worker's vehicle:{id}:state hash; speed m/s and heading north=0. */
export interface VehicleState extends Coordinates {
  vehicleId: string; vehicleType: VehicleType; speedMps: number; headingDegrees: number;
  h3CellId: string; lastSequence: number; lastMeasuredAt: number; lastUpdatedAt: string;
  stateVersion: number; status: 'ACTIVE' | 'INACTIVE';
}
export interface DispatchState {
  dispatchId: string; emergencyId: string; emergencyVehicleId: string; status: DispatchStatus;
  origin: Coordinates; destination: Coordinates; decisionVersion: number;
  routeVersion: number; updatedAt: string;
}
export interface Cone {
  cells: string[]; origin: Coordinates; headingDegrees: number; distanceM: number; halfAngleDegrees: number;
}
export interface YieldDecision {
  alertId: string; emergencyId: string; dispatchId: string; emergencyVehicleId: string; targetVehicleId: string;
  approachDirection: ApproachDirection; estimatedArrivalSeconds: number; recommendedAction: RecommendedAction;
  reason: YieldReason; decisionVersion: number; issuedAt: string; expiresAt: string;
}
export interface DecisionDiff {
  accepted: boolean; decisionVersion: number; added: YieldDecision[]; changed: YieldDecision[];
  removed: {decision: YieldDecision; reason: ClearReason}[]; outboxIds: string[];
}
export type RouteResult = {
  outcome: 'ROUTE_FOUND'; waypoints: Coordinates[]; roadSegments: string[]; congestedSegments: string[];
  estimatedTravelSeconds: number; distanceM: number;
} | {outcome: 'NO_ROUTE'; reason: 'GRAPH_UNAVAILABLE' | 'DISCONNECTED' | 'INVALID_ENDPOINT'};
export interface DensitySnapshot {
  generation: string; generatedAt: string; h3Resolution: number;
  cells: Readonly<Record<string, number>>; edges: Readonly<Record<string, number>>;
  edgeDensity: 'enabled' | `disabled:${string}`; degraded: boolean;
}
export interface OutboxRecord {eventKey: string; event: CanonicalRequestEvent}
export interface PublishReceipt {messageId: string; publishedAt: string}
export interface EventPublisher {publish(stream: string, event: CanonicalRequestEvent): Promise<PublishReceipt>}
export type ProcessingOutcome = 'APPLIED' | 'IGNORED' | 'DLQ' | 'TRANSIENT_ERROR' | 'CONFIGURATION_BLOCKED';
