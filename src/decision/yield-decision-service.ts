import {createHash} from 'node:crypto';
import type {WorkerConfig} from '../config/config';
import {WORKER_EVENT_PRODUCER, YIELD_ALERT_CLEAR_REQUESTED, YIELD_ALERT_REQUESTED,
  type ClearReason, type VehicleStateUpdatedEvent} from '../domain/contracts';
import type {Cone, DecisionDiff, DispatchState, OutboxRecord, RouteResult, YieldDecision} from '../domain/types';
import type {DispatchLifecycleService} from '../dispatch/dispatch-lifecycle-service';
import type {VehicleStateFastPath} from '../dispatch/vehicle-state-subscriber';
import type {RoadGraph} from '../route/road-graph';
import {decisionEventKey, type DecisionStore} from '../store/decision-store';
import type {DispatchStateStore} from '../store/dispatch-state-store';
import type {VehicleIndexReader} from '../store/vehicle-index-reader';
import type {MovingConeCalculator} from './moving-cone-calculator';
import type {YieldActionResolver} from './yield-action-resolver';
import type {YieldTargetEvaluator} from './yield-target-evaluator';

export interface EvaluationOptions {targetVehicleId?: string; route?: RouteResult}
type ActiveCone = Cone & {route?: RouteResult};
const semanticFields = ['alertId', 'emergencyId', 'dispatchId', 'emergencyVehicleId', 'targetVehicleId',
  'approachDirection', 'estimatedArrivalSeconds', 'recommendedAction', 'reason'] as const;
const sameDecision = (a: YieldDecision, b: YieldDecision) => semanticFields.every(field => a[field] === b[field]);

/** Fast-Path computes deltas; only DecisionStore commits decisions and durable delivery intent. */
export class YieldDecisionService implements VehicleStateFastPath {
  constructor(private readonly lifecycle: Pick<DispatchLifecycleService, 'findActiveByVehicleId' | 'findActiveDispatches'>,
    private readonly dispatches: DispatchStateStore, private readonly decisions: DecisionStore,
    private readonly vehicles: VehicleIndexReader, private readonly cones: MovingConeCalculator,
    private readonly evaluator: YieldTargetEvaluator, private readonly actions: YieldActionResolver,
    private readonly config: Pick<WorkerConfig, 'inactiveTtlMs' | 'alertTtlMs'>,
    private readonly now: () => number = Date.now, private readonly graph?: RoadGraph) {}

  async onEmergencyState(_event: VehicleStateUpdatedEvent, dispatch: DispatchState): Promise<void> {
    await this.evaluate(dispatch.dispatchId);
  }

  async onNormalState(event: VehicleStateUpdatedEvent): Promise<void> {
    await this.onNormalVehicleMoved(event.payload.vehicleId);
  }

  async onEmergencyVehicleMoved(vehicleId: string): Promise<void> {
    const dispatch = await this.lifecycle.findActiveByVehicleId(vehicleId);
    if (dispatch) await this.evaluate(dispatch.dispatchId);
  }

  async onNormalVehicleMoved(vehicleId: string): Promise<void> {
    const [target] = await this.vehicles.getStates([vehicleId]);
    for (const dispatch of await this.lifecycle.findActiveDispatches()) {
      const cone = await this.dispatches.findCone(dispatch.dispatchId);
      if (!cone) continue;
      // An alerted target must also be examined after leaving the cone or becoming inactive.
      const previous = await this.dispatches.findDecisions(dispatch.dispatchId);
      if ((target && cone.cells.includes(target.h3CellId)) || previous.some(value => value.targetVehicleId === vehicleId)) {
        await this.evaluate(dispatch.dispatchId, {targetVehicleId: vehicleId});
      }
    }
  }

  async evaluate(dispatchId: string, options: EvaluationOptions = {}): Promise<DecisionDiff | null> {
    for (let attempt = 0; attempt < 16; attempt++) {
      const dispatch = await this.dispatches.findById(dispatchId);
      if (!dispatch || dispatch.status === 'COMPLETED' || dispatch.status === 'CANCELED') return null;
      const timestamp = this.now(), cutoff = timestamp - this.config.inactiveTtlMs;
      const [emergency] = await this.vehicles.getStates([dispatch.emergencyVehicleId]);
      if (!emergency || emergency.vehicleType !== 'EMERGENCY' || emergency.lastMeasuredAt <= cutoff) return null;
      const previous = await this.dispatches.findDecisions(dispatchId);
      const previousCone = await this.dispatches.findCone(dispatchId) as ActiveCone | null;
      const route = options.route ?? previousCone?.route;
      let cone: ActiveCone | null;
      let ids: string[];
      if (options.targetVehicleId) {
        cone = previousCone;
        if (!cone) return null;
        ids = [options.targetVehicleId];
      } else {
        const rebuilt = route ? this.cones.buildRouteCone(emergency, route, this.graph) : this.cones.buildCone(emergency);
        // Persist route context with the cone so notifications and process restarts use the same topology.
        cone = route ? {...rebuilt, route} : rebuilt;
        if (!await this.dispatches.saveCone(dispatchId, cone, emergency.stateVersion)) continue;
        // Include former targets in the bulk read to distinguish passing from other removals.
        ids = [...new Set([...await this.vehicles.findVehicleIdsInCells(cone.cells, cutoff),
          ...previous.map(value => value.targetVehicleId)])];
      }
      const states = await this.vehicles.getStates(ids);
      const candidates = this.evaluator.filterCandidates(emergency, states.filter(value => value.lastMeasuredAt > cutoff), cone, route);
      const next = options.targetVehicleId ? previous.filter(value => value.targetVehicleId !== options.targetVehicleId) : [];
      const issuedAt = new Date(timestamp).toISOString();
      for (const candidate of candidates) {
        const target = candidate.vehicle;
        const context = {sameRoute: candidate.sameRoute, intersection: candidate.intersection};
        const value: YieldDecision = {
          alertId: `yield-${createHash('sha256').update(JSON.stringify([dispatchId, target.vehicleId])).digest('hex')}`,
          emergencyId: dispatch.emergencyId, dispatchId, emergencyVehicleId: emergency.vehicleId, targetVehicleId: target.vehicleId,
          approachDirection: this.actions.resolveDirection(emergency, target),
          // Eligibility uses the fractional ETA. The wire contract alone requires an integer.
          estimatedArrivalSeconds: Math.round(candidate.estimatedArrivalSeconds),
          recommendedAction: this.actions.resolveAction(context), reason: this.actions.resolveReason(context),
          decisionVersion: dispatch.decisionVersion + 1, issuedAt,
          expiresAt: new Date(timestamp + this.config.alertTtlMs).toISOString(),
        };
        const old = previous.find(item => item.targetVehicleId === target.vehicleId);
        next.push(old && sameDecision(old, value) ? old : value);
      }
      const records: OutboxRecord[] = [];
      for (const value of next) {
        const old = previous.find(item => item.targetVehicleId === value.targetVehicleId);
        if (old && sameDecision(old, value)) continue;
        const {issuedAt: occurredAt, ...payload} = value;
        const eventKey = decisionEventKey(dispatchId, dispatch.decisionVersion + 1, YIELD_ALERT_REQUESTED, value.targetVehicleId);
        records.push({eventKey, event: {eventId: eventKey, eventType: YIELD_ALERT_REQUESTED,
          schemaVersion: 1, occurredAt, producer: WORKER_EVENT_PRODUCER, payload}});
      }
      for (const old of previous) {
        if (next.some(value => value.targetVehicleId === old.targetVehicleId)) continue;
        const target = states.find(value => value.vehicleId === old.targetVehicleId);
        const reason: ClearReason = target && this.evaluator.isPassed(emergency, target) ? 'EMERGENCY_PASSED'
          : options.route ? 'ROUTE_CHANGED' : 'NO_LONGER_RELEVANT';
        const eventKey = decisionEventKey(dispatchId, dispatch.decisionVersion + 1, YIELD_ALERT_CLEAR_REQUESTED, old.targetVehicleId);
        records.push({eventKey, event: {eventId: eventKey, eventType: YIELD_ALERT_CLEAR_REQUESTED,
          schemaVersion: 1, occurredAt: issuedAt, producer: WORKER_EVENT_PRODUCER,
          payload: {alertId: old.alertId, dispatchId, targetVehicleId: old.targetVehicleId, reason}}});
      }
      const result = await this.decisions.compareSaveAndEnqueue(dispatchId, dispatch.decisionVersion, next, records);
      if (result.accepted) return result;
      // A concurrent evaluator or completion won. Re-read all authoritative input before retrying.
    }
    throw new Error(`Decision contention for dispatch ${dispatchId}`);
  }
}
