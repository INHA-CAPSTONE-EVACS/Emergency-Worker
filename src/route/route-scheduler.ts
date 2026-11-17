import type {WorkerConfig} from '../config/config';
import type {Coordinates} from '../domain/contracts';
import type {DensitySnapshot, DispatchState, RouteResult, YieldDecision} from '../domain/types';
import type {DispatchLifecycleService} from '../dispatch/dispatch-lifecycle-service';
import type {DispatchStateStore} from '../store/dispatch-state-store';
import type {VehicleIndexReader} from '../store/vehicle-index-reader';
import {DensitySnapshotError, type EdgeWeightCalculator} from './edge-weight-calculator';
import type {DispatchRoute, DispatchRouteRepository, FoundRoute, RouteChangeClear} from './dispatch-route-repository';
import type {RouteStabilizer} from './route-stabilizer';

export interface SchedulerPlanner {
  findRoute(origin: Coordinates, destination: Coordinates, density: DensitySnapshot): RouteResult | Promise<RouteResult>;
}
export interface RouteTickResult {
  outcome: 'APPLIED' | 'SKIPPED' | 'CONFIGURATION_BLOCKED' | 'TRANSIENT_ERROR';
  routesCommitted: number; density?: DensitySnapshot; error?: Error;
}
export interface RouteSchedulerDependencies {
  lifecycle: Pick<DispatchLifecycleService, 'findActiveDispatches'>;
  dispatches: Pick<DispatchStateStore, 'findById' | 'findDecisions'>;
  vehicles: Pick<VehicleIndexReader, 'getStates'>;
  /** Supply a calculator backed by a dedicated non-blocking Redis connection. */
  weights: Pick<EdgeWeightCalculator, 'loadDensitySnapshot'>;
  planner: SchedulerPlanner;
  repository: Pick<DispatchRouteRepository, 'findLatest' | 'saveNewVersionWithOutbox'>;
  stabilizer: RouteStabilizer;
  config: Pick<WorkerConfig, 'routeIntervalMs' | 'inactiveTtlMs'>;
  selectRouteChangeClears(dispatch: DispatchState, previous: DispatchRoute, next: FoundRoute,
    decisions: readonly YieldDecision[]): readonly RouteChangeClear[] | Promise<readonly RouteChangeClear[]>;
  now?: () => number;
}
const active = (state: DispatchState | null): state is DispatchState => state !== null
  && state.status !== 'COMPLETED' && state.status !== 'CANCELED';
const sameDestination = (a: DispatchState, b: DispatchState) => a.destination.latitude === b.destination.latitude
  && a.destination.longitude === b.destination.longitude && a.emergencyVehicleId === b.emergencyVehicleId;

/** Independent Slow-Path loop. A burst produces at most one queued follow-up per dispatch. */
export class RouteScheduler {
  private timer?: ReturnType<typeof setInterval>;
  private running?: Promise<RouteTickResult>;
  private immediateScheduled = false;
  private stopping = false;
  private readonly pending = new Set<string>();
  private readonly now: () => number;
  lastResult: RouteTickResult = {outcome: 'SKIPPED', routesCommitted: 0};

  constructor(private readonly dependencies: RouteSchedulerDependencies) {
    const interval = dependencies.config.routeIntervalMs;
    if (!Number.isInteger(interval) || interval < 1000 || interval > 3000) throw new Error('Route interval must be 1000–3000ms');
    this.now = dependencies.now ?? Date.now;
  }

  start(): void {
    if (this.timer) return;
    this.stopping = false;
    this.timer = setInterval(() => {void this.tick().catch(error => this.recordFailure(error));}, this.dependencies.config.routeIntervalMs);
  }

  async stop(): Promise<void> {
    this.stopping = true;
    if (this.timer) clearInterval(this.timer);
    this.timer = undefined;
    if (this.running) await this.running;
    else if (this.pending.size) await this.run(false);
  }

  requestImmediate(dispatchId: string): void {
    if (this.stopping) return;
    this.pending.add(dispatchId);
    if (this.running || this.immediateScheduled) return;
    this.immediateScheduled = true;
    queueMicrotask(() => {
      this.immediateScheduled = false;
      if (!this.running && this.pending.size && !this.stopping) {
        void this.run(false).catch(error => this.recordFailure(error));
      }
    });
  }

  tick(): Promise<RouteTickResult> {
    return this.running ?? this.run(true);
  }

  private run(includeActive: boolean): Promise<RouteTickResult> {
    this.running = this.drain(includeActive).then(result => {this.lastResult = result; return result;})
      .finally(() => {this.running = undefined;});
    return this.running;
  }

  private async drain(includeActive: boolean): Promise<RouteTickResult> {
    if (includeActive) {
      for (const dispatch of await this.dependencies.lifecycle.findActiveDispatches()) this.pending.add(dispatch.dispatchId);
    }
    let result: RouteTickResult = {outcome: 'SKIPPED', routesCommitted: 0}, total = 0;
    while (this.pending.size) {
      const ids = [...this.pending]; this.pending.clear();
      result = await this.batch(ids);
      total += result.routesCommitted;
    }
    return {...result, routesCommitted: total};
  }

  private async batch(ids: readonly string[]): Promise<RouteTickResult> {
    const deps = this.dependencies;
    let density: DensitySnapshot;
    try {density = await deps.weights.loadDensitySnapshot();} catch (error) {
      if (!(error instanceof DensitySnapshotError)) throw error;
      return {outcome: error.configurationBlocked ? 'CONFIGURATION_BLOCKED'
        : error.code === 'GENERATION_MISMATCH' ? 'SKIPPED' : 'TRANSIENT_ERROR', routesCommitted: 0, error};
    }
    let routesCommitted = 0;
    for (const id of ids) {
      const dispatch = await deps.dispatches.findById(id);
      if (!active(dispatch)) continue;
      const [emergency] = await deps.vehicles.getStates([dispatch.emergencyVehicleId]);
      if (!emergency || emergency.vehicleType !== 'EMERGENCY'
        || emergency.lastMeasuredAt <= this.now() - deps.config.inactiveTtlMs) continue;
      const previous = await deps.repository.findLatest(id);
      let route: RouteResult;
      try {route = await deps.planner.findRoute(emergency, dispatch.destination, density);} catch (error) {
        if (!(error instanceof DensitySnapshotError)) throw error;
        return {outcome: error.configurationBlocked ? 'CONFIGURATION_BLOCKED' : 'TRANSIENT_ERROR', routesCommitted, density, error};
      }
      if (route.outcome !== 'ROUTE_FOUND') continue;
      // Travel times to different destinations are not comparable. A new leg must be issued
      // immediately; stabilization applies only to alternatives for the accepted destination.
      const destinationChanged = previous !== null && (previous.destination.latitude !== dispatch.destination.latitude
        || previous.destination.longitude !== dispatch.destination.longitude);
      if (!destinationChanged && !deps.stabilizer.shouldReplace(previous, route.estimatedTravelSeconds, this.now())) continue;
      const current = await deps.dispatches.findById(id);
      if (!active(current) || !sameDestination(dispatch, current)) continue;
      const clears = previous ? await deps.selectRouteChangeClears(current, previous, route, await deps.dispatches.findDecisions(id)) : [];
      const saved = await deps.repository.saveNewVersionWithOutbox({dispatch: current, route,
        expectedVersion: previous?.routeVersion ?? 0, changeReason: destinationChanged ? 'DESTINATION_CHANGED' : previous ? 'FASTER_ROUTE' : 'INITIAL_ROUTE',
        createdAt: new Date(this.now()).toISOString(), clears});
      if (saved) routesCommitted++;
    }
    return {outcome: routesCommitted ? 'APPLIED' : 'SKIPPED', routesCommitted, density};
  }

  private recordFailure(error: unknown): void {
    this.lastResult = {outcome: 'TRANSIENT_ERROR', routesCommitted: 0,
      error: error instanceof Error ? error : new Error(String(error))};
  }
}
