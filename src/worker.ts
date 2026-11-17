import {readFile} from 'node:fs/promises';
import type Redis from 'ioredis';
import type {Pool} from 'pg';
import type {WorkerConfig} from './config/config';
import {YIELD_ALERT_CLEAR_REQUESTED} from './domain/contracts';
import type {EventPublisher} from './domain/types';
import {StreamConsumer} from './consumer/stream-consumer';
import {MovingConeCalculator} from './decision/moving-cone-calculator';
import {YieldActionResolver} from './decision/yield-action-resolver';
import {YieldDecisionService} from './decision/yield-decision-service';
import {YieldTargetEvaluator} from './decision/yield-target-evaluator';
import {DispatchLifecycleService} from './dispatch/dispatch-lifecycle-service';
import {EmergencyEventSubscriber} from './dispatch/emergency-event-subscriber';
import {VehicleStateSubscriber} from './dispatch/vehicle-state-subscriber';
import {HealthServer, type HealthStatus} from './infra/health-server';
import {logger, type Logger} from './infra/logger';
import {createPostgres} from './infra/postgres';
import {createRedis} from './infra/redis';
import {RedisAlertOutboxDispatcher} from './publishing/redis-alert-outbox-dispatcher';
import {RedisStreamEventPublisher} from './publishing/redis-stream-event-publisher';
import {DispatchRouteRepository} from './route/dispatch-route-repository';
import {DensitySnapshotError, EdgeWeightCalculator} from './route/edge-weight-calculator';
import {RoadGraph} from './route/road-graph';
import {RouteOutboxDispatcher} from './route/route-outbox-dispatcher';
import {RoutePlanner} from './route/route-planner';
import {RouteScheduler} from './route/route-scheduler';
import {RouteStabilizer} from './route/route-stabilizer';
import {DecisionStore} from './store/decision-store';
import {DispatchStateStore} from './store/dispatch-state-store';
import {VehicleIndexReader} from './store/vehicle-index-reader';

export interface WorkerDependencies {redis?: Redis; pool?: Pool; logger?: Logger}
/** Owns its Redis connections; an injected PostgreSQL pool remains caller-owned. */
export class Worker {
  private readonly log: Logger;
  private readonly health: HealthServer;
  private connections: Redis[] = [];
  private pool?: Pool;
  private consumers: StreamConsumer[] = [];
  private scheduler?: RouteScheduler;
  private weights?: EdgeWeightCalculator;
  private timer?: ReturnType<typeof setInterval>;
  private delivery?: Promise<void>;
  private deliveryError?: unknown;
  private started = false;
  private stopping?: Promise<{drained: boolean}>;
  constructor(private readonly config: WorkerConfig, private readonly dependencies: WorkerDependencies = {}) {
    this.log = dependencies.logger ?? logger;
    this.health = new HealthServer(config.healthPort, () => this.readiness());
  }
  get healthUrl(): string {return this.health.url;}

  async start(): Promise<void> {
    if (this.started) return;
    if (this.stopping) throw new Error('A stopped worker cannot be restarted; create a new instance');
    await this.health.start();
    try {
      const makeConnection = (role: string) => {
        const options = {maxRetriesPerRequest: 1, commandTimeout: 2000, connectTimeout: 2000,
          enableOfflineQueue: false, connectionName: `${this.config.instanceId}:${role}`};
        const redis = this.dependencies.redis?.duplicate(options) ?? createRedis(this.config, options);
        redis.on('error', error => this.log.error('Redis connection error', error));
        this.connections.push(redis); return redis;
      };
      const commands = makeConnection('commands'), publisherConnection = makeConnection('publisher'), density = makeConnection('density');
      await Promise.all(this.connections.map(redis => new Promise<void>((resolve, reject) => {
        if (redis.status === 'ready') {resolve(); return;}
        redis.once('ready', resolve); redis.once('error', reject);
      })));
      const graph = this.config.graphPath
        ? RoadGraph.load(JSON.parse(await readFile(this.config.graphPath, 'utf8')), this.config.h3Resolution) : undefined;
      const dispatches = new DispatchStateStore(commands, this.config), vehicles = new VehicleIndexReader(commands);
      const cones = new MovingConeCalculator(this.config.h3Resolution), evaluator = new YieldTargetEvaluator(graph);
      const lifecycle = new DispatchLifecycleService(commands, dispatches,
        {requestImmediate: id => this.scheduler?.requestImmediate(id)});
      const decisions = new YieldDecisionService(lifecycle, dispatches, new DecisionStore(commands, this.config),
        vehicles, cones, evaluator, new YieldActionResolver(), this.config, Date.now, graph);
      const transport = new RedisStreamEventPublisher(publisherConnection, this.config);
      const publisher: EventPublisher = {publish: async (stream, event) => {
        const receipt = await transport.publish(stream, event);
        this.log.info('Canonical request published', {stream, eventKey: event.eventId, eventType: event.eventType,
          targetVehicleId: event.payload.targetVehicleId, messageId: receipt.messageId,
          ...(event.eventType === YIELD_ALERT_CLEAR_REQUESTED ? {reason: event.payload.reason} : {})});
        return receipt;
      }};
      const alertOutbox = new RedisAlertOutboxDispatcher(publisherConnection, this.config, publisher);
      let repository: DispatchRouteRepository | undefined, routeOutbox: RouteOutboxDispatcher | undefined;
      if (this.config.routePersistenceEnabled) {
        this.pool = this.dependencies.pool ?? createPostgres(this.config, {query_timeout: 2000, statement_timeout: 2000});
        this.pool.on('error', this.onPoolError);
        repository = new DispatchRouteRepository(this.pool);
        this.weights = new EdgeWeightCalculator(density, this.config);
        routeOutbox = new RouteOutboxDispatcher(repository, publisher, this.config.alertStream);
        this.scheduler = new RouteScheduler({lifecycle, dispatches, vehicles, weights: this.weights,
          planner: new RoutePlanner(graph ?? null, this.weights), repository, stabilizer: new RouteStabilizer(), config: this.config,
          selectRouteChangeClears: async (dispatch, _previous, next, activeDecisions) => {
            const states = await vehicles.getStates([dispatch.emergencyVehicleId, ...activeDecisions.map(value => value.targetVehicleId)]);
            const emergency = states.find(value => value.vehicleId === dispatch.emergencyVehicleId);
            if (!emergency) return [];
            const cone = cones.buildRouteCone(emergency, next, graph);
            const relevant = new Set(evaluator.filterCandidates(emergency,
              states.filter(value => value.lastMeasuredAt > Date.now() - this.config.inactiveTtlMs), cone, next)
              .map(value => value.vehicle.vehicleId));
            return activeDecisions.filter(value => !relevant.has(value.targetVehicleId))
              .map(({alertId, targetVehicleId}) => ({alertId, targetVehicleId}));
          }});
      }
      const stateSubscriber = new VehicleStateSubscriber(lifecycle, {
        onEmergencyState: async (event, dispatch) => {
          const route = await repository?.findLatest(dispatch.dispatchId);
          if (route) await decisions.evaluate(dispatch.dispatchId, {route: {outcome: 'ROUTE_FOUND',
            roadSegments: route.roadSegments, waypoints: route.routeCoordinates,
            estimatedTravelSeconds: route.estimatedDuration, congestedSegments: [], distanceM: 0}});
          else await decisions.onEmergencyState(event, dispatch);
        },
        onNormalState: event => decisions.onNormalState(event),
      });
      this.consumers = [
        new StreamConsumer(commands, this.config.stateStream, this.config.vehicleConsumerGroup, this.config, stateSubscriber.handle),
        new StreamConsumer(commands, this.config.emergencyStream, this.config.emergencyConsumerGroup, this.config,
          new EmergencyEventSubscriber(lifecycle).handle),
      ];
      for (const consumer of this.consumers) {
        await consumer.start();
      }
      const deliver = () => {
        if (this.delivery) return;
        this.delivery = (async () => {await alertOutbox.dispatchPending(); await routeOutbox?.dispatchPending();})()
          .then(() => {this.deliveryError = undefined;})
          .catch(error => {this.deliveryError = error; this.log.error('Outbox delivery failed', error);})
          .finally(() => {this.delivery = undefined;});
      };
      this.started = true;
      this.scheduler?.start();
      deliver(); this.timer = setInterval(deliver, Math.min(250, this.config.routeIntervalMs));
      this.log.info('Emergency Worker started', {healthUrl: this.healthUrl, routing: this.config.routePersistenceEnabled,
        graph: graph?.version ?? 'unavailable'});
    } catch (error) {await this.stop(); throw error;}
  }
  private readonly onPoolError = (error: Error) => this.log.error('PostgreSQL connection error', error);

  async readiness(): Promise<HealthStatus> {
    const reasons: string[] = [], degraded: string[] = [];
    if (!this.started) reasons.push('worker not running');
    for (const consumer of this.consumers) {
      if (!consumer.ready) reasons.push(`${consumer.stream}: ${consumer.configurationBlocked ? 'configuration blocked' : 'disconnected'}`);
    }
    await Promise.all(this.connections.map(async (redis, index) => {
      try {await redis.ping();} catch {reasons.push(`redis connection ${index} unavailable`);}
    }));
    if (this.pool) {
      try {await this.pool.query('SELECT route_id FROM dispatch_route LIMIT 0');
        await this.pool.query('SELECT outbox_id FROM emergency_route_outbox LIMIT 0');}
      catch {reasons.push('PostgreSQL or route schema unavailable');}
    }
    if (this.weights) {
      try {const snapshot = await this.weights.loadDensitySnapshot(); if (snapshot.degraded) degraded.push(snapshot.edgeDensity);}
      catch (error) {reasons.push(error instanceof DensitySnapshotError ? error.code : 'density unavailable');}
      if (!this.config.graphPath) degraded.push('GRAPH_UNAVAILABLE');
    }
    if (this.deliveryError) reasons.push('outbox delivery failed');
    if (this.scheduler?.lastResult.outcome === 'CONFIGURATION_BLOCKED') reasons.push('route configuration blocked');
    return {ready: reasons.length === 0, reasons, degraded};
  }

  stop(): Promise<{drained: boolean}> {
    return this.stopping ??= this.shutdown();
  }
  private async shutdown(): Promise<{drained: boolean}> {
    this.started = false;
    if (this.timer) clearInterval(this.timer);
    let timer: ReturnType<typeof setTimeout> | undefined;
    const work = Promise.allSettled([...this.consumers.map(consumer => consumer.stop()), this.scheduler?.stop(), this.delivery])
      .then(results => {
        const failures = results.filter(result => result.status === 'rejected');
        for (const failure of failures) this.log.error('Worker drain failed', failure.reason);
        return failures.length === 0;
      });
    const finished = await Promise.race([work, new Promise<boolean>(resolve => {
      timer = setTimeout(() => resolve(false), this.config.shutdownDrainTimeoutMs);
    })]);
    if (timer) clearTimeout(timer);
    for (const connection of this.connections) connection.disconnect();
    if (this.pool) {
      this.pool.off('error', this.onPoolError);
      if (!this.dependencies.pool) void this.pool.end().catch(error => this.log.error('PostgreSQL shutdown failed', error));
    }
    await this.health.stop();
    const drained = finished && this.consumers.every(consumer => consumer.drained);
    this.log.info('Emergency Worker stopped', {drained});
    return {drained};
  }
}
