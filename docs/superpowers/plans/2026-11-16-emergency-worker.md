# Emergency Worker Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a standalone TypeScript Emergency Worker that maintains active dispatches, identifies vehicles that should yield to an emergency vehicle, publishes only decision changes, and independently recalculates routes from the current traffic-density snapshot.

**Architecture:** Two Redis Streams consumers feed one application: dispatch events update dispatch lifecycle state, and `VEHICLE_STATE_UPDATED` events drive the low-latency decision path. The decision path reads Location Worker's H3/ZSET and state hashes, then uses one Redis Lua transaction to persist current dispatch/cone/decision state and append durable alert outbox entries. A separate scheduler reads a generation-consistent density snapshot, runs A*, and atomically persists immutable route versions plus route outbox records in PostgreSQL; dispatchers deliver both outboxes to Redis Streams without blocking decisions.

**Tech Stack:** Node.js 26+, TypeScript 6, Jest 30, ioredis 6, h3-js 4.5, PostgreSQL (`pg`), Redis Streams/Lua, Docker Compose.

**Spec:** `../../../../상세요구사항명세서_윰씨윰씨.pdf` (F-DEC, F-ALT, F-RTE, F-VAL) and `../../../../상세설계서수정본_윰씨윰씨.pdf` (pp. 12–15, 65–74, 99–106). Existing producer contract: `../../../../Location-Worker/.moai/specs/SPEC-LOC-WORKER-001/design.md` and `../../../../Location-Worker/services/location-worker/src/domain/contracts.ts`.

## Scope and decisions

- Implement the Emergency Worker as an independent repository in this directory, following the Location Worker's Node/TypeScript/Jest conventions.
- Fast-Path is the only synchronous stream work: emergency vehicle updates re-evaluate its cone; normal vehicle updates re-evaluate only that vehicle when it is in an active cone. Route planning always runs outside this path at a configurable 1–3 second interval.
- Use `stream:vehicle:state` as the mandatory input and `h3:vehicles:{cell}` ZSETs plus `vehicle:{id}:state` hashes as Location Worker read models. Accept a density snapshot when `density:cells._generation` equals `density:meta.generation`; require `density:edges._generation` only when `density:meta.edgeDensity` is `enabled`. When edge density is disabled and `density:edges` is absent, compute free-flow edge weights, expose degraded routing in readiness/logs, and keep Fast-Path running.
- Store active dispatches under `dispatch:{dispatchId}:state`, cone cells under `dispatch:{dispatchId}:cone`, and per-target decisions under `dispatch:{dispatchId}:decisions`. State is reconstructable from dispatch events; route history is append-only in PostgreSQL `dispatch_route`.
- Publish canonical `YIELD_ALERT_REQUESTED`, `YIELD_ALERT_CLEAR_REQUESTED`, and `ROUTE_UPDATE_REQUESTED` envelopes to `stream:alert:event` by default (configurable). `YIELD_ALERT_REQUESTED` has exactly `alertId`, `emergencyId`, `dispatchId`, `emergencyVehicleId`, `targetVehicleId`, `approachDirection`, `estimatedArrivalSeconds`, `recommendedAction`, `reason`, `decisionVersion`, and `expiresAt`; `alertId` is stable per dispatch/target and `decisionVersion` is monotonic. The Gateway projection maps `targetVehicleId` to `vehicleId`, `decisionVersion` to `alertVersion`, adds `issuedAt`, and places the full canonical event in `details` on `EVACS_ALERT_STREAM=evacs:stream:yield-alert`; `expiresAt = issuedAt + ALERT_TTL_MS` (default `20_000`, configurable and positive).
- Fast-Path never uses a save-then-publish sequence: its Lua transaction saves the next decision state and `XADD`s idempotency-keyed alert outbox records together. A Redis outbox dispatcher marks an entry delivered only after its destination `XADD` succeeds. Slow-Path inserts the new `dispatch_route` row and an idempotency-keyed route/clear outbox row in the same PostgreSQL transaction; its dispatcher provides at-least-once, deduplicable delivery.
- A Gateway consumer for clear requests does not exist today. This repository will prove clear-event emission in Redis, but end-to-end clearing at a client requires the separately owned Gateway contract to add `YIELD_ALERT_CLEARED` handling. Do not silently pretend that a request event has reached a client.
- Treat road-graph import / SUMO-or-OSM conversion and the optional vehicle-plus-walking route (P2) as explicit follow-up inputs, not hidden production assumptions. The first implementation accepts a versioned precomputed graph file and returns a typed `NO_ROUTE` outcome when it is absent.
- Normalize the ERD's legacy `ARRIVED` state to the event-contract status `ARRIVED_ON_SCENE`. On `ARRIVED_ON_SCENE` followed by `TRANSPORTING` with a next destination, retain the dispatch, update its destination, request an immediate new route, and supersede only decisions no longer relevant to the new leg. On `COMPLETED` or `CANCELED`, one Redis Lua transaction must enqueue one `YIELD_ALERT_CLEAR_REQUESTED(reason=EMERGENCY_COMPLETED)` per active target before deleting its cone/decision/dispatch state; this is the only path that clears every active alert.

## Global Constraints

- Inputs from real GPS and SUMO use identical event contracts; no SUMO-specific branch may enter decision logic.
- H3 resolution is 9 by default; active membership is read only when ZSET score is newer than the configured inactive TTL.
- Validate every consumed `h3CellId` has `H3_RESOLUTION`; when density metadata exists, require `density:meta.h3Resolution` to match too. On mismatch, classify an otherwise valid record as `CONFIGURATION_BLOCKED` (not malformed): fail readiness and retain it in the PEL without applying `MAX_DELIVERIES`; after configuration correction/restart, reclaim and reprocess it. Syntax/schema-invalid envelopes alone go to the bounded DLQ.
- Moving-cone distance is `clamp(speedMps * 20 s, 100 m, 500 m)` with a half-angle of 30°; include the emergency vehicle cell and its immediate H3 neighbors.
- Exclude candidates with heading difference at least 135°. Without road-edge data, retain only candidates whose heading difference is at most 45°.
- The alert ETA window is inclusive 3–30 seconds. A target behind the emergency vehicle is passed and must be cleared.
- Persist and publish a new decision version only when the semantically relevant decision changes; alerts have stable IDs per dispatch/target and monotonically increasing versions.
- Route replacement requires at least 15% lower estimated travel time and at least 5 seconds since the prior replacement.
- The route scheduler interval must be configurable and validated within 1000–3000 ms. It must not share a blocking Redis connection with stream readers.
- Consumer messages are acknowledged only after their Redis/PostgreSQL state-and-outbox transaction succeeds. Outbox delivery is independently retried; syntax/schema-invalid envelopes go to a bounded DLQ; transient failures and valid `CONFIGURATION_BLOCKED` records remain pending under their distinct retry policies.
- Use JSON-line logs plus `/healthz` and `/readyz`; readiness requires Redis, both consumer groups, and PostgreSQL when route persistence is enabled.

## Review Focus

- A state update for an emergency vehicle without an active dispatch must be acknowledged and produce no read-model or alert side effect. Test in Task 3.
- A normal vehicle that exits a cone after being alerted must emit exactly one clear request, including when it moves between cells. Test in Task 5.
- Concurrent updates that read a stale decision must not downgrade `decisionVersion` or re-issue the same alert. Test in Task 5 with a Lua compare-and-set fixture.
- Density keys with mismatched generations must never form a mixed route calculation; scheduler skips that tick and logs the reason. Test in Task 7.
- A malformed/expired dispatch event must be DLQ'd or ignored safely without blocking later valid entries in the consumer group. Test in Task 3.

## File Structure

| Path | Responsibility |
| --- | --- |
| `package.json`, `tsconfig*.json`, `jest.config.js`, `.gitignore` | Node 26 / TypeScript / Jest project tooling. |
| `src/domain/contracts.ts`, `src/domain/types.ts` | Versioned event envelopes, dispatch/vehicle/cone/decision/route types and constants. |
| `src/config/config.ts` | Typed, fail-fast environment configuration. |
| `src/infra/redis.ts`, `src/infra/postgres.ts`, `src/infra/health-server.ts`, `src/infra/logger.ts` | Connections, readiness/liveness and operational output. |
| `src/consumer/stream-consumer.ts`, `src/consumer/codecs.ts`, `src/consumer/dlq.ts` | Generic Redis Stream group lifecycle, parsing, retry/DLQ and ACK ordering. |
| `src/store/dispatch-state-store.ts`, `src/store/vehicle-index-reader.ts`, `src/store/decision-store.ts`, `src/store/lua/compare-save-and-enqueue.lua` | Redis dispatch/cone/decision persistence, H3 state read model, atomic decision/outbox changes. |
| `src/decision/{moving-cone-calculator,yield-target-evaluator,yield-action-resolver,yield-decision-service}.ts`, `src/route/road-graph.ts` | Fast-Path geometry, route-cell mapping, filtering and alert-outbox construction. |
| `src/dispatch/{dispatch-lifecycle-service,emergency-event-subscriber,vehicle-state-subscriber}.ts` | Emergency creation, dispatch/start/end transitions, both input-stream subscribers, and associated cleanup. |
| `src/route/{edge-weight-calculator,route-planner,route-stabilizer,route-scheduler,dispatch-route-repository,route-outbox-dispatcher}.ts`, `migrations/001_dispatch_route.sql` | Slow-Path density-aware cost, A*, append-only route/outbox schema, persistence and cadence. |
| `src/publishing/{redis-stream-event-publisher,redis-alert-outbox-dispatcher}.ts` | Canonical event publishing, current Gateway-compatible alert projection, and Redis outbox delivery. |
| `src/worker.ts`, `src/main.ts` | Dependency composition, startup and graceful shutdown. |
| `test/unit/**`, `test/integration/**`, `test/fixtures/**`, `test/support/**` | Deterministic algorithms, Redis/Postgres integration and local-stack end-to-end fixtures. |
| `Dockerfile.local`, `deploy/.env.example` | Reproducible local container build and documented configuration. |

### Task 1: Bootstrap the isolated worker and freeze its contracts

**Files:**
- Create: `package.json`, `package-lock.json`, `tsconfig.json`, `tsconfig.build.json`, `jest.config.js`, `.gitignore`
- Create: `src/domain/contracts.ts`, `src/domain/types.ts`, `src/config/config.ts`
- Test: `test/unit/config/config.test.ts`, `test/unit/domain/contracts.test.ts`

**Interfaces:**
- Produces: `WorkerConfig`, `EventEnvelope<T>`, `VehicleStateUpdatedEvent`, `EmergencyEvent`, `YieldAlertRequestedEvent`, `YieldAlertClearRequestedEvent`, `RouteUpdateRequestedEvent`, `DispatchState`, `YieldDecision`, and `RouteResult`.
- Consumes: Location Worker's `VEHICLE_STATE_UPDATED` v1 envelope and Redis field names exactly as cited above.

- [ ] **Step 1: Write failing configuration and codec tests**

Assert Node 26-compatible defaults, reject a scheduler interval outside 1000–3000 ms or a non-positive `ALERT_TTL_MS`, parse the Location Worker event with `event` (not legacy stream fields), and reject any request event missing a required p103 field or using an unknown `approachDirection`, `recommendedAction`, or `reason` enum.

- [ ] **Step 2: Run the focused tests to verify they fail**

Run: `npm test -- --selectProjects unit test/unit/config/config.test.ts test/unit/domain/contracts.test.ts`

Expected: FAIL because project scripts and exported contracts do not yet exist.

- [ ] **Step 3: Add package/tooling and `loadConfig(env): WorkerConfig`**

Use the Location Worker dependency versions (`ioredis@6.0.0`, `h3-js@4.5.0`, Jest 30, TypeScript 6); add `pg` and its types. Define every stream/key/environment constant in `contracts.ts`, including all p103–105 request/clear fields, fixed enums, `ALERT_TTL_MS`, outbox/deduplication keys, and the local Gateway projection fields.

- [ ] **Step 4: Run unit checks**

Run: `npm run typecheck && npm test -- --selectProjects unit test/unit/config/config.test.ts test/unit/domain/contracts.test.ts`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add package.json package-lock.json tsconfig.json tsconfig.build.json jest.config.js .gitignore src/domain src/config test/unit
git commit -m "chore: bootstrap emergency worker contracts"
```

### Task 2: Implement read models and atomic decision persistence

**Files:**
- Create: `src/infra/redis.ts`, `src/store/dispatch-state-store.ts`, `src/store/vehicle-index-reader.ts`, `src/store/decision-store.ts`, `src/store/lua/compare-save-and-enqueue.lua`, `src/publishing/redis-alert-outbox-dispatcher.ts`
- Test: `test/integration/store/dispatch-state-store.test.ts`, `test/integration/store/vehicle-index-reader.test.ts`, `test/integration/store/decision-store.test.ts`

**Interfaces:**
- Consumes: `WorkerConfig`, `DispatchState`, `Cone`, `YieldDecision` from Task 1 and Location Worker keys `h3:vehicles:{cell}`, `vehicle:{id}:state`.
- Produces: `DispatchStateStore.save/findById/saveCone/findCone/findDecisions/completeAndEnqueueClears`, `VehicleIndexReader.findVehicleIdsInCells(cells, cutoffMs)`, `getStates(ids)`, `DecisionStore.compareSaveAndEnqueue(dispatchId, expectedVersion, next, events): Promise<DecisionDiff>`, and `RedisAlertOutboxDispatcher.dispatchPending(): Promise<void>`.

- [ ] **Step 1: Write Redis integration tests with the local Redis schema**

Seed ZSET scores at `cutoff-1`, `cutoff`, and `cutoff+1`; assert only the strict-active member and matching state hash are returned. Assert two concurrent writes for one dispatch yield one accepted next version and no duplicate delta. For a completed dispatch with three active decisions, assert one Lua execution adds three `EMERGENCY_COMPLETED` clear records before removing all decision/cone/dispatch keys. Inject a destination-stream `XADD` failure after the Lua commit, restart the dispatcher, and assert the durable outbox emits exactly one deduplicable request rather than losing it.

- [ ] **Step 2: Run the integration tests to verify they fail**

Run: `REDIS_URL=redis://127.0.0.1:6401 npm test -- --selectProjects integration test/integration/store`

Expected: FAIL because repositories and Lua script do not exist.

- [ ] **Step 3: Implement repositories and one Redis Lua compare-and-save**

The decision Lua script must compare stored `decisionVersion`, atomically write the serialized decision map plus next version, append all added/changed/removed canonical request/clear events to `stream:emergency-worker:alert-outbox`, and return the delta/outbox IDs. A separate completion Lua script must append every `EMERGENCY_COMPLETED` clear before deleting the dispatch's active decision/cone/state keys. Outbox records carry a deterministic event key; the dispatcher retries its destination `XADD` until receipt, then atomically records delivery. Reads must pipeline HGETALL calls, validate `h3CellId` resolution, and ignore missing/inactive vehicle states.

- [ ] **Step 4: Run store checks**

Run: `REDIS_URL=redis://127.0.0.1:6401 npm test -- --selectProjects integration test/integration/store`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/infra/redis.ts src/store src/publishing/redis-alert-outbox-dispatcher.ts test/integration/store
git commit -m "feat: add Redis dispatch and vehicle read models"
```

### Task 3: Add reliable stream consumption and dispatch lifecycle

**Files:**
- Create: `src/consumer/stream-consumer.ts`, `src/consumer/codecs.ts`, `src/consumer/dlq.ts`
- Create: `src/dispatch/dispatch-lifecycle-service.ts`, `src/dispatch/emergency-event-subscriber.ts`, `src/dispatch/vehicle-state-subscriber.ts`
- Test: `test/integration/consumer/stream-consumer.test.ts`, `test/integration/dispatch/dispatch-lifecycle-service.test.ts`

**Interfaces:**
- Consumes: stores from Task 2; `EventPublisher.publish(stream, event)` (stubbed until Task 5); `RouteScheduler.requestImmediate(dispatchId)` (stubbed until Task 7).
- Produces: `StreamConsumer.start/stop`, `DispatchLifecycleService.onEmergencyCreated/onDispatched/onStatusChanged/completeDispatch/findActiveByVehicleId/findActiveDispatches`.

- [ ] **Step 1: Write consumer and lifecycle integration tests**

Cover `EMERGENCY_DISPATCHED` activating a single dispatch and scheduling its first route search; `COMPLETED` and `CANCELED` atomically enqueueing one `EMERGENCY_COMPLETED` clear per active target before state deletion; malformed/expired events going to DLQ; a transient handler error remaining pending; an emergency vehicle state update without an active dispatch producing no side effect; and `ARRIVED` normalization to `ARRIVED_ON_SCENE`, then `TRANSPORTING` with a new destination retaining the dispatch and requesting an immediate next-leg route. Assert a valid incompatible-H3 event enters `CONFIGURATION_BLOCKED`, bypasses delivery-cap DLQ, and is reclaimed successfully after configuration correction.

- [ ] **Step 2: Run tests to verify they fail**

Run: `REDIS_URL=redis://127.0.0.1:6401 npm test -- --selectProjects integration test/integration/consumer test/integration/dispatch`

Expected: FAIL because consumers and lifecycle handlers do not exist.

- [ ] **Step 3: Implement `StreamConsumer` with consumer groups, reclaim, ACK-after-commit, DLQ, and drain**

Use named groups for dispatch and vehicle-state streams, bounded `MAX_DELIVERIES`, and a separate blocking connection. `EmergencyEventSubscriber` routes lifecycle events and `VehicleStateSubscriber` routes typed emergency/normal state events to the Fast-Path. `DispatchLifecycleService` must validate legal status transitions, normalize legacy `ARRIVED`, save current state, request the initial or next-leg route, and call `completeAndEnqueueClears` on completion/cancellation. A state event whose H3 resolution does not match configuration becomes `CONFIGURATION_BLOCKED`, remains pending without delivery-cap DLQ, and sets readiness false until corrected/reclaimed.

- [ ] **Step 4: Run consumer/lifecycle tests**

Run: `REDIS_URL=redis://127.0.0.1:6401 npm test -- --selectProjects integration test/integration/consumer test/integration/dispatch`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/consumer src/dispatch test/integration/consumer test/integration/dispatch
git commit -m "feat: consume dispatch events and manage lifecycle"
```

### Task 4: Implement deterministic Fast-Path geometry and candidate evaluation

**Files:**
- Create: `src/decision/moving-cone-calculator.ts`, `src/decision/yield-target-evaluator.ts`, `src/decision/yield-action-resolver.ts`
- Create: `src/route/road-graph.ts`
- Test: `test/unit/decision/moving-cone-calculator.test.ts`, `test/unit/decision/yield-target-evaluator.test.ts`, `test/unit/decision/yield-action-resolver.test.ts`, `test/unit/route/road-graph.test.ts`

**Interfaces:**
- Consumes: `VehicleState`, `DispatchState`, `Cone`, route-edge identifiers, and `RouteResult` types.
- Produces: `RoadGraph.load/getCellsForEdge/findNearestEdge`, `MovingConeCalculator.computeDistance/buildCone/buildRouteCone/diffCells`, `YieldTargetEvaluator.filterCandidates/calculateEta/isInAlertWindow/isPassed`, and `YieldActionResolver.resolveDirection/resolveAction/resolveReason`.

- [ ] **Step 1: Write failing algorithm tests**

Start by loading a minimal versioned edge–cell graph fixture. Assert 0 m/s → 100 m, 25 m/s → 500 m, and values beyond → 500 m; include self + H3 neighbors; test ±30° geometry; add turn-edge cells to the cone; reject 135° opposing headings; enforce 3, 30 second ETA bounds; mark a rear target passed; and assert action/reason enum mapping for same-route, slow, and intersection cases. Include F-DEC-05: an incoming road at a planned turn is considered while a nearby unconnected road is excluded.

- [ ] **Step 2: Run tests to verify they fail**

Run: `npm test -- --selectProjects unit test/unit/decision`

Expected: FAIL because algorithm modules do not exist.

- [ ] **Step 3: Implement pure calculation modules**

Implement and validate the read-only `RoadGraph` edge–cell mapping before Fast-Path. Calculate great-circle distance, bearings, relative closing speed, and the H3 cell set without Redis access. Add route cells for the current/upcoming turn when available; otherwise keep the documented heading fallback.

- [ ] **Step 4: Run decision-algorithm checks**

Run: `npm run typecheck && npm test -- --selectProjects unit test/unit/decision`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/decision src/route/road-graph.ts test/unit/decision test/unit/route/road-graph.test.ts
git commit -m "feat: add moving-cone yield evaluation"
```

### Task 5: Orchestrate Fast-Path decisions and publish deltas

**Files:**
- Create: `src/decision/yield-decision-service.ts`, `src/publishing/redis-stream-event-publisher.ts`
- Test: `test/integration/decision/yield-decision-service.test.ts`, `test/integration/publishing/redis-stream-event-publisher.test.ts`

**Interfaces:**
- Consumes: Task 2 stores, Task 4 evaluators, `DispatchLifecycleService.findActiveByVehicleId/findActiveDispatches` and `EventPublisher.publish(stream, event): Promise<PublishReceipt>`.
- Produces: `YieldDecisionService.onEmergencyVehicleMoved/onNormalVehicleMoved/evaluate`, atomic alert-outbox records, and the Gateway-compatible issue projection.

- [ ] **Step 1: Write failing delta and integration tests**

Seed Location Worker state for one emergency and several normal vehicles. Verify: only valid cone/ETA targets receive requests; the canonical request has every p103 field and a valid enum; the second identical update writes no outbox event; a materially changed decision increments `decisionVersion`/projected `alertVersion`; an alerted vehicle leaving the cone emits exactly one `YIELD_ALERT_CLEAR_REQUESTED`; concurrent evaluations cannot decrease `decisionVersion`; and a crash after the decision transaction but before delivery is recovered from the outbox without duplicate external effects.

- [ ] **Step 2: Run tests to verify they fail**

Run: `REDIS_URL=redis://127.0.0.1:6401 npm test -- --selectProjects integration test/integration/decision test/integration/publishing`

Expected: FAIL because Fast-Path orchestration and publisher do not exist.

- [ ] **Step 3: Implement evaluation, diffing, and atomic save-and-enqueue**

For an emergency update, rebuild the cone, bulk-read candidates, evaluate, then call Task 2's atomic compare-save-and-enqueue operation. For a normal update, inspect all active cones and evaluate that vehicle only. Generate deterministic alert IDs from `dispatchId` and target vehicle ID; `expiresAt` is exactly `issuedAt + ALERT_TTL_MS`; include the full canonical event in the Gateway `payload.details` without making the decision layer Gateway-aware. The Task 2 dispatcher—not this service—publishes durable records.

- [ ] **Step 4: Run Fast-Path checks**

Run: `REDIS_URL=redis://127.0.0.1:6401 npm test -- --selectProjects integration test/integration/decision test/integration/publishing`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/decision/yield-decision-service.ts src/publishing test/integration/decision test/integration/publishing
git commit -m "feat: publish yield decision deltas"
```

### Task 6: Add graph loading and density-aware A* routing

**Files:**
- Create: `src/route/edge-weight-calculator.ts`, `src/route/route-planner.ts`, `test/fixtures/road-graph.json`
- Test: `test/unit/route/edge-weight-calculator.test.ts`, `test/unit/route/route-planner.test.ts`

**Interfaces:**
- Consumes: Task 4's `RoadGraph`, `RouteResult`, `DensitySnapshot`, and a versioned graph JSON (`nodes`, directed `edges`, length, lanes, speed limit, H3 cells).
- Produces: `EdgeWeightCalculator.loadDensitySnapshot/weight/isBlocked`, `RoutePlanner.findRoute/findCongestedSegments`.

- [ ] **Step 1: Write failing route tests**

Use a four-node fixture to prove A* chooses lower travel time rather than lower distance, rejects an edge at the blocked threshold, accepts `cells/meta` with edge density disabled and uses free-flow weights, rejects a snapshot with a mismatched required generation, and returns `NO_ROUTE` for a disconnected destination.

- [ ] **Step 2: Run tests to verify they fail**

Run: `npm test -- --selectProjects unit test/unit/route`

Expected: FAIL because graph and routing modules do not exist.

- [ ] **Step 3: Implement graph validation, BPR weights, and A***

Implement `t0 * (1 + alpha * (density / capacity)^beta)` with alpha, beta, capacity, and blocked multiplier configured explicitly. `loadDensitySnapshot` reads all density hashes in one Redis transaction/pipeline: it accepts `cells/meta` matching generation with `edgeDensity=disabled:*` and emits an explicit free-flow/degraded status; it requires matching `edges` generation only with `edgeDensity=enabled`. It validates `density:meta.h3Resolution` against `H3_RESOLUTION` and fails readiness rather than calculate with incompatible cells.

- [ ] **Step 4: Run routing tests**

Run: `npm run typecheck && npm test -- --selectProjects unit test/unit/route`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/route/edge-weight-calculator.ts src/route/route-planner.ts test/unit/route/edge-weight-calculator.test.ts test/unit/route/route-planner.test.ts test/fixtures/road-graph.json
git commit -m "feat: add density-aware route planning"
```

### Task 7: Add route persistence, stabilization, and the Slow-Path scheduler

**Files:**
- Create: `src/infra/postgres.ts`, `src/route/dispatch-route-repository.ts`, `src/route/route-stabilizer.ts`, `src/route/route-scheduler.ts`, `src/route/route-outbox-dispatcher.ts`
- Create: `migrations/001_dispatch_route.sql`
- Modify: `../../../../docker-compose.local.yml` (add local PostgreSQL before the scheduler is integrated)
- Test: `test/unit/route/route-stabilizer.test.ts`, `test/integration/route/route-scheduler.test.ts`, `test/integration/route/dispatch-route-repository.test.ts`

**Interfaces:**
- Consumes: active dispatch list from Task 3; planner/calculator from Task 6; publisher from Task 5.
- Produces: `DispatchRouteRepository.saveNewVersionWithOutbox/findLatest`, `RouteStabilizer.shouldReplace`, `RouteScheduler.start/stop/requestImmediate/tick`, and `RouteOutboxDispatcher.dispatchPending`.

- [ ] **Step 1: Write failing persistence/scheduler tests**

Assert the migration creates immutable `dispatch_route(route_id UUID PK, dispatch_id UUID NOT NULL, route_version INT NOT NULL, route_coordinates JSONB NOT NULL, road_segments JSONB NOT NULL, estimated_duration INT NOT NULL, change_reason VARCHAR NULL, created_at TIMESTAMPTZ NOT NULL, UNIQUE(dispatch_id, route_version))` plus a durable route outbox table. Assert versions append rather than overwrite; 14% improvement or an elapsed 4.999 seconds does not replace a route; 15% improvement after 5 seconds does; matching density keys issue one `ROUTE_UPDATE_REQUESTED` carrying route version, waypoints, congested segments, and estimated duration; mismatched required density generations skip the tick; a database commit followed by Redis delivery failure is retried from the outbox; and an immediate request coalesces per dispatch instead of creating concurrent searches.

- [ ] **Step 2: Add the local PostgreSQL dependency and run tests to verify they fail**

Add a healthy `postgres:16-alpine` `postgres` service to the root local Compose file with host port `5433`, database/user/password `evacs`, and run the migration once.

Run: `REDIS_URL=redis://127.0.0.1:6401 DATABASE_URL=postgres://evacs:evacs@127.0.0.1:5433/evacs npm test -- --selectProjects unit integration test/unit/route/route-stabilizer.test.ts test/integration/route`

Expected: FAIL because persistence and scheduler do not exist.

- [ ] **Step 3: Implement append-only persistence and scheduler**

Add a migration-owned append-only `dispatch_route` repository and `emergency_route_outbox` table (no implicit table creation in production startup). The repository inserts a route version and idempotency-keyed route/route-change-clear records in one PostgreSQL transaction. Scheduler uses an independent timer and connection, snapshots density once per tick, re-plans from latest emergency state, applies the stabilizer, then commits through that repository. `RouteOutboxDispatcher` retries Redis delivery until receipt and marks the record delivered afterward; route-change clears are created in the same transaction as the replacement route.

- [ ] **Step 4: Run Slow-Path checks**

Run: `REDIS_URL=redis://127.0.0.1:6401 DATABASE_URL=postgres://evacs:evacs@127.0.0.1:5433/evacs npm test -- --selectProjects unit integration test/unit/route/route-stabilizer.test.ts test/integration/route`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add migrations/001_dispatch_route.sql src/infra/postgres.ts src/route test/unit/route/route-stabilizer.test.ts test/integration/route
git commit -m "feat: schedule stabilized route updates"
```

### Task 8: Compose the worker, containerize it, and prove the local sandbox flow

**Files:**
- Create: `src/infra/health-server.ts`, `src/infra/logger.ts`, `src/worker.ts`, `src/main.ts`, `Dockerfile.local`, `deploy/.env.example`, `README.md`
- Modify: `../../../../docker-compose.local.yml`
- Modify: `../../../../README.local.md`
- Test: `test/integration/worker.e2e-spec.ts`

**Interfaces:**
- Consumes: all completed services and `Worker.start(): Promise<void>`, `Worker.stop(): Promise<{drained: boolean}>`.
- Produces: health endpoints, an `emergency-worker` Compose service, and repeatable Redis/SUMO sandbox verification.

- [ ] **Step 1: Write the end-to-end test and compose assertions**

With local Redis, seed a dispatched emergency vehicle plus normal vehicle state, start the worker, append `VEHICLE_STATE_UPDATED`, and assert a Gateway-compatible alert request is read from `evacs:stream:yield-alert`. Kill/restart the alert-outbox dispatcher after the decision commit and assert eventual one-key delivery. Then move the normal vehicle behind/outside the cone and assert exactly one canonical clear request. Seed an H3 resolution mismatch and assert `/readyz` becomes 503 with the event pending; assert the same for a required backing dependency disconnect.

- [ ] **Step 2: Run the end-to-end test to verify it fails**

Run: `REDIS_URL=redis://127.0.0.1:6401 npm test -- --selectProjects integration test/integration/worker.e2e-spec.ts`

Expected: FAIL because application composition, health server, and Compose service do not exist.

- [ ] **Step 3: Assemble lifecycle, health, Docker, and Compose configuration**

Use separate Redis command, reader, and publisher connections. Set the root Compose `emergency-worker` service dependencies on healthy Redis, Location Worker, and PostgreSQL; expose a non-conflicting health port, and document required `DATABASE_URL`, graph path, density settings, stream names, and the current Gateway-clear limitation. The root Compose file is a workspace integration artifact outside this child repository, so record its exact diff in `README.md`; do not imply that it is committed by this repository. Do not change existing service ports or stream names without an explicit compatibility adapter.

- [ ] **Step 4: Verify the local sandbox end-to-end**

Run: `docker compose -f ../docker-compose.local.yml up --build emergency-worker`

Expected: Worker becomes healthy, consumes Location Worker state events, emits a request the current Gateway accepts, and logs canonical clear requests when targets become irrelevant.

- [ ] **Step 5: Run the full quality gate**

Run: `npm run typecheck && npm test && npm run build && docker compose -f ../docker-compose.local.yml config`

Expected: all commands exit 0.

- [ ] **Step 6: Commit**

```bash
git add src Dockerfile.local deploy README.md test/integration/worker.e2e-spec.ts
git commit -m "feat: run emergency worker in local EVACS stack"
```

## Self-review

- **Spec coverage:** F-DEC candidate selection, direction, refresh, false-positive exclusion, and turns are covered by Tasks 4–5. F-ALT selective issue, content, dedupe/update, and clear requests are covered by Task 5 (Gateway clear delivery is explicitly external). F-RTE density-aware route, reroute, anti-flapping, and route update are covered by Tasks 6–7; P2 walking access remains an explicit follow-up. F-VAL uses the existing SUMO path and is exercised by Task 8.
- **Intentional gaps:** Dispatch/emergency REST creation, dashboard, Alert Worker delivery ledger, and Gateway clear consumption are outside this repository. They require owner-approved cross-service changes rather than speculative copies here.
- **Consistency:** All producer/consumer event codecs are centralized in Task 1; Redis writes that alter decision versions are centralized in Task 2; only Task 5 creates alert requests/clears; only Task 7 creates route updates.
- **Review focus mapping:** Items 1 and 5 are Task 3, items 2 and 3 are Task 5, and item 4 is Task 7.

## Execution Handoff

The repository and plan are prepared; no product code or dependencies have been created yet. The most sensible implementation method is native, sequential execution because the later tasks depend directly on stable contracts and Redis semantics established by the earlier ones. Review this plan before implementation, especially the documented Gateway clear-event contract gap.
