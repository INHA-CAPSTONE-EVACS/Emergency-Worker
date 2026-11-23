# Emergency Worker

Consumes Location Worker state and emergency lifecycle events, computes selective yield decisions, and publishes durable alert and route requests. Node.js 26+, Redis 7+, and MySQL 8.4 (the Control API Server's `evacs` database) are used by the local stack.

## Run locally

From this repository:

```sh
npm ci
cp deploy/.env.example .env
# Review .env, then load it in your shell (the application does not load dotenv).
set -a
. ./.env
set +a
npm run build
npm start
```

`DATABASE_URL` (`mysql://user:password@host:port/evacs`) is required when `ROUTE_PERSISTENCE_ENABLED=true` (the default). Install the schema explicitly before starting; startup never runs DDL. Apply the migration **once**, after the Control API Server's Flyway has initialized the database (Flyway refuses a non-empty schema without its history table), with an account allowed to create triggers (root locally; binary logging is on in MySQL 8.4):

```sh
mysql -h 127.0.0.1 -P 33306 -uroot -proot evacs < migrations/001_dispatch_route.sql
```

From the workspace root, `./scripts/migrate-local.sh` waits for the API Server and applies both worker migrations, skipping one whose table exists. The migration intentionally is not a repeatable startup operation. Preserve route outbox rows after delivery: they retain the accepted destination used by stabilization.

## Compose sandbox

Run these commands from the workspace root containing `docker-compose.local.yml`. First use `./scripts/setup-local.sh` to prepare the existing SUMO stack and adapter packages. Then run `./scripts/migrate-local.sh`; the root MySQL instructions are in `README.local.md`.

```sh
# For the implementation worktree; omit this for a normal checkout at Emergency-Worker.
export EMERGENCY_WORKER_DIR=./Emergency-Worker/.worktrees/implementation
docker compose -f docker-compose.local.yml up -d --build --wait emergency-worker
curl -fsS http://127.0.0.1:18082/readyz
docker compose -f docker-compose.local.yml config --format json | \
  node "${EMERGENCY_WORKER_DIR:-Emergency-Worker}/deploy/verify-compose.cjs"

# Start the simulation and transport services if they are not already running.
docker compose -f docker-compose.local.yml up -d --build --wait \
  gateway sumo-sim sumo-adapter client-adapter
node "${EMERGENCY_WORKER_DIR:-Emergency-Worker}/deploy/verify-local.cjs"
docker compose -f docker-compose.local.yml logs --tail=20 emergency-worker
```

The smoke script needs the worker's local `npm ci` and the client adapter's existing `socket.io-client` dependency installed by setup. It checks live `EVACS_*` state from SUMO, registers a unique deterministic ambulance/target pair through the real Gateway, verifies Location Worker state, creates the emergency through the Control API Server (`CONTROL_API_URL`, default `http://127.0.0.1:18090`) so activation comes from its Outbox, checks the dashboard cone, reads the canonical `YIELD_ALERT_REQUESTED` for the target from `stream:alert:event`, moves the target behind the ambulance, and checks one canonical clear after the repeated update is acknowledged. It ends the emergency through the API, checks that the dispatch and cone are removed, and closes sockets, leaving uniquely named records for inspection and ordinary vehicle expiry. It does not flush shared Redis or alter other services. This is a synthetic geometry proof alongside live SUMO traffic; it does not assert that a randomly selected SUMO vehicle receives a yield action.

The container runs as the `node` user, includes the Lua scripts beside compiled stores, and installs production dependencies only in the final image. It requires healthy Redis, Location Worker, and MySQL. Health uses `18082`, leaving existing service ports unchanged. SIGINT/SIGTERM stop readers, drain current handlers/scheduler/outboxes up to `SHUTDOWN_DRAIN_TIMEOUT_MS`, and close owned connections. An unfinished handler returns `drained: false` and keeps its input pending.

## Configuration and contracts

All configuration names and representative values are in [deploy/.env.example](deploy/.env.example). Redis commands, blocking readers, publishing, and density reads use separate connections. Set distinct consumer/instance identities for concurrent worker instances.

| Setting | Behavior |
| --- | --- |
| `REDIS_URL` | Host sandbox default `redis://127.0.0.1:6401/0`; Compose uses `redis:6379` |
| `DATABASE_URL` | Host sandbox `mysql://evacs:evacs@127.0.0.1:33306/evacs`; required for route persistence |
| `EVACS_CONE_KEY_PATTERN` | Default `evacs:emergency:%s:cone`; must match the Control API Server's setting |
| `HEALTH_PORT` | Process default `8082`; supplied local config/Compose use `18082` |
| `H3_RESOLUTION` | Default `9`; must match Location Worker, density metadata, and graph |
| `GRAPH_PATH` | Optional validated RoadGraph JSON; unset means `GRAPH_UNAVAILABLE`, no new routes. Root compose default `/graphs/road-graph.json` |
| `ROUTE_PERSISTENCE_ENABLED` | Default `true`; disabling runs alert Fast-Path without MySQL/routing |
| `ROUTE_INTERVAL_MS` | Default `2000`, accepted range `1000`–`3000` |
| `INACTIVE_TTL_MS` / `ALERT_TTL_MS` | Default `5000` / `20000`; eligibility freshness and alert expiry |
| `LOG_LEVEL` | `info` (default) or `debug`. `debug` logs one `Route evaluated` line per active dispatch per route pass: `outcome` (`COMMITTED`, `KEPT`, `NO_ROUTE`, `VEHICLE_STALE`, `SUPERSEDED`), stabilizer `verdict` (`INITIAL`, `OFF_ROUTE`, `FASTER`, `HOLD`, `NOT_FASTER`, `DESTINATION_CHANGED`), candidate/remaining seconds |
| `BPR_ALPHA` / `BPR_BETA` | Default `0.15` / `4` for density weights |
| `ROAD_CAPACITY` / `BLOCKED_MULTIPLIER` | Default `180` vehicles per lane / `10` times free-flow travel time |

The graph must match the simulation's directed roads and H3 cells; the unit-test graph is not a production SUMO network. A missing graph is explicit degradation, whereas a supplied invalid graph (or a configured path with no file) fails startup.

### Road graph from a SUMO network

`src/tools/build-road-graph.ts` converts a netconvert network into the RoadGraph JSON and validates it with `RoadGraph.load`:

```sh
npm run build
npm run graph:build -- ../sumo_scenario/scenario-inhadae/roads.net.xml ../sumo_scenario/scenario-inhadae/road-graph.json \
  --edge-cell-table=../sumo_scenario/scenario-inhadae/edge-cell-table.json
```

`--edge-cell-table` also writes the Location Worker's `EDGE_CELL_TABLE_PATH` table (schema v1, `source: "sumo-net"`) from the same graph, so `density:edges` uses the edge IDs the route planner looks up. Each edge's geometry is walked in 1 m steps credited to the H3 cell of each step's midpoint, then scaled so the cell lengths sum to the SUMO lane length. Edges the Location Worker would reject (`lengthM <= 0`, IDs starting with `_`) are omitted rather than invalidating the table. The local compose stack builds both files into the `road-graph` volume on every `up`.

It prints the node/edge counts, skipped edges, and the share of junctions in the largest strongly connected component (routes outside it end `DISCONNECTED`). Conversion rules: junctions become nodes; normal edges with at least one lane open to the SUMO `emergency` vehicle class become edges (internal, crossing and footpath-only edges are dropped); edge IDs stay SUMO edge IDs; `lanes`/`speedLimitMps`/`lengthM` come from the usable lanes; geometry is the middle usable lane's centreline (where SUMO reports vehicles); cells are the resolution-9 H3 cells sampled every 10 m along it. Coordinates are inverted from the network's `+proj=utm` WGS84 projection and `netOffset`; other projections are rejected. Turn restrictions (`<connection>`) are not modelled, so a route may contain a turn SUMO forbids. On the Inha scenario: 45 junctions, 95 edges, 95.6% strongly connected; live vehicles lie within 3.2 m of their reported edge.

In the root compose a one-shot `road-graph` service (this image, run as root to write the fresh named volume) converts the scenario's `roads.net.xml` into the `road-graph` volume on every `up`; `emergency-worker` depends on it with `service_completed_successfully`, mounts the volume read-only at `/graphs`, and defaults `GRAPH_PATH` to `/graphs/road-graph.json`. A failed conversion leaves the worker unstarted with the reason in `docker compose logs road-graph`. `EMERGENCY_GRAPH_PATH=` (empty) disables routing.

Density reads atomically consume `density:cells`, `density:edges`, and `density:meta`. Required generations must match. Metadata must specify resolution and either `enabled` or `disabled:<reason>` for edge density. Location Worker's `disabled:table-not-configured` is reported as degraded and uses free-flow weights. Missing/invalid metadata, generation mismatch, or H3 mismatch makes readiness fail until a valid snapshot is available; the scheduler skips mixed generations and blocks incompatible H3 configuration.

| Direction | Default stream / group | Format |
| --- | --- | --- |
| State input | `stream:vehicle:state` / `emergency-worker:vehicle-state` | `event` JSON envelope, `VEHICLE_STATE_UPDATED` |
| Emergency input | `evacs:stream:emergency-event` / `emergency-worker:emergency` | Control API Server Outbox: envelope split into `eventId`, `eventType`, `schemaVersion`, `occurredAt`, `producer`, JSON `payload` fields (an `event` JSON envelope is also accepted) |
| Dashboard cone | `evacs:emergency:{emergencyId}:cone` | `ConeSnapshot` JSON (`emergencyId`, `vehicleId`, `version`, `resolution`, `cells`, `updatedAt`), 10-minute TTL, deleted on completion |
| Canonical output | `stream:alert:event` | `eventKey` plus `event` JSON for yield issue, clear, or route request |
| Gateway route output | `stream:evacs_route:update` (`ROUTE_UPDATE_STREAM`) | Same `eventKey` plus `event` JSON, `ROUTE_UPDATE_REQUESTED` only, for the WebSocket Gateway |
| Alert delivery intent | `stream:emergency-worker:alert-outbox` | Durable Redis decision outbox |
| Dead letter | `stream:emergency-worker:dlq` | Invalid/expired input or exhausted ordinary retries |

`/livez` returns 200 while the HTTP process is running. `/readyz` returns 200 with `{ready, reasons, degraded}` only while both groups/readers, required Redis connections, MySQL tables, density configuration, and outbox delivery are available. H3-incompatible input remains pending, bypasses the ordinary retry cap, and returns readiness 503 without entering DLQ.

Both outboxes provide **at-least-once** delivery. A crash after Redis accepts a message but before its delivery receipt is saved may repeat it with the same immutable `eventKey`/`eventId`; downstream consumers must deduplicate by that key. The MySQL route transaction includes affected-target `ROUTE_CHANGED` clear requests selected against the new graph route. These requests do not atomically remove Redis Fast-Path decisions. The latest persisted route is loaded when the next emergency state event refreshes the Fast-Path cone; before that refresh, normal events can still use the previous cone. Redis lifecycle changes and MySQL route commits are not one cross-database transaction.

### Emergency lifecycle (Control API Server contract)

The Control API Server has no separate dispatch: an emergency names its vehicle. So `EMERGENCY_CREATED` with a `vehicleId` starts a dispatch whose `dispatchId` **is the emergency ID** (alert, route and history records carry it). Its origin is `origin`, else the first `route` point, else the destination; its destination is `destination`, else the last `route` point, else `location`. A creation with a vehicle but none of these is a contract error (DLQ). A creation without `vehicleId` (design p.100 registration) starts nothing. `EMERGENCY_ENDED` completes every active dispatch of the emergency, queues `EMERGENCY_COMPLETED` clears, and removes the dashboard cone; an end processed first keeps a late creation from activating. The design's `EMERGENCY_DISPATCHED` / `EMERGENCY_STATUS_CHANGED` remain supported on the same stream. Java `Instant` timestamps (up to nanoseconds) are truncated to milliseconds.

The worker publishes canonical requests to `stream:alert:event` and copies each `ROUTE_UPDATE_REQUESTED` to `stream:evacs_route:update` for the WebSocket Gateway. A route outbox row is marked delivered only after both streams accept it, so a partial failure republishes to both with the same `eventKey`; client delivery is the Alert Worker's job via `ws:vehicle:{id}` and `channel:gateway:{gatewayId}`. Until the Gateway implements that contract, vehicles receive no socket alerts from this worker. The existing Gateway/client behavior, downstream deduplication/ledger, REST dispatch creation, and pedestrian access routing require separate owner work. Logs record successful canonical publication with its stable event key; they do not report client receipt.

WebSocket Gateway 담당자는 [`docs/gateway-route-stream-consumer-guide.md`](docs/gateway-route-stream-consumer-guide.md)를 따릅니다. 기존 Pub/Sub 경로는 레거시 코드로 유지하며, 이 전달 계약의 범위 밖입니다.

## Verification

```sh
REDIS_URL=redis://127.0.0.1:6401 \
TEST_MYSQL_ADMIN_URL=mysql://root:root@127.0.0.1:33306 \
npm test -- --runInBand
npm run typecheck
npm run build
```

Integration tests isolate Redis prefixes and create a throwaway MySQL database per suite (hence the admin URL). The composed-worker suite covers restart after committed alert intent with delivery stopped, one clear under repeated target movement, bounded drain with pending input, concrete route-change target selection, durable route publication, H3 readiness/pending behavior, Redis disconnect, and MySQL schema failure. Compose validation and the live smoke script supplement the automated suite.

## Workspace integration

The root `docker-compose.local.yml`, `README.local.md` and `scripts/` are outside this child repository. The root compose now runs this worker against the shared MySQL (`DATABASE_URL: mysql://evacs:evacs@mysql:3306/evacs`, depending on healthy `redis`, `location-worker` and `mysql`); `deploy/verify-compose.cjs` asserts that wiring. The earlier PostgreSQL integration patch recorded here is superseded.
