# Emergency Worker

Consumes Location Worker state and emergency lifecycle events, computes selective yield decisions, and publishes durable alert and route requests. Node.js 26+, Redis 7+, and PostgreSQL 16 are used by the local stack.

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

`DATABASE_URL` is required when `ROUTE_PERSISTENCE_ENABLED=true` (the default). Install the schema explicitly before starting; startup never runs DDL. Apply the migration **once** to a new database:

```sh
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f migrations/001_dispatch_route.sql
```

The root stack also documents applying it with `docker compose exec -T postgres psql`. The migration intentionally is not a repeatable startup operation. Preserve route outbox rows after delivery: they retain the accepted destination used by stabilization.

## Compose sandbox

Run these commands from the workspace root containing `docker-compose.local.yml`. First use `./scripts/setup-local.sh` to prepare the existing SUMO stack and adapter packages. The root PostgreSQL instructions are in `README.local.md`.

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

The smoke script needs the worker's local `npm ci` and the client adapter's existing `socket.io-client` dependency installed by setup. It checks live `EVACS_*` state from SUMO, registers a unique deterministic ambulance/target pair through the real Gateway, verifies Location Worker state, receives `YIELD_ALERT_ISSUED` on the target socket, moves the target behind the ambulance, and checks one canonical clear after the repeated update is acknowledged. It completes its dispatch and closes sockets, leaving uniquely named records for inspection and ordinary vehicle expiry. It does not flush shared Redis or alter other services. This is a synthetic geometry proof alongside live SUMO traffic; it does not assert that a randomly selected SUMO vehicle receives a yield action.

The container runs as the `node` user, includes the Lua scripts beside compiled stores, and installs production dependencies only in the final image. It requires healthy Redis, Location Worker, and PostgreSQL. Health uses `18082`, leaving existing service ports unchanged. SIGINT/SIGTERM stop readers, drain current handlers/scheduler/outboxes up to `SHUTDOWN_DRAIN_TIMEOUT_MS`, and close owned connections. An unfinished handler returns `drained: false` and keeps its input pending.

## Configuration and contracts

All configuration names and representative values are in [deploy/.env.example](deploy/.env.example). Redis commands, blocking readers, publishing, and density reads use separate connections. Set distinct consumer/instance identities for concurrent worker instances.

| Setting | Behavior |
| --- | --- |
| `REDIS_URL` | Host sandbox default `redis://127.0.0.1:6401/0`; Compose uses `redis:6379` |
| `DATABASE_URL` | Host sandbox `postgres://evacs:evacs@127.0.0.1:5433/evacs`; required for route persistence |
| `HEALTH_PORT` | Process default `8082`; supplied local config/Compose use `18082` |
| `H3_RESOLUTION` | Default `9`; must match Location Worker, density metadata, and graph |
| `GRAPH_PATH` | Optional validated RoadGraph JSON; no file means `GRAPH_UNAVAILABLE`, no new routes |
| `ROUTE_PERSISTENCE_ENABLED` | Default `true`; disabling runs alert Fast-Path without PostgreSQL/routing |
| `ROUTE_INTERVAL_MS` | Default `2000`, accepted range `1000`–`3000` |
| `INACTIVE_TTL_MS` / `ALERT_TTL_MS` | Default `5000` / `20000`; eligibility freshness and alert expiry |
| `BPR_ALPHA` / `BPR_BETA` | Default `0.15` / `4` for density weights |
| `ROAD_CAPACITY` / `BLOCKED_MULTIPLIER` | Default `180` vehicles per lane / `10` times free-flow travel time |

For a graph mounted in Compose, set `EMERGENCY_GRAPH_DIR` to the host directory and `EMERGENCY_GRAPH_PATH=/graphs/<file>.json`. The graph must match the simulation's directed roads and H3 cells; the unit-test graph is not a production SUMO network. A missing graph is explicit degradation, whereas a supplied invalid graph fails startup.

Density reads atomically consume `density:cells`, `density:edges`, and `density:meta`. Required generations must match. Metadata must specify resolution and either `enabled` or `disabled:<reason>` for edge density. Location Worker's `disabled:table-not-configured` is reported as degraded and uses free-flow weights. Missing/invalid metadata, generation mismatch, or H3 mismatch makes readiness fail until a valid snapshot is available; the scheduler skips mixed generations and blocks incompatible H3 configuration.

| Direction | Default stream / group | Format |
| --- | --- | --- |
| State input | `stream:vehicle:state` / `emergency-worker:vehicle-state` | `event` JSON envelope, `VEHICLE_STATE_UPDATED` |
| Emergency input | `stream:emergency:event` / `emergency-worker:emergency` | `event` JSON lifecycle envelope |
| Canonical output | `stream:alert:event` | `eventKey` plus `event` JSON for yield issue, clear, or route request |
| Current Gateway output | `evacs:stream:yield-alert` | `eventKey` plus `payload` JSON with `vehicleId`, `alertVersion`, expiry, details |
| Alert delivery intent | `stream:emergency-worker:alert-outbox` | Durable Redis decision outbox |
| Dead letter | `stream:emergency-worker:dlq` | Invalid/expired input or exhausted ordinary retries |

`/livez` returns 200 while the HTTP process is running. `/readyz` returns 200 with `{ready, reasons, degraded}` only while both groups/readers, required Redis connections, PostgreSQL tables, density configuration, and outbox delivery are available. H3-incompatible input remains pending, bypasses the ordinary retry cap, and returns readiness 503 without entering DLQ.

Both outboxes provide **at-least-once** delivery. A crash after Redis accepts a message but before its delivery receipt is saved may repeat it with the same immutable `eventKey`/`eventId`; downstream consumers must deduplicate by that key. The PostgreSQL route transaction includes affected-target `ROUTE_CHANGED` clear requests selected against the new graph route. These requests do not atomically remove Redis Fast-Path decisions. The latest persisted route is loaded when the next emergency state event refreshes the Fast-Path cone; before that refresh, normal events can still use the previous cone. Redis lifecycle changes and PostgreSQL route commits are not one cross-database transaction.

The current Gateway consumes yield **issue** payloads only. Canonical `YIELD_ALERT_CLEAR_REQUESTED` and `ROUTE_UPDATE_REQUESTED` publication does **not** establish client clear/route delivery. The existing Gateway/client behavior, downstream deduplication/ledger, REST dispatch creation, and pedestrian access routing require separate owner work. Logs record successful canonical publication with its stable event key; they do not report client receipt.

## Verification

```sh
REDIS_URL=redis://127.0.0.1:6401 \
DATABASE_URL=postgres://evacs:evacs@127.0.0.1:5433/evacs \
npm test -- --runInBand
npm run typecheck
npm run build
```

Integration tests isolate Redis prefixes and PostgreSQL schemas. The composed-worker suite covers restart after committed alert intent with delivery stopped, Gateway projection, one clear under repeated target movement, bounded drain with pending input, concrete route-change target selection, durable route publication, H3 readiness/pending behavior, Redis disconnect, and PostgreSQL schema failure. Compose validation and the live smoke script supplement the automated suite.

## External workspace integration patch

The root `docker-compose.local.yml` and `README.local.md` are outside this child repository. The following exact Task 8 diff is recorded here for review; **it is not committed by this repository**. It is relative to the root after Task 7 and preserves that task's PostgreSQL service, named volume, and migration instructions.

```diff
--- a/docker-compose.local.yml
+++ b/docker-compose.local.yml
@@ -78,0 +79,33 @@
+  emergency-worker:
+    build:
+      context: ${EMERGENCY_WORKER_DIR:-./Emergency-Worker}
+      dockerfile: Dockerfile.local
+    environment:
+      REDIS_URL: redis://redis:6379/0
+      DATABASE_URL: postgres://evacs:evacs@postgres:5432/evacs
+      HEALTH_PORT: '18082'
+      H3_RESOLUTION: '9'
+      CONSUMER_NAME: emergency-worker-local
+      INSTANCE_ID: emergency-worker-local
+      GRAPH_PATH: ${EMERGENCY_GRAPH_PATH:-}
+      ROUTE_PERSISTENCE_ENABLED: 'true'
+      ROUTE_INTERVAL_MS: '2000'
+      BLOCK_MS: '1000'
+    volumes:
+      - ${EMERGENCY_GRAPH_DIR:-./Emergency-Worker/deploy}:/graphs:ro
+    ports:
+      - '18082:18082'
+    depends_on:
+      redis:
+        condition: service_healthy
+      location-worker:
+        condition: service_healthy
+      postgres:
+        condition: service_healthy
+    healthcheck:
+      test: ['CMD', 'node', '-e', "fetch('http://127.0.0.1:18082/readyz').then(r => process.exit(r.ok ? 0 : 1)).catch(() => process.exit(1))"]
+      interval: 5s
+      timeout: 3s
+      retries: 12
+    stop_grace_period: 15s
+
--- a/README.local.md
+++ b/README.local.md
@@ -76,0 +77,27 @@
+
+## Emergency Worker 실행 및 상태
+
+위 PostgreSQL 마이그레이션 후 실행한다. 구현 worktree라면 빌드 경로를 지정한다.
+
+```sh
+export EMERGENCY_WORKER_DIR=./Emergency-Worker/.worktrees/implementation
+docker compose -f docker-compose.local.yml up -d --build --wait emergency-worker
+curl -fsS http://127.0.0.1:18082/readyz
+```
+
+Emergency Worker는 건강한 Redis, Location Worker, PostgreSQL에 의존하며 `18082`를
+사용한다. `/livez`는 프로세스 상태, `/readyz`는 소비자/H3 설정, Redis 연결,
+PostgreSQL 스키마 및 density snapshot을 점검한다. H3 불일치는 pending 상태를
+유지하고 readiness 503을 반환한다.
+
+경로 그래프는 `EMERGENCY_GRAPH_DIR`를 호스트 그래프 폴더로,
+`EMERGENCY_GRAPH_PATH=/graphs/<파일명>.json`으로 지정한다. 그래프가 없으면
+`GRAPH_UNAVAILABLE` degraded 상태이며 새 경로를 만들지 않는다. Location Worker가
+`disabled:<reason>` edge-density를 명시하면 free-flow 경로를 계산한다. 이 둘은
+실패 상태와 구분된다. 누락/잘못된 density metadata 또는 H3 불일치는 readiness 실패다.
+
+`stream:vehicle:state`, `stream:emergency:event`를 소비하고 canonical 요청은
+`stream:alert:event`, 현재 Gateway 호환 payload는 `evacs:stream:yield-alert`에 쓴다.
+Gateway는 현재 clear 요청을 소비하지 않으므로 canonical `YIELD_ALERT_CLEAR_REQUESTED`가
+발행되어도 클라이언트 clear 전달이 완료된 것은 아니다. 전체 재현 절차와
+환경 변수는 Emergency Worker `README.md`, `deploy/.env.example`에 있다.
```
