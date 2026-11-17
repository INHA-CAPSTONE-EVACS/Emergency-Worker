// From the stack root: docker compose -f docker-compose.local.yml config --format json | node <worker>/deploy/verify-compose.cjs
const assert = require('node:assert/strict');
const {readFileSync} = require('node:fs');
const config = JSON.parse(readFileSync(0, 'utf8'));
const worker = config.services['emergency-worker'];
assert.equal(worker.build.dockerfile, 'Dockerfile.local');
for (const dependency of ['redis', 'location-worker', 'postgres']) {
  assert.equal(worker.depends_on[dependency].condition, 'service_healthy', dependency);
}
assert.equal(worker.environment.DATABASE_URL, 'postgres://evacs:evacs@postgres:5432/evacs');
assert.equal(worker.environment.REDIS_URL, 'redis://redis:6379/0');
assert.equal(worker.environment.HEALTH_PORT, '18082');
for (const [service, port] of Object.entries({gateway: '8080', 'location-worker': '18081',
  redis: '6401', postgres: '5433', 'emergency-worker': '18082'})) {
  assert.equal(config.services[service].ports[0].published, port, service);
}
assert.ok(config.volumes['postgres-data']);
assert.ok(worker.healthcheck.test.join(' ').includes('/readyz'));
console.log('PASS: Compose health dependencies, database/Redis URLs, existing ports, worker health port and PostgreSQL volume');
