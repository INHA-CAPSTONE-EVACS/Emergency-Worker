// From the stack root: docker compose -f docker-compose.local.yml config --format json | node <worker>/deploy/verify-compose.cjs
const assert = require('node:assert/strict');
const {readFileSync} = require('node:fs');
const config = JSON.parse(readFileSync(0, 'utf8'));
const worker = config.services['emergency-worker'];
assert.equal(worker.build.dockerfile, 'Dockerfile.local');
for (const dependency of ['redis', 'location-worker', 'mysql']) {
  assert.equal(worker.depends_on[dependency].condition, 'service_healthy', dependency);
}
assert.equal(worker.environment.DATABASE_URL, 'mysql://evacs:evacs@mysql:3306/evacs');
assert.equal(worker.environment.REDIS_URL, 'redis://redis:6379/0');
assert.equal(worker.environment.HEALTH_PORT, '18082');
// The road graph is converted from the scenario network before the worker starts.
const graph = config.services['road-graph'];
assert.equal(worker.depends_on['road-graph'].condition, 'service_completed_successfully');
assert.deepEqual(graph.command.slice(1), ['dist/src/tools/build-road-graph.js', '/scenario/roads.net.xml', '/graphs/road-graph.json']);
assert.equal(worker.environment.GRAPH_PATH, '/graphs/road-graph.json');
assert.ok(worker.volumes.some(volume => volume.source === 'road-graph' && volume.target === '/graphs' && volume.read_only));
for (const [service, port] of Object.entries({gateway: '8080', 'location-worker': '18081',
  redis: '6401', mysql: '33306', 'api-server': '18090', 'emergency-worker': '18082'})) {
  assert.equal(config.services[service].ports[0].published, port, service);
}
assert.ok(config.volumes['mysql-data']);
assert.equal(config.services.postgres, undefined, 'PostgreSQL was replaced by the shared MySQL');
assert.ok(worker.healthcheck.test.join(' ').includes('/readyz'));
console.log('PASS: Compose health dependencies, MySQL/Redis URLs, existing ports, worker health port, MySQL volume and road graph build');
