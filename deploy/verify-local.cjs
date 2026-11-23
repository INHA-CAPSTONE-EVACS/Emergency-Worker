/* Run from the stack root after setup-local.sh and scripts/migrate-local.sh (MySQL worker tables). */
const assert = require('node:assert/strict');
const {randomUUID} = require('node:crypto');
const {createRequire} = require('node:module');
const {resolve} = require('node:path');
const Redis = require('ioredis');
// Reuse the sandbox's Socket.IO client; it is not a worker runtime dependency.
const {io} = createRequire(resolve(process.cwd(), 'SUMO_adapter/client-adapter/package.json'))('socket.io-client');
const compareIds = (a, b) => {
  const [am, as] = a.split('-').map(BigInt), [bm, bs] = b.split('-').map(BigInt);
  return am === bm ? as > bs ? 1 : as < bs ? -1 : 0 : am > bm ? 1 : -1;
};

async function eventually(check, description) {
  const deadline = Date.now() + 15000;
  while (Date.now() < deadline) {
    const result = await check();
    if (result) return result;
    await new Promise(resolve => setTimeout(resolve, 50));
  }
  throw new Error(`Timed out: ${description}`);
}
async function main() {
  const redis = new Redis(process.env.REDIS_URL ?? 'redis://127.0.0.1:6401/0', {maxRetriesPerRequest: 1});
  const runId = randomUUID();
  const emergencyVehicleId = `sandbox-ambulance-${runId}`, targetVehicleId = `sandbox-target-${runId}`;
  const sockets = [];
  // The Control API Server owns emergency creation/termination; its Outbox publishes to the worker.
  const api = process.env.CONTROL_API_URL ?? 'http://127.0.0.1:18090';
  const control = async (path, body) => {
    const response = await fetch(`${api}/control/emergencies${path}`, {method: body === undefined ? 'GET' : 'POST',
      headers: {'content-type': 'application/json'}, ...(body === undefined || body === null ? {} : {body: JSON.stringify(body)})});
    assert.ok(response.ok, `${path || 'create'} returned ${response.status}`);
    return await response.json();
  };
  let dispatchId;
  try {
    const readiness = await fetch(process.env.WORKER_HEALTH_URL ?? 'http://127.0.0.1:18082/readyz');
    assert.equal(readiness.status, 200);
    const initial = (await redis.xrevrange('stream:vehicle:state', '+', '-', 'COUNT', 1))[0]?.[0];
    await eventually(async () => {
      const rows = await redis.xrevrange('stream:vehicle:state', '+', '-', 'COUNT', 100);
      return rows.some(([id, fields]) => (!initial || compareIds(id, initial) > 0) &&
        JSON.parse(fields[fields.indexOf('event') + 1]).payload.vehicleId.startsWith('EVACS_'));
    }, 'live SUMO state through adapters, Gateway and Location Worker');
    const connect = async (vehicleId, vehicleType) => {
      const socket = io(process.env.GATEWAY_URL ?? 'http://127.0.0.1:8080', {
        query: {vehicleId, vehicleType}, transports: ['websocket'], forceNew: true, reconnection: false});
      sockets.push(socket);
      await new Promise((resolve, reject) => {
        const timer = setTimeout(() => reject(new Error('Gateway handshake timeout')), 5000);
        socket.once('connection.ready', () => {clearTimeout(timer); resolve();});
        socket.once('connect_error', error => {clearTimeout(timer); reject(error);});
      });
      return socket;
    };
    const ambulance = await connect(emergencyVehicleId, 'EMERGENCY');
    const target = await connect(targetVehicleId, 'NORMAL');
    const alerts = [];
    target.on('yield.alert', message => alerts.push(message));
    const location = async (socket, vehicleId, latitude, speedMps, sequence) => {
      const ack = await socket.timeout(5000).emitWithAck('location.update', {vehicleId, latitude,
        longitude: 126.65, speedMps, headingDegrees: 0, sequence, measuredAt: new Date().toISOString(),
        laneIndex: 0, laneCount: 1});
      assert.equal(ack.status, 'ACCEPTED');
      await eventually(async () => await redis.hget(`vehicle:${vehicleId}:state`, 'lastSequence') === String(sequence),
        'Location Worker state commit');
    };
    await location(target, targetVehicleId, 37.4518, 5, 0);
    await location(ambulance, emergencyVehicleId, 37.45, 20, 0);
    // Under the API contract the dispatch is identified by the emergency ID.
    dispatchId = (await control('', {vehicleId: emergencyVehicleId, emergencyType: 'MEDICAL',
      originLat: 37.45, originLng: 126.65, destinationLat: 37.46, destinationLng: 126.65})).id;
    await eventually(async () => await redis.exists(`dispatch:${dispatchId}:state`), 'dispatch activation from the API Outbox');
    await location(ambulance, emergencyVehicleId, 37.45, 20, 1);
    const cone = await eventually(async () => {
      const value = await control(`/${dispatchId}/cone`);
      return value.available && value;
    }, 'cone projected for GET /control/emergencies/{id}/cone');
    assert.equal(cone.vehicleId, emergencyVehicleId);
    await eventually(async () => alerts.length, 'Gateway yield.alert delivery');
    assert.equal(alerts[0].type, 'YIELD_ALERT_ISSUED');
    assert.equal(alerts[0].details.payload.targetVehicleId, targetVehicleId);
    const canonical = async () => (await redis.xrevrange('stream:alert:event', '+', '-', 'COUNT', 1000))
      .map(([, fields]) => JSON.parse(fields[fields.indexOf('event') + 1]))
      .filter(value => value.payload.dispatchId === dispatchId);
    await location(target, targetVehicleId, 37.449, 5, 1);
    await eventually(async () => (await canonical()).some(value => value.eventType === 'YIELD_ALERT_CLEAR_REQUESTED'),
      'canonical clear request');
    await location(target, targetVehicleId, 37.449, 5, 2);
    const stateRows = await redis.xrevrange('stream:vehicle:state', '+', '-', 'COUNT', 1000);
    const notificationId = stateRows.find(([, fields]) => {
      const value = JSON.parse(fields[fields.indexOf('event') + 1]);
      return value.payload.vehicleId === targetVehicleId && value.payload.stateVersion === 3;
    })?.[0];
    assert.ok(notificationId, 'Location Worker emitted the repeated target notification');
    await eventually(async () => {
      const group = (await redis.xinfo('GROUPS', 'stream:vehicle:state'))
        .find(fields => fields[fields.indexOf('name') + 1] === 'emergency-worker:vehicle-state');
      return group && compareIds(group[group.indexOf('last-delivered-id') + 1], notificationId) >= 0
        && (await redis.xpending('stream:vehicle:state', 'emergency-worker:vehicle-state'))[0] === 0;
    }, 'Emergency Worker acknowledged the repeated target notification');
    const requests = await canonical();
    const clears = requests.filter(value => value.eventType === 'YIELD_ALERT_CLEAR_REQUESTED');
    assert.equal(clears.length, 1);
    assert.equal(clears[0].payload.reason, 'EMERGENCY_PASSED');
    await control(`/${dispatchId}/end`, null);
    await eventually(async () => !await redis.exists(`dispatch:${dispatchId}:state`), 'completion from EMERGENCY_ENDED');
    assert.equal((await control(`/${dispatchId}/cone`)).available, false);
    console.log(JSON.stringify({result: 'PASS', emergencyId: dispatchId, emergencyVehicleId, targetVehicleId,
      gatewayMessage: alerts[0].type, clearEventKey: clears[0].eventId, coneCells: cone.cells.length,
      scope: 'Control API create/end → Outbox → Emergency Worker; live SUMO plus deterministic Gateway → Location Worker → Emergency Worker → Gateway alert, canonical clear and dashboard cone'}));
  } finally {
    try {
      if (dispatchId) await fetch(`${api}/control/emergencies/${dispatchId}/end`, {method: 'POST'}).catch(() => undefined);
    } finally {
      for (const socket of sockets) socket.disconnect();
      await redis.quit();
    }
  }
}
void main().catch(error => {console.error(error); process.exitCode = 1;});
