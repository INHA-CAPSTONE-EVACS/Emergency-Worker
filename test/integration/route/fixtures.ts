import {randomUUID} from 'node:crypto';
import {readFile} from 'node:fs/promises';
import {Pool} from 'pg';
import {createPostgres} from '../../../src/infra/postgres';
import type {DispatchState, RouteResult} from '../../../src/domain/types';

export const databaseUrl = process.env.DATABASE_URL ?? 'postgres://evacs:evacs@127.0.0.1:5433/evacs';
export const dispatch: DispatchState = {dispatchId: 'b34865e1-95ac-45f9-bac4-0316f4078306', emergencyId: 'emergency',
  emergencyVehicleId: 'ambulance', status: 'DISPATCHED', origin: {latitude: 0, longitude: 0},
  destination: {latitude: 0.001, longitude: 0.001}, decisionVersion: 0, routeVersion: 0,
  updatedAt: '2026-11-16T12:00:00.000Z'};
export const route: Extract<RouteResult, {outcome: 'ROUTE_FOUND'}> = {outcome: 'ROUTE_FOUND',
  roadSegments: ['ab', 'bd'], waypoints: [{latitude: 0, longitude: 0}, {latitude: 0.001, longitude: 0},
    {latitude: 0.001, longitude: 0.001}], congestedSegments: [], estimatedTravelSeconds: 100, distanceM: 200};
export const input = () => ({dispatch, route, expectedVersion: 0, changeReason: 'INITIAL_ROUTE',
  createdAt: '2026-11-16T12:00:00.000Z', clears: []});
export function postgresFixture() {
  const schema = `route_test_${randomUUID().replaceAll('-', '')}`;
  const admin = new Pool({connectionString: databaseUrl});
  const pool = createPostgres({databaseUrl}, {options: `-c search_path=${schema}`});
  beforeAll(async () => {
    await admin.query(`CREATE SCHEMA ${schema}`);
    await pool.query(await readFile('migrations/001_dispatch_route.sql', 'utf8'));
  });
  beforeEach(async () => {await pool.query('TRUNCATE emergency_route_outbox, dispatch_route');});
  afterAll(async () => {
    await pool.end();
    await admin.query(`DROP SCHEMA ${schema} CASCADE`);
    await admin.end();
  });
  return pool;
}
