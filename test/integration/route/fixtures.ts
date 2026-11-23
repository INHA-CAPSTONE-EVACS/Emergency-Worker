import {randomUUID} from 'node:crypto';
import {readFile} from 'node:fs/promises';
import {createConnection, createPool} from 'mysql2/promise';
import {createMysql} from '../../../src/infra/mysql';
import type {DispatchState, RouteResult} from '../../../src/domain/types';

/** Server URL without a database. Tests create a throwaway database, so they need an account allowed to create one. */
export const mysqlAdminUrl = process.env.TEST_MYSQL_ADMIN_URL ?? 'mysql://root:root@127.0.0.1:33306';
export const dispatch: DispatchState = {dispatchId: 'b34865e1-95ac-45f9-bac4-0316f4078306', emergencyId: 'emergency',
  emergencyVehicleId: 'ambulance', status: 'DISPATCHED', origin: {latitude: 0, longitude: 0},
  destination: {latitude: 0.001, longitude: 0.001}, decisionVersion: 0, routeVersion: 0,
  updatedAt: '2026-11-16T12:00:00.000Z'};
export const route: Extract<RouteResult, {outcome: 'ROUTE_FOUND'}> = {outcome: 'ROUTE_FOUND',
  roadSegments: ['ab', 'bd'], waypoints: [{latitude: 0, longitude: 0}, {latitude: 0.001, longitude: 0},
    {latitude: 0.001, longitude: 0.001}], congestedSegments: [], estimatedTravelSeconds: 100, distanceM: 200};
export const input = () => ({dispatch, route, expectedVersion: 0, changeReason: 'INITIAL_ROUTE',
  createdAt: '2026-11-16T12:00:00.000Z', clears: []});
export function mysqlFixture() {
  const database = `route_test_${randomUUID().replaceAll('-', '')}`;
  const admin = createPool({uri: mysqlAdminUrl, connectionLimit: 1});
  const pool = createMysql({databaseUrl: `${mysqlAdminUrl}/${database}`});
  beforeAll(async () => {
    await admin.query(`CREATE DATABASE ${database}`);
    const migrator = await createConnection({uri: `${mysqlAdminUrl}/${database}`, multipleStatements: true});
    try {await migrator.query(await readFile('migrations/001_dispatch_route.sql', 'utf8'));}
    finally {await migrator.end();}
  });
  beforeEach(async () => {
    // TRUNCATE bypasses the append-only triggers but not the outbox foreign key.
    const connection = await pool.getConnection();
    try {
      await connection.query('SET FOREIGN_KEY_CHECKS = 0');
      await connection.query('TRUNCATE emergency_route_outbox');
      await connection.query('TRUNCATE dispatch_route');
      await connection.query('SET FOREIGN_KEY_CHECKS = 1');
    } finally {connection.release();}
  });
  afterAll(async () => {
    await pool.end();
    await admin.query(`DROP DATABASE ${database}`);
    await admin.end();
  });
  return pool;
}
