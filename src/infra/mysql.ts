import {createPool, type Pool, type PoolOptions} from 'mysql2/promise';
import type {WorkerConfig} from '../config/config';

/**
 * Schema is installed by migrations, never by connection creation or application startup.
 * Timestamps are stored as UTC DATETIME(3): the session time zone and driver conversion are both pinned to UTC.
 */
export function createMysql(config: Pick<WorkerConfig, 'databaseUrl'>, options: PoolOptions = {}): Pool {
  if (!config.databaseUrl) throw new Error('DATABASE_URL is required for route persistence');
  return createPool({uri: config.databaseUrl, connectionLimit: 5, connectTimeout: 5000, timezone: 'Z',
    supportBigNumbers: true, bigNumberStrings: true, ...options});
}
