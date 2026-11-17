import {Pool, type PoolConfig} from 'pg';
import type {WorkerConfig} from '../config/config';

/** Schema is installed by migrations, never by connection creation or application startup. */
export function createPostgres(config: Pick<WorkerConfig, 'databaseUrl'>, options: PoolConfig = {}): Pool {
  if (!config.databaseUrl) throw new Error('DATABASE_URL is required for route persistence');
  return new Pool({connectionString: config.databaseUrl, max: 5, connectionTimeoutMillis: 5000, ...options});
}
