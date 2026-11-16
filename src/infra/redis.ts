import Redis, {type RedisOptions} from 'ioredis';
import type {WorkerConfig} from '../config/config';

/** Create separate instances for commands, blocking readers, and publishing. */
export function createRedis(config: Pick<WorkerConfig, 'redisUrl'>, options: RedisOptions = {}): Redis {
  return new Redis(config.redisUrl, {maxRetriesPerRequest: 1, ...options});
}
