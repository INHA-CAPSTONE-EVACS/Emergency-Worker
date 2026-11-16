import type Redis from 'ioredis';
import type {WorkerConfig} from '../config/config';
import {failureBudgetKey} from './retry-budget';

// The source ACK and durable DLQ append are one operation; failures leave the PEL intact.
const deadLetterScript = `
local kind = redis.call('TYPE', KEYS[2]).ok
if kind ~= 'none' and kind ~= 'stream' then error('Invalid DLQ key type') end
local pending = redis.call('XPENDING', KEYS[1], ARGV[1], ARGV[2], ARGV[2], 1)
if #pending == 0 then
  redis.call('HDEL', KEYS[3], ARGV[2])
  return 0
end
redis.call('XADD', KEYS[2], 'MAXLEN', '~', ARGV[3], '*',
  'sourceStream', ARGV[4], 'sourceId', ARGV[2], 'group', ARGV[1],
  'reason', ARGV[5], 'error', ARGV[6], 'fields', ARGV[7], 'failedAt', ARGV[8])
local acknowledged = redis.call('XACK', KEYS[1], ARGV[1], ARGV[2])
redis.call('HDEL', KEYS[3], ARGV[2])
return acknowledged`;
export async function deadLetter(redis: Redis, config: Pick<WorkerConfig, 'dlqStream' | 'dlqMaxlen'>,
  stream: string, group: string, id: string, fields: readonly string[], reason: string, error: unknown): Promise<void> {
  await redis.eval(deadLetterScript, 3, stream, config.dlqStream, failureBudgetKey(stream, group), group, id, config.dlqMaxlen,
    stream, reason, error instanceof Error ? error.message : String(error), JSON.stringify(fields), new Date().toISOString());
}
