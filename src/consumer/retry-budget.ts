import type Redis from 'ioredis';

/** A shared counter survives reclaim without charging configuration/predecessor wait attempts. */
export const failureBudgetKey = (stream: string, group: string): string =>
  `emergency-worker:consumer-failures:${JSON.stringify([stream, group])}`;
const recordFailureScript = `
local pending = redis.call('XPENDING', KEYS[1], ARGV[1], ARGV[2], ARGV[2], 1)
if #pending == 0 then return 0 end
return redis.call('HINCRBY', KEYS[2], ARGV[2], 1)`;
const acknowledgeScript = `
local acknowledged = redis.call('XACK', KEYS[1], ARGV[1], ARGV[2])
redis.call('HDEL', KEYS[2], ARGV[2])
return acknowledged`;

export async function recordOrdinaryFailure(redis: Redis, stream: string, group: string, id: string): Promise<number> {
  return await redis.eval(recordFailureScript, 2, stream, failureBudgetKey(stream, group), group, id) as number;
}
export async function acknowledge(redis: Redis, stream: string, group: string, id: string): Promise<void> {
  await redis.eval(acknowledgeScript, 2, stream, failureBudgetKey(stream, group), group, id);
}
