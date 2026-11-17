import type Redis from 'ioredis';
import {loadConfig, type WorkerConfig} from '../config/config';
import {parseEventEnvelope, YIELD_ALERT_REQUESTED, type CanonicalRequestEvent, type GatewayYieldAlertPayload} from '../domain/contracts';
import type {EventPublisher, PublishReceipt} from '../domain/types';

// Validate both destinations before mutation: Redis script errors do not undo earlier writes.
const publishScript = `
local function stream(key)
  local kind = redis.call('TYPE', key).ok
  if kind ~= 'none' and kind ~= 'stream' then error('Invalid stream destination: ' .. key) end
end
stream(KEYS[1])
if ARGV[4] ~= '' then stream(KEYS[2]) end
local id = redis.call('XADD', KEYS[1], 'MAXLEN', '~', ARGV[1], '*', 'eventKey', ARGV[2], 'event', ARGV[3])
if ARGV[4] ~= '' then
  redis.call('XADD', KEYS[2], 'MAXLEN', '~', ARGV[1], '*', 'eventKey', ARGV[2], 'payload', ARGV[4])
end
return id`;

/** Transport adapter. Canonical decisions stay independent of the current Gateway projection. */
export class RedisStreamEventPublisher implements EventPublisher {
  constructor(private readonly redis: Redis, private readonly config: Pick<WorkerConfig,
    'gatewayAlertStream' | 'alertStreamMaxlen'> = loadConfig()) {}

  async publish(stream: string, event: CanonicalRequestEvent): Promise<PublishReceipt> {
    parseEventEnvelope(event);
    let projection: GatewayYieldAlertPayload | undefined;
    if (event.eventType === YIELD_ALERT_REQUESTED) {
      const p = event.payload;
      projection = {alertId: p.alertId, vehicleId: p.targetVehicleId, emergencyVehicleId: p.emergencyVehicleId,
        alertVersion: p.decisionVersion, issuedAt: event.occurredAt, expiresAt: p.expiresAt, details: event};
    }
    const messageId = await this.redis.call('EVAL', publishScript, 2, stream, this.config.gatewayAlertStream,
      this.config.alertStreamMaxlen, event.eventId, JSON.stringify(event), projection ? JSON.stringify(projection) : '') as string;
    return {messageId, publishedAt: new Date().toISOString()};
  }
}
