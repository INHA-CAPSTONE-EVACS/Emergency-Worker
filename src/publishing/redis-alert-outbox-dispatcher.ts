import type Redis from 'ioredis';
import type {WorkerConfig} from '../config/config';
import {outboxDeliveredKey, parseStreamEvent} from '../domain/contracts';
import type {CanonicalRequestEvent} from '../domain/contracts';
import type {EventPublisher} from '../domain/types';
import {RedisStreamEventPublisher} from './redis-stream-event-publisher';

const cursorSuffix = ':delivery-cursor';
const receiptScript = `
redis.call('SET', KEYS[1], ARGV[1], 'NX')
local current = redis.call('GET', KEYS[2])
local function newer(a, b)
  local am, as = string.match(a, '^(%d+)%-(%d+)$')
  local bm, bs = string.match(b, '^(%d+)%-(%d+)$')
  return tonumber(am) > tonumber(bm) or (tonumber(am) == tonumber(bm) and tonumber(as) > tonumber(bs))
end
if not current or newer(ARGV[2], current) then redis.call('SET', KEYS[2], ARGV[2]) end
return 1`;

/** At-least-once delivery. The durable record survives an XADD/receipt crash gap. */
export class RedisAlertOutboxDispatcher {
  private readonly publisher: EventPublisher;
  constructor(private readonly redis: Redis, private readonly config: Pick<WorkerConfig,
    'alertOutboxStream' | 'alertStream' | 'readCount' | 'alertStreamMaxlen' | 'gatewayAlertStream'>,
    publisher?: EventPublisher) {
    this.publisher = publisher ?? new RedisStreamEventPublisher(redis, config);
  }

  async dispatchPending(): Promise<void> {
    const cursorKey = `${this.config.alertOutboxStream}${cursorSuffix}`;
    let cursor = await this.redis.get(cursorKey) ?? '0-0';
    for (;;) {
      const records = await this.redis.xrange(this.config.alertOutboxStream, `(${cursor}`, '+', 'COUNT', this.config.readCount);
      if (!records.length) return;
      for (const [id, fields] of records) {
        const eventKey = fields[fields.indexOf('eventKey') + 1];
        if (fields.indexOf('eventKey') < 0 || !eventKey) throw new Error('Missing outbox event key');
        const event = parseStreamEvent(fields) as CanonicalRequestEvent;
        const deliveredKey = outboxDeliveredKey(eventKey);
        let receipt = await this.redis.get(deliveredKey);
        if (!receipt) {
          receipt = (await this.publisher.publish(this.config.alertStream, event)).messageId;
          if (!receipt) throw new Error('Destination XADD returned no receipt');
        }
        // Both receipt and checkpoint are recorded only after successful destination delivery.
        await this.redis.eval(receiptScript, 2, deliveredKey, cursorKey, receipt, id);
        cursor = id;
      }
    }
  }
}
