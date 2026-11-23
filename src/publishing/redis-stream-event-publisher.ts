import type Redis from 'ioredis';
import {loadConfig, type WorkerConfig} from '../config/config';
import {parseEventEnvelope, type CanonicalRequestEvent} from '../domain/contracts';
import type {EventPublisher, PublishReceipt} from '../domain/types';

/** Transport adapter: canonical request events to the Alert Worker stream. */
export class RedisStreamEventPublisher implements EventPublisher {
  constructor(private readonly redis: Redis,
    private readonly config: Pick<WorkerConfig, 'alertStreamMaxlen'> = loadConfig()) {}

  async publish(stream: string, event: CanonicalRequestEvent): Promise<PublishReceipt> {
    parseEventEnvelope(event);
    // A wrong-type destination fails with WRONGTYPE before anything is written.
    const messageId = await this.redis.xadd(stream, 'MAXLEN', '~', String(this.config.alertStreamMaxlen), '*',
      'eventKey', event.eventId, 'event', JSON.stringify(event));
    if (!messageId) throw new Error('Destination XADD returned no receipt');
    return {messageId, publishedAt: new Date().toISOString()};
  }
}
