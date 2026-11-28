import {ROUTE_UPDATE_REQUESTED} from '../domain/contracts';
import type {EventPublisher} from '../domain/types';
import type {DispatchRouteRepository} from './dispatch-route-repository';

/**
 * At-least-once delivery; the immutable event key survives crashes after Redis accepts a message.
 * ROUTE_UPDATE_REQUESTED is routed exclusively to routeUpdateStream (WebSocket Gateway);
 * other outbox records (such as YIELD_ALERT_CLEAR_REQUESTED) go to the alert stream.
 */
export class RouteOutboxDispatcher {
  private running?: Promise<number>;
  constructor(private readonly repository: Pick<DispatchRouteRepository, 'findPendingOutbox' | 'markOutboxDelivered'>,
    private readonly publisher: EventPublisher, private readonly stream: string,
    private readonly routeUpdateStream: string = stream) {}

  dispatchPending(limit = 100): Promise<number> {
    if (this.running) return this.running;
    this.running = this.deliver(limit).finally(() => {this.running = undefined;});
    return this.running;
  }

  private async deliver(limit: number): Promise<number> {
    const records = await this.repository.findPendingOutbox(limit);
    for (const record of records) {
      const targetStream = record.event.eventType === ROUTE_UPDATE_REQUESTED ? this.routeUpdateStream : this.stream;
      const receipt = await this.publisher.publish(targetStream, record.event);
      await this.repository.markOutboxDelivered(record.outboxId, receipt);
    }
    return records.length;
  }
}
