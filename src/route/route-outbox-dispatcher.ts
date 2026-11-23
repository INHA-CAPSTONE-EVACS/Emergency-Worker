import {ROUTE_UPDATE_REQUESTED} from '../domain/contracts';
import type {EventPublisher} from '../domain/types';
import type {DispatchRouteRepository} from './dispatch-route-repository';

/**
 * At-least-once delivery; the immutable event key survives crashes after Redis accepts a message.
 * Route requests also go to the WebSocket Gateway stream. A record is marked delivered only after every
 * destination accepted it, so a partial failure republishes to both with the same event key.
 */
export class RouteOutboxDispatcher {
  private running?: Promise<number>;
  constructor(private readonly repository: Pick<DispatchRouteRepository, 'findPendingOutbox' | 'markOutboxDelivered'>,
    private readonly publisher: EventPublisher, private readonly stream: string,
    private readonly routeUpdateStream?: string) {}

  dispatchPending(limit = 100): Promise<number> {
    if (this.running) return this.running;
    this.running = this.deliver(limit).finally(() => {this.running = undefined;});
    return this.running;
  }

  private async deliver(limit: number): Promise<number> {
    const records = await this.repository.findPendingOutbox(limit);
    for (const record of records) {
      const receipt = await this.publisher.publish(this.stream, record.event);
      if (this.routeUpdateStream && record.event.eventType === ROUTE_UPDATE_REQUESTED) {
        await this.publisher.publish(this.routeUpdateStream, record.event);
      }
      await this.repository.markOutboxDelivered(record.outboxId, receipt);
    }
    return records.length;
  }
}
