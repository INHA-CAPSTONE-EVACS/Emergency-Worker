import {ContractError, type WorkerEvent} from '../domain/contracts';
import type {DispatchLifecycleService} from './dispatch-lifecycle-service';

export class EmergencyEventSubscriber {
  constructor(private readonly lifecycle: Pick<DispatchLifecycleService, 'onEmergencyCreated' | 'onDispatched' | 'onStatusChanged'>) {}
  readonly handle = async (event: WorkerEvent): Promise<void> => {
    switch (event.eventType) {
      case 'EMERGENCY_CREATED': return this.lifecycle.onEmergencyCreated(event);
      case 'EMERGENCY_DISPATCHED': return this.lifecycle.onDispatched(event);
      case 'EMERGENCY_STATUS_CHANGED': return this.lifecycle.onStatusChanged(event);
      default: throw new ContractError('Unexpected event type on emergency stream');
    }
  };
}
