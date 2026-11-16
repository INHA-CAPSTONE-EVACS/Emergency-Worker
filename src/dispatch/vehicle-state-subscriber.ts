import {ContractError, type VehicleStateUpdatedEvent, type WorkerEvent} from '../domain/contracts';
import type {DispatchState} from '../domain/types';
import type {DispatchLifecycleService} from './dispatch-lifecycle-service';

/** Fast-Path implementation is injected by the decision-service composition layer. */
export interface VehicleStateFastPath {
  onEmergencyState(event: VehicleStateUpdatedEvent, dispatch: DispatchState): Promise<void>;
  onNormalState(event: VehicleStateUpdatedEvent): Promise<void>;
}
export class VehicleStateSubscriber {
  constructor(private readonly lifecycle: Pick<DispatchLifecycleService, 'findActiveByVehicleId'>,
    private readonly fastPath: VehicleStateFastPath) {}
  readonly handle = async (event: WorkerEvent): Promise<void> => {
    if (event.eventType !== 'VEHICLE_STATE_UPDATED') throw new ContractError('Unexpected event type on vehicle state stream');
    if (event.payload.vehicleType === 'NORMAL') return this.fastPath.onNormalState(event);
    const dispatch = await this.lifecycle.findActiveByVehicleId(event.payload.vehicleId);
    if (dispatch) await this.fastPath.onEmergencyState(event, dispatch);
  };
}
