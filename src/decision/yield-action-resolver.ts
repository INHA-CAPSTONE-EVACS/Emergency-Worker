import type {ApproachDirection, RecommendedAction, YieldReason} from '../domain/contracts';
import type {VehicleState} from '../domain/types';
import {bearingDegrees, signedAngle} from './geometry';

export interface YieldActionContext {sameRoute: boolean; intersection: boolean; slow?: boolean}

export class YieldActionResolver {
  /** Direction describes where the emergency vehicle is relative to the target's heading. */
  resolveDirection(emergency: VehicleState, target: VehicleState): ApproachDirection {
    const relative = signedAngle(target.headingDegrees, bearingDegrees(target, emergency));
    if (Math.abs(relative) >= 157.5) return 'REAR';
    if (relative <= -112.5) return 'REAR_LEFT';
    if (relative >= 112.5) return 'REAR_RIGHT';
    return relative < 0 ? 'LEFT' : 'RIGHT';
  }

  resolveAction(context: YieldActionContext): RecommendedAction {
    if (context.intersection) return 'CLEAR_INTERSECTION_FIRST';
    return context.sameRoute ? 'MOVE_RIGHT_WHEN_SAFE' : 'YIELD_WHEN_SAFE';
  }

  resolveReason(context: YieldActionContext): YieldReason {
    if (context.intersection) return 'BLOCK_SECTION';
    if (context.slow) return 'MOVING_SLOW';
    return context.sameRoute ? 'SAME_ROUTE_AHEAD' : 'BLOCK_SECTION';
  }
}
