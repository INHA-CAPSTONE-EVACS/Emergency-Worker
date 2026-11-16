import type {Cone, RouteResult, VehicleState} from '../domain/types';
import type {RoadGraph} from '../route/road-graph';
import {bearingDegrees, distanceM, headingDifference} from './geometry';

export interface YieldCandidate {
  vehicle: VehicleState; distanceM: number; estimatedArrivalSeconds: number; sameRoute: boolean; intersection: boolean;
}

export class YieldTargetEvaluator {
  constructor(private readonly graph?: RoadGraph) {}

  calculateEta(emergency: VehicleState, target: VehicleState): number {
    const bearing = bearingDegrees(emergency, target);
    const projection = (vehicle: VehicleState) => vehicle.speedMps * Math.cos((vehicle.headingDegrees - bearing) * Math.PI / 180);
    const closingSpeed = projection(emergency) - projection(target);
    return closingSpeed <= 1e-9 ? Infinity : distanceM(emergency, target) / closingSpeed;
  }

  isInAlertWindow(etaSeconds: number): boolean { return Number.isFinite(etaSeconds) && etaSeconds >= 3 && etaSeconds <= 30; }

  isPassed(emergency: VehicleState, target: VehicleState): boolean {
    return distanceM(emergency, target) > 1e-7 && headingDifference(emergency.headingDegrees, bearingDegrees(emergency, target)) > 90 + 1e-9;
  }

  filterCandidates(emergency: VehicleState, candidates: readonly VehicleState[], cone: Cone, route?: RouteResult): YieldCandidate[] {
    const cells = new Set(cone.cells);
    const emergencyEdge = this.graph?.findNearestEdge(emergency);
    const segments = route?.outcome === 'ROUTE_FOUND' ? route.roadSegments : emergencyEdge ? [emergencyEdge.id] : [];
    const context = this.graph?.getRouteContext(emergency, segments, cone.distanceM);
    const routeEdges = new Set(context?.routeEdges.map(edge => edge.id));
    const incomingEdges = new Set(context?.incomingEdges.map(edge => edge.id));
    const result: YieldCandidate[] = [];
    for (const vehicle of candidates) {
      if (vehicle.vehicleId === emergency.vehicleId || vehicle.vehicleType !== 'NORMAL' || vehicle.status !== 'ACTIVE'
        || !cells.has(vehicle.h3CellId) || this.isPassed(emergency, vehicle)) continue;
      const difference = headingDifference(emergency.headingDegrees, vehicle.headingDegrees);
      if (difference >= 135) continue;
      const distance = distanceM(emergency, vehicle);
      if (distance > cone.distanceM + 1e-7) continue;
      const targetEdge = this.graph?.findNearestEdge(vehicle);
      const hasRoadData = !!targetEdge && routeEdges.size > 0;
      const sameRoute = !!targetEdge && routeEdges.has(targetEdge.id);
      const intersection = !!targetEdge && incomingEdges.has(targetEdge.id);
      if (hasRoadData ? !sameRoute && !intersection : difference > 45) continue;
      const inSector = distance < 1e-7 || headingDifference(cone.headingDegrees,
        bearingDegrees(cone.origin, vehicle)) <= cone.halfAngleDegrees + 1e-9;
      if (!inSector && !(hasRoadData && (sameRoute || intersection))) continue;
      const estimatedArrivalSeconds = this.calculateEta(emergency, vehicle);
      if (this.isInAlertWindow(estimatedArrivalSeconds)) result.push({vehicle, distanceM: distance,
        estimatedArrivalSeconds, sameRoute, intersection});
    }
    return result;
  }
}
