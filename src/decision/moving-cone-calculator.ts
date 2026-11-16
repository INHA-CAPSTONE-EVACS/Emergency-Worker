import {gridDisk, latLngToCell, polygonToCellsExperimental, POLYGON_TO_CELLS_FLAGS} from 'h3-js';
import type {Coordinates} from '../domain/contracts';
import type {Cone, RouteResult, VehicleState} from '../domain/types';
import type {RoadGraph} from '../route/road-graph';
import {bearingDegrees, destination, distanceM, headingDifference, normalizeHeading} from './geometry';

export class MovingConeCalculator {
  constructor(readonly h3Resolution = 9) {
    if (!Number.isInteger(h3Resolution) || h3Resolution < 0 || h3Resolution > 15) throw new Error('Invalid H3 resolution');
  }

  computeDistance(speedMps: number): number {
    if (!Number.isFinite(speedMps) || speedMps < 0) throw new Error('Invalid speed');
    return Math.max(100, Math.min(500, speedMps * 20));
  }

  buildCone(vehicle: VehicleState): Cone {
    const origin = {latitude: vehicle.latitude, longitude: vehicle.longitude};
    const distance = this.computeDistance(vehicle.speedMps), heading = normalizeHeading(vehicle.headingDegrees);
    const ring: number[][] = [[origin.latitude, origin.longitude]];
    for (let offset = -30; offset <= 30; offset++) {
      const point = destination(origin, heading + offset, distance);
      ring.push([point.latitude, point.longitude]);
    }
    ring.push([origin.latitude, origin.longitude]);
    const cells = new Set(polygonToCellsExperimental(ring, this.h3Resolution, POLYGON_TO_CELLS_FLAGS.containmentOverlapping));
    for (const cell of gridDisk(latLngToCell(origin.latitude, origin.longitude, this.h3Resolution), 1)) cells.add(cell);
    return {origin, headingDegrees: heading, distanceM: distance, halfAngleDegrees: 30, cells: [...cells].sort()};
  }

  buildRouteCone(vehicle: VehicleState, route: RouteResult, graph?: RoadGraph): Cone {
    const cone = this.buildCone(vehicle);
    if (!graph || route.outcome !== 'ROUTE_FOUND') return cone;
    if (graph.h3Resolution !== this.h3Resolution) throw new Error('Road graph H3 resolution mismatch');
    const context = graph.getRouteContext(vehicle, route.roadSegments, cone.distanceM);
    const cells = new Set(cone.cells);
    for (const edge of [...context.routeEdges, ...context.incomingEdges]) for (const cell of graph.getCellsForEdge(edge.id)) cells.add(cell);
    return {...cone, cells: [...cells].sort()};
  }

  containsPoint(cone: Cone, point: Coordinates): boolean {
    const distance = distanceM(cone.origin, point);
    return distance <= cone.distanceM + 1e-7 && (distance < 1e-7
      || headingDifference(cone.headingDegrees, bearingDegrees(cone.origin, point)) <= cone.halfAngleDegrees + 1e-9);
  }

  diffCells(previous: readonly string[], next: readonly string[]): {added: string[]; removed: string[]} {
    const before = new Set(previous), after = new Set(next);
    return {added: [...after].filter(cell => !before.has(cell)).sort(), removed: [...before].filter(cell => !after.has(cell)).sort()};
  }
}
