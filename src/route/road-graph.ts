import {getHexagonEdgeLengthAvg, getResolution, isValidCell, latLngToCell, UNITS} from 'h3-js';
import type {Coordinates} from '../domain/contracts';
import {bearingDegrees, distanceM, headingDifference, projectOnPolyline} from '../decision/geometry';

export interface RoadNode extends Readonly<Coordinates> {readonly id: string}
export interface RoadEdge {
  readonly id: string; readonly from: string; readonly to: string; readonly lengthM: number;
  readonly lanes: number; readonly speedLimitMps: number; readonly cells: readonly string[];
  readonly geometry: readonly Readonly<Coordinates>[];
}
export interface RouteContext {routeEdges: readonly RoadEdge[]; incomingEdges: readonly RoadEdge[]}
const record = (value: unknown): value is Record<string, unknown> => typeof value === 'object' && value !== null && !Array.isArray(value);
const text = (value: unknown): value is string => typeof value === 'string' && value.trim().length > 0;
const positive = (value: unknown): value is number => typeof value === 'number' && Number.isFinite(value) && value > 0;
const point = (value: unknown): value is Coordinates => record(value) && typeof value.latitude === 'number'
  && Number.isFinite(value.latitude) && Math.abs(value.latitude) <= 90 && typeof value.longitude === 'number'
  && Number.isFinite(value.longitude) && Math.abs(value.longitude) <= 180;
const empty: readonly RoadEdge[] = Object.freeze([]);
const noCells: readonly string[] = Object.freeze([]);

/** Immutable, directed graph snapshot. Loading/refresh I/O belongs to application composition. */
export class RoadGraph {
  private readonly nodeIndex: ReadonlyMap<string, RoadNode>;
  private readonly edgeIndex: ReadonlyMap<string, RoadEdge>;
  private readonly outgoing: ReadonlyMap<string, readonly RoadEdge[]>;
  private readonly incoming: ReadonlyMap<string, readonly RoadEdge[]>;
  private readonly derivedCells = new Map<number, Map<string, readonly string[]>>();

  private constructor(readonly version: string, readonly h3Resolution: number,
    readonly nodes: readonly RoadNode[], readonly edges: readonly RoadEdge[]) {
    this.nodeIndex = new Map(nodes.map(node => [node.id, node]));
    this.edgeIndex = new Map(edges.map(edge => [edge.id, edge]));
    this.outgoing = new Map(nodes.map(node => [node.id, Object.freeze(edges.filter(edge => edge.from === node.id))]));
    this.incoming = new Map(nodes.map(node => [node.id, Object.freeze(edges.filter(edge => edge.to === node.id))]));
    Object.freeze(this);
  }

  static load(document: unknown, expectedH3Resolution = 9): RoadGraph {
    if (!record(document) || !text(document.version) || document.h3Resolution !== expectedH3Resolution
      || !Number.isInteger(expectedH3Resolution) || expectedH3Resolution < 0 || expectedH3Resolution > 15
      || !Array.isArray(document.nodes) || !Array.isArray(document.edges)) throw new Error('Invalid road graph header');
    const nodes: RoadNode[] = [];
    const nodeIds = new Set<string>();
    for (const node of document.nodes) {
      if (!record(node) || !text(node.id) || !point(node) || nodeIds.has(node.id)) throw new Error('Invalid road graph node');
      nodes.push(Object.freeze({id: node.id, latitude: node.latitude, longitude: node.longitude}));
      nodeIds.add(node.id);
    }
    const byNode = new Map(nodes.map(node => [node.id, node]));
    const edges: RoadEdge[] = [];
    const edgeIds = new Set<string>();
    for (const edge of document.edges) {
      if (!record(edge) || !text(edge.id) || edgeIds.has(edge.id) || !text(edge.from) || !text(edge.to)
        || !byNode.has(edge.from) || !byNode.has(edge.to) || !positive(edge.lengthM)
        || !positive(edge.speedLimitMps) || !positive(edge.lanes) || !Number.isInteger(edge.lanes)
        || !Array.isArray(edge.cells) || !edge.cells.length || !edge.cells.every(cell => typeof cell === 'string'
          && isValidCell(cell) && getResolution(cell) === expectedH3Resolution)) throw new Error('Invalid road graph edge');
      const geometry: unknown = edge.geometry ?? [byNode.get(edge.from), byNode.get(edge.to)];
      if (!Array.isArray(geometry) || geometry.length < 2 || !geometry.every(point)) throw new Error('Invalid road edge geometry');
      edges.push(Object.freeze({id: edge.id, from: edge.from, to: edge.to, lengthM: edge.lengthM,
        lanes: edge.lanes, speedLimitMps: edge.speedLimitMps,
        cells: Object.freeze([...new Set<string>(edge.cells)].sort()),
        geometry: Object.freeze(geometry.map(value => Object.freeze({latitude: value.latitude, longitude: value.longitude})))}));
      edgeIds.add(edge.id);
    }
    return new RoadGraph(document.version, expectedH3Resolution, Object.freeze(nodes), Object.freeze(edges));
  }

  getNode(id: string): RoadNode | undefined { return this.nodeIndex.get(id); }
  getEdge(id: string): RoadEdge | undefined { return this.edgeIndex.get(id); }
  getOutgoingEdges(nodeId: string): readonly RoadEdge[] { return this.outgoing.get(nodeId) ?? empty; }
  getCellsForEdge(id: string, resolution = this.h3Resolution): readonly string[] {
    const edge = this.edgeIndex.get(id);
    if (!edge) return noCells;
    if (resolution === this.h3Resolution) return edge.cells;
    if (!Number.isInteger(resolution) || resolution < 0 || resolution > 15) throw new Error('Invalid H3 resolution');
    let byEdge = this.derivedCells.get(resolution);
    if (!byEdge) { byEdge = new Map(); this.derivedCells.set(resolution, byEdge); }
    const cached = byEdge.get(id);
    if (cached) return cached;
    const spacingM = Math.max(1, Math.min(10, getHexagonEdgeLengthAvg(resolution, UNITS.m) / 2));
    const cells = new Set<string>();
    for (let i = 0; i < edge.geometry.length; i++) {
      const from = edge.geometry[i]!;
      cells.add(latLngToCell(from.latitude, from.longitude, resolution));
      const to = edge.geometry[i + 1];
      if (!to) break;
      const steps = Math.ceil(distanceM(from, to) / spacingM);
      for (let step = 1; step < steps; step++) {
        const t = step / steps;
        cells.add(latLngToCell(from.latitude + (to.latitude - from.latitude) * t,
          from.longitude + (to.longitude - from.longitude) * t, resolution));
      }
    }
    const result = Object.freeze([...cells].sort());
    byEdge.set(id, result);
    return result;
  }

  findNearestEdge(position: Coordinates & {headingDegrees?: number}, maxDistanceM = 30, edgeIds?: readonly string[]): RoadEdge | null {
    let nearest: RoadEdge | null = null, nearestDistance = maxDistanceM, nearestHeadingDifference = Infinity;
    const candidates = edgeIds ? edgeIds.flatMap(id => this.getEdge(id) ?? []) : this.edges;
    for (const edge of candidates) {
      const projection = projectOnPolyline(position, edge.geometry);
      const distance = projection.distanceM;
      if (distance > maxDistanceM) continue;
      const difference = typeof position.headingDegrees === 'number' && Number.isFinite(position.headingDegrees)
        ? headingDifference(position.headingDegrees, projection.headingDegrees) : 0;
      // Keep geometric proximity primary, then distinguish coincident directed roads by local travel direction.
      const tiedDistance = Math.abs(distance - nearestDistance) <= 1e-7;
      if (!nearest || distance < nearestDistance - 1e-7 || (tiedDistance && (difference < nearestHeadingDifference
        || (difference === nearestHeadingDifference && edge.id < nearest.id)))) {
        nearest = edge; nearestDistance = distance; nearestHeadingDifference = difference;
      }
    }
    return nearest;
  }

  /** Follow the active route up to the cone horizon; add roads entering its planned turns. */
  getRouteContext(position: Coordinates, segments: readonly string[], maxDistanceM: number): RouteContext {
    const current = this.findNearestEdge(position, 30, segments);
    if (!current) return {routeEdges: empty, incomingEdges: empty};
    const start = segments.indexOf(current.id);
    const routeEdges: RoadEdge[] = [current], incomingEdges: RoadEdge[] = [];
    let travelled = current.lengthM * projectOnPolyline(position, current.geometry).remainingFraction;
    for (let i = start + 1; i < segments.length && travelled <= maxDistanceM; i++) {
      const previous = routeEdges[routeEdges.length - 1]!, next = this.getEdge(segments[i]!);
      if (!next || previous.to !== next.from) break;
      routeEdges.push(next);
      const before = bearingDegrees(previous.geometry[previous.geometry.length - 2]!, previous.geometry[previous.geometry.length - 1]!);
      const after = bearingDegrees(next.geometry[0]!, next.geometry[1]!);
      if (headingDifference(before, after) > 30) {
        incomingEdges.push(...(this.incoming.get(next.from) ?? empty).filter(edge => !segments.includes(edge.id)));
      }
      travelled += next.lengthM;
    }
    return {routeEdges, incomingEdges: [...new Map(incomingEdges.map(edge => [edge.id, edge])).values()]};
  }
}
