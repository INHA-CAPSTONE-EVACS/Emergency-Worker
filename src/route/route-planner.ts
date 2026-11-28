import type {Coordinates} from '../domain/contracts';
import type {DensitySnapshot, RouteResult} from '../domain/types';
import {distanceM, projectOnPolyline} from '../decision/geometry';
import {DensitySnapshotError, EdgeWeightCalculator} from './edge-weight-calculator';
import {RoadGraph, type RoadEdge, type RoadNode} from './road-graph';

type QueueEntry = {id: string; seconds: number; priority: number};
class MinQueue {
  private readonly entries: QueueEntry[] = [];
  push(entry: QueueEntry): void {
    this.entries.push(entry);
    let index = this.entries.length - 1;
    while (index > 0) {
      const parent = Math.floor((index - 1) / 2);
      if (this.entries[parent]!.priority <= entry.priority) break;
      this.entries[index] = this.entries[parent]!; index = parent;
    }
    this.entries[index] = entry;
  }
  pop(): QueueEntry | undefined {
    const head = this.entries[0], last = this.entries.pop();
    if (!this.entries.length || !last) return head;
    let index = 0;
    while (2 * index + 1 < this.entries.length) {
      let child = 2 * index + 1;
      if (child + 1 < this.entries.length && this.entries[child + 1]!.priority < this.entries[child]!.priority) child++;
      if (last.priority <= this.entries[child]!.priority) break;
      this.entries[index] = this.entries[child]!; index = child;
    }
    this.entries[index] = last;
    return head;
  }
}

/** A* uses seconds throughout. No density state is retained between route calculations. */
export class RoutePlanner {
  private readonly secondsPerMetre: number;
  constructor(private readonly graph: RoadGraph | null, private readonly weights: EdgeWeightCalculator) {
    // Graph lengths need not match geographic chords. This lower bound stays admissible even then.
    let bound = Infinity;
    for (const edge of graph?.edges ?? []) {
      const chord = distanceM(graph!.getNode(edge.from)!, graph!.getNode(edge.to)!);
      if (chord > 0) bound = Math.min(bound, edge.lengthM / edge.speedLimitMps / chord);
    }
    this.secondsPerMetre = Number.isFinite(bound) ? bound : 0;
  }

  findRoute(origin: string | (Coordinates & {headingDegrees?: number}), destination: string | Coordinates, snapshot: DensitySnapshot): RouteResult {
    if (!this.graph) return {outcome: 'NO_ROUTE', reason: 'GRAPH_UNAVAILABLE'};
    const start = this.startEndpoint(origin), goal = this.endpoint(destination);
    if (!start || !goal) return {outcome: 'NO_ROUTE', reason: 'INVALID_ENDPOINT'};
    if (snapshot.h3Resolution !== this.graph.h3Resolution) {
      throw new DensitySnapshotError('H3_RESOLUTION_MISMATCH', 'Graph and density H3 resolution mismatch');
    }
    const queue = new MinQueue(), costs = new Map<string, number>([[start.id, 0]]);
    const previous = new Map<string, RoadEdge>();
    queue.push({id: start.id, seconds: 0, priority: this.heuristic(start, goal)});
    for (let current = queue.pop(); current; current = queue.pop()) {
      if (current.seconds !== costs.get(current.id)) continue;
      if (current.id === goal.id) return this.result(start, goal, previous, current.seconds, snapshot);
      for (const edge of this.graph.getOutgoingEdges(current.id)) {
        const seconds = current.seconds + this.edgeCost(edge, snapshot);
        if (seconds >= (costs.get(edge.to) ?? Infinity)) continue;
        costs.set(edge.to, seconds); previous.set(edge.to, edge);
        queue.push({id: edge.to, seconds, priority: seconds + this.heuristic(this.graph.getNode(edge.to)!, goal)});
      }
    }
    return {outcome: 'NO_ROUTE', reason: 'DISCONNECTED'};
  }

  /** Price the accepted directed suffix from the same snapped endpoints used by findRoute. */
  estimateRemainingSeconds(origin: Coordinates & {headingDegrees?: number}, destination: Coordinates, segments: readonly string[],
    snapshot: DensitySnapshot): number | null {
    const graph = this.graph;
    if (!graph) return null;
    const start = this.startEndpoint(origin), goal = this.endpoint(destination);
    if (!start || !goal) return null;
    if (start.id === goal.id) return 0;
    const first = segments.findIndex(id => graph.getEdge(id)?.from === start.id);
    if (first === -1) return null;
    let node = start.id, seconds = 0;
    for (const id of segments.slice(first)) {
      const edge = graph.getEdge(id);
      if (!edge || edge.from !== node) return null;
      seconds += this.edgeCost(edge, snapshot);
      node = edge.to;
    }
    return node === goal.id ? seconds : null;
  }

  /** Congested means at or above per-lane capacity; blocked segments are included. */
  findCongestedSegments(segments: readonly string[], snapshot: DensitySnapshot): string[] {
    return [...new Set(segments)].filter(id => {
      const edge = this.graph?.getEdge(id);
      return edge !== undefined && this.weights.isCongested(edge, snapshot);
    });
  }

  /** Blocked edges stay traversable with a heavy fixed delay so detours win without causing DISCONNECTED. */
  private edgeCost(edge: RoadEdge, snapshot: DensitySnapshot): number {
    const base = this.weights.weight(edge, snapshot);
    return this.weights.isBlocked(edge, snapshot) ? base + 180 : base;
  }

  private heuristic(node: RoadNode, goal: RoadNode): number {return distanceM(node, goal) * this.secondsPerMetre;}

  private startEndpoint(value: string | (Coordinates & {headingDegrees?: number})): RoadNode | undefined {
    if (typeof value === 'string') return this.graph!.getNode(value);
    if (!Number.isFinite(value.latitude) || Math.abs(value.latitude) > 90
      || !Number.isFinite(value.longitude) || Math.abs(value.longitude) > 180) return undefined;
    const nearestEdge = this.graph!.findNearestEdge(value, 30);
    if (nearestEdge) {
      const fromNode = this.graph!.getNode(nearestEdge.from);
      const toNode = this.graph!.getNode(nearestEdge.to);
      if (fromNode && toNode) {
        const projection = projectOnPolyline(value, nearestEdge.geometry);
        if (distanceM(value, fromNode) > 5 && projection.remainingFraction < 0.95) {
          return toNode;
        }
        return fromNode;
      }
    }
    return this.endpoint(value);
  }

  private endpoint(value: string | Coordinates): RoadNode | undefined {
    if (typeof value === 'string') return this.graph!.getNode(value);
    if (!Number.isFinite(value.latitude) || Math.abs(value.latitude) > 90
      || !Number.isFinite(value.longitude) || Math.abs(value.longitude) > 180) return undefined;
    let nearest: RoadNode | undefined, distance = Infinity;
    for (const node of this.graph!.nodes) {
      const candidate = distanceM(value, node);
      if (candidate < distance) {distance = candidate; nearest = node;}
    }
    return nearest;
  }

  private result(start: RoadNode, goal: RoadNode, previous: Map<string, RoadEdge>, seconds: number, snapshot: DensitySnapshot): RouteResult {
    const edges: RoadEdge[] = [];
    for (let id = goal.id; id !== start.id;) {
      const edge = previous.get(id)!;
      edges.push(edge); id = edge.from;
    }
    edges.reverse();
    const waypoints: Coordinates[] = [];
    for (const point of edges.length ? edges.flatMap(edge => [...edge.geometry]) : [start]) {
      const last = waypoints[waypoints.length - 1];
      if (!last || point.latitude !== last.latitude || point.longitude !== last.longitude) {
        waypoints.push({latitude: point.latitude, longitude: point.longitude});
      }
    }
    const roadSegments = edges.map(edge => edge.id);
    return {outcome: 'ROUTE_FOUND', waypoints, roadSegments,
      congestedSegments: this.findCongestedSegments(roadSegments, snapshot), estimatedTravelSeconds: seconds,
      distanceM: edges.reduce((sum, edge) => sum + edge.lengthM, 0)};
  }
}
