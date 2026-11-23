import {createHash} from 'node:crypto';
import {latLngToCell} from 'h3-js';
import type {Coordinates} from '../domain/contracts';
import {distanceM} from '../decision/geometry';

/**
 * Converts a SUMO network (`*.net.xml`, written by netconvert) into the RoadGraph document the Slow-Path loads.
 *
 * - Nodes are junctions; edges are normal SUMO edges with at least one lane open to `vehicleClass`
 *   (internal/crossing/walking-area edges and lanes closed to the class are dropped).
 * - Edge IDs stay SUMO edge IDs, so routes and congested segments name the roads SUMO and its adapter use.
 * - Geometry is the middle usable lane's centreline, which is where SUMO vehicles are reported, so the
 *   nearest-edge match and its travel direction follow the vehicle's lane rather than the road axis.
 * - Turn restrictions (`<connection>`) are not modelled: any edge entering a junction may continue on any edge
 *   leaving it. Routes may therefore include turns SUMO forbids.
 */
export interface RoadGraphDocument {
  version: string; h3Resolution: number;
  nodes: {id: string; latitude: number; longitude: number}[];
  edges: {id: string; from: string; to: string; lengthM: number; lanes: number; speedLimitMps: number;
    cells: string[]; geometry: Coordinates[]}[];
}
export interface ConversionOptions {
  h3Resolution?: number;
  /** SUMO vehicle class whose permissions decide which lanes are usable. */
  vehicleClass?: string;
  /** Spacing of the points sampled along an edge to collect its H3 cells. */
  cellSampleSpacingM?: number;
  /** Label for the version string, typically the scenario name. */
  source?: string;
}
export interface ConversionReport {
  graph: RoadGraphDocument;
  skippedEdges: {internal: number; closedToClass: number};
  /** Share of nodes in the largest strongly connected component: routes between other nodes may be DISCONNECTED. */
  largestComponentShare: number;
}

type Attributes = Record<string, string>;
interface Lane {speed: number; length: number; shape: [number, number][]; usable: boolean}
interface Edge {id: string; from: string; to: string; func: string | undefined; lanes: Lane[]}

const entity: Record<string, string> = {amp: '&', lt: '<', gt: '>', quot: '"', apos: "'"};
const unescape = (value: string) => value.replace(/&(amp|lt|gt|quot|apos);/g, (_, name: string) => entity[name]!);
function attributes(source: string): Attributes {
  const result: Attributes = {};
  for (const match of source.matchAll(/([\w:.-]+)="([^"]*)"/g)) result[match[1]!] = unescape(match[2]!);
  return result;
}
function shape(value: string | undefined): [number, number][] {
  if (!value) return [];
  return value.trim().split(/\s+/).map(pair => {
    const [x, y] = pair.split(',').map(Number);
    if (!Number.isFinite(x) || !Number.isFinite(y)) throw new Error(`Invalid SUMO shape point: ${pair}`);
    return [x!, y!];
  });
}
function number(value: string | undefined, name: string): number {
  const parsed = Number(value);
  if (value === undefined || !Number.isFinite(parsed)) throw new Error(`Invalid SUMO ${name}: ${value}`);
  return parsed;
}

/** SUMO permissions: `allow` lists the only permitted classes, `disallow` removes classes from all. */
export function laneAllows(lane: Attributes, vehicleClass: string): boolean {
  const list = (value: string | undefined) => (value ?? '').split(/\s+/).filter(Boolean);
  if (lane.allow !== undefined) {
    const allowed = list(lane.allow);
    return allowed.includes('all') || allowed.includes(vehicleClass);
  }
  return !list(lane.disallow).includes(vehicleClass) && !list(lane.disallow).includes('all');
}

/**
 * Inverse transverse Mercator for the UTM zones netconvert writes (`+proj=utm +zone=N [+south]`, WGS84).
 * Accuracy is well below a metre inside a zone, far finer than an H3 resolution-9 cell.
 */
export function utmProjection(projParameter: string): (easting: number, northing: number) => Coordinates {
  const zone = /\+zone=(\d+)/.exec(projParameter)?.[1];
  if (!/\+proj=utm\b/.test(projParameter) || !zone) throw new Error(`Unsupported SUMO projection: ${projParameter}`);
  if (/\+(ellps|datum)=(?!WGS84)/.test(projParameter)) throw new Error(`Only WGS84 UTM is supported: ${projParameter}`);
  const south = /\+south\b/.test(projParameter);
  const a = 6378137, f = 1 / 298.257223563, k0 = 0.9996;
  const e2 = f * (2 - f), ep2 = e2 / (1 - e2);
  const e1 = (1 - Math.sqrt(1 - e2)) / (1 + Math.sqrt(1 - e2));
  const lon0 = ((Number(zone) - 1) * 6 - 180 + 3) * Math.PI / 180;
  return (easting, northing) => {
    const x = easting - 500000, y = south ? northing - 10_000_000 : northing;
    const mu = y / k0 / (a * (1 - e2 / 4 - 3 * e2 ** 2 / 64 - 5 * e2 ** 3 / 256));
    const phi1 = mu + (3 * e1 / 2 - 27 * e1 ** 3 / 32) * Math.sin(2 * mu)
      + (21 * e1 ** 2 / 16 - 55 * e1 ** 4 / 32) * Math.sin(4 * mu)
      + (151 * e1 ** 3 / 96) * Math.sin(6 * mu) + (1097 * e1 ** 4 / 512) * Math.sin(8 * mu);
    const sin = Math.sin(phi1), cos = Math.cos(phi1), tan = Math.tan(phi1);
    const n1 = a / Math.sqrt(1 - e2 * sin ** 2), r1 = a * (1 - e2) / (1 - e2 * sin ** 2) ** 1.5;
    const t1 = tan ** 2, c1 = ep2 * cos ** 2, d = x / (n1 * k0);
    const latitude = phi1 - (n1 * tan / r1) * (d ** 2 / 2
      - (5 + 3 * t1 + 10 * c1 - 4 * c1 ** 2 - 9 * ep2) * d ** 4 / 24
      + (61 + 90 * t1 + 298 * c1 + 45 * t1 ** 2 - 252 * ep2 - 3 * c1 ** 2) * d ** 6 / 720);
    const longitude = lon0 + (d - (1 + 2 * t1 + c1) * d ** 3 / 6
      + (5 - 2 * c1 + 28 * t1 - 3 * c1 ** 2 + 8 * ep2 + 24 * t1 ** 2) * d ** 5 / 120) / cos;
    return {latitude: latitude * 180 / Math.PI, longitude: longitude * 180 / Math.PI};
  };
}

const round = (value: number) => Math.round(value * 1e7) / 1e7;

function cellsAlong(points: readonly Coordinates[], resolution: number, spacingM: number): string[] {
  const cells = new Set<string>();
  for (let i = 0; i < points.length; i++) {
    const from = points[i]!;
    cells.add(latLngToCell(from.latitude, from.longitude, resolution));
    const to = points[i + 1];
    if (!to) break;
    const steps = Math.ceil(distanceM(from, to) / spacingM);
    for (let step = 1; step < steps; step++) {
      const t = step / steps;
      cells.add(latLngToCell(from.latitude + (to.latitude - from.latitude) * t,
        from.longitude + (to.longitude - from.longitude) * t, resolution));
    }
  }
  return [...cells].sort();
}

/** Kosaraju over the directed node graph. */
function largestStronglyConnectedShare(nodeIds: readonly string[], edges: readonly {from: string; to: string}[]): number {
  if (!nodeIds.length) return 0;
  const forward = new Map(nodeIds.map(id => [id, [] as string[]])), backward = new Map(nodeIds.map(id => [id, [] as string[]]));
  for (const edge of edges) { forward.get(edge.from)!.push(edge.to); backward.get(edge.to)!.push(edge.from); }
  const order: string[] = [], seen = new Set<string>();
  for (const root of nodeIds) {
    if (seen.has(root)) continue;
    const stack: [string, number][] = [[root, 0]]; seen.add(root);
    while (stack.length) {
      const top = stack[stack.length - 1]!, next = forward.get(top[0])![top[1]++];
      if (next === undefined) { order.push(top[0]); stack.pop(); }
      else if (!seen.has(next)) { seen.add(next); stack.push([next, 0]); }
    }
  }
  const assigned = new Set<string>();
  let largest = 0;
  for (const root of order.reverse()) {
    if (assigned.has(root)) continue;
    let size = 0;
    const stack = [root]; assigned.add(root);
    while (stack.length) {
      const id = stack.pop()!; size++;
      for (const next of backward.get(id)!) if (!assigned.has(next)) { assigned.add(next); stack.push(next); }
    }
    largest = Math.max(largest, size);
  }
  return largest / nodeIds.length;
}

export function convertSumoNet(xml: string, options: ConversionOptions = {}): ConversionReport {
  const resolution = options.h3Resolution ?? 9, vehicleClass = options.vehicleClass ?? 'emergency';
  const spacing = options.cellSampleSpacingM ?? 10;
  const text = xml.replace(/<!--[\s\S]*?-->/g, '');
  let project: ((x: number, y: number) => Coordinates) | undefined;
  const junctions = new Map<string, [number, number]>();
  const edges: Edge[] = [];
  let current: Edge | undefined;
  for (const match of text.matchAll(/<(\/?)(location|edge|lane|junction)\b([^>]*?)(\/?)>/g)) {
    const [, closing, tag, body, selfClosing] = match;
    if (closing) { if (tag === 'edge') current = undefined; continue; }
    const attrs = attributes(body!);
    if (tag === 'location') {
      const [ox, oy] = (attrs.netOffset ?? '').split(',').map(Number);
      if (!Number.isFinite(ox) || !Number.isFinite(oy)) throw new Error('SUMO location has no netOffset');
      const inverse = utmProjection(attrs.projParameter ?? '');
      // netconvert writes network coordinates as projected coordinates plus netOffset.
      project = (x, y) => inverse(x - ox!, y - oy!);
    } else if (tag === 'junction') {
      if (attrs.id && !attrs.id.startsWith(':')) junctions.set(attrs.id, [number(attrs.x, 'junction x'), number(attrs.y, 'junction y')]);
    } else if (tag === 'edge') {
      const edge: Edge = {id: attrs.id ?? '', from: attrs.from ?? '', to: attrs.to ?? '', func: attrs.function, lanes: []};
      edges.push(edge);
      current = selfClosing ? undefined : edge;
    } else if (tag === 'lane' && current) {
      current.lanes.push({speed: number(attrs.speed, 'lane speed'), length: number(attrs.length, 'lane length'),
        shape: shape(attrs.shape), usable: laneAllows(attrs, vehicleClass)});
    }
  }
  if (!project) throw new Error('SUMO network has no <location> element');

  const skippedEdges = {internal: 0, closedToClass: 0};
  const graphEdges: RoadGraphDocument['edges'] = [];
  const usedNodes = new Set<string>();
  for (const edge of edges) {
    if (edge.func !== undefined && edge.func !== 'normal') { skippedEdges.internal++; continue; }
    const usable = edge.lanes.filter(lane => lane.usable);
    if (!usable.length) { skippedEdges.closedToClass++; continue; }
    if (!junctions.has(edge.from) || !junctions.has(edge.to)) throw new Error(`Edge ${edge.id} references an unknown junction`);
    const middle = usable[Math.floor(usable.length / 2)]!;
    const geometry = middle.shape.length >= 2 ? middle.shape : [junctions.get(edge.from)!, junctions.get(edge.to)!];
    const points = geometry.map(([x, y]) => project!(x, y)).map(p => ({latitude: round(p.latitude), longitude: round(p.longitude)}));
    graphEdges.push({id: edge.id, from: edge.from, to: edge.to,
      lengthM: Math.max(...usable.map(lane => lane.length)), lanes: usable.length,
      speedLimitMps: Math.max(...usable.map(lane => lane.speed)),
      cells: cellsAlong(points, resolution, spacing), geometry: points});
    usedNodes.add(edge.from); usedNodes.add(edge.to);
  }
  const nodes = [...usedNodes].sort().map(id => {
    const [x, y] = junctions.get(id)!;
    const p = project!(x, y);
    return {id, latitude: round(p.latitude), longitude: round(p.longitude)};
  });
  const digest = createHash('sha256').update(xml).digest('hex').slice(0, 12);
  const graph: RoadGraphDocument = {version: `sumo:${options.source ?? 'net'}:${vehicleClass}:${digest}`,
    h3Resolution: resolution, nodes, edges: graphEdges};
  return {graph, skippedEdges, largestComponentShare: largestStronglyConnectedShare(nodes.map(node => node.id), graphEdges)};
}
