import {latLngToCell} from 'h3-js';
import type {Coordinates} from '../domain/contracts';
import {distanceM} from '../decision/geometry';
import type {RoadGraphDocument} from './sumo-road-graph';

/**
 * Location Worker edge-cell table (Location-Worker design §5.3), built from the same RoadGraph the Slow-Path loads,
 * so the edge IDs in `density:edges` are exactly the IDs the route planner looks up.
 */
export interface EdgeCellTableDocument {
  schemaVersion: 1; h3Resolution: number; source: 'sumo-net';
  /** RoadGraph version the table was derived from. The Location Worker ignores it. */
  graphVersion: string;
  /** Published by the Location Worker as `density:meta.tableVersion`. */
  generatedAt: string;
  edges: Record<string, {lengthM: number; lanes: number; cells: Record<string, number>; geometry?: readonly Coordinates[]}>;
}

const round = (value: number) => Math.round(value * 1000) / 1000;

/**
 * Walks each edge's geometry in steps of at most `stepM` metres and credits each step to the cell of its midpoint,
 * then scales the per-cell lengths so they sum to the SUMO lane length (`lengthM`).
 * Edges the Location Worker would reject (`lengthM <= 0`, IDs starting with `_`) are left out, because one invalid
 * edge disables edge density for the whole table.
 */
export function buildEdgeCellTable(graph: RoadGraphDocument, generatedAt: string, stepM = 1): EdgeCellTableDocument {
  const resolution = graph.h3Resolution;
  const cellOf = (point: Coordinates) => latLngToCell(point.latitude, point.longitude, resolution);
  const edges: EdgeCellTableDocument['edges'] = {};
  for (const edge of graph.edges) {
    if (!(edge.lengthM > 0) || edge.id.startsWith('_') || !edge.geometry.length) continue;
    const lengths = new Map<string, number>();
    let geometryLength = 0;
    for (let i = 0; i + 1 < edge.geometry.length; i++) {
      const from = edge.geometry[i]!, to = edge.geometry[i + 1]!, length = distanceM(from, to);
      const steps = Math.max(1, Math.ceil(length / stepM));
      for (let step = 0; step < steps; step++) {
        const t = (step + 0.5) / steps;
        const cell = cellOf({latitude: from.latitude + (to.latitude - from.latitude) * t,
          longitude: from.longitude + (to.longitude - from.longitude) * t});
        lengths.set(cell, (lengths.get(cell) ?? 0) + length / steps);
      }
      geometryLength += length;
    }
    if (geometryLength <= 0) {
      // A degenerate (single-point or zero-length) geometry still occupies the cell it sits in.
      lengths.clear();
      lengths.set(cellOf(edge.geometry[0]!), 1);
      geometryLength = 1;
    }
    const scale = edge.lengthM / geometryLength;
    const cells: Record<string, number> = {};
    for (const [cell, length] of [...lengths].sort(([a], [b]) => a.localeCompare(b))) {
      const scaled = round(length * scale);
      if (scaled > 0) cells[cell] = scaled;
    }
    if (Object.keys(cells).length) {
      const entry: EdgeCellTableDocument['edges'][string] = {lengthM: edge.lengthM, lanes: edge.lanes, cells};
      if (edge.geometry.length >= 2) {
        entry.geometry = edge.geometry.map(p => ({latitude: p.latitude, longitude: p.longitude}));
      }
      edges[edge.id] = entry;
    }
  }
  return {schemaVersion: 1, h3Resolution: resolution, source: 'sumo-net', graphVersion: graph.version, generatedAt, edges};
}
