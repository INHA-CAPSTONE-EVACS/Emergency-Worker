import {cellToLatLng, getResolution, isValidCell, latLngToCell} from 'h3-js';
import {buildEdgeCellTable} from '../../../src/tools/edge-cell-table';
import type {RoadGraphDocument} from '../../../src/tools/sumo-road-graph';

// Cell centre and a point roughly 1.2 km east of it: a straight road between them crosses several res-9 cells.
const [lat, lng] = cellToLatLng(latLngToCell(37.4462, 126.6450, 9));
const edge = (id: string, geometry: {latitude: number; longitude: number}[], lengthM: number, lanes = 1) =>
  ({id, from: 'a', to: 'b', lengthM, lanes, speedLimitMps: 10, cells: [], geometry});
const graph = (edges: RoadGraphDocument['edges']): RoadGraphDocument => ({version: 'sumo:test:emergency:abc', h3Resolution: 9,
  nodes: [{id: 'a', latitude: lat, longitude: lng}, {id: 'b', latitude: lat, longitude: lng + 0.0135}], edges});
const total = (cells: Record<string, number>) => Object.values(cells).reduce((sum, value) => sum + value, 0);

describe('edge-cell table for the Location Worker', () => {
  test('writes the Location Worker header', () => {
    const table = buildEdgeCellTable(graph([]), '2026-11-23T09:00:00.000Z');
    expect(table).toEqual({schemaVersion: 1, h3Resolution: 9, source: 'sumo-net', graphVersion: 'sumo:test:emergency:abc',
      generatedAt: '2026-11-23T09:00:00.000Z', edges: {}});
  });

  test('a short edge inside one cell puts its whole SUMO length and lanes in that cell', () => {
    const table = buildEdgeCellTable(graph([edge('inside', [{latitude: lat, longitude: lng},
      {latitude: lat, longitude: lng + 0.0001}], 9.5, 2)]), 'now');
    expect(table.edges.inside).toEqual({lengthM: 9.5, lanes: 2, cells: {[latLngToCell(lat, lng, 9)]: 9.5}});
  });

  test('a crossing edge splits its length over the cells it passes, summing to the SUMO length', () => {
    const table = buildEdgeCellTable(graph([edge('long', [{latitude: lat, longitude: lng},
      {latitude: lat, longitude: lng + 0.0135}], 1300)]), 'now');
    const cells = table.edges.long!.cells;
    expect(Object.keys(cells).length).toBeGreaterThan(2);
    expect(cells[latLngToCell(lat, lng, 9)]).toBeGreaterThan(0);
    expect(cells[latLngToCell(lat, lng + 0.0135, 9)]).toBeGreaterThan(0);
    expect(total(cells)).toBeCloseTo(1300, 1);
    for (const [cell, length] of Object.entries(cells)) {
      expect(isValidCell(cell) && getResolution(cell) === 9).toBe(true);
      expect(length).toBeGreaterThan(0);
    }
  });

  test('omits edges the Location Worker would reject instead of invalidating the whole table', () => {
    const point = [{latitude: lat, longitude: lng}, {latitude: lat, longitude: lng + 0.0001}];
    const table = buildEdgeCellTable(graph([edge('zero', point, 0), edge('_reserved', point, 10), edge('ok', point, 10)]), 'now');
    expect(Object.keys(table.edges)).toEqual(['ok']);
  });

  test('a single-point geometry still assigns the length to its cell', () => {
    const table = buildEdgeCellTable(graph([edge('dot', [{latitude: lat, longitude: lng}], 4)]), 'now');
    expect(table.edges.dot!.cells).toEqual({[latLngToCell(lat, lng, 9)]: 4});
  });
});
