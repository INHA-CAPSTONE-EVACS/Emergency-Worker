import type Redis from 'ioredis';
import {loadConfig} from '../../../src/config/config';
import type {DensitySnapshot} from '../../../src/domain/types';
import {DensitySnapshotError, EdgeWeightCalculator} from '../../../src/route/edge-weight-calculator';
import {RoadGraph} from '../../../src/route/road-graph';
import {RoutePlanner} from '../../../src/route/route-planner';
import document from '../../fixtures/road-graph.json';

const config = loadConfig({BPR_ALPHA: '0.5', BPR_BETA: '2', ROAD_CAPACITY: '10', BLOCKED_MULTIPLIER: '3'});
const weights = new EdgeWeightCalculator({} as Pick<Redis, 'multi'>, config);
const graph = RoadGraph.load(document);
const density = (edges: Record<string, number> = {}): DensitySnapshot => ({generation: 'g1',
  generatedAt: '2026-11-16T12:00:00.000Z', h3Resolution: 9, cells: {}, edges, edgeDensity: 'enabled', degraded: false});

describe('RoutePlanner', () => {
  test('chooses the longer route with the lowest travel time and produces ordered waypoints', () => {
    expect(new RoutePlanner(graph, weights).findRoute('a', 'd', density())).toEqual({outcome: 'ROUTE_FOUND',
      roadSegments: ['ac', 'cd'], congestedSegments: [], estimatedTravelSeconds: 20, distanceM: 400,
      waypoints: [{latitude: 0, longitude: 0}, {latitude: 0, longitude: 0.001}, {latitude: 0.001, longitude: 0.001}]});
  });
  test('uses density-weighted seconds rather than free-flow cost', () => {
    expect(new RoutePlanner(graph, weights).findRoute('a', 'd', density({ac: 39, cd: 39}))).toMatchObject({
      outcome: 'ROUTE_FOUND', roadSegments: ['ab', 'bd'], estimatedTravelSeconds: 40});
  });
  test('excludes an edge at the blocked threshold from route selection', () => {
    expect(new RoutePlanner(graph, weights).findRoute('a', 'd', density({ac: 40}))).toMatchObject({
      outcome: 'ROUTE_FOUND', roadSegments: ['ab', 'bd']});
  });
  test('finds congested route segments in order without duplicates', () => {
    const snapshot = density({ab: 10, cd: 40});
    expect(new RoutePlanner(graph, weights).findCongestedSegments(['ab', 'ac', 'cd', 'ab'], snapshot)).toEqual(['ab', 'cd']);
  });
  test('treats disabled edge density as free-flow routing', () => {
    expect(new RoutePlanner(graph, weights).findRoute('a', 'd', {...density({ac: 1e6}),
      edgeDensity: 'disabled:no-table', degraded: true})).toMatchObject({outcome: 'ROUTE_FOUND', roadSegments: ['ac', 'cd']});
  });
  test('returns typed NO_ROUTE for a disconnected directed destination or all blocked exits', () => {
    const planner = new RoutePlanner(graph, weights);
    expect(planner.findRoute('d', 'a', density())).toEqual({outcome: 'NO_ROUTE', reason: 'DISCONNECTED'});
    expect(planner.findRoute('a', 'd', density({ab: 20, ac: 40}))).toEqual({outcome: 'NO_ROUTE', reason: 'DISCONNECTED'});
  });
  test('returns typed failure for unavailable graph and invalid endpoint', () => {
    expect(new RoutePlanner(null, weights).findRoute('a', 'd', density())).toEqual({outcome: 'NO_ROUTE', reason: 'GRAPH_UNAVAILABLE'});
    expect(new RoutePlanner(graph, weights).findRoute('missing', 'd', density())).toEqual({outcome: 'NO_ROUTE', reason: 'INVALID_ENDPOINT'});
    expect(new RoutePlanner(graph, weights).findRoute({latitude: NaN, longitude: 0}, 'd', density())).toEqual({outcome: 'NO_ROUTE', reason: 'INVALID_ENDPOINT'});
  });
  test('classifies graph and accepted snapshot H3 mismatch as configuration blocked', () => {
    const resolution8Weights = new EdgeWeightCalculator({} as Pick<Redis, 'multi'>, {...config, h3Resolution: 8});
    const planner = new RoutePlanner(graph, resolution8Weights);
    const snapshot = {...density(), h3Resolution: 8};
    expect(() => planner.findRoute('a', 'd', snapshot)).toThrow(DensitySnapshotError);
    expect(() => planner.findRoute('a', 'd', snapshot)).toThrow(expect.objectContaining({
      code: 'H3_RESOLUTION_MISMATCH', configurationBlocked: true,
    }));
  });
  test('supports coordinate endpoints and a zero-edge route at the destination', () => {
    const planner = new RoutePlanner(graph, weights);
    expect(planner.findRoute({latitude: 0, longitude: 0}, {latitude: 0.001, longitude: 0.001}, density())).toMatchObject({
      outcome: 'ROUTE_FOUND', roadSegments: ['ac', 'cd']});
    expect(planner.findRoute('a', 'a', density())).toMatchObject({outcome: 'ROUTE_FOUND', roadSegments: [], estimatedTravelSeconds: 0, distanceM: 0});
  });
});
