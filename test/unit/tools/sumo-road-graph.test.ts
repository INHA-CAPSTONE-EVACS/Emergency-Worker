import {readFileSync} from 'node:fs';
import {resolve} from 'node:path';
import {getResolution} from 'h3-js';
import {distanceM, projectOnPolyline} from '../../../src/decision/geometry';
import {RoadGraph} from '../../../src/route/road-graph';
import {RoutePlanner} from '../../../src/route/route-planner';
import {EdgeWeightCalculator} from '../../../src/route/edge-weight-calculator';
import {loadConfig} from '../../../src/config/config';
import type {DensitySnapshot} from '../../../src/domain/types';
import {convertSumoNet, laneAllows, utmProjection} from '../../../src/tools/sumo-road-graph';

const location = '<location netOffset="-291509.32,-4146353.11" convBoundary="0.00,0.00,1170.21,1089.72" '
  + 'origBoundary="126.617327,37.430650,126.745641,37.554308" projParameter="+proj=utm +zone=52 +ellps=WGS84 +datum=WGS84 +units=m +no_defs"/>';
// Two junctions joined both ways by a car road, plus an internal edge and a footpath that must be dropped.
const net = `<?xml version="1.0"?>
<!-- <edge id="commented-out" from="a" to="b"> -->
<net version="1.20">
  ${location}
  <edge id=":a_0" function="internal"><lane id=":a_0_0" index="0" speed="10" length="5" shape="0,0 1,1"/></edge>
  <edge id="ab" from="a" to="b" priority="1">
    <lane id="ab_0" index="0" disallow="tram rail" speed="13.89" length="100.50" shape="0.00,-1.60 100.00,-1.60"/>
    <lane id="ab_1" index="1" disallow="tram rail" speed="13.89" length="100.50" shape="0.00,1.60 100.00,1.60"/>
  </edge>
  <edge id="ba" from="b" to="a" priority="1">
    <lane id="ba_0" index="0" speed="8.33" length="100.50" shape="100.00,4.80 0.00,4.80"/>
  </edge>
  <edge id="path" from="a" to="b"><lane id="path_0" index="0" allow="pedestrian bicycle" speed="5" length="100" shape="0,9 100,9"/></edge>
  <junction id="a" type="priority" x="0.00" y="0.00"/>
  <junction id="b" type="priority" x="100.00" y="0.00"/>
  <junction id=":a_0_0" type="internal" x="0" y="0"/>
</net>`;

describe('SUMO network → RoadGraph', () => {
  test('UTM inverse returns the zone origin on its central meridian', () => {
    const inverse = utmProjection('+proj=utm +zone=52 +ellps=WGS84 +datum=WGS84 +units=m +no_defs');
    const origin = inverse(500000, 0);
    expect(origin.latitude).toBeCloseTo(0, 9);
    expect(origin.longitude).toBeCloseTo(129, 9);
    const south = utmProjection('+proj=utm +zone=52 +south +datum=WGS84')(500000, 10_000_000);
    expect(south.latitude).toBeCloseTo(0, 9);
  });
  test('rejects projections other than WGS84 UTM', () => {
    expect(() => utmProjection('+proj=tmerc +lat_0=38')).toThrow(/Unsupported/);
    expect(() => utmProjection('+proj=utm +zone=52 +ellps=bessel')).toThrow(/WGS84/);
  });
  test.each([
    [{}, true], [{disallow: 'tram rail'}, true], [{disallow: 'emergency'}, false], [{disallow: 'all'}, false],
    [{allow: 'pedestrian bicycle'}, false], [{allow: 'emergency passenger'}, true], [{allow: 'all'}, true],
  ])('lane permissions %p → %p', (lane, allowed) => {
    expect(laneAllows(lane, 'emergency')).toBe(allowed);
  });
  test('keeps directed roads open to emergency vehicles with lane-centre geometry and H3 cells', () => {
    const {graph, skippedEdges, largestComponentShare} = convertSumoNet(net, {source: 'fixture'});
    expect(skippedEdges).toEqual({internal: 1, closedToClass: 1});
    expect(graph.nodes.map(node => node.id)).toEqual(['a', 'b']);
    expect(graph.edges.map(edge => [edge.id, edge.from, edge.to, edge.lanes, edge.speedLimitMps, edge.lengthM])).toEqual([
      ['ab', 'a', 'b', 2, 13.89, 100.5], ['ba', 'b', 'a', 1, 8.33, 100.5]]);
    const ab = graph.edges[0]!;
    expect(ab.geometry).toHaveLength(2);
    expect(distanceM(ab.geometry[0]!, ab.geometry[1]!)).toBeCloseTo(100, 0);
    expect(ab.cells.length).toBeGreaterThan(0);
    expect(ab.cells.every(cell => getResolution(cell) === 9)).toBe(true);
    expect(largestComponentShare).toBe(1);
    expect(graph.version).toMatch(/^sumo:fixture:emergency:[0-9a-f]{12}$/);
    expect(() => RoadGraph.load(graph, 9)).not.toThrow();
  });
  test('fails without a projection', () => {
    expect(() => convertSumoNet('<net><edge id="x" from="a" to="b"></edge></net>')).toThrow(/location/);
  });

  // The real scenario, when present in the workspace: the worker's own loader and planner accept the output.
  const scenario = resolve(__dirname, '../../../../sumo_scenario/scenario-inhadae/roads.net.xml');
  let xml: string | undefined;
  try { xml = readFileSync(scenario, 'utf8'); } catch { xml = undefined; }
  (xml ? test : test.skip)('the Inha scenario converts to a loadable, mostly connected graph that routes', () => {
    const {graph, largestComponentShare} = convertSumoNet(xml!, {source: 'scenario-inhadae'});
    const loaded = RoadGraph.load(graph, 9);
    expect(loaded.edges.length).toBeGreaterThan(50);
    // Recorded from the running SUMO: TraCI convertGeo placed EVACS_CAR_000229 on edge -955217469#3 here.
    const sample = {latitude: 37.44567348644627, longitude: 126.6504407367447};
    expect(projectOnPolyline(sample, loaded.getEdge('-955217469#3')!.geometry).distanceM).toBeLessThan(5);
    expect(largestComponentShare).toBeGreaterThan(0.8);
    const density: DensitySnapshot = {generation: '1', generatedAt: '2026-11-23T00:00:00.000Z', h3Resolution: 9,
      cells: {}, edges: {}, edgeDensity: 'disabled:table-not-configured', degraded: true};
    const planner = new RoutePlanner(loaded, new EdgeWeightCalculator(undefined as never, loadConfig({})));
    const route = planner.findRoute({latitude: 37.4425, longitude: 126.6465}, {latitude: 37.4485, longitude: 126.6545}, density);
    expect(route.outcome).toBe('ROUTE_FOUND');
  });
});
