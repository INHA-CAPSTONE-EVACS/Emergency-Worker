import {RoadGraph} from '../../../src/route/road-graph';
import {graphDocument, vehicle} from '../decision/fixtures';

describe('RoadGraph', () => {
  test('validates a versioned edge-cell snapshot and isolates it from caller mutation', () => {
    const document = graphDocument();
    const graph = RoadGraph.load(document);
    const cells = [...document.edges[0]!.cells];
    document.edges[0]!.cells.length = 0;
    expect(graph.version).toBe('fixture-v1');
    expect(graph.getCellsForEdge('current')).toEqual(cells);
    expect(Object.isFrozen(graph.getCellsForEdge('current'))).toBe(true);
    expect(Object.isFrozen(graph.edges[0])).toBe(true);
    expect(graph.getCellsForEdge('missing')).toEqual([]);
    expect(graph.getOutgoingEdges('junction').map(edge => edge.id)).toEqual(['turn']);
  });

  test.each(['version', 'h3', 'node', 'duplicate', 'weight'] as const)('rejects invalid %s data before use', kind => {
    const data = graphDocument();
    if (kind === 'version') data.version = '';
    if (kind === 'h3') data.h3Resolution = 8;
    if (kind === 'node') data.edges[0]!.to = 'missing';
    if (kind === 'duplicate') data.edges[1]!.id = 'current';
    if (kind === 'weight') data.edges[0]!.lengthM = -1;
    expect(() => RoadGraph.load(data)).toThrow();
  });

  test('matches the nearest road segment rather than its endpoints or shared H3 cell', () => {
    const graph = RoadGraph.load(graphDocument());
    expect(graph.findNearestEdge({latitude: 0.002, longitude: -0.0018})?.id).toBe('incoming');
    expect(graph.findNearestEdge({latitude: 0.0021, longitude: -0.0018})?.id).toBe('unconnected');
    expect(graph.findNearestEdge({latitude: 1, longitude: 1})).toBeNull();
  });

  test.each([[0, 'current'], [180, 'a-reverse']] as const)('matches coincident directed roads using travel heading %s', (headingDegrees, expected) => {
    const document = graphDocument();
    const current = document.edges[0]!;
    document.edges.push({...current, id: 'a-reverse', from: current.to, to: current.from});
    const graph = RoadGraph.load(document);
    expect(graph.findNearestEdge(vehicle({latitude: 0.001, headingDegrees}))?.id).toBe(expected);
  });

  test('uses the local polyline direction on coincident curved roads', () => {
    const document = graphDocument();
    const current = document.edges[0]!;
    const geometry = [{latitude: 0, longitude: 0}, {latitude: 0.001, longitude: 0},
      {latitude: 0.001, longitude: -0.001}, {latitude: 0.002, longitude: 0}];
    const graph = RoadGraph.load({...document, edges: [
      {...current, geometry}, {...current, id: 'a-reverse', from: current.to, to: current.from, geometry: [...geometry].reverse()},
    ]});
    expect(graph.findNearestEdge(vehicle({latitude: 0.001, longitude: -0.0005, headingDegrees: 270}))?.id).toBe('current');
  });

  test('includes incoming roads only at reachable planned turns', () => {
    const graph = RoadGraph.load(graphDocument());
    const context = graph.getRouteContext(vehicle(), ['current', 'turn', 'far'], 400);
    expect(context.routeEdges.map(edge => edge.id)).toEqual(['current', 'turn']);
    expect(context.incomingEdges.map(edge => edge.id)).toEqual(['incoming']);
    expect(graph.getRouteContext(vehicle(), ['current', 'turn'], 100).incomingEdges).toEqual([]);
  });
});
