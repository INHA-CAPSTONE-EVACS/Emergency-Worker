import {getResolution, gridDisk, latLngToCell} from 'h3-js';
import {MovingConeCalculator} from '../../../src/decision/moving-cone-calculator';
import {RoadGraph} from '../../../src/route/road-graph';
import {graphDocument, turnRoute, vehicle} from './fixtures';

describe('MovingConeCalculator', () => {
  const calculator = new MovingConeCalculator();
  test.each([[0, 100], [5, 100], [10, 200], [25, 500], [60, 500]])('speed %s gives distance %s', (speed, distance) => {
    expect(calculator.computeDistance(speed)).toBe(distance);
  });
  test('includes the emergency cell and every immediate neighbor at the default resolution', () => {
    const cone = calculator.buildCone(vehicle({speedMps: 0}));
    expect(cone.halfAngleDegrees).toBe(30);
    expect(cone.cells).toEqual(expect.arrayContaining(gridDisk(latLngToCell(0, 0, 9), 1)));
    expect(cone.cells.every(cell => getResolution(cell) === 9)).toBe(true);
  });
  test('includes forward sector cells while excluding distant rear and side cells', () => {
    const cone = calculator.buildCone(vehicle({speedMps: 25}));
    expect(cone.cells).toContain(latLngToCell(0.004, 0, 9));
    expect(cone.cells).not.toContain(latLngToCell(-0.004, 0, 9));
    expect(cone.cells).not.toContain(latLngToCell(0, 0.004, 9));
  });
  test.each([[-30, true], [30, true], [-30.01, false], [30.01, false]])('uses inclusive bearing %s boundary', (angle, inside) => {
    // At the equator these 200 m points have hand-derived spherical bearings.
    const radians = angle * Math.PI / 180;
    const point = {latitude: Math.asin(Math.sin(200 / 6371008.8) * Math.cos(radians)) * 180 / Math.PI,
      longitude: Math.atan2(Math.sin(radians) * Math.sin(200 / 6371008.8), Math.cos(200 / 6371008.8)) * 180 / Math.PI};
    expect(calculator.containsPoint(calculator.buildCone(vehicle()), point)).toBe(inside);
  });
  test('expands with upcoming turn and incoming-edge cells without admitting disconnected edges', () => {
    const graph = RoadGraph.load(graphDocument());
    const cone = calculator.buildRouteCone(vehicle(), turnRoute, graph);
    expect(cone.cells).toEqual(expect.arrayContaining([...graph.getCellsForEdge('turn'), ...graph.getCellsForEdge('incoming')]));
    expect(cone.cells).not.toContain(latLngToCell(0.006, 0.004, 9));
  });
  test('NO_ROUTE preserves geometric fallback and cell diff is deterministic', () => {
    expect(calculator.buildRouteCone(vehicle(), {outcome: 'NO_ROUTE', reason: 'GRAPH_UNAVAILABLE'}).cells)
      .toEqual(calculator.buildCone(vehicle()).cells);
    expect(calculator.diffCells(['b', 'a', 'a'], ['c', 'b', 'c'])).toEqual({added: ['c'], removed: ['a']});
  });
});
