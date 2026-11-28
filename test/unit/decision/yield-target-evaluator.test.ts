import {MovingConeCalculator} from '../../../src/decision/moving-cone-calculator';
import {YieldTargetEvaluator} from '../../../src/decision/yield-target-evaluator';
import {RoadGraph} from '../../../src/route/road-graph';
import {graphDocument, target, turnRoute, vehicle} from './fixtures';

describe('YieldTargetEvaluator', () => {
  const calculator = new MovingConeCalculator();
  const evaluator = new YieldTargetEvaluator();
  test.each([[2.999, false], [3, true], [30, true], [30.001, false], [Infinity, false]])('ETA %s is eligible: %s', (eta, eligible) => {
    expect(evaluator.isInAlertWindow(eta)).toBe(eligible);
  });
  test('uses great-circle distance and projected relative closing speed in m/s', () => {
    expect(evaluator.calculateEta(vehicle(), target())).toBeCloseTo(13.3434, 3);
    expect(evaluator.calculateEta(vehicle(), target({speedMps: 20}))).toBe(Infinity);
    expect(evaluator.calculateEta(vehicle(), target({speedMps: 30}))).toBe(Infinity);
  });
  test.each([[45, true], [45.01, false], [135, false], [359, true]])('without edge data accepts only headings <=45 degrees: %s', (heading, expected) => {
    const result = evaluator.filterCandidates(vehicle(), [target({headingDegrees: heading})], calculator.buildCone(vehicle()));
    expect(result.length > 0).toBe(expected);
  });
  test('marks a rear target passed and does not alert it even in the mandatory neighbor cells', () => {
    const rear = target({latitude: -0.0001});
    expect(evaluator.isPassed(vehicle(), rear)).toBe(true);
    expect(evaluator.isPassed(vehicle(), target())).toBe(false);
    expect(evaluator.filterCandidates(vehicle(), [rear], calculator.buildCone(vehicle()))).toEqual([]);
  });
  test('filters inactive, emergency, outside-sector, outside-distance and outside-ETA targets', () => {
    const candidates = [target(), target({vehicleId: 'inactive', status: 'INACTIVE'}),
      target({vehicleId: 'other-emergency', vehicleType: 'EMERGENCY'}), vehicle(),
      target({vehicleId: 'side', latitude: 0, longitude: 0.001}),
      target({vehicleId: 'far', latitude: 0.005}), target({vehicleId: 'too-soon', latitude: 0.0001}),
      target({vehicleId: 'too-late', speedMps: 15})];
    expect(evaluator.filterCandidates(vehicle(), candidates, calculator.buildCone(vehicle())).map(candidate => candidate.vehicle.vehicleId))
      .toEqual(['target']);
  });
  test('F-DEC-05 considers a connected incoming road at a planned turn, excludes a nearby disconnected road', () => {
    const graph = RoadGraph.load(graphDocument());
    const connected = target({vehicleId: 'incoming', latitude: 0.002, longitude: -0.0018, headingDegrees: 90});
    const unrelated = target({vehicleId: 'unconnected', latitude: 0.0021, longitude: -0.0018, headingDegrees: 90});
    // Both occupy one H3 cell: topological matching must make the distinction.
    expect(connected.h3CellId).toBe(unrelated.h3CellId);
    const result = new YieldTargetEvaluator(graph).filterCandidates(vehicle(), [connected, unrelated],
      calculator.buildRouteCone(vehicle(), turnRoute, graph), turnRoute);
    expect(result.map(candidate => candidate.vehicle.vehicleId)).toEqual(['incoming']);
    expect(result[0]).toMatchObject({sameRoute: false, intersection: true});
  });
  test('retains a northbound same-route target when a coincident reverse edge sorts first', () => {
    const document = graphDocument();
    const current = document.edges[0]!;
    document.edges.push({...current, id: 'a-reverse', from: current.to, to: current.from});
    const graph = RoadGraph.load(document);
    const result = new YieldTargetEvaluator(graph).filterCandidates(vehicle(), [target()],
      calculator.buildRouteCone(vehicle(), turnRoute, graph), turnRoute);
    expect(result.map(candidate => candidate.vehicle.vehicleId)).toEqual(['target']);
    expect(result[0]).toMatchObject({sameRoute: true, intersection: false});
    expect(result[0]!.estimatedArrivalSeconds).toBeCloseTo(13.3434, 3);
  });
  test('rejects the exact 135 degree boundary even with relevant road-edge data', () => {
    const graph = RoadGraph.load(graphDocument());
    const cone = calculator.buildRouteCone(vehicle(), turnRoute, graph);
    const result = new YieldTargetEvaluator(graph).filterCandidates(vehicle(),
      [target({headingDegrees: 134.99}), target({vehicleId: 'opposing', headingDegrees: 135})], cone, turnRoute);
    expect(result.map(candidate => candidate.vehicle.vehicleId)).toEqual(['target']);
  });
  test('matches resolution-9 vehicle states against a resolution-10 cone', () => {
    const graph = RoadGraph.load(graphDocument());
    const fine = new MovingConeCalculator(10);
    const connected = target({vehicleId: 'incoming', latitude: 0.002, longitude: -0.0018, headingDegrees: 90});
    const unrelated = target({vehicleId: 'unconnected', latitude: 0.0021, longitude: -0.0018, headingDegrees: 90});
    const cone = fine.buildRouteCone(vehicle(), turnRoute, graph);
    const result = new YieldTargetEvaluator(graph).filterCandidates(vehicle(), [target(), connected, unrelated], cone, turnRoute);
    expect(result.map(candidate => candidate.vehicle.vehicleId)).toEqual(['target', 'incoming']);
  });
});
