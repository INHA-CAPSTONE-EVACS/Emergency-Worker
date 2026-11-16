import {YieldActionResolver} from '../../../src/decision/yield-action-resolver';
import {target, vehicle} from './fixtures';

describe('YieldActionResolver', () => {
  const resolver = new YieldActionResolver();
  test.each([
    [0, -0.001, 'REAR'], [-0.001, -0.001, 'REAR_LEFT'], [0.001, -0.001, 'REAR_RIGHT'],
    [-0.001, 0, 'LEFT'], [0.001, 0, 'RIGHT'],
  ] as const)('describes emergency position (%s,%s) relative to target heading', (longitude, latitude, expected) => {
    expect(resolver.resolveDirection(vehicle({latitude, longitude}), target({latitude: 0, longitude: 0}))).toBe(expected);
  });
  test.each([
    [{sameRoute: true, slow: false, intersection: false}, 'MOVE_RIGHT_WHEN_SAFE', 'SAME_ROUTE_AHEAD'],
    [{sameRoute: true, slow: true, intersection: false}, 'MOVE_RIGHT_WHEN_SAFE', 'MOVING_SLOW'],
    [{sameRoute: false, slow: false, intersection: true}, 'CLEAR_INTERSECTION_FIRST', 'BLOCK_SECTION'],
    [{sameRoute: false, slow: false, intersection: false}, 'YIELD_WHEN_SAFE', 'BLOCK_SECTION'],
  ] as const)('maps relevant candidate %o to contract enums', (context, action, reason) => {
    expect(resolver.resolveAction(context)).toBe(action);
    expect(resolver.resolveReason(context)).toBe(reason);
  });
});
