import {RouteStabilizer} from '../../../src/route/route-stabilizer';

describe('RouteStabilizer', () => {
  const stabilizer = new RouteStabilizer();
  const previous = {remainingSeconds: 100, createdAt: '2026-11-16T12:00:00.000Z'};
  test.each([[86, 5000, false], [85, 4999, false], [85, 5000, true], [80, 6000, true],
    [100, 6000, false], [0, 5000, true]])('candidate %s after %sms => %s', (seconds, elapsed, expected) => {
    expect(stabilizer.shouldReplace(previous, seconds, Date.parse(previous.createdAt) + elapsed)).toBe(expected);
  });
  test.each([
    [null, 20, 0, 'INITIAL'],
    [{remainingSeconds: 100, createdAt: '2026-11-16T12:00:00.000Z'}, 50, Date.parse('2026-11-16T12:00:04.999Z'), 'HOLD'],
    [{remainingSeconds: null, createdAt: '2026-11-16T12:00:00.000Z'}, 120, Date.parse('2026-11-16T12:00:01.000Z'), 'OFF_ROUTE'],
    [{remainingSeconds: null, createdAt: '2026-11-16T12:00:00.000Z'}, 120, Date.parse('2026-11-16T12:00:05.000Z'), 'OFF_ROUTE'],
    [{remainingSeconds: 100, createdAt: '2026-11-16T12:00:00.000Z'}, 85, Date.parse('2026-11-16T12:00:05.000Z'), 'FASTER'],
    [{remainingSeconds: 100, createdAt: '2026-11-16T12:00:00.000Z'}, 86, Date.parse('2026-11-16T12:00:05.000Z'), 'NOT_FASTER'],
    [{remainingSeconds: 0, createdAt: '2026-11-16T12:00:00.000Z'}, 0, Date.parse('2026-11-16T12:00:05.000Z'), 'NOT_FASTER'],
    [null, NaN, 0, 'INVALID'],
  ] as const)('verdict for %j, candidate %s at %s is %s', (baseline, seconds, now, verdict) => {
    expect(stabilizer.verdict(baseline, seconds, now)).toBe(verdict);
    expect(stabilizer.shouldReplace(baseline, seconds, now)).toBe(['INITIAL', 'OFF_ROUTE', 'FASTER'].includes(verdict));
  });
  test('accepts an initial route and refuses invalid or zero-baseline comparisons', () => {
    expect(stabilizer.shouldReplace(null, 20, 0)).toBe(true);
    expect(stabilizer.shouldReplace(previous, NaN, Date.now())).toBe(false);
    expect(stabilizer.shouldReplace({...previous, remainingSeconds: 0}, 0, Date.now())).toBe(false);
  });
});
