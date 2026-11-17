import {RouteStabilizer} from '../../../src/route/route-stabilizer';

describe('RouteStabilizer', () => {
  const stabilizer = new RouteStabilizer();
  const previous = {remainingSeconds: 100, createdAt: '2026-11-16T12:00:00.000Z'};
  test.each([[86, 5000, false], [85, 4999, false], [85, 5000, true], [80, 6000, true],
    [100, 6000, false], [0, 5000, true]])('candidate %s after %sms => %s', (seconds, elapsed, expected) => {
    expect(stabilizer.shouldReplace(previous, seconds, Date.parse(previous.createdAt) + elapsed)).toBe(expected);
  });
  test('accepts an initial route and refuses invalid or zero-baseline comparisons', () => {
    expect(stabilizer.shouldReplace(null, 20, 0)).toBe(true);
    expect(stabilizer.shouldReplace(previous, NaN, Date.now())).toBe(false);
    expect(stabilizer.shouldReplace({...previous, remainingSeconds: 0}, 0, Date.now())).toBe(false);
  });
});
