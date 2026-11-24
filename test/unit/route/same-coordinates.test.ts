import {sameCoordinates} from '../../../src/route/route-scheduler';

describe('sameCoordinates', () => {
  test('treats a MySQL JSON round-trip difference in the last bit as the same destination', () => {
    expect(sameCoordinates({latitude: 37.44788080273004, longitude: 126.64631479062069},
      {latitude: 37.44788080273004, longitude: 126.64631479062068})).toBe(true);
  });

  test('detects a real destination change', () => {
    expect(sameCoordinates({latitude: 37.44788, longitude: 126.64631}, {latitude: 37.44788, longitude: 126.64632})).toBe(false);
    expect(sameCoordinates({latitude: 37.44788, longitude: 126.64631}, {latitude: 37.44789, longitude: 126.64631})).toBe(false);
  });
});
