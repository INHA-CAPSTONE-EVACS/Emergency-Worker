import {VehicleIndexReader} from '../../../src/store/vehicle-index-reader';
import {redisFixture, config} from './fixtures';

describe('Location Worker read models', () => {
  const {redis, close} = redisFixture();
  const reader = new VehicleIndexReader(redis, config);
  afterAll(close);
  test('uses a strict active cutoff and returns only valid matching ACTIVE hashes', async () => {
    await redis.zadd('h3:vehicles:8930e0a845bffff', 999, 'old', 1000, 'boundary', 1001, 'active');
    await redis.zadd('h3:vehicles:8930e0a8453ffff', 1002, 'active');
    const hash = {vehicleId: 'active', vehicleType: 'NORMAL', latitude: '37.45', longitude: '126.65',
      speedMps: '10', headingDegrees: '30', h3CellId: '8930e0a845bffff', lastSequence: '2',
      lastMeasuredAt: '1001', lastUpdatedAt: '2026-11-16T12:00:00.000Z', stateVersion: '2', status: 'ACTIVE'};
    await redis.hset('vehicle:active:state', hash);
    await redis.hset('vehicle:inactive:state', {...hash, vehicleId: 'inactive', status: 'INACTIVE'});
    await redis.hset('vehicle:malformed:state', {...hash, vehicleId: 'malformed', h3CellId: 'broken'});
    await redis.hset('vehicle:mismatched-id:state', {...hash, vehicleId: 'someone-else'});
    await redis.hset('vehicle:nan:state', {...hash, vehicleId: 'nan', speedMps: 'NaN'});
    const ids = await reader.findVehicleIdsInCells(['8930e0a845bffff', '8930e0a8453ffff'], 1000);
    expect(ids).toEqual(['active']);
    expect(await reader.getStates([...ids, 'inactive', 'missing', 'malformed', 'mismatched-id', 'nan']))
      .toEqual([{...hash, latitude: 37.45, longitude: 126.65, speedMps: 10, headingDegrees: 30,
        lastSequence: 2, lastMeasuredAt: 1001, stateVersion: 2}]);
  });
  test('retains a valid different-resolution cell for consumer configuration policy', async () => {
    await redis.hset('vehicle:resolution-8:state', {vehicleId: 'resolution-8', vehicleType: 'NORMAL',
      latitude: '37', longitude: '126', speedMps: '0', headingDegrees: '0', h3CellId: '8830e0a845fffff',
      lastSequence: '1', lastMeasuredAt: '1001', lastUpdatedAt: '2026-11-16T12:00:00.000Z',
      stateVersion: '1', status: 'ACTIVE'});
    expect((await reader.getStates(['resolution-8']))[0]?.h3CellId).toBe('8830e0a845fffff');
    expect(await reader.getStates([])).toEqual([]);
    expect(await reader.findVehicleIdsInCells([], 0)).toEqual([]);
  });
});
