import type Redis from 'ioredis';
import {getResolution, isValidCell} from 'h3-js';
import type {WorkerConfig} from '../config/config';
import {cellVehiclesKey, vehicleStateKey, VEHICLE_STATE_FIELDS, VEHICLE_TYPES} from '../domain/contracts';
import type {VehicleState} from '../domain/types';

export class VehicleIndexReader {
  constructor(private readonly redis: Redis, _config?: Pick<WorkerConfig, 'h3Resolution'>) {}

  async findVehicleIdsInCells(cells: readonly string[], cutoffMs: number): Promise<string[]> {
    if (!Number.isFinite(cutoffMs)) throw new Error('Invalid active cutoff');
    const unique = [...new Set(cells)];
    if (!unique.length) return [];
    const pipeline = this.redis.pipeline();
    for (const cell of unique) {
      if (!isValidCell(cell)) throw new Error('Invalid H3 cell');
      pipeline.zrangebyscore(cellVehiclesKey(cell), `(${cutoffMs}`, '+inf');
    }
    const results = await pipeline.exec();
    if (!results) throw new Error('Vehicle index pipeline aborted');
    const ids = new Set<string>();
    for (const [error, result] of results) {
      if (error) throw error;
      for (const id of result as string[]) ids.add(id);
    }
    return [...ids];
  }

  async getStates(ids: readonly string[]): Promise<VehicleState[]> {
    const unique = [...new Set(ids)];
    if (!unique.length) return [];
    const pipeline = this.redis.pipeline();
    for (const id of unique) pipeline.hgetall(vehicleStateKey(id));
    const results = await pipeline.exec();
    if (!results) throw new Error('Vehicle state pipeline aborted');
    const states: VehicleState[] = [];
    for (const [i, [error, value]] of results.entries()) {
      if (error) throw error;
      const hash = value as Record<string, string>;
      if (!VEHICLE_STATE_FIELDS.every(field => typeof hash[field] === 'string' && hash[field]!.length > 0)
        || hash.vehicleId !== unique[i] || hash.status !== 'ACTIVE'
        || !VEHICLE_TYPES.includes(hash.vehicleType as VehicleState['vehicleType']) || !isValidCell(hash.h3CellId!)) continue;
      const resolution = getResolution(hash.h3CellId!);
      // Preserve incompatible-but-valid H3 for the consumer's CONFIGURATION_BLOCKED policy.
      if (!Number.isInteger(resolution) || resolution < 0 || resolution > 15) continue;
      const state = {...hash, latitude: Number(hash.latitude), longitude: Number(hash.longitude),
        speedMps: Number(hash.speedMps), headingDegrees: Number(hash.headingDegrees),
        lastSequence: Number(hash.lastSequence), lastMeasuredAt: Number(hash.lastMeasuredAt),
        stateVersion: Number(hash.stateVersion)} as unknown as VehicleState;
      if (![state.latitude, state.longitude, state.speedMps, state.headingDegrees, state.lastMeasuredAt].every(Number.isFinite)
        || Math.abs(state.latitude) > 90 || Math.abs(state.longitude) > 180 || state.speedMps < 0
        || state.headingDegrees < 0 || state.headingDegrees >= 360 || state.lastMeasuredAt < 0
        || !Number.isSafeInteger(state.lastSequence) || state.lastSequence < 0
        || !Number.isSafeInteger(state.stateVersion) || state.stateVersion < 1
        || !Number.isFinite(Date.parse(state.lastUpdatedAt))) continue;
      states.push(state);
    }
    return states;
  }
}
