import type Redis from 'ioredis';
import {getResolution, isValidCell} from 'h3-js';
import type {WorkerConfig} from '../config/config';
import {DENSITY_CELLS_KEY, DENSITY_EDGES_KEY, DENSITY_GENERATION_FIELD, DENSITY_META_KEY} from '../domain/contracts';
import type {DensitySnapshot} from '../domain/types';
import type {RoadEdge} from './road-graph';

type WeightConfig = Pick<WorkerConfig, 'h3Resolution' | 'bprAlpha' | 'bprBeta' | 'roadCapacity' | 'blockedMultiplier'>;
export type DensitySnapshotErrorCode = 'GENERATION_MISMATCH' | 'H3_RESOLUTION_MISMATCH' | 'INVALID_SNAPSHOT' | 'REDIS_READ_FAILED';
export class DensitySnapshotError extends Error {
  readonly configurationBlocked: boolean;
  constructor(readonly code: DensitySnapshotErrorCode, message: string) {
    super(message); this.name = 'DensitySnapshotError';
    this.configurationBlocked = code === 'H3_RESOLUTION_MISMATCH';
  }
}
const invalid = (message: string): never => {throw new DensitySnapshotError('INVALID_SNAPSHOT', message);};
const record = (value: unknown): value is Record<string, string> => typeof value === 'object' && value !== null && !Array.isArray(value)
  && Object.values(value).every(item => typeof item === 'string');

/** Both Location edge density and roadCapacity are in vehicles per kilometre per lane. */
export class EdgeWeightCalculator {
  constructor(private readonly redis: Pick<Redis, 'multi'>, private readonly config: WeightConfig) {}

  async loadDensitySnapshot(): Promise<DensitySnapshot> {
    let rows: [Error | null, unknown][] | null;
    try {
      // MULTI/EXEC observes one atomic snapshot, including absent hashes after expiry.
      rows = await this.redis.multi().hgetall(DENSITY_CELLS_KEY).hgetall(DENSITY_EDGES_KEY).hgetall(DENSITY_META_KEY).exec();
    } catch (error) {
      throw new DensitySnapshotError('REDIS_READ_FAILED', `Density read failed: ${String(error)}`);
    }
    if (!rows || rows.length !== 3 || rows.some(([error]) => error)) {
      throw new DensitySnapshotError('REDIS_READ_FAILED', 'Density transaction failed');
    }
    const cells = rows[0]![1], edges = rows[1]![1], meta = rows[2]![1];
    if (!record(cells) || !record(edges) || !record(meta)) return invalid('Invalid density hashes');
    if (!meta.generation?.trim() || !meta.generatedAt || !Number.isFinite(Date.parse(meta.generatedAt))
      || !meta.h3Resolution?.trim() || !Number.isInteger(Number(meta.h3Resolution))
      || Number(meta.h3Resolution) < 0 || Number(meta.h3Resolution) > 15) return invalid('Invalid density metadata');
    this.assertResolution(Number(meta.h3Resolution));
    const edgeDensity = meta.edgeDensity;
    if (edgeDensity !== 'enabled' && !/^disabled:.+/.test(edgeDensity ?? '')) return invalid('Invalid edge density mode');
    if (cells[DENSITY_GENERATION_FIELD] !== meta.generation
      || (edgeDensity === 'enabled' && edges[DENSITY_GENERATION_FIELD] !== meta.generation)) {
      throw new DensitySnapshotError('GENERATION_MISMATCH', 'Required density generations differ');
    }
    const parsedCells = this.parseValues(cells);
    if (Object.keys(parsedCells).some(cell => !isValidCell(cell) || getResolution(cell) !== this.config.h3Resolution)) {
      return invalid('Invalid density cell');
    }
    return Object.freeze({generation: meta.generation, generatedAt: meta.generatedAt,
      h3Resolution: Number(meta.h3Resolution), cells: parsedCells,
      edges: edgeDensity === 'enabled' ? this.parseValues(edges) : Object.freeze({}),
      edgeDensity: edgeDensity as DensitySnapshot['edgeDensity'], degraded: edgeDensity !== 'enabled'});
  }

  weight(edge: RoadEdge, snapshot: DensitySnapshot): number {
    const ratio = this.densityRatio(edge, snapshot);
    const freeFlowSeconds = edge.lengthM / edge.speedLimitMps;
    if (this.config.bprAlpha === 0 || ratio === 0) return freeFlowSeconds;
    return freeFlowSeconds * (1 + this.config.bprAlpha * ratio ** this.config.bprBeta);
  }

  isBlocked(edge: RoadEdge, snapshot: DensitySnapshot): boolean {
    // Disabled density is explicitly free-flow, even if the configured threshold is 1.
    if (snapshot.edgeDensity !== 'enabled') {
      this.assertResolution(snapshot.h3Resolution);
      return false;
    }
    return this.weight(edge, snapshot) >= edge.lengthM / edge.speedLimitMps * this.config.blockedMultiplier;
  }

  isCongested(edge: RoadEdge, snapshot: DensitySnapshot): boolean {
    return this.densityRatio(edge, snapshot) >= 1;
  }

  private densityRatio(edge: RoadEdge, snapshot: DensitySnapshot): number {
    this.assertResolution(snapshot.h3Resolution);
    if (snapshot.edgeDensity.startsWith('disabled:')) return 0;
    const value = snapshot.edges[edge.id] ?? 0;
    if (!Number.isFinite(value) || value < 0) return invalid('Invalid edge density');
    return value / this.config.roadCapacity;
  }

  private assertResolution(resolution: number): void {
    if (resolution !== this.config.h3Resolution) {
      throw new DensitySnapshotError('H3_RESOLUTION_MISMATCH', `Density H3 resolution ${resolution} differs from configured ${this.config.h3Resolution}`);
    }
  }

  private parseValues(hash: Record<string, string>): Readonly<Record<string, number>> {
    const entries: [string, number][] = [];
    for (const [key, raw] of Object.entries(hash)) {
      if (key === DENSITY_GENERATION_FIELD) continue;
      const value = Number(raw);
      if (!raw.trim() || !Number.isFinite(value) || value < 0) return invalid('Invalid density value');
      entries.push([key, value]);
    }
    return Object.freeze(Object.fromEntries(entries));
  }
}
