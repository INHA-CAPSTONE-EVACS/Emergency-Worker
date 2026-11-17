import type Redis from 'ioredis';
import {loadConfig} from '../../../src/config/config';
import type {DensitySnapshot} from '../../../src/domain/types';
import {EdgeWeightCalculator} from '../../../src/route/edge-weight-calculator';
import {RoadGraph} from '../../../src/route/road-graph';
import document from '../../fixtures/road-graph.json';

const config = loadConfig({BPR_ALPHA: '0.5', BPR_BETA: '2', ROAD_CAPACITY: '10', BLOCKED_MULTIPLIER: '3'});
const edge = RoadGraph.load(document).getEdge('ac')!;
const density = (edges: Record<string, number> = {}): DensitySnapshot => ({
  generation: 'g1', generatedAt: '2026-11-16T12:00:00.000Z', h3Resolution: 9,
  cells: {'8930e0a845bffff': 99}, edges, edgeDensity: 'enabled', degraded: false,
});
// The only fake boundary is Redis I/O; it checks all three hashes are read atomically.
function redisHashes(cells: Record<string, string>, edges: Record<string, string>, meta: Record<string, string>) {
  const hashes: Record<string, Record<string, string>> = {'density:cells': cells, 'density:edges': edges, 'density:meta': meta};
  const keys: string[] = [];
  const transaction = {hgetall(key: string) {keys.push(key); return transaction;},
    async exec() {
      if (keys.length !== 3 || new Set(keys).size !== 3) throw new Error('Expected a single complete snapshot transaction');
      return keys.map(key => [null, hashes[key]]);
    }};
  return {multi: () => transaction} as unknown as Pick<Redis, 'multi'>;
}
const meta = (extra: Record<string, string> = {}) => ({generation: 'g1', generatedAt: '2026-11-16T12:00:00.000Z',
  intervalMs: '2000', ttlMs: '10000', h3Resolution: '9', tableVersion: 'fixture-v1', edgeDensity: 'enabled', ...extra});
const calculator = () => new EdgeWeightCalculator(redisHashes({}, {}, {}), config);

describe('EdgeWeightCalculator', () => {
  test('uses configured BPR and per-lane capacity to return seconds', () => {
    // t0=10, capacity=10*2, density=20: 10*(1+0.5*1^2)=15.
    expect(calculator().weight(edge, density({ac: 20}))).toBe(15);
    expect(calculator().weight(edge, density())).toBe(10);
  });
  test('supports zero congestion coefficient without overflow for finite high densities', () => {
    const weights = new EdgeWeightCalculator(redisHashes({}, {}, {}), {...config, bprAlpha: 0});
    expect(weights.weight(edge, density({ac: Number.MAX_VALUE}))).toBe(10);
  });
  test('blocks at the configured travel-time multiplier inclusively', () => {
    expect(calculator().isBlocked(edge, density({ac: 39}))).toBe(false);
    expect(calculator().isBlocked(edge, density({ac: 40}))).toBe(true);
  });
  test('loads matching enabled generations and excludes generation fields from values', async () => {
    const weights = new EdgeWeightCalculator(redisHashes({_generation: 'g1', '8930e0a845bffff': '4'},
      {_generation: 'g1', ac: '20'}, meta()), config);
    const snapshot = await weights.loadDensitySnapshot();
    expect(snapshot).toMatchObject({generation: 'g1', degraded: false, edges: {ac: 20}, cells: {'8930e0a845bffff': 4}});
    expect(weights.weight(edge, snapshot)).toBe(15);
    expect(Object.isFrozen(snapshot.edges)).toBe(true);
  });
  test.each<Record<string, string>>([{}, {_generation: 'old', ac: '10000'}])('accepts disabled edge density and calculates free flow despite cell counts', async edges => {
    const weights = new EdgeWeightCalculator(redisHashes({_generation: 'g1', '8930e0a845bffff': '9999'},
      edges, meta({edgeDensity: 'disabled:no-table'})), config);
    const snapshot = await weights.loadDensitySnapshot();
    expect(snapshot).toMatchObject({degraded: true, edgeDensity: 'disabled:no-table', edges: {}});
    expect(weights.weight(edge, snapshot)).toBe(10);
    expect(weights.isBlocked(edge, snapshot)).toBe(false);
  });
  test.each([
    ['old', 'g1', 'enabled'], ['g1', 'old', 'enabled'], ['g1', undefined, 'enabled'], ['old', undefined, 'disabled:no-table'],
  ])('rejects required generation mismatch (%s, %s, %s)', async (cellsGeneration, edgeGeneration, edgeDensity) => {
    const weights = new EdgeWeightCalculator(redisHashes({_generation: cellsGeneration!},
      edgeGeneration ? {_generation: edgeGeneration} : {}, meta({edgeDensity: edgeDensity!})), config);
    await expect(weights.loadDensitySnapshot()).rejects.toMatchObject({code: 'GENERATION_MISMATCH', configurationBlocked: false});
  });
  test('exposes incompatible metadata for readiness instead of calculating', async () => {
    const weights = new EdgeWeightCalculator(redisHashes({_generation: 'g1'}, {_generation: 'g1'}, meta({h3Resolution: '8'})), config);
    await expect(weights.loadDensitySnapshot()).rejects.toMatchObject({code: 'H3_RESOLUTION_MISMATCH', configurationBlocked: true});
    expect(() => weights.weight(edge, {...density(), h3Resolution: 8})).toThrow();
  });
  test.each(['-1', 'NaN', '', 'Infinity'])('rejects invalid enabled edge density %s', async value => {
    const weights = new EdgeWeightCalculator(redisHashes({_generation: 'g1'}, {_generation: 'g1', ac: value}, meta()), config);
    await expect(weights.loadDensitySnapshot()).rejects.toMatchObject({code: 'INVALID_SNAPSHOT'});
  });
});
