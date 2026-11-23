import {readFile, rename, writeFile} from 'node:fs/promises';
import {basename, dirname} from 'node:path';
import {RoadGraph} from '../route/road-graph';
import {buildEdgeCellTable} from './edge-cell-table';
import {convertSumoNet} from './sumo-road-graph';

/**
 * Usage: node dist/src/tools/build-road-graph.js <roads.net.xml> <road-graph.json> [h3Resolution]
 *          [--edge-cell-table=<edge-cell-table.json>]
 * Writes the RoadGraph document for GRAPH_PATH and validates it with the worker's own loader.
 * With --edge-cell-table it also writes the Location Worker's EDGE_CELL_TABLE_PATH table from the same graph.
 */
async function writeAtomically(path: string, value: unknown): Promise<void> {
  const temporary = `${path}.tmp`;
  await writeFile(temporary, JSON.stringify(value));
  await rename(temporary, path);
}

async function main(): Promise<void> {
  const args = process.argv.slice(2);
  const tableFlag = '--edge-cell-table=';
  const tablePath = args.find(arg => arg.startsWith(tableFlag))?.slice(tableFlag.length);
  const [input, output, resolutionArg] = args.filter(arg => !arg.startsWith('--'));
  if (!input || !output) throw new Error('Usage: build-road-graph <roads.net.xml> <road-graph.json> [h3Resolution]');
  const h3Resolution = resolutionArg ? Number(resolutionArg) : 9;
  const report = convertSumoNet(await readFile(input, 'utf8'),
    {h3Resolution, source: basename(dirname(input)) || basename(input)});
  RoadGraph.load(report.graph, h3Resolution); // Fails here instead of at worker startup.
  await writeAtomically(output, report.graph);
  let edgeCellTable: {output: string; edges: number} | undefined;
  if (tablePath) {
    const table = buildEdgeCellTable(report.graph, new Date().toISOString());
    await writeAtomically(tablePath, table);
    edgeCellTable = {output: tablePath, edges: Object.keys(table.edges).length};
  }
  console.log(JSON.stringify({output, edgeCellTable, version: report.graph.version, nodes: report.graph.nodes.length,
    edges: report.graph.edges.length, skippedEdges: report.skippedEdges,
    largestComponentShare: Number(report.largestComponentShare.toFixed(3))}));
}

main().catch(error => { console.error(error instanceof Error ? error.message : error); process.exitCode = 1; });
