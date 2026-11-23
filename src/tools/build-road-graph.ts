import {readFile, rename, writeFile} from 'node:fs/promises';
import {basename, dirname} from 'node:path';
import {RoadGraph} from '../route/road-graph';
import {convertSumoNet} from './sumo-road-graph';

/**
 * Usage: node dist/src/tools/build-road-graph.js <roads.net.xml> <road-graph.json> [h3Resolution]
 * Writes the RoadGraph document for GRAPH_PATH and validates it with the worker's own loader.
 */
async function main(): Promise<void> {
  const [input, output, resolutionArg] = process.argv.slice(2);
  if (!input || !output) throw new Error('Usage: build-road-graph <roads.net.xml> <road-graph.json> [h3Resolution]');
  const h3Resolution = resolutionArg ? Number(resolutionArg) : 9;
  const report = convertSumoNet(await readFile(input, 'utf8'),
    {h3Resolution, source: basename(dirname(input)) || basename(input)});
  RoadGraph.load(report.graph, h3Resolution); // Fails here instead of at worker startup.
  const temporary = `${output}.tmp`;
  await writeFile(temporary, JSON.stringify(report.graph));
  await rename(temporary, output);
  console.log(JSON.stringify({output, version: report.graph.version, nodes: report.graph.nodes.length,
    edges: report.graph.edges.length, skippedEdges: report.skippedEdges,
    largestComponentShare: Number(report.largestComponentShare.toFixed(3))}));
}

main().catch(error => { console.error(error instanceof Error ? error.message : error); process.exitCode = 1; });
