import {loadConfig} from './config/config';
import {logger} from './infra/logger';
import {Worker} from './worker';
async function main(): Promise<void> {
  const worker = new Worker(loadConfig());
  let stopping = false;
  const stop = () => {
    if (stopping) return;
    stopping = true;
    void worker.stop().then(({drained}) => {process.exitCode = drained ? 0 : 1;})
      .catch(error => {logger.error('Shutdown failed', error); process.exitCode = 1;});
  };
  process.once('SIGINT', stop); process.once('SIGTERM', stop);
  await worker.start();
}
void main().catch(error => {logger.error('Startup failed', error); process.exitCode = 1;});
