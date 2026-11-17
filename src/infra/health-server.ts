import {createServer, type Server} from 'node:http';
import type {Socket} from 'node:net';
export interface HealthStatus {ready: boolean; reasons: string[]; degraded: string[]}
export class HealthServer {
  private server?: Server;
  private readonly sockets = new Set<Socket>();
  private starting?: Promise<void>;
  private stopping?: Promise<void>;
  private stopRequested = false;
  url = '';
  constructor(private readonly port: number, private readonly readiness: () => Promise<HealthStatus>) {}
  start(): Promise<void> {
    if (this.stopRequested) return Promise.resolve();
    return this.starting ??= this.listen();
  }
  private async listen(): Promise<void> {
    this.server = createServer((request, response) => {
      const send = (status: number, body: unknown) => {
        response.writeHead(status, {'Content-Type': 'application/json'}); response.end(JSON.stringify(body));
      };
      if (request.url === '/livez') {send(200, {live: true}); return;}
      if (request.url !== '/readyz') {send(404, {error: 'not found'}); return;}
      void this.readiness().then(state => send(state.ready ? 200 : 503, state))
        .catch(() => send(503, {ready: false, reasons: ['health check failed']}));
    });
    this.server.on('connection', socket => {
      this.sockets.add(socket);
      socket.once('close', () => this.sockets.delete(socket));
      if (this.stopRequested) socket.destroy();
    });
    await new Promise<void>((resolve, reject) => {
      this.server!.once('error', reject);
      this.server!.listen(this.port, '0.0.0.0', () => {this.server!.off('error', reject); resolve();});
    });
    const address = this.server.address();
    if (address && typeof address !== 'string') this.url = `http://127.0.0.1:${address.port}`;
    if (this.stopRequested) this.forceClose();
  }
  stop(): Promise<void> {
    this.stopRequested = true;
    return this.stopping ??= this.close();
  }
  private async close(): Promise<void> {
    await this.starting?.catch(() => {});
    if (!this.server?.listening) return;
    await new Promise<void>((resolve, reject) => this.server!.close(error => error ? reject(error) : resolve()));
  }
  /** Called at the worker drain deadline, including sockets with incomplete HTTP headers. */
  forceClose(): void {
    this.stopRequested = true;
    if (this.server?.listening) this.server.close();
    for (const socket of this.sockets) socket.destroy();
  }
}
