import {createServer, type Server} from 'node:http';
export interface HealthStatus {ready: boolean; reasons: string[]; degraded: string[]}
export class HealthServer {
  private server?: Server;
  url = '';
  constructor(private readonly port: number, private readonly readiness: () => Promise<HealthStatus>) {}
  async start(): Promise<void> {
    this.server = createServer((request, response) => {
      const send = (status: number, body: unknown) => {
        response.writeHead(status, {'Content-Type': 'application/json'}); response.end(JSON.stringify(body));
      };
      if (request.url === '/livez') {send(200, {live: true}); return;}
      if (request.url !== '/readyz') {send(404, {error: 'not found'}); return;}
      void this.readiness().then(state => send(state.ready ? 200 : 503, state))
        .catch(() => send(503, {ready: false, reasons: ['health check failed']}));
    });
    await new Promise<void>((resolve, reject) => {
      this.server!.once('error', reject);
      this.server!.listen(this.port, '0.0.0.0', () => {this.server!.off('error', reject); resolve();});
    });
    const address = this.server.address();
    if (address && typeof address !== 'string') this.url = `http://127.0.0.1:${address.port}`;
  }
  async stop(): Promise<void> {
    if (!this.server?.listening) return;
    await new Promise<void>((resolve, reject) => this.server!.close(error => error ? reject(error) : resolve()));
  }
}
