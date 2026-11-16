import type Redis from 'ioredis';
import type {WorkerConfig} from '../config/config';
import {ContractError, type WorkerEvent} from '../domain/contracts';
import type {ProcessingOutcome} from '../domain/types';
import {ConfigurationBlockedError, decodeInput, ExpiredEventError} from './codecs';
import {deadLetter} from './dlq';
import {AwaitingPredecessorError} from './errors';

export type StreamHandler = (event: WorkerEvent) => Promise<ProcessingOutcome | void>;
type Entry = [id: string, fields: string[]];
type ConsumerConfig = Pick<WorkerConfig, 'consumerName' | 'readCount' | 'blockMs' | 'pendingMinIdleMs' |
  'maxDeliveries' | 'dlqStream' | 'dlqMaxlen' | 'shutdownDrainTimeoutMs' | 'h3Resolution' | 'inactiveTtlMs'>;

/** One sequential reader per stream/group; only the dedicated duplicate connection blocks. */
export class StreamConsumer {
  private blocking?: Redis;
  private loop?: Promise<void>;
  private running = false;
  private connected = false;
  private readonly blocked = new Set<string>();
  private reclaimCursor = '0-0';
  lastError: unknown;
  constructor(private readonly redis: Redis, readonly stream: string, readonly group: string,
    private readonly config: ConsumerConfig, private readonly handler: StreamHandler) {}
  get configurationBlocked(): boolean { return this.blocked.size > 0; }
  get ready(): boolean { return this.running && this.connected && !this.configurationBlocked; }

  async start(): Promise<void> {
    if (this.running) return;
    try { await this.redis.xgroup('CREATE', this.stream, this.group, '0', 'MKSTREAM'); }
    catch (error) { if (!(error instanceof Error) || !error.message.includes('BUSYGROUP')) throw error; }
    this.blocking = this.redis.duplicate({disconnectTimeout: 0});
    // ioredis emits connection errors independently of rejected commands.
    this.blocking.on('error', (error: Error) => { this.lastError = error; this.connected = false; });
    this.running = true;
    this.connected = true;
    this.loop = this.run();
  }

  async stop(): Promise<void> {
    if (!this.loop) return;
    this.running = false;
    this.connected = false;
    // Disconnect interrupts XREADGROUP without interrupting the command/handler connection.
    const disconnected = this.blocking?.status === 'end' ? Promise.resolve() : new Promise<void>(resolve => {
      this.blocking!.once('end', resolve);
      this.blocking!.disconnect();
    });
    let timer: ReturnType<typeof setTimeout> | undefined;
    await Promise.race([Promise.all([this.loop, disconnected]), new Promise<void>(resolve => {
      timer = setTimeout(resolve, this.config.shutdownDrainTimeoutMs);
    })]);
    if (timer) clearTimeout(timer);
    // A handler beyond the drain deadline may finish and ACK later; its entry stays pending meanwhile.
  }

  private async run(): Promise<void> {
    while (this.running) {
      try {
        const claimed = await this.redis.xautoclaim(this.stream, this.group, this.config.consumerName,
          this.config.pendingMinIdleMs, this.reclaimCursor, 'COUNT', this.config.readCount) as [string, Entry[], string[]?];
        this.reclaimCursor = claimed[0];
        for (const id of claimed[2] ?? []) this.blocked.delete(id);
        await this.reconcileBlocked();
        for (const entry of claimed[1]) { if (!this.running) break; await this.process(entry); }
        if (!this.running) break;
        const records = await this.blocking!.xreadgroup('GROUP', this.group, this.config.consumerName,
          'COUNT', this.config.readCount, 'BLOCK', this.config.blockMs, 'STREAMS', this.stream, '>') as [string, Entry[]][] | null;
        this.connected = true;
        for (const [, entries] of records ?? []) {
          for (const entry of entries) { if (!this.running) break; await this.process(entry); }
        }
      } catch (error) {
        if (!this.running) break;
        this.lastError = error; this.connected = false;
        await new Promise(resolve => setTimeout(resolve, Math.min(this.config.blockMs, 1000)));
      }
    }
  }

  private async reconcileBlocked(): Promise<void> {
    if (!this.blocked.size) return;
    const ids = [...this.blocked];
    const pipeline = this.redis.pipeline();
    for (const id of ids) pipeline.xpending(this.stream, this.group, id, id, 1);
    const results = await pipeline.exec();
    if (!results) throw new Error('Blocked-entry reconciliation aborted');
    for (const [index, [error, entries]] of results.entries()) {
      if (error) throw error;
      if (!(entries as unknown[]).length) this.blocked.delete(ids[index]!);
    }
  }

  private async process([id, fields]: Entry): Promise<void> {
    try {
      const event = decodeInput(fields, this.config);
      const outcome = await this.handler(event);
      if (outcome === 'CONFIGURATION_BLOCKED') throw new ConfigurationBlockedError('Handler configuration mismatch');
      if (outcome === 'TRANSIENT_ERROR') throw new Error('Transient handler failure');
      // Validation failures must be thrown as ContractError; handlers cannot silently request an ACKed DLQ.
      if (outcome === 'DLQ') throw new ContractError('Handler rejected event schema');
      await this.redis.xack(this.stream, this.group, id);
      this.blocked.delete(id);
    } catch (error) {
      this.lastError = error;
      if (error instanceof ConfigurationBlockedError) { this.blocked.add(id); return; }
      this.blocked.delete(id);
      if (error instanceof AwaitingPredecessorError) return;
      if (error instanceof ContractError || error instanceof ExpiredEventError) {
        await deadLetter(this.redis, this.config, this.stream, this.group, id, fields,
          error instanceof ExpiredEventError ? 'EXPIRED_EVENT' : 'INVALID_EVENT', error);
        return;
      }
      const pending = await this.redis.xpending(this.stream, this.group, id, id, 1) as [string, string, number, number][];
      if ((pending[0]?.[3] ?? 0) >= this.config.maxDeliveries) {
        await deadLetter(this.redis, this.config, this.stream, this.group, id, fields, 'MAX_DELIVERIES', error);
      }
    }
  }
}
