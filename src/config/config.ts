import {hostname} from 'node:os';
import * as contracts from '../domain/contracts';

export interface WorkerConfig {
  redisUrl: string; databaseUrl: string | undefined; h3Resolution: number; inactiveTtlMs: number;
  stateStream: string; emergencyStream: string; alertStream: string; gatewayAlertStream: string;
  dlqStream: string; alertOutboxStream: string; vehicleConsumerGroup: string; emergencyConsumerGroup: string;
  consumerName: string; instanceId: string; routeIntervalMs: number; alertTtlMs: number;
  graphPath: string | undefined; routePersistenceEnabled: boolean; readCount: number; blockMs: number;
  pendingMinIdleMs: number; maxDeliveries: number; dlqMaxlen: number; alertStreamMaxlen: number;
  shutdownDrainTimeoutMs: number; healthPort: number; bprAlpha: number; bprBeta: number;
  /** Vehicles per kilometre per lane, matching the Location edge-density snapshot. */
  roadCapacity: number; blockedMultiplier: number;
  /** Control API Server cone key; must contain %s for the emergency ID. */
  coneKeyPattern: string;
}
export interface ConfigIssue {variable: string; message: string}
export class ConfigError extends Error {
  constructor(readonly issues: readonly ConfigIssue[]) {
    super('Invalid configuration: ' + issues.map(issue => `${issue.variable} ${issue.message}`).join('; '));
    this.name = 'ConfigError';
  }
}
type Env = Readonly<Record<string, string | undefined>>;
/** Blank strings use defaults; all invalid settings are reported together. */
export function loadConfig(env: Env = process.env): WorkerConfig {
  const issues: ConfigIssue[] = [];
  const read = (name: string) => env[name]?.trim() || undefined;
  const number = (name: string, fallback: number, min: number, max: number, integral = true) => {
    const raw = read(name);
    if (raw === undefined) return fallback;
    const value = Number(raw);
    if (!Number.isFinite(value) || (integral && (!/^-?\d+$/.test(raw) || !Number.isSafeInteger(value)))
      || value < min || value > max) {
      issues.push({variable: name, message: `must be ${integral ? 'an integer' : 'a number'} between ${min} and ${max}`});
      return fallback;
    }
    return value;
  };
  const positive = (name: string, fallback: number) => number(name, fallback, 1, Number.MAX_SAFE_INTEGER);
  const boolean = (name: string, fallback: boolean) => {
    const raw = read(name);
    if (raw === undefined) return fallback;
    if (raw === 'true' || raw === 'false') return raw === 'true';
    issues.push({variable: name, message: 'must be true or false'}); return fallback;
  };
  const e = contracts.ENV;
  const host = read('HOSTNAME') ?? hostname();
  const config: WorkerConfig = {
    redisUrl: read(e.redisUrl) ?? 'redis://localhost:6379', databaseUrl: read(e.databaseUrl),
    h3Resolution: number(e.h3Resolution, 9, 8, 9), inactiveTtlMs: positive(e.inactiveTtlMs, 5000),
    stateStream: read(e.stateStream) ?? contracts.STATE_STREAM,
    emergencyStream: read(e.emergencyStream) ?? contracts.EMERGENCY_STREAM,
    alertStream: read(e.alertStream) ?? contracts.ALERT_STREAM,
    gatewayAlertStream: read(e.gatewayAlertStream) ?? contracts.GATEWAY_ALERT_STREAM,
    dlqStream: read(e.dlqStream) ?? contracts.DLQ_STREAM,
    alertOutboxStream: read(e.alertOutboxStream) ?? contracts.ALERT_OUTBOX_STREAM,
    vehicleConsumerGroup: read(e.vehicleConsumerGroup) ?? contracts.VEHICLE_CONSUMER_GROUP,
    emergencyConsumerGroup: read(e.emergencyConsumerGroup) ?? contracts.EMERGENCY_CONSUMER_GROUP,
    consumerName: read(e.consumerName) ?? host, instanceId: read(e.instanceId) ?? `${host}-${process.pid}`,
    routeIntervalMs: number(e.routeIntervalMs, contracts.ROUTE_INTERVAL_MS, 1000, 3000),
    alertTtlMs: positive(e.alertTtlMs, contracts.ALERT_TTL_MS), graphPath: read(e.graphPath),
    routePersistenceEnabled: boolean(e.routePersistenceEnabled, true),
    readCount: positive(e.readCount, 100), blockMs: positive(e.blockMs, 1000),
    pendingMinIdleMs: positive(e.pendingMinIdleMs, 10000), maxDeliveries: positive(e.maxDeliveries, 5),
    dlqMaxlen: positive(e.dlqMaxlen, 10000), alertStreamMaxlen: positive(e.alertStreamMaxlen, 100000),
    shutdownDrainTimeoutMs: number(e.shutdownDrainTimeoutMs, 10000, 0, Number.MAX_SAFE_INTEGER),
    healthPort: number(e.healthPort, 8082, 1, 65535),
    bprAlpha: number(e.bprAlpha, 0.15, 0, Number.MAX_VALUE, false),
    bprBeta: number(e.bprBeta, 4, 1, Number.MAX_VALUE, false),
    roadCapacity: positive(e.roadCapacity, 180), blockedMultiplier: number(e.blockedMultiplier, 10, 1, Number.MAX_VALUE, false),
    coneKeyPattern: read(e.coneKeyPattern) ?? contracts.CONE_PROJECTION_KEY_PATTERN,
  };
  if ((config.coneKeyPattern.match(/%s/g) ?? []).length !== 1) {
    issues.push({variable: e.coneKeyPattern, message: 'must contain exactly one %s'});
  }
  if (issues.length) throw new ConfigError(issues);
  return config;
}
