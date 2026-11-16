import {getResolution} from 'h3-js';
import type {WorkerConfig} from '../config/config';
import {parseStreamEvent, STATE_EVENT_TYPE, type WorkerEvent} from '../domain/contracts';

export class ConfigurationBlockedError extends Error {
  constructor(message: string) { super(message); this.name = 'ConfigurationBlockedError'; }
}
export class ExpiredEventError extends Error {
  constructor() { super('Vehicle state event has expired'); this.name = 'ExpiredEventError'; }
}
export function decodeInput(fields: readonly string[], config: Pick<WorkerConfig, 'h3Resolution' | 'inactiveTtlMs'>,
  now = Date.now()): WorkerEvent {
  const event = parseStreamEvent(fields);
  if (event.eventType === STATE_EVENT_TYPE) {
    const cells = [event.payload.h3CellId, event.payload.previousH3CellId].filter((cell): cell is string => cell !== null);
    if (cells.some(cell => getResolution(cell) !== config.h3Resolution)) {
      throw new ConfigurationBlockedError(`State H3 resolution differs from configured ${config.h3Resolution}`);
    }
    if (now - Date.parse(event.payload.measuredAt) >= config.inactiveTtlMs) throw new ExpiredEventError();
  }
  return event;
}
