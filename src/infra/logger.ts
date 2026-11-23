export type LogLevel = 'info' | 'debug';
export interface Logger {
  info(message: string, fields?: Record<string, unknown>): void;
  error(message: string, error: unknown): void;
  /** Optional so injected test loggers need not implement it; emitted only at LOG_LEVEL=debug. */
  debug?(message: string, fields?: Record<string, unknown>): void;
}
export function createLogger(level: LogLevel = 'info'): Logger {
  return {
    info(message, fields = {}) {console.log(JSON.stringify({level: 'info', message, ...fields}));},
    error(message, error) {console.error(JSON.stringify({level: 'error', message,
      error: error instanceof Error ? error.message : String(error)}));},
    debug(message, fields = {}) {if (level === 'debug') console.log(JSON.stringify({level: 'debug', message, ...fields}));},
  };
}
export const logger: Logger = createLogger();
