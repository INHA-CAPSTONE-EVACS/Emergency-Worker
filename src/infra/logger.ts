export interface Logger {info(message: string, fields?: Record<string, unknown>): void; error(message: string, error: unknown): void}
export const logger: Logger = {
  info(message, fields = {}) {console.log(JSON.stringify({level: 'info', message, ...fields}));},
  error(message, error) {console.error(JSON.stringify({level: 'error', message,
    error: error instanceof Error ? error.message : String(error)}));},
};
