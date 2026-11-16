import {ConfigError, loadConfig} from '../../../src/config/config';

describe('worker configuration', () => {
  test('defaults keep canonical streams separate from the Gateway projection', () => {
    expect(loadConfig({})).toMatchObject({
      redisUrl: 'redis://localhost:6379', h3Resolution: 9, inactiveTtlMs: 5000,
      stateStream: 'stream:vehicle:state', emergencyStream: 'stream:emergency:event',
      alertStream: 'stream:alert:event', gatewayAlertStream: 'evacs:stream:yield-alert',
      routeIntervalMs: 2000, alertTtlMs: 20000, healthPort: 8082,
      routePersistenceEnabled: true,
    });
  });
  test.each(['999', '3001', '0', 'abc', '2000.5', 'Infinity'])('rejects route cadence %s', value => {
    expect(() => loadConfig({ROUTE_INTERVAL_MS: value})).toThrow(/ROUTE_INTERVAL_MS/);
  });
  test.each(['1000', '3000'])('accepts inclusive route boundary %s', value => {
    expect(loadConfig({ROUTE_INTERVAL_MS: value}).routeIntervalMs).toBe(Number(value));
  });
  test.each(['0', '-1', 'NaN', '1.5', '9007199254740992'])('rejects invalid alert TTL %s', value => {
    expect(() => loadConfig({ALERT_TTL_MS: value})).toThrow(/ALERT_TTL_MS/);
  });
  test('allows explicit streams and positive alert TTL', () => {
    expect(loadConfig({ALERT_TTL_MS: '1', ALERT_STREAM: 'canonical', EVACS_ALERT_STREAM: 'gateway',
      DATABASE_URL: 'postgres://db/evacs', GRAPH_PATH: '/graph.json', ROUTE_PERSISTENCE_ENABLED: 'false'}))
      .toMatchObject({alertTtlMs: 1, alertStream: 'canonical', gatewayAlertStream: 'gateway',
        databaseUrl: 'postgres://db/evacs', graphPath: '/graph.json', routePersistenceEnabled: false});
  });
  test('aggregates invalid configuration rather than partially accepting it', () => {
    try { loadConfig({H3_RESOLUTION: '16', MAX_DELIVERIES: '0', HEALTH_PORT: '65536',
      ROUTE_PERSISTENCE_ENABLED: 'perhaps'}); throw new Error('accepted'); }
    catch (error) {
      expect(error).toBeInstanceOf(ConfigError);
      expect((error as ConfigError).issues.map(issue => issue.variable)).toEqual(
        expect.arrayContaining(['H3_RESOLUTION', 'MAX_DELIVERIES', 'HEALTH_PORT', 'ROUTE_PERSISTENCE_ENABLED']));
    }
  });
});
