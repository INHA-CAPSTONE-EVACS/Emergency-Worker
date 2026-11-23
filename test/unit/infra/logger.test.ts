import {createLogger} from '../../../src/infra/logger';

describe('logger', () => {
  let output: jest.SpyInstance;
  beforeEach(() => { output = jest.spyOn(console, 'log').mockImplementation(() => undefined); });
  afterEach(() => output.mockRestore());

  test('info level drops debug lines but keeps info', () => {
    const log = createLogger('info');
    log.debug?.('Route evaluated', {dispatchId: 'd'});
    log.info('Canonical request published', {eventKey: 'k'});
    expect(output.mock.calls.map(([line]) => JSON.parse(line as string))).toEqual([
      {level: 'info', message: 'Canonical request published', eventKey: 'k'}]);
  });

  test('debug level writes debug lines as JSON', () => {
    createLogger('debug').debug?.('Route evaluated', {dispatchId: 'd', outcome: 'KEPT'});
    expect(JSON.parse(output.mock.calls[0]![0] as string)).toEqual({level: 'debug', message: 'Route evaluated',
      dispatchId: 'd', outcome: 'KEPT'});
  });
});
