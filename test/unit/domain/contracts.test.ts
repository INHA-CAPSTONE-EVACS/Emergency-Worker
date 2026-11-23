import {parseEventEnvelope, parseStreamEvent, validateEventEnvelope} from '../../../src/domain/contracts';

const envelope = (eventType: string, payload: unknown) => ({eventId: 'evt-1', eventType,
  schemaVersion: 1, occurredAt: '2026-11-16T12:00:00.000Z', producer: 'emergency-worker', payload});
const requested = {alertId: 'ALT-001', emergencyId: 'EMG-001', dispatchId: 'DSP-001',
  emergencyVehicleId: 'AMB-001', targetVehicleId: 'VEH-001', approachDirection: 'REAR',
  estimatedArrivalSeconds: 15, recommendedAction: 'YIELD_WHEN_SAFE', reason: 'SAME_ROUTE_AHEAD',
  decisionVersion: 3, expiresAt: '2026-11-16T12:00:20.000Z'};
const clear = {alertId: 'ALT-001', dispatchId: 'DSP-001', targetVehicleId: 'VEH-001', reason: 'EMERGENCY_PASSED'};
const point = {latitude: 37.451, longitude: 126.654};
const route = {dispatchId: 'DSP-001', targetVehicleId: 'AMB-001', routeVersion: 3,
  destination: point, waypoints: [point], congestedSegments: ['edge-1'], estimatedTravelSeconds: 180};

describe('canonical event codecs', () => {
  test('consumes the Location Worker v1 envelope from event, preserving first-seen null', () => {
    const event = {...envelope('VEHICLE_STATE_UPDATED', {vehicleId: 'VEH-001', vehicleType: 'NORMAL',
      h3CellId: '8930e0a845bffff', previousH3CellId: null, stateVersion: 1,
      measuredAt: '2026-11-16T11:59:59.000Z'}), producer: 'location-worker'};
    expect(parseStreamEvent(['event', JSON.stringify(event)])).toEqual(event);
    expect(() => parseStreamEvent(['payload', JSON.stringify(event)])).toThrow();
  });
  test('leaves valid incompatible H3 resolutions for the configuration-blocked consumer policy', () => {
    expect(() => parseEventEnvelope(envelope('VEHICLE_STATE_UPDATED', {vehicleId: 'v', vehicleType: 'NORMAL',
      h3CellId: '8830e0a845fffff', previousH3CellId: null, stateVersion: 2,
      measuredAt: '2026-11-16T12:00:00.000Z'}))).not.toThrow();
  });
  test.each(['h3CellId', 'previousH3CellId'])('rejects malformed H3 in %s directly', field => {
    expect(() => parseEventEnvelope(envelope('VEHICLE_STATE_UPDATED', {vehicleId: 'v', vehicleType: 'NORMAL',
      h3CellId: '8930e0a845bffff', previousH3CellId: null, stateVersion: 2,
      measuredAt: '2026-11-16T12:00:00.000Z', [field]: 'not-an-h3-cell'}))).toThrow();
  });
  test.each(Object.keys(requested))('rejects requested alert missing %s', field => {
    const payload: Record<string, unknown> = {...requested}; delete payload[field];
    expect(() => parseEventEnvelope(envelope('YIELD_ALERT_REQUESTED', payload))).toThrow();
  });
  test.each(['approachDirection', 'recommendedAction', 'reason'])('rejects unknown %s', field => {
    expect(() => parseEventEnvelope(envelope('YIELD_ALERT_REQUESTED', {...requested, [field]: 'UNKNOWN'}))).toThrow();
  });
  test.each(['REAR', 'REAR_LEFT', 'REAR_RIGHT', 'LEFT', 'RIGHT'])('accepts direction %s', approachDirection => {
    expect(validateEventEnvelope(envelope('YIELD_ALERT_REQUESTED', {...requested, approachDirection}))).toBe(true);
  });
  test.each(['YIELD_WHEN_SAFE', 'MOVE_RIGHT_WHEN_SAFE', 'CLEAR_INTERSECTION_FIRST'])('accepts action %s', recommendedAction => {
    expect(validateEventEnvelope(envelope('YIELD_ALERT_REQUESTED', {...requested, recommendedAction}))).toBe(true);
  });
  test.each(['SAME_ROUTE_AHEAD', 'MOVING_SLOW', 'BLOCK_SECTION'])('accepts reason %s', reason => {
    expect(validateEventEnvelope(envelope('YIELD_ALERT_REQUESTED', {...requested, reason}))).toBe(true);
  });
  test('keeps Gateway-only fields out of canonical requested payloads', () => {
    expect(parseEventEnvelope(envelope('YIELD_ALERT_REQUESTED', requested)).payload).toEqual(requested);
    expect(() => parseEventEnvelope(envelope('YIELD_ALERT_REQUESTED', {...requested, vehicleId: 'v', issuedAt: 'x'}))).toThrow();
  });
  test.each(Object.keys(clear))('rejects clear missing %s', field => {
    const payload: Record<string, unknown> = {...clear}; delete payload[field];
    expect(() => parseEventEnvelope(envelope('YIELD_ALERT_CLEAR_REQUESTED', payload))).toThrow();
  });
  test.each(['EMERGENCY_PASSED', 'ROUTE_CHANGED', 'NO_LONGER_RELEVANT', 'EMERGENCY_COMPLETED'])('accepts clear reason %s', reason => {
    expect(validateEventEnvelope(envelope('YIELD_ALERT_CLEAR_REQUESTED', {...clear, reason}))).toBe(true);
  });
  test('rejects alert reasons used as clear reasons', () => {
    expect(validateEventEnvelope(envelope('YIELD_ALERT_CLEAR_REQUESTED', {...clear, reason: 'MOVING_SLOW'}))).toBe(false);
  });
  test('validates route destination, waypoints, duration and version', () => {
    expect(parseEventEnvelope(envelope('ROUTE_UPDATE_REQUESTED', route)).payload).toEqual(route);
    for (const bad of [{...route, routeVersion: 0}, {...route, waypoints: [{latitude: 91, longitude: 1}]},
      {...route, estimatedTravelSeconds: -1}, {...route, destination: null}]) {
      expect(validateEventEnvelope(envelope('ROUTE_UPDATE_REQUESTED', bad))).toBe(false);
    }
  });
  test('accepts typed emergency events and legacy ARRIVED without normalizing at the codec boundary', () => {
    for (const [eventType, payload] of [
      ['EMERGENCY_CREATED', {emergencyId: 'e', emergencyType: 'MEDICAL', location: point, status: 'REGISTERED'}],
      ['EMERGENCY_DISPATCHED', {emergencyId: 'e', dispatchId: 'd', vehicleId: 'v', origin: point, destination: point}],
      ['EMERGENCY_STATUS_CHANGED', {emergencyId: 'e', dispatchId: 'd', previousStatus: 'DISPATCHED', currentStatus: 'ARRIVED', destination: point}],
    ]) expect(validateEventEnvelope(envelope(eventType as string, payload))).toBe(true);
  });
  test.each([null, [], {}, envelope('UNKNOWN', {}), {...envelope('YIELD_ALERT_REQUESTED', requested), schemaVersion: 2},
    {...envelope('YIELD_ALERT_REQUESTED', requested), occurredAt: 'not-a-date'},
    envelope('YIELD_ALERT_REQUESTED', {...requested, expiresAt: '2026-02-30T12:00:00.000Z'}),
    envelope('YIELD_ALERT_REQUESTED', {...requested, decisionVersion: 0}),
    envelope('YIELD_ALERT_REQUESTED', {...requested, estimatedArrivalSeconds: 1.5}),
    envelope('YIELD_ALERT_REQUESTED', {...requested, targetVehicleId: ''})])('rejects malformed input %#', value => {
    expect(validateEventEnvelope(value)).toBe(false);
  });
  test.each([[], ['event'], ['event', '{'], ['event', '{}', 'event', '{}']].map(fields => [fields]))('rejects malformed stream fields %#', fields => {
    expect(() => parseStreamEvent(fields)).toThrow();
  });
});

describe('Control API Server emergency events', () => {
  // EVACS-API-Server BrokerEventPublisher field layout; Java Instant.toString() keeps microseconds.
  const flat = (eventType: string, payload: unknown, overrides: Record<string, string> = {}) => Object.entries({
    eventId: '1efd7945-de90-44c9-9b09-c11128c4b40c', eventType, schemaVersion: '1',
    aggregateType: 'EMERGENCY', aggregateId: 'EMG-001', occurredAt: '2026-11-23T06:43:25.308869Z',
    producer: 'control-api', payload: JSON.stringify(payload), ...overrides}).flat();
  const created = {emergencyId: 'EMG-001', vehicleId: 'AMB-001', emergencyType: 'MEDICAL', status: 'ACTIVE',
    startedAt: '2026-11-23T06:43:25.293115713Z', origin: point, destination: point,
    route: [{sequence: 0, ...point, h3Index: '8930e0a845bffff'}]};

  test('reads the split envelope and normalizes the timestamp to milliseconds', () => {
    expect(parseStreamEvent(flat('EMERGENCY_CREATED', created))).toEqual({eventId: '1efd7945-de90-44c9-9b09-c11128c4b40c',
      eventType: 'EMERGENCY_CREATED', schemaVersion: 1, occurredAt: '2026-11-23T06:43:25.308Z',
      producer: 'control-api', payload: created});
  });
  test('accepts EMERGENCY_ENDED with a null endedAt', () => {
    expect(parseStreamEvent(flat('EMERGENCY_ENDED', {emergencyId: 'EMG-001', vehicleId: 'AMB-001',
      status: 'ENDED', endedAt: null})).eventType).toBe('EMERGENCY_ENDED');
  });
  test.each([['schemaVersion', '2'], ['producer', ''], ['occurredAt', 'yesterday'], ['payload', '{']])(
    'rejects %s=%p', (field, value) => {
      expect(() => parseStreamEvent(flat('EMERGENCY_CREATED', created, {[field]: value}))).toThrow();
    });
  test('requires a destination only when the emergency names a vehicle', () => {
    const {origin: _o, destination: _d, route: _r, ...bare} = created;
    expect(() => parseStreamEvent(flat('EMERGENCY_CREATED', bare))).toThrow();
    const {vehicleId: _v, ...registration} = bare;
    expect(() => parseStreamEvent(flat('EMERGENCY_CREATED', registration))).not.toThrow();
    expect(() => parseStreamEvent(flat('EMERGENCY_CREATED', {...bare, route: [{sequence: 0, ...point}]}))).not.toThrow();
  });
});
