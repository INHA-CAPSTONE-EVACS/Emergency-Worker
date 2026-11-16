import {latLngToCell} from 'h3-js';
import type {RouteResult, VehicleState} from '../../../src/domain/types';

export function vehicle(overrides: Partial<VehicleState> = {}): VehicleState {
  const state: VehicleState = {vehicleId: 'ambulance', vehicleType: 'EMERGENCY', latitude: 0, longitude: 0,
    speedMps: 20, headingDegrees: 0, h3CellId: '', lastSequence: 1, lastMeasuredAt: 1,
    lastUpdatedAt: '2026-11-16T12:00:00.000Z', stateVersion: 1, status: 'ACTIVE', ...overrides};
  state.h3CellId = latLngToCell(state.latitude, state.longitude, 9);
  return state;
}
export const target = (overrides: Partial<VehicleState> = {}) => vehicle({vehicleId: 'target',
  vehicleType: 'NORMAL', latitude: 0.0018, speedMps: 5, ...overrides});
export const turnRoute: RouteResult = {outcome: 'ROUTE_FOUND', roadSegments: ['current', 'turn'],
  waypoints: [{latitude: 0, longitude: 0}, {latitude: 0.002, longitude: 0}, {latitude: 0.002, longitude: 0.004}],
  congestedSegments: [], distanceM: 667, estimatedTravelSeconds: 34};
export function graphDocument() {
  const nodes = [
    {id: 'start', latitude: 0, longitude: 0}, {id: 'junction', latitude: 0.002, longitude: 0},
    {id: 'end', latitude: 0.002, longitude: 0.004}, {id: 'incoming', latitude: 0.002, longitude: -0.004},
    {id: 'island-a', latitude: 0.0021, longitude: -0.004}, {id: 'island-b', latitude: 0.0021, longitude: 0},
    {id: 'far', latitude: 0.006, longitude: 0.004},
  ];
  const edge = (id: string, from: string, to: string, lengthM: number, points: number[][]) => ({id, from, to,
    lengthM, lanes: 1, speedLimitMps: 20, cells: [...new Set(points.map(([lat, lon]) => latLngToCell(lat!, lon!, 9)))]});
  return {version: 'fixture-v1', h3Resolution: 9, nodes, edges: [
    edge('current', 'start', 'junction', 222, [[0, 0], [0.002, 0]]),
    edge('turn', 'junction', 'end', 445, [[0.002, 0], [0.002, 0.004]]),
    edge('incoming', 'incoming', 'junction', 445, [[0.002, -0.0018], [0.002, 0]]),
    edge('unconnected', 'island-a', 'island-b', 445, [[0.0021, -0.0018]]),
    edge('far', 'end', 'far', 445, [[0.006, 0.004]]),
  ]};
}
