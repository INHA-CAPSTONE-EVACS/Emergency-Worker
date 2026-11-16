import type {Coordinates} from '../domain/contracts';

const EARTH_RADIUS_M = 6_371_008.8;
const radians = (degrees: number) => degrees * Math.PI / 180;
const degrees = (angle: number) => angle * 180 / Math.PI;
export const normalizeHeading = (heading: number) => ((heading % 360) + 360) % 360;
export const signedAngle = (from: number, to: number) => ((to - from + 540) % 360 + 360) % 360 - 180;
export const headingDifference = (a: number, b: number) => Math.abs(signedAngle(a, b));

export function distanceM(a: Coordinates, b: Coordinates): number {
  const dLat = radians(b.latitude - a.latitude);
  const dLon = radians(signedAngle(a.longitude, b.longitude));
  const h = Math.sin(dLat / 2) ** 2 + Math.cos(radians(a.latitude)) * Math.cos(radians(b.latitude)) * Math.sin(dLon / 2) ** 2;
  return 2 * EARTH_RADIUS_M * Math.asin(Math.sqrt(Math.min(1, Math.max(0, h))));
}

export function bearingDegrees(a: Coordinates, b: Coordinates): number {
  const latA = radians(a.latitude), latB = radians(b.latitude);
  const lon = radians(signedAngle(a.longitude, b.longitude));
  return normalizeHeading(degrees(Math.atan2(Math.sin(lon) * Math.cos(latB),
    Math.cos(latA) * Math.sin(latB) - Math.sin(latA) * Math.cos(latB) * Math.cos(lon))));
}

export function destination(origin: Coordinates, heading: number, metres: number): Coordinates {
  const lat = radians(origin.latitude), lon = radians(origin.longitude), angle = radians(heading);
  const arc = metres / EARTH_RADIUS_M;
  const latitude = Math.asin(Math.sin(lat) * Math.cos(arc) + Math.cos(lat) * Math.sin(arc) * Math.cos(angle));
  const longitude = lon + Math.atan2(Math.sin(angle) * Math.sin(arc) * Math.cos(lat), Math.cos(arc) - Math.sin(lat) * Math.sin(latitude));
  return {latitude: degrees(latitude), longitude: signedAngle(0, degrees(longitude))};
}

/** Local tangent projection is used only for matching a point to nearby road geometry. */
export function projectOnPolyline(point: Coordinates, geometry: readonly Coordinates[]): {distanceM: number; remainingFraction: number} {
  let nearest = Infinity, remaining = 0;
  const lengths = geometry.slice(1).map((end, i) => distanceM(geometry[i]!, end));
  const total = lengths.reduce((sum, length) => sum + length, 0);
  let traversed = 0;
  for (let i = 1; i < geometry.length; i++) {
    const start = geometry[i - 1]!, end = geometry[i]!;
    const scale = Math.cos(radians(point.latitude));
    const ax = radians(signedAngle(point.longitude, start.longitude)) * EARTH_RADIUS_M * scale;
    const ay = radians(start.latitude - point.latitude) * EARTH_RADIUS_M;
    const bx = radians(signedAngle(point.longitude, end.longitude)) * EARTH_RADIUS_M * scale;
    const by = radians(end.latitude - point.latitude) * EARTH_RADIUS_M;
    const dx = bx - ax, dy = by - ay, lengthSquared = dx * dx + dy * dy;
    const t = lengthSquared === 0 ? 0 : Math.max(0, Math.min(1, -(ax * dx + ay * dy) / lengthSquared));
    const distance = Math.hypot(ax + t * dx, ay + t * dy);
    if (distance < nearest) {
      nearest = distance;
      remaining = total - traversed - t * lengths[i - 1]!;
    }
    traversed += lengths[i - 1]!;
  }
  return {distanceM: nearest, remainingFraction: total === 0 ? 0 : remaining / total};
}
