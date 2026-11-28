export interface RouteBaseline {remainingSeconds: number | null; createdAt: string}
/** INITIAL, OFF_ROUTE and FASTER replace the accepted route; the others keep it. */
export type RouteVerdict = 'INITIAL' | 'OFF_ROUTE' | 'FASTER' | 'HOLD' | 'NOT_FASTER' | 'INVALID';
const REPLACING: ReadonlySet<RouteVerdict> = new Set(['INITIAL', 'OFF_ROUTE', 'FASTER']);

/** Both inclusive thresholds apply: at least 15% faster and five seconds since acceptance. */
export class RouteStabilizer {
  verdict(previous: RouteBaseline | null, candidateSeconds: number, nowMs: number): RouteVerdict {
    if (!Number.isFinite(candidateSeconds) || candidateSeconds < 0 || !Number.isFinite(nowMs)) return 'INVALID';
    if (!previous) return 'INITIAL';
    // A disconnected, blocked, or obsolete suffix has no usable baseline from the current origin.
    if (previous.remainingSeconds === null) return 'OFF_ROUTE';
    if (nowMs - Date.parse(previous.createdAt) < 5000) return 'HOLD';
    return previous.remainingSeconds > 0 && candidateSeconds <= previous.remainingSeconds * 0.85 ? 'FASTER' : 'NOT_FASTER';
  }

  shouldReplace(previous: RouteBaseline | null, candidateSeconds: number, nowMs: number): boolean {
    return REPLACING.has(this.verdict(previous, candidateSeconds, nowMs));
  }
}
