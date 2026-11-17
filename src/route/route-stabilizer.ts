export interface RouteBaseline {remainingSeconds: number | null; createdAt: string}

/** Both inclusive thresholds apply: at least 15% faster and five seconds since acceptance. */
export class RouteStabilizer {
  shouldReplace(previous: RouteBaseline | null, candidateSeconds: number, nowMs: number): boolean {
    if (!Number.isFinite(candidateSeconds) || candidateSeconds < 0 || !Number.isFinite(nowMs)) return false;
    if (!previous) return true;
    // A disconnected, blocked, or obsolete suffix has no usable baseline from the current origin.
    return nowMs - Date.parse(previous.createdAt) >= 5000 && (previous.remainingSeconds === null
      || (previous.remainingSeconds > 0 && candidateSeconds <= previous.remainingSeconds * 0.85));
  }
}
