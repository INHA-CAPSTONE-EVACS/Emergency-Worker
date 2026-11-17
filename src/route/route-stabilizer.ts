export interface RouteBaseline {estimatedDuration: number; createdAt: string}

/** Both inclusive thresholds apply: at least 15% faster and five seconds since acceptance. */
export class RouteStabilizer {
  shouldReplace(previous: RouteBaseline | null, candidateSeconds: number, nowMs: number): boolean {
    if (!Number.isFinite(candidateSeconds) || candidateSeconds < 0 || !Number.isFinite(nowMs)) return false;
    if (!previous) return true;
    return previous.estimatedDuration > 0 && nowMs - Date.parse(previous.createdAt) >= 5000
      && candidateSeconds <= previous.estimatedDuration * 0.85;
  }
}
