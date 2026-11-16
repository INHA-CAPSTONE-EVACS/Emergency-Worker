/** A valid lifecycle record must wait for earlier state, rather than consume its failure budget. */
export class AwaitingPredecessorError extends Error {
  constructor(message: string) { super(message); this.name = 'AwaitingPredecessorError'; }
}
