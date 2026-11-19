/**
 * Option keys of `Broker` requests that only this package's own modules can set. A symbol defined
 * here is not reachable from a broker instance, so code handed a broker (a custom partition
 * assigner, for one) cannot pass these options.
 */
module.exports = {
  /** `offsetCommit`: called once, immediately before the request is written to the socket. */
  ON_WRITE: Symbol('onWrite'),
}
