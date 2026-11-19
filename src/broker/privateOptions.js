/**
 * Option keys of `Broker` requests meant only for this package's own modules. The symbols are not
 * exported from the package entry point, so a caller cannot name them and no string-keyed option
 * reaches them. They are not a capability: code handed a broker (a custom partition assigner, for
 * one) can still answer a symbol-keyed read of the options it passes, for example from a Proxy,
 * and so run its own callback where the internal one would run. Nothing relies on them for
 * isolation; they keep internal options apart from the public ones.
 */
module.exports = {
  /** `offsetCommit`: called once, immediately before the request is written to the socket. */
  ON_WRITE: Symbol('onWrite'),
}
