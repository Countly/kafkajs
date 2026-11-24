/**
 * Option keys of `Broker` requests meant only for this package's own modules. The symbols are not
 * exported from the package entry point, so a caller cannot name them and no string-keyed option
 * reaches them. They are not a capability: code handed a broker (a custom partition assigner, for
 * one) can still answer a symbol-keyed read of the options it passes, for example from a Proxy,
 * and so run its own callback where the internal one would run. Nothing relies on them for
 * isolation; they keep internal options apart from the public ones.
 */
/**
 * The cells a decoded OffsetCommit reply accepted, indexed once: the topics and partitions it names
 * with error code 0. A cell the reply does not name is not accepted, including a v10 answer for a
 * topic id the request did not carry (decoded with a null `topic`).
 * @param {{ responses: { topic: string|null, partitions: { partition: number, errorCode: number }[] }[] }} decoded
 * @returns {Map<string|null, Set<number>>} accepted partitions by topic
 */
const acceptedIndex = (decoded) => {
  const index = new Map()
  for (const { topic, partitions } of decoded.responses) {
    let accepted = index.get(topic)
    if (!accepted) index.set(topic, (accepted = new Set()))
    for (const cell of partitions) if (cell.errorCode === 0) accepted.add(cell.partition)
  }
  return index
}

module.exports = {
  /** Admin ownership of InitProducerId and LeaveGroup. */
  ADMIN_MUTATION: Symbol('adminMutation'),
  /** `offsetCommit` and `endTxn`: called once, immediately before the request is written to the socket. */
  ON_WRITE: Symbol('onWrite'),
  /**
   * `offsetCommit`: called once with the decoded reply, before a caller refusal, the throttle or
   * parse can end the request, so what the broker accepted is seen even when the caller is not
   * answered. Read it with `acceptedIndex`.
   */
  ON_DECODED: Symbol('onDecoded'),
  acceptedIndex,
}
