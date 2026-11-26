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
 * with error code 0 and no row with another code. A cell the reply does not name is not accepted,
 * including a v10 answer for a topic id the request did not carry (decoded with a null `topic`).
 * A partition the request repeated and the broker answered with a failed row is not accepted,
 * whichever row comes first and whichever topic entry holds it: the answer does not say which of
 * the repeated offsets the broker kept.
 * @param {{ responses: { topic: string|null, partitions: { partition: number, errorCode: number }[] }[] }} decoded
 * @returns {Map<string|null, Set<number>>} accepted partitions by topic
 */
const acceptedIndex = (decoded) => {
  const index = new Map()
  let failed
  for (const { topic, partitions } of decoded.responses) {
    let accepted = index.get(topic)
    if (!accepted) index.set(topic, (accepted = new Set()))
    for (const { partition, errorCode } of partitions) {
      if (errorCode === 0) {
        accepted.add(partition)
        continue
      }
      failed ??= new Map()
      let refused = failed.get(topic)
      if (!refused) failed.set(topic, (refused = new Set()))
      refused.add(partition)
    }
  }
  if (failed) {
    for (const [topic, refused] of failed) {
      for (const partition of refused) index.get(topic).delete(partition)
    }
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
  /**
   * `offsetCommit`: the caller reads every row of the answer itself, so the broker does not refuse
   * an answer whose rows do not match the request (a partition left out, repeated or added).
   */
  UNCHECKED_ROWS: Symbol('uncheckedRows'),
  acceptedIndex,
}
