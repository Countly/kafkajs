/** Case two: longer than the blocking admin commands, so a long poll spans a move. */
const MOVE_WAIT_MS = 5000
/** One round trip and the round that follows it, ms: the allowance of the idle and busy bounds. */
const ROUND_ALLOWANCE_MS = 1000

/**
 * How the broker of an unhinted move goes down. A clean stop runs a controlled shutdown that moves
 * the leaders first, so for about two seconds the old leader still answers and names the new one;
 * only a kill leaves the client without a hint.
 */
const UNHINTED_OUTAGE = 'kill'

/**
 * @param {object} measured what `Episode#measure` returned
 * @returns {string[]} how the move breaks its bound; empty when it meets it. A busy move needs a
 *   long poll written at or before the route and answered after it; none fails the arm.
 */
const moveProblems = (measured) => {
  const { kind, hinted, newNode, routeAt, nextAt, priorFetches, hint, anyHint } = measured
  const { pollWriteAt, pollDoneAt } = measured
  const problems = []
  if (hinted && hint?.leaderId !== newNode) problems.push('the old leader named no new leader')
  if (!hinted && anyHint) problems.push('a hint was present')
  if (!hinted && !measured.refreshed) problems.push('no metadata refresh before the route')
  if (kind === 'idle') {
    if (priorFetches !== 0) problems.push('the new node had a Fetch before the move')
    if (nextAt - routeAt > ROUND_ALLOWANCE_MS)
      problems.push('the idle route took longer than a round')
  } else {
    if (pollWriteAt == null || pollDoneAt == null) {
      problems.push('the new node had no long poll outstanding')
    } else {
      if (!(pollWriteAt <= routeAt && routeAt < pollDoneAt))
        problems.push('the long poll was not outstanding at the route')
      if (nextAt < pollDoneAt) problems.push('the route started before the long poll ended')
      if (nextAt - pollDoneAt > ROUND_ALLOWANCE_MS)
        problems.push('the route waited after the long poll')
    }
    if (nextAt - routeAt > MOVE_WAIT_MS + ROUND_ALLOWANCE_MS) {
      problems.push('the busy route took longer than the wait and a round')
    }
  }
  return problems
}

/**
 * Reads through DescribeTopicPartitions: a plain `fetchTopicMetadata` waits for a missing leader
 * and rejects with `KafkaJSTimeout` after 10 s, so it can never report one.
 * @param {{ fetchTopicMetadata: Function }} admin a connected admin client
 * @param {string} topic a topic of one partition
 * @returns {Promise<number>} the leader of its partition 0, -1 when it has none
 */
const partitionLeader = async (admin, topic) => {
  const { topics } = await admin.fetchTopicMetadata({
    topics: [topic],
    useDescribeTopicPartitions: true,
  })
  return topics[0].partitions[0].leader
}

module.exports = {
  MOVE_WAIT_MS,
  ROUND_ALLOWANCE_MS,
  UNHINTED_OUTAGE,
  moveProblems,
  partitionLeader,
}
