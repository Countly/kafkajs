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
 *   long poll written at or before the loss of the old leader and answered after it; none fails
 *   the arm. The loss, not the route, is the anchor because a client that lost a leader without a
 *   hint refreshes its metadata only once the fetch round it is in has ended, so the route comes
 *   at or after the poll's end by design. The wait after the poll is counted from the later of
 *   the poll's end and the route, so the cluster's own election time is not charged to the client.
 *
 *   A move that keeps the consumer's node set keeps the strict bounds. One that changes it makes
 *   the fetch manager stop every fetcher, await its in-flight Fetches and recreate them, so every
 *   destination starts within the wait and a round of the route and within a round of the drain
 *   end (`drainEndAt`, the last in-flight Fetch's completion). No bound grows with a constant.
 */
const moveProblems = (measured) => {
  const { kind, hinted, newNode, errorAt, routeAt, nextAt, priorFetches, hint, anyHint } = measured
  const { pollWriteAt, pollDoneAt, nodeSetChanged, drainEndAt } = measured
  const problems = []
  if (hinted && hint?.leaderId !== newNode) problems.push('the old leader named no new leader')
  if (!hinted && anyHint) problems.push('a hint was present')
  if (!hinted && !measured.refreshed) problems.push('no metadata refresh before the route')
  if (kind === 'idle') {
    if (priorFetches !== 0) problems.push('the new node had a Fetch before the move')
    if (!nodeSetChanged && nextAt - routeAt > ROUND_ALLOWANCE_MS)
      problems.push('the idle route took longer than a round')
  } else {
    if (pollWriteAt == null || pollDoneAt == null) {
      problems.push('the new node had no long poll outstanding')
    } else {
      if (!(pollWriteAt <= errorAt && errorAt < pollDoneAt))
        problems.push('the long poll was not outstanding at the route')
      if (nextAt < pollDoneAt) problems.push('the route started before the long poll ended')
      if (!nodeSetChanged && nextAt - Math.max(pollDoneAt, routeAt) > ROUND_ALLOWANCE_MS)
        problems.push('the route waited after the long poll')
    }
    if (nextAt - routeAt > MOVE_WAIT_MS + ROUND_ALLOWANCE_MS) {
      problems.push('the busy route took longer than the wait and a round')
    }
  }
  if (nodeSetChanged) {
    if (kind === 'idle' && nextAt - routeAt > MOVE_WAIT_MS + ROUND_ALLOWANCE_MS) {
      problems.push('the idle route took longer than the wait and a round')
    }
    if (drainEndAt == null) problems.push('no drain end was measured for the node-set change')
    else if (nextAt - drainEndAt > ROUND_ALLOWANCE_MS) {
      problems.push('the fetchers started more than a round after the drain')
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

/**
 * @param {{ topic: string, partition: number, value: string }} record
 * @returns {string} the identity of a record: where it is and what it says
 */
const identity = ({ topic, partition, value }) => `${topic}|${partition}|${value}`

/**
 * @param {object} evidence the sibling case's facts
 * @param {Array<{ topic: string, partition: number, value: string }>} evidence.outage the healthy
 *   records sent while the stopped partition had no leader
 * @param {Array<{ topic: string, partition: number, value: string, at: number }>} evidence.handled
 * @param {{ leaderlessAt: number, tRestored: number }} evidence.window from when the partition was
 *   seen without a leader to when the stopped broker was back and in sync. It starts at that
 *   observation, not when the stop command returned: a stop takes seconds and the leader is gone
 *   within its first moments.
 * @param {{ joins: number, rebalances: number, crashes: number }} evidence.events the membership
 *   events of the stable window
 * @returns {string[]} how the evidence breaks the case's oracle; empty when it meets it
 */
const siblingProblems = ({ outage, handled, window, events }) => {
  const problems = []
  for (const record of outage) {
    const first = handled.find((h) => identity(h) === identity(record))
    if (!first || first.at < window.leaderlessAt || first.at > window.tRestored) {
      problems.push(`${identity(record)} was not handled while the sibling had no leader`)
    }
  }
  for (const [name, count] of Object.entries(events)) {
    if (count !== 0) problems.push(`${count} ${name} in the stable window`)
  }
  return problems
}

module.exports = {
  MOVE_WAIT_MS,
  ROUND_ALLOWANCE_MS,
  UNHINTED_OUTAGE,
  moveProblems,
  partitionLeader,
  siblingProblems,
}
