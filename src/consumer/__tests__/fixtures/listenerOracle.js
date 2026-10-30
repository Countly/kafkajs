/**
 * The independent oracle of the awaited-listener live scenario. It judges one finished transcript
 * of what the run observed; it reads no client, broker or hook, so a transcript built by hand is
 * judged exactly as a recorded one is.
 *
 * A record is identified by its `(topic, partition, value)`; the offset and the incarnation are
 * further checks of the same record, never its identity. A transcript carries:
 * - `topic`, `incarnation`: the topic name and UUID of the run
 * - `produced`: `{ topic, incarnation, partition, offset, value }` for every acknowledged record
 * - `deliveries`: `{ topic, incarnation, partition, offset, value, member, ordinal }` for every
 *   handler entry, taken from the delivered message itself
 * - `hooks`: `{ kind: 'assigned'|'revoked'|'lost', stage: 'initial'|'round'|'close', member,
 *   enteredOrdinal, returnedOrdinal, partitions: number[] }` for every callback invocation
 * - `fetches`: `{ member, partition, ordinal }` for the first Fetch of each acquisition
 * - `episodes`: one per completed revoke or close: `{ id, member, memberId, partition, incarnation,
 *   saved, processed, brokerObserved, eligible, expected, correlation }`; an `expected` episode is
 *   one the pre-response facts said must commit, so it must also be eligible
 * - `seeks`: `{ partition, from, to }` for every declared assigned-hook seek
 * - `replays`: `{ partition, from, to }` for every offset interval a crash or rejoin may repeat
 * - `members`: the member labels that joined
 * - `requireExpected`: optional; when true, at least one episode must be `expected`
 */

/** Fields a correlated commit response must carry, each non-null. */
const CORRELATION = [
  'apiKey',
  'correlationId',
  'groupId',
  'memberId',
  'topic',
  'incarnation',
  'partition',
]

const key = ({ topic, partition, value }) => JSON.stringify([topic, partition, value])

/**
 * @param {{ from: number, to: number }[]} intervals half-open offset intervals
 * @param {number} offset
 * @returns {boolean} whether any interval holds the offset
 */
const within = (intervals, offset) =>
  intervals.some(({ from, to }) => offset >= from && offset < to)

/**
 * Judges a transcript.
 * @param {object} transcript see the file header
 * @returns {string[]} one line per violation, empty when the transcript is acceptable
 */
const judge = (transcript) => {
  const {
    topic,
    incarnation,
    produced,
    deliveries,
    hooks,
    fetches,
    episodes,
    seeks,
    replays,
    members,
    requireExpected,
  } = transcript
  const found = []

  const corpus = new Map(produced.map((record) => [key(record), record]))
  if (corpus.size !== produced.length) found.push('the acknowledged corpus holds a duplicate key')
  for (const record of produced) {
    if (record.topic !== topic) found.push(`corpus ${key(record)} is of another topic`)
    if (record.incarnation !== incarnation)
      found.push(`corpus ${key(record)} is of another incarnation`)
  }
  const seen = new Map()
  for (const delivery of deliveries) {
    const id = key(delivery)
    if (delivery.topic !== topic) {
      found.push(`delivery ${id} is of another topic`)
      continue
    }
    if (delivery.incarnation !== incarnation) {
      found.push(`delivery ${id} is of another incarnation`)
      continue
    }
    const record = corpus.get(id)
    if (!record) {
      found.push(`delivery ${id} is not in the corpus`)
      continue
    }
    if (record.offset !== delivery.offset) {
      found.push(`delivery ${id} is at offset ${delivery.offset}, acknowledged at ${record.offset}`)
    }
    const entry = seen.get(id) ?? { count: 0, partition: delivery.partition, offset: record.offset }
    entry.count += 1
    seen.set(id, entry)
  }
  for (const id of corpus.keys()) {
    if (!seen.has(id)) found.push(`corpus record ${id} was never delivered`)
  }

  for (const member of members) {
    const mine = hooks.filter((hook) => hook.member === member)
    if (!mine.some((hook) => hook.kind === 'assigned' && hook.stage === 'initial')) {
      found.push(`${member} never had its initial assigned callback invoked`)
    }
  }
  const closes = hooks.filter((hook) => hook.stage === 'close' && hook.kind !== 'assigned')
  for (const member of members) {
    if (!closes.some((hook) => hook.member === member)) {
      found.push(`${member} never had its graceful revoked callback invoked`)
    }
  }

  for (const hook of hooks.filter(({ kind }) => kind === 'assigned')) {
    if (hook.returnedOrdinal == null) {
      found.push(`${hook.member} assigned callback never returned`)
      continue
    }
    for (const partition of hook.partitions) {
      const early = deliveries.filter(
        (d) =>
          d.member === hook.member &&
          d.partition === partition &&
          d.ordinal > hook.enteredOrdinal &&
          d.ordinal < hook.returnedOrdinal
      )
      const first = fetches.find(
        (f) =>
          f.member === hook.member &&
          f.partition === partition &&
          f.ordinal > hook.enteredOrdinal &&
          f.ordinal < hook.returnedOrdinal
      )
      if (early.length > 0 || first) {
        found.push(
          `${hook.member} partition ${partition} was ${early.length ? 'delivered' : 'fetched'} before its assigned callback returned`
        )
      }
    }
  }
  for (const delivery of deliveries) {
    const owning = hooks.filter(
      (hook) =>
        hook.kind === 'assigned' &&
        hook.member === delivery.member &&
        hook.partitions.includes(delivery.partition) &&
        hook.enteredOrdinal < delivery.ordinal
    )
    if (owning.length === 0) {
      found.push(
        `${delivery.member} partition ${delivery.partition} was delivered with no assigned callback`
      )
    }
  }

  const ids = new Set()
  for (const episode of episodes) {
    if (ids.has(episode.id)) found.push(`episode ${episode.id} is recorded twice`)
    ids.add(episode.id)
    if (episode.incarnation !== incarnation) {
      found.push(`episode ${episode.id} is of another incarnation`)
    }
    if (episode.expected && !episode.eligible) {
      found.push(
        `episode ${episode.id} was expected to commit at close but no held commit followed`
      )
    }
    if (!episode.eligible) continue
    if (episode.brokerObserved == null) {
      found.push(`episode ${episode.id} has no broker-observed offset`)
      continue
    }
    const missing = CORRELATION.filter((field) => episode.correlation?.[field] == null)
    if (missing.length > 0) {
      found.push(`episode ${episode.id} lacks the correlation fields ${missing.join(',')}`)
    } else if (
      episode.correlation.incarnation !== incarnation ||
      episode.correlation.partition !== episode.partition ||
      episode.correlation.memberId !== episode.memberId
    ) {
      found.push(`episode ${episode.id} correlates another episode's commit`)
    }
    if (episode.brokerObserved !== episode.saved) {
      found.push(
        `episode ${episode.id} saved ${episode.saved} but the broker held ${episode.brokerObserved}`
      )
    }
    const declared = seeks.filter(({ partition }) => partition === episode.partition)
    if (
      episode.brokerObserved > episode.processed &&
      !declared.some(({ to }) => to >= episode.brokerObserved)
    ) {
      found.push(
        `episode ${episode.id} broker offset ${episode.brokerObserved} passes the processed ${episode.processed}`
      )
    }
  }

  if (requireExpected && !episodes.some(({ expected }) => expected)) {
    found.push('no close episode was expected to commit, so the final commit was not exercised')
  }

  for (const [id, { count, partition, offset }] of seen) {
    if (count < 2) continue
    const backward = seeks.filter((s) => s.partition === partition && s.to < s.from)
    const allowed = [
      ...backward.map(({ from, to }) => ({ from: to, to: from })),
      ...replays.filter((r) => r.partition === partition),
    ]
    if (!within(allowed, offset)) {
      found.push(`record ${id} was delivered ${count} times outside every declared replay`)
    }
  }
  return found
}

/**
 * @param {object} transcript see the file header
 * @returns {void}
 * @throws {Error} one message naming every violation
 */
const verifyListenerEvidence = (transcript) => {
  const violations = judge(transcript)
  if (violations.length > 0) {
    throw new Error(`The listener evidence is rejected:\n${violations.join('\n')}`)
  }
}

/**
 * A small transcript the oracle accepts: two members, one partition each of one incarnation, a
 * revoke episode and a close episode with matching broker reads. Controls break one field of it.
 * @returns {object} a fresh, mutable transcript
 */
const exampleTranscript = () => {
  const topic = 't'
  const incarnation = 'topic-a'
  const produced = []
  const deliveries = []
  let ordinal = 100
  for (const [partition, member] of [
    [0, 'm1'],
    [1, 'm2'],
  ]) {
    for (let offset = 0; offset < 3; offset++) {
      const value = `value-${partition}-${offset}`
      produced.push({ topic, incarnation, partition, offset, value })
      deliveries.push({ topic, incarnation, partition, offset, value, member, ordinal: ordinal++ })
    }
  }
  const correlation = (partition, memberId) => ({
    apiKey: 8,
    correlationId: 7 + partition,
    groupId: 'g',
    memberId,
    topic,
    incarnation,
    partition,
  })
  return {
    topic,
    incarnation,
    members: ['m1', 'm2'],
    produced,
    deliveries,
    hooks: [
      {
        kind: 'revoked',
        stage: 'initial',
        member: 'm1',
        enteredOrdinal: 1,
        returnedOrdinal: 2,
        partitions: [],
      },
      {
        kind: 'assigned',
        stage: 'initial',
        member: 'm1',
        enteredOrdinal: 3,
        returnedOrdinal: 10,
        partitions: [0],
      },
      {
        kind: 'assigned',
        stage: 'initial',
        member: 'm2',
        enteredOrdinal: 4,
        returnedOrdinal: 11,
        partitions: [1],
      },
      {
        kind: 'revoked',
        stage: 'close',
        member: 'm1',
        enteredOrdinal: 200,
        returnedOrdinal: 201,
        partitions: [0],
      },
      {
        kind: 'revoked',
        stage: 'close',
        member: 'm2',
        enteredOrdinal: 202,
        returnedOrdinal: 203,
        partitions: [1],
      },
    ],
    fetches: [
      { member: 'm1', partition: 0, ordinal: 12 },
      { member: 'm2', partition: 1, ordinal: 13 },
    ],
    episodes: [
      {
        id: 'm1-close-0',
        member: 'm1',
        memberId: 'id-m1',
        partition: 0,
        incarnation,
        saved: 3,
        processed: 3,
        brokerObserved: 3,
        eligible: true,
        correlation: correlation(0, 'id-m1'),
      },
      {
        id: 'm2-close-1',
        member: 'm2',
        memberId: 'id-m2',
        partition: 1,
        incarnation,
        saved: 3,
        processed: 3,
        brokerObserved: 3,
        eligible: true,
        expected: true,
        correlation: correlation(1, 'id-m2'),
      },
    ],
    seeks: [],
    replays: [],
    requireExpected: true,
  }
}

module.exports = { judge, verifyListenerEvidence, exampleTranscript, CORRELATION }
