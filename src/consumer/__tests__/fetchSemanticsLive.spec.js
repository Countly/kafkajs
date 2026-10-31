const crypto = require('crypto')
const { secureRandom } = require('testHelpers')
const { committedRecords } = require('../../../testHelpers/rawLog')
const defaultSocketFactory = require('../../network/socketFactory')
const ISOLATION_LEVEL = require('../../protocol/isolationLevel')
const apiKeys = require('../../protocol/requests/apiKeys')
const {
  caseTimeout,
  delay,
  liveCase,
  pollUntil,
} = require('../../producer/__tests__/fixtures/bounded')
const { decodeFetchRequest } = require('./fixtures/fetchHarness')
const { JournalMember, LiveCorpus, STEP_MS, runArm, step } = require('./fixtures/liveGroupRig')

// Gated: it needs the three-broker cluster. Run alone, as the only cluster user:
// `npm run test:e2e:fetch-semantics`.
const GATED = process.env.FETCH_SEMANTICS_E2E === '1'
const describeSemantics = GATED ? describe : describe.skip

const EVIDENCE = 'fetch-semantics'
/** The arm titles, exactly as the guard's required-title list names them. */
const TITLES = {
  isolation: 'committed isolation excludes open and aborted transactions and all control records',
  compacted: 'compacted transactional history preserves visibility rules',
  oversize: 'an oversized first record is delivered once and commits past it',
}
const VISIBILITY_BUDGET_MS = 120000
const OVERSIZE_BUDGET_MS = 90000
const MIN_ISR = [{ name: 'min.insync.replicas', value: '2' }]
const OPEN_RECORDS = 50
const BLOCK_RECORDS = 10
const INVISIBLE_MS = 5000
// No interval: it commits only when a later batch arrives, which strands the tail of a burst.
const AUTO_COMMIT = { autoCommit: true }
const LARGE_BYTES = 400000
const MAX_BYTES = 100000
const MAX_BYTES_PER_PARTITION = 50000
const COMPACTION = [
  { name: 'cleanup.policy', value: 'compact' },
  { name: 'segment.ms', value: '1000' },
  { name: 'min.cleanable.dirty.ratio', value: '0.01' },
  { name: 'max.compaction.lag.ms', value: '1000' },
  { name: 'delete.retention.ms', value: '100' },
]

const REPLAY = {
  log: 'the raw log read through the partition leaders; the broker is the oracle of what exists',
  offsets: 'broker-confirmed committed offsets per group; a skipped range may be committed past',
  handlers:
    'delivered records per group; a record is delivered at least once, never a control record',
}

/**
 * @param {string} stem names a block of records
 * @param {number} count how many records
 * @param {boolean} keyed whether the records carry keys, so a compacted topic holds versions
 * @returns {Array<{key?: string, value: string}>} records with unique values
 */
const block = (stem, count, keyed) =>
  Array.from({ length: count }, (_, index) => ({
    ...(keyed ? { key: `key-${index % 3}` } : {}),
    value: `${stem}-${index}`,
  }))

/**
 * Sends one block of records inside a transaction and ends it as asked.
 * @param {AbortSignal} scope the case scope
 * @param {import('../../../types').Producer} producer a transactional producer
 * @param {string} topic
 * @param {Array<{key?: string, value: string}>} records
 * @param {'commit'|'abort'|'open'} how `open` leaves the transaction pending
 * @returns {Promise<object>} the transaction
 */
const transact = async (scope, producer, topic, records, how) => {
  const transaction = await step(scope, 'The transaction begin', () => producer.transaction())
  await step(scope, 'The transactional send', () =>
    transaction.send({
      topic,
      acks: -1,
      messages: records.map((record) => ({ ...record, partition: 0 })),
    })
  )
  if (how === 'commit') await step(scope, 'The commit', () => transaction.commit())
  if (how === 'abort') await step(scope, 'The abort', () => transaction.abort())
  return transaction
}

/**
 * @param {string} topic
 * @param {Array<{key?: string, value: string}>} records
 * @returns {Array<{topic: string, partition: number, value: string}>} the identities of the records
 */
const identitiesOf = (topic, records) =>
  records.map(({ value }) => ({ topic, partition: 0, value }))

/**
 * @param {Map<number, {records: object[]}>} log a raw log read
 * @returns {{data: Set<string>, controlOffsets: Set<string>}} the identities of every data record
 *   the log holds and the offsets of its control records
 */
const rawSets = (log) => {
  const data = new Set()
  const controlOffsets = new Set()
  for (const [partition, { records }] of log) {
    for (const record of records) {
      if (record.batchContext.isControlBatch) controlOffsets.add(`${partition}|${record.offset}`)
      else data.add(record.value.toString())
    }
  }
  return { data, controlOffsets }
}

/**
 * Two groups over one topic, one per isolation level, each with autoCommit on.
 * @param {object} ctx
 * @param {AbortSignal} ctx.scope
 * @param {import('../../producer/__tests__/fixtures/liveProbe').CleanupStack} ctx.cleanup
 * @param {LiveCorpus} ctx.corpus
 * @returns {{committed: JournalMember, uncommitted: JournalMember}}
 */
const isolationGroups = ({ scope, cleanup, corpus }) => {
  const make = (name, isolationLevel) =>
    new JournalMember({
      scope,
      cleanup,
      corpus,
      name,
      groupId: `${corpus.groupId}-${name}`,
      isolationLevel,
      run: AUTO_COMMIT,
    })
  return {
    committed: make('committed', ISOLATION_LEVEL.READ_COMMITTED),
    uncommitted: make('uncommitted', ISOLATION_LEVEL.READ_UNCOMMITTED),
  }
}

/**
 * Waits until a group's broker-confirmed commit reached the next offset after its last delivery.
 * @param {AbortSignal} scope
 * @param {LiveCorpus} corpus
 * @param {JournalMember} member
 * @returns {Promise<number>} the committed next offset of partition 0
 */
const untilCommittedPast = async (scope, corpus, member) => {
  const last = Math.max(-1, ...member.delivered.map(({ offset }) => Number(offset)))
  let next = -1
  await pollUntil(
    scope,
    STEP_MS * 2,
    async () => {
      next = (await corpus.committed(member.groupId)).get(0) ?? -1
      return next >= last + 1
    },
    { label: 'The broker-confirmed commit', delayMs: 1000 }
  )
  return next
}

/**
 * The delivered values of a member, once each, in delivery order.
 * @param {JournalMember} member
 * @returns {string[]}
 */
const distinctValues = (member) => [...new Set(member.delivered.map(({ value }) => value))]

/**
 * Runs the whole visibility sequence against a topic and checks both isolation levels against the
 * raw log: open, then committed, then aborted blocks between committed blocks.
 * @param {object} ctx
 * @param {AbortSignal} ctx.scope
 * @param {import('../../producer/__tests__/fixtures/liveProbe').CleanupStack} ctx.cleanup
 * @param {object} ctx.observed the evidence the arm accumulates
 * @param {string} ctx.prefix
 * @param {boolean} ctx.keyed
 * @param {Array<{name: string, value: string}>} ctx.configEntries
 * @returns {Promise<LiveCorpus>}
 */
const visibilityEpisode = async ({ scope, cleanup, observed, prefix, keyed, configEntries }) => {
  const transactionalId = `e2e-p9c-${prefix}-tx-${secureRandom()}`
  const corpus = new LiveCorpus({
    scope,
    cleanup,
    prefix,
    configEntries: [...MIN_ISR, ...configEntries],
    partitions: 1,
    producer: { transactionalId, idempotent: true, maxInFlightRequests: 1 },
  })
  await corpus.prepare()
  const { topic } = corpus
  const { committed, uncommitted } = isolationGroups({ scope, cleanup, corpus })
  const groups = [committed, uncommitted]
  const sets = { open: [], aborted: [], committed: [] }
  const send = async (stem, count, how, into) => {
    const records = block(stem, count, keyed)
    sets[into].push(...identitiesOf(topic, records))
    return transact(scope, corpus.producer, topic, records, how)
  }

  const open = await send('open', OPEN_RECORDS, 'open', 'open')
  await committed.start()
  await uncommitted.start()
  await pollUntil(scope, STEP_MS, () => distinctValues(uncommitted).length >= OPEN_RECORDS, {
    label: 'The read-uncommitted sight of the open transaction',
    delayMs: 250,
  })
  await delay(scope, INVISIBLE_MS)
  observed.beforeCommit = {
    uncommitted: distinctValues(uncommitted).length,
    committed: distinctValues(committed).length,
  }
  expect(observed.beforeCommit).toEqual({ uncommitted: OPEN_RECORDS, committed: 0 })

  await step(scope, 'The commit of the open transaction', () => open.commit())
  sets.committed.push(...sets.open)
  sets.open.length = 0
  await send('committed-one', BLOCK_RECORDS, 'commit', 'committed')
  await send('aborted-one', BLOCK_RECORDS, 'abort', 'aborted')
  await send('committed-two', BLOCK_RECORDS, 'commit', 'committed')
  await send('aborted-two', BLOCK_RECORDS, 'abort', 'aborted')
  await send('committed-three', BLOCK_RECORDS, 'commit', 'committed')

  const everything = [...sets.committed, ...sets.aborted]
  const wanted = new Set(sets.committed.map(({ value }) => value))
  const extant = async () => rawSets(await corpus.rawLog(ISOLATION_LEVEL.READ_UNCOMMITTED)).data
  const covered = (member, records, present) =>
    records.every(({ value }) => !present.has(value) || distinctValues(member).includes(value))
  await pollUntil(
    scope,
    STEP_MS * 2,
    async () => {
      const present = await extant()
      return (
        covered(committed, sets.committed, present) && covered(uncommitted, everything, present)
      )
    },
    { label: 'The delivery of every extant record to both isolation levels', delayMs: 2000 }
  )

  const uncommittedLog = await corpus.rawLog(ISOLATION_LEVEL.READ_UNCOMMITTED)
  const committedLog = await corpus.rawLog(ISOLATION_LEVEL.READ_COMMITTED)
  const { data, controlOffsets } = rawSets(uncommittedLog)
  const expectedCommitted = new Set(
    [...committedLog].flatMap(([, log]) =>
      committedRecords(log).map((record) => record.value.toString())
    )
  )
  observed.raw = {
    dataRecords: data.size,
    controlRecords: controlOffsets.size,
    compactedAway: everything.filter((id) => !data.has(id.value)).length,
    expectedCommitted: expectedCommitted.size,
  }
  observed.handlers = {
    committed: distinctValues(committed).length,
    uncommitted: distinctValues(uncommitted).length,
  }

  expect(controlOffsets.size).toBeGreaterThan(0)
  const extantCommitted = [...wanted].filter((value) => data.has(value))
  for (const value of extantCommitted) expect(distinctValues(committed)).toContain(value)
  for (const { value } of sets.aborted) expect(distinctValues(committed)).not.toContain(value)
  for (const value of expectedCommitted) expect(wanted.has(value)).toBe(true)
  for (const member of groups)
    for (const { partition, offset } of member.delivered)
      expect(controlOffsets.has(`${partition}|${offset}`)).toBe(false)
  for (const { value } of everything)
    if (data.has(value)) expect(distinctValues(uncommitted)).toContain(value)
  observed.commits = {
    committed: await untilCommittedPast(scope, corpus, committed),
    uncommitted: await untilCommittedPast(scope, corpus, uncommitted),
  }
  return corpus
}

describeSemantics('Consumer > Fetch semantics (live)', () => {
  const add = (title, budgetMs, body) =>
    test(title, liveCase(budgetMs, body), caseTimeout(budgetMs))

  add(TITLES.isolation, VISIBILITY_BUDGET_MS, (scope) =>
    runArm({
      evidence: EVIDENCE,
      title: TITLES.isolation,
      config: {
        isolationLevels: ['read_committed', 'read_uncommitted'],
        openRecords: OPEN_RECORDS,
        blockRecords: BLOCK_RECORDS,
        autoCommit: true,
      },
      budgetMs: VISIBILITY_BUDGET_MS,
      replay: REPLAY,
      body: ({ cleanup, observed }) =>
        visibilityEpisode({
          scope,
          cleanup,
          observed,
          prefix: 'isolation',
          keyed: false,
          configEntries: [],
        }),
    })
  )

  add(TITLES.compacted, VISIBILITY_BUDGET_MS, (scope) =>
    runArm({
      evidence: EVIDENCE,
      title: TITLES.compacted,
      config: {
        cleanupPolicy: 'compact',
        isolationLevels: ['read_committed', 'read_uncommitted'],
        autoCommit: true,
      },
      budgetMs: VISIBILITY_BUDGET_MS,
      replay: REPLAY,
      body: ({ cleanup, observed }) =>
        visibilityEpisode({
          scope,
          cleanup,
          observed,
          prefix: 'compacted',
          keyed: true,
          configEntries: COMPACTION,
        }),
    })
  )

  add(TITLES.oversize, OVERSIZE_BUDGET_MS, (scope) =>
    runArm({
      evidence: EVIDENCE,
      title: TITLES.oversize,
      config: {
        maxBytes: MAX_BYTES,
        maxBytesPerPartition: MAX_BYTES_PER_PARTITION,
        largeBytes: LARGE_BYTES,
        autoCommit: true,
      },
      budgetMs: OVERSIZE_BUDGET_MS,
      replay: {
        log: 'the producer input fixes every identity, length and hash',
        offsets: 'the broker-confirmed committed offset must pass the large record',
        handlers: 'each record exactly once, in order',
      },
      body: async ({ cleanup, observed }) => {
        const corpus = new LiveCorpus({ scope, cleanup, prefix: 'oversize', partitions: 1 })
        await corpus.prepare()
        const large = Buffer.alloc(LARGE_BYTES)
        for (let index = 0; index < large.length; index++) large[index] = (index * 31 + 7) % 251
        const hash = crypto.createHash('sha256').update(large).digest('hex')
        const small = (index) => Buffer.from(`small-${index}`)
        await step(scope, 'The corpus production', () =>
          corpus.producer.send({
            topic: corpus.topic,
            acks: -1,
            messages: [
              { partition: 0, value: small(0) },
              { partition: 0, value: large },
              { partition: 0, value: small(2) },
            ],
          })
        )
        corpus.acknowledged.push(
          ...['small-0', `large-${hash}`, 'small-2'].map((value) => ({
            topic: corpus.topic,
            partition: 0,
            value,
          }))
        )

        const fetches = []
        const factory = (options) => {
          const socket = defaultSocketFactory()(options)
          const write = socket.write.bind(socket)
          socket.write = (data, encoding) => {
            const frame = Buffer.from(data, encoding)
            if (frame.length > 6 && frame.readInt16BE(4) === apiKeys.Fetch) fetches.push(frame)
            return write(data, encoding)
          }
          return socket
        }
        const seen = []
        const member = new JournalMember({
          scope,
          cleanup,
          corpus,
          name: 'oversize',
          groupId: corpus.groupId,
          consumer: { maxBytes: MAX_BYTES, maxBytesPerPartition: MAX_BYTES_PER_PARTITION },
          run: AUTO_COMMIT,
          socketFactory: factory,
          onMessage: ({ message }) =>
            seen.push({
              length: message.value.length,
              hash: crypto.createHash('sha256').update(message.value).digest('hex'),
              value: message.value.length < 100 ? message.value.toString() : null,
            }),
        })
        await member.start()
        await pollUntil(scope, STEP_MS, () => seen.length >= 3, {
          label: 'The delivery of all three records',
          delayMs: 250,
        })
        const [first] = fetches.map((frame) => decodeFetchRequest([frame]))
        observed.firstFetch = {
          maxBytes: first.maxBytes,
          partitionMaxBytes: first.topics[0].partitions[0].maxBytes,
        }
        observed.delivered = seen.map(({ length, hash: digest, value }) => ({
          length,
          digest,
          value,
        }))
        expect(observed.firstFetch).toEqual({
          maxBytes: MAX_BYTES,
          partitionMaxBytes: MAX_BYTES_PER_PARTITION,
        })
        expect(seen).toHaveLength(3)
        expect(seen[0].value).toBe('small-0')
        expect(seen[1]).toMatchObject({ length: LARGE_BYTES, hash })
        expect(seen[2].value).toBe('small-2')
        observed.committed = await untilCommittedPast(scope, corpus, member)
        expect(observed.committed).toBe(3)
        expect(member.delivered.map(({ offset }) => offset)).toEqual(['0', '1', '2'])
        return corpus
      },
    })
  )
})
