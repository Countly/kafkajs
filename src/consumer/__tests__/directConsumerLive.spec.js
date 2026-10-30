const { Kafka, logLevel } = require('../../../index')
const defaultSocketFactory = require('../../network/socketFactory')
const { createScriptedCluster, groupScript } = require('../../../testHelpers/scriptedCluster')
const { lanes } = require('../../../testHelpers/lane')
const { plainTextBrokers, secureRandom, writeEvidence } = require('testHelpers')
const { RecordBatch } = require('../../protocol/recordBatch/v0')
const RecordEncoder = require('../../protocol/recordBatch/record/v0')
const { TOPIC } = require('./fixtures/fetchHarness')
const { runWithEvidence } = require('./fixtures/evidenceCase')
const { FrameCounter } = require('./fixtures/frameCounter')
const { CleanupStack, describeError } = require('../../producer/__tests__/fixtures/liveProbe')
const {
  bounded,
  caseTimeout,
  liveCase,
  pollUntil,
} = require('../../producer/__tests__/fixtures/bounded')

// Gated: needs DIRECT_CONSUMER_E2E=1. The docker lane runs against the compose cluster, the fake
// lane against the scripted cluster; `KAFKAJS_SCENARIO_LANES=fake,docker` asks for both. Run alone:
// `npm run test:e2e:direct-consumer`.
const describeDirect = process.env.DIRECT_CONSUMER_E2E === '1' ? describe : describe.skip

const EVIDENCE = 'direct-consumer'
const CASE_BUDGET_MS = 90000
const WAIT_MS = 60000
const POLL_MS = 100
const STEP_MS = 30000
const TRUTH_MS = 5000
const CLEANUP_BUDGET_MS = 20000
const MISSING_TOPIC = 'UNKNOWN_TOPIC_OR_PARTITION'
const TOPIC_NOT_READY = [MISSING_TOPIC, 'LEADER_NOT_AVAILABLE']
const PARTITIONS = 3
const RECORDS = 12
const BASE_TIMESTAMP = 1700000000000
const NODE = { id: 0, host: 'node-0', port: 9092 }
const MEMBERSHIP_APIS = [
  'JoinGroup',
  'SyncGroup',
  'Heartbeat',
  'LeaveGroup',
  'ConsumerGroupHeartbeat',
]
const OFFSET_APIS = ['FindCoordinator', 'OffsetFetch', 'OffsetCommit']
const ILLEGAL_GENERATION = 22

/**
 * One single-record batch written by the production encoder.
 * @param {number} offset the record's offset
 * @param {string} value
 * @returns {Promise<Buffer>}
 */
const batchAt = async (offset, value) =>
  Buffer.from(
    (
      await RecordBatch({
        firstOffset: String(offset),
        firstTimestamp: BASE_TIMESTAMP + offset,
        maxTimestamp: BASE_TIMESTAMP + offset,
        partitionLeaderEpoch: 5,
        records: [RecordEncoder({ key: Buffer.from(`key-${offset}`), value: Buffer.from(value) })],
      })
    ).buffer
  )

/**
 * A public consumer of the topic and everything it was told: deliveries, events and halts.
 */
class Member {
  /**
   * @param {object} options
   * @param {import('../../../types').Consumer} options.consumer
   * @param {FrameCounter} options.frames this client's own counter
   * @param {string} options.topic
   * @param {AbortSignal} options.scope the case's scope
   * @param {Kafka} options.kafka the client the consumer was built from
   */
  constructor({ consumer, frames, topic, scope, kafka }) {
    this.kafka = kafka
    this.consumer = consumer
    this.frames = frames
    this.topic = topic
    this.scope = scope
    /** @type {Array<{ topic: string, partition: number, offset: string, value: string }>} */
    this.delivered = []
    this.halts = []
    this.crashes = []
    this.membershipEvents = []
    const { events } = consumer
    consumer.on(events.PARTITION_HALTED, ({ payload }) => this.halts.push(payload))
    consumer.on(events.CRASH, ({ payload }) => this.crashes.push(payload))
    for (const name of [
      'GROUP_JOIN',
      'REBALANCING',
      'PARTITIONS_ASSIGNED',
      'PARTITIONS_REVOKED',
      'HEARTBEAT',
    ]) {
      consumer.on(events[name], () => this.membershipEvents.push(name))
    }
  }

  /**
   * Connects, selects the partitions and runs with manual commits.
   * @param {Array<{ topic?: string, partitions: number[] }>} selection
   * @param {object} [options] the assign options
   * @returns {Promise<void>}
   */
  async start(selection, options) {
    await bounded(this.scope, Infinity, () => this.consumer.connect())
    await bounded(this.scope, Infinity, () =>
      this.consumer.assign(
        selection.map(({ partitions }) => ({ topic: this.topic, partitions })),
        options
      )
    )
    await bounded(this.scope, Infinity, () => this.run())
  }

  /** @returns {Promise<void>} */
  run() {
    return this.consumer.run({
      autoCommit: false,
      eachMessage: async ({ topic, partition, message }) => {
        this.delivered.push({
          topic,
          partition,
          offset: message.offset,
          value: message.value.toString(),
        })
      },
    })
  }

  /**
   * @param {number} partition
   * @param {number} [from=0] skips the first deliveries
   * @returns {string[]} the offsets delivered for the partition, in order
   */
  offsetsOf(partition, from = 0) {
    return this.delivered
      .slice(from)
      .filter((entry) => entry.partition === partition)
      .map(({ offset }) => offset)
  }
}

/**
 * The topic, its corpus and the broker truth, on either lane. The fake lane drives the real
 * consumer stack over a scripted cluster with an in-memory log and a coordinator that stores
 * commits; the docker lane uses the compose cluster, a fresh topic and the case's own admin and
 * producer.
 */
class Lane {
  /**
   * @param {'fake'|'docker'} name
   * @param {AbortSignal} scope the case's scope, which bounds every step
   */
  constructor(name, scope) {
    this.name = name
    this.scope = scope
    const id = secureRandom()
    this.id = id
    this.topic = name === 'docker' ? `e2e-p9c-direct-${id}` : TOPIC
    this.groupId = `e2e-p9c-direct-group-${id}`
    this.logs = Object.fromEntries(Array.from({ length: PARTITIONS }, (_, p) => [p, []]))
    this.stored = {}
    this.batches = new Map()
    /** @type {Array<{ topic: string, partition: number, value: string }>} what appends confirmed */
    this.acknowledged = []
    /** @type {Set<string>} groups whose commits the fake coordinator refuses as an active group does */
    this.refusing = new Set()
    this.members = []
    this.brokers = name === 'docker' ? plainTextBrokers() : null
  }

  /**
   * Builds the topic and its fixed corpus: twelve records per partition, no later appends.
   * @param {CleanupStack} cleanup closes what this lane opens
   * @returns {Promise<Lane>}
   */
  async open(cleanup) {
    if (this.name === 'docker') await this.openDocker(cleanup)
    else this.openFake(cleanup)
    for (let partition = 0; partition < PARTITIONS; partition++) {
      await this.append(
        partition,
        Array.from({ length: RECORDS }, (_, offset) => `${this.id}-p${partition}-${offset}`)
      )
    }
    return this
  }

  /** @param {CleanupStack} cleanup @returns {void} */
  openFake(cleanup) {
    const end = (partition) => String(this.logs[partition].length)
    this.cluster = groupScript(createScriptedCluster({ nodes: [NODE] }), {
      partitions: PARTITIONS,
      batches: 0,
    })
      .answer('OffsetFetch', ({ groupId, topics }) => ({
        groupId,
        topics: topics.map((entry) => ({
          topic: entry.topic,
          partitions: entry.partitions.map(({ partition }) => ({
            partition,
            offset: this.stored[groupId]?.[partition]?.offset ?? '-1',
            leaderEpoch: -1,
            metadata: this.stored[groupId]?.[partition]?.metadata ?? null,
            errorCode: 0,
          })),
        })),
      }))
      .answer('OffsetCommit', ({ groupId, topics }) => ({
        topics: topics.map((entry) => ({
          topic: entry.topic,
          partitions: entry.partitions.map(({ partition, offset, metadata }) => {
            if (this.refusing.has(groupId)) return { partition, errorCode: ILLEGAL_GENERATION }
            this.stored[groupId] = { ...this.stored[groupId], [partition]: { offset, metadata } }
            return { partition, errorCode: 0 }
          }),
        })),
      }))
      .answer('ListOffsets', ({ topics }) => ({
        topics: topics.map((entry) => ({
          name: entry.name,
          partitions: entry.partitions.map(({ partitionIndex, timestamp }) => ({
            partitionIndex,
            errorCode: 0,
            timestamp: String(timestamp),
            offset: Number(timestamp) === -2 ? '0' : end(partitionIndex),
            leaderEpoch: 5,
          })),
        })),
      }))
      .answer('OffsetForLeaderEpoch', ({ topics }) => ({
        partitions: topics[0].partitions.map(({ partition }) => ({
          partition,
          errorCode: 0,
          leaderEpoch: 5,
          endOffset: end(partition),
        })),
      }))
      .answer('Fetch', async ({ maxWaitMs, topics }) => {
        const partitions = topics[0].partitions.map(({ partition, fetchOffset }) => {
          const from = this.logs[partition].filter(({ offset }) => offset >= Number(fetchOffset))
          return {
            partition,
            errorCode: 0,
            records:
              from.length > 0
                ? Buffer.concat(
                    from.map(({ offset }) => this.batches.get(`${partition}|${offset}`))
                  )
                : false,
            abortedTransactions: [],
          }
        })
        if (partitions.every(({ records }) => records === false)) {
          await new Promise((resolve) => setTimeout(resolve, maxWaitMs))
        }
        return { partitions }
      })
    cleanup.add('scripted cluster is clean', () => this.cluster.assertClean())
  }

  /** @param {CleanupStack} cleanup @returns {Promise<void>} */
  async openDocker(cleanup) {
    this.kafka = new Kafka({
      clientId: `e2e-p9c-direct-${this.id}`,
      brokers: this.brokers,
      logLevel: logLevel.NOTHING,
    })
    this.admin = this.kafka.admin()
    this.producer = this.kafka.producer({ idempotent: true })
    cleanup.add('admin', () => this.admin.disconnect())
    cleanup.add('producer', () => this.producer.disconnect())
    await bounded(this.scope, STEP_MS, () => this.admin.connect(), { label: 'The admin connect' })
    await bounded(this.scope, STEP_MS, () => this.producer.connect(), {
      label: 'The producer connect',
    })
    cleanup.add('topic', (signal) => this.deleteTopic(signal))
    await pollUntil(
      this.scope,
      STEP_MS,
      async () => {
        try {
          await this.admin.createTopics({
            waitForLeaders: true,
            topics: [{ topic: this.topic, numPartitions: PARTITIONS, replicationFactor: 2 }],
          })
          return true
        } catch (error) {
          if (!TOPIC_NOT_READY.includes(error.type)) throw error
          return false
        }
      },
      { delayMs: 200, label: 'The topic creation' }
    )
    await pollUntil(
      this.scope,
      STEP_MS,
      async () => {
        const { topics } = await this.admin
          .fetchTopicMetadata({ topics: [this.topic] })
          .catch(() => ({ topics: [] }))
        return topics.length === 1 && topics[0].partitions.length === PARTITIONS
      },
      { delayMs: 250, label: 'The topic metadata' }
    )
  }

  /**
   * Deletes the topic as a cleanup step of its own; a confirmed missing topic is success.
   * @param {AbortSignal} signal the cleanup's own
   * @returns {Promise<void>}
   * @throws {Error} a deletion failure other than a missing topic
   */
  async deleteTopic(signal) {
    try {
      await bounded(
        signal,
        STEP_MS,
        () => this.admin.deleteTopics({ topics: [this.topic], timeout: 10000 }),
        { label: 'The topic deletion' }
      )
    } catch (error) {
      if (error.type !== MISSING_TOPIC) throw error
    }
  }

  /**
   * @param {number} partition
   * @param {string[]} values appended in order, with their offsets as timestamps
   * @returns {Promise<void>}
   */
  async append(partition, values) {
    if (this.name === 'docker') {
      const first = this.logs[partition].length
      await bounded(this.scope, Infinity, () =>
        this.producer.send({
          topic: this.topic,
          acks: -1,
          messages: values.map((value, index) => ({
            partition,
            value,
            timestamp: String(BASE_TIMESTAMP + first + index),
          })),
        })
      )
      for (const value of values) {
        this.logs[partition].push({ offset: this.logs[partition].length, value })
      }
    } else {
      for (const value of values) {
        const offset = this.logs[partition].length
        this.logs[partition].push({ offset, value })
        this.batches.set(`${partition}|${offset}`, await batchAt(offset, value))
      }
    }
    for (const value of values) this.acknowledged.push({ topic: this.topic, partition, value })
  }

  /**
   * @param {number[]} partitions
   * @param {number} [from=0] first offset
   * @returns {Array<{ topic: string, partition: number, offset: string, value: string }>} the
   *   acknowledged corpus of the partitions from the offset on, as identities
   */
  corpus(partitions, from = 0) {
    return partitions.flatMap((partition) =>
      this.logs[partition]
        .filter(({ offset }) => offset >= from)
        .map(({ offset, value }) => ({
          topic: this.topic,
          partition,
          offset: String(offset),
          value,
        }))
    )
  }

  /**
   * A consumer with its own client, so its frame counter holds only its own traffic.
   * @param {CleanupStack} cleanup
   * @param {object} config extra consumer options
   * @param {Member} [sibling] builds the consumer from this member's client and counter instead
   * @returns {Member}
   */
  member(cleanup, config = {}, sibling) {
    this.scope.throwIfAborted()
    const frames = sibling ? sibling.frames : new FrameCounter()
    const options =
      this.name === 'docker'
        ? {
            clientId: `e2e-p9c-direct-${this.id}-${this.members.length}`,
            brokers: this.brokers,
            logLevel: logLevel.NOTHING,
            socketFactory: frames.wrap(defaultSocketFactory()),
          }
        : (() => {
            const base = this.cluster.kafkaOptions({ logLevel: logLevel.NOTHING })
            return { ...base, socketFactory: frames.wrap(base.socketFactory) }
          })()
    const kafka = sibling ? sibling.kafka : new Kafka(options)
    const consumer = kafka.consumer({
      maxWaitTimeInMs: this.name === 'docker' ? 500 : 50,
      heartbeatInterval: this.name === 'docker' ? 3000 : 100,
      ...config,
    })
    cleanup.add('consumer', () => consumer.disconnect())
    const member = new Member({ consumer, frames, topic: this.topic, scope: this.scope, kafka })
    this.members.push(member)
    return member
  }

  /**
   * @param {Record<number, {offset: string, metadata: string|null}>} commits made before a member joins
   * @param {string} [groupId]
   * @returns {Promise<void>}
   */
  async setCommitted(commits, groupId = this.groupId) {
    if (this.name === 'docker') {
      await bounded(this.scope, Infinity, () =>
        this.admin.setOffsets({
          groupId,
          topic: this.topic,
          partitions: Object.entries(commits).map(([partition, { offset }]) => ({
            partition: Number(partition),
            offset,
          })),
        })
      )
    } else {
      this.stored[groupId] = { ...this.stored[groupId], ...commits }
    }
  }

  /**
   * @param {string} [groupId]
   * @returns {Promise<Record<number, {offset: string, metadata: string|null}>>} the broker's committed
   *   offsets of the cells it holds, as the broker reports them
   */
  async committed(groupId = this.groupId) {
    if (this.name === 'docker') {
      const [{ partitions }] = await bounded(this.scope, Infinity, () =>
        this.admin.fetchOffsets({ groupId, topics: [this.topic] })
      )
      return Object.fromEntries(
        partitions
          .filter(({ offset }) => String(offset) !== '-1')
          .map(({ partition, offset, metadata }) => [
            partition,
            { offset: String(offset), metadata: metadata ?? null },
          ])
      )
    }
    return Object.fromEntries(
      Object.entries(this.stored[groupId] ?? {}).map(([partition, cell]) => [
        partition,
        { offset: cell.offset, metadata: cell.metadata ?? null },
      ])
    )
  }
}

/**
 * @param {Member} member
 * @param {AbortSignal} scope
 * @param {() => boolean} check
 * @param {string} label
 * @returns {Promise<void>} resolves when the check holds; a crash of the member fails it first
 */
const until = (member, scope, check, label) =>
  pollUntil(scope, WAIT_MS, () => member.crashes.length > 0 || check(), {
    delayMs: POLL_MS,
    label,
  }).then(() => expect(member.crashes).toEqual([]))

/**
 * @param {{ topic: string, partition: number, value: string }} record
 * @returns {string} the record's identity
 */
const identityOf = ({ topic, partition, value }) => `${topic}|${partition}|${value}`

/**
 * @param {Array<{ topic: string, partition: number, value: string }>} found records, possibly with more fields
 * @returns {string[]} their identities, sorted, duplicates kept
 */
const identitiesOf = (found) => found.map(identityOf).sort()

/**
 * What one case retains while it runs, so its evidence row is written from whatever the case
 * reached, whether it passed or failed.
 */
class CaseTrace {
  /**
   * @param {'fake'|'docker'} laneName
   * @param {string} name the row's `case`
   */
  constructor(laneName, name) {
    this.laneName = laneName
    this.name = name
    /** @type {string} */
    this.stage = 'created'
    /** @type {Lane|null} */
    this.lane = null
    /** @type {Record<string, unknown>} */
    this.facts = {}
  }

  /** @param {string} stage @returns {void} */
  reach(stage) {
    this.stage = stage
  }

  /** @param {Record<string, unknown>} fields @returns {void} */
  keep(fields) {
    Object.assign(this.facts, fields)
  }

  /**
   * Adopts the lane before it opens, so a failed open is still traced.
   * @param {Lane} lane
   * @param {CleanupStack} cleanup
   * @returns {Promise<Lane>}
   */
  async open(lane, cleanup) {
    this.lane = lane
    this.reach('open')
    return lane.open(cleanup)
  }

  /**
   * Writes the row, synchronously after its broker reads, so an aborted case scope cannot stop it.
   * @param {AbortSignal} scope
   * @param {Error|null} failure the case's own failure
   * @returns {Promise<Error|null>} the write's failure, which never throws
   */
  async write(scope, failure) {
    const { lane, facts } = this
    try {
      const committed = lane
        ? await bounded(scope, TRUTH_MS, () => lane.committed(), {
            label: 'The committed read',
          }).catch((error) => ({ unavailable: describeError(error) }))
        : undefined
      writeEvidence(EVIDENCE, {
        scenario: EVIDENCE,
        case: this.name,
        scenarioLane: this.laneName,
        stage: this.stage,
        error: describeError(failure),
        acknowledged: lane?.acknowledged ?? [],
        members: (lane?.members ?? []).map((member) => ({
          delivered: member.delivered,
          frames: member.frames.frames,
          halts: member.halts.map(({ partition, offset, reason }) => ({
            partition,
            offset,
            reason,
          })),
          crashes: member.crashes.map(({ restart, groupId }) => ({ restart, groupId })),
          membershipEvents: member.membershipEvents,
        })),
        committed: facts.committed ?? committed,
        ...facts,
      })
      return null
    } catch (error) {
      return error
    }
  }
}

/**
 * Runs a case body under its own cleanup stack, its row written before any cleanup runs.
 * @param {'fake'|'docker'} laneName
 * @param {string} name the row's `case`
 * @param {AbortSignal} scope the case's scope
 * @param {(context: { trace: CaseTrace, cleanup: CleanupStack, lane: Lane }) => Promise<void>} body
 * @returns {Promise<void>}
 */
const runCase = (laneName, name, scope, body) => {
  const cleanup = new CleanupStack()
  const trace = new CaseTrace(laneName, name)
  return runWithEvidence(
    cleanup,
    (failure) => trace.write(scope, failure),
    async () => {
      const lane = await trace.open(new Lane(laneName, scope), cleanup)
      await body({ trace, cleanup, lane })
    },
    CLEANUP_BUDGET_MS
  )
}

describeDirect.each(lanes())('Consumer > manual assignment (live, %s lane)', (laneName) => {
  test(
    'manual assignment with a group ID delivers the selected partitions through simple offsets only',
    liveCase(CASE_BUDGET_MS, (scope) =>
      runCase(laneName, 'group-manual', scope, async ({ trace, cleanup, lane }) => {
        const member = lane.member(cleanup, { groupId: lane.groupId })
        trace.reach('start')
        await member.start([{ partitions: [2, 0] }], { offsetReset: 'earliest' })
        await until(member, scope, () => member.delivered.length === 2 * RECORDS, 'both partitions')
        expect(identitiesOf(member.delivered)).toEqual(identitiesOf(lane.corpus([0, 2])))
        expect(member.offsetsOf(1)).toEqual([])
        trace.reach('commit')
        await bounded(scope, STEP_MS, () =>
          member.consumer.commitOffsets(
            [0, 2].map((partition) => ({
              topic: lane.topic,
              partition,
              offset: String(RECORDS),
              metadata: 'round-trip',
            }))
          )
        )
        const committed = await lane.committed()
        trace.keep({ committed, counts: member.frames.of([...MEMBERSHIP_APIS, ...OFFSET_APIS]) })
        expect(committed).toEqual({
          0: { offset: String(RECORDS), metadata: 'round-trip' },
          2: { offset: String(RECORDS), metadata: 'round-trip' },
        })
        const read = await bounded(scope, STEP_MS, () =>
          member.consumer.committed(
            [0, 1, 2].map((partition) => ({ topic: lane.topic, partition }))
          )
        )
        expect(
          read.map(({ partition, offset, metadata }) => [partition, offset, metadata])
        ).toEqual([
          [0, String(RECORDS), 'round-trip'],
          [1, null, null],
          [2, String(RECORDS), 'round-trip'],
        ])
        for (const api of MEMBERSHIP_APIS) expect(member.frames.count(api)).toBe(0)
        expect(member.frames.count('FindCoordinator')).toBeGreaterThan(0)
        expect(member.membershipEvents).toEqual([])
      })
    ),
    caseTimeout(CASE_BUDGET_MS)
  )

  test(
    'manual assignment without a group delivers with no group, coordinator or offset traffic',
    liveCase(CASE_BUDGET_MS, (scope) =>
      runCase(laneName, 'no-group', scope, async ({ trace, cleanup, lane }) => {
        const member = lane.member(cleanup)
        const refused = lane.member(cleanup)
        await bounded(scope, Infinity, () => refused.consumer.connect())
        await bounded(scope, Infinity, () =>
          refused.consumer.assign([{ topic: lane.topic, partitions: [1] }])
        )
        await expect(
          bounded(scope, Infinity, () =>
            refused.consumer.run({ autoCommit: true, eachMessage: async () => {} })
          )
        ).rejects.toThrow('autoCommit requires a groupId')
        expect(refused.frames.of([...OFFSET_APIS, ...MEMBERSHIP_APIS])).toEqual(
          Object.fromEntries([...OFFSET_APIS, ...MEMBERSHIP_APIS].map((api) => [api, 0]))
        )
        trace.reach('start')
        await member.start([{ partitions: [1] }], { offsetReset: 'earliest' })
        await until(member, scope, () => member.delivered.length === RECORDS, 'the partition')
        expect(identitiesOf(member.delivered)).toEqual(identitiesOf(lane.corpus([1])))
        trace.keep({ counts: member.frames.of([...OFFSET_APIS, ...MEMBERSHIP_APIS]) })
        for (const api of [...OFFSET_APIS, ...MEMBERSHIP_APIS]) {
          expect(member.frames.count(api)).toBe(0)
        }
        expect(await lane.committed()).toEqual({})
        expect(member.membershipEvents).toEqual([])
      })
    ),
    caseTimeout(CASE_BUDGET_MS)
  )

  test(
    'two group-free consumers on one client keep independent positions and state',
    liveCase(CASE_BUDGET_MS, (scope) =>
      runCase(laneName, 'peers', scope, async ({ trace, cleanup, lane }) => {
        const first = lane.member(cleanup)
        const second = lane.member(cleanup, {}, first)
        await bounded(scope, Infinity, () => first.consumer.connect())
        await bounded(scope, Infinity, () => second.consumer.connect())
        for (const member of [first, second]) {
          await bounded(scope, Infinity, () =>
            member.consumer.assign([{ topic: lane.topic, partitions: [0] }])
          )
        }
        first.consumer.seek({ topic: lane.topic, partition: 0, offset: '0' })
        second.consumer.seek({ topic: lane.topic, partition: 0, offset: '6' })
        trace.reach('start')
        await bounded(scope, Infinity, () => first.run())
        await bounded(scope, Infinity, () => second.run())
        await until(
          first,
          scope,
          () => first.delivered.length === RECORDS && second.delivered.length === RECORDS - 6,
          'both consumers'
        )
        expect(identitiesOf(first.delivered)).toEqual(identitiesOf(lane.corpus([0])))
        expect(identitiesOf(second.delivered)).toEqual(identitiesOf(lane.corpus([0], 6)))
        expect(
          await bounded(scope, STEP_MS, () =>
            first.consumer.position({ topic: lane.topic, partition: 0 })
          )
        ).toBe(String(RECORDS))
        trace.reach('stop first')
        await bounded(scope, STEP_MS, () => first.consumer.stop())
        expect(first.consumer.assignment()).toEqual([])
        expect(second.consumer.assignment()).toEqual([{ topic: lane.topic, partitions: [0] }])
        expect(
          await bounded(scope, STEP_MS, () =>
            second.consumer.position({ topic: lane.topic, partition: 0 })
          )
        ).toBe(String(RECORDS))
        await bounded(scope, STEP_MS, () => first.consumer.disconnect())
        expect(second.delivered).toHaveLength(RECORDS - 6)
        trace.keep({
          counts: [first, second].map((member) =>
            member.frames.of([...OFFSET_APIS, ...MEMBERSHIP_APIS])
          ),
        })
        for (const member of [first, second]) {
          for (const api of [...OFFSET_APIS, ...MEMBERSHIP_APIS])
            expect(member.frames.count(api)).toBe(0)
        }
      })
    ),
    caseTimeout(CASE_BUDGET_MS)
  )

  test(
    'a manual commit against an active group is accepted or refused without joining',
    liveCase(CASE_BUDGET_MS, (scope) =>
      runCase(laneName, 'beside-active-group', scope, async ({ trace, cleanup, lane }) => {
        const peer = lane.member(cleanup, { groupId: lane.groupId })
        await bounded(scope, Infinity, () => peer.consumer.connect())
        await bounded(scope, Infinity, () =>
          peer.consumer.subscribe({ topic: lane.topic, fromBeginning: true })
        )
        await bounded(scope, Infinity, () => peer.run())
        await until(peer, scope, () => peer.delivered.length === PARTITIONS * RECORDS, 'the peer')
        if (lane.name === 'fake') lane.refusing.add(lane.groupId)
        const manual = lane.member(cleanup, { groupId: lane.groupId })
        await manual.start([{ partitions: [0] }], { offsetReset: 'earliest' })
        let outcome = 'accepted'
        let failure = null
        trace.reach('commit')
        try {
          await bounded(scope, STEP_MS, () =>
            manual.consumer.commitOffsets([{ topic: lane.topic, partition: 0, offset: '5' }])
          )
        } catch (error) {
          outcome = 'refused'
          failure = describeError(error)
        }
        const committed = await lane.committed()
        trace.keep({ outcome, failure, committed, counts: manual.frames.of(MEMBERSHIP_APIS) })
        if (lane.name === 'fake') expect(outcome).toBe('refused')
        if (outcome === 'accepted') expect(committed[0]?.offset).toBe('5')
        for (const api of MEMBERSHIP_APIS) expect(manual.frames.count(api)).toBe(0)
        expect(manual.membershipEvents).toEqual([])
        expect(manual.crashes).toEqual([])
        expect(identitiesOf(manual.delivered.filter(({ partition }) => partition === 0))).toEqual(
          identitiesOf(lane.corpus([0]))
        )
      })
    ),
    caseTimeout(CASE_BUDGET_MS)
  )

  test(
    'a manual seek before run applies once and a crash restart does not replay it',
    liveCase(CASE_BUDGET_MS, (scope) =>
      runCase(laneName, 'seek-once', scope, async ({ trace, cleanup, lane }) => {
        const member = lane.member(cleanup, {
          groupId: lane.groupId,
          retry: { retries: 0, initialRetryTime: 100 },
        })
        await bounded(scope, Infinity, () => member.consumer.connect())
        await bounded(scope, Infinity, () =>
          member.consumer.assign([{ topic: lane.topic, partitions: [0] }], {
            offsetReset: 'earliest',
          })
        )
        member.consumer.seek({ topic: lane.topic, partition: 0, offset: '6' })
        trace.reach('start')
        await bounded(scope, Infinity, () => member.run())
        await until(
          member,
          scope,
          () => member.delivered.length === RECORDS - 6,
          'the seeked records'
        )
        expect(member.offsetsOf(0)).toEqual(['6', '7', '8', '9', '10', '11'])
        trace.reach('commit')
        await bounded(scope, STEP_MS, () =>
          member.consumer.commitOffsets([{ topic: lane.topic, partition: 0, offset: '9' }])
        )
        expect((await lane.committed())[0].offset).toBe('9')
        const before = member.delivered.length
        trace.reach('crash')
        member.frames.killFetchConnection()
        await pollUntil(scope, WAIT_MS, () => member.crashes.length > 0, {
          delayMs: POLL_MS,
          label: 'The crash',
        })
        expect(member.crashes[0]).toEqual(expect.objectContaining({ restart: true }))
        await pollUntil(scope, WAIT_MS, () => member.delivered.length > before, {
          delayMs: POLL_MS,
          label: 'The restarted run',
        })
        const replay = member.delivered.slice(before).map(({ offset }) => Number(offset))
        trace.keep({ replay, committed: await lane.committed() })
        expect(Math.min(...replay)).toBeGreaterThanOrEqual(9)
        for (const api of MEMBERSHIP_APIS) expect(member.frames.count(api)).toBe(0)
      })
    ),
    caseTimeout(CASE_BUDGET_MS)
  )

  test(
    'getters and mode transitions follow the manual lifecycle',
    liveCase(CASE_BUDGET_MS, (scope) =>
      runCase(laneName, 'getters', scope, async ({ trace, cleanup, lane }) => {
        const member = lane.member(cleanup, { groupId: lane.groupId })
        const { consumer } = member
        await bounded(scope, Infinity, () => consumer.connect())
        expect([consumer.assignment(), consumer.subscription()]).toEqual([[], []])
        await bounded(scope, Infinity, () =>
          consumer.assign([{ topic: lane.topic, partitions: [2, 0] }], { offsetReset: 'earliest' })
        )
        expect([consumer.assignment(), consumer.subscription()]).toEqual([[], []])
        trace.reach('manual run')
        await bounded(scope, Infinity, () => member.run())
        await until(member, scope, () => member.delivered.length === 2 * RECORDS, 'the manual run')
        expect(consumer.assignment()).toEqual([{ topic: lane.topic, partitions: [0, 2] }])
        expect(consumer.subscription()).toEqual([])
        expect(identitiesOf(member.delivered)).toEqual(identitiesOf(lane.corpus([0, 2])))
        await bounded(scope, STEP_MS, () => consumer.stop())
        expect(consumer.assignment()).toEqual([])
        trace.reach('to group')
        await bounded(scope, STEP_MS, () => consumer.unsubscribe())
        await bounded(scope, Infinity, () =>
          consumer.subscribe({ topic: lane.topic, fromBeginning: true })
        )
        expect(consumer.subscription()).toEqual([lane.topic])
        const manualCount = member.delivered.length
        await bounded(scope, Infinity, () => member.run())
        await until(member, scope, () => consumer.assignment().length === 1, 'the group assignment')
        expect(consumer.assignment()).toEqual([{ topic: lane.topic, partitions: [0, 1, 2] }])
        await until(
          member,
          scope,
          () => member.delivered.length - manualCount >= PARTITIONS * RECORDS,
          'the group deliveries'
        )
        trace.reach('back to manual')
        await bounded(scope, STEP_MS, () => consumer.unsubscribe())
        expect(consumer.subscription()).toEqual([])
        expect(consumer.assignment()).toEqual([])
        await bounded(scope, Infinity, () =>
          consumer.assign([{ topic: lane.topic, partitions: [1] }], { offsetReset: 'earliest' })
        )
        const groupCount = member.delivered.length
        await bounded(scope, Infinity, () => member.run())
        await until(
          member,
          scope,
          () => member.delivered.length - groupCount >= RECORDS,
          'the second manual run'
        )
        expect(consumer.assignment()).toEqual([{ topic: lane.topic, partitions: [1] }])
        trace.keep({ phases: { manual: manualCount, group: groupCount - manualCount } })
        expect(
          identitiesOf(
            member.delivered.slice(groupCount).filter(({ partition }) => partition === 1)
          )
        ).toEqual(identitiesOf(lane.corpus([1])))
      })
    ),
    caseTimeout(CASE_BUDGET_MS)
  )

  test(
    'a none halt stays on its partition until a seek resumes it',
    liveCase(CASE_BUDGET_MS, (scope) =>
      runCase(laneName, 'none-halt', scope, async ({ trace, cleanup, lane }) => {
        await lane.setCommitted({ 1: { offset: '0', metadata: null } })
        const member = lane.member(cleanup, { groupId: lane.groupId })
        trace.reach('start')
        await member.start([{ partitions: [0, 1] }], { offsetReset: 'none' })
        await until(
          member,
          scope,
          () => member.offsetsOf(1).length === RECORDS && member.halts.length === 1,
          'the sibling and one halt'
        )
        expect(member.halts[0]).toEqual(
          expect.objectContaining({ partition: 0, reason: 'no-offset', offset: null })
        )
        expect(member.offsetsOf(0)).toEqual([])
        expect(member.frames.count('ListOffsets')).toBe(0)
        await expect(
          bounded(scope, STEP_MS, () =>
            member.consumer.position({ topic: lane.topic, partition: 0 })
          )
        ).rejects.toMatchObject({ name: 'KafkaJSNoOffsetError' })
        trace.reach('seek')
        member.consumer.seek({ topic: lane.topic, partition: 0, offset: '6' })
        await until(
          member,
          scope,
          () => member.offsetsOf(0).length === RECORDS - 6,
          'the resumed partition'
        )
        expect(member.offsetsOf(0)).toEqual(['6', '7', '8', '9', '10', '11'])
        expect(member.offsetsOf(1)).toHaveLength(RECORDS)
        expect(member.halts).toHaveLength(1)
        expect(identitiesOf(member.delivered)).toEqual(
          identitiesOf([...lane.corpus([0], 6), ...lane.corpus([1])])
        )
      })
    ),
    caseTimeout(CASE_BUDGET_MS)
  )
})
