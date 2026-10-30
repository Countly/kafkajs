const { Kafka, logLevel, KafkaJSNoOffsetError } = require('../../../index')
const { createScriptedCluster, groupScript } = require('../../../testHelpers/scriptedCluster')
const { lanes } = require('../../../testHelpers/lane')
const { readLog } = require('../../../testHelpers/rawLog')
const { plainTextBrokers, secureRandom, writeEvidence } = require('testHelpers')
const { RecordBatch } = require('../../protocol/recordBatch/v0')
const RecordEncoder = require('../../protocol/recordBatch/record/v0')
const { TOPIC, TOPIC_ID } = require('./fixtures/fetchHarness')
const { runWithEvidence } = require('./fixtures/evidenceCase')
const { CleanupStack, describeError } = require('../../producer/__tests__/fixtures/liveProbe')
const {
  bounded,
  caseTimeout,
  delay,
  liveCase,
  pollUntil,
} = require('../../producer/__tests__/fixtures/bounded')

// Gated: needs OFFSET_RESET_E2E=1. The docker lane runs against the compose cluster, the fake lane
// against the scripted cluster; `KAFKAJS_SCENARIO_LANES=fake,docker` asks for both. Run alone:
// `npm run test:e2e:offset-reset`.
const describeReset = process.env.OFFSET_RESET_E2E === '1' ? describe : describe.skip

const EVIDENCE = 'offset-reset'
const CASE_BUDGET_MS = 90000
const WAIT_MS = 60000
const POLL_MS = 100
const STEP_MS = 30000
const TRUTH_MS = 5000
const CLEANUP_BUDGET_MS = 20000
const MISSING_TOPIC = 'UNKNOWN_TOPIC_OR_PARTITION'
const TOPIC_NOT_READY = [MISSING_TOPIC, 'LEADER_NOT_AVAILABLE']
const PARTITIONS = 3
const MINUTE = 60000
const NODE = { id: 0, host: 'node-0', port: 9092 }
const NEW_TOPIC_ID = '11121314-1516-1718-191a-1b1c1d1e1f20'

/**
 * One single-record batch written by the production encoder, so the client's decoder reads it as
 * it reads a broker's.
 * @param {number} offset the record's offset
 * @param {number} timestamp the record's create time, ms
 * @param {string} value
 * @returns {Promise<Buffer>}
 */
const batchAt = async (offset, timestamp, value) =>
  Buffer.from(
    (
      await RecordBatch({
        firstOffset: String(offset),
        firstTimestamp: timestamp,
        maxTimestamp: timestamp,
        partitionLeaderEpoch: 5,
        records: [RecordEncoder({ key: Buffer.from(`key-${offset}`), value: Buffer.from(value) })],
      })
    ).buffer
  )

/**
 * A public consumer of the topic and everything it was given and told.
 */
class Member {
  /**
   * @param {object} options
   * @param {import('../../../types').Consumer} options.consumer
   * @param {string} options.topic
   * @param {object} options.subscribe the options of `subscribe` besides the topic
   */
  constructor({ consumer, topic, subscribe }) {
    this.consumer = consumer
    this.topic = topic
    this.subscribeOptions = subscribe
    /** @type {Array<{ topic: string, partition: number, offset: string, value: string }>} */
    this.delivered = []
    this.halts = []
    this.recreated = []
    this.crashes = []
  }

  /**
   * Connects, subscribes and runs with manual commits.
   * @returns {Promise<void>}
   */
  async start() {
    const { consumer } = this
    consumer.on(consumer.events.PARTITION_HALTED, ({ payload }) => this.halts.push(payload))
    consumer.on(consumer.events.TOPIC_RECREATED, ({ payload }) => this.recreated.push(payload))
    consumer.on(consumer.events.CRASH, ({ payload }) => this.crashes.push(payload.error))
    await consumer.connect()
    await consumer.subscribe({ topic: this.topic, ...this.subscribeOptions })
    await this.run()
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
 * The scripted cluster as one lane: an in-memory log per partition, a coordinator that stores
 * commits, ListOffsets that answers by timestamp, and a topic id that can change.
 */
class FakeLane {
  /** @param {AbortSignal} scope the case's scope, which bounds every step */
  constructor(scope) {
    this.scope = scope
    this.name = 'fake'
    this.topic = TOPIC
    this.groupId = `group-${secureRandom()}`
    this.topicId = TOPIC_ID
    this.logs = Object.fromEntries(Array.from({ length: PARTITIONS }, (_, p) => [p, []]))
    this.stored = {}
    this.batches = new Map()
    /** @type {Array<{ topic: string, partition: number, value: string }>} what appends confirmed */
    this.acknowledged = []
  }

  /**
   * @param {CleanupStack} cleanup closes what this lane opens
   * @returns {Promise<FakeLane>}
   */
  async open(cleanup) {
    const end = (partition) => String(this.logs[partition].length)
    const positionAt = (partition, timestamp) => {
      if (timestamp === -2) return '0'
      if (timestamp === -1) return end(partition)
      const hit = this.logs[partition].find((record) => record.timestamp >= timestamp)
      return hit ? String(hit.offset) : '-1'
    }
    this.cluster = groupScript(createScriptedCluster({ nodes: [NODE] }), {
      partitions: PARTITIONS,
      batches: 0,
    })
      .answer('Metadata', () => ({
        brokers: [{ nodeId: NODE.id, host: NODE.host, port: NODE.port, rack: null }],
        controllerId: NODE.id,
        topics: [
          {
            errorCode: 0,
            name: this.topic,
            topicId: this.topicId,
            isInternal: false,
            topicAuthorizedOperations: -2147483648,
            partitions: Object.keys(this.logs).map((partitionIndex) => ({
              errorCode: 0,
              partitionIndex: Number(partitionIndex),
              leaderId: NODE.id,
              leaderEpoch: 5,
              replicas: [NODE.id],
              isr: [NODE.id],
              offlineReplicas: [],
            })),
          },
        ],
      }))
      .answer('OffsetFetch', ({ groupId, topics }) => ({
        groupId,
        topics: topics.map((entry) => ({
          topic: entry.topic,
          partitions: entry.partitions.map(({ partition }) => ({
            partition,
            offset: this.stored[groupId]?.[partition] ?? '-1',
            leaderEpoch: -1,
            metadata: null,
            errorCode: 0,
          })),
        })),
      }))
      .answer('OffsetCommit', ({ groupId, topics }) => ({
        topics: topics.map((entry) => ({
          topic: entry.topic,
          partitions: entry.partitions.map(({ partition, offset }) => {
            this.stored[groupId] = { ...this.stored[groupId], [partition]: offset }
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
            offset: positionAt(partitionIndex, Number(timestamp)),
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
    return this
  }

  /**
   * @param {number} partition
   * @param {Array<{ value: string, timestamp: number }>} records appended in order
   * @returns {Promise<void>}
   */
  async append(partition, records) {
    for (const { value, timestamp } of records) {
      const offset = this.logs[partition].length
      this.logs[partition].push({ offset, value, timestamp })
      this.batches.set(`${partition}|${offset}`, await batchAt(offset, timestamp, value))
      this.acknowledged.push({ topic: this.topic, partition, value })
    }
  }

  /**
   * @param {string} groupId
   * @param {Record<number, string>} offsets the group's commits before any member joins
   * @returns {Promise<void>}
   */
  async setCommitted(groupId, offsets) {
    this.stored[groupId] = { ...this.stored[groupId], ...offsets }
  }

  /**
   * @param {string} groupId
   * @returns {Promise<Record<number, string>>} the coordinator's committed offsets, '-1' for none
   */
  async committed(groupId) {
    return Object.fromEntries(
      Object.keys(this.logs).map((p) => [p, this.stored[groupId]?.[p] ?? '-1'])
    )
  }

  /** @returns {Promise<Record<number, string[]>>} the log's values by partition */
  async log() {
    return Object.fromEntries(
      Object.entries(this.logs).map(([partition, records]) => [
        partition,
        records.map(({ value }) => value),
      ])
    )
  }

  /**
   * @param {CleanupStack} cleanup
   * @param {object} config extra consumer options
   * @param {object} subscribe the subscribe options besides the topic
   * @returns {Member}
   */
  member(cleanup, config, subscribe) {
    this.scope.throwIfAborted()
    const kafka = new Kafka(this.cluster.kafkaOptions({ logLevel: logLevel.NOTHING }))
    const consumer = kafka.consumer({
      groupId: this.groupId,
      maxWaitTimeInMs: 50,
      heartbeatInterval: 100,
      ...config,
    })
    cleanup.add('consumer', () => consumer.disconnect())
    return new Member({ consumer, topic: this.topic, subscribe })
  }

  /**
   * @returns {number[]|null} every partition of every ListOffsets frame, in order; with
   *   `timestamps`, the asked timestamps by partition
   */
  listed() {
    return this.cluster
      .requests()
      .filter(({ api }) => api === 'ListOffsets')
      .flatMap(({ fields }) =>
        fields.topics.flatMap(({ partitions }) =>
          partitions.map(({ partitionIndex, timestamp }) => ({
            partition: partitionIndex,
            timestamp: Number(timestamp),
          }))
        )
      )
  }

  /**
   * A new incarnation of the topic: another id, an empty log, the committed offsets kept.
   * @returns {Promise<void>}
   */
  async recreate() {
    this.topicId = NEW_TOPIC_ID
    for (const partition of Object.keys(this.logs)) this.logs[partition] = []
    this.batches.clear()
  }
}

/**
 * The compose cluster as one lane: a fresh topic, real produces with explicit timestamps, and the
 * broker's own committed offsets and log read back separately.
 */
class DockerLane {
  /** @param {AbortSignal} scope the case's scope, which bounds every step */
  constructor(scope) {
    this.scope = scope
    this.name = 'docker'
    const id = secureRandom()
    this.topic = `e2e-p9c-offset-reset-${id}`
    this.groupId = `e2e-p9c-offset-reset-group-${id}`
    this.brokers = plainTextBrokers()
    this.kafka = new Kafka({
      clientId: `e2e-p9c-offset-reset-${id}`,
      brokers: this.brokers,
      logLevel: logLevel.NOTHING,
    })
    /** @type {Array<{ topic: string, partition: number, value: string }>} what sends confirmed */
    this.acknowledged = []
  }

  /**
   * Connects the case's own admin and producer, then creates the topic through that admin. Each
   * close, the topic's deletion included, is its own cleanup step.
   * @param {CleanupStack} cleanup closes what this lane opens
   * @returns {Promise<DockerLane>}
   */
  async open(cleanup) {
    this.admin = this.kafka.admin()
    this.producer = this.kafka.producer({ idempotent: true })
    cleanup.add('admin', () => this.admin.disconnect())
    cleanup.add('producer', () => this.producer.disconnect())
    await bounded(this.scope, STEP_MS, () => this.admin.connect(), { label: 'The admin connect' })
    await bounded(this.scope, STEP_MS, () => this.producer.connect(), {
      label: 'The producer connect',
    })
    cleanup.add('topic', (signal) => this.deleteTopic(signal))
    await this.createTopic()
    return this
  }

  /**
   * Creates the topic through the case's admin, retrying while the cluster is not ready, and waits
   * until the metadata shows every partition.
   * @returns {Promise<void>}
   */
  async createTopic() {
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
   * Deletes the topic as a cleanup; a confirmed missing topic is success.
   * @param {AbortSignal} signal the cleanup's own, which aborts when its allowance ends
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
   * @param {Array<{ value: string, timestamp: number }>} records appended in order
   * @returns {Promise<void>}
   */
  async append(partition, records) {
    await bounded(this.scope, Infinity, () =>
      this.producer.send({
        topic: this.topic,
        acks: -1,
        messages: records.map(({ value, timestamp }) => ({
          partition,
          value,
          timestamp: String(timestamp),
        })),
      })
    )
    for (const { value } of records) {
      this.acknowledged.push({ topic: this.topic, partition, value })
    }
  }

  /**
   * @param {string} groupId
   * @param {Record<number, string>} offsets the group's commits before any member joins
   * @returns {Promise<void>}
   */
  async setCommitted(groupId, offsets) {
    await bounded(this.scope, Infinity, () =>
      this.admin.setOffsets({
        groupId,
        topic: this.topic,
        partitions: Object.entries(offsets).map(([partition, offset]) => ({
          partition: Number(partition),
          offset,
        })),
      })
    )
  }

  /**
   * @param {string} groupId
   * @returns {Promise<Record<number, string>>} the broker's committed offsets, '-1' for none
   */
  async committed(groupId) {
    const [{ partitions }] = await bounded(this.scope, Infinity, () =>
      this.admin.fetchOffsets({ groupId, topics: [this.topic] })
    )
    return Object.fromEntries(
      partitions.map(({ partition, offset }) => [partition, String(offset)])
    )
  }

  /** @returns {Promise<Record<number, string[]>>} the physical log's values by partition */
  async log() {
    const log = await bounded(this.scope, Infinity, () =>
      readLog({ brokers: this.brokers, topic: this.topic })
    )
    return Object.fromEntries(
      [...log.entries()].map(([partition, { records }]) => [
        partition,
        records.filter((r) => !r.batchContext.isControlBatch).map((r) => r.value.toString()),
      ])
    )
  }

  /**
   * @param {CleanupStack} cleanup
   * @param {object} config extra consumer options
   * @param {object} subscribe the subscribe options besides the topic
   * @returns {Member}
   */
  member(cleanup, config, subscribe) {
    this.scope.throwIfAborted()
    const consumer = this.kafka.consumer({
      groupId: this.groupId,
      maxWaitTimeInMs: 500,
      ...config,
    })
    cleanup.add('consumer', () => consumer.disconnect())
    return new Member({ consumer, topic: this.topic, subscribe })
  }

  /** @returns {null} the broker's own ListOffsets frames are not observable here */
  listed() {
    return null
  }

  /**
   * Deletes the topic and creates it again under the same name; the id is the only thing that
   * moves.
   * @returns {Promise<void>}
   */
  async recreate() {
    await bounded(this.scope, Infinity, () =>
      this.admin.deleteTopics({ topics: [this.topic], timeout: 10000 })
    )
    await pollUntil(
      this.scope,
      WAIT_MS,
      async () => !(await this.admin.listTopics()).includes(this.topic),
      { delayMs: 250, label: 'The topic deletion' }
    )
    await this.createTopic()
  }
}

/**
 * @param {'fake'|'docker'} name
 * @param {AbortSignal} scope the case's scope
 * @returns {FakeLane|DockerLane} the lane, not yet opened
 */
const newLane = (name, scope) => (name === 'docker' ? new DockerLane(scope) : new FakeLane(scope))

/**
 * @param {string} prefix
 * @param {number} count
 * @param {(index: number) => number} timestampOf
 * @returns {Array<{ value: string, timestamp: number }>} records with explicit timestamps
 */
const records = (prefix, count, timestampOf) =>
  Array.from({ length: count }, (_, index) => ({
    value: `${prefix}-${index}`,
    timestamp: timestampOf(index),
  }))

/**
 * @param {Member} member
 * @param {AbortSignal} scope
 * @param {() => boolean} check
 * @param {string} label
 * @returns {Promise<void>}
 */
const until = (member, scope, check, label) =>
  pollUntil(scope, WAIT_MS, () => member.crashes.length > 0 || check(), {
    delayMs: POLL_MS,
    label,
  }).then(() => expect(member.crashes).toEqual([]))

/**
 * @param {object} record `{ topic, partition, value }`, possibly with more fields
 * @returns {string} the record's identity
 */
const identityOf = ({ topic, partition, value }) => `${topic}|${partition}|${value}`

/**
 * @param {object[]} found `{ topic, partition, value }` records, possibly with more fields
 * @returns {string[]} their identities sorted, duplicates kept
 */
const identitiesOf = (found) => found.map(identityOf).sort()

/**
 * @param {FakeLane|DockerLane} lane
 * @param {number} partition
 * @param {number} [from=0] skips the first acknowledged records
 * @returns {Array<{ topic: string, partition: number, value: string }>} what the lane's appends
 *   confirmed for the partition, in production order
 */
const acknowledgedOf = (lane, partition, from = 0) =>
  lane.acknowledged.filter((record) => record.partition === partition).slice(from)

/**
 * @param {Array<{ partition: number, offset: string, value: string }>} delivered
 * @param {Record<number, string[]>} produced the log's values by partition
 * @returns {void} asserts every delivered record sits at its own offset of the log
 */
const expectLogPositions = (delivered, produced) => {
  for (const { partition, offset, value } of delivered) {
    expect(produced[partition][Number(offset)]).toBe(value)
  }
}

/**
 * What one case retains while it runs, so that its evidence row is written from whatever the case
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
    /** @type {string} the step the case reached */
    this.stage = 'created'
    /** @type {FakeLane|DockerLane|null} */
    this.lane = null
    /** @type {Member|null} */
    this.member = null
    /** @type {Record<string, unknown>} what the case retained as it went */
    this.facts = {}
  }

  /**
   * @param {string} stage the step the case is about to await
   * @returns {void}
   */
  reach(stage) {
    this.stage = stage
  }

  /**
   * @param {Record<string, unknown>} fields retained for the row; later calls replace a field
   * @returns {void}
   */
  keep(fields) {
    Object.assign(this.facts, fields)
  }

  /**
   * Adopts the lane before it opens, so a failed open is still traced.
   * @param {FakeLane|DockerLane} lane
   * @param {CleanupStack} cleanup
   * @returns {Promise<FakeLane|DockerLane>} the opened lane
   */
  async open(lane, cleanup) {
    this.lane = lane
    this.reach('open')
    return lane.open(cleanup)
  }

  /**
   * @param {Member} member
   * @returns {Member} the member, now traced
   */
  join(member) {
    this.member = member
    return member
  }

  /**
   * Reads what the broker holds now, each read bounded and its failure recorded, never thrown.
   * @param {AbortSignal} scope
   * @returns {Promise<{ produced: unknown, committed: unknown }>}
   */
  async readTruth(scope) {
    const { lane } = this
    const read = (label, op) =>
      bounded(scope, TRUTH_MS, op, { label }).catch((error) => ({
        unavailable: describeError(error),
      }))
    return {
      produced: await read('The log read', () => lane.log()),
      committed: await read('The committed read', () => lane.committed(lane.groupId)),
    }
  }

  /**
   * Writes the row. Everything it retained is written even when the broker reads fail, and the
   * write happens synchronously, so an aborted case scope cannot stop it.
   * @param {AbortSignal} scope
   * @param {Error|null} failure the case's own failure, when it has one
   * @returns {Promise<Error|null>} the write's failure, which never throws
   */
  async write(scope, failure) {
    const { lane, member, facts } = this
    try {
      const reads =
        lane && (failure || facts.produced === undefined || facts.committed === undefined)
      const truth = reads ? await this.readTruth(scope) : null
      writeEvidence(EVIDENCE, {
        scenario: EVIDENCE,
        case: this.name,
        scenarioLane: this.laneName,
        stage: this.stage,
        error: describeError(failure),
        acknowledged: lane?.acknowledged ?? [],
        delivered: member?.delivered ?? [],
        halts: (member?.halts ?? []).map(({ partition, offset, reason, error }) => ({
          partition,
          offset,
          reason,
          retriable: error?.retriable,
          isNoOffset: error instanceof KafkaJSNoOffsetError,
        })),
        recreated: member?.recreated ?? [],
        crashes: (member?.crashes ?? []).map((error) => describeError(error)),
        listed: lane?.listed() ?? null,
        produced: facts.produced ?? truth?.produced,
        committed: facts.committed ?? truth?.committed,
        brokerAtFailure: failure ? truth : undefined,
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
 * @param {(context: { trace: CaseTrace, cleanup: CleanupStack }) => Promise<void>} body
 * @returns {Promise<void>}
 * @throws {Error} see `runWithEvidence`
 */
const runCase = (laneName, name, scope, body) => {
  const cleanup = new CleanupStack()
  const trace = new CaseTrace(laneName, name)
  return runWithEvidence(
    cleanup,
    (failure) => trace.write(scope, failure),
    () => body({ trace, cleanup }),
    CLEANUP_BUDGET_MS
  )
}

describeReset.each(lanes())('Consumer > offset reset policies (live, %s lane)', (laneName) => {
  test(
    'none halts only partitions without committed offsets and seek recovers',
    liveCase(CASE_BUDGET_MS, (scope) =>
      runCase(laneName, 'none', scope, async ({ trace, cleanup }) => {
        const lane = await trace.open(newLane(laneName, scope), cleanup)
        const now = Date.now()
        trace.reach('produce')
        for (let partition = 0; partition < PARTITIONS; partition++) {
          await lane.append(
            partition,
            records(`p${partition}`, 3, (i) => now - (3 - i) * 1000)
          )
        }
        trace.reach('seed the commit')
        await lane.setCommitted(lane.groupId, { 1: '1' })
        const before = await lane.committed(lane.groupId)
        trace.keep({ committed: { before } })

        const member = trace.join(lane.member(cleanup, {}, { offsetReset: 'none' }))
        trace.reach('start')
        await bounded(scope, Infinity, () => member.start())
        trace.reach('wait for the halts')
        await until(
          member,
          scope,
          () => member.halts.length === 2 && member.offsetsOf(1).length === 2,
          'The two halts and the committed sibling'
        )
        const listedBefore = lane.listed()
        const haltedBeforeSeek = member.halts.map(({ partition, offset, reason, error }) => ({
          partition,
          offset,
          reason,
          retriable: error.retriable,
          isNoOffset: error instanceof KafkaJSNoOffsetError,
        }))
        const deliveredBeforeSeek = [...member.delivered]
        trace.keep({ deliveredBeforeSeek, listedBeforeSeek: listedBefore })

        trace.reach('seek')
        await bounded(scope, Infinity, async () => {
          await member.consumer.seek({ topic: lane.topic, partition: 0, offset: '1' })
          await member.consumer.seek({ topic: lane.topic, partition: 2, offset: '2' })
        })
        trace.reach('wait for the sought suffixes')
        await until(
          member,
          scope,
          () => member.offsetsOf(0).length === 2 && member.offsetsOf(2).length === 1,
          'The sought suffixes'
        )
        trace.reach('read the broker')
        const committed = await lane.committed(lane.groupId)
        const produced = await lane.log()
        trace.keep({ produced, committed: { before, after: committed } })

        trace.reach('assert')
        expect(haltedBeforeSeek.map(({ partition }) => partition).sort()).toEqual([0, 2])
        for (const halt of haltedBeforeSeek) {
          expect(halt).toMatchObject({
            offset: null,
            reason: 'no-offset',
            retriable: false,
            isNoOffset: true,
          })
        }
        expect(deliveredBeforeSeek.map(({ partition, offset }) => ({ partition, offset }))).toEqual(
          [
            { partition: 1, offset: '1' },
            { partition: 1, offset: '2' },
          ]
        )
        expect(identitiesOf(deliveredBeforeSeek)).toEqual(identitiesOf(acknowledgedOf(lane, 1, 1)))
        if (listedBefore !== null) {
          expect(listedBefore.filter(({ partition }) => partition !== 1)).toEqual([])
        }
        expect(member.halts).toHaveLength(2)
        expect(member.offsetsOf(0)).toEqual(['1', '2'])
        expect(member.offsetsOf(2)).toEqual(['2'])
        expect(member.offsetsOf(1)).toEqual(['1', '2'])
        expect(identitiesOf(member.delivered)).toEqual(
          identitiesOf([
            ...acknowledgedOf(lane, 0, 1),
            ...acknowledgedOf(lane, 1, 1),
            ...acknowledgedOf(lane, 2, 2),
          ])
        )
        expectLogPositions(member.delivered, produced)
        expect(produced[0]).toEqual(['p0-0', 'p0-1', 'p0-2'])
        expect(before).toEqual({ 0: '-1', 1: '1', 2: '-1' })
        expect(committed).toEqual({ 0: '-1', 1: '1', 2: '-1' })
      })
    ),
    caseTimeout(CASE_BUDGET_MS)
  )

  test(
    'duration selects the explicit timestamp window and a missing window resolves to log end',
    liveCase(CASE_BUDGET_MS, (scope) =>
      runCase(laneName, 'duration', scope, async ({ trace, cleanup }) => {
        const lane = await trace.open(newLane(laneName, scope), cleanup)
        const t0 = Date.now()
        const old = [t0 - 5 * MINUTE, t0 - 4 * MINUTE, t0 - MINUTE]
        trace.reach('produce')
        for (const partition of [0, 1]) {
          await lane.append(
            partition,
            records(`p${partition}`, 3, (i) => old[i])
          )
        }
        await lane.append(
          2,
          records('p2', 2, (i) => old[i])
        )

        const member = trace.join(lane.member(cleanup, {}, { offsetReset: 'by_duration:PT3M' }))
        trace.reach('start')
        await bounded(scope, Infinity, () => member.start())
        trace.reach('wait for the in-window records')
        await until(
          member,
          scope,
          () => member.offsetsOf(0).length === 1 && member.offsetsOf(1).length === 1,
          'The in-window records'
        )
        await delay(scope, 1500)
        const initial = [...member.delivered]
        const queried = lane.listed()
        trace.keep({ initial, queried })

        trace.reach('produce the fresh record')
        await lane.append(2, [{ value: 'p2-2', timestamp: Date.now() }])
        trace.reach('wait for the fresh record')
        await until(member, scope, () => member.offsetsOf(2).length === 1, 'The fresh p2 record')

        const fromReplay = member.delivered.length
        trace.keep({ replayFrom: fromReplay })
        trace.reach('replay')
        await bounded(scope, Infinity, () =>
          member.consumer.seek({ topic: lane.topic, partition: 0, offset: '1' })
        )
        await until(
          member,
          scope,
          () => member.offsetsOf(0, fromReplay).length === 2,
          'The explicit replay'
        )
        trace.reach('read the broker')
        const committed = await lane.committed(lane.groupId)
        const produced = await lane.log()
        trace.keep({ produced, committed })

        trace.reach('assert')
        expect(
          initial
            .map(({ partition, offset }) => ({ partition, offset }))
            .sort((a, b) => a.partition - b.partition)
        ).toEqual([
          { partition: 0, offset: '2' },
          { partition: 1, offset: '2' },
        ])
        expect(member.offsetsOf(2)).toEqual(['2'])
        expect(member.offsetsOf(0, fromReplay)).toEqual(['1', '2'])
        expect(identitiesOf(initial)).toEqual(
          identitiesOf([...acknowledgedOf(lane, 0, 2), ...acknowledgedOf(lane, 1, 2)])
        )
        expect(identitiesOf(member.delivered.slice(initial.length, fromReplay))).toEqual(
          identitiesOf(acknowledgedOf(lane, 2, 2))
        )
        expect(identitiesOf(member.delivered.slice(fromReplay))).toEqual(
          identitiesOf(acknowledgedOf(lane, 0, 1))
        )
        expectLogPositions(member.delivered, produced)
        expect(member.halts).toEqual([])
        expect(produced[0]).toEqual(['p0-0', 'p0-1', 'p0-2'])
        expect(produced[2]).toEqual(['p2-0', 'p2-1', 'p2-2'])
        expect(committed).toEqual({ 0: '-1', 1: '-1', 2: '-1' })
        if (queried !== null) {
          expect(
            queried.filter(({ partition }) => partition !== 2).every((q) => q.timestamp > 0)
          ).toBe(true)
          expect(
            queried.filter(({ timestamp }) => timestamp < 0).map(({ partition }) => partition)
          ).toEqual([2])
        }
      })
    ),
    caseTimeout(CASE_BUDGET_MS)
  )

  test(
    'explicit commits publish real offsets after duration resolution',
    liveCase(CASE_BUDGET_MS, (scope) =>
      runCase(laneName, 'explicit-commit', scope, async ({ trace, cleanup }) => {
        const lane = await trace.open(newLane(laneName, scope), cleanup)
        const now = Date.now()
        trace.reach('produce')
        for (let partition = 0; partition < PARTITIONS; partition++) {
          await lane.append(
            partition,
            records(`p${partition}`, 3, (i) => now - 30000 + i * 1000)
          )
        }
        const member = trace.join(lane.member(cleanup, {}, { offsetReset: 'by_duration:PT3M' }))
        trace.reach('start')
        await bounded(scope, Infinity, () => member.start())
        trace.reach('wait for every record')
        await until(
          member,
          scope,
          () => [0, 1, 2].every((partition) => member.offsetsOf(partition).length === 3),
          'Every record of every partition'
        )
        trace.reach('commit')
        const beforeCommit = await lane.committed(lane.groupId)
        trace.keep({ committed: { before: beforeCommit } })
        await bounded(scope, Infinity, () =>
          member.consumer.commitOffsets(
            [0, 1, 2].map((partition) => ({ topic: lane.topic, partition, offset: '3' }))
          )
        )
        trace.reach('read the broker')
        const committed = await lane.committed(lane.groupId)
        const produced = await lane.log()
        trace.keep({ produced, committed: { before: beforeCommit, after: committed } })

        trace.reach('assert')
        expect(beforeCommit).toEqual({ 0: '-1', 1: '-1', 2: '-1' })
        expect(committed).toEqual({ 0: '3', 1: '3', 2: '3' })
        expect(member.offsetsOf(0)).toEqual(['0', '1', '2'])
        expect(identitiesOf(member.delivered)).toEqual(identitiesOf(lane.acknowledged))
        expectLogPositions(member.delivered, produced)
      })
    ),
    caseTimeout(CASE_BUDGET_MS)
  )

  test(
    'a recreated topic resets again before its first confirmed commit',
    liveCase(CASE_BUDGET_MS, (scope) =>
      runCase(laneName, 'recreated', scope, async ({ trace, cleanup }) => {
        const lane = await trace.open(newLane(laneName, scope), cleanup)
        const now = Date.now()
        trace.reach('produce the old incarnation')
        await lane.append(
          0,
          records('old', 100, () => now - MINUTE)
        )
        const oldAcknowledged = acknowledgedOf(lane, 0)
        const member = trace.join(
          lane.member(cleanup, { metadataMaxAge: 500 }, { fromBeginning: true })
        )
        trace.reach('start')
        await bounded(scope, Infinity, () => member.start())
        trace.reach('wait for the old incarnation')
        await until(member, scope, () => member.offsetsOf(0).length === 100, 'The old incarnation')
        trace.reach('commit the old position')
        await bounded(scope, Infinity, () =>
          member.consumer.commitOffsets([{ topic: lane.topic, partition: 0, offset: '99' }])
        )
        const oldCommit = await lane.committed(lane.groupId)
        trace.keep({ committed: { old: oldCommit } })

        trace.reach('recreate')
        await lane.recreate()
        trace.reach('produce the new incarnation')
        await lane.append(
          0,
          records('new', 100, () => now)
        )
        const newAcknowledged = lane.acknowledged.filter(({ value }) => value.startsWith('new-'))
        trace.reach('wait for the recreation event')
        await until(member, scope, () => member.recreated.length === 1, 'The recreation event')
        trace.reach('wait for the new incarnation')
        await until(
          member,
          scope,
          () => member.delivered.some(({ value }) => value === 'new-0'),
          'The new incarnation'
        )
        const firstNew = member.delivered.find(({ value }) => value.startsWith('new-'))
        const unconfirmed = await lane.committed(lane.groupId)
        trace.keep({ committed: { old: oldCommit, unconfirmed } })

        trace.reach('stop')
        await bounded(scope, Infinity, () => member.consumer.stop())
        const mark = member.delivered.length
        trace.keep({ stoppedAt: mark })
        trace.reach('run again')
        await bounded(scope, Infinity, () => member.run())
        trace.reach('wait for the restarted first delivery')
        await until(
          member,
          scope,
          () => member.delivered.slice(mark).some(({ value }) => value === 'new-0'),
          'The restarted first delivery'
        )
        const restarted = member.delivered
          .slice(mark)
          .filter(({ value }) => value.startsWith('new-'))

        trace.reach('confirm a commit')
        await bounded(scope, Infinity, () =>
          member.consumer.commitOffsets([{ topic: lane.topic, partition: 0, offset: '1' }])
        )
        const confirmed = await lane.committed(lane.groupId)
        trace.keep({ committed: { old: oldCommit, unconfirmed, confirmed } })
        trace.reach('stop before the rebuild')
        await bounded(scope, Infinity, () => member.consumer.stop())
        const rebuiltAt = member.delivered.length
        trace.keep({ stoppedAt: mark, rebuiltAt })
        const listedBefore = lane.listed()
        trace.reach('rebuild')
        await bounded(scope, Infinity, () => member.run())
        trace.reach('wait for the rebuilt first delivery')
        await until(
          member,
          scope,
          () => member.delivered.slice(rebuiltAt).length > 0,
          'The rebuilt first delivery'
        )
        const rebuilt = member.delivered[rebuiltAt]
        const listedAfter = lane.listed()
        trace.reach('read the broker')
        const produced = await lane.log()
        trace.keep({ produced, listed: { before: listedBefore, after: listedAfter } })

        trace.reach('assert')
        expect(firstNew).toMatchObject({ partition: 0, offset: '0', value: 'new-0' })
        expect(restarted[0]).toMatchObject({ partition: 0, offset: '0', value: 'new-0' })
        expect(member.delivered.some(({ value }) => value === 'old-99')).toBe(true)
        expect(
          identitiesOf(member.delivered.filter(({ value }) => value.startsWith('old-')))
        ).toEqual(identitiesOf(oldAcknowledged))
        const deliveredNew = (from, to) =>
          member.delivered.slice(from, to).filter(({ value }) => value.startsWith('new-'))
        const prefixOf = (found, from = 0) =>
          newAcknowledged.slice(from, from + found.length).map(identityOf)
        const acknowledgedNew = new Set(newAcknowledged.map(identityOf))
        for (const record of deliveredNew(0, mark)) {
          expect(acknowledgedNew.has(identityOf(record))).toBe(true)
        }
        expect(restarted.map(identityOf)).toEqual(prefixOf(restarted))
        expect(member.delivered.slice(rebuiltAt).map(identityOf)).toEqual(
          prefixOf(member.delivered.slice(rebuiltAt), 1)
        )
        expectLogPositions(deliveredNew(0), produced)
        expect(produced[0].filter((value) => value.startsWith('new-'))).toHaveLength(100)
        if (laneName === 'fake') {
          expect(oldCommit[0]).toBe('99')
          expect(unconfirmed[0]).toBe('99')
        }
        expect(unconfirmed[0]).not.toBe('1')
        expect(confirmed[0]).toBe('1')
        expect(rebuilt).toMatchObject({ partition: 0, offset: '1', value: 'new-1' })
        if (listedBefore !== null) expect(listedAfter).toEqual(listedBefore)
      })
    ),
    caseTimeout(CASE_BUDGET_MS)
  )
})
