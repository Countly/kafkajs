const { Kafka, logLevel } = require('../../../index')
const Runner = require('../runner')
const {
  createScriptedCluster,
  groupScript,
  consumerProtocolScript,
} = require('../../../testHelpers/scriptedCluster')
const { TOPIC, TOPIC_ID, fetchResponse } = require('./fixtures/fetchHarness')
const { MemberAssignment } = require('../assignerProtocol')
const { RecordBatch } = require('../../protocol/recordBatch/v0')
const RecordEncoder = require('../../protocol/recordBatch/record/v0')
const { KafkaJSCorruptRecordError } = require('../../errors')
const { compressedBatch } = require('../../protocol/message/compression/fixtures/compressedBatch')

const NODES = [
  { id: 0, host: 'node-0', port: 9092 },
  { id: 1, host: 'node-1', port: 9093 },
]
const OTHER_TOPIC_ID = 'a1a2a3a4-a5a6-a7a8-a9aa-abacadaeafb0'
/** A second topic whose Metadata row carries no id, as one does inside a delete and recreate. */
const IDLESS_TOPIC = 'topic-without-id'
const STEP_MS = 5
const BUDGET_MS = 10000

/**
 * One single-record batch at `offset`, written by the production encoder so the decoder reads it
 * as it reads a broker's.
 * @param {number} offset
 * @param {object} [options] passed to `RecordBatch`
 * @returns {Promise<Buffer>}
 */
const batchAt = async (offset, options = {}) =>
  Buffer.from(
    (
      await RecordBatch({
        firstOffset: String(offset),
        firstTimestamp: 1700000000000,
        maxTimestamp: 1700000000000,
        partitionLeaderEpoch: 5,
        records: [
          RecordEncoder({
            key: Buffer.from(`key-${offset}`),
            value: Buffer.from(`value-${offset}`),
          }),
        ],
        ...options,
      })
    ).buffer
  )

/**
 * Builds a complete batch with either an invalid magic byte or a malformed gzip body. The gzip
 * batch has a valid checksum and boundary, so its decoder marker carries nextOffset and codec.
 * @param {number} offset first batch offset
 * @param {'magic'|'decompression'} reason corruption kind
 * @returns {Promise<Buffer>} corrupt batch bytes
 */
const corruptAt = async (offset, reason) => {
  if (reason === 'decompression') {
    return compressedBatch({
      baseOffset: BigInt(offset),
      records: 1,
      codec: 'gzip',
      body: Buffer.from('broken gzip'),
    })
  }
  const bytes = await batchAt(offset)
  bytes[16] = 3
  return bytes
}

/**
 * @param {Array<{ offset: number, bytes: Buffer }>} log one partition's batches, in offset order
 * @param {string} fetchOffset
 * @returns {Buffer|false} what a broker returns from `fetchOffset`: every batch from there on, or
 *   `false` when nothing is there yet
 */
const readFrom = (log, fetchOffset) => {
  const from = log.filter(({ offset }) => offset >= Number(fetchOffset))
  return from.length ? Buffer.concat(from.map(({ bytes }) => bytes)) : false
}

/**
 * A Fetch v18 body keyed by another topic id than the harness's: the harness writes its own id,
 * which is replaced where it stands.
 * @param {object[]} partitions as `fetchResponse` takes them
 * @param {string} topicId
 * @returns {Buffer}
 */
const responseForTopicId = (partitions, topicId) => {
  const body = fetchResponse(partitions)
  const from = Buffer.from(TOPIC_ID.replace(/-/g, ''), 'hex')
  const at = body.indexOf(from)
  if (at < 0 || body.indexOf(from, at + 1) >= 0) {
    throw new Error('The Fetch body does not hold the harness topic id exactly once')
  }
  Buffer.from(topicId.replace(/-/g, ''), 'hex').copy(body, at)
  return body
}

/**
 * One public consumer over a two-node scripted cluster, with a log per partition that the Fetch
 * answers read and a committed offset per partition that OffsetCommit writes and OffsetFetch
 * reads. Partition leaders, the topic id and each log can change while the consumer runs. The
 * runner is captured, and its group's request builder observed, only to read what no API exposes.
 */
class HaltCase {
  /**
   * @param {object} options
   * @param {'magic'|'decompression'} options.corruption corruption used in every schedule
   * @param {Record<number, number>} options.leaders the leader node of each partition
   * @param {'classic'|'consumer'} [options.protocol='classic']
   * @param {object} [options.consumer] over the consumer's options
   * @param {object} [options.run] over `consumer.run`'s options
   * @param {Record<string, [number, number]>} [options.advertise] over the profile's versions, as
   *   `createScriptedCluster` takes them
   * @param {string} [options.idless] a second subscribed topic of one partition, led by node 0,
   *   whose Metadata row carries no topic id, so every Fetch that names it is keyed by name
   * @param {(halt: HaltCase) => void} [options.prepare] called once the cluster is scripted,
   *   before the consumer connects, to hold or script its first calls
   */
  constructor({
    leaders,
    corruption,
    protocol = 'classic',
    consumer = {},
    run = {},
    advertise,
    idless,
    prepare,
  }) {
    this.corruption = corruption
    this.prepare = prepare
    this.advertise = advertise
    this.idless = idless
    this.leaders = { ...leaders }
    this.partitions = Object.keys(leaders).map(Number)
    this.protocol = protocol
    this.consumerOptions = consumer
    this.runOptions = run
    this.logs = Object.fromEntries(this.partitions.map((partition) => [partition, []]))
    this.aborted = Object.fromEntries(this.partitions.map((partition) => [partition, []]))
    this.topicId = TOPIC_ID
    this.committed = {}
    this.rounds = []
    this.delivered = []
    this.halts = []
    this.crashes = []
    this.leaderEpochs = {}
    this.errors = []
    this.listeners = []
    this.spies = []
    this.runner = null
  }

  /** @param {number} partition @param {...number} offsets one batch is appended at each */
  async append(partition, ...offsets) {
    for (const offset of offsets) {
      this.logs[partition].push({ offset, bytes: await batchAt(offset) })
    }
  }

  /** @param {number} partition @param {number} offset a corrupt batch is appended there */
  async appendCorrupt(partition, offset) {
    this.logs[partition].push({ offset, bytes: await corruptAt(offset, this.corruption) })
  }

  /** @returns {object} the Metadata answer for the current leaders and topic id */
  metadata() {
    return {
      brokers: NODES.map(({ id, host, port }) => ({ nodeId: id, host, port, rack: null })),
      controllerId: 0,
      topics: [
        {
          errorCode: 0,
          name: TOPIC,
          topicId: this.topicId,
          isInternal: false,
          topicAuthorizedOperations: -2147483648,
          partitions: this.partitions.map((partitionIndex) => ({
            errorCode: 0,
            partitionIndex,
            leaderId: this.leaders[partitionIndex],
            leaderEpoch: this.leaderEpochs[partitionIndex] ?? 5,
            replicas: [this.leaders[partitionIndex]],
            isr: [this.leaders[partitionIndex]],
            offlineReplicas: [],
          })),
        },
        ...(this.idless === undefined
          ? []
          : [
              {
                errorCode: 0,
                name: this.idless,
                topicId: null,
                isInternal: false,
                topicAuthorizedOperations: -2147483648,
                partitions: [
                  {
                    errorCode: 0,
                    partitionIndex: 0,
                    leaderId: 0,
                    leaderEpoch: 5,
                    replicas: [0],
                    isr: [0],
                    offlineReplicas: [],
                  },
                ],
              },
            ]),
      ],
    }
  }

  /**
   * @param {Array<{ topic: string|null }>} topics the topic entries of a Fetch request or a round
   * @returns {Array<{ topic: string|null }>} those of the topic under test, the idless one left out
   */
  own(topics) {
    return topics.filter(({ topic }) => topic !== this.idless)
  }

  /**
   * @param {{ partitions: Array<{ partition: number, fetchOffset: string }> }} topic the request's
   * @returns {object[]} each asked partition with what its log holds from its fetch offset
   */
  answerOf({ partitions }) {
    return partitions.map(({ partition, fetchOffset }) => ({
      partition,
      errorCode: 0,
      records: readFrom(this.logs[partition], fetchOffset),
      abortedTransactions: this.aborted[partition],
    }))
  }

  /**
   * Answers a Fetch from the logs, as a broker would: at once when some asked partition has
   * records, else after the wait the request asked for.
   * @param {{ maxWaitMs: number, topics: object[] }} fields the request's
   * @returns {Promise<object[]>} the answer's partitions
   */
  async fetchAnswer({ maxWaitMs, topics }) {
    const [topic] = this.own(topics)
    const partitions = topic ? this.answerOf(topic) : []
    if (partitions.every(({ records }) => records === false)) {
      await new Promise((resolve) => setTimeout(resolve, maxWaitMs))
    }
    return partitions
  }

  async start() {
    const script = this.protocol === 'consumer' ? consumerProtocolScript : groupScript
    this.cluster = script(
      createScriptedCluster({
        nodes: NODES,
        profile: this.protocol === 'consumer' ? 'consumer' : 'default',
        advertise: this.advertise,
      }),
      { partitions: this.partitions.length }
    )
      .answer('Metadata', () => this.metadata())
      .answer('OffsetForLeaderEpoch', ({ topics }) => ({
        partitions: topics[0].partitions.map(({ partition }) => ({
          partition,
          errorCode: 0,
          leaderEpoch: 5,
          endOffset: '100',
        })),
      }))
      .answer('Fetch', async (fields) => ({ partitions: await this.fetchAnswer(fields) }))
      .answer('OffsetCommit', ({ topics }) => ({
        topics: topics.map(({ partitions, ...key }) => ({
          ...key,
          partitions: partitions.map(({ partition, offset }) => {
            this.committed[partition] = offset
            return { partition, errorCode: 0 }
          }),
        })),
      }))
      .answer('OffsetFetch', ({ groupId, topics }) => ({
        groupId,
        topics: (
          topics ?? [
            {
              topicId: this.topicId,
              partitions: this.partitions.map((partition) => ({ partition })),
            },
          ]
        ).map(({ partitions, ...key }) => ({
          ...key,
          partitions: partitions.map(({ partition }) => ({
            partition,
            offset: this.committed[partition] ?? '-1',
            leaderEpoch: -1,
            metadata: null,
            errorCode: 0,
          })),
        })),
      }))

    if (this.idless !== undefined) {
      this.cluster.answer('SyncGroup', () => ({
        memberAssignment: MemberAssignment.encode({
          version: 1,
          assignment: { [TOPIC]: this.partitions, [this.idless]: [0] },
        }).toString('hex'),
      }))
    }

    this.gates = []
    const hold = this.cluster.schedule.hold.bind(this.cluster.schedule)
    this.cluster.schedule.hold = (selector) => {
      const gate = hold(selector)
      this.gates.push(gate)
      return gate
    }
    this.prepare?.(this)

    const self = this
    const start = Runner.prototype.start
    this.spies.push(
      jest.spyOn(Runner.prototype, 'start').mockImplementation(function (...args) {
        self.runner = this
        return start.apply(this, args)
      })
    )

    const kafka = new Kafka(
      this.cluster.kafkaOptions({
        logLevel: logLevel.ERROR,
        logCreator:
          () =>
          ({ level, log }) => {
            if (level === logLevel.ERROR) this.errors.push(log)
          },
        retry: { retries: 2, initialRetryTime: 10, maxRetryTime: 20 },
      })
    )
    this.consumer = kafka.consumer({
      groupId: 'halt-group',
      maxWaitTimeInMs: 20,
      ...(this.protocol === 'consumer'
        ? { groupProtocol: 'consumer' }
        : { heartbeatInterval: 100 }),
      ...this.consumerOptions,
    })
    this.listeners.push(
      this.consumer.on(this.consumer.events.PARTITION_HALTED, (event) => this.halts.push(event)),
      this.consumer.on(this.consumer.events.CRASH, ({ payload }) =>
        this.crashes.push(payload.error)
      )
    )
    await this.consumer.connect()
    await this.consumer.subscribe({
      topics: this.idless === undefined ? [TOPIC] : [TOPIC, this.idless],
      fromBeginning: true,
    })
    await this.consumer.run({
      autoCommitThreshold: 1,
      eachMessage: async ({ partition, message }) => {
        this.delivered.push({ partition, offset: message.offset })
      },
      ...this.runOptions,
    })
    await this.until(() => this.group?.offsetManager != null, 'the first assignment')
    expect(this.group.assignmentEpoch).toBeGreaterThan(0)
    for (const partition of this.partitions) {
      const acquired = this.group.acquired.get(`${TOPIC}|${partition}`)
      expect(acquired).toEqual({ acquiredAt: expect.any(Number), changedAt: expect.any(Number) })
      expect(acquired.acquiredAt).toBeGreaterThan(0)
      expect(acquired.changedAt).toBe(acquired.acquiredAt)
    }

    const build = this.group.buildFetchRequests
    this.spies.push(
      jest.spyOn(this.group, 'buildFetchRequests').mockImplementation(function (...args) {
        const requests = build.apply(this, args)
        self.rounds.push(requests)
        return requests
      })
    )
  }

  /** @returns {import('../consumerGroup')|undefined} the running consumer's group */
  get group() {
    return this.runner?.consumerGroup
  }

  /**
   * @param {() => boolean} predicate
   * @param {string} label
   * @returns {Promise<void>}
   * @throws {Error} naming `label` when the predicate is still false after the budget
   */
  async until(predicate, label) {
    const deadline = Date.now() + BUDGET_MS
    while (!predicate()) {
      if (Date.now() > deadline) throw new Error(`Did not happen in time: ${label}`)
      await new Promise((resolve) => setTimeout(resolve, STEP_MS))
    }
  }

  /** @param {number} partition @returns {string[]} the offsets delivered for it, in order */
  offsets(partition) {
    return this.delivered.filter((entry) => entry.partition === partition).map((e) => e.offset)
  }

  /**
   * @param {number} node
   * @returns {Array<{ partition: number, fetchOffset: string }>} what each Fetch written to the
   *   node asked for, one entry per partition, oldest first
   */
  fetched(node) {
    return this.cluster
      .requests()
      .filter((record) => record.api === 'Fetch' && record.node === node)
      .flatMap(({ fields }) => this.own(fields.topics).flatMap(({ partitions }) => partitions))
  }

  /** @param {number} partition @returns {string[]} the offsets committed for it, oldest first */
  commits(partition) {
    return this.cluster
      .requests()
      .filter(({ api }) => api === 'OffsetCommit')
      .flatMap(({ fields }) => this.own(fields.topics).flatMap(({ partitions }) => partitions))
      .filter((entry) => entry.partition === partition)
      .map(({ offset }) => offset)
  }

  /** @param {number} partition @returns {string|undefined} where the next fetch of it starts */
  position(partition) {
    return this.group.offsetManager.resolvedOffsets[TOPIC][partition]
  }

  /**
   * Holds the next Fetch to `node` that asks for `partition`.
   * @param {number} node
   * @param {number} partition
   * @returns {import('../../../testHelpers/schedule').Gate}
   */
  holdFetch(node, partition) {
    return this.cluster.schedule.hold({
      api: 'Fetch',
      node,
      match: ({ fields }) =>
        this.own(fields.topics).some(({ partitions }) =>
          partitions.some((entry) => entry.partition === partition)
        ),
    })
  }

  /**
   * Waits until both nodes have built `count` more Fetch rounds, an empty round included, and
   * returns the partitions those rounds asked for.
   * @param {number} [count=3]
   * @returns {Promise<number[]>} one entry per partition asked for
   */
  async laterRounds(count = 3) {
    const since = this.rounds.length
    await this.until(() => this.rounds.length >= since + count * NODES.length, `${count} rounds`)
    return this.rounds
      .slice(since)
      .flatMap((requests) => this.own(requests).flatMap(({ partitions }) => partitions))
      .map(({ partition }) => partition)
  }

  async close() {
    for (const remove of this.listeners) remove()
    // A case that failed while a call was held must not leave the disconnect waiting on it.
    for (const gate of this.gates ?? []) if (gate.settle) gate.release()
    try {
      if (this.consumer) await this.consumer.disconnect()
    } finally {
      for (const spy of this.spies.reverse()) spy.mockRestore()
    }
    this.cluster?.assertClean()
  }
}

describe.each(['magic', 'decompression'])(
  'Consumer > a corrupt Fetch batch halts only its partition (%s)',
  (reason) => {
    /**
     * Uses the selected corruption for every positive and stale-answer scenario.
     * @param {object} options scenario settings
     * @returns {HaltCase} isolated public consumer fixture
     */
    const scenario = (options) => new HaltCase({ ...options, corruption: reason })
    let halt

    afterEach(async () => {
      const closing = halt
      halt = null
      if (closing) await closing.close()
    })

    test('the batches before it are delivered, the partition stops, the others keep going', async () => {
      halt = scenario({ leaders: { 0: 0, 1: 0, 2: 1 } })
      await halt.append(0, 0, 1, 2)
      await halt.appendCorrupt(0, 3)
      await halt.append(0, 4)
      await halt.append(1, 0, 1, 2)
      await halt.append(2, 0, 1, 2)
      await halt.start()

      await halt.until(
        () =>
          halt.halts.length === 1 && halt.offsets(1).length === 3 && halt.offsets(2).length === 3,
        'the halt and the other partitions'
      )
      await halt.until(() => halt.commits(0).includes('3'), 'the commit up to the corrupt batch')
      await halt.append(1, 3)
      await halt.append(2, 3)
      await halt.until(
        () => halt.offsets(1).includes('3') && halt.offsets(2).includes('3'),
        'the other partitions after the halt'
      )
      const later = await halt.laterRounds()

      expect(halt.offsets(0)).toEqual(['0', '1', '2'])
      expect(halt.halts).toHaveLength(1)
      const [{ type, payload }] = halt.halts
      expect(type).toBe('consumer.partition_halted')
      expect(payload).toEqual({
        groupId: 'halt-group',
        memberId: 'member-1',
        topic: TOPIC,
        partition: 0,
        offset: '3',
        reason,
        error: expect.any(KafkaJSCorruptRecordError),
        ...(reason === 'decompression' && { nextOffset: '4', codec: 'gzip', limit: 1073741824 }),
      })
      expect(payload.error).toMatchObject({ reason, offset: '3', retriable: false })
      expect(halt.errors.filter(({ message }) => message.includes('Halted a partition'))).toEqual([
        expect.objectContaining({ topic: TOPIC, partition: 0, offset: '3', reason }),
      ])
      expect(later).not.toContain(0)
      expect(halt.fetched(0).filter(({ partition }) => partition === 0)).toHaveLength(1)
      expect(halt.position(0)).toBe('3')
      expect(halt.commits(0).at(-1)).toBe('3')
      expect(halt.commits(0).every((offset) => Number(offset) <= 3)).toBe(true)
    })

    test('a corrupt first batch halts at once, with nothing delivered and nothing committed', async () => {
      halt = scenario({ leaders: { 0: 0, 1: 0 } })
      await halt.appendCorrupt(0, 0)
      await halt.append(1, 0, 1)
      await halt.start()

      await halt.until(() => halt.halts.length === 1 && halt.offsets(1).length === 2, 'the halt')
      await halt.append(1, 2)
      await halt.until(() => halt.offsets(1).includes('2'), 'the other partition after the halt')
      const later = await halt.laterRounds()

      expect(halt.offsets(0)).toEqual([])
      expect(halt.offsets(1)).toEqual(['0', '1', '2'])
      expect(halt.halts).toHaveLength(1)
      expect(halt.halts[0].payload).toMatchObject({ partition: 0, offset: '0', reason })
      expect(later).not.toContain(0)
      expect(halt.position(0)).toBe('0')
      expect(halt.commits(0)).toEqual([])
    })

    test('an aborted prefix is stepped over by the empty-batch path, up to the corrupt batch', async () => {
      halt = scenario({ leaders: { 0: 0, 1: 1 } })
      halt.logs[0].push({
        offset: 0,
        bytes: await batchAt(0, { transactional: true, producerId: '1000' }),
      })
      halt.aborted[0].push({ producerId: '1000', firstOffset: '0' })
      await halt.appendCorrupt(0, 1)
      await halt.append(1, 0)
      await halt.start()

      await halt.until(() => halt.halts.length === 1 && halt.offsets(1).length === 1, 'the halt')
      await halt.until(() => halt.commits(0).includes('1'), 'the commit past the aborted record')
      await halt.append(1, 1)
      await halt.until(() => halt.offsets(1).includes('1'), 'the other partition after the halt')
      const later = await halt.laterRounds()

      expect(halt.offsets(0)).toEqual([])
      expect(halt.offsets(1)).toEqual(['0', '1'])
      expect(halt.halts).toHaveLength(1)
      expect(halt.halts[0].payload).toMatchObject({ partition: 0, offset: '1' })
      expect(later).not.toContain(0)
      expect(halt.position(0)).toBe('1')
      expect([...new Set(halt.commits(0))]).toEqual(['1'])
    })

    test('a manual resolution stays where the handler left it', async () => {
      halt = scenario({
        leaders: { 0: 0, 1: 1 },
        run: {
          eachMessage: undefined,
          eachBatchAutoResolve: false,
          eachBatch: async ({ batch, resolveOffset }) => {
            const { partition, messages } = batch
            halt.delivered.push(...messages.map(({ offset }) => ({ partition, offset })))
            resolveOffset(partition === 0 ? messages[0].offset : batch.lastOffset())
          },
        },
      })
      await halt.append(0, 0, 1, 2)
      await halt.appendCorrupt(0, 3)
      await halt.append(1, 0)
      await halt.start()

      await halt.until(() => halt.halts.length === 1 && halt.offsets(1).length === 1, 'the halt')
      await halt.until(() => halt.commits(0).includes('1'), 'the commit of the manual resolution')
      await halt.append(1, 1)
      await halt.until(() => halt.offsets(1).includes('1'), 'the other partition after the halt')
      const later = await halt.laterRounds()

      expect(halt.offsets(0)).toEqual(['0', '1', '2'])
      expect(halt.offsets(1)).toEqual(['0', '1'])
      expect(halt.halts).toHaveLength(1)
      expect(later).not.toContain(0)
      expect(halt.position(0)).toBe('1')
      expect([...new Set(halt.commits(0))]).toEqual(['1'])
    })

    test('an offset the application commits itself may pass the batch, and the halt stays', async () => {
      halt = scenario({ leaders: { 0: 0 } })
      await halt.append(0, 0)
      await halt.appendCorrupt(0, 1)
      await halt.start()
      await halt.until(() => halt.halts.length === 1, 'the halt')
      await halt.until(() => halt.commits(0).includes('1'), 'the commit before the batch')

      await halt.consumer.commitOffsets([{ topic: TOPIC, partition: 0, offset: '5' }])
      const later = await halt.laterRounds()

      expect(halt.commits(0).at(-1)).toBe('5')
      expect(later).not.toContain(0)
      expect(halt.group.haltedPartitions.get(`${TOPIC}|0`)).toMatchObject({ offset: '1' })
    })

    test('a throwing listener and a throwing logger neither undo the halt nor stop the round', async () => {
      halt = scenario({ leaders: { 0: 0, 1: 0 } })
      await halt.append(0, 0)
      await halt.appendCorrupt(0, 1)
      await halt.append(1, 0, 1)
      await halt.start()
      halt.listeners.push(
        halt.consumer.on(halt.consumer.events.PARTITION_HALTED, () => {
          throw new Error('listener failure')
        })
      )
      const error = jest.spyOn(halt.group.logger, 'error').mockImplementation(() => {
        throw new Error('logger failure')
      })
      halt.spies.push(error)

      await halt.until(() => halt.halts.length === 1 && halt.offsets(1).length === 2, 'the halt')
      await halt.append(1, 2)
      await halt.until(() => halt.offsets(1).includes('2'), 'the other partition after the halt')
      const later = await halt.laterRounds()

      expect(halt.offsets(0)).toEqual(['0'])
      expect(halt.halts).toHaveLength(1)
      expect(error).toHaveBeenCalledTimes(1)
      expect(error.mock.calls[0][1]).toMatchObject({
        partition: 0,
        offset: '1',
        listenerError: 'listener failure',
      })
      expect(later).not.toContain(0)
      expect(halt.group.haltedPartitions.has(`${TOPIC}|0`)).toBe(true)
    })

    test('a seek made inside the halt event resumes the partition past the batch', async () => {
      halt = scenario({ leaders: { 0: 0 } })
      await halt.append(0, 0)
      await halt.appendCorrupt(0, 1)
      await halt.append(0, 2, 3)
      await halt.start()
      halt.listeners.push(
        halt.consumer.on(halt.consumer.events.PARTITION_HALTED, ({ payload }) =>
          halt.consumer.seek({ topic: payload.topic, partition: payload.partition, offset: '2' })
        )
      )

      await halt.until(() => halt.offsets(0).includes('3'), 'the records after the batch')
      await halt.until(() => halt.commits(0).includes('4'), 'the commit past the last record')

      // The prefix answer was fetched before the seek, so the seek wins over it as over any record.
      expect(halt.offsets(0)).toEqual(['2', '3'])
      expect(halt.halts).toHaveLength(1)
      expect(halt.group.haltedPartitions.size).toBe(0)
      expect(halt.fetched(0).map(({ fetchOffset }) => fetchOffset)).toEqual(
        expect.arrayContaining(['0', '2'])
      )
      expect(halt.commits(0).at(-1)).toBe('4')
    })

    describe('operator actions on a halted partition', () => {
      test('resume does not restart it, and a seek before the batch halts it again as a new episode', async () => {
        halt = scenario({ leaders: { 0: 0 } })
        await halt.append(0, 0, 1)
        await halt.appendCorrupt(0, 2)
        await halt.start()
        await halt.until(() => halt.halts.length === 1, 'the first halt')
        await halt.until(() => halt.offsets(0).length === 2, 'the records before the first halt')

        halt.consumer.pause([{ topic: TOPIC, partitions: [0] }])
        halt.consumer.resume([{ topic: TOPIC, partitions: [0] }])
        expect(await halt.laterRounds()).not.toContain(0)

        const before = halt.fetched(0).length
        halt.consumer.seek({ topic: TOPIC, partition: 0, offset: '1' })
        await halt.until(() => halt.halts.length === 2, 'the second halt')
        await halt.until(
          () => halt.offsets(0).length === 3,
          'the record read again before the halt'
        )
        const after = halt.fetched(0).length
        expect(await halt.laterRounds()).not.toContain(0)

        expect(halt.halts.map(({ payload }) => payload.offset)).toEqual(['2', '2'])
        expect(halt.offsets(0)).toEqual(['0', '1', '1'])
        expect(halt.fetched(0).slice(before, after)).toEqual([
          expect.objectContaining({ partition: 0, fetchOffset: '1' }),
        ])
        expect(halt.fetched(0).slice(after)).toEqual([])
      })

      test('a marker for a paused partition is dropped, and the partition halts once after resume', async () => {
        halt = scenario({ leaders: { 0: 0 } })
        await halt.append(0, 0)
        await halt.start()
        await halt.until(() => halt.offsets(0).length === 1, 'the first record')
        const gate = halt.holdFetch(0, 0)
        await halt.appendCorrupt(0, 1)
        const call = await gate.arrived()
        halt.consumer.pause([{ topic: TOPIC, partitions: [0] }])
        gate.release({ partitions: halt.answerOf(call.fields.topics[0]) })
        expect(await halt.laterRounds()).not.toContain(0)
        expect(halt.halts).toHaveLength(0)

        halt.consumer.resume([{ topic: TOPIC, partitions: [0] }])
        await halt.until(() => halt.halts.length === 1, 'the halt after resume')
        expect(await halt.laterRounds()).not.toContain(0)

        expect(halt.halts).toHaveLength(1)
        expect(halt.halts[0].payload).toMatchObject({ partition: 0, offset: '1' })
      })
    })

    describe('an old corrupt answer is ignored', () => {
      /**
       * Holds node 0's next Fetch of partition 0, lets `between` change the partition while it is
       * held, then answers it with a corrupt batch at the offset it asked for.
       * @param {HaltCase} halt
       * @param {() => Promise<void>} between
       * @param {(partitions: object[]) => object|Buffer} [answer] the held call's answer
       * @returns {Promise<object>} the held call
       */
      const heldAcross = async (halt, between, answer = (partitions) => ({ partitions })) => {
        const gate = halt.holdFetch(0, 0)
        const call = await gate.arrived()
        const asked = halt
          .own(call.fields.topics)
          .flatMap(({ partitions }) => partitions)
          .find(({ partition }) => partition === 0)
        await between()
        gate.release(
          answer([
            {
              partition: 0,
              errorCode: 0,
              records: await corruptAt(Number(asked.fetchOffset), halt.corruption),
            },
          ])
        )
        return call
      }

      /**
       * Seeks partition 0 to `offset` and waits until node 1 fetched it from there.
       * @param {HaltCase} halt
       * @param {string} offset
       * @returns {Promise<void>}
       */
      const seekOnNewLeader = async (halt, offset) => {
        const since = halt.fetched(1).length
        halt.consumer.seek({ topic: TOPIC, partition: 0, offset })
        await halt.until(
          () =>
            halt
              .fetched(1)
              .slice(since)
              .some((entry) => entry.partition === 0 && entry.fetchOffset === offset),
          `partition 0 fetched from ${offset} on its new leader`
        )
      }

      test('across an applied seek to the same offset, after a leader move', async () => {
        halt = scenario({ leaders: { 0: 0, 1: 1 }, consumer: { metadataMaxAge: 0 } })
        await halt.append(0, 0, 1, 2)
        await halt.start()
        await halt.until(() => halt.offsets(0).length === 3, 'the first records')
        await heldAcross(halt, async () => {
          halt.leaders[0] = 1
          await seekOnNewLeader(halt, '3')
        })
        await halt.append(0, 3, 4)
        await halt.until(() => halt.offsets(0).includes('4'), 'the records after the seek')

        expect(halt.halts).toHaveLength(0)
        expect(halt.offsets(0)).toEqual(['0', '1', '2', '3', '4'])
        expect(halt.group.haltedPartitions.size).toBe(0)
      })

      test('across an away-and-back seek', async () => {
        halt = scenario({ leaders: { 0: 0, 1: 1 }, consumer: { metadataMaxAge: 0 } })
        await halt.append(0, 0, 1, 2)
        await halt.start()
        await halt.until(() => halt.offsets(0).length === 3, 'the first records')
        await heldAcross(halt, async () => {
          halt.leaders[0] = 1
          await seekOnNewLeader(halt, '1')
          await halt.until(() => halt.offsets(0).length === 5, 'the records read again')
          await seekOnNewLeader(halt, '3')
        })
        await halt.append(0, 3)
        await halt.until(() => halt.offsets(0).includes('3'), 'the record after the seeks')

        expect(halt.halts).toHaveLength(0)
        expect(halt.offsets(0)).toEqual(['0', '1', '2', '1', '2', '3'])
      })

      test.each([
        ['after it was reassigned', true],
        ['while it is revoked', false],
      ])('across a revoke of the partition, released %s', async (_, reassignFirst) => {
        halt = scenario({ leaders: { 0: 0, 1: 1 }, protocol: 'consumer' })
        await halt.append(0, 0)
        await halt.start()
        await halt.until(() => halt.offsets(0).length === 1, 'the first record')
        const assignment = (memberEpoch, partitions) => ({
          memberId: 'member-1',
          memberEpoch,
          heartbeatIntervalMs: 100,
          assignment: { topicPartitions: [{ topicId: TOPIC_ID, partitions }] },
        })
        const reassign = async () => {
          halt.cluster.schedule.once({ api: 'ConsumerGroupHeartbeat' }, assignment(3, [0, 1]))
          await halt.until(() => halt.group.isAssigned(TOPIC, 0), 'the reassignment')
        }
        await heldAcross(halt, async () => {
          halt.cluster.schedule.once({ api: 'ConsumerGroupHeartbeat' }, assignment(2, [1]))
          await halt.until(() => !halt.group.isAssigned(TOPIC, 0), 'the revoke')
          if (reassignFirst) await reassign()
        })
        if (!reassignFirst) {
          await halt.laterRounds(1)
          const marker = { offset: '1', reason, error: null }
          const context = {
            assignmentEpoch: halt.group.assignmentEpoch,
            requestClock: halt.group.acquireClock,
          }
          expect(halt.group.acceptHalt(TOPIC, 0, marker, context)).toBe(false)
          await reassign()
        }
        await halt.append(0, 1)
        await halt.until(() => halt.offsets(0).includes('1'), 'the record after the reassignment')

        expect(halt.halts).toHaveLength(0)
        expect(halt.group.haltedPartitions.size).toBe(0)
        expect(halt.crashes).toEqual([])
      })

      test('across a recreation of the topic resolved to the same offset', async () => {
        halt = scenario({ leaders: { 0: 0, 1: 1 }, consumer: { metadataMaxAge: 0 } })
        await halt.start()
        await halt.until(() => halt.fetched(0).length > 0, 'the first fetch')
        halt.cluster.answer('Fetch', async (fields) =>
          responseForTopicId(await halt.fetchAnswer(fields), halt.topicId)
        )
        await heldAcross(
          halt,
          async () => {
            const recreated = new Promise((resolve) =>
              halt.listeners.push(halt.consumer.on(halt.consumer.events.TOPIC_RECREATED, resolve))
            )
            halt.topicId = OTHER_TOPIC_ID
            await recreated
          },
          (partitions) => responseForTopicId(partitions, TOPIC_ID)
        )
        await halt.append(0, 0)
        await halt.until(() => halt.offsets(0).includes('0'), 'the record of the new incarnation')

        expect(halt.halts).toHaveLength(0)
        expect(halt.offsets(0)).toEqual(['0'])
      })

      test('across a recreation, fetched by name because another topic of the request has no id', async () => {
        halt = scenario({
          leaders: { 0: 0, 1: 1 },
          consumer: { metadataMaxAge: 0 },
          advertise: { Fetch: [12, 12] },
          idless: IDLESS_TOPIC,
        })
        await halt.start()
        await halt.until(() => halt.fetched(0).length > 0, 'the first fetch')
        const call = await heldAcross(halt, async () => {
          const recreated = new Promise((resolve) =>
            halt.listeners.push(halt.consumer.on(halt.consumer.events.TOPIC_RECREATED, resolve))
          )
          halt.topicId = OTHER_TOPIC_ID
          await recreated
        })
        await halt.append(0, 0)
        await halt.until(() => halt.offsets(0).includes('0'), 'the record of the new incarnation')

        expect(call.version).toBe(12)
        expect(call.fields.topics.map(({ topic }) => topic).sort()).toEqual([TOPIC, IDLESS_TOPIC])
        expect(halt.halts).toHaveLength(0)
        expect(halt.offsets(0)).toEqual(['0'])
      })

      test.each([
        [
          'an out-of-range reset',
          (halt) =>
            halt.cluster.schedule.once(
              {
                api: 'Fetch',
                node: 1,
                match: ({ fields }) =>
                  fields.topics[0].partitions.some((entry) => entry.partition === 0),
              },
              { partitions: [{ partition: 0, errorCode: 1 }] }
            ),
        ],
        [
          'a log-truncation reset',
          (halt) => {
            halt.leaderEpochs[0] = 6
            halt.cluster.schedule.once(
              { api: 'OffsetForLeaderEpoch', node: 1 },
              { partitions: [{ partition: 0, errorCode: 0, leaderEpoch: 5, endOffset: '2' }] }
            )
          },
        ],
        [
          'a reset after a divergence it cannot place',
          (halt) => {
            halt.leaderEpochs[0] = 6
            halt.cluster.schedule.once(
              { api: 'OffsetForLeaderEpoch', node: 1 },
              { partitions: [{ partition: 0, errorCode: 0, leaderEpoch: -1, endOffset: '-1' }] }
            )
          },
        ],
      ])('across %s on its new leader', async (_, reset) => {
        halt = scenario({ leaders: { 0: 0, 1: 1 }, consumer: { metadataMaxAge: 0 } })
        await halt.append(0, 0, 1, 2)
        await halt.start()
        await halt.until(() => halt.offsets(0).length === 3, 'the first records')
        await halt.until(() => halt.commits(0).includes('3'), 'the commit of the first records')
        const commits = halt.commits(0).length
        await heldAcross(halt, async () => {
          reset(halt)
          halt.leaders[0] = 1
          await halt.until(
            () => halt.fetched(1).some((entry) => entry.partition === 0),
            'partition 0 fetched from its new leader'
          )
          await halt.until(
            () => halt.offsets(0).length > 3,
            'the records read again after the reset'
          )
        })
        await halt.until(() => halt.commits(0).length > commits, 'a commit after the reset')
        await halt.laterRounds(1)

        expect(halt.halts).toHaveLength(0)
        expect(halt.commits(0).length).toBeGreaterThan(commits)
        expect(halt.crashes).toEqual([])
      })

      test('a second marker for a halted partition, from its old leader, is dropped', async () => {
        let held
        halt = scenario({
          leaders: { 0: 0, 1: 1 },
          consumer: { metadataMaxAge: 0 },
          prepare: (scripted) => {
            held = scripted.holdFetch(0, 0)
          },
        })
        await halt.appendCorrupt(0, 0)
        const started = halt.start()
        const call = await held.arrived()
        halt.leaders[0] = 1
        await halt.until(() => halt.halts.length === 1, 'the halt on the new leader')
        held.release({ partitions: halt.answerOf(call.fields.topics[0]) })
        await started
        await halt.laterRounds()

        expect(halt.halts).toHaveLength(1)
        expect(
          halt.errors.filter(({ message }) => message.includes('Halted a partition'))
        ).toHaveLength(1)
        expect(halt.group.haltedPartitions.get(`${TOPIC}|0`)).toMatchObject({ offset: '0' })
      })

      test('an answer from the old leader for a halted partition is dropped', async () => {
        let held
        halt = scenario({
          leaders: { 0: 0, 1: 1 },
          consumer: { metadataMaxAge: 0 },
          prepare: (scripted) => {
            held = scripted.holdFetch(0, 0)
          },
        })
        await halt.appendCorrupt(0, 0)
        await halt.append(1, 0)
        const started = halt.start()
        await held.arrived()
        halt.leaders[0] = 1
        await halt.until(() => halt.halts.length === 1, 'the halt on the new leader')
        const records = Buffer.concat([await batchAt(0), await batchAt(1), await batchAt(2)])
        held.release({ partitions: [{ partition: 0, errorCode: 0, records }] })
        await started
        await halt.laterRounds()
        const marker = { offset: '0', reason, error: null }
        const context = {
          assignmentEpoch: halt.group.assignmentEpoch,
          requestClock: halt.group.acquireClock,
        }

        expect(halt.offsets(0)).toEqual([])
        expect(halt.commits(0)).toEqual([])
        expect(halt.position(0)).toBe('0')
        expect(halt.halts).toHaveLength(1)
        expect(halt.group.acceptHalt(TOPIC, 0, marker, context)).toBe(false)
      })

      test('after the membership it was fetched under was lost', async () => {
        halt = scenario({ leaders: { 0: 0 } })
        await halt.append(0, 0)
        await halt.start()
        await halt.until(() => halt.offsets(0).length === 1, 'the first record')
        const rejoin = halt.cluster.schedule.hold({ api: 'JoinGroup' })
        let expired
        await heldAcross(halt, async () => {
          const epoch = halt.group.assignmentEpoch
          expired = halt.runner.rebalanceCoordinator.onPollExpired({
            elapsedMs: 1001,
            isStatic: false,
          })
          await halt.until(() => halt.group.assignmentEpoch !== epoch, 'the lost membership')
        })
        await rejoin.arrived()
        rejoin.release()
        await expired
        await halt.laterRounds(1)
        await halt.append(0, 1)
        await halt.until(() => halt.offsets(0).includes('1'), 'the record after the rejoin')

        expect(halt.halts).toHaveLength(0)
        expect(halt.crashes).toEqual([])
      })

      test('when the cluster knows the topic by another id than the answer', async () => {
        halt = scenario({ leaders: { 0: 0 } })
        await halt.start()
        await halt.until(() => halt.fetched(0).length > 0, 'the first fetch')
        const topicId = jest.spyOn(halt.group.cluster, 'topicId')
        halt.spies.push(topicId)
        await heldAcross(halt, async () => {
          topicId.mockReturnValue(OTHER_TOPIC_ID)
        })
        await halt.laterRounds(1)
        topicId.mockRestore()

        expect(halt.halts).toHaveLength(0)
        expect(halt.group.haltedPartitions.size).toBe(0)
      })
    })

    test('a revoke clears the halt, and the partition reassigned halts again as a new episode', async () => {
      halt = scenario({ leaders: { 0: 0, 1: 0 }, protocol: 'consumer' })
      await halt.appendCorrupt(0, 0)
      await halt.append(1, 0)
      await halt.start()
      await halt.until(
        () => halt.halts.length === 1 && halt.offsets(1).length === 1,
        'the first halt'
      )
      const assignment = (memberEpoch, partitions) => ({
        memberId: 'member-1',
        memberEpoch,
        heartbeatIntervalMs: 100,
        assignment: { topicPartitions: [{ topicId: TOPIC_ID, partitions }] },
      })

      halt.cluster.schedule.once({ api: 'ConsumerGroupHeartbeat' }, assignment(2, [1]))
      await halt.until(() => !halt.group.isAssigned(TOPIC, 0), 'the revoke')
      const cleared = halt.group.haltedPartitions.size
      halt.cluster.schedule.once({ api: 'ConsumerGroupHeartbeat' }, assignment(3, [0, 1]))
      await halt.until(() => halt.halts.length === 2, 'the halt after the reassignment')
      await halt.append(1, 1)
      await halt.until(() => halt.offsets(1).includes('1'), 'the other partition after the halts')

      expect(cleared).toBe(0)
      expect(halt.halts.map(({ payload }) => payload.offset)).toEqual(['0', '0'])
    })

    test('a rejoin that rebuilds the assignment clears the halt, and the partition halts again', async () => {
      halt = scenario({ leaders: { 0: 0, 1: 1 } })
      await halt.appendCorrupt(0, 0)
      await halt.append(1, 0)
      await halt.start()
      await halt.until(
        () => halt.halts.length === 1 && halt.offsets(1).length === 1,
        'the first halt'
      )

      halt.cluster.schedule.once({ api: 'Heartbeat' }, { errorCode: 27 })
      await halt.until(() => halt.halts.length === 2, 'the halt after the rejoin')
      await halt.append(1, 1)
      await halt.until(() => halt.offsets(1).includes('1'), 'the other partition after the rejoin')

      expect(halt.halts.map(({ payload }) => payload.offset)).toEqual(['0', '0'])
      expect(halt.cluster.requests().filter(({ api }) => api === 'JoinGroup')).toHaveLength(2)
    })

    test('a recreation clears a halt of the old incarnation, and the new one is fetched', async () => {
      halt = scenario({ leaders: { 0: 0, 1: 1 }, consumer: { metadataMaxAge: 0 } })
      await halt.appendCorrupt(0, 0)
      await halt.append(1, 0)
      await halt.start()
      await halt.until(
        () => halt.halts.length === 1 && halt.offsets(1).length === 1,
        'the halt of the old incarnation'
      )
      await halt.append(1, 1)
      await halt.until(() => halt.offsets(1).includes('1'), 'the other partition after the halt')

      halt.logs[0] = []
      halt.logs[1] = []
      await halt.append(0, 0)
      halt.cluster.answer('Fetch', async (fields) =>
        responseForTopicId(await halt.fetchAnswer(fields), halt.topicId)
      )
      halt.topicId = OTHER_TOPIC_ID
      await halt.until(() => halt.offsets(0).includes('0'), 'the record of the new incarnation')

      expect(halt.halts).toHaveLength(1)
      expect(halt.offsets(0)).toEqual(['0'])
      expect(halt.group.haltedPartitions.size).toBe(0)
    })
  }
)
