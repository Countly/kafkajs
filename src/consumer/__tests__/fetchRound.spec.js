const { compressedBatch } = require('../../protocol/message/compression/fixtures/compressedBatch')
const { bombOf, MIB } = require('../../protocol/message/compression/fixtures/hostileCases')
const {
  TOPIC,
  NODE_ID,
  createFetchGroup,
  prepareRecordBatches,
  fetchResponse,
} = require('./fixtures/fetchHarness')
const { mulberry32 } = require('../../../testHelpers/fuzz')
const ConsumerGroup = require('../consumerGroup')
const SubscriptionState = require('../subscriptionState')
const { KafkaJSError } = require('../../errors')

/** Hand-built magic 2 batch at offset 0, one record `A`, correct CRC32C. */
const FIRST = Buffer.from(
  '0000000000000000000000390000000002e9d177b30000000000000000018bcfe568000000018bcfe56800ffffffffffffffffffffffffffff000000010e00000001024100',
  'hex'
)
/** The same shape at offset 1, one record `B`. */
const LAST = Buffer.from(
  '0000000000000001000000390000000002dd36df2a0000000000000000018bcfe568000000018bcfe56800ffffffffffffffffffffffffffff000000010e00000001024200',
  'hex'
)
/** One batch at offsets 0 and 1, records `A` and `B`, correct CRC32C. */
const TWO_RECORDS = Buffer.from(
  '0000000000000000000000410000000002bc552b590000000000010000018bcfe568000000018bcfe56800ffffffffffffffffffffffffffff000000020e000000010241000e00000201024200',
  'hex'
)

let harness
let CORRUPTED_RECORD_BATCH
beforeAll(async () => {
  ;({ CORRUPTED_RECORD_BATCH } = await prepareRecordBatches())
})
afterEach(async () => {
  if (harness) await harness.broker.disconnect()
  harness = null
})

/**
 * @param {object} batch a delivered batch
 * @returns {string[][]} `[offset, value]` of each of its messages
 */
const valuesOf = ({ messages }) => messages.map(({ offset, value }) => [offset, value.toString()])

/**
 * @param {object[]} batches the batches one round returned
 * @returns {object[]} every message they carry, in order
 */
const messagesOf = (batches) => batches.flatMap(({ messages }) => messages)

describe('a Fetch response with a cut trailing batch', () => {
  it('delivers the complete batches before it and halts nothing', async () => {
    harness = await createFetchGroup()
    const halted = []
    harness.group.instrumentationEmitter.addListener('consumer.partition_halted', (event) =>
      halted.push(event)
    )

    for (const cut of [1, 11, 12, 16, 60, LAST.length - 1]) {
      harness.setResponse(
        fetchResponse([
          { partition: 0, errorCode: 0, records: Buffer.concat([FIRST, LAST.subarray(0, cut)]) },
        ])
      )
      const batches = await harness.group.fetch(NODE_ID)

      expect(batches).toHaveLength(1)
      expect(valuesOf(batches[0])).toEqual([['0', 'A']])
      expect(harness.group.haltedPartitions.size).toBe(0)
      expect(halted).toEqual([])
      expect(harness.requestRejoin).not.toHaveBeenCalled()
    }
  })

  it('delivers both batches when the same bytes are complete', async () => {
    harness = await createFetchGroup()
    harness.setResponse(
      fetchResponse([{ partition: 0, errorCode: 0, records: Buffer.concat([FIRST, LAST]) }])
    )

    const batches = await harness.group.fetch(NODE_ID)

    expect(valuesOf(batches[0])).toEqual([
      ['0', 'A'],
      ['1', 'B'],
    ])
  })

  it('does not skip into a later batch after the cut', async () => {
    harness = await createFetchGroup()
    harness.setResponse(
      fetchResponse([
        {
          partition: 0,
          errorCode: 0,
          records: Buffer.concat([FIRST, LAST.subarray(0, 30), FIRST]),
        },
      ])
    )

    const batches = await harness.group.fetch(NODE_ID)

    expect(valuesOf(batches[0])).toEqual([['0', 'A']])
  })
})

describe('the Fetch response walk', () => {
  it('delivers batches in the response order, whatever order the request used', async () => {
    harness = await createFetchGroup({ partitions: [0, 1, 2] })
    harness.setResponse(
      fetchResponse([2, 0, 1].map((partition) => ({ partition, errorCode: 0, records: true })))
    )

    const batches = await harness.group.fetch(NODE_ID)

    expect(batches.map(({ partition }) => partition)).toEqual([2, 0, 1])
    expect(harness.fetchRequestPartitions().sort()).toEqual([0, 1, 2])
  })
})

describe('node selection', () => {
  it('keeps the input order and the corrected eligibility rules under randomized metadata', async () => {
    harness = await createFetchGroup()
    for (let seed = 1; seed <= 200; seed++) {
      const random = mulberry32(seed)
      const ids = Array.from({ length: 17 }, (_, i) => i)
      const metadata = ids
        .filter(() => random() > 0.1)
        .map((partitionId) => ({
          partitionId,
          leader: Math.floor(random() * 4) - 1,
          offlineReplicas: random() < 0.5 ? [1] : [0],
        }))
      const preferences = Object.fromEntries(
        ids
          .filter(() => random() < 0.5)
          .map((id) => [id, { nodeId: 1, expireAt: random() < 0.5 ? 0 : Number.MAX_SAFE_INTEGER }])
      )
      const expected = {}
      let leaderless = false
      for (const id of ids) {
        const row = metadata.find(({ partitionId }) => partitionId === id)
        if (!row) continue
        if (row.leader < 0) {
          leaderless = true
          continue
        }
        const preference = preferences[id]
        const node =
          preference &&
          preference.expireAt > Date.now() &&
          !row.offlineReplicas.includes(preference.nodeId)
            ? preference.nodeId
            : row.leader
        ;(expected[node] ||= []).push(id)
      }
      harness.cluster.findTopicPartitionMetadata = () => metadata
      harness.group.preferredReadReplicasPerTopicPartition[TOPIC] = preferences
      const routing = { expiresAt: Infinity, leaderless: false }
      try {
        expect(harness.group.findReadReplicaForPartitions(TOPIC, ids, routing)).toEqual(expected)
        expect(routing.leaderless).toBe(leaderless)
      } catch (error) {
        error.message += ` seed=${seed} metadata=${JSON.stringify(metadata)}`
        throw error
      }
    }
  })
})

describe('decoder limits of concurrent consumers', () => {
  /**
   * Creates one consumer per setting in the given order, answers every one with the same bytes,
   * and fetches from all of them at once.
   * @param {object[]} settings the options of each consumer, in construction order
   * @param {Buffer} records the Fetch response records every consumer is answered with
   * @param {number} [version] the Fetch version the harness advertises
   * @returns {Promise<{ batches: object[][], consumers: object[] }>} what each consumer returned
   */
  const fetchAll = async (settings, records, version = 18) => {
    const consumers = []
    try {
      for (const options of settings) {
        consumers.push(await createFetchGroup({ fetchMaxVersion: version, ...options }))
      }
      for (const consumer of consumers) {
        consumer.setResponse(
          fetchResponse([{ partition: 0, errorCode: 0, records }], { apiVersion: version })
        )
      }
      const batches = await Promise.all(consumers.map(({ group }) => group.fetch(NODE_ID)))
      return { batches, consumers }
    } finally {
      harness = {
        broker: { disconnect: () => Promise.all(consumers.map((c) => c.broker.disconnect())) },
      }
    }
  }

  it.each([
    [512, 1024],
    [1024, 512],
  ])('keeps the record ceiling private in construction order %j', async (...limits) => {
    const { batches, consumers } = await fetchAll(
      limits.map((maxDecompressedBatchBytes) => ({ maxDecompressedBatchBytes })),
      TWO_RECORDS
    )

    limits.forEach((limit, index) => {
      if (limit === 512) {
        expect(messagesOf(batches[index])).toEqual([])
        expect(consumers[index].group.haltedPartitions.get(`${TOPIC}|0`)).toMatchObject({
          error: { reason: 'count', limit: 1 },
        })
      } else {
        expect(valuesOf(batches[index][0])).toEqual([
          ['0', 'A'],
          ['1', 'B'],
        ])
        expect(consumers[index].group.haltedPartitions.size).toBe(0)
      }
    })
  })

  it.each([
    [true, false],
    [false, true],
  ])('keeps CRC checking private in construction order %j', async (...crcs) => {
    const { batches, consumers } = await fetchAll(
      crcs.map((checkCrcs) => ({ checkCrcs })),
      CORRUPTED_RECORD_BATCH
    )

    crcs.forEach((checkCrcs, index) => {
      if (checkCrcs) {
        expect(messagesOf(batches[index])).toEqual([])
        expect(consumers[index].group.haltedPartitions.get(`${TOPIC}|0`)).toMatchObject({
          error: { reason: 'checksum' },
        })
      } else {
        expect(messagesOf(batches[index])).toHaveLength(1)
        expect(consumers[index].group.haltedPartitions.size).toBe(0)
      }
    })
  })

  it.each([
    [4 * MIB, 9 * MIB],
    [9 * MIB, 4 * MIB],
  ])('keeps the decompressed byte bound private in construction order %j', async (...bounds) => {
    const bomb = await compressedBatch({
      codec: 'gzip',
      body: await bombOf('gzip', 8),
      records: 3,
    })
    const { consumers } = await fetchAll(
      bounds.map((maxDecompressedBatchBytes) => ({ maxDecompressedBatchBytes })),
      bomb
    )

    bounds.forEach((bound, index) => {
      const { error } = consumers[index].group.haltedPartitions.get(`${TOPIC}|0`)
      expect(error.reason).toBe(bound === 4 * MIB ? 'decompressed-size' : 'length')
    })
  })

  it.each([
    [2, 2, 0],
    [3, 2, 1],
  ])(
    'holds a batch of %d records to a ceiling of %d at its boundary',
    async (records, ceiling, halts) => {
      const { batches, consumers } = await fetchAll(
        [{ maxDecompressedBatchBytes: ceiling * 512 }],
        await compressedBatch({ codec: 'none', records })
      )

      expect(consumers[0].group.haltedPartitions.size).toBe(halts)
      expect(messagesOf(batches[0])).toHaveLength(halts ? 0 : records)
      if (halts) {
        expect(consumers[0].group.haltedPartitions.get(`${TOPIC}|0`).error).toMatchObject({
          reason: 'count',
          limit: ceiling,
        })
      }
    }
  )
})

/**
 * The linear-search routing this client shipped before, kept verbatim as the oracle.
 * @this {ConsumerGroup}
 * @param {string} topic
 * @param {Array<number|string>} partitions
 * @param {{expiresAt?: number, leaderless?: boolean}} [routing]
 * @returns {{[nodeId: number]: number[]}}
 */
function oracleReadReplicas(topic, partitions, routing) {
  const partitionMetadata = this.cluster.findTopicPartitionMetadata(topic)
  const preferredReadReplicas = this.preferredReadReplicasPerTopicPartition[topic]
  return partitions.reduce((result, id) => {
    const partitionId = parseInt(id, 10)
    const metadata = partitionMetadata.find((p) => p.partitionId === partitionId)
    if (!metadata) {
      return result
    }
    if (metadata.leader == null) {
      throw new KafkaJSError('Invalid partition metadata', { topic, partitionId, metadata })
    }
    if (metadata.leader < 0) {
      if (routing) routing.leaderless = true
      return result
    }
    let nodeId = metadata.leader
    if (preferredReadReplicas) {
      const { nodeId: preferredReadReplica, expireAt } = preferredReadReplicas[partitionId] || {}
      if (Date.now() >= expireAt) {
        this.logger.debug('Preferred read replica information has expired, using leader', {
          topic,
          partitionId,
          groupId: this.groupId,
          memberId: this.memberId,
          preferredReadReplica,
          leader: metadata.leader,
        })
        delete preferredReadReplicas[partitionId]
        this.routingChanged()
      } else if (preferredReadReplica != null) {
        const offlineReplicas = metadata.offlineReplicas
        if (Array.isArray(offlineReplicas) && offlineReplicas.includes(preferredReadReplica)) {
          this.logger.debug('Preferred read replica is offline, using leader', {
            topic,
            partitionId,
            groupId: this.groupId,
            memberId: this.memberId,
            preferredReadReplica,
            leader: metadata.leader,
          })
        } else {
          nodeId = preferredReadReplica
          if (routing && expireAt < routing.expiresAt) routing.expiresAt = expireAt
        }
      }
    }
    const current = result[nodeId] || []
    return { ...result, [nodeId]: [...current, partitionId] }
  }, {})
}

/**
 * The request builder this client shipped before, with its whole-assignment active set, kept
 * verbatim as the oracle.
 * @this {ConsumerGroup}
 * @param {{topic: string, partitions: number[]}[]} topicPartitions
 * @returns {object[]}
 */
function oracleBuildFetchRequests(topicPartitions) {
  const manager = this.offsetManager
  const activeTopicPartitions = {}
  this.subscriptionState.active().forEach(({ topic, partitions }) => {
    activeTopicPartitions[topic] = new Set(partitions)
  })
  return topicPartitions
    .filter(({ topic }) => this.obligations.current(topic) !== null)
    .map(({ topic, partitions }) => ({
      topic,
      partitions: partitions.reduce((requests, partition) => {
        const baseline = manager.heldOffsetAt(topic, partition)
        if (
          baseline != null &&
          activeTopicPartitions[topic].has(partition) &&
          !this.positionValidator.holdsFetch(topic, partition) &&
          (this.haltedPartitions.size === 0 ||
            !this.haltedPartitions.has(`${topic}|${partition}`)) &&
          !(this.hasSeekGates() && this.isSeekGated(topic, partition))
        ) {
          requests.push({
            partition,
            fetchOffset: manager.nextOffset(topic, partition, baseline).toString(),
            currentLeaderEpoch: this.cluster.leaderEpoch(topic, partition),
            maxBytes: this.maxBytesPerPartition,
          })
        }
        return requests
      }, []),
    }))
    .filter(({ partitions }) => partitions.length)
}

/**
 * A group with only the fields the fetch-round derivation reads, over generated state.
 * @param {object} fields overrides
 * @returns {ConsumerGroup}
 */
const derivationGroup = (fields) =>
  Object.assign(Object.create(ConsumerGroup.prototype), {
    groupId: 'g',
    groupMembership: { memberId: 'm' },
    logger: { debug: jest.fn() },
    routingChanged: jest.fn(),
    preferredReadReplicasPerTopicPartition: {},
    haltedPartitions: new Set(),
    hasSeekGates: () => false,
    maxBytesPerPartition: 1024,
    ...fields,
  })

/**
 * @param {() => number} random
 * @param {number} size how many partition ids to draw from
 * @returns {{ids: Array<number|string>, rows: object[], preferences: object}}
 */
const generateRouting = (random, size) => {
  const ids = Array.from({ length: size }, (_, i) => (random() < 0.05 ? String(i) : i))
  const rows = []
  for (let partitionId = 0; partitionId < size; partitionId++) {
    if (random() < 0.1) continue
    const leader = random() < 0.1 ? -1 : Math.floor(random() * 4)
    rows.push({ partitionId, leader, offlineReplicas: random() < 0.3 ? [7] : [] })
    if (random() < 0.05) rows.push({ partitionId, leader: 9, offlineReplicas: [] })
  }
  rows.sort(() => random() - 0.5)
  const preferences = {}
  for (let id = 0; id < size; id++) {
    if (random() < 0.3) {
      const expired = random() < 0.3
      preferences[id] = { nodeId: random() < 0.5 ? 7 : 5, expireAt: expired ? 0 : 1e15 + id }
    }
  }
  return { ids: random() < 0.5 ? ids : ids.reverse(), rows, preferences }
}

describe('Consumer > ConsumerGroup > fetch-round derivation', () => {
  it('routes generated assignments exactly as the linear search did', () => {
    for (let seed = 1; seed <= 300; seed++) {
      const random = mulberry32(seed)
      const size = [0, 1, 3, 8, 9, 17, 64, 200][seed % 8]
      const { ids, rows, preferences } = generateRouting(random, size)
      const run = (route) => {
        const group = derivationGroup({
          cluster: { findTopicPartitionMetadata: () => rows },
          preferredReadReplicasPerTopicPartition: { t: structuredClone(preferences) },
        })
        const routing = { expiresAt: Infinity, leaderless: false }
        const result = route.call(group, 't', ids, routing)
        return {
          result,
          routing,
          remaining: { ...group.preferredReadReplicasPerTopicPartition.t },
          changed: group.routingChanged.mock.calls.length,
          logs: group.logger.debug.mock.calls,
        }
      }
      try {
        const actual = run(ConsumerGroup.prototype.findReadReplicaForPartitions)
        expect(actual).toEqual(run(oracleReadReplicas))
        expect(Object.keys(actual.result)).toEqual(Object.keys(run(oracleReadReplicas).result))
      } catch (error) {
        error.message += ` seed=${seed}`
        throw error
      }
    }
  })

  it('builds the same request entries as the whole-assignment active set', () => {
    for (let seed = 1; seed <= 200; seed++) {
      const random = mulberry32(seed)
      const topics = Array.from({ length: 1 + (seed % 6) }, (_, i) => `t${i}`)
      const assignment = topics.map((topic) => ({
        topic,
        partitions: Array.from({ length: 1 + Math.floor(random() * 12) }, (_, i) => i),
      }))
      const state = new SubscriptionState()
      state.assign(assignment)
      for (const { topic, partitions } of assignment) {
        const roll = random()
        if (roll < 0.2) state.pause([{ topic }])
        if (roll < 0.1)
          state.resume([{ topic, partitions: partitions.filter(() => random() < 0.5) }])
        if (roll >= 0.2 && roll < 0.5) {
          state.pause([{ topic, partitions: partitions.filter(() => random() < 0.3) }])
        }
      }
      if (random() < 0.3) state.hold([{ topic: topics[0], partitions: [0, 1] }], {})
      const coin = (salt) => (topic, partition) =>
        mulberry32(seed * 1000 + salt + partition)() < 0.15
      const group = derivationGroup({
        subscriptionState: state,
        offsetManager: {
          heldOffsetAt: (topic, partition) => (coin(1)(topic, partition) ? null : '4'),
          nextOffset: (topic, partition, baseline) => BigInt(baseline) + BigInt(partition),
        },
        obligations: { current: (topic) => (topic === 't5' ? null : {}) },
        positionValidator: { holdsFetch: coin(2) },
        haltedPartitions: new Set(random() < 0.5 ? ['t0|2', 't1|0'] : []),
        hasSeekGates: () => true,
        isSeekGated: coin(3),
        cluster: { leaderEpoch: (topic, partition) => partition % 3 },
      })
      // Node-filtered parts as a round sees them: every topic, a subset of its partitions.
      const parts = state.assigned().map(({ topic, partitions }) => ({
        topic,
        partitions: partitions.filter(() => random() < 0.6),
      }))
      try {
        expect(group.buildFetchRequests(parts)).toEqual(oracleBuildFetchRequests.call(group, parts))
      } catch (error) {
        error.message += ` seed=${seed}`
        throw error
      }
    }
  })

  it('follows every change of its inputs from one round to the next', () => {
    const rows = [0, 1, 2].map((partitionId) => ({ partitionId, leader: 1 }))
    const cluster = { findTopicPartitionMetadata: () => rows, leaderEpoch: () => 0 }
    const state = new SubscriptionState()
    state.assign([{ topic: 't', partitions: [0, 1, 2] }])
    const group = derivationGroup({
      cluster,
      subscriptionState: state,
      offsetManager: { heldOffsetAt: () => '0', nextOffset: () => 0n },
      obligations: { current: () => ({}) },
      positionValidator: { holdsFetch: () => false },
    })
    const route = () => group.filterPartitionsByNode(1, state.assigned())[0].partitions
    const fetched = () =>
      group
        .buildFetchRequests(state.assigned())
        .flatMap(({ partitions }) => partitions.map(({ partition }) => partition))

    expect(route()).toEqual([0, 1, 2])
    rows[1].leader = 2
    expect(route()).toEqual([0, 2])
    cluster.findTopicPartitionMetadata = () => [{ partitionId: 0, leader: 2 }]
    expect(route()).toEqual([])
    group.preferredReadReplicasPerTopicPartition.t = { 0: { nodeId: 1, expireAt: 1e15 } }
    expect(route()).toEqual([0])

    state.pause([{ topic: 't', partitions: [1] }])
    expect(fetched()).toEqual([0, 2])
    state.resume([{ topic: 't', partitions: [1] }])
    expect(fetched()).toEqual([0, 1, 2])
    const token = {}
    state.hold([{ topic: 't', partitions: [2] }], token)
    expect(fetched()).toEqual([0, 1])
    state.release(token)
    state.assign([{ topic: 't', partitions: [1] }])
    expect(group.buildFetchRequests([{ topic: 't', partitions: [0, 1] }])).toEqual([
      {
        topic: 't',
        partitions: [{ partition: 1, fetchOffset: '0', currentLeaderEpoch: 0, maxBytes: 1024 }],
      },
    ])
  })

  it('reads each metadata row a bounded number of times, however many partitions a topic has', () => {
    const reads = { count: 0 }
    const rows = Array.from({ length: 2000 }, (_, id) => {
      const row = { leader: id % 3 }
      Object.defineProperty(row, 'partitionId', {
        get: () => {
          reads.count += 1
          return id
        },
      })
      return row
    })
    const group = derivationGroup({ cluster: { findTopicPartitionMetadata: () => rows } })
    const ids = Array.from({ length: 2000 }, (_, id) => id)

    const result = group.findReadReplicaForPartitions('t', ids)

    expect(result[0]).toHaveLength(667)
    expect(reads.count).toBeLessThanOrEqual(2 * rows.length)
  })

  it('consults the pause and hold state only for the partitions the round asks for', () => {
    const state = new SubscriptionState()
    const assignment = Array.from({ length: 50 }, (_, i) => ({
      topic: `t${i}`,
      partitions: Array.from({ length: 40 }, (_, p) => p),
    }))
    state.assign(assignment)
    const isPaused = jest.spyOn(state, 'isPaused')
    const isHeld = jest.spyOn(state, 'isHeld')
    const group = derivationGroup({
      subscriptionState: state,
      offsetManager: { heldOffsetAt: () => '0', nextOffset: () => 0n },
      obligations: { current: () => ({}) },
      positionValidator: { holdsFetch: () => false },
      cluster: { leaderEpoch: () => 0 },
    })
    const parts = assignment.map(({ topic }) => ({
      topic,
      partitions: topic === 't7' ? [3, 9] : [],
    }))

    expect(group.buildFetchRequests(parts)).toHaveLength(1)
    expect(isPaused).toHaveBeenCalledTimes(2)
    expect(isHeld).toHaveBeenCalledTimes(2)
  })
})

describe('Consumer > ConsumerGroup > fetch round over the wire', () => {
  afterEach(async () => {
    for (const rig of rigs.splice(0)) await rig.broker.disconnect()
  })
  const rigs = []

  /**
   * Runs one round over generated leaders and pauses; the answer lists the requested partitions
   * in reverse order.
   * @param {number} seed
   * @param {boolean} oracle whether the group derives the round with the previous code
   * @returns {Promise<{frames: Buffer[], batches: object[]}>}
   */
  const round = async (seed, oracle) => {
    const random = mulberry32(seed)
    const partitions = Array.from({ length: 5 + (seed % 40) }, (_, i) => i).sort(
      () => random() - 0.5
    )
    // Partition 0 stays on this node and unpaused, so every round sends a Fetch.
    const partitionLeaders = Object.fromEntries(
      partitions.map((p) => [p, p === 0 || random() < 0.6 ? NODE_ID : 1 + Math.floor(random() * 2)])
    )
    const rig = await createFetchGroup({ partitions, partitionLeaders, knownNodes: [0, 1, 2] })
    rigs.push(rig)
    if (oracle) {
      rig.group.findReadReplicaForPartitions = oracleReadReplicas
      rig.group.buildFetchRequests = oracleBuildFetchRequests
    }
    rig.group.pause([
      { topic: TOPIC, partitions: partitions.filter((p) => p !== 0 && random() < 0.2) },
    ])
    rig.setResponse(() =>
      fetchResponse(
        rig
          .fetchRequestPartitions()
          .reverse()
          .map((partition) => ({ partition, errorCode: 0, records: true }))
      )
    )
    // The broker shuffles a Fetch's partitions; both runs get the same shuffle.
    const shuffle = jest.spyOn(Math, 'random').mockImplementation(mulberry32(seed + 1000))
    const batches = await rig.group.fetch(NODE_ID).finally(() => shuffle.mockRestore())
    return {
      frames: rig.fetchFrames(),
      batches: batches.map(({ topic, partition, fetchedOffset, messages }) => ({
        topic,
        partition,
        fetchedOffset,
        offsets: messages.map(({ offset }) => offset),
      })),
    }
  }

  it('sends the same Fetch frames and delivers the same batches as the linear search did', async () => {
    for (let seed = 1; seed <= 12; seed++) {
      const actual = await round(seed, false)
      const expected = await round(seed, true)
      expect(actual.batches.length).toBeGreaterThan(0)
      expect(actual).toEqual(expected)
    }
  })

  it('looks up each answered partition in its request once', async () => {
    const partitions = Array.from({ length: 300 }, (_, i) => i)
    const rig = await createFetchGroup({ partitions })
    rigs.push(rig)
    const reads = { count: 0 }
    const build = rig.group.buildFetchRequests.bind(rig.group)
    rig.group.buildFetchRequests = (parts) =>
      build(parts).map(({ topic, partitions: entries }) => ({
        topic,
        partitions: entries.map((entry) => {
          const { partition, ...rest } = entry
          return Object.defineProperty(rest, 'partition', {
            enumerable: true,
            get: () => {
              reads.count += 1
              return partition
            },
          })
        }),
      }))
    rig.setResponse(
      fetchResponse(partitions.map((partition) => ({ partition, errorCode: 0, records: true })))
    )

    const batches = await rig.group.fetch(NODE_ID)

    expect(batches).toHaveLength(300)
    expect(reads.count).toBeLessThanOrEqual(5 * partitions.length)
  })
})
