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
