const { Kafka, logLevel } = require('../../../index')
const { createScriptedCluster, groupScript } = require('../../../testHelpers/scriptedCluster')
const Cluster = require('../../cluster')
const { RecordBatch } = require('../../protocol/recordBatch/v0')
const RecordEncoder = require('../../protocol/recordBatch/record/v0')
const { TOPIC, TOPIC_ID } = require('./fixtures/fetchHarness')

const NODE = { id: 0, host: 'node-0', port: 9092 }
const NEW_ID = '11121314-1516-1718-191a-1b1c1d1e1f20'
const THIRD_ID = '21222324-2526-2728-292a-2b2c2d2e2f30'
const GROUP = 'debt-group'
const BUDGET_MS = 10000

/** @param {() => boolean} predicate @param {string} label @returns {Promise<void>} resolves when it holds */
const until = async (predicate, label) => {
  const deadline = Date.now() + BUDGET_MS
  while (!predicate()) {
    if (Date.now() > deadline) throw new Error(`Did not happen in time: ${label}`)
    await new Promise((resolve) => setTimeout(resolve, 5))
  }
}

/**
 * A scripted broker whose topic can be recreated: the group holds `committed` for the old
 * incarnation, the log starts at `earliest`, and every OffsetCommit it confirms is kept in `commits`.
 * @param {object} [options]
 * @param {string} [options.committed='99'] the old incarnation's committed offset, partition 0 and 1
 * @returns {{ cluster: object, kafka: Kafka, broker: { topicId: string, committed: object, commits: object[], failCommit: boolean } }}
 */
const scripted = ({ committed = '99' } = {}) => {
  const broker = {
    topicId: TOPIC_ID,
    committed: { 0: committed, 1: committed },
    commits: [],
    failCommit: false,
  }
  const cluster = groupScript(createScriptedCluster({ nodes: [NODE] }), { partitions: 3 })
    .answer('Metadata', () => ({
      brokers: [{ nodeId: NODE.id, host: NODE.host, port: NODE.port, rack: null }],
      controllerId: NODE.id,
      topics: [
        {
          errorCode: 0,
          name: TOPIC,
          topicId: broker.topicId,
          isInternal: false,
          topicAuthorizedOperations: -2147483648,
          partitions: [0, 1, 2].map((partitionIndex) => ({
            errorCode: 0,
            partitionIndex,
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
          offset: broker.committed[partition] ?? '-1',
          leaderEpoch: -1,
          metadata: null,
          errorCode: 0,
        })),
      })),
    }))
    .answer('OffsetCommit', ({ topics }) => ({
      topics: topics.map((entry) => ({
        topic: entry.topic,
        partitions: entry.partitions.map(({ partition, offset }) => {
          if (!broker.failCommit) {
            broker.committed[partition] = String(offset)
            broker.commits.push({ topic: entry.topic, partition, offset: String(offset) })
          }
          return { partition, errorCode: broker.failCommit ? 7 : 0 }
        }),
      })),
    }))
    .answer('Fetch', async ({ maxWaitMs, topics }) => {
      await new Promise((resolve) => setTimeout(resolve, maxWaitMs))
      return {
        partitions: topics[0].partitions.map(({ partition }) => ({
          partition,
          errorCode: 0,
          records: false,
        })),
      }
    })
  const kafka = new Kafka(
    cluster.kafkaOptions({
      logLevel: logLevel.NOTHING,
      retry: { retries: 1, initialRetryTime: 10, maxRetryTime: 20 },
    })
  )
  return { cluster, kafka, broker }
}

const flush = () => new Promise((resolve) => setTimeout(resolve, 60))

describe('A recreated topic keeps owing its reset through manual assignment', () => {
  let wire
  let stage
  const consumers = []

  /** @returns {object} a consumer the suite disconnects afterwards */
  const track = (consumer) => {
    consumers.push(consumer)
    return consumer
  }

  /** @returns {object} the Cluster of the first consumer the test built */
  const clusterOf = () => stage.mock.contexts[0]

  /**
   * Starts a manual consumer on partition 0 and waits until it resolved the old incarnation's
   * committed offset.
   * @param {object} [options] consumer config
   * @param {string} [expected] the position the first resolution reaches
   * @returns {Promise<{ consumer: object, run: () => Promise<void> }>}
   */
  const started = async (options = { groupId: GROUP }, expected = '99') => {
    const consumer = track(wire.kafka.consumer({ maxWaitTimeInMs: 20, ...options }))
    const run = () => consumer.run({ autoCommit: false, eachMessage: async () => {} })
    await consumer.connect()
    await consumer.assign([{ topic: TOPIC, partitions: [0] }], { offsetReset: 'earliest' })
    await run()
    expect(await consumer.position({ topic: TOPIC, partition: 0 })).toBe(expected)
    return { consumer, run }
  }

  /** The topic is deleted and created again: a new id, and a refresh that notices. */
  const recreate = async () => {
    wire.broker.topicId = NEW_ID
    await clusterOf().refreshMetadata()
  }

  /** @param {object} consumer @returns {Promise<string>} partition 0's position */
  const position = (consumer) => consumer.position({ topic: TOPIC, partition: 0 })

  beforeEach(() => {
    wire = scripted()
    stage = jest.spyOn(Cluster.prototype, 'stageMetadata')
  })

  afterEach(async () => {
    for (const consumer of consumers.splice(0)) await consumer.disconnect().catch(() => {})
    jest.restoreAllMocks()
  })

  /** Each way the next run is built from the same manual selection. */
  const REBUILDS = {
    'a stop and a run': async ({ consumer, run }) => {
      await consumer.stop()
      await run()
    },
    'an identical manual replacement': async ({ consumer, run }) => {
      await consumer.stop()
      await consumer.assign([{ topic: TOPIC, partitions: [0] }], { offsetReset: 'earliest' })
      await run()
    },
  }

  describe.each(Object.entries(REBUILDS))('across %s', (_, rebuild) => {
    test('a ListOffsets answer held over the rebuild leaves the next resolution at the new log start', async () => {
      const handle = await started()
      const gate = wire.cluster.schedule.hold({ api: 'ListOffsets' })
      await recreate()
      await gate.arrived()
      const rebuilding = rebuild(handle)
      await flush()
      gate.release()
      await rebuilding
      expect(await position(handle.consumer)).toBe('0')
    })

    test('an OffsetFetch answer held over the rebuild, then a ListOffsets, still ends at the new log start', async () => {
      const handle = await started()
      const gate = wire.cluster.schedule.hold({ api: 'OffsetFetch' })
      await recreate()
      await gate.arrived()
      const rebuilding = rebuild(handle)
      await flush()
      gate.release()
      await rebuilding
      expect(await position(handle.consumer)).toBe('0')
    })

    test('a reset resolved before the rebuild, with no durable commit, is still owed after it', async () => {
      const handle = await started()
      await recreate()
      expect(await position(handle.consumer)).toBe('0')
      await rebuild(handle)
      expect(await position(handle.consumer)).toBe('0')
      expect(wire.broker.commits).toEqual([])
    })
  })

  test('a confirmed concrete commit for the new incarnation settles the debt', async () => {
    const { consumer, run } = await started()
    await recreate()
    expect(await position(consumer)).toBe('0')
    await consumer.commitOffsets([{ topic: TOPIC, partition: 0, offset: '3' }])
    await consumer.stop()
    await consumer.assign([{ topic: TOPIC, partitions: [0, 1, 2] }])
    await run()
    expect(await position(consumer)).toBe('3')
    expect(wire.broker.commits).toEqual([{ topic: TOPIC, partition: 0, offset: '3' }])
  })

  test('a refused commit leaves the debt owed', async () => {
    const handle = await started()
    await recreate()
    wire.broker.failCommit = true
    await expect(
      handle.consumer.commitOffsets([{ topic: TOPIC, partition: 0, offset: '3' }])
    ).rejects.toBeDefined()
    await handle.consumer.stop()
    await handle.run()
    expect(await position(handle.consumer)).toBe('0')
    expect(wire.broker.commits).toEqual([])
  })

  test('a commit answered after a newer recreation does not settle the newer debt', async () => {
    const handle = await started()
    await recreate()
    await position(handle.consumer)
    const gate = wire.cluster.schedule.hold({ api: 'OffsetCommit' })
    const commit = handle.consumer.commitOffsets([{ topic: TOPIC, partition: 0, offset: '3' }])
    await gate.arrived()
    wire.broker.topicId = THIRD_ID
    await clusterOf().refreshMetadata()
    gate.release()
    await commit.catch(() => {})
    await handle.consumer.stop()
    await handle.run()
    expect(await position(handle.consumer)).toBe('0')
  })

  test('stopping a manual consumer and running it again raises no crash', async () => {
    const handle = await started()
    const crashes = []
    handle.consumer.on(handle.consumer.events.CRASH, ({ payload }) => crashes.push(payload))
    await handle.consumer.stop()
    await handle.run()
    await flush()
    expect(crashes).toEqual([])
    expect(handle.consumer.assignment()).toEqual([{ topic: TOPIC, partitions: [0] }])
  })

  test('a sentinel seek publishes a boundary without settling the debt', async () => {
    const handle = await started()
    await recreate()
    handle.consumer.seek({ topic: TOPIC, partition: 0, offset: '-2' })
    expect(await position(handle.consumer)).toBe('0')
    await handle.consumer.stop()
    await handle.run()
    expect(await position(handle.consumer)).toBe('0')
    expect(wire.broker.commits).toEqual([])
  })

  test('a commit for another partition leaves the unassigned partition owing', async () => {
    const handle = await started()
    await recreate()
    await position(handle.consumer)
    await handle.consumer.commitOffsets([{ topic: TOPIC, partition: 0, offset: '2' }])
    await handle.consumer.stop()
    await handle.consumer.assign([{ topic: TOPIC, partitions: [1] }], { offsetReset: 'earliest' })
    await handle.run()
    expect(await handle.consumer.position({ topic: TOPIC, partition: 1 })).toBe('0')
  })

  describe('without a group', () => {
    test('a local reset never settles: every later resolution starts again at the new log start', async () => {
      const handle = await started({}, '0')
      // the private store has no committed offset, so the old-incarnation position is the local one
      await recreate()
      expect(await position(handle.consumer)).toBe('0')
      handle.consumer.seek({ topic: TOPIC, partition: 0, offset: '5' })
      expect(await position(handle.consumer)).toBe('5')
      await handle.consumer.stop()
      await handle.run()
      expect(await position(handle.consumer)).toBe('0')
      for (const api of ['FindCoordinator', 'OffsetFetch', 'OffsetCommit']) {
        expect(wire.cluster.requests().filter((request) => request.api === api)).toEqual([])
      }
    })

    test("a group-free consumer neither clears nor changes a sibling group consumer's debt", async () => {
      const grouped = await started()
      const free = track(wire.kafka.consumer({ maxWaitTimeInMs: 20 }))
      await free.connect()
      await free.assign([{ topic: TOPIC, partitions: [0] }], { offsetReset: 'earliest' })
      await free.run({ eachMessage: async () => {} })
      await recreate()
      expect(await position(free)).toBe('0')
      await grouped.consumer.stop()
      await grouped.run()
      expect(await position(grouped.consumer)).toBe('0')
    })
  })
})

/**
 * @param {number} offset the batch's only offset
 * @param {boolean} [corrupt=false] flip one byte inside the CRC-covered region
 * @returns {Promise<Buffer>} a one-record batch
 */
const batchAt = async (offset, corrupt = false) => {
  const batch = Buffer.from(
    (
      await RecordBatch({
        firstOffset: String(offset),
        firstTimestamp: 1700000000000,
        maxTimestamp: 1700000000000,
        records: [
          RecordEncoder({ key: Buffer.from('key'), value: Buffer.from(`value-${offset}`) }),
        ],
      })
    ).buffer
  )
  // FirstOffset 8, Length 4, LeaderEpoch 4, Magic 1, CRC 4, Attributes 2, LastOffsetDelta 4,
  // timestamps 16, ProducerId 8: the next byte is covered by the CRC and otherwise inert.
  if (corrupt) batch[8 + 4 + 4 + 1 + 4 + 2 + 4 + 8 + 8 + 8] ^= 0x01
  return batch
}

describe('A valid prefix before a halt, under a commit cadence and an ordinary close', () => {
  let wire
  const consumers = []

  beforeEach(async () => {
    wire = scripted({ committed: '0' })
    const prefix = Buffer.concat([await batchAt(0), await batchAt(1, true)])
    wire.cluster.answer('Fetch', async ({ maxWaitMs, topics }) => {
      const [{ partition, fetchOffset }] = topics[0].partitions
      if (String(fetchOffset) === '0') {
        return { partitions: [{ partition, errorCode: 0, records: prefix }] }
      }
      await new Promise((resolve) => setTimeout(resolve, maxWaitMs))
      return { partitions: [{ partition, errorCode: 0, records: false }] }
    })
  })

  afterEach(async () => {
    for (const consumer of consumers.splice(0)) await consumer.disconnect().catch(() => {})
  })

  /**
   * Runs one manual consumer over the prefix-then-corrupt log until its partition halts, then
   * closes it the ordinary way.
   * @param {object} config consumer config
   * @param {object} runOptions run options
   * @param {object} [assignOptions] reset options
   * @returns {Promise<{ delivered: string[], halts: object[], consumer: object }>}
   */
  const play = async (config, runOptions, assignOptions = { offsetReset: 'earliest' }) => {
    const consumer = wire.kafka.consumer({ maxWaitTimeInMs: 20, ...config })
    consumers.push(consumer)
    const delivered = []
    const halts = []
    consumer.on(consumer.events.PARTITION_HALTED, ({ payload }) => halts.push(payload))
    await consumer.connect()
    await consumer.assign([{ topic: TOPIC, partitions: [0] }], assignOptions)
    await consumer.run({
      ...runOptions,
      eachMessage: async ({ message }) => {
        delivered.push(message.offset)
      },
    })
    await until(() => halts.length === 1, 'the halt')
    return { delivered, halts, consumer }
  }

  test('a cadence of one commits the prefix boundary and never crosses the unreadable batch', async () => {
    const { delivered, halts, consumer } = await play(
      { groupId: GROUP },
      { autoCommit: true, autoCommitThreshold: 1 }
    )
    await until(() => wire.broker.commits.some(({ offset }) => offset === '1'), 'the prefix commit')
    await consumer.disconnect()

    expect(delivered).toEqual(['0'])
    expect(halts[0]).toEqual(expect.objectContaining({ partition: 0, reason: 'checksum' }))
    expect(wire.broker.commits.every(({ offset }) => Number(offset) <= 1)).toBe(true)
    expect(wire.broker.committed[0]).toBe('1')
  })

  test('with automatic commits off nothing is committed, before the halt or at the close', async () => {
    const { delivered, consumer } = await play({ groupId: GROUP }, { autoCommit: false })
    await flush()
    await consumer.disconnect()

    expect(delivered).toEqual(['0'])
    expect(wire.broker.commits).toEqual([])
  })

  test('an explicit commit still writes exactly the offset the caller named', async () => {
    const { consumer } = await play({ groupId: GROUP }, { autoCommit: false })
    await consumer.commitOffsets([{ topic: TOPIC, partition: 0, offset: '1' }])
    await consumer.disconnect()

    expect(wire.broker.commits).toEqual([{ topic: TOPIC, partition: 0, offset: '1' }])
  })

  test('a none reset with no committed offset halts without any derived progress', async () => {
    wire.broker.committed = {}
    const { delivered, halts, consumer } = await play(
      { groupId: GROUP },
      { autoCommit: true, autoCommitThreshold: 1 },
      { offsetReset: 'none' }
    )
    await flush()
    await consumer.disconnect()

    expect(delivered).toEqual([])
    expect(halts[0]).toEqual(expect.objectContaining({ reason: 'no-offset', offset: null }))
    expect(wire.broker.commits).toEqual([])
  })

  test('without a group the halt names no group and no commit is ever sent', async () => {
    const { delivered, halts, consumer } = await play({}, {})
    await flush()
    await consumer.disconnect()

    expect(delivered).toEqual(['0'])
    expect(halts[0]).toEqual(expect.objectContaining({ groupId: null, reason: 'checksum' }))
    expect(wire.cluster.requests().filter(({ api }) => api === 'OffsetCommit')).toEqual([])
  })
})
