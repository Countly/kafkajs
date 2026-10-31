const { Kafka, logLevel } = require('../../../index')
const { createScriptedCluster, groupScript } = require('../../../testHelpers/scriptedCluster')
const { RecordBatch } = require('../../protocol/recordBatch/v0')
const RecordEncoder = require('../../protocol/recordBatch/record/v0')
const { TOPIC } = require('./fixtures/fetchHarness')
const { KafkaJSConnectionError } = require('../../errors')
const Cluster = require('../../cluster')

const NODE = { id: 0, host: 'node-0', port: 9092 }
const BUDGET_MS = 10000
const MEMBERSHIP_APIS = [
  'JoinGroup',
  'SyncGroup',
  'Heartbeat',
  'LeaveGroup',
  'ConsumerGroupHeartbeat',
]

/**
 * One write of the production encoder, so the decoder reads it as it reads a broker's.
 * @param {number} offset first and only offset of the batch
 * @returns {Promise<Buffer>} a one-record batch whose value names its offset
 */
const batchAt = async (offset) =>
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
      })
    ).buffer
  )

/**
 * A scripted three-partition topic whose every partition holds records at offsets `0..last`, one
 * record per batch.
 * @param {object} [options]
 * @param {number} [options.last=2] the last record offset of every partition
 * @param {Record<number, string>} [options.committed] committed offsets by partition
 * @returns {Promise<{ cluster: object, kafka: Kafka, errors: object[] }>} the scripted cluster and a client
 */
const scripted = async ({ last = 2, committed = {} } = {}) => {
  const batches = {}
  for (let partition = 0; partition < 3; partition++) {
    for (let offset = 0; offset <= last; offset++) {
      batches[`${partition}|${offset}`] = await batchAt(offset)
    }
  }
  const errors = []
  const cluster = groupScript(createScriptedCluster({ nodes: [NODE] }), {
    partitions: 3,
    committed,
  }).answer('Fetch', async ({ maxWaitMs, topics }) => {
    const partitions = topics[0].partitions.map(({ partition, fetchOffset }) => ({
      partition,
      errorCode: 0,
      records: batches[`${partition}|${fetchOffset}`] ?? false,
      abortedTransactions: [],
    }))
    if (partitions.every(({ records }) => records === false)) {
      await new Promise((resolve) => setTimeout(resolve, maxWaitMs))
    }
    return { partitions }
  })
  const kafka = new Kafka(
    cluster.kafkaOptions({
      logLevel: logLevel.ERROR,
      logCreator:
        () =>
        ({ level, log }) => {
          if (level === logLevel.ERROR) errors.push(log)
        },
      retry: { retries: 2, initialRetryTime: 10, maxRetryTime: 20 },
    })
  )
  return { cluster, kafka, errors }
}

/** @param {() => boolean} predicate @param {string} label @returns {Promise<void>} resolves when it holds */
const until = async (predicate, label) => {
  const deadline = Date.now() + BUDGET_MS
  while (!predicate()) {
    if (Date.now() > deadline) throw new Error(`Did not happen in time: ${label}`)
    await new Promise((resolve) => setTimeout(resolve, 5))
  }
}

/** @param {object} cluster @param {string} api @returns {object[]} the frames of one API */
const frames = (cluster, api) => cluster.requests().filter((request) => request.api === api)

describe('A manual consumer over a scripted broker', () => {
  let wire
  const consumers = []

  /** @returns {object} a consumer that the suite disconnects afterwards */
  const track = (consumer) => {
    consumers.push(consumer)
    return consumer
  }

  afterEach(async () => {
    const current = wire
    wire = null
    for (const consumer of consumers.splice(0)) await consumer.disconnect().catch(() => {})
    if (current) current.cluster.assertClean()
  })

  test('with a group delivers the selected partitions with simple offsets and no membership frame', async () => {
    wire = await scripted()
    const { cluster, kafka } = wire
    const consumer = track(kafka.consumer({ groupId: 'direct-group', maxWaitTimeInMs: 20 }))
    const delivered = []
    await consumer.connect()
    await consumer.assign([{ topic: TOPIC, partitions: [2, 0] }], { offsetReset: 'earliest' })
    await consumer.run({
      autoCommit: false,
      eachMessage: async ({ partition, message }) => {
        delivered.push(`${partition}:${message.offset}`)
      },
    })
    await until(() => delivered.length === 6, 'six records')
    expect(delivered.sort()).toEqual(['0:0', '0:1', '0:2', '2:0', '2:1', '2:2'])
    for (const api of MEMBERSHIP_APIS) expect(frames(cluster, api)).toEqual([])
    expect(frames(cluster, 'OffsetFetch').length).toBeGreaterThan(0)
    expect(frames(cluster, 'FindCoordinator').length).toBeGreaterThan(0)
  })

  test('without a group delivers with no coordinator, offset or membership frame', async () => {
    wire = await scripted()
    const { cluster, kafka } = wire
    const consumer = track(kafka.consumer({ maxWaitTimeInMs: 20, heartbeatInterval: 20 }))
    const delivered = []
    await consumer.connect()
    await consumer.assign([{ topic: TOPIC, partitions: [1] }], { offsetReset: 'earliest' })
    await consumer.run({
      eachMessage: async ({ partition, message }) => {
        delivered.push(`${partition}:${message.offset}`)
      },
    })
    await until(() => delivered.length === 3, 'three records')
    await new Promise((resolve) => setTimeout(resolve, 100))
    expect(delivered).toEqual(['1:0', '1:1', '1:2'])
    for (const api of [...MEMBERSHIP_APIS, 'FindCoordinator', 'OffsetFetch', 'OffsetCommit']) {
      expect(frames(cluster, api)).toEqual([])
    }
    expect(frames(cluster, 'ListOffsets').length).toBeGreaterThan(0)
  })

  test('without a group refuses autoCommit true and every group-only call before any wire', async () => {
    wire = await scripted()
    const { cluster, kafka } = wire
    const consumer = track(kafka.consumer({}))
    await consumer.connect()
    await consumer.assign([{ topic: TOPIC, partitions: [0] }])
    await expect(consumer.run({ autoCommit: true, eachMessage: async () => {} })).rejects.toThrow(
      'autoCommit requires a groupId'
    )
    const message = 'Consumer groupId must be a non-empty string.'
    await expect(consumer.subscribe({ topic: TOPIC })).rejects.toThrow(message)
    await expect(consumer.commitOffsets([])).rejects.toThrow(message)
    await expect(consumer.committed([])).rejects.toThrow(message)
    await expect(consumer.describeGroup()).rejects.toThrow(message)
    expect(() => consumer.groupMetadata()).toThrow(message)
    expect(frames(cluster, 'OffsetFetch')).toEqual([])
    expect(frames(cluster, 'FindCoordinator')).toEqual([])
  })

  test('a handler of a consumer without a group may check automatic commits but not name offsets', async () => {
    wire = await scripted()
    const { cluster, kafka } = wire
    const consumer = track(kafka.consumer({ maxWaitTimeInMs: 20 }))
    const outcomes = []
    await consumer.connect()
    await consumer.assign([{ topic: TOPIC, partitions: [0] }], { offsetReset: 'earliest' })
    await consumer.run({
      eachBatch: async ({ batch, commitOffsetsIfNecessary }) => {
        if (outcomes.length > 0) return
        await commitOffsetsIfNecessary()
        outcomes.push(
          await commitOffsetsIfNecessary({
            topics: [{ topic: TOPIC, partitions: [{ partition: 0, offset: batch.lastOffset() }] }],
          }).then(
            () => 'allowed',
            (error) => error.message
          )
        )
      },
    })
    await until(() => outcomes.length === 1, 'the handler check')
    expect(outcomes).toEqual(['Consumer groupId must be a non-empty string.'])
    expect(frames(cluster, 'OffsetCommit')).toEqual([])
  })

  test('commits with simple-consumer identity and never a member id', async () => {
    wire = await scripted()
    const { cluster, kafka } = wire
    const consumer = track(
      kafka.consumer({ groupId: 'simple-group', groupInstanceId: 'static-1', maxWaitTimeInMs: 20 })
    )
    await consumer.connect()
    await consumer.assign([{ topic: TOPIC, partitions: [0] }])
    await consumer.run({ autoCommit: false, eachMessage: async () => {} })
    expect(consumer.groupMetadata()).toEqual({
      groupId: 'simple-group',
      generationId: -1,
      memberId: '',
      groupInstanceId: null,
    })
    await consumer.commitOffsets([{ topic: TOPIC, partition: 0, offset: '5', metadata: 'note' }])
    const [commit] = frames(cluster, 'OffsetCommit')
    expect(commit.fields).toEqual(
      expect.objectContaining({
        groupId: 'simple-group',
        groupGenerationId: -1,
        memberId: '',
        groupInstanceId: null,
      })
    )
    const [fetched] = frames(cluster, 'OffsetFetch')
    expect(fetched.fields).toEqual(expect.objectContaining({ groupId: 'simple-group' }))
    expect(fetched.fields.memberId ?? null).toBeNull()
    expect(fetched.fields.memberEpoch ?? -1).toBe(-1)
    expect(fetched.fields.requireStable).toBe(true)
    for (const api of MEMBERSHIP_APIS) expect(frames(cluster, api)).toEqual([])
  })

  test('committed reports null metadata for a partition without a commit, as the broker answers it', async () => {
    wire = await scripted()
    const { cluster, kafka } = wire
    const cells = {
      0: { offset: '-1', metadata: '' },
      1: { offset: '7', metadata: 'note' },
      2: { offset: '3', metadata: '' },
    }
    cluster.answer('OffsetFetch', ({ groupId, topics }) => ({
      groupId,
      topics: topics.map((entry) => ({
        topic: entry.topic,
        partitions: entry.partitions.map(({ partition }) => ({
          partition,
          ...cells[partition],
          leaderEpoch: -1,
          errorCode: 0,
        })),
      })),
    }))
    const consumer = track(kafka.consumer({ groupId: 'observer-group' }))
    await consumer.connect()
    expect(
      await consumer.committed([0, 1, 2].map((partition) => ({ topic: TOPIC, partition })))
    ).toEqual([
      { topic: TOPIC, partition: 0, offset: null, metadata: null },
      { topic: TOPIC, partition: 1, offset: '7', metadata: 'note' },
      { topic: TOPIC, partition: 2, offset: '3', metadata: '' },
    ])
  })

  test('committed reads broker truth of unassigned pairs without moving any position', async () => {
    wire = await scripted({ committed: { 1: '7' } })
    const { cluster, kafka } = wire
    const consumer = track(kafka.consumer({ groupId: 'observer-group' }))
    await consumer.connect()
    expect(await consumer.committed([])).toEqual([])
    expect(frames(cluster, 'OffsetFetch')).toEqual([])
    await expect(
      consumer.committed([
        { topic: TOPIC, partition: 1 },
        { topic: TOPIC, partition: 1 },
      ])
    ).rejects.toThrow('unique')
    await expect(consumer.committed([{ topic: TOPIC, partition: -1 }])).rejects.toThrow()
    expect(frames(cluster, 'OffsetFetch')).toEqual([])
    expect(
      await consumer.committed([
        { topic: TOPIC, partition: 1 },
        { topic: TOPIC, partition: 0 },
      ])
    ).toEqual([
      { topic: TOPIC, partition: 1, offset: '7', metadata: null },
      { topic: TOPIC, partition: 0, offset: null, metadata: null },
    ])
    const [fetched] = frames(cluster, 'OffsetFetch')
    expect(fetched.fields.requireStable).toBe(true)
  })

  test('exposes sorted defensive getters and follows the mode transitions', async () => {
    wire = await scripted()
    const { kafka } = wire
    const consumer = track(kafka.consumer({ groupId: 'modes-group', maxWaitTimeInMs: 20 }))
    await consumer.connect()
    expect(consumer.assignment()).toEqual([])
    expect(consumer.subscription()).toEqual([])
    await consumer.subscribe({ topic: TOPIC })
    expect(consumer.subscription()).toEqual([TOPIC])
    const copy = consumer.subscription()
    copy.push('mutated')
    expect(consumer.subscription()).toEqual([TOPIC])
    await expect(consumer.assign([{ topic: TOPIC, partitions: [0] }])).rejects.toThrow(
      'unsubscribe() first'
    )
    await consumer.unsubscribe()
    await consumer.unsubscribe()
    expect(consumer.subscription()).toEqual([])
    await consumer.assign([
      { topic: TOPIC, partitions: [2] },
      { topic: TOPIC, partitions: [0] },
    ])
    expect(consumer.subscription()).toEqual([])
    await expect(consumer.subscribe({ topic: TOPIC })).rejects.toThrow('unsubscribe() first')
    await consumer.run({ autoCommit: false, eachMessage: async () => {} })
    const acquired = consumer.assignment()
    expect(acquired).toEqual([{ topic: TOPIC, partitions: [0, 2] }])
    acquired[0].partitions.push(99)
    expect(consumer.assignment()).toEqual([{ topic: TOPIC, partitions: [0, 2] }])
    await consumer.stop()
    expect(consumer.assignment()).toEqual([])
    await consumer.unsubscribe()
    await consumer.subscribe({ topic: TOPIC })
    expect(consumer.subscription()).toEqual([TOPIC])
  })

  test('a rejected assignment keeps the previous selection usable', async () => {
    wire = await scripted()
    const { cluster, kafka } = wire
    const consumer = track(kafka.consumer({ maxWaitTimeInMs: 20 }))
    const delivered = []
    await consumer.connect()
    await consumer.assign([{ topic: TOPIC, partitions: [0] }], { offsetReset: 'earliest' })
    await expect(consumer.assign([{ topic: TOPIC, partitions: [0, 7] }])).rejects.toThrow(
      'has no partition 7'
    )
    await expect(consumer.assign([])).rejects.toThrow()
    await consumer.run({
      eachMessage: async ({ partition, message }) => {
        delivered.push(`${partition}:${message.offset}`)
      },
    })
    await until(() => delivered.length === 3, 'three records')
    expect(consumer.assignment()).toEqual([{ topic: TOPIC, partitions: [0] }])
    expect(frames(cluster, 'ListOffsets').length).toBeGreaterThan(0)
  })

  test('applies a seek queued before run once and never replays it after a crash restart', async () => {
    wire = await scripted()
    const { cluster, kafka } = wire
    const consumer = track(
      kafka.consumer({ maxWaitTimeInMs: 20, retry: { retries: 0, initialRetryTime: 5 } })
    )
    const crashes = []
    consumer.on(consumer.events.CRASH, ({ payload }) => crashes.push(payload))
    await consumer.connect()
    await consumer.assign([{ topic: TOPIC, partitions: [0] }])
    expect(() => consumer.seek({ topic: TOPIC, partition: 1, offset: '1' })).toThrow(
      'not in the manual'
    )
    consumer.seek({ topic: TOPIC, partition: 0, offset: '1' })
    const delivered = []
    await consumer.run({
      eachMessage: async ({ message }) => {
        delivered.push(message.offset)
      },
    })
    await until(() => delivered.length === 2, 'records after the seek')
    expect(delivered).toEqual(['1', '2'])
    const gate = cluster.schedule.hold({ api: 'Fetch' })
    await gate.arrived()
    gate.fail(new KafkaJSConnectionError('connection lost'))
    await until(() => crashes.length === 1, 'the crash')
    expect(crashes[0]).toEqual(expect.objectContaining({ groupId: null, restart: true }))
    const before = frames(cluster, 'Fetch').length
    await until(() => frames(cluster, 'Fetch').length > before, 'a fetch after the restart')
    const firstAfterRestart = frames(cluster, 'Fetch')[before].fields.topics[0].partitions[0]
    expect(String(firstAfterRestart.fetchOffset)).toBe('0')
  })

  test('two consumers without a group on one client keep independent positions', async () => {
    wire = await scripted()
    const { cluster, kafka } = wire
    const registry = jest.spyOn(Cluster.prototype, 'groupEntry')
    const first = track(kafka.consumer({ maxWaitTimeInMs: 20 }))
    const second = track(kafka.consumer({ maxWaitTimeInMs: 20 }))
    const seen = { first: [], second: [] }
    await first.connect()
    await second.connect()
    await first.assign([{ topic: TOPIC, partitions: [0] }])
    await second.assign([{ topic: TOPIC, partitions: [0] }])
    first.seek({ topic: TOPIC, partition: 0, offset: '0' })
    second.seek({ topic: TOPIC, partition: 0, offset: '2' })
    await first.run({ eachMessage: async ({ message }) => seen.first.push(message.offset) })
    await second.run({ eachMessage: async ({ message }) => seen.second.push(message.offset) })
    await until(() => seen.first.length === 3 && seen.second.length === 1, 'both deliveries')
    expect(seen).toEqual({ first: ['0', '1', '2'], second: ['2'] })
    expect(await first.position({ topic: TOPIC, partition: 0 })).toBe('3')
    await first.stop()
    expect(await second.position({ topic: TOPIC, partition: 0 })).toBe('3')
    for (const api of ['FindCoordinator', 'OffsetFetch', 'OffsetCommit']) {
      expect(frames(cluster, api)).toEqual([])
    }
    expect(registry).not.toHaveBeenCalled()
    registry.mockRestore()
  })

  test('a none reset halts its partition until a seek resumes it while siblings continue', async () => {
    wire = await scripted({ committed: { 1: '0' } })
    const { cluster, kafka } = wire
    const consumer = track(kafka.consumer({ groupId: 'none-group', maxWaitTimeInMs: 20 }))
    const halts = []
    const delivered = []
    consumer.on(consumer.events.PARTITION_HALTED, ({ payload }) => halts.push(payload))
    await consumer.connect()
    await consumer.assign(
      [
        { topic: TOPIC, partitions: [0] },
        { topic: TOPIC, partitions: [1] },
      ],
      { offsetReset: 'none' }
    )
    await consumer.run({
      autoCommit: false,
      eachMessage: async ({ partition, message }) => {
        delivered.push(`${partition}:${message.offset}`)
      },
    })
    await until(() => delivered.length === 3 && halts.length === 1, 'sibling delivery and one halt')
    expect(halts[0]).toEqual(
      expect.objectContaining({ partition: 0, reason: 'no-offset', offset: null })
    )
    expect(delivered.every((entry) => entry.startsWith('1:'))).toBe(true)
    expect(frames(cluster, 'ListOffsets')).toEqual([])
    await expect(consumer.position({ topic: TOPIC, partition: 0 })).rejects.toMatchObject({
      name: 'KafkaJSNoOffsetError',
    })
    consumer.seek({ topic: TOPIC, partition: 0, offset: '1' })
    await until(() => delivered.filter((entry) => entry.startsWith('0:')).length === 2, 'resumed')
    expect(halts).toHaveLength(1)
  })

  test.each([
    ['', []],
    [', including stop()', [(consumer) => consumer.stop()]],
    [', including disconnect()', [(consumer) => consumer.disconnect()]],
  ])(
    'work a retired grouped handler left behind cannot touch the replacement manual run%s',
    async (_title, lifecycle) => {
      wire = await scripted()
      const { cluster, kafka } = wire
      const consumer = track(kafka.consumer({ groupId: 'adopt-group', maxWaitTimeInMs: 20 }))
      let letGo
      const held = new Promise((resolve) => {
        letGo = resolve
      })
      let detached = null
      await consumer.connect()
      await consumer.subscribe({ topic: TOPIC, fromBeginning: true })
      await consumer.run({
        autoCommit: false,
        eachMessage: async () => {
          if (detached) return
          detached = held.then(async () => {
            const outcomes = []
            for (const call of [
              async () => consumer.seek({ topic: TOPIC, partition: 0, offset: '2' }),
              () => consumer.position({ topic: TOPIC, partition: 0 }),
              () => consumer.commitOffsets([{ topic: TOPIC, partition: 0, offset: '1' }]),
              () => consumer.assign([{ topic: TOPIC, partitions: [0] }]),
              () => consumer.unsubscribe(),
              () => consumer.subscribe({ topic: TOPIC }),
              () => consumer.run({ eachMessage: async () => {} }),
              async () => consumer.pause([{ topic: TOPIC, partitions: [1] }]),
              async () => consumer.resume([{ topic: TOPIC, partitions: [1] }]),
              () => consumer.connect(),
              ...lifecycle.map((call) => () => call(consumer)),
            ]) {
              outcomes.push(
                await call().then(
                  () => 'allowed',
                  (error) => error.type
                )
              )
            }
            return outcomes
          })
        },
      })
      await until(() => detached !== null, 'the grouped handler')
      await consumer.unsubscribe()
      await consumer.assign([{ topic: TOPIC, partitions: [1] }])
      const commitsBefore = frames(cluster, 'OffsetCommit').length
      await consumer.run({ autoCommit: false, eachMessage: async () => {} })
      letGo()
      expect(await detached).toEqual(Array(10 + lifecycle.length).fill('CONSUMER_STOPPING'))
      expect(consumer.assignment()).toEqual([{ topic: TOPIC, partitions: [1] }])
      expect(consumer.paused()).toEqual([])
      expect(frames(cluster, 'OffsetCommit')).toHaveLength(commitsBefore)
      await consumer.stop()
      await consumer.run({ autoCommit: false, eachMessage: async () => {} })
      expect(consumer.assignment()).toEqual([{ topic: TOPIC, partitions: [1] }])
    }
  )

  describe('control over the same script', () => {
    test('a classic member rides out a lost fetch connection instead of crashing', async () => {
      wire = await scripted()
      const { cluster, kafka } = wire
      const consumer = track(
        kafka.consumer({
          groupId: 'classic-control',
          maxWaitTimeInMs: 20,
          retry: { retries: 0, initialRetryTime: 5 },
        })
      )
      const crashes = []
      consumer.on(consumer.events.CRASH, ({ payload }) => crashes.push(payload))
      await consumer.connect()
      await consumer.subscribe({ topic: TOPIC })
      const delivered = []
      await consumer.run({
        autoCommit: false,
        eachMessage: async ({ partition, message }) => {
          delivered.push(`${partition}:${message.offset}`)
        },
      })
      await until(() => delivered.length > 0, 'a first record')
      const gate = cluster.schedule.hold({ api: 'Fetch' })
      await gate.arrived()
      gate.fail(new KafkaJSConnectionError('connection lost'))
      await new Promise((resolve) => setTimeout(resolve, 300))
      expect(crashes).toEqual([])
    })
  })
})

describe('A consumer that was disconnected over a scripted broker', () => {
  const CYCLES = 12
  const PROCESS_EVENTS = ['unhandledRejection', 'uncaughtException', 'exit', 'SIGINT', 'SIGTERM']

  test('leaves only the inert error listener on its sockets and nothing on the process', async () => {
    const { cluster } = await scripted({ last: -1 })
    const sockets = []
    const kafka = new Kafka(
      cluster.kafkaOptions({
        socketFactory: (options) => {
          const socket = cluster.socketFactory(options)
          sockets.push(socket)
          return socket
        },
      })
    )
    const processBefore = PROCESS_EVENTS.map((event) => process.listenerCount(event))
    const lifecycle = async (cycle) => {
      const consumer = kafka.consumer({ groupId: `closed-${cycle}`, maxWaitTimeInMs: 20 })
      await consumer.connect()
      await consumer.assign([{ topic: TOPIC, partitions: [0] }])
      await consumer.run({ eachMessage: async () => {} })
      await until(() => frames(cluster, 'Fetch').length > cycle, 'a fetch')
      await consumer.disconnect()
    }
    for (let cycle = 0; cycle < CYCLES; cycle++) await lifecycle(cycle)
    await until(() => sockets.every(({ destroyed }) => destroyed), 'every socket destroyed')
    await new Promise((resolve) => setImmediate(resolve))

    expect(sockets.length).toBeGreaterThanOrEqual(CYCLES)
    expect(sockets.map((socket) => socket.eventNames())).toEqual(sockets.map(() => ['error']))
    expect(PROCESS_EVENTS.map((event) => process.listenerCount(event))).toEqual(processBefore)
  })
})
