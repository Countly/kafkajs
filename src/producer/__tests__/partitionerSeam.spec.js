const { Kafka, Partitioners } = require('../../../index')
const { Schedule } = require('../../../testHelpers/schedule')
const { createScriptedCluster, producerScript } = require('../../../testHelpers/scriptedCluster')
const { TOPIC, TOPIC_ID } = require('../../consumer/__tests__/fixtures/fetchHarness')

const tick = () => new Promise((resolve) => setImmediate(resolve))
const until = async (predicate) => {
  for (let i = 0; i < 500 && !predicate(); i += 1) await tick()
  expect(predicate()).toBe(true)
}

describe('Producer > the partitioner seam', () => {
  let producer, cluster

  /** A producer over a scripted cluster of two nodes in racks a and b; partition 0 led by node 0. */
  const start = async (createPartitioner, schedule = new Schedule()) => {
    cluster = producerScript(
      createScriptedCluster({
        nodes: [
          { id: 0, host: 'node0', port: 9092 },
          { id: 1, host: 'node1', port: 9092 },
        ],
        schedule,
      })
    ).answer('Metadata', () => ({
      brokers: [
        { nodeId: 0, host: 'node0', port: 9092, rack: 'rack-a' },
        { nodeId: 1, host: 'node1', port: 9092, rack: 'rack-b' },
      ],
      controllerId: 0,
      topics: [
        {
          errorCode: 0,
          name: TOPIC,
          topicId: TOPIC_ID,
          isInternal: false,
          topicAuthorizedOperations: -2147483648,
          partitions: [0, 1].map((partitionIndex) => ({
            errorCode: 0,
            partitionIndex,
            leaderId: 0,
            leaderEpoch: 5,
            replicas: [0],
            isr: [0],
            offlineReplicas: [],
          })),
        },
      ],
    }))
    producer = new Kafka(cluster.kafkaOptions({ brokers: ['node0:9092'] })).producer({
      linger: 0,
      createPartitioner,
    })
    await producer.connect()
  }

  /** A built-in adaptive partitioner whose `attach` call is recorded, with `rackOf` watched. */
  const recordedBuiltIn = () => {
    const seen = { attached: [], rackReads: 0 }
    const createPartitioner = () => {
      const partitioner = Partitioners.UniformStickyPartitioner({ adaptive: true })()
      const attach = partitioner.attach
      partitioner.attach = (context) => {
        seen.attached.push(context)
        attach({
          ...context,
          rackOf: (nodeId) => {
            seen.rackReads += 1
            return context.rackOf(nodeId)
          },
        })
      }
      return partitioner
    }
    return { seen, createPartitioner }
  }

  afterEach(async () => {
    if (producer) await producer.disconnect()
    if (cluster) cluster.assertClean()
    producer = cluster = null
  })

  test('a custom partitioner is called with exactly topic, partitionMetadata and message', async () => {
    const calls = []
    await start(() => (args) => {
      calls.push(args)
      return 0
    })

    await producer.send({ topic: TOPIC, messages: [{ value: 'v' }, { key: 'k', value: 'v' }] })

    expect(calls).toHaveLength(2)
    expect(calls.map((args) => Object.keys(args).sort())).toEqual([
      ['message', 'partitionMetadata', 'topic'],
      ['message', 'partitionMetadata', 'topic'],
    ])
  })

  test.each([() => {}, 7, null])(
    'a custom attach property %p is never called or read as a seam',
    async (property) => {
      const partitioner = jest.fn(() => 0)
      const attach = jest.fn(
        property === null || typeof property !== 'function' ? undefined : property
      )
      partitioner.attach = typeof property === 'function' ? attach : property
      await start(() => partitioner)
      await producer.send({ topic: TOPIC, messages: [{ value: 'v' }] })
      expect(attach).not.toHaveBeenCalled()
      expect(Object.keys(partitioner.mock.calls[0][0]).sort()).toEqual([
        'message',
        'partitionMetadata',
        'topic',
      ])
    }
  )

  test.each(['called', 'uncalled'])(
    'the %s built-in form attaches once to its record instance',
    async (form) => {
      const { attach } = require('../partitioners/builtIn')
      const factory = Partitioners.UniformStickyPartitioner({ stickyBytes: 24, adaptive: true })
      const instance = form === 'called' ? factory() : factory
      const attached = jest.spyOn(instance, 'attach')
      await start(() => instance)
      await producer.send({ topic: TOPIC, messages: [{ value: 'v' }] })
      expect(attached).toHaveBeenCalledTimes(1)
      expect(Object.keys(attached.mock.calls[0][0]).sort()).toEqual(['loadOf', 'rackOf'])
      const random = jest.spyOn(Math, 'random').mockReturnValue(0.3)
      attach(instance, {
        loadOf: (_, partition) => (partition === 0 ? 100 : 0),
        rackOf: () => undefined,
      })
      expect(
        instance({
          topic: 'probe',
          partitionMetadata: [
            { partitionId: 0, leader: 0 },
            { partitionId: 1, leader: 0 },
          ],
          message: { value: '' },
        })
      ).toBe(1)
      random.mockRestore()
    }
  )

  test('a built-in partitioner is attached once, to how many batches wait for a partition and to the racks', async () => {
    const { seen, createPartitioner } = recordedBuiltIn()
    const schedule = new Schedule()
    const gate = schedule.hold({ api: 'Produce' })
    await start(createPartitioner, schedule)

    const sent = producer.send({ topic: TOPIC, messages: [{ value: 'v', partition: 0 }] })
    await gate.arrived()
    const [context] = seen.attached

    expect(seen.attached).toHaveLength(1)
    expect(Object.keys(context).sort()).toEqual(['loadOf', 'rackOf'])
    expect([context.loadOf(TOPIC, 0), context.loadOf(TOPIC, 1)]).toEqual([1, 0])
    expect([context.rackOf(0), context.rackOf(1), context.rackOf(9)]).toEqual([
      'rack-a',
      'rack-b',
      undefined,
    ])
    gate.release()
    await sent
    await until(() => context.loadOf(TOPIC, 0) === 0)
  })

  test('no decision reads a rack', async () => {
    const { seen, createPartitioner } = recordedBuiltIn()
    await start(createPartitioner)

    for (let i = 0; i < 20; i += 1) {
      await producer.send({ topic: TOPIC, messages: [{ value: 'x'.repeat(2000) }] })
    }

    expect(seen.attached).toHaveLength(1)
    expect(seen.rackReads).toBe(0)
  })
  test('depth counts every post-guard child and each repeated settlement removes only its own leaf', async () => {
    const Accumulator = require('../state/accumulator')
    const handed = []
    const accumulator = new Accumulator({
      linger: 0,
      batchSize: 16384,
      partition: async ({ records }) =>
        records.map(({ topic, messages }) => ({ topic, partition: 0, messages })),
      sendPartitioned: async ({ batches }) => {
        handed.push(...batches)
      },
    })
    const sent = accumulator
      .append({
        acks: -1,
        topicMessages: [
          {
            topic: TOPIC,
            messages: [{ value: 'a' }, { value: 'b' }, { value: 'c' }, { value: 'd' }],
          },
        ],
      })
      .catch((e) => e)
    await tick()
    expect(accumulator.depthOf(TOPIC, 0)).toBe(1)
    const [head, tail] = Accumulator.cut(handed[0], 2)
    expect(accumulator.depthOf(TOPIC, 0)).toBe(2)
    const [left, right] = Accumulator.cut(head, 1)
    expect(accumulator.depthOf(TOPIC, 0)).toBe(3)
    const failure = new Error('expired')
    for (const [index, child] of [left, tail, right].entries()) {
      child.settlers.forEach((settler) => settler.reject(failure))
      child.settlers.forEach((settler) => settler.reject(failure))
      child.detach()
      child.retire()
      child.retire()
      expect(accumulator.depthOf(TOPIC, 0)).toBe(2 - index)
    }
    expect(await sent).toBe(failure)
  })

  test('depth counts the pending batch a lingering send opened, until it is delivered', async () => {
    const Accumulator = require('../state/accumulator')
    const accumulator = new Accumulator({
      linger: 1000,
      batchSize: 16384,
      partition: async ({ records }) =>
        records.map(({ topic, messages }) => ({ topic, partition: 0, messages })),
      sendPartitioned: async ({ batches }) => {
        for (const { settlers } of batches)
          settlers.forEach((settler) =>
            settler.resolve({ topicName: TOPIC, partition: 0, baseOffset: '0' })
          )
      },
    })
    const sent = accumulator.append({
      acks: -1,
      topicMessages: [{ topic: TOPIC, messages: [{ value: 'a' }] }],
    })
    await tick()
    expect([accumulator.depthOf(TOPIC, 0), accumulator.depthOf(TOPIC, 1)]).toEqual([1, 0])
    await accumulator.close()
    await sent
    expect(accumulator.depthOf(TOPIC, 0)).toBe(0)
  })

  test('an observed partitioner is attachable only when it wraps a built-in one', () => {
    const { attach, observe } = require('../partitioners/builtIn')
    const context = { loadOf: () => 0, rackOf: () => undefined }
    const record = { topic: TOPIC, partitionMetadata: [{ partitionId: 0, leader: 0 }] }
    let calls = 0
    const builtIn = Partitioners.UniformStickyPartitioner({ adaptive: true })()
    const attached = jest.spyOn(builtIn, 'attach')
    const observed = observe(builtIn, () => (calls += 1))
    const custom = observe(
      () => 0,
      () => (calls += 1)
    )

    attach(observed, context)
    attach(custom, context)

    expect(attached).toHaveBeenCalledTimes(1)
    expect(attached.mock.calls[0][0]).toBe(context)
    expect(custom.attach).toBeUndefined()
    expect([observed({ ...record, message: { value: '' } }), custom(record)]).toEqual([0, 0])
    expect(calls).toBe(2)
  })
})
