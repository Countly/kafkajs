const {
  bufferedProducer,
  flushAll,
  collect,
  succeeded,
  watch,
} = require('../../__tests__/fixtures/bufferHarness')
describe('retired payload ownership', () => {
  beforeEach(() => jest.useFakeTimers({ now: 0, doNotFake: ['nextTick', 'setImmediate'] }))
  afterEach(() => {
    jest.restoreAllMocks()
    jest.useRealTimers()
  })
  test.each([
    ['idempotent', true],
    ['plain', false],
  ])(
    '%s: a successful partition releases its snapshotted record while another broker is held',
    async (_, idempotent) => {
      let ref
      const { producer, cluster, admission } = bufferedProducer({
        partitions: 2,
        idempotent,
        deliveryTimeout: 60000,
        retry: { initialRetryTime: 10, maxRetryTime: 10, factor: 0 },
        createPartitioner:
          () =>
          ({ message }) => {
            if (message.partition === 0) ref = new WeakRef(message)
            return message.partition
          },
      })
      let resolveHeld
      let requests = 0
      const slow = {
        nodeId: 1,
        produce: jest.fn((request) => {
          requests++
          return new Promise((resolve) => {
            resolveHeld = () => resolve(succeeded(request))
          })
        }),
      }
      cluster.findTopicPartitionMetadata = () => [
        { partitionId: 0, leader: 0 },
        { partitionId: 1, leader: 1 },
      ]
      cluster.cachedLeader = (_topic, p) => p
      cluster.findLeaderForPartitions = (_topic, ids) =>
        Object.fromEntries(ids.map((p) => [p, [Number(p)]]))
      cluster.findBroker.mockImplementation(async ({ nodeId }) =>
        Number(nodeId) === 0 ? cluster.broker : slow
      )
      await producer.connect()
      const start = () =>
        watch(
          producer.send({
            topic: 'a',
            messages: [
              { partition: 0, value: 'x'.repeat(100000) },
              { partition: 1, value: 'v' },
            ],
          })
        )
      const sent = start()
      await flushAll()
      await jest.advanceTimersByTimeAsync(20)
      await flushAll()
      cluster.broker.produce.mockClear()
      expect(requests).toBe(1)
      expect(admission.reserved).toBe(1625)
      await collect()
      try {
        expect(ref.deref() === undefined).toBe(true)
      } finally {
        resolveHeld()
        await flushAll()
        await producer.disconnect()
      }
      expect(sent.state).toBe('fulfilled')
    }
  )
})
