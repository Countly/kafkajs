const Cluster = require('../index')
const InstrumentationEventEmitter = require('../../instrumentation/emitter')
const { newLogger, connectionOpts } = require('testHelpers')

for (const fromBeginning of [false, true]) {
  test(`per-partition timestamps override topic defaults with fromBeginning ${fromBeginning}`, async () => {
    const cluster = new Cluster({
      ...connectionOpts(),
      logger: newLogger(),
      instrumentationEmitter: new InstrumentationEventEmitter(),
    })
    const broker = {
      listOffsets: jest.fn(async ({ topics }) => ({
        responses: topics.map(({ topic, partitions }) => ({
          topic,
          partitions: partitions.map(({ partition }) => ({ partition, offset: '37' })),
        })),
      })),
    }
    cluster.findLeaderForPartitions = () => ({ 1: [0, 1, 2, 3] })
    cluster.findBroker = async () => broker
    const partitions = [
      { partition: 0, timestamp: -2 },
      { partition: 1, timestamp: -1 },
      { partition: 2, timestamp: 0 },
      { partition: 3 },
    ]
    const before = partitions.map((p) => ({ ...p }))
    await cluster.fetchTopicsOffset([{ topic: 'test-topic', partitions, fromBeginning }])
    expect(
      broker.listOffsets.mock.calls[0][0].topics[0].partitions.map(({ timestamp }) => timestamp)
    ).toEqual([-2, -1, 0, fromBeginning ? -2 : -1])
    expect(partitions).toEqual(before)
  })
}

test('an omitted or null partition timestamp uses the topic fromTimestamp', async () => {
  const cluster = new Cluster({
    ...connectionOpts(),
    logger: newLogger(),
    instrumentationEmitter: new InstrumentationEventEmitter(),
  })
  const broker = { listOffsets: jest.fn(async () => ({ responses: [] })) }
  cluster.findLeaderForPartitions = () => ({ 1: [0, 1] })
  cluster.findBroker = async () => broker
  await cluster.fetchTopicsOffset([
    {
      topic: 'test-topic',
      partitions: [{ partition: 0 }, { partition: 1, timestamp: null }],
      fromTimestamp: 123,
    },
  ])
  expect(
    broker.listOffsets.mock.calls[0][0].topics[0].partitions.map(({ timestamp }) => timestamp)
  ).toEqual([123, 123])
})
