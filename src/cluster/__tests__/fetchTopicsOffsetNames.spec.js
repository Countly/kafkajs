const Cluster = require('../index')
const InstrumentationEventEmitter = require('../../instrumentation/emitter')
const { newLogger, connectionOpts } = require('testHelpers')

describe('Cluster > fetchTopicsOffset > topic names', () => {
  test.each(['__proto__', 'constructor', 'toString', 'hasOwnProperty', 'valueOf'])(
    'a topic named %s, listed after an ordinary one, keeps its offsets',
    async (name) => {
      const cluster = new Cluster({
        ...connectionOpts(),
        logger: newLogger(),
        instrumentationEmitter: new InstrumentationEventEmitter(),
      })
      const broker = {
        listOffsets: async ({ topics }) => ({
          responses: topics.map(({ topic, partitions }) => ({
            topic,
            partitions: partitions.map(({ partition }) => ({ partition, offset: '37' })),
          })),
        }),
      }
      cluster.findLeaderForPartitions = () => ({ 0: [0] })
      cluster.findBroker = async () => broker
      const partitions = [{ partition: 0 }]

      const offsets = await cluster.fetchTopicsOffset([
        { topic: 'ordinary-topic', partitions },
        { topic: name, partitions },
      ])

      expect(offsets).toEqual([
        { topic: 'ordinary-topic', partitions: [{ partition: 0, offset: '37' }] },
        { topic: name, partitions: [{ partition: 0, offset: '37' }] },
      ])
    }
  )
})
