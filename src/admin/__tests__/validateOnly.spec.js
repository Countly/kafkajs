const createAdmin = require('..')
const logger = { namespace: () => logger, debug() {}, info() {}, warn() {}, error() {} }
const methods = [
  ['createTopics', 'createTopics/v2/request', { topics: [{ topic: 't' }] }, { topicErrors: [] }],
  [
    'createPartitions',
    'createPartitions/v0/request',
    { topicPartitions: [{ topic: 't', count: 2 }] },
    { topicErrors: [] },
  ],
  [
    'alterConfigs',
    'alterConfigs/v0/request',
    { resources: [{ type: 2, name: 't', configEntries: [] }] },
    { resources: [] },
  ],
  [
    'incrementalAlterConfigs',
    'incrementalAlterConfigs/v0/request',
    { resources: [{ type: 2, name: 't', configEntries: [] }] },
    { resources: [{ resourceType: 2, resourceName: 't', errorCode: 0, errorMessage: null }] },
  ],
]
describe('validation-only admin requests', () => {
  it.each(methods)(
    '%s sends its flag without follow-up work',
    async (method, codec, options, response) => {
      const bytes = []
      const cache = new Map([['existing', 'unchanged']])
      const metadata = jest.fn(async () => ({ topicMetadata: [] }))
      const broker = {
        metadata,
        [method]: jest.fn(async (request) => {
          bytes.push((await require('../../protocol/requests/' + codec)(request).encode()).buffer)
          return response
        }),
      }
      const cluster = {
        refreshMetadata: jest.fn(async () => {}),
        findControllerBroker: async () => broker,
        metadataCache: cache,
      }
      const admin = createAdmin({ cluster, logger, retry: { retries: 0 } })
      await admin[method]({ ...options, validateOnly: true })
      expect(bytes).toHaveLength(1)
      expect(bytes[0][bytes[0].length - 1]).toBe(1)
      expect(broker[method]).toHaveBeenCalledTimes(1)
      expect(metadata).not.toHaveBeenCalled()
      expect([...cache]).toEqual([['existing', 'unchanged']])
    }
  )
})
