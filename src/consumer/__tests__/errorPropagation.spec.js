const ConsumerGroup = require('../consumerGroup')
const { newLogger } = require('testHelpers')
const InstrumentationEventEmitter = require('../../instrumentation/emitter')
const { createErrorFromCode } = require('../../protocol/error')

const REBALANCE_IN_PROGRESS = 27
const UNKNOWN_MEMBER_ID = 25

const createGroup = (joinImpl) => {
  const group = new ConsumerGroup({
    logger: newLogger(),
    topics: ['t'],
    topicConfigurations: {},
    retry: { retries: 0 },
    cluster: {
      findGroupCoordinator: jest.fn(async () => ({ joinGroup: joinImpl })),
      refreshMetadata: jest.fn(async () => {}),
      isConnected: () => true,
      getNodeIds: () => [],
    },
    groupId: 'g',
    assigners: [{ name: 'a', protocol: () => ({ name: 'a', metadata: Buffer.alloc(0) }) }],
    instrumentationEmitter: new InstrumentationEventEmitter(),
    sessionTimeout: 30000,
    rebalanceTimeout: 60000,
    maxBytesPerPartition: 1024,
    minBytes: 1,
    maxBytes: 1024,
    maxWaitTimeInMs: 100,
    isolationLevel: 1,
    metadataMaxAge: 1000,
  })
  return group
}

describe('Consumer > ConsumerGroup > error propagation', () => {
  it.each([
    ['REBALANCE_IN_PROGRESS', REBALANCE_IN_PROGRESS],
    ['UNKNOWN_MEMBER_ID', UNKNOWN_MEMBER_ID],
  ])('keeps error.type on %s after joinAndSync wraps it', async (type, code) => {
    const group = createGroup(async () => {
      throw createErrorFromCode(code)
    })

    await expect(group.joinAndSync()).rejects.toMatchObject({ type, code })
  })
})
