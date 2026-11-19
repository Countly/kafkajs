const Cluster = require('../index')
const InstrumentationEventEmitter = require('../../instrumentation/emitter')
const { CLUSTER_TOPIC_RECREATED } = require('../instrumentationEvents')
const { protocol: metadataProtocol } = require('../../protocol/requests/metadata')
const { newLogger, connectionOpts } = require('testHelpers')

const HEALTHY_ID = '01020304-0506-0708-090a-0b0c0d0e0f10'
const DOOMED_ID = '11121314-1516-1718-191a-1b1c1d1e1f20'
const RECREATED_ID = '21222324-2526-2728-292a-2b2c2d2e2f30'

/**
 * A cluster over one broker double whose answers go through the real Metadata v13 parse. A name in
 * `refused` is answered with its code and no partitions; `ids` gives each name's current topic id,
 * `epochs` the leader epoch of its one partition and `partitionErrors` that partition's code. An
 * answer waits for the promise at the head of `holds`, when there is one.
 * @param {object} [options] cluster options over the defaults
 * @returns {object} the cluster, the broker double, its controls and the status reports
 */
const build = (options = {}) => {
  const reports = []
  const refused = new Map()
  const ids = new Map([
    ['healthy', HEALTHY_ID],
    ['doomed', DOOMED_ID],
    ['other', RECREATED_ID],
  ])
  const epochs = new Map()
  const partitionErrors = new Map()
  const holds = []
  const emitter = new InstrumentationEventEmitter()
  const recreated = []
  emitter.addListener(CLUSTER_TOPIC_RECREATED, ({ payload }) => recreated.push(payload))
  const cluster = new Cluster({
    ...connectionOpts(),
    logger: newLogger(),
    retry: { retries: 0 },
    allowAutoTopicCreation: false,
    instrumentationEmitter: emitter,
    onTopicMetadataStatus: (report) => reports.push(report),
    ...options,
  })
  const broker = {
    nodeId: 0,
    connectionPool: { host: 'localhost', port: 9092 },
    isConnected: () => true,
    disconnect: async () => {},
    retire: async () => {},
    metadata: jest.fn(async (topics, requestOptions = {}) => {
      const hold = holds.shift()
      if (hold) await hold
      const data = {
        errorCode: 0,
        brokers: [{ nodeId: 0, host: 'localhost', port: 9092, rack: null }],
        controllerId: 0,
        topicMetadata: topics.map((topic) =>
          refused.has(topic)
            ? { topic, topicId: null, topicErrorCode: refused.get(topic), partitionMetadata: [] }
            : {
                topic,
                topicId: ids.get(topic),
                topicErrorCode: 0,
                partitionMetadata: [
                  {
                    partitionErrorCode: partitionErrors.get(topic) ?? 0,
                    partitionId: 0,
                    leader: 0,
                    leaderEpoch: epochs.get(topic) ?? 1,
                    replicas: [0],
                    isr: [0],
                    offlineReplicas: [],
                  },
                ],
              }
        ),
      }
      return metadataProtocol({ version: 13 })({ topics, ...requestOptions }).response.parse(data)
    }),
  }
  cluster.brokerPool.seedBroker = broker
  cluster.brokerPool.brokers = { 0: broker }
  cluster.brokerPool.findConnectedBroker = async () => broker
  return { cluster, broker, refused, ids, epochs, partitionErrors, holds, reports, recreated }
}

/** @param {Cluster} cluster @returns {string[]} the topics the installed metadata routes */
const routed = (cluster) => cluster.brokerPool.metadata.topicMetadata.map(({ topic }) => topic)

const UNAVAILABLE = {
  topic: 'doomed',
  status: 'unavailable',
  type: 'UNKNOWN_TOPIC_OR_PARTITION',
  code: 3,
}
const AVAILABLE = { topic: 'doomed', status: 'available', type: null, code: 0 }

describe('Cluster > a consumer target answered UNKNOWN_TOPIC_OR_PARTITION', () => {
  beforeEach(() => {
    jest.useFakeTimers({ now: 0, doNotFake: ['setImmediate'] })
  })
  afterEach(() => {
    jest.useRealTimers()
  })

  test('installs the healthy topic, keeps the missing one a target without routing and reports it once', async () => {
    const { cluster, refused, epochs, reports } = build()
    await cluster.addMultipleTargetTopics(['healthy', 'doomed'])
    refused.set('doomed', 3)
    epochs.set('healthy', 7)

    await cluster.refreshMetadata()
    await cluster.refreshMetadata()

    expect([...cluster.targetTopics]).toEqual(['healthy', 'doomed'])
    expect(routed(cluster)).toEqual(['healthy'])
    expect(cluster.findTopicPartitionMetadata('healthy')[0]).toMatchObject({ leaderEpoch: 7 })
    expect(cluster.findTopicPartitionMetadata('doomed')).toEqual([])
    expect(cluster.topicId('doomed')).toBe(DOOMED_ID)
    expect(reports).toEqual([UNAVAILABLE])
  })

  test('a fresh cache does not ask again for the missing topic before its metadata expires', async () => {
    const { cluster, broker, refused } = build({ metadataMaxAge: 1000 })
    await cluster.addMultipleTargetTopics(['healthy', 'doomed'])
    refused.set('doomed', 3)
    await cluster.refreshMetadata()
    const asked = broker.metadata.mock.calls.length

    await cluster.refreshMetadataIfNecessary()
    await cluster.refreshMetadataIfNecessary()
    expect(broker.metadata).toHaveBeenCalledTimes(asked)

    await jest.advanceTimersByTimeAsync(1001)
    await cluster.refreshMetadataIfNecessary()
    expect(broker.metadata).toHaveBeenCalledTimes(asked + 1)
    expect(broker.metadata.mock.calls[asked][0]).toEqual(['healthy', 'doomed'])
  })

  test('with the default metadata age, the missing topic is asked for again after a second, not after the whole age', async () => {
    const { cluster, broker, refused } = build({ metadataMaxAge: 300000 })
    await cluster.addMultipleTargetTopics(['healthy', 'doomed'])
    refused.set('doomed', 3)
    await cluster.refreshMetadata()
    const asked = broker.metadata.mock.calls.length

    await jest.advanceTimersByTimeAsync(999)
    await cluster.refreshMetadataIfNecessary()
    expect(broker.metadata).toHaveBeenCalledTimes(asked)

    await jest.advanceTimersByTimeAsync(2)
    await cluster.refreshMetadataIfNecessary()
    expect(broker.metadata).toHaveBeenCalledTimes(asked + 1)

    refused.delete('doomed')
    await jest.advanceTimersByTimeAsync(1001)
    await cluster.refreshMetadataIfNecessary()
    await jest.advanceTimersByTimeAsync(1001)
    await cluster.refreshMetadataIfNecessary()
    expect(broker.metadata).toHaveBeenCalledTimes(asked + 2)
  })

  test('a recreated topic is routed under its new id, reported available once and seen as a recreation', async () => {
    const { cluster, refused, ids, reports, recreated } = build()
    await cluster.addMultipleTargetTopics(['healthy', 'doomed'])
    refused.set('doomed', 3)
    await cluster.refreshMetadata()

    refused.delete('doomed')
    ids.set('doomed', RECREATED_ID)
    await cluster.refreshMetadata()
    await cluster.refreshMetadata()

    expect(routed(cluster)).toEqual(['healthy', 'doomed'])
    expect(cluster.topicId('doomed')).toBe(RECREATED_ID)
    expect(reports).toEqual([UNAVAILABLE, AVAILABLE])
    expect(recreated).toEqual([
      expect.objectContaining({
        topic: 'doomed',
        previousTopicId: DOOMED_ID,
        topicId: RECREATED_ID,
      }),
    ])
  })

  test('a topic missing again after it came back is reported again', async () => {
    const { cluster, refused, reports } = build()
    await cluster.addMultipleTargetTopics(['healthy', 'doomed'])
    refused.set('doomed', 3)
    await cluster.refreshMetadata()
    refused.delete('doomed')
    await cluster.refreshMetadata()
    refused.set('doomed', 3)
    await cluster.refreshMetadata()

    expect(reports).toEqual([UNAVAILABLE, AVAILABLE, UNAVAILABLE])
  })

  test.each([
    [17, 'INVALID_TOPIC_EXCEPTION'],
    [29, 'TOPIC_AUTHORIZATION_FAILED'],
  ])('code %i still fails the whole refresh and installs nothing', async (code, type) => {
    const { cluster, refused, epochs, reports } = build()
    await cluster.addMultipleTargetTopics(['healthy', 'doomed'])
    refused.set('doomed', code)
    epochs.set('healthy', 7)

    await expect(cluster.refreshMetadata()).rejects.toMatchObject({ type })

    expect(cluster.findTopicPartitionMetadata('healthy')[0]).toMatchObject({ leaderEpoch: 1 })
    expect(routed(cluster)).toEqual(['healthy', 'doomed'])
    expect(reports).toEqual([])
  })

  test('code 17 beside a missing topic fails the refresh before anything is reported', async () => {
    const { cluster, refused, reports } = build()
    await cluster.addMultipleTargetTopics(['healthy', 'doomed', 'other'])
    refused.set('doomed', 3)
    refused.set('other', 17)

    await expect(cluster.refreshMetadata()).rejects.toMatchObject({
      type: 'INVALID_TOPIC_EXCEPTION',
    })
    expect(reports).toEqual([])
  })

  test('a topic being added that is missing still fails the add and is rolled back', async () => {
    const { cluster, refused, reports } = build()
    await cluster.addMultipleTargetTopics(['healthy'])
    refused.set('doomed', 3)

    await expect(cluster.addMultipleTargetTopics(['doomed'])).rejects.toMatchObject({
      type: 'UNKNOWN_TOPIC_OR_PARTITION',
    })

    expect([...cluster.targetTopics]).toEqual(['healthy'])
    expect(reports).toEqual([])
  })

  test('an established missing topic does not fail the add of another topic', async () => {
    const { cluster, refused, reports } = build()
    await cluster.addMultipleTargetTopics(['healthy', 'doomed'])
    refused.set('doomed', 3)

    await cluster.addMultipleTargetTopics(['other'])

    expect([...cluster.targetTopics]).toEqual(['healthy', 'doomed', 'other'])
    expect(routed(cluster)).toEqual(['healthy', 'other'])
    expect(reports).toEqual([UNAVAILABLE])
  })

  test('replacing the targets drops the state of a removed topic without reporting it', async () => {
    const { cluster, refused, reports } = build()
    await cluster.addMultipleTargetTopics(['healthy', 'doomed'])
    refused.set('doomed', 3)
    await cluster.refreshMetadata()

    await cluster.resetTargetTopics(['healthy'])
    refused.delete('doomed')
    await cluster.addMultipleTargetTopics(['doomed'])
    await cluster.refreshMetadata()

    expect(cluster.brokerPool.unavailableTopics.size).toBe(0)
    expect(reports).toEqual([UNAVAILABLE])
  })

  test('the state never holds more than the current targets', async () => {
    const { cluster, refused } = build()
    await cluster.addMultipleTargetTopics(['healthy', 'doomed', 'other'])
    refused.set('doomed', 3)
    refused.set('other', 3)
    await cluster.refreshMetadata()
    expect([...cluster.brokerPool.unavailableTopics].sort()).toEqual(['doomed', 'other'])

    await cluster.resetTargetTopics([])

    expect(cluster.brokerPool.unavailableTopics.size).toBe(0)
  })

  test('a topic whose add failed with another error is not established, so a later code 3 fails the refresh', async () => {
    const { cluster, refused, reports } = build()
    await cluster.addMultipleTargetTopics(['healthy'])
    refused.set('doomed', 42)
    await expect(cluster.addMultipleTargetTopics(['doomed'])).rejects.toMatchObject({
      type: 'INVALID_REQUEST',
    })
    expect([...cluster.targetTopics]).toEqual(['healthy', 'doomed'])

    refused.set('doomed', 3)
    await expect(cluster.refreshMetadata()).rejects.toMatchObject({
      type: 'UNKNOWN_TOPIC_OR_PARTITION',
    })
    expect(reports).toEqual([])

    refused.delete('doomed')
    await cluster.refreshMetadata()
    refused.set('doomed', 3)
    await cluster.refreshMetadata()
    expect(reports).toEqual([UNAVAILABLE])
  })

  test('targets a reset declared without an accepted answer are not established', async () => {
    const { cluster, refused, reports } = build()
    await cluster.resetTargetTopics(['healthy', 'doomed'])
    refused.set('doomed', 3)

    await expect(cluster.refreshMetadata()).rejects.toMatchObject({
      type: 'UNKNOWN_TOPIC_OR_PARTITION',
    })
    expect(reports).toEqual([])
  })

  test('targets a reset declared with their accepted answer are established under the accepted ids', async () => {
    const { cluster, broker, refused, ids, reports, recreated } = build()
    const accepted = await broker.metadata(['healthy', 'doomed'])
    await cluster.resetTargetTopics(['healthy', 'doomed'], { accepted })
    expect(cluster.topicId('doomed')).toBe(DOOMED_ID)

    refused.set('doomed', 3)
    await cluster.refreshMetadata()
    expect(reports).toEqual([UNAVAILABLE])

    refused.delete('doomed')
    ids.set('doomed', RECREATED_ID)
    await cluster.refreshMetadata()
    expect(recreated).toEqual([
      expect.objectContaining({
        topic: 'doomed',
        previousTopicId: DOOMED_ID,
        topicId: RECREATED_ID,
      }),
    ])
  })

  test('an answer sent before a topic was removed does not establish the topic declared again', async () => {
    const { cluster, refused, holds, reports } = build()
    await cluster.addMultipleTargetTopics(['healthy', 'doomed'])
    let release
    holds.push(new Promise((resolve) => (release = resolve)))
    const early = cluster.refreshMetadata()
    await cluster.resetTargetTopics(['healthy'])
    const added = cluster.addMultipleTargetTopics(['doomed'])

    release()
    await early
    await added
    refused.set('doomed', 3)

    await expect(cluster.refreshMetadata()).rejects.toMatchObject({
      type: 'UNKNOWN_TOPIC_OR_PARTITION',
    })
    expect(reports).toEqual([])
  })

  test.each([
    [17, 'INVALID_TOPIC_EXCEPTION'],
    [29, 'TOPIC_AUTHORIZATION_FAILED'],
  ])(
    'code %i for one topic fails the refresh before a partition error of a healthy topic',
    async (code, type) => {
      const { cluster, refused, partitionErrors, reports } = build()
      await cluster.addMultipleTargetTopics(['healthy', 'doomed'])
      partitionErrors.set('healthy', 6)
      refused.set('doomed', code)

      await expect(cluster.refreshMetadata()).rejects.toMatchObject({ type })
      expect(reports).toEqual([])
    }
  )

  test('a partition error of a healthy topic beside a missing one fails the refresh and changes no status', async () => {
    const { cluster, refused, partitionErrors, reports } = build()
    await cluster.addMultipleTargetTopics(['healthy', 'doomed'])
    partitionErrors.set('healthy', 6)
    refused.set('doomed', 3)

    await expect(cluster.refreshMetadata()).rejects.toMatchObject({
      type: 'NOT_LEADER_FOR_PARTITION',
    })
    expect(reports).toEqual([])
  })
})

describe('Cluster > lookups that still fail on a missing topic', () => {
  beforeEach(() => {
    jest.useFakeTimers({ now: 0, doNotFake: ['setImmediate'] })
  })
  afterEach(() => {
    jest.useRealTimers()
  })

  test('an admin cluster still fails the refresh of its targets on a missing topic', async () => {
    const { cluster, refused } = build({ onTopicMetadataStatus: undefined })
    await cluster.addTargetTopic('healthy')
    await cluster.addTargetTopic('doomed')
    refused.set('doomed', 3)

    await expect(cluster.refreshMetadata()).rejects.toMatchObject({
      type: 'UNKNOWN_TOPIC_OR_PARTITION',
    })
    expect(cluster.brokerPool.unavailableTopics).toBeNull()
  })

  test('an explicit lookup of a topic that is not a target still fails with its own error', async () => {
    const { cluster, refused } = build()
    await cluster.addTargetTopic('healthy')
    refused.set('doomed', 3)

    await expect(cluster.metadata({ topics: ['doomed'] })).rejects.toMatchObject({
      type: 'UNKNOWN_TOPIC_OR_PARTITION',
    })
  })
})

describe('Cluster > a producer target refusal beside a partition error', () => {
  test.each([
    [17, 'INVALID_TOPIC_EXCEPTION'],
    [29, 'TOPIC_AUTHORIZATION_FAILED'],
  ])(
    'a new target answered %i fails the add with %s, not with the partition error',
    async (code, type) => {
      const { cluster, refused, partitionErrors } = build({
        onTopicMetadataStatus: undefined,
        metadataMaxIdle: 60000,
      })
      await cluster.addMultipleTargetTopics(['healthy'])
      partitionErrors.set('healthy', 6)
      refused.set('doomed', code)

      await expect(cluster.addMultipleTargetTopics(['doomed'])).rejects.toMatchObject({ type })
    }
  )
})
