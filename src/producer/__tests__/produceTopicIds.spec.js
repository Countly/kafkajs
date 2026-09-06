const apiKeys = require('../../protocol/requests/apiKeys')
const { requests, selectVersion } = require('../../protocol/requests')
const { createErrorFromCode } = require('../../protocol/error')
const createSendMessages = require('../sendMessages')
const createRetry = require('../../retry')
const createDefaultPartitioner = require('../partitioners/default')
const { newLogger } = require('../../../testHelpers')

const TOPIC_A_ID = 'zTaJqQ7VQmSCTFmL6-oQ1w'

/** The Produce versions a stub broker in this file advertises, for `selectVersion` below. */
const BROKER_VERSIONS = { [apiKeys.Produce]: { minVersion: 3, maxVersion: 13 } }

/**
 * A stub cluster and a set of stub brokers, one per node id in `leaders`, wired to exercise only
 * what `sendMessages` needs to build and route a produce request: no real `Cluster`/`Broker`.
 *
 * Each stub broker's `produce` records the arguments `sendMessages` passed it - `topicIds` and
 * `maxVersion` are exactly this task's contract - alongside the version the real selection rule
 * (`selectVersion`) would pick for that call, so a test can assert on the wire version without
 * re-implementing negotiation. It then either returns a canned success response or throws a
 * canned failure, depending on `failWith`/`failOnceWith`.
 *
 * @param {object} options
 * @param {{[topic: string]: string}} [options.topicIds] topic name to topic id, for topics whose
 *   id `cluster.topicId` should answer with; a topic absent here has no known id.
 * @param {{[topic: string]: number}} options.leaders topic name to the single node id leading its
 *   only partition (partition 0).
 * @param {boolean} [options.transactional=false]
 * @param {{errorCode: number, recordErrors?: object[]}} [options.failWith] every produce call
 *   throws a protocol error built from this.
 * @param {{errorCode: number}} [options.failOnceWith] only the first produce call throws; later
 *   calls (i.e. the retry) succeed.
 * @returns {{send: Function, produceCalls: object[], refreshMetadata: jest.Mock}}
 */
const createProducerCluster = ({
  topicIds = {},
  leaders,
  transactional = false,
  failWith,
  failOnceWith,
} = {}) => {
  const produceCalls = []
  const brokersByNode = {}
  let failOnceLeft = !!failOnceWith

  const buildFailure = ({ errorCode, recordErrors }, { topic, partition }) => {
    const error = createErrorFromCode(errorCode)
    error.topic = topic
    error.partition = partition
    if (recordErrors) {
      error.recordErrors = recordErrors
    }
    return error
  }

  const brokerFor = (nodeId) => {
    if (brokersByNode[nodeId] === undefined) {
      brokersByNode[nodeId] = {
        nodeId,
        produce: async ({ topicData, topicIds: requestTopicIds, maxVersion }) => {
          const maxVersions =
            maxVersion === undefined ? undefined : { [apiKeys.Produce]: maxVersion }
          const apiVersion = selectVersion(
            BROKER_VERSIONS,
            apiKeys.Produce,
            requests.Produce,
            maxVersions
          )
          produceCalls.push({
            nodeId,
            topicIds: requestTopicIds,
            maxVersion,
            topicData,
            apiVersion,
          })

          const { topic } = topicData[0]
          const { partition } = topicData[0].partitions[0]

          if (failOnceWith && failOnceLeft) {
            failOnceLeft = false
            throw buildFailure(failOnceWith, { topic, partition })
          }

          if (failWith) {
            throw buildFailure(failWith, { topic, partition })
          }

          return {
            topics: topicData.map((entry) => ({
              topicName: entry.topic,
              partitions: entry.partitions.map((p) => ({
                partition: p.partition,
                errorCode: 0,
                baseOffset: '0',
                logAppendTime: '-1',
                logStartOffset: '0',
              })),
            })),
          }
        },
      }
    }

    return brokersByNode[nodeId]
  }

  const cluster = {
    topicId: (topic) => topicIds[topic],
    findTopicPartitionMetadata: (topic) => {
      const leader = leaders[topic]
      return leader === undefined ? [] : [{ partitionId: 0, leader }]
    },
    findLeaderForPartitions: (topic, partitions) => {
      const leader = leaders[topic]
      return { [leader]: partitions.map(Number) }
    },
    findBroker: async ({ nodeId }) => brokerFor(nodeId),
    refreshMetadataIfNecessary: jest.fn(async () => {}),
    refreshMetadata: jest.fn(async () => {}),
    addMultipleTargetTopics: jest.fn(async () => {}),
    isConnected: () => true,
  }

  const eosManager = {
    acquireBrokerLock: async () => {},
    releaseBrokerLock: async () => {},
    isTransactional: () => transactional,
    addPartitionsToTransaction: async () => {},
    getTransactionalId: () => 'a-transactional-id',
    getProducerId: () => 1,
    getProducerEpoch: () => 0,
    getSequence: () => 0,
    updateSequence: () => {},
  }

  const sendMessages = createSendMessages({
    logger: newLogger(),
    cluster,
    partitioner: createDefaultPartitioner(),
    eosManager,
    retrier: createRetry({ retries: 2, initialRetryTime: 1, maxRetryTime: 2 }),
  })

  const send = ({ acks = -1, timeout = 30000, compression, topicMessages }) =>
    sendMessages({ acks, timeout, compression, topicMessages })

  return { cluster, send, produceCalls, refreshMetadata: cluster.refreshMetadata }
}

describe('Producer > topic ids on the produce path', () => {
  test('v13 is selected when every id in this broker request is known', async () => {
    const { send, produceCalls } = createProducerCluster({
      topicIds: { 'topic-a': TOPIC_A_ID },
      leaders: { 'topic-a': 1 },
    })

    await send({ topicMessages: [{ topic: 'topic-a', messages: [{ value: 'a' }] }] })

    expect(produceCalls).toHaveLength(1)
    expect(produceCalls[0].maxVersion).toBeUndefined()
    expect(produceCalls[0].topicIds).toEqual(new Map([['topic-a', TOPIC_A_ID]]))
    // And the request that came out of it really is v13 - an absent cap is only half the claim.
    expect(produceCalls[0].apiVersion).toEqual(13)
  })

  test('the choice is per broker request: one broker gets v13 and the other v11', async () => {
    // topic-a lives on broker 1 and has an id; topic-b lives on broker 2 and does not. A global
    // rule would downgrade both.
    const { send, produceCalls } = createProducerCluster({
      topicIds: { 'topic-a': TOPIC_A_ID },
      leaders: { 'topic-a': 1, 'topic-b': 2 },
    })

    await send({
      topicMessages: [
        { topic: 'topic-a', messages: [{ value: 'a' }] },
        { topic: 'topic-b', messages: [{ value: 'b' }] },
      ],
    })

    const byNode = Object.fromEntries(produceCalls.map((call) => [call.nodeId, call]))
    expect(byNode[1].maxVersion).toBeUndefined()
    expect(byNode[1].topicIds).toEqual(new Map([['topic-a', TOPIC_A_ID]]))
    expect(byNode[2].maxVersion).toEqual(11)
    expect(byNode[2].topicIds).toBeUndefined()
  })

  test('a transactional send is capped at v11 even when every id is known', async () => {
    const { send, produceCalls } = createProducerCluster({
      topicIds: { 'topic-a': TOPIC_A_ID },
      leaders: { 'topic-a': 1 },
      transactional: true,
    })

    await send({ topicMessages: [{ topic: 'topic-a', messages: [{ value: 'a' }] }] })

    expect(produceCalls[0].maxVersion).toEqual(11)
    expect(produceCalls[0].topicIds).toBeUndefined()
  })

  test('a v13 produce failure carries the topic name and its recordErrors', async () => {
    const { send } = createProducerCluster({
      topicIds: { 'topic-a': TOPIC_A_ID },
      leaders: { 'topic-a': 1 },
      // The broker answers v13 with a partition error and two per-record errors. code 29
      // (TOPIC_AUTHORIZATION_FAILED) is genuinely non-retriable, so the real retrier's `bail()`
      // path surfaces this exact error - no special-cased retriability needed in the fake.
      failWith: { errorCode: 29, recordErrors: [{ batchIndex: 1, batchIndexErrorMessage: 'bad' }] },
    })

    const error = await send({
      topicMessages: [{ topic: 'topic-a', messages: [{ value: 'a' }, { value: 'b' }] }],
    }).catch((e) => e)

    // The restoration happens inside decode, before parse sets error.topic - if it happened later
    // this would be undefined and attachFailingRecords would silently stop matching.
    expect(error.topic).toEqual('topic-a')
    expect(error.recordErrors).toEqual([
      { batchIndex: 1, batchIndexErrorMessage: 'bad', record: { value: 'b' } },
    ])
  })

  test('an unknown topic id on the response takes the existing refresh-and-retry path', async () => {
    const { send, refreshMetadata } = createProducerCluster({
      topicIds: { 'topic-a': TOPIC_A_ID },
      leaders: { 'topic-a': 1 },
      failOnceWith: { errorCode: 100 }, // UNKNOWN_TOPIC_ID, already in staleMetadata()
    })

    await expect(
      send({ topicMessages: [{ topic: 'topic-a', messages: [{ value: 'a' }] }] })
    ).resolves.toBeDefined()
    expect(refreshMetadata).toHaveBeenCalled()
  })
})
