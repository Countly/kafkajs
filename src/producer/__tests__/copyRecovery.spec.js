jest.mock('../../utils/shuffle', () => (list) => [...list])

const { Kafka } = require('../../../index')
const {
  KafkaJSApplicationRecoverableError,
  KafkaJSConnectionError,
  KafkaJSGroupCoordinatorNotFound,
  KafkaJSInvalidConfigurationError,
  KafkaJSNonRetriableError,
  KafkaJSNumberOfRetriesExceeded,
  KafkaJSRequestTimeoutError,
  KafkaJSTransactionAbortableError,
} = require('../../errors')
const { createScriptedCluster, producerScript } = require('../../../testHelpers/scriptedCluster')
const { TOPIC, TOPIC_ID } = require('../../consumer/__tests__/fixtures/fetchHarness')
const { recoveryFor } = require('./fixtures/copyRecovery')

const NODES = [
  { id: 0, host: 'node0', port: 9092 },
  { id: 1, host: 'node1', port: 9092 },
]
const RETRY = { retries: 12, initialRetryTime: 5, multiplier: 2, maxRetryTime: 20, factor: 0 }
const GROUP = {
  consumerGroupMetadata: { groupId: 'g', generationId: 1, memberId: 'm', groupInstanceId: null },
  topics: [{ topic: TOPIC, partitions: [{ partition: 0, offset: '5' }] }],
}

/** @returns {Error} the socket error a reset connection raises */
const reset = () => Object.assign(new Error('read ECONNRESET'), { code: 'ECONNRESET' })

/**
 * A producer on transaction version 2 that has taken over its transactional id, against two nodes:
 * the transaction coordinator, the group coordinator and the partition leader are where the options
 * put them, and the node the client does not know yet is dialled when it is first needed. The
 * client asks the nodes in id order.
 * @param {{ txn?: number, group?: number, leader?: number, takeover?: boolean }} [placement]
 * @returns {Promise<{ cluster: object, producer: object }>}
 */
const rig = async ({ txn = 0, group = 1, leader = 1, takeover = true } = {}) => {
  const cluster = producerScript(createScriptedCluster({ nodes: NODES, profile: 'tv2' }))
    .answer('Metadata', () => ({
      brokers: NODES.map(({ id, host, port }) => ({ nodeId: id, host, port, rack: null })),
      controllerId: 0,
      topics: [
        {
          errorCode: 0,
          name: TOPIC,
          topicId: TOPIC_ID,
          isInternal: false,
          topicAuthorizedOperations: -2147483648,
          partitions: [
            {
              errorCode: 0,
              partitionIndex: 0,
              leaderId: leader,
              leaderEpoch: 5,
              replicas: [leader],
              isr: [leader],
              offlineReplicas: [],
            },
          ],
        },
      ],
    }))
    .answer('InitProducerId', ({ producerEpoch = -1 }) => ({
      producerId: '1000',
      producerEpoch: Math.max(0, producerEpoch + 1),
    }))
    .answer('FindCoordinator', ({ coordinatorKeys, keyType }) => ({
      coordinators: coordinatorKeys.map((key) => {
        const node = NODES[keyType === 1 ? txn : group]
        return {
          key,
          nodeId: node.id,
          host: node.host,
          port: node.port,
          errorCode: 0,
          errorMessage: null,
        }
      }),
    }))
  const producer = new Kafka(cluster.kafkaOptions()).producer({
    transactionalId: 'copier',
    retry: RETRY,
  })
  await producer.connect()
  if (takeover) await (await producer.transaction()).abort()
  return { cluster, producer }
}

/**
 * Ends the open connections to a node the way a broker that went away does.
 * @param {object} cluster the scripted cluster
 * @param {number} node the node's id
 * @returns {Promise<void>}
 */
const loseConnections = async (cluster, node) => {
  for (const socket of cluster.sockets()) {
    if (socket.endpoint.host === `node${node}` && !socket.destroyed) socket.emit('error', reset())
  }
  await new Promise((resolve) => setImmediate(resolve))
}

/**
 * One chunk as the copier copies it: begin, send, send the offsets, commit.
 * @param {object} producer
 * @returns {Promise<{ transaction: object|null, error: Error|null }>} the transaction, once it
 *   began, and what the first failing call rejected with
 */
const copyChunk = async (producer) => {
  let transaction = null
  try {
    transaction = await producer.transaction()
    await transaction.send({ topic: TOPIC, messages: [{ partition: 0, value: 'x' }] })
    await transaction.sendOffsets(GROUP)
    await transaction.commit()
    return { transaction, error: null }
  } catch (error) {
    return { transaction, error }
  }
}

/**
 * @param {object} cluster the scripted cluster
 * @param {number} since how many requests to skip
 * @returns {string[]} what the cluster was asked from `since` on, as `Api@node`
 */
const askedSince = (cluster, since) =>
  cluster
    .requests()
    .slice(since)
    .map(({ api, node }) => `${api}@${node}`)

describe('Producer > the copier answers a failed chunk as the Java exactly-once loop does', () => {
  describe('a transaction() whose coordinator connection is reset', () => {
    it('rejects at once with the retriable error, without a retry, and the same producer begins the next one', async () => {
      const { cluster, producer } = await rig({ txn: 0 })
      await loseConnections(cluster, 0)
      cluster.schedule.once({ api: 'ApiVersions', node: 0 }, reset())
      const since = cluster.requests().length

      const failed = await copyChunk(producer)

      expect(failed.transaction).toBeNull()
      expect(failed.error).toBeInstanceOf(KafkaJSConnectionError)
      expect(failed.error).toMatchObject({
        code: 'ECONNRESET',
        retriable: true,
        category: 'retriable',
      })
      expect(failed.error.cause).toBeUndefined()
      expect(askedSince(cluster, since)).toEqual(['ApiVersions@0'])
      expect(recoveryFor(failed.error, { begun: false })).toBe('retry')

      const retried = await copyChunk(producer)

      expect(retried.error).toBeNull()
      expect(
        cluster
          .requests()
          .filter(
            ({ api, fields }) => api === 'InitProducerId' && fields.transactionalId === 'copier'
          )
      ).toHaveLength(1)
      await producer.disconnect()
    })

    it('is the same for the takeover of a replacement, which the next call completes', async () => {
      const { cluster, producer } = await rig({ txn: 1, takeover: false })
      cluster.schedule.once({ api: 'ApiVersions', node: 1 }, reset())
      const since = cluster.requests().length

      const failed = await copyChunk(producer)

      expect(failed.error).toMatchObject({ code: 'ECONNRESET', retriable: true })
      expect(askedSince(cluster, since)).toEqual(['FindCoordinator@0', 'ApiVersions@1'])

      expect((await copyChunk(producer)).error).toBeNull()
      await producer.disconnect()
    })
  })

  it('aborts a chunk whose sendOffsets lost its coordinator lookup, and the same producer commits the next', async () => {
    const { cluster, producer } = await rig({ group: 1, leader: 0 })
    cluster.schedule.once({ api: 'ApiVersions', node: 1 }, reset())

    const failed = await copyChunk(producer)

    expect(failed.error).toBeInstanceOf(KafkaJSTransactionAbortableError)
    expect(failed.error.cause).toMatchObject({ code: 'ECONNRESET', retriable: true })
    expect(recoveryFor(failed.error, { begun: true })).toBe('abort')
    await failed.transaction.abort()

    expect((await copyChunk(producer)).error).toBeNull()
    await producer.disconnect()
  })

  it.each(['Produce', 'TxnOffsetCommit', 'EndTxn'])(
    'commits the chunk at once when a %s is reset in flight, the client retrying it itself',
    async (api) => {
      const { cluster, producer } = await rig({ group: 0, leader: 0 })
      cluster.schedule.once({ api }, reset())
      const since = cluster.requests().length

      const { error } = await copyChunk(producer)

      expect(error).toBeNull()
      expect(askedSince(cluster, since).filter((asked) => asked.startsWith(api))).toHaveLength(2)
      await producer.disconnect()
    }
  )

  describe('recoveryFor', () => {
    const retried = new KafkaJSNumberOfRetriesExceeded(new KafkaJSConnectionError('lost'), {
      retryCount: 12,
      retryTime: 3000,
    })

    it.each([
      ['a lost connection', new KafkaJSConnectionError('lost')],
      ['a request timeout', new KafkaJSRequestTimeoutError('timed out')],
      ['a spent retry budget over a retriable error', retried],
      ['a coordinator that was not found', new KafkaJSGroupCoordinatorNotFound('none')],
    ])('begins again on the same producer after %s when nothing was begun', (_name, error) => {
      expect(recoveryFor(error, { begun: false })).toBe('retry')
    })

    it.each([
      [
        'an error that leaves the producer unusable',
        new KafkaJSApplicationRecoverableError('fenced'),
      ],
      ['a configuration error', new KafkaJSInvalidConfigurationError('acl')],
      ['a plain failure', new Error('boom')],
      [
        'a spent retry budget over a non-retriable error',
        new KafkaJSNumberOfRetriesExceeded(new Error('x'), { retryCount: 1, retryTime: 1 }),
      ],
      ['a state error', new KafkaJSNonRetriableError('There is already an ongoing transaction')],
    ])('replaces the producer after %s', (_name, error) => {
      expect(recoveryFor(error, { begun: false })).toBe('replace')
    })

    it('aborts an abortable error and replaces after any other failure of a begun transaction', () => {
      expect(recoveryFor(new KafkaJSTransactionAbortableError('abort me'), { begun: true })).toBe(
        'abort'
      )
      expect(recoveryFor(new KafkaJSConnectionError('lost'), { begun: true })).toBe('replace')
      expect(recoveryFor(new KafkaJSApplicationRecoverableError('fenced'), { begun: true })).toBe(
        'replace'
      )
    })
  })
})
