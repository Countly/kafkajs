const { createGroupApiBroker } = require('../../broker/__tests__/fixtures/groupApiHarness')
const apiKeys = require('../../protocol/requests/apiKeys')
const Decoder = require('../../protocol/decoder')
const Encoder = require('../../protocol/encoder')
const { createCluster, newLogger } = require('testHelpers')

const TOPIC_ID = '01020304-0506-0708-090a-0b0c0d0e0f10'
const SECOND_TOPIC_ID = '21222324-2526-2728-292a-2b2c2d2e2f30'
const DEAD_TOPIC_ID = '11121314-1516-1718-191a-1b1c1d1e1f20'
const ROUTED_TOPIC_ID = '31323334-3536-3738-393a-3b3c3d3e3f40'
const TOPIC_NAME = 'resolved-topic'
const SECOND_TOPIC_NAME = 'second-topic'
const ROUTED_TOPIC = 'routed-topic'
const UNKNOWN_TOPIC_ID = 100

/** The topics this fake cluster hosts. Anything else answers UNKNOWN_TOPIC_ID. */
const HOSTED = new Map([
  [TOPIC_ID, TOPIC_NAME],
  [SECOND_TOPIC_ID, SECOND_TOPIC_NAME],
])

/**
 * A flexible Metadata response body (v10-v13 share this shape) carrying one broker and the given
 * topics.
 *
 * This is the answer a broker gives to a by-id request: a topic comes back with both its id and
 * its name, which is the whole content of an id-to-name resolution. A topic the broker cannot
 * serve comes back with an error code and a null name, which is what makes the batch throw.
 *
 * @param {Array<{ topicId: string, name: string|null, errorCode?: number }>} topics
 * @returns {Buffer} the response body, without a header
 */
const metadataBody = (topics) => {
  const broker = new Encoder()
    .writeInt32(0)
    .writeUVarIntString('127.0.0.1')
    .writeInt32(9092)
    .writeUVarIntString(null)
    .writeTaggedFields()

  const partition = new Encoder()
    .writeInt16(0)
    .writeInt32(0)
    .writeInt32(0)
    .writeInt32(5)
    .writeUVarIntArray([0], 'int32')
    .writeUVarIntArray([0], 'int32')
    .writeUVarIntArray([], 'int32')
    .writeTaggedFields()

  const encodeTopic = ({ topicId, name, errorCode = 0 }) =>
    new Encoder()
      .writeInt16(errorCode)
      .writeUVarIntString(name)
      .writeUuid(topicId)
      .writeBoolean(false)
      .writeUVarIntArray(errorCode === 0 ? [partition] : [])
      .writeInt32(-2147483648)
      .writeTaggedFields()

  return new Encoder()
    .writeInt32(0)
    .writeUVarIntArray([broker])
    .writeUVarIntString('test-cluster')
    .writeInt32(0)
    .writeUVarIntArray(topics.map(encodeTopic))
    .writeTaggedFields().buffer
}

/**
 * A decoded Metadata response of the shape `BrokerPool#refreshMetadata` assigns to
 * `brokerPool.metadata` - the object every cluster route reads through.
 * @returns {object}
 */
const routedMetadata = () => ({
  brokers: [{ nodeId: 0 }],
  topicMetadata: [
    {
      topic: ROUTED_TOPIC,
      topicId: ROUTED_TOPIC_ID,
      partitionMetadata: [{ partitionId: 0, leader: 0, leaderEpoch: 3 }],
    },
  ],
})

describe('Cluster > refreshMetadataForTopicIds', () => {
  let harness
  let cluster

  /** The topic ids the fake broker was last asked for. */
  const askedTopicIds = () => {
    const request = harness.requests[harness.requests.length - 1]
    return new Decoder(request.body).readUVarIntArray((d) => {
      const topicId = d.readUuid()
      d.readUVarIntString()
      d.readTaggedFields()
      return topicId
    })
  }

  /** Answers whatever was asked: hosted ids by name, anything else with UNKNOWN_TOPIC_ID. */
  const answerAsked = () =>
    metadataBody(
      askedTopicIds().map((topicId) =>
        HOSTED.has(topicId)
          ? { topicId, name: HOSTED.get(topicId) }
          : { topicId, name: null, errorCode: UNKNOWN_TOPIC_ID }
      )
    )

  /**
   * Points a real Cluster at a real Broker over a fake socket, at one negotiated Metadata version.
   * @param {number} maxVersion the highest Metadata version the fake broker advertises
   * @param {() => Buffer} [respond] the response body builder
   */
  const useBroker = async (maxVersion, respond = answerAsked) => {
    if (harness) {
      await harness.pool.destroy()
    }

    harness = createGroupApiBroker({
      versions: { [apiKeys.Metadata]: { minVersion: 0, maxVersion } },
      respond,
    })
    cluster.brokerPool.withBroker = async (callback) => callback({ broker: harness.broker })
  }

  beforeEach(async () => {
    harness = null
    cluster = createCluster({ logger: newLogger() })
    cluster.refreshMetadata = jest.fn()
    await useBroker(12)
  })

  afterEach(async () => {
    if (harness) {
      await harness.pool.destroy()
      harness = null
    }
  })

  test('asks by topic id, with a null name and no auto-creation', async () => {
    await cluster.refreshMetadataForTopicIds([TOPIC_ID])

    const decoder = new Decoder(harness.requests[0].body)
    const topics = decoder.readUVarIntArray((d) => {
      const topic = { topicId: d.readUuid(), name: d.readUVarIntString() }
      d.readTaggedFields()
      return topic
    })

    expect(harness.requests[0].apiKey).toEqual(apiKeys.Metadata)
    expect(topics).toEqual([{ topicId: TOPIC_ID, name: null }])
    expect(decoder.readBoolean()).toBe(false)
  })

  test('resolves the id to a name, readable through the cluster accessor', async () => {
    // The whole point of the call: after it, the assignment resolver can name the topic - and it
    // reads the name through `cluster.topicName`, never through the cache itself.
    const result = await cluster.refreshMetadataForTopicIds([TOPIC_ID])

    expect(result).toEqual({ resolved: [TOPIC_ID], unresolved: [] })
    expect(cluster.topicName(TOPIC_ID)).toEqual(TOPIC_NAME)
    expect(cluster.metadataCache.topicName(TOPIC_ID)).toEqual(TOPIC_NAME)
  })

  test('answers undefined for an id it has never seen', async () => {
    expect(cluster.topicName(DEAD_TOPIC_ID)).toBeUndefined()
  })

  test('leaves brokerPool.metadata alone, so every other topic stays routable', async () => {
    // The reason this does not go through BrokerPool#refreshMetadata: that method replaces
    // `brokerPool.metadata` wholesale, and `findTopicPartitionMetadata` reads it for EVERY topic.
    const before = cluster.brokerPool.metadata

    await cluster.refreshMetadataForTopicIds([TOPIC_ID])

    expect(cluster.brokerPool.metadata).toBe(before)
  })

  test.each([9, 10, 11])('falls back to a refresh by name at Metadata v%i', async (maxVersion) => {
    // v10 and v11 carry TopicId in the schema but the server never reads it there
    // (MetadataRequest.json), so the fallback floor is v12, not v10.
    await useBroker(maxVersion)

    const result = await cluster.refreshMetadataForTopicIds([TOPIC_ID])

    expect(harness.requests).toHaveLength(0)
    expect(cluster.refreshMetadata).toHaveBeenCalled()
    expect(result).toEqual({ resolved: [], unresolved: [TOPIC_ID] })
  })

  test('sends nothing for an empty or all-null id list, and asks once for duplicates', async () => {
    await expect(cluster.refreshMetadataForTopicIds([])).resolves.toEqual({
      resolved: [],
      unresolved: [],
    })
    await cluster.refreshMetadataForTopicIds([null, undefined])

    expect(harness.requests).toHaveLength(0)

    await cluster.refreshMetadataForTopicIds([TOPIC_ID, TOPIC_ID])

    expect(harness.requests).toHaveLength(1)
  })

  test('leaves a single unknown id unresolved without falling back to a full refresh', async () => {
    // UNKNOWN_TOPIC_ID (100) is what a broker answers for an id it does not host, and the
    // Metadata response codec throws on the first failing topic. With one id there is nothing to
    // rescue, so it is not re-asked - and it must not become a full metadata refresh either: this
    // runs at the heartbeat cadence.
    const result = await cluster.refreshMetadataForTopicIds([DEAD_TOPIC_ID])

    expect(result).toEqual({ resolved: [], unresolved: [DEAD_TOPIC_ID] })
    expect(harness.requests).toHaveLength(1)
    expect(cluster.refreshMetadata).not.toHaveBeenCalled()
    expect(cluster.topicName(DEAD_TOPIC_ID)).toBeUndefined()
  })

  test('re-asks one id at a time when a dead id poisons the batch', async () => {
    // `parse` throws on the FIRST errored topic, so one id the cluster will never resolve discards
    // the ids that decoded perfectly beside it. Left alone that id would block every other id for
    // as long as the membership holds it, so the batch is re-asked per id.
    const result = await cluster.refreshMetadataForTopicIds([
      TOPIC_ID,
      DEAD_TOPIC_ID,
      SECOND_TOPIC_ID,
    ])

    expect(harness.requests).toHaveLength(4) // the batch, then one per id
    expect(result).toEqual({
      resolved: [TOPIC_ID, SECOND_TOPIC_ID],
      unresolved: [DEAD_TOPIC_ID],
    })
    expect(cluster.topicName(TOPIC_ID)).toEqual(TOPIC_NAME)
    expect(cluster.topicName(SECOND_TOPIC_ID)).toEqual(SECOND_TOPIC_NAME)
    expect(cluster.topicName(DEAD_TOPIC_ID)).toBeUndefined()
  })

  test('does not re-ask per id when the failure was not about one topic', async () => {
    // A transport failure says nothing about any particular id, so retrying each one would be
    // three more doomed round trips at the heartbeat cadence.
    await useBroker(12, () => Buffer.from([0xff]))

    const result = await cluster.refreshMetadataForTopicIds([TOPIC_ID, SECOND_TOPIC_ID])

    expect(harness.requests).toHaveLength(1)
    expect(result).toEqual({ resolved: [], unresolved: [TOPIC_ID, SECOND_TOPIC_ID] })
  })

  test('resolves rather than throwing when the pool has no broker', async () => {
    // `withBroker` throws KafkaJSBrokerNotFound on an empty pool, which is reachable before the
    // first connect and right after a rebootstrap. This is called from the heartbeat path, where
    // a throw would take the membership down over a metadata hiccup.
    const { KafkaJSBrokerNotFound } = require('../../errors')
    cluster.brokerPool.withBroker = async () => {
      throw new KafkaJSBrokerNotFound('No brokers in the broker pool')
    }

    await expect(cluster.refreshMetadataForTopicIds([TOPIC_ID])).resolves.toEqual({
      resolved: [],
      unresolved: [TOPIC_ID],
    })
  })

  test('resolves rather than throwing when the fallback refresh rejects', async () => {
    await useBroker(9)
    cluster.refreshMetadata = jest.fn().mockRejectedValue(new Error('nope'))

    await expect(cluster.refreshMetadataForTopicIds([TOPIC_ID])).resolves.toEqual({
      resolved: [],
      unresolved: [TOPIC_ID],
    })
  })

  describe('the routing path is untouched', () => {
    beforeEach(() => {
      const routed = routedMetadata()
      cluster.brokerPool.metadata = routed
      cluster.metadataCache.update(routed)
    })

    const hint = () =>
      cluster.metadataCache.applyLeaderHint(ROUTED_TOPIC, 0, { leaderId: 1, leaderEpoch: 4 })

    test('a leader hint on another topic still reaches the objects the routes read', async () => {
      // Putting a response nothing routes through `MetadataCache#update` would bump the generation
      // and release the write-through reference of every topic the response did not carry - after
      // which `applyLeaderHint` still answers true but writes where nothing reads.
      await cluster.refreshMetadataForTopicIds([TOPIC_ID])

      expect(hint()).toBe(true)
      expect(cluster.findTopicPartitionMetadata(ROUTED_TOPIC)[0]).toMatchObject({
        leader: 1,
        leaderEpoch: 4,
      })

      expect(cluster.metadataCache.forgetLeaderEpoch(ROUTED_TOPIC, 0)).toBe(true)
      expect(cluster.findTopicPartitionMetadata(ROUTED_TOPIC)[0].leaderEpoch).toEqual(-1)
    })

    test('control: the same hint with no by-id refresh in between', () => {
      expect(hint()).toBe(true)
      expect(cluster.findTopicPartitionMetadata(ROUTED_TOPIC)[0]).toMatchObject({
        leader: 1,
        leaderEpoch: 4,
      })
    })

    test('a by-id answer for a routed topic advances both views together', () => {
      // The cached pair and its write-through copy must not diverge: an accepted pair is written
      // onto the routing object exactly as an accepted leader hint is.
      cluster.metadataCache.updateTopicsById({
        brokers: [{ nodeId: 0 }],
        topicMetadata: [
          {
            topic: ROUTED_TOPIC,
            topicId: ROUTED_TOPIC_ID,
            partitionMetadata: [{ partitionId: 0, leader: 1, leaderEpoch: 7 }],
          },
        ],
      })

      expect(cluster.leaderEpoch(ROUTED_TOPIC, 0)).toEqual(7)
      expect(cluster.findTopicPartitionMetadata(ROUTED_TOPIC)[0]).toMatchObject({
        leader: 1,
        leaderEpoch: 7,
      })
    })

    test('a stale by-id answer changes neither view', () => {
      cluster.metadataCache.updateTopicsById({
        brokers: [{ nodeId: 0 }],
        topicMetadata: [
          {
            topic: ROUTED_TOPIC,
            topicId: ROUTED_TOPIC_ID,
            partitionMetadata: [{ partitionId: 0, leader: 1, leaderEpoch: 1 }],
          },
        ],
      })

      expect(cluster.leaderEpoch(ROUTED_TOPIC, 0)).toEqual(3)
      expect(cluster.findTopicPartitionMetadata(ROUTED_TOPIC)[0]).toMatchObject({
        leader: 0,
        leaderEpoch: 3,
      })
    })

    test('a topic the broker reports without a usable id is recorded but not named', () => {
      // A broker answers with the all-zero "no uuid" while a topic is being created or deleted.
      // It is not an id: nothing may be indexed under it, and a remembered id is not erased by it.
      const answer = (leaderEpoch) => ({
        brokers: [{ nodeId: 0 }],
        topicMetadata: [
          {
            topic: 'pending-topic',
            topicId: '00000000-0000-0000-0000-000000000000',
            partitionMetadata: [{ partitionId: 0, leader: 0, leaderEpoch }],
          },
        ],
      })

      cluster.metadataCache.updateTopicsById(answer(1))

      expect(cluster.topicId('pending-topic')).toBeUndefined()
      expect(cluster.topicName('00000000-0000-0000-0000-000000000000')).toBeUndefined()
      expect(cluster.leaderEpoch('pending-topic', 0)).toEqual(1)

      // The entry has no write-through reference of its own - the topic is in no routed response -
      // so a later answer advances the cached pair and nothing else.
      cluster.metadataCache.updateTopicsById(answer(2))

      expect(cluster.leaderEpoch('pending-topic', 0)).toEqual(2)
    })

    test('a topic recreated under a new id is detected through the by-id path', async () => {
      cluster.metadataCache.updateTopicsById({
        brokers: [{ nodeId: 0 }],
        topicMetadata: [
          {
            topic: ROUTED_TOPIC,
            topicId: TOPIC_ID,
            partitionMetadata: [{ partitionId: 0, leader: 0, leaderEpoch: 0 }],
          },
        ],
      })

      expect(cluster.topicId(ROUTED_TOPIC)).toEqual(TOPIC_ID)
      expect(cluster.topicName(ROUTED_TOPIC_ID)).toBeUndefined()
      expect(cluster.topicName(TOPIC_ID)).toEqual(ROUTED_TOPIC)
      expect(cluster.leaderEpoch(ROUTED_TOPIC, 0)).toEqual(0)
    })
  })
})
