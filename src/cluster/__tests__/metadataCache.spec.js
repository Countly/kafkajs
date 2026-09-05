const MetadataCache = require('../metadataCache')
const InstrumentationEventEmitter = require('../../instrumentation/emitter')
const { CLUSTER_TOPIC_RECREATED } = require('../instrumentationEvents')
const { newLogger } = require('testHelpers')

const ID_A = '01020304-0506-0708-090a-0b0c0d0e0f10'
const ID_B = '11121314-1516-1718-191a-1b1c1d1e1f20'
const NO_TOPIC_ID = '00000000-0000-0000-0000-000000000000'

/**
 * A decoded Metadata response shaped exactly like metadata/v12/response.js#decode returns:
 * the objects under partitionMetadata are the ones the cache writes through onto.
 */
const response = ({ topicId = ID_A, leader = 1, leaderEpoch = 5, brokers = [0, 1, 2] } = {}) => ({
  throttleTime: 0,
  clientSideThrottleTime: 0,
  brokers: brokers.map((nodeId) => ({
    nodeId,
    host: '127.0.0.1',
    port: 9092 + nodeId,
    rack: null,
  })),
  clusterId: 'test-cluster-id',
  controllerId: 0,
  topicMetadata: [
    {
      topicErrorCode: 0,
      topic: 'test-topic',
      topicId,
      isInternal: false,
      topicAuthorizedOperations: -2147483648,
      partitionMetadata: [
        {
          partitionErrorCode: 0,
          partitionId: 0,
          leader,
          leaderEpoch,
          replicas: [1],
          isr: [1],
          offlineReplicas: [],
        },
      ],
    },
  ],
})

const partition0 = (metadata) => metadata.topicMetadata[0].partitionMetadata[0]

describe('Cluster > MetadataCache', () => {
  let cache
  let emitter
  let events

  beforeEach(() => {
    emitter = new InstrumentationEventEmitter()
    events = []
    emitter.addListener(CLUSTER_TOPIC_RECREATED, (event) => events.push(event.payload))
    cache = new MetadataCache({ logger: newLogger(), instrumentationEmitter: emitter })
  })

  test('records topic ids in both directions', () => {
    cache.update(response())
    expect(cache.topicId('test-topic')).toEqual(ID_A)
    expect(cache.topicName(ID_A)).toEqual('test-topic')
    expect(cache.topicId('nope')).toBeUndefined()
  })

  test('records leader epochs, and reports -1 for anything it has not seen', () => {
    cache.update(response({ leaderEpoch: 5 }))
    expect(cache.leaderEpoch('test-topic', 0)).toEqual(5)
    expect(cache.leaderEpoch('test-topic', 9)).toEqual(-1)
    expect(cache.leaderEpoch('other', 0)).toEqual(-1)
  })

  test('leaderEpoch() never returns undefined, even for a decoded partition missing the field', () => {
    // Kafka 4.0+ Metadata codecs always decode leaderEpoch, but leaderEpoch() is a public
    // contract ("-1 when unknown") that ListOffsets feeds straight onto the wire.
    const metadata = response()
    delete metadata.topicMetadata[0].partitionMetadata[0].leaderEpoch
    cache.update(metadata)
    expect(cache.leaderEpoch('test-topic', 0)).toEqual(-1)
  })

  test('a known name arriving with a different id emits TOPIC_RECREATED', () => {
    cache.update(response({ topicId: ID_A }))
    cache.update(response({ topicId: ID_B }))

    expect(events).toEqual([{ topic: 'test-topic', previousTopicId: ID_A, topicId: ID_B }])
    expect(cache.topicId('test-topic')).toEqual(ID_B)
    expect(cache.topicName(ID_A)).toBeUndefined()
  })

  test('the first sighting of a topic is not a recreation', () => {
    cache.update(response({ topicId: ID_A }))
    expect(events).toEqual([])
  })

  test('a deleted window (the topic answered with the all-zero id) keeps the remembered id, so the recreation that follows is detected', () => {
    cache.update(response({ topicId: ID_A }))

    const deleted = response({ topicId: NO_TOPIC_ID })
    deleted.topicMetadata[0].topicErrorCode = 3
    deleted.topicMetadata[0].partitionMetadata = []
    cache.update(deleted)

    expect(events).toEqual([])
    expect(cache.topicId('test-topic')).toEqual(ID_A)

    cache.update(response({ topicId: ID_B, leaderEpoch: 0 }))

    expect(events).toEqual([{ topic: 'test-topic', previousTopicId: ID_A, topicId: ID_B }])
    expect(cache.topicId('test-topic')).toEqual(ID_B)
  })

  test('a topic first seen with the all-zero id is not a recreation when its real id arrives', () => {
    cache.update(response({ topicId: NO_TOPIC_ID }))
    cache.update(response({ topicId: ID_A }))

    expect(events).toEqual([])
    expect(cache.topicId('test-topic')).toEqual(ID_A)
  })

  test('a topic-id change resets that topic epochs, so epoch 0 on the new incarnation is accepted', () => {
    cache.update(response({ topicId: ID_A, leader: 1, leaderEpoch: 9 }))
    const fresh = response({ topicId: ID_B, leader: 2, leaderEpoch: 0 })
    cache.update(fresh)

    expect(cache.leaderEpoch('test-topic', 0)).toEqual(0)
    expect(partition0(fresh).leader).toEqual(2)
    expect(partition0(fresh).leaderEpoch).toEqual(0)
  })

  test('a cleared cache accepts epoch 0 afterwards, and the routing structure points at the new leader', () => {
    cache.update(response({ leader: 1, leaderEpoch: 9 }))
    cache.clear()

    expect(cache.topicId('test-topic')).toBeUndefined()
    expect(cache.leaderEpoch('test-topic', 0)).toEqual(-1)

    const fresh = response({ topicId: ID_B, leader: 2, leaderEpoch: 0 })
    cache.update(fresh)
    expect(partition0(fresh).leader).toEqual(2)
    expect(events).toEqual([]) // clear() forgets the id, so this is a first sighting, not a recreation
  })

  test('a cached leader whose node id is absent from the fresh broker list is not re-written', () => {
    cache.update(response({ leader: 7, leaderEpoch: 9, brokers: [7] }))
    // node 7 is gone; the fresh response has a lower epoch, which would normally be re-written
    const fresh = response({ leader: 2, leaderEpoch: 3, brokers: [0, 1, 2] })
    cache.update(fresh)

    expect(partition0(fresh).leader).toEqual(2)
    expect(partition0(fresh).leaderEpoch).toEqual(3)
    expect(cache.leaderEpoch('test-topic', 0)).toEqual(3)
  })

  test('an update carrying a lower epoch does not clobber the cached leader', () => {
    cache.update(response({ leader: 1, leaderEpoch: 9 }))
    const stale = response({ leader: 2, leaderEpoch: 4 })
    cache.update(stale)

    expect(partition0(stale).leader).toEqual(1)
    expect(partition0(stale).leaderEpoch).toEqual(9)
    expect(cache.leaderEpoch('test-topic', 0)).toEqual(9)
  })

  test('an equal epoch takes the decoded value', () => {
    cache.update(response({ leader: 1, leaderEpoch: 9 }))
    const same = response({ leader: 2, leaderEpoch: 9 })
    cache.update(same)
    expect(partition0(same).leader).toEqual(2)
  })

  test('an unknown (-1) epoch on either side lets the decoded value win outright', () => {
    // decoded -1: near-unreachable in production (Metadata parse throws on any partition error
    // code), kept as a defensive floor and tested as one.
    cache.update(response({ leader: 1, leaderEpoch: 9 }))
    const unknown = response({ leader: 3, leaderEpoch: -1 })
    cache.update(unknown)
    expect(partition0(unknown).leader).toEqual(3)
    expect(cache.leaderEpoch('test-topic', 0)).toEqual(-1)

    // cached -1: never defends, never overwrites a known epoch with -1
    const known = response({ leader: 4, leaderEpoch: 2 })
    cache.update(known)
    expect(partition0(known).leader).toEqual(4)
    expect(cache.leaderEpoch('test-topic', 0)).toEqual(2)
  })

  test('an update from a single-topic admin refresh leaves every other topic cached epochs and ids intact', () => {
    cache.update(response({ topicId: ID_A, leaderEpoch: 9 }))

    const scoped = response({ topicId: ID_B, leaderEpoch: 1 })
    scoped.topicMetadata[0].topic = 'other-topic'
    cache.update(scoped)

    expect(cache.topicId('test-topic')).toEqual(ID_A)
    expect(cache.leaderEpoch('test-topic', 0)).toEqual(9)
    expect(events).toEqual([])
  })

  test('reuses the cached partition entry across refreshes and allocates only on first sight', () => {
    // The admin client refreshes metadata on every call (metadataMaxAge 0), so a per-refresh
    // allocation per partition would be per-call garbage proportional to topic x partition count.
    // Read through the instance's own fields - no accessor is added to production for this.
    const entry = () => cache.topics.get('test-topic').partitions.get(0)

    cache.update(response({ leader: 1, leaderEpoch: 5 }))
    const first = entry()
    const partitionsMap = cache.topics.get('test-topic').partitions

    cache.update(response({ leader: 1, leaderEpoch: 6 }))
    expect(entry()).toBe(first)
    expect(cache.topics.get('test-topic').partitions).toBe(partitionsMap)
    expect(first.leaderEpoch).toEqual(6)
  })

  describe('applyLeaderHint', () => {
    test('accepts a higher epoch and writes through to the routing structure', () => {
      const metadata = response({ leader: 1, leaderEpoch: 5 })
      cache.update(metadata)

      expect(cache.applyLeaderHint('test-topic', 0, { leaderId: 2, leaderEpoch: 6 })).toBe(true)
      expect(cache.leaderEpoch('test-topic', 0)).toEqual(6)
      expect(partition0(metadata).leader).toEqual(2)
      expect(partition0(metadata).leaderEpoch).toEqual(6)
    })

    test('rejects an equal or lower epoch and leaves routing untouched', () => {
      const metadata = response({ leader: 1, leaderEpoch: 5 })
      cache.update(metadata)

      expect(cache.applyLeaderHint('test-topic', 0, { leaderId: 2, leaderEpoch: 5 })).toBe(false)
      expect(cache.applyLeaderHint('test-topic', 0, { leaderId: 2, leaderEpoch: 4 })).toBe(false)
      expect(partition0(metadata).leader).toEqual(1)
    })

    test('accepts a hint for a partition whose epoch is unknown', () => {
      const metadata = response({ leader: 1, leaderEpoch: -1 })
      cache.update(metadata)
      expect(cache.applyLeaderHint('test-topic', 0, { leaderId: 2, leaderEpoch: 0 })).toBe(true)
      expect(partition0(metadata).leader).toEqual(2)
    })

    test('is a no-op for a topic or partition the cache has never seen', () => {
      expect(cache.applyLeaderHint('nope', 0, { leaderId: 2, leaderEpoch: 1 })).toBe(false)
    })

    test('ignores a hint whose leaderId is the CurrentLeader default sentinel (-1), regardless of epoch', () => {
      // ProduceResponse's CurrentLeader defaults to { LeaderId: -1, LeaderEpoch: -1 } - a cached
      // epoch of -1 would otherwise let this hint through, writing nodeId -1 into routing.
      const metadata = response({ leader: 1, leaderEpoch: -1 })
      cache.update(metadata)

      expect(cache.applyLeaderHint('test-topic', 0, { leaderId: -1, leaderEpoch: 5 })).toBe(false)
      expect(partition0(metadata).leader).toEqual(1)
      expect(cache.leaderEpoch('test-topic', 0)).toEqual(-1)
    })

    test('a hint accepted after a defended stale refresh still moves routing', () => {
      // The defend branch keeps the cached leader/epoch but must still re-seat the entry's
      // reference to the *current* response's partition object: brokerPool.metadata is
      // wholesale-replaced on every refresh, so a stale reference would make the next accepted
      // hint write into an object no reader holds - silently, with no throw and no log.
      cache.update(response({ leader: 1, leaderEpoch: 5 }))
      expect(cache.applyLeaderHint('test-topic', 0, { leaderId: 2, leaderEpoch: 6 })).toBe(true)

      const stale = response({ leader: 1, leaderEpoch: 5 })
      cache.update(stale)
      expect(partition0(stale).leader).toEqual(2) // defended

      expect(cache.applyLeaderHint('test-topic', 0, { leaderId: 3, leaderEpoch: 7 })).toBe(true)
      expect(partition0(stale).leader).toEqual(3)
      expect(partition0(stale).leaderEpoch).toEqual(7)
    })
  })

  describe('a throwing TOPIC_RECREATED listener', () => {
    beforeEach(() => {
      // Registered after the recording listener in the outer beforeEach, so the recording one
      // still runs: what is asserted here is the cache's own resilience, not listener ordering.
      emitter.addListener(CLUSTER_TOPIC_RECREATED, () => {
        throw new Error('boom from a listener')
      })
    })

    test('cannot fail the update, and cannot stop the cache from advancing', () => {
      cache.update(response({ topicId: ID_A }))

      expect(() => cache.update(response({ topicId: ID_B }))).not.toThrow()
      expect(cache.topicId('test-topic')).toEqual(ID_B)

      // The entry advanced, so the recreation is not re-diffed (and not re-emitted) forever.
      expect(() => cache.update(response({ topicId: ID_B }))).not.toThrow()
      expect(events).toEqual([{ topic: 'test-topic', previousTopicId: ID_A, topicId: ID_B }])
    })
  })

  test('the all-zero topic id is treated as no id at all', () => {
    cache.update(response({ topicId: '00000000-0000-0000-0000-000000000000' }))
    expect(cache.topicId('test-topic')).toBeUndefined()

    // The first real id is therefore a first sighting, not a recreation.
    cache.update(response({ topicId: ID_A }))
    expect(events).toEqual([])
    expect(cache.topicId('test-topic')).toEqual(ID_A)
  })

  describe('retention', () => {
    const otherTopic = (options) => {
      const metadata = response(options)
      metadata.topicMetadata[0].topic = 'other-topic'
      return metadata
    }

    test('a scoped refresh releases the decoded objects of the topics it does not carry', () => {
      // Those objects belong to a response no route reads any more; holding them pins the whole
      // partition record (replicas/isr/offlineReplicas included) for the client's lifetime.
      const first = response({ topicId: ID_A, leaderEpoch: 5 })
      cache.update(first)
      expect(cache.topics.get('test-topic').partitions.get(0).decoded).toBe(partition0(first))

      cache.update(otherTopic({ topicId: ID_B, leaderEpoch: 1 }))

      const cached = cache.topics.get('test-topic').partitions.get(0)
      expect(cached.decoded).toBeNull()
      // The id and the epoch stay: absence from a scoped response is not evidence of anything.
      expect(cached.leaderEpoch).toEqual(5)
      expect(cache.topicId('test-topic')).toEqual(ID_A)
    })

    test('a full-cluster refresh prunes the topics it does not carry, ids included', () => {
      cache.update(response({ topicId: ID_A }))
      cache.update(otherTopic({ topicId: ID_B }), { full: true })

      expect(cache.topics.has('test-topic')).toBe(false)
      expect(cache.topicId('test-topic')).toBeUndefined()
      expect(cache.topicName(ID_A)).toBeUndefined()
      expect(cache.topicId('other-topic')).toEqual(ID_B)
    })

    test('a topic the refresh does carry keeps its entry, and nothing is reallocated', () => {
      cache.update(response({ topicId: ID_A, leaderEpoch: 5 }))
      const entry = cache.topics.get('test-topic')
      const cached = entry.partitions.get(0)

      const fresh = response({ topicId: ID_A, leaderEpoch: 6 })
      cache.update(fresh, { full: true })

      expect(cache.topics.get('test-topic')).toBe(entry)
      expect(entry.partitions.get(0)).toBe(cached)
      expect(cached.decoded).toBe(partition0(fresh))
    })

    test('a hint still moves the leader for a topic whose decoded object was released', () => {
      cache.update(response({ leader: 1, leaderEpoch: 5 }))
      cache.update(otherTopic({ topicId: ID_B }))

      expect(cache.applyLeaderHint('test-topic', 0, { leaderId: 2, leaderEpoch: 6 })).toBe(true)
      expect(cache.leaderEpoch('test-topic', 0)).toEqual(6)

      // The next refresh carrying the topic re-seats decoded, and routing catches up there.
      const back = response({ leader: 3, leaderEpoch: 6 })
      cache.update(back)
      expect(partition0(back).leader).toEqual(3)
      expect(cache.topics.get('test-topic').partitions.get(0).decoded).toBe(partition0(back))
    })
  })

  test('tolerates a null instrumentation emitter', () => {
    const quiet = new MetadataCache({ logger: newLogger(), instrumentationEmitter: null })
    quiet.update(response({ topicId: ID_A }))
    expect(() => quiet.update(response({ topicId: ID_B }))).not.toThrow()
  })
})
