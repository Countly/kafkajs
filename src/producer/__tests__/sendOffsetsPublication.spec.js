const { Kafka } = require('../../../index')
const Cluster = require('../../cluster')
const { createScriptedCluster, producerScript } = require('../../../testHelpers/scriptedCluster')
const { WireOrder, WireOrderViolation } = require('../../../testHelpers/wireOrder')
const { TOPIC } = require('../../consumer/__tests__/fixtures/fetchHarness')

const NODES = [{ id: 0, host: 'node0', port: 9092 }]
const GROUP = 'publication-group'

/**
 * Runs one transaction that sends a record and the offsets of a consumer group, and commits it, on
 * a scripted cluster of the given transaction profile. Every write of the shared committed-offset
 * cache is marked on the cluster's timeline as `cache write`.
 * @param {'tv1'|'tv2'} profile
 * @returns {Promise<{ cluster: object, writes: object[] }>} the cluster, and the arguments of each
 *   cache write
 */
const commitOneTransaction = async (profile) => {
  const cluster = producerScript(createScriptedCluster({ nodes: NODES, profile }))
  const writes = []
  const write = Cluster.prototype.markOffsetAsCommitted
  jest.spyOn(Cluster.prototype, 'markOffsetAsCommitted').mockImplementation(function (offsets) {
    cluster.mark('cache write')
    writes.push(offsets)
    return write.call(this, offsets)
  })

  const producer = new Kafka(cluster.kafkaOptions()).producer({
    transactionalId: `publication-${profile}`,
    maxInFlightRequests: 1,
  })
  try {
    await producer.connect()
    const transaction = await producer.transaction()
    await transaction.send({ topic: TOPIC, messages: [{ value: 'a' }] })
    await transaction.sendOffsets({
      consumerGroupId: GROUP,
      topics: [{ topic: TOPIC, partitions: [{ partition: 0, offset: '5' }] }],
    })
    await transaction.commit()
    cluster.assertClean()
  } finally {
    await producer.disconnect()
  }

  return { cluster, writes }
}

describe.each(['tv1', 'tv2'])('Producer > transactional offsets on the wire (%s)', (profile) => {
  let run
  /** What `assertOrder` found: null when the publication order holds, else its violation. */
  let publicationOrder

  beforeEach(async () => {
    run = await commitOneTransaction(profile)
    try {
      WireOrder.of(run.cluster).assertOrder([
        'TxnOffsetCommit response',
        'EndTxn request',
        'EndTxn response',
        'cache write',
      ])
      publicationOrder = null
    } catch (error) {
      if (!(error instanceof WireOrderViolation)) {
        throw error
      }
      publicationOrder = error
    }
  })

  afterEach(() => jest.restoreAllMocks())

  it('commits the transaction and marks the offsets of the call committed once', () => {
    expect(run.writes).toEqual([{ groupId: GROUP, topic: TOPIC, partition: 0, offset: '5' }])
    expect(run.cluster.requests().map(({ api }) => api)).toContain('EndTxn')
    run.cluster.assertClean()
  })

  /**
   * The cache is the shared per-instance record of what a group has committed, so it may be
   * written only once the commit that carries the offsets is confirmed. Today the producer writes
   * it as soon as the TxnOffsetCommit is answered, before the EndTxn is even sent, so an abort or
   * an unknown outcome has already published them. The run happens in `beforeEach`, so only the
   * ordering check can make this case fail as expected; the mark stays until that is fixed.
   */
  it.failing('writes the cache only after the EndTxn response', () => {
    expect(publicationOrder).toBeNull()
  })
})
