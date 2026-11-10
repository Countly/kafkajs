const { Kafka } = require('../../../index')
const { createScriptedCluster } = require('../../../testHelpers/scriptedCluster')
const { Schedule } = require('../../../testHelpers/schedule')

const TOPIC = 'leader-topic'
const TOPIC_ID = '00112233-4455-6677-8899-aabbccddeeff'
const NOT_LEADER_OR_FOLLOWER = 6
const RETRY = { retries: 3, initialRetryTime: 10, maxRetryTime: 10, factor: 0 }

/**
 * Advances fake time in 10 ms steps until `promise` settles.
 * @template T
 * @param {Promise<T>} promise work that needs timers to run
 * @param {number} [limit] fake milliseconds to wait at most
 * @returns {Promise<T>} its outcome
 * @throws {Error} when it has not settled by `limit`
 */
const settle = async (promise, limit = 5000) => {
  const state = { done: false }
  const tracked = promise.finally(() => (state.done = true))
  tracked.catch(() => {})
  for (let elapsed = 0; !state.done && elapsed < limit; elapsed += 10) {
    await jest.advanceTimersByTimeAsync(10)
  }
  if (!state.done) throw new Error(`Did not settle within ${limit} ms of fake time`)
  return tracked
}

describe('Kafka 4.4 client fix audit', () => {
  let admin

  beforeEach(() => jest.useFakeTimers())
  afterEach(async () => {
    if (admin) await settle(admin.disconnect())
    expect(jest.getTimerCount()).toBe(0)
    jest.useRealTimers()
  })

  test('KAFKA-20673 removed cached leader cannot strand an admin call', async () => {
    const schedule = new Schedule()
    const cluster = createScriptedCluster({
      nodes: [
        { id: 0, host: 'node0', port: 9092 },
        { id: 1, host: 'node1', port: 9092 },
      ],
      schedule,
    })
    let brokers = [0, 1]
    let leader = 0
    cluster.answer('Metadata', () => ({
      brokers: brokers.map((id) => ({ nodeId: id, host: `node${id}`, port: 9092, rack: null })),
      controllerId: brokers[0],
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
              leaderEpoch: 5 + leader,
              replicas: [leader],
              isr: [leader],
              offlineReplicas: [],
            },
          ],
        },
      ],
    }))
    cluster.answer('ListOffsets', ({ topics }) => ({
      topics: topics.map(({ name, partitions }) => ({
        name,
        partitions: partitions.map(({ partitionIndex, timestamp }) => ({
          partitionIndex,
          errorCode: 0,
          timestamp,
          offset: timestamp === '-2' ? '3' : '42',
          leaderEpoch: 6,
        })),
      })),
    }))
    const held = schedule.hold({ api: 'ListOffsets', node: 0 })
    admin = new Kafka(cluster.kafkaOptions({ retry: RETRY })).admin()
    await settle(admin.connect())

    const results = []
    const offsets = admin.fetchTopicOffsets(TOPIC).then((value) => results.push(value))
    await settle(held.arrived())

    // The cached leader leaves the cluster while the request to it is outstanding.
    brokers = [1]
    leader = 1
    held.release({
      topics: [
        {
          name: TOPIC,
          partitions: [
            {
              partitionIndex: 0,
              errorCode: NOT_LEADER_OR_FOLLOWER,
              timestamp: '-1',
              offset: '-1',
              leaderEpoch: -1,
            },
          ],
        },
      ],
    })
    await settle(offsets)

    expect(results).toEqual([[{ partition: 0, offset: '42', high: '42', low: '3' }]])
    expect(
      cluster
        .requests()
        .filter(({ api }) => api === 'ListOffsets')
        .map(({ node }) => node)
    ).toEqual([0, 1, 1])
  })
})
