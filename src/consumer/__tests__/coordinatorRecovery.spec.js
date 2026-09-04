const createConsumer = require('../index')
const createProducer = require('../../producer')
const {
  secureRandom,
  createCluster,
  createTopic,
  newLogger,
  waitForConsumerToJoinGroup,
} = require('testHelpers')

// After a coordinator move, the cached broker used to be dead forever: getCoordinator() resolved
// it with findBroker() (same stale node) and the commit catch only refreshed metadata.
//
// A plain "disconnect the socket" cannot reproduce a real broker failover against a live,
// unmoved cluster: cluster.findBroker() and cluster.findGroupCoordinator() both resolve to the
// same cached Broker instance for a nodeId that never actually changed, and the connection pool
// transparently reconnects it either way, so the commit succeeds either way too. What the fix
// actually changes is *which cluster method* the offset manager calls to recover -- findBroker()
// (old) vs. findGroupCoordinator() (new, upstream PR #1779) -- so that is what this spec asserts,
// via spies that still delegate to the real implementation. The genuine failover case (the
// coordinator moving to a different node) is covered separately by a manual broker-restart run.
//
// Two races have to be closed for the assertion to mean anything:
//   - a heartbeat on the same Broker object between disconnect() and commitOffsets() would
//     transparently reconnect the socket first (ConnectionPool#send does that regardless of
//     Broker#isConnected()), making getCoordinator() short-circuit and never call
//     findGroupCoordinator -- a false negative that would flake instead of failing outright.
//   - anything that trips isRebalancing during that window makes ConsumerGroup rejoin, which also
//     calls cluster.findGroupCoordinator({ groupId }) -- a byte-identical call that would satisfy
//     the spy assertion without the offset manager ever recovering -- a false positive.
// Fixed by giving the consumer a heartbeat interval far longer than the test, pausing the fetch
// loop before disconnecting, and asserting no rebalance happened in the window.
describe('Consumer > coordinator recovery', () => {
  let topicName, groupId, cluster, consumer, producer

  beforeEach(async () => {
    topicName = `test-topic-${secureRandom()}`
    groupId = `consumer-group-id-${secureRandom()}`
    await createTopic({ topic: topicName })
    cluster = createCluster()
  })

  afterEach(async () => {
    consumer && (await consumer.disconnect())
    producer && (await producer.disconnect())
  })

  it('re-discovers the group coordinator when the cached one is disconnected', async () => {
    consumer = createConsumer({
      cluster,
      groupId,
      logger: newLogger(),
      maxWaitTimeInMs: 100,
      autoCommit: false,
      // No heartbeat should fire between disconnect() and commitOffsets() below.
      sessionTimeout: 60000,
      heartbeatInterval: 55000,
    })

    const rebalances = []
    consumer.on(consumer.events.REBALANCING, (e) => rebalances.push(e))

    await consumer.connect()
    await consumer.subscribe({ topic: topicName, fromBeginning: true })

    const received = []
    // Do not await run(): it only resolves after joinAndSync() emits GROUP_JOIN, so awaiting it
    // first would attach the waitForConsumerToJoinGroup listener after the event already fired.
    consumer.run({ eachMessage: async ({ message }) => received.push(message) })
    await waitForConsumerToJoinGroup(consumer)

    producer = createProducer({ cluster: createCluster(), logger: newLogger() })
    await producer.connect()
    await producer.send({ topic: topicName, messages: [{ key: 'k', value: 'v' }] })

    await new Promise((resolve) => setTimeout(resolve, 1000))
    expect(received.length).toBeGreaterThan(0)

    // Quiet the fetch loop so the only remaining network activity is the commit itself.
    consumer.pause([{ topic: topicName }])

    // Simulate the coordinator going away: disconnect the cached broker (via the same cluster
    // instance the consumer uses, so it is the same broker-pool object the consumer group and
    // offset manager hold) so isConnected() is false on the next commit.
    const cachedCoordinator = await cluster.findGroupCoordinator({ groupId })
    const findGroupCoordinatorSpy = jest.spyOn(cluster, 'findGroupCoordinator')
    const findBrokerSpy = jest.spyOn(cluster, 'findBroker')
    await cachedCoordinator.disconnect()
    expect(cachedCoordinator.isConnected()).toBe(false)

    // A commit must now succeed...
    await expect(
      consumer.commitOffsets([{ topic: topicName, partition: 0, offset: '1' }])
    ).resolves.not.toThrow()
    expect(cachedCoordinator.isConnected()).toBe(true)

    // ...and it must have recovered by re-running FindCoordinator, not by a rejoin (no rebalance
    // happened) and not by silently reconnecting the same cached broker reference via
    // findBroker(staleCoordinator), which the old code called directly.
    expect(rebalances).toHaveLength(0)
    expect(findGroupCoordinatorSpy).toHaveBeenCalledWith({ groupId })
    expect(findBrokerSpy).not.toHaveBeenCalledWith(cachedCoordinator)
  }, 60000)
})
