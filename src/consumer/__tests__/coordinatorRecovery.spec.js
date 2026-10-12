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
// Races closed for the assertion to mean anything, found by tracing runner.js/consumerGroup.js
// and, for the last one, by instrumenting a live run (Connection.prototype.connect patched to log
// a stack trace) because reasoning about the code alone missed it:
//   - anything that trips isRebalancing during the window makes ConsumerGroup rejoin, which also
//     calls cluster.findGroupCoordinator({ groupId }) -- a byte-identical call that would satisfy
//     the spy assertion without the offset manager ever recovering (false positive). Closed by
//     recording REBALANCING events and asserting none fired after the disconnect.
//   - a heartbeat firing between disconnect() and commitOffsets() would transparently reconnect
//     the coordinator's socket first, making getCoordinator() short-circuit and never call
//     findGroupCoordinator (false negative). ConsumerGroup's shared heartbeat is gated purely by
//     wall-clock time (`now >= this.lastRequest + interval`, `lastRequest` set once at
//     construction), not by pause() -- so a heartbeatInterval merely "longer than the test
//     typically takes" is not enough: under load, wall-clock time can approach the test's own
//     Jest timeout while the test still finishes within it. Closed by setting heartbeatInterval
//     well above this test's own timeout, so the interval is structurally unreachable -- Jest
//     would kill the test on its own budget long before it could elapse -- not merely improbable.
//   - the one that actually flaked in CI, and is invisible from reading runner.js alone: once the
//     sole partition is paused, ConsumerGroup#fetch's per-node requests array is empty, so it
//     never calls cluster.findBroker/broker.fetch for data -- but it *unconditionally* calls
//     cluster.refreshMetadataIfNecessary() first, on every ~100ms cycle, regardless of pause.
//     BrokerPool's metadataMaxAge defaults to 0 (`this.metadataMaxAge = metadataMaxAge || 0`,
//     src/cluster/brokerPool.js:33), so that refresh is not a once-per-test no-op: it happens on
//     essentially every idle cycle, from all 3 per-node fetchers (FetchManager creates one per
//     cluster.getNodeIds(), not per assigned partition). refreshMetadata() picks a broker via
//     findConnectedBroker(), whose notion of "connected" is `CONNECTED_STATUS = [CONNECTED,
//     DISCONNECTING]` (src/network/connectionStatus.js) -- so a broker mid-disconnect() still
//     counts, because its Fetch-side pooled connection (index 1) is usually still draining an
//     in-flight long-poll request via requestQueue.waitForPendingRequests(). If the *other*
//     pooled connection (index 0, used for Metadata/Heartbeat/OffsetCommit/everything but Fetch)
//     has already finished disconnecting by then, findConnectedBroker() can pick our own poisoned
//     coordinator for the metadata refresh, and ConnectionPool#getConnection transparently
//     reconnects that index-0 connection to serve it -- all while our own
//     `await cachedCoordinator.disconnect()` is still awaiting the slower Fetch-side teardown.
//     By the time our disconnect() call resolves, the coordinator is connected again, and the
//     precondition below observes true instead of false. Closed by giving the *cluster* (not the
//     consumer -- metadataMaxAge is a Cluster/BrokerPool option; a consumer-level metadataMaxAge
//     only feeds an unrelated preferred-read-replica cache in consumerGroup.js and has no effect
//     on this refresh path) a metadataMaxAge far longer than the test, so
//     refreshMetadataIfNecessary() is a genuine no-op after the initial join and this path is
//     never taken again.
describe('Consumer > coordinator recovery', () => {
  let topicName, groupId, cluster, consumer, producer

  beforeEach(async () => {
    topicName = `test-topic-${secureRandom()}`
    groupId = `consumer-group-id-${secureRandom()}`
    await createTopic({ topic: topicName })
    // metadataMaxAge is a Cluster/BrokerPool option, not a consumer one -- passing it to
    // createConsumer() below has no effect on this cluster's own metadata cache (see the file
    // header). It must be set here, on the Cluster the consumer is handed.
    cluster = createCluster({ metadataMaxAge: 3600000 })
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
      // Structurally unreachable within the test's own lifetime, not just "usually enough
      // headroom" -- see the file header for why that distinction matters for both.
      sessionTimeout: 130000,
      heartbeatInterval: 120000,
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
    // A topic created moments ago can still be propagating when the consumer joins, and the
    // resync that causes is not what this spec measures: only a rebalance from here on counts.
    rebalances.length = 0
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
