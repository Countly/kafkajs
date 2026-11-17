const { Kafka, logLevel } = require('../../../index')
const { KafkaJSProtocolError } = require('../../errors')
const { plainTextBrokers } = require('testHelpers')
const { execInKafka1 } = require('../../../testHelpers/leaderElection')

// Gated: it sends UnregisterController to a live cluster. `UNREGISTER_CONTROLLER_E2E=1` runs it
// against the compose cluster, which must be Kafka 4.4.0 or later.
const describeLive = process.env.UNREGISTER_CONTROLLER_E2E === '1' ? describe : describe.skip

/** An id no controller of the compose cluster has: its voters are 1, 2 and 3. */
const UNKNOWN_CONTROLLER_ID = 9999
const CASE_BUDGET_MS = 60000

/**
 * The quorum lines that name the cluster and its members, read with the broker's own
 * `kafka-metadata-quorum.sh`. Leader, epoch, high watermark and lag move on their own and are left out.
 * @returns {{ ClusterId: string, CurrentVoters: string, CurrentObservers: string }}
 * @throws {Error} when the tool fails or prints none of the three lines
 */
const quorumMembers = () => {
  const text = execInKafka1([
    '/opt/kafka/bin/kafka-metadata-quorum.sh',
    '--bootstrap-server',
    'kafka1:29092',
    'describe',
    '--status',
  ]).toString()
  const lines = Object.fromEntries(
    text
      .split('\n')
      .map((line) => line.match(/^(\w+):\s+(.*)$/))
      .filter(Boolean)
      .map(([, key, value]) => [key, value.trim()])
  )
  const members = {
    ClusterId: lines.ClusterId,
    CurrentVoters: lines.CurrentVoters,
    CurrentObservers: lines.CurrentObservers,
  }
  if (Object.values(members).some((value) => !value)) {
    throw new Error(`kafka-metadata-quorum.sh printed no members: ${text.slice(0, 2000)}`)
  }
  return members
}

/**
 * Live proof that UnregisterController (KIP-1312, Kafka 4.4) refuses an id no controller has with
 * CONTROLLER_ID_NOT_REGISTERED (136), sends the request once and changes neither the cluster the
 * brokers describe nor the controller quorum.
 *
 * @see https://github.com/apache/kafka/blob/4.4.0/metadata/src/main/java/org/apache/kafka/controller/ClusterControlManager.java
 */
describeLive('Admin > unregisterController (live)', () => {
  const kafka = new Kafka({
    clientId: 'unregister-controller-live',
    brokers: plainTextBrokers(),
    logLevel: logLevel.NOTHING,
  })
  const admin = kafka.admin()
  const sent = []

  /** @returns {Promise<object>} the cluster id and brokers, sorted by node id */
  const cluster = async () => {
    const { clusterId, brokers } = await admin.describeCluster()
    return {
      clusterId,
      brokers: brokers
        .map(({ nodeId, host, port }) => ({ nodeId, host, port }))
        .sort((a, b) => a.nodeId - b.nodeId),
    }
  }

  beforeAll(async () => {
    admin.on(admin.events.REQUEST, ({ payload }) => sent.push(payload.apiName))
    await admin.connect()
  })

  afterAll(async () => {
    await admin.disconnect()
  })

  test(
    'an unknown controller id is refused with code 136 and unregisters nothing',
    async () => {
      const clusterBefore = await cluster()
      const quorumBefore = quorumMembers()
      expect(clusterBefore.brokers).toHaveLength(3)

      const error = await admin
        .unregisterController({ controllerId: UNKNOWN_CONTROLLER_ID })
        .catch((e) => e)

      expect(error).toBeInstanceOf(KafkaJSProtocolError)
      expect(error).toMatchObject({
        type: 'CONTROLLER_ID_NOT_REGISTERED',
        code: 136,
        retriable: false,
      })
      expect(sent.filter((api) => api === 'UnregisterController')).toHaveLength(1)
      expect(await cluster()).toEqual(clusterBefore)
      expect(quorumMembers()).toEqual(quorumBefore)
    },
    CASE_BUDGET_MS
  )
})
