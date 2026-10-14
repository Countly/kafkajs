const Encoder = require('../../protocol/encoder')
const { KafkaJSServerDoesNotSupportApiKey } = require('../../errors')
const apiKeys = require('../../protocol/requests/apiKeys')
const { createGroupApiBroker } = require('./fixtures/groupApiHarness')

const TOPIC = 'test-topic'
const TOPIC_ID = '01020304-0506-0708-090a-0b0c0d0e0f10'
const TRANSACTIONAL = {
  transactionalId: 'test-transactional-id',
  producerId: '1000',
  producerEpoch: 3,
}
const WITH_TOPIC_IDS = { topicIds: new Map([[TOPIC, TOPIC_ID]]) }

/** A Produce v9+ response with no topics: every flexible version decodes it. */
const EMPTY_FLEXIBLE_RESPONSE = new Encoder()
  .writeUVarIntArray([])
  .writeInt32(0)
  .writeTaggedFields().buffer

const topicData = [
  {
    topic: TOPIC,
    partitions: [
      { partition: 0, firstSequence: 0, messages: [{ value: 'a', timestamp: 1700000000000 }] },
    ],
  },
]

describe('Broker > produce > the transaction-version gate', () => {
  const harnesses = []

  afterEach(async () => {
    await Promise.all(harnesses.splice(0).map(({ pool }) => pool.destroy()))
  })

  /**
   * Sends one produce through a real Broker over a fake socket, against a broker advertising
   * Produce v3-v13, and answers the version that went on the wire.
   * @param {object} request the arguments beside `topicData`, `acks` and `timeout`
   * @returns {Promise<number>}
   */
  const versionSentFor = async (request) => {
    const harness = createGroupApiBroker({
      versions: { [apiKeys.Produce]: { minVersion: 3, maxVersion: 13 } },
      respond: () => EMPTY_FLEXIBLE_RESPONSE,
    })
    harnesses.push(harness)

    await harness.broker.produce({ acks: -1, timeout: 30000, topicData, ...request })

    return harness.requests[0].apiVersion
  }

  test('transaction version 2 sends a transactional produce at v12 when every topic has an id', async () => {
    expect(
      await versionSentFor({ ...TRANSACTIONAL, ...WITH_TOPIC_IDS, transactionVersion: 2 })
    ).toEqual(12)
  })

  test('transaction version 2 sends it at v12 without topic ids too', async () => {
    expect(await versionSentFor({ ...TRANSACTIONAL, transactionVersion: 2 })).toEqual(12)
  })

  test('transaction version 1 keeps a transactional produce at v11, topic ids or not', async () => {
    expect(
      await versionSentFor({ ...TRANSACTIONAL, ...WITH_TOPIC_IDS, transactionVersion: 1 })
    ).toEqual(11)
    expect(await versionSentFor({ ...TRANSACTIONAL, transactionVersion: 1 })).toEqual(11)
  })

  test.each([3, '2', true])(
    'only the number 2 means transaction version 2: %p keeps a transactional produce at v11',
    async (transactionVersion) => {
      expect(
        await versionSentFor({ ...TRANSACTIONAL, ...WITH_TOPIC_IDS, transactionVersion })
      ).toEqual(11)
    }
  )

  test('a transactional produce with no transaction version never goes above v11', async () => {
    expect(await versionSentFor({ ...TRANSACTIONAL, ...WITH_TOPIC_IDS })).toEqual(11)
  })

  test('transaction version 2 refuses a broker that offers no Produce v12, naming it and its versions, and sends nothing', async () => {
    const harness = createGroupApiBroker({
      versions: { [apiKeys.Produce]: { minVersion: 3, maxVersion: 11 } },
      respond: () => EMPTY_FLEXIBLE_RESPONSE,
    })
    harnesses.push(harness)

    const error = await harness.broker
      .produce({ acks: -1, timeout: 30000, topicData, ...TRANSACTIONAL, transactionVersion: 2 })
      .catch((e) => e)

    expect(error).toBeInstanceOf(KafkaJSServerDoesNotSupportApiKey)
    expect(error).toMatchObject({ apiKey: apiKeys.Produce, apiName: 'Produce', retriable: false })
    expect(error.message).toEqual(
      'Broker 127.0.0.1:9092 offers Produce v3-v11, but a transaction under transaction version 2 needs v12'
    )
    expect(harness.requests).toHaveLength(0)
  })

  test('a caller maxVersion of 12 or more still sends a transaction under transaction version 2 at v12', async () => {
    for (const maxVersion of [12, 13, 99]) {
      expect(await versionSentFor({ ...TRANSACTIONAL, transactionVersion: 2, maxVersion })).toEqual(
        12
      )
    }
  })

  test('a lower maxVersion from the caller still wins under transaction version 1', async () => {
    expect(
      await versionSentFor({ ...TRANSACTIONAL, transactionVersion: 1, maxVersion: 10 })
    ).toEqual(10)
  })

  test.each([10, 11])(
    'transaction version 2 refuses a maxVersion of %p, naming it and the broker versions, and sends nothing',
    async (maxVersion) => {
      const harness = createGroupApiBroker({
        versions: { [apiKeys.Produce]: { minVersion: 3, maxVersion: 13 } },
        respond: () => EMPTY_FLEXIBLE_RESPONSE,
      })
      harnesses.push(harness)

      const error = await harness.broker
        .produce({
          acks: -1,
          timeout: 30000,
          topicData,
          ...TRANSACTIONAL,
          transactionVersion: 2,
          maxVersion,
        })
        .catch((e) => e)

      expect(error).toBeInstanceOf(KafkaJSServerDoesNotSupportApiKey)
      expect(error).toMatchObject({ apiKey: apiKeys.Produce, apiName: 'Produce', retriable: false })
      expect(error.message).toEqual(
        `Broker 127.0.0.1:9092 offers Produce v3-v13, but maxVersion ${maxVersion} leaves a ` +
          'transaction under transaction version 2 below the v12 it needs'
      )
      expect(harness.requests).toHaveLength(0)
    }
  )

  test('the maxVersion refusal does not touch a produce that carries no transaction', async () => {
    expect(await versionSentFor({ transactionVersion: 2, maxVersion: 10 })).toEqual(10)
  })

  test('the transaction version does not move a produce without a transactional id', async () => {
    expect(await versionSentFor({ ...WITH_TOPIC_IDS, transactionVersion: 2 })).toEqual(13)
    expect(await versionSentFor({ transactionVersion: 2 })).toEqual(11)
  })

  test('an idempotent-only produce, which carries a null transactional id, still goes at v13 with topic ids', async () => {
    expect(
      await versionSentFor({
        ...TRANSACTIONAL,
        transactionalId: null,
        ...WITH_TOPIC_IDS,
        transactionVersion: 2,
      })
    ).toEqual(13)
  })
})
