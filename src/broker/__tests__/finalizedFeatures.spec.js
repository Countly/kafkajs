const { EventEmitter } = require('events')
const Broker = require('../index')
const ConnectionPool = require('../../network/connectionPool')
const Encoder = require('../../protocol/encoder')
const apiKeys = require('../../protocol/requests/apiKeys')
const { newLogger, readRequestHeader } = require('testHelpers')

const UNSUPPORTED_VERSION = 35
const FINALIZED_FEATURES_EPOCH_TAG = 1
const FINALIZED_FEATURES_TAG = 2

/**
 * An ApiVersions v3/v4 body advertising ApiVersions v0-v4 and, when given, the finalized features
 * epoch and the finalized features as the response's tagged fields 1 and 2.
 * @param {{ epoch?: number, features?: { name: string, minVersionLevel: number, maxVersionLevel: number }[] }} [tags]
 * @returns {Buffer}
 */
const flexibleBody = ({ epoch, features } = {}) => {
  const tags = []
  if (epoch !== undefined) {
    tags.push({ tag: FINALIZED_FEATURES_EPOCH_TAG, value: new Encoder().writeInt64(epoch) })
  }
  if (features !== undefined) {
    tags.push({
      tag: FINALIZED_FEATURES_TAG,
      value: new Encoder().writeUVarIntArray(
        features.map(({ name, minVersionLevel, maxVersionLevel }) =>
          new Encoder()
            .writeUVarIntString(name)
            .writeInt16(maxVersionLevel)
            .writeInt16(minVersionLevel)
            .writeTaggedFields()
        )
      ),
    })
  }

  return new Encoder()
    .writeInt16(0)
    .writeUVarIntArray([
      new Encoder().writeInt16(apiKeys.ApiVersions).writeInt16(0).writeInt16(4).writeTaggedFields(),
    ])
    .writeInt32(0)
    .writeTaggedFields(tags).buffer
}

/** A v0-shaped body (error code, classic array): UNSUPPORTED_VERSION's answer, and v2's. */
const classicBody = (errorCode, maxVersion) =>
  new Encoder()
    .writeInt16(errorCode)
    .writeArray(
      [new Encoder().writeInt16(apiKeys.ApiVersions).writeInt16(0).writeInt16(maxVersion)],
      'object'
    ).buffer

/**
 * A real Broker over a real ConnectionPool whose fake sockets answer every ApiVersions request at
 * once (response header v0, as for every ApiVersions version) with `answer(header)`'s body.
 * `answerWith` swaps the answer for the next negotiation.
 * @param {(header: { apiVersion: number }) => Buffer} initialAnswer
 * @returns {{ broker: Broker, pool: ConnectionPool, answerWith: (next: Function) => void }}
 */
const createBroker = (initialAnswer) => {
  let answer = initialAnswer
  const socketFactory = ({ onConnect }) => {
    const socket = new EventEmitter()
    socket.write = (buffer) => {
      const header = readRequestHeader(Buffer.from(buffer))
      if (header.apiKey === apiKeys.ApiVersions) {
        const framed = new Encoder().writeInt32(header.correlationId).writeBuffer(answer(header))
        setImmediate(() =>
          socket.emit('data', new Encoder().writeInt32(framed.size()).writeEncoder(framed).buffer)
        )
      }
      return true
    }
    socket.end = () => {}
    socket.unref = () => {}
    setImmediate(onConnect)
    return socket
  }

  const pool = new ConnectionPool({
    host: '127.0.0.1',
    port: 9092,
    logger: newLogger(),
    socketFactory,
    clientId: 'test-client',
    connectionTimeout: 1000,
    requestTimeout: 1000,
    enforceRequestTimeout: false,
  })
  const broker = new Broker({ connectionPool: pool, logger: newLogger() })

  return {
    broker,
    pool,
    answerWith: (next) => {
      answer = next
    },
  }
}

const TV1 = { name: 'transaction.version', minVersionLevel: 0, maxVersionLevel: 1 }
const TV2 = { name: 'transaction.version', minVersionLevel: 0, maxVersionLevel: 2 }
const METADATA = { name: 'metadata.version', minVersionLevel: 1, maxVersionLevel: 25 }

describe('Broker > finalized features', () => {
  const brokers = []

  /** `createBroker`, with the broker disconnected after the test. */
  const build = (answer) => {
    const created = createBroker(answer)
    brokers.push(created.broker)
    return created
  }

  afterEach(async () => {
    await Promise.all(brokers.splice(0).map((broker) => broker.disconnect()))
  })

  test('holds no features before its first negotiation', () => {
    const { broker } = build(() => flexibleBody())

    expect(broker.finalizedFeatures).toEqual({})
    expect(broker.finalizedFeaturesEpoch).toEqual(-1)
  })

  test('keeps the finalized features and their epoch from the ApiVersions response', async () => {
    const { broker } = build(() => flexibleBody({ epoch: 7, features: [METADATA, TV2] }))

    await broker.connect()

    expect(broker.finalizedFeatures).toEqual({
      'metadata.version': { minVersionLevel: 1, maxVersionLevel: 25 },
      'transaction.version': { minVersionLevel: 0, maxVersionLevel: 2 },
    })
    expect(broker.finalizedFeaturesEpoch).toBe(7)
    // The public negotiation still answers the version table alone.
    await expect(broker.apiVersions()).resolves.toEqual({
      [apiKeys.ApiVersions]: { minVersion: 0, maxVersion: 4 },
    })
  })

  test('replaces them, with the version table, when the pool generation changes', async () => {
    const { broker, pool, answerWith } = build(() => flexibleBody({ epoch: 7, features: [TV1] }))
    await broker.connect()
    const firstGeneration = broker.versionsGeneration

    answerWith(() => flexibleBody({ epoch: 9, features: [TV2] }))
    await broker.disconnect()
    await broker.connect()

    expect(broker.versionsGeneration).toBeGreaterThan(firstGeneration)
    expect(broker.versionsGeneration).toEqual(pool.generation)
    expect(broker.finalizedFeatures).toEqual({
      'transaction.version': { minVersionLevel: 0, maxVersionLevel: 2 },
    })
    expect(broker.finalizedFeaturesEpoch).toBe(9)
  })

  test('clears them when the re-negotiated response carries none', async () => {
    const { broker, answerWith } = build(() => flexibleBody({ epoch: 7, features: [TV2] }))
    await broker.connect()

    answerWith(() => flexibleBody())
    await broker.disconnect()
    await broker.connect()

    expect(broker.finalizedFeatures).toEqual({})
    expect(broker.finalizedFeaturesEpoch).toEqual(-1)
  })

  test('a broker that only answers ApiVersions v2 finalizes nothing', async () => {
    const { broker } = build(({ apiVersion }) =>
      apiVersion === 4 ? classicBody(UNSUPPORTED_VERSION, 2) : classicBody(0, 2)
    )

    await broker.connect()

    expect(broker.versions).toEqual({ [apiKeys.ApiVersions]: { minVersion: 0, maxVersion: 2 } })
    expect(broker.finalizedFeatures).toEqual({})
    expect(broker.finalizedFeaturesEpoch).toEqual(-1)
  })
})
