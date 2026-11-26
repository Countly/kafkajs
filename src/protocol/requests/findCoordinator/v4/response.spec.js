const { decode, parse } = require('./response')
const { protocol } = require('../index')
const Encoder = require('../../../encoder')
const Decoder = require('../../../decoder')
const COORDINATOR_TYPES = require('../../../coordinatorTypes')

const COORDINATOR = { nodeId: 1, host: 'broker-1', port: 9092 }
const NO_NODE = { nodeId: -1, host: '', port: -1 }

// A lone surrogate is written as U+FFFD, so a broker echoes that and not the string that was asked.
const KEYS = [
  ['group id', 'team-\uD800', 'team-�', COORDINATOR_TYPES.GROUP],
  ['transactional id', 'txn-\uDC00-1', 'txn-�-1', COORDINATOR_TYPES.TRANSACTION],
  ['group id of ordinary Unicode', 'grüppe-日本', 'grüppe-日本', 0],
]
const VERSIONS = [4, 6]
const LOOKUPS = VERSIONS.flatMap((version) => KEYS.map((row) => [version, ...row]))

/**
 * @param {{ key: string|null, nodeId: number, host: string, port: number, errorCode?: number }} entry
 * @returns {Buffer} the bytes of a v4 answer with that one coordinator entry
 */
const answerFor = ({ key, nodeId, host, port, errorCode = 0 }) =>
  new Encoder()
    .writeInt32(0)
    .writeUVarIntArray([
      new Encoder()
        .writeUVarIntString(key)
        .writeInt32(nodeId)
        .writeUVarIntString(host)
        .writeInt32(port)
        .writeInt16(errorCode)
        .writeUVarIntString(null)
        .writeTaggedFields(),
    ])
    .writeTaggedFields().buffer

/**
 * Plays the broker: reads the key type and the one key out of the bytes the request encodes, and
 * answers with the key echoed as read.
 * @param {{ encode: () => Promise<Encoder> }} request
 * @returns {Promise<{ keyType: number, sentKey: string, answer: (fields: object) => Buffer }>}
 */
const brokerReading = async (request) => {
  const decoder = new Decoder((await request.encode()).buffer)
  const keyType = decoder.readInt8()
  const [sentKey] = decoder.readUVarIntArray((entry) => entry.readUVarIntString())

  return { keyType, sentKey, answer: (fields) => answerFor({ key: sentKey, ...fields }) }
}

describe('Protocol > Requests > FindCoordinator > v4', () => {
  test.each(LOOKUPS)(
    'at v%i a %s echoed as the bytes the request carried is accepted, an error answer included',
    async (version, _, key, wireKey, coordinatorType) => {
      const { request, response } = protocol({ version })({ groupId: key, coordinatorType })
      const broker = await brokerReading(request)

      expect(broker.keyType).toBe(coordinatorType)
      expect(broker.sentKey).toBe(wireKey)

      await expect(response.decode(broker.answer(COORDINATOR))).resolves.toEqual({
        throttleTime: 0,
        clientSideThrottleTime: 0,
        errorCode: 0,
        errorMessage: null,
        coordinator: COORDINATOR,
      })

      const refused = await response.decode(broker.answer({ ...NO_NODE, errorCode: 15 }))
      expect(refused.errorCode).toBe(15)
      await expect(response.parse(refused)).rejects.toMatchObject({
        type: 'GROUP_COORDINATOR_NOT_AVAILABLE',
      })
    }
  )

  test.each(VERSIONS.map((version) => [version]))(
    'at v%i a key with a lone surrogate still refuses an answer for another key or a null key',
    async (version) => {
      const asked = protocol({ version })({
        groupId: 'team-\uD800',
        coordinatorType: COORDINATOR_TYPES.GROUP,
      })
      const wellFormed = protocol({ version })({
        groupId: 'team-A',
        coordinatorType: COORDINATOR_TYPES.GROUP,
      })
      const refusals = [
        [asked, 'other-�'],
        [asked, 'team-'],
        [asked, 'team-A'],
        [asked, null],
        [wellFormed, 'team-�'],
      ]

      for (const [{ response }, key] of refusals) {
        await expect(response.decode(answerFor({ key, ...COORDINATOR }))).rejects.toMatchObject({
          name: 'KafkaJSError',
          retriable: true,
          message: expect.stringMatching(/^FindCoordinator response is for key /),
        })
      }
    }
  )

  test('flattens the single-entry Coordinators array into the shape every caller reads', async () => {
    const data = await decode(Buffer.from(require('../fixtures/v4_response.json')))

    expect(data).toEqual({
      throttleTime: 0,
      clientSideThrottleTime: 0,
      errorCode: 0,
      errorMessage: null,
      coordinator: { nodeId: 1, host: '127.0.0.1', port: 9092 },
    })

    await expect(parse(data)).resolves.toBeTruthy()
  })

  test('a coordinator-level error is thrown by parse', async () => {
    const data = await decode(Buffer.from(require('../fixtures/v4_response_error.json')))
    expect(data.errorCode).toEqual(15)
    await expect(parse(data)).rejects.toMatchObject({ code: 15 })
  })

  test('a response with anything other than exactly one coordinator is rejected', async () => {
    const empty = new Encoder().writeInt32(0).writeUVarIntArray([]).writeTaggedFields().buffer
    await expect(decode(empty)).rejects.toThrow(/exactly one coordinator/)
  })

  test('decode with an expected key refuses an answer for another key or a null key', async () => {
    for (const key of ['group-B', null]) {
      await expect(
        decode(answerFor({ key, ...COORDINATOR }), { key: 'group-A' })
      ).rejects.toMatchObject({
        name: 'KafkaJSError',
        retriable: true,
        message: expect.stringMatching(/^FindCoordinator response is for key .*group-A/),
      })
    }
  })

  test('decode with an expected key refuses an error answer for another key too', async () => {
    const answer = answerFor({ key: 'group-B', ...NO_NODE, errorCode: 15 })

    await expect(decode(answer, { key: 'group-A' })).rejects.toMatchObject({
      name: 'KafkaJSError',
      retriable: true,
    })
  })

  test('decode with an expected key passes an answer that echoes it, an error answer with no node included', async () => {
    const asked = { key: 'group-A' }

    await expect(decode(answerFor({ key: 'group-A', ...COORDINATOR }), asked)).resolves.toEqual({
      throttleTime: 0,
      clientSideThrottleTime: 0,
      errorCode: 0,
      errorMessage: null,
      coordinator: COORDINATOR,
    })

    const refused = await decode(answerFor({ key: 'group-A', ...NO_NODE, errorCode: 15 }), asked)
    expect(refused.errorCode).toBe(15)
    await expect(parse(refused)).rejects.toMatchObject({ type: 'GROUP_COORDINATOR_NOT_AVAILABLE' })
  })

  test('decode without an expected key does not look at the key', async () => {
    const data = await decode(answerFor({ key: 'any-key', ...COORDINATOR }))

    expect(data.coordinator).toEqual(COORDINATOR)
  })

  test('TRANSACTION_ABORTABLE surfaces unchanged', async () => {
    const data = await decode(Buffer.from(require('../fixtures/v4_response.json')))
    data.errorCode = 120
    await expect(parse(data)).rejects.toMatchObject({
      type: 'TRANSACTION_ABORTABLE',
      code: 120,
      retriable: false,
    })
  })

  test('decodes the vector the Apache kafka-clients 4.3.1 codec produced', async () => {
    const data = await decode(Buffer.from(require('../fixtures/v4_response_java.json')))

    expect(data).toEqual({
      throttleTime: 0,
      clientSideThrottleTime: 0,
      errorCode: 0,
      errorMessage: null,
      coordinator: { nodeId: 1, host: '127.0.0.1', port: 9092 },
    })

    await expect(parse(data)).resolves.toBeTruthy()
  })
})
