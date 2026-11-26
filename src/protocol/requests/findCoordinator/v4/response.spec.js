const { decode, parse } = require('./response')
const Encoder = require('../../../encoder')

const COORDINATOR = { nodeId: 1, host: 'broker-1', port: 9092 }
const NO_NODE = { nodeId: -1, host: '', port: -1 }

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

describe('Protocol > Requests > FindCoordinator > v4', () => {
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
