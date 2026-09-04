const Connection = require('../connection')
const Encoder = require('../../protocol/encoder')
const Decoder = require('../../protocol/decoder')
const apiKeys = require('../../protocol/requests/apiKeys')
const { newLogger, createFakeSocketFactory, respondWithFrame } = require('testHelpers')

const respond = respondWithFrame

const connectionWith = (factory) =>
  new Connection({
    host: '127.0.0.1',
    port: 9092,
    logger: newLogger(),
    socketFactory: factory,
    clientId: 'test-client',
    connectionTimeout: 1000,
    requestTimeout: 1000,
    enforceRequestTimeout: false,
  })

describe('Network > Connection > flexible framing', () => {
  let connection
  let fake

  beforeEach(async () => {
    fake = createFakeSocketFactory()
    connection = connectionWith(fake.factory)
    await connection.connect()
  })

  afterEach(async () => {
    await connection.disconnect()
  })

  test('writes request header v1 for a non-flexible version', async () => {
    const request = {
      apiKey: apiKeys.Metadata,
      apiVersion: 6,
      apiName: 'Metadata',
      encode: async () => new Encoder().writeInt8(0x2a),
    }
    const response = { decode: async (raw) => raw, parse: async (data) => data }

    const promise = connection.send({ request, response })
    await new Promise(setImmediate)

    const written = new Decoder(fake.state.writes[0])
    expect(written.readInt32()).toEqual(fake.state.writes[0].length - 4)
    expect(written.readInt16()).toEqual(apiKeys.Metadata)
    expect(written.readInt16()).toEqual(6)
    const correlationId = written.readInt32()
    expect(written.readString()).toEqual('test-client')
    // header v1: the body starts immediately after client_id, no tag buffer
    expect(written.readInt8()).toEqual(0x2a)

    respond(fake.state, correlationId, 0, Buffer.from([0x01]))
    await expect(promise).resolves.toEqual(Buffer.from([0x01]))
  })

  test('writes request header v2 for a flexible version, with a classic client_id', async () => {
    const request = {
      apiKey: apiKeys.Metadata,
      apiVersion: 12,
      apiName: 'Metadata',
      encode: async () => new Encoder().writeInt8(0x2a),
    }
    const response = { decode: async (raw) => raw, parse: async (data) => data }

    const promise = connection.send({ request, response })
    await new Promise(setImmediate)

    const written = new Decoder(fake.state.writes[0])
    written.readInt32() // size
    expect(written.readInt16()).toEqual(apiKeys.Metadata)
    expect(written.readInt16()).toEqual(12)
    const correlationId = written.readInt32()
    // client_id is still the classic INT16-prefixed NULLABLE_STRING in header v2
    expect(written.readString()).toEqual('test-client')
    // ...followed by the header's (always empty) tag buffer
    expect(written.readInt8()).toEqual(0x00)
    expect(written.readInt8()).toEqual(0x2a)

    respond(fake.state, correlationId, 1, Buffer.from([0x01]))
    await expect(promise).resolves.toEqual(Buffer.from([0x01]))
  })

  test('strips the response header tag buffer before the codec sees the body', async () => {
    const seen = []
    const request = {
      apiKey: apiKeys.Metadata,
      apiVersion: 12,
      apiName: 'Metadata',
      encode: async () => new Encoder(),
    }
    const response = {
      decode: async (raw) => {
        seen.push(Buffer.from(raw))
        return raw
      },
      parse: async (data) => data,
    }

    const promise = connection.send({ request, response })
    await new Promise(setImmediate)
    const correlationId = new Decoder(fake.state.writes[0].slice(8)).readInt32()

    respond(fake.state, correlationId, 1, Buffer.from([0xaa, 0xbb]))
    await promise

    expect([...seen[0]]).toEqual([0xaa, 0xbb])
  })

  test('skips a non-empty response header tag buffer too', async () => {
    const seen = []
    const request = {
      apiKey: apiKeys.Metadata,
      apiVersion: 12,
      apiName: 'Metadata',
      encode: async () => new Encoder(),
    }
    const response = {
      decode: async (raw) => {
        seen.push(Buffer.from(raw))
        return raw
      },
      parse: async (data) => data,
    }

    const promise = connection.send({ request, response })
    await new Promise(setImmediate)
    const correlationId = new Decoder(fake.state.writes[0].slice(8)).readInt32()

    // One unknown tagged field in the header: the fast path cannot take this, so the general
    // Decoder path runs. No broker sends this today; the branch exists so that one day it can.
    respond(fake.state, correlationId, 1, Buffer.from([0xaa, 0xbb]), [
      { tag: 0, value: Buffer.from([0x01]) },
    ])
    await promise

    expect([...seen[0]]).toEqual([0xaa, 0xbb])
  })

  test('does not strip anything from an ApiVersions v4 response (header stays v0)', async () => {
    const seen = []
    const request = {
      apiKey: apiKeys.ApiVersions,
      apiVersion: 4,
      apiName: 'ApiVersions',
      encode: async () => new Encoder(),
    }
    const response = {
      decode: async (raw) => {
        seen.push(Buffer.from(raw))
        return raw
      },
      parse: async (data) => data,
    }

    const promise = connection.send({ request, response })
    await new Promise(setImmediate)
    const correlationId = new Decoder(fake.state.writes[0].slice(8)).readInt32()

    // v0 response header: correlationId only, no tag buffer
    respond(fake.state, correlationId, 0, Buffer.from([0xaa, 0xbb]))
    await promise

    expect([...seen[0]]).toEqual([0xaa, 0xbb])
  })

  test('a flexible codec sees its body without the response header tag buffer', async () => {
    const apiKeysMap = require('../../protocol/requests/apiKeys')
    const listReassignments = require('../../protocol/requests/listPartitionReassignments/v0/request')
    const response = require('../../protocol/requests/listPartitionReassignments/v0/response')

    const request = listReassignments({ topics: null, timeout: 5000 })
    expect(request.apiKey).toEqual(apiKeysMap.ListPartitionReassignments)

    const promise = connection.send({ request, response })
    await new Promise(setImmediate)

    // header v2 on the way out: api_key, api_version, correlation_id, client_id, TAG_BUFFER
    const written = new Decoder(fake.state.writes[0])
    written.readInt32()
    written.readInt16()
    written.readInt16()
    const correlationId = written.readInt32()
    written.readString()
    expect(written.readInt8()).toEqual(0x00)
    expect(written.readInt32()).toEqual(5000) // the body starts at timeout_ms, not at a tag buffer

    const body = new Encoder()
      .writeInt32(0)
      .writeInt16(0)
      .writeUVarIntString(null)
      .writeUVarIntArray([])
      .writeTaggedFields().buffer

    respond(fake.state, correlationId, 1, body)
    await expect(promise).resolves.toEqual({ throttleTime: 0, errorCode: 0, topics: [] })
  })
})
