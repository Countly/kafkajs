const fs = require('fs')
const path = require('path')
const Connection = require('../connection')
const Encoder = require('../../protocol/encoder')
const apiKeys = require('../../protocol/requests/apiKeys')
const {
  newLogger,
  createFakeSocketFactory,
  readRequestHeader,
  respondWithFrame,
} = require('testHelpers')
const { parseMessage } = require('../../../scripts/compat/parse')

const ROOT = path.join(__dirname, '../..')
const MESSAGES = path.join(__dirname, '../../../scripts/compat/messages/4.4.0')
const THROTTLE = 50

/**
 * The first response version a client throttles on, per API, where it is above the API's oldest
 * version (Apache Kafka 4.4.0 `RequestResponseTest#testClientThrottlesResponsesWithThrottleTime`
 * and the response classes' `shouldClientThrottle` overrides, KIP-219). Older versions carry a
 * throttle the broker already served; every API not listed throttles on all its versions.
 */
const FIRST_CLIENT_THROTTLED = {
  Produce: 6,
  Fetch: 8,
  ListOffsets: 3,
  Metadata: 6,
  OffsetCommit: 4,
  OffsetFetch: 4,
  FindCoordinator: 2,
  JoinGroup: 3,
  Heartbeat: 2,
  LeaveGroup: 2,
  SyncGroup: 2,
  DescribeGroups: 2,
  ListGroups: 2,
  ApiVersions: 2,
  CreateTopics: 3,
  DeleteTopics: 2,
  DeleteRecords: 1,
  InitProducerId: 1,
  AddPartitionsToTxn: 1,
  AddOffsetsToTxn: 1,
  EndTxn: 1,
  TxnOffsetCommit: 1,
  DescribeConfigs: 2,
  AlterConfigs: 1,
  CreatePartitions: 1,
  DeleteGroups: 1,
}

/** Arrays a codec requires to hold exactly one entry, by API: the rest are written empty. */
const ONE_ENTRY = { FindCoordinator: ['Coordinators'], OffsetFetch: ['Groups'] }

/**
 * @param {string|undefined} spec a message-definition version range: `N`, `N-M` or `N+`
 * @param {number} version
 * @returns {boolean} whether `version` is in it
 */
const inRange = (spec, version) => {
  const text = String(spec ?? 'none')
  const open = /^(\d+)\+$/.exec(text)
  if (open) return version >= Number(open[1])
  const closed = /^(\d+)-(\d+)$/.exec(text)
  if (closed) return version >= Number(closed[1]) && version <= Number(closed[2])
  return /^\d+$/.test(text) && version === Number(text)
}

/**
 * Writes the smallest response body the Apache schema allows at `version`: zero numbers, empty
 * strings, bytes and arrays, no tagged fields, a null for each nullable struct, and the
 * `throttleTimeMs` the test sets. The bytes come from the schema alone, never from a fork codec.
 * @param {object} message the parsed `<Api>Response.json`
 * @param {number} version
 * @param {number} throttleTimeMs
 * @returns {Buffer}
 */
const minimalBody = (message, version, throttleTimeMs) => {
  const out = []
  const flexibleMessage = inRange(message.flexibleVersions, version)
  const structs = Object.fromEntries((message.commonStructs ?? []).map((s) => [s.name, s]))
  const bytes = (...values) => out.push(Buffer.from(values))
  const int = (size, value) => {
    const buffer = Buffer.alloc(size)
    if (size === 2) buffer.writeInt16BE(value)
    if (size === 4) buffer.writeInt32BE(value)
    out.push(buffer)
  }
  const length = (compact, count) => (compact ? bytes(count + 1) : int(4, count))

  const writeFields = (fields, flexible, oneEntry) => {
    for (const field of fields) {
      if (field.tag !== undefined || !inRange(field.versions, version)) continue
      const compact = flexible && field.flexibleVersions !== 'none'
      writeField(field, compact, flexible, oneEntry)
    }
    if (flexible) bytes(0)
  }

  const writeField = (field, compact, flexible, oneEntry) => {
    const { type } = field
    if (type.startsWith('[]')) {
      if (!oneEntry.includes(field.name)) return length(compact, 0)
      length(compact, 1)
      return writeFields(structs[type.slice(2)]?.fields ?? field.fields, flexible, [])
    }
    switch (type) {
      case 'int8':
      case 'bool':
        return bytes(0)
      case 'int16':
      case 'uint16':
        return int(2, 0)
      case 'int32':
      case 'uint32':
        return int(4, field.name === 'ThrottleTimeMs' ? throttleTimeMs : 0)
      case 'int64':
      case 'float64':
        return out.push(Buffer.alloc(8))
      case 'uuid':
        return out.push(Buffer.alloc(16))
      case 'string':
        return compact ? bytes(1) : int(2, 0)
      case 'bytes':
      case 'records':
        return compact ? bytes(1) : int(4, 0)
      default:
        if (inRange(field.nullableVersions, version)) return bytes(0xff)
        return writeFields(structs[type]?.fields ?? field.fields, flexible, [])
    }
  }

  writeFields(
    message.fields,
    flexibleMessage,
    ONE_ENTRY[message.name.replace(/Response$/, '')] ?? []
  )
  return Buffer.concat(out)
}

/** @returns {Array<{ api: string, version: number, codec: object, message: object }>} every fork response codec with an Apache schema */
const forkResponseCodecs = () => {
  const cases = []
  const dirs = fs.readdirSync(path.join(ROOT, 'protocol/requests'), { withFileTypes: true })
  for (const dir of dirs.filter((entry) => entry.isDirectory())) {
    const api = dir.name[0].toUpperCase() + dir.name.slice(1)
    const file = path.join(MESSAGES, `${api}Response.json`)
    if (!fs.existsSync(file)) continue
    const message = parseMessage(fs.readFileSync(file, 'utf8'))
    const base = path.join(ROOT, 'protocol/requests', dir.name)
    for (const entry of fs.readdirSync(base).filter((name) => /^v\d+$/.test(name))) {
      const file = path.join(base, entry, 'response.js')
      if (!fs.existsSync(file)) continue
      const exported = require(file)
      const codec = typeof exported === 'function' ? exported({ topics: [] }) : exported
      cases.push({ api, version: Number(entry.slice(1)), codec, message })
    }
  }
  return cases
}

describe('Kafka 4.4 client fix audit', () => {
  test('KAFKA-20828 per API throttling matches the verified response version boundaries', async () => {
    const cases = forkResponseCodecs()
    const wrong = []
    const unreadable = []
    let throttled = 0
    let notThrottled = 0

    for (const { api, version, codec, message } of cases) {
      const hasThrottle = message.fields.some(
        (field) => field.name === 'ThrottleTimeMs' && inRange(field.versions, version)
      )
      const expected = hasThrottle && version >= (FIRST_CLIENT_THROTTLED[api] ?? 0) ? THROTTLE : 0
      for (const sent of [THROTTLE, 0]) {
        try {
          const decoded = await codec.decode(minimalBody(message, version, sent))
          const delay = decoded.clientSideThrottleTime ?? 0
          const want = sent === 0 ? 0 : expected
          if (delay !== want)
            wrong.push(`${api} v${version} throttle ${sent}: delay ${delay}, expected ${want}`)
          if (sent === THROTTLE) expected === THROTTLE ? throttled++ : notThrottled++
        } catch (error) {
          unreadable.push(`${api} v${version}: ${error.message}`)
        }
      }
    }

    expect(unreadable).toEqual([])
    expect(wrong).toEqual([])
    expect(throttled).toBeGreaterThan(150)
    expect(notThrottled).toBeGreaterThan(30)

    jest.useFakeTimers()
    try {
      for (const api of ['DeleteTopics', 'Metadata', 'Fetch']) {
        const boundary = FIRST_CLIENT_THROTTLED[api]
        for (const [version, sent, delay] of [
          [boundary, THROTTLE, THROTTLE],
          [boundary, 0, 0],
          [boundary - 1, THROTTLE, 0],
        ]) {
          const { codec, message } = cases.find((row) => row.api === api && row.version === version)
          const fake = createFakeSocketFactory()
          const connection = new Connection({
            host: 'node',
            port: 9092,
            logger: newLogger(),
            socketFactory: fake.factory,
            clientId: 'throttle-audit',
            connectionTimeout: 1000,
            requestTimeout: 1000,
            enforceRequestTimeout: false,
          })
          try {
            const connecting = connection.connect()
            await jest.advanceTimersByTimeAsync(0)
            await connecting
            const send = () =>
              connection.send({
                request: {
                  apiKey: apiKeys[api],
                  apiName: api,
                  apiVersion: version,
                  encode: async () => new Encoder(),
                },
                response: { decode: codec.decode, parse: async (decoded) => decoded },
              })
            const first = send()
            await jest.advanceTimersByTimeAsync(0)
            expect(fake.state.writes).toHaveLength(1)
            respondWithFrame(
              fake.state,
              readRequestHeader(fake.state.writes[0]).correlationId,
              0,
              minimalBody(message, version, sent)
            )
            await first
            const startedAt = Date.now()
            const second = send()
            second.catch(() => {})
            await jest.advanceTimersByTimeAsync(0)
            if (delay > 0) {
              expect(fake.state.writes).toHaveLength(1)
              await jest.advanceTimersByTimeAsync(THROTTLE - 1)
              expect(fake.state.writes).toHaveLength(1)
              await jest.advanceTimersByTimeAsync(1)
            }
            expect(Date.now() - startedAt).toBe(delay)
            expect(fake.state.writes).toHaveLength(2)
            respondWithFrame(
              fake.state,
              readRequestHeader(fake.state.writes[1]).correlationId,
              0,
              minimalBody(message, version, 0)
            )
            await second
          } finally {
            await connection.disconnect({ drainTimeout: 0 })
            await jest.advanceTimersByTimeAsync(0)
          }
          expect(fake.state.writesAfterClose).toBe(0)
          expect(fake.state.sockets.every((socket) => socket.destroyed)).toBe(true)
          expect(jest.getTimerCount()).toBe(0)
        }
      }
    } finally {
      jest.useRealTimers()
    }
  })
})
