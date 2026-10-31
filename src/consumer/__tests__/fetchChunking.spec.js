const fs = require('fs')
const path = require('path')
const apiKeys = require('../../protocol/requests/apiKeys')
const { mulberry32 } = require('../../../testHelpers/fuzz')
const { TOPIC, TOPIC_ID, apiVersionsBody, createFakeBroker } = require('./fixtures/fetchHarness')

const directory = path.join(__dirname, '../../protocol/requests/fetch/fixtures')
const sessionDirectory = path.join(directory, 'java')
const retainedDirectory = path.join(__dirname, '../../protocol/fixtures/fuzz')
const SEEDS = 200
const MAX_CHUNK = 4096

/**
 * The independent expectation of every committed root vector, written from the recorded Java and
 * broker captures and the version's message schema, never from a decode.
 * @param {string} name committed fixture filename
 * @returns {{errorCode: number, sessionId: number, partition: number, partitionError: number,
 *   highWatermark: string, messages: number, endpoints: object[]|undefined}} what its decode holds
 */
const rootLiteral = (name) => {
  const version = Number(/^v(\d+)_/.exec(name)[1])
  const capture = /kafka4(12|31)/.test(name)
  const endpoints = /^v16_response_node_endpoints/.test(name)
    ? [
        { nodeId: 2, host: 'kafka2', port: 9095, rack: null },
        { nodeId: 3, host: 'kafka3', port: 9098, rack: 'eu-1' },
      ]
    : version >= 16 && !capture
      ? [{ nodeId: 2, host: '127.0.0.1', port: 9092, rack: null }]
      : version >= 16
        ? []
        : undefined
  return {
    errorCode: 0,
    sessionId: 0,
    partition: capture ? 1 : 0,
    partitionError: /node_endpoints/.test(name) ? 6 : 0,
    highWatermark: capture ? '0' : '100',
    messages: /_records\.json$/.test(name) ? 1 : 0,
    endpoints,
  }
}

/**
 * Lists every committed Fetch response the chunk corpus covers: the root vectors and the Java
 * session vectors, each with the body to send and the independent expectation of its decode.
 * @returns {Array<{name: string, version: number, body: Buffer, literal: object}>} sorted by name
 */
const loadVectors = () => {
  const root = fs
    .readdirSync(directory)
    .filter((name) => /^v(12|13|14|15|16|17|18)_response.*\.json$/.test(name))
    .map((name) => ({
      name,
      version: Number(/^v(\d+)_/.exec(name)[1]),
      body: Buffer.from(JSON.parse(fs.readFileSync(path.join(directory, name), 'utf8')).data),
      literal: rootLiteral(name),
    }))
  const sessions = fs
    .readdirSync(sessionDirectory)
    .filter((name) => /^fetchResponse-v(12|13|14|15|16|17|18)-session/.test(name))
    .map((name) => {
      const { version, fields, hex } = JSON.parse(
        fs.readFileSync(path.join(sessionDirectory, name), 'utf8')
      )
      return {
        name: `java/${name}`,
        version,
        body: Buffer.from(hex, 'hex'),
        literal: {
          errorCode: fields.errorCode,
          sessionId: fields.sessionId,
          partition: fields.partition,
          partitionError: 0,
          highWatermark: fields.highWatermark,
          messages: 0,
          topics: fields.includeTopic ? 1 : 0,
          endpoints: version >= 16 ? [] : undefined,
        },
      }
    })
  return [...root, ...sessions].sort((a, b) => a.name.localeCompare(b.name))
}

const vectors = loadVectors()

/**
 * Splits a complete frame with the inherited seeded generator, without changing any byte.
 * @param {Buffer} frame length prefix, correlation header and response body
 * @param {number} seed deterministic random stream seed
 * @returns {Buffer[]} nonempty consecutive chunks, each at most 4096 bytes and at most the frame
 */
const split = (frame, seed) => {
  const random = mulberry32(seed)
  const largest = Math.min(MAX_CHUNK, frame.length)
  const chunks = []
  for (let offset = 0; offset < frame.length;) {
    const end = Math.min(frame.length, offset + 1 + Math.floor(random() * largest))
    chunks.push(frame.subarray(offset, end))
    offset = end
  }
  return chunks
}

/**
 * Cuts the Fetch frame the way a delivery asks for.
 * @param {Buffer} frame length prefix, correlation header and response body
 * @param {number|null|number[]} how null keeps one chunk; 0 is the fixed prefix and header cut; a
 *   number is a seeded split; an array is the exact chunk lengths
 * @returns {Buffer[]} consecutive chunks that rejoin to the frame
 */
const cut = (frame, how) => {
  if (how === null) return [frame]
  if (how === 0)
    return [frame.subarray(0, 1), frame.subarray(1, 3), frame.subarray(3, 7), frame.subarray(7)]
  if (!Array.isArray(how)) return split(frame, how)
  let start = 0
  return how.map((size) => frame.subarray(start, (start += size)))
}

/**
 * Plays one committed body through Broker, Connection and its real FrameBuffer.
 * @param {{name: string, version: number, body: Buffer}} vector the response to deliver
 * @param {number|null|number[]} split null sends one whole frame; a number is a seeded split; an
 *   array is the exact chunk lengths to cut the frame into
 * @returns {Promise<{outcome: object, chunks: number[], frame: Buffer}>} the decoded response or
 *   the error's name and code, the lengths actually delivered and the delivered bytes rejoined
 * @throws {Error} on a transport failure other than a decoded answer, naming vector and split
 */
const delivery = async ({ name, version, body }, how) => {
  let lengths = []
  let delivered = Buffer.alloc(0)
  const { broker } = createFakeBroker(
    ({ apiKey }) => (apiKey === apiKeys.ApiVersions ? apiVersionsBody(version, false) : body),
    (frame, header) => {
      if (header.apiKey !== apiKeys.Fetch) return [frame]
      const chunks = cut(frame, how)
      lengths = chunks.map(({ length }) => length)
      delivered = Buffer.concat(chunks)
      return chunks
    }
  )
  try {
    await broker.connect()
    let outcome
    try {
      outcome = {
        decoded: await broker.fetch({
          maxWaitTime: 10,
          minBytes: 1,
          maxBytes: 1048576,
          isolationLevel: 0,
          topics: [
            {
              topic: TOPIC,
              partitions: [
                { partition: 0, fetchOffset: '0', maxBytes: 1048576, currentLeaderEpoch: -1 },
              ],
            },
          ],
          topicIds: version >= 13 ? new Map([[TOPIC, TOPIC_ID]]) : undefined,
          maxVersion: version,
          checkCrcs: true,
        }),
      }
    } catch (error) {
      if (!error.code) throw error
      outcome = { error: { name: error.name, code: error.code } }
    }
    return { outcome, chunks: lengths, frame: delivered }
  } catch (error) {
    error.message += ` vector=${name} split=${JSON.stringify(how)} chunks=${JSON.stringify(lengths)}`
    throw error
  } finally {
    await broker.disconnect()
  }
}

/**
 * Asserts a decoded answer against the independent expectation of its vector.
 * @param {{outcome: object}} whole the whole-frame delivery
 * @param {{literal: object}} vector the vector with its literal
 * @returns {void}
 */
const expectLiteral = ({ outcome }, { literal }) => {
  const { errorCode, sessionId, partition, highWatermark, messages, endpoints } = literal
  if (outcome.error) {
    expect(outcome.error.code).toBe(errorCode)
    expect(errorCode).not.toBe(0)
    return
  }
  const { decoded } = outcome
  expect(decoded.errorCode).toBe(errorCode)
  expect(decoded.sessionId).toBe(sessionId)
  expect(decoded.nodeEndpoints).toEqual(endpoints)
  if (literal.topics === 0) {
    expect(decoded.responses).toEqual([])
    return
  }
  expect(decoded.responses).toHaveLength(1)
  const [entry] = decoded.responses[0].partitions
  expect(entry.partition).toBe(partition)
  expect(entry.errorCode).toBe(literal.partitionError)
  expect(entry.highWatermark).toBe(highWatermark)
  expect(entry.messages).toHaveLength(messages)
}

describe('response stream chunking', () => {
  it('has independent Java and captured fixtures for every supported flexible version', () => {
    const names = vectors.map(({ name }) => name)
    for (let version = 12; version <= 18; version++) {
      expect(names).toContain(`v${version}_response_java.json`)
      for (const kind of ['Full', 'Sparse', 'Error70', 'Error71', 'Error106'])
        expect(names).toContain(`java/fetchResponse-v${version}-session${kind}.json`)
    }
    expect(names.some((name) => !name.endsWith('_java.json') && !name.startsWith('java/'))).toBe(
      true
    )
    expect(vectors).toHaveLength(49)
  })

  it('keeps a fixed fragmented delivery equal through the real broker path', async () => {
    const vector = vectors.find(({ name }) => name === 'v18_response_java.json')
    const whole = await delivery(vector, null)
    const fragmented = await delivery(vector, 0)
    expect(fragmented.outcome).toEqual(whole.outcome)
    expect(fragmented.chunks.slice(0, 3)).toEqual([1, 2, 4])
    expect(fragmented.frame).toEqual(whole.frame)
    expect(whole.outcome.decoded.responses).toHaveLength(1)
    expect(whole.outcome.decoded.responses[0].partitions[0].partition).toBe(0)
  })

  it('delivers a frame one byte at a time through the real broker path', async () => {
    const vector = vectors.find(({ name }) => name === 'java/fetchResponse-v13-sessionFull.json')
    const whole = await delivery(vector, null)
    const sizes = Array.from({ length: whole.frame.length }, () => 1)
    const bytewise = await delivery(vector, sizes)
    expect(bytewise.chunks).toEqual(sizes)
    expect(bytewise.outcome).toEqual(whole.outcome)
  })

  it('replays every retained failing split before any randomized split', async () => {
    const retained = fs.existsSync(retainedDirectory)
      ? fs.readdirSync(retainedDirectory).filter((name) => /^fetch-\d+-chunk-\d+\.json$/.test(name))
      : []
    for (const file of retained) {
      const { seed, chunks, hex, version } = JSON.parse(
        fs.readFileSync(path.join(retainedDirectory, file), 'utf8')
      )
      const vector = { name: file, version, body: Buffer.from(hex, 'hex') }
      const whole = await delivery(vector, null)
      expect((await delivery(vector, chunks)).outcome).toEqual(whole.outcome)
      expect(seed).toBeGreaterThan(0)
    }
  })

  it.each(vectors.map(({ name }) => [name]))(
    'preserves the full decoded response for %s across 200 splits',
    async (name) => {
      const vector = vectors.find((candidate) => candidate.name === name)
      const whole = await delivery(vector, null)
      expectLiteral(whole, vector)
      let comparisons = 0
      let fragmented = 0
      for (let seed = 1; seed <= SEEDS; seed++) {
        let actual
        try {
          actual = await delivery(vector, seed)
          expect(actual.outcome).toEqual(whole.outcome)
          expect(actual.frame).toEqual(whole.frame)
          expect(actual.chunks.every((size) => size >= 1 && size <= MAX_CHUNK)).toBe(true)
          comparisons++
          if (actual.chunks.length > 1) fragmented++
        } catch (error) {
          error.message += ` vector=${name} seed=${seed} chunks=${JSON.stringify(actual?.chunks)}`
          throw error
        }
      }
      expect(comparisons).toBe(200)
      expect(fragmented).toBeGreaterThan(SEEDS / 2)
    },
    5000
  )
})
