const path = require('path')
const Connection = require('../../network/connection')
const Encoder = require('../encoder')
const apiKeys = require('../requests/apiKeys')
const {
  KafkaJSConnectionClosedError,
  KafkaJSPartialMessageError,
  KafkaJSProtocolError,
} = require('../../errors')
const {
  caseLabel,
  loadCorpus,
  mutations,
  runCases,
  truncations,
} = require('../../../testHelpers/fuzz')
const { newLogger, createFakeSocketFactory, readRequestHeader } = require('testHelpers')
const { keyOfMessage, messageOfDirectory } = require('../../../scripts/protocol/apiNames')
const {
  loadVectors,
  targetOf,
  codecOf,
  optionsOf,
  project,
  differences,
  frameOf,
} = require('./fixtures/fuzzResponseVectors')
const handler = require.resolve('./fixtures/fuzzResponseHandler')
const runHandler = require('./fixtures/fuzzResponseHandler')

/** The seed every stream of cases here comes from: a failure prints it with the case index. */
const SEED = 20261017

/** The longest the cases of one API may take to run: a hung decoder fails the test, not the suite. */
const API_TIMEOUT_MS = 300000

const { vectors, missing, unused, excluded } = loadVectors()
const apis = [...new Set(vectors.map(({ directory }) => directory))]

/** How many response vectors each API has: adding one means writing its control. */
const COUNTS = {
  addOffsetsToTxn: 7,
  addPartitionsToTxn: 5,
  alterConfigs: 3,
  alterPartitionReassignments: 4,
  alterReplicaLogDirs: 2,
  apiVersions: 9,
  consumerGroupDescribe: 4,
  consumerGroupHeartbeat: 8,
  createAcls: 5,
  createPartitions: 6,
  createTopics: 12,
  deleteAcls: 4,
  deleteGroups: 2,
  deleteRecords: 3,
  deleteTopics: 16,
  describeAcls: 4,
  describeConfigs: 3,
  describeGroups: 7,
  describeLogDirs: 5,
  describeTopicPartitions: 6,
  endTxn: 10,
  findCoordinator: 12,
  heartbeat: 6,
  incrementalAlterConfigs: 4,
  initProducerId: 8,
  joinGroup: 11,
  leaveGroup: 8,
  listGroups: 3,
  listOffsets: 14,
  listPartitionReassignments: 2,
  metadata: 19,
  offsetCommit: 14,
  offsetDelete: 1,
  offsetFetch: 26,
  offsetForLeaderEpoch: 5,
  produce: 25,
  saslAuthenticate: 6,
  saslHandshake: 1,
  syncGroup: 8,
  txnOffsetCommit: 9,
}

const settle = () => new Promise((done) => setImmediate(() => setImmediate(done)))

/**
 * The paired control of a vector's mutated runs: its unmutated body decodes to its control, or
 * fails with the typed error its control names.
 * @param {import('./fixtures/fuzzResponseVectors').ResponseVector} vector
 * @returns {Promise<string|null>} what is wrong, or null
 */
const controlProblem = async (vector) => {
  try {
    const decoded = await codecOf(vector).decode(vector.body, optionsOf(vector))
    if (vector.control.error) return `decoded, expected ${vector.control.error}`
    const found = differences(vector.control, project(vector, decoded))
    return found && `${vector.name}${found}`
  } catch (error) {
    return error.constructor.name === vector.control.error
      ? null
      : `${vector.name}: ${error.constructor.name}: ${error.message}`
  }
}

/**
 * A connected `Connection` over a fake socket, and a way to send it a request of the vector's API
 * and version, so the correlation id of the frame sent back is matched and the vector's decoder is
 * what runs on it.
 * @param {import('./fixtures/fuzzResponseVectors').ResponseVector} vector
 * @returns {Promise<{ state: object, counters: { decoded: number }, send: Function, connection: Connection }>}
 *   `send()` resolves with the correlation id it was written under and its outcome
 */
const connected = async (vector) => {
  const { factory, state } = createFakeSocketFactory()
  const connection = new Connection({
    host: '127.0.0.1',
    port: 9092,
    logger: newLogger(),
    socketFactory: factory,
    clientId: 'fuzz',
    connectionTimeout: 1000,
    requestTimeout: 1000,
    enforceRequestTimeout: false,
  })
  await connection.connect()

  const counters = { decoded: 0 }
  const codec = codecOf(vector)
  const key = keyOfMessage(messageOfDirectory(vector.directory))
  const send = async () => {
    const outcome = connection
      .send({
        request: {
          apiKey: apiKeys[key],
          apiName: key,
          apiVersion: vector.version,
          encode: async () => new Encoder(),
        },
        response: {
          decode: (raw) => {
            counters.decoded++
            return codec.decode(raw, optionsOf(vector))
          },
          parse: async (decoded) => decoded,
        },
      })
      .then(
        (value) => ({ value }),
        (error) => ({ error })
      )
    await settle()
    const { correlationId } = readRequestHeader(state.writes[state.writes.length - 1])
    return { correlationId, outcome }
  }

  return { state, counters, send, connection }
}

/**
 * Sends the vector's frame a byte at a time, then once more.
 * @param {import('./fixtures/fuzzResponseVectors').ResponseVector} vector
 * @returns {Promise<{ early: number, decoded: number, value?: *, error?: Error, afterDuplicate: number }>}
 *   how many decodes happened before the last byte, after the frame, and after a second frame
 *   for the same correlation id
 */
const throughConnection = async (vector) => {
  const { state, counters, send, connection } = await connected(vector)
  const { correlationId, outcome } = await send()

  const frame = frameOf(vector, correlationId)
  let early = 0
  for (let at = 0; at < frame.length; at++) {
    state.socket.emit('data', frame.subarray(at, at + 1))
    if (at < frame.length - 1) early += counters.decoded
  }
  const result = await outcome
  const decoded = counters.decoded

  state.socket.emit('data', frame)
  await settle()
  await connection.disconnect()
  return { early, decoded, ...result, afterDuplicate: counters.decoded }
}

/** The cases of one vector: its body cut at every byte, then mutated, each carrying its label. */
const casesOf = (vector) => {
  const { body } = vector
  const empty = (buffer) => body.subarray(buffer.length).every((byte) => byte === 0)
  const flag = (name) => /= (2147483647|-2147483648)$/.test(name)
  return [
    ...[...truncations(body)].map(({ name, buffer }) => ({
      vector: vector.name,
      name,
      buffer,
      truncated: true,
      tailIsEmptyTags: empty(buffer),
    })),
    ...[...mutations(body, SEED)].map(({ name, buffer }) => ({
      vector: vector.name,
      name,
      buffer,
      prompt: flag(name),
    })),
  ]
}

/**
 * Runs the cases in workers and reports what they found.
 * @param {object[]} cases
 * @returns {Promise<{ ran: number, categories: Record<string, number>, violations: string[] }>}
 */
const runAndSummarise = async (cases) => {
  const results = await runCases({ handler, cases, seed: SEED })
  const categories = {}
  const violations = []

  results.forEach(({ ran, category, violation }, index) => {
    categories[category] = (categories[category] || 0) + ran
    if (violation !== null) {
      violations.push(
        `${violation}: ${cases[index].vector} ${cases[index].name}, ${caseLabel({ seed: SEED, index, buffer: cases[index].buffer })}`
      )
    }
  })

  return { ran: results.reduce((total, { ran }) => total + ran, 0), categories, violations }
}

describe('Protocol > response vectors of every API but Fetch > the corpus', () => {
  test('has a control for every response fixture, and nothing else', () => {
    expect(missing).toEqual([])
    expect(unused).toEqual([])
  })

  test('lets a control name another vector, one that holds a literal', () => {
    const { controls } = require('../fixtures/fuzzResponseControls.json')
    const named = Object.values(controls).filter((control) => typeof control === 'string')

    expect(named.length).toBeGreaterThan(0)
    for (const target of named) expect(controls[target]).toEqual(expect.any(Object))
  })

  test('sets aside only what is not the body of a response codec, each with its reason', () => {
    expect(excluded).toEqual([
      'saslAuthenticate/scram256_firstRequest_response.json',
      'saslAuthenticate/scram256_firstRequest_response_v1.json',
    ])
    const { exclusions, evidence } = require('../fixtures/fuzzResponseControls.json')
    for (const name of excluded) expect(exclusions[name].length).toBeGreaterThan(40)
    expect(Object.keys(evidence)).toEqual(['javaBuffer', 'capture', 'hand'])
  })

  test('is the committed count of vectors for each API', () => {
    const found = {}
    for (const { directory } of vectors) found[directory] = (found[directory] || 0) + 1

    expect(found).toEqual(COUNTS)
  })

  test('holds each control as the values of its vector: the fields of a Java vector, a literal for bytes', () => {
    for (const vector of vectors) {
      expect(vector.control).toEqual(expect.any(Object))
      expect(vector.kind === 'structured' || vector.control !== undefined).toBe(true)
    }
  })
})

describe.each(
  apis.map((directory) => [directory, vectors.filter((vector) => vector.directory === directory)])
)('Protocol > %s response > the corpus', (_, own) => {
  test(
    'each unmutated body decodes to its control, through the codec and through a connection that delivers it a byte at a time',
    async () => {
      const problems = []
      for (const vector of own) {
        const found = await controlProblem(vector)
        if (found) problems.push(found)

        const delivered = await throughConnection(vector)
        if (vector.control.error) {
          if (!delivered.error || delivered.error.constructor.name !== vector.control.error) {
            problems.push(`${vector.name}: through a connection, expected ${vector.control.error}`)
          }
        } else if (delivered.error) {
          problems.push(`${vector.name}: through a connection, ${delivered.error.message}`)
        } else if (differences(vector.control, project(vector, delivered.value)) !== null) {
          problems.push(`${vector.name}: through a connection, a different value`)
        }
        if (delivered.early !== 0 || delivered.decoded !== 1 || delivered.afterDuplicate !== 1) {
          problems.push(
            `${vector.name}: decoded ${delivered.early}, ${delivered.decoded}, ${delivered.afterDuplicate} times`
          )
        }
      }

      expect(problems).toEqual([])
    },
    API_TIMEOUT_MS
  )

  test(
    'cut at every byte and mutated by seed, each body decodes or fails typed, and the target ran each time',
    async () => {
      const problems = (await Promise.all(own.map(controlProblem))).filter(Boolean)
      expect(problems).toEqual([])

      const cases = own.flatMap(casesOf)
      const { ran, categories, violations } = await runAndSummarise(cases)

      expect(violations.slice(0, 5)).toEqual([])
      expect(ran).toBe(cases.length)
      expect(Object.values(categories).reduce((total, count) => total + count, 0)).toBe(
        cases.length
      )
    },
    API_TIMEOUT_MS
  )
})

describe('Protocol > response vectors > what the oracle can tell', () => {
  const heartbeat = vectors.find(
    ({ name }) => name === 'heartbeat/java/heartbeatResponse-v4-ok.json'
  )
  const body = heartbeat.body
  const stand = (extra = {}) => ({
    directory: heartbeat.directory,
    version: heartbeat.version,
    codec: heartbeat.codec,
    control: heartbeat.control,
    ...extra,
  })
  const withCodec = (value, extra = {}) =>
    runHandler({
      target: stand(),
      buffer: body,
      codec: { decode: async () => value, parse: async () => {} },
      ...extra,
    })

  test('a body cut inside its required fields is a typed error, and one that is whole is a value', async () => {
    await expect(
      runHandler({ vector: heartbeat.name, buffer: body.subarray(0, 3) })
    ).resolves.toEqual({
      ran: 1,
      category: 'typed:KafkaJSPartialMessageError',
      violation: null,
    })
    await expect(runHandler({ vector: heartbeat.name, buffer: body })).resolves.toEqual({
      ran: 1,
      category: 'decoded',
      violation: null,
    })
  })

  test.each([
    ['a RangeError', new RangeError('Invalid array length')],
    ['a TypeError', new TypeError('Cannot read properties of null')],
  ])('%s is a violation, and each typed error of the allow-list is not', async (_, untyped) => {
    const failing = (error) => ({
      decode: async () => {
        throw error
      },
      parse: async () => {},
    })
    const { KafkaJSAggregateError, KafkaJSInvariantViolation } = require('../../errors')

    const [broken, partial, protocol, aggregate, invariant] = await Promise.all([
      runHandler({ target: stand(), buffer: body, codec: failing(untyped) }),
      runHandler({
        target: stand(),
        buffer: body,
        codec: failing(new KafkaJSPartialMessageError('cut')),
      }),
      runHandler({
        target: stand(),
        buffer: body,
        codec: failing(new KafkaJSProtocolError('code')),
      }),
      runHandler({
        target: stand(),
        buffer: body,
        codec: failing(new KafkaJSAggregateError('many', [])),
      }),
      runHandler({
        target: stand(),
        buffer: body,
        codec: failing(new KafkaJSInvariantViolation('rule')),
      }),
    ])

    expect(broken.violation).toBe(`${untyped.name}: ${untyped.message}`)
    for (const outcome of [partial, protocol, aggregate, invariant])
      expect(outcome.violation).toBeNull()
  })

  test('more elements than bytes, and a change to Object.prototype', async () => {
    await expect(
      withCodec({ errorCode: 0, list: new Array(body.length + 1).fill(0) })
    ).resolves.toMatchObject({
      violation: `${body.length + 1} elements from ${body.length} bytes`,
    })

    const codec = {
      decode: async () => {
        // eslint-disable-next-line no-extend-native
        Object.prototype.fuzzPolluted = 1
        return {}
      },
      parse: async () => {},
    }
    try {
      await expect(runHandler({ target: stand(), buffer: body, codec })).resolves.toMatchObject({
        violation: 'Object.prototype changed',
      })
    } finally {
      delete Object.prototype.fuzzPolluted
    }
  })

  test('the values of the whole body from a body with bytes removed, unless the bytes are empty tags', async () => {
    const whole = { throttleTime: 25, errorCode: 0 }

    await expect(withCodec(whole, { truncated: true })).resolves.toMatchObject({
      violation: 'a body with bytes removed decoded to the values of the whole body',
    })
    await expect(
      withCodec(whole, { truncated: true, tailIsEmptyTags: true })
    ).resolves.toMatchObject({
      violation: null,
    })
    await expect(
      withCodec({ throttleTime: 25, errorCode: 1 }, { truncated: true })
    ).resolves.toMatchObject({
      violation: null,
    })
  })

  test('an outcome other than the one expected, and a decoder that is slow to refuse three times running', async () => {
    await expect(
      withCodec({}, { expect: { error: 'KafkaJSPartialMessageError' } })
    ).resolves.toMatchObject({
      violation: 'expected KafkaJSPartialMessageError, got decoded',
    })
    await expect(
      runHandler({ vector: heartbeat.name, buffer: body.subarray(0, 3), expect: { decoded: true } })
    ).resolves.toMatchObject({
      violation: 'expected a decoded value, got typed:KafkaJSPartialMessageError',
    })

    const slow = {
      decode: async () => {
        const until = performance.now() + 60
        while (performance.now() < until) {
          // a decoder that takes long to refuse an impossible count
        }
        return {}
      },
      parse: async () => {},
    }
    await expect(
      runHandler({ target: stand(), buffer: body, codec: slow, prompt: true })
    ).resolves.toMatchObject({
      violation: expect.stringMatching(/^took \d+\.\d ms$/),
    })
    await expect(withCodec({}, { prompt: true })).resolves.toMatchObject({ violation: null })
  })

  test('counts the call of the target decoder as the case having run, once', async () => {
    const decode = jest.fn(async () => ({}))

    await expect(
      runHandler({ target: stand(), buffer: body, codec: { decode, parse: async () => {} } })
    ).resolves.toMatchObject({ ran: 1, category: 'decoded', violation: null })
    expect(decode).toHaveBeenCalledTimes(1)
  })

  test('names a difference between a control and what was decoded', () => {
    expect(differences({ a: [1, { b: 'x' }] }, { a: [1, { b: 'y', c: 2 }] })).toBe(
      '.a[1].b: expected "x", got "y"'
    )
    expect(differences({ a: [1] }, { a: [1, 2] })).toBe('.a: expected a list of 1, got 2')
    expect(differences({ a: 1 }, {})).toBe('.a: missing')
    expect(differences({ a: { b: 1 } }, { a: null })).toBe('.a: expected an object, got null')
    expect(differences({ a: 1 }, { a: 1, extra: true })).toBeNull()
  })
})

describe('Protocol > response vectors > a frame that is not finished, on each header shape', () => {
  const hasSettled = async (promise) => {
    let settled = false
    promise.then(() => {
      settled = true
    })
    await settle()
    return settled
  }

  test.each([
    ['a classic response header', 'createTopics/v2_response.json'],
    ['a flexible response header', 'heartbeat/java/heartbeatResponse-v4-ok.json'],
    [
      "ApiVersions' header, which a flexible body does not change",
      'apiVersions/v4_response_java.json',
    ],
  ])(
    '%s: a frame cut anywhere settles nothing, and a close settles the queue with the connection error',
    async (_, name) => {
      const vector = vectors.find((found) => found.name === name)
      const length = frameOf(vector).length

      for (const bytes of [3, 4, 6, 8, length - 1]) {
        const { state, counters, send } = await connected(vector)
        const { correlationId, outcome } = await send()

        state.socket.emit('data', frameOf(vector, correlationId).subarray(0, bytes))

        expect({ bytes, settled: await hasSettled(outcome), decoded: counters.decoded }).toEqual({
          bytes,
          settled: false,
          decoded: 0,
        })
        state.socket.emit('end')
        const { error } = await outcome
        expect(error).toBeInstanceOf(KafkaJSConnectionClosedError)
      }
    }
  )
})

describe.each(
  loadCorpus(path.join(__dirname, '..', 'fixtures', 'fuzz'))
    .filter(({ api }) => api !== 'Fetch')
    .map((found) => [found.name, found])
)('the corpus case %s', (_, found) => {
  test('decodes as it is expected to', async () => {
    expect(found.direction).toBe('response')

    await expect(
      runHandler({
        target: {
          ...targetOf(found.api.charAt(0).toLowerCase() + found.api.slice(1), found.version),
          control: {},
        },
        buffer: Buffer.from(found.hex, 'hex'),
        expect: found.expect,
      })
    ).resolves.toMatchObject({ ran: 1, violation: null })
  })
})
