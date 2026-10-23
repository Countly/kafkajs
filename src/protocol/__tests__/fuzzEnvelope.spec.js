const path = require('path')
const {
  caseLabel,
  loadCorpus,
  mutations,
  runCases,
  truncations,
} = require('../../../testHelpers/fuzz')
const {
  vectors,
  bodyOf,
  project,
  codecOf,
  withRecordsCutTo,
  offsetsWithin,
  withForgedField,
} = require('./fixtures/fuzzVectors')
const { KafkaJSCorruptRecordError, KafkaJSPartialMessageError } = require('../../errors')
const Broker = require('../../broker')
const ConnectionPool = require('../../network/connectionPool')
const { createScriptedCluster } = require('../../../testHelpers/scriptedCluster')
const { fetchResponse, TOPIC, TOPIC_ID } = require('../../consumer/__tests__/fixtures/fetchHarness')
const { newLogger } = require('testHelpers')
const { batch, record } = require('../requests/fetch/__tests__/fixtures/recordBatchBytes')
const handler = require.resolve('./fixtures/fuzzDecodeHandler')
const runHandler = require('./fixtures/fuzzDecodeHandler')

/** The seed every stream of cases here comes from: a failure prints it with the case index. */
const SEED = 20261017
const CHECK_CRCS = [{ checkCrcs: true }, { checkCrcs: false }]
const WITH_RECORDS = vectors.filter(({ records }) => records)

/** The longest a vector's cases may take to run: a hung decoder fails the test, not the suite. */
const VECTOR_TIMEOUT_MS = 120000

/**
 * The cases of one stream of mutations of a vector's body, each decoded under both CRC modes where
 * the vector has batches to check.
 * @param {import('./fixtures/fuzzVectors').FuzzVector} vector
 * @param {Iterable<{ name: string, buffer: Buffer }>} stream
 * @param {(buffer: Buffer) => object} [extra] more fields of a case, from its body
 * @returns {object[]}
 */
const casesOf = (vector, stream, extra = () => ({})) =>
  [...stream].flatMap(({ name, buffer }) =>
    (vector.records ? CHECK_CRCS : [{}]).map((options) => ({
      vector: vector.name,
      version: vector.version,
      name,
      buffer,
      options,
      ...extra(buffer),
    }))
  )

/** The cases that cut the RECORDS block of a vector at every byte, with the batches left whole. */
const reframedCuts = (vector) =>
  Array.from({ length: vector.records.blockLength }, (_, bytes) =>
    CHECK_CRCS.map((options) => ({
      vector: vector.name,
      version: vector.version,
      name: `records cut to ${bytes}`,
      buffer: withRecordsCutTo(vector, bytes),
      options,
      expect: { offsets: offsetsWithin(vector, bytes), corruption: null },
    }))
  ).flat()

const FORGED_LENGTHS = [-100, -1, 0, 5, 13, 14, 48]
const FORGED_MAGICS = [-128, -1, 3, 7, 127]
const FORGED_COUNTS = [-2147483648, -1, 0x7fffffff]

/**
 * The cases that write an impossible value into one field of one batch, each with the outcome the
 * rules of the format give it, not the outcome of any decoder.
 * @param {import('./fixtures/fuzzVectors').FuzzVector} vector one with `records`
 * @returns {object[]}
 */
const forgedFields = (vector) =>
  vector.records.batches.flatMap((_, batchIndex) => {
    const before = vector.records.batches.slice(0, batchIndex).flatMap(({ offsets }) => offsets)
    const forged = (field, value, options, expect, prompt = false) => ({
      vector: vector.name,
      version: vector.version,
      name: `batch ${batchIndex} ${field} = ${value}`,
      buffer: withForgedField(vector, batchIndex, field, value),
      options,
      expect,
      prompt,
    })

    return [
      ...FORGED_LENGTHS.flatMap((value) =>
        CHECK_CRCS.map((options) =>
          forged('length', value, options, { offsets: before, corruption: 'length' })
        )
      ),
      ...CHECK_CRCS.map((options) =>
        forged('length', 0x7fffffff, options, { offsets: before, corruption: null })
      ),
      ...FORGED_MAGICS.flatMap((value) =>
        CHECK_CRCS.map((options) =>
          forged('magic', value, options, { offsets: before, corruption: 'magic' })
        )
      ),
      ...FORGED_COUNTS.flatMap((value) => [
        forged(
          'count',
          value,
          { checkCrcs: false },
          { offsets: before, corruption: 'count' },
          true
        ),
        forged('count', value, { checkCrcs: true }, { offsets: before, corruption: 'checksum' }),
      ]),
    ]
  })

/**
 * The paired control of a vector's mutated runs: its unmutated body decodes to its control under
 * both CRC modes, in this thread, in the same test that counts the mutated runs.
 * @param {import('./fixtures/fuzzVectors').FuzzVector} vector
 * @returns {Promise<void>}
 */
const controlHolds = async (vector) => {
  for (const options of CHECK_CRCS) {
    const decoded = await codecOf(vector).decode(bodyOf(vector), options)
    expect(JSON.parse(JSON.stringify(project(decoded)))).toEqual(vector.control)
  }
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
        `${violation}: ${cases[index].name}, ${caseLabel({ seed: SEED, index, buffer: cases[index].buffer })}`
      )
    }
  })

  return { ran: results.reduce((total, { ran }) => total + ran, 0), categories, violations }
}

describe('Protocol > Fetch response > the fuzz corpus', () => {
  test('is the committed list of vectors, each with the values it decodes to', () => {
    expect(vectors.map(({ name }) => name)).toEqual([
      'fetch-v12-java-null-records',
      'fetch-v13-java-null-records',
      'fetch-v14-java-null-records',
      'fetch-v15-java-null-records',
      'fetch-v16-java-null-records',
      'fetch-v17-java-null-records',
      'fetch-v18-java-null-records',
      'fetch-v18-kafka431-capture',
      'fetch-v18-kafka412-capture',
      'fetch-v12-hand-null-records',
      'fetch-v13-hand-null-records',
      'fetch-v12-hand-tagged-partition',
      'fetch-v16-hand-node-endpoints',
      'fetch-v12-hand-one-batch',
      'fetch-v4-hand-three-records',
      'fetch-v5-hand-three-records',
      'fetch-v6-hand-three-records',
      'fetch-v7-hand-three-records',
      'fetch-v8-hand-three-records',
      'fetch-v9-hand-three-records',
      'fetch-v10-hand-three-records',
      'fetch-v11-hand-three-records',
      'fetch-v12-built-prototype-named-headers',
      'fetch-v4-scala-six-batches',
    ])
    for (const vector of vectors) {
      expect(vector.control).toEqual(expect.any(Object))
      expect(vector.evidence).toEqual(expect.any(String))
    }
  })

  test('the vector that is built is the bytes its batch inputs give', () => {
    const built = vectors.find(({ name }) => name === 'fetch-v12-built-prototype-named-headers')
    const { blockAt, blockLength } = built.records
    const rebuilt = batch({
      records: [
        record({
          key: 'k0',
          value: 'v0',
          headers: [
            ['__proto__', 'a'],
            ['constructor', 'b'],
            ['toString', 'c'],
            ['hasOwnProperty', 'd'],
          ],
        }),
        record({
          offsetDelta: 1,
          key: 'k1',
          value: 'v1',
          headers: [
            ['dup', '1'],
            ['dup', '2'],
            ['dup', '3'],
            ['plain', 'p'],
          ],
        }),
      ],
    })

    expect(bodyOf(built).subarray(blockAt, blockAt + blockLength)).toEqual(rebuilt)
  })

  describe.each(vectors.map((vector) => [vector.name, vector]))('%s', (_, vector) => {
    const body = bodyOf(vector)

    test.each(CHECK_CRCS)('the unmutated body decodes to its control with %o', async (options) => {
      const decoded = await codecOf(vector).decode(body, options)

      expect(JSON.parse(JSON.stringify(project(decoded)))).toEqual(vector.control)
    })

    test(
      'cut at every byte and mutated by seed, it decodes or fails typed, and the target ran each time',
      async () => {
        await controlHolds(vector)
        const cases = [
          ...casesOf(vector, truncations(body), (buffer) => ({
            control: vector.control,
            tailIsEmptyTags: body.subarray(buffer.length).every((byte) => byte === 0),
          })),
          ...casesOf(vector, mutations(body, SEED)),
        ]

        const { ran, categories, violations } = await runAndSummarise(cases)

        expect(violations.slice(0, 5)).toEqual([])
        expect(ran).toBe(cases.length)
        expect(Object.values(categories).reduce((total, count) => total + count, 0)).toBe(
          cases.length
        )
      },
      VECTOR_TIMEOUT_MS
    )
  })

  describe.each(WITH_RECORDS.map((vector) => [vector.name, vector]))('%s records', (_, vector) => {
    test(
      'a block cut at every byte returns the batches before the cut and nothing else',
      async () => {
        await controlHolds(vector)
        const cases = reframedCuts(vector)

        const { ran, violations } = await runAndSummarise(cases)

        expect(violations.slice(0, 5)).toEqual([])
        expect(ran).toBe(cases.length)
      },
      VECTOR_TIMEOUT_MS
    )

    test(
      'an impossible length, magic or count in any batch is named, with the batches before it kept',
      async () => {
        await controlHolds(vector)
        const cases = forgedFields(vector)

        const { ran, violations } = await runAndSummarise(cases)

        expect(violations.slice(0, 5)).toEqual([])
        expect(ran).toBe(cases.length)
      },
      VECTOR_TIMEOUT_MS
    )
  })

  describe('each outcome has a case that shows the oracle can tell it', () => {
    const scala = vectors.find(({ name }) => name === 'fetch-v4-scala-six-batches')
    const javaNullRecords = vectors.find(({ name }) => name === 'fetch-v18-java-null-records')
    const firstThree = ['0', '1', '2']

    const outcomeOf = (buffer, expect) =>
      runHandler({ version: scala.version, buffer, options: { checkCrcs: true }, expect })

    test('a body cut inside its required fields is a typed error', async () => {
      const cut = bodyOf(javaNullRecords).subarray(0, 30)

      await expect(runHandler({ version: javaNullRecords.version, buffer: cut })).resolves.toEqual({
        ran: 1,
        category: 'typed:KafkaJSPartialMessageError',
        violation: null,
      })
    })

    test('a complete batch and a cut one is the complete prefix, the cut one contributing nothing', async () => {
      const buffer = withRecordsCutTo(scala, 107 * 3 + 50)

      await expect(outcomeOf(buffer, { offsets: firstThree, corruption: null })).resolves.toEqual({
        ran: 1,
        category: 'decoded',
        violation: null,
      })
    })

    test('a complete batch that is impossible is the typed marker with the batches before it', async () => {
      const buffer = withForgedField(scala, 3, 'length', -1)

      await expect(
        outcomeOf(buffer, { offsets: firstThree, corruption: 'length' })
      ).resolves.toEqual({
        ran: 1,
        category: 'decoded',
        violation: null,
      })
    })

    test('the oracle reports an outcome that is not the one expected', async () => {
      const cut = withRecordsCutTo(scala, 107 * 3 + 50)
      const forged = withForgedField(scala, 3, 'length', -1)

      const [prefixAsCorrupt, corruptAsPrefix] = await Promise.all([
        outcomeOf(cut, { offsets: firstThree, corruption: 'length' }),
        outcomeOf(forged, { offsets: firstThree, corruption: null }),
      ])

      expect(prefixAsCorrupt.violation).toMatch(/^decoded .*, expected /)
      expect(corruptAsPrefix.violation).toMatch(/^decoded .*, expected /)
    })

    describe('the oracle names what is wrong with a decoded value', () => {
      const data = (value) => Buffer.from(value)
      const decoded = (partition = {}) => ({
        responses: [{ partitions: [{ messages: [], ...partition }] }],
      })
      const message = (headers) => ({ offset: '0', headers })
      const withCodec = (value, extra = {}) =>
        runHandler({
          version: 18,
          buffer: Buffer.alloc(10),
          codec: { decode: async () => value, parse: async () => {} },
          ...extra,
        })

      test.each([
        [
          'more elements than bytes',
          () => decoded({ messages: new Array(11).fill(message({})) }),
          '13 elements from 10 bytes',
        ],
        ['a null list of topics', () => ({ responses: null }), 'responses is not an array'],
        [
          'a null list of partitions',
          () => ({ responses: [{ partitions: null }] }),
          'partitions is not an array',
        ],
        [
          'messages that are not a list',
          () => decoded({ messages: null }),
          'messages is not an array',
        ],
        [
          'a header that is the constructor of the object',
          () => decoded({ messages: [message({ constructor: [Object, data('x')] })] }),
          'header constructor is not data',
        ],
      ])('%s', async (_, make, violation) => {
        await expect(withCodec(make())).resolves.toMatchObject({ ran: 1, violation })
      })

      test('a change to Object.prototype', async () => {
        const codec = {
          decode: async () => {
            // eslint-disable-next-line no-extend-native
            Object.prototype.fuzzPolluted = 1
            return decoded()
          },
          parse: async () => {},
        }

        try {
          await expect(
            runHandler({ version: 18, buffer: Buffer.alloc(10), codec })
          ).resolves.toMatchObject({ violation: 'Object.prototype changed' })
        } finally {
          delete Object.prototype.fuzzPolluted
        }
      })

      test('an outcome other than the one expected, and a typed error where a value was', async () => {
        await expect(
          withCodec(decoded({ messages: [message({})] }), {
            expect: { offsets: [], corruption: null },
          })
        ).resolves.toMatchObject({ violation: expect.stringMatching(/^decoded .*expected /) })
        await expect(
          runHandler({
            version: 18,
            buffer: Buffer.alloc(10),
            expect: { decoded: true },
            codec: {
              decode: async () => {
                throw new KafkaJSPartialMessageError('cut')
              },
              parse: async () => {},
            },
          })
        ).resolves.toMatchObject({
          violation: 'expected a decoded value, got typed:KafkaJSPartialMessageError',
        })
      })

      test('the values of the whole body from a body with bytes removed, unless the bytes are empty tags', async () => {
        const control = { errorCode: 0, topics: [] }
        const whole = { responses: [], errorCode: 0 }

        await expect(withCodec(whole, { control })).resolves.toMatchObject({
          violation: 'a body with bytes removed decoded to the values of the whole body',
        })
        await expect(withCodec(whole, { control, tailIsEmptyTags: true })).resolves.toMatchObject({
          violation: null,
        })
      })

      test('a decoder that is slow to refuse, three times running, and one that is quick', async () => {
        const slow = {
          decode: async () => {
            const until = performance.now() + 60
            while (performance.now() < until) {
              // a decoder that takes long to refuse an impossible count
            }
            return decoded()
          },
          parse: async () => {},
        }

        await expect(
          runHandler({ version: 18, buffer: Buffer.alloc(10), codec: slow, prompt: true })
        ).resolves.toMatchObject({ violation: expect.stringMatching(/^took \d+\.\d ms$/) })
        await expect(withCodec(decoded(), { prompt: true })).resolves.toMatchObject({
          violation: null,
        })
      })

      test('counts the call of the target decoder as the case having run, once', async () => {
        const decode = jest.fn(async () => decoded())

        await expect(
          runHandler({
            version: 18,
            buffer: Buffer.alloc(10),
            codec: { decode, parse: async () => {} },
          })
        ).resolves.toMatchObject({ ran: 1, category: 'decoded', violation: null })
        expect(decode).toHaveBeenCalledTimes(1)
      })
    })

    test.each([
      ['a RangeError', new RangeError('Invalid array length')],
      ['a TypeError', new TypeError('Cannot read properties of undefined')],
    ])('%s is a violation, a typed error is not', async (_, untyped) => {
      const failing = (error) => ({
        decode: async () => {
          throw error
        },
        parse: async () => {},
      })
      const body = bodyOf(javaNullRecords)

      const [broken, typed] = await Promise.all([
        runHandler({ version: 18, buffer: body, codec: failing(untyped) }),
        runHandler({
          version: 18,
          buffer: body,
          codec: failing(new KafkaJSPartialMessageError('cut')),
        }),
      ])

      expect(broken.violation).toBe(`${untyped.name}: ${untyped.message}`)
      expect(typed.violation).toBeNull()
    })
  })

  describe('through Broker#fetch on the scripted cluster', () => {
    const brokers = []

    afterEach(async () => {
      await Promise.all(brokers.splice(0).map((broker) => broker.disconnect().catch(() => {})))
    })

    /** Answers the first Fetch with `partitions` and returns what `Broker#fetch` decoded. */
    const fetched = async (partitions, { checkCrcs = true } = {}) => {
      const cluster = createScriptedCluster({ nodes: [{ id: 0, host: 'node0', port: 9092 }] })
      cluster.schedule.once({ api: 'Fetch' }, fetchResponse(partitions, { apiVersion: 18 }))
      const connectionPool = new ConnectionPool({
        host: 'node0',
        port: 9092,
        logger: newLogger(),
        socketFactory: cluster.socketFactory,
        clientId: 'fuzz',
        connectionTimeout: 1000,
        requestTimeout: 1000,
        enforceRequestTimeout: false,
      })
      const broker = new Broker({ connectionPool, logger: newLogger(), nodeId: 0 })
      brokers.push(broker)
      await broker.connect()

      const result = await broker
        .fetch({
          checkCrcs,
          topicIds: new Map([[TOPIC, TOPIC_ID]]),
          topics: [
            {
              topic: TOPIC,
              partitions: partitions.map(({ partition }) => ({
                partition,
                fetchOffset: '0',
                maxBytes: 1048576,
              })),
            },
          ],
        })
        .then(
          (response) => ({ response }),
          (error) => ({ error })
        )

      expect(cluster.requests().filter(({ api }) => api === 'Fetch')).toHaveLength(1)
      cluster.assertClean()
      return result
    }

    const good = (baseOffset) =>
      batch({
        baseOffset: BigInt(baseOffset),
        records: [record({ key: `k${baseOffset}`, value: `v${baseOffset}` })],
      })
    const offsetsOf = ({ messages }) => messages.map(({ offset }) => offset)

    test('a corrupt batch is carried on its partition, and the other partition of the response is delivered', async () => {
      const { response, error } = await fetched([
        {
          partition: 0,
          errorCode: 0,
          records: Buffer.concat([good(0), batch({ baseOffset: 1n, length: -1 })]),
        },
        { partition: 1, errorCode: 0, records: Buffer.concat([good(5), good(6)]) },
      ])

      expect(error).toBeUndefined()
      const [first, second] = response.responses[0].partitions
      expect(offsetsOf(first)).toEqual(['0'])
      expect(first.corruption).toMatchObject({ reason: 'length', offset: '1' })
      expect(first.corruption.error).toBeInstanceOf(KafkaJSCorruptRecordError)
      expect(offsetsOf(second)).toEqual(['5', '6'])
      expect(second).not.toHaveProperty('corruption')
    })

    test('a RECORDS field shorter than a batch header is a partial tail, not a crash', async () => {
      const { response, error } = await fetched([
        { partition: 0, errorCode: 0, records: good(0).subarray(0, 5) },
      ])

      expect(error).toBeUndefined()
      expect(response.responses[0].partitions[0].messages).toEqual([])
    })

    test('a CRC mismatch is carried as a checksum corruption when checkCrcs is on', async () => {
      const wrongCrc = batch({ crc: 0xdeadbeef, records: [record()] })

      const { response, error } = await fetched([{ partition: 0, errorCode: 0, records: wrongCrc }])

      expect(error).toBeUndefined()
      expect(response.responses[0].partitions[0].corruption).toMatchObject({ reason: 'checksum' })
    })

    test('and is decoded when it is off', async () => {
      const wrongCrc = batch({ crc: 0xdeadbeef, records: [record()] })

      const { response } = await fetched([{ partition: 0, errorCode: 0, records: wrongCrc }], {
        checkCrcs: false,
      })

      expect(offsetsOf(response.responses[0].partitions[0])).toEqual(['0'])
    })
  })

  describe.each(
    loadCorpus(path.join(__dirname, '..', 'fixtures', 'fuzz'))
      .filter(({ api }) => api === 'Fetch')
      .map((found) => [found.name, found])
  )('the corpus case %s', (_, found) => {
    test('decodes as it is expected to', async () => {
      expect({ api: found.api, direction: found.direction }).toEqual({
        api: 'Fetch',
        direction: 'response',
      })

      await expect(
        runHandler({
          version: found.version,
          buffer: Buffer.from(found.hex, 'hex'),
          options: found.options,
          expect: found.expect,
        })
      ).resolves.toMatchObject({ ran: 1, violation: null })
    })
  })
})
