const { KafkaJSInvariantViolation, KafkaJSPartialMessageError } = require('../../errors')
const expected = require('../../../testHelpers/wire/expected')

const codecOf = (api, version) => require(`./${api}/v${version}/response`)
const wireOf = (api, version) => require(`../../../testHelpers/wire/${api}V${version}`)
const javaVector = (api, version) =>
  Buffer.from(require(`./${api}/fixtures/java/${api}Response-v${version}-storage.json`).hex, 'hex')

/** Resolves with the rejection reason, and fails the test when the promise resolves. */
const rejection = (promise) =>
  promise.then(
    () => {
      throw new Error('expected a rejection')
    },
    (error) => error
  )

describe('Protocol > Requests > the transaction and log directory response codecs', () => {
  describe('required transaction strings', () => {
    const topicNull = (api, version, build) => [
      api,
      version,
      (base) => build(base, expected[`${api}V${version}`].response),
    ]
    const CASES = [
      topicNull('writeTxnMarkers', 1, (_base, response) => ({
        markers: [
          { ...response.markers[0], topics: [{ ...response.markers[0].topics[0], topic: null }] },
        ],
      })),
      topicNull('writeTxnMarkers', 2, (_base, response) => ({
        markers: [
          { ...response.markers[0], topics: [{ ...response.markers[0].topics[0], topic: null }] },
        ],
      })),
      topicNull('describeProducers', 0, (_base, response) => ({
        topics: [{ ...response.topics[0], topic: null }],
      })),
      topicNull('describeTransactions', 0, (_base, response) => ({
        transactions: [
          {
            ...response.transactions[0],
            topics: [{ ...response.transactions[0].topics[0], topic: null }],
          },
        ],
      })),
    ]

    test.each(CASES)('%s v%i decode rejects a null topic name', async (api, version, fields) => {
      const bytes = wireOf(api, version).buildResponse(fields())

      const error = await rejection(codecOf(api, version).decode(bytes))

      expect(error).toBeInstanceOf(KafkaJSPartialMessageError)
      expect(error.message).toContain('Required transaction string is null')
    })
  })

  describe('trailing bytes', () => {
    const CASES = [
      ['describeLogDirs', 1, 'logDirs', 'DescribeLogDirs response has trailing bytes'],
      ['describeLogDirs', 5, 'logDirs', 'DescribeLogDirs response has trailing bytes'],
      ['alterReplicaLogDirs', 1, 'topics', 'AlterReplicaLogDirs response has trailing bytes'],
      ['alterReplicaLogDirs', 2, 'topics', 'AlterReplicaLogDirs response has trailing bytes'],
      [
        'alterPartitionReassignments',
        1,
        'responses',
        'AlterPartitionReassignments response has trailing bytes',
      ],
    ]

    test.each(CASES)(
      '%s v%i decode accepts the Apache vector and rejects it with one more byte',
      async (api, version, key, message) => {
        const { decode } = codecOf(api, version)
        const vector = javaVector(api, version)

        const decoded = await decode(vector)
        const error = await rejection(decode(Buffer.concat([vector, Buffer.from([0xff])])))

        expect(decoded[key]).toHaveLength(1)
        expect(error).toBeInstanceOf(KafkaJSInvariantViolation)
        expect(error.message).toContain(message)
      }
    )
  })

  describe('parse', () => {
    test.each([
      ['describeLogDirs', 1],
      ['describeLogDirs', 2],
      ['describeLogDirs', 3],
      ['describeLogDirs', 4],
      ['alterReplicaLogDirs', 1],
    ])('%s v%i parse returns the decoded response unchanged', async (api, version) => {
      const { decode, parse } = codecOf(api, version)
      const data = await decode(javaVector(api, version))

      await expect(parse(data)).resolves.toBe(data)
    })
  })
})
