const { KafkaJSPartialMessageError, KafkaJSProtocolError } = require('../../errors')
const expected = require('../../../testHelpers/wire/expected')

const codecOf = (api, version) => require(`./${api}/v${version}/response`)
const wireOf = (api, version) => require(`../../../testHelpers/wire/${api}V${version}`)
const fixtureOf = (api, version) => expected[`${api}V${version}`].response

/** Resolves with the rejection reason, and fails the test when the promise resolves. */
const rejection = (promise) =>
  promise.then(
    () => {
      throw new Error('expected a rejection')
    },
    (error) => error
  )

/** Decodes the bytes a broker would send for `fields`, then parses them. */
const parsedFailure = (api, version, fields) => {
  const { decode, parse } = codecOf(api, version)
  return rejection(decode(wireOf(api, version).buildResponse(fields)).then(parse))
}

const rowsOf = (api, versions) => versions.map((version) => [api, version])

const WITH_MESSAGE = [
  ...rowsOf('describeCluster', [0, 1, 2]),
  ...rowsOf('updateFeatures', [0, 1, 2]),
]
const WITHOUT_MESSAGE = [
  ...WITH_MESSAGE,
  ...rowsOf('electLeaders', [1, 2]),
  ...rowsOf('listConfigResources', [0, 1]),
]

describe('Protocol > Requests > the cluster admin response codecs', () => {
  describe('call-level error', () => {
    test.each(WITH_MESSAGE)(
      '%s v%i parse rejects with the broker error and its message',
      async (api, version) => {
        const error = await parsedFailure(api, version, { errorCode: 31, errorMessage: 'denied' })

        expect(error).toBeInstanceOf(KafkaJSProtocolError)
        expect({
          type: error.type,
          code: error.code,
          brokerErrorMessage: error.brokerErrorMessage,
        }).toEqual({
          type: 'CLUSTER_AUTHORIZATION_FAILED',
          code: 31,
          brokerErrorMessage: 'denied',
        })
      }
    )

    test.each(WITHOUT_MESSAGE)(
      '%s v%i parse rejects with a null brokerErrorMessage when the broker sent no message',
      async (api, version) => {
        const error = await parsedFailure(api, version, { errorCode: 31 })

        expect(error).toBeInstanceOf(KafkaJSProtocolError)
        expect({
          type: error.type,
          code: error.code,
          brokerErrorMessage: error.brokerErrorMessage,
        }).toEqual({
          type: 'CLUSTER_AUTHORIZATION_FAILED',
          code: 31,
          brokerErrorMessage: null,
        })
      }
    )

    test.each(rowsOf('listTransactions', [0, 1, 2]))(
      '%s v%i parse rejects the aggregate error code',
      async (api, version) => {
        const error = await parsedFailure(api, version, { errorCode: 31 })

        expect(error).toBeInstanceOf(KafkaJSProtocolError)
        expect({ type: error.type, code: error.code }).toEqual({
          type: 'CLUSTER_AUTHORIZATION_FAILED',
          code: 31,
        })
      }
    )

    test.each(rowsOf('listTransactions', [0, 1, 2]))(
      '%s v%i parse returns an error-free response as the same object',
      async (api, version) => {
        const { decode, parse } = codecOf(api, version)
        const data = await decode(wireOf(api, version).buildResponse())

        expect(data.errorCode).toBe(0)
        await expect(parse(data)).resolves.toBe(data)
      }
    )
  })

  describe('required strings', () => {
    const CASES = [
      ...[0, 1, 2].map((version) => [
        'describeCluster',
        version,
        'broker host',
        (base) => ({ brokers: [{ ...base.brokers[0], host: null }] }),
        'host must not be null',
      ]),
      ...[0, 1].map((version) => [
        'describeCluster',
        version,
        'cluster id',
        () => ({ clusterId: null }),
        'clusterId must not be null',
      ]),
      ...[0, 1].map((version) => [
        'electLeaders',
        version,
        'topic',
        () => ({ replicaElectionResults: [{ topic: null, partitionResult: [] }] }),
        'topic must not be null',
      ]),
      [
        'updateFeatures',
        0,
        'feature',
        () => ({ results: [{ feature: null, errorCode: 0, errorMessage: null }] }),
        'feature must not be null',
      ],
      [
        'listConfigResources',
        0,
        'resource name',
        () => ({ configResources: [{ resourceName: null }] }),
        'resourceName must not be null',
      ],
      ...[0, 1, 2].map((version) => [
        'listTransactions',
        version,
        'transactional id',
        (base) => ({ transactions: [{ ...base.transactions[0], transactionalId: null }] }),
        'Required transaction string is null',
      ]),
      ...[0, 1, 2].map((version) => [
        'listTransactions',
        version,
        'unknown state filter',
        () => ({ unknownStateFilters: [null] }),
        'Required transaction string is null',
      ]),
    ]

    test.each(CASES)(
      '%s v%i decode rejects a null %s',
      async (api, version, _what, fields, message) => {
        const bytes = wireOf(api, version).buildResponse(fields(fixtureOf(api, version)))

        const error = await rejection(codecOf(api, version).decode(bytes))

        expect(error).toBeInstanceOf(KafkaJSPartialMessageError)
        expect(error.message).toContain(message)
      }
    )
  })
})
