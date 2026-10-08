const Encoder = require('../../protocol/encoder')
const apiKeys = require('../../protocol/requests/apiKeys')
const { requests } = require('../../protocol/requests')
const { createGroupApiBroker } = require('./fixtures/groupApiHarness')

const INIT_PRODUCER_ID = '../../protocol/requests/initProducerId/fixtures'
const OFFSET_FETCH = '../../protocol/requests/offsetFetch/fixtures'

/** @returns {{ hex: string, fields: object }} an Apache 4.3.1 InitProducerId request vector */
const initProducerIdVector = (version, caseId) =>
  require(`${INIT_PRODUCER_ID}/java/initProducerIdRequest-v${version}-${caseId}.json`)

/** @returns {{ hex: string, fields: object }} an Apache 4.3.1 OffsetFetch request vector */
const offsetFetchVector = (version, caseId) =>
  require(`${OFFSET_FETCH}/java/offsetFetchRequest-v${version}-${caseId}.json`)

/** A successful InitProducerId response body (producer id 1000, epoch 0); v2-v5 share it. */
const INIT_PRODUCER_ID_RESPONSE = Buffer.from(require(`${INIT_PRODUCER_ID}/v5_response_java.json`))

/** A successful OffsetFetch response body for each version this spec negotiates. */
const OFFSET_FETCH_RESPONSES = {
  6: Buffer.from(require(`${OFFSET_FETCH}/v6_response_java.json`)),
  7: Buffer.from(require(`${OFFSET_FETCH}/v7_response_java.json`)),
  8: Buffer.from(require(`${OFFSET_FETCH}/v8_response_java.json`)),
  9: Buffer.from(require(`${OFFSET_FETCH}/java/offsetFetchResponse-v9-basic.json`).hex, 'hex'),
}

/**
 * A codec definition offering versions 0-5 whose requests carry an empty body, recording the
 * version the broker selected and the arguments it built the request from. It stands in for the
 * EndTxn and TxnOffsetCommit codecs, which this client implements only up to v1 so far, so the
 * transaction-version gate - not the codec inventory - decides which version goes out.
 * @param {number} apiKey
 * @param {string} apiName
 * @param {{ version: number, args: object }[]} built receives one entry per request built
 * @returns {{ versions: string[], protocol: Function }}
 */
const standInDefinition = (apiKey, apiName, built) => ({
  versions: ['0', '1', '2', '3', '4', '5'],
  protocol:
    ({ version }) =>
    (args) => {
      built.push({ version, args })
      return {
        request: { apiKey, apiVersion: version, apiName, encode: async () => new Encoder() },
        response: { decode: async () => ({}), parse: async (data) => data },
      }
    },
})

describe('Broker > transactional request parameters', () => {
  const harnesses = []

  /**
   * A real Broker over a real Connection on a fake socket, negotiating against `versions` and
   * answering every request with `respond(header)`'s body.
   */
  const build = (versions, respond) => {
    const harness = createGroupApiBroker({ versions, respond })
    harnesses.push(harness)
    return harness
  }

  afterEach(async () => {
    jest.restoreAllMocks()
    await Promise.all(harnesses.splice(0).map(({ pool }) => pool.destroy()))
  })

  describe('initProducerId', () => {
    const pinnedAt = (maxVersion) =>
      build(
        { [apiKeys.InitProducerId]: { minVersion: 0, maxVersion } },
        () => INIT_PRODUCER_ID_RESPONSE
      )

    test.each([3, 4, 5])(
      'v%i carries the producer id and epoch it is given, byte for byte',
      async (version) => {
        const { broker, requests: sent } = pinnedAt(version)
        const { hex, fields } = initProducerIdVector(version, 'reinit')

        await broker.initProducerId(fields)

        expect(sent).toHaveLength(1)
        expect(sent[0]).toMatchObject({ apiKey: apiKeys.InitProducerId, apiVersion: version })
        expect(sent[0].body.toString('hex')).toEqual(hex)
      }
    )

    test.each([3, 4, 5])(
      'v%i sends -1/-1 when it is given no pair: a first initialization, unchanged',
      async (version) => {
        const { broker, requests: sent } = pinnedAt(version)
        const { hex, fields } = initProducerIdVector(version, 'firstInit')

        await broker.initProducerId({
          transactionalId: fields.transactionalId,
          transactionTimeout: fields.transactionTimeout,
        })

        expect(sent[0].body.toString('hex')).toEqual(hex)
      }
    )

    test('an idempotent-only producer sends a null transactional id', async () => {
      const { broker, requests: sent } = pinnedAt(5)
      const { hex, fields } = initProducerIdVector(5, 'nullTransactionalId')

      await broker.initProducerId(fields)

      expect(sent[0].body.toString('hex')).toEqual(hex)
    })

    test('below v3 the pair is dropped: that body has no field for it', async () => {
      const { broker, requests: sent } = pinnedAt(2)
      const { fields } = initProducerIdVector(5, 'reinit')
      const { transactionalId, transactionTimeout } = fields
      const { request } = requests.InitProducerId.protocol({ version: 2 })({
        transactionalId,
        transactionTimeout,
      })

      await broker.initProducerId(fields)

      expect(sent[0].apiVersion).toEqual(2)
      expect(sent[0].body.toString('hex')).toEqual((await request.encode()).buffer.toString('hex'))
    })

    test('resolves the decoded response', async () => {
      const { broker } = pinnedAt(5)
      const { fields } = initProducerIdVector(5, 'reinit')

      await expect(broker.initProducerId(fields)).resolves.toMatchObject({
        errorCode: 0,
        producerId: '1000',
        producerEpoch: 0,
      })
    })
  })

  describe('offsetFetch', () => {
    const pinnedAt = (maxVersion) =>
      build(
        { [apiKeys.OffsetFetch]: { minVersion: 1, maxVersion } },
        ({ apiVersion }) => OFFSET_FETCH_RESPONSES[apiVersion]
      )

    test.each([7, 8, 9])(
      'v%i asks for stable offsets when told to, byte for byte',
      async (version) => {
        const { broker, requests: sent } = pinnedAt(version)
        const { hex, fields } = offsetFetchVector(version, 'requireStable')

        await broker.offsetFetch(fields)

        expect(sent[0]).toMatchObject({ apiKey: apiKeys.OffsetFetch, apiVersion: version })
        expect(sent[0].body.toString('hex')).toEqual(hex)
      }
    )

    test.each([7, 8, 9])(
      "v%i defaults RequireStable to false, so the admin client's request is unchanged",
      async (version) => {
        const { broker, requests: sent } = pinnedAt(version)
        const { hex, fields } = offsetFetchVector(version, 'notRequireStable')
        const { requireStable, ...withoutFlag } = fields

        expect(requireStable).toBe(false)
        await broker.offsetFetch(withoutFlag)

        expect(sent[0].body.toString('hex')).toEqual(hex)
      }
    )

    test('below v7 the flag is dropped: that body has no field for it', async () => {
      const { broker, requests: sent } = pinnedAt(6)
      const { fields } = offsetFetchVector(7, 'requireStable')
      const { groupId, topics } = fields
      const { request } = requests.OffsetFetch.protocol({ version: 6 })({ groupId, topics })

      await broker.offsetFetch(fields)

      expect(sent[0].apiVersion).toEqual(6)
      expect(sent[0].body.toString('hex')).toEqual((await request.encode()).buffer.toString('hex'))
    })
  })

  describe('the transaction-version gate on EndTxn and TxnOffsetCommit', () => {
    const TRANSACTIONAL = {
      transactionalId: 'test-transactional-id',
      producerId: '1000',
      producerEpoch: 3,
    }
    const ARGUMENTS = {
      endTxn: { ...TRANSACTIONAL, transactionResult: true },
      txnOffsetCommit: {
        ...TRANSACTIONAL,
        groupId: 'test-group',
        topics: [{ topic: 'test-topic', partitions: [{ partition: 0, offset: '10' }] }],
      },
    }
    const METHODS = [
      ['endTxn', apiKeys.EndTxn, 'EndTxn'],
      ['txnOffsetCommit', apiKeys.TxnOffsetCommit, 'TxnOffsetCommit'],
    ]

    /** A broker offering v0-v5 of the API, with the stand-in codec in place of the real one. */
    const offeringUpTo5 = (apiKey, apiName) => {
      const built = []
      jest.replaceProperty(requests, apiName, standInDefinition(apiKey, apiName, built))
      const harness = build({ [apiKey]: { minVersion: 0, maxVersion: 5 } }, () => Buffer.alloc(0))
      return { ...harness, built }
    }

    test.each(METHODS)(
      '%s under transaction version 1 stops at v4 although v5 is on offer',
      async (method, apiKey, apiName) => {
        const { broker, built, requests: sent } = offeringUpTo5(apiKey, apiName)

        await broker[method]({ ...ARGUMENTS[method], transactionVersion: 1 })

        expect(built.map(({ version }) => version)).toEqual([4])
        expect(sent[0]).toMatchObject({ apiKey, apiVersion: 4 })
      }
    )

    test.each(
      METHODS.flatMap(([method, apiKey, apiName]) =>
        [3, '2', true].map((value) => [method, value, apiKey, apiName])
      )
    )(
      '%s: only the number 2 means transaction version 2, so %p stops at v4',
      async (method, transactionVersion, apiKey, apiName) => {
        const { broker, built } = offeringUpTo5(apiKey, apiName)

        await broker[method]({ ...ARGUMENTS[method], transactionVersion })

        expect(built.map(({ version }) => version)).toEqual([4])
      }
    )

    test.each(METHODS)(
      '%s with no transaction version is version 1: never v5',
      async (method, apiKey, apiName) => {
        const { broker, built } = offeringUpTo5(apiKey, apiName)

        await broker[method](ARGUMENTS[method])

        expect(built.map(({ version }) => version)).toEqual([4])
      }
    )

    test.each(METHODS)(
      '%s under transaction version 2 takes the highest version on offer',
      async (method, apiKey, apiName) => {
        const { broker, built, requests: sent } = offeringUpTo5(apiKey, apiName)

        await broker[method]({ ...ARGUMENTS[method], transactionVersion: 2 })

        expect(built.map(({ version }) => version)).toEqual([5])
        expect(sent[0]).toMatchObject({ apiKey, apiVersion: 5 })
      }
    )

    test('endTxn hands the codec exactly the arguments it builds from', async () => {
      const { broker, built } = offeringUpTo5(apiKeys.EndTxn, 'EndTxn')

      await broker.endTxn({ ...ARGUMENTS.endTxn, transactionVersion: 2 })

      expect(built[0].args).toEqual(ARGUMENTS.endTxn)
    })

    test('txnOffsetCommit hands the codec the group metadata, -1/""/null when absent', async () => {
      const { broker, built } = offeringUpTo5(apiKeys.TxnOffsetCommit, 'TxnOffsetCommit')
      const metadata = { generationId: 5, memberId: 'member-a', groupInstanceId: 'instance-1' }

      await broker.txnOffsetCommit({ ...ARGUMENTS.txnOffsetCommit, ...metadata })
      await broker.txnOffsetCommit(ARGUMENTS.txnOffsetCommit)

      expect(built.map(({ args }) => args)).toEqual([
        { ...ARGUMENTS.txnOffsetCommit, ...metadata },
        { ...ARGUMENTS.txnOffsetCommit, generationId: -1, memberId: '', groupInstanceId: null },
      ])
    })
  })
})
