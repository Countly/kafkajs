const setupPool = require('../../../testHelpers/setupPool')
const Broker = require('../index')
const apiKeys = require('../../protocol/requests/apiKeys')
const { requests, lookup, selectVersion } = require('../../protocol/requests')
const { newLogger } = require('testHelpers')

const versions = {
  [apiKeys.Fetch]: { minVersion: 4, maxVersion: 18 },
  [apiKeys.Produce]: { minVersion: 3, maxVersion: 13 },
  [apiKeys.JoinGroup]: { minVersion: 0, maxVersion: 9 },
}

/**
 * A definition shaped like `requests.Fetch` (a `versions` list plus a `protocol({ version })`
 * lookup) but reaching v18, the ceiling this client eventually implements once the Fetch codec
 * catches up. The cap-selection tests below use this instead of the real `requests.Fetch` so they
 * exercise `selectVersion`'s and `lookupRequestWithMax`'s arithmetic rather than today's Fetch
 * codec inventory, which currently tops out below the caps these tests exercise. Builders are
 * memoised per version, like the real codec's version table, so two calls for the same version
 * return the same reference and two calls for different versions do not - the property the
 * `lookupRequestWithMax` cache-invalidation test below relies on.
 */
const wideFetchVersions = Array.from({ length: 15 }, (_, i) => i + 4) // 4..18
const wideFetchBuilders = wideFetchVersions.reduce(
  (builders, version) =>
    Object.assign(builders, {
      [version]: (requestArgs) => ({ request: { apiVersion: version, ...requestArgs } }),
    }),
  {}
)
const wideFetchLikeDefinition = {
  versions: wideFetchVersions.map(String),
  protocol: ({ version }) => wideFetchBuilders[version],
}

/** A Broker with a stub pool: nothing here sends, only version selection is exercised. */
const createBroker = (brokerVersions = versions) => {
  const connectionPool = {
    host: '127.0.0.1',
    port: 9092,
    sasl: null,
    generation: 0,
    connectionTimeout: 1000,
    isConnected: () => true,
    isAuthenticated: () => true,
    setVersions: () => {},
    destroy: async () => {},
  }
  const broker = new Broker({
    connectionPool: setupPool(connectionPool),
    logger: newLogger(),
    versions: brokerVersions,
  })
  broker.lookupRequest = lookup(brokerVersions)
  broker.versionsGeneration = 0
  return broker
}

describe('Broker > capped lookup and negotiated version', () => {
  describe('selectVersion', () => {
    // A fabricated definition, so the expected number is a constant rather than a restatement of
    // the implementation - and so this case keeps meaning the same thing as Fetch's own
    // implemented range grows later in this release.
    const definition = { versions: ['4', '5', '6', '7', '8'], protocol: ({ version }) => version }

    test('returns the highest version both sides implement', () => {
      expect(
        selectVersion(
          { [apiKeys.Fetch]: { minVersion: 4, maxVersion: 6 } },
          apiKeys.Fetch,
          definition
        )
      ).toEqual(6)
      expect(
        selectVersion(
          { [apiKeys.Fetch]: { minVersion: 0, maxVersion: 18 } },
          apiKeys.Fetch,
          definition
        )
      ).toEqual(8)
    })

    test('honours a per-api ceiling', () => {
      // wideFetchLikeDefinition, not the real requests.Fetch: see the comment on its declaration
      // above. The real codec's own inventory is exercised separately below.
      expect(
        selectVersion(versions, apiKeys.Fetch, wideFetchLikeDefinition, { [apiKeys.Fetch]: 12 })
      ).toEqual(12)
    })

    test('returns an implemented version, not the broker maximum, when the set has a hole', () => {
      // A definition whose implemented versions skip 6 and 7. min(best, brokerMax) would answer 7,
      // for which `protocol({ version })` has no builder.
      const holed = {
        versions: ['4', '5', '8'],
        protocol: ({ version }) => version,
      }

      expect(
        selectVersion({ [apiKeys.Fetch]: { minVersion: 4, maxVersion: 7 } }, apiKeys.Fetch, holed)
      ).toEqual(5)
      expect(holed.protocol({ version: 5 })).toBeDefined()
    })

    test('throws when the broker advertises nothing usable', () => {
      expect(() => selectVersion({}, apiKeys.Fetch, requests.Fetch)).toThrow(
        'The Kafka server does not support the requested API version'
      )
    })

    test('lookup() and selectVersion() agree, so there is one rule and not two', () => {
      const built = lookup(versions)(apiKeys.Produce, requests.Produce)({
        acks: -1,
        timeout: 30000,
        topicData: [],
      })
      expect(built.request.apiVersion).toEqual(
        selectVersion(versions, apiKeys.Produce, requests.Produce)
      )
    })
  })

  describe('negotiatedVersion', () => {
    // Same fabricated definition as above, so the expected number is a constant rather than a
    // restatement of the implementation.
    const definition = { versions: ['4', '5', '6', '7', '8'], protocol: ({ version }) => version }

    test('reports the number, and null instead of throwing for an unadvertised api', () => {
      const broker = createBroker({ [apiKeys.JoinGroup]: { minVersion: 0, maxVersion: 6 } })

      expect(broker.negotiatedVersion(apiKeys.JoinGroup, definition)).toEqual(6)
      expect(
        broker.negotiatedVersion(apiKeys.OffsetForLeaderEpoch, requests.OffsetForLeaderEpoch)
      ).toBeNull()
    })

    // Deliberately against the real `requests.Fetch`, unlike the cap tests above: this exercises
    // `negotiatedVersion`'s wiring to the actual codec rather than a stand-in. The expected values
    // restate `selectVersion`'s own min-of-both-sides arithmetic against `requests.Fetch.versions`
    // instead of pinning today's implemented ceiling as a literal number, so this stays true
    // however many versions the codec implements - including after a later task extends Fetch
    // toward v18.
    test('answers null before the broker has negotiated anything', () => {
      const broker = createBroker()
      broker.versions = null

      // Documented as total: a caller reads null as "not available here" and degrades.
      expect(broker.negotiatedVersion(apiKeys.Fetch, requests.Fetch)).toBeNull()
    })

    test('tracks the real Fetch codec inventory, uncapped and capped', () => {
      const broker = createBroker()
      const brokerMax = versions[apiKeys.Fetch].maxVersion
      const bestImplemented = Math.max(...requests.Fetch.versions.map(Number))
      const cap = 12

      expect(broker.negotiatedVersion(apiKeys.Fetch, requests.Fetch)).toEqual(
        Math.min(bestImplemented, brokerMax)
      )
      expect(
        broker.negotiatedVersion(apiKeys.Fetch, requests.Fetch, { [apiKeys.Fetch]: cap })
      ).toEqual(Math.min(Math.min(bestImplemented, cap), brokerMax))
    })
  })

  describe('lookupRequestWithMax', () => {
    test('an undefined cap returns the uncapped builder', () => {
      const broker = createBroker()
      expect(broker.lookupRequestWithMax(apiKeys.Fetch, requests.Fetch, undefined)).toBe(
        broker.lookupRequest(apiKeys.Fetch, requests.Fetch)
      )
    })

    test('a cap selects the capped version and is memoised per (apiKey, cap)', () => {
      // wideFetchLikeDefinition, not the real requests.Fetch: see the comment on its declaration
      // above.
      const broker = createBroker()
      const first = broker.lookupRequestWithMax(apiKeys.Fetch, wideFetchLikeDefinition, 12)
      const second = broker.lookupRequestWithMax(apiKeys.Fetch, wideFetchLikeDefinition, 12)

      expect(first).toBe(second)
      expect(
        first({ maxWaitTime: 100, minBytes: 1, maxBytes: 1024, topics: [] }).request.apiVersion
      ).toEqual(12)
    })

    test('the memo does not outlive its version table', async () => {
      const broker = createBroker()
      const capped = broker.lookupRequestWithMax(apiKeys.Fetch, wideFetchLikeDefinition, 12)

      broker.replaceConnectionPool(
        setupPool({
          host: '127.0.0.1',
          port: 9092,
          sasl: null,
          generation: 0,
          connectionTimeout: 1000,
          continueGenerationFrom: () => {},
          isConnected: () => true,
          isAuthenticated: () => true,
          setVersions: () => {},
          destroy: async () => {},
        })
      )

      // A different table, as a re-negotiation would produce.
      broker.versions = { [apiKeys.Fetch]: { minVersion: 4, maxVersion: 11 } }
      broker.lookupRequest = lookup(broker.versions)
      broker.versionsGeneration = 0

      const rebuilt = broker.lookupRequestWithMax(apiKeys.Fetch, wideFetchLikeDefinition, 12)
      expect(rebuilt).not.toBe(capped)
      expect(
        rebuilt({ maxWaitTime: 100, minBytes: 1, maxBytes: 1024, topics: [] }).request.apiVersion
      ).toEqual(11)
    })
  })

  describe('the fetch cap defends itself', () => {
    /**
     * A `Broker` whose pool records the version of every request it is asked to send and answers
     * with an empty decoded body, so a case can assert which version the selection rule picked
     * without a socket. Built on this file's `createBroker` stub pool.
     * @param {import("../../../types").ApiVersions} versions the broker's advertised ranges
     * @returns {{ broker: import('../index'), sentVersions: {[apiKey: number]: number} }}
     */
    const createRecordingBroker = (versions) => {
      const sentVersions = {}
      const connectionPool = {
        host: '127.0.0.1',
        port: 9092,
        sasl: null,
        generation: 0,
        connectionTimeout: 1000,
        isConnected: () => true,
        isAuthenticated: () => true,
        setVersions: () => {},
        destroy: async () => {},
        getConnectionByRequest: async () => ({ isConnected: () => true }),
        send: async (protocolRequest) => {
          sentVersions[protocolRequest.request.apiKey] = protocolRequest.request.apiVersion
          // Nothing decodes here: the assertion is about the version that was selected, and a real
          // response would need a version-specific fixture per case.
          return { responses: [], topics: [] }
        },
      }

      const broker = new Broker({
        connectionPool: setupPool(connectionPool),
        logger: newLogger(),
        versions,
      })
      broker.lookupRequest = lookup(versions)
      broker.versionsGeneration = 0
      return { broker, sentVersions }
    }

    test('a fetch with no topicIds is sent at v12 against a broker advertising 18', async () => {
      const { broker, sentVersions } = createRecordingBroker({
        [apiKeys.Fetch]: { minVersion: 4, maxVersion: 18 },
      })

      await broker.fetch({
        maxWaitTime: 100,
        minBytes: 1,
        maxBytes: 1024,
        topics: [
          { topic: 'test-topic', partitions: [{ partition: 0, fetchOffset: '0', maxBytes: 1024 }] },
        ],
      })

      expect(sentVersions[apiKeys.Fetch]).toEqual(12)
    })

    test('an explicit lower maxVersion still wins over the cap', async () => {
      const { broker, sentVersions } = createRecordingBroker({
        [apiKeys.Fetch]: { minVersion: 4, maxVersion: 18 },
      })

      await broker.fetch({
        maxWaitTime: 100,
        minBytes: 1,
        maxBytes: 1024,
        maxVersion: 11,
        topics: [
          { topic: 'test-topic', partitions: [{ partition: 0, fetchOffset: '0', maxBytes: 1024 }] },
        ],
      })

      expect(sentVersions[apiKeys.Fetch]).toEqual(11)
    })

    test('a produce with no topicIds is sent at v11 against a broker advertising 13', async () => {
      const { broker, sentVersions } = createRecordingBroker({
        [apiKeys.Produce]: { minVersion: 3, maxVersion: 13 },
      })

      await broker.produce({
        acks: -1,
        timeout: 30000,
        topicData: [
          {
            topic: 'test-topic',
            partitions: [
              {
                partition: 0,
                firstSequence: 0,
                messages: [{ value: 'a', timestamp: 1700000000000 }],
              },
            ],
          },
        ],
      })

      expect(sentVersions[apiKeys.Produce]).toEqual(11)
    })

    test('a transactional produce is sent at v11 even when every topic has an id', async () => {
      const { broker, sentVersions } = createRecordingBroker({
        [apiKeys.Produce]: { minVersion: 3, maxVersion: 13 },
      })

      await broker.produce({
        acks: -1,
        timeout: 30000,
        transactionalId: 'transactional-id',
        producerId: '1000',
        producerEpoch: 0,
        topicIds: new Map([['test-topic', '01020304-0506-0708-090a-0b0c0d0e0f10']]),
        topicData: [
          {
            topic: 'test-topic',
            partitions: [
              {
                partition: 0,
                firstSequence: 0,
                messages: [{ value: 'a', timestamp: 1700000000000 }],
              },
            ],
          },
        ],
      })

      // v12 and up imply adding the partition to the transaction, which this client does not do.
      expect(sentVersions[apiKeys.Produce]).toEqual(11)
    })
  })
})
