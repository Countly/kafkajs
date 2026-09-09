const { KafkaJSServerDoesNotSupportApiKey } = require('../../errors')
const apiKeys = require('./apiKeys')
const { lookup, requests } = require('./index')

describe('Protocol > Requests', () => {
  describe('requests', () => {
    Object.entries(requests).forEach(([apiName, impls]) => {
      if (!impls.versions) {
        return
      }

      impls.versions.forEach((version) => {
        test(`${apiName} > v${version} > metadata`, () => {
          const { request } = impls.protocol({ version })({})
          expect(request.apiKey).toEqual(apiKeys[apiName])
          expect(request.apiVersion).toEqual(Number(version))
          expect(request.apiName).toEqual(apiName)
        })
      })
    })
  })
  // SF-20: the version factories of the two group APIs added in 6B destructured their options
  // object with no default, so a shape probe - reading an api key, name and version without
  // building a real request - threw where `ListGroups` returns.
  describe('a shape-only request, built with no options', () => {
    const PROBEABLE = ['ConsumerGroupHeartbeat', 'ConsumerGroupDescribe', 'ListGroups']

    PROBEABLE.forEach((apiName) => {
      requests[apiName].versions.forEach((version) => {
        test(`${apiName} > v${version}`, () => {
          const { request } = requests[apiName].protocol({ version })()

          expect(request.apiKey).toEqual(apiKeys[apiName])
          expect(request.apiVersion).toEqual(Number(version))
          expect(request.apiName).toEqual(apiName)
        })
      })
    })
  })

  describe('lookup', () => {
    describe('when the client support more versions than the server', () => {
      it('returns the maximum version supported by the server', async () => {
        const apiKey = 1
        const protocol = jest.fn(() => true)

        // versions supported by the server
        const versions = { [apiKey]: { minVersion: 0, maxVersion: 1 } }

        // versions supported by the client
        const definition = { versions: [0, 1, 2], protocol }

        expect(lookup(versions)(apiKey, definition)).toEqual(true)
        expect(protocol).toHaveBeenCalledWith({ version: 1 })
      })
    })

    describe('when the server support more versions than the client', () => {
      it('returns the maximum version supported by the client', () => {
        const apiKey = 1
        const protocol = jest.fn(() => true)

        // versions supported by the server
        const versions = { [apiKey]: { minVersion: 1, maxVersion: 3 } }

        // versions supported by the client
        const definition = { versions: [0, 1, 2], protocol }

        expect(lookup(versions)(apiKey, definition)).toEqual(true)
        expect(protocol).toHaveBeenCalledWith({ version: 2 })
      })
    })

    describe('when the server does not support the requested version', () => {
      it('throws KafkaJSServerDoesNotSupportApiKey', () => {
        // versions supported by the server
        const versions = { 1: { minVersion: 1, maxVersion: 3 } }

        // versions supported by the client
        const protocol = jest.fn(() => true)
        const definition = { versions: [0, 1, 2], protocol }

        const apiKeyNotSupportedByTheServer = 34
        expect(() => lookup(versions)(apiKeyNotSupportedByTheServer, definition)).toThrow(
          KafkaJSServerDoesNotSupportApiKey
        )
      })
    })

    describe('when the broker minimum version is above every version the client implements', () => {
      it('throws KafkaJSServerDoesNotSupportApiKey naming both ranges', () => {
        const apiKey = apiKeys.Produce
        const protocol = jest.fn(() => true)

        // A future broker that dropped Produce v0-v8 (KIP-896 style floor raise)
        const versions = { [apiKey]: { minVersion: 9, maxVersion: 13 } }
        const definition = { versions: ['3', '4', '5', '6', '7'], protocol }

        expect(() => lookup(versions)(apiKey, definition)).toThrow(
          KafkaJSServerDoesNotSupportApiKey
        )
        expect(() => lookup(versions)(apiKey, definition)).toThrow(
          /Produce.*client implements v3-v7.*broker supports v9-v13/
        )
        expect(protocol).not.toHaveBeenCalled()
      })
    })

    describe('when the broker maximum version is below every version the client implements', () => {
      it('throws KafkaJSServerDoesNotSupportApiKey instead of selecting an unimplemented version', () => {
        const apiKey = apiKeys.Fetch
        const protocol = jest.fn(() => true)

        const versions = { [apiKey]: { minVersion: 0, maxVersion: 3 } }
        const definition = { versions: ['4', '5', '6'], protocol }

        expect(() => lookup(versions)(apiKey, definition)).toThrow(
          KafkaJSServerDoesNotSupportApiKey
        )
        expect(protocol).not.toHaveBeenCalled()
      })
    })

    describe('against the real Kafka 4.x version table', () => {
      // Kafka 4.3 ranges, as recorded in docs/KafkaCompatibility.md (generated from the apache/kafka 4.3 message JSON)
      const KAFKA_4_3 = {
        [apiKeys.Produce]: { minVersion: 3, maxVersion: 13 },
        [apiKeys.Fetch]: { minVersion: 4, maxVersion: 18 },
        [apiKeys.ListOffsets]: { minVersion: 1, maxVersion: 11 },
        [apiKeys.OffsetCommit]: { minVersion: 2, maxVersion: 10 },
        [apiKeys.CreateTopics]: { minVersion: 2, maxVersion: 7 },
      }

      it.each(Object.keys(KAFKA_4_3).map(Number))('resolves api key %i', (apiKey) => {
        const apiName = Object.keys(apiKeys).find((name) => apiKeys[name] === apiKey)
        expect(() => lookup(KAFKA_4_3)(apiKey, requests[apiName])).not.toThrow()
      })
    })

    describe('lookup with an internal version cap', () => {
      const versions = { 3: { minVersion: 0, maxVersion: 13 } }
      const definition = { versions: ['0', '6', '12'], protocol: ({ version }) => ({ version }) }

      test('caps the negotiated version for one API', () => {
        expect(lookup(versions, { 3: 6 })(3, definition)).toEqual({ version: 6 })
      })

      test('leaves other APIs alone', () => {
        expect(lookup(versions, { 18: 2 })(3, definition)).toEqual({ version: 12 })
      })

      test('a cap above the negotiated version changes nothing', () => {
        expect(lookup(versions, { 3: 99 })(3, definition)).toEqual({ version: 12 })
      })

      test('a cap below every implemented version is rejected like any other non-overlap', () => {
        expect(() => lookup(versions, { 3: -1 })(3, definition)).toThrow(
          KafkaJSServerDoesNotSupportApiKey
        )
      })

      test('no cap behaves exactly as before', () => {
        expect(lookup(versions)(3, definition)).toEqual({ version: 12 })
      })
    })
  })
})
