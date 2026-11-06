const { KafkaJSServerDoesNotSupportApiKey, KafkaJSNotImplemented } = require('../../errors')
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
  // The version factories of the two consumer-group APIs destructured their options
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

  describe('apiKeys and requests', () => {
    test.each(Object.keys(apiKeys))('%s has an entry in requests', (apiName) => {
      expect(requests[apiName]).toBeDefined()
    })

    test('key 75 resolves to DescribeTopicPartitions at v0, the only version it has', () => {
      const versions = { 75: { minVersion: 0, maxVersion: 0 } }
      const build = lookup(versions)(75, requests.DescribeTopicPartitions)
      const { request } = build({ topics: ['topic'] })

      expect(apiKeys.DescribeTopicPartitions).toBe(75)
      expect(request).toMatchObject({
        apiKey: 75,
        apiVersion: 0,
        apiName: 'DescribeTopicPartitions',
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

/** The Apache Kafka releases whose message definitions the repository keeps a snapshot of. */
const SNAPSHOT_TAGS = ['4.3.1', '4.4.0']

describe('released share protocol inventory', () => {
  const fs = require('fs')
  const path = require('path')
  const { createHash } = require('crypto')
  const tracked = require('../../../scripts/compat/tracked-apis.json').tracked
  const provenance = require('../../../scripts/protocol/provenance.json')
  const ranges = require('../../../scripts/protocol/brokerRanges.json')
  const { isFlexible, requestHeaderVersion, responseHeaderVersion } = require('../headers')
  const messages = path.resolve(__dirname, '../../../scripts/compat/messages')

  /** The 4.3.1 digest of key 76's response; its 4.4.0 file differs, the other thirteen do not. */
  const HEARTBEAT_RESPONSE_4_3_1 =
    '2fbdac30942cf96d89cc5e34d9441152ca13162ff61f3bb95ae9acd3926fbe2c'

  /**
   * One row per released share API: name, key, the one codec version this client will implement,
   * and the SHA-256 of the 4.4.0 request and response definitions.
   * @type {[string, number, number, string, string][]}
   */
  const share = [
    [
      'ShareGroupHeartbeat',
      76,
      1,
      '199cf57ffa9ccafe4ae8aa2dd35cefbf5c36a3ce69864ce08c8b5982bf25cf8f',
      'f8284acb296a502e7f16d2b0cae7bb3e0160cc8cfa0726c69dc88d1b7698255d',
    ],
    [
      'ShareGroupDescribe',
      77,
      1,
      'b363de3fb7454b17c15c9d0ad76ff0a9bc105416f32e617757077df946bbac24',
      '5bca1927fd05b7c43d55a67fcee0a3e7230c1b498195301c86931d8807014fa3',
    ],
    [
      'ShareFetch',
      78,
      2,
      '0290657f8faa79a24a9a53dab0ab949ae75095fd6cf414f2920e72c5da1723b2',
      'fa90ee102ad98ce88b14f0d38a775d0e4b32c8e53e11577a3d93923089a69f5e',
    ],
    [
      'ShareAcknowledge',
      79,
      2,
      '9f75e74cec5c35bbd5142a31e7116e981942ad194b916fafa775084764bc73d1',
      'dbfd4f0fa6c9617a6823af7434167e57d5d748fb3c5d3fd623cad671777d912b',
    ],
    [
      'DescribeShareGroupOffsets',
      90,
      1,
      'f1ff3872ed908875237c4f69a281b1fe07899837c57198b35497d343fce75021',
      '5da70be446cd41ddf1de9d2e95779d43c606113bf633d89c3d492c69e240a4cb',
    ],
    [
      'AlterShareGroupOffsets',
      91,
      0,
      '3650ba32b42a0977b0375e6376980886cb0c99dc592a9fc1e847768786e479d4',
      'f388f17dc34f89c6cfd2451adac103ccb2c58172ce445a984278b1b8ea8b7ef6',
    ],
    [
      'DeleteShareGroupOffsets',
      92,
      0,
      'a0450f46bdd8178c83d5cead0c53028f5ff282d382527fdbf53f0eaa0f39c8c6',
      'd0d1264bee6371bd39e1d7020860329ef4cb791a51196e6dd643fe5b3084b0fe',
    ],
  ]

  /**
   * @param {string} tag a snapshot tag
   * @param {string} name a share API name
   * @param {'Request'|'Response'} direction
   * @param {string} hash the 4.4.0 digest of that definition
   * @returns {string} the digest the definition must have in `tag`'s snapshot
   */
  const pinnedHash = (tag, name, direction, hash) =>
    tag === '4.3.1' && name === 'ShareGroupHeartbeat' && direction === 'Response'
      ? HEARTBEAT_RESPONSE_4_3_1
      : hash

  /** @param {string} name @returns {string} `name` with its first character lowercased */
  const lowerCamel = (name) => name.charAt(0).toLowerCase() + name.slice(1)

  test.each(share)(
    '%s has sources, a name and a slot',
    (name, key, version, requestHash, responseHash) => {
      expect(apiKeys[name]).toBe(key)
      const slot = requests[name]
      const implemented = slot.versions.map(Number)
      expect(implemented).toEqual(implemented.length ? [version] : [])
      if (implemented.length) {
        expect(slot.protocol).toEqual(expect.any(Function))
      } else {
        expect(slot).toBe(requests.LeaderAndIsr)
        expect(() => slot.protocol()).toThrow(KafkaJSNotImplemented)
      }
      expect(tracked.find((row) => row.apiKey === key)).toMatchObject({
        kafkaName: name,
        apiKeysName: name,
        forkName: implemented.length ? lowerCamel(name) : null,
        planned: implemented.length ? 'done' : 'yes',
      })
      for (const tag of SNAPSHOT_TAGS) {
        for (const [direction, hash] of [
          ['Request', requestHash],
          ['Response', responseHash],
        ]) {
          const file = path.join(messages, tag, name + direction + '.json')
          expect(createHash('sha256').update(fs.readFileSync(file)).digest('hex')).toBe(
            pinnedHash(tag, name, direction, hash)
          )
        }
      }
      expect(provenance[name]).toMatchObject({
        kafkaTag: '4.4.0',
        messageSha256: { Request: requestHash, Response: responseHash },
      })
      expect(isFlexible(key, 0)).toBe(true)
      expect(requestHeaderVersion(key, version)).toBe(2)
      expect(responseHeaderVersion(key, version)).toBe(1)
      for (const [line, table] of Object.entries(ranges)) {
        expect(table['ApiKey' + key]).toBeUndefined()
        if (line === '4.0.2') expect(table[name]).toBeUndefined()
        else expect(table[name]).toEqual([expect.any(Number), expect.any(Number)])
      }
    }
  )

  test('has no duplicate key or tracked name', () => {
    expect(new Set(Object.values(apiKeys)).size).toBe(Object.keys(apiKeys).length)
    expect(new Set(tracked.map((row) => row.apiKey)).size).toBe(tracked.length)
    expect(new Set(tracked.map((row) => row.kafkaName)).size).toBe(tracked.length)
  })
})

describe('released share source domains', () => {
  const fs = require('fs')
  const path = require('path')
  const { parseMessage } = require('../../../scripts/compat/parse')
  const ranges = require('../../../scripts/protocol/brokerRanges.json')
  const messages = path.resolve(__dirname, '../../../scripts/compat/messages')

  /** @type {[string, number, string][]} name, key and the `validVersions` of both definitions */
  const rows = [
    ['ShareGroupHeartbeat', 76, '1'],
    ['ShareGroupDescribe', 77, '1'],
    ['ShareFetch', 78, '1-2'],
    ['ShareAcknowledge', 79, '1-2'],
    ['DescribeShareGroupOffsets', 90, '0-1'],
    ['AlterShareGroupOffsets', 91, '0'],
    ['DeleteShareGroupOffsets', 92, '0'],
  ]

  const { readBrokerCapture } = require('../../../testHelpers/brokerCapture')
  const internal = [
    ['InitializeShareGroupState', 83],
    ['ReadShareGroupState', 84],
    ['WriteShareGroupState', 85],
    ['DeleteShareGroupState', 86],
    ['ReadShareGroupStateSummary', 87],
  ]

  test.each(rows)('%s pins the released source range', (name, key, validVersions) => {
    for (const tag of SNAPSHOT_TAGS) {
      for (const direction of ['Request', 'Response']) {
        const definition = parseMessage(
          fs.readFileSync(path.join(messages, tag, name + direction + '.json'), 'utf8')
        )
        expect(definition).toMatchObject({ apiKey: key, validVersions, flexibleVersions: '0+' })
        if (direction === 'Request') expect(definition.listeners).toEqual(['broker'])
      }
    }
  })

  test.each(Object.entries(ranges))(
    '%s records the share ranges by name and the internal ones by key',
    (line, table) => {
      expect(line).toMatch(/^\d+\.\d+\.\d+$/)
      expect(table).toEqual(expect.any(Object))
      expect(Array.isArray(table)).toBe(false)
      for (const range of Object.values(table)) {
        expect(range).toHaveLength(2)
        expect(range.every((n) => Number.isInteger(n) && n >= 0)).toBe(true)
        expect(range[0]).toBeLessThanOrEqual(range[1])
      }

      const { apiRanges } = readBrokerCapture(line)
      for (const [name, key] of rows) {
        expect(Object.hasOwn(table, 'ApiKey' + key)).toBe(false)
        expect(Object.hasOwn(table, name)).toBe(Object.hasOwn(apiRanges, key))
        expect(table[name]).toEqual(apiRanges[key])
      }
      for (const [name, key] of internal) {
        expect(Object.hasOwn(table, name)).toBe(false)
        expect(Object.hasOwn(table, 'ApiKey' + key)).toBe(Object.hasOwn(apiRanges, key))
        expect(table['ApiKey' + key]).toEqual(apiRanges[key])
      }
    }
  )
})
