'use strict'

const crypto = require('crypto')
const fs = require('fs')
const path = require('path')
const apiKeys = require('../../../src/protocol/requests/apiKeys')
const { requests } = require('../../../src/protocol/requests')
const { buildModel } = require('../../compat/build')
const { readSpec } = require('../../compat/parse')
const notes = require('../../compat/notes.json')
const trackedApis = require('../../compat/tracked-apis.json')

const SNAPSHOT = path.join(__dirname, '..', '..', 'compat', 'messages', '4.3.1')
const PROVENANCE = path.join(__dirname, '..', 'provenance.json')
const BROKER_RANGES = path.join(__dirname, '..', 'brokerRanges.json')
const LINES = ['4.0.2', '4.1.2', '4.2.2', '4.3.1']
/** Tracked APIs whose message definitions first ship after the 4.3.1 snapshot. */
const ADDED_AFTER_4_3_1 = ['UnregisterController']

/**
 * The Apache Kafka 4.3.1 definitions of the admin APIs the client adds, read from the SHA-512
 * checked source release: name, key, valid versions, first flexible version and the SHA-256 of
 * the request and the response file.
 * @type {[string, number, string, number|null, string, string][]}
 */
const LEDGER = [
  [
    'WriteTxnMarkers',
    27,
    '1-2',
    1,
    '3845cb619a6da7b3d9ca8588d3f23e9a7adf6bf8fef3fe64ddc96013e49ac0f9',
    '0dc0ffd22775d3f05dc9a588ef8541d8e938651431350b75890d7fccb304af39',
  ],
  [
    'AlterReplicaLogDirs',
    34,
    '1-2',
    2,
    '36b2407c1b6964bf1a8b5e405fe317844c0f00d8e946efeaa3fb3df858ab373d',
    '23ecf9485b51be6433378e8ca82d2ff2d8db2255e2815fb68d7328eab8e07be1',
  ],
  [
    'DescribeLogDirs',
    35,
    '1-5',
    2,
    '63ba440915c66d6ed6f2e17ef4aac5d2eb36e1f14bc6bf9d3129b3ce6c99f218',
    '4e3b47c05309d177a004e11f2e317a65393294106e7074ae65f70921c29ebe53',
  ],
  [
    'CreateDelegationToken',
    38,
    '1-3',
    2,
    '159c3ed2d3d9e7f2ad4df8ece1783184ffe0b19de176e9a67a4ff76c90f36093',
    'f42e46e7433dd594e9a837e805f9cde1c90a63c86c6a05d28f541c4cac3f084e',
  ],
  [
    'RenewDelegationToken',
    39,
    '1-2',
    2,
    'c87758d9a4f7abf1e7e457107ca0d84e22770c7b5aadf35b4f33b07ee847e544',
    '30d8490c8fcc122a11490a33949d24cdcbf1367ba6b3d9c04e5a4241c8b38842',
  ],
  [
    'ExpireDelegationToken',
    40,
    '1-2',
    2,
    'a7f118857c35f3e6cf95b3b81eee31e13080ba92345b3016aa7525f6e70f9c3b',
    'e0e3e4c82e5cd796480b95b6dc79d691e8994fae89c77699d09fb8b351a322e7',
  ],
  [
    'DescribeDelegationToken',
    41,
    '1-3',
    2,
    '22fe3781b361acbd3951b31a4dd7f7db15f71315eb8b934404b8e76bea5b825b',
    '3306725d50bbca00ba4fad6d7e0dc8fff284ac7dcaf231e5d05398a4afa4b0b9',
  ],
  [
    'ElectLeaders',
    43,
    '0-2',
    2,
    '3048a2c953cd8e1a3343269c6eb827070471c18a5e62f4232dbc247768d1b3f5',
    'b47545dec74fc336fe46b0ad6d6ad416b8b4ff032d564cd23b2bf83ad093c400',
  ],
  [
    'OffsetDelete',
    47,
    '0-0',
    null,
    '9508470c5727082e6212e8ca43e6966a79874495ead10cf724824b5934ee2a39',
    'ea99e5b080ad9ed6b56d19e9e749b9a4e5c37dbcd6aa69a663e97ff9fc7a1656',
  ],
  [
    'DescribeClientQuotas',
    48,
    '0-1',
    1,
    '16451280b34bfdb2dc706ee793ff85ab9553d10b1f57268652bcb244d0a9c270',
    '8835639bb0223508cdaa9b0eded648e5394c5311276995ff401ea9b58514a100',
  ],
  [
    'AlterClientQuotas',
    49,
    '0-1',
    1,
    '195bd3af15a5d91037d3394db88d433c9c62985e04a896310fe4074638cc3526',
    'db655393cbc6485bf10575f0dc4d29b3c47883a16a4162d190c74f4000cb6b6a',
  ],
  [
    'DescribeUserScramCredentials',
    50,
    '0-0',
    0,
    'e3551378a9f2d59fb65de32641f79f40ea5a9b5a554ee668ed06d182c5064323',
    '2ead5ef7c1fc92546da6c50bc703e0ed13eee56ae108dfc76857c9069b6cde8b',
  ],
  [
    'AlterUserScramCredentials',
    51,
    '0-0',
    0,
    'fcf7ca27e8028316c0957b346c93108c235388d06f829f7ecc5a304407719d26',
    'f9bba249467a5f499ae10523be22f96aab4ddc6bdff0f54f7cf0be030820ff2d',
  ],
  [
    'UpdateFeatures',
    57,
    '0-2',
    0,
    'afb94b3e2a0a6f14f7535128f61ecb00e4498f0b1a0820cd9740ae4f8a384a03',
    '789762f475eafb2fa9823146f9368d7b8299687eb1e02a8a5493f2936561f7c4',
  ],
  [
    'DescribeCluster',
    60,
    '0-2',
    0,
    '821b86c736d98bdd3de825daa56edd30fb46d39b8a156f273f3985d0d09c00b1',
    '04a23344c659434c30914a76f161d6705d2028864800e735b20fe85bb8efd407',
  ],
  [
    'DescribeProducers',
    61,
    '0-0',
    0,
    'b7db1a8154ace225ca9eb3a35e78d038baeee567934a0a74265fce1ae4289fcd',
    '6019232d4144a18aed721cc50724304f4d6e260680ba0c1f373cf25346deb3b0',
  ],
  [
    'DescribeTransactions',
    65,
    '0-0',
    0,
    'c0a31e10bc7525cd0d61a49e4035a8ab67ae49387d20db61205dc4a060f07769',
    'd14d889ed117bb0c2d5c2a39b3937f6df997abc443a5ad7ac754d4112ec6aeae',
  ],
  [
    'ListTransactions',
    66,
    '0-2',
    0,
    'b54931d4ab03cf2a6da501737b5e59709447448e680e54037a741b824bb94176',
    '154abac86cb57d4e87827194d890efb5a01deb16970e31476bbd4cc86686abed',
  ],
  [
    'ListConfigResources',
    74,
    '0-1',
    0,
    '3b8a841835ddd392cabb39c4e930f5fc3b7f0c74d8353311f729bee52e9b4f21',
    'e41bf574f16f52df3b8b5de1013ad3cb93378c4d9d3f77c62fc26f6a7511ceed',
  ],
]

/**
 * The `[min, max]` range each broker line offers, in the order of `LINES`, as the live probes
 * recorded it before the entries were renamed from `ApiKey<n>`.
 * @type {Record<string, [number, number][]>}
 */
const OFFERED = {
  WriteTxnMarkers: [
    [1, 1],
    [1, 1],
    [1, 2],
    [1, 2],
  ],
  AlterReplicaLogDirs: [
    [1, 2],
    [1, 2],
    [1, 2],
    [1, 2],
  ],
  DescribeLogDirs: [
    [1, 4],
    [1, 4],
    [1, 4],
    [1, 5],
  ],
  CreateDelegationToken: [
    [1, 3],
    [1, 3],
    [1, 3],
    [1, 3],
  ],
  RenewDelegationToken: [
    [1, 2],
    [1, 2],
    [1, 2],
    [1, 2],
  ],
  ExpireDelegationToken: [
    [1, 2],
    [1, 2],
    [1, 2],
    [1, 2],
  ],
  DescribeDelegationToken: [
    [1, 3],
    [1, 3],
    [1, 3],
    [1, 3],
  ],
  ElectLeaders: [
    [0, 2],
    [0, 2],
    [0, 2],
    [0, 2],
  ],
  OffsetDelete: [
    [0, 0],
    [0, 0],
    [0, 0],
    [0, 0],
  ],
  DescribeClientQuotas: [
    [0, 1],
    [0, 1],
    [0, 1],
    [0, 1],
  ],
  AlterClientQuotas: [
    [0, 1],
    [0, 1],
    [0, 1],
    [0, 1],
  ],
  DescribeUserScramCredentials: [
    [0, 0],
    [0, 0],
    [0, 0],
    [0, 0],
  ],
  AlterUserScramCredentials: [
    [0, 0],
    [0, 0],
    [0, 0],
    [0, 0],
  ],
  UpdateFeatures: [
    [0, 2],
    [0, 2],
    [0, 2],
    [0, 2],
  ],
  DescribeCluster: [
    [0, 2],
    [0, 2],
    [0, 2],
    [0, 2],
  ],
  DescribeProducers: [
    [0, 0],
    [0, 0],
    [0, 0],
    [0, 0],
  ],
  DescribeTransactions: [
    [0, 0],
    [0, 0],
    [0, 0],
    [0, 0],
  ],
  ListTransactions: [
    [0, 1],
    [0, 2],
    [0, 2],
    [0, 2],
  ],
  ListConfigResources: [
    [0, 0],
    [0, 1],
    [0, 1],
    [0, 1],
  ],
}

const LEDGER_KEYS = new Set(LEDGER.map(([, apiKey]) => apiKey))

/**
 * @param {string} file
 * @returns {string} the SHA-256 of the file's bytes, in hex
 */
const sha256 = (file) => crypto.createHash('sha256').update(fs.readFileSync(file)).digest('hex')

/** @param {string} file @returns {any} the parsed JSON file */
const readJson = (file) => JSON.parse(fs.readFileSync(file, 'utf8'))

/** @param {string} name @returns {string} `name` with its first character lowercased */
const lowerCamel = (name) => name.charAt(0).toLowerCase() + name.slice(1)

/**
 * @param {string} name an Apache message name
 * @returns {object} its tracked entry
 * @throws {Error} when the tracked list has none
 */
const trackedOf = (name) => {
  const entry = trackedApis.tracked.find((candidate) => candidate.kafkaName === name)
  if (entry === undefined) throw new Error(`${name} is not in the tracked list`)
  return entry
}

describe('scripts/protocol > admin ledger APIs', () => {
  describe('the Apache 4.3.1 definitions', () => {
    test.each(LEDGER)(
      '%s: both definitions are the pinned bytes and name this key and these versions',
      (name, apiKey, valid, flexibleFrom, requestSha, responseSha) => {
        const [min, max] = valid.split('-').map(Number)
        const request = path.join(SNAPSHOT, `${name}Request.json`)
        const response = path.join(SNAPSHOT, `${name}Response.json`)

        expect(sha256(request)).toBe(requestSha)
        expect(sha256(response)).toBe(responseSha)
        expect(readSpec(request)).toMatchObject({
          name: `${name}Request`,
          type: 'request',
          apiKey,
          validVersions: { min, max },
          flexibleFrom,
        })
        expect(readSpec(response)).toMatchObject({
          name: `${name}Response`,
          type: 'response',
          apiKey,
          validVersions: { min, max },
          flexibleFrom,
        })
      }
    )

    test('the snapshot holds a request and a response for each tracked API, the two headers and nothing else', () => {
      const expected = [
        'RequestHeader.json',
        'ResponseHeader.json',
        ...trackedApis.tracked
          .filter(({ kafkaName }) => !ADDED_AFTER_4_3_1.includes(kafkaName))
          .flatMap(({ kafkaName }) => [`${kafkaName}Request.json`, `${kafkaName}Response.json`]),
      ].sort()

      expect(fs.readdirSync(SNAPSHOT).sort()).toEqual(expected)
    })
  })

  describe('the tracked list and the request table', () => {
    test.each(LEDGER)(
      '%s: tracked once as key %i, planned until its codec lands',
      (name, apiKey) => {
        const entry = trackedOf(name)

        expect(trackedApis.tracked.filter((candidate) => candidate.apiKey === apiKey)).toHaveLength(
          1
        )
        expect(entry).toMatchObject({ apiKey, apiKeysName: name })
        expect(['yes', 'done']).toContain(entry.planned)
        expect(entry.forkName).toBe(entry.planned === 'done' ? lowerCamel(name) : null)
      }
    )

    test('only the four controller-to-broker keys stay untracked', () => {
      const keys = [...trackedApis.tracked, ...trackedApis.untracked].map(({ apiKey }) => apiKey)

      expect(trackedApis.untracked.map(({ apiKey }) => apiKey)).toEqual([4, 5, 6, 7])
      expect(new Set(keys).size).toBe(keys.length)
    })

    test.each(LEDGER)('%s: apiKeys.js names key %i', (name, apiKey) => {
      expect(apiKeys[name]).toBe(apiKey)
    })

    test.each(LEDGER)('%s: the request slot is a stub until its codec lands', (name) => {
      const definition = requests[name]

      expect(definition).toBeDefined()
      if (trackedOf(name).planned === 'done') {
        expect(definition.versions.length).toBeGreaterThan(0)
      } else {
        expect(definition.versions).toEqual([])
      }
    })

    test('apiKeys.js and the tracked list agree on the key of every API they both name', () => {
      for (const { apiKey, apiKeysName } of trackedApis.tracked.filter((e) => e.apiKeysName)) {
        expect(apiKeys[apiKeysName]).toBe(apiKey)
      }
    })
  })

  describe('the generated matrix model', () => {
    let model
    beforeAll(() => {
      model = buildModel()
    })

    test.each(LEDGER)(
      '%s: classified as a stub while planned, with the Apache versions',
      (name, apiKey, valid, flexibleFrom) => {
        const api = model.apis.find((candidate) => candidate.kafkaName === name)
        const [min, max] = valid.split('-').map(Number)

        expect(api).toMatchObject({ apiKey, validVersions: [min, max], flexibleFrom })
        if (trackedOf(name).planned === 'yes') expect(api.gap).toBe('stub')
        else expect(api.gap).not.toBe('stub')
      }
    )
  })

  describe('the provenance record', () => {
    /**
     * The one ledger definition the 4.4.0 release changed (its Topics.Name became a map key),
     * by the SHA-256 of the 4.4.0 file in the SHA-512 checked source release.
     */
    const CHANGED_IN_4_4_0 = {
      DescribeProducers: {
        Request: 'd6c4a35f9af9e5c88c96a0d9edf8f1bb247d7a8049cdd5e8b83b215fbe3c385b',
      },
    }

    test.each(LEDGER)(
      '%s: records the tag and the SHA-256 of both definitions',
      (name, apiKey, valid, flexibleFrom, requestSha, responseSha) => {
        const entry = readJson(PROVENANCE)[name]

        expect(entry).toBeDefined()
        const { javaClientsJar, javaVectors, ...recorded } = entry
        expect(recorded).toEqual({
          kafkaTag: '4.4.0',
          messageSha256: {
            Request: requestSha,
            Response: responseSha,
            ...CHANGED_IN_4_4_0[name],
          },
        })
        expect([null, '4.3.1']).toContain(javaClientsJar)
        expect(javaVectors.every((vector) => vector.javaClientsJar === javaClientsJar)).toBe(true)
      }
    )
  })

  describe('the broker ranges', () => {
    const table = () => readJson(BROKER_RANGES)

    test('records the floor line and the three lines the suite runs on', () => {
      expect(Object.keys(table())).toEqual(LINES)
    })

    test.each(LEDGER)('%s: every line records its range under the API name', (name) => {
      LINES.forEach((line, index) => {
        expect(table()[line][name]).toEqual(OFFERED[name][index])
      })
    })

    test('no ledger key is left recorded as ApiKey<n>', () => {
      for (const line of LINES) {
        const left = Object.keys(table()[line])
          .map((key) => /^ApiKey(\d+)$/.exec(key))
          .filter((found) => found && LEDGER_KEYS.has(Number(found[1])))

        expect(left.map((found) => found[0])).toEqual([])
      }
    })

    test('each line stays in ascending key order', () => {
      const keyOf = new Map()
      for (const file of fs.readdirSync(SNAPSHOT).filter((f) => f.endsWith('Request.json'))) {
        const { apiKey } = readSpec(path.join(SNAPSHOT, file))
        if (apiKey !== null) keyOf.set(file.replace('Request.json', ''), apiKey)
      }
      for (const [name, apiKey] of Object.entries(apiKeys)) keyOf.set(name, apiKey)

      for (const line of LINES) {
        const keys = Object.keys(table()[line]).map(
          (name) => keyOf.get(name) ?? Number(/^ApiKey(\d+)$/.exec(name)[1])
        )

        expect(keys).toEqual([...keys].sort((a, b) => a - b))
      }
    })
  })

  describe('the notes', () => {
    test('WriteTxnMarkers is also sent by abortTransaction', () => {
      expect(notes.apis.WriteTxnMarkers.note).toMatch(/also sent by `abortTransaction`/)
    })

    test('no ListOffsets version is called a deliberate gap', () => {
      expect(notes.apis.ListOffsets.note).not.toMatch(/deliberate/)
    })

    test('ListConfigResources and ListOffsets v10 and v11 read as not yet implemented until they land', () => {
      if (trackedOf('ListConfigResources').planned === 'yes') {
        expect(notes.apis.ListConfigResources.note).toMatch(/not yet implemented/i)
      }
      if (Math.max(...requests.ListOffsets.versions.map(Number)) < 11) {
        expect(notes.apis.ListOffsets.note).toMatch(/v10 .* and v11 .* are not yet implemented/)
      }
    })
  })
})
