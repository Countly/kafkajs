const fs = require('fs')
const path = require('path')
const Encoder = require('../../src/protocol/encoder')
const expected = require('../wire/expected')

const REQUESTS = path.join(__dirname, '..', '..', 'src', 'protocol', 'requests')

/** @returns {Buffer} the bytes of a vector: its `hex`, or the `data` of a bytes-only one */
const bytesOf = (vector) =>
  vector.type === 'Buffer' ? Buffer.from(vector.data) : Buffer.from(vector.hex, 'hex')

const load = (file) => JSON.parse(fs.readFileSync(path.join(REQUESTS, file), 'utf8'))

/** The structured vectors of one message: every `<message>-v<version>-*.json` of its folder. */
const structured = (folder, version, direction) => {
  const directory = path.join(REQUESTS, folder, 'fixtures', 'java')
  const prefix = `${folder}${direction}-v${version}-`

  return fs
    .readdirSync(directory)
    .filter((file) => file.startsWith(prefix))
    .map((file) => [
      file.slice(prefix.length, -'.json'.length),
      load(path.join(folder, 'fixtures', 'java', file)),
    ])
}

/**
 * The pairs of the bootstrap and classic-group subset. A structured pair names the folder and
 * message of its Java vectors; a bytes-only pair names its two vector files and its entry in
 * `expected`, whose fields come from the Java generator's constructors.
 */
const STRUCTURED = [
  ['joinGroupV8', 'joinGroup', 'JoinGroup', 8],
  ['syncGroupV5', 'syncGroup', 'SyncGroup', 5],
  ['heartbeatV4', 'heartbeat', 'Heartbeat', 4],
  ['leaveGroupV5', 'leaveGroup', 'LeaveGroup', 5],
  ['offsetFetchV9', 'offsetFetch', 'OffsetFetch', 9],
  ['offsetCommitV9', 'offsetCommit', 'OffsetCommit', 9],
]

const BYTES_ONLY = [
  ['apiVersionsV4', 'apiVersions', 'v4'],
  ['metadataV13', 'metadata', 'v13'],
  ['findCoordinatorV6', 'findCoordinator', 'v6'],
  ['listOffsetsV9', 'listOffsets', 'v9'],
  ['fetchV18', 'fetch', 'v18'],
  ['offsetForLeaderEpochV4', 'offsetForLeaderEpoch', 'v4'],
]

describe('wire pairs checked against structured Java vectors', () => {
  describe.each(STRUCTURED)('%s', (name, folder, message, version) => {
    const pair = require(`../wire/${name}`)
    const responses = structured(folder, version, 'Response')
    const requests = structured(folder, version, 'Request')

    test('names its API and version', () => {
      expect(pair).toMatchObject({ api: message, version })
      expect(Number.isInteger(pair.apiKey)).toBe(true)
      expect(responses.length).toBeGreaterThan(0)
      expect(requests.length).toBeGreaterThan(0)
    })

    test.each(responses)(
      'builds the bytes of Java’s %s response from the vector’s fields',
      (_case, vector) => {
        expect(pair.buildResponse(vector.fields).toString('hex')).toBe(vector.hex)
      }
    )

    test('builds the bytes of the first response vector, unvaried', () => {
      expect(pair.buildResponse().toString('hex')).toBe(
        responses.find(([caseName]) => caseName === 'ok' || caseName === 'basic')[1].hex
      )
    })

    test.each(requests)('reads the fields of Java’s %s request', (_case, vector) => {
      expect(pair.readRequest(bytesOf(vector))).toMatchObject(vector.fields)
    })

    test.each(requests)(
      'refuses Java’s %s request with a byte after its tag buffer',
      (_case, vector) => {
        const bytes = Buffer.concat([bytesOf(vector), Buffer.from([0xff])])

        expect(() => pair.readRequest(bytes)).toThrow('its tag buffer')
      }
    )

    test.each(requests)('refuses Java’s %s request cut short by a byte', (_case, vector) => {
      expect(() => pair.readRequest(bytesOf(vector).subarray(0, -1))).toThrow()
    })
  })
})

describe('wire pairs checked against bytes-only Java vectors', () => {
  describe.each(BYTES_ONLY)('%s', (name, folder, version) => {
    const pair = require(`../wire/${name}`)
    const literals = expected[name]
    const response = bytesOf(load(`${folder}/fixtures/${version}_response_java.json`))
    const request = bytesOf(load(`${folder}/fixtures/${version}_request_java.json`))

    test('names its API, its version and the generator constructors its fields come from', () => {
      expect(pair.version).toBe(Number(version.slice(1)))
      expect(Number.isInteger(pair.apiKey)).toBe(true)
      expect(literals.source).toMatch(/^build\w+Request l\.\d+.*build\w+Response l\.\d+/)
    })

    test('builds the bytes of Java’s response, unvaried', () => {
      expect(pair.buildResponse().toString('hex')).toBe(response.toString('hex'))
    })

    test('builds the bytes of Java’s response from the literal fields of the generator', () => {
      expect(pair.buildResponse(literals.response).toString('hex')).toBe(response.toString('hex'))
    })

    test('reads the literal fields of the generator out of Java’s request', () => {
      expect(pair.readRequest(request)).toEqual(literals.request)
    })

    test('refuses Java’s request with a byte after its tag buffer', () => {
      const bytes = Buffer.concat([request, Buffer.from([0xff])])

      expect(() => pair.readRequest(bytes)).toThrow('its tag buffer')
    })

    test('refuses Java’s request cut short by a byte', () => {
      expect(() => pair.readRequest(request.subarray(0, -1))).toThrow()
    })
  })
})

describe('offsetForLeaderEpochV4 walks the whole request body', () => {
  const { readRequest } = require('../wire/offsetForLeaderEpochV4')
  const encodeRequest = require('../../src/protocol/requests/offsetForLeaderEpoch/v4/request')
  const java = bytesOf(load('offsetForLeaderEpoch/fixtures/v4_request_java.json'))

  const partitionsOf = (count) =>
    Array.from({ length: count }, (_, partition) => ({
      partition,
      currentLeaderEpoch: 5,
      leaderEpoch: 4,
    }))
  const topicsOf = (count) =>
    Array.from({ length: count }, (_, index) => ({
      topic: `topic-${index}`,
      partitions: partitionsOf(1),
    }))
  const named = (topic) => [{ topic, partitions: partitionsOf(1) }]

  /** One topic, one partition, the given tag sections, written with the Encoder's primitives. */
  const bodyWithTags = ({ partition, topic, request }) =>
    new Encoder()
      .writeInt32(-1)
      .writeUVarIntArray([
        new Encoder()
          .writeUVarIntString('t')
          .writeUVarIntArray([
            new Encoder().writeInt32(0).writeInt32(5).writeInt32(4).writeTaggedFields(partition),
          ])
          .writeTaggedFields(topic),
      ])
      .writeTaggedFields(request).buffer
  const unknown = [{ tag: 3, value: Buffer.from([1, 2, 3]) }]
  const oneTopic = (topic) =>
    new Encoder().writeInt32(-1).writeUVarIntArray([topic]).writeTaggedFields().buffer
  const NULLS = {
    topics: new Encoder().writeInt32(-1).writeUVarIntArray(null).writeTaggedFields().buffer,
    name: oneTopic(
      new Encoder().writeUVarIntString(null).writeUVarIntArray(null).writeTaggedFields()
    ),
    partitions: oneTopic(
      new Encoder().writeUVarIntString('t').writeUVarIntArray(null).writeTaggedFields()
    ),
  }

  test.each([
    ['126 partitions', [{ topic: 'test-topic', partitions: partitionsOf(126) }]],
    ['127 partitions', [{ topic: 'test-topic', partitions: partitionsOf(127) }]],
    ['a 126-byte topic name', named('a'.repeat(126))],
    ['a 127-byte topic name', named('a'.repeat(127))],
    ['a 127-byte topic name of two-byte characters', named(`${'é'.repeat(63)}a`)],
    ['126 topics', topicsOf(126)],
    ['127 topics', topicsOf(127)],
  ])('reads %s as the production encoder writes them', async (_what, topics) => {
    const { buffer } = await encodeRequest({ topics }).encode()

    expect(readRequest(buffer)).toEqual({ replicaId: -1, topics })
  })

  test.each([
    ['partition', { partition: unknown }],
    ['topic', { topic: unknown }],
    ['request', { request: unknown }],
  ])('reads a request whose %s tag section holds an unknown tag', (_where, tags) => {
    expect(readRequest(bodyWithTags(tags))).toEqual({
      replicaId: -1,
      topics: [{ topic: 't', partitions: partitionsOf(1) }],
    })
  })

  test.each(['01', '80', 'ff'])('refuses Java’s request with %s for its last byte', (last) => {
    const bytes = Buffer.from(java)
    bytes[bytes.length - 1] = parseInt(last, 16)

    expect(bytes).toHaveLength(java.length)
    expect(() => readRequest(bytes)).toThrow(/Tried to read 1 byte\(s\) with readUVarInt/)
  })

  test.each([
    ['topics', /null topics/],
    ['name', /null topic name/],
    ['partitions', /null partitions/],
  ])('refuses a request with null %s', (where, message) => {
    expect(() => readRequest(NULLS[where])).toThrow(message)
  })

  test('refuses a request a byte past a tag section that holds an unknown tag', () => {
    const bytes = Buffer.concat([bodyWithTags({ request: unknown }), Buffer.from([0])])

    expect(() => readRequest(bytes)).toThrow('its tag buffer')
  })
})

describe('wire pairs with bytes fields', () => {
  const hex = '000300000001'
  const bytes = Buffer.from(hex, 'hex')

  test('joinGroupV8 takes member metadata as a Buffer as well as hex', () => {
    const { buildResponse } = require('../wire/joinGroupV8')
    const members = (memberMetadata) => ({
      members: [{ memberId: 'member', groupInstanceId: null, memberMetadata }],
    })

    expect(buildResponse(members(bytes))).toEqual(buildResponse(members(hex)))
  })

  test('syncGroupV5 takes the assignment as a Buffer as well as hex', () => {
    const { buildResponse } = require('../wire/syncGroupV5')

    expect(buildResponse({ memberAssignment: bytes })).toEqual(
      buildResponse({ memberAssignment: hex })
    )
  })
})

/**
 * A second opinion, not the evidence: what a builder writes for fields the test chose is read back
 * by the client's own decoder, which must find those fields. It catches a builder that is right for
 * the vector's values and wrong for any other (a swapped pair of equal strings, a fixed width).
 */
const VARIED = [
  [
    'apiVersionsV4',
    'apiVersions/v4/response',
    { apiKeys: [{ apiKey: 3, minVersion: 12, maxVersion: 12 }], throttleTime: 7 },
    { apiVersions: [{ apiKey: 3, minVersion: 12, maxVersion: 12 }], clientSideThrottleTime: 7 },
  ],
  [
    'metadataV13',
    'metadata/v13/response',
    {
      clusterId: 'c1',
      controllerId: 4,
      brokers: [{ nodeId: 4, host: 'h', port: 9, rack: 'r' }],
      topics: [],
    },
    {
      clusterId: 'c1',
      controllerId: 4,
      brokers: [{ nodeId: 4, host: 'h', port: 9, rack: 'r' }],
      topicMetadata: [],
    },
  ],
  [
    'findCoordinatorV6',
    'findCoordinator/v6/response',
    {
      coordinators: [{ key: 'k', nodeId: 3, host: 'h', port: 7, errorCode: 0, errorMessage: null }],
    },
    { coordinator: { nodeId: 3, host: 'h', port: 7 } },
  ],
  [
    'listOffsetsV9',
    'listOffsets/v9/response',
    {
      topics: [
        {
          name: 't',
          partitions: [
            { partitionIndex: 2, errorCode: 0, timestamp: '-1', offset: '55', leaderEpoch: 8 },
          ],
        },
      ],
    },
    {
      responses: [
        { topic: 't', partitions: [{ partition: 2, errorCode: 0, offset: '55', leaderEpoch: 8 }] },
      ],
    },
  ],
  [
    'joinGroupV8',
    'joinGroup/v8/response',
    { leaderId: 'a', memberId: 'b', generationId: 9 },
    { leaderId: 'a', memberId: 'b', generationId: 9 },
  ],
  [
    'syncGroupV5',
    'syncGroup/v5/response',
    { memberAssignment: '0102' },
    { memberAssignment: Buffer.from([1, 2]) },
  ],
  ['heartbeatV4', 'heartbeat/v4/response', { errorCode: 27 }, { errorCode: 27 }],
  [
    'leaveGroupV5',
    'leaveGroup/v5/response',
    { members: [{ memberId: 'x', groupInstanceId: 'y', errorCode: 0 }] },
    { members: [{ memberId: 'x', groupInstanceId: 'y', errorCode: 0 }] },
  ],
  [
    'offsetFetchV9',
    'offsetFetch/v9/response',
    {
      topics: [
        {
          topic: 't2',
          partitions: [{ partition: 3, offset: '77', leaderEpoch: 4, metadata: 'm', errorCode: 0 }],
        },
      ],
    },
    {
      responses: [
        {
          topic: 't2',
          partitions: [
            { partition: 3, offset: '77', committedLeaderEpoch: 4, metadata: 'm', errorCode: 0 },
          ],
        },
      ],
    },
  ],
  [
    'offsetCommitV9',
    'offsetCommit/v9/response',
    { topics: [{ topic: 't3', partitions: [{ partition: 5, errorCode: 22 }] }] },
    { responses: [{ topic: 't3', partitions: [{ partition: 5, errorCode: 22 }] }] },
  ],
]

describe('builders agree with the client’s decoder on fields the vectors do not hold', () => {
  test.each(VARIED)('%s', async (name, decoder, fields, decoded) => {
    const { decode } = require(`../../src/protocol/requests/${decoder}`)

    expect(await decode(require(`../wire/${name}`).buildResponse(fields))).toMatchObject(decoded)
  })
})

/**
 * The entries a profile adds to the ApiVersions answer, written as a Kafka 4.3.1 broker writes
 * them. The literals are written out by hand from the ApiVersionsResponse layout (an entry is its
 * fields and an empty tag buffer; the arrays and the tagged fields carry their lengths as unsigned
 * varints), never decoded, and each entry literal is also found in the broker's own bytes.
 */
describe('ApiVersions entries a profile adds are written as a Kafka 4.3.1 broker writes them', () => {
  const { buildResponse } = require('../wire/apiVersionsV4')
  const broker = bytesOf(load('apiVersions/fixtures/v4_response_kafka431.json')).toString('hex')
  const text = (value) => Buffer.from(value).toString('hex')

  const METADATA_0_TO_13 = '0003' + '0000' + '000d' + '00'
  const TRANSACTION_LEVEL_2 = '14' + text('transaction.version') + '0002' + '0002' + '00'
  const METADATA_LEVEL_30 = '11' + text('metadata.version') + '001e' + '001e' + '00'

  test.each([
    ['an API entry', METADATA_0_TO_13, { apiKeys: [{ apiKey: 3, minVersion: 0, maxVersion: 13 }] }],
    [
      'a finalized feature entry',
      TRANSACTION_LEVEL_2,
      {
        finalizedFeatures: [
          { name: 'transaction.version', minVersionLevel: 2, maxVersionLevel: 2 },
        ],
      },
    ],
  ])('%s is the bytes the broker sent', (_what, literal, fields) => {
    expect(broker).toContain(literal)
    expect(buildResponse(fields).toString('hex')).toContain(literal)
  })

  test('frames three API entries, no supported feature and two finalized features as the layout says', () => {
    const golden = [
      '0000', // error code
      '04', // three API entries
      '0012' + '0004' + '0004' + '00', // ApiVersions 4 to 4
      '0003' + '000d' + '000d' + '00', // Metadata 13 to 13
      '0001' + '0012' + '0012' + '00', // Fetch 18 to 18
      '00000000', // throttle time
      '03', // three tagged fields
      '00' + '01' + '01', // supported features, 1 byte long: none
      '01' + '08' + '0000000000000007', // finalized features epoch 7
      '02' + '30' + '03' + TRANSACTION_LEVEL_2 + METADATA_LEVEL_30, // finalized features, 48 bytes
    ].join('')

    const body = buildResponse({
      apiKeys: [
        { apiKey: 18, minVersion: 4, maxVersion: 4 },
        { apiKey: 3, minVersion: 13, maxVersion: 13 },
        { apiKey: 1, minVersion: 18, maxVersion: 18 },
      ],
      supportedFeatures: [],
      finalizedFeaturesEpoch: 7,
      finalizedFeatures: [
        { name: 'transaction.version', minVersionLevel: 2, maxVersionLevel: 2 },
        { name: 'metadata.version', minVersionLevel: 30, maxVersionLevel: 30 },
      ],
    })

    expect(body.toString('hex')).toBe(golden)
  })
})

/**
 * A diagnostic against a real broker, not evidence: what a pair builds, read by the client's own
 * decoder, has the keys of what a Kafka 4.3.1 broker answered (a capture). Only keys are compared,
 * and an array that is empty on either side matches any shape, since a capture holds whatever the
 * broker had. The same decoder reads both sides, so a field it leaves out is invisible here; the
 * captured fields are checked as the literals of `expected` in the cases beside it.
 */
const shapeOf = (value) => {
  if (Array.isArray(value)) {
    return value.length === 0 ? [] : [shapeOf(value[0])]
  }
  if (value !== null && typeof value === 'object' && !Buffer.isBuffer(value)) {
    return Object.fromEntries(
      Object.keys(value)
        .sort()
        .map((key) => [key, shapeOf(value[key])])
    )
  }

  return null
}

const shapesAgree = (a, b) => {
  if (Array.isArray(a) && Array.isArray(b)) {
    return a.length === 0 || b.length === 0 || shapesAgree(a[0], b[0])
  }
  if (a === null || b === null) {
    return a === b
  }

  const keys = Object.keys(a)
  return (
    keys.length === Object.keys(b).length &&
    keys.every((key) => key in b && shapesAgree(a[key], b[key]))
  )
}

describe('pairs agree with what a Kafka 4.3.1 broker sent', () => {
  test.each([
    ['apiVersionsV4', 'apiVersions', 'v4', 'apiVersions/v4/response'],
    ['metadataV13', 'metadata', 'v13', 'metadata/v13/response'],
    ['findCoordinatorV6', 'findCoordinator', 'v6', 'findCoordinator/v6/response'],
    ['listOffsetsV9', 'listOffsets', 'v9', 'listOffsets/v9/response'],
    ['fetchV18', 'fetch', 'v18', 'fetch/v18/response'],
  ])('%s builds a response shaped like the broker’s', async (name, folder, version, decoder) => {
    const { decode } = require(`../../src/protocol/requests/${decoder}`)
    const built = await decode(require(`../wire/${name}`).buildResponse())
    const broker = await decode(
      bytesOf(load(`${folder}/fixtures/${version}_response_kafka431.json`))
    )

    expect(shapesAgree(shapeOf(built), shapeOf(broker))).toBe(true)
  })

  test.each([
    ['findCoordinatorV6', 'findCoordinator', 'v6'],
    ['listOffsetsV9', 'listOffsets', 'v9'],
    ['fetchV18', 'fetch', 'v18'],
  ])(
    '%s reads the independently documented fields of the captured request',
    (name, folder, version) => {
      const { readRequest } = require(`../wire/${name}`)
      const sent = readRequest(bytesOf(load(`${folder}/fixtures/${version}_request_kafka431.json`)))

      expect(sent).toEqual(expected[name].capture.request)
    }
  )

  test.each([
    ['findCoordinatorV6', 'findCoordinator', 'v6'],
    ['listOffsetsV9', 'listOffsets', 'v9'],
  ])(
    '%s builds the captured response from its documented literal fields',
    (name, folder, version) => {
      const { buildResponse } = require(`../wire/${name}`)
      const captured = bytesOf(load(`${folder}/fixtures/${version}_response_kafka431.json`))

      expect(buildResponse(expected[name].capture.response)).toEqual(captured)
    }
  )

  test('the shape check tells two shapes apart', () => {
    expect(shapesAgree(shapeOf({ a: [{ b: 1 }] }), shapeOf({ a: [{ b: 2 }] }))).toBe(true)
    expect(shapesAgree(shapeOf({ a: [{ b: 1 }] }), shapeOf({ a: [{ c: 2 }] }))).toBe(false)
    expect(shapesAgree(shapeOf({ a: [] }), shapeOf({ a: [{ c: 2 }] }))).toBe(true)
    expect(shapesAgree(shapeOf({ a: 1 }), shapeOf({ a: 1, b: 2 }))).toBe(false)
  })
})
