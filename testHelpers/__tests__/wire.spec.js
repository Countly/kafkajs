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
  ['electLeadersV0', 'electLeaders', 'ElectLeaders', 0, 'mixed'],
  ['electLeadersV1', 'electLeaders', 'ElectLeaders', 1, 'mixed'],
  ['electLeadersV2', 'electLeaders', 'ElectLeaders', 2, 'mixed'],
  ['updateFeaturesV0', 'updateFeatures', 'UpdateFeatures', 0, 'mixed'],
  ['updateFeaturesV1', 'updateFeatures', 'UpdateFeatures', 1, 'mixed'],
  ['updateFeaturesV2', 'updateFeatures', 'UpdateFeatures', 2, 'mixed'],
  ['describeClusterV0', 'describeCluster', 'DescribeCluster', 0, 'mixed'],
  ['describeClusterV1', 'describeCluster', 'DescribeCluster', 1, 'mixed'],
  ['describeClusterV2', 'describeCluster', 'DescribeCluster', 2, 'mixed'],
  ['listConfigResourcesV0', 'listConfigResources', 'ListConfigResources', 0, 'mixed'],
  ['listConfigResourcesV1', 'listConfigResources', 'ListConfigResources', 1, 'mixed'],
  ['joinGroupV8', 'joinGroup', 'JoinGroup', 8],
  ['syncGroupV5', 'syncGroup', 'SyncGroup', 5],
  ['heartbeatV4', 'heartbeat', 'Heartbeat', 4],
  ['leaveGroupV5', 'leaveGroup', 'LeaveGroup', 5],
  ['offsetFetchV9', 'offsetFetch', 'OffsetFetch', 9],
  ['offsetCommitV9', 'offsetCommit', 'OffsetCommit', 9],
  ['addPartitionsToTxnV3', 'addPartitionsToTxn', 'AddPartitionsToTxn', 3],
  ['addOffsetsToTxnV4', 'addOffsetsToTxn', 'AddOffsetsToTxn', 4],
  ['txnOffsetCommitV4', 'txnOffsetCommit', 'TxnOffsetCommit', 4],
  ['txnOffsetCommitV5', 'txnOffsetCommit', 'TxnOffsetCommit', 5],
  ['endTxnV4', 'endTxn', 'EndTxn', 4],
  ['endTxnV5', 'endTxn', 'EndTxn', 5],
  [
    'describeTopicPartitionsV0',
    'describeTopicPartitions',
    'DescribeTopicPartitions',
    0,
    'noNextCursor',
  ],
  ['consumerGroupHeartbeatV1', 'consumerGroupHeartbeat', 'ConsumerGroupHeartbeat', 1, 'assignment'],
  ['offsetFetchV10', 'offsetFetch', 'OffsetFetch', 10],
  ['offsetCommitV10', 'offsetCommit', 'OffsetCommit', 10],
]

const BYTES_ONLY = [
  ['apiVersionsV4', 'apiVersions', 'v4'],
  ['metadataV13', 'metadata', 'v13'],
  ['findCoordinatorV6', 'findCoordinator', 'v6'],
  ['listOffsetsV9', 'listOffsets', 'v9'],
  ['fetchV12', 'fetch', 'v12'],
  ['fetchV18', 'fetch', 'v18'],
  ['offsetForLeaderEpochV4', 'offsetForLeaderEpoch', 'v4'],
  ['produceV11', 'produce', 'v11'],
  ['produceV12', 'produce', 'v12'],
  ['produceV13', 'produce', 'v13'],
  ['saslAuthenticateV2', 'saslAuthenticate', 'v2'],
]

describe('wire pairs checked against structured Java vectors', () => {
  describe.each(STRUCTURED)('%s', (name, folder, message, version, defaultCase) => {
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
        responses.find(([caseName]) =>
          defaultCase ? caseName === defaultCase : ['ok', 'basic', 'defaults'].includes(caseName)
        )[1].hex
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
  [
    'initProducerIdV5',
    'initProducerId/v5/response',
    { errorCode: 53, producerId: '77', producerEpoch: 4, throttleTime: 3 },
    { errorCode: 53, producerId: '77', producerEpoch: 4, clientSideThrottleTime: 3 },
  ],
  [
    'produceV11',
    'produce/v11/response',
    {
      responses: [
        {
          topic: 't4',
          partitions: [
            {
              partition: 2,
              errorCode: 6,
              baseOffset: '9',
              logAppendTime: '-1',
              logStartOffset: '1',
              leader: { leaderId: 4, leaderEpoch: 8 },
            },
          ],
        },
      ],
    },
    {
      topics: [
        {
          topicName: 't4',
          partitions: [
            {
              partition: 2,
              errorCode: 6,
              baseOffset: '9',
              currentLeader: { leaderId: 4, leaderEpoch: 8 },
            },
          ],
        },
      ],
    },
  ],
  [
    'produceV12',
    'produce/v12/response',
    {
      responses: [
        {
          topic: 't4',
          partitions: [
            {
              partition: 2,
              errorCode: 6,
              baseOffset: '9',
              logAppendTime: '-1',
              logStartOffset: '1',
              leader: { leaderId: 4, leaderEpoch: 8 },
            },
          ],
        },
      ],
    },
    {
      topics: [
        {
          topicName: 't4',
          partitions: [
            {
              partition: 2,
              errorCode: 6,
              baseOffset: '9',
              currentLeader: { leaderId: 4, leaderEpoch: 8 },
            },
          ],
        },
      ],
    },
  ],
  [
    'produceV13',
    'produce/v13/response',
    {
      responses: [
        {
          topicId: '0a0b0c0d-0e0f-1011-1213-141516171819',
          partitions: [
            {
              partition: 2,
              errorCode: 6,
              baseOffset: '9',
              logAppendTime: '-1',
              logStartOffset: '1',
              leader: { leaderId: 4, leaderEpoch: 8 },
            },
          ],
        },
      ],
    },
    {
      topics: [
        {
          topicId: '0a0b0c0d-0e0f-1011-1213-141516171819',
          partitions: [
            {
              partition: 2,
              errorCode: 6,
              baseOffset: '9',
              currentLeader: { leaderId: 4, leaderEpoch: 8 },
            },
          ],
        },
      ],
    },
  ],
  [
    'addPartitionsToTxnV3',
    'addPartitionsToTxn/v3/response',
    { errors: [{ topic: 't5', partitionErrors: [{ partition: 4, errorCode: 48 }] }] },
    { errors: [{ topic: 't5', partitionErrors: [{ partition: 4, errorCode: 48 }] }] },
  ],
  ['addOffsetsToTxnV4', 'addOffsetsToTxn/v4/response', { errorCode: 49 }, { errorCode: 49 }],
  [
    'txnOffsetCommitV4',
    'txnOffsetCommit/v4/response',
    { topics: [{ topic: 't6', partitions: [{ partition: 7, errorCode: 22 }] }] },
    { topics: [{ topic: 't6', partitions: [{ partition: 7, errorCode: 22 }] }] },
  ],
  [
    'txnOffsetCommitV5',
    'txnOffsetCommit/v5/response',
    { topics: [{ topic: 't6', partitions: [{ partition: 7, errorCode: 22 }] }] },
    { topics: [{ topic: 't6', partitions: [{ partition: 7, errorCode: 22 }] }] },
  ],
  ['endTxnV4', 'endTxn/v4/response', { errorCode: 48 }, { errorCode: 48 }],
  [
    'endTxnV5',
    'endTxn/v5/response',
    { errorCode: 0, producerId: '2000', producerEpoch: 6 },
    { errorCode: 0, producerId: '2000', producerEpoch: 6 },
  ],
  [
    'describeTopicPartitionsV0',
    'describeTopicPartitions/v0/response',
    {
      throttleTime: 3,
      topics: [
        {
          errorCode: 0,
          name: 't7',
          topicId: '0a0b0c0d-0e0f-1011-1213-141516171819',
          isInternal: false,
          partitions: [
            {
              errorCode: 0,
              partitionIndex: 4,
              leaderId: 2,
              leaderEpoch: 9,
              replicaNodes: [2, 3],
              isrNodes: [2],
              eligibleLeaderReplicas: [3],
              lastKnownElr: null,
              offlineReplicas: [],
            },
          ],
          topicAuthorizedOperations: 7,
        },
      ],
      nextCursor: { topicName: 't7', partitionIndex: 5 },
    },
    {
      clientSideThrottleTime: 3,
      topics: [
        {
          name: 't7',
          topicId: '0a0b0c0d-0e0f-1011-1213-141516171819',
          partitions: [
            { partitionIndex: 4, leaderId: 2, eligibleLeaderReplicas: [3], lastKnownElr: null },
          ],
          topicAuthorizedOperations: 7,
        },
      ],
      nextCursor: { topicName: 't7', partitionIndex: 5 },
    },
  ],
  [
    'consumerGroupHeartbeatV1',
    'consumerGroupHeartbeat/v1/response',
    {
      errorCode: 0,
      memberId: 'm9',
      memberEpoch: 9,
      heartbeatIntervalMs: 700,
      assignment: {
        topicPartitions: [{ topicId: '0a0b0c0d-0e0f-1011-1213-141516171819', partitions: [3, 4] }],
      },
    },
    {
      memberId: 'm9',
      memberEpoch: 9,
      heartbeatIntervalMs: 700,
      assignment: {
        topicPartitions: [{ topicId: '0a0b0c0d-0e0f-1011-1213-141516171819', partitions: [3, 4] }],
      },
    },
  ],
  [
    'offsetFetchV10',
    'offsetFetch/v10/response',
    {
      groupId: 'g2',
      topics: [
        {
          topicId: '0a0b0c0d-0e0f-1011-1213-141516171819',
          partitions: [{ partition: 3, offset: '77', leaderEpoch: 4, metadata: 'm', errorCode: 0 }],
        },
      ],
    },
    {
      responses: [
        {
          topicId: '0a0b0c0d-0e0f-1011-1213-141516171819',
          partitions: [
            { partition: 3, offset: '77', committedLeaderEpoch: 4, metadata: 'm', errorCode: 0 },
          ],
        },
      ],
    },
  ],
  [
    'offsetCommitV10',
    'offsetCommit/v10/response',
    {
      topics: [
        {
          topicId: '0a0b0c0d-0e0f-1011-1213-141516171819',
          partitions: [{ partition: 5, errorCode: 100 }],
        },
      ],
    },
    {
      responses: [
        {
          topicId: '0a0b0c0d-0e0f-1011-1213-141516171819',
          partitions: [{ partition: 5, errorCode: 100 }],
        },
      ],
    },
  ],
  [
    'listPartitionReassignmentsV0',
    'listPartitionReassignments/v0/response',
    {
      topics: [
        {
          name: 't8',
          partitions: [
            { partitionIndex: 2, replicas: [1, 2], addingReplicas: [3], removingReplicas: [4] },
          ],
        },
      ],
    },
    {
      topics: [
        {
          name: 't8',
          partitions: [
            { partition: 2, replicas: [1, 2], addingReplicas: [3], removingReplicas: [4] },
          ],
        },
      ],
    },
  ],
  [
    'saslHandshakeV1',
    'saslHandshake/v1/response',
    { errorCode: 33, enabledMechanisms: ['PLAIN', 'SCRAM-SHA-512'] },
    { errorCode: 33, enabledMechanisms: ['PLAIN', 'SCRAM-SHA-512'] },
  ],
  [
    'saslAuthenticateV2',
    'saslAuthenticate/v2/response',
    { errorCode: 58, errorMessage: 'bad', authBytes: Buffer.from('xy'), sessionLifetimeMs: '7' },
    { errorCode: 58, errorMessage: 'bad', sessionLifetimeMs: '7' },
  ],
]

describe('builders agree with the client’s decoder on fields the vectors do not hold', () => {
  test.each(VARIED)('%s', async (name, decoder, fields, decoded) => {
    const { decode } = require(`../../src/protocol/requests/${decoder}`)

    expect(await decode(require(`../wire/${name}`).buildResponse(fields))).toMatchObject(decoded)
  })
})

describe('InitProducerId v5: a bytes-only response and structured requests', () => {
  const pair = require('../wire/initProducerIdV5')
  const response = bytesOf(load('initProducerId/fixtures/v5_response_java.json'))
  const requests = structured('initProducerId', 5, 'Request')

  test('names its API, its version and the generator constructor its fields come from', () => {
    expect(pair).toMatchObject({ api: 'InitProducerId', version: 5 })
    expect(expected.initProducerIdV5.source).toMatch(/^buildInitProducerIdResponse l\.\d+/)
    expect(requests.map(([caseName]) => caseName)).toEqual([
      'firstInit',
      'nullTransactionalId',
      'reinit',
    ])
  })

  test('builds the bytes of Java’s response, unvaried and from the literal fields', () => {
    expect(pair.buildResponse().toString('hex')).toBe(response.toString('hex'))
    expect(pair.buildResponse(expected.initProducerIdV5.response).toString('hex')).toBe(
      response.toString('hex')
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

/**
 * The Produce request framing around record batches. Java's vectors have a null `Records` field,
 * so the batch header and a populated transactional id are read from the hand-computed goldens of
 * `expected`, whose fields were tabulated by hand, never decoded.
 */
describe('Produce readers against the hand-computed goldens', () => {
  const { readBatches } = require('../wire/produceLayout')
  const header = Buffer.from(expected.recordBatchHeaderV2.hex, 'hex')
  const withAttributes = (attributes) => {
    const copy = Buffer.from(header)
    copy.writeInt16BE(attributes, 21)
    return copy
  }

  test('the header golden is the 61 bytes of the layout, with the length field naming the rest', () => {
    expect(header.length).toBe(61)
    expect(header.readInt32BE(8)).toBe(header.length - 12)
  })

  test('reads the producer id, epoch, base sequence, count and flags of the header golden', () => {
    expect(readBatches(header)).toEqual([expected.recordBatchHeaderV2.fields])
  })

  test('tells an idempotent batch, a transactional one and a control batch apart by the attributes', () => {
    expect(readBatches(withAttributes(0x0000))[0]).toMatchObject({
      transactional: false,
      control: false,
    })
    expect(readBatches(withAttributes(0x0030))[0]).toMatchObject({
      transactional: true,
      control: true,
    })
  })

  test('walks two batches in one Records field', () => {
    expect(
      readBatches(Buffer.concat([header, withAttributes(0)])).map((b) => b.transactional)
    ).toEqual([true, false])
  })

  test('digests the whole batch, so two batches that differ only in their records differ', () => {
    const body = (record) => {
      const batch = Buffer.concat([header, Buffer.from([record])])
      batch.writeInt32BE(batch.length - 12, 8)
      return batch
    }

    expect(readBatches(body(1))[0].digest).not.toBe(readBatches(body(2))[0].digest)
    expect(readBatches(body(1))[0].digest).toBe(readBatches(body(1))[0].digest)
  })

  test('gives the digest of the batch bytes, so a resend of the same bytes is told from a rewrite', () => {
    const other = Buffer.from(header)
    other.writeInt32BE(8, 53)
    expect(readBatches(header)[0].digest).toBe(readBatches(Buffer.from(header))[0].digest)
    expect(readBatches(other)[0].digest).not.toBe(readBatches(header)[0].digest)
  })

  test('refuses Records that are not whole batches', () => {
    expect(() => readBatches(header.subarray(0, 60))).toThrow(
      '60 bytes that are not a batch header'
    )
    expect(() => readBatches(Buffer.concat([header, Buffer.from([1, 2])]))).toThrow(
      '2 bytes that are not a batch header'
    )

    const tooLong = Buffer.from(header)
    tooLong.writeInt32BE(500, 8)
    expect(() => readBatches(tooLong)).toThrow('a batch says it is 500 bytes long, 49 follow')

    const tooShort = Buffer.from(header)
    tooShort.writeInt32BE(10, 8)
    expect(() => readBatches(tooShort)).toThrow('a batch says it is 10 bytes long')
  })

  test.each(['produceV11', 'produceV12'])(
    '%s refuses the golden request with a byte after its tag buffer',
    (name) => {
      const bytes = Buffer.from(expected.produceRequestTransactionalV11.hex + 'ff', 'hex')

      expect(() => require(`../wire/${name}`).readRequest(bytes)).toThrow('its tag buffer')
    }
  )

  test.each(['produceV11', 'produceV12'])(
    '%s reads a request with a transactional id and a transactional batch',
    (name) => {
      const golden = expected.produceRequestTransactionalV11
      const pair = require(`../wire/${name}`)

      expect(pair.readRequest(Buffer.from(golden.hex, 'hex'))).toEqual(golden.fields)
    }
  )
})

/**
 * SaslHandshake v1 has no vector, so its pair is checked against the hand-computed goldens of
 * `expected`, each assembled from the message layout and never decoded.
 */
describe('SaslHandshake v1 against its hand-computed goldens', () => {
  const pair = require('../wire/saslHandshakeV1')
  const golden = expected.saslHandshakeV1

  test('names its API and version', () => {
    expect(pair).toMatchObject({ api: 'SaslHandshake', version: 1 })
    expect(Number.isInteger(pair.apiKey)).toBe(true)
  })

  test.each(golden.responses.map((entry, index) => [index, entry]))(
    'builds golden response %i from its fields',
    (_index, { hex, fields }) => {
      expect(pair.buildResponse(fields).toString('hex')).toBe(hex)
    }
  )

  test('builds, unvaried, the answer of a broker that enables PLAIN', () => {
    expect(pair.buildResponse().toString('hex')).toBe('0000' + '00000001' + '0005' + '504c41494e')
  })

  test('the smallest answer is the six bytes of an error code and an empty list', () => {
    expect(golden.responses[2].hex).toHaveLength(12)
    expect(pair.buildResponse({ enabledMechanisms: [] })).toEqual(
      Buffer.from('000000000000', 'hex')
    )
  })

  test.each(golden.requests.map((entry, index) => [index, entry]))(
    'reads the fields of golden request %i',
    (_index, { hex, fields }) => {
      expect(pair.readRequest(Buffer.from(hex, 'hex'))).toEqual(fields)
    }
  )

  test.each(golden.requests.map((entry, index) => [index, entry]))(
    'refuses golden request %i with a byte after its mechanism, and cut short by a byte',
    (_index, { hex }) => {
      const bytes = Buffer.from(hex, 'hex')

      expect(() => pair.readRequest(Buffer.concat([bytes, Buffer.from([0xff])]))).toThrow(
        'a byte after its mechanism'
      )
      expect(() => pair.readRequest(bytes.subarray(0, -1))).toThrow()
    }
  )
})

/**
 * ListPartitionReassignments has no Java vector: the capture of a Kafka 4.3.1 broker holds the
 * request and the answer while nothing is in flight, and the two goldens of `expected` hold a
 * reassignment under way and a request for named partitions.
 */
describe('ListPartitionReassignments v0 against the broker capture and its goldens', () => {
  const pair = require('../wire/listPartitionReassignmentsV0')
  const golden = expected.listPartitionReassignmentsV0
  const request = bytesOf(load('listPartitionReassignments/fixtures/v0_request_kafka431.json'))
  const response = bytesOf(load('listPartitionReassignments/fixtures/v0_response_kafka431.json'))

  test('names its API and version', () => {
    expect(pair).toMatchObject({ api: 'ListPartitionReassignments', version: 0 })
    expect(Number.isInteger(pair.apiKey)).toBe(true)
  })

  test('reads the documented fields of the captured request', () => {
    expect(pair.readRequest(request)).toEqual(golden.capture.request)
  })

  test('builds the captured response, unvaried and from the documented fields', () => {
    expect(pair.buildResponse()).toEqual(response)
    expect(pair.buildResponse(golden.capture.response)).toEqual(response)
  })

  test('builds the golden of a reassignment in flight from its fields', () => {
    expect(pair.buildResponse(golden.inFlight.fields).toString('hex')).toBe(golden.inFlight.hex)
  })

  test('reads the golden of a request for named partitions', () => {
    expect(pair.readRequest(Buffer.from(golden.named.hex, 'hex'))).toEqual(golden.named.fields)
  })

  test.each([
    ['the captured request', request],
    ['the golden request', Buffer.from(golden.named.hex, 'hex')],
  ])('refuses %s with a byte after its tag buffer, and cut short by a byte', (_name, bytes) => {
    expect(() => pair.readRequest(Buffer.concat([bytes, Buffer.from([0xff])]))).toThrow(
      'its tag buffer'
    )
    expect(() => pair.readRequest(bytes.subarray(0, -1))).toThrow()
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
    ['initProducerIdV5', 'initProducerId', 'v5', 'initProducerId/v5/response'],
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
    ['initProducerIdV5', 'initProducerId', 'v5'],
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
    ['initProducerIdV5', 'initProducerId', 'v5'],
    ['produceV11', 'produce', 'v11'],
    ['produceV13', 'produce', 'v13'],
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

describe('consumer response variants preserve independent templates', () => {
  const responseVector = (folder, name) => load(`${folder}/fixtures/java/${name}`)

  test.each([
    ['joinGroupV8', 'joinGroup', 'joinGroupResponse-v8-ok.json', 26],
    ['syncGroupV5', 'syncGroup', 'syncGroupResponse-v5-ok.json', 27],
    ['heartbeatV4', 'heartbeat', 'heartbeatResponse-v4-ok.json', 27],
    ['leaveGroupV5', 'leaveGroup', 'leaveGroupResponse-v5-ok.json', 30],
    [
      'consumerGroupHeartbeatV1',
      'consumerGroupHeartbeat',
      'consumerGroupHeartbeatResponse-v1-assignment.json',
      110,
    ],
  ])('%s changes only the top-level error in the Java template', (name, folder, file, code) => {
    const vector = responseVector(folder, file)
    const bytes = bytesOf(vector)
    expect(bytes.subarray(4, 6).toString('hex')).toBe('0000')
    bytes.writeInt16BE(code, 4)
    expect(require(`../wire/${name}`).buildResponse({ errorCode: code })).toEqual(bytes)
  })

  test('the commit partition error occupies its documented two bytes', () => {
    const vector = responseVector('offsetCommit', 'offsetCommitResponse-v9-basic.json')
    const bytes = bytesOf(vector)
    expect(bytes.subarray(21, 23).toString('hex')).toBe('0000')
    bytes.writeInt16BE(28, 21)
    expect(
      require('../wire/offsetCommitV9').buildResponse({
        topics: [{ topic: 'test-topic', partitions: [{ partition: 0, errorCode: 28 }] }],
      })
    ).toEqual(bytes)
  })

  test('a leave member error changes only that member', () => {
    const vector = responseVector('leaveGroup', 'leaveGroupResponse-v5-ok.json')
    const bytes = bytesOf(vector)
    expect(bytes.subarray(20, 22).toString('hex')).toBe('0000')
    bytes.writeInt16BE(25, 20)
    expect(
      require('../wire/leaveGroupV5').buildResponse({
        members: vector.fields.members.map((member, index) => ({
          ...member,
          errorCode: index === 0 ? 25 : 0,
        })),
      })
    ).toEqual(bytes)
  })

  test.each([70, 71])('an empty session response carries error %s and session id', (topLevel) => {
    const bytes = Buffer.from('000000000000000000250100', 'hex')
    bytes.writeInt16BE(topLevel, 4)
    expect(
      require('../wire/fetchV18').buildResponse({
        sessionId: 37,
        topLevel,
        includeTopic: false,
        nodeEndpoints: undefined,
      })
    ).toEqual(bytes)
  })

  test('a successful empty session response contains no topic entry', () => {
    expect(
      require('../wire/fetchV18').buildResponse({
        sessionId: 37,
        includeTopic: false,
        nodeEndpoints: undefined,
      })
    ).toEqual(Buffer.from('000000000000000000250100', 'hex'))
  })

  test('a session id patches only its four bytes in the Java response', () => {
    const bytes = bytesOf(load('fetch/fixtures/v18_response_java.json'))
    bytes.writeInt32BE(37, 6)
    expect(require('../wire/fetchV18').buildResponse({ sessionId: 37 })).toEqual(bytes)
  })

  test('a metadata request error patches only its trailing error', () => {
    const bytes = bytesOf(load('metadata/fixtures/v13_response_java.json'))
    expect(bytes.subarray(-3, -1).toString('hex')).toBe('0000')
    bytes.writeInt16BE(42, bytes.length - 3)
    expect(require('../wire/metadataV13').buildResponse({ errorCode: 42 })).toEqual(bytes)
  })

  test('a metadata topic error preserves the other independent fields', () => {
    const bytes = bytesOf(load('metadata/fixtures/v13_response_java.json'))
    expect(bytes.subarray(46, 48).toString('hex')).toBe('0000')
    bytes.writeInt16BE(3, 46)
    expect(
      require('../wire/metadataV13').buildResponse({
        topics: expected.metadataV13.response.topics.map((topic) => ({ ...topic, errorCode: 3 })),
      })
    ).toEqual(bytes)
  })
})

describe('Fetch forgotten topics use the recorded wire key', () => {
  const { decodeFetchRequest } = require('../../src/consumer/__tests__/fixtures/fetchHarness')
  const pair = require('../wire/fetchV18')
  // Apache FetchRequest: empty topics, one forgotten key, partitions 1 and 3, rack and tags.
  const body = Buffer.from(
    '000013880000000100a0000001000000250000000201020102030405060708090a0b0c0d0e0f1003000000010000000300027200',
    'hex'
  )
  const fields = {
    apiVersion: 18,
    replicaId: null,
    maxWaitMs: 5000,
    minBytes: 1,
    maxBytes: 10485760,
    isolationLevel: 1,
    sessionId: 37,
    sessionEpoch: 2,
    topics: [],
    forgottenTopics: [
      { topic: null, topicId: '01020304-0506-0708-090a-0b0c0d0e0f10', partitions: [1, 3] },
    ],
    rackId: 'r',
  }

  test('reads a nonempty forgotten entry independently of current metadata', () => {
    expect(pair.readRequest(body)).toEqual(fields)
  })

  test('reads the empty forgotten map in the unchanged Java request', () => {
    expect(
      pair.readRequest(bytesOf(load('fetch/fixtures/v18_request_java.json'))).forgottenTopics
    ).toEqual([])
  })

  test('refuses bytes after a valid empty terminal tag buffer', () => {
    expect(() => pair.readRequest(Buffer.concat([body, Buffer.from([0])]))).toThrow(
      'after its tag buffer'
    )
  })

  test.each([1, 2, 5, 10, 20])('refuses a forgotten map cut short by %s bytes', (removed) => {
    expect(() => pair.readRequest(body.subarray(0, -removed))).toThrow()
  })

  test.each([12, 13, 14, 15, 16, 17, 18])(
    'reads the supported v%s layer with its actual key',
    (version) => {
      const prefix = version <= 14 ? 'ffffffff' : ''
      const key = version <= 12 ? '0274' : '0102030405060708090a0b0c0d0e0f10'
      const bytes = Buffer.from(
        `${prefix}000013880000000100a000000100000025000000020102${key}03000000010000000300027200`,
        'hex'
      )
      const header = new Encoder()
        .writeInt32(0)
        .writeInt16(1)
        .writeInt16(version)
        .writeInt32(1)
        .writeString('')
        .writeTaggedFields().buffer
      const read = (data) => decodeFetchRequest([Buffer.concat([header, data])])
      expect(read(bytes)).toEqual({
        ...fields,
        apiVersion: version,
        replicaId: version <= 14 ? -1 : null,
        forgottenTopics: [
          {
            topic: version <= 12 ? 't' : null,
            topicId: version >= 13 ? fields.forgottenTopics[0].topicId : null,
            partitions: [1, 3],
          },
        ],
      })
      expect(() => read(Buffer.concat([bytes, Buffer.from([0])]))).toThrow('after its tag buffer')
      expect(() => read(bytes.subarray(0, -1))).toThrow()
    }
  )

  test.each([11, 19])('refuses unsupported Fetch version %s', (version) => {
    const header = new Encoder()
      .writeInt32(0)
      .writeInt16(1)
      .writeInt16(version)
      .writeInt32(1)
      .writeString('')
      .writeTaggedFields().buffer
    expect(() => decodeFetchRequest([Buffer.concat([header, body])])).toThrow(
      `does not support v${version}`
    )
  })
})

describe('independent record-containing Produce requests', () => {
  test.each([11, 12])(
    'reads the Java-generated v%i batch header and reproduces its request bytes',
    async (version) => {
      const vector = require(
        `../../src/protocol/requests/produce/fixtures/java/produceRequest-v${version}-records.json`
      )
      const pair = require(`../wire/produceV${version}`)
      expect(pair.readRequest(Buffer.from(vector.hex, 'hex'))).toEqual(vector.fields)
      expect(vector.fields.topics[0].partitions[0].batches[0]).toMatchObject({
        producerId: '1000',
        producerEpoch: 3,
        baseSequence: 7,
        recordCount: 2,
        transactional: true,
        control: false,
      })
      const request = require(`../../src/protocol/requests/produce/v${version}/request`)({
        transactionalId: 'tx-1',
        acks: -1,
        timeout: 1000,
        producerId: require('../../src/utils/long').fromInt(1000),
        producerEpoch: 3,
        topicData: [
          {
            topic: 'test-topic',
            partitions: [
              {
                partition: 0,
                firstSequence: 7,
                messages: vector.source.timestamps.map((timestamp, i) => ({
                  timestamp,
                  key: vector.source.keys[i],
                  value: vector.source.values[i],
                  headers: vector.source.headers[i],
                })),
              },
            ],
          },
        ],
      })
      expect((await request.encode()).buffer.toString('hex')).toBe(vector.hex)
      expect(() =>
        pair.readRequest(Buffer.concat([Buffer.from(vector.hex, 'hex'), Buffer.from([0])]))
      ).toThrow(/after/)
      expect(() => pair.readRequest(Buffer.from(vector.hex, 'hex').subarray(0, -1))).toThrow()
    }
  )

  test('the hand-computed ApiVersions feature golden selects transaction version two', async () => {
    const golden = expected.apiVersionsTransactionV2
    const pair = require('../wire/apiVersionsV4')
    expect(pair.buildResponse(golden.fields).toString('hex')).toBe(golden.hex)
    const { decode } = require('../../src/protocol/requests/apiVersions/v4/response')
    const decoded = await decode(Buffer.from(golden.hex, 'hex'))
    expect(decoded.finalizedFeatures).toEqual(golden.fields.finalizedFeatures)
    expect(Number(decoded.finalizedFeaturesEpoch)).toBe(7)
    const TransactionVersion = require('../../src/producer/state/transactionVersion')
    const version = new TransactionVersion()
    expect(
      version.begin({
        finalizedFeaturesEpoch: Number(decoded.finalizedFeaturesEpoch),
        finalizedFeatures: Object.fromEntries(
          decoded.finalizedFeatures.map(({ name, ...levels }) => [name, levels])
        ),
      })
    ).toEqual({ version: 2, bumpRequired: false })
  })
})

describe('Metadata v7 pair', () => {
  const pair = require('../wire/metadataV7')
  test('reads independent classic request literals', () => {
    expect(pair.readRequest(Buffer.from('00000001000a746573742d746f70696301', 'hex'))).toEqual({
      topics: ['test-topic'],
      allowAutoTopicCreation: true,
    })
    expect(pair.readRequest(Buffer.from('ffffffff00', 'hex'))).toEqual({
      topics: null,
      allowAutoTopicCreation: false,
    })
  })
  test('matches the independent classic response layout literals', async () => {
    const hex =
      '00000000000000010000000000093132372e302e302e3100002384ffff000f746573742d636c75737465722d696400000000000000010000000a746573742d746f706963000000000100000000000000000001000000050000000100000001000000010000000100000000'
    expect(pair.buildResponse().toString('hex')).toBe(hex)
    const result = await require('../../src/protocol/requests/metadata/v7/response').decode(
      Buffer.from(hex, 'hex')
    )
    expect(result.topicMetadata[0]).toMatchObject({
      topic: 'test-topic',
      partitionMetadata: [
        {
          partitionId: 0,
          leader: 1,
          leaderEpoch: 5,
          replicas: [1],
          isr: [1],
          offlineReplicas: [],
          partitionErrorCode: 0,
        },
      ],
    })
    expect(result.topicMetadata[0].topicAuthorizedOperations).toBeUndefined()
  })
  test('rejects a cut request and trailing bytes', () => {
    expect(() => pair.readRequest(Buffer.from('00000001000a74', 'hex'))).toThrow()
    expect(() =>
      pair.readRequest(Buffer.from('00000001000a746573742d746f7069630100', 'hex'))
    ).toThrow()
  })
})
