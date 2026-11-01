/**
 * The fields of the wire vectors that carry bytes only (`{ type: 'Buffer', data }`), written out
 * as literals from the Java generator that produced them, never obtained by decoding the bytes.
 * The metadataV7 construction literals are schema-derived until their separately required
 * Java serialization/provenance comparison succeeds; its source entry identifies that gate.
 * Each entry names its source; the structured vectors (`hex` and `fields`) need no entry because
 * the vector holds its own fields.
 *
 * `request` is what a reader must return for the request vector; `response` is what the response
 * builder is given, and must turn into the response vector's bytes.
 *
 * `capture` is the same for the messages a Kafka 4.3.1 broker exchanged with this client in
 * scripts/protocol/capture.js: the fields are written out by hand from the captured bytes under
 * the Apache message layout, never decoded. The script fixes the topic, the group and the
 * normalised topic id; the partition, epoch and offset values are what this one capture holds.
 *
 * Generator: scripts/protocol/javaVectors/GenerateVectors.java, kafka-clients 4.3.1; the line
 * numbers below are that file's.
 */
module.exports = {
  metadataV7: {
    source:
      'MetadataRequest/Response 4.3.1 v7 field literals; independent Java v7 serialization acceptance required',
    request: {
      topics: ['test-topic'],
      allowAutoTopicCreation: true,
    },
    response: {
      throttleTime: 0,
      brokers: [
        {
          nodeId: 0,
          host: '127.0.0.1',
          port: 9092,
          rack: null,
        },
      ],
      clusterId: 'test-cluster-id',
      controllerId: 0,
      topics: [
        {
          errorCode: 0,
          name: 'test-topic',
          isInternal: false,
          partitions: [
            {
              errorCode: 0,
              partitionIndex: 0,
              leaderId: 1,
              leaderEpoch: 5,
              replicas: [1],
              isr: [1],
              offlineReplicas: [],
            },
          ],
        },
      ],
    },
  },
  apiVersionsV4: {
    source:
      'buildApiVersionsRequest l.262 (client software l.59-60), buildApiVersionsResponse l.275',
    request: { clientSoftwareName: 'countly-kafkajs', clientSoftwareVersion: '3.0.0-alpha.4' },
    response: {
      errorCode: 0,
      apiKeys: [
        { apiKey: 0, minVersion: 3, maxVersion: 13 },
        { apiKey: 18, minVersion: 0, maxVersion: 4 },
      ],
      throttleTime: 0,
      supportedFeatures: [{ name: 'metadata.version', minVersion: 1, maxVersion: 25 }],
      finalizedFeaturesEpoch: 7,
      finalizedFeatures: [{ name: 'metadata.version', minVersionLevel: 1, maxVersionLevel: 25 }],
    },
  },

  metadataV13: {
    source: 'buildMetadataRequest l.323, buildMetadataResponse l.352',
    request: {
      topics: [{ topicId: null, name: 'test-topic' }],
      allowAutoTopicCreation: true,
      includeTopicAuthorizedOperations: false,
    },
    response: {
      throttleTime: 0,
      brokers: [{ nodeId: 0, host: '127.0.0.1', port: 9092, rack: null }],
      clusterId: 'test-cluster-id',
      controllerId: 0,
      topics: [
        {
          errorCode: 0,
          name: 'test-topic',
          topicId: '01020304-0506-0708-090a-0b0c0d0e0f10',
          isInternal: false,
          partitions: [
            {
              errorCode: 0,
              partitionIndex: 0,
              leaderId: 1,
              leaderEpoch: 5,
              replicas: [1],
              isr: [1],
              offlineReplicas: [],
            },
          ],
          topicAuthorizedOperations: -2147483648,
        },
      ],
      errorCode: 0,
    },
  },

  findCoordinatorV6: {
    source: 'buildFindCoordinatorRequest l.505, buildFindCoordinatorResponse l.524',
    request: { keyType: 0, coordinatorKeys: ['test-group'] },
    response: {
      throttleTime: 0,
      coordinators: [
        {
          key: 'test-group',
          nodeId: 1,
          host: '127.0.0.1',
          port: 9092,
          errorCode: 0,
          errorMessage: null,
        },
      ],
    },
    capture: {
      source:
        'scripts/protocol/capture.js CAPTURE_GROUP and driveConsumer; v6_*_kafka431.json, hand-transcribed using clients/src/main/resources/common/message/FindCoordinator{Request,Response}.json at Kafka 4.3.1',
      request: { keyType: 0, coordinatorKeys: ['kafkajs-capture-group'] },
      response: {
        throttleTime: 0,
        coordinators: [
          {
            key: 'kafkajs-capture-group',
            nodeId: 1,
            host: 'localhost',
            port: 9092,
            errorCode: 0,
            errorMessage: '',
          },
        ],
      },
    },
  },

  listOffsetsV9: {
    source: 'buildListOffsetsRequest l.685, buildListOffsetsResponse l.707',
    request: {
      replicaId: -1,
      isolationLevel: 0,
      topics: [
        {
          name: 'test-topic',
          partitions: [{ partitionIndex: 0, currentLeaderEpoch: 5, timestamp: '-2' }],
        },
      ],
    },
    response: {
      throttleTime: 0,
      topics: [
        {
          name: 'test-topic',
          partitions: [
            { partitionIndex: 0, errorCode: 0, timestamp: '-1', offset: '100', leaderEpoch: 5 },
          ],
        },
      ],
    },
    capture: {
      source:
        'scripts/protocol/capture.js CAPTURE_TOPIC and driveConsumer; v9_*_kafka431.json, hand-transcribed using clients/src/main/resources/common/message/ListOffsets{Request,Response}.json at Kafka 4.3.1',
      request: {
        replicaId: -1,
        isolationLevel: 1,
        topics: [
          {
            name: 'kafkajs-capture-topic',
            partitions: [{ partitionIndex: 1, currentLeaderEpoch: 0, timestamp: '-2' }],
          },
        ],
      },
      response: {
        throttleTime: 0,
        topics: [
          {
            name: 'kafkajs-capture-topic',
            partitions: [
              { partitionIndex: 1, errorCode: 0, timestamp: '-1', offset: '0', leaderEpoch: 0 },
            ],
          },
        ],
      },
    },
  },

  fetchV12: {
    source: 'buildFetchRequest l.815, buildFetchResponse l.860',
    request: {
      apiVersion: 12,
      replicaId: -1,
      maxWaitMs: 5000,
      minBytes: 1,
      maxBytes: 10485760,
      isolationLevel: 1,
      sessionId: 0,
      sessionEpoch: -1,
      topics: [
        {
          topic: 'test-topic',
          topicId: null,
          partitions: [
            {
              partition: 0,
              currentLeaderEpoch: 5,
              fetchOffset: '42',
              lastFetchedEpoch: -1,
              logStartOffset: '-1',
              maxBytes: 1048576,
            },
          ],
        },
      ],
      forgottenTopics: [],
      rackId: '',
    },
    response: {
      partitions: [{ partition: 0, errorCode: 0 }],
    },
  },

  fetchV18: {
    source: 'buildFetchRequest l.815, buildFetchResponse l.860',
    request: {
      apiVersion: 18,
      replicaId: null,
      maxWaitMs: 5000,
      minBytes: 1,
      maxBytes: 10485760,
      isolationLevel: 1,
      sessionId: 0,
      sessionEpoch: -1,
      topics: [
        {
          topic: null,
          topicId: '01020304-0506-0708-090a-0b0c0d0e0f10',
          partitions: [
            {
              partition: 0,
              currentLeaderEpoch: 5,
              fetchOffset: '42',
              lastFetchedEpoch: -1,
              logStartOffset: '-1',
              maxBytes: 1048576,
            },
          ],
        },
      ],
      forgottenTopics: [],
      rackId: '',
    },
    response: {
      partitions: [{ partition: 0, errorCode: 0 }],
      nodeEndpoints: [{ nodeId: 2, host: '127.0.0.1', port: 9092, rack: null }],
    },
    capture: {
      source:
        'scripts/protocol/capture.js TOPIC_ID_SENTINEL, normalise and driveConsumer; v18_request_kafka431.json, hand-transcribed using clients/src/main/resources/common/message/FetchRequest.json at Kafka 4.3.1',
      request: {
        apiVersion: 18,
        replicaId: null,
        maxWaitMs: 5000,
        minBytes: 1,
        maxBytes: 10485760,
        isolationLevel: 1,
        sessionId: 0,
        sessionEpoch: -1,
        topics: [
          {
            topic: null,
            topicId: '01020304-0506-0708-090a-0b0c0d0e0f10',
            partitions: [
              {
                partition: 1,
                currentLeaderEpoch: 0,
                fetchOffset: '0',
                lastFetchedEpoch: -1,
                logStartOffset: '-1',
                maxBytes: 1048576,
              },
            ],
          },
        ],
        forgottenTopics: [],
        rackId: '',
      },
    },
  },

  offsetForLeaderEpochV4: {
    source: 'buildOffsetForLeaderEpochRequest l.910, buildOffsetForLeaderEpochResponse l.933',
    request: {
      replicaId: -1,
      topics: [
        {
          topic: 'test-topic',
          partitions: [{ partition: 0, currentLeaderEpoch: 5, leaderEpoch: 4 }],
        },
      ],
    },
    response: {
      partitions: [{ partition: 0, errorCode: 0, leaderEpoch: 5, endOffset: '120' }],
    },
  },

  initProducerIdV5: {
    source: 'buildInitProducerIdResponse l.750; the request is the structured vectors',
    response: { throttleTime: 0, errorCode: 0, producerId: '1000', producerEpoch: 0 },
    capture: {
      source:
        'scripts/protocol/capture.js CAPTURE_TRANSACTIONAL_ID and driveProducers (the default transaction timeout, 60000 ms); v5_*_kafka431.json, hand-transcribed using clients/src/main/resources/common/message/InitProducerId{Request,Response}.json at Kafka 4.3.1',
      request: {
        transactionalId: 'kafkajs-capture-txn',
        transactionTimeout: 60000,
        producerId: '-1',
        producerEpoch: -1,
      },
      response: { throttleTime: 0, errorCode: 0, producerId: '1000', producerEpoch: 0 },
    },
  },

  produceV11: {
    source: 'buildProduceRequest l.414, buildProduceResponse l.449',
    request: {
      transactionalId: null,
      acks: -1,
      timeoutMs: 30000,
      topics: [{ topic: 'test-topic', partitions: [{ partition: 0, batches: null }] }],
    },
    response: {
      responses: [
        {
          topic: 'test-topic',
          partitions: [
            {
              partition: 0,
              errorCode: 0,
              baseOffset: '100',
              logAppendTime: '-1',
              logStartOffset: '0',
              leader: { leaderId: 2, leaderEpoch: 7 },
            },
          ],
        },
      ],
      throttleTime: 0,
      nodeEndpoints: [{ nodeId: 2, host: '127.0.0.1', port: 9092, rack: null }],
    },
    capture: {
      source:
        'scripts/protocol/capture.js CAPTURE_TOPIC, TOPIC_ID_SENTINEL and driveProducers (the request is not captured: it carries a record batch); v11_response_kafka431.json, hand-transcribed using clients/src/main/resources/common/message/ProduceResponse.json at Kafka 4.3.1; the broker sends no KIP-951 tagged field while leadership has not moved',
      response: {
        responses: [
          {
            topic: 'kafkajs-capture-topic',
            partitions: [
              {
                partition: 0,
                errorCode: 0,
                baseOffset: '0',
                logAppendTime: '-1',
                logStartOffset: '0',
                leader: null,
              },
            ],
          },
        ],
        throttleTime: 0,
        nodeEndpoints: [],
      },
    },
  },

  produceV12: {
    source: 'buildProduceRequest l.414, buildProduceResponse l.449',
    request: {
      transactionalId: null,
      acks: -1,
      timeoutMs: 30000,
      topics: [{ topic: 'test-topic', partitions: [{ partition: 0, batches: null }] }],
    },
    response: {
      responses: [
        {
          topic: 'test-topic',
          partitions: [
            {
              partition: 0,
              errorCode: 0,
              baseOffset: '100',
              logAppendTime: '-1',
              logStartOffset: '0',
              leader: { leaderId: 2, leaderEpoch: 7 },
            },
          ],
        },
      ],
      throttleTime: 0,
      nodeEndpoints: [{ nodeId: 2, host: '127.0.0.1', port: 9092, rack: null }],
    },
  },

  produceV13: {
    source: 'buildProduceRequest l.414, buildProduceResponse l.449',
    request: {
      transactionalId: null,
      acks: -1,
      timeoutMs: 30000,
      topics: [
        {
          topicId: '01020304-0506-0708-090a-0b0c0d0e0f10',
          partitions: [{ partition: 0, batches: null }],
        },
      ],
    },
    response: {
      responses: [
        {
          topicId: '01020304-0506-0708-090a-0b0c0d0e0f10',
          partitions: [
            {
              partition: 0,
              errorCode: 0,
              baseOffset: '100',
              logAppendTime: '-1',
              logStartOffset: '0',
              leader: { leaderId: 2, leaderEpoch: 7 },
            },
          ],
        },
      ],
      throttleTime: 0,
      nodeEndpoints: [{ nodeId: 2, host: '127.0.0.1', port: 9092, rack: null }],
    },
    capture: {
      source:
        'scripts/protocol/capture.js CAPTURE_TOPIC, TOPIC_ID_SENTINEL and driveProducers (the request is not captured: it carries a record batch); v13_response_kafka431.json, hand-transcribed using clients/src/main/resources/common/message/ProduceResponse.json at Kafka 4.3.1; the broker sends no KIP-951 tagged field while leadership has not moved',
      response: {
        responses: [
          {
            topicId: '01020304-0506-0708-090a-0b0c0d0e0f10',
            partitions: [
              {
                partition: 0,
                errorCode: 0,
                baseOffset: '0',
                logAppendTime: '-1',
                logStartOffset: '0',
                leader: null,
              },
            ],
          },
        ],
        throttleTime: 0,
        nodeEndpoints: [],
      },
    },
  },

  /**
   * Hand-computed goldens: the Java Produce vectors carry a null `Records` field and a null
   * `TransactionalId`, so the batch header and a populated transactional id have no Java bytes.
   * Each hex literal below was assembled from the documented layout with plain `Buffer` calls
   * (never with a codec of this repository) and is written out here; the fields beside it are the
   * same values tabulated by hand.
   * @see https://kafka.apache.org/documentation/#recordbatch
   */
  recordBatchHeaderV2: {
    source: 'the RecordBatch v2 layout, 61 header bytes, no records',
    hex:
      '0000000000000000' + // base offset 0
      '00000031' + // batch length 49: the bytes after this field
      'ffffffff' + // partition leader epoch -1
      '02' + // magic 2
      '00000000' + // crc, not computed
      '0010' + // attributes: transactional
      '00000001' + // last offset delta 1
      '0000018bcfe56800' + // base timestamp
      '0000018bcfe56801' + // max timestamp
      '00000000000003e8' + // producer id 1000
      '0003' + // producer epoch 3
      '00000007' + // base sequence 7
      '00000002', // record count 2
    fields: {
      producerId: '1000',
      producerEpoch: 3,
      baseSequence: 7,
      recordCount: 2,
      transactional: true,
      control: false,
      digest: '6a5c2f88f9c1afc1f66e6a138b97faa92363d895',
    },
  },

  produceRequestTransactionalV11: {
    source: 'the Produce v11 request layout around recordBatchHeaderV2, transactional id "tx-1"',
    hex:
      '05' +
      '74782d31' + // transactional id "tx-1"
      'ffff' + // acks -1
      '00007530' + // timeout 30000
      '02' + // one topic
      '0b' +
      '746573742d746f706963' + // "test-topic"
      '02' + // one partition
      '00000000' + // partition index 0
      '3e' + // records: 61 bytes
      '0000000000000000' +
      '00000031' +
      'ffffffff' +
      '02' +
      '00000000' +
      '0010' +
      '00000001' +
      '0000018bcfe56800' +
      '0000018bcfe56801' +
      '00000000000003e8' +
      '0003' +
      '00000007' +
      '00000002' +
      '00' +
      '00' +
      '00', // partition, topic and request tags
    fields: {
      transactionalId: 'tx-1',
      acks: -1,
      timeoutMs: 30000,
      topics: [
        {
          topic: 'test-topic',
          partitions: [
            {
              partition: 0,
              batches: [
                {
                  producerId: '1000',
                  producerEpoch: 3,
                  baseSequence: 7,
                  recordCount: 2,
                  transactional: true,
                  control: false,
                  digest: '6a5c2f88f9c1afc1f66e6a138b97faa92363d895',
                },
              ],
            },
          ],
        },
      ],
    },
  },

  /**
   * SaslHandshake v1 has no vector: the goldens below are assembled by hand from the message
   * layout (an INT16-prefixed string; an INT16 error code and an INT32-counted array of
   * INT16-prefixed strings), never by a codec of this repository.
   */
  saslHandshakeV1: {
    source: 'the SaslHandshake v1 layout; no Java vector exists',
    requests: [
      { hex: '0005' + '504c41494e', fields: { mechanism: 'PLAIN' } },
      { hex: '000b' + '4f41555448424541524552', fields: { mechanism: 'OAUTHBEARER' } },
    ],
    responses: [
      {
        hex: '0000' + '00000002' + '0005' + '504c41494e' + '000d' + '534352414d2d5348412d323536',
        fields: { errorCode: 0, enabledMechanisms: ['PLAIN', 'SCRAM-SHA-256'] },
      },
      {
        hex: '0021' + '00000001' + '0005' + '504c41494e',
        fields: { errorCode: 33, enabledMechanisms: ['PLAIN'] },
      },
      { hex: '0000' + '00000000', fields: { errorCode: 0, enabledMechanisms: [] } },
    ],
  },

  saslAuthenticateV2: {
    source:
      'buildSaslAuthenticateRequest l.956 (SASL_AUTH_BYTES l.102), buildSaslAuthenticateResponse l.966 (SASL_SESSION_LIFETIME_MS l.103)',
    request: { authBytes: Buffer.from('server-final-message') },
    response: {
      errorCode: 0,
      errorMessage: null,
      authBytes: Buffer.from('server-final-message'),
      sessionLifetimeMs: '3600000',
    },
  },

  /**
   * ListPartitionReassignments has no Java vector. `capture` is what the Kafka 4.3.1 broker's own
   * bytes hold: the request is the capture script's call (`listPartitionReassignments({ topics:
   * null })` with the client's default timeout, 5000 ms), the response the documented answer while
   * no reassignment is in flight. `inFlight` and `named` are hand-computed goldens for a
   * reassignment under way and a request for named partitions.
   */
  listPartitionReassignmentsV0: {
    source:
      'scripts/protocol/capture.js (admin.listPartitionReassignments({ topics: null })); v0_*_kafka431.json, hand-transcribed using clients/src/main/resources/common/message/ListPartitionReassignments{Request,Response}.json at Kafka 4.3.1',
    capture: {
      request: { timeoutMs: 5000, topics: null },
      response: { throttleTime: 0, errorCode: 0, errorMessage: null, topics: [] },
    },
    named: {
      hex: '00001388' + '02' + '02' + '74' + '03' + '00000000' + '00000001' + '00' + '00',
      fields: { timeoutMs: 5000, topics: [{ name: 't', partitionIndexes: [0, 1] }] },
    },
    inFlight: {
      hex:
        '00000000' + // throttle time
        '0000' + // error code
        '00' + // error message: null
        '02' + // one topic
        '02' +
        '74' + // name "t"
        '02' + // one partition
        '00000000' + // partition index 0
        '03' +
        '00000001' +
        '00000002' + // replicas 1, 2
        '02' +
        '00000003' + // adding replicas 3
        '01' + // removing replicas: none
        '00' +
        '00' +
        '00', // partition, topic and response tags
      fields: {
        throttleTime: 0,
        errorCode: 0,
        errorMessage: null,
        topics: [
          {
            name: 't',
            partitions: [
              { partitionIndex: 0, replicas: [1, 2], addingReplicas: [3], removingReplicas: [] },
            ],
          },
        ],
      },
    },
  },
  /**
   * Hand-computed ApiVersions v4 response body: error 0, empty API table, throttle 0; tags 0
   * (empty supported array, one byte), 1 (eight-byte epoch 7), and 2 (one finalized feature,
   * 26 bytes: array count 2, compact name length 20, 19 UTF-8 name bytes, max/min 2, empty tags).
   * @see https://github.com/apache/kafka/blob/4.3.1/clients/src/main/resources/common/message/ApiVersionsResponse.json
   */
  apiVersionsTransactionV2: {
    source:
      'ApiVersionsResponse v4 schema; integer widths and compact-array/tag lengths computed by hand',
    hex: '000001000000000300010101080000000000000007021a02147472616e73616374696f6e2e76657273696f6e0002000200',
    fields: {
      errorCode: 0,
      apiKeys: [],
      throttleTime: 0,
      supportedFeatures: [],
      finalizedFeaturesEpoch: 7,
      finalizedFeatures: [{ name: 'transaction.version', minVersionLevel: 2, maxVersionLevel: 2 }],
    },
  },
}

module.exports.offsetDeleteV0 = {
  source:
    'OffsetDeleteRequestData and OffsetDeleteResponseData; generated vector acceptance required',
  request: {
    groupId: 'test-group',
    topics: [{ topic: 'test-topic', partitions: [{ partition: 0 }, { partition: 1 }] }],
  },
  response: {
    errorCode: 0,
    throttleTime: 17,
    topics: [
      {
        topic: 'test-topic',
        partitions: [
          { partition: 0, errorCode: 0 },
          { partition: 1, errorCode: 3 },
        ],
      },
    ],
  },
}
module.exports.offsetFetchV7 = {
  source:
    'OffsetFetchRequestData and OffsetFetchResponseData v7; generated vector acceptance required',
  request: {
    groupId: 'test-group',
    topics: [{ topic: 'test-topic', partitions: [{ partition: 0 }] }],
    requireStable: true,
  },
  response: {
    errorCode: 0,
    throttleTime: 17,
    topics: [
      {
        topic: 'test-topic',
        partitions: [{ partition: 0, offset: '10', leaderEpoch: 3, metadata: null, errorCode: 0 }],
      },
    ],
  },
}

module.exports.describeLogDirsV1 = {
  source: 'Kafka 4.3.1 schema literals; Java comparison required',
  request: {
    topics: [
      {
        topic: 't',
        partitions: [0],
      },
    ],
  },
  response: {
    throttleTime: 17,
    errorCode: 0,
    logDirs: [
      {
        errorCode: 0,
        path: '/data/a',
        topics: [
          {
            topic: 't',
            partitions: [
              {
                partition: 0,
                size: '9007199254740993',
                offsetLag: '2',
                isFuture: true,
              },
            ],
          },
        ],
      },
    ],
  },
}
module.exports.describeLogDirsV2 = {
  source: 'Kafka 4.3.1 schema literals; Java comparison required',
  request: {
    topics: [
      {
        topic: 't',
        partitions: [0],
      },
    ],
  },
  response: {
    throttleTime: 17,
    errorCode: 0,
    logDirs: [
      {
        errorCode: 0,
        path: '/data/a',
        topics: [
          {
            topic: 't',
            partitions: [
              {
                partition: 0,
                size: '9007199254740993',
                offsetLag: '2',
                isFuture: true,
              },
            ],
          },
        ],
      },
    ],
  },
}
module.exports.describeLogDirsV3 = {
  source: 'Kafka 4.3.1 schema literals; Java comparison required',
  request: {
    topics: [
      {
        topic: 't',
        partitions: [0],
      },
    ],
  },
  response: {
    throttleTime: 17,
    errorCode: 0,
    logDirs: [
      {
        errorCode: 0,
        path: '/data/a',
        topics: [
          {
            topic: 't',
            partitions: [
              {
                partition: 0,
                size: '9007199254740993',
                offsetLag: '2',
                isFuture: true,
              },
            ],
          },
        ],
      },
    ],
  },
}
module.exports.describeLogDirsV4 = {
  source: 'Kafka 4.3.1 schema literals; Java comparison required',
  request: {
    topics: [
      {
        topic: 't',
        partitions: [0],
      },
    ],
  },
  response: {
    throttleTime: 17,
    errorCode: 0,
    logDirs: [
      {
        errorCode: 0,
        path: '/data/a',
        topics: [
          {
            topic: 't',
            partitions: [
              {
                partition: 0,
                size: '9007199254740993',
                offsetLag: '2',
                isFuture: true,
              },
            ],
          },
        ],
        totalBytes: '9007199254740995',
        usableBytes: '9007199254740994',
      },
    ],
  },
}
module.exports.describeLogDirsV5 = {
  source: 'Kafka 4.3.1 schema literals; Java comparison required',
  request: {
    topics: [
      {
        topic: 't',
        partitions: [0],
      },
    ],
  },
  response: {
    throttleTime: 17,
    errorCode: 0,
    logDirs: [
      {
        errorCode: 0,
        path: '/data/a',
        topics: [
          {
            topic: 't',
            partitions: [
              {
                partition: 0,
                size: '9007199254740993',
                offsetLag: '2',
                isFuture: true,
              },
            ],
          },
        ],
        totalBytes: '9007199254740995',
        usableBytes: '9007199254740994',
        isCordoned: true,
      },
    ],
  },
}
module.exports.alterReplicaLogDirsV1 = {
  source: 'Kafka 4.3.1 schema literals; Java comparison required',
  request: {
    dirs: [
      {
        path: '/data/b',
        topics: [
          {
            topic: 't',
            partitions: [0],
          },
        ],
      },
    ],
  },
  response: {
    throttleTime: 17,
    topics: [
      {
        topic: 't',
        partitions: [
          {
            partition: 0,
            errorCode: 57,
          },
        ],
      },
    ],
  },
}
module.exports.alterReplicaLogDirsV2 = {
  source: 'Kafka 4.3.1 schema literals; Java comparison required',
  request: {
    dirs: [
      {
        path: '/data/b',
        topics: [
          {
            topic: 't',
            partitions: [0],
          },
        ],
      },
    ],
  },
  response: {
    throttleTime: 17,
    topics: [
      {
        topic: 't',
        partitions: [
          {
            partition: 0,
            errorCode: 57,
          },
        ],
      },
    ],
  },
}
module.exports.listOffsetsV10 = {
  source: 'Kafka 4.3.1 schema literals; Java comparison required',
  request: {
    replicaId: -1,
    isolationLevel: 1,
    topics: [
      {
        topic: 't',
        partitions: [
          {
            partition: 0,
            currentLeaderEpoch: -1,
            timestamp: '-5',
          },
        ],
      },
    ],
    timeout: 5000,
  },
  response: {
    throttleTime: 17,
    responses: [
      {
        topic: 't',
        partitions: [
          {
            partition: 0,
            errorCode: 0,
            timestamp: '-1',
            offset: '-1',
            leaderEpoch: -1,
          },
        ],
      },
    ],
  },
}
module.exports.listOffsetsV11 = {
  source: 'Kafka 4.3.1 schema literals; Java comparison required',
  request: {
    replicaId: -1,
    isolationLevel: 1,
    topics: [
      {
        topic: 't',
        partitions: [
          {
            partition: 0,
            currentLeaderEpoch: -1,
            timestamp: '-6',
          },
        ],
      },
    ],
    timeout: 5000,
  },
  response: {
    throttleTime: 17,
    responses: [
      {
        topic: 't',
        partitions: [
          {
            partition: 0,
            errorCode: 0,
            timestamp: '-1',
            offset: '-1',
            leaderEpoch: -1,
          },
        ],
      },
    ],
  },
}
module.exports.alterPartitionReassignmentsV1 = {
  source: 'Kafka 4.3.1 schema literals; Java comparison required',
  request: {
    timeout: 5000,
    allowReplicationFactorChange: false,
    topics: [
      {
        topic: 't',
        partitionAssignment: [
          {
            partition: 0,
            replicas: [1, 2],
          },
        ],
      },
    ],
  },
  response: {
    throttleTime: 17,
    allowReplicationFactorChange: false,
    errorCode: 0,
    errorMessage: null,
    responses: [
      {
        topic: 't',
        partitions: [
          {
            partition: 0,
            errorCode: 0,
            errorMessage: null,
          },
        ],
      },
    ],
  },
}
module.exports.deleteTopicsV6 = {
  source: 'Kafka 4.3.1 schema literals; Java comparison required',
  request: {
    topics: [
      {
        topic: null,
        topicId: '00112233-4455-6677-8899-aabbccddeeff',
      },
    ],
    timeout: 5000,
  },
  response: {
    throttleTime: 17,
    topicErrors: [
      {
        topic: null,
        topicId: '00112233-4455-6677-8899-aabbccddeeff',
        errorCode: 89,
        errorMessage: 'quota',
      },
    ],
  },
}

Object.assign(module.exports.describeLogDirsV1, {
  requestHex: '000000010001740000000100000000',
  responseHex:
    '0000001100000001000000072f646174612f610000000100017400000001000000000020000000000001000000000000000201',
})
Object.assign(module.exports.describeLogDirsV2, {
  requestHex: '02027402000000000000',
  responseHex:
    '00000011020000082f646174612f610202740200000000002000000000000100000000000000020100000000',
})
Object.assign(module.exports.describeLogDirsV3, {
  requestHex: '02027402000000000000',
  responseHex:
    '000000110000020000082f646174612f610202740200000000002000000000000100000000000000020100000000',
})
Object.assign(module.exports.describeLogDirsV4, {
  requestHex: '02027402000000000000',
  responseHex:
    '000000110000020000082f646174612f61020274020000000000200000000000010000000000000002010000002000000000000300200000000000020000',
})
Object.assign(module.exports.describeLogDirsV5, {
  requestHex: '02027402000000000000',
  responseHex:
    '000000110000020000082f646174612f6102027402000000000020000000000001000000000000000201000000200000000000030020000000000002010000',
})
Object.assign(module.exports.alterReplicaLogDirsV1, {
  requestHex: '0000000100072f646174612f62000000010001740000000100000000',
  responseHex: '000000110000000100017400000001000000000039',
})
Object.assign(module.exports.alterReplicaLogDirsV2, {
  requestHex: '02082f646174612f620202740200000000000000',
  responseHex: '0000001102027402000000000039000000',
})
Object.assign(module.exports.listOffsetsV10, {
  requestHex: 'ffffffff010202740200000000fffffffffffffffffffffffb00000000138800',
  responseHex: '0000001102027402000000000000ffffffffffffffffffffffffffffffffffffffff000000',
})
Object.assign(module.exports.listOffsetsV11, {
  requestHex: 'ffffffff010202740200000000fffffffffffffffffffffffa00000000138800',
  responseHex: '0000001102027402000000000000ffffffffffffffffffffffffffffffffffffffff000000',
})
Object.assign(module.exports.alterPartitionReassignmentsV1, {
  requestHex: '00001388000202740200000000030000000100000002000000',
  responseHex: '00000011000000000202740200000000000000000000',
})
Object.assign(module.exports.deleteTopicsV6, {
  requestHex: '020000112233445566778899aabbccddeeff000000138800',
  responseHex: '00000011020000112233445566778899aabbccddeeff00590671756f74610000',
})
