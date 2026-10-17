/**
 * The fields of the wire vectors that carry bytes only (`{ type: 'Buffer', data }`), written out
 * as literals from the Java generator that produced them, never obtained by decoding the bytes.
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
}
