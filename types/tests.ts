import * as fs from 'fs'

import * as kafkajs from './index'
import {
  Producer,
  Kafka,
  PartitionAssigners,
  logLevel,
  CompressionTypes,
  CompressionCodecs,
  ConfigResourceTypes,
  ConfigSource,
  ConfigTypes,
  AlterConfigOpTypes,
  IncrementalAlterConfigEntry,
  IncrementalAlterConfigsResult,
  RemoveMembersResult,
  ITopicMetadata,
  ConfigEntries,
  DescribeTopicPartitionsPartition,
  DescribeTopicPartitionsTopic,
  Consumer,
  AclResourceTypes,
  AclOperationTypes,
  AclPermissionTypes,
  ResourcePatternTypes,
  LogEntry,
  KafkaJSError,
  KafkaJSOffsetOutOfRange,
  KafkaJSNumberOfRetriesExceeded,
  KafkaJSConnectionError,
  KafkaJSRequestTimeoutError,
  KafkaJSTopicMetadataNotLoaded,
  KafkaJSStaleTopicMetadataAssignment,
  PartitionMetadata,
  KafkaJSServerDoesNotSupportApiKey,
  AssignerProtocol,
  MemberMetadata,
  Assigner,
  GroupMember,
  ListGroupsOptions,
  GroupDescription,
  MemberDescription,
  ConsumerGroupState,
  TopicPartitions,
  Partitioners,
  Broker,
  Offsets,
  ConsumerGroupMetadata,
  TransactionOffsets,
  KafkaJSErrorCategory,
  KafkaJSNonRetriableError,
  KafkaJSRefreshRetriableError,
  KafkaJSTransactionAbortableError,
  KafkaJSApplicationRecoverableError,
  KafkaJSInvalidConfigurationError,
  KafkaJSDeliveryTimeout,
  KafkaJSProducerClosedError,
  KafkaJSAggregateError,
  KafkaJSFetcherRebalanceError,
  KafkaJSLockTimeout,
  KafkaJSProtocolError,
  KafkaJSInvalidFrameError,
  KafkaJSCorruptRecordError,
  KafkaJSCommitFailedError,
  KafkaJSBufferExhaustedError,
  KafkaJSRecordTooLargeError,
  KafkaJSConnectionClosedError,
  KafkaJSMemberIdRequired,
  KafkaJSInvariantViolation,
  KafkaJSInvalidVarIntError,
  KafkaJSInvalidLongError,
  KafkaJSCreateTopicError,
  KafkaJSNoBrokerAvailableError,
  Cluster,
} from './index'

const { roundRobin } = PartitionAssigners

// COMMON
const host = 'localhost'
const topic = 'topic-test'

const kafka = new Kafka({
  logLevel: logLevel.INFO,
  brokers: [`${host}:9094`, `${host}:9097`, `${host}:9100`],
  clientId: 'example-consumer',
  ssl: {
    rejectUnauthorized: false,
    ca: [fs.readFileSync('./testHelpers/certs/cert-signed', 'utf-8')],
  },
  sasl: {
    mechanism: 'plain',
    username: 'test',
    password: 'testtest',
  },
  logCreator: (logLevel: logLevel) => (entry: LogEntry) => {
    Date.parse(entry.log.timestamp);
  },
})

kafka.logger().error('Instantiated KafkaJS')

// CONSUMER
const consumer = kafka.consumer({ groupId: 'test-group' })
// @ts-expect-error message delivery uses run; there is no poll method
consumer.poll()
consumer.logger().info('Instantiated logger', { groupId: 'test-group' })

let removeListener = consumer.on(consumer.events.HEARTBEAT, e =>
  console.log(`heartbeat at ${e.timestamp}`)
)
removeListener()

// The committed leader epoch is part of the COMMIT_OFFSETS payload and of the uncommitted offsets
consumer.on(consumer.events.COMMIT_OFFSETS, e => {
  const committedLeaderEpoch: number | undefined = e.payload.topics[0].partitions[0].leaderEpoch
  const committedOffset: string = e.payload.topics[0].partitions[0].offset
  console.log(committedLeaderEpoch, committedOffset)
})

const runConsumer = async () => {
  await consumer.connect()
  await consumer.subscribe({ topics: [topic] })
  await consumer.run({
    eachBatch: async ({
      batch,
      resolveOffset,
      heartbeat,
      commitOffsetsIfNecessary,
      uncommittedOffsets,
      isRunning,
      isStale,
    }) => {
      resolveOffset('123')
      await heartbeat()
      commitOffsetsIfNecessary({
        topics: [
          {
            topic: 'topic-name',
            partitions: [{ partition: 0, offset: '500' }],
          },
        ],
      })
      const uncommitted = uncommittedOffsets()
      const uncommittedLeaderEpoch: number | undefined =
        uncommitted.topics[0].partitions[0].leaderEpoch
      console.log(uncommittedLeaderEpoch)
      isRunning()
      isStale()
      console.log(batch)
      console.log(batch.offsetLagLow())
    },
    eachMessage: async ({ topic, partition, message }) => {
      const prefix = `${topic}[${partition} | ${message.offset}] / ${message.timestamp}`
      console.log(`- ${prefix} ${message.key}#${message.value}`)
    },
  })

  consumer.pause([{ topic: 'topic1' }])
  consumer.pause([{ topic: 'topic2', partitions: [1, 2] }])

  consumer.resume([{ topic: 'topic1' }])
  consumer.resume([{ topic: 'topic1', partitions: [2] }])

  consumer.paused()
  consumer.paused().length
  consumer.paused()[0].topic
  consumer.paused()[0].partitions

  await consumer.commitOffsets([{ topic: 'topic-name', partition: 0, offset: '500' }])
  await consumer.commitOffsets([
    { topic: 'topic-name', partition: 0, offset: '501', metadata: null },
  ])
  await consumer.commitOffsets([
    { topic: 'topic-name', partition: 0, offset: '501', metadata: 'some-metadata' },
  ])
  await consumer.disconnect()
}

runConsumer().catch(console.error)

// PRODUCER
const producer = kafka.producer({ allowAutoTopicCreation: true })
producer.logger().debug('Instantiated producer')

removeListener = producer.on(producer.events.CONNECT, e =>
  console.log(`Producer connect at ${e.timestamp}`)
)
removeListener()

const getRandomNumber = () => Math.round(Math.random() * 1000)
const createMessage = (num: number) => ({
  key: Buffer.from(`key-${num}`),
  value: Buffer.from(`value-${num}-${new Date().toISOString()}`),
})

const sendMessage = () => {
  return producer
    .send({
      topic,
      compression: CompressionTypes.GZIP,
      messages: Array(getRandomNumber())
        .fill(0)
        .map(_ => createMessage(getRandomNumber())),
    })
    .then(console.log)
    .catch(console.error)
}

const runProducer = async () => {
  await producer.connect()
  setInterval(sendMessage, 3000)
  await producer.disconnect()
}

runProducer().catch(console.error)

// ADMIN
const admin = kafka.admin({ retry: { retries: 10 } })
admin.logger().warn('Instantiated admin')

removeListener = admin.on(admin.events.CONNECT, e => console.log(`Admin connect at ${e.timestamp}`))
removeListener()

const runAdmin = async () => {
  await admin.connect()

  const { controller, brokers, clusterId } = await admin.describeCluster()
  admin.logger().debug('Fetched cluster metadata', {
    controller,
    clusterId,
    brokers: brokers.map(({ nodeId, host, port }) => ({
      nodeId,
      host,
      port,
    })),
  })

  await admin.fetchTopicMetadata({ topics: ['string'] }).then(metadata => {
    metadata.topics.forEach(topic => {
      console.log(topic.name, topic.partitions)
    })
  })

  await admin.listTopics()

  await admin.fetchOffsets({ groupId: 'test-group' })
  await admin.fetchOffsets({ groupId: 'test-group', topics: ['topic1', 'topic2'] })

  await admin.createTopics({
    topics: [{ topic, numPartitions: 10, replicationFactor: 1 }],
    timeout: 30000,
    waitForLeaders: true,
  })

  await admin.describeConfigs({
    includeSynonyms: false,
    resources: [
      {
        type: ConfigResourceTypes.TOPIC,
        name: topic,
      },
    ],
  })

  const describeAcls = await admin.describeAcls({
    resourceName: 'topic-name',
    resourceType: AclResourceTypes.TOPIC,
    host: '*',
    permissionType: AclPermissionTypes.ALLOW,
    operation: AclOperationTypes.ANY,
    resourcePatternType: ResourcePatternTypes.LITERAL,
  })
  admin.logger().debug('Describe ACls', {
    resources: describeAcls.resources.map(r => ({
      resourceType: r.resourceType,
      resourceName: r.resourceName,
      resourcePatternType: r.resourcePatternType,
      acls: r.acls,
    })),
  })

  const createAcls = await admin.createAcls({
    acl: [
      {
        resourceType: AclResourceTypes.TOPIC,
        resourceName: 'topic-name',
        resourcePatternType: ResourcePatternTypes.LITERAL,
        principal: 'User:bob',
        host: '*',
        operation: AclOperationTypes.ALL,
        permissionType: AclPermissionTypes.DENY,
      },
    ],
  })
  admin.logger().debug('Create ACls', {
    created: createAcls === true,
  })

  const deleteAcls = await admin.deleteAcls({
    filters: [
      {
        resourceName: 'topic-name',
        resourceType: AclResourceTypes.TOPIC,
        host: '*',
        permissionType: AclPermissionTypes.ALLOW,
        operation: AclOperationTypes.ANY,
        resourcePatternType: ResourcePatternTypes.LITERAL,
      },
    ],
  })
  admin.logger().debug('Delete ACls', {
    filterResponses: deleteAcls.filterResponses.map(f => ({
      errorCode: f.errorCode,
      errorMessage: f.errorMessage,
      machingAcls: f.matchingAcls.map(m => ({
        errorCode: m.errorCode,
        errorMessage: m.errorMessage,
        resourceType: m.resourceType,
        resourceName: m.resourceName,
        resourcePatternType: m.resourcePatternType,
        principal: m.principal,
        host: m.host,
        operation: m.operation,
        permissionType: m.permissionType,
      })),
    })),
  })

  const { groups } = await admin.listGroups()
  const groupIds = groups.map(({ groupId }) => groupId)
  const groupDescription = await admin.describeGroups(groupIds)
  await admin.deleteGroups(groupDescription.groups.map(({ groupId }) => groupId))

  await admin.disconnect()
}

runAdmin().catch(console.error)

// OTHERS
const produceWithGZIP = async () => {
  await producer.send({
    topic: 'topic-name',
    compression: CompressionTypes.GZIP,
    messages: [{ key: Buffer.from('key1'), value: Buffer.from('hello world!') }],
  })
}

produceWithGZIP().catch(console.error)

const SnappyCodec: any = undefined
CompressionCodecs[CompressionTypes.Snappy] = SnappyCodec

kafka.consumer({
  groupId: 'my-group',
  partitionAssigners: [roundRobin],
})

// ERROR
new KafkaJSError('Invalid partition metadata', { retriable: true })
new KafkaJSError('The group is rebalancing')
new KafkaJSError(new Error('💣'), { retriable: true })

new KafkaJSOffsetOutOfRange(new Error(), { topic: topic, partition: 0 })

new KafkaJSNumberOfRetriesExceeded(new Error(), { retryCount: 0, retryTime: 0 })

new KafkaJSConnectionError('Connection error: ECONNREFUSED', {
  broker: `${host}:9094`,
  code: 'ECONNREFUSED',
})
new KafkaJSConnectionError('Connection error: ECONNREFUSED', { code: 'ECONNREFUSED' })

new KafkaJSRequestTimeoutError('Request requestInfo timed out', {
  broker: `${host}:9094`,
  clientId: 'example-consumer',
  correlationId: 0,
  createdAt: 0,
  sentAt: 0,
  pendingDuration: 0,
})

new KafkaJSTopicMetadataNotLoaded('Topic metadata not loaded', { topic: topic })

const partitionMetadata: PartitionMetadata = {
  partitionErrorCode: 0,
  partitionId: 0,
  leader: 2,
  replicas: [2],
  isr: [2],
}
new KafkaJSStaleTopicMetadataAssignment('Topic has been updated', {
  topic: topic,
  unknownPartitions: [partitionMetadata],
})

new KafkaJSServerDoesNotSupportApiKey(
  'The Kafka server does not support the requested API version',
  {
    apiKey: 0,
    apiName: 'Produce',
  }
)

// maxInFlightRequests: null is the documented opt-out, positive integers cap in-flight requests
kafka.producer({ maxInFlightRequests: null })
kafka.consumer({ groupId: 'g', maxInFlightRequests: 5 })

// The membership predicates are internal. If either ever reaches the public surface this stops
// compiling, which is the point: no predicate is exported, and these least of all.
const noInternalPredicateIsPublic: 'isPollIntervalExceeded' | 'isFatalMembership' extends keyof typeof kafkajs
  ? never
  : true = true
void noInternalPredicateIsPublic

// The documented custom assigner still compiles unchanged.
AssignerProtocol.MemberMetadata.encode({ version: 1, topics: ['t'] })
// The new fields are optional and typed.
AssignerProtocol.MemberMetadata.encode({
  version: 3,
  topics: ['t'],
  userData: null,
  ownedPartitions: [{ topic: 't', partitions: [0] }],
  generationId: 1,
  rackId: 'rack-1',
})
const decoded: MemberMetadata | null = AssignerProtocol.MemberMetadata.decode(Buffer.alloc(0))
void decoded

// The four typed listeners.
consumer.on(consumer.events.PARTITIONS_ASSIGNED, e => {
  const topics: string[] = e.payload.partitions.map(({ topic }) => topic)
  void topics
})
// A rejected member id is reported as null, on the rebalance and on the partition events alike.
consumer.on(consumer.events.REBALANCING, e => {
  const memberId: string | null = e.payload.memberId
  void memberId
})
consumer.on(consumer.events.PARTITIONS_LOST, e => {
  const memberId: string | null = e.payload.memberId
  // The generation is gone on this event, so it is nullable too.
  const generationId: number | null = e.payload.generationId
  void memberId
  void generationId
})
consumer.on(consumer.events.PARTITION_HALTED, e => {
  const offset: string = e.payload.offset
  const reason: 'length' | 'magic' | 'count' | 'control' | 'checksum' | 'decompressed-size' | 'decompression' = e.payload.reason
  const error: KafkaJSCorruptRecordError = e.payload.error
  const partition: number = e.payload.partition
  const nextOffset: string | undefined = e.payload.nextOffset
  const codec: 'gzip' | 'snappy' | 'lz4' | 'zstd' | undefined = e.payload.codec
  const limit: number | undefined = e.payload.limit
  // @ts-expect-error an unknown halt reason is not part of the event contract
  const unknownReason: 'unknown' = e.payload.reason
  void offset
  void reason
  void error
  void partition
  void nextOffset
  void codec
  void limit
  void unknownReason
})
consumer.on(consumer.events.POLL_INTERVAL_EXCEEDED, e => {
  const elapsed: number = e.payload.elapsedMs
  const isStatic: boolean = e.payload.isStatic
  const generationId: number | null = e.payload.generationId
  void elapsed
  void isStatic
  void generationId
})
// The option and the deprecated alias both compile.
kafka.consumer({ groupId: 'g', maxPollInterval: 300000 })
kafka.consumer({ groupId: 'g', rebalanceTimeout: 60000 })
// A 2.2.4-shaped assigner literal still satisfies Assigner under strictFunctionTypes.
const legacyAssigner: Assigner = {
  name: 'legacy',
  version: 1,
  assign: async () => [],
  protocol: ({ topics }) => ({ name: 'legacy', metadata: Buffer.from(topics.join(',')) }),
}
void legacyAssigner
// The new built-ins are exported by name.
kafka.consumer({ groupId: 'g', partitionAssigners: [PartitionAssigners.cooperativeSticky] })
// GroupMember carries the optional static id.
const member: GroupMember = {
  memberId: 'm',
  memberMetadata: Buffer.alloc(0),
  groupInstanceId: null,
}
void member

// The two membership options and the prefetch knob compile.
kafka.consumer({ groupId: 'g', groupProtocol: 'consumer' })
kafka.consumer({ groupId: 'g', groupProtocol: 'consumer', groupRemoteAssignor: 'uniform' })
kafka.consumer({ groupId: 'g', groupProtocol: 'classic' })
kafka.consumer({ groupId: 'g', maxDecompressedBatchBytes: 4194304 })
// @ts-expect-error a number of bytes
kafka.consumer({ groupId: 'g', maxDecompressedBatchBytes: '4 MiB' })
kafka.consumer({ groupId: 'g', maxPrefetchBatches: 0 })
kafka.consumer({ groupId: 'g', maxPrefetchBatches: 1 })
// @ts-expect-error deeper prefetch is rejected at runtime, so it is a type error too
kafka.consumer({ groupId: 'g', maxPrefetchBatches: 2 })

// The regex subscription form.
const regexConsumer = kafka.consumer({ groupId: 'g', groupProtocol: 'consumer' })
regexConsumer.subscribe({ regex: '^orders-.*', fromBeginning: true })
// The two existing forms still compile.
regexConsumer.subscribe({ topics: ['t'], fromBeginning: false })
regexConsumer.subscribe({ topic: /^t-.*/ })
// @ts-expect-error a regex cannot be combined with topics; the runtime throws for this too
regexConsumer.subscribe({ regex: '^orders-.*', topics: ['t'] })
// @ts-expect-error nor with a single topic
regexConsumer.subscribe({ regex: '^orders-.*', topic: 't' })
// @ts-expect-error and the two topic forms stay mutually exclusive
regexConsumer.subscribe({ topic: 't', topics: ['t'] })

// `groupRemoteAssignor: null` is the documented default and compiles under both protocols.
kafka.consumer({ groupId: 'g', groupRemoteAssignor: null })
kafka.consumer({ groupId: 'g', groupProtocol: 'consumer', groupRemoteAssignor: null })

// listGroups filters and the two new row fields.
const listGroupsOptions: ListGroupsOptions = { states: ['stable'], types: ['consumer'] }
admin.listGroups(listGroupsOptions).then(({ groups }) => {
  const state: string = groups[0].groupState
  // The union is open: a group type a newer broker introduces is passed through, so a consumer
  // reads it as a string and an exhaustive switch needs a default.
  const type: string = groups[0].groupType
  void state
  void type
})
admin.listGroups().then(({ groups }) => void groups)

// describeGroups keeps errorCode and adds the new fields.
admin.describeGroups(['g']).then(({ groups }) => {
  const group: GroupDescription = groups[0]
  const code: number = group.errorCode
  const state: ConsumerGroupState = group.state
  const epoch: number | null | undefined = group.groupEpoch
  const member: MemberDescription = group.members[0]
  const memberEpoch: number | null | undefined = member.memberEpoch
  const assignment: TopicPartitions[] | undefined = member.assignment
  void code
  void state
  void epoch
  void memberEpoch
  void assignment
})

// A 'Reconciling' state is assignable, and a made-up one is not.
const reconciling: ConsumerGroupState = 'Reconciling'
const assigning: ConsumerGroupState = 'Assigning'
void reconciling
void assigning
// @ts-expect-error not a group state
const notAState: ConsumerGroupState = 'Rebalancing'
void notAState

// The two event payloads.
regexConsumer.on(regexConsumer.events.GROUP_JOIN, e => {
  const membership: 'classic' | 'consumer' = e.payload.membershipProtocol
  const leader: string | null = e.payload.leaderId
  const assigner: string = e.payload.groupProtocol
  void membership
  void leader
  void assigner
})
regexConsumer.on(regexConsumer.events.REBALANCING, e => {
  const membership: 'classic' | 'consumer' = e.payload.membershipProtocol
  void membership
  if (e.payload.reason === 'assignment-changed' || e.payload.reason === 'fenced') {
    void e.payload.reason
  }
})

// The producer options: idempotent by default, the accumulator and the delivery deadline, and
// the retrier's deadline; every 2.2.4 form still compiles.
kafka.producer({ idempotent: true, maxInFlightRequests: 5 })
kafka.producer({ idempotent: false, transactionalId: 'tx', transactionTimeout: 30000 })
kafka.producer({ linger: 5, batchSize: 16384, deliveryTimeout: 120000 })
kafka.producer({ linger: 0 })
kafka.producer({ retry: { retries: 5, maxTotalTime: 60000 } })
new Kafka({ brokers: ['localhost:9092'], retry: { maxTotalTime: 30000 } })
// @ts-expect-error linger is a number of milliseconds
kafka.producer({ linger: '5' })

// The uniform sticky partitioner, with its options or none.
kafka.producer({
  createPartitioner: Partitioners.UniformStickyPartitioner({ stickyBytes: 8192, ignoreKeys: true }),
})
kafka.producer({ createPartitioner: Partitioners.UniformStickyPartitioner() })
kafka.producer({ createPartitioner: Partitioners.UniformStickyPartitioner({ adaptive: true }) })
// @ts-expect-error adaptive is a boolean
Partitioners.UniformStickyPartitioner({ adaptive: 'yes' })
kafka.producer({ createPartitioner: Partitioners.DefaultPartitioner })

// groupMetadata() and the two sendOffsets shapes.
const runTransaction = async () => {
  const transactional = kafka.producer({ transactionalId: 'tx' })
  const metadata: ConsumerGroupMetadata = consumer.groupMetadata()
  const generationId: number = metadata.generationId
  const groupInstanceId: string | null = metadata.groupInstanceId
  void generationId
  void groupInstanceId
  const built: ConsumerGroupMetadata = {
    groupId: 'g',
    generationId: 1,
    memberId: 'member',
    groupInstanceId: null,
  }
  void built
  const offsets: TransactionOffsets['topics'] = [
    { topic, partitions: [{ partition: 0, offset: '5', leaderEpoch: 3, metadata: null }] },
  ]

  const transaction = await transactional.transaction()
  await transaction.sendOffsets({ consumerGroupMetadata: metadata, topics: offsets })
  await transaction.sendOffsets({ consumerGroupId: 'g', topics: offsets })
  // The 2.2.4 shape, partitions without a leader epoch.
  await transaction.sendOffsets({
    consumerGroupId: 'g',
    topics: [{ topic, partitions: [{ partition: 0, offset: '5' }] }],
  })
  // The 2.2.4 type, a value declared as Offsets with a group id.
  const legacy: Offsets & { consumerGroupId: string } = {
    consumerGroupId: 'g',
    topics: [{ topic, partitions: [{ partition: 0, offset: '5' }] }],
  }
  await transaction.sendOffsets(legacy)
  // @ts-expect-error both identifiers at once; the runtime throws for this too
  await transaction.sendOffsets({ consumerGroupMetadata: metadata, consumerGroupId: 'g', topics: offsets })
  // @ts-expect-error neither identifier
  await transaction.sendOffsets({ topics: offsets })
  await transaction.commit()

  await consumer.run({
    eachBatch: async ({ uncommittedOffsets }) => {
      const next = await transactional.transaction()
      // The uncommitted offsets of a batch are what sendOffsets takes.
      await next.sendOffsets({
        consumerGroupMetadata: consumer.groupMetadata(),
        topics: uncommittedOffsets().topics,
      })
      await next.commit()
    },
  })
}
runTransaction().catch(console.error)

// Every error carries its category, and the five classes exist.
const categoryOf = (error: KafkaJSError): KafkaJSErrorCategory => error.category
categoryOf(new KafkaJSError('x'))
const categories: KafkaJSErrorCategory[] = [
  'retriable',
  'refreshRetriable',
  'abortable',
  'applicationRecoverable',
  'invalidConfiguration',
]
void categories
const cause = new Error('cause')
const abortable: KafkaJSNonRetriableError = new KafkaJSTransactionAbortableError('x', { cause })
const recoverable: KafkaJSNonRetriableError = new KafkaJSApplicationRecoverableError('x')
const invalid: KafkaJSNonRetriableError = new KafkaJSInvalidConfigurationError('x', { cause })
const expired: KafkaJSNonRetriableError = new KafkaJSDeliveryTimeout('x', { cause })
const refresh: KafkaJSError = new KafkaJSRefreshRetriableError('x')
void [abortable, recoverable, invalid, expired, refresh]
const aggregateCategory: KafkaJSErrorCategory = new KafkaJSAggregateError('x', []).category
const rebalanceCategory: KafkaJSErrorCategory = new KafkaJSFetcherRebalanceError('x').category
void aggregateCategory
void rebalanceCategory
// @ts-expect-error not a category
const notACategory: KafkaJSErrorCategory = 'fatal'
void notACategory

// The error classes of a refused frame, a corrupt record batch and a refused commit.
const frameError = new KafkaJSInvalidFrameError('x', {
  broker: 'localhost:9092',
  size: -1,
  reason: 'negative',
})
const frameIsConnectionError: KafkaJSConnectionError = frameError
const frameReason: 'negative' | 'too-short' | 'too-large' | 'unreadable' = frameError.reason
const frameSize: number | null = frameError.size
const corruptError = new KafkaJSCorruptRecordError('x', { reason: 'length', offset: '42', cause })
const corruptIsNonRetriable: KafkaJSNonRetriableError = corruptError
const corruptReason:
  | 'length'
  | 'magic'
  | 'count'
  | 'control'
  | 'checksum'
  | 'decompressed-size'
  | 'decompression' = corruptError.reason
const boundError = new KafkaJSCorruptRecordError('x', {
  reason: 'decompressed-size',
  offset: '42',
  nextOffset: '50',
  codec: 'lz4',
  limit: 4194304,
})
const boundNext: string | undefined = boundError.nextOffset
const boundCodec: 'gzip' | 'snappy' | 'lz4' | 'zstd' | undefined = boundError.codec
const boundLimit: number | undefined = boundError.limit
// @ts-expect-error not a reason
new KafkaJSCorruptRecordError('x', { reason: 'bomb' })
const commitError = new KafkaJSCommitFailedError('x', { reason: 'membership-lost', cause })
const commitIsNonRetriable: KafkaJSNonRetriableError = commitError
const commitReason: 'membership-lost' | 'poll-interval-exceeded' | 'stale-assignment' =
  commitError.reason
const commitCause: Error = commitError.cause
const commitType: string | undefined = commitError.type
const commitCode: number | undefined = commitError.code
// @ts-expect-error `cause` is required: a refused commit always names the loss
new KafkaJSCommitFailedError('x', { reason: 'membership-lost' })
// @ts-expect-error not a reason
new KafkaJSInvalidFrameError('x', { reason: 'too-big' })
void [frameIsConnectionError, frameReason, frameSize, corruptIsNonRetriable, corruptReason]
void [boundNext, boundCodec, boundLimit]
void [commitIsNonRetriable, commitReason, commitCause, commitType, commitCode]

// The error classes of a closed connection, a join that needs its member id, a broken invariant,
// an undecodable number, a refused topic creation and a consumer with no broker to fetch from.
const closedError = new KafkaJSConnectionClosedError('x', { host: 'localhost', port: 9092 })
const closedIsConnectionError: KafkaJSConnectionError = closedError
const closedBroker: string = closedError.broker
const closedHost: string | undefined = closedError.host
const closedPort: number | undefined = closedError.port
new KafkaJSConnectionClosedError('x')
const memberIdError = new KafkaJSMemberIdRequired(new Error('x'), { memberId: 'member-1' })
const memberIdIsProtocolError: KafkaJSProtocolError = memberIdError
const requiredMemberId: string = memberIdError.memberId
// @ts-expect-error the assigned member id is required
new KafkaJSMemberIdRequired(new Error('x'))
const invariantError: KafkaJSNonRetriableError = new KafkaJSInvariantViolation('x')
const invariantHelpUrl: string | undefined = new KafkaJSInvariantViolation(new Error('x')).helpUrl
const varIntError: KafkaJSNonRetriableError = new KafkaJSInvalidVarIntError('x', { cause })
const longError: KafkaJSNonRetriableError = new KafkaJSInvalidLongError('x')
const createTopicError = new KafkaJSCreateTopicError(new Error('x'), topic)
const createTopicIsProtocolError: KafkaJSProtocolError = createTopicError
const createTopicName: string | undefined = createTopicError.topic
const noBrokerError: KafkaJSError = new KafkaJSNoBrokerAvailableError()
// @ts-expect-error it takes no arguments
new KafkaJSNoBrokerAvailableError('x')
void [closedIsConnectionError, closedBroker, closedHost, closedPort]
void [memberIdIsProtocolError, requiredMemberId, invariantError, invariantHelpUrl]
void [varIntError, longError, createTopicIsProtocolError, createTopicName, noBrokerError]

// The aggregate says whether its children can be retried.
const aggregateRetriable: boolean = new KafkaJSAggregateError('x', ['y']).retriable
void aggregateRetriable

// A node id is a string, as `Object.keys` of the broker table makes it.
declare const cluster: Cluster
const nodeIds: string[] = cluster.getNodeIds()
// @ts-expect-error a node id never equals a number
void (nodeIds[0] === 1)

// The client option for the response-size limit.
new Kafka({ brokers: ['localhost:9092'], maxResponseSize: 1048576 })

// KafkaJSLockTimeout stays exported.
new KafkaJSLockTimeout()

// The broker surface transactions use.
const useBroker = async (broker: Broker) => {
  await broker.fetch({
    topics: [{ topic, partitions: [{ partition: 0, fetchOffset: '0', maxBytes: 1048576 }] }],
    checkCrcs: true,
    maxDecompressedBatchBytes: 4194304,
  })
  const offsets: TransactionOffsets['topics'] = [
    { topic, partitions: [{ partition: 0, offset: '5' }] },
  ]
  const level: number = broker.finalizedFeatures['transaction.version'].maxVersionLevel
  const featuresEpoch: number = broker.finalizedFeaturesEpoch
  void level
  void featuresEpoch
  const { producerId, producerEpoch } = await broker.initProducerId({
    transactionalId: 'tx',
    transactionTimeout: 60000,
    producerId: '1000',
    producerEpoch: 3,
  })
  const heldProducerId: string = producerId
  const heldProducerEpoch: number = producerEpoch
  void heldProducerId
  void heldProducerEpoch
  await broker.initProducerId({ transactionalId: null, transactionTimeout: 60000 })
  // The defaults: no transaction timeout, no producer id held.
  await broker.initProducerId({ transactionalId: 'tx' })
  await broker.initProducerId({})
  await broker.produce({ topicData: [], transactionalId: 'tx', transactionVersion: 2 })
  await broker.produce({ topicData: [], producerId: '1000', producerEpoch: 3 })
  await broker.produce({ topicData: [], producerId: 1000, producerEpoch: 3 })
  // @ts-expect-error only transaction versions 1 and 2 exist
  await broker.produce({ topicData: [], transactionalId: 'tx', transactionVersion: 3 })
  await broker.offsetFetch({ groupId: 'g', topics: [], requireStable: true })
  await broker.offsetFetch({ groupId: 'g', topics: [], memberId: null, memberEpoch: -1 })
  await broker.txnOffsetCommit({
    transactionalId: 'tx',
    groupId: 'g',
    producerId,
    producerEpoch,
    topics: [{ topic, partitions: [{ partition: 0, offset: '5', leaderEpoch: 3 }] }],
    generationId: 7,
    memberId: 'member',
    groupInstanceId: null,
    transactionVersion: 2,
  })
  // The defaults: an unfenced commit under transaction version 1.
  await broker.txnOffsetCommit({
    transactionalId: 'tx',
    groupId: 'g',
    producerId,
    producerEpoch,
    topics: offsets,
  })
  const ended = await broker.endTxn({
    transactionalId: 'tx',
    producerId,
    producerEpoch,
    transactionResult: true,
    transactionVersion: 2,
  })
  const bumped: string | undefined = ended.producerId
  void bumped
  // @ts-expect-error the producer id is a decimal string
  const bumpedAsNumber: number | undefined = ended.producerId
  void bumpedAsNumber
  // @ts-expect-error the pair is absent from an EndTxn response below v5
  void ended.producerId.length
  // @ts-expect-error likewise the epoch
  const bumpedEpoch: number = ended.producerEpoch
  void bumpedEpoch
  // @ts-expect-error only transaction versions 1 and 2 exist
  await broker.endTxn({ transactionalId: 'tx', producerId, producerEpoch, transactionResult: false, transactionVersion: 3 })
}
void useBroker

// Configuration administration: individual entries, the new resource types and sources.
const useConfigAdmin = async () => {
  const entries: IncrementalAlterConfigEntry[] = [
    { name: 'retention.ms', operation: AlterConfigOpTypes.SET, value: '60000' },
    { name: 'cleanup.policy', operation: AlterConfigOpTypes.APPEND, value: 'compact' },
    { name: 'cleanup.policy', operation: AlterConfigOpTypes.SUBTRACT, value: 'delete' },
    { name: 'retention.bytes', operation: AlterConfigOpTypes.DELETE },
    { name: 'retention.bytes', operation: AlterConfigOpTypes.DELETE, value: null },
  ]
  const result = await admin.incrementalAlterConfigs({
    validateOnly: true,
    resources: [{ type: ConfigResourceTypes.TOPIC, name: topic, configEntries: entries }],
  })
  const failed = result.resources.filter(({ errorCode }) => errorCode !== 0)
  const failure: string | null = failed[0].errorMessage
  const failedType: ConfigResourceTypes = failed[0].resourceType
  const failedName: string = failed[0].resourceName
  void [failure, failedType, failedName]
  // The cluster-wide default broker config is the BROKER resource with an empty name.
  await admin.incrementalAlterConfigs({
    resources: [{ type: ConfigResourceTypes.BROKER, name: '', configEntries: entries }],
  })
  // @ts-expect-error the resource type is a ConfigResourceTypes, not any number
  await admin.incrementalAlterConfigs({ resources: [{ type: 99, name: topic, configEntries: entries }] })
  // @ts-expect-error a SET needs a value
  const setWithoutValue: IncrementalAlterConfigEntry = { name: 'a', operation: AlterConfigOpTypes.SET }
  // A DELETE's value is ignored at run time.
  const deleteWithValue: IncrementalAlterConfigEntry = { name: 'a', operation: AlterConfigOpTypes.DELETE, value: 'x' }
  // @ts-expect-error the operation is an AlterConfigOpTypes
  const unknownOperation: IncrementalAlterConfigEntry = { name: 'a', operation: 9, value: 'x' }
  void [setWithoutValue, deleteWithValue, unknownOperation]
  // Entries kept in a variable without an annotation, or built by a helper, carry the whole enum.
  const stored = [
    { name: 'retention.ms', operation: AlterConfigOpTypes.SET, value: '60000' },
    { name: 'cleanup.policy', operation: AlterConfigOpTypes.APPEND, value: 'compact' },
  ]
  const entry = (name: string, operation: AlterConfigOpTypes, value: string) => ({ name, operation, value })
  await admin.incrementalAlterConfigs({
    resources: [
      { type: ConfigResourceTypes.TOPIC, name: topic, configEntries: stored },
      { type: ConfigResourceTypes.TOPIC, name: topic, configEntries: [entry('retention.ms', AlterConfigOpTypes.SET, '1')] },
    ],
  })
  // @ts-expect-error a resource has a name, which is empty for the cluster-wide default broker config
  await admin.incrementalAlterConfigs({ resources: [{ type: ConfigResourceTypes.BROKER, configEntries: entries }] })

  const described = await admin.describeConfigs({
    includeSynonyms: false,
    includeDocumentation: true,
    resources: [
      { type: ConfigResourceTypes.GROUP, name: 'group-id' },
      { type: ConfigResourceTypes.CLIENT_METRICS, name: 'metrics-subscription', configNames: ['interval.ms'] },
    ],
  })
  const described0 = described.resources[0].configEntries[0]
  const configType: ConfigTypes | undefined = described0.configType
  const documentation: string | null | undefined = described0.documentation
  const configSource: ConfigSource = ConfigSource.GROUP_CONFIG
  const metricsSource: ConfigSource = ConfigSource.CLIENT_METRICS_CONFIG
  const passwordType: ConfigTypes = ConfigTypes.PASSWORD
  void [configType, documentation, configSource, metricsSource, passwordType]

  await admin.createAcls({
    acl: [
      {
        resourceType: AclResourceTypes.USER,
        resourceName: 'User:bob',
        resourcePatternType: ResourcePatternTypes.LITERAL,
        principal: 'User:alice',
        host: '*',
        operation: AclOperationTypes.ALTER,
        permissionType: AclPermissionTypes.ALLOW,
      },
    ],
  })
}
void useConfigAdmin

// What the new result types may hold: every nullable and optional field in its other state.
const noTopicId: ITopicMetadata = { name: topic, partitions: [] }
const nullTopicId: ITopicMetadata = { name: topic, partitions: [], topicId: null, isInternal: false }
const trackedElr: PartitionMetadata = {
  partitionErrorCode: 0,
  partitionId: 0,
  leader: 1,
  replicas: [1],
  isr: [1],
  leaderEpoch: 3,
  eligibleLeaderReplicas: [1],
  lastKnownElr: [],
}
const untrackedElr: PartitionMetadata = { ...trackedElr, eligibleLeaderReplicas: null, lastKnownElr: null }
const undocumented: ConfigEntries = {
  configName: 'a',
  configValue: 'b',
  isDefault: true,
  configSource: ConfigSource.DEFAULT_CONFIG,
  isSensitive: false,
  readOnly: false,
  configSynonyms: [],
  configType: ConfigTypes.STRING,
  documentation: null,
}
const unmessaged: IncrementalAlterConfigsResult = {
  resources: [{ errorCode: 0, errorMessage: null, resourceType: ConfigResourceTypes.TOPIC, resourceName: topic }],
}
const removed: RemoveMembersResult = [{ memberId: 'm', groupInstanceId: null, errorCode: 0, error: null }]
const undescribed: DescribeTopicPartitionsPartition = {
  errorCode: 0,
  partitionIndex: 0,
  leaderId: 1,
  leaderEpoch: 0,
  replicaNodes: [1],
  isrNodes: [1],
  eligibleLeaderReplicas: null,
  lastKnownElr: null,
  offlineReplicas: [],
}
// A topic the broker cannot describe comes back with the all-zero id, which reads as null; the schema also allows a null name.
const unauthorized: DescribeTopicPartitionsTopic = {
  errorCode: 29,
  name: null,
  topicId: null,
  isInternal: false,
  partitions: [],
  topicAuthorizedOperations: 0,
}
const lastPage: Awaited<ReturnType<Broker['describeTopicPartitions']>> = {
  throttleTime: 0,
  clientSideThrottleTime: 0,
  topics: [unauthorized],
  nextCursor: null,
}
const staticLeft: Awaited<ReturnType<Broker['leaveGroup']>> = {
  throttleTime: 0,
  clientSideThrottleTime: 0,
  errorCode: 0,
  members: [{ memberId: '', groupInstanceId: null, errorCode: 0 }],
}
// The entry of a describeConfigs answer below DescribeConfigs v3 has neither new key.
const bareEntry: ConfigEntries = {
  configName: 'a',
  configValue: 'b',
  isDefault: true,
  configSource: ConfigSource.DEFAULT_CONFIG,
  isSensitive: false,
  readOnly: false,
  configSynonyms: [],
}
void [noTopicId, nullTopicId, trackedElr, untrackedElr, undocumented, unmessaged, removed, undescribed]
void [unauthorized, lastPage, staticLeft, bareEntry]

// Removing members of a group, and what a failed topic call reports.
const useGroupAndTopicAdmin = async () => {
  const everyone = await admin.removeMembersFromConsumerGroup({ groupId: 'g' })
  const some = await admin.removeMembersFromConsumerGroup({
    groupId: 'g',
    members: [{ groupInstanceId: 'instance-1' }],
    reason: 'the instance is gone',
  })
  await admin.removeMembersFromConsumerGroup({ groupId: 'g', reason: null })
  // @ts-expect-error a member is named by its groupInstanceId
  await admin.removeMembersFromConsumerGroup({ groupId: 'g', members: [{}] })
  const refused = [...everyone, ...some].filter(({ error }) => error !== null)
  const refusedType: string | undefined = refused[0].error?.type
  const memberId: string = refused[0].memberId
  const instance: string | null = refused[0].groupInstanceId
  const code: number = refused[0].errorCode
  void [refusedType, memberId, instance, code]

  try {
    await admin.deleteTopics({ topics: [topic] })
  } catch (error) {
    if (error instanceof KafkaJSProtocolError) {
      const rows: Array<{ topic: string; errorCode: number }> | undefined = error.topicErrors
      const message: string | null | undefined = error.topicErrors?.[0].errorMessage
      const rowTopicId: string | null | undefined = error.topicErrors?.[0].topicId
      void [rows, message, rowTopicId]
      // @ts-expect-error a row names a topic, never a partition
      void error.topicErrors?.[0].partition
    }
  }

  const described = await admin.fetchTopicMetadata({ topics: [topic], useDescribeTopicPartitions: true })
  const first = described.topics[0]
  const topicId: string | null | undefined = first.topicId
  const isInternal: boolean | undefined = first.isInternal
  const partition = first.partitions[0]
  const epoch: number | undefined = partition.leaderEpoch
  const eligible: number[] | null | undefined = partition.eligibleLeaderReplicas
  const lastKnown: number[] | null | undefined = partition.lastKnownElr
  void [topicId, isInternal, epoch, eligible, lastKnown]
  // Without topics the option takes the Metadata path, as it does without the option.
  await admin.fetchTopicMetadata({ useDescribeTopicPartitions: true })
  await admin.fetchTopicMetadata({ topics: [topic] })
  await admin.fetchTopicMetadata()
  // @ts-expect-error the option is a boolean
  await admin.fetchTopicMetadata({ topics: [topic], useDescribeTopicPartitions: 'yes' })
}
void useGroupAndTopicAdmin

// Leaving the group when the consumer disconnects.
const useDisconnect = async () => {
  await consumer.disconnect()
  await consumer.disconnect({})
  await consumer.disconnect({ leaveGroup: true })
  await consumer.disconnect({ leaveGroup: false })
  // The 2.2.4 pattern: disconnect is handed to a signal handler, which passes the signal name.
  const onSignal: (signal: 'SIGTERM') => void = consumer.disconnect
  void onSignal
  // The parameters of an overloaded function come from its last signature: none, as in 2.2.4.
  const noParameters: Parameters<Consumer['disconnect']> = []
  // A wrapper that forwards an options argument it may not have.
  const forwarded = (options?: { leaveGroup?: boolean }) => consumer.disconnect(options)
  void [noParameters, forwarded]
  // @ts-expect-error a boolean is not an options object (the runtime ignores it)
  await consumer.disconnect(true)
  // @ts-expect-error leaveGroup is a boolean
  await consumer.disconnect({ leaveGroup: 'yes' })
  await consumer.stop()
}
void useDisconnect

// The broker methods behind the new admin calls and the group and offset additions.
const useBrokerAdditions = async (broker: Broker) => {
  const incremental = await broker.incrementalAlterConfigs({
    validateOnly: false,
    resources: [
      {
        type: ConfigResourceTypes.TOPIC,
        name: topic,
        configEntries: [{ name: 'retention.ms', operation: AlterConfigOpTypes.SET, value: '1' }],
      },
    ],
  })
  const incrementalCode: number = incremental.resources[0].errorCode
  const incrementalThrottle: number = incremental.clientSideThrottleTime
  void [incrementalCode, incrementalThrottle]
  const bare = await broker.incrementalAlterConfigs({
    resources: [{ type: ConfigResourceTypes.TOPIC, name: topic, configEntries: [] }],
  })
  const bareThrottle: number = bare.throttleTime
  void bareThrottle
  await broker.describeConfigs({ resources: [{ type: ConfigResourceTypes.TOPIC, name: topic }] })
  await broker.describeConfigs({
    resources: [{ type: ConfigResourceTypes.TOPIC, name: topic }],
    includeSynonyms: true,
    includeDocumentation: true,
  })

  const page = await broker.describeTopicPartitions({
    topics: [topic],
    responsePartitionLimit: 100,
    cursor: { topicName: topic, partitionIndex: 3 },
  })
  await broker.describeTopicPartitions({ topics: [topic] })
  await broker.describeTopicPartitions({ topics: [topic], cursor: null })
  const next: { topicName: string; partitionIndex: number } | null = page.nextCursor
  const described = page.topics[0]
  const describedId: string | null = described.topicId
  const describedName: string | null = described.name
  const describedPartition = described.partitions[0]
  const leaderId: number = describedPartition.leaderId
  const eligible: number[] | null = describedPartition.eligibleLeaderReplicas
  const authorized: number = described.topicAuthorizedOperations
  void [next, describedId, describedName, leaderId, eligible, authorized]

  // One member, which throws the first member's error; or several, which report each member.
  await broker.leaveGroup({ groupId: 'g', memberId: 'm', groupInstanceId: null, reason: 'closing' })
  await broker.leaveGroup({ groupId: 'g' })
  const left = await broker.leaveGroup({
    groupId: 'g',
    members: [{ memberId: '', groupInstanceId: 'instance-1', reason: null }, { memberId: 'm2' }],
  })
  const memberCode: number = left.members[0].errorCode
  const memberInstance: string | null = left.members[0].groupInstanceId
  void [memberCode, memberInstance]
  // @ts-expect-error a batch member has a memberId, which is empty to name a static member by instance id
  await broker.leaveGroup({ groupId: 'g', members: [{ groupInstanceId: 'instance-1' }] })
  // @ts-expect-error the batch form and the single-member fields exclude each other
  await broker.leaveGroup({ groupId: 'g', memberId: 'm', members: [] })
  await broker.syncGroup({
    groupId: 'g',
    generationId: 1,
    memberId: 'm',
    groupInstanceId: null,
    groupAssignment: [{ memberId: 'm', memberAssignment: Buffer.alloc(0) }],
    protocolType: 'consumer',
    protocolName: 'RoundRobinAssigner',
    sessionTimeout: 30000,
    rebalanceTimeout: 60000,
  })
  await broker.syncGroup({ groupId: 'g', generationId: 1, memberId: 'm', groupAssignment: [] })

  await broker.offsetCommit({
    groupId: 'g',
    groupGenerationId: 1,
    memberId: 'm',
    topics: [
      {
        topic,
        topicId: '01020304-0506-0708-090a-0b0c0d0e0f10',
        partitions: [{ partition: 0, offset: '5' }],
      },
      { topic, topicId: null, partitions: [{ partition: 0, offset: '5' }] },
    ],
  })
  await broker.offsetFetch({
    groupId: 'g',
    topics: [{ topic, topicId: '01020304-0506-0708-090a-0b0c0d0e0f10', partitions: [{ partition: 0, offset: '0' }] }],
  })
  await broker.offsetFetch({ groupId: 'g', topics: [{ topic, topicId: null, partitions: [{ partition: 0, offset: '0' }] }] })
}
void useBrokerAdditions

// Compression levels are optional and available on both the producer and direct broker API.
const producerWithCompressionLevels = new Kafka({ brokers: ['localhost:9092'] }).producer({
  compressionLevels: { gzip: 9, zstd: 7 },
})
void producerWithCompressionLevels
declare const compressionLevelBroker: import('./index').Broker
compressionLevelBroker.produce({
  topicData: [], compression: CompressionTypes.GZIP, compressionLevel: 1,
})

const deliveryEvidence = new KafkaJSDeliveryTimeout('delivery deadline passed', {
  cause: new Error('last failure'),
  outcome: 'unknown',
})
const maybeDelivered: 'unsent' | 'unknown' | undefined = deliveryEvidence.outcome
new KafkaJSDeliveryTimeout('retry deadline passed')
// @ts-expect-error an outcome is a transmission classification
new KafkaJSDeliveryTimeout('delivery deadline passed', { outcome: 'sent' })

// The producer's buffer: its two options and the two errors its refusals reject with.
kafka.producer({ bufferMemory: 33554432, maxBlockMs: 60000 })
kafka.producer({ bufferMemory: Number.MAX_SAFE_INTEGER, maxBlockMs: 0 })
// @ts-expect-error bufferMemory is a number of bytes
kafka.producer({ bufferMemory: '32m' })
const exhausted = new KafkaJSBufferExhaustedError('no room', {
  reason: 'queue-full',
  requested: 600,
  available: 0,
  capacity: 1024,
  waitedMs: 0,
})
const exhaustedIsRetriableClass: KafkaJSError = exhausted
const exhaustedReason: 'timeout' | 'queue-full' = exhausted.reason
const exhaustedFields: number[] = [
  exhausted.requested,
  exhausted.available,
  exhausted.capacity,
  exhausted.waitedMs,
]
// @ts-expect-error not a reason
new KafkaJSBufferExhaustedError('x', { reason: 'full', requested: 1, available: 0, capacity: 1, waitedMs: 0 })
const tooLarge = new KafkaJSRecordTooLargeError('never fits', {
  reason: 'bufferMemory',
  scope: 'record',
  size: 2048,
  limit: 1024,
  topic: 'topic-a',
})
const tooLargeIsNonRetriable: KafkaJSNonRetriableError = tooLarge
const tooLargeType: 'MESSAGE_TOO_LARGE' = tooLarge.type
const tooLargeCode: 10 = tooLarge.code
const tooLargeScope: 'record' | 'send' = tooLarge.scope
const tooLargeTopic: string | undefined = tooLarge.topic
new KafkaJSRecordTooLargeError('never fits', { reason: 'bufferMemory', scope: 'send', size: 2048, limit: 1024 })
// @ts-expect-error not a scope
new KafkaJSRecordTooLargeError('x', { reason: 'bufferMemory', scope: 'batch', size: 1, limit: 0 })
const narrowed = (error: KafkaJSError) => {
  if (error.name === 'KafkaJSBufferExhaustedError') return (error as KafkaJSBufferExhaustedError).waitedMs
  if (error instanceof KafkaJSRecordTooLargeError) return error.size
  return 0
}
void [exhaustedIsRetriableClass, exhaustedReason, exhaustedFields]
void [tooLargeIsNonRetriable, tooLargeType, tooLargeCode, tooLargeScope, tooLargeTopic, narrowed]

// The request-size bound and its refusal.
kafka.producer({ maxRequestSize: 1048576 })
kafka.producer({ maxRequestSize: 2147483647, transactionalId: 'tx' })
// @ts-expect-error maxRequestSize is a number of bytes
kafka.producer({ maxRequestSize: '1m' })
const requestTooLarge = new KafkaJSRecordTooLargeError('never fits a request', {
  reason: 'maxRequestSize',
  scope: 'record',
  size: 1048577,
  limit: 1048409,
  topic: 'topic-a',
})
const tooLargeReason: 'maxRequestSize' | 'bufferMemory' = requestTooLarge.reason
// @ts-expect-error not a reason
new KafkaJSRecordTooLargeError('x', { reason: 'frame', scope: 'record', size: 1, limit: 0 })
void [tooLargeReason]


// Producer lifecycle calls preserve the signal handler and no-argument parameter tuple.
const lifecycleProducer = new Kafka({ brokers: ['localhost:9092'] }).producer({ metadataMaxIdle: 5000 })
void lifecycleProducer.flush()
void lifecycleProducer.flush({ timeout: 0 })
void lifecycleProducer.flush({ timeout: 2147483647 })
void lifecycleProducer.disconnect({ timeout: 0 })
process.on('SIGTERM', lifecycleProducer.disconnect)
const noCloseArguments: Parameters<Producer['disconnect']> = []
void noCloseArguments
const closeEvidence = new KafkaJSProducerClosedError('closed', { outcome: 'unknown' })
const closeOutcome: 'unsent' | 'unknown' = closeEvidence.outcome
void closeOutcome

// @ts-expect-error the close outcome is required
new KafkaJSProducerClosedError('closed', {})
// @ts-expect-error transmission evidence is readonly
closeEvidence.outcome = 'unsent'

new Kafka({ brokers: ['localhost:9092'] }).producer({ metadataMaxIdle: 5000 })
new Kafka({ brokers: ['localhost:9092'] }).producer({ metadataMaxIdle: 2147483647 })

const unionAssignerContract: kafkajs.Assigner = {
  name: 'union-contract',
  version: 0,
  async assign({ members, topics, groupTopics, groupTopicMetadata }) {
    const names: string[] = groupTopics ?? topics
    const rows: PartitionMetadata[] = groupTopicMetadata?.get(names[0]) ?? []
    void members
    void rows
    if (groupTopicMetadata) {
      // @ts-expect-error A supplied group snapshot is readonly.
      groupTopicMetadata.set('orders', [])
    }
    return []
  },
  protocol() {
    return { name: 'union-contract', metadata: Buffer.alloc(0) }
  },
}
void unionAssignerContract.assign({ members: [], topics: [] })
void unionAssignerContract.assign({
  members: [],
  topics: ['orders'],
  groupTopics: ['orders'],
  groupTopicMetadata: new Map<string, PartitionMetadata[]>(),
})
