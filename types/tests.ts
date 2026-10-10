import * as fs from 'fs'

import * as kafkajs from './index'
import {
  Kafka,
  PartitionAssigners,
  logLevel,
  CompressionTypes,
  CompressionCodecs,
  ConfigResourceTypes,
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
  KafkaJSAggregateError,
  KafkaJSFetcherRebalanceError,
  KafkaJSLockTimeout,
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
// KafkaJSLockTimeout stays exported.
new KafkaJSLockTimeout()

// The broker surface transactions use.
const useBroker = async (broker: Broker) => {
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
