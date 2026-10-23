/// <reference types="node" />

import * as tls from 'tls'
import * as net from 'net'

type Without<T, U> = { [P in Exclude<keyof T, keyof U>]?: never }
type XOR<T, U> = T | U extends object ? (Without<T, U> & U) | (Without<U, T> & T) : T | U

export class Kafka {
  constructor(config: KafkaConfig)
  producer(config?: ProducerConfig): Producer
  consumer(config: ConsumerConfig): Consumer
  admin(config?: AdminConfig): Admin
  logger(): Logger
}

export type BrokersFunction = () => string[] | Promise<string[]>

type SaslAuthenticationRequest = {
  encode: () => Buffer | Promise<Buffer>
}
type SaslAuthenticationResponse<ParseResult> = {
  decode: (rawResponse: Buffer) => Buffer | Promise<Buffer>
  parse: (data: Buffer) => ParseResult
}

export type Authenticator = {
  authenticate: () => Promise<void>
}

export type SaslAuthenticateArgs<ParseResult> = {
  request: SaslAuthenticationRequest
  response?: SaslAuthenticationResponse<ParseResult>
}

export type AuthenticationProviderArgs = {
  host: string
  port: number
  logger: Logger
  saslAuthenticate: <ParseResult>(
    args: SaslAuthenticateArgs<ParseResult>
  ) => Promise<ParseResult | void>
}

export type Mechanism = {
  mechanism: string
  authenticationProvider: (args: AuthenticationProviderArgs) => Authenticator
}

export interface KafkaConfig {
  brokers: string[] | BrokersFunction
  ssl?: tls.ConnectionOptions | boolean
  sasl?: SASLOptions | Mechanism
  clientId?: string
  connectionTimeout?: number
  authenticationTimeout?: number
  reauthenticationThreshold?: number
  requestTimeout?: number
  enforceRequestTimeout?: boolean
  /**
   * The response-size limit, in bytes, the 4-byte length prefix excluded, handed to every broker
   * connection. Default 104857600 (100 MiB); an integer from 4 to 2147483647, else the
   * constructor throws `KafkaJSInvalidConfigurationError`. It is not `maxBytes`.
   */
  maxResponseSize?: number
  retry?: RetryOptions
  socketFactory?: ISocketFactory
  logLevel?: logLevel
  logCreator?: logCreator
  metadataRecoveryStrategy?: 'rebootstrap' | 'none'
  metadataRecoveryRebootstrapTriggerMs?: number
}

export interface ISocketFactoryArgs {
  host: string
  port: number
  ssl: tls.ConnectionOptions
  onConnect: () => void
}

export type ISocketFactory = (args: ISocketFactoryArgs) => net.Socket

export interface OauthbearerProviderResponse {
  value: string
}

type SASLMechanismOptionsMap = {
  plain: { username: string; password: string }
  'scram-sha-256': { username: string; password: string }
  'scram-sha-512': { username: string; password: string }
  aws: {
    authorizationIdentity: string
    accessKeyId: string
    secretAccessKey: string
    sessionToken?: string
  }
  oauthbearer: { oauthBearerProvider: () => Promise<OauthbearerProviderResponse> }
}

export type SASLMechanism = keyof SASLMechanismOptionsMap
type SASLMechanismOptions<T> = T extends SASLMechanism
  ? { mechanism: T } & SASLMechanismOptionsMap[T]
  : never
export type SASLOptions = SASLMechanismOptions<SASLMechanism>

export interface ProducerConfig {
  /** Per-codec levels. Gzip: -1 or 1..9; zstd: -131072..22. Omission keeps codec defaults.
   * LZ4 levels are unavailable in lz4-napi and are rejected, as are Snappy levels.
   */
  compressionLevels?: { gzip?: number; zstd?: number }
  createPartitioner?: ICustomPartitioner
  retry?: RetryOptions
  metadataMaxAge?: number
  allowAutoTopicCreation?: boolean
  /**
   * Idempotent produce (KIP-679): every record written once per partition, in order. Defaults to
   * true from 3.0.0; `false` runs without idempotence (ignored with a `transactionalId`). A
   * setting idempotence does not allow (`acks` other than -1 on a send, `maxInFlightRequests`
   * above 5 or null, `retry.retries` of 0) turns it off with a warning, or throws when
   * `idempotent: true` or a `transactionalId` asked for it.
   */
  idempotent?: boolean
  transactionalId?: string
  transactionTimeout?: number
  /**
   * Maximum unacknowledged requests per broker connection. Defaults to 5 (Java parity).
   * Pass null for unbounded (the pre-3.0 behavior). Above 5, or null, is more than an idempotent
   * producer allows.
   */
  maxInFlightRequests?: number | null
  /**
   * ms a partition's batch waits for records of further sends before it goes out. An integer
   * from 0 to 2147483647; defaults to 5 (Java parity). A lone send waits up to this long; 0 sends
   * at once - on an idempotent producer still merging the sends that queue behind a partition's
   * batch in flight.
   */
  linger?: number
  /**
   * Bytes of keys and values at which a batch is sent without waiting for `linger`. An integer of
   * at least 1; defaults to 16384 (Java parity).
   */
  batchSize?: number
  /**
   * Opt-in bound, in ms, on the waiting and the retries of one send (KIP-91); past it the send
   * rejects with `KafkaJSDeliveryTimeout`. An integer of at least `linger + requestTimeout` (5 ms
   * more than `requestTimeout` at the default `linger`) and at most 2147483647. The bound runs
   * from the send and includes the wait for `linger` and, on an idempotent producer, for the
   * producer id. A queued or on-wire request settles at the deadline; a late answer cannot settle
   * the send again.
   */
  deliveryTimeout?: number
}

export interface Message {
  key?: Buffer | string | null
  value: Buffer | string | null
  partition?: number
  headers?: IHeaders
  timestamp?: string
}

export interface PartitionerArgs {
  topic: string
  partitionMetadata: PartitionMetadata[]
  message: Message
}

export type ICustomPartitioner = () => (args: PartitionerArgs) => number
export type DefaultPartitioner = ICustomPartitioner
export type LegacyPartitioner = ICustomPartitioner

export const Partitioners: {
  DefaultPartitioner: DefaultPartitioner
  LegacyPartitioner: LegacyPartitioner
  /**
   * @deprecated Use DefaultPartitioner instead
   *
   * The JavaCompatiblePartitioner was renamed DefaultPartitioner
   * and made to be the default in 2.0.0.
   */
  JavaCompatiblePartitioner: DefaultPartitioner
  /**
   * KIP-794 uniform sticky partitioner (opt-in): keyless records stick to a partition for
   * `stickyBytes` of keys and values, keyed ones keep `DefaultPartitioner`'s mapping unless
   * `ignoreKeys`.
   */
  UniformStickyPartitioner: (options?: {
    stickyBytes?: number
    ignoreKeys?: boolean
  }) => ICustomPartitioner
}

export type PartitionMetadata = {
  partitionErrorCode: number
  partitionId: number
  leader: number
  replicas: number[]
  isr: number[]
  offlineReplicas?: number[]
  /** The leader epoch (KIP-320); absent below Metadata v7. */
  leaderEpoch?: number
  /**
   * The eligible leader replicas (KIP-966); only `admin.fetchTopicMetadata` with
   * `useDescribeTopicPartitions` reports them, and `null` when the cluster does not track them.
   */
  eligibleLeaderReplicas?: number[] | null
  /** The last known eligible leader replicas (KIP-966); reported and `null` as above. */
  lastKnownElr?: number[] | null
}

export interface IHeaders {
  [key: string]: Buffer | string | (Buffer | string)[] | undefined
}

export interface ConsumerConfig {
  groupId: string
  partitionAssigners?: PartitionAssigner[]
  metadataMaxAge?: number
  sessionTimeout?: number
  /**
   * The maximum time between two batches handed to your handler, in milliseconds, and the
   * rebalance timeout sent on the wire. Default 300000, matching the Java client's
   * `max.poll.interval.ms`. A handler that exceeds it loses its partitions and the consumer
   * rejoins.
   */
  maxPollInterval?: number
  /**
   * @deprecated Use `maxPollInterval`. This value is still sent on the wire as the JoinGroup
   * rebalance timeout when it is the only one you set, but it never becomes your handler
   * deadline: that stays at least 300000 ms.
   */
  rebalanceTimeout?: number
  heartbeatInterval?: number
  maxBytesPerPartition?: number
  minBytes?: number
  maxBytes?: number
  maxWaitTimeInMs?: number
  /**
   * Verify the CRC32C checksum of every record batch received (Java's `check.crcs`). Default
   * `true`. A mismatch is a non-retriable error: no message from that batch is delivered, the
   * consumer crashes, and the committed offset is not advanced past it. Set to `false` to skip
   * the check and restore the pre-3.0 behavior.
   */
  checkCrcs?: boolean
  retry?: RetryOptions & { restartOnFailure?: (err: Error) => Promise<boolean> }
  allowAutoTopicCreation?: boolean
  /**
   * Maximum unacknowledged requests per broker connection. Defaults to 5 (Java parity).
   * Pass null for unbounded (the pre-3.0 behavior).
   */
  maxInFlightRequests?: number | null
  readUncommitted?: boolean
  rackId?: string
  groupInstanceId?: string
  /**
   * Which group membership protocol to use. `'classic'` (the default) is the JoinGroup/SyncGroup
   * protocol every kafkajs release has used. `'consumer'` is the KIP-848 protocol, in which the
   * coordinator computes assignments and the member reconciles them over heartbeats; it requires
   * Kafka 4.0 or newer.
   *
   * Under `'consumer'` the broker owns the session timeout and the heartbeat interval and there are
   * no client-side assigners, so setting `sessionTimeout`, `heartbeatInterval` or
   * `partitionAssigners` alongside it throws at construction.
   */
  groupProtocol?: 'classic' | 'consumer'
  /**
   * Name of the server-side assignor to request under `groupProtocol: 'consumer'`, e.g. `'uniform'`
   * or `'range'`. `null` (the default) lets the coordinator pick its first configured assignor.
   * Setting it under `groupProtocol: 'classic'` throws at construction.
   */
  groupRemoteAssignor?: string | null
  /**
   * How many prefetched batches may be in flight ahead of your handlers. **Inert in this release**:
   * `0` (the default) and `1` behave identically, and the fetch loop is exactly what it was. The
   * one-deep prefetch the option was to control did not clear the throughput bar it was designed
   * against and was withdrawn; the option is reserved so a later release can turn the prefetch on
   * without a configuration change. Values other than `0` and `1` throw at construction.
   */
  maxPrefetchBatches?: 0 | 1
}

export type PartitionAssigner = (config: {
  cluster: Cluster
  groupId: string
  logger: Logger
}) => Assigner

export interface CoordinatorMetadata {
  errorCode: number
  coordinator: {
    nodeId: number
    host: string
    port: number
  }
}

export type Cluster = {
  getNodeIds(): string[]
  metadata(): Promise<BrokerMetadata>
  removeBroker(options: { host: string; port: number }): void
  addMultipleTargetTopics(topics: string[]): Promise<void>
  isConnected(): boolean
  connect(): Promise<void>
  disconnect(): Promise<void>
  refreshMetadata(): Promise<void>
  refreshMetadataIfNecessary(): Promise<void>
  addTargetTopic(topic: string): Promise<void>
  findBroker(node: { nodeId: string }): Promise<Broker>
  findControllerBroker(): Promise<Broker>
  findConnectedBroker(): Promise<Broker>
  findTopicPartitionMetadata(topic: string): PartitionMetadata[]
  findLeaderForPartitions(topic: string, partitions: number[]): { [leader: string]: number[] }
  findGroupCoordinator(group: { groupId: string }): Promise<Broker>
  findGroupCoordinatorMetadata(group: { groupId: string }): Promise<CoordinatorMetadata>
  defaultOffset(config: { fromBeginning: boolean }): number
  fetchTopicsOffset(
    topics: Array<
      {
        topic: string
        partitions: Array<{ partition: number }>
      } & XOR<{ fromBeginning: boolean }, { fromTimestamp: number }>
    >
  ): Promise<TopicOffsets[]>
}

export type Assignment = { [topic: string]: number[] }

export type GroupMember = {
  memberId: string
  memberMetadata: Buffer
  /** The member's static membership id (KIP-345), `null` for a dynamic member. */
  groupInstanceId?: string | null
}

export type GroupMemberAssignment = { memberId: string; memberAssignment: Buffer }

export type GroupState = { name: string; metadata: Buffer }

export type Assigner = {
  name: string
  version: number
  assign(group: { members: GroupMember[]; topics: string[] }): Promise<GroupMemberAssignment[]>
  protocol(subscription: {
    topics: string[]
    ownedPartitions?: Array<{ topic: string; partitions: number[] }>
    generationId?: number
    rackId?: string | null
  }): GroupState
  /** Which rebalance protocols this assigner can take part in. Absent means `['eager']`. */
  supportedProtocols?(): Array<'eager' | 'cooperative'>
  /** Optional bytes for the subscription. An assigner that defines `protocol()` calls this itself. */
  subscriptionUserData?(topics: string[]): Buffer | null
  /** Invoked after every successful sync, and with `null` when the member leaves or resets. */
  onAssignment?(
    assignment: {
      partitions: Array<{ topic: string; partitions: number[] }>
      userData: Buffer | null
    } | null,
    metadata: { generationId: number; groupProtocol: string | null }
  ): void
}

export interface RetryOptions {
  maxRetryTime?: number
  initialRetryTime?: number
  factor?: number
  multiplier?: number
  retries?: number
  /**
   * Deadline in ms for one retried operation, from its first attempt: when the wait before the
   * next attempt would end past it, the operation rejects with `KafkaJSDeliveryTimeout`. An
   * integer from 1 to 2147483647. Producer sends and transaction retries, `commitOffsets()`, the
   * consumer group's join and sync, admin calls, and the client's connect, metadata and coordinator
   * lookups honour it. The consumer's fetch loop (with its auto-commits), the consumer-protocol
   * heartbeat backoff and position validation, which has its own fixed budget, ignore it.
   */
  maxTotalTime?: number
  restartOnFailure?: (e: Error) => Promise<boolean>
}

export interface AdminConfig {
  retry?: RetryOptions
}

export interface ITopicConfig {
  topic: string
  numPartitions?: number
  replicationFactor?: number
  replicaAssignment?: ReplicaAssignment[]
  configEntries?: IResourceConfigEntry[]
}

export interface ITopicPartitionConfig {
  topic: string
  count: number
  assignments?: Array<Array<number>>
}

export interface ITopicMetadata {
  name: string
  partitions: PartitionMetadata[]
  /** The topic's id (KIP-516); `null` when the broker sends none, absent below Metadata v10. */
  topicId?: string | null
  /** `true` for a Kafka internal topic; absent below Metadata v1. */
  isInternal?: boolean
}

export interface ReplicaAssignment {
  partition: number
  replicas: Array<number>
}

export interface PartitionReassignment {
  topic: string
  partitionAssignment: Array<ReplicaAssignment>
}

export enum AclResourceTypes {
  UNKNOWN = 0,
  ANY = 1,
  TOPIC = 2,
  GROUP = 3,
  CLUSTER = 4,
  TRANSACTIONAL_ID = 5,
  DELEGATION_TOKEN = 6,
  USER = 7,
}

export enum ConfigResourceTypes {
  UNKNOWN = 0,
  TOPIC = 2,
  BROKER = 4,
  BROKER_LOGGER = 8,
  CLIENT_METRICS = 16,
  GROUP = 32,
}

export enum ConfigSource {
  UNKNOWN = 0,
  TOPIC_CONFIG = 1,
  DYNAMIC_BROKER_CONFIG = 2,
  DYNAMIC_DEFAULT_BROKER_CONFIG = 3,
  STATIC_BROKER_CONFIG = 4,
  DEFAULT_CONFIG = 5,
  DYNAMIC_BROKER_LOGGER_CONFIG = 6,
  CLIENT_METRICS_CONFIG = 7,
  GROUP_CONFIG = 8,
}

/** The operation of one `incrementalAlterConfigs` entry. */
export enum AlterConfigOpTypes {
  SET = 0,
  DELETE = 1,
  APPEND = 2,
  SUBTRACT = 3,
}

/** The value type DescribeConfigs reports for a config entry (v3 and later). */
export enum ConfigTypes {
  UNKNOWN = 0,
  BOOLEAN = 1,
  STRING = 2,
  INT = 3,
  SHORT = 4,
  LONG = 5,
  DOUBLE = 6,
  LIST = 7,
  CLASS = 8,
  PASSWORD = 9,
}

export enum AclPermissionTypes {
  UNKNOWN = 0,
  ANY = 1,
  DENY = 2,
  ALLOW = 3,
}

export enum AclOperationTypes {
  UNKNOWN = 0,
  ANY = 1,
  ALL = 2,
  READ = 3,
  WRITE = 4,
  CREATE = 5,
  DELETE = 6,
  ALTER = 7,
  DESCRIBE = 8,
  CLUSTER_ACTION = 9,
  DESCRIBE_CONFIGS = 10,
  ALTER_CONFIGS = 11,
  IDEMPOTENT_WRITE = 12,
}

export enum ResourcePatternTypes {
  UNKNOWN = 0,
  ANY = 1,
  MATCH = 2,
  LITERAL = 3,
  PREFIXED = 4,
}

export interface ResourceConfigQuery {
  type: ConfigResourceTypes
  name: string
  configNames?: string[]
}

export interface ConfigEntries {
  configName: string
  configValue: string
  isDefault: boolean
  configSource: ConfigSource
  isSensitive: boolean
  readOnly: boolean
  configSynonyms: ConfigSynonyms[]
  /** The value type the broker reports (DescribeConfigs v3 and later). */
  configType?: ConfigTypes
  /** The config's documentation; `null` unless `includeDocumentation` was asked for. */
  documentation?: string | null
}

export interface ConfigSynonyms {
  configName: string
  configValue: string
  configSource: ConfigSource
}

export interface DescribeConfigResponse {
  resources: {
    configEntries: ConfigEntries[]
    errorCode: number
    errorMessage: string
    resourceName: string
    resourceType: ConfigResourceTypes
  }[]
  throttleTime: number
}

export interface IResourceConfigEntry {
  name: string
  value: string
}

export interface IResourceConfig {
  type: ConfigResourceTypes
  name: string
  configEntries: IResourceConfigEntry[]
}

/** One change of `incrementalAlterConfigs`; the `value` of a DELETE is ignored. */
export type IncrementalAlterConfigEntry =
  | {
      name: string
      operation:
        | AlterConfigOpTypes.SET
        | AlterConfigOpTypes.APPEND
        | AlterConfigOpTypes.SUBTRACT
      value: string
    }
  | { name: string; operation: AlterConfigOpTypes.DELETE; value?: string | null }

/** One resource of `incrementalAlterConfigs` and the changes to make to it. */
export interface IncrementalResourceConfig {
  type: ConfigResourceTypes
  /** Empty for the cluster-wide default broker config (`type: ConfigResourceTypes.BROKER`). */
  name: string
  configEntries: IncrementalAlterConfigEntry[]
}

/** One result per resource, in the order given; a failed resource sets its own `errorCode`. */
export type IncrementalAlterConfigsResult = {
  resources: Array<{
    errorCode: number
    errorMessage: string | null
    resourceType: ConfigResourceTypes
    resourceName: string
  }>
}

/** One result per member sent. */
export type RemoveMembersResult = Array<{
  memberId: string
  groupInstanceId: string | null
  errorCode: number
  /** The protocol error of this member, or `null` when it was removed. */
  error: KafkaJSProtocolError | null
}>

type ValueOf<T> = T[keyof T]

export type AdminEvents = {
  CONNECT: 'admin.connect'
  DISCONNECT: 'admin.disconnect'
  REQUEST: 'admin.network.request'
  REQUEST_TIMEOUT: 'admin.network.request_timeout'
  REQUEST_QUEUE_SIZE: 'admin.network.request_queue_size'
  AUTHENTICATION_FAILURE: 'admin.network.authentication_failure'
  TOPIC_RECREATED: 'admin.cluster.topic_recreated'
  REBOOTSTRAP: 'admin.cluster.rebootstrap'
}

export interface InstrumentationEvent<T> {
  id: string
  type: string
  timestamp: number
  payload: T
}

export type RemoveInstrumentationEventListener<T> = () => void

export type ConnectEvent = InstrumentationEvent<null>
export type InitializedEvent = InstrumentationEvent<null>
export type DisconnectEvent = InstrumentationEvent<null>
export type RequestEvent = InstrumentationEvent<{
  apiKey: number
  apiName: string
  apiVersion: number
  broker: string
  clientId: string
  correlationId: number
  createdAt: number
  duration: number
  pendingDuration: number
  sentAt: number
  size: number
}>
export type RequestTimeoutEvent = InstrumentationEvent<{
  apiKey: number
  apiName: string
  apiVersion: number
  broker: string
  clientId: string
  correlationId: number
  createdAt: number
  pendingDuration: number
  sentAt: number
}>
export type RequestQueueSizeEvent = InstrumentationEvent<{
  broker: string
  clientId: string
  queueSize: number
}>
export type AuthenticationFailureEvent = InstrumentationEvent<{
  broker: string
  mechanism: string
  errorMessage: string
}>
export type LockTimeoutEvent = InstrumentationEvent<{
  description: string
  waiting: number
  timeout: number
}>
export type TopicRecreatedEvent = InstrumentationEvent<{
  topic: string
  previousTopicId: string
  topicId: string
}>
export type RebootstrapEvent = InstrumentationEvent<{
  reason: 'timeout' | 'required' | 'all-brokers-unavailable'
}>

export type SeekEntry = PartitionOffset

export type FetchOffsetsPartition = PartitionOffset & { metadata: string | null }
export interface Acl {
  principal: string
  host: string
  operation: AclOperationTypes
  permissionType: AclPermissionTypes
}

export interface AclResource {
  resourceType: AclResourceTypes
  resourceName: string
  resourcePatternType: ResourcePatternTypes
}

export type AclEntry = Acl & AclResource

export type DescribeAclResource = AclResource & {
  acls: Acl[]
}

export interface DescribeAclResponse {
  throttleTime: number
  errorCode: number
  errorMessage?: string
  resources: DescribeAclResource[]
}

export interface AclFilter {
  resourceType: AclResourceTypes
  resourceName?: string
  resourcePatternType: ResourcePatternTypes
  principal?: string
  host?: string
  operation: AclOperationTypes
  permissionType: AclPermissionTypes
}

export interface MatchingAcl {
  errorCode: number
  errorMessage?: string
  resourceType: AclResourceTypes
  resourceName: string
  resourcePatternType: ResourcePatternTypes
  principal: string
  host: string
  operation: AclOperationTypes
  permissionType: AclPermissionTypes
}

export interface DeleteAclFilterResponses {
  errorCode: number
  errorMessage?: string
  matchingAcls: MatchingAcl[]
}

export interface DeleteAclResponse {
  throttleTime: number
  filterResponses: DeleteAclFilterResponses[]
}

export interface ListPartitionReassignmentsResponse {
  topics: OngoingTopicReassignment[]
}

export interface OngoingTopicReassignment {
  topic: string
  partitions: OngoingPartitionReassignment[]
}

export interface OngoingPartitionReassignment {
  partitionIndex: number
  replicas: number[]
  addingReplicas?: number[]
  removingReplicas?: number[]
}

export type Admin = {
  connect(): Promise<void>
  disconnect(): Promise<void>
  listTopics(): Promise<string[]>
  createTopics(options: {
    validateOnly?: boolean
    waitForLeaders?: boolean
    timeout?: number
    topics: ITopicConfig[]
  }): Promise<boolean>
  deleteTopics(options: { topics: string[]; timeout?: number }): Promise<void>
  createPartitions(options: {
    validateOnly?: boolean
    timeout?: number
    topicPartitions: ITopicPartitionConfig[]
  }): Promise<boolean>
  /**
   * With `useDescribeTopicPartitions` and at least one topic the topics are read through
   * DescribeTopicPartitions (KIP-966); without topics, or on a broker that lacks the API, through
   * Metadata, which lists every topic when none is named.
   */
  fetchTopicMetadata(options?: {
    topics?: string[]
    useDescribeTopicPartitions?: boolean
  }): Promise<{ topics: Array<ITopicMetadata> }>
  fetchOffsets(options: {
    groupId: string
    topics?: string[]
    resolveOffsets?: boolean
  }): Promise<Array<{ topic: string; partitions: FetchOffsetsPartition[] }>>
  fetchTopicOffsets(topic: string): Promise<Array<SeekEntry & { high: string; low: string }>>
  fetchTopicOffsetsByTimestamp(topic: string, timestamp?: number): Promise<Array<SeekEntry>>
  describeCluster(): Promise<{
    brokers: Array<{ nodeId: number; host: string; port: number }>
    controller: number | null
    clusterId: string
  }>
  setOffsets(options: { groupId: string; topic: string; partitions: SeekEntry[] }): Promise<void>
  resetOffsets(options: { groupId: string; topic: string; earliest: boolean }): Promise<void>
  describeConfigs(configs: {
    resources: ResourceConfigQuery[]
    includeSynonyms: boolean
    /** Ask for each entry's `documentation`; sent from DescribeConfigs v3. */
    includeDocumentation?: boolean
  }): Promise<DescribeConfigResponse>
  alterConfigs(configs: { validateOnly: boolean; resources: IResourceConfig[] }): Promise<any>
  /**
   * A failed resource is a result with its own `errorCode`, not an error; `NOT_CONTROLLER` is
   * retried. An invalid argument throws `KafkaJSNonRetriableError` before anything is sent.
   */
  incrementalAlterConfigs(configs: {
    validateOnly?: boolean
    resources: IncrementalResourceConfig[]
  }): Promise<IncrementalAlterConfigsResult>
  listGroups(options?: ListGroupsOptions): Promise<{ groups: GroupOverview[] }>
  deleteGroups(groupIds: string[]): Promise<DeleteGroupsResult[]>
  /** Without `members`, removes every member `describeGroups` reports. */
  removeMembersFromConsumerGroup(options: {
    groupId: string
    members?: Array<{ groupInstanceId: string }>
    reason?: string | null
  }): Promise<RemoveMembersResult>
  describeGroups(groupIds: string[]): Promise<GroupDescriptions>
  describeAcls(options: AclFilter): Promise<DescribeAclResponse>
  deleteAcls(options: { filters: AclFilter[] }): Promise<DeleteAclResponse>
  createAcls(options: { acl: AclEntry[] }): Promise<boolean>
  deleteTopicRecords(options: { topic: string; partitions: SeekEntry[] }): Promise<void>
  alterPartitionReassignments(request: {
    topics: PartitionReassignment[]
    timeout?: number
  }): Promise<void>
  listPartitionReassignments(request: {
    topics?: TopicPartitions[]
    timeout?: number
  }): Promise<ListPartitionReassignmentsResponse>
  logger(): Logger
  on(
    eventName: AdminEvents['CONNECT'],
    listener: (event: ConnectEvent) => void
  ): RemoveInstrumentationEventListener<typeof eventName>
  on(
    eventName: AdminEvents['DISCONNECT'],
    listener: (event: DisconnectEvent) => void
  ): RemoveInstrumentationEventListener<typeof eventName>
  on(
    eventName: AdminEvents['REQUEST'],
    listener: (event: RequestEvent) => void
  ): RemoveInstrumentationEventListener<typeof eventName>
  on(
    eventName: AdminEvents['REQUEST_QUEUE_SIZE'],
    listener: (event: RequestQueueSizeEvent) => void
  ): RemoveInstrumentationEventListener<typeof eventName>
  on(
    eventName: AdminEvents['REQUEST_TIMEOUT'],
    listener: (event: RequestTimeoutEvent) => void
  ): RemoveInstrumentationEventListener<typeof eventName>
  on(
    eventName: AdminEvents['AUTHENTICATION_FAILURE'],
    listener: (event: AuthenticationFailureEvent) => void
  ): RemoveInstrumentationEventListener<typeof eventName>
  on(
    eventName: AdminEvents['TOPIC_RECREATED'],
    listener: (event: TopicRecreatedEvent) => void
  ): RemoveInstrumentationEventListener<typeof eventName>
  on(
    eventName: AdminEvents['REBOOTSTRAP'],
    listener: (event: RebootstrapEvent) => void
  ): RemoveInstrumentationEventListener<typeof eventName>
  on(
    eventName: ValueOf<AdminEvents>,
    listener: (event: InstrumentationEvent<any>) => void
  ): RemoveInstrumentationEventListener<typeof eventName>
  readonly events: AdminEvents
}

export const PartitionAssigners: {
  roundRobin: PartitionAssigner
  range: PartitionAssigner
  sticky: PartitionAssigner
  cooperativeSticky: PartitionAssigner
}

export interface ISerializer<TIn, TOut = TIn> {
  encode(value: TIn): Buffer
  decode(buffer: Buffer): TOut | null
}

/**
 * What `AssignerProtocol.MemberMetadata.encode` accepts. `userData: null` writes the wire null
 * sentinel; omitting it, or passing `undefined`, writes an empty byte string. The wire format is
 * positional, so the `version` decides which of the optional fields are written, each omitted
 * value taking its Kafka default.
 */
export type MemberMetadataInput = {
  version: number
  topics: string[]
  userData?: Buffer | null
  ownedPartitions?: Array<{ topic: string; partitions: number[] }>
  generationId?: number
  rackId?: string | null
}

/** What `AssignerProtocol.MemberMetadata.decode` returns. */
export type MemberMetadata = {
  version: number
  topics: string[]
  userData: Buffer
  ownedPartitions?: Array<{ topic: string; partitions: number[] }>
  generationId?: number
  rackId?: string | null
}

export type MemberAssignment = {
  version: number
  assignment: Assignment
  userData: Buffer
}

export const AssignerProtocol: {
  MemberMetadata: ISerializer<MemberMetadataInput, MemberMetadata>
  MemberAssignment: ISerializer<MemberAssignment>
}

export enum logLevel {
  NOTHING = 0,
  ERROR = 1,
  WARN = 2,
  INFO = 4,
  DEBUG = 5,
}

export interface LogEntry {
  namespace: string
  level: logLevel
  label: string
  log: LoggerEntryContent
}

export interface LoggerEntryContent {
  readonly timestamp: string
  readonly message: string
  [key: string]: any
}

export type logCreator = (logLevel: logLevel) => (entry: LogEntry) => void

export type Logger = {
  info: (message: string, extra?: object) => void
  error: (message: string, extra?: object) => void
  warn: (message: string, extra?: object) => void
  debug: (message: string, extra?: object) => void

  namespace: (namespace: string, logLevel?: logLevel) => Logger
  setLogLevel: (logLevel: logLevel) => void
}

export interface BrokerMetadata {
  brokers: Array<{ nodeId: number; host: string; port: number; rack?: string }>
  topicMetadata: Array<{
    topicErrorCode: number
    topic: string
    partitionMetadata: PartitionMetadata[]
  }>
}

export interface ApiVersions {
  [apiKey: number]: {
    minVersion: number
    maxVersion: number
  }
}

/** The topic and partition a page resumes from. */
export type DescribeTopicPartitionsCursor = { topicName: string; partitionIndex: number }

/** One partition of a DescribeTopicPartitions page. */
export type DescribeTopicPartitionsPartition = {
  errorCode: number
  partitionIndex: number
  leaderId: number
  leaderEpoch: number
  replicaNodes: number[]
  isrNodes: number[]
  /** `null` when the cluster does not track eligible leader replicas, `[]` when none is eligible. */
  eligibleLeaderReplicas: number[] | null
  lastKnownElr: number[] | null
  offlineReplicas: number[]
}

/** One topic of a DescribeTopicPartitions page; one the broker cannot describe sets `errorCode`. */
export type DescribeTopicPartitionsTopic = {
  errorCode: number
  name: string | null
  /** `null` for a topic the broker cannot describe (the all-zero id). */
  topicId: string | null
  isInternal: boolean
  partitions: DescribeTopicPartitionsPartition[]
  topicAuthorizedOperations: number
}

/** One member of a batch `leaveGroup`. */
export type LeaveGroupMember = {
  /** Empty to name a static member by its instance id alone. */
  memberId: string
  groupInstanceId?: string | null
  reason?: string | null
}

export type Broker = {
  /**
   * The finalized features of the ApiVersions response the broker's current versions came from;
   * `{}` before the first negotiation.
   */
  finalizedFeatures: { [name: string]: { minVersionLevel: number; maxVersionLevel: number } }
  /** The epoch `finalizedFeatures` were finalized at; -1 when the broker sent none. */
  finalizedFeaturesEpoch: number
  isConnected(): boolean
  connect(): Promise<void>
  disconnect(): Promise<void>
  apiVersions(): Promise<ApiVersions>
  metadata(topics: string[]): Promise<BrokerMetadata>
  describeGroups: (options: { groupIds: string[] }) => Promise<any>
  offsetCommit(request: {
    groupId: string
    groupGenerationId: number
    memberId: string
    retentionTime?: number
    topics: Array<{
      topic: string
      /** When every entry has one the request is keyed by id and may go out at OffsetCommit v10. */
      topicId?: string | null
      partitions: Array<PartitionOffset & { leaderEpoch?: number }>
    }>
  }): Promise<any>
  offsetFetch(request: {
    groupId: string
    /** An entry may carry `topicId`; the request is keyed by id when every entry does. */
    topics: Array<TopicOffsets & { topicId?: string | null }>
    memberId?: string | null
    memberEpoch?: number
    /** Hold back offsets with a transactional commit pending (KIP-447); default false. */
    requireStable?: boolean
  }): Promise<{
    responses: TopicOffsets[]
  }>
  fetch(request: {
    replicaId?: number
    isolationLevel?: number
    maxWaitTime?: number
    minBytes?: number
    maxBytes?: number
    topics: Array<{
      topic: string
      partitions: Array<{
        partition: number
        fetchOffset: string
        maxBytes: number
        currentLeaderEpoch?: number
      }>
    }>
    topicIds?: Map<string, string>
    maxVersion?: number
    rackId?: string
    checkCrcs?: boolean
  }): Promise<any>
  produce(request: {
    topicData: Array<{
      topic: string
      partitions: Array<{ partition: number; firstSequence?: number; messages: Message[] }>
    }>
    topicIds?: Map<string, string>
    maxVersion?: number
    transactionalId?: string
    producerId?: number | string
    producerEpoch?: number
    acks?: number
    timeout?: number
    compression?: CompressionTypes
    /** Optional codec level for this Produce request. */
    compressionLevel?: number
    /**
     * With a `transactionalId`: 2 sends Produce v12, which adds the partition itself (KIP-890). A
     * request that would go out below v12, for want of a broker offer or under a lower
     * `maxVersion`, rejects with `KafkaJSServerDoesNotSupportApiKey` and sends nothing.
     */
    transactionVersion?: 1 | 2
  }): Promise<any>
  /**
   * InitProducerId; on a re-initialization (KIP-360) pass the producer id and epoch held, which
   * default to -1 for a first one.
   */
  initProducerId(request: {
    transactionalId?: string | null
    /** In ms; 5000 when omitted. */
    transactionTimeout?: number
    producerId?: number | string
    producerEpoch?: number
  }): Promise<{ errorCode: number; producerId: string; producerEpoch: number }>
  /**
   * EndTxn; under transaction version 2 the response carries the bumped producer id and epoch
   * (-1 and -1 when the coordinator has no pair to report).
   */
  endTxn(request: {
    transactionalId: string
    producerId: number | string
    producerEpoch: number
    transactionResult: boolean
    transactionVersion?: 1 | 2
  }): Promise<{ errorCode: number; producerId?: string; producerEpoch?: number }>
  /**
   * TxnOffsetCommit; the consumer's generation, member id and group instance id let the group
   * coordinator fence a stale member (KIP-447), and default to an unfenced commit.
   */
  txnOffsetCommit(request: {
    transactionalId: string
    groupId: string
    producerId: number | string
    producerEpoch: number
    topics: TransactionOffsets['topics']
    generationId?: number
    memberId?: string
    groupInstanceId?: string | null
    transactionVersion?: 1 | 2
  }): Promise<any>
  alterPartitionReassignments(request: {
    topics: PartitionReassignment[]
    timeout?: number
  }): Promise<any>
  listPartitionReassignments(request: {
    topics?: TopicPartitions[]
    timeout?: number
  }): Promise<ListPartitionReassignmentsResponse>
  /** `includeDocumentation` is sent from DescribeConfigs v3 and ignored by older versions. */
  describeConfigs(request: {
    resources: ResourceConfigQuery[]
    includeSynonyms?: boolean
    includeDocumentation?: boolean
  }): Promise<any>
  /** A failed resource is a result with its own `errorCode`, not an error. */
  incrementalAlterConfigs(request: {
    resources: IncrementalResourceConfig[]
    validateOnly?: boolean
  }): Promise<
    IncrementalAlterConfigsResult & { throttleTime: number; clientSideThrottleTime: number }
  >
  /** One page; resume from `nextCursor`, which is `null` after the last page. */
  describeTopicPartitions(request: {
    topics: string[]
    responsePartitionLimit?: number
    cursor?: DescribeTopicPartitionsCursor | null
  }): Promise<{
    throttleTime: number
    clientSideThrottleTime: number
    topics: DescribeTopicPartitionsTopic[]
    nextCursor: DescribeTopicPartitionsCursor | null
  }>
  /**
   * One member, which throws that member's error, or `members`, which reports each member's own
   * `errorCode` and throws only for a top-level error. Kafka 4.x answers LeaveGroup v3 or later,
   * which carries `members`.
   */
  leaveGroup(
    request:
      | {
          groupId: string
          memberId?: string
          groupInstanceId?: string | null
          reason?: string | null
          members?: undefined
        }
      | { groupId: string; members: LeaveGroupMember[] }
  ): Promise<{
    throttleTime: number
    clientSideThrottleTime: number
    errorCode: number
    members: Array<{ memberId: string; groupInstanceId: string | null; errorCode: number }>
  }>
  /** The leader sends every member's assignment; each member receives its own. */
  syncGroup(request: {
    groupId: string
    generationId: number
    memberId: string
    groupInstanceId?: string | null
    groupAssignment: Array<{ memberId: string; memberAssignment: Buffer }>
    /** The request goes out at SyncGroup v5 only when both are given, and at v4 otherwise. */
    protocolType?: string | null
    protocolName?: string | null
    sessionTimeout?: number
    rebalanceTimeout?: number
  }): Promise<any>
  offsetForLeaderEpoch(request: {
    topics: Array<{
      topic: string
      partitions: Array<{
        partition: number
        currentLeaderEpoch?: number
        leaderEpoch: number
      }>
    }>
  }): Promise<{
    throttleTime: number
    clientSideThrottleTime: number
    topics: Array<{
      topic: string
      partitions: Array<{
        errorCode: number
        partition: number
        leaderEpoch: number
        endOffset: string
      }>
    }>
  }>
}

interface MessageSetEntry {
  key: Buffer | null
  value: Buffer | null
  timestamp: string
  attributes: number
  offset: string
  size: number
  headers?: never
}

interface RecordBatchEntry {
  key: Buffer | null
  value: Buffer | null
  timestamp: string
  attributes: number
  offset: string
  headers: IHeaders
  size?: never
}

export type KafkaMessage = MessageSetEntry | RecordBatchEntry

export interface ProducerRecord {
  topic: string
  messages: Message[]
  acks?: number
  timeout?: number
  compression?: CompressionTypes
}

export type RecordMetadata = {
  topicName: string
  partition: number
  errorCode: number
  offset?: string
  timestamp?: string
  baseOffset?: string
  logAppendTime?: string
  logStartOffset?: string
}

export interface TopicMessages {
  topic: string
  messages: Message[]
}

export interface ProducerBatch {
  acks?: number
  timeout?: number
  compression?: CompressionTypes
  topicMessages?: TopicMessages[]
}

export interface PartitionOffset {
  partition: number
  offset: string
}

export interface TopicOffsets {
  topic: string
  partitions: PartitionOffset[]
}

export interface Offsets {
  topics: TopicOffsets[]
}

type Sender = {
  /**
   * Sends a produce request for one topic's messages. Rejects with a `KafkaJSProtocolError`
   * (`topic`, `partition`, `errorMessage` set) when the broker fails the whole batch; when it
   * rejected individual records (KIP-467), the error also carries
   * `recordErrors: ProduceRecordError[]`.
   */
  send(record: ProducerRecord): Promise<RecordMetadata[]>
  sendBatch(batch: ProducerBatch): Promise<RecordMetadata[]>
}

export type ProducerEvents = {
  CONNECT: 'producer.connect'
  INITIALIZED: 'producer.initialized'
  DISCONNECT: 'producer.disconnect'
  REQUEST: 'producer.network.request'
  REQUEST_TIMEOUT: 'producer.network.request_timeout'
  REQUEST_QUEUE_SIZE: 'producer.network.request_queue_size'
  AUTHENTICATION_FAILURE: 'producer.network.authentication_failure'
  LOCK_TIMEOUT: 'producer.network.lock_timeout'
  TOPIC_RECREATED: 'producer.cluster.topic_recreated'
  REBOOTSTRAP: 'producer.cluster.rebootstrap'
}

export type Producer = Sender & {
  connect(): Promise<void>
  disconnect(): Promise<void>
  isIdempotent(): boolean
  readonly events: ProducerEvents
  on(
    eventName: ProducerEvents['CONNECT'],
    listener: (event: ConnectEvent) => void
  ): RemoveInstrumentationEventListener<typeof eventName>
  on(
    eventName: ProducerEvents['INITIALIZED'],
    listener: (event: InitializedEvent) => void
  ): RemoveInstrumentationEventListener<typeof eventName>
  on(
    eventName: ProducerEvents['DISCONNECT'],
    listener: (event: DisconnectEvent) => void
  ): RemoveInstrumentationEventListener<typeof eventName>
  on(
    eventName: ProducerEvents['REQUEST'],
    listener: (event: RequestEvent) => void
  ): RemoveInstrumentationEventListener<typeof eventName>
  on(
    eventName: ProducerEvents['REQUEST_QUEUE_SIZE'],
    listener: (event: RequestQueueSizeEvent) => void
  ): RemoveInstrumentationEventListener<typeof eventName>
  on(
    eventName: ProducerEvents['REQUEST_TIMEOUT'],
    listener: (event: RequestTimeoutEvent) => void
  ): RemoveInstrumentationEventListener<typeof eventName>
  on(
    eventName: ProducerEvents['AUTHENTICATION_FAILURE'],
    listener: (event: AuthenticationFailureEvent) => void
  ): RemoveInstrumentationEventListener<typeof eventName>
  on(
    eventName: ProducerEvents['LOCK_TIMEOUT'],
    listener: (event: LockTimeoutEvent) => void
  ): RemoveInstrumentationEventListener<typeof eventName>
  on(
    eventName: ProducerEvents['TOPIC_RECREATED'],
    listener: (event: TopicRecreatedEvent) => void
  ): RemoveInstrumentationEventListener<typeof eventName>
  on(
    eventName: ProducerEvents['REBOOTSTRAP'],
    listener: (event: RebootstrapEvent) => void
  ): RemoveInstrumentationEventListener<typeof eventName>
  on(
    eventName: ValueOf<ProducerEvents>,
    listener: (event: InstrumentationEvent<any>) => void
  ): RemoveInstrumentationEventListener<typeof eventName>
  transaction(): Promise<Transaction>
  logger(): Logger
}

/** What `consumer.groupMetadata()` returns: the membership a transactional offset commit carries. */
export interface ConsumerGroupMetadata {
  groupId: string
  /** Generation id under the classic protocol; member epoch under groupProtocol: 'consumer'. */
  generationId: number
  memberId: string
  groupInstanceId: string | null
}

export type TransactionOffsets = {
  topics: Array<{
    topic: string
    partitions: Array<{
      partition: number
      offset: string
      /** KIP-320 committed leader epoch; -1 when omitted. */
      leaderEpoch?: number
      metadata?: string | null
    }>
  }>
}

export type Transaction = Sender & {
  /**
   * Adds consumed offsets to the transaction: with `consumerGroupMetadata` (from
   * `consumer.groupMetadata()`) the commit is fenced against a stale member (KIP-447), with
   * `consumerGroupId` it is not. Exactly one of the two.
   */
  sendOffsets(
    offsets: TransactionOffsets &
      (
        | { consumerGroupMetadata: ConsumerGroupMetadata; consumerGroupId?: never }
        | { consumerGroupId: string; consumerGroupMetadata?: never }
      )
  ): Promise<void>
  commit(): Promise<void>
  abort(): Promise<void>
  isActive(): boolean
}

export type ConsumerGroup = {
  groupId: string
  generationId: number
  memberId: string
  coordinator: Broker
}

export type MemberDescription = {
  clientHost: string
  clientId: string
  memberId: string
  memberAssignment: Buffer
  memberMetadata: Buffer
  /** Static membership id, `null` for a dynamic member. */
  groupInstanceId?: string | null
  rackId?: string | null
  /** Consumer-protocol members only; `null` for a classic member. */
  memberEpoch?: number | null
  /** Decoded current assignment. `[]` for a classic member whose assignment could not be decoded. */
  assignment?: TopicPartitions[]
  /** Consumer-protocol members only; `null` otherwise. */
  targetAssignment?: TopicPartitions[] | null
  /** Reported by ConsumerGroupDescribe v1 only; `null` otherwise. */
  memberType?: 'classic' | 'consumer' | null
  /** Consumer-protocol members only; `[]` for a classic member. */
  subscribedTopics?: string[]
  /** Consumer-protocol members only; `null` otherwise. */
  subscribedTopicRegex?: string | null
}

// See https://github.com/apache/kafka/blob/2.4.0/clients/src/main/java/org/apache/kafka/common/ConsumerGroupState.java#L25
// 'Assigning', 'Reconciling' and 'NotReady' are the newer group states of
// https://github.com/apache/kafka/blob/trunk/clients/src/main/java/org/apache/kafka/common/GroupState.java
export type ConsumerGroupState =
  | 'Unknown'
  | 'PreparingRebalance'
  | 'CompletingRebalance'
  | 'Stable'
  | 'Dead'
  | 'Empty'
  | 'Assigning'
  | 'Reconciling'
  | 'NotReady'

export type GroupDescription = {
  groupId: string
  members: MemberDescription[]
  protocol: string
  protocolType: string
  state: ConsumerGroupState
  /** Group-level error for this id; `0` when the description succeeded. */
  errorCode: number
  errorMessage?: string | null
  groupType?: 'classic' | 'consumer' | 'share' | 'streams' | 'unknown'
  /** Consumer groups only; `null` for a classic group. */
  groupEpoch?: number | null
  assignmentEpoch?: number | null
  /** Server-side assignor name for a consumer group; `null` for a classic group. */
  assignorName?: string | null
  /** `null` when authorized operations were not requested. */
  authorizedOperations?: number | null
}

export type GroupDescriptions = {
  groups: GroupDescription[]
}

export type TopicPartitions = { topic: string; partitions: number[] }

export type TopicPartition = {
  topic: string
  partition: number
}
export type TopicPartitionOffset = TopicPartition & {
  offset: string
}
export type TopicPartitionOffsetAndMetadata = TopicPartitionOffset & {
  metadata?: string | null
}

export type Batch = {
  topic: string
  partition: number
  highWatermark: string
  messages: KafkaMessage[]
  isEmpty(): boolean
  firstOffset(): string | null
  lastOffset(): string
  offsetLag(): string
  offsetLagLow(): string
}

/** Filters for `admin.listGroups`. Values are lower-case, e.g. `'stable'`, `'consumer'`. */
export type ListGroupsOptions = { states?: string[]; types?: string[] }

export type GroupOverview = {
  groupId: string
  protocolType: string
  /** Wire-cased state, e.g. `'Stable'`. `''` against a broker whose ListGroups version lacks it. */
  groupState: string
  /**
   * Lower-cased by the client. `''` against a broker whose ListGroups version lacks it.
   *
   * The literals are the group types Kafka defines today and are there for autocomplete; a type a
   * newer broker introduces is passed through lower-cased rather than dropped, so the union stays
   * open and an exhaustive `switch` over it needs a `default`.
   */
  groupType: 'classic' | 'consumer' | 'share' | 'streams' | 'unknown' | '' | (string & {})
}

export type DeleteGroupsResult = {
  groupId: string
  errorCode?: number
  error?: KafkaJSProtocolError
}

export type ConsumerEvents = {
  HEARTBEAT: 'consumer.heartbeat'
  LOG_TRUNCATION: 'consumer.log_truncation'
  COMMIT_OFFSETS: 'consumer.commit_offsets'
  GROUP_JOIN: 'consumer.group_join'
  FETCH_START: 'consumer.fetch_start'
  FETCH: 'consumer.fetch'
  START_BATCH_PROCESS: 'consumer.start_batch_process'
  END_BATCH_PROCESS: 'consumer.end_batch_process'
  CONNECT: 'consumer.connect'
  DISCONNECT: 'consumer.disconnect'
  STOP: 'consumer.stop'
  CRASH: 'consumer.crash'
  REBALANCING: 'consumer.rebalancing'
  PARTITION_HALTED: 'consumer.partition_halted'
  PARTITIONS_ASSIGNED: 'consumer.partitions_assigned'
  PARTITIONS_REVOKED: 'consumer.partitions_revoked'
  PARTITIONS_LOST: 'consumer.partitions_lost'
  POLL_INTERVAL_EXCEEDED: 'consumer.poll_interval_exceeded'
  RECEIVED_UNSUBSCRIBED_TOPICS: 'consumer.received_unsubscribed_topics'
  REQUEST: 'consumer.network.request'
  REQUEST_TIMEOUT: 'consumer.network.request_timeout'
  REQUEST_QUEUE_SIZE: 'consumer.network.request_queue_size'
  AUTHENTICATION_FAILURE: 'consumer.network.authentication_failure'
  TOPIC_RECREATED: 'consumer.cluster.topic_recreated'
  REBOOTSTRAP: 'consumer.cluster.rebootstrap'
}
export type LogTruncationEvent = InstrumentationEvent<{
  topic: string
  partition: number
  offset: string
  divergingOffset: string | null
  leaderEpoch: number
}>
export type ConsumerHeartbeatEvent = InstrumentationEvent<{
  groupId: string
  memberId: string
  groupGenerationId: number
  groupInstanceId: string | null
}>
export type ConsumerCommitOffsetsEvent = InstrumentationEvent<{
  groupId: string
  memberId: string
  groupGenerationId: number
  groupInstanceId: string | null
  topics: Array<{
    topic: string
    partitions: Array<PartitionOffset & { leaderEpoch?: number }>
  }>
}>
export interface IMemberAssignment {
  [key: string]: number[]
}
export type ConsumerGroupJoinEvent = InstrumentationEvent<{
  duration: number
  groupId: string
  isLeader: boolean
  /** `null` under `groupProtocol: 'consumer'`, where the group has no leader. */
  leaderId: string | null
  /** The selected assigner protocol name; the server assignor name, or `''`, under 'consumer'. */
  groupProtocol: string
  /** Which membership protocol this member joined with. */
  membershipProtocol: 'classic' | 'consumer'
  memberId: string
  memberAssignment: IMemberAssignment
  rebalanceProtocol: 'eager' | 'cooperative'
}>
export type ConsumerFetchStartEvent = InstrumentationEvent<{ nodeId: number }>
export type ConsumerFetchEvent = InstrumentationEvent<{
  numberOfBatches: number
  duration: number
  nodeId: number
}>
interface IBatchProcessEvent {
  topic: string
  partition: number
  highWatermark: string
  offsetLag: string
  offsetLagLow: string
  batchSize: number
  firstOffset: string
  lastOffset: string
}
export type ConsumerStartBatchProcessEvent = InstrumentationEvent<IBatchProcessEvent>
export type ConsumerEndBatchProcessEvent = InstrumentationEvent<
  IBatchProcessEvent & { duration: number }
>
export type ConsumerCrashEvent = InstrumentationEvent<{
  error: Error
  groupId: string
  restart: boolean
}>
export type ConsumerRebalancingEvent = InstrumentationEvent<{
  groupId: string
  /** `null` when the coordinator rejected this member's id, which is dropped before the rejoin. */
  memberId: string | null
  /** Which membership protocol this round belongs to. */
  membershipProtocol: 'classic' | 'consumer'
  /** Why the rebalance started. Absent on the first join of a run, which reports no reason. */
  reason?:
    | 'rebalance-in-progress'
    | 'lost'
    | 'poll-interval-exceeded'
    | 'partitions-revoked'
    | 'stale-assignment'
    | 'coordinator-changed'
    | 'assignment-changed'
    | 'fenced'
}>
/** Payload of `PARTITIONS_ASSIGNED`, `PARTITIONS_REVOKED` and `PARTITIONS_LOST`. */
export type ConsumerPartitionsEvent = InstrumentationEvent<{
  groupId: string
  /** `null` while this member has no id: before its first successful join, or after one was lost. */
  memberId: string | null
  /** `null` once the generation has been dropped, which is the case on `PARTITIONS_LOST`. */
  generationId: number | null
  partitions: Array<{ topic: string; partitions: number[] }>
}>
export type ConsumerPartitionsAssignedEvent = ConsumerPartitionsEvent
export type ConsumerPartitionsRevokedEvent = ConsumerPartitionsEvent
export type ConsumerPartitionsLostEvent = ConsumerPartitionsEvent
/** A structurally corrupt Fetch batch halted one partition; a seek past the batch resumes it. */
export interface ConsumerPartitionHaltedEventPayload {
  groupId: string
  memberId: string
  topic: string
  partition: number
  /** The first offset of the unreadable batch, as a decimal string. */
  offset: string
  reason: KafkaJSCorruptRecordError['reason']
  error: KafkaJSCorruptRecordError
}
export type ConsumerPartitionHaltedEvent = InstrumentationEvent<ConsumerPartitionHaltedEventPayload>
export type ConsumerPollIntervalExceededEvent = InstrumentationEvent<{
  groupId: string
  /** `null` while this member has no id: before its first successful join, or after one was lost. */
  memberId: string | null
  /** `null` once the generation has been dropped. */
  generationId: number | null
  maxPollInterval: number
  elapsedMs: number
  isStatic: boolean
}>
export type ConsumerReceivedUnsubcribedTopicsEvent = InstrumentationEvent<{
  groupId: string
  generationId: number
  memberId: string
  assignedTopics: string[]
  topicsSubscribed: string[]
  topicsNotSubscribed: string[]
}>

export interface OffsetsByTopicPartition {
  topics: Array<{
    topic: string
    partitions: Array<PartitionOffset & { leaderEpoch?: number }>
  }>
}

export interface EachMessagePayload {
  topic: string
  partition: number
  message: KafkaMessage
  heartbeat(): Promise<void>
  pause(): () => void
}

export interface EachBatchPayload {
  batch: Batch
  resolveOffset(offset: string): void
  heartbeat(): Promise<void>
  pause(): () => void
  commitOffsetsIfNecessary(offsets?: Offsets): Promise<void>
  uncommittedOffsets(): OffsetsByTopicPartition
  isRunning(): boolean
  isStale(): boolean
}

/**
 * Type alias to keep compatibility with @types/kafkajs
 * @see https://github.com/DefinitelyTyped/DefinitelyTyped/blob/712ad9d59ccca6a3cc92f347fea0d1c7b02f5eeb/types/kafkajs/index.d.ts#L321-L325
 */
export type ConsumerEachMessagePayload = EachMessagePayload

/**
 * Type alias to keep compatibility with @types/kafkajs
 * @see https://github.com/DefinitelyTyped/DefinitelyTyped/blob/712ad9d59ccca6a3cc92f347fea0d1c7b02f5eeb/types/kafkajs/index.d.ts#L327-L336
 */
export type ConsumerEachBatchPayload = EachBatchPayload

export type EachBatchHandler = (payload: EachBatchPayload) => Promise<void>
export type EachMessageHandler = (payload: EachMessagePayload) => Promise<void>

export type ConsumerRunConfig = {
  autoCommit?: boolean
  autoCommitInterval?: number | null
  autoCommitThreshold?: number | null
  eachBatchAutoResolve?: boolean
  partitionsConsumedConcurrently?: number
  eachBatch?: EachBatchHandler
  eachMessage?: EachMessageHandler
}

/**
 * @deprecated Replaced by ConsumerSubscribeTopics
 */
export type ConsumerSubscribeTopic = {
  topic: string | RegExp
  topics?: never
  regex?: never
  fromBeginning?: boolean
}
export type ConsumerSubscribeTopics = {
  topics: (string | RegExp)[]
  topic?: never
  regex?: never
  fromBeginning?: boolean
}
/**
 * A broker-evaluated regular expression subscription, available only under
 * `groupProtocol: 'consumer'`. The pattern is sent verbatim as an RE2 expression and is evaluated
 * by the coordinator, so topics created later are picked up without a client-side metadata poll.
 * It cannot be combined with a topic or `topics` subscription on the same consumer - the runtime
 * throws, and the `never`-branded keys above make the same mistake a compile error.
 */
export type ConsumerSubscribeRegex = {
  regex: string
  topic?: never
  topics?: never
  fromBeginning?: boolean
}

export type Consumer = {
  connect(): Promise<void>
  /**
   * `leaveGroup: true` makes the member leave the group, so its partitions are handed over at
   * once, even a static member, which otherwise stays until its `sessionTimeout`; `false` skips
   * the leave a dynamic member sends by default, and the group notices when the session or the
   * heartbeat expires. An omitted `leaveGroup` keeps the default. An argument that is not an
   * options object is ignored at run time, as in 2.2.4; the signature rejects it, but passing
   * `consumer.disconnect` itself to a signal handler still compiles. The signature without
   * parameters comes last so that `Parameters<Consumer['disconnect']>` is `[]`, as in 2.2.4.
   */
  disconnect(options?: { leaveGroup?: boolean }): Promise<void>
  disconnect(): Promise<void>
  subscribe(
    subscription: ConsumerSubscribeTopics | ConsumerSubscribeTopic | ConsumerSubscribeRegex
  ): Promise<void>
  stop(): Promise<void>
  run(config?: ConsumerRunConfig): Promise<void>
  /**
   * The consumer's group membership, for `transaction.sendOffsets({ consumerGroupMetadata })`.
   * A snapshot taken at the call: read it again after every rebalance.
   * @throws KafkaJSNonRetriableError before run() and after stop()
   */
  groupMetadata(): ConsumerGroupMetadata
  commitOffsets(topicPartitions: Array<TopicPartitionOffsetAndMetadata>): Promise<void>
  seek(topicPartitionOffset: TopicPartitionOffset): void
  describeGroup(): Promise<GroupDescription>
  pause(topics: Array<{ topic: string; partitions?: number[] }>): void
  paused(): TopicPartitions[]
  resume(topics: Array<{ topic: string; partitions?: number[] }>): void
  on(
    eventName: ConsumerEvents['HEARTBEAT'],
    listener: (event: ConsumerHeartbeatEvent) => void
  ): RemoveInstrumentationEventListener<typeof eventName>
  on(
    eventName: ConsumerEvents['LOG_TRUNCATION'],
    listener: (event: LogTruncationEvent) => void
  ): RemoveInstrumentationEventListener<typeof eventName>
  on(
    eventName: ConsumerEvents['COMMIT_OFFSETS'],
    listener: (event: ConsumerCommitOffsetsEvent) => void
  ): RemoveInstrumentationEventListener<typeof eventName>
  on(
    eventName: ConsumerEvents['GROUP_JOIN'],
    listener: (event: ConsumerGroupJoinEvent) => void
  ): RemoveInstrumentationEventListener<typeof eventName>
  on(
    eventName: ConsumerEvents['FETCH_START'],
    listener: (event: ConsumerFetchStartEvent) => void
  ): RemoveInstrumentationEventListener<typeof eventName>
  on(
    eventName: ConsumerEvents['FETCH'],
    listener: (event: ConsumerFetchEvent) => void
  ): RemoveInstrumentationEventListener<typeof eventName>
  on(
    eventName: ConsumerEvents['START_BATCH_PROCESS'],
    listener: (event: ConsumerStartBatchProcessEvent) => void
  ): RemoveInstrumentationEventListener<typeof eventName>
  on(
    eventName: ConsumerEvents['END_BATCH_PROCESS'],
    listener: (event: ConsumerEndBatchProcessEvent) => void
  ): RemoveInstrumentationEventListener<typeof eventName>
  on(
    eventName: ConsumerEvents['CONNECT'],
    listener: (event: ConnectEvent) => void
  ): RemoveInstrumentationEventListener<typeof eventName>
  on(
    eventName: ConsumerEvents['DISCONNECT'],
    listener: (event: DisconnectEvent) => void
  ): RemoveInstrumentationEventListener<typeof eventName>
  on(
    eventName: ConsumerEvents['STOP'],
    listener: (event: InstrumentationEvent<null>) => void
  ): RemoveInstrumentationEventListener<typeof eventName>
  on(
    eventName: ConsumerEvents['CRASH'],
    listener: (event: ConsumerCrashEvent) => void
  ): RemoveInstrumentationEventListener<typeof eventName>
  on(
    eventName: ConsumerEvents['REBALANCING'],
    listener: (event: ConsumerRebalancingEvent) => void
  ): RemoveInstrumentationEventListener<typeof eventName>
  on(
    eventName: ConsumerEvents['PARTITIONS_ASSIGNED'],
    listener: (event: ConsumerPartitionsAssignedEvent) => void
  ): RemoveInstrumentationEventListener<typeof eventName>
  on(
    eventName: ConsumerEvents['PARTITIONS_REVOKED'],
    listener: (event: ConsumerPartitionsRevokedEvent) => void
  ): RemoveInstrumentationEventListener<typeof eventName>
  on(
    eventName: ConsumerEvents['PARTITIONS_LOST'],
    listener: (event: ConsumerPartitionsLostEvent) => void
  ): RemoveInstrumentationEventListener<typeof eventName>
  on(
    eventName: ConsumerEvents['PARTITION_HALTED'],
    listener: (event: ConsumerPartitionHaltedEvent) => void
  ): RemoveInstrumentationEventListener<typeof eventName>
  on(
    eventName: ConsumerEvents['POLL_INTERVAL_EXCEEDED'],
    listener: (event: ConsumerPollIntervalExceededEvent) => void
  ): RemoveInstrumentationEventListener<typeof eventName>
  on(
    eventName: ConsumerEvents['RECEIVED_UNSUBSCRIBED_TOPICS'],
    listener: (event: ConsumerReceivedUnsubcribedTopicsEvent) => void
  ): RemoveInstrumentationEventListener<typeof eventName>
  on(
    eventName: ConsumerEvents['REQUEST'],
    listener: (event: RequestEvent) => void
  ): RemoveInstrumentationEventListener<typeof eventName>
  on(
    eventName: ConsumerEvents['REQUEST_TIMEOUT'],
    listener: (event: RequestTimeoutEvent) => void
  ): RemoveInstrumentationEventListener<typeof eventName>
  on(
    eventName: ConsumerEvents['REQUEST_QUEUE_SIZE'],
    listener: (event: RequestQueueSizeEvent) => void
  ): RemoveInstrumentationEventListener<typeof eventName>
  on(
    eventName: ConsumerEvents['AUTHENTICATION_FAILURE'],
    listener: (event: AuthenticationFailureEvent) => void
  ): RemoveInstrumentationEventListener<typeof eventName>
  on(
    eventName: ConsumerEvents['TOPIC_RECREATED'],
    listener: (event: TopicRecreatedEvent) => void
  ): RemoveInstrumentationEventListener<typeof eventName>
  on(
    eventName: ConsumerEvents['REBOOTSTRAP'],
    listener: (event: RebootstrapEvent) => void
  ): RemoveInstrumentationEventListener<typeof eventName>
  on(
    eventName: ValueOf<ConsumerEvents>,
    listener: (event: InstrumentationEvent<any>) => void
  ): RemoveInstrumentationEventListener<typeof eventName>
  logger(): Logger
  readonly events: ConsumerEvents
}

export enum CompressionTypes {
  None = 0,
  GZIP = 1,
  Snappy = 2,
  LZ4 = 3,
  ZSTD = 4,
}

export var CompressionCodecs: {
  [CompressionTypes.GZIP]: () => any
  [CompressionTypes.Snappy]: () => any
  [CompressionTypes.LZ4]: () => any
  [CompressionTypes.ZSTD]: () => any
}

/** What the caller should do about an error (KIP-1050). */
export type KafkaJSErrorCategory =
  | 'retriable'
  | 'refreshRetriable'
  | 'abortable'
  | 'applicationRecoverable'
  | 'invalidConfiguration'

export class KafkaJSError extends Error {
  readonly message: Error['message']
  readonly name: string
  readonly retriable: boolean
  /** KIP-1050 category. Present on every error this package raises through its error classes. */
  readonly category: KafkaJSErrorCategory
  readonly helpUrl?: string
  readonly cause?: Error
  /** Protocol error type, carried through when the wrapped error had one */
  readonly type?: string
  /** Protocol error code, carried through when the wrapped error had one */
  readonly code?: number

  constructor(e: Error | string, metadata?: KafkaJSErrorMetadata)
}

export class KafkaJSNonRetriableError extends KafkaJSError {
  constructor(e: Error | string)
}

export class KafkaJSProtocolError extends KafkaJSError {
  readonly code: number
  readonly type: string
  /** Set when the error came from a per-partition response (e.g. a failed produce batch) */
  readonly topic?: string
  /** Set when the error came from a per-partition response (e.g. a failed produce batch) */
  readonly partition?: number
  /** The broker's own message for this error, when it sent one */
  readonly errorMessage?: string | null
  /** The records the broker rejected individually out of a failed produce batch (KIP-467) */
  readonly recordErrors?: ProduceRecordError[]
  /**
   * Set on what `admin.deleteTopics` and `admin.createPartitions` throw: every topic the broker
   * failed, where the error itself names the first one's code.
   */
  readonly topicErrors?: Array<{
    topic: string
    errorCode: number
    errorMessage?: string | null
    topicId?: string | null
  }>
  constructor(e: Error | string)
}

/**
 * A single record the broker rejected out of an otherwise-failed produce batch (KIP-467).
 * `batchIndex` is the record's position as sent to that partition; `record` is the corresponding
 * entry from the `messages` array passed to `Producer#send`/`sendBatch`, when the client could
 * resolve it.
 */
export type ProduceRecordError = {
  batchIndex: number
  record?: Message
  errorMessage: string | null
}

export class KafkaJSAggregateError extends Error {
  readonly errors: (Error | string)[]
  readonly category: KafkaJSErrorCategory
  /** True when there is at least one child and every child is retriable. */
  readonly retriable: boolean
  constructor(message: Error | string, errors: (Error | string)[])
}

/** Raised inside the consumer's fetch loop when the group rebalances; its `name` is `'Error'`. */
export class KafkaJSFetcherRebalanceError extends Error {
  readonly category: KafkaJSErrorCategory
}

/** Retriable once the client refreshed its metadata or coordinator. */
export class KafkaJSRefreshRetriableError extends KafkaJSError {
  constructor(e: Error | string, options?: { cause?: Error })
}

/** The transaction cannot complete: abort it and keep the producer. */
export class KafkaJSTransactionAbortableError extends KafkaJSNonRetriableError {
  constructor(e: Error | string, options?: { cause?: Error })
}

/** The producer cannot continue: create a new one. */
export class KafkaJSApplicationRecoverableError extends KafkaJSNonRetriableError {
  constructor(e: Error | string, options?: { cause?: Error })
}

/** The configuration, a record or an ACL is wrong: fix it. */
export class KafkaJSInvalidConfigurationError extends KafkaJSNonRetriableError {
  constructor(e: Error | string, options?: { cause?: Error })
}

/**
 * A send ran out of its `deliveryTimeout` (KIP-91), or any retried operation out of its
 * `retry.maxTotalTime`; `cause` is the last error it met. Abortable: inside a transaction the
 * transaction has to be aborted, outside one the producer stays usable.
 */
export class KafkaJSDeliveryTimeout extends KafkaJSNonRetriableError {
  /** Transmission evidence for this delivery; absence makes no claim about delivery. */
  readonly outcome?: 'unsent' | 'unknown' 
  constructor(e: Error | string, options?: { cause?: Error; outcome?: 'unsent' | 'unknown' })
}

/**
 * A response frame was negative, too short, or larger than `maxResponseSize`, or could not be
 * processed (`reason: 'unreadable'`, with `size` null and the failure as `cause`). The connection
 * it arrived on is closed and every request on it fails with this error, which is retriable.
 */
export class KafkaJSInvalidFrameError extends KafkaJSConnectionError {
  readonly size: number | null
  readonly reason: 'negative' | 'too-short' | 'too-large' | 'unreadable'
  constructor(
    e: Error | string,
    options: {
      broker?: string
      size?: number | null
      reason: KafkaJSInvalidFrameError['reason']
      cause?: Error
    }
  )
}

/**
 * A Fetch record batch is structurally corrupt; non-retriable. The partition is halted
 * (`consumer.partition_halted`).
 */
export class KafkaJSCorruptRecordError extends KafkaJSNonRetriableError {
  readonly reason: 'length' | 'magic' | 'count' | 'control'
  /** The first offset of the batch as a decimal string, when it could be read. */
  readonly offset?: string
  constructor(
    e: Error | string,
    options: { reason: KafkaJSCorruptRecordError['reason']; offset?: string; cause?: Error }
  )
}

/**
 * An offset commit was refused because the consumer lost its group membership, or a handler or
 * retry outlived a rejoin. For a loss the broker reported, `type`, `code` and `helpUrl` are those
 * of the loss; a synthetic loss carries no `code`, and a stale handler has the `type`
 * `STALE_ASSIGNMENT` and no `code`. Test `instanceof KafkaJSCommitFailedError` and `reason`.
 * `cause` is the loss.
 */
export class KafkaJSCommitFailedError extends KafkaJSNonRetriableError {
  readonly reason: 'membership-lost' | 'poll-interval-exceeded' | 'stale-assignment'
  readonly cause: Error
  constructor(e: Error | string, options: { reason: KafkaJSCommitFailedError['reason']; cause: Error })
}

export class KafkaJSOffsetOutOfRange extends KafkaJSProtocolError {
  readonly topic: string
  readonly partition: number
  constructor(e: Error | string, metadata?: KafkaJSOffsetOutOfRangeMetadata)
}

/**
 * JoinGroup answered `MEMBER_ID_REQUIRED`: `memberId` is the id the coordinator assigned. The
 * client joins again with it by itself.
 */
export class KafkaJSMemberIdRequired extends KafkaJSProtocolError {
  readonly memberId: string
  constructor(e: Error | string, options: { memberId: string })
}

export class KafkaJSAlterPartitionReassignmentsError extends KafkaJSProtocolError {
  readonly topic?: string
  readonly partition?: number
  constructor(e: Error | string, topic?: string, partition?: number)
}

/**
 * The broker refused to create `topic`. `admin.createTopics` throws one of these per refused topic
 * among the `errors` of a `KafkaJSAggregateError`.
 */
export class KafkaJSCreateTopicError extends KafkaJSProtocolError {
  readonly topic?: string
  constructor(e: Error | string, topicName?: string)
}

export class KafkaJSNumberOfRetriesExceeded extends KafkaJSNonRetriableError {
  readonly stack: string
  readonly retryCount: number
  readonly retryTime: number
  constructor(e: Error | string, metadata?: KafkaJSNumberOfRetriesExceededMetadata)
}

/**
 * A condition the client's own logic rules out: a bug in the client, to report. The message starts
 * with `Invariant violated:`, and `helpUrl` links a prefilled issue report.
 */
export class KafkaJSInvariantViolation extends KafkaJSNonRetriableError {
  constructor(e: Error | string)
}

/**
 * A variable-length integer ran past its 5 bytes while a response or record was decoded. Its
 * `name` is `'KafkaJSNonRetriableError'`.
 */
export class KafkaJSInvalidVarIntError extends KafkaJSNonRetriableError {
  constructor(e: Error | string, options?: { cause?: Error })
}

/**
 * A variable-length long ran past its 9 bytes while a response or record was decoded. Its `name`
 * is `'KafkaJSNonRetriableError'`.
 */
export class KafkaJSInvalidLongError extends KafkaJSNonRetriableError {
  constructor(e: Error | string, options?: { cause?: Error })
}

export class KafkaJSConnectionError extends KafkaJSError {
  readonly broker: string
  constructor(e: Error | string, metadata?: KafkaJSConnectionErrorMetadata)
}

/**
 * A connection was closed under a request: the broker ended it, a request on it timed out, the
 * client disconnected it, or its pool was replaced before the request was sent. Retriable.
 * `broker` is `host:port`.
 */
export class KafkaJSConnectionClosedError extends KafkaJSConnectionError {
  readonly host?: string
  readonly port?: number
  constructor(e: Error | string, options?: { host?: string; port?: number })
}

export class KafkaJSRequestTimeoutError extends KafkaJSError {
  readonly broker: string
  readonly correlationId: number
  readonly createdAt: number
  readonly sentAt: number
  readonly pendingDuration: number
  constructor(e: Error | string, metadata?: KafkaJSRequestTimeoutErrorMetadata)
}

export class KafkaJSMetadataNotLoaded extends KafkaJSError {
  constructor()
}

export class KafkaJSTopicMetadataNotLoaded extends KafkaJSMetadataNotLoaded {
  readonly topic: string
  constructor(e: Error | string, metadata?: KafkaJSTopicMetadataNotLoadedMetadata)
}

export class KafkaJSStaleTopicMetadataAssignment extends KafkaJSError {
  readonly topic: string
  readonly unknownPartitions: number
  constructor(e: Error | string, metadata?: KafkaJSStaleTopicMetadataAssignmentMetadata)
}

export class KafkaJSServerDoesNotSupportApiKey extends KafkaJSNonRetriableError {
  readonly apiKey: number
  readonly apiName: string
  constructor(e: Error | string, metadata?: KafkaJSServerDoesNotSupportApiKeyMetadata)
}

export class KafkaJSBrokerNotFound extends KafkaJSError {
  constructor()
}

/** The consumer's fetch manager found no broker to fetch from. Retriable. */
export class KafkaJSNoBrokerAvailableError extends KafkaJSError {
  constructor()
}

export class KafkaJSPartialMessageError extends KafkaJSError {
  constructor()
  /** Set when this error is a record-batch CRC32C mismatch (see `checkCrcs`), rather than a truncated fetch tail. */
  crcMismatch?: boolean
}

export class KafkaJSSASLAuthenticationError extends KafkaJSError {
  constructor()
}

export class KafkaJSGroupCoordinatorNotFound extends KafkaJSError {
  constructor()
}

export class KafkaJSNotImplemented extends KafkaJSError {
  constructor()
}

export class KafkaJSTimeout extends KafkaJSError {
  constructor()
}

export class KafkaJSLockTimeout extends KafkaJSError {
  constructor()
}

export class KafkaJSUnsupportedMagicByteInMessageSet extends KafkaJSError {
  constructor()
}

export class KafkaJSDeleteGroupsError extends KafkaJSError {
  readonly groups: DeleteGroupsResult[]
  constructor(e: Error | string, groups?: KafkaJSDeleteGroupsErrorGroups[])
}

export class KafkaJSDeleteTopicRecordsError extends KafkaJSError {
  constructor(metadata: KafkaJSDeleteTopicRecordsErrorTopic)
}

export interface KafkaJSDeleteGroupsErrorGroups {
  groupId: string
  errorCode: number
  error: KafkaJSError
}

export interface KafkaJSDeleteTopicRecordsErrorTopic {
  topic: string
  partitions: KafkaJSDeleteTopicRecordsErrorPartition[]
}

export interface KafkaJSDeleteTopicRecordsErrorPartition {
  partition: number
  offset: string
  error: KafkaJSError
}

export interface KafkaJSErrorMetadata {
  retriable?: boolean
  topic?: string
  partitionId?: number
  metadata?: PartitionMetadata
}

export interface KafkaJSOffsetOutOfRangeMetadata {
  topic: string
  partition: number
}

export interface KafkaJSNumberOfRetriesExceededMetadata {
  retryCount: number
  retryTime: number
}

export interface KafkaJSConnectionErrorMetadata {
  broker?: string
  code?: string
  /** Original failure, retained by identity. */
  cause?: Error
}

export interface KafkaJSRequestTimeoutErrorMetadata {
  broker: string
  clientId: string
  correlationId: number
  createdAt: number
  sentAt: number
  pendingDuration: number
}

export interface KafkaJSTopicMetadataNotLoadedMetadata {
  topic: string
}

export interface KafkaJSStaleTopicMetadataAssignmentMetadata {
  topic: string
  unknownPartitions: PartitionMetadata[]
}

export interface KafkaJSServerDoesNotSupportApiKeyMetadata {
  apiKey: number
  apiName: string
}
