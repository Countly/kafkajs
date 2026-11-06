// Compiled with moduleResolution node16 against the packed package — proves the "types" export condition resolves.
import {
  Kafka,
  logLevel,
  CompressionTypes,
  KafkaJSError,
  KafkaJSCommitFailedError,
  KafkaJSNoOffsetError,
  KafkaJSCorruptRecordError,
  KafkaJSBufferExhaustedError,
  KafkaJSRecordTooLargeError,
  KafkaJSConnectionClosedError,
  KafkaJSMemberIdRequired,
  KafkaJSInvariantViolation,
  KafkaJSInvalidVarIntError,
  KafkaJSInvalidLongError,
  KafkaJSCreateTopicError,
  KafkaJSNoBrokerAvailableError,
} from '@countly/kafkajs'

const kafka = new Kafka({ clientId: 'types-node16', brokers: ['localhost:9092'], logLevel: logLevel.NOTHING, maxResponseSize: 1048576 })
const producer = kafka.producer()
const send: Promise<unknown> = producer.send({ topic: 't', messages: [{ value: 'v' }], compression: CompressionTypes.None })
const err: KafkaJSError = new KafkaJSError('x')
const refused: KafkaJSCommitFailedError = new KafkaJSCommitFailedError('x', { reason: 'stale-assignment', cause: err })
void send
void err
void refused

// The reset policy and the error a none-policy halt carries.
const resetConsumer = kafka.consumer({ groupId: 'g' })
void resetConsumer.subscribe({ topics: ['t'], offsetReset: 'by_duration:PT10M' })
const noOffset: KafkaJSError = new KafkaJSNoOffsetError('x', { topic: 't', partition: 0, reason: 'no-offset' })
void noOffset
resetConsumer.on(resetConsumer.events.PARTITION_HALTED, e => {
  if (e.payload.reason === 'no-offset') {
    const missing: null = e.payload.offset
    void missing
  }
})

// Compression levels are optional and available on both the producer and direct broker API.
const producerWithCompressionLevels = new Kafka({ brokers: ['localhost:9092'] }).producer({
  compressionLevels: { gzip: 9, zstd: 7 },
})
void producerWithCompressionLevels

// The producer's buffer options and the two errors its refusals reject with.
const buffered = kafka.producer({ bufferMemory: 33554432, maxBlockMs: 60000 })
const tooLarge: KafkaJSRecordTooLargeError = new KafkaJSRecordTooLargeError('x', { reason: 'bufferMemory', scope: 'send', size: 2, limit: 1 })
const exhausted: KafkaJSBufferExhaustedError = new KafkaJSBufferExhaustedError('x', { reason: 'timeout', requested: 1, available: 0, capacity: 1, waitedMs: 1 })
void buffered
void tooLarge
void exhausted
const closed: KafkaJSError = new KafkaJSConnectionClosedError('x', { host: 'localhost', port: 9092 })
const memberIdRequired: KafkaJSError = new KafkaJSMemberIdRequired(new Error('x'), { memberId: 'm' })
const invariant: KafkaJSError = new KafkaJSInvariantViolation('x')
const invalidVarInt: KafkaJSError = new KafkaJSInvalidVarIntError('x')
const invalidLong: KafkaJSError = new KafkaJSInvalidLongError('x')
const createTopic: KafkaJSError = new KafkaJSCreateTopicError(new Error('x'), 't')
const noBroker: KafkaJSError = new KafkaJSNoBrokerAvailableError()
void [closed, memberIdRequired, invariant, invalidVarInt, invalidLong, createTopic, noBroker]

const lifecycle = new Kafka({ brokers: ['localhost:9092'] }).producer({ metadataMaxIdle: 300000 })
void lifecycle.flush({ timeout: 2147483647 })
void lifecycle.disconnect({ timeout: 0 })
process.on('SIGTERM', lifecycle.disconnect)

new Kafka({ brokers: ['localhost:9092'] }).producer({ metadataMaxIdle: 5000 })
new Kafka({ brokers: ['localhost:9092'] }).producer({ metadataMaxIdle: 2147483647 })

import type {
  Assigner as UnionAssignerContract,
  PartitionMetadata as UnionPartitionMetadata,
} from '@countly/kafkajs'
const unionNode16: UnionAssignerContract = {
  name: 'union-contract',
  version: 0,
  async assign({ topics, groupTopics, groupTopicMetadata }) {
    const names: string[] = groupTopics ?? topics
    const rows: UnionPartitionMetadata[] = groupTopicMetadata?.get(names[0]) ?? []
    void rows
    return []
  },
  protocol() {
    return { name: 'union-contract', metadata: Buffer.alloc(0) }
  },
}
void unionNode16.assign({ members: [], topics: [] })
void unionNode16.assign({
  members: [],
  topics: [],
  groupTopics: ['orders'],
  groupTopicMetadata: new Map<string, UnionPartitionMetadata[]>(),
})

// Range rack matching is opt-in and the factory sees a boolean.
kafka.consumer({ groupId: 'rack-range', rackAwareAssignment: true })
kafka.consumer({ groupId: 'rack-range', rackAwareAssignment: false })
// @ts-expect-error the rack-aware option is a boolean
kafka.consumer({ groupId: 'rack-range', rackAwareAssignment: 'true' })

{
  const kafka = new Kafka({ brokers: ['unused:9092'] })
  const direct: import('@countly/kafkajs').DirectConsumerConfig = {}
  const grouped: import('@countly/kafkajs').ConsumerConfig = { groupId: 'typed-group' }
  const configs: Parameters<Kafka['consumer']>[0][] = [direct, grouped, { groupId: undefined }]
  for (const config of configs) kafka.consumer(config)
  const consumer = kafka.consumer(direct)
  consumer.assign([{ topic: '__proto__', partitions: [0, 2147483647] }], {
    offsetReset: 'by_duration:PT0.000000001S',
  })
  consumer.assign([{ topic: 'records', partitions: [0] }], {
    offsetReset: 'earliest',
    fromBeginning: true,
  })
  consumer.unsubscribe()
  const assignment: import('@countly/kafkajs').TopicPartitions[] = consumer.assignment()
  const subscription: string[] = consumer.subscription()
  const position: Promise<string> = consumer.position({ topic: 'records', partition: 0 })
  const committed: Promise<import('@countly/kafkajs').ConsumerCommittedOffset[]> = consumer.committed([])
  const group: import('@countly/kafkajs').ConsumerGroupMetadata = consumer.groupMetadata()
  const noParameters: Parameters<typeof consumer.disconnect> = []
  consumer.disconnect()
  consumer.disconnect({ leaveGroup: true })
  process.on('SIGTERM', consumer.disconnect)
  // @ts-expect-error groupId is still required on the grouped configuration
  const invalidGrouped: import('@countly/kafkajs').ConsumerConfig = {}
  // @ts-expect-error runtime and declarations reject null group identity
  kafka.consumer({ groupId: null })
  // @ts-expect-error offsets are returned as decimal strings
  const invalidPosition: Promise<number> = consumer.position({ topic: 'records', partition: 0 })
  // @ts-expect-error reset policy must use a declared string form
  consumer.assign([{ topic: 'records', partitions: [0] }], { offsetReset: 1 })
  const corrupt = new KafkaJSCorruptRecordError('bad', {
    reason: 'decompression',
    offset: '12',
    codec: 'gzip',
    limit: 1024,
    nextOffset: '18',
  })
  const none = new KafkaJSNoOffsetError('missing', {
    topic: 'records',
    partition: 0,
    reason: 'no-offset',
  })
  const inherited: import('@countly/kafkajs').ConsumerPartitionHaltedEventPayload = {
    groupId: 'typed-group',
    memberId: 'member',
    topic: 'records',
    partition: 0,
    reason: 'decompression',
    offset: '12',
    error: corrupt,
    codec: 'gzip',
    limit: 1024,
    nextOffset: '18',
  }
  const missing: import('@countly/kafkajs').ConsumerPartitionHaltedEventPayload = {
    groupId: null,
    memberId: '',
    topic: 'records',
    partition: 0,
    reason: 'no-offset',
    offset: null,
    error: none,
  }
  const outside: import('@countly/kafkajs').ConsumerPartitionHaltedEventPayload = {
    groupId: null,
    memberId: '',
    topic: 'records',
    partition: 0,
    reason: 'offset-out-of-range',
    offset: '99',
    error: none,
  }
  /** @param {import('@countly/kafkajs').ConsumerPartitionHaltedEventPayload} payload actual event payload */
  const narrow = (payload: import('@countly/kafkajs').ConsumerPartitionHaltedEventPayload): void => {
    const id: string | null = payload.groupId
    if (payload.reason === 'no-offset') {
      const offset: null = payload.offset
      const error: KafkaJSNoOffsetError = payload.error
      void [offset, error, id]
    } else if (payload.reason === 'offset-out-of-range') {
      const offset: string | null = payload.offset
      const error: KafkaJSNoOffsetError = payload.error
      void [offset, error]
    } else {
      const offset: string = payload.offset
      const codec: 'gzip' | 'snappy' | 'lz4' | 'zstd' | undefined = payload.codec
      const error: KafkaJSCorruptRecordError = payload.error
      const limit: number | undefined = payload.limit
      const next: string | undefined = payload.nextOffset
      void [offset, codec, error, limit, next]
    }
  }
  narrow(inherited)
  narrow(missing)
  narrow(outside)
  consumer.on(consumer.events.CRASH, (event) => {
    const id: string | null = event.payload.groupId
    void id
  })
  consumer.on(consumer.events.PARTITION_HALTED, (event) => narrow(event.payload))
  void [
    assignment,
    subscription,
    position,
    committed,
    group,
    noParameters,
    invalidGrouped,
    invalidPosition,
  ]
}

import {
  ConsumerRebalanceListener as AwaitedListener,
  KafkaJSRebalanceListenerError as ListenerFailure,
} from '@countly/kafkajs'
{
  const hookConsumer = new Kafka({ brokers: ['localhost:1'] }).consumer({ groupId: 'hooks' })
  const listener: AwaitedListener = {
    onPartitionsAssigned: async (payload) => {
      const member: string | null = payload.memberId
      const generation: number | null = payload.generationId
      void member
      void generation
      void payload.partitions
    },
  }
  void hookConsumer.run({ rebalanceListener: listener })
  void hookConsumer.enforceRebalance('external offsets changed')
  const error = new ListenerFailure('callback', {
    hook: 'onPartitionsAssigned',
    reason: 'threw',
    cause: new Error(),
    thrownValue: Symbol('store'),
  })
  const cause: Error = error.cause
  const original: unknown = error.thrownValue
  void cause
  void original
  // @ts-expect-error callback must be a function
  hookConsumer.run({ rebalanceListener: { onPartitionsLost: 1 } })
  // @ts-expect-error reason must be a string
  hookConsumer.enforceRebalance(5)
}

import {
  ConsumerAssignOptions,
  ConsumerCommittedOffset,
  ConsumerOffsetReset,
  ConsumerPartitionHaltedBase,
  ConsumerRebalancePayload,
} from '@countly/kafkajs'
{
  const resets: ConsumerOffsetReset[] = ['earliest', 'latest', 'none', 'by_duration:PT10M']
  const assignOptions: ConsumerAssignOptions = { fromBeginning: true, offsetReset: resets[0] }
  const payload: ConsumerRebalancePayload = { groupId: 'g', memberId: null, generationId: null, partitions: [] }
  const base: ConsumerPartitionHaltedBase = { groupId: null, memberId: '', topic: 't', partition: 0 }
  const row: ConsumerCommittedOffset = { topic: 't', partition: 0, offset: null, metadata: null }
  const offset: string | null = row.offset
  const failure = new ListenerFailure('callback', { hook: 'onPartitionsRevoked', reason: 'timeout', cause: new Error() })
  const hook: 'onPartitionsRevoked' | 'onPartitionsAssigned' | 'onPartitionsLost' = failure.hook
  const reason: 'threw' | 'timeout' = failure.reason
  void [resets, assignOptions, payload, base, offset, hook, reason]
  // @ts-expect-error a reset policy outside the declared forms
  const invalidReset: ConsumerOffsetReset = 'sometimes'
  void invalidReset
}

/**
 * Checks bounded consumer close without changing the legacy signal signature.
 * @param target consumer whose overloads are checked
 * @returns resolved when the accepted examples have closed
 */
async function useBoundedDisconnect(target: import('@countly/kafkajs').Consumer): Promise<void> {
  await target.disconnect({ timeout: 0 })
  await target.disconnect({ timeout: 2147483647, leaveGroup: false })
  await target.disconnect({ timeout: undefined, leaveGroup: true })
  const signal: (name: 'SIGTERM') => void = target.disconnect
  const parameters: Parameters<typeof target.disconnect> = []
  void signal
  void parameters
  // @ts-expect-error Timeout is a millisecond number.
  await target.disconnect({ timeout: '1000' })
  // @ts-expect-error Leave choice remains boolean.
  await target.disconnect({ timeout: 1000, leaveGroup: 'yes' })
}
void useBoundedDisconnect

{
  kafka.consumer({ groupId: 'sessions', fetchSessions: true })
  kafka.consumer({ groupId: 'sessions', fetchSessions: false })
  kafka.consumer({ fetchSessions: true })
  const config: Parameters<Kafka['consumer']>[0] = { fetchSessions: false }
  kafka.consumer(config)
  // @ts-expect-error fetchSessions accepts only booleans
  kafka.consumer({ groupId: 'sessions', fetchSessions: 'true' })
}

import {
  KafkaJSShareGroupsNotSupportedError,
  KafkaJSServerDoesNotSupportApiKey,
  ShareGroupsNotSupportedReason,
} from '@countly/kafkajs'
{
  const shareUnsupported = new KafkaJSShareGroupsNotSupportedError('ShareFetch requires v2', {
    apiKey: 78, apiName: 'ShareFetch', reason: 'api-version-range', shareVersion: 0,
  })
  const base: KafkaJSServerDoesNotSupportApiKey = shareUnsupported
  const reason: ShareGroupsNotSupportedReason = shareUnsupported.reason
  const level: number | null = shareUnsupported.shareVersion
  void [base, reason, level]
  // @ts-expect-error the refusal stage is required
  new KafkaJSShareGroupsNotSupportedError('x', { apiKey: 78, apiName: 'ShareFetch' })
}

import { KafkaJSShareAcknowledgementError as CompactAckError } from '@countly/kafkajs'
const compactAckCause = new Error('broker refusal')
const compactAckError = new CompactAckError('not confirmed', {
  reason: 'rejected-by-broker', topic: 'topic', partition: 0,
  ranges: [{ firstOffset: '9007199254740993', lastOffset: '9223372036854775807' }],
  cause: compactAckCause,
})
const compactAckReason: 'unacknowledged' | 'session-lost' | 'membership-lost' | 'not-sent' | 'rejected-by-broker' = compactAckError.reason
const compactAckRanges: Array<{ firstOffset: string; lastOffset: string }> = compactAckError.ranges
const compactAckOptionalCause: Error | undefined = compactAckError.cause
void [compactAckReason, compactAckRanges, compactAckOptionalCause]
// @ts-expect-error reason is readonly
compactAckError.reason = 'session-lost'
// @ts-expect-error ranges is readonly
compactAckError.ranges = []
// @ts-expect-error acknowledgement outcomes do not introduce a retry reason
new CompactAckError('bad', { reason: 'retry', topic: 'topic', partition: 0, ranges: [] })
// @ts-expect-error offsets are exact decimal strings
new CompactAckError('bad', { reason: 'not-sent', topic: 'topic', partition: 0, ranges: [{ firstOffset: 1, lastOffset: '2' }] })
// @ts-expect-error constructor never exposes a retry option
new CompactAckError('bad', { reason: 'not-sent', topic: 'topic', partition: 0, ranges: [], retriable: true })

import type { Admin as ShareAdmin, ShareGroupDescription, ShareGroupTopicOffsets, ShareGroupAlterOffsetsResult, ShareGroupDeleteOffsetsResult } from '@countly/kafkajs'

/** Compiles the additive administration surface and exact public result domains. */
async function shareAdminTypes(admin: ShareAdmin) {
  const described: { groups: ShareGroupDescription[] } = await admin.describeShareGroups(['share'], { includeAuthorizedOperations: true, apiTimeout: 1000 })
  const mask: number | null = described.groups[0].authorizedOperations
  const listed: ShareGroupTopicOffsets[] = await admin.listShareGroupOffsets({ groupId: 'share' })
  const lag: string | null = listed[0].partitions[0].lag
  const changed: ShareGroupAlterOffsetsResult[] = await admin.alterShareGroupOffsets({ groupId: 'share', topics: [{ topic: 'alpha', partitions: [{ partition: 0, offset: '9007199254740993' }] }], apiTimeout: 1000 })
  const deleted: ShareGroupDeleteOffsetsResult[] = await admin.deleteShareGroupOffsets({ groupId: 'share', topics: ['alpha'], apiTimeout: 1000 })
  await admin.listShareGroupOffsets({ groupId: 'share', topics: [] })
  await admin.listShareGroupOffsets({ groupId: 'share', topics: [{ topic: 'alpha', partitions: [0] }] })
  // @ts-expect-error Offsets must remain exact decimal strings.
  await admin.alterShareGroupOffsets({ groupId: 'share', topics: [{ topic: 'alpha', partitions: [{ partition: 0, offset: 9007199254740994 }] }] })
  // @ts-expect-error Authorization is a raw signed mask, never an array.
  const operations: string[] = described.groups[0].authorizedOperations
  // @ts-expect-error Unknown lag is null rather than a number.
  const numericLag: number = listed[0].partitions[0].lag
  // @ts-expect-error Delete selects topic names, not partition objects.
  await admin.deleteShareGroupOffsets({ groupId: 'share', topics: [{ topic: 'alpha' }] })
  // @ts-expect-error Share deletion uses the existing generic method.
  await admin.deleteShareGroups(['share'])
  // @ts-expect-error Timeout keeps the existing integer number domain.
  await admin.describeShareGroups(['share'], { apiTimeout: '1000' })
  return { mask, lag, changed, deleted }
}

import * as ShareSurface from '@countly/kafkajs'

namespace ShareConsumerSurface {
  const kafka = new ShareSurface.Kafka({ brokers: [] })
  const share = kafka.shareConsumer({ groupId: 'g' })
  const full = kafka.shareConsumer({ groupId: 'g', acknowledgementMode: 'explicit', acquireMode: 'batch_optimized', maxRecords: 500, minBytes: 0, maxBytes: 10485760, maxWaitTimeInMs: 0, metadataMaxAge: 300000, maxPollInterval: 300000, rackId: '', checkCrcs: true, allowAutoTopicCreation: false, maxInFlightRequests: null, retry: { retries: 5, restartOnFailure: async (error) => error instanceof Error } })
  share.run({ eachMessage: async ({ topic, partition, message, acknowledge, renew, isRunning }) => {
    const count: number = message.deliveryCount; const offset: string = message.offset
    const value: Buffer | null = message.value; const key: Buffer | null = message.key
    acknowledge(); acknowledge('release'); const duration: number = await renew()
    const running: boolean = isRunning(); void [topic, partition, count, offset, value, key, duration, running]
  } })
  full.run({ eachBatch: async ({ batch, acknowledge, renew, isRunning }) => {
    acknowledge(batch.messages[0], 'reject'); acknowledge({ offset: batch.messages[0].offset })
    const duration: number = await renew([batch.messages[0], { offset: batch.messages[0].offset }])
    await renew(); const running: boolean = isRunning(); void [duration, running, batch.acquisitionLockTimeoutMs]
  } })
  share.on(share.events.ACKNOWLEDGE, ({ payload }) => {
    const first: string = payload.firstOffset; const last: string = payload.lastOffset
    const error: ShareSurface.KafkaJSShareAcknowledgementError | null = payload.error; void [first, last, error]
  })
  share.on(share.events.REQUEST, ({ payload }) => { void payload })
  const event: ShareSurface.ShareConsumerEvents[keyof ShareSurface.ShareConsumerEvents] = share.events.FETCH
  share.on(event, (record) => { void record.type })
  process.on('SIGTERM', share.disconnect)
  const parameters: Parameters<ShareSurface.ShareConsumer['disconnect']> = []
  const duration: number | null = share.acquisitionLockTimeoutMs()
  void [parameters, duration, share.subscription(), share.assignment(), share.logger(), share.stop(), share.disconnect({ timeout: 0 })]
  const admin = kafka.admin()
  void [admin.describeShareGroups(['g']), admin.listShareGroupOffsets({ groupId: 'g' }), admin.alterShareGroupOffsets({ groupId: 'g', topics: [{ topic: 't', partitions: [{ partition: 0, offset: '9007199254740993' }] }] }), admin.deleteShareGroupOffsets({ groupId: 'g', topics: ['t'] }), admin.deleteGroups(['g'])]
  const error: Error = new ShareSurface.KafkaJSShareGroupsNotSupportedError('unsupported', { apiKey: 78, apiName: 'shareFetch', reason: 'api-version-range' })
  if (error instanceof ShareSurface.KafkaJSShareGroupsNotSupportedError) {
    const base: ShareSurface.KafkaJSServerDoesNotSupportApiKey = error; void [base, error.reason]
  }
  // @ts-expect-error Group isolation is configured on the broker.
  kafka.shareConsumer({ groupId: 'g', readUncommitted: true })
  // @ts-expect-error Offset reset is configured on the broker.
  kafka.shareConsumer({ groupId: 'g', fromBeginning: true })
  // @ts-expect-error Share groups have no static membership.
  kafka.shareConsumer({ groupId: 'g', groupInstanceId: 'member' })
  // @ts-expect-error Only implicit and explicit modes exist.
  kafka.shareConsumer({ groupId: 'g', acknowledgementMode: 'manual' })
  // @ts-expect-error Only the two documented acquisition modes exist.
  kafka.shareConsumer({ groupId: 'g', acquireMode: 'strict' })
  // @ts-expect-error A group id is required.
  kafka.shareConsumer({})
  // @ts-expect-error Topics are strings.
  share.subscribe({ topics: [/topic/] })
  // @ts-expect-error Regex subscription is unavailable.
  share.subscribe({ topics: ['t'], regex: /topic/ })
  // @ts-expect-error Offset reset is a group configuration.
  share.subscribe({ topics: ['t'], offsetReset: 'earliest' })
  // @ts-expect-error Renew is a separate operation.
  const terminalType: ShareSurface.ShareAcknowledgeType = 'renew'
  // @ts-expect-error Numeric disconnect arguments are accepted only at runtime for signals.
  share.disconnect(5)
  // @ts-expect-error A handler receives the share payload.
  share.run({ eachMessage: async (payload: string) => { void payload } })
  // @ts-expect-error Only share event names are accepted.
  share.on('share_consumer.missing', () => {})
  // @ts-expect-error Lock duration is a number or null.
  const stringDuration: string = share.acquisitionLockTimeoutMs()
  void [terminalType, stringDuration]
}
