// Compiled with moduleResolution node16 against the packed package — proves the "types" export condition resolves.
import {
  Kafka,
  logLevel,
  CompressionTypes,
  KafkaJSError,
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
} from '@countly/kafkajs'

const kafka = new Kafka({ clientId: 'types-node16', brokers: ['localhost:9092'], logLevel: logLevel.NOTHING, maxResponseSize: 1048576 })
const producer = kafka.producer()
const send: Promise<unknown> = producer.send({ topic: 't', messages: [{ value: 'v' }], compression: CompressionTypes.None })
const err: KafkaJSError = new KafkaJSError('x')
const refused: KafkaJSCommitFailedError = new KafkaJSCommitFailedError('x', { reason: 'stale-assignment', cause: err })
void send
void err
void refused

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
