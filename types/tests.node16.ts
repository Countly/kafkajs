// Compiled with moduleResolution node16 against the packed package — proves the "types" export condition resolves.
import {
  Kafka,
  logLevel,
  CompressionTypes,
  KafkaJSError,
  KafkaJSCommitFailedError,
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
const closed: KafkaJSError = new KafkaJSConnectionClosedError('x', { host: 'localhost', port: 9092 })
const memberIdRequired: KafkaJSError = new KafkaJSMemberIdRequired(new Error('x'), { memberId: 'm' })
const invariant: KafkaJSError = new KafkaJSInvariantViolation('x')
const invalidVarInt: KafkaJSError = new KafkaJSInvalidVarIntError('x')
const invalidLong: KafkaJSError = new KafkaJSInvalidLongError('x')
const createTopic: KafkaJSError = new KafkaJSCreateTopicError(new Error('x'), 't')
const noBroker: KafkaJSError = new KafkaJSNoBrokerAvailableError()
void [closed, memberIdRequired, invariant, invalidVarInt, invalidLong, createTopic, noBroker]
