// Compiled with moduleResolution node16 against the packed package — proves the "types" export condition resolves.
import { Kafka, logLevel, CompressionTypes, KafkaJSError } from '@countly/kafkajs'

const kafka = new Kafka({ clientId: 'types-node16', brokers: ['localhost:9092'], logLevel: logLevel.NOTHING })
const producer = kafka.producer()
const send: Promise<unknown> = producer.send({ topic: 't', messages: [{ value: 'v' }], compression: CompressionTypes.None })
const err: KafkaJSError = new KafkaJSError('x')
void send
void err
