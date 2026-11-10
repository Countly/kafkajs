import * as named from '@countly/kafkajs'
import * as alias from 'kafkajs'

const kafka = new named.Kafka({ brokers: [], enableMetricsPush: true })
const producer = kafka.producer({ telemetryMaxBytes: 1 })
const id: Promise<string> = producer.clientInstanceId()
const sameType: typeof named.KafkaJSTelemetryError = alias.KafkaJSTelemetryError
const error: named.KafkaJSTelemetryError = new sameType('x', { reason: 'limit' })
const share = kafka.shareConsumer({ groupId: 'package-exports', enableMetricsPush: true })
const shareId: Promise<string> = share.clientInstanceId({ timeout: 0 })
share.registerMetricForSubscription({ name: 'com.example.share', type: 'gauge', value: 1 })
share.unregisterMetricFromSubscription({ name: 'com.example.share' })
process.on('SIGTERM', share.disconnect)
// @ts-expect-error private metrics have no exported package path
import { privateMetric } from '@countly/kafkajs/src/telemetry/metrics'
void [id, error, shareId, privateMetric]
