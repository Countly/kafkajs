/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

const assert = require('node:assert')
const { SpanKind, SpanStatusCode } = require('@opentelemetry/api')
const { DataPointType } = require('@opentelemetry/sdk-metrics')
const { ATTR_ERROR_TYPE } = require('@opentelemetry/semantic-conventions')
const {
  METRIC_MESSAGING_CLIENT_SENT_MESSAGES,
  ATTR_MESSAGING_SYSTEM,
  ATTR_MESSAGING_DESTINATION_NAME,
  ATTR_MESSAGING_OPERATION_NAME,
  MESSAGING_SYSTEM_VALUE_KAFKA,
  ATTR_MESSAGING_DESTINATION_PARTITION_ID,
} = require('../src/semconv')

/**
 * Asserts that a collection holds exactly the expected metrics, data point by data point.
 * @param {import('@opentelemetry/sdk-metrics').CollectionResult} collection
 * @param {Record<string, Array<{ count?: number, value?: number,
 *   buckets?: Record<number, number>, attributes: object }>>} expected by metric name
 * @returns {void}
 */
function assertMetricCollection({ errors, resourceMetrics }, expected) {
  assert.strictEqual(errors.length, 0)
  const { metrics } = resourceMetrics.scopeMetrics[0]
  assert.strictEqual(
    Object.keys(expected).length,
    metrics.length,
    'A different number of metrics were found than expected'
  )
  Object.entries(expected).forEach(([name, values]) => {
    const match = metrics.find((metric) => metric.descriptor.name === name)
    assert.ok(match, `metric ${name} not found`)

    if (match.dataPointType === DataPointType.HISTOGRAM) {
      assert.deepStrictEqual(
        match.dataPoints.map((d) => d.value.count),
        values.map((v) => v.count),
        `${name} datapoints do not have the same count`
      )
      values.forEach(({ buckets }, i) => {
        if (buckets) {
          const { boundaries, counts } = match.dataPoints[i].value.buckets
          const actualBuckets = counts.reduce((acc, n, j) => {
            if (n > 0) {
              acc[boundaries[j]] = n
            }
            return acc
          }, {})
          assert.deepStrictEqual(actualBuckets, buckets)
        }
      })
    } else {
      assert.deepStrictEqual(
        match.dataPoints.map((d) => d.value),
        values.map((v) => v.value),
        `${name} datapoint values do not match`
      )
    }
    assert.deepStrictEqual(
      match.dataPoints.map((d) => d.attributes),
      values.map((v) => v.attributes)
    )
  })
}

/**
 * Asserts that every span failed with the send error and that the sent-messages counter carries
 * the error type.
 * @param {object} input
 * @param {import('@opentelemetry/sdk-trace').ReadableSpan[]} input.spans
 * @param {{ collect(): Promise<import('@opentelemetry/sdk-metrics').CollectionResult> }} input.metricReader
 * @param {string} [input.errorMessage]
 * @param {Record<string, number>} input.expectedTopicCounts messages per topic
 * @returns {Promise<void>}
 */
async function assertFailedSendSpans({
  spans,
  errorMessage = 'error thrown from kafka client send',
  metricReader,
  expectedTopicCounts,
}) {
  spans.forEach((span, i) => {
    assert.strictEqual(
      span.status.code,
      SpanStatusCode.ERROR,
      `Expected span #${i} status.code to be ERROR`
    )

    assert.strictEqual(
      span.status.message,
      errorMessage,
      `Expected span #${i} status.message to match Kafka send error`
    )
  })

  const expectedMetric = Object.entries(expectedTopicCounts).map(([topic, value]) => ({
    value,
    attributes: {
      [ATTR_MESSAGING_SYSTEM]: MESSAGING_SYSTEM_VALUE_KAFKA,
      [ATTR_MESSAGING_DESTINATION_NAME]: topic,
      [ATTR_MESSAGING_OPERATION_NAME]: 'send',
      [ATTR_ERROR_TYPE]: 'Error',
    },
  }))

  const result = await metricReader.collect()

  assertMetricCollection(result, {
    [METRIC_MESSAGING_CLIENT_SENT_MESSAGES]: expectedMetric,
  })
}

/**
 * Asserts that every span is a successful PRODUCER `send` span with the expected attributes, and
 * that the sent-messages counter matches.
 * @param {object} input
 * @param {import('@opentelemetry/sdk-trace').ReadableSpan[]} input.spans
 * @param {{ collect(): Promise<import('@opentelemetry/sdk-metrics').CollectionResult> }} input.metricReader
 * @param {Array<{ topic: string, value: number, partitionId?: string }>} input.expectedMetrics
 * @param {Record<number, object>} [input.perSpan] attributes expected on the span of that index
 * @returns {Promise<void>}
 */
async function assertSuccessfulSendSpans({ spans, metricReader, expectedMetrics, perSpan = {} }) {
  spans.forEach((span, i) => {
    assert.strictEqual(span.kind, SpanKind.PRODUCER, `Span #${i} should be PRODUCER`)
    assert.strictEqual(
      span.status.code,
      SpanStatusCode.UNSET,
      `Span #${i} status.code should be UNSET`
    )
    assert.ok(span.name.startsWith('send '), `Span #${i} name should start with "send "`)
    assert.strictEqual(
      span.attributes[ATTR_MESSAGING_SYSTEM],
      'kafka',
      `Span #${i} ATTR_MESSAGING_SYSTEM mismatch`
    )

    const extra = perSpan[i]
    if (extra) {
      Object.entries(extra).forEach(([key, value]) => {
        assert.strictEqual(span.attributes[key], value, `Span #${i} attribute ${key} mismatch`)
      })
    }
  })

  const expectedMetricArray = expectedMetrics.map(({ topic, value, partitionId }) => ({
    value,
    attributes: {
      [ATTR_MESSAGING_SYSTEM]: MESSAGING_SYSTEM_VALUE_KAFKA,
      [ATTR_MESSAGING_DESTINATION_NAME]: topic,
      [ATTR_MESSAGING_OPERATION_NAME]: 'send',
      ...(partitionId !== undefined && {
        [ATTR_MESSAGING_DESTINATION_PARTITION_ID]: partitionId,
      }),
    },
  }))

  const collected = await metricReader.collect()
  assertMetricCollection(collected, {
    [METRIC_MESSAGING_CLIENT_SENT_MESSAGES]: expectedMetricArray,
  })
}

/**
 * @param {import('@opentelemetry/sdk-trace').ReadableSpan[]} spans
 * @returns {boolean} whether there is a span and every span shares the first one's trace id
 */
function haveSameTraceId(spans) {
  if (spans.length === 0) return false
  const referenceId = spans[0].spanContext().traceId
  return spans.every((span) => span.spanContext().traceId === referenceId)
}

module.exports = {
  assertMetricCollection,
  assertFailedSendSpans,
  assertSuccessfulSendSpans,
  haveSameTraceId,
}
