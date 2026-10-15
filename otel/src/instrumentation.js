/*
 * Copyright The OpenTelemetry Authors, Aspecto
 * SPDX-License-Identifier: Apache-2.0
 */

const {
  context,
  propagation,
  ROOT_CONTEXT,
  SpanKind,
  SpanStatusCode,
  trace,
} = require('@opentelemetry/api')
const {
  InstrumentationBase,
  InstrumentationNodeModuleDefinition,
  isWrapped,
  safeExecuteInTheMiddle,
} = require('@opentelemetry/instrumentation')
const {
  ATTR_ERROR_TYPE,
  ATTR_SERVER_ADDRESS,
  ATTR_SERVER_PORT,
  ERROR_TYPE_VALUE_OTHER,
} = require('@opentelemetry/semantic-conventions')
const { bufferTextMapGetter } = require('./propagator')
const {
  ATTR_MESSAGING_BATCH_MESSAGE_COUNT,
  ATTR_MESSAGING_DESTINATION_NAME,
  ATTR_MESSAGING_DESTINATION_PARTITION_ID,
  ATTR_MESSAGING_KAFKA_MESSAGE_KEY,
  ATTR_MESSAGING_KAFKA_MESSAGE_TOMBSTONE,
  ATTR_MESSAGING_KAFKA_OFFSET,
  ATTR_MESSAGING_OPERATION_NAME,
  ATTR_MESSAGING_OPERATION_TYPE,
  ATTR_MESSAGING_SYSTEM,
  MESSAGING_OPERATION_TYPE_VALUE_PROCESS,
  MESSAGING_OPERATION_TYPE_VALUE_RECEIVE,
  MESSAGING_OPERATION_TYPE_VALUE_SEND,
  MESSAGING_SYSTEM_VALUE_KAFKA,
  METRIC_MESSAGING_CLIENT_CONSUMED_MESSAGES,
  METRIC_MESSAGING_CLIENT_OPERATION_DURATION,
  METRIC_MESSAGING_CLIENT_SENT_MESSAGES,
  METRIC_MESSAGING_PROCESS_DURATION,
} = require('./semconv')
const { name: PACKAGE_NAME, version: PACKAGE_VERSION } = require('../package.json')

/** Marks a producer or consumer whose `REQUEST` listener is already set. */
const EVENT_LISTENERS_SET = Symbol('opentelemetry.instrumentation.kafkajs.eventListenersSet')

/**
 * The names the client is installed under, and the versions it accepts. The hook knows a package
 * only by its `node_modules/<name>` directory, so an application that installs this client as
 * `kafkajs` (the npm alias `"kafkajs": "npm:@countly/kafkajs@3"`) is matched by the second name;
 * the range keeps `kafkajs` 2.x, which `@opentelemetry/instrumentation-kafkajs` instruments, out.
 */
const MODULE_NAMES = ['@countly/kafkajs', 'kafkajs']
const SUPPORTED_VERSIONS = ['>=3.0.0-0 <4']

const HISTOGRAM_BUCKET_BOUNDARIES = [
  0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1, 2.5, 5, 7.5, 10,
]

/**
 * @typedef {(errorType?: string) => void} RecordPendingMetric records a metric once its operation
 *   has settled, with the error type when it failed
 */

/**
 * @param {import('@opentelemetry/api').Counter} meter
 * @param {number} value
 * @param {import('@opentelemetry/api').Attributes} attributes
 * @returns {RecordPendingMetric}
 */
function prepareCounter(meter, value, attributes) {
  return (errorType) => {
    meter.add(value, {
      ...attributes,
      ...(errorType ? { [ATTR_ERROR_TYPE]: errorType } : {}),
    })
  }
}

/**
 * @param {import('@opentelemetry/api').Histogram} meter
 * @param {number} value the start time, from `Date.now()`
 * @param {import('@opentelemetry/api').Attributes} attributes
 * @returns {RecordPendingMetric} records the seconds elapsed since `value`
 */
function prepareDurationHistogram(meter, value, attributes) {
  return (errorType) => {
    meter.record((Date.now() - value) / 1000, {
      ...attributes,
      ...(errorType ? { [ATTR_ERROR_TYPE]: errorType } : {}),
    })
  }
}

/**
 * Calls `call` and turns a synchronous throw into a rejected promise, so a caller that ends its
 * spans on the outcome treats a handler that throws like one that rejects.
 * @template T
 * @param {() => T} call
 * @returns {T | Promise<never>} what `call` returns, or a promise rejected with the original error
 */
function rejectOnThrow(call) {
  try {
    return call()
  } catch (error) {
    return Promise.reject(error)
  }
}

/**
 * OpenTelemetry instrumentation for `@countly/kafkajs`: a span per produced message, a span per
 * consumed message (and one per batch), a span per transaction, context propagation through the
 * message headers, and the messaging metrics.
 *
 * Only the client's public surface is patched: `Kafka.prototype.producer` and `consumer`, each
 * producer's `send`, `sendBatch` and `transaction` (and the transaction's `send`, `sendBatch`,
 * `commit` and `abort`), each consumer's `run` with its `eachMessage` and `eachBatch`, and one
 * `REQUEST` listener per client. A CommonJS `require` is hooked by `require-in-the-middle`; an ESM
 * `import` only when Node runs with `--experimental-loader=@opentelemetry/instrumentation/hook.mjs`.
 *
 * `disable()` restores `Kafka#producer` and `Kafka#consumer`, so clients created afterwards are not
 * instrumented. A producer or consumer created while the instrumentation was enabled keeps its
 * wrapped methods and its `REQUEST` listener and stays instrumented, as with the upstream package.
 *
 * Span names, kinds, attributes, metric names and buckets are those of
 * `@opentelemetry/instrumentation-kafkajs` 0.32.0, without `messaging.kafka.cluster.id`, which
 * only the client's internals know.
 */
class KafkaJsInstrumentation extends InstrumentationBase {
  /**
   * @param {import('./index').KafkaJsInstrumentationConfig} [config]
   */
  constructor(config = {}) {
    super(PACKAGE_NAME, PACKAGE_VERSION, config)
  }

  /**
   * Creates the four metric instruments; the base class calls it again whenever the meter
   * provider changes.
   * @returns {void}
   */
  _updateMetricInstruments() {
    this._clientDuration = this.meter.createHistogram(METRIC_MESSAGING_CLIENT_OPERATION_DURATION, {
      advice: { explicitBucketBoundaries: HISTOGRAM_BUCKET_BOUNDARIES },
    })
    this._sentMessages = this.meter.createCounter(METRIC_MESSAGING_CLIENT_SENT_MESSAGES)
    this._consumedMessages = this.meter.createCounter(METRIC_MESSAGING_CLIENT_CONSUMED_MESSAGES)
    this._processDuration = this.meter.createHistogram(METRIC_MESSAGING_PROCESS_DURATION, {
      advice: { explicitBucketBoundaries: HISTOGRAM_BUCKET_BOUNDARIES },
    })
  }

  /**
   * The module definitions: the client's 3.x versions, prereleases included, under either name,
   * with no file hooks. Patching first unpatches, so enabling twice wraps once; unpatching restores
   * the two factories only, not the clients they already created.
   * @protected
   * @returns {InstrumentationNodeModuleDefinition[]}
   */
  init() {
    const unpatch = (moduleExports) => {
      if (isWrapped(moduleExports?.Kafka?.prototype.producer)) {
        this._unwrap(moduleExports.Kafka.prototype, 'producer')
      }
      if (isWrapped(moduleExports?.Kafka?.prototype.consumer)) {
        this._unwrap(moduleExports.Kafka.prototype, 'consumer')
      }
    }

    const patch = (moduleExports) => {
      unpatch(moduleExports)
      this._wrap(moduleExports?.Kafka?.prototype, 'producer', this._getProducerPatch())
      this._wrap(moduleExports?.Kafka?.prototype, 'consumer', this._getConsumerPatch())

      return moduleExports
    }

    return MODULE_NAMES.map(
      (name) => new InstrumentationNodeModuleDefinition(name, SUPPORTED_VERSIONS, patch, unpatch)
    )
  }

  /**
   * @private
   * @returns {(original: Function) => Function} wraps `Kafka#consumer`: every consumer it creates
   *   gets a wrapped `run` and the `REQUEST` listener
   */
  _getConsumerPatch() {
    const instrumentation = this
    return (original) => {
      return function consumer(...args) {
        const newConsumer = original.apply(this, args)

        if (isWrapped(newConsumer.run)) {
          instrumentation._unwrap(newConsumer, 'run')
        }

        instrumentation._wrap(newConsumer, 'run', instrumentation._getConsumerRunPatch())

        instrumentation._setKafkaEventListeners(newConsumer)

        return newConsumer
      }
    }
  }

  /**
   * Adds one `REQUEST` listener per client, which records the client operation duration.
   * @private
   * @param {{ on: Function, events: { REQUEST: string } }} kafkaObj a producer or a consumer
   * @returns {void}
   */
  _setKafkaEventListeners(kafkaObj) {
    if (kafkaObj[EVENT_LISTENERS_SET]) return

    if (kafkaObj.events?.REQUEST) {
      kafkaObj.on(kafkaObj.events.REQUEST, this._recordClientDurationMetric.bind(this))
    }

    kafkaObj[EVENT_LISTENERS_SET] = true
  }

  /**
   * @private
   * @param {{ payload: { broker: string, duration: number, apiName: string } }} event a `REQUEST`
   *   instrumentation event; its `broker` is `host:port`, split at the last colon so that an IPv6
   *   host keeps its own
   * @returns {void}
   */
  _recordClientDurationMetric(event) {
    const { broker } = event.payload
    const portStart = broker.lastIndexOf(':') + 1
    this._clientDuration.record(event.payload.duration / 1000, {
      [ATTR_MESSAGING_SYSTEM]: MESSAGING_SYSTEM_VALUE_KAFKA,
      [ATTR_MESSAGING_OPERATION_NAME]: `${event.payload.apiName}`,
      [ATTR_SERVER_ADDRESS]: broker.slice(0, portStart - 1),
      [ATTR_SERVER_PORT]: Number.parseInt(broker.slice(portStart), 10),
    })
  }

  /**
   * @private
   * @returns {(original: Function) => Function} wraps `Kafka#producer`: every producer it creates
   *   gets wrapped `send`, `sendBatch` and `transaction` and the `REQUEST` listener
   */
  _getProducerPatch() {
    const instrumentation = this
    return (original) => {
      return function producer(...args) {
        const newProducer = original.apply(this, args)

        if (isWrapped(newProducer.sendBatch)) {
          instrumentation._unwrap(newProducer, 'sendBatch')
        }
        instrumentation._wrap(newProducer, 'sendBatch', instrumentation._getSendBatchPatch())

        if (isWrapped(newProducer.send)) {
          instrumentation._unwrap(newProducer, 'send')
        }
        instrumentation._wrap(newProducer, 'send', instrumentation._getSendPatch())

        if (isWrapped(newProducer.transaction)) {
          instrumentation._unwrap(newProducer, 'transaction')
        }
        instrumentation._wrap(
          newProducer,
          'transaction',
          instrumentation._getProducerTransactionPatch()
        )

        instrumentation._setKafkaEventListeners(newProducer)

        return newProducer
      }
    }
  }

  /**
   * @private
   * @returns {(original: Function) => Function} wraps `consumer.run`: the `eachMessage` and
   *   `eachBatch` of the config it is given are wrapped in place
   */
  _getConsumerRunPatch() {
    const instrumentation = this
    return (original) => {
      return function run(...args) {
        const config = args[0]
        if (config?.eachMessage) {
          if (isWrapped(config.eachMessage)) {
            instrumentation._unwrap(config, 'eachMessage')
          }
          instrumentation._wrap(
            config,
            'eachMessage',
            instrumentation._getConsumerEachMessagePatch()
          )
        }
        if (config?.eachBatch) {
          if (isWrapped(config.eachBatch)) {
            instrumentation._unwrap(config, 'eachBatch')
          }
          instrumentation._wrap(config, 'eachBatch', instrumentation._getConsumerEachBatchPatch())
        }
        return original.call(this, config)
      }
    }
  }

  /**
   * @private
   * @returns {(original: Function) => Function} wraps `eachMessage`: a `process <topic>` span in
   *   the context the message headers carry, active while the handler runs. A handler that throws
   *   synchronously ends the span and records its failure like a rejection, and the wrapper
   *   returns the rejected promise
   */
  _getConsumerEachMessagePatch() {
    const instrumentation = this
    return (original) => {
      return function eachMessage(...args) {
        const payload = args[0]
        const propagatedContext = propagation.extract(
          ROOT_CONTEXT,
          payload.message.headers,
          bufferTextMapGetter
        )
        const span = instrumentation._startConsumerSpan({
          topic: payload.topic,
          message: payload.message,
          operationType: MESSAGING_OPERATION_TYPE_VALUE_PROCESS,
          ctx: propagatedContext,
          attributes: {
            [ATTR_MESSAGING_DESTINATION_PARTITION_ID]: String(payload.partition),
          },
        })

        const pendingMetrics = [
          prepareDurationHistogram(instrumentation._processDuration, Date.now(), {
            [ATTR_MESSAGING_SYSTEM]: MESSAGING_SYSTEM_VALUE_KAFKA,
            [ATTR_MESSAGING_OPERATION_NAME]: 'process',
            [ATTR_MESSAGING_DESTINATION_NAME]: payload.topic,
            [ATTR_MESSAGING_DESTINATION_PARTITION_ID]: String(payload.partition),
          }),
          prepareCounter(instrumentation._consumedMessages, 1, {
            [ATTR_MESSAGING_SYSTEM]: MESSAGING_SYSTEM_VALUE_KAFKA,
            [ATTR_MESSAGING_OPERATION_NAME]: 'process',
            [ATTR_MESSAGING_DESTINATION_NAME]: payload.topic,
            [ATTR_MESSAGING_DESTINATION_PARTITION_ID]: String(payload.partition),
          }),
        ]

        const eachMessagePromise = rejectOnThrow(() =>
          context.with(trace.setSpan(propagatedContext, span), () => original.apply(this, args))
        )
        return instrumentation._endSpansOnPromise([span], pendingMetrics, eachMessagePromise)
      }
    }
  }

  /**
   * @private
   * @returns {(original: Function) => Function} wraps `eachBatch`: a `poll <topic>` span that
   *   starts a new trace, with one `process <topic>` span per message under it, each linked to the
   *   producer span its headers carry. A handler that throws synchronously ends every span and
   *   records its failure like a rejection, and the wrapper returns the rejected promise
   */
  _getConsumerEachBatchPatch() {
    return (original) => {
      const instrumentation = this
      return function eachBatch(...args) {
        const payload = args[0]
        // https://github.com/open-telemetry/opentelemetry-specification/blob/master/specification/trace/semantic_conventions/messaging.md#topic-with-multiple-consumers
        const receivingSpan = instrumentation._startConsumerSpan({
          topic: payload.batch.topic,
          message: undefined,
          operationType: MESSAGING_OPERATION_TYPE_VALUE_RECEIVE,
          ctx: ROOT_CONTEXT,
          attributes: {
            [ATTR_MESSAGING_BATCH_MESSAGE_COUNT]: payload.batch.messages.length,
            [ATTR_MESSAGING_DESTINATION_PARTITION_ID]: String(payload.batch.partition),
          },
        })
        return context.with(trace.setSpan(context.active(), receivingSpan), () => {
          const startTime = Date.now()
          const spans = [receivingSpan]
          const pendingMetrics = [
            prepareCounter(instrumentation._consumedMessages, payload.batch.messages.length, {
              [ATTR_MESSAGING_SYSTEM]: MESSAGING_SYSTEM_VALUE_KAFKA,
              [ATTR_MESSAGING_OPERATION_NAME]: 'process',
              [ATTR_MESSAGING_DESTINATION_NAME]: payload.batch.topic,
              [ATTR_MESSAGING_DESTINATION_PARTITION_ID]: String(payload.batch.partition),
            }),
          ]
          payload.batch.messages.forEach((message) => {
            const propagatedContext = propagation.extract(
              ROOT_CONTEXT,
              message.headers,
              bufferTextMapGetter
            )
            const spanContext = trace.getSpan(propagatedContext)?.spanContext()
            let origSpanLink
            if (spanContext) {
              origSpanLink = {
                context: spanContext,
              }
            }
            spans.push(
              instrumentation._startConsumerSpan({
                topic: payload.batch.topic,
                message,
                operationType: MESSAGING_OPERATION_TYPE_VALUE_PROCESS,
                link: origSpanLink,
                attributes: {
                  [ATTR_MESSAGING_DESTINATION_PARTITION_ID]: String(payload.batch.partition),
                },
              })
            )
            pendingMetrics.push(
              prepareDurationHistogram(instrumentation._processDuration, startTime, {
                [ATTR_MESSAGING_SYSTEM]: MESSAGING_SYSTEM_VALUE_KAFKA,
                [ATTR_MESSAGING_OPERATION_NAME]: 'process',
                [ATTR_MESSAGING_DESTINATION_NAME]: payload.batch.topic,
                [ATTR_MESSAGING_DESTINATION_PARTITION_ID]: String(payload.batch.partition),
              })
            )
          })
          const batchMessagePromise = rejectOnThrow(() => original.apply(this, args))
          return instrumentation._endSpansOnPromise(spans, pendingMetrics, batchMessagePromise)
        })
      }
    }
  }

  /**
   * Wraps `producer.transaction`: a `transaction` span, the parent of the spans of the
   * transaction's `send` and `sendBatch`, ended by `commit` (status OK) or `abort`, or with an
   * error when one of them, a send in it or the transaction's start fails. The transaction's
   * methods are replaced on the object it resolves; `sendOffsets` is left as it is.
   *
   * The span ends once, at the first `commit()` or `abort()` that settles. A `commit()` that
   * rejects with a retriable error, because its outcome is not known yet, ends it with that error;
   * a later `commit()` of the transaction, or the one `producer.disconnect()` sends, finds the span
   * ended and is not recorded on it. Each `commit()` returns its own promise, even when the client
   * hands two joined calls the same one.
   * @private
   * @returns {(original: Function) => Function}
   */
  _getProducerTransactionPatch() {
    const instrumentation = this
    return (original) => {
      return function transaction(...args) {
        const transactionSpan = instrumentation.tracer.startSpan('transaction')

        const transactionPromise = original.apply(this, args)

        transactionPromise
          .then((transaction) => {
            const originalSend = transaction.send
            transaction.send = function send(...args) {
              return context.with(trace.setSpan(context.active(), transactionSpan), () => {
                const patched = instrumentation._getSendPatch()(originalSend)
                return patched.apply(this, args).catch((err) => {
                  transactionSpan.setStatus({
                    code: SpanStatusCode.ERROR,
                    message: err?.message,
                  })
                  transactionSpan.recordException(err)
                  throw err
                })
              })
            }

            const originalSendBatch = transaction.sendBatch
            transaction.sendBatch = function sendBatch(...args) {
              return context.with(trace.setSpan(context.active(), transactionSpan), () => {
                const patched = instrumentation._getSendBatchPatch()(originalSendBatch)
                return patched.apply(this, args).catch((err) => {
                  transactionSpan.setStatus({
                    code: SpanStatusCode.ERROR,
                    message: err?.message,
                  })
                  transactionSpan.recordException(err)
                  throw err
                })
              })
            }

            const originalCommit = transaction.commit
            transaction.commit = function commit(...args) {
              const originCommitPromise = originalCommit.apply(this, args).then(() => {
                transactionSpan.setStatus({ code: SpanStatusCode.OK })
              })
              return instrumentation._endSpansOnPromise([transactionSpan], [], originCommitPromise)
            }

            const originalAbort = transaction.abort
            transaction.abort = function abort(...args) {
              const originAbortPromise = originalAbort.apply(this, args)
              return instrumentation._endSpansOnPromise([transactionSpan], [], originAbortPromise)
            }
          })
          .catch((err) => {
            transactionSpan.setStatus({
              code: SpanStatusCode.ERROR,
              message: err?.message,
            })
            transactionSpan.recordException(err)
            transactionSpan.end()
          })

        return transactionPromise
      }
    }
  }

  /**
   * @private
   * @returns {(original: Function) => Function} wraps a producer's or a transaction's
   *   `sendBatch`: one `send <topic>` span per message, its context added to the message's headers
   *   (the caller's message object is changed, as upstream does, unless it or its headers cannot be
   *   changed). The instrumentation never decides whether the produce runs: `original` is always
   *   called, and with no `topicMessages` array it is called with nothing else done
   */
  _getSendBatchPatch() {
    const instrumentation = this
    return (original) => {
      return function sendBatch(...args) {
        const topicMessages = args[0]?.topicMessages
        if (!Array.isArray(topicMessages)) return original.apply(this, args)

        const { spans, pendingMetrics } = instrumentation._startProducerSpans(topicMessages)
        const origSendResult = original.apply(this, args)
        return instrumentation._endSpansOnPromise(spans, pendingMetrics, origSendResult)
      }
    }
  }

  /**
   * @private
   * @returns {(original: Function) => Function} wraps a producer's or a transaction's `send`, as
   *   `_getSendBatchPatch` does for one topic; with no `messages` array `original` is called with
   *   nothing else done
   */
  _getSendPatch() {
    const instrumentation = this
    return (original) => {
      return function send(...args) {
        const record = args[0]
        if (!Array.isArray(record?.messages)) return original.apply(this, args)

        const { spans, pendingMetrics } = instrumentation._startProducerSpans([record])
        const origSendResult = original.apply(this, args)
        return instrumentation._endSpansOnPromise(spans, pendingMetrics, origSendResult)
      }
    }
  }

  /**
   * Starts a `send <topic>` span and prepares the sent-messages metric for every message. When any
   * of them fails, the spans already started are ended and none are returned, so the produce goes
   * on untraced.
   * @private
   * @param {Array<{ topic: string, messages: object[] }>} topicMessages
   * @returns {{ spans: import('@opentelemetry/api').Span[], pendingMetrics: RecordPendingMetric[] }}
   */
  _startProducerSpans(topicMessages) {
    const spans = []
    const pendingMetrics = []
    try {
      topicMessages.forEach(({ topic, messages }) => {
        messages.forEach((message) => {
          spans.push(this._startProducerSpan(topic, message))
          pendingMetrics.push(
            prepareCounter(this._sentMessages, 1, {
              [ATTR_MESSAGING_SYSTEM]: MESSAGING_SYSTEM_VALUE_KAFKA,
              [ATTR_MESSAGING_OPERATION_NAME]: 'send',
              [ATTR_MESSAGING_DESTINATION_NAME]: topic,
              ...(message.partition !== undefined
                ? { [ATTR_MESSAGING_DESTINATION_PARTITION_ID]: String(message.partition) }
                : {}),
            })
          )
        })
      })
    } catch (error) {
      this._diag.error('producer span error', error)
      this._recordAndEnd(spans, [])
      return { spans: [], pendingMetrics: [] }
    }
    return { spans, pendingMetrics }
  }

  /**
   * Ends the spans and records the metrics once the operation settles. A rejection sets every
   * span's status to ERROR with the reason's message and `error.type` to the reason's class name
   * (`_OTHER` for a reason that is `null`, has no `message` or has no constructor name). The
   * returned promise settles with the operation's own result or reason: a meter, a span or a span
   * processor that throws is logged, never passed on.
   * @private
   * @template T
   * @param {import('@opentelemetry/api').Span[]} spans
   * @param {RecordPendingMetric[]} pendingMetrics
   * @param {Promise<T> | T} sendPromise
   * @returns {Promise<T>}
   */
  _endSpansOnPromise(spans, pendingMetrics, sendPromise) {
    return Promise.resolve(sendPromise).then(
      (result) => {
        this._recordAndEnd(spans, pendingMetrics)
        return result
      },
      (reason) => {
        let errorMessage
        let errorType = ERROR_TYPE_VALUE_OTHER
        if (typeof reason === 'string' || reason === undefined) {
          errorMessage = reason
        } else if (
          typeof reason === 'object' &&
          reason !== null &&
          Object.prototype.hasOwnProperty.call(reason, 'message')
        ) {
          errorMessage = reason.message
          errorType = reason.constructor?.name || errorType
        }
        this._recordAndEnd(spans, pendingMetrics, { message: errorMessage, errorType })
        throw reason
      }
    )
  }

  /**
   * Records the metrics, then marks and ends every span. Each step is guarded on its own, so a
   * failure is logged and the steps after it still run.
   * @private
   * @param {import('@opentelemetry/api').Span[]} spans
   * @param {RecordPendingMetric[]} pendingMetrics
   * @param {{ message: string | undefined, errorType: string }} [failure] set when the operation
   *   failed
   * @returns {void}
   */
  _recordAndEnd(spans, pendingMetrics, failure) {
    const guard = (what, work) => {
      try {
        work()
      } catch (error) {
        this._diag.error(`${what} error`, error)
      }
    }

    pendingMetrics.forEach((record) => guard('metric', () => record(failure?.errorType)))
    spans.forEach((span) => {
      if (failure) {
        guard('span status', () => {
          span.setAttribute(ATTR_ERROR_TYPE, failure.errorType)
          span.setStatus({ code: SpanStatusCode.ERROR, message: failure.message })
        })
      }
      guard('span end', () => span.end())
    })
  }

  /**
   * @private
   * @param {object} options
   * @param {string} options.topic
   * @param {object | undefined} options.message
   * @param {string} options.operationType `process` or `receive`
   * @param {import('@opentelemetry/api').Attributes} options.attributes
   * @param {import('@opentelemetry/api').Context} [options.ctx]
   * @param {import('@opentelemetry/api').Link} [options.link]
   * @returns {import('@opentelemetry/api').Span} a `process <topic>` CONSUMER span, or a
   *   `poll <topic>` CLIENT span for `receive`; the consumer hook runs on it, and its errors are
   *   logged, never thrown
   */
  _startConsumerSpan({ topic, message, operationType, ctx, link, attributes }) {
    const operationName =
      operationType === MESSAGING_OPERATION_TYPE_VALUE_RECEIVE
        ? 'poll' // for batch processing spans
        : operationType // for individual message processing spans

    const span = this.tracer.startSpan(
      `${operationName} ${topic}`,
      {
        kind:
          operationType === MESSAGING_OPERATION_TYPE_VALUE_RECEIVE
            ? SpanKind.CLIENT
            : SpanKind.CONSUMER,
        attributes: {
          ...attributes,
          [ATTR_MESSAGING_SYSTEM]: MESSAGING_SYSTEM_VALUE_KAFKA,
          [ATTR_MESSAGING_DESTINATION_NAME]: topic,
          [ATTR_MESSAGING_OPERATION_TYPE]: operationType,
          [ATTR_MESSAGING_OPERATION_NAME]: operationName,
          [ATTR_MESSAGING_KAFKA_MESSAGE_KEY]: message?.key ? String(message.key) : undefined,
          [ATTR_MESSAGING_KAFKA_MESSAGE_TOMBSTONE]:
            message?.key && message.value === null ? true : undefined,
          [ATTR_MESSAGING_KAFKA_OFFSET]: message?.offset,
        },
        links: link ? [link] : [],
      },
      ctx
    )

    const { consumerHook } = this.getConfig()
    if (consumerHook && message) {
      safeExecuteInTheMiddle(
        () => consumerHook(span, { topic, message }),
        (e) => {
          if (e) this._diag.error('consumerHook error', e)
        },
        true
      )
    }

    return span
  }

  /**
   * @private
   * @param {string} topic
   * @param {object} message the caller's message; its `headers` gain the span's context when the
   *   message and its headers can take it, and are left as they are when not
   * @returns {import('@opentelemetry/api').Span} a `send <topic>` PRODUCER span; the producer hook
   *   runs on it, and its errors are logged, never thrown
   * @throws when the span's context cannot be added for another reason; the span is ended first
   */
  _startProducerSpan(topic, message) {
    const span = this.tracer.startSpan(`send ${topic}`, {
      kind: SpanKind.PRODUCER,
      attributes: {
        [ATTR_MESSAGING_SYSTEM]: MESSAGING_SYSTEM_VALUE_KAFKA,
        [ATTR_MESSAGING_DESTINATION_NAME]: topic,
        [ATTR_MESSAGING_KAFKA_MESSAGE_KEY]: message.key ? String(message.key) : undefined,
        [ATTR_MESSAGING_KAFKA_MESSAGE_TOMBSTONE]:
          message.key && message.value === null ? true : undefined,
        [ATTR_MESSAGING_DESTINATION_PARTITION_ID]:
          message.partition !== undefined ? String(message.partition) : undefined,
        [ATTR_MESSAGING_OPERATION_NAME]: 'send',
        [ATTR_MESSAGING_OPERATION_TYPE]: MESSAGING_OPERATION_TYPE_VALUE_SEND,
      },
    })

    try {
      this._addContextHeaders(span, message)
    } catch (error) {
      span.end()
      throw error
    }

    const { producerHook } = this.getConfig()
    if (producerHook) {
      safeExecuteInTheMiddle(
        () => producerHook(span, { topic, message }),
        (e) => {
          if (e) this._diag.error('producerHook error', e)
        },
        true
      )
    }

    return span
  }

  /**
   * Injects the span's context into a fresh carrier, so a propagator never touches the caller's
   * object, and merges the carrier into the message's headers, creating them when absent. Nothing
   * is written to a message that is not extensible, or to headers that are not.
   * @private
   * @param {import('@opentelemetry/api').Span} span
   * @param {{ headers?: Record<string, unknown> | null }} message
   * @returns {void}
   */
  _addContextHeaders(span, message) {
    const carrier = {}
    propagation.inject(trace.setSpan(context.active(), span), carrier)

    if (message.headers == null) {
      if (Object.isExtensible(message)) message.headers = carrier
    } else if (Object.isExtensible(message.headers)) {
      Object.assign(message.headers, carrier)
    }
  }
}

module.exports = { KafkaJsInstrumentation }
