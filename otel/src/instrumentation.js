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

/** What a metric instrument that could not be created is replaced by, so recording still works. */
const INERT_HISTOGRAM = Object.freeze({ record() {} })
const INERT_COUNTER = Object.freeze({ add() {} })

/** What an operation has when nothing could be observed of it. */
const NO_SPANS = Object.freeze([])
const NO_METRICS = Object.freeze([])
const NO_OBSERVATION = Object.freeze({ spans: NO_SPANS, pendingMetrics: NO_METRICS })

/** How a rejection is described when its reason cannot be read. */
const UNKNOWN_FAILURE = Object.freeze({ message: undefined, errorType: ERROR_TYPE_VALUE_OTHER })

/**
 * Runs work that only observes the application, so that no failure of it can reach the
 * application: a throw is logged as `<what> error` and `fallback` is returned instead. The logging
 * is contained too, so a diagnostic logger that throws changes nothing. `work` is synchronous and
 * never retried; do not use this for application calls or to start promises nobody handles.
 * @template T, F
 * @param {import('@opentelemetry/api').DiagLogger} diag
 * @param {string} what names the work in the log line
 * @param {() => T} work
 * @param {F} [fallback] what is returned when `work` throws
 * @returns {T | F} what `work` returned, or `fallback`
 */
function contained(diag, what, work, fallback) {
  try {
    return work()
  } catch (error) {
    try {
      diag.error(`${what} error`, error)
    } catch {
      // A logger that fails must not fail the operation either.
    }
    return fallback
  }
}

/**
 * Runs `call` inside `ctx` exactly once, whatever the context manager does. A manager that throws
 * before it runs the callback has the callback run once without the context; one that throws after,
 * runs it twice or returns something else changes nothing: the callback's own result or throw is
 * what comes back, and the manager's failure is logged. A throw is captured with a flag, since
 * `undefined` can be thrown.
 * @template T
 * @param {import('@opentelemetry/api').DiagLogger} diag
 * @param {import('@opentelemetry/api').Context} ctx
 * @param {() => T} call the application's own work
 * @returns {T} what `call` returned, the very object, not a copy
 * @throws exactly what `call` threw
 */
function withContext(diag, ctx, call) {
  let called = false
  let failed = false
  let result
  let reason
  const once = () => {
    if (!called) {
      called = true
      try {
        result = call()
      } catch (error) {
        failed = true
        reason = error
      }
    }
    if (failed) throw reason
    return result
  }
  contained(diag, 'context', () => context.with(ctx, once))
  if (!called) return once()
  if (failed) throw reason
  return result
}

/**
 * @param {unknown} reason what an operation rejected with
 * @returns {{ message: string | undefined, errorType: string }} the reason's message and the
 *   `error.type` for it: its constructor name, `_OTHER` for a reason that is `null`, has no own
 *   `message` or has no constructor name
 */
function describeFailure(reason) {
  let message
  let errorType = ERROR_TYPE_VALUE_OTHER
  if (typeof reason === 'string' || reason === undefined) {
    message = reason
  } else if (
    typeof reason === 'object' &&
    reason !== null &&
    Object.prototype.hasOwnProperty.call(reason, 'message')
  ) {
    message = reason.message
    errorType = reason.constructor?.name || errorType
  }
  return { message, errorType }
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
 * Observing never changes what the application sees. A tracer, meter, span, context manager, hook
 * or diagnostic logger that throws is logged and skipped, the client's own call runs exactly once,
 * and its result or rejection is passed on as it was. Every span an operation started is ended.
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
   * provider changes. An instrument that cannot be created is logged and replaced by one that
   * records nothing, so the others still work.
   * @returns {void}
   */
  _updateMetricInstruments() {
    const diag = this._diag
    this._clientDuration = contained(
      diag,
      'client histogram',
      () =>
        this.meter.createHistogram(METRIC_MESSAGING_CLIENT_OPERATION_DURATION, {
          advice: { explicitBucketBoundaries: HISTOGRAM_BUCKET_BOUNDARIES },
        }),
      INERT_HISTOGRAM
    )
    this._sentMessages = contained(
      diag,
      'sent counter',
      () => this.meter.createCounter(METRIC_MESSAGING_CLIENT_SENT_MESSAGES),
      INERT_COUNTER
    )
    this._consumedMessages = contained(
      diag,
      'consumed counter',
      () => this.meter.createCounter(METRIC_MESSAGING_CLIENT_CONSUMED_MESSAGES),
      INERT_COUNTER
    )
    this._processDuration = contained(
      diag,
      'process histogram',
      () =>
        this.meter.createHistogram(METRIC_MESSAGING_PROCESS_DURATION, {
          advice: { explicitBucketBoundaries: HISTOGRAM_BUCKET_BOUNDARIES },
        }),
      INERT_HISTOGRAM
    )
  }

  /**
   * The module definitions: the client's 3.x versions, prereleases included, under either name,
   * with no file hooks. Patching first unpatches, so enabling twice wraps once; unpatching restores
   * the two factories only, not the clients they already created. A factory that cannot be wrapped
   * or unwrapped is logged and left as it is, and the module's exports are always returned.
   * @protected
   * @returns {InstrumentationNodeModuleDefinition[]}
   */
  init() {
    const diag = this._diag
    const unpatch = (moduleExports) => {
      for (const factory of ['producer', 'consumer']) {
        contained(diag, `${factory} unpatch`, () => {
          if (isWrapped(moduleExports?.Kafka?.prototype[factory])) {
            this._unwrap(moduleExports.Kafka.prototype, factory)
          }
        })
      }
    }

    const patch = (moduleExports) => {
      unpatch(moduleExports)
      contained(diag, 'producer patch', () =>
        this._wrap(moduleExports?.Kafka?.prototype, 'producer', this._getProducerPatch())
      )
      contained(diag, 'consumer patch', () =>
        this._wrap(moduleExports?.Kafka?.prototype, 'consumer', this._getConsumerPatch())
      )

      return moduleExports
    }

    return MODULE_NAMES.map(
      (name) => new InstrumentationNodeModuleDefinition(name, SUPPORTED_VERSIONS, patch, unpatch)
    )
  }

  /**
   * Replaces any earlier wrapper of `target[name]` with `patch`, and does nothing when `target` has
   * no such method. A failure is logged; an unwrap completed before a failed wrap is not rolled back.
   * @private
   * @param {object | undefined} target
   * @param {string} name
   * @param {(original: Function) => Function} patch
   * @returns {void}
   */
  _rewrap(target, name, patch) {
    contained(this._diag, `${name} wrapper`, () => {
      if (!target?.[name]) return
      if (isWrapped(target[name])) this._unwrap(target, name)
      this._wrap(target, name, patch)
    })
  }

  /**
   * @private
   * @returns {(original: Function) => Function} wraps `Kafka#consumer`: every consumer it creates
   *   gets a wrapped `run` and the `REQUEST` listener. The client's own factory runs once, outside
   *   any containment: its result and its throw are the application's
   */
  _getConsumerPatch() {
    const instrumentation = this
    return (original) => {
      return function consumer(...args) {
        const newConsumer = original.apply(this, args)

        instrumentation._rewrap(newConsumer, 'run', instrumentation._getConsumerRunPatch())
        instrumentation._setKafkaEventListeners(newConsumer)

        return newConsumer
      }
    }
  }

  /**
   * Adds one `REQUEST` listener per client, which records the client operation duration. A client
   * that cannot take the listener is logged and left without it.
   * @private
   * @param {{ on: Function, events: { REQUEST: string } }} kafkaObj a producer or a consumer
   * @returns {void}
   */
  _setKafkaEventListeners(kafkaObj) {
    contained(this._diag, 'request listener', () => {
      if (kafkaObj[EVENT_LISTENERS_SET]) return

      if (kafkaObj.events?.REQUEST) {
        kafkaObj.on(kafkaObj.events.REQUEST, this._recordClientDurationMetric.bind(this))
      }

      kafkaObj[EVENT_LISTENERS_SET] = true
    })
  }

  /**
   * Records the duration of one client request. A payload or a meter that fails is logged and the
   * event is dropped; the listener never throws into the client that emitted it.
   * @private
   * @param {{ payload: { broker: string, duration: number, apiName: string } }} event a `REQUEST`
   *   instrumentation event; its `broker` is `host:port`, split at the last colon so that an IPv6
   *   host keeps its own
   * @returns {void}
   */
  _recordClientDurationMetric(event) {
    contained(this._diag, 'request metric', () => {
      const { broker } = event.payload
      const portStart = broker.lastIndexOf(':') + 1
      this._clientDuration.record(event.payload.duration / 1000, {
        [ATTR_MESSAGING_SYSTEM]: MESSAGING_SYSTEM_VALUE_KAFKA,
        [ATTR_MESSAGING_OPERATION_NAME]: `${event.payload.apiName}`,
        [ATTR_SERVER_ADDRESS]: broker.slice(0, portStart - 1),
        [ATTR_SERVER_PORT]: Number.parseInt(broker.slice(portStart), 10),
      })
    })
  }

  /**
   * @private
   * @returns {(original: Function) => Function} wraps `Kafka#producer`: every producer it creates
   *   gets wrapped `send`, `sendBatch` and `transaction` and the `REQUEST` listener. The client's
   *   own factory runs once, outside any containment: its result and its throw are the
   *   application's
   */
  _getProducerPatch() {
    const instrumentation = this
    return (original) => {
      return function producer(...args) {
        const newProducer = original.apply(this, args)

        instrumentation._rewrap(newProducer, 'sendBatch', instrumentation._getSendBatchPatch())
        instrumentation._rewrap(newProducer, 'send', instrumentation._getSendPatch())
        instrumentation._rewrap(
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
   *   `eachBatch` of the config it is given are wrapped in place, each on its own, and `run` is
   *   called once with that config whatever happens to the wrapping
   */
  _getConsumerRunPatch() {
    const instrumentation = this
    return (original) => {
      return function run(...args) {
        const config = args[0]
        instrumentation._rewrap(
          config,
          'eachMessage',
          instrumentation._getConsumerEachMessagePatch()
        )
        instrumentation._rewrap(config, 'eachBatch', instrumentation._getConsumerEachBatchPatch())
        return original.call(this, config)
      }
    }
  }

  /**
   * @private
   * @returns {(original: Function) => Function} wraps `eachMessage`: a `process <topic>` span in
   *   the context the message headers carry, active while the handler runs. A handler that throws
   *   synchronously ends the span and records its failure like a rejection, and the wrapper
   *   returns the rejected promise. Every step of the preparation is contained on its own: the
   *   handler runs once whatever fails, untraced where it must be
   */
  _getConsumerEachMessagePatch() {
    const instrumentation = this
    return (original) => {
      return function eachMessage(...args) {
        const diag = instrumentation._diag
        const payload = args[0]
        const propagatedContext = contained(
          diag,
          'message preparation',
          () => propagation.extract(ROOT_CONTEXT, payload.message.headers, bufferTextMapGetter),
          ROOT_CONTEXT
        )
        const span = contained(
          diag,
          'message preparation',
          () =>
            instrumentation._startConsumerSpan({
              topic: payload.topic,
              message: payload.message,
              operationType: MESSAGING_OPERATION_TYPE_VALUE_PROCESS,
              ctx: propagatedContext,
              attributes: {
                [ATTR_MESSAGING_DESTINATION_PARTITION_ID]: String(payload.partition),
              },
            }),
          undefined
        )

        const pendingMetrics = contained(
          diag,
          'message preparation',
          () => [
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
          ],
          NO_METRICS
        )

        const handlerContext = span
          ? contained(
              diag,
              'message context',
              () => trace.setSpan(propagatedContext, span),
              propagatedContext
            )
          : propagatedContext
        const eachMessagePromise = rejectOnThrow(() =>
          withContext(diag, handlerContext, () => original.apply(this, args))
        )
        return instrumentation._endSpansOnPromise(
          span ? [span] : NO_SPANS,
          pendingMetrics,
          eachMessagePromise
        )
      }
    }
  }

  /**
   * @private
   * @returns {(original: Function) => Function} wraps `eachBatch`: a `poll <topic>` span that
   *   starts a new trace, with one `process <topic>` span per message under it, each linked to the
   *   producer span its headers carry. A handler that throws synchronously ends every span and
   *   records its failure like a rejection, and the wrapper returns the rejected promise. Every
   *   step of the preparation, and each message's own, is contained: the handler runs once
   *   whatever fails, and a message whose span cannot be made goes without one
   */
  _getConsumerEachBatchPatch() {
    return (original) => {
      const instrumentation = this
      return function eachBatch(...args) {
        const diag = instrumentation._diag
        const payload = args[0]
        // https://github.com/open-telemetry/opentelemetry-specification/blob/master/specification/trace/semantic_conventions/messaging.md#topic-with-multiple-consumers
        const receivingSpan = contained(
          diag,
          'batch preparation',
          () =>
            instrumentation._startConsumerSpan({
              topic: payload.batch.topic,
              message: undefined,
              operationType: MESSAGING_OPERATION_TYPE_VALUE_RECEIVE,
              ctx: ROOT_CONTEXT,
              attributes: {
                [ATTR_MESSAGING_BATCH_MESSAGE_COUNT]: payload.batch.messages.length,
                [ATTR_MESSAGING_DESTINATION_PARTITION_ID]: String(payload.batch.partition),
              },
            }),
          undefined
        )
        const activeContext = contained(
          diag,
          'batch preparation',
          () => context.active(),
          ROOT_CONTEXT
        )
        const batchContext = receivingSpan
          ? contained(
              diag,
              'batch preparation',
              () => trace.setSpan(activeContext, receivingSpan),
              activeContext
            )
          : activeContext
        return withContext(diag, batchContext, () => {
          const startTime = Date.now()
          const spans = receivingSpan ? [receivingSpan] : []
          const pendingMetrics = []
          contained(diag, 'batch preparation', () => {
            pendingMetrics.push(
              prepareCounter(instrumentation._consumedMessages, payload.batch.messages.length, {
                [ATTR_MESSAGING_SYSTEM]: MESSAGING_SYSTEM_VALUE_KAFKA,
                [ATTR_MESSAGING_OPERATION_NAME]: 'process',
                [ATTR_MESSAGING_DESTINATION_NAME]: payload.batch.topic,
                [ATTR_MESSAGING_DESTINATION_PARTITION_ID]: String(payload.batch.partition),
              })
            )
          })
          contained(diag, 'batch messages', () => {
            payload.batch.messages.forEach((message) => {
              const propagatedContext = contained(
                diag,
                'batch preparation',
                () => propagation.extract(ROOT_CONTEXT, message.headers, bufferTextMapGetter),
                ROOT_CONTEXT
              )
              const link = contained(
                diag,
                'batch link',
                () => {
                  const spanContext = trace.getSpan(propagatedContext)?.spanContext()
                  return spanContext ? { context: spanContext } : undefined
                },
                undefined
              )
              const span = contained(
                diag,
                'batch preparation',
                () =>
                  instrumentation._startConsumerSpan({
                    topic: payload.batch.topic,
                    message,
                    operationType: MESSAGING_OPERATION_TYPE_VALUE_PROCESS,
                    link,
                    attributes: {
                      [ATTR_MESSAGING_DESTINATION_PARTITION_ID]: String(payload.batch.partition),
                    },
                  }),
                undefined
              )
              if (span) spans.push(span)
              contained(diag, 'batch preparation', () => {
                pendingMetrics.push(
                  prepareDurationHistogram(instrumentation._processDuration, startTime, {
                    [ATTR_MESSAGING_SYSTEM]: MESSAGING_SYSTEM_VALUE_KAFKA,
                    [ATTR_MESSAGING_OPERATION_NAME]: 'process',
                    [ATTR_MESSAGING_DESTINATION_NAME]: payload.batch.topic,
                    [ATTR_MESSAGING_DESTINATION_PARTITION_ID]: String(payload.batch.partition),
                  })
                )
              })
            })
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
   *
   * The returned promise settles as the client's own does, with the same transaction object or the
   * same reason; a span that cannot be made leaves the transaction untraced. The client's call
   * happens once, and a synchronous throw of it comes back as a rejection.
   * @private
   * @returns {(original: Function) => Function}
   */
  _getProducerTransactionPatch() {
    const instrumentation = this
    return (original) => {
      return function transaction(...args) {
        const transactionSpan = contained(
          instrumentation._diag,
          'transaction preparation',
          () => instrumentation.tracer.startSpan('transaction'),
          undefined
        )

        const transactionPromise = Promise.resolve(rejectOnThrow(() => original.apply(this, args)))
        if (!transactionSpan) return transactionPromise

        return transactionPromise.then(
          (transaction) => {
            instrumentation._decorateTransaction(transaction, transactionSpan)
            return transaction
          },
          (error) => {
            instrumentation._failTransactionSpan(transactionSpan, error, true)
            throw error
          }
        )
      }
    }
  }

  /**
   * Replaces the transaction's `commit` and `abort`, which end its span, and then its `send` and
   * `sendBatch`, which make the span their sends' parent. Each is replaced on its own. When
   * `commit` and `abort` cannot both be replaced nothing could end the span later, so it is ended
   * now; method replacements that already completed remain in place.
   * @private
   * @param {{ send: Function, sendBatch: Function, commit: Function, abort: Function }} transaction
   * @param {import('@opentelemetry/api').Span} transactionSpan
   * @returns {void}
   */
  _decorateTransaction(transaction, transactionSpan) {
    const diag = this._diag
    const closable = contained(
      diag,
      'transaction decoration',
      () => {
        const originalCommit = transaction.commit
        const originalAbort = transaction.abort
        transaction.commit = this._getTransactionCommit(originalCommit, transactionSpan)
        transaction.abort = this._getTransactionAbort(originalAbort, transactionSpan)
        return true
      },
      false
    )
    if (!closable) {
      this._recordAndEnd([transactionSpan], NO_METRICS)
      return
    }

    contained(diag, 'transaction send decoration', () => {
      const originalSend = transaction.send
      transaction.send = this._getTransactionSend(
        this._getSendPatch()(originalSend),
        transactionSpan
      )
    })
    contained(diag, 'transaction sendBatch decoration', () => {
      const originalSendBatch = transaction.sendBatch
      transaction.sendBatch = this._getTransactionSend(
        this._getSendBatchPatch()(originalSendBatch),
        transactionSpan
      )
    })
  }

  /**
   * @private
   * @param {Function} patched a producer `send` or `sendBatch` wrapper over the transaction's own
   * @param {import('@opentelemetry/api').Span} transactionSpan
   * @returns {Function} the transaction's `send` or `sendBatch`: `patched` runs once in the
   *   transaction span's context; a failure also marks the transaction span, which commit or abort
   *   ends later, and is rethrown as it was
   */
  _getTransactionSend(patched, transactionSpan) {
    const instrumentation = this
    const diag = this._diag
    return function send(...args) {
      const activeContext = contained(
        diag,
        'transaction context',
        () => context.active(),
        ROOT_CONTEXT
      )
      const sendContext = contained(
        diag,
        'transaction context',
        () => trace.setSpan(activeContext, transactionSpan),
        activeContext
      )
      const pending = rejectOnThrow(() =>
        withContext(diag, sendContext, () => patched.apply(this, args))
      )
      return Promise.resolve(pending).catch((error) => {
        instrumentation._failTransactionSpan(transactionSpan, error, false)
        throw error
      })
    }
  }

  /**
   * @private
   * @param {Function} originalCommit
   * @param {import('@opentelemetry/api').Span} transactionSpan
   * @returns {Function} the transaction's `commit`: it resolves with the client's own value, after
   *   marking the span OK, and ends the span with the outcome
   */
  _getTransactionCommit(originalCommit, transactionSpan) {
    const instrumentation = this
    const diag = this._diag
    return function commit(...args) {
      const pending = rejectOnThrow(() => originalCommit.apply(this, args))
      const committed = Promise.resolve(pending).then((result) => {
        contained(diag, 'transaction success', () =>
          transactionSpan.setStatus({ code: SpanStatusCode.OK })
        )
        return result
      })
      return instrumentation._endSpansOnPromise([transactionSpan], NO_METRICS, committed)
    }
  }

  /**
   * @private
   * @param {Function} originalAbort
   * @param {import('@opentelemetry/api').Span} transactionSpan
   * @returns {Function} the transaction's `abort`, which ends the span with the outcome
   */
  _getTransactionAbort(originalAbort, transactionSpan) {
    const instrumentation = this
    return function abort(...args) {
      const pending = rejectOnThrow(() => originalAbort.apply(this, args))
      return instrumentation._endSpansOnPromise([transactionSpan], NO_METRICS, pending)
    }
  }

  /**
   * Marks the transaction span failed, the status and the exception each on their own, and ends it
   * when the failure is the transaction's own.
   * @private
   * @param {import('@opentelemetry/api').Span} transactionSpan
   * @param {unknown} error what the client rejected with
   * @param {boolean} end whether this failure ends the transaction span
   * @returns {void}
   */
  _failTransactionSpan(transactionSpan, error, end) {
    const diag = this._diag
    const message = contained(diag, 'transaction error message', () => error?.message, undefined)
    contained(diag, 'transaction status', () =>
      transactionSpan.setStatus({ code: SpanStatusCode.ERROR, message })
    )
    contained(diag, 'transaction exception', () => transactionSpan.recordException(error))
    if (end) contained(diag, 'transaction end', () => transactionSpan.end())
  }

  /**
   * @private
   * @returns {(original: Function) => Function} wraps a producer's or a transaction's
   *   `sendBatch`: one `send <topic>` span per message, its context added to the message's headers
   *   (the caller's message object is changed, as upstream does, unless it or its headers cannot be
   *   changed). The instrumentation never decides whether the produce runs: `original` is always
   *   called once, and with no `topicMessages` array it is called with nothing else done and its
   *   own promise is returned
   */
  _getSendBatchPatch() {
    const instrumentation = this
    return (original) => {
      return function sendBatch(...args) {
        const observed = contained(
          instrumentation._diag,
          'producer preparation',
          () => {
            const topicMessages = args[0]?.topicMessages
            return Array.isArray(topicMessages)
              ? instrumentation._startProducerSpans(topicMessages)
              : undefined
          },
          NO_OBSERVATION
        )
        if (!observed) return original.apply(this, args)

        const origSendResult = rejectOnThrow(() => original.apply(this, args))
        return instrumentation._endSpansOnPromise(
          observed.spans,
          observed.pendingMetrics,
          origSendResult
        )
      }
    }
  }

  /**
   * @private
   * @returns {(original: Function) => Function} wraps a producer's or a transaction's `send`, as
   *   `_getSendBatchPatch` does for one topic; with no `messages` array `original` is called with
   *   nothing else done and its own promise is returned
   */
  _getSendPatch() {
    const instrumentation = this
    return (original) => {
      return function send(...args) {
        const observed = contained(
          instrumentation._diag,
          'producer preparation',
          () => {
            const record = args[0]
            return Array.isArray(record?.messages)
              ? instrumentation._startProducerSpans([record])
              : undefined
          },
          NO_OBSERVATION
        )
        if (!observed) return original.apply(this, args)

        const origSendResult = rejectOnThrow(() => original.apply(this, args))
        return instrumentation._endSpansOnPromise(
          observed.spans,
          observed.pendingMetrics,
          origSendResult
        )
      }
    }
  }

  /**
   * Starts a `send <topic>` span and prepares the sent-messages metric for every message. When any
   * of them fails, the failure is logged, the spans already started are ended and none are
   * returned, so the produce goes on untraced.
   * @private
   * @param {Array<{ topic: string, messages: object[] }>} topicMessages
   * @returns {{ spans: import('@opentelemetry/api').Span[], pendingMetrics: RecordPendingMetric[] }}
   */
  _startProducerSpans(topicMessages) {
    const spans = []
    const pendingMetrics = []
    const prepared = contained(
      this._diag,
      'producer span',
      () => {
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
        return true
      },
      false
    )
    if (!prepared) {
      this._recordAndEnd(spans, NO_METRICS)
      return NO_OBSERVATION
    }
    return { spans, pendingMetrics }
  }

  /**
   * Ends the spans and records the metrics once the operation settles. A rejection sets every
   * span's status to ERROR with the reason's message and `error.type` to the reason's class name
   * (`_OTHER` for a reason that is `null`, has no `message` or has no constructor name, or whose
   * description cannot be read). The returned promise settles with the operation's own result or
   * reason: a meter, a span or a span processor that throws is logged, never passed on.
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
        const failure = contained(
          this._diag,
          'error description',
          () => describeFailure(reason),
          UNKNOWN_FAILURE
        )
        this._recordAndEnd(spans, pendingMetrics, failure)
        throw reason
      }
    )
  }

  /**
   * Records the metrics, then marks and ends every span. Each step is contained on its own, so a
   * failure is logged and the steps after it still run. A span whose `end` throws before it ends
   * gets that one attempt and no retry; the spans after it are still ended.
   * @private
   * @param {import('@opentelemetry/api').Span[]} spans
   * @param {RecordPendingMetric[]} pendingMetrics
   * @param {{ message: string | undefined, errorType: string }} [failure] set when the operation
   *   failed
   * @returns {void}
   */
  _recordAndEnd(spans, pendingMetrics, failure) {
    const diag = this._diag
    pendingMetrics.forEach((record) => contained(diag, 'metric', () => record(failure?.errorType)))
    spans.forEach((span) => {
      if (failure) {
        contained(diag, 'span attribute', () =>
          span.setAttribute(ATTR_ERROR_TYPE, failure.errorType)
        )
        contained(diag, 'span status', () =>
          span.setStatus({ code: SpanStatusCode.ERROR, message: failure.message })
        )
      }
      contained(diag, 'span end', () => span.end())
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
   * @throws when the tracer or a message getter fails, before any span exists
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

    contained(this._diag, 'consumer hook', () => {
      const { consumerHook } = this.getConfig()
      if (consumerHook && message) consumerHook(span, { topic, message })
    })

    return span
  }

  /**
   * @private
   * @param {string} topic
   * @param {object} message the caller's message; its `headers` gain the span's context when the
   *   message and its headers can take it, and are left as they are when not
   * @returns {import('@opentelemetry/api').Span} a `send <topic>` PRODUCER span; the producer hook
   *   runs on it, and its errors are logged, never thrown
   * @throws when the tracer fails, before any span exists, or when the span's context cannot be
   *   added for another reason; the span is ended first
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
      contained(this._diag, 'span end', () => span.end())
      throw error
    }

    contained(this._diag, 'producer hook', () => {
      const { producerHook } = this.getConfig()
      if (producerHook) producerHook(span, { topic, message })
    })

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
