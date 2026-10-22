/*
 * Fixtures for the telemetry-failure matrix: a stand-in client wired to an instrumentation whose
 * every collaborator (tracer, meter, context manager, propagator, diagnostic logger, messages,
 * rejections) can be made to throw at a named point, and the oracle that checks the application's
 * operation did not notice.
 */

const assert = require('node:assert/strict')
const {
  context,
  diag,
  DiagLogLevel,
  propagation,
  ROOT_CONTEXT,
  trace,
} = require('@opentelemetry/api')
const { isWrapped } = require('@opentelemetry/instrumentation')
const { KafkaJsInstrumentation } = require('../src')
const {
  METRIC_MESSAGING_CLIENT_OPERATION_DURATION,
  METRIC_MESSAGING_CLIENT_SENT_MESSAGES,
} = require('../src/semconv')

/** The API's own `setSpan`, which `trace.setSpan` is replaced by a trapped wrapper of. */
const setSpan = trace.setSpan

/**
 * @returns {{ promise: Promise<unknown>, resolve: Function, reject: Function }} a promise settled
 *   from outside
 */
const deferred = () => {
  const result = {}
  result.promise = new Promise((resolve, reject) => Object.assign(result, { resolve, reject }))
  return result
}

/**
 * Counts every visit to a named injection point and throws the observer error at the armed ones.
 */
class Traps {
  /** @param {unknown} error what an armed point throws */
  constructor(error) {
    this.error = error
    this.armed = {}
    this.reached = {}
    this.fired = 0
  }

  /**
   * @param {Record<string, true | number>} armed point names; `true` fires at every visit, a number
   *   at that visit only
   * @returns {void}
   */
  arm(armed) {
    this.armed = armed
    this.reached = {}
  }

  /**
   * @param {string} name
   * @returns {boolean} whether this visit is one the point is armed to fail at
   */
  probe(name) {
    const visit = (this.reached[name] = (this.reached[name] ?? 0) + 1)
    const armed = this.armed[name]
    const fires = armed === true || armed === visit
    if (fires) this.fired += 1
    return fires
  }

  /**
   * @param {string} name
   * @returns {void}
   * @throws the observer error when the point is armed for this visit
   */
  hit(name) {
    if (this.probe(name)) throw this.error
  }
}

/** A span that counts what is done to it and fails at armed points. */
class SpySpan {
  /**
   * @param {Harness} harness
   * @param {string} name `<kind> <topic>`
   * @param {{ attributes?: object, links?: unknown[] }} [options]
   */
  constructor(harness, name, options) {
    this.traps = harness.traps
    this.name = name
    this.kind = name.split(' ')[0]
    this.attributes = { ...options?.attributes }
    this.links = options?.links ?? []
    this.status = undefined
    this.exceptions = []
    this.endAttempts = 0
    this.ended = false
  }

  /**
   * @param {string} key
   * @param {unknown} value
   * @returns {SpySpan} this span
   * @throws the observer error when `span.setAttribute:<kind>` is armed
   */
  setAttribute(key, value) {
    this.traps.hit(`span.setAttribute:${this.kind}`)
    this.attributes[key] = value
    return this
  }

  /**
   * @param {{ code: number, message?: string }} status
   * @returns {SpySpan} this span
   * @throws the observer error when `span.setStatus:<kind>` is armed
   */
  setStatus(status) {
    this.traps.hit(`span.setStatus:${this.kind}`)
    this.status = status
    return this
  }

  /**
   * @param {unknown} error
   * @returns {void}
   * @throws the observer error when `span.recordException:<kind>` is armed
   */
  recordException(error) {
    this.traps.hit(`span.recordException:${this.kind}`)
    this.exceptions.push(error)
  }

  /**
   * Counts the attempt, then fails before ending when `span.end:before:<kind>` is armed, or after
   * ending when `span.end:after:<kind>` is.
   * @returns {void}
   * @throws the observer error
   */
  end() {
    this.endAttempts += 1
    this.traps.hit(`span.end:before:${this.kind}`)
    this.ended = true
    this.traps.hit(`span.end:after:${this.kind}`)
  }

  /** @returns {import('@opentelemetry/api').SpanContext} a fixed span context */
  spanContext() {
    return { traceId: 'a'.repeat(32), spanId: 'b'.repeat(16), traceFlags: 1 }
  }

  /** @returns {boolean} whether the span has not ended */
  isRecording() {
    return !this.ended
  }
}

/** A context manager whose `with` fails before, after or around the callback when armed. */
class TestContextManager {
  /** @param {Harness} harness */
  constructor(harness) {
    this.harness = harness
    this.current = ROOT_CONTEXT
  }

  /**
   * @returns {import('@opentelemetry/api').Context} the context of the call in progress
   * @throws the observer error when `context.active` is armed
   */
  active() {
    this.harness.traps.hit('context.active')
    return this.current
  }

  /**
   * Runs `fn` in `ctx`. Armed, it throws before running `fn`, throws after, runs `fn` twice, or
   * returns another object in place of `fn`'s result; `fn`'s own throw is rethrown otherwise.
   * @param {import('@opentelemetry/api').Context} ctx
   * @param {Function} fn
   * @param {unknown} [thisArg]
   * @param {...unknown} args
   * @returns {unknown}
   */
  with(ctx, fn, thisArg, ...args) {
    const { traps } = this.harness
    this.harness.entered.push(ctx)
    traps.hit('context.with:before')
    const previous = this.current
    this.current = ctx
    try {
      let result
      let threw = false
      let error
      try {
        result = fn.call(thisArg, ...args)
      } catch (thrown) {
        threw = true
        error = thrown
      }
      traps.hit('context.with:after')
      if (traps.probe('context.with:twice')) {
        try {
          fn.call(thisArg, ...args)
        } catch {
          // The second attempt is the manager's fault; only the first outcome counts.
        }
      }
      if (traps.probe('context.with:replacement')) return { substituted: true }
      if (threw) throw error
      return result
    } finally {
      this.current = previous
    }
  }

  /** @returns {unknown} `target`, unbound */
  bind(_ctx, target) {
    return target
  }

  /** @returns {TestContextManager} this manager */
  enable() {
    return this
  }

  /** @returns {TestContextManager} this manager */
  disable() {
    return this
  }
}

/** A context wrapping another, whose reads fail at an armed point. */
class ExtractedContext {
  /**
   * @param {Harness} harness
   * @param {import('@opentelemetry/api').Context} base
   */
  constructor(harness, base) {
    this.harness = harness
    this.base = base
  }

  /**
   * @param {symbol} key
   * @returns {unknown}
   * @throws the observer error when `context.getValue` is armed
   */
  getValue(key) {
    this.harness.traps.hit('context.getValue')
    return this.base.getValue(key)
  }

  /** @returns {ExtractedContext} a copy holding `value` under `key` */
  setValue(key, value) {
    return new ExtractedContext(this.harness, this.base.setValue(key, value))
  }

  /** @returns {ExtractedContext} a copy without `key` */
  deleteValue(key) {
    return new ExtractedContext(this.harness, this.base.deleteValue(key))
  }
}

/**
 * @param {Traps} traps
 * @param {string} name
 * @param {unknown} value
 * @returns {PropertyDescriptor} an accessor that returns `value` and fails at the armed point
 */
const trapped = (traps, name, value) => ({
  enumerable: true,
  configurable: true,
  get() {
    traps.hit(name)
    return value
  },
})

/**
 * Builds one instrumented stand-in operation with injectable faults. Use {@link createHarness}.
 */
class Harness {
  /**
   * @param {object} options
   * @param {string} options.operation a key of {@link OPERATIONS}
   * @param {string} [options.outcome] one of `resolve`, `reject`, `throw`, `throw-undefined`,
   *   `pending-resolve`, `pending-reject`
   * @param {{ arm: Record<string, true | number>, atConstruction?: boolean }} [options.fault]
   * @param {boolean} [options.loggerThrows] whether the diagnostic logger throws too
   * @param {boolean} [options.hostileError] whether the observer error throws when its `message`
   *   is read, as the logger does
   */
  constructor({
    operation,
    outcome = 'resolve',
    fault,
    loggerThrows = false,
    hostileError = false,
  }) {
    this.operation = operation
    this.outcome = outcome
    this.fault = fault
    this.loggerThrows = loggerThrows
    this.spec = OPERATIONS[operation]
    this.hostileError = hostileError
    this.observerError = hostileError
      ? Object.freeze(
          Object.defineProperty({}, 'message', {
            get() {
              throw new Error('the failure cannot be read')
            },
          })
        )
      : Object.freeze(new Error('observer failure'))
    this.diagnosticError = Object.freeze(new Error('diagnostic failure'))
    this.traps = new Traps(this.observerError)
    this.spans = []
    this.startContexts = []
    this.entered = []
    this.measurements = []
    this.created = []
    this.logged = []
    this.extracted = []
    this.release = undefined
    this.setupError = undefined
    this.restorers = []

    this.reason = outcome === 'throw-undefined' ? undefined : this.makeReason()
    this.instrumentation = new KafkaJsInstrumentation({ enabled: false })
    this.installDiagnostics()
    this.installContext()
    this.installPropagation()
    this.installInstrumentation()
    this.parts = this.spec.build(this)
    this.value = this.parts.value
    this.fault?.configure?.(this)
  }

  /** @returns {Function} the counted original operation under test */
  get original() {
    return this.parts.original
  }

  /** @returns {unknown} the receiver the wrapped operation is called on */
  get receiver() {
    return this.parts.receiver
  }

  /** @returns {unknown[]} the arguments the wrapped operation is called with */
  get args() {
    return this.parts.args
  }

  /** @returns {() => unknown} calls the wrapped operation once */
  get invoke() {
    return this.parts.invoke
  }

  /** @returns {string[]} which instrument each recorded measurement went to, in order */
  get points() {
    return this.measurements.map(({ point }) => point)
  }

  /**
   * @param {Function} [implementation]
   * @returns {Function & { calls: Array<{ receiver: unknown, args: unknown[] }> }} a function that
   *   records its receiver and arguments
   */
  counted(implementation) {
    const fn = function (...args) {
      fn.calls.push({ receiver: this, args })
      return implementation.apply(this, args)
    }
    fn.calls = []
    return fn
  }

  /**
   * @returns {unknown} what the original operation returns, or throws, for the chosen outcome
   */
  outcomeResult() {
    switch (this.outcome) {
      case 'resolve':
        return Promise.resolve(this.value)
      case 'reject':
        return Promise.reject(this.reason)
      case 'pending-resolve':
      case 'pending-reject': {
        const pending = deferred()
        this.release = () =>
          this.outcome === 'pending-resolve'
            ? pending.resolve(this.value)
            : pending.reject(this.reason)
        return pending.promise
      }
      default:
        throw this.reason
    }
  }

  /**
   * @returns {unknown} the synchronous value of the original, or its throw
   */
  syncResult() {
    if (this.outcome === 'resolve') return this.value
    throw this.reason
  }

  /**
   * @returns {object} a frozen rejection reason whose `message`, `constructor` and descriptor reads
   *   fail at armed points
   */
  makeReason() {
    const { traps } = this
    const reason = {}
    Object.defineProperty(
      reason,
      'message',
      trapped(traps, 'reason.message', 'application failure')
    )
    Object.defineProperty(reason, 'constructor', {
      enumerable: false,
      configurable: true,
      get() {
        traps.hit('reason.constructor')
        return {
          get name() {
            traps.hit('reason.constructor.name')
            return 'ApplicationFailure'
          },
        }
      },
    })
    Object.freeze(reason)
    return new Proxy(reason, {
      getOwnPropertyDescriptor(target, key) {
        traps.hit('reason.descriptor')
        return Reflect.getOwnPropertyDescriptor(target, key)
      },
    })
  }

  /**
   * @param {number} index 1-based
   * @param {object} [options]
   * @param {boolean} [options.producer] a produced message, with a partition, and headers a test
   *   can make unwritable
   * @returns {object} a message whose every field read fails at an armed point
   */
  message(index, { producer = false } = {}) {
    const { traps } = this
    const message = {}
    Object.defineProperty(message, 'key', trapped(traps, `message.key:${index}`, `key-${index}`))
    Object.defineProperty(message, 'value', trapped(traps, `message.value:${index}`, `v-${index}`))
    if (producer) {
      Object.defineProperty(
        message,
        'partition',
        trapped(traps, `message.partition:${index}`, index - 1)
      )
    } else {
      Object.defineProperty(
        message,
        'offset',
        trapped(traps, `message.offset:${index}`, `${index}`)
      )
    }
    const headers = producer
      ? new Proxy(
          {},
          {
            isExtensible(target) {
              traps.hit(`headers.isExtensible:${index}`)
              return Reflect.isExtensible(target)
            },
            set(target, key, value) {
              traps.hit(`headers.set:${index}`)
              target[key] = value
              return true
            },
          }
        )
      : {}
    Object.defineProperty(message, 'headers', trapped(traps, `message.headers:${index}`, headers))
    return message
  }

  /**
   * @returns {{ topic: string, partition: number, message: object }} an `eachMessage` payload
   */
  messagePayload() {
    const payload = { message: this.message(1) }
    Object.defineProperty(payload, 'topic', trapped(this.traps, 'payload.topic', 't'))
    Object.defineProperty(payload, 'partition', trapped(this.traps, 'payload.partition', 3))
    return payload
  }

  /**
   * @returns {{ batch: { topic: string, partition: number, messages: object[] } }} an `eachBatch`
   *   payload of two messages
   */
  batchPayload() {
    const { traps } = this
    const batch = {}
    Object.defineProperty(batch, 'topic', trapped(traps, 'batch.topic', 't'))
    Object.defineProperty(batch, 'partition', trapped(traps, 'batch.partition', 3))
    Object.defineProperty(
      batch,
      'messages',
      trapped(traps, 'batch.messages', [this.message(1), this.message(2)])
    )
    return { batch }
  }

  /**
   * @returns {{ topic: string, messages: object[] }} a `send` record of two produced messages
   */
  sendRecord() {
    const record = { topic: 't' }
    Object.defineProperty(
      record,
      'messages',
      trapped(this.traps, 'record.messages', [
        this.message(1, { producer: true }),
        this.message(2, { producer: true }),
      ])
    )
    return record
  }

  /**
   * @returns {{ topicMessages: object[] }} a `sendBatch` record of one topic with two messages
   */
  sendBatchRecord() {
    const { traps } = this
    const topicMessage = { topic: 't' }
    Object.defineProperty(
      topicMessage,
      'messages',
      trapped(traps, 'topicMessage.messages', [
        this.message(1, { producer: true }),
        this.message(2, { producer: true }),
      ])
    )
    const record = {}
    Object.defineProperty(
      record,
      'topicMessages',
      trapped(traps, 'record.topicMessages', [topicMessage])
    )
    return record
  }

  /**
   * @param {Record<string, Function>} originals the transaction's methods, by name
   * @returns {object} a transaction whose methods are read and written through trapped accessors
   */
  transaction(originals) {
    const { traps } = this
    const transaction = {}
    for (const [name, original] of Object.entries(originals)) {
      let current = original
      Object.defineProperty(transaction, name, {
        enumerable: true,
        configurable: true,
        get() {
          traps.hit(`transaction.${name}.read`)
          return current
        },
        set(value) {
          traps.hit(`transaction.${name}.write`)
          current = value
        },
      })
    }
    transaction.sendOffsets = async () => {}
    return transaction
  }

  /**
   * @param {Record<string, Function>} [methods] members that replace the stand-in's own
   * @returns {{ listeners: Function[] } & object} a producer-and-consumer stand-in with the
   *   `REQUEST` event and a trapped `on`
   */
  client(methods = {}) {
    const { traps } = this
    const listeners = []
    return {
      listeners,
      events: { REQUEST: 'request' },
      on(_event, listener) {
        traps.hit('client.on')
        listeners.push(listener)
      },
      send: async () => {},
      sendBatch: async () => {},
      transaction: async () => {},
      run: async () => {},
      ...methods,
    }
  }

  /** Makes the diagnostic logger record what it is given, and throw when asked. */
  installDiagnostics() {
    const logger = {
      error: (...args) => {
        this.logged.push(args)
        if (this.loggerThrows) throw this.diagnosticError
        if (this.hostileError) args.forEach((arg) => String(arg?.message))
      },
    }
    diag.setLogger(logger, { logLevel: DiagLogLevel.ERROR, suppressOverrideMessage: true })
    this.restorers.push(() => diag.disable())
  }

  /** Registers the context manager and traps `trace.setSpan`. */
  installContext() {
    context.disable()
    context.setGlobalContextManager(new TestContextManager(this))
    const original = trace.setSpan
    trace.setSpan = (ctx, span) => {
      this.traps.hit('trace.setSpan')
      return original(ctx, span)
    }
    this.restorers.push(() => {
      trace.setSpan = original
      context.disable()
    })
  }

  /** Registers a propagator that writes one header and extracts a context carrying a span. */
  installPropagation() {
    let injected = 0
    propagation.disable()
    propagation.setGlobalPropagator({
      inject: (_ctx, carrier, setter) => {
        this.traps.hit('propagation.inject')
        setter.set(carrier, 'traceparent', `injected-${(injected += 1)}`)
      },
      extract: () => {
        this.traps.hit('propagation.extract')
        const linked = {
          spanContext: () => {
            this.traps.hit('span.spanContext:link')
            return { traceId: 'c'.repeat(32), spanId: 'd'.repeat(16), traceFlags: 1 }
          },
        }
        const extracted = new ExtractedContext(this, setSpan(ROOT_CONTEXT, linked))
        this.extracted.push(extracted)
        return extracted
      },
      fields: () => ['traceparent'],
    })
    this.restorers.push(() => propagation.disable())
  }

  /** Gives the instrumentation the fake tracer and meter and the trapped internals. */
  installInstrumentation() {
    const { instrumentation, traps } = this
    const wrap = instrumentation._wrap
    const unwrap = instrumentation._unwrap
    instrumentation._wrap = (target, name, wrapper) => {
      traps.hit(`wrap:${name}`)
      return wrap(target, name, wrapper)
    }
    instrumentation._unwrap = (target, name) => {
      traps.hit(`unwrap:${name}`)
      return unwrap(target, name)
    }
    const getConfig = instrumentation.getConfig.bind(instrumentation)
    instrumentation.getConfig = () => {
      traps.hit('config.get')
      return getConfig()
    }
    for (const [property, point] of [
      ['_clientDuration', 'client'],
      ['_sentMessages', 'sent'],
      ['_consumedMessages', 'consumed'],
      ['_processDuration', 'process'],
    ]) {
      let current = instrumentation[property]
      Object.defineProperty(instrumentation, property, {
        configurable: true,
        get() {
          traps.hit(`instrument.${point}`)
          return current
        },
        set(value) {
          current = value
        },
      })
    }
    instrumentation.setTracerProvider({ getTracer: () => this.tracer() })
    if (this.fault?.atConstruction) traps.arm(this.fault.arm)
    try {
      instrumentation.setMeterProvider({ getMeter: () => this.meter() })
    } catch (error) {
      this.setupError = error
    }
    this.restorers.push(() => instrumentation.disable())
  }

  /** @returns {object} a tracer whose spans are kept in {@link Harness#spans} */
  tracer() {
    return {
      startSpan: (name, options, ctx) => {
        this.traps.hit(`startSpan:${name.split(' ')[0]}`)
        const span = new SpySpan(this, name, options)
        this.spans.push(span)
        this.startContexts.push(ctx)
        return span
      },
    }
  }

  /** @returns {object} a meter whose instruments fail at armed points and record what they get */
  meter() {
    const { traps, measurements, created } = this
    return {
      createHistogram: (name) => {
        const point = name === METRIC_MESSAGING_CLIENT_OPERATION_DURATION ? 'client' : 'process'
        traps.hit(`createHistogram:${point}`)
        created.push(`histogram:${point}`)
        return {
          record(value, attributes) {
            traps.hit(`record:${point}`)
            measurements.push({ point, value, attributes })
          },
        }
      },
      createCounter: (name) => {
        const point = name === METRIC_MESSAGING_CLIENT_SENT_MESSAGES ? 'sent' : 'consumed'
        traps.hit(`createCounter:${point}`)
        created.push(`counter:${point}`)
        return {
          add(value, attributes) {
            traps.hit(`add:${point}`)
            measurements.push({ point, value, attributes })
          },
        }
      },
    }
  }

  /**
   * Runs the operation's own preparation (not part of the measured call), unarmed.
   * @returns {Promise<void>}
   */
  async setup() {
    await this.parts.setup?.()
  }

  /**
   * Arms the fault; a fault armed at construction stays armed.
   * @returns {void}
   */
  arm() {
    if (!this.fault || this.fault.atConstruction) return
    this.traps.arm(this.fault.arm)
  }

  /**
   * Commits or aborts a transaction the operation left open, so its span ends; it has its own call
   * counter and cannot hide a second invocation of the operation under test.
   * @returns {Promise<void>}
   */
  async settleTransaction() {
    await this.parts.settle?.()
  }

  /** Disarms every fault and undoes every global the harness installed. */
  restore() {
    this.traps.arm({})
    for (const restorer of this.restorers.reverse()) restorer()
    this.restorers = []
  }
}

/**
 * @param {ConstructorParameters<typeof Harness>[0]} options
 * @returns {Harness} one instrumented stand-in operation with its observers ready to be armed
 */
const createHarness = (options) => new Harness(options)

/**
 * @param {Harness} h
 * @param {string} method the transaction method that settles as the harness outcome says; the
 *   others resolve
 * @returns {{ originals: Record<string, Function>, object: object }} the stand-in transaction and
 *   its counted methods
 */
const buildTransactionParts = (h, method) => {
  const originals = {}
  for (const name of ['send', 'sendBatch', 'commit', 'abort']) {
    const impl =
      name === method ? () => h.outcomeResult() : () => Promise.resolve({ settled: name })
    originals[name] = h.counted(impl)
  }
  const object = h.transaction(originals)
  return { originals, object }
}

/**
 * Builds the transaction from the patched `producer.transaction`, which decorates it, with an
 * unarmed original of its own.
 * @param {Harness} h
 * @param {object} object the stand-in transaction
 * @returns {Promise<object>} the decorated transaction
 */
const openTransaction = (h, object) => {
  const open = h.counted(() => Promise.resolve(object))
  const wrapped = h.instrumentation._getProducerTransactionPatch()(open)
  return wrapped.call({ role: 'producer' })
}

/**
 * Builds the parts of a transactional operation: the decorated transaction opened unarmed in
 * `setup`, the operation under test called on it, and a settle that aborts it when it is open.
 * @param {string} method `send`, `sendBatch`, `commit` or `abort`
 * @returns {(h: Harness) => object}
 */
const transactionMethod = (method) => (h) => {
  const { originals, object } = buildTransactionParts(h, method)
  const args =
    method === 'send' ? [h.sendRecord()] : method === 'sendBatch' ? [h.sendBatchRecord()] : []
  const closes = method === 'commit' || method === 'abort'
  return {
    value: closes ? Object.freeze({ closed: method }) : Object.freeze({ sent: method }),
    original: originals[method],
    receiver: object,
    args,
    setup: () => openTransaction(h, object),
    invoke: () => object[method](...args),
    settle: closes ? undefined : () => object.abort(),
    transaction: object,
    originals,
  }
}

/** The operations the matrix runs; `throws` says how a synchronous throw of the original shows. */
const OPERATIONS = {
  request: {
    throws: 'sync',
    result: 'value',
    spans: 0,
    points: ['client'],
    contexts: 0,
    build(h) {
      const operation = h.counted(() => {
        const event = { payload: {} }
        Object.defineProperty(
          event.payload,
          'broker',
          trapped(h.traps, 'event.broker', 'host:9092')
        )
        Object.assign(event.payload, { duration: 250, apiName: 'Produce' })
        for (const listener of client.listeners) listener(event)
        return h.syncResult()
      })
      const client = h.client({ operation })
      const producer = h.instrumentation
        ._getProducerPatch()(() => client)
        .call({})
      const args = [{ requestId: 1 }]
      return {
        value: Object.freeze({ result: 'request' }),
        original: operation,
        receiver: producer,
        args,
        invoke: () => producer.operation(...args),
      }
    },
  },
  producerFactory: {
    throws: 'sync',
    result: 'value',
    spans: 0,
    points: [],
    contexts: 0,
    observed(h) {
      if (h.outcome !== 'resolve') return
      for (const method of ['send', 'sendBatch', 'transaction']) {
        assert.ok(isWrapped(h.value[method]), `${method} was not wrapped`)
      }
      assert.strictEqual(h.value.listeners.length, 1)
    },
    build(h) {
      const producer = h.client()
      const original = h.counted(() => h.syncResult())
      const wrapped = h.instrumentation._getProducerPatch()(original)
      const kafka = { role: 'kafka' }
      const args = [{ clientConfig: true }]
      return {
        value: producer,
        original,
        receiver: kafka,
        args,
        invoke: () => wrapped.apply(kafka, args),
      }
    },
  },
  consumerFactory: {
    throws: 'sync',
    result: 'value',
    spans: 0,
    points: [],
    contexts: 0,
    observed(h) {
      if (h.outcome !== 'resolve') return
      assert.ok(isWrapped(h.value.run), 'run was not wrapped')
      assert.strictEqual(h.value.listeners.length, 1)
    },
    build(h) {
      const consumer = h.client()
      const original = h.counted(() => h.syncResult())
      const wrapped = h.instrumentation._getConsumerPatch()(original)
      const kafka = { role: 'kafka' }
      const args = [{ clientConfig: true }]
      return {
        value: consumer,
        original,
        receiver: kafka,
        args,
        invoke: () => wrapped.apply(kafka, args),
      }
    },
  },
  run: {
    throws: 'sync',
    result: 'promise',
    spans: 0,
    points: [],
    contexts: 0,
    observed(h) {
      for (const handler of ['eachMessage', 'eachBatch']) {
        assert.ok(isWrapped(h.args[0][handler]), `${handler} was not wrapped`)
      }
    },
    build(h) {
      const original = h.counted(() => h.outcomeResult())
      const wrapped = h.instrumentation._getConsumerRunPatch()(original)
      const consumer = { role: 'consumer' }
      const config = {}
      for (const name of ['eachMessage', 'eachBatch']) {
        let handler = async () => {}
        Object.defineProperty(config, name, {
          enumerable: true,
          configurable: true,
          get() {
            h.traps.hit(`config.${name}.read`)
            return handler
          },
          set(value) {
            handler = value
          },
        })
      }
      return {
        value: Object.freeze({ result: 'run' }),
        original,
        receiver: consumer,
        args: [config],
        config,
        invoke: () => wrapped.apply(consumer, [config]),
      }
    },
  },
  eachMessage: {
    throws: 'rejection',
    result: 'promise',
    spans: 1,
    points: ['process', 'consumed'],
    contexts: 1,
    build(h) {
      const original = h.counted(() => h.outcomeResult())
      const wrapped = h.instrumentation._getConsumerEachMessagePatch()(original)
      const receiver = { role: 'consumer' }
      const args = [h.messagePayload()]
      return {
        value: Object.freeze({ result: 'eachMessage' }),
        original,
        receiver,
        args,
        invoke: () => wrapped.apply(receiver, args),
      }
    },
  },
  eachBatch: {
    throws: 'rejection',
    result: 'promise',
    spans: 3,
    points: ['consumed', 'process', 'process'],
    contexts: 1,
    build(h) {
      const original = h.counted(() => h.outcomeResult())
      const wrapped = h.instrumentation._getConsumerEachBatchPatch()(original)
      const receiver = { role: 'consumer' }
      const args = [h.batchPayload()]
      return {
        value: Object.freeze({ result: 'eachBatch' }),
        original,
        receiver,
        args,
        invoke: () => wrapped.apply(receiver, args),
      }
    },
  },
  send: {
    throws: 'rejection',
    result: 'promise',
    spans: 2,
    points: ['sent', 'sent'],
    contexts: 0,
    build(h) {
      const original = h.counted(() => h.outcomeResult())
      const wrapped = h.instrumentation._getSendPatch()(original)
      const receiver = { role: 'producer' }
      const args = [h.sendRecord()]
      return {
        value: Object.freeze({ result: 'send' }),
        original,
        receiver,
        args,
        invoke: () => wrapped.apply(receiver, args),
      }
    },
  },
  sendBatch: {
    throws: 'rejection',
    result: 'promise',
    spans: 2,
    points: ['sent', 'sent'],
    contexts: 0,
    build(h) {
      const original = h.counted(() => h.outcomeResult())
      const wrapped = h.instrumentation._getSendBatchPatch()(original)
      const receiver = { role: 'producer' }
      const args = [h.sendBatchRecord()]
      return {
        value: Object.freeze({ result: 'sendBatch' }),
        original,
        receiver,
        args,
        invoke: () => wrapped.apply(receiver, args),
      }
    },
  },
  transaction: {
    throws: 'rejection',
    result: 'promise',
    spans: 1,
    points: [],
    contexts: 0,
    build(h) {
      const { object } = buildTransactionParts(h, 'none')
      const original = h.counted(() => h.outcomeResult())
      const wrapped = h.instrumentation._getProducerTransactionPatch()(original)
      const receiver = { role: 'producer' }
      const args = [{ transactionOptions: true }]
      return {
        value: object,
        original,
        receiver,
        args,
        invoke: () => wrapped.apply(receiver, args),
        settle: () => (h.outcome.endsWith('resolve') ? object.abort() : undefined),
        transaction: object,
      }
    },
  },
  txSend: {
    throws: 'rejection',
    result: 'promise',
    spans: 3,
    points: ['sent', 'sent'],
    contexts: 1,
    build: transactionMethod('send'),
  },
  txSendBatch: {
    throws: 'rejection',
    result: 'promise',
    spans: 3,
    points: ['sent', 'sent'],
    contexts: 1,
    build: transactionMethod('sendBatch'),
  },
  txCommit: {
    throws: 'rejection',
    result: 'promise',
    spans: 1,
    points: [],
    contexts: 0,
    build: transactionMethod('commit'),
  },
  txAbort: {
    throws: 'rejection',
    result: 'promise',
    spans: 1,
    points: [],
    contexts: 0,
    build: transactionMethod('abort'),
  },
}

/**
 * @param {Harness} h
 * @param {unknown} error what a check threw
 * @returns {unknown} a plain assertion error when `error` is one of the harness's own failures,
 *   which the test runner cannot report safely (the failure may be hostile), else `error`
 */
const describeEscape = (h, error) => {
  if (error === h.observerError) {
    return new assert.AssertionError({ message: 'an observer failure reached the application' })
  }
  if (error === h.diagnosticError) {
    return new assert.AssertionError({
      message: 'a diagnostic logger failure reached the application',
    })
  }
  return error
}

/**
 * Runs the harness's operation once and asserts the application's view of it: the original ran
 * once with the same receiver and first argument, the result or rejection is the original's, no
 * secondary rejection escaped, every span had one cleanup attempt, and the diagnostic logger saw
 * the observer failure. The harness is always restored.
 * @param {Harness} h
 * @param {object} [options]
 * @param {'ended' | 'attempted'} [options.spans] `attempted` accepts a span whose `end` threw
 *   before it ended
 * @param {(h: Harness) => Promise<void> | void} [options.check] extra assertions after settling
 * @returns {Promise<void>} rejects on the first failed assertion
 */
const assertContained = async (h, { spans = 'ended', check } = {}) => {
  const secondary = []
  const recordUnhandled = (reason) => secondary.push(reason)
  const before = process.listenerCount('unhandledRejection')
  process.prependListener('unhandledRejection', recordUnhandled)
  try {
    assert.strictEqual(h.setupError, undefined, 'the observers failed while being set up')
    await h.setup()
    h.arm()
    await compareOutcome(h)
    assert.strictEqual(h.original.calls.length, 1)
    assert.strictEqual(h.original.calls[0].receiver, h.receiver)
    assert.strictEqual(h.original.calls[0].args[0], h.args[0])
    await h.settleTransaction()
    await new Promise((resolve) => setImmediate(resolve))
    await new Promise((resolve) => setImmediate(resolve))
    assert.deepStrictEqual(secondary, [])
    assert.strictEqual(h.original.calls.length, 1)
    for (const span of h.spans) {
      assert.strictEqual(span.endAttempts, 1, `${span.name} was not cleaned up exactly once`)
      if (spans === 'ended') assert.strictEqual(span.ended, true, `${span.name} did not end`)
    }
    assertDiagnostics(h)
    await check?.(h)
  } catch (error) {
    throw describeEscape(h, error)
  } finally {
    process.removeListener('unhandledRejection', recordUnhandled)
    h.restore()
  }
  assert.strictEqual(process.listenerCount('unhandledRejection'), before)
}

/**
 * Invokes the operation and asserts it settles exactly as the original did.
 * @param {Harness} h
 * @returns {Promise<void>}
 */
const compareOutcome = async (h) => {
  const throwing = h.outcome === 'throw' || h.outcome === 'throw-undefined'
  const isReason = (reason) => reason === h.reason
  if (h.spec.throws === 'sync' && throwing) {
    assert.throws(() => h.invoke(), isReason)
    return
  }
  let pending
  assert.doesNotThrow(() => {
    pending = h.invoke()
  })
  if (h.spec.result === 'value') {
    assert.strictEqual(pending, h.value)
    return
  }
  assert.strictEqual(h.original.calls.length, 1, 'the original did not run synchronously once')
  h.release?.()
  if (h.outcome === 'resolve' || h.outcome === 'pending-resolve') {
    assert.strictEqual(await pending, h.value)
  } else {
    await assert.rejects(Promise.resolve(pending), isReason)
  }
}

/**
 * Asserts the fault was reached, and what the diagnostic logger was told.
 * @param {Harness} h
 * @returns {void}
 */
const assertDiagnostics = (h) => {
  const allowed = [h.observerError, h.diagnosticError]
  for (const entry of h.logged) {
    assert.ok(
      entry.some((part) => allowed.includes(part)),
      `an unexpected diagnostic was logged: ${entry.map(String).join(' ')}`
    )
  }
  assert.ok(
    !h.logged.some((entry) => entry.includes(h.diagnosticError)),
    'the diagnostic logger failure was itself logged'
  )
  if (!h.fault) {
    assert.strictEqual(h.traps.fired, 0)
    assert.ok(!h.logged.some((entry) => entry.includes(h.observerError)))
    return
  }
  assert.ok(h.traps.fired >= 1, 'the injected fault was never reached')
  if (h.fault.logs !== false) {
    assert.ok(
      h.logged.some((entry) => entry.includes(h.observerError)),
      'the observer failure was not logged'
    )
  }
}

module.exports = { OPERATIONS, assertContained, createHarness }
