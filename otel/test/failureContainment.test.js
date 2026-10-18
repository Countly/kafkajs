/*
 * A telemetry component that throws must not change what the application sees: the wrapped
 * operation runs once with its own receiver and arguments, settles with its own value or reason,
 * leaves no secondary rejection, and every span gets its cleanup. Each case below injects one
 * failure at one observation point, crossed with how the original settles and whether the
 * diagnostic logger throws as well, and has a control that runs the same operation unfaulted.
 */

const { describe, it, after } = require('node:test')
const assert = require('node:assert/strict')
const fs = require('node:fs')
const os = require('node:os')
const path = require('node:path')
const { createRequire } = require('node:module')
const { ROOT_CONTEXT, trace } = require('@opentelemetry/api')
const { isWrapped } = require('@opentelemetry/instrumentation')
const { ATTR_ERROR_TYPE } = require('@opentelemetry/semantic-conventions')
const instrumentationExports = require('../src/instrumentation')
const { OPERATIONS, assertContained, createHarness } = require('./failureContainmentHelpers')

const ASYNC = ['resolve', 'reject', 'throw']
const SYNC = ['resolve', 'throw']
const FAILING = ['reject', 'throw']
const TIMEOUT = { timeout: 10000 }

/** @param {string} operation @returns {string[]} the span kinds the operation creates */
const spanKinds = (operation) =>
  ({
    send: ['send'],
    sendBatch: ['send'],
    eachMessage: ['process'],
    eachBatch: ['poll', 'process'],
    txCommit: ['transaction'],
  })[operation]

/**
 * @param {string} prefix a trap prefix such as `span.setStatus`
 * @param {string[]} kinds
 * @returns {Record<string, true>} every kind's trap armed
 */
const armKinds = (prefix, kinds) =>
  Object.fromEntries(kinds.map((kind) => [`${prefix}:${kind}`, true]))

/**
 * @param {import('./failureContainmentHelpers').Harness} h
 * @returns {object[]} the messages the application passed to a producer operation
 */
const sentMessages = (h) => {
  const record = h.args[0]
  return h.operation === 'sendBatch' ? record.topicMessages[0].messages : record.messages
}

/**
 * Asserts the unfaulted control ran the operation's whole observation.
 * @param {import('./failureContainmentHelpers').Harness} h
 */
const assertObserved = (h) => {
  const spec = OPERATIONS[h.operation]
  assert.strictEqual(h.setupError, undefined)
  assert.strictEqual(h.spans.length, spec.spans)
  assert.deepStrictEqual(h.points, spec.points)
  assert.strictEqual(h.entered.length, spec.contexts)
  spec.observed?.(h)
}

/**
 * Declares one family of cases: for each case and each way the original can settle, a cell with the
 * diagnostic logger recording and one with it throwing, and a control per operation and outcome.
 * @param {string} family
 * @param {Array<{ name: string, operation: string, outcomes?: string[], fault: object,
 *   check?: Function, spans?: string }>} cases
 */
const matrix = (family, cases) => {
  describe(family, () => {
    const controls = new Set(
      cases.flatMap(({ operation, outcomes = ASYNC }) =>
        outcomes.map((outcome) => `${operation} / ${outcome}`)
      )
    )
    for (const control of controls) {
      const [operation, outcome] = control.split(' / ')
      it(`control without a fault / ${control}`, TIMEOUT, () =>
        assertContained(createHarness({ operation, outcome }), { check: assertObserved })
      )
    }
    for (const { name, operation, outcomes = ASYNC, fault, check, spans } of cases) {
      for (const outcome of outcomes) {
        for (const loggerThrows of [false, true]) {
          const diagnostic = loggerThrows ? 'logger throws' : 'logger records'
          it(`${name} / ${operation} / ${outcome} / ${diagnostic}`, TIMEOUT, () =>
            assertContained(createHarness({ operation, outcome, fault, loggerThrows }), {
              check,
              spans,
            })
          )
        }
      }
    }
  })
}

/**
 * @param {'producer' | 'consumer'} name
 * @returns {(h: import('./failureContainmentHelpers').Harness) => void} configures a span hook that
 *   throws at the armed `<name>Hook` point
 */
const hook = (name) => (h) =>
  h.instrumentation.setConfig({
    [`${name}Hook`]: () => h.traps.hit(`${name}Hook`),
  })

matrix('request metric failure preserves original outcome', [
  {
    name: 'the histogram throws',
    operation: 'request',
    outcomes: SYNC,
    fault: { arm: { 'record:client': true } },
    check: (h) => assert.strictEqual(h.traps.reached['record:client'], 1),
  },
  {
    name: 'the event payload getter throws',
    operation: 'request',
    outcomes: SYNC,
    fault: { arm: { 'event.broker': true } },
    check: (h) => assert.strictEqual(h.measurements.length, 0),
  },
  {
    name: 'the instrument cannot be read',
    operation: 'request',
    outcomes: SYNC,
    fault: { arm: { 'instrument.client': true } },
    check: (h) => assert.strictEqual(h.measurements.length, 0),
  },
])

matrix('instrument creation failure preserves original outcome', [
  {
    name: 'the client duration histogram cannot be created',
    operation: 'request',
    outcomes: SYNC,
    fault: { atConstruction: true, arm: { 'createHistogram:client': true } },
    check: (h) => {
      assert.deepStrictEqual(h.created, ['counter:sent', 'counter:consumed', 'histogram:process'])
      assert.strictEqual(h.measurements.length, 0)
    },
  },
  {
    name: 'the sent messages counter cannot be created',
    operation: 'send',
    fault: { atConstruction: true, arm: { 'createCounter:sent': true } },
    check: (h) => {
      assert.deepStrictEqual(h.created, [
        'histogram:client',
        'counter:consumed',
        'histogram:process',
      ])
      assert.strictEqual(h.measurements.length, 0)
    },
  },
  {
    name: 'the consumed messages counter cannot be created',
    operation: 'eachMessage',
    fault: { atConstruction: true, arm: { 'createCounter:consumed': true } },
    check: (h) => {
      assert.deepStrictEqual(h.created, ['histogram:client', 'counter:sent', 'histogram:process'])
      assert.deepStrictEqual(h.points, ['process'])
    },
  },
  {
    name: 'the process duration histogram cannot be created',
    operation: 'eachMessage',
    fault: { atConstruction: true, arm: { 'createHistogram:process': true } },
    check: (h) => {
      assert.deepStrictEqual(h.created, ['histogram:client', 'counter:sent', 'counter:consumed'])
      assert.deepStrictEqual(h.points, ['consumed'])
    },
  },
])

matrix('factory decoration failure preserves original outcome', [
  ...['send', 'sendBatch', 'transaction'].map((method) => ({
    name: `wrapping the producer's ${method} fails`,
    operation: 'producerFactory',
    outcomes: ['resolve'],
    fault: { arm: { [`wrap:${method}`]: true } },
    check: (h) => {
      for (const other of ['send', 'sendBatch', 'transaction']) {
        assert.strictEqual(isWrapped(h.value[other]), other !== method)
      }
      assert.strictEqual(h.value.listeners.length, 1)
    },
  })),
  {
    name: 'registering the request listener fails',
    operation: 'producerFactory',
    outcomes: ['resolve'],
    fault: { arm: { 'client.on': true } },
    check: (h) => assert.ok(isWrapped(h.value.send) && isWrapped(h.value.transaction)),
  },
  {
    name: "wrapping the consumer's run fails",
    operation: 'consumerFactory',
    outcomes: ['resolve'],
    fault: { arm: { 'wrap:run': true } },
    check: (h) => {
      assert.strictEqual(isWrapped(h.value.run), false)
      assert.strictEqual(h.value.listeners.length, 1)
    },
  },
  {
    name: 'registering the consumer request listener fails',
    operation: 'consumerFactory',
    outcomes: ['resolve'],
    fault: { arm: { 'client.on': true } },
    check: (h) => assert.ok(isWrapped(h.value.run)),
  },
  ...['eachMessage', 'eachBatch'].flatMap((handler) => [
    {
      name: `reading the config's ${handler} fails`,
      operation: 'run',
      fault: { arm: { [`config.${handler}.read`]: true } },
      check: (h) => {
        const other = handler === 'eachMessage' ? 'eachBatch' : 'eachMessage'
        assert.strictEqual(isWrapped(h.args[0][other]), true)
      },
    },
    {
      name: `wrapping the config's ${handler} fails`,
      operation: 'run',
      fault: { arm: { [`wrap:${handler}`]: true } },
      check: (h) => {
        const other = handler === 'eachMessage' ? 'eachBatch' : 'eachMessage'
        assert.strictEqual(isWrapped(h.args[0][other]), true)
      },
    },
  ]),
])

describe('factory decoration failure preserves original outcome: module hooks', () => {
  /** @returns {{ Kafka: Function, exports: object }} a module with an unwrapped client class */
  const standInModule = () => {
    class Kafka {
      producer() {
        return {}
      }

      consumer() {
        return {}
      }
    }
    return { Kafka, exports: { Kafka } }
  }

  for (const loggerThrows of [false, true]) {
    const diagnostic = loggerThrows ? 'logger throws' : 'logger records'
    for (const factory of ['producer', 'consumer']) {
      const other = factory === 'producer' ? 'consumer' : 'producer'
      it(
        `a failed ${factory} wrap returns the exports and still wraps the ${other} / ${diagnostic}`,
        TIMEOUT,
        () => {
          const h = createHarness({
            operation: 'request',
            fault: { arm: { [`wrap:${factory}`]: true } },
            loggerThrows,
          })
          try {
            const { Kafka, exports } = standInModule()
            const [definition] = h.instrumentation.getModuleDefinitions()
            h.arm()
            assert.strictEqual(definition.patch(exports), exports)
            assert.strictEqual(isWrapped(Kafka.prototype[factory]), false)
            assert.strictEqual(isWrapped(Kafka.prototype[other]), true)
            assert.ok(h.traps.fired >= 1)
          } finally {
            h.restore()
          }
        }
      )
    }

    it(`an unreadable Kafka export returns the exports untouched / ${diagnostic}`, TIMEOUT, () => {
      const h = createHarness({ operation: 'request', loggerThrows })
      try {
        const [definition] = h.instrumentation.getModuleDefinitions()
        const exports = {}
        Object.defineProperty(exports, 'Kafka', {
          get() {
            throw h.observerError
          },
        })
        assert.strictEqual(definition.patch(exports), exports)
        assert.strictEqual(definition.unpatch(exports), undefined)
      } finally {
        h.restore()
      }
    })

    it(
      `a failed unwrap returns without throwing and leaves the other factory unwrapped / ${diagnostic}`,
      TIMEOUT,
      () => {
        const h = createHarness({
          operation: 'request',
          fault: { arm: { 'unwrap:producer': true } },
          loggerThrows,
        })
        try {
          const { Kafka, exports } = standInModule()
          const [definition] = h.instrumentation.getModuleDefinitions()
          definition.patch(exports)
          h.arm()
          assert.doesNotThrow(() => definition.unpatch(exports))
          assert.strictEqual(isWrapped(Kafka.prototype.consumer), false)
          assert.ok(h.traps.fired >= 1)
        } finally {
          h.restore()
        }
      }
    )
  }
})

describe('factory decoration failure preserves original outcome: a handler wrapped before', () => {
  for (const loggerThrows of [false, true]) {
    const diagnostic = loggerThrows ? 'logger throws' : 'logger records'

    it(
      `running the same config twice replaces the wrapper instead of stacking it / ${diagnostic}`,
      TIMEOUT,
      async () => {
        const h = createHarness({ operation: 'run', loggerThrows })
        try {
          await h.invoke()
          await h.invoke()
          await h.args[0].eachMessage(h.messagePayload())
          assert.strictEqual(h.original.calls.length, 2)
          assert.strictEqual(h.spans.length, 1)
        } finally {
          h.restore()
        }
      }
    )

    it(
      `a wrapper that cannot be removed stays and run still runs / ${diagnostic}`,
      TIMEOUT,
      async () => {
        const h = createHarness({
          operation: 'run',
          fault: { arm: { 'unwrap:eachMessage': true } },
          loggerThrows,
        })
        try {
          await h.invoke()
          h.arm()
          await h.invoke()
          await h.args[0].eachMessage(h.messagePayload())
          assert.strictEqual(h.original.calls.length, 2)
          assert.strictEqual(h.spans.length, 1)
          assert.ok(h.traps.fired >= 1)
        } finally {
          h.restore()
        }
      }
    )
  }
})

matrix('message preparation failure preserves original outcome', [
  ...[
    ['the propagator throws on extract', { 'propagation.extract': true }],
    ['the headers getter throws', { 'message.headers:1': true }],
  ].map(([name, arm]) => ({
    name,
    operation: 'eachMessage',
    fault: { arm },
    check: (h) => {
      assert.strictEqual(h.startContexts[0], ROOT_CONTEXT)
      assert.strictEqual(trace.getSpan(h.entered[0]), h.spans[0])
    },
  })),
  ...[
    ['the tracer throws on startSpan', { 'startSpan:process': true }],
    ['the message key getter throws', { 'message.key:1': true }],
    ['the message value getter throws', { 'message.value:1': true }],
    ['the message offset getter throws', { 'message.offset:1': true }],
  ].map(([name, arm]) => ({
    name,
    operation: 'eachMessage',
    fault: { arm },
    check: (h) => {
      assert.strictEqual(h.spans.length, 0)
      assert.strictEqual(h.entered[0], h.extracted[0])
      assert.deepStrictEqual(h.points, ['process', 'consumed'])
    },
  })),
  {
    name: 'the payload getters throw',
    operation: 'eachMessage',
    fault: { arm: { 'payload.topic': true, 'payload.partition': true } },
    check: (h) => {
      assert.strictEqual(h.spans.length, 0)
      assert.strictEqual(h.measurements.length, 0)
    },
  },
  {
    name: 'the instruments cannot be read',
    operation: 'eachMessage',
    fault: { arm: { 'instrument.process': true } },
    check: (h) => {
      assert.strictEqual(h.spans.length, 1)
      assert.strictEqual(h.measurements.length, 0)
    },
  },
  {
    name: 'trace.setSpan throws',
    operation: 'eachMessage',
    fault: { arm: { 'trace.setSpan': true } },
    check: (h) => {
      assert.strictEqual(h.entered[0], h.extracted[0])
      assert.strictEqual(h.spans.length, 1)
      assert.deepStrictEqual(h.points, ['process', 'consumed'])
    },
  },
  {
    name: 'the consumer hook throws',
    operation: 'eachMessage',
    fault: { arm: { consumerHook: true }, configure: hook('consumer') },
    check: (h) => assert.strictEqual(h.spans.length, 1),
  },
  {
    name: 'the instrumentation config cannot be read',
    operation: 'eachMessage',
    fault: { arm: { 'config.get': true } },
    check: (h) => assert.strictEqual(h.spans.length, 1),
  },
])

matrix('batch preparation failure preserves original outcome', [
  {
    name: 'the tracer throws on the receiving span',
    operation: 'eachBatch',
    fault: { arm: { 'startSpan:poll': true } },
    check: (h) => {
      assert.deepStrictEqual(
        h.spans.map(({ kind }) => kind),
        ['process', 'process']
      )
      assert.deepStrictEqual(h.points, ['consumed', 'process', 'process'])
    },
  },
  {
    name: 'the batch topic getter throws',
    operation: 'eachBatch',
    fault: { arm: { 'batch.topic': true } },
    check: (h) => {
      assert.strictEqual(h.spans.length, 0)
      assert.strictEqual(h.measurements.length, 0)
    },
  },
  {
    name: 'the batch messages getter throws',
    operation: 'eachBatch',
    fault: { arm: { 'batch.messages': true } },
    check: (h) => {
      assert.strictEqual(h.spans.length, 0)
      assert.strictEqual(h.measurements.length, 0)
    },
  },
  {
    name: 'the active context cannot be read',
    operation: 'eachBatch',
    fault: { arm: { 'context.active': true } },
    check: (h) => assert.strictEqual(trace.getSpan(h.entered[0]), h.spans[0]),
  },
  {
    name: 'trace.setSpan throws',
    operation: 'eachBatch',
    fault: { arm: { 'trace.setSpan': true } },
    check: (h) => assert.strictEqual(h.entered[0], ROOT_CONTEXT),
  },
  {
    name: 'the consumed counter cannot be prepared',
    operation: 'eachBatch',
    fault: { arm: { 'instrument.consumed': true } },
    check: (h) => {
      assert.strictEqual(h.spans.length, 3)
      assert.deepStrictEqual(h.points, ['process', 'process'])
    },
  },
  {
    name: 'the propagator throws on extract',
    operation: 'eachBatch',
    fault: { arm: { 'propagation.extract': true } },
    check: (h) => {
      assert.strictEqual(h.spans.length, 3)
      assert.deepStrictEqual(
        h.spans.slice(1).map(({ links }) => links.length),
        [0, 0]
      )
    },
  },
  {
    name: "the first message's headers getter throws",
    operation: 'eachBatch',
    fault: { arm: { 'message.headers:1': true } },
    check: (h) =>
      assert.deepStrictEqual(
        h.spans.slice(1).map(({ links }) => links.length),
        [0, 1]
      ),
  },
  {
    name: 'reading the extracted span throws',
    operation: 'eachBatch',
    fault: { arm: { 'context.getValue': true } },
    check: (h) => {
      assert.strictEqual(h.spans.length, 3)
      assert.deepStrictEqual(
        h.spans.slice(1).map(({ links }) => links.length),
        [0, 0]
      )
    },
  },
  {
    name: "the extracted span's context throws",
    operation: 'eachBatch',
    fault: { arm: { 'span.spanContext:link': true } },
    check: (h) =>
      assert.deepStrictEqual(
        h.spans.slice(1).map(({ links }) => links.length),
        [0, 0]
      ),
  },
  ...[
    ['the tracer throws on the second message', { 'startSpan:process': 2 }],
    ['the second message key getter throws', { 'message.key:2': true }],
  ].map(([name, arm]) => ({
    name,
    operation: 'eachBatch',
    fault: { arm },
    check: (h) => {
      assert.strictEqual(h.spans.length, 2)
      assert.deepStrictEqual(h.points, ['consumed', 'process', 'process'])
    },
  })),
  {
    name: 'the second message duration cannot be prepared',
    operation: 'eachBatch',
    fault: { arm: { 'instrument.process': 2 } },
    check: (h) => {
      assert.strictEqual(h.spans.length, 3)
      assert.deepStrictEqual(h.points, ['consumed', 'process'])
    },
  },
  {
    name: 'the consumer hook throws',
    operation: 'eachBatch',
    fault: { arm: { consumerHook: true }, configure: hook('consumer') },
    check: (h) => assert.strictEqual(h.spans.length, 3),
  },
])

describe('context failure preserves original outcome', () => {
  const MODES = ['before', 'after', 'twice', 'replacement']
  const OUTCOMES = [...ASYNC, 'throw-undefined', 'pending-resolve', 'pending-reject']

  for (const operation of ['eachMessage', 'eachBatch', 'txSend', 'txSendBatch']) {
    for (const mode of MODES) {
      for (const outcome of OUTCOMES) {
        for (const loggerThrows of [false, true]) {
          const diagnostic = loggerThrows ? 'logger throws' : 'logger records'
          it(`${mode} / ${operation} / ${outcome} / ${diagnostic}`, TIMEOUT, () =>
            assertContained(
              createHarness({
                operation,
                outcome,
                loggerThrows,
                fault: {
                  arm: { [`context.with:${mode}`]: true },
                  logs: ['before', 'after'].includes(mode),
                },
              })
            )
          )
        }
      }
    }
  }

  for (const operation of ['eachMessage', 'eachBatch', 'txSend', 'txSendBatch']) {
    for (const outcome of OUTCOMES) {
      it(`control without a fault / ${operation} / ${outcome}`, TIMEOUT, () =>
        assertContained(createHarness({ operation, outcome }), { check: assertObserved })
      )
    }
  }
})

matrix('transaction preparation failure preserves original outcome', [
  {
    name: 'the tracer throws on the transaction span',
    operation: 'transaction',
    fault: { arm: { 'startSpan:transaction': true } },
    check: (h) => assert.strictEqual(h.spans.length, 0),
  },
  ...['send', 'sendBatch', 'commit', 'abort'].flatMap((method) => [
    {
      name: `reading the transaction's ${method} fails`,
      operation: 'transaction',
      outcomes: ['resolve'],
      fault: { arm: { [`transaction.${method}.read`]: 1 } },
    },
    {
      name: `assigning the transaction's ${method} fails`,
      operation: 'transaction',
      outcomes: ['resolve'],
      fault: { arm: { [`transaction.${method}.write`]: true } },
    },
  ]),
])

matrix('transaction send cleanup failure preserves original outcome', [
  ...['txSend', 'txSendBatch'].flatMap((operation) => [
    ...[
      ['the active context cannot be read', { 'context.active': true }],
      ['trace.setSpan throws', { 'trace.setSpan': true }],
    ].map(([name, arm]) => ({
      name,
      operation,
      fault: { arm },
    })),
    ...[
      ['the transaction status cannot be set', { 'span.setStatus:transaction': true }],
      ['the exception cannot be recorded', { 'span.recordException:transaction': true }],
      ['the rejection message getter throws', { 'reason.message': true }],
      [
        'the status and the exception both fail',
        { 'span.setStatus:transaction': true, 'span.recordException:transaction': true },
      ],
    ].map(([name, arm]) => ({
      name,
      operation,
      outcomes: FAILING,
      fault: { arm },
      check: (h) => {
        assert.strictEqual(h.traps.reached['span.setStatus:transaction'], 1)
        assert.strictEqual(h.traps.reached['span.recordException:transaction'], 1)
      },
    })),
  ]),
])

matrix('transaction commit cleanup failure preserves original outcome', [
  {
    name: 'the commit status cannot be set',
    operation: 'txCommit',
    outcomes: ['resolve'],
    fault: { arm: { 'span.setStatus:transaction': true } },
    check: (h) => assert.notStrictEqual(h.value, undefined),
  },
  {
    name: 'ending the span throws after it ended',
    operation: 'txCommit',
    outcomes: ['resolve'],
    fault: { arm: { 'span.end:after:transaction': true } },
  },
  {
    name: 'the span attribute, status and end all throw',
    operation: 'txCommit',
    outcomes: FAILING,
    fault: {
      arm: {
        'span.setAttribute:transaction': true,
        'span.setStatus:transaction': true,
        'span.end:after:transaction': true,
      },
    },
    check: (h) => {
      assert.strictEqual(h.traps.reached['span.setStatus:transaction'], 1)
      assert.strictEqual(h.traps.reached['span.end:after:transaction'], 1)
    },
  },
])

matrix('transaction initialization failure preserves original outcome', [
  ...[
    ['the status cannot be set', { 'span.setStatus:transaction': true }],
    ['the exception cannot be recorded', { 'span.recordException:transaction': true }],
    ['ending the span throws after it ended', { 'span.end:after:transaction': true }],
    ['the rejection message getter throws', { 'reason.message': true }],
    [
      'every observer throws',
      {
        'span.setStatus:transaction': true,
        'span.recordException:transaction': true,
        'span.end:after:transaction': true,
        'reason.message': true,
      },
    ],
  ].map(([name, arm]) => ({
    name,
    operation: 'transaction',
    outcomes: FAILING,
    fault: { arm },
    check: (h) => {
      assert.strictEqual(h.traps.reached['span.setStatus:transaction'], 1)
      assert.strictEqual(h.traps.reached['span.recordException:transaction'], 1)
      assert.strictEqual(h.traps.reached['span.end:after:transaction'], 1)
    },
  })),
])

matrix(
  'producer preparation failure preserves original outcome',
  ['send', 'sendBatch'].flatMap((operation) => [
    ...(operation === 'send'
      ? [['the messages getter throws', { 'record.messages': true }]]
      : [
          ['the topic messages getter throws', { 'record.topicMessages': true }],
          ['the messages of a topic getter throws', { 'topicMessage.messages': true }],
        ]
    ).map(([name, arm]) => ({
      name,
      operation,
      fault: { arm },
      check: (h) => assert.strictEqual(h.spans.length, 0),
    })),
    ...[
      ['the tracer throws on the second message', { 'startSpan:send': 2 }, 1],
      ['the second message key getter throws', { 'message.key:2': true }, 1],
      ['the second message partition getter throws', { 'message.partition:2': true }, 1],
      ['the second message counter cannot be prepared', { 'instrument.sent': 2 }, 2],
      ['the second message header injection fails', { 'propagation.inject': 2 }, 2],
      ['the second message headers getter throws', { 'message.headers:2': true }, 2],
      ['the second message headers cannot be checked', { 'headers.isExtensible:2': true }, 2],
      ['the second message headers cannot be assigned', { 'headers.set:2': true }, 2],
      ['the active context cannot be read', { 'context.active': true }, 1],
      ['trace.setSpan throws', { 'trace.setSpan': true }, 1],
      ['the first message header injection fails', { 'propagation.inject': 1 }, 1],
      [
        'the second message fails and ending a span throws after it ended',
        { 'propagation.inject': 2, 'span.end:after:send': true },
        2,
      ],
    ].map(([name, arm, spans]) => ({
      name,
      operation,
      fault: { arm },
      check: (h) => {
        assert.strictEqual(h.spans.length, spans)
        assert.strictEqual(h.measurements.length, 0)
        if (/injection fails|checked|assigned/.test(name) && !name.startsWith('the first')) {
          assert.deepStrictEqual(Object.keys(sentMessages(h)[1].headers), [])
        }
      },
    })),
    ...[
      ['the producer hook throws', { producerHook: true }, hook('producer')],
      ['the instrumentation config cannot be read', { 'config.get': true }, undefined],
    ].map(([name, arm, configure]) => ({
      name,
      operation,
      fault: { arm, configure },
      check: (h) => {
        assert.strictEqual(h.spans.length, 2)
        assert.deepStrictEqual(h.points, ['sent', 'sent'])
      },
    })),
  ])
)

matrix(
  'rejection descriptor failure preserves original outcome',
  ['send', 'eachMessage', 'txCommit'].flatMap((operation) =>
    [
      ['the message getter throws', { 'reason.message': true }],
      ['the constructor getter throws', { 'reason.constructor': true }],
      ['the constructor name getter throws', { 'reason.constructor.name': true }],
      ['the descriptor trap throws', { 'reason.descriptor': true }],
    ].map(([name, arm]) => ({
      name,
      operation,
      outcomes: FAILING,
      fault: { arm },
      check: (h) => {
        for (const span of h.spans) {
          assert.deepStrictEqual(span.status, { code: 2, message: undefined })
          assert.strictEqual(span.attributes[ATTR_ERROR_TYPE], '_OTHER')
        }
        for (const { attributes } of h.measurements) {
          assert.strictEqual(attributes[ATTR_ERROR_TYPE], '_OTHER')
        }
      },
    }))
  )
)

matrix(
  'settlement observation failure preserves original outcome',
  ['send', 'sendBatch', 'eachMessage', 'eachBatch'].flatMap((operation) => {
    const kinds = spanKinds(operation)
    const metrics = ['send', 'sendBatch'].includes(operation)
      ? { 'add:sent': true }
      : { 'add:consumed': true, 'record:process': true }
    const expectedSpans = OPERATIONS[operation].spans
    return [
      {
        name: 'the meters throw',
        operation,
        fault: { arm: metrics },
        check: (h) => {
          for (const trap of Object.keys(metrics)) assert.ok(h.traps.reached[trap] >= 1)
          assert.strictEqual(h.spans.length, expectedSpans)
          assert.strictEqual(h.measurements.length, 0)
        },
      },
      {
        name: 'the span attribute cannot be set',
        operation,
        outcomes: FAILING,
        fault: { arm: armKinds('span.setAttribute', kinds) },
        check: (h) => {
          const attempted = (prefix) =>
            kinds.reduce((sum, kind) => sum + (h.traps.reached[`${prefix}:${kind}`] ?? 0), 0)
          assert.strictEqual(attempted('span.setAttribute'), expectedSpans)
          assert.strictEqual(attempted('span.setStatus'), expectedSpans)
          assert.deepStrictEqual(h.points, OPERATIONS[operation].points)
        },
      },
      {
        name: 'the span status cannot be set',
        operation,
        outcomes: FAILING,
        fault: { arm: armKinds('span.setStatus', kinds) },
        check: (h) => assert.deepStrictEqual(h.points, OPERATIONS[operation].points),
      },
      {
        name: 'ending a span throws after it ended',
        operation,
        fault: { arm: armKinds('span.end:after', kinds) },
        check: (h) => assert.deepStrictEqual(h.points, OPERATIONS[operation].points),
      },
      {
        name: 'every observer throws',
        operation,
        outcomes: FAILING,
        fault: {
          arm: {
            ...metrics,
            ...armKinds('span.setAttribute', kinds),
            ...armKinds('span.setStatus', kinds),
            ...armKinds('span.end:after', kinds),
          },
        },
      },
    ]
  })
)

describe('end cleanup boundary: a span whose end throws before it ends', () => {
  for (const operation of ['send', 'eachBatch']) {
    for (const outcome of ['resolve', 'reject']) {
      const first = operation === 'send' ? 'send' : 'poll'
      it(
        `is attempted once and the other spans are still ended / ${operation} / ${outcome}`,
        TIMEOUT,
        () =>
          assertContained(
            createHarness({
              operation,
              outcome,
              fault: { arm: { [`span.end:before:${first}`]: 1 } },
            }),
            {
              spans: 'attempted',
              check: (h) => {
                const [failed, ...rest] = h.spans
                assert.strictEqual(failed.endAttempts, 1)
                assert.strictEqual(failed.ended, false)
                assert.ok(rest.length >= 1)
                assert.ok(rest.every((span) => span.ended))
              },
            }
          )
      )
    }
  }
})

describe('diagnostic failure preserves original outcome', () => {
  for (const [operation, arm] of [
    ['send', { 'span.end:after:send': true }],
    ['eachMessage', { 'startSpan:process': true }],
    ['txCommit', { 'span.setStatus:transaction': true }],
  ]) {
    it(`${operation} / the failure's message cannot be read by the logger`, TIMEOUT, () =>
      assertContained(createHarness({ operation, fault: { arm }, hostileError: true }))
    )
  }
})

describe('synchronous application failure preserves original reason', () => {
  for (const operation of Object.keys(OPERATIONS)) {
    for (const outcome of ['throw', 'throw-undefined']) {
      it(`${operation} / ${outcome} / spans are cleaned up`, TIMEOUT, () =>
        assertContained(createHarness({ operation, outcome }), { check: assertObserved })
      )
    }
  }
})

describe('untraced arguments retain their original promise', () => {
  for (const [operation, patch, args] of [
    ['send', '_getSendPatch', [[], [{}], [{ topic: 't' }], [{ topic: 't', messages: 'abc' }]]],
    ['sendBatch', '_getSendBatchPatch', [[], [{}], [{ topicMessages: 'abc' }]]],
  ]) {
    for (const call of args) {
      it(
        `${operation} returns the promise of the original for ${JSON.stringify(call)}`,
        TIMEOUT,
        () => {
          const h = createHarness({ operation })
          try {
            const returned = Promise.resolve('sent')
            const wrapped = h.instrumentation[patch]()(() => returned)
            assert.strictEqual(wrapped(...call), returned)
            assert.strictEqual(h.spans.length, 0)
          } finally {
            h.restore()
          }
        }
      )
    }
  }
})

describe('the module exports', () => {
  it('only the instrumentation class', () => {
    assert.deepStrictEqual(Object.keys(instrumentationExports), ['KafkaJsInstrumentation'])
  })
})

describe('a client found by the module hook', () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'kafkajs-otel-containment-'))
  after(() => fs.rmSync(root, { recursive: true, force: true }))

  /**
   * Installs a stand-in client under a regular `node_modules` directory and requires it, so the
   * hook sees the path and version a real install gives it.
   * @returns {{ Kafka: Function }}
   */
  const install = () => {
    const project = fs.mkdtempSync(path.join(root, 'project-'))
    const dir = path.join(project, 'node_modules', '@countly', 'kafkajs')
    fs.mkdirSync(dir, { recursive: true })
    fs.writeFileSync(
      path.join(dir, 'package.json'),
      JSON.stringify({ name: '@countly/kafkajs', version: '3.0.0-alpha.8', main: 'index.js' })
    )
    fs.writeFileSync(
      path.join(dir, 'index.js'),
      `const transaction = { send: async () => 'sent', sendBatch: async () => 'batch', commit: async () => 'committed', abort: async () => 'aborted' }
class Kafka {
  producer() { return { send: async () => 'produced', sendBatch: async () => 'produced', transaction: async () => transaction, events: { REQUEST: 'request' }, on() {} } }
  consumer() { return { run: async () => 'ran', events: { REQUEST: 'request' }, on() {} } }
}
module.exports = { Kafka }`
    )
    return createRequire(path.join(project, 'index.js'))('@countly/kafkajs')
  }

  const withHook = async (arm, run) => {
    const h = createHarness({ operation: 'request', fault: { arm } })
    try {
      h.instrumentation.enable()
      const { Kafka } = install()
      h.arm()
      await run(new Kafka(), h)
      assert.ok(h.traps.fired >= 1, 'the injected fault was never reached')
    } finally {
      h.restore()
    }
  }

  it('settles a producer send with the client value although the tracer and a span end throw', () =>
    withHook({ 'startSpan:send': true }, async (kafka) => {
      const producer = kafka.producer()
      assert.ok(isWrapped(producer.send))
      assert.strictEqual(
        await producer.send({ topic: 't', messages: [{ value: 'a' }] }),
        'produced'
      )
    }))

  it('settles a consumer handler with its value although the tracer throws', () =>
    withHook({ 'startSpan:process': true }, async (kafka) => {
      const config = { eachMessage: async () => 'handled' }
      assert.strictEqual(await kafka.consumer().run(config), 'ran')
      assert.ok(isWrapped(config.eachMessage))
      const payload = { topic: 't', partition: 0, message: { headers: {} } }
      assert.strictEqual(await config.eachMessage(payload), 'handled')
    }))

  it('commits a transaction with the client value although the status cannot be set', () =>
    withHook({ 'span.setStatus:transaction': true }, async (kafka) => {
      const transaction = await kafka.producer().transaction()
      assert.strictEqual(await transaction.send({ topic: 't', messages: [{ value: 'a' }] }), 'sent')
      assert.strictEqual(await transaction.commit(), 'committed')
    }))
})
