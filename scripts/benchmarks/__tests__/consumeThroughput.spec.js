const path = require('path')
const { spawnSync } = require('child_process')
const { percentiles, deliveryGaps, countLedgerCalls } = require('../consumeThroughput')
const OffsetOperations = require('../../../src/consumer/offsetManager/offsetOperations')

const SCRIPT = path.join(__dirname, '..', 'consumeThroughput.js')
const NO_NETWORK = path.join(__dirname, 'fixtures', 'noNetwork.js')
const NO_LEDGER = path.join(__dirname, 'fixtures', 'noLedger.js')

/**
 * Runs the script with arguments it refuses before it connects to anything. The script's brokers
 * are fixed at `localhost:9092`, so the run cannot open a socket: one that tries exits with 97.
 */
const refused = (...args) =>
  spawnSync(process.execPath, ['--require', NO_NETWORK, SCRIPT, ...args], {
    encoding: 'utf8',
    timeout: 20000,
  })

/**
 * Loads a fresh copy of the script as its flags would configure it. The script runs nothing when
 * required, so no client is built.
 * @param {...string} flags command line flags the script reads while it loads
 * @returns {typeof import('../consumeThroughput')} the script's exports
 */
const loadBenchmark = (...flags) => {
  const { argv } = process
  const { BENCH_TOPIC } = process.env
  process.argv = [...argv, ...flags]
  delete process.env.BENCH_TOPIC
  try {
    let loaded
    jest.isolateModules(() => {
      loaded = require('../consumeThroughput')
    })
    return loaded
  } finally {
    process.argv = argv
    if (BENCH_TOPIC !== undefined) {
      process.env.BENCH_TOPIC = BENCH_TOPIC
    }
  }
}

describe('Benchmarks > consumeThroughput', () => {
  describe('percentiles', () => {
    test('are read from the sorted values at the fraction of their count', () => {
      const values = Array.from({ length: 100 }, (_, index) => 100 - index)

      expect(percentiles(values)).toEqual({ p50: 51, p90: 91, p99: 100 })
    })

    test('take the lower index when the fraction of the count is not whole', () => {
      const values = [11, 3, 7, 1, 9, 5, 10, 2, 8, 4, 6]

      expect(percentiles(values)).toEqual({ p50: 6, p90: 10, p99: 11 })
    })

    test('give the one value of one value', () => {
      expect(percentiles([4])).toEqual({ p50: 4, p90: 4, p99: 4 })
    })
  })

  describe('deliveryGaps', () => {
    test('are the time between consecutive entries, in milliseconds', () => {
      expect(deliveryGaps([0n, 1000000n, 3500000n, 3500000n])).toEqual([1, 2.5, 0])
    })

    test('are none until there are two entries', () => {
      expect(deliveryGaps([])).toEqual([])
      expect(deliveryGaps([5n])).toEqual([])
    })

    test('see a stall between two entries that the handler time cannot: the point of the measure', () => {
      const entries = [0n, 1000000n, 2000000n, 502000000n]

      expect(percentiles(deliveryGaps(entries)).p99).toBe(500)
    })
  })

  describe('countLedgerCalls', () => {
    const cell = [{ topic: 't', partition: 0 }]

    test('counts the begin calls from the first delivery on, and none before it', () => {
      const ledger = countLedgerCalls()
      try {
        const operations = new OffsetOperations()
        operations.end(operations.begin(cell))
        expect(ledger.count()).toBe(0)

        ledger.delivered()
        operations.end(operations.begin(cell))
        operations.end(operations.begin(cell, { mutation: true }))

        expect(ledger.count()).toBe(2)
      } finally {
        ledger.restore()
      }
    })

    test('leaves the ledger as it was found', () => {
      const original = OffsetOperations.prototype.begin

      const ledger = countLedgerCalls()
      expect(OffsetOperations.prototype.begin).not.toBe(original)
      ledger.restore()

      expect(OffsetOperations.prototype.begin).toBe(original)
    })

    test('refuses a tree that has no ledger, naming the mode', () => {
      const missing = Object.assign(new Error('Cannot find module'), { code: 'MODULE_NOT_FOUND' })

      expect(() =>
        countLedgerCalls(() => {
          throw missing
        })
      ).toThrow('--control 1 counts OffsetOperations#begin, which this tree does not have')
    })

    test('lets any other failure of loading through', () => {
      const broken = new SyntaxError('Unexpected token')

      expect(() =>
        countLedgerCalls(() => {
          throw broken
        })
      ).toThrow(broken)
    })
  })

  describe('--control 1', () => {
    test('is refused on a tree without the ledger before the script connects or creates a topic', () => {
      const run = spawnSync(
        process.execPath,
        ['--require', NO_NETWORK, '--require', NO_LEDGER, SCRIPT, '--control', '1'],
        { encoding: 'utf8', timeout: 20000 }
      )

      expect(run.status).toBe(1)
      expect(run.stderr).toContain(
        '--control 1 counts OffsetOperations#begin, which this tree does not have'
      )
    })
  })

  describe('the flags', () => {
    test.each([
      [['--control', '2'], '--control must be 0 or 1, got 2'],
      [['--listener', '2'], '--listener must be 0 or 1, got 2'],
      [['--otel', '3'], '--otel must be 0 or 1, got 3'],
      [['--intern-offsets', 'x'], '--intern-offsets must be 0 or 1, got NaN'],
      [
        ['--autocommit-threshold', '0'],
        '--autocommit-threshold must be an integer of at least 1, got 0',
      ],
      [
        ['--max-bytes-per-partition', '1.5'],
        '--max-bytes-per-partition must be an integer of at least 1, got 1.5',
      ],
      [['--handler', 'neither'], '--handler must be eachBatch or eachMessage, got neither'],
    ])('%j is refused before anything connects', (args, message) => {
      const run = refused(...args)

      expect(run.status).toBe(1)
      expect(run.stderr).toContain(message)
    })
  })

  describe('the clients a helper builds', () => {
    const MESSAGES = 1
    const TIMEOUT_MS = 40
    const { createTopic, deleteTopic, produceCorpus, consumeOnce } = loadBenchmark(
      '--messages',
      String(MESSAGES),
      '--timeout',
      String(TIMEOUT_MS)
    )

    /**
     * One fake client for any role. `connect` marks it active and `disconnect` clears that, so what
     * is still active at settlement is what a helper left behind. A method named in `fail` rejects
     * with the error given for it; `hold` makes a method wait for the promise given for it first;
     * `deliver` makes `run` hand the handler its one message. `calls` is every call, in order.
     */
    const fakeClient = ({ fail = {}, hold = {}, deliver = false } = {}) => {
      const calls = []
      const client = {
        active: false,
        disconnects: 0,
        calls,
        connect: async () => {
          calls.push('connect')
          await hold.connect
          client.active = true
          if (fail.connect) throw fail.connect
        },
        run: async (config) => {
          calls.push('run')
          await hold.run
          if (fail.run) throw fail.run
          if (deliver) {
            await config.eachBatch({
              batch: { partition: 0, messages: [{ offset: '0', value: Buffer.alloc(200) }] },
            })
          }
        },
        disconnect: async () => {
          calls.push('disconnect')
          client.active = false
          client.disconnects += 1
          if (fail.disconnect) throw fail.disconnect
        },
      }
      for (const method of ['subscribe', 'stop', 'createTopics', 'deleteTopics', 'send']) {
        client[method] = async () => {
          calls.push(method)
          await hold[method]
          if (fail[method]) throw fail[method]
        }
      }
      return client
    }
    const kafkaOf = (client) => ({
      admin: () => client,
      producer: () => client,
      consumer: () => client,
    })
    const settle = (promise) =>
      promise.then(
        (value) => ({ value }),
        (error) => ({ error })
      )

    describe.each([
      ['createTopic', 'connect', createTopic],
      ['createTopic', 'createTopics', createTopic],
      ['produceCorpus', 'send', produceCorpus],
    ])('%s whose %s rejects', (_, method, helper) => {
      test('rejects with it, and the client is disconnected', async () => {
        const failure = new Error(`${method} failed`)
        const client = fakeClient({ fail: { [method]: failure } })

        const { error } = await settle(helper(kafkaOf(client)))

        expect(error).toBe(failure)
        expect(client.disconnects).toBe(1)
        expect(client.active).toBe(false)
      })
    })

    test('deleteTopic whose deleteTopics rejects reports it once, resolves, and disconnects', async () => {
      const failure = new Error('deleteTopics failed')
      const client = fakeClient({ fail: { deleteTopics: failure } })
      const report = jest.spyOn(console, 'error').mockImplementation(() => {})

      try {
        const { error } = await settle(deleteTopic(kafkaOf(client)))

        expect(error).toBeUndefined()
        expect(report).toHaveBeenCalledTimes(1)
        expect(report.mock.calls[0][0]).toContain('could not delete')
        expect(report.mock.calls[0][0]).toContain('deleteTopics failed')
        expect(client.disconnects).toBe(1)
        expect(client.active).toBe(false)
      } finally {
        report.mockRestore()
      }
    })

    describe('consumeOnce', () => {
      beforeEach(() => {
        jest.useFakeTimers({
          doNotFake: [
            'Date',
            'hrtime',
            'nextTick',
            'performance',
            'queueMicrotask',
            'setImmediate',
            'clearImmediate',
          ],
        })
      })
      afterEach(() => {
        jest.useRealTimers()
      })

      test.each(['connect', 'subscribe'])(
        'whose %s rejects rejects with it, disconnected, with no timer left',
        async (method) => {
          const failure = new Error(`${method} failed`)
          const client = fakeClient({ fail: { [method]: failure } })

          const { error } = await settle(consumeOnce(kafkaOf(client), 1))

          expect(error).toBe(failure)
          expect(client.disconnects).toBe(1)
          expect(client.active).toBe(false)
          expect(jest.getTimerCount()).toBe(0)
        }
      )

      test.each(
        ['connect', 'subscribe', 'run'].flatMap((method) => [
          [method, 'resolves'],
          [method, 'rejects'],
        ])
      )(
        'whose %s is held past the deadline and then %s leaves no orphan rejection and no later activity',
        async (method, outcome) => {
          let release
          const gate = new Promise((resolve, reject) => {
            release = { resolves: resolve, rejects: () => reject(failure) }
          })
          const failure = new Error(`${method} released late`)
          const client = fakeClient({ hold: { [method]: gate } })
          const turn = () => new Promise((resolve) => setImmediate(resolve))

          let over = false
          const settled = settle(consumeOnce(kafkaOf(client), 1))
          settled.then(() => {
            over = true
          })
          await jest.advanceTimersByTimeAsync(TIMEOUT_MS * 2)
          await turn()
          release[outcome]()
          await turn()
          expect(over).toBe(outcome === 'rejects')
          await jest.advanceTimersByTimeAsync(TIMEOUT_MS)
          const { error } = await settled

          if (outcome === 'rejects') {
            expect(error).toBe(failure)
          } else {
            expect(error.message).toBe(
              `run 1 timed out after ${TIMEOUT_MS} ms with 0 of ${MESSAGES} messages consumed`
            )
          }
          expect(client.disconnects).toBe(1)
          expect(client.active).toBe(false)
          expect(jest.getTimerCount()).toBe(0)
          const calls = [...client.calls]
          await jest.advanceTimersByTimeAsync(TIMEOUT_MS * 2)
          await turn()
          expect(client.calls).toEqual(calls)
        }
      )

      test('whose run rejects leaves no timer, and no unhandled rejection when the deadline would have fired', async () => {
        const failure = new Error('run failed')
        const client = fakeClient({ fail: { run: failure } })

        const { error } = await settle(consumeOnce(kafkaOf(client), 1))

        expect(error).toBe(failure)
        expect(client.disconnects).toBe(1)
        expect(client.active).toBe(false)
        expect(jest.getTimerCount()).toBe(0)
        await jest.advanceTimersByTimeAsync(TIMEOUT_MS * 2)
        await new Promise((resolve) => setImmediate(resolve))
      })

      test('whose deadline fires rejects with the timeout, disconnected, with no timer left', async () => {
        const client = fakeClient()

        const settled = settle(consumeOnce(kafkaOf(client), 1))
        await jest.advanceTimersByTimeAsync(TIMEOUT_MS)
        const { error } = await settled

        expect(error.message).toBe(
          `run 1 timed out after ${TIMEOUT_MS} ms with 0 of ${MESSAGES} messages consumed`
        )
        expect(client.disconnects).toBe(1)
        expect(client.active).toBe(false)
        expect(jest.getTimerCount()).toBe(0)
      })

      test('that delivers and then fails to disconnect rejects with the disconnect failure', async () => {
        const failure = new Error('disconnect failed')
        const client = fakeClient({ deliver: true, fail: { disconnect: failure } })

        const { error } = await settle(consumeOnce(kafkaOf(client), 1))

        expect(error).toBe(failure)
        expect(client.disconnects).toBe(1)
        expect(jest.getTimerCount()).toBe(0)
      })

      test('that delivers resolves with its statistics, disconnected, with no timer left', async () => {
        const client = fakeClient({ deliver: true })

        const { value, error } = await settle(consumeOnce(kafkaOf(client), 1))

        expect(error).toBeUndefined()
        expect(value).toMatchObject({ batches: 1, p50: expect.any(Number) })
        expect(value.messagesPerSecond).toBeGreaterThan(0)
        expect(client.disconnects).toBe(1)
        expect(client.active).toBe(false)
        expect(jest.getTimerCount()).toBe(0)
      })
    })
  })
})

const {
  DeliveryAccounting,
  ListenerAccounting,
  verifyAccounting,
  accountingControl,
  consumeOnce,
} = require('../consumeThroughput')

/** @param {number[]} sizes records per batch @param {object} [options] teardown and listener controls @returns {object} a public-API-shaped finite source */
const countedSource = (
  sizes,
  { teardownTicks = 0n, failRun = false, failStop = false, omit = [] } = {}
) => {
  let clock = 0n
  const topics = [{ topic: 't', partitions: [0] }]
  let listener
  const consumer = {
    connect: jest.fn(async () => {}),
    subscribe: jest.fn(async () => {}),
    run: jest.fn(async (options) => {
      if (failRun) throw new Error('run failed')
      listener = options.rebalanceListener
      if (!omit.includes('startupRevoked')) await listener?.onPartitionsRevoked({ partitions: [] })
      if (!omit.includes('assigned')) await listener?.onPartitionsAssigned({ partitions: topics })
      let offset = 0
      for (const size of sizes) {
        const messages = Array.from({ length: size }, () => ({
          value: Buffer.alloc(2),
          offset: String(offset++),
        }))
        clock += 1000000n
        if (options.eachBatch) await options.eachBatch({ batch: { partition: 0, messages } })
        else
          for (const message of messages) {
            clock += 1000000n
            await options.eachMessage({ partition: 0, message })
          }
      }
    }),
    stop: jest.fn(async () => {
      clock += teardownTicks
      if (failStop) throw new Error('stop failed')
      if (!omit.includes('teardownRevoked'))
        await listener?.onPartitionsRevoked({ partitions: topics })
    }),
    disconnect: jest.fn(async () => {}),
  }
  return {
    kafka: { consumer: jest.fn(() => consumer) },
    consumer,
    now: () => {
      clock += 1000n
      return clock
    },
  }
}

describe('Benchmarks > counted consume controls', () => {
  test('one multi-record batch is one entry and three messages', () => {
    const counter = new DeliveryAccounting('eachBatch')
    counter.enter(3, 1000000n)
    expect(counter.result()).toEqual({
      deliveredMessages: 3,
      deliveredBatches: 1,
      handlerEntries: 1,
      gapSamples: 0,
      deliveryGapP50: null,
      deliveryGapP90: null,
      deliveryGapP99: null,
    })
  })

  test('gaps come from entry times even when record counts differ', () => {
    const counter = new DeliveryAccounting('eachBatch')
    counter.enter(5, 0n)
    counter.enter(2, 1000000n)
    counter.enter(5, 3500000n)
    expect(counter.result()).toEqual({
      deliveredMessages: 12,
      deliveredBatches: 3,
      handlerEntries: 3,
      gapSamples: 2,
      deliveryGapP50: 2.5,
      deliveryGapP90: 2.5,
      deliveryGapP99: 2.5,
    })
  })

  test.each(['eachBatch', 'eachMessage'])(
    '%s untimed controls preserve the same total across batch shapes',
    async (handler) => {
      const rows = await accountingControl(handler)
      expect(rows.map(({ deliveredMessages }) => deliveredMessages)).toEqual([12, 12, 12])
      expect(rows.map(({ deliveredBatches }) => deliveredBatches)).toEqual(
        handler === 'eachBatch' ? [1, 4, 6] : [0, 0, 0]
      )
      expect(rows.map(({ handlerEntries }) => handlerEntries)).toEqual(
        handler === 'eachBatch' ? [1, 4, 6] : [12, 12, 12]
      )
      expect(rows.map(({ gapSamples }) => gapSamples)).toEqual(
        handler === 'eachBatch' ? [0, 3, 5] : [11, 11, 11]
      )
      expect(rows.map(({ deliveredBytes }) => deliveredBytes)).toEqual([24, 24, 24])
    }
  )

  test.each([
    ['deliveredMessages', 11],
    ['deliveredBatches', 3],
    ['handlerEntries', 3],
    ['gapSamples', 11],
    ['deliveredBytes', 22],
  ])('rejects a false %s against independent source totals', (field, value) => {
    const expected = {
      deliveredMessages: 12,
      deliveredBatches: 4,
      handlerEntries: 4,
      gapSamples: 3,
      deliveredBytes: 24,
    }
    expect(() => verifyAccounting({ ...expected, [field]: value }, expected)).toThrow(
      `consume accounting ${field}: expected ${expected[field]}, received ${value}`
    )
    expect(() => verifyAccounting(expected, expected)).not.toThrow()
  })

  test.each(['eachBatch', 'eachMessage'])(
    '%s uses the real handler wiring and excludes teardown from the interval',
    async (handler) => {
      const fast = countedSource([3, 3, 3, 3])
      const slow = countedSource([3, 3, 3, 3], { teardownTicks: 1000000000000n })
      const run = (source) =>
        consumeOnce(source.kafka, 1, undefined, {
          handler,
          messages: 12,
          size: 2,
          timeout: 50,
          now: source.now,
        })
      const first = await run(fast)
      const second = await run(slow)
      expect(second).toEqual(first)
      expect(first.deliveredMessages).toBe(12)
      expect(first.handlerEntries).toBe(handler === 'eachBatch' ? 4 : 12)
      expect(fast.consumer.stop).toHaveBeenCalledTimes(1)
      expect(fast.consumer.disconnect).toHaveBeenCalledTimes(1)
      expect(fast.consumer.run.mock.calls[0][0].autoCommit).toBe(false)
      expect(fast.consumer.subscribe).toHaveBeenCalledWith(
        expect.objectContaining({ fromBeginning: true })
      )
    }
  )

  test('a missing batch fails at the driver deadline with the actual delivered total', async () => {
    const source = countedSource([3, 3, 3])
    await expect(
      consumeOnce(source.kafka, 1, undefined, {
        handler: 'eachBatch',
        messages: 12,
        size: 2,
        timeout: 20,
        now: source.now,
      })
    ).rejects.toThrow('with 9 of 12 messages consumed')
    expect(source.consumer.disconnect).toHaveBeenCalledTimes(1)
  })

  test('a dropped record fails at the driver deadline', async () => {
    const source = countedSource([3, 3, 3, 2])
    await expect(
      consumeOnce(source.kafka, 1, undefined, {
        handler: 'eachBatch',
        messages: 12,
        size: 2,
        timeout: 20,
        now: source.now,
      })
    ).rejects.toThrow('with 11 of 12 messages consumed')
  })

  test('duplicate delivery cannot satisfy the exact-total control', async () => {
    const source = countedSource([3, 3, 3, 4])
    await expect(
      consumeOnce(source.kafka, 1, undefined, {
        handler: 'eachBatch',
        messages: 12,
        size: 2,
        timeout: 20,
        now: source.now,
      })
    ).rejects.toThrow('consume accounting deliveredMessages: expected 12, received 13')
  })

  test.each([{ failRun: true }, { failStop: true }])(
    'disconnects and clears the deadline after %j',
    async (options) => {
      const source = countedSource([12], options)
      await expect(
        consumeOnce(source.kafka, 1, undefined, {
          handler: 'eachBatch',
          messages: 12,
          size: 2,
          timeout: 50,
          now: source.now,
        })
      ).rejects.toThrow(options.failRun ? 'run failed' : 'stop failed')
      expect(source.consumer.disconnect).toHaveBeenCalledTimes(1)
    }
  )
})

describe('Benchmarks > listener controls', () => {
  const run = (source, handler = 'eachBatch', listener = true) =>
    consumeOnce(source.kafka, 1, undefined, {
      handler,
      messages: 12,
      size: 2,
      timeout: 50,
      now: source.now,
      listener,
    })

  test('initial assignment is required by the listener control', async () => {
    await expect(run(countedSource([12], { omit: ['assigned'] }))).rejects.toThrow(
      'Listener control requires initial assigned and graceful teardown revoked once'
    )
    await expect(run(countedSource([12]))).resolves.toMatchObject({ deliveredMessages: 12 })
  })

  test('graceful teardown revoke is required independently of eager startup revoke', async () => {
    const source = countedSource([12], { omit: ['teardownRevoked'] })
    await expect(run(source)).rejects.toThrow(
      'Listener control requires initial assigned and graceful teardown revoked once'
    )
    // the eager startup revoke callback ran, and did not stand in for the teardown one
    expect(source.consumer.run.mock.calls[0][0].rebalanceListener.onPartitionsRevoked).toEqual(
      expect.any(Function)
    )
  })

  test.each(['eachBatch', 'eachMessage'])(
    '%s listener controls preserve message and batch accounting',
    async (handler) => {
      const grouped = await run(countedSource([4, 4, 4]), handler)
      const other = await run(countedSource([3, 3, 3, 3]), handler)
      expect(grouped.deliveredMessages).toBe(12)
      expect(other.deliveredMessages).toBe(12)
      expect(grouped.handlerEntries).toBe(handler === 'eachBatch' ? 3 : 12)
      expect(other.handlerEntries).toBe(handler === 'eachBatch' ? 4 : 12)
      expect(grouped.gapSamples).toBe(handler === 'eachBatch' ? 2 : 11)
      expect(other.gapSamples).toBe(handler === 'eachBatch' ? 3 : 11)
    }
  )

  test('teardown is outside the listener throughput interval', async () => {
    const fast = await run(countedSource([3, 3, 3, 3]))
    const slow = await run(countedSource([3, 3, 3, 3], { teardownTicks: 1000000000000n }))
    expect(slow).toEqual(fast)
  })

  test('the counter ignores a startup revoke, a teardown assignment and a duplicate teardown revoke', () => {
    const counter = new ListenerAccounting()
    counter.onRevoked()
    counter.onAssigned()
    counter.beginTeardown()
    counter.onAssigned()
    counter.onRevoked()
    expect(() => counter.verify()).not.toThrow()
    counter.onRevoked()
    expect(() => counter.verify()).toThrow('Listener control requires')
  })

  test('a run without the listener flag registers no callbacks and verifies nothing', async () => {
    const source = countedSource([12])
    await run(source, 'eachBatch', false)
    expect(source.consumer.run.mock.calls[0][0].rebalanceListener).toBeUndefined()
  })

  test.each([{ failRun: true }, { failStop: true }])(
    'listener addition keeps cleanup and error identity after %j',
    async (options) => {
      const source = countedSource([12], options)
      await expect(run(source)).rejects.toThrow(options.failRun ? 'run failed' : 'stop failed')
      expect(source.consumer.disconnect).toHaveBeenCalledTimes(1)
    }
  )
})
