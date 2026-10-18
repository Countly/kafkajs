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
