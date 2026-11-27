const { spawnSync } = require('node:child_process')
const { builtinModules } = require('node:module')
const fs = require('node:fs')
const os = require('node:os')
const path = require('node:path')
const { constants } = require('node:perf_hooks')
const {
  parseArgs,
  runWorkload,
  valuePool,
  WORKLOADS,
  SAME_SETTINGS,
  PACKAGE_DEFAULTS,
  SAME_PRODUCER_SETTINGS,
  PRODUCER_DEFAULTS,
  CORPUS_PRODUCER_SETTINGS,
  CORPUS_LAYOUT,
  countFullGcs,
  trackGcs,
  customRouting,
} = require('../clientCompare')

const SCRIPT = path.join(__dirname, '..', 'clientCompare.js')
const BROKERS = ['localhost:9092', 'localhost:9095', 'localhost:9098']
const ROOT = path.join(__dirname, '..', '..', '..')
const LZ4_VERSION = JSON.parse(
  fs.readFileSync(path.join(ROOT, 'node_modules', 'lz4-napi', 'package.json'), 'utf8')
).version

/** Runs node in a fresh process, so module resolution is Node's and not Jest's. */
const node = (args) => spawnSync(process.execPath, args, { encoding: 'utf8' })

/** Writes `<prefix>/node_modules/<name>` as a stub package whose Kafka carries `origin`. */
const installStub = (prefix, name, version, origin) => {
  const dir = path.join(prefix, 'node_modules', ...name.split('/'))
  fs.mkdirSync(dir, { recursive: true })
  fs.writeFileSync(
    path.join(dir, 'package.json'),
    JSON.stringify({ name, version, main: 'index.js' })
  )
  fs.writeFileSync(
    path.join(dir, 'index.js'),
    `exports.Kafka = class Kafka { static origin = ${JSON.stringify(origin)} }\nexports.logLevel = { NOTHING: 0 }\n`
  )
}

/** What kafkajs 2.2.4's admin throws while a broker does not know a topic yet. */
const unknownTopic = () =>
  Object.assign(new Error('This server does not host this topic-partition'), {
    type: 'UNKNOWN_TOPIC_OR_PARTITION',
  })
/** Stands in for a package's `Partitioners.DefaultPartitioner`. */
const legacyPartitioner = () => () => 0

/**
 * An in-memory Kafka that keeps what it was asked to do in `state`. Its admin throws, as kafkajs
 * 2.2.4's does, when `createTopics` waits for leaders the broker does not know yet, which both
 * packages do unless `waitForLeaders` is false; `state.leaderWaits` lists each call's choice;
 * `metadata` lists what each `fetchTopicMetadata` answers in turn (`'unknown'`, `'no leader'`,
 * leaders for every partition when absent). `offsets` replaces what `fetchTopicOffsets` answers
 * (each partition's count of the records sent to it when absent); `state.eachMessage` is the
 * consumer's handler, which gets each record in send order from its partition, with offsets counted
 * per partition, and a sought partition's records again from the sought offset.
 * `threadCpu: false` stands for a Node without `process.threadCpuUsage`; `state.gc` records when the
 * full-GC count started, stopped, counted and closed, in callbacks.
 */
const fakeKafka = ({
  failSend = false,
  failConnect,
  failRun = false,
  failAfter,
  manualDelivery = false,
  sendMs,
  metadata = [],
  offsets,
  threadCpu = true,
} = {}) => {
  const state = {
    topics: new Map(),
    sends: [],
    closed: [],
    deleted: [],
    configs: [],
    consumerGroups: [],
    deletedGroups: [],
    events: [],
    callbacks: 0,
    timers: new Map(),
    createdTopics: [],
    leaderWaits: [],
    consumerConfigs: [],
    producerConfigs: [],
    metadataReads: 0,
    sleeps: [],
    seeks: [],
    compressions: [],
    listeners: [],
    delay: { enabledAt: [], disabledAt: [] },
    gc: { startedAt: [], stoppedAt: [], countedAt: [], closedAt: [] },
    allGcs: { started: 0, stopped: 0, resulted: 0, closed: 0 },
    intervals: [],
    clearedIntervals: [],
  }
  let elapsedNs = 0n
  let ready
  state.ready = new Promise((resolve) => {
    ready = resolve
  })
  const timing = {
    now: () => elapsedNs,
    setTimeout: (callback, delay) => {
      const timer = Symbol('timeout')
      state.timers.set(timer, { callback, delay })
      return timer
    },
    clearTimeout: (timer) => state.timers.delete(timer),
    sleep: async (ms) => {
      state.sleeps.push(ms)
      elapsedNs += BigInt(ms) * 1000000n
    },
    cpuUsage: (previous) =>
      previous ? { user: 600 * state.callbacks + 1200, system: 300 } : { user: 0, system: 0 },
    threadCpuUsage: threadCpu
      ? (previous) =>
          previous ? { user: 400 * state.callbacks + 800, system: 100 } : { user: 0, system: 0 }
      : null,
    countFullGcs: () => {
      state.gc.startedAt.push(state.callbacks)
      return {
        stop: () => state.gc.stoppedAt.push(state.callbacks),
        count: async () => {
          state.gc.countedAt.push(state.callbacks)
          return 3
        },
        close: () => state.gc.closedAt.push(state.callbacks),
      }
    },
    memoryUsage: () => ({
      rss: 1000 + 10 * state.sends.length,
      arrayBuffers: 50 + state.sends.length,
    }),
    setInterval: (callback, delay) => {
      const timer = Symbol('interval')
      state.intervals.push({ timer, callback, delay })
      return timer
    },
    clearInterval: (timer) => state.clearedIntervals.push(timer),
    trackGcs: () => {
      state.allGcs.started++
      return {
        open: (at) => {
          state.allGcs.from = at
        },
        stop: (at) => {
          state.allGcs.stopped++
          state.allGcs.to = at
        },
        result: async () => {
          state.allGcs.resulted++
          return { count: 3, pauseMs: 1.5 }
        },
        close: () => state.allGcs.closed++,
      }
    },
    eventLoopDelay: () => ({
      enable: () => state.delay.enabledAt.push(state.callbacks),
      disable: () => state.delay.disabledAt.push(state.callbacks),
      percentile: (p) => p * 1e6,
      max: 250e6,
    }),
  }
  state.request = (payload) => {
    for (const listener of state.listeners) listener({ payload })
  }
  const connect = async (kind) => {
    if (failConnect === kind) throw new Error(`${kind} connect failed`)
  }
  const disconnect = async (kind) => {
    state.closed.push(kind)
    state.events.push(`disconnect ${kind}`)
  }
  class Kafka {
    constructor(config) {
      state.configs.push(config)
    }

    admin() {
      return {
        connect: () => connect('admin'),
        createTopics: async ({ topics, waitForLeaders = true }) => {
          state.leaderWaits.push(waitForLeaders)
          if (waitForLeaders) throw unknownTopic()
          for (const { topic } of topics) state.topics.set(topic, [])
          state.createdTopics.push(...topics)
          return true
        },
        fetchTopicMetadata: async ({ topics }) => {
          const answer = metadata[state.metadataReads++]
          if (answer === 'unknown') throw unknownTopic()
          const leader = answer === 'no leader' ? -1 : 1
          return {
            topics: topics.map((name) => {
              const { numPartitions } = state.createdTopics.find(({ topic }) => topic === name)
              const partitions = Array.from({ length: numPartitions }, (_, partitionId) => ({
                partitionId,
                leader,
              }))
              return { name, partitions }
            }),
          }
        },
        fetchTopicOffsets: async (topic) => {
          if (offsets) return offsets
          const { numPartitions } = state.createdTopics.find((created) => created.topic === topic)
          const counts = Array(numPartitions).fill(0)
          for (const { partition = 0 } of state.topics.get(topic)) counts[partition]++
          return counts.map((count, partition) => ({
            partition,
            offset: String(count),
            high: String(count),
            low: '0',
          }))
        },
        deleteTopics: async ({ topics }) => {
          for (const topic of topics) state.deleted.push(topic)
          state.events.push('delete topics')
        },
        deleteGroups: async (groups) => {
          state.deletedGroups.push(...groups)
          state.events.push('delete groups')
        },
        disconnect: () => disconnect('admin'),
      }
    }

    producer(config) {
      state.producerConfigs.push(config)
      return {
        connect: () => connect('producer'),
        send: async ({ topic, messages, compression }) => {
          if (failSend || state.sends.length === failAfter) throw new Error('send failed')
          const durationMs = sendMs ?? (state.sends.length === 0 ? 1000 : state.sends.length)
          state.compressions.push(compression)
          elapsedNs += BigInt(durationMs) * 1000000n
          state.sends.push(messages.length)
          state.topics.get(topic).push(...messages)
        },
        disconnect: () => disconnect('producer'),
      }
    }

    consumer(config) {
      state.consumerGroups.push(config.groupId)
      state.consumerConfigs.push(config)
      let topic
      let seekTo = () => {}
      return {
        events: { REQUEST: 'consumer.network.request' },
        on: (event, listener) => {
          if (event === 'consumer.network.request') state.listeners.push(listener)
          return () => {}
        },
        connect: () => connect('consumer'),
        subscribe: async (subscription) => {
          topic = subscription.topic
          state.subscription = subscription
        },
        seek: ({ topic: sought, partition, offset }) => {
          state.seeks.push({ topic: sought, partition, offset })
          seekTo(partition, Number(offset))
        },
        run: async ({ eachMessage }) => {
          state.eachMessage = eachMessage
          const records = state.topics.get(topic)
          state.deliverable = records.length
          const logs = new Map()
          const queue = records.map((record) => {
            const partition = record.partition ?? 0
            if (!logs.has(partition)) logs.set(partition, [])
            const log = logs.get(partition)
            log.push(record)
            return { partition, offset: log.length - 1, record }
          })
          seekTo = (partition, from) =>
            logs
              .get(partition)
              .slice(from)
              .forEach((record, i) => queue.push({ partition, offset: from + i, record }))
          let index = 0
          state.deliver = async (count = Infinity) => {
            for (let i = 0; i < count && index < queue.length; i++) {
              const { partition, offset, record } = queue[index++]
              const message = { ...record, offset: String(offset) }
              elapsedNs += 10000000n
              state.callbacks += 1
              await eachMessage({ topic, partition, message })
            }
          }
          ready()
          if (failRun) throw new Error('run failed')
          if (!manualDelivery) await state.deliver()
        },
        disconnect: () => disconnect('consumer'),
      }
    }
  }
  const client = {
    Kafka,
    logLevel: { NOTHING: 0 },
    CompressionTypes: { GZIP: 1, LZ4: 3 },
    Partitioners: { DefaultPartitioner: legacyPartitioner },
  }
  return { client, state, timing }
}

/**
 * Counts, for the producers a fake client creates from now on, how many sends are in flight at
 * once: `peak` is the most any producer had unanswered together.
 */
const trackInFlight = ({ client }) => {
  const tracker = { inFlight: 0, peak: 0 }
  const create = client.Kafka.prototype.producer
  client.Kafka.prototype.producer = function producer(config) {
    const created = create.call(this, config)
    const send = created.send
    created.send = async (request) => {
      tracker.peak = Math.max(tracker.peak, ++tracker.inFlight)
      try {
        return await send(request)
      } finally {
        tracker.inFlight--
      }
    }
    return created
  }
  return tracker
}

/**
 * Makes every send of the producers a fake client creates from now on wait until the test answers
 * it: `sizes` lists the records of each send in call order, `answer(n)` answers the `n` oldest
 * unanswered sends and `unanswered()` counts the rest.
 */
const holdReplies = ({ client }) => {
  const sizes = []
  const waiting = []
  const create = client.Kafka.prototype.producer
  client.Kafka.prototype.producer = function producer(config) {
    const created = create.call(this, config)
    created.send = ({ messages }) =>
      new Promise((resolve) => {
        sizes.push(messages.length)
        waiting.push(resolve)
      })
    return created
  }
  return {
    sizes,
    answer: (count) => {
      for (const resolve of waiting.splice(0, count)) resolve()
    },
    unanswered: () => waiting.length,
  }
}

/** Lets the event loop turn `count` times, so awaited work that is ready can run. */
const turns = async (count = 5) => {
  for (let i = 0; i < count; i++) await new Promise(setImmediate)
}

/** Waits, turn by turn, until `condition` holds; fails after a hundred turns. */
const until = async (condition) => {
  for (let i = 0; i < 100 && !condition(); i++) await new Promise(setImmediate)
  expect(condition()).toBe(true)
}

describe('Benchmarks > clientCompare', () => {
  let prefix
  beforeAll(() => {
    prefix = fs.mkdtempSync(path.join(os.tmpdir(), 'client-compare-'))
    fs.mkdirSync(path.join(prefix, 'fork'))
    fs.mkdirSync(path.join(prefix, 'base'))
    fs.mkdirSync(path.join(prefix, 'empty'))
    installStub(path.join(prefix, 'fork'), '@countly/kafkajs', '3.0.0-test', 'fork')
    installStub(path.join(prefix, 'base'), 'kafkajs', '2.2.4', 'baseline')
  })
  afterAll(() => fs.rmSync(prefix, { recursive: true, force: true }))

  describe('loadClient', () => {
    const load = (dir, pkg) =>
      node([
        '-e',
        `const { loadClient } = require(process.argv[1])
         const c = loadClient(process.argv[2], process.argv[3])
         console.log(JSON.stringify({ name: c.name, version: c.version, origin: c.Kafka.origin, level: c.logLevel }))`,
        SCRIPT,
        dir,
        pkg,
      ])

    test('each arm loads and reports only its own package name and version', () => {
      const fork = load(path.join(prefix, 'fork'), '@countly/kafkajs')
      const base = load(path.join(prefix, 'base'), 'kafkajs')

      expect({ status: fork.status, stderr: fork.stderr }).toEqual({ status: 0, stderr: '' })
      expect({ status: base.status, stderr: base.stderr }).toEqual({ status: 0, stderr: '' })
      expect(JSON.parse(fork.stdout)).toEqual({
        name: '@countly/kafkajs',
        version: '3.0.0-test',
        origin: 'fork',
        level: { NOTHING: 0 },
      })
      expect(JSON.parse(base.stdout)).toEqual({
        name: 'kafkajs',
        version: '2.2.4',
        origin: 'baseline',
        level: { NOTHING: 0 },
      })
    })

    test('rejects an ancestor package before evaluating it when the requested prefix is empty', () => {
      const ancestor = path.join(prefix, 'ancestor')
      const empty = path.join(ancestor, 'empty')
      fs.mkdirSync(empty, { recursive: true })
      installStub(ancestor, 'kafkajs', '2.2.4', 'ancestor')
      fs.writeFileSync(
        path.join(ancestor, 'node_modules', 'kafkajs', 'index.js'),
        "process.stdout.write('ancestor evaluated')\n" +
          'exports.Kafka = class Kafka {}\nexports.logLevel = { NOTHING: 0 }\n'
      )

      const result = load(empty, 'kafkajs')

      expect({ status: result.status, stderr: result.stderr }).toEqual({
        status: 1,
        stderr: expect.stringMatching(/outside.*prefix/i),
      })
      expect(result.stdout).toBe('')
    })

    test('an arm cannot load the other arm package from its directory', () => {
      for (const [dir, pkg] of [
        ['fork', 'kafkajs'],
        ['base', '@countly/kafkajs'],
      ]) {
        const { status, stderr } = load(path.join(prefix, dir), pkg)
        expect({ status, stderr }).toEqual({ status: 1, stderr: expect.stringContaining(pkg) })
      }
    })
  })

  describe('command line', () => {
    const run = (...args) => node([SCRIPT, ...args])

    test('a --package that is absent from --client exits non-zero before any broker is contacted', () => {
      const result = run(
        '--client',
        path.join(prefix, 'empty'),
        '--package',
        'kafkajs',
        '--workload',
        'produce-latency'
      )

      expect({ status: result.status, stderr: result.stderr }).toEqual({
        status: 1,
        stderr: expect.stringMatching(/kafkajs/),
      })
      expect(result.stdout).toBe('')
    })

    test('a missing or unknown flag or workload exits 2 and names the problem', () => {
      const bad = [
        ['--package', 'kafkajs', '--workload', 'produce-latency'],
        ['--client', 'x', '--workload', 'produce-latency'],
        ['--client', 'x', '--package', 'kafkajs'],
        ['--client', 'x', '--package', 'kafkajs', '--workload', 'nope'],
        [
          '--client',
          'x',
          '--package',
          'kafkajs',
          '--workload',
          'produce-latency',
          '--messages',
          '0',
        ],
        ['--client', 'x', '--package', 'kafkajs', '--workload', 'produce-latency', '--bogus', '1'],
      ]

      for (const args of bad) {
        const result = run(...args)
        expect({ status: result.status, stderr: result.stderr }).toEqual({
          status: 2,
          stderr: expect.stringMatching(/usage|--/i),
        })
      }
    })

    test('parseArgs applies the defaults per workload', () => {
      const parse = (workload, ...extra) =>
        parseArgs(['--client', '/c', '--package', 'kafkajs', '--workload', workload, ...extra])

      expect(WORKLOADS).toEqual(['produce-latency', 'produce-throughput', 'consume-throughput'])
      expect(parse('produce-latency')).toEqual({
        client: path.resolve('/c'),
        package: 'kafkajs',
        workload: 'produce-latency',
        messages: 20000,
        durationMs: null,
        size: 200,
        partitions: 3,
        compression: 'none',
        settings: 'defaults',
        passes: 1,
        brokers: BROKERS,
      })
      expect(parse('produce-throughput').messages).toBe(200000)
      expect(
        parse('consume-throughput', '--messages', '50', '--size', '10', '--brokers', 'a:1,b:2')
      ).toMatchObject({
        messages: 50,
        size: 10,
        brokers: ['a:1', 'b:2'],
      })
    })
    test('parseArgs reads the window, partitions, compression and settings flags and refuses a duration with --messages or on consume', () => {
      const parse = (...extra) => parseArgs(['--client', '/c', '--package', 'kafkajs', ...extra])

      expect(
        parse(
          '--workload',
          'produce-throughput',
          '--duration-ms',
          '30000',
          '--partitions',
          '12',
          '--compression',
          'lz4'
        )
      ).toMatchObject({ messages: null, durationMs: 30000, partitions: 12, compression: 'lz4' })
      expect(parse('--workload', 'consume-throughput', '--settings', 'same')).toMatchObject({
        messages: 200000,
        durationMs: null,
        settings: 'same',
      })
      for (const bad of [
        ['--workload', 'produce-throughput', '--duration-ms', '30000', '--messages', '5'],
        ['--workload', 'consume-throughput', '--duration-ms', '30000'],
        ['--workload', 'produce-throughput', '--compression', 'zstd'],
        ['--workload', 'consume-throughput', '--settings', 'other'],
        ['--workload', 'produce-throughput', '--partitions', '0'],
        ['--workload', 'produce-throughput', '--duration-ms', '1.5'],
      ]) {
        expect(() => parse(...bad)).toThrow(/usage/)
      }
    })

    test('parseArgs reads --batch, --unawaited, --keys, --headers, --partitioner and --value-type on the produce workloads, gzip as a compression, and leaves the flags absent otherwise', () => {
      const parse = (...extra) => parseArgs(['--client', '/c', '--package', 'kafkajs', ...extra])

      expect(
        parse(
          '--workload',
          'produce-throughput',
          '--batch',
          '8',
          '--keys',
          'index',
          '--headers',
          '20',
          '--partitioner',
          'custom',
          '--value-type',
          'string',
          '--compression',
          'gzip'
        )
      ).toMatchObject({
        batch: 8,
        keys: 'index',
        headers: 20,
        partitioner: 'custom',
        valueType: 'string',
        compression: 'gzip',
      })
      expect(parse('--workload', 'produce-throughput', '--unawaited', '100')).toMatchObject({
        unawaited: 100,
      })
      expect(parse('--workload', 'produce-latency', '--value-type', 'buffer')).toMatchObject({
        valueType: 'buffer',
      })
      const plain = parse('--workload', 'produce-throughput')
      for (const key of ['batch', 'unawaited', 'keys', 'headers', 'partitioner', 'valueType']) {
        expect([key, plain[key]]).toEqual([key, undefined])
      }
    })

    test('parseArgs refuses the shape flags outside produce-throughput, a bad key mode, partitioner, value type or count, --batch with --unawaited, and a custom partitioner with --settings same', () => {
      const parse = (...extra) => parseArgs(['--client', '/c', '--package', 'kafkajs', ...extra])

      for (const bad of [
        ['--workload', 'consume-throughput', '--batch', '8'],
        ['--workload', 'consume-throughput', '--unawaited', '2'],
        ['--workload', 'consume-throughput', '--keys', 'index'],
        ['--workload', 'consume-throughput', '--headers', '2'],
        ['--workload', 'consume-throughput', '--partitioner', 'custom'],
        ['--workload', 'consume-throughput', '--value-type', 'string'],
        ['--workload', 'produce-latency', '--batch', '8'],
        ['--workload', 'produce-latency', '--unawaited', '8'],
        ['--workload', 'produce-latency', '--keys', 'index'],
        ['--workload', 'produce-latency', '--headers', '8'],
        ['--workload', 'produce-latency', '--partitioner', 'custom'],
        ['--workload', 'produce-throughput', '--value-type', 'bytes'],
        ['--workload', 'produce-throughput', '--keys', '1024'],
        ['--workload', 'produce-throughput', '--partitioner', 'random'],
        ['--workload', 'produce-throughput', '--batch', '0'],
        ['--workload', 'produce-throughput', '--headers', '1.5'],
        ['--workload', 'produce-throughput', '--batch', '8', '--unawaited', '8'],
        ['--workload', 'produce-throughput', '--partitioner', 'custom', '--settings', 'same'],
        ['--workload', 'produce-throughput', '--max-request-bytes', '1048576'],
        ['--workload', 'produce-throughput', '--concurrency', '4'],
      ]) {
        expect(() => parse(...bad)).toThrow(/usage/)
      }
    })

    test('parseArgs reads --probe on on produce-throughput only, and leaves it absent otherwise', () => {
      const parse = (...extra) => parseArgs(['--client', '/c', '--package', 'kafkajs', ...extra])

      expect(parse('--workload', 'produce-throughput', '--probe', 'on')).toMatchObject({
        probe: 'on',
      })
      expect(parse('--workload', 'produce-throughput').probe).toBeUndefined()
      for (const bad of [
        ['--workload', 'produce-latency', '--probe', 'on'],
        ['--workload', 'consume-throughput', '--probe', 'on'],
        ['--workload', 'produce-throughput', '--probe', 'off'],
        ['--workload', 'produce-throughput', '--probe'],
      ]) {
        expect(() => parse(...bad)).toThrow(/usage/)
      }
    })

    test('parseArgs reads --passes on the consume workload only and defaults it to one', () => {
      const parse = (...extra) => parseArgs(['--client', '/c', '--package', 'kafkajs', ...extra])

      expect(parse('--workload', 'consume-throughput', '--passes', '4').passes).toBe(4)
      expect(parse('--workload', 'consume-throughput').passes).toBe(1)
      for (const bad of [
        ['--workload', 'produce-throughput', '--passes', '2'],
        ['--workload', 'consume-throughput', '--passes', '0'],
      ]) {
        expect(() => parse(...bad)).toThrow(/usage/)
      }
    })
  })

  describe('module', () => {
    test('requires nothing from the fork tree: only Node built-ins', () => {
      const source = fs.readFileSync(SCRIPT, 'utf8')
      const specifiers = [...source.matchAll(/\brequire\(\s*['"`]([^'"`]+)['"`]\s*\)/g)].map(
        (match) => match[1]
      )
      const builtin = (id) => id.startsWith('node:') || builtinModules.includes(id)

      expect(specifiers.filter((id) => !builtin(id))).toEqual([])
      expect(source).not.toMatch(/\.\.\/\.\.\/(src|index|testHelpers)/)
    })
  })

  describe('runWorkload', () => {
    const run = (workload, messages, kafka = fakeKafka()) =>
      runWorkload(
        kafka.client,
        workload,
        { messages, size: 8, brokers: BROKERS },
        kafka.timing
      ).then((metrics) => ({ metrics, ...kafka }))

    test('produce-latency sends one single-message batch per record after one untimed warm-up', async () => {
      const { metrics, state } = await run('produce-latency', 40)

      expect(state.sends).toEqual(Array(41).fill(1))
      expect(metrics).toEqual({
        p50: 21,
        p90: 37,
        p99: 40,
        elapsedMs: 820,
        measuredMessages: 40,
        messagesPerSecond: (40 / 820) * 1000,
        settings: { defaults: null },
      })
    })

    test('produce-throughput sends batches of 500 and the last batch carries the remainder', async () => {
      const { metrics, state } = await run('produce-throughput', 1200)

      expect(state.sends).toEqual([1, 500, 500, 200])
      expect(metrics).toEqual({
        elapsedMs: 6,
        messagesPerSecond: 200000,
        measuredMessages: 1200,
        cpuMicrosPerMessage: 1500 / 1200,
        settings: { defaults: null },
      })
    })

    test('consume-throughput waits for the final callback and measures only the delivery interval', async () => {
      const kafka = fakeKafka({ manualDelivery: true })
      const { state } = kafka
      let completed = false
      const pending = run('consume-throughput', 50, kafka).then((result) => {
        completed = true
        return result
      })
      await state.ready

      expect(state.sends).toEqual([50])
      expect(state.subscription).toMatchObject({ fromBeginning: true })
      expect([...state.timers.values()].map(({ delay }) => delay)).toEqual([300000])
      await state.deliver(49)
      expect(state.callbacks).toBe(49)
      expect(completed).toBe(false)
      expect(state.closed).toEqual(['producer'])
      expect(state.deletedGroups).toEqual([])
      expect(state.deleted).toEqual([])

      await state.deliver(1)
      const { metrics } = await pending

      expect(state.callbacks).toBe(50)
      expect(metrics).toMatchObject({
        elapsedMs: 450,
        measuredMessages: 45,
        messagesPerSecond: 100,
      })
      expect(state.events).toEqual([
        'disconnect producer',
        'disconnect consumer',
        'delete groups',
        'delete topics',
        'disconnect admin',
      ])
      expect(state.deletedGroups).toEqual(state.consumerGroups)
      expect(state.deleted).toEqual([...state.topics.keys()])
      expect(state.timers.size).toBe(0)
    })

    test('deletes its group and topic after shutting down a failed consumer', async () => {
      const kafka = fakeKafka({ failRun: true })

      await expect(run('consume-throughput', 5, kafka)).rejects.toThrow('run failed')

      expect(kafka.state.deletedGroups).toEqual(kafka.state.consumerGroups)
      expect(kafka.state.deletedGroups).toHaveLength(1)
      expect(kafka.state.deleted).toEqual([...kafka.state.topics.keys()])
      expect(kafka.state.events).toEqual([
        'disconnect producer',
        'disconnect consumer',
        'delete groups',
        'delete topics',
        'disconnect admin',
      ])
      expect(kafka.state.timers.size).toBe(0)
    })

    test('a consume timeout rejects with the delivery count and releases its timer and clients', async () => {
      const kafka = fakeKafka({ manualDelivery: true })
      const pending = run('consume-throughput', 5, kafka).catch((error) => error)
      await kafka.state.ready
      await kafka.state.deliver(2)

      expect(kafka.state.timers.size).toBe(1)
      const [{ callback }] = kafka.state.timers.values()
      callback()
      expect(await pending).toEqual(new Error('received 2 of 5 messages in time'))

      expect(kafka.state.timers.size).toBe(0)
      expect(kafka.state.closed).toEqual(['producer', 'consumer', 'admin'])
      expect(kafka.state.deletedGroups).toEqual(kafka.state.consumerGroups)
      expect(kafka.state.deleted).toEqual([...kafka.state.topics.keys()])
    })

    test('disconnects the producer and admin when producer connect rejects', async () => {
      const kafka = fakeKafka({ failConnect: 'producer' })

      await expect(run('produce-latency', 5, kafka)).rejects.toThrow('producer connect failed')

      expect(kafka.state.closed).toEqual(['producer', 'admin'])
      expect(kafka.state.deleted).toEqual([...kafka.state.topics.keys()])
    })

    test('disconnects the consumer and admin when consumer connect rejects', async () => {
      const kafka = fakeKafka({ failConnect: 'consumer' })

      await expect(run('consume-throughput', 5, kafka)).rejects.toThrow('consumer connect failed')

      expect(kafka.state.closed).toEqual(['producer', 'consumer', 'admin'])
      expect(kafka.state.deleted).toEqual([...kafka.state.topics.keys()])
    })

    test('disconnects the admin when admin connect rejects', async () => {
      const kafka = fakeKafka({ failConnect: 'admin' })

      await expect(run('produce-latency', 5, kafka)).rejects.toThrow('admin connect failed')

      expect(kafka.state.closed).toEqual(['admin'])
      expect(kafka.state.sends).toEqual([])
    })

    test('names the topic and the consumer group with the pid and the time', async () => {
      const { state } = await run('consume-throughput', 5)

      const [topic] = state.topics.keys()
      expect(topic).toMatch(new RegExp(`^bench-compare-consume-throughput-${process.pid}-\\d+$`))
      expect(state.consumerGroups[0]).toContain(topic)
      expect(state.configs[0].brokers).toEqual(BROKERS)
      expect(state.configs[0].logLevel).toBe(0)
    })

    test('deletes its topic and disconnects every client when the workload fails', async () => {
      const kafka = fakeKafka({ failSend: true })

      await expect(run('produce-latency', 5, kafka)).rejects.toThrow('send failed')

      expect(kafka.state.deleted).toEqual([...kafka.state.topics.keys()])
      expect(kafka.state.closed).toEqual(expect.arrayContaining(['producer', 'admin']))
    })

    test('rejects an unknown workload', async () => {
      await expect(run('nope', 5)).rejects.toThrow(/workload/)
    })

    const runWith = (workload, options, kafka = fakeKafka()) =>
      runWorkload(
        kafka.client,
        workload,
        { size: 8, brokers: BROKERS, ...options },
        kafka.timing
      ).then((metrics) => ({ metrics, ...kafka }))

    test('produce-throughput with --duration-ms sends through an unmeasured warm-up and measures only the timed window', async () => {
      const { metrics, state } = await runWith(
        'produce-throughput',
        { messages: null, durationMs: 30000 },
        fakeKafka({ sendMs: 100 })
      )

      expect(state.sends).toEqual([1, ...Array(50).fill(500), ...Array(300).fill(500)])
      expect(metrics).toEqual({
        elapsedMs: 30000,
        measuredMessages: 150000,
        messagesPerSecond: 5000,
        cpuMicrosPerMessage: 1500 / 150000,
        settings: { defaults: null },
      })
    })

    test('produce-throughput with --batch sends that many records per send, warm-up and window alike', async () => {
      const { metrics, state } = await runWith(
        'produce-throughput',
        { messages: null, durationMs: 30000, batch: 8 },
        fakeKafka({ sendMs: 100 })
      )

      expect(state.sends).toEqual([1, ...Array(350).fill(8)])
      expect(metrics).toMatchObject({ elapsedMs: 30000, measuredMessages: 300 * 8 })
      expect(metrics.messagesPerSecond).toBeCloseTo(80, 6)
    })

    test('produce-throughput with --unawaited issues that many one-record sends before awaiting any, and counts every one', async () => {
      const kafka = fakeKafka({ sendMs: 100 })
      const flight = trackInFlight(kafka)

      const { metrics, state } = await runWith(
        'produce-throughput',
        { messages: null, durationMs: 30000, unawaited: 4 },
        kafka
      )

      expect(flight.peak).toBe(4)
      expect(state.sends).toEqual([1, ...Array(52 + 300).fill(1)])
      expect(metrics).toMatchObject({ elapsedMs: 30000, measuredMessages: 300 })
    })

    test('produce-throughput with --unawaited and a message count issues each round of that many sends together, waits for every reply before the next round, and ends on a shorter round', async () => {
      const kafka = fakeKafka()
      const held = holdReplies(kafka)

      const pending = runWith('produce-throughput', { messages: 10, unawaited: 4 }, kafka)
      await until(() => held.sizes.length === 1)
      held.answer(1)
      await until(() => held.sizes.length === 5)
      expect(held.unanswered()).toBe(4)
      held.answer(3)
      await turns()
      expect(held.sizes).toHaveLength(5)
      held.answer(1)
      await until(() => held.sizes.length === 9)
      expect(held.unanswered()).toBe(4)
      held.answer(4)
      await until(() => held.sizes.length === 11)
      expect(held.unanswered()).toBe(2)
      held.answer(2)
      const { metrics } = await pending

      expect(held.sizes).toEqual(Array(11).fill(1))
      expect(metrics.measuredMessages).toBe(10)
    })

    test('produce-throughput sends one send at a time unless --unawaited says otherwise', async () => {
      const kafka = fakeKafka({ sendMs: 100 })
      const flight = trackInFlight(kafka)

      await runWith('produce-throughput', { messages: 1200, durationMs: null }, kafka)

      expect(flight.peak).toBe(1)
    })

    test('produce-throughput with --keys gives message n of the window a key of n in seven digits across sends, the first send and the warm-up keyed too, and no key without it', async () => {
      const keyed = await runWith(
        'produce-throughput',
        { messages: null, durationMs: 30000, batch: 3, keys: 'index' },
        fakeKafka({ sendMs: 100 })
      )
      const sized = await runWith('produce-throughput', { messages: 25, batch: 7, keys: 'index' })
      const plain = await runWith('produce-throughput', { messages: 5 })

      const logOf = ({ state }) => state.topics.values().next().value
      const seven = (n) => String(n).padStart(7, '0')
      const [first, ...rest] = logOf(keyed)
      const warmup = rest.slice(0, 50 * 3)
      const window = rest.slice(50 * 3)
      expect(first.key).toBe(seven(0))
      expect(warmup.every(({ key }) => typeof key === 'string' && key.length === 7)).toBe(true)
      expect(window).toHaveLength(300 * 3)
      expect(window.map(({ key }) => key)).toEqual(
        Array.from({ length: window.length }, (_, n) => seven(n))
      )
      expect(sized.state.sends).toEqual([1, 7, 7, 7, 4])
      expect(logOf(sized).map(({ key }) => key)).toEqual([
        seven(0),
        ...Array.from({ length: 25 }, (_, n) => seven(n)),
      ])
      expect(logOf(plain).some((record) => 'key' in record)).toBe(false)
    })

    test('produce-throughput numbers the records of its window from 0 whatever its warm-up sent, so two packages send the same keys and values in it', async () => {
      const many = await runWith(
        'produce-throughput',
        { messages: null, durationMs: 30000, batch: 3, keys: 'index' },
        fakeKafka({ sendMs: 100 })
      )
      const few = await runWith(
        'produce-throughput',
        { messages: null, durationMs: 30000, batch: 3, keys: 'index' },
        fakeKafka({ sendMs: 250 })
      )

      const windowOf = ({ state }, warmupSends) =>
        state.topics
          .values()
          .next()
          .value.slice(1 + warmupSends * 3, 1 + warmupSends * 3 + 90)
          .map(({ key, value }) => [key, String(value)])
      expect(many.state.sends.slice(1, 51)).toEqual(Array(50).fill(3))
      expect(few.state.sends.slice(1, 21)).toEqual(Array(20).fill(3))
      expect(many.state.sends[51]).toBe(3)
      expect(few.state.sends[21]).toBe(3)
      const [manyWindow, fewWindow] = [windowOf(many, 50), windowOf(few, 20)]
      expect(manyWindow[0][0]).toBe('0000000')
      expect(fewWindow).toEqual(manyWindow)
    })

    test('produce-throughput with --headers gives every record of the run, the first send included, that many string headers', async () => {
      const { state } = await runWith('produce-throughput', { messages: 5, headers: 3 })

      const records = state.topics.values().next().value
      expect(records).toHaveLength(6)
      for (const { headers } of records) expect(headers).toEqual({ h0: 'v0', h1: 'v1', h2: 'v2' })
    })

    test('produce-throughput walks the corpus on a message count, repeats the first values on a timed run without a shape flag, and walks it on a timed run with one', async () => {
      const pool = valuePool(8).map(String)
      const counted = await runWith('produce-throughput', { messages: 1200 })
      const timed = await runWith(
        'produce-throughput',
        { messages: null, durationMs: 30000 },
        fakeKafka({ sendMs: 100 })
      )
      const shaped = await runWith(
        'produce-throughput',
        { messages: null, durationMs: 30000, batch: 8 },
        fakeKafka({ sendMs: 100 })
      )

      const valuesOf = ({ state }) =>
        state.topics
          .values()
          .next()
          .value.map(({ value }) => String(value))
      expect(valuesOf(counted)).toEqual([
        pool[0],
        ...Array.from({ length: 1200 }, (_, i) => pool[i % 1024]),
      ])
      const timedValues = valuesOf(timed)
      expect(timedValues).toHaveLength(1 + 350 * 500)
      expect(timedValues[0]).toBe(pool[0])
      expect(timedValues.slice(1).every((value, i) => value === pool[i % 500])).toBe(true)
      const shapedWindow = valuesOf(shaped).slice(1 + 50 * 8)
      expect(shapedWindow).toHaveLength(300 * 8)
      expect(shapedWindow.every((value, i) => value === pool[i % 1024])).toBe(true)
    })

    test('--partitioner custom gives the producer a plain partitioner that places record n in partition n % partitions, and the default gives it none', async () => {
      const custom = await runWith('produce-throughput', { messages: 5, partitioner: 'custom' })
      const plain = await runWith('produce-throughput', { messages: 5 })

      const [{ createPartitioner }] = custom.state.producerConfigs
      const place = createPartitioner()
      const metadata = { partitionMetadata: Array(3).fill({}) }
      expect(Array.from({ length: 7 }, () => place(metadata))).toEqual([0, 1, 2, 0, 1, 2, 0])
      expect(createPartitioner()(metadata)).toBe(1)
      expect(plain.state.producerConfigs).toEqual([{}])
    })

    test('customRouting keeps one count for every partitioner it creates and starts it again at reset', () => {
      const routing = customRouting()
      const first = routing.createPartitioner()
      const second = routing.createPartitioner()
      const metadata = { partitionMetadata: Array(4).fill({}) }

      expect([first(metadata), second(metadata), first(metadata)]).toEqual([0, 1, 2])
      routing.reset()
      expect([second(metadata), first(metadata)]).toEqual([0, 1])
    })

    test('the custom routing of the measured window starts at partition 0 whatever the warm-up sent, so two packages route the same records alike', async () => {
      const routed = async (sendMs) => {
        const kafka = fakeKafka({ sendMs })
        const routes = []
        const create = kafka.client.Kafka.prototype.producer
        kafka.client.Kafka.prototype.producer = function producer(config) {
          const created = create.call(this, config)
          const place = config.createPartitioner()
          const send = created.send
          created.send = (request) => {
            const metadata = { partitionMetadata: Array(12).fill({}) }
            routes.push(request.messages.map(() => place(metadata)))
            return send(request)
          }
          return created
        }
        await runWith(
          'produce-throughput',
          { messages: null, durationMs: 30000, partitions: 12, batch: 5, partitioner: 'custom' },
          kafka
        )
        return { routes, warmupSends: Math.floor(5000 / sendMs) }
      }

      const many = await routed(100)
      const few = await routed(250)

      const windowOf = ({ routes, warmupSends }) =>
        routes.slice(1 + warmupSends, 1 + warmupSends + 3)
      expect(many.warmupSends).not.toBe(few.warmupSends)
      expect(windowOf(many)).toEqual([
        [0, 1, 2, 3, 4],
        [5, 6, 7, 8, 9],
        [10, 11, 0, 1, 2],
      ])
      expect(windowOf(few)).toEqual(windowOf(many))
    })

    test('produce-throughput with --probe on reports the send-call time, peak memory and collections of the measured window only', async () => {
      const kafka = fakeKafka()

      const { metrics, state } = await runWith(
        'produce-throughput',
        { messages: 1200, durationMs: null, probe: 'on' },
        kafka
      )

      // The first send takes 1000 ms on this fake and the three measured sends 1, 2 and 3 ms.
      expect(metrics.sendCallMicrosP50).toBeGreaterThanOrEqual(2000)
      expect(metrics.sendCallMicrosP50).toBeLessThan(2010)
      expect(metrics.sendCallMicrosP99).toBeGreaterThanOrEqual(3000)
      expect(metrics.sendCallMicrosP99).toBeLessThan(3010)
      expect(metrics).toMatchObject({
        elapsedMs: 6,
        peakRssBytes: 1040,
        peakArrayBuffers: 54,
        gcCountPer10kMessages: 25,
        gcPauseMsPer10kMessages: 12.5,
      })
      expect(Object.hasOwn(metrics, 'shape')).toBe(false)
      expect(state.intervals.map(({ delay }) => delay)).toEqual([100])
      expect(state.clearedIntervals).toEqual([state.intervals[0].timer])
      expect(state.allGcs).toMatchObject({ started: 1, stopped: 1, resulted: 1, closed: 1 })
    })

    test('the collections of a probed run are counted over the exact clock readings that bound the rate window', async () => {
      const kafka = fakeKafka()
      const reads = []
      const read = kafka.timing.now
      kafka.timing.now = () => {
        const reading = read() + BigInt(reads.length) * 1000000n
        reads.push(reading)
        return reading
      }

      const { metrics, state } = await runWith(
        'produce-throughput',
        { messages: 1200, durationMs: null, probe: 'on' },
        kafka
      )

      // Every read advances this clock by 1 ms: the leader wait, the window start, two reads for
      // each of the three timed calls, and the window end.
      expect(reads).toHaveLength(9)
      expect(state.allGcs.from).toBe(reads[1])
      expect(state.allGcs.to).toBe(reads[8])
      expect(Number(state.allGcs.to - state.allGcs.from) / 1e6).toBe(metrics.elapsedMs)
    })

    test('the send-call time starts once the request is built and stops when send returns its promise, before the reply', async () => {
      const kafka = fakeKafka({ sendMs: 5 })
      const log = []
      const read = kafka.timing.now
      kafka.timing.now = () => {
        log.push('clock')
        return read()
      }
      const create = kafka.client.Kafka.prototype.producer
      kafka.client.Kafka.prototype.producer = function producer(config) {
        const created = create.call(this, config)
        const send = created.send
        created.send = (request) => {
          log.push('send')
          return send(request).then(() => kafka.timing.sleep(700))
        }
        return created
      }
      const padStart = String.prototype.padStart
      const spy = jest.spyOn(String.prototype, 'padStart').mockImplementation(function (...args) {
        if (args[0] === 7 && args[1] === '0') log.push('built')
        return padStart.apply(this, args)
      })

      let result
      try {
        result = await runWith(
          'produce-throughput',
          { messages: 3, batch: 1, keys: 'index', probe: 'on' },
          kafka
        )
      } finally {
        spy.mockRestore()
      }

      const sends = log.flatMap((entry, at) => (entry === 'send' ? [at] : []))
      expect(sends).toHaveLength(4)
      expect(log[sends[0] - 1]).toBe('built')
      for (const at of sends.slice(1)) {
        expect(log.slice(at - 2, at + 2)).toEqual(['built', 'clock', 'send', 'clock'])
      }
      expect(result.metrics.sendCallMicrosP50).toBeGreaterThanOrEqual(5000)
      expect(result.metrics.sendCallMicrosP50).toBeLessThan(5050)
      expect(result.metrics.sendCallMicrosP99).toBeLessThan(5050)
    })

    test('a run without --probe adds no clock read per send, starts no memory sampling or collection observer, and reports none of their metrics', async () => {
      const clockReads = async (messages, probe) => {
        const kafka = fakeKafka()
        let reads = 0
        const read = kafka.timing.now
        kafka.timing.now = () => {
          reads++
          return read()
        }
        const { metrics, state } = await runWith(
          'produce-throughput',
          { messages, durationMs: null, ...(probe && { probe }) },
          kafka
        )
        return { reads, metrics, state }
      }

      const one = await clockReads(500)
      const three = await clockReads(1500)
      const probedOne = await clockReads(500, 'on')
      const probedThree = await clockReads(1500, 'on')

      expect(three.reads).toBe(one.reads)
      expect(probedThree.reads - probedOne.reads).toBe(2 * 2)
      expect(Object.keys(three.metrics).sort()).toEqual([
        'cpuMicrosPerMessage',
        'elapsedMs',
        'measuredMessages',
        'messagesPerSecond',
        'settings',
      ])
      expect(three.state.intervals).toEqual([])
      expect(three.state.allGcs).toEqual({ started: 0, stopped: 0, resulted: 0, closed: 0 })
      expect(probedThree.state.allGcs).toMatchObject({ started: 1 })
    })

    test('a produce run that fails inside its window stops sampling memory and recording collections', async () => {
      const kafka = fakeKafka({ failAfter: 2 })

      await expect(
        runWith('produce-throughput', { messages: 1200, durationMs: null, probe: 'on' }, kafka)
      ).rejects.toThrow('send failed')

      expect(kafka.state.clearedIntervals).toEqual([kafka.state.intervals[0].timer])
      expect(kafka.state.allGcs).toMatchObject({ started: 1, stopped: 0, resulted: 0, closed: 1 })
    })

    test('produce-latency with --duration-ms times single sends only after the warm-up', async () => {
      const { metrics, state } = await runWith(
        'produce-latency',
        { messages: null, durationMs: 30000 },
        fakeKafka({ sendMs: 100 })
      )

      expect(state.sends).toEqual(Array(1 + 50 + 300).fill(1))
      expect(metrics).toEqual({
        p50: 100,
        p90: 100,
        p99: 100,
        elapsedMs: 30000,
        measuredMessages: 300,
        messagesPerSecond: 10,
        settings: { defaults: null },
      })
    })

    test('consume-throughput reports CPU per message, event-loop delay and Fetch requests and bytes of the measured window only', async () => {
      const kafka = fakeKafka({ manualDelivery: true })
      const { state } = kafka
      const pending = runWith('consume-throughput', { messages: 100 }, kafka)
      await state.ready

      state.request({ apiName: 'Fetch', size: 999999, apiVersion: 11 })
      await state.deliver(9)
      state.request({ apiName: 'Fetch', size: 999999, apiVersion: 11 })
      await state.deliver(1)
      state.request({ apiName: 'Fetch', size: 4000, apiVersion: 17 })
      state.request({ apiName: 'Metadata', size: 777, apiVersion: 12 })
      await state.deliver(45)
      state.request({ apiName: 'Fetch', size: 2000, apiVersion: 17 })
      await state.deliver(45)
      state.request({ apiName: 'Fetch', size: 999999, apiVersion: 18 })
      const { metrics } = await pending

      expect(state.delay).toEqual({ enabledAt: [10], disabledAt: [100, 100] })
      expect(metrics).toEqual({
        elapsedMs: 900,
        measuredMessages: 90,
        messagesPerSecond: 100,
        cpuMicrosPerMessage: (600 * 100 + 1200 + 300) / 90,
        threadCpuUsageSupported: true,
        mainThreadCpuMicrosPerMessage: (400 * 100 + 800 + 100) / 90,
        fullGcCount: 3,
        eventLoopDelayP50: 50,
        eventLoopDelayP99: 99,
        eventLoopDelayMax: 250,
        fetchRequests: 2,
        bytesPerFetch: 3000,
        messagesPerFetch: 45,
        fetchVersion: 17,
        passes: 1,
        corpus: CORPUS_LAYOUT,
        settings: { defaults: null },
      })
    })

    test('consume-throughput writes one corpus layout on both arms under either settings: record i to partition i % partitions, sends of 500, the corpus producer settings', async () => {
      const runs = []
      for (const name of ['@countly/kafkajs', 'kafkajs']) {
        for (const settings of ['defaults', 'same']) {
          const kafka = fakeKafka()
          kafka.client.name = name
          runs.push(await runWith('consume-throughput', { messages: 1200, settings }, kafka))
        }
      }
      const layoutOf = ({ state, metrics }) => {
        const [records] = state.topics.values()
        return {
          producerConfigs: state.producerConfigs,
          sends: state.sends,
          partitions: records.map(({ partition }) => partition),
          corpus: metrics.corpus,
        }
      }
      const expected = {
        producerConfigs: [CORPUS_PRODUCER_SETTINGS],
        sends: [500, 500, 200],
        partitions: Array.from({ length: 1200 }, (_, i) => i % 3),
        corpus: CORPUS_LAYOUT,
      }

      for (const run of runs) expect(layoutOf(run)).toEqual(expected)
      expect(CORPUS_PRODUCER_SETTINGS).toEqual({
        idempotent: false,
        maxInFlightRequests: null,
        retry: { retries: 5 },
        linger: 0,
        batchSize: 1,
      })
      expect(CORPUS_LAYOUT).toEqual({
        partitioning: 'record i to partition i % partitions',
        recordsPerSend: 500,
        concurrentSends: 8,
        producer: CORPUS_PRODUCER_SETTINGS,
      })
    })

    test('consume-throughput spreads the corpus over the requested partitions', async () => {
      const { state } = await runWith('consume-throughput', { messages: 30, partitions: 12 })

      const [records] = state.topics.values()
      expect(records.map(({ partition }) => partition)).toEqual(
        Array.from({ length: 30 }, (_, i) => i % 12)
      )
    })

    test('consume-throughput counts main-thread CPU and full GCs over the measured window only', async () => {
      const kafka = fakeKafka({ manualDelivery: true })
      const pending = runWith('consume-throughput', { messages: 100 }, kafka)
      await kafka.state.ready
      await kafka.state.deliver(9)

      expect(kafka.state.gc).toEqual({ startedAt: [], stoppedAt: [], countedAt: [], closedAt: [] })
      await kafka.state.deliver(91)
      const { metrics } = await pending

      expect(kafka.state.gc).toEqual({
        startedAt: [10],
        stoppedAt: [100],
        countedAt: [100],
        closedAt: [100],
      })
      expect(metrics.mainThreadCpuMicrosPerMessage).toBe((400 * 100 + 800 + 100) / 90)
      expect(metrics.fullGcCount).toBe(3)
    })

    test('consume-throughput leaves main-thread CPU out, without failing, on a Node without process.threadCpuUsage', async () => {
      const { metrics } = await runWith(
        'consume-throughput',
        { messages: 100 },
        fakeKafka({ threadCpu: false })
      )

      expect(Object.hasOwn(metrics, 'mainThreadCpuMicrosPerMessage')).toBe(false)
      expect(metrics).toMatchObject({
        threadCpuUsageSupported: false,
        measuredMessages: 90,
        cpuMicrosPerMessage: (600 * 100 + 1200 + 300) / 90,
        fullGcCount: 3,
      })
    })

    test('a consume run that fails stops its full-GC count', async () => {
      const kafka = fakeKafka({ manualDelivery: true })
      const pending = runWith('consume-throughput', { messages: 10 }, kafka).catch((error) => error)
      await kafka.state.ready
      await kafka.state.deliver(2)
      const [{ callback }] = kafka.state.timers.values()
      callback()

      expect(await pending).toEqual(new Error('received 2 of 10 messages in time'))
      expect(kafka.state.gc).toEqual({
        startedAt: [1],
        stoppedAt: [],
        countedAt: [],
        closedAt: [2],
      })
    })

    describe('countFullGcs', () => {
      const major = (startTime) => ({
        startTime,
        detail: { kind: constants.NODE_PERFORMANCE_GC_MAJOR },
      })
      const minor = (startTime) => ({
        startTime,
        detail: { kind: constants.NODE_PERFORMANCE_GC_MINOR },
      })
      /** A `gc` observer the test drives: `deliver` calls its callback, `buffered` is what `takeRecords` returns. */
      const controlled = () => {
        const observers = []
        class Observer {
          constructor(callback) {
            this.callback = callback
            this.buffered = []
            this.disconnects = 0
            observers.push(this)
          }

          observe(options) {
            this.options = options
          }

          takeRecords() {
            const taken = this.buffered
            this.buffered = []
            return taken
          }

          disconnect() {
            this.disconnects++
          }

          deliver(...entries) {
            this.callback({ getEntries: () => entries })
          }
        }
        return { Observer, observers }
      }

      test('counts a full collection that started in the window though its entry arrives in the drain after stop, and not one that started after stop', async () => {
        const { Observer, observers } = controlled()
        let clock = 10
        let drained = false
        const drain = async () => {
          observer.deliver(major(15), minor(16))
          observer.deliver(major(25))
          observer.buffered.push(major(18), major(5))
          drained = true
        }
        const counter = countFullGcs({ Observer, now: () => clock, drain })
        const [observer] = observers
        observer.deliver(major(12), minor(13))
        clock = 20
        counter.stop()
        clock = 30
        counter.stop()

        expect(observer.options).toEqual({ entryTypes: ['gc'] })
        expect(drained).toBe(false)
        expect(await counter.count()).toBe(3)
        expect(drained).toBe(true)
        expect(observer.disconnects).toBe(1)
      })

      test('disconnects its observer once, whether it was counted or only closed', async () => {
        const { Observer, observers } = controlled()
        const counted = countFullGcs({ Observer, now: () => 0, drain: async () => {} })
        const closed = countFullGcs({ Observer, now: () => 0, drain: async () => {} })

        counted.stop()
        await counted.count()
        counted.close()
        closed.close()
        closed.close()

        expect(observers.map(({ disconnects }) => disconnects)).toEqual([1, 1])
      })
    })

    describe('trackGcs', () => {
      const entry = (startTime, duration, kind = constants.NODE_PERFORMANCE_GC_MINOR) => ({
        startTime,
        duration,
        detail: { kind },
      })
      /** A `gc` observer the test drives, as in `countFullGcs`'s tests. */
      const controlled = () => {
        const observers = []
        class Observer {
          constructor(callback) {
            this.callback = callback
            this.buffered = []
            this.disconnects = 0
            observers.push(this)
          }

          observe(options) {
            this.options = options
          }

          takeRecords() {
            const taken = this.buffered
            this.buffered = []
            return taken
          }

          disconnect() {
            this.disconnects++
          }

          deliver(...entries) {
            this.callback({ getEntries: () => entries })
          }
        }
        return { Observer, observers }
      }

      /** The monotonic reading, in ns, that stands for `ms` on the performance clock in these tests. */
      const hr = (ms) => BigInt(ms + 5000) * 1000000n

      test('counts every collection kind that started between the two marks, with its pause, whether its entry came before, during or after the window, and not one that started in the gap before the start mark or after the end mark', async () => {
        const { Observer, observers } = controlled()
        const { MAJOR, MINOR, INCREMENTAL, WEAKCB } = {
          MAJOR: constants.NODE_PERFORMANCE_GC_MAJOR,
          MINOR: constants.NODE_PERFORMANCE_GC_MINOR,
          INCREMENTAL: constants.NODE_PERFORMANCE_GC_INCREMENTAL,
          WEAKCB: constants.NODE_PERFORMANCE_GC_WEAKCB,
        }
        let clock = 10
        const drain = async () => {
          observer.deliver(entry(13, 5, MAJOR), entry(26, 3, WEAKCB))
          observer.buffered.push(entry(28, 1, MINOR), entry(31, 7, MAJOR))
        }
        const tracker = trackGcs({ Observer, now: () => clock, hrtime: () => hr(clock), drain })
        const [observer] = observers
        observer.deliver(entry(12, 9, MAJOR))
        clock = 20
        tracker.open(hr(20))
        observer.deliver(entry(19, 4, MAJOR), entry(21, 2, MAJOR), entry(24, 6, INCREMENTAL))
        clock = 30
        tracker.stop(hr(30))

        const result = await tracker.result()

        expect(new Set([MAJOR, MINOR, INCREMENTAL, WEAKCB]).size).toBe(4)
        expect(observer.options).toEqual({ entryTypes: ['gc'] })
        expect(result).toEqual({ count: 4, pauseMs: 12 })
        expect(observer.disconnects).toBe(1)
      })

      test('ends its window at the stop mark, not at result: a collection that started after it is not counted, whenever its entry arrives', async () => {
        const { Observer, observers } = controlled()
        let clock = 10
        const drain = async () => {
          observer.deliver(entry(18, 4), entry(26, 8))
        }
        const tracker = trackGcs({ Observer, now: () => clock, hrtime: () => hr(clock), drain })
        const [observer] = observers
        tracker.open(hr(10))
        observer.deliver(entry(11, 1))
        clock = 20
        tracker.stop(hr(20))
        clock = 50
        observer.deliver(entry(19, 2), entry(30, 100))

        const result = await tracker.result()

        expect(result).toEqual({ count: 3, pauseMs: 7 })
      })

      test('counts nothing before its window is opened, and disconnects its observer once whether it ended or was only closed', async () => {
        const { Observer, observers } = controlled()
        const ended = trackGcs({ Observer, now: () => 0, hrtime: () => 0n, drain: async () => {} })
        const closed = trackGcs({ Observer, now: () => 0, hrtime: () => 0n, drain: async () => {} })
        observers[0].deliver(entry(1, 5))

        expect(await ended.result()).toEqual({ count: 0, pauseMs: 0 })
        ended.close()
        closed.close()
        closed.close()

        expect(observers.map(({ disconnects }) => disconnects)).toEqual([1, 1])
      })
    })

    test('consume-throughput fails before consuming when a partition does not hold the records its layout puts there', async () => {
      const exact = [
        { partition: 0, offset: '2', high: '2', low: '0' },
        { partition: 1, offset: '2', high: '2', low: '0' },
        { partition: 2, offset: '2', high: '2', low: '0' },
      ]
      const retried = fakeKafka({ offsets: [exact[0], { ...exact[1], high: '3' }, exact[2]] })
      const missing = fakeKafka({ offsets: exact.slice(0, 2) })
      const shifted = fakeKafka({ offsets: [exact[0], exact[1], { ...exact[2], low: '1' }] })

      await expect(runWith('consume-throughput', { messages: 6 }, retried)).rejects.toThrow(
        'corpus partition 1 holds 3 records where its layout puts 2: a send was retried or lost, so the arms would not read the same corpus'
      )
      await expect(runWith('consume-throughput', { messages: 6 }, missing)).rejects.toThrow(
        'corpus partition 2 holds 0 records where its layout puts 2'
      )
      await expect(runWith('consume-throughput', { messages: 6 }, shifted)).rejects.toThrow(
        'corpus partition 2 holds 1 records where its layout puts 2'
      )
      for (const { state } of [retried, missing, shifted]) {
        expect(state.consumerConfigs).toEqual([])
        expect(state.deleted).toEqual([...state.topics.keys()])
      }
      const { metrics } = await runWith(
        'consume-throughput',
        { messages: 6 },
        fakeKafka({ offsets: exact })
      )
      expect(metrics.measuredMessages).toBe(5)
    })

    test('consume-throughput with --settings same gives the consumer one explicit configuration and reports it; defaults report the package defaults', async () => {
      const same = await runWith('consume-throughput', { messages: 5, settings: 'same' })
      const fork = fakeKafka()
      fork.client.name = '@countly/kafkajs'
      const defaults = await runWith('consume-throughput', { messages: 5 }, fork)

      expect(same.state.consumerConfigs).toEqual([
        { groupId: same.state.consumerGroups[0], ...SAME_SETTINGS },
      ])
      expect(same.metrics.settings).toEqual({ same: SAME_SETTINGS })
      expect(defaults.state.consumerConfigs).toEqual([
        { groupId: defaults.state.consumerGroups[0] },
      ])
      expect(defaults.metrics.settings).toEqual({ defaults: PACKAGE_DEFAULTS['@countly/kafkajs'] })
      for (const [key, value] of Object.entries(SAME_SETTINGS)) {
        expect([key, value]).toEqual([key, PACKAGE_DEFAULTS.kafkajs[key]])
      }
    })

    test('creates the topic with the requested partitions, the consume corpus with one replica, and quick file deletion', async () => {
      const produce = await runWith('produce-throughput', { messages: 5, partitions: 12 })
      const consume = await runWith('consume-throughput', { messages: 5 })
      const quick = [{ name: 'file.delete.delay.ms', value: '1000' }]

      expect(produce.state.createdTopics).toEqual([
        { topic: expect.any(String), numPartitions: 12, configEntries: quick },
      ])
      expect(consume.state.createdTopics).toEqual([
        { topic: expect.any(String), numPartitions: 3, replicationFactor: 1, configEntries: quick },
      ])
    })

    test('corpus values have the requested size and vary from record to record', () => {
      for (const size of [200, 1024]) {
        const values = valuePool(size)

        expect(values).toHaveLength(1024)
        expect(values.every((value) => value.length === size)).toBe(true)
        expect(new Set(values.map(String)).size).toBe(1024)
      }
      expect(valuePool(200).map(String)).toEqual(valuePool(200).map(String))
    })

    test('lz4 registers an lz4-napi frame codec on a client without one, keeps a built-in codec, and compresses every send', async () => {
      const bare = fakeKafka()
      bare.client.CompressionCodecs = {}
      bare.client.lz4Version = null
      const builtIn = fakeKafka()
      builtIn.client.CompressionCodecs = {}
      builtIn.client.lz4Version = '9.9.9'

      const registered = await runWith(
        'produce-throughput',
        { messages: 5, compression: 'lz4' },
        bare
      )
      const kept = await runWith('produce-throughput', { messages: 5, compression: 'lz4' }, builtIn)

      expect(registered.metrics.codec).toEqual({
        codec: 'registered',
        package: 'lz4-napi',
        version: LZ4_VERSION,
      })
      expect(registered.state.compressions).toEqual([3, 3])
      const codec = bare.client.CompressionCodecs[3]()
      const input = Buffer.from('a record, a record, a record, a record')
      const frame = await codec.compress({ buffer: input })
      expect(frame.readUInt32LE(0)).toBe(0x184d2204)
      expect(await codec.decompress(frame)).toEqual(input)
      expect(kept.metrics.codec).toEqual({
        codec: 'built-in',
        package: 'lz4-napi',
        version: '9.9.9',
      })
      expect(builtIn.client.CompressionCodecs).toEqual({})
      expect(kept.state.compressions).toEqual([3, 3])
    })

    test('gzip compresses every send with the built-in codec, registers nothing and reports the codec', async () => {
      const kafka = fakeKafka()
      kafka.client.CompressionCodecs = {}

      const { metrics, state } = await runWith(
        'produce-throughput',
        { messages: 5, compression: 'gzip' },
        kafka
      )

      expect(state.compressions).toEqual([1, 1])
      expect(kafka.client.CompressionCodecs).toEqual({})
      expect(metrics.codec).toEqual({
        name: 'gzip',
        codec: 'built-in',
        package: 'zlib',
        version: process.versions.zlib,
      })
    })

    test('produce workloads with --value-type string send string values with the corpus text, and Buffers otherwise', async () => {
      const asString = await runWith('produce-throughput', { messages: 5, valueType: 'string' })
      const asLatency = await runWith('produce-latency', { messages: 3, valueType: 'string' })
      const asBuffer = await runWith('produce-throughput', { messages: 5 })
      const logOf = ({ state }) => state.topics.values().next().value

      expect(logOf(asString).map(({ value }) => typeof value)).toEqual(Array(6).fill('string'))
      expect(logOf(asLatency).map(({ value }) => typeof value)).toEqual(Array(4).fill('string'))
      expect(logOf(asString)[0].value).toBe(valuePool(8)[0].toString())
      expect(logOf(asBuffer).every(({ value }) => Buffer.isBuffer(value))).toBe(true)
    })

    test('a run reports its shape flags as shape, and no shape when none was given', async () => {
      const plain = await runWith('produce-throughput', { messages: 5 })
      const shaped = await runWith('produce-throughput', {
        messages: 5,
        batch: 2,
        keys: 'index',
        headers: 3,
        partitioner: 'custom',
        valueType: 'string',
      })
      const unawaited = await runWith('produce-throughput', { messages: 5, unawaited: 2 })

      expect(Object.hasOwn(plain.metrics, 'shape')).toBe(false)
      expect(shaped.metrics.shape).toEqual({
        batch: 2,
        keys: 'index',
        headers: 3,
        partitioner: 'custom',
        valueType: 'string',
      })
      expect(unawaited.metrics.shape).toEqual({ unawaited: 2 })
    })

    test('a run creates its topic with no setting beyond quick file deletion, whatever its shape', async () => {
      const { state } = await runWith('produce-throughput', {
        messages: 5,
        batch: 2,
        keys: 'index',
      })

      expect(state.createdTopics[0].configEntries).toEqual([
        { name: 'file.delete.delay.ms', value: '1000' },
      ])
    })

    test('the corpus holds at most 64 MiB: a 1 MiB record size gives 64 values of that size', () => {
      const values = valuePool(1048576)

      expect(values).toHaveLength(64)
      expect(values.every((value) => value.length === 1048576)).toBe(true)
      expect(values[0].equals(values[1])).toBe(false)
    })

    test('the fork consumer defaults it reports are the ones its source sets', () => {
      const consumer = fs.readFileSync(path.join(ROOT, 'src', 'consumer', 'index.js'), 'utf8')
      const client = fs.readFileSync(path.join(ROOT, 'src', 'index.js'), 'utf8')
      const timeouts = fs.readFileSync(
        path.join(ROOT, 'src', 'consumer', 'groupTimeouts.js'),
        'utf8'
      )
      const fork = PACKAGE_DEFAULTS['@countly/kafkajs']
      const named = Object.entries(fork).filter(
        ([key]) => !['pollTimeout', 'maxInFlightRequests', 'readUncommitted'].includes(key)
      )

      for (const [key, value] of named) {
        expect(consumer).toMatch(new RegExp(`\\n {4}${key} = ${value},`))
      }
      expect(client).toMatch(
        new RegExp(`DEFAULT_MAX_IN_FLIGHT_REQUESTS = ${fork.maxInFlightRequests}\\n`)
      )
      expect(client).toMatch(/\n {6}readUncommitted = false,\n/)
      expect(timeouts).toMatch(new RegExp(`DEFAULT_MAX_POLL_INTERVAL = ${fork.pollTimeout}\\n`))
    })

    test('waits for the new topic through unknown-topic and leaderless answers instead of createTopics waitForLeaders, which kafkajs 2.2.4 gives up on', async () => {
      const kafka = fakeKafka({ metadata: ['unknown', 'no leader', 'unknown'] })

      const { metrics, state } = await runWith('produce-throughput', { messages: 5 }, kafka)

      expect(metrics.measuredMessages).toBe(5)
      expect(state.createdTopics).toHaveLength(1)
      expect(state.metadataReads).toBe(4)
      expect(state.sleeps).toEqual([100, 100, 100])
    })

    test('every workload asks createTopics not to wait for leaders, which both packages do by default', async () => {
      for (const workload of WORKLOADS) {
        const { state } = await runWith(workload, { messages: 5 })

        expect([workload, state.leaderWaits]).toEqual([workload, [false]])
      }
    })

    test('gives up on a topic without leaders after 30 seconds with the last answer', async () => {
      const kafka = fakeKafka({ metadata: Array(400).fill('unknown') })

      await expect(runWith('produce-throughput', { messages: 5 }, kafka)).rejects.toThrow(
        'This server does not host this topic-partition'
      )
      expect(kafka.state.metadataReads).toBe(301)
      expect(kafka.state.sends).toEqual([])
      expect(kafka.state.deleted).toHaveLength(1)
    })

    test('consume-throughput with --passes reads the corpus that many times, seeking each partition back after its last record', async () => {
      const { metrics, state } = await runWith('consume-throughput', { messages: 50, passes: 3 })

      expect(state.seeks).toEqual(
        [2, 0, 1, 2, 0, 1].map((partition) => ({
          topic: state.subscription.topic,
          partition,
          offset: '0',
        }))
      )
      expect(state.callbacks).toBe(150)
      expect(metrics).toMatchObject({ measuredMessages: 145, elapsedMs: 1450, passes: 3 })
    })

    describe('consume-throughput replay completion', () => {
      const twoPartitions = [
        { partition: 0, offset: '3', high: '3', low: '0' },
        { partition: 1, offset: '3', high: '3', low: '0' },
      ]
      /** Starts a 6-record, two-pass replay over two partitions of three records each. */
      const replay = async () => {
        const kafka = fakeKafka({ manualDelivery: true, offsets: twoPartitions })
        let completed = false
        const pending = runWith(
          'consume-throughput',
          { messages: 6, passes: 2, partitions: 2 },
          kafka
        ).then(
          (result) => {
            completed = true
            return result
          },
          (error) => error
        )
        await kafka.state.ready
        const deliver = async (partition, offsets) => {
          for (const offset of offsets)
            await kafka.state.eachMessage({
              topic: kafka.state.subscription.topic,
              partition,
              message: { offset: String(offset) },
            })
          await new Promise(setImmediate)
        }
        return { kafka, pending, deliver, isCompleted: () => completed }
      }

      test('a duplicate delivery does not complete a pass: the run ends only when every partition delivered each pass in order', async () => {
        const { kafka, pending, deliver, isCompleted } = await replay()
        await deliver(0, [0, 1, 2, 0, 1, 2])
        await deliver(1, [0, 1, 2, 0, 0, 1])
        expect(isCompleted()).toBe(false)
        expect(kafka.state.seeks.map(({ partition, offset }) => [partition, offset])).toEqual([
          [0, '0'],
          [1, '0'],
        ])
        await deliver(1, [2])
        const { metrics } = await pending
        expect(isCompleted()).toBe(true)
        expect(metrics).toMatchObject({ measuredMessages: 11, passes: 2 })
        expect(kafka.state.seeks).toHaveLength(2)
      })

      test('a missing offset is never counted past: the replay times out with the deliveries in order', async () => {
        const { kafka, pending, deliver, isCompleted } = await replay()
        await deliver(0, [0, 1, 2, 0, 1, 2])
        await deliver(1, [0, 2])
        expect(isCompleted()).toBe(false)
        expect(kafka.state.seeks.map(({ partition }) => partition)).toEqual([0])
        const [{ callback }] = kafka.state.timers.values()
        callback()
        expect(await pending).toEqual(new Error('received 7 of 12 messages in time'))
      })
    })

    test('produce workloads with --settings same give the producer one explicit configuration and report it; defaults report the package defaults', async () => {
      const same = await runWith('produce-throughput', { messages: 5, settings: 'same' })
      const fork = fakeKafka()
      fork.client.name = '@countly/kafkajs'
      const defaults = await runWith('produce-latency', { messages: 5 }, fork)

      expect(same.state.producerConfigs).toEqual([
        { ...SAME_PRODUCER_SETTINGS, createPartitioner: legacyPartitioner },
      ])
      expect(same.metrics.settings).toEqual({
        same: { ...SAME_PRODUCER_SETTINGS, partitioner: 'DefaultPartitioner' },
      })
      expect(defaults.state.producerConfigs).toEqual([{}])
      expect(defaults.metrics.settings).toEqual({ defaults: PRODUCER_DEFAULTS['@countly/kafkajs'] })
      expect(SAME_PRODUCER_SETTINGS).toEqual({
        idempotent: false,
        maxInFlightRequests: null,
        retry: { retries: 5 },
        linger: 0,
        batchSize: 1048576,
      })
    })

    test('the fork producer defaults it reports are the ones its source sets', () => {
      const producer = fs.readFileSync(path.join(ROOT, 'src', 'producer', 'index.js'), 'utf8')
      const client = fs.readFileSync(path.join(ROOT, 'src', 'index.js'), 'utf8')
      const fork = PRODUCER_DEFAULTS['@countly/kafkajs']

      expect(producer).toMatch(
        new RegExp(`\\n {2}linger: ${fork.linger},\\n {2}batchSize: ${fork.batchSize},`)
      )
      expect(producer).toMatch(new RegExp(`\\n {2}retries: ${fork.retries},\\n`))
      expect(producer).toMatch(/\n {2}adaptive: true,\n/)
      expect(producer).toMatch(/UniformStickyPartitioner\(\{ stickyBytes: batchSize/)
      expect(client).toMatch(
        new RegExp(`DEFAULT_MAX_IN_FLIGHT_REQUESTS = ${fork.maxInFlightRequests}\\n`)
      )
      expect(client).toMatch(/const requested = transactional \|\| idempotent == null \|\|/)
      expect(fork).toMatchObject({
        idempotent: true,
        acks: -1,
        partitioner: 'UniformStickyPartitioner (adaptive)',
      })
    })
  })
})
