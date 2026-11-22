const { spawnSync } = require('node:child_process')
const { builtinModules } = require('node:module')
const fs = require('node:fs')
const os = require('node:os')
const path = require('node:path')
const {
  parseArgs,
  runWorkload,
  valuePool,
  WORKLOADS,
  SAME_SETTINGS,
  PACKAGE_DEFAULTS,
  SAME_PRODUCER_SETTINGS,
  PRODUCER_DEFAULTS,
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
 * 2.2.4's does, when `createTopics` is asked to wait for leaders the broker does not know yet;
 * `metadata` lists what each `fetchTopicMetadata` answers in turn (`'unknown'`, `'no leader'`,
 * leaders for every partition when absent).
 */
const fakeKafka = ({
  failSend = false,
  failConnect,
  failRun = false,
  manualDelivery = false,
  sendMs,
  metadata = [],
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
    consumerConfigs: [],
    producerConfigs: [],
    metadataReads: 0,
    sleeps: [],
    seeks: [],
    compressions: [],
    listeners: [],
    delay: { enabledAt: [], disabledAt: [] },
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
        createTopics: async ({ topics, waitForLeaders }) => {
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
        fetchTopicOffsets: async (topic) => [
          {
            partition: 0,
            offset: String(state.topics.get(topic).length),
            high: String(state.topics.get(topic).length),
            low: '0',
          },
        ],
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
          if (failSend) throw new Error('send failed')
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
      let index = 0
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
          index = Number(offset)
        },
        run: async ({ eachMessage }) => {
          const records = state.topics.get(topic)
          state.deliverable = records.length
          state.deliver = async (count = Infinity) => {
            for (let i = 0; i < count && index < records.length; i++) {
              const offset = String(index)
              const message = { ...records[index++], offset }
              elapsedNs += 10000000n
              state.callbacks += 1
              await eachMessage({ topic, partition: 0, message })
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
    CompressionTypes: { LZ4: 3 },
    Partitioners: { DefaultPartitioner: legacyPartitioner },
  }
  return { client, state, timing }
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
        ['--workload', 'produce-throughput', '--compression', 'gzip'],
        ['--workload', 'consume-throughput', '--settings', 'other'],
        ['--workload', 'produce-throughput', '--partitions', '0'],
        ['--workload', 'produce-throughput', '--duration-ms', '1.5'],
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
        eventLoopDelayP50: 50,
        eventLoopDelayP99: 99,
        eventLoopDelayMax: 250,
        fetchRequests: 2,
        bytesPerFetch: 3000,
        messagesPerFetch: 45,
        fetchVersion: 17,
        passes: 1,
        settings: { defaults: null },
      })
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

      expect(state.seeks).toEqual([
        { topic: state.subscription.topic, partition: 0, offset: '0' },
        { topic: state.subscription.topic, partition: 0, offset: '0' },
      ])
      expect(state.callbacks).toBe(150)
      expect(metrics).toMatchObject({ measuredMessages: 145, elapsedMs: 1450, passes: 3 })
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
