const { spawnSync } = require('node:child_process')
const { builtinModules } = require('node:module')
const fs = require('node:fs')
const os = require('node:os')
const path = require('node:path')
const { parseArgs, runWorkload, WORKLOADS } = require('../clientCompare')

const SCRIPT = path.join(__dirname, '..', 'clientCompare.js')
const BROKERS = ['localhost:9092', 'localhost:9095', 'localhost:9098']

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

/** An in-memory Kafka that keeps what it was asked to do in `state`. */
const fakeKafka = ({
  failSend = false,
  failConnect,
  failRun = false,
  manualDelivery = false,
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
        createTopics: async ({ topics }) => {
          for (const { topic } of topics) state.topics.set(topic, [])
          return true
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

    producer() {
      return {
        connect: () => connect('producer'),
        send: async ({ topic, messages }) => {
          if (failSend) throw new Error('send failed')
          const durationMs = state.sends.length === 0 ? 1000 : state.sends.length
          elapsedNs += BigInt(durationMs) * 1000000n
          state.sends.push(messages.length)
          state.topics.get(topic).push(...messages)
        },
        disconnect: () => disconnect('producer'),
      }
    }

    consumer({ groupId }) {
      state.consumerGroups.push(groupId)
      let topic
      return {
        connect: () => connect('consumer'),
        subscribe: async (subscription) => {
          topic = subscription.topic
          state.subscription = subscription
        },
        run: async ({ eachMessage }) => {
          state.deliverable = state.topics.get(topic).length
          let index = 0
          state.deliver = async (count) => {
            for (let i = 0; i < count; i++) {
              const message = state.topics.get(topic)[index++]
              elapsedNs += 10000000n
              state.callbacks += 1
              await eachMessage({ topic, partition: 0, message })
            }
          }
          ready()
          if (failRun) throw new Error('run failed')
          if (!manualDelivery) await state.deliver(state.deliverable)
        },
        disconnect: () => disconnect('consumer'),
      }
    }
  }
  return { client: { Kafka, logLevel: { NOTHING: 0 } }, state, timing }
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

      expect(result.status).toBe(1)
      expect(result.stdout).toBe('')
      expect(result.stderr).toMatch(/outside.*prefix/i)
    })

    test('an arm cannot load the other arm package from its directory', () => {
      expect(load(path.join(prefix, 'fork'), 'kafkajs').status).not.toBe(0)
      expect(load(path.join(prefix, 'base'), '@countly/kafkajs').status).not.toBe(0)
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

      expect(result.status).toBe(1)
      expect(result.stdout).toBe('')
      expect(result.stderr).toMatch(/kafkajs/)
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
        expect(result.status).toBe(2)
        expect(result.stderr).toMatch(/usage|--/i)
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
        size: 200,
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
        messagesPerSecond: (40 / 820) * 1000,
      })
    })

    test('produce-throughput sends batches of 500 and the last batch carries the remainder', async () => {
      const { metrics, state } = await run('produce-throughput', 1200)

      expect(state.sends).toEqual([1, 500, 500, 200])
      expect(metrics).toEqual({ elapsedMs: 6, messagesPerSecond: 200000 })
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
      expect(metrics).toEqual({ elapsedMs: 490, messagesPerSecond: 100 })
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
  })
})
