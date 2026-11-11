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
const fakeKafka = ({ failSend = false } = {}) => {
  const state = {
    topics: new Map(),
    sends: [],
    closed: [],
    deleted: [],
    configs: [],
    consumerGroups: [],
  }
  class Kafka {
    constructor(config) {
      state.configs.push(config)
    }

    admin() {
      return {
        connect: async () => {},
        createTopics: async ({ topics }) => {
          for (const { topic } of topics) state.topics.set(topic, [])
          return true
        },
        deleteTopics: async ({ topics }) => {
          for (const topic of topics) state.deleted.push(topic)
        },
        disconnect: async () => state.closed.push('admin'),
      }
    }

    producer() {
      return {
        connect: async () => {},
        send: async ({ topic, messages }) => {
          if (failSend) throw new Error('send failed')
          state.sends.push(messages.length)
          state.topics.get(topic).push(...messages)
        },
        disconnect: async () => state.closed.push('producer'),
      }
    }

    consumer({ groupId }) {
      state.consumerGroups.push(groupId)
      let topic
      return {
        connect: async () => {},
        subscribe: async (subscription) => {
          topic = subscription.topic
          state.subscription = subscription
        },
        run: async ({ eachMessage }) => {
          state.deliverable = state.topics.get(topic).length
          setImmediate(async () => {
            for (const message of state.topics.get(topic)) {
              await eachMessage({ topic, partition: 0, message })
            }
          })
        },
        disconnect: async () => state.closed.push('consumer'),
      }
    }
  }
  return { client: { Kafka, logLevel: { NOTHING: 0 } }, state }
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
      runWorkload(kafka.client, workload, { messages, size: 8, brokers: BROKERS }).then(
        (metrics) => ({ metrics, ...kafka })
      )

    test('produce-latency sends one single-message batch per record after one untimed warm-up', async () => {
      const { metrics, state } = await run('produce-latency', 40)

      expect(state.sends).toEqual(Array(41).fill(1))
      expect(Object.keys(metrics).sort()).toEqual([
        'elapsedMs',
        'messagesPerSecond',
        'p50',
        'p90',
        'p99',
      ])
      expect(metrics.p50).toBeLessThanOrEqual(metrics.p90)
      expect(metrics.p90).toBeLessThanOrEqual(metrics.p99)
      expect(Number.isFinite(metrics.messagesPerSecond)).toBe(true)
      expect(metrics.messagesPerSecond).toBeGreaterThan(0)
    })

    test('produce-throughput sends batches of 500 and the last batch carries the remainder', async () => {
      const { metrics, state } = await run('produce-throughput', 1200)

      expect(state.sends).toEqual([1, 500, 500, 200])
      expect(Object.keys(metrics).sort()).toEqual(['elapsedMs', 'messagesPerSecond'])
      expect(metrics.messagesPerSecond).toBeGreaterThan(0)
    })

    test('consume-throughput produces the whole corpus first and counts delivery of every message', async () => {
      const { metrics, state } = await run('consume-throughput', 50)

      expect(state.sends.reduce((a, b) => a + b, 0)).toBe(50)
      expect(state.deliverable).toBe(50)
      expect(state.subscription).toMatchObject({ fromBeginning: true })
      expect(Object.keys(metrics).sort()).toEqual(['elapsedMs', 'messagesPerSecond'])
      expect(metrics.messagesPerSecond).toBeGreaterThan(0)
      expect(state.closed).toContain('consumer')
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
