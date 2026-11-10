const dns = require('dns')
const { Kafka } = require('../../../index')
const { KafkaJSBootstrapResolutionError } = require('../../errors')
const {
  createScriptedCluster,
  groupScript,
  consumerProtocolScript,
} = require('../../../testHelpers/scriptedCluster')
const { prepareRecordBatches, TOPIC } = require('../../consumer/__tests__/fixtures/fetchHarness')

const NODES = [
  { id: 0, host: 'node0', port: 9092 },
  { id: 1, host: 'node1', port: 9092 },
]
const DEADLINE = 1000
const STEP = 50
const RETRY_TIME = 100
const RETRY = {
  initialRetryTime: RETRY_TIME,
  maxRetryTime: RETRY_TIME,
  factor: 0,
  multiplier: 1,
  retries: 2,
}
const ADDRESSES = [{ address: '10.0.0.7', family: 4 }]
const KINDS = ['classic consumer', 'consumer protocol consumer']
const FACTORY_KINDS = [
  'producer',
  'classic consumer',
  'consumer protocol consumer',
  'share consumer',
  'admin',
]

/**
 * Replaces `dns.lookup` for the file: every call is recorded and answered as `plan` says.
 * @param {{ current: (host: string, attempt: number) => 'hold'|'fail'|'resolve' }} plan
 * @returns {{ calls: object[], perHost: (host: string) => number, restore: () => void }}
 */
const stubDns = (plan) => {
  const calls = []
  const attempts = new Map()
  const spy = jest.spyOn(dns, 'lookup').mockImplementation((host, options, callback) => {
    const attempt = attempts.get(host) ?? 0
    attempts.set(host, attempt + 1)
    calls.push({ host, options, attempt, callback })
    const verdict = plan.current(host, attempt)
    if (verdict === 'fail') {
      Promise.resolve().then(() => callback(new Error(`getaddrinfo ENOTFOUND ${host}`)))
    } else if (verdict === 'resolve') {
      Promise.resolve().then(() => callback(null, ADDRESSES))
    }
  })
  return {
    calls,
    perHost: (host) => calls.filter((call) => call.host === host).length,
    restore: () => spy.mockRestore(),
  }
}

/**
 * Advances fake time in fixed steps.
 * @param {number} total milliseconds
 * @param {() => void} [each] called after every step
 * @returns {Promise<void>}
 */
const advance = async (total, each = () => {}) => {
  for (let elapsed = 0; elapsed < total; elapsed += STEP) {
    await jest.advanceTimersByTimeAsync(STEP)
    each()
  }
}

/**
 * Advances fake time in steps until `promise` settles.
 * @template T
 * @param {Promise<T>} promise the work that needs timers to run
 * @param {number} [limit] fake milliseconds to wait at most
 * @returns {Promise<T>} its outcome
 * @throws {Error} when it has not settled by `limit`
 */
const until = async (promise, limit = 3 * DEADLINE) => {
  const state = { settled: false }
  const tracked = promise.finally(() => (state.settled = true))
  tracked.catch(() => {})
  for (let elapsed = 0; !state.settled && elapsed < limit; elapsed += STEP) {
    await jest.advanceTimersByTimeAsync(STEP)
  }
  if (!state.settled) throw new Error(`Did not settle within ${limit} ms of fake time`)
  return tracked
}

describe('Bootstrap DNS resolution', () => {
  let scripted
  let plan
  let dnsStub
  let clients

  /**
   * @param {object} [options] Kafka options added to the scripted cluster's
   * @returns {Kafka}
   */
  const newKafka = (options = {}) =>
    new Kafka(
      scripted.kafkaOptions({ bootstrapResolveTimeout: DEADLINE, retry: RETRY, ...options })
    )

  /** @param {{ disconnect: Function }} client @returns {typeof client} the client, closed after the test */
  const track = (client) => {
    clients.push(client)
    return client
  }

  /**
   * @param {string} kind one of `FACTORY_KINDS`
   * @param {Kafka} kafka
   * @returns {{ connect: Function, disconnect: Function }}
   */
  const create = (kind, kafka) => {
    const group = { groupId: `group-${clients.length}`, retry: RETRY }
    switch (kind) {
      case 'producer':
        return track(kafka.producer({ retry: RETRY }))
      case 'classic consumer':
        return track(kafka.consumer(group))
      case 'consumer protocol consumer':
        return track(kafka.consumer({ ...group, groupProtocol: 'consumer' }))
      case 'share consumer':
        return track(kafka.shareConsumer({ groupId: group.groupId, retry: RETRY }))
      default:
        return track(kafka.admin({ retry: RETRY }))
    }
  }

  beforeAll(prepareRecordBatches)

  beforeEach(() => {
    jest.useFakeTimers()
    clients = []
    plan = { current: () => 'fail' }
    dnsStub = stubDns(plan)
    scripted = groupScript(createScriptedCluster({ nodes: NODES }), { batches: 1 })
  })

  afterEach(async () => {
    plan.current = () => 'fail'
    await until(Promise.all(clients.map((client) => client.disconnect().catch(() => {}))))
    await jest.advanceTimersByTimeAsync(DEADLINE)
    dnsStub.restore()
    jest.useRealTimers()
  })

  test('zero performs no lookup and preserves the existing connect path through the public client', async () => {
    for (const options of [{}, { bootstrapResolveTimeout: 0 }]) {
      const admin = track(newKafka({ bootstrapResolveTimeout: undefined, ...options }).admin())

      await until(admin.connect())

      expect(dnsStub.calls).toHaveLength(0)
      expect(scripted.sockets().length).toBeGreaterThan(0)
      await until(admin.disconnect())
    }
  })

  test('all failed seeds reach the deadline and cache one terminal error through the public client', async () => {
    const producer = track(newKafka().producer({ retry: RETRY }))

    const first = producer.connect().catch((error) => error)
    await advance(DEADLINE)
    const error = await first

    expect(error).toBeInstanceOf(KafkaJSBootstrapResolutionError)
    expect(error.retriable).toBe(false)
    expect(scripted.sockets()).toHaveLength(0)
    const lookups = dnsStub.calls.length
    expect(lookups).toBeGreaterThanOrEqual(NODES.length)

    const second = await producer.connect().catch((reason) => reason)
    await advance(2 * DEADLINE)

    expect(second).toBe(error)
    expect(dnsStub.calls).toHaveLength(lookups)
    expect(scripted.sockets()).toHaveLength(0)
  })

  test('concurrent connects share one pending lookup round through the public client', async () => {
    const admin = track(newKafka().admin({ retry: RETRY }))
    plan.current = () => 'hold'

    const connects = [admin.connect(), admin.connect(), admin.connect()]
    await advance(STEP)

    expect(dnsStub.calls.map(({ host }) => host)).toEqual(['node0', 'node1'])
    expect(dnsStub.calls.map(({ options }) => options)).toEqual([{ all: true }, { all: true }])
    expect(scripted.sockets()).toHaveLength(0)

    dnsStub.calls[1].callback(null, ADDRESSES)
    await until(Promise.all(connects))

    expect(dnsStub.calls).toHaveLength(NODES.length)
    expect(scripted.sockets().length).toBeGreaterThan(0)
  })

  test('disconnect cancels pending work and the next connect checks again through the public client', async () => {
    const producer = track(newKafka().producer({ retry: RETRY }))
    plan.current = () => 'hold'

    const first = producer.connect().catch((error) => error)
    await advance(STEP)
    expect(dnsStub.calls).toHaveLength(NODES.length)

    await until(producer.disconnect())
    const outcome = await first
    await advance(DEADLINE)

    expect(outcome).toBeInstanceOf(Error)
    expect(outcome).not.toBeInstanceOf(KafkaJSBootstrapResolutionError)
    expect(dnsStub.calls).toHaveLength(NODES.length)
    expect(jest.getTimerCount()).toBe(0)

    plan.current = () => 'resolve'
    await until(producer.connect())

    expect(dnsStub.calls).toHaveLength(2 * NODES.length)
    expect(scripted.sockets().length).toBeGreaterThan(0)
  })

  test('TLS and custom factories receive the original hostname through the public client', async () => {
    const ssl = { rejectUnauthorized: true, servername: 'sni.example.com' }
    const dialled = []
    plan.current = () => 'resolve'
    const kafka = newKafka({
      ssl,
      socketFactory: (args) => {
        dialled.push({ host: args.host, port: args.port, ssl: args.ssl })
        return scripted.socketFactory(args)
      },
    })
    const admin = track(kafka.admin({ retry: RETRY }))

    await until(admin.connect())

    expect(dnsStub.calls.map(({ host }) => host)).toEqual(['node0', 'node1'])
    expect(dialled.length).toBeGreaterThan(0)
    for (const { host, port, ssl: used } of dialled) {
      expect(['node0', 'node1']).toContain(host)
      expect(port).toBe(9092)
      expect(used).toEqual(ssl)
    }
    expect(JSON.stringify(dialled)).not.toContain('10.0.0.7')
  })

  /**
   * @param {string} kind one of `KINDS`
   * @param {{ batches?: number }} [options] how many record batches the cluster serves
   * @returns {{ consumer: object, crashes: number[] }} a consumer that restarts after every crash
   */
  const startConsumer = (kind, { batches = 0 } = {}) => {
    const script = kind === 'classic consumer' ? groupScript : consumerProtocolScript
    scripted = script(
      createScriptedCluster({
        nodes: NODES,
        profile: kind === 'classic consumer' ? 'default' : 'consumer',
      }),
      { batches }
    )
    const consumer = track(
      newKafka().consumer({
        groupId: 'startup-group',
        ...(kind === 'classic consumer' ? {} : { groupProtocol: 'consumer' }),
        maxWaitTimeInMs: 20,
        retry: { ...RETRY, restartOnFailure: async () => true },
      })
    )
    const crashes = []
    consumer.on(consumer.events.CRASH, () => crashes.push(Date.now()))
    return { consumer, crashes }
  }

  test.each(KINDS)(
    'a %s started with unresolved bootstrap DNS blocks before joining and keeps scheduling lookup and connect bounded through the public client',
    async (kind) => {
      const { consumer, crashes } = startConsumer(kind)
      const timers = []

      const connected = consumer.connect().catch((error) => error)
      const running = consumer.run({
        autoCommit: true,
        autoCommitInterval: 10,
        eachMessage: async () => {},
      })
      await advance(DEADLINE - STEP, () => timers.push(jest.getTimerCount()))

      expect(scripted.sockets()).toHaveLength(0)
      expect(scripted.requests()).toHaveLength(0)
      expect(crashes).toHaveLength(0)
      for (const { host } of NODES) {
        expect(dnsStub.perHost(host)).toBeGreaterThan(1)
        expect(dnsStub.perHost(host)).toBeLessThanOrEqual(DEADLINE / RETRY_TIME)
      }
      expect(Math.max(...timers)).toBeLessThanOrEqual(NODES.length + 1)

      await advance(2 * STEP)
      expect(await connected).toBeInstanceOf(KafkaJSBootstrapResolutionError)
      const afterDeadline = dnsStub.calls.length

      await advance(DEADLINE, () => timers.push(jest.getTimerCount()))
      expect(dnsStub.calls).toHaveLength(afterDeadline)
      expect(scripted.sockets()).toHaveLength(0)
      expect(scripted.requests()).toHaveLength(0)
      expect(crashes.length).toBeGreaterThanOrEqual(2)
      expect(crashes.length).toBeLessThanOrEqual(DEADLINE / RETRY_TIME + 1)
      for (let index = 1; index < crashes.length; index++) {
        expect(crashes[index] - crashes[index - 1]).toBeGreaterThanOrEqual(RETRY_TIME)
      }
      expect(Math.max(...timers)).toBeLessThanOrEqual(NODES.length + 1)

      await consumer.disconnect()
      await running.catch(() => {})
      await advance(DEADLINE)
      expect(jest.getTimerCount()).toBe(0)
      expect(dnsStub.calls).toHaveLength(afterDeadline)
      expect(scripted.requests()).toHaveLength(0)
    }
  )

  test.each(KINDS)(
    'a %s whose bootstrap DNS resolves after a pending wait then joins and auto-commits through the public client',
    async (kind) => {
      const { consumer } = startConsumer(kind, { batches: 1 })
      const delivered = []

      const connected = consumer.connect()
      await advance(DEADLINE / 2)
      expect(scripted.requests()).toHaveLength(0)
      expect(dnsStub.calls.length).toBeGreaterThan(NODES.length)

      plan.current = () => 'resolve'
      const before = dnsStub.calls.length
      await until(connected)
      await until(consumer.subscribe({ topic: TOPIC, fromBeginning: true }))
      await until(
        consumer.run({
          autoCommit: true,
          autoCommitInterval: 10,
          eachMessage: async ({ message }) => delivered.push(message.offset),
        })
      )
      await advance(DEADLINE)

      const apis = scripted.requests().map(({ api }) => api)
      expect(apis).toContain(kind === 'classic consumer' ? 'JoinGroup' : 'ConsumerGroupHeartbeat')
      expect(apis).toContain('OffsetCommit')
      expect(delivered.length).toBeGreaterThan(0)
      expect(dnsStub.calls.length - before).toBeLessThanOrEqual(NODES.length)
    }
  )

  test.each(FACTORY_KINDS)(
    'each %s owns an independent gate through the public client',
    async (kind) => {
      const kafka = newKafka()
      const first = create(kind, kafka)
      const second = create(kind, kafka)

      const failed = first.connect().catch((error) => error)
      await advance(DEADLINE)
      const terminal = await failed
      expect(terminal).toBeInstanceOf(KafkaJSBootstrapResolutionError)
      const lookups = dnsStub.calls.length

      plan.current = () => 'resolve'
      const outcome = second.connect().catch((error) => error)
      await advance(STEP)
      expect(await outcome).not.toBeInstanceOf(KafkaJSBootstrapResolutionError)
      expect(dnsStub.calls.length).toBeGreaterThan(lookups)

      const settled = dnsStub.calls.length
      await expect(first.connect()).rejects.toBe(terminal)
      expect(dnsStub.calls).toHaveLength(settled)
    }
  )
})
