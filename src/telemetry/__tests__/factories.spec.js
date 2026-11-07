const {
  Kafka,
  logLevel,
  KafkaJSTelemetryError,
  KafkaJSInvalidConfigurationError,
} = require('../../..')
const Cluster = require('../../cluster')
const ConnectionPool = require('../../network/connectionPool')
const BrokerPool = require('../../cluster/brokerPool')
const Generation = require('../../producer/generation')
const createConsumer = require('../../consumer')
const Runner = require('../../consumer/runner')
const OffsetOperations = require('../../consumer/offsetManager/offsetOperations')
const InstrumentationEventEmitter = require('../../instrumentation/emitter')
const { ShareRunner } = require('../../consumer/share/shareRunner')
const { ShareRun } = require('../../consumer/share/shareRun')
const { validateShareConsumerOptions } = require('../../consumer/share')
const { CONNECTION_STATUS } = require('../../network/connectionStatus')
const ClientTelemetry = require('../client')
const TelemetryMetrics = require('../metrics')
const { TelemetryFactory } = require('../factory')
const { createScriptedCluster } = require('../../../testHelpers/scriptedCluster')
const { newLogger } = require('testHelpers')

const ID = '01020304-0506-0708-090a-0b0c0d0e0f10'
const NODES = [{ id: 1, host: 'a', port: 9092 }]
const kinds = ['producer', 'consumer', 'admin', 'shareConsumer']

/**
 * @param {object} kafka public root
 * @param {string} kind client kind
 * @param {object} [extra] factory options
 * @returns {object} the actual returned client
 */
const make = (kafka, kind, extra = {}) => {
  switch (kind) {
    case 'producer':
      return kafka.producer({ idempotent: false, ...extra })
    case 'consumer':
      return kafka.consumer({ groupId: 'g', ...extra })
    case 'admin':
      return kafka.admin(extra)
    default:
      return kafka.shareConsumer({ groupId: 'g', ...extra })
  }
}

/**
 * @param {string} kind client kind
 * @returns {object} a scripted cluster that serves the kind and both telemetry pairs
 */
const scripted = (kind) =>
  kind === 'shareConsumer'
    ? createScriptedCluster({
        nodes: NODES,
        profile: 'share',
        advertise: { GetTelemetrySubscriptions: [0, 0], PushTelemetry: [0, 0] },
      })
    : createScriptedCluster({ nodes: NODES, profile: 'telemetry' })

/**
 * @param {Function} predicate bounded independent observation
 * @returns {Promise<void>} resolves once true; the assertion fails after about one second
 */
const until = async (predicate) => {
  for (let n = 0; n < 100 && !predicate(); n++) {
    await new Promise((resolve) => setTimeout(resolve, 10))
  }
  expect(predicate()).toBe(true)
}

const held = () => {
  let release
  const promise = new Promise((resolve) => {
    release = resolve
  })
  return { promise, release }
}

/**
 * Reads the small independently defined protobuf wire subset without the production OTLP writer.
 * @param {Buffer} bytes one length-delimited message
 * @returns {Array<{number:number,value:Buffer|number}>} bounded field entries
 * @throws {RangeError} truncated or unsupported field
 */
const fields = (bytes) => {
  let at = 0
  const result = []
  const integer = () => {
    let value = 0
    let factor = 1
    for (let n = 0; n < 8 && at < bytes.length; n++) {
      const byte = bytes[at++]
      value += (byte & 127) * factor
      if (byte < 128 && Number.isSafeInteger(value)) return value
      factor *= 128
    }
    throw RangeError('Invalid protobuf integer')
  }
  while (at < bytes.length) {
    const tag = integer()
    const type = tag % 8
    const number = Math.floor(tag / 8)
    if (!number) throw RangeError('Invalid protobuf field')
    if (type === 0) result.push({ number, value: integer() })
    else {
      const size = type === 1 ? 8 : type === 2 ? integer() : -1
      if (size < 0 || at + size > bytes.length) throw RangeError('Invalid protobuf size')
      result.push({ number, value: bytes.subarray(at, at + size) })
      at += size
    }
  }
  return result
}
const values = (bytes, number) =>
  fields(bytes)
    .filter((f) => f.number === number)
    .map((f) => f.value)
const metrics = (bytes) => values(values(values(bytes, 1)[0], 2)[0], 2)
const resourceKeys = (bytes) => {
  const resource = values(values(bytes, 1)[0], 1)[0]
  return resource ? values(resource, 1).map((kv) => values(kv, 1)[0].toString()) : []
}

describe('returned telemetry surfaces', () => {
  test('all four kinds and same-kind siblings own independent bindings', () => {
    const bind = jest.spyOn(TelemetryFactory.prototype, 'bind')
    try {
      const kafka = new Kafka({
        brokers: ['localhost:9092'],
        logLevel: logLevel.NOTHING,
        enableMetricsPush: true,
      })
      const clients = [...kinds.map((kind) => make(kafka, kind)), make(kafka, 'producer')]
      const owners = bind.mock.contexts
      expect(owners).toHaveLength(5)
      expect(new Set(owners).size).toBe(5)
      expect(new Set(owners.map((o) => o.metrics)).size).toBe(5)
      expect(new Set(owners.map((o) => o.machine)).size).toBe(5)
      for (const client of clients) {
        for (const name of [
          'clientInstanceId',
          'registerMetricForSubscription',
          'unregisterMetricFromSubscription',
        ]) {
          expect(typeof client[name]).toBe('function')
        }
      }
      expect(kafka.clientInstanceId).toBeUndefined()
      expect(Object.isFrozen(clients[3])).toBe(true)
    } finally {
      bind.mockRestore()
    }
  })

  test.each(kinds)(
    'returned %s keeps a registration of its own before connect, without a network',
    async (kind) => {
      const first = make(
        new Kafka({ brokers: ['localhost:9092'], logLevel: logLevel.NOTHING }),
        kind,
        {
          enableMetricsPush: true,
        }
      )
      const second = make(
        new Kafka({ brokers: ['localhost:9092'], logLevel: logLevel.NOTHING }),
        kind,
        {
          enableMetricsPush: true,
        }
      )
      first.registerMetricForSubscription({ name: 'com.example.one', type: 'gauge', value: 1 })
      first.registerMetricForSubscription({ name: 'com.example.one', type: 'gauge', value: 2 })
      second.registerMetricForSubscription({ name: 'com.example.one', type: 'sum', value: 1n })
      expect(first.unregisterMetricFromSubscription({ name: 'com.example.one' })).toBeUndefined()
      expect(first.unregisterMetricFromSubscription({ name: 'com.example.absent' })).toBeUndefined()
      expect(() =>
        first.registerMetricForSubscription({ name: 'com.example.one', type: 'sum', value: 1n })
      ).not.toThrow()
      await expect(first.clientInstanceId({ timeout: 0 })).rejects.toMatchObject({
        reason: 'not-connected',
      })
    }
  )

  test.each(kinds)(
    'disabled returned %s refuses registration and validates removal',
    async (kind) => {
      const client = make(
        new Kafka({ brokers: ['localhost:9092'], logLevel: logLevel.NOTHING }),
        kind
      )
      let refusal
      try {
        client.registerMetricForSubscription({ name: 'com.example.x', type: 'gauge', value: 1 })
      } catch (error) {
        refusal = error
      }
      expect(refusal).toBeInstanceOf(KafkaJSTelemetryError)
      expect(refusal).toMatchObject({ reason: 'disabled' })
      expect(client.unregisterMetricFromSubscription({ name: 'com.example.x' })).toBeUndefined()
      expect(() => client.unregisterMetricFromSubscription({ name: 'bad' })).toThrow(
        KafkaJSInvalidConfigurationError
      )
      await expect(client.clientInstanceId()).rejects.toMatchObject({ reason: 'disabled' })
    }
  )

  test.each(kinds)(
    'returned %s delegates numeric registration through strict wire and independent payload decoding',
    async (kind) => {
      const wire = scripted(kind)
      wire.answer('GetTelemetrySubscriptions', () => ({
        clientInstanceId: ID,
        subscriptionId: 1,
        pushIntervalMs: 100,
        acceptedCompressionTypes: [0],
        requestedMetrics: ['com.example.'],
      }))
      const bind = jest.spyOn(TelemetryFactory.prototype, 'bind')
      const client = make(
        new Kafka(wire.kafkaOptions({ enableMetricsPush: true })),
        kind,
        kind === 'consumer' ? { rackId: 'rack-1' } : {}
      )
      const owner = bind.mock.contexts.at(-1)
      const normalConnections = jest.spyOn(owner.metrics, 'observeConnection')
      try {
        client.registerMetricForSubscription({ name: 'com.example.depth', type: 'gauge', value: 1 })
        client.registerMetricForSubscription({ name: 'com.example.depth', type: 'gauge', value: 3 })
        await expect(client.clientInstanceId()).rejects.toMatchObject({ reason: 'not-connected' })
        await client.connect()
        await expect(client.clientInstanceId({ timeout: 1000 })).resolves.toBe(ID)
        expect(normalConnections).toHaveBeenCalled()
        await until(() => wire.requests().some((r) => r.api === 'PushTelemetry'))
        const first = wire.requests().find((r) => r.api === 'PushTelemetry')
        expect(first.fields.terminating).toBe(false)
        expect(first.fields.compressionType).toBe(0)
        expect(first.fields.metrics.length).toBeGreaterThan(0)
        const bytes = Buffer.from(first.fields.metrics, 'hex')
        const emitted = metrics(bytes)
        expect(emitted).toHaveLength(1)
        expect(values(emitted[0], 1)[0].toString()).toBe('com.example.depth')
        const point = values(values(emitted[0], 5)[0], 1)[0]
        expect(values(point, 4)[0].readDoubleLE()).toBe(3)
        const keys = resourceKeys(bytes)
        if (kind === 'consumer')
          expect(keys).toEqual(expect.arrayContaining(['group_id', 'client_rack']))
        if (kind === 'shareConsumer') expect(keys).toContain('group_id')
        if (kind === 'admin' || kind === 'producer') expect(keys).not.toContain('group_id')
        client.unregisterMetricFromSubscription({ name: 'com.example.depth' })
        const previous = wire.requests().length
        await until(() =>
          wire
            .requests()
            .slice(previous)
            .some((r) => r.api === 'PushTelemetry')
        )
        expect(
          wire
            .requests()
            .slice(previous)
            .find((r) => r.api === 'PushTelemetry').fields.metrics.length
        ).toBe(0)
      } finally {
        await client.disconnect({ timeout: 0 })
        wire.assertClean()
        expect(wire.sockets().every((socket) => socket.destroyed)).toBe(true)
        expect(owner.machine.timer).toBeNull()
        expect(owner.machine.waiters.size).toBe(0)
        bind.mockRestore()
      }
    }
  )

  test('every factory supplies only its applicable static resource strings', () => {
    const bind = jest.spyOn(TelemetryFactory.prototype, 'bind')
    try {
      const kafka = new Kafka({
        brokers: ['localhost:9092'],
        logLevel: logLevel.NOTHING,
        enableMetricsPush: true,
      })
      kafka.producer({ transactionalId: 'tx-1', rackId: 'r1' })
      kafka.consumer({ groupId: 'g', groupInstanceId: 'static-1', rackId: 'r2' })
      kafka.admin()
      kafka.shareConsumer({ groupId: 'sg', rackId: 'r3' })
      const [producer, consumer, admin, share] = bind.mock.contexts.map((owner) => ({
        ...owner.captureResources().values,
      }))
      expect(producer).toEqual({ client_rack: 'r1', transactional_id: 'tx-1' })
      expect(consumer).toEqual({
        client_rack: 'r2',
        group_id: 'g',
        group_instance_id: 'static-1',
      })
      expect(admin).toEqual({})
      expect(share).toEqual({ client_rack: 'r3', group_id: 'sg' })
    } finally {
      bind.mockRestore()
    }
  })

  test.each(kinds)(
    'enabled public %s retains registration after close and reconnect and stores while stopped',
    async (kind) => {
      const wire = scripted(kind)
      wire.answer('GetTelemetrySubscriptions', () => ({
        clientInstanceId: ID,
        subscriptionId: 1,
        requestedMetrics: ['com.example.'],
        pushIntervalMs: 100,
        acceptedCompressionTypes: [0],
        deltaTemporality: false,
      }))
      const bind = jest.spyOn(TelemetryFactory.prototype, 'bind')
      const client = make(new Kafka(wire.kafkaOptions({ enableMetricsPush: true })), kind)
      const owner = bind.mock.contexts.at(-1)
      try {
        client.registerMetricForSubscription({ name: 'com.example.total', type: 'sum', value: 1n })
        await client.connect()
        await client.clientInstanceId({ timeout: 1000 })
        await until(
          () =>
            owner.machine.state === 'READY' &&
            wire.requests().some((r) => r.api === 'PushTelemetry')
        )
        await client.disconnect({ timeout: 1000 })
        const beforeReconnect = wire.requests().length
        client.registerMetricForSubscription({ name: 'com.example.total', type: 'sum', value: 2n })
        await client.connect()
        await client.clientInstanceId({ timeout: 1000 })
        await until(() =>
          wire
            .requests()
            .slice(beforeReconnect)
            .some((r) => r.api === 'PushTelemetry' && !r.fields.terminating)
        )
        const push = wire
          .requests()
          .slice(beforeReconnect)
          .find((r) => r.api === 'PushTelemetry' && !r.fields.terminating)
        const emitted = metrics(Buffer.from(push.fields.metrics, 'hex'))
        expect(values(emitted[0], 1)[0].toString()).toBe('com.example.total')
        const point = values(values(emitted[0], 7)[0], 1)[0]
        expect(values(point, 6)[0].readBigInt64LE()).toBe(2n)
        wire.answer('PushTelemetry', () => ({ errorCode: 42 }))
        await until(() => owner.machine.stopped)
        const before = wire.requests().length
        client.registerMetricForSubscription({ name: 'com.example.total', type: 'sum', value: 3n })
        client.unregisterMetricFromSubscription({ name: 'com.example.absent' })
        expect(() =>
          client.registerMetricForSubscription({
            name: 'com.example.total',
            type: 'sum',
            value: 2n,
          })
        ).toThrow(KafkaJSInvalidConfigurationError)
        await new Promise((resolve) => setTimeout(resolve, 120))
        expect(wire.requests()).toHaveLength(before)
      } finally {
        await client.disconnect({ timeout: 0 })
        wire.assertClean()
        expect(wire.sockets().every((socket) => socket.destroyed)).toBe(true)
        expect(owner.machine.timer).toBeNull()
        expect(owner.machine.waiters.size).toBe(0)
        bind.mockRestore()
      }
    }
  )

  test('disabled registration refuses and absent unregistration validates without a machine', () => {
    const binding = new TelemetryFactory({
      kind: 'admin',
      config: { enableMetricsPush: false, telemetryMaxBytes: 1 },
    })
    expect(() =>
      binding.registerMetricForSubscription({ name: 'com.example.x', type: 'gauge', value: 1 })
    ).toThrow(KafkaJSTelemetryError)
    expect(
      binding.unregisterMetricFromSubscription({
        name: 'com.example.x',
        get type() {
          throw Error()
        },
      })
    ).toBeUndefined()
    expect(() => binding.unregisterMetricFromSubscription({ name: 'bad' })).toThrow(
      KafkaJSInvalidConfigurationError
    )
    expect(binding.machine).toBeNull()
    expect(binding.metrics).toBeNull()
  })

  test('a failing machine or collector construction stays inside telemetry', async () => {
    const binding = new TelemetryFactory({
      kind: 'producer',
      config: { enableMetricsPush: true, telemetryMaxBytes: 1 },
    })
    binding.bind({
      createTelemetryTransport: () => {
        throw Error('private')
      },
    })
    expect(binding.machine).toBeNull()
    await expect(binding.methods().clientInstanceId()).rejects.toMatchObject({ reason: 'stopped' })
    expect(() =>
      binding
        .methods()
        .registerMetricForSubscription({ name: 'com.example.x', type: 'gauge', value: 1 })
    ).not.toThrow()
    expect(binding.close({ zero: true })).toBeNull()
    expect(() => binding.suspend()).not.toThrow()
    expect(() => binding.resume()).not.toThrow()
  })

  test('machine close failures are contained and a finished close clears its resources', async () => {
    const binding = new TelemetryFactory({
      kind: 'consumer',
      config: { enableMetricsPush: true, telemetryMaxBytes: 1 },
    })
    binding.machine = { closing: null, close: jest.fn().mockRejectedValue(new Error('private')) }
    await expect(binding.close({ zero: false })).resolves.toBeUndefined()
    expect(binding.finalResources).toBeNull()
    binding.machine = {
      closing: null,
      close: () => {
        throw new Error('private')
      },
    }
    await expect(binding.close({ zero: false })).resolves.toBeUndefined()
    expect(binding.finalResources).toBeNull()
  })

  test('captured final resources stay frozen when core membership is cleared', async () => {
    const binding = new TelemetryFactory({
      kind: 'consumer',
      config: { enableMetricsPush: true, telemetryMaxBytes: 1048576 },
    })
    const pending = held()
    binding.machine = { close: jest.fn(() => pending.promise), closing: null }
    binding.updateResources({ group_id: 'g', group_member_id: 'old' })
    const normal = binding.captureResources()
    const closed = binding.close({ zero: true })
    const final = binding.captureResources()
    binding.updateResources({ group_id: 'g', group_member_id: 'new' })
    expect(normal.isCurrent()).toBe(false)
    expect(final.isCurrent()).toBe(true)
    expect(final.values.group_member_id).toBe('old')
    pending.release()
    await closed
    expect(final.isCurrent()).toBe(false)
    expect(binding.captureResources().values.group_member_id).toBe('new')
  })

  test('ordinary resource readers fence equal-valued membership replacements', () => {
    const binding = new TelemetryFactory({
      kind: 'consumer',
      config: { enableMetricsPush: true, telemetryMaxBytes: 1 },
    })
    let revision = 1n
    binding.setResourceReader(() => {
      const captured = revision
      return {
        values: { group_id: 'g', group_member_id: 'same' },
        isCurrent: () => revision === captured,
      }
    })
    const old = binding.captureResources()
    revision++
    expect(old.isCurrent()).toBe(false)
    const current = binding.captureResources()
    expect(current.values.group_member_id).toBe('same')
    expect(current.isCurrent()).toBe(true)
  })

  test('a reader blank removes a static group field and invalid values are never copied', () => {
    const binding = new TelemetryFactory({
      kind: 'consumer',
      config: { enableMetricsPush: true, telemetryMaxBytes: 1 },
    })
    binding.updateResources({ group_id: 'g', client_rack: 'r', group_instance_id: 7, unknown: 'x' })
    binding.setResourceReader(() => ({ values: { group_id: '' }, isCurrent: () => true }))
    expect({ ...binding.captureResources().values }).toEqual({ client_rack: 'r' })
  })

  test('a resource snapshot revoked at close entry cannot authorize a final', async () => {
    const binding = new TelemetryFactory({
      kind: 'consumer',
      config: { enableMetricsPush: true, telemetryMaxBytes: 1 },
    })
    binding.setResourceReader(() => ({
      values: { group_id: 'g', group_member_id: 'old' },
      isCurrent: () => false,
    }))
    let allowed
    binding.machine = {
      closing: null,
      close: async () => {
        allowed = binding.captureResources().isCurrent()
      },
    }
    await binding.close({ zero: false, remaining: () => 1000 })
    expect(allowed).toBe(false)
  })

  test('normal node inactivity waits for both sockets and obsolete retirement fences the slot', () => {
    const collector = new TelemetryMetrics({ kind: 'producer' })
    const slot = collector.bindNode({ nodeId: 1, endpoint: 'a:1', endpointRevision: 1n })
    const release = jest.spyOn(collector, 'releaseNode')
    const retire = jest.spyOn(collector, 'retireNode')
    const pool = new ConnectionPool({ host: 'a', port: 1, clientId: 'c', logger: newLogger() })
    for (const connection of pool.pool) {
      connection.setTelemetryObserver(collector, slot, pool)
      connection.connectionStatus = CONNECTION_STATUS.CONNECTED
    }
    pool.pool[0].closeGeneration(new Error('local'))
    expect(release).not.toHaveBeenCalled()
    pool.pool[1].closeGeneration(new Error('local'))
    expect(release).toHaveBeenCalledWith(slot)
    pool.telemetrySlot = slot
    const broker = { connectionPool: pool, retire: jest.fn(async () => {}) }
    const owner = {
      seedBroker: null,
      brokers: { 1: broker },
      retiredBrokers: new WeakSet(),
      telemetryMetrics: collector,
      retireTelemetryNode: BrokerPool.prototype.retireTelemetryNode,
      closeInBackground: (promise) => {
        promise.catch(() => {})
      },
      drainTimeout: 0,
    }
    BrokerPool.prototype.closeObsolete.call(owner, broker)
    expect(retire).toHaveBeenCalledWith(slot)
    expect(broker.retire).toHaveBeenCalledTimes(1)
    expect(owner.brokers).toEqual({})
  })

  test('a known node binds one slot per pool, an unknown node gets none and disabled binds nothing', () => {
    const collector = new TelemetryMetrics({ kind: 'consumer' })
    const owner = { telemetryMetrics: collector, telemetryNodeRevision: 0n }
    const pool = new ConnectionPool({ host: 'a', port: 1, clientId: 'c', logger: newLogger() })
    BrokerPool.prototype.bindTelemetryNode.call(owner, { nodeId: 1, connectionPool: pool })
    const slot = pool.telemetrySlot
    expect(slot).not.toBeNull()
    for (const connection of pool.pool) {
      expect(connection.telemetrySink).toBe(collector)
      expect(connection.telemetrySlot).toBe(slot)
      expect(connection.telemetryPool).toBe(pool)
    }
    BrokerPool.prototype.bindTelemetryNode.call(owner, { nodeId: 1, connectionPool: pool })
    expect(pool.telemetrySlot).toBe(slot)
    const seed = new ConnectionPool({ host: 'b', port: 2, clientId: 'c', logger: newLogger() })
    BrokerPool.prototype.bindTelemetryNode.call(owner, { connectionPool: seed })
    expect(seed.telemetrySlot).toBeNull()
    const idle = new ConnectionPool({ host: 'c', port: 3, clientId: 'c', logger: newLogger() })
    BrokerPool.prototype.bindTelemetryNode.call(
      { telemetryMetrics: null },
      { nodeId: 2, connectionPool: idle }
    )
    expect(idle.telemetrySlot).toBeUndefined()
    expect(idle.pool[0].telemetrySink).toBeNull()
  })

  test('resume rebinds current pools once and never revives retired brokers or pools', () => {
    const collector = new TelemetryMetrics({ kind: 'consumer' })
    const brokers = [1, 2, 3].map((nodeId) => ({
      nodeId,
      connectionPool: new ConnectionPool({
        host: 'a',
        port: nodeId,
        clientId: 'c',
        logger: newLogger(),
      }),
    }))
    const owner = {
      telemetryMetrics: collector,
      telemetryNodeRevision: 0n,
      seedBroker: brokers[0],
      brokers: Object.fromEntries(brokers.map((broker) => [broker.nodeId, broker])),
      retiredBrokers: new WeakSet([brokers[1]]),
      bindTelemetryNode: BrokerPool.prototype.bindTelemetryNode,
    }
    for (const broker of brokers) owner.bindTelemetryNode(broker)
    const old = brokers.map((broker) => broker.connectionPool.telemetrySlot)
    brokers[2].connectionPool.retire()
    collector.resetTransient()
    BrokerPool.prototype.rebindTelemetryNodes.call(owner)
    expect(brokers[0].connectionPool.telemetrySlot).not.toBe(old[0])
    expect(brokers[1].connectionPool.telemetrySlot).toBe(old[1])
    expect(brokers[2].connectionPool.telemetrySlot).toBe(old[2])
    const subscription = { revision: 1n, requestedMetrics: ['*'], deltaTemporality: true }
    collector.select(subscription)
    for (const ref of old) collector.observeRequest(ref, 99)
    collector.observeRequest(brokers[0].connectionPool.telemetrySlot, 7)
    const points = collector.snapshot(subscription).points.filter((point) => point.type === 'gauge')
    expect(points.map((point) => [point.attributes[0].value, point.value])).toEqual([
      ['1', 7],
      ['1', 7],
    ])
  })

  test('a public ShareConsumer crash restart rebinds a surviving ready broker and fences its old reference', async () => {
    const { shareClient, A } = require('../../../testHelpers/shareHarness')
    const bind = jest.spyOn(TelemetryFactory.prototype, 'bind')
    const api = jest.spyOn(ShareRunner.prototype, 'api')
    const h = shareClient({
      nodes: 2,
      advertise: { GetTelemetrySubscriptions: [0, 0], PushTelemetry: [0, 0] },
      kafka: { enableMetricsPush: true },
      share: { retry: { initialRetryTime: 5, restartOnFailure: async () => true } },
      topics: { 'topic-a': { topicId: A, partitions: [] } },
      interval: 300000,
    })
    h.cluster.answer('GetTelemetrySubscriptions', () => ({
      clientInstanceId: ID,
      subscriptionId: 1,
      requestedMetrics: ['org.apache.kafka.consumer.node.request.latency.'],
      pushIntervalMs: 300000,
      acceptedCompressionTypes: [0],
    }))
    const owner = bind.mock.contexts.at(-1)
    const cluster = bind.mock.calls.at(-1)[0]
    const runner = api.mock.contexts.at(-1)
    try {
      await h.consumer.subscribe({ topics: ['topic-a'] })
      await h.consumer.run({ eachMessage: async () => {} })
      await h.consumer.clientInstanceId({ timeout: 1000 })
      const survivor = await cluster.findBroker({ nodeId: 1 })
      await survivor.connect()
      const pool = survivor.connectionPool
      const connection = pool.pool[0]
      const socket = connection.socket
      const old = pool.telemetrySlot
      const crashed = runner.current
      await crashed.crash(
        Object.assign(new Error('retry this run'), { retriable: true, retryTime: 5 })
      )
      await until(
        () =>
          runner.current !== crashed &&
          runner.current?.membership.memberEpoch > 0 &&
          owner.machine.subscription !== null
      )
      expect(survivor.isConnected()).toBe(true)
      expect(connection.socket).toBe(socket)
      expect(pool.telemetrySlot).not.toBe(old)
      expect(connection.telemetrySlot).toBe(pool.telemetrySlot)
      const subscription = owner.machine.subscription
      const previous = owner.metrics.snapshot(subscription)
      owner.metrics.written(
        previous,
        previous.points.map((point) => point.identity)
      )
      const observe = jest.spyOn(owner.metrics, 'observeRequest')
      const staleObserve = TelemetryMetrics.prototype.observeRequest
      staleObserve.call(owner.metrics, old, 9000)
      await survivor.metadata()
      const samples = observe.mock.calls.filter(([slot]) => slot === pool.telemetrySlot)
      expect(samples).toHaveLength(1)
      const snapshot = owner.metrics.snapshot(subscription)
      const gauges = snapshot.points.filter((point) =>
        point.attributes.some((a) => a.key === 'node_id' && a.value === '1')
      )
      expect(gauges.map((point) => point.value)).toEqual([samples[0][1], samples[0][1]])
      observe.mockRestore()
    } finally {
      await h.close()
      expect(h.cluster.sockets().every((socket) => socket.destroyed)).toBe(true)
      expect(owner.machine.waiters.size).toBe(0)
      expect(owner.machine.timer).toBeNull()
      bind.mockRestore()
      api.mockRestore()
    }
  })

  test('a disabled client installs no observer on its normal sockets', () => {
    const cluster = new Cluster({
      logger: newLogger(),
      brokers: ['a:1'],
      clientId: 'c',
      retry: { retries: 0 },
    })
    const pool = cluster.connectionPoolBuilder
    return pool.build({ host: 'a', port: 1 }).then((built) => {
      for (const connection of built.pool) {
        expect(connection.telemetrySink).toBeNull()
        expect(connection.telemetryPool).toBeNull()
      }
    })
  })

  test('an enabled client installs its own collector on every normal socket before the first dial', async () => {
    const collector = new TelemetryMetrics({ kind: 'admin' })
    const cluster = new Cluster({
      logger: newLogger(),
      brokers: ['a:1'],
      clientId: 'c',
      retry: { retries: 0 },
      telemetryMetrics: collector,
    })
    const built = await cluster.connectionPoolBuilder.build({ host: 'a', port: 1 })
    for (const connection of built.pool) {
      expect(connection.telemetrySink).toBe(collector)
      expect(connection.telemetrySlot).toBeNull()
      expect(connection.telemetryPool).toBe(built)
    }
    const dedicated = cluster.connectionPoolBuilder.buildTelemetry(
      { host: 'a', port: 1 },
      { guard: () => null, remaining: () => 1000 }
    )
    expect(dedicated.pool[0].telemetrySink).toBeNull()
  })
})

describe('same-Kafka siblings and reconnect', () => {
  test('siblings accept distinct UUIDs and keep a cached identity through reconnect', async () => {
    let allocations = 0
    const answer = (fields) => ({
      clientInstanceId:
        fields.clientInstanceId ||
        `${(++allocations).toString(16).padStart(8, '0')}-0506-0708-090a-0b0c0d0e0f10`,
      subscriptionId: allocations,
      requestedMetrics: ['com.example.'],
      pushIntervalMs: 100,
      acceptedCompressionTypes: [0],
    })
    const wire = createScriptedCluster({
      nodes: NODES,
      profile: 'share',
      advertise: {
        GetTelemetrySubscriptions: [0, 0],
        PushTelemetry: [0, 0],
        InitProducerId: [5, 5],
        Produce: [13, 13],
      },
    })
    wire.answer('GetTelemetrySubscriptions', answer)
    const bind = jest.spyOn(TelemetryFactory.prototype, 'bind')
    const kafka = new Kafka(wire.kafkaOptions({ enableMetricsPush: true }))
    const clients = [
      ...kinds.map((kind) => make(kafka, kind)),
      make(kafka, 'producer'),
      make(kafka, 'producer'),
    ]
    const owners = [...bind.mock.contexts]
    try {
      const ids = []
      for (const [index, client] of clients.entries()) {
        client.registerMetricForSubscription({
          name: 'com.example.sibling',
          type: 'gauge',
          value: index + 1,
        })
        await client.connect()
        ids.push(await client.clientInstanceId({ timeout: 1000 }))
      }
      expect(new Set(ids).size).toBe(6)
      expect(allocations).toBe(6)
      await until(() =>
        ids.every((id) =>
          wire
            .requests()
            .some(
              (r) =>
                r.api === 'PushTelemetry' &&
                !r.fields.terminating &&
                r.fields.clientInstanceId === id
            )
        )
      )
      for (const [index, id] of ids.entries()) {
        const push = wire
          .requests()
          .find((r) => r.api === 'PushTelemetry' && r.fields.clientInstanceId === id)
        const emitted = metrics(Buffer.from(push.fields.metrics, 'hex'))
        const point = values(values(emitted[0], 5)[0], 1)[0]
        expect(values(point, 4)[0].readDoubleLE()).toBe(index + 1)
      }
      await clients[0].disconnect({ timeout: 1000 })
      clients[0].registerMetricForSubscription({
        name: 'com.example.sibling',
        type: 'gauge',
        value: 73,
      })
      const reconnectAt = wire.requests().length
      await expect(clients[0].clientInstanceId({ timeout: 0 })).resolves.toBe(ids[0])
      await clients[0].connect()
      await expect(clients[0].clientInstanceId({ timeout: 0 })).resolves.toBe(ids[0])
      await until(
        () =>
          wire.requests().filter((r) => r.api === 'GetTelemetrySubscriptions').length === 7 &&
          wire
            .requests()
            .filter((r) => r.api === 'GetTelemetrySubscriptions')
            .at(-1).fields.clientInstanceId === ids[0]
      )
      expect(allocations).toBe(6)
      await until(() =>
        wire
          .requests()
          .slice(reconnectAt)
          .some(
            (r) =>
              r.api === 'PushTelemetry' &&
              !r.fields.terminating &&
              r.fields.clientInstanceId === ids[0]
          )
      )
      const normal = wire
        .requests()
        .slice(reconnectAt)
        .find(
          (r) =>
            r.api === 'PushTelemetry' &&
            !r.fields.terminating &&
            r.fields.clientInstanceId === ids[0]
        )
      const emitted = metrics(Buffer.from(normal.fields.metrics, 'hex'))
      expect(values(values(values(emitted[0], 5)[0], 1)[0], 4)[0].readDoubleLE()).toBe(73)
      await until(() => owners[0].machine.reopen === null)
      expect(owners[0].machine.reopen).toBeNull()
      expect(new Set(owners.map((owner) => owner.machine.subscription)).size).toBe(6)
    } finally {
      for (const client of clients) await client.disconnect({ timeout: 0 })
      wire.assertClean()
      expect(wire.sockets().every((socket) => socket.destroyed)).toBe(true)
      for (const owner of owners) {
        expect(owner.machine.timer).toBeNull()
        expect(owner.machine.waiters.size).toBe(0)
      }
      bind.mockRestore()
    }
  })
})

describe('owner lifecycle joins', () => {
  afterEach(() => jest.restoreAllMocks())

  test.each(
    kinds.flatMap((kind) =>
      [undefined, 1000, 5000].flatMap((timeout) =>
        [true, false].map((coreFirst) => [kind, timeout, coreFirst])
      )
    )
  )(
    'public %s close at %p joins a real final Push independently of core (core first %p)',
    async (kind, timeout, coreFirst) => {
      const wire = scripted(kind)
      wire.answer('GetTelemetrySubscriptions', () => ({
        clientInstanceId: ID,
        subscriptionId: 1,
        requestedMetrics: ['com.example.'],
        pushIntervalMs: 300000,
        acceptedCompressionTypes: [0],
      }))
      const final = held()
      const core = held()
      let finalArrived = false
      let coreArrived = false
      wire.answer('PushTelemetry', async (fields) => {
        if (fields.terminating) {
          finalArrived = true
          await final.promise
        }
        return {}
      })
      const bind = jest.spyOn(TelemetryFactory.prototype, 'bind')
      const client = make(
        new Kafka(wire.kafkaOptions({ enableMetricsPush: true, enforceRequestTimeout: false })),
        kind
      )
      const owner = bind.mock.contexts.at(-1)
      const cluster = bind.mock.calls.at(-1)[0]
      let closing
      let coreDisconnect
      try {
        client.registerMetricForSubscription({ name: 'com.example.close', type: 'gauge', value: 7 })
        await client.connect()
        await client.clientInstanceId({ timeout: 1000 })
        const before = wire.requests().filter((r) => r.api === 'GetTelemetrySubscriptions').length
        const disconnect = cluster.disconnect.bind(cluster)
        coreDisconnect = jest.spyOn(cluster, 'disconnect').mockImplementation(async (options) => {
          coreArrived = true
          await core.promise
          return disconnect(options)
        })
        const events = []
        client.on(client.events.DISCONNECT, () => events.push('disconnect'))
        let settled = false
        closing = client.disconnect(timeout === undefined ? undefined : { timeout })
        closing.then(() => {
          settled = true
        })
        await until(() => finalArrived && coreArrived)
        const episode = owner.machine.closing
        expect(episode.deadline - owner.machine.clock.now()).toBeLessThanOrEqual(1000)
        const second = client.disconnect({ timeout: 0 })
        expect(owner.machine.closing).toBe(episode)
        if (coreFirst) {
          core.release()
          await until(() => wire.sockets().filter((socket) => !socket.destroyed).length === 1)
        } else {
          final.release()
          await until(() => owner.machine.state === 'CLOSED')
        }
        expect(settled).toBe(false)
        core.release()
        final.release()
        await Promise.all([closing, second])
        expect(events).toEqual(['disconnect'])
        expect(
          wire.requests().filter((r) => r.api === 'PushTelemetry' && r.fields.terminating)
        ).toHaveLength(1)
        expect(wire.requests().filter((r) => r.api === 'GetTelemetrySubscriptions')).toHaveLength(
          before
        )
      } finally {
        core.release()
        final.release()
        if (closing) await closing
        coreDisconnect?.mockRestore()
        await client.disconnect({ timeout: 0 })
        wire.assertClean()
        expect(wire.sockets().every((socket) => socket.destroyed)).toBe(true)
        expect(owner.machine.timer).toBeNull()
        expect(owner.machine.waiters.size).toBe(0)
        bind.mockRestore()
      }
    }
  )

  test.each(kinds.flatMap((kind) => [0, 20, 1000, 5000].map((timeout) => [kind, timeout])))(
    'public %s close at %p sends no final after its captured budget expires during encoding',
    async (kind, timeout) => {
      const wire = scripted(kind)
      wire.answer('GetTelemetrySubscriptions', () => ({
        clientInstanceId: ID,
        subscriptionId: 1,
        requestedMetrics: ['com.example.'],
        pushIntervalMs: 300000,
        acceptedCompressionTypes: [0],
      }))
      const bind = jest.spyOn(TelemetryFactory.prototype, 'bind')
      const client = make(new Kafka(wire.kafkaOptions({ enableMetricsPush: true })), kind)
      const owner = bind.mock.contexts.at(-1)
      const cluster = bind.mock.calls.at(-1)[0]
      const encode = held()
      let building = false
      let clock = owner.machine.clock.now()
      owner.machine.clock = { ...owner.machine.clock, now: () => clock }
      let build
      try {
        client.registerMetricForSubscription({ name: 'com.example.close', type: 'gauge', value: 7 })
        await client.connect()
        await client.clientInstanceId({ timeout: 1000 })
        const original = owner.machine.payload.buildPayload.bind(owner.machine.payload)
        build = jest
          .spyOn(owner.machine.payload, 'buildPayload')
          .mockImplementation(async (...args) => {
            building = true
            await encode.promise
            return original(...args)
          })
        const sockets = wire.sockets().length
        const gets = wire.requests().filter((r) => r.api === 'GetTelemetrySubscriptions').length
        const disconnect = jest.spyOn(cluster, 'disconnect')
        const closing = client.disconnect({ timeout })
        if (timeout > 0) {
          await until(() => building)
          const episode = owner.machine.closing
          expect(episode.deadline - clock).toBeLessThanOrEqual(Math.min(1000, timeout))
          clock = episode.deadline
        }
        encode.release()
        await closing
        expect(disconnect).toHaveBeenCalledTimes(1)
        expect(wire.requests().filter((r) => r.api === 'PushTelemetry')).toHaveLength(0)
        expect(wire.requests().filter((r) => r.api === 'GetTelemetrySubscriptions')).toHaveLength(
          gets
        )
        expect(wire.sockets()).toHaveLength(sockets)
      } finally {
        encode.release()
        build?.mockRestore()
        await client.disconnect({ timeout: 0 })
        wire.assertClean()
        expect(wire.sockets().every((socket) => socket.destroyed)).toBe(true)
        expect(owner.machine.waiters.size).toBe(0)
        bind.mockRestore()
      }
    }
  )

  test.each(['handler', 'hook', 'commit', 'leave'])(
    'a real consumer close keeps its %s stage independent of the terminating Push',
    async (stage) => {
      const { groupScript } = require('../../../testHelpers/scriptedCluster')
      const {
        TOPIC,
        prepareRecordBatches,
      } = require('../../consumer/__tests__/fixtures/fetchHarness')
      await prepareRecordBatches()
      const brokerAnswerTimers = new Set()
      const wire = groupScript(scripted('consumer'), { brokerAnswerTimers })
      wire.answer('GetTelemetrySubscriptions', () => ({
        clientInstanceId: ID,
        subscriptionId: 1,
        requestedMetrics: ['com.example.'],
        pushIntervalMs: 300000,
        acceptedCompressionTypes: [0],
      }))
      const core = held()
      const final = held()
      let coreArrived = false
      let finalArrived = false
      const deliveries = []
      const events = []
      wire.answer('PushTelemetry', async ({ terminating }) => {
        if (terminating) {
          finalArrived = true
          await final.promise
        }
        return {}
      })
      for (const [name, target] of [
        ['OffsetCommit', 'commit'],
        ['LeaveGroup', 'leave'],
      ]) {
        const original = wire.answerers.get(name)
        wire.answer(name, async (...args) => {
          if (stage === target) {
            coreArrived = true
            await core.promise
          }
          return original(...args)
        })
      }
      const bind = jest.spyOn(TelemetryFactory.prototype, 'bind')
      const consumer = make(new Kafka(wire.kafkaOptions({ enableMetricsPush: true })), 'consumer', {
        maxWaitTimeInMs: 20,
      })
      const owner = bind.mock.contexts.at(-1)
      let closing
      try {
        consumer.on(consumer.events.STOP, () => events.push('stop'))
        consumer.on(consumer.events.DISCONNECT, () => events.push('disconnect'))
        consumer.registerMetricForSubscription({
          name: 'com.example.close',
          type: 'gauge',
          value: 7,
        })
        await consumer.connect()
        await consumer.subscribe({ topic: TOPIC, fromBeginning: true })
        await consumer.run({
          autoCommit: true,
          autoCommitInterval: 60000,
          eachMessage: async ({ message }) => {
            deliveries.push(message.offset)
            if (stage === 'handler') {
              coreArrived = true
              await core.promise
            }
          },
          rebalanceListener: {
            onPartitionsRevoked: async ({ partitions }) => {
              if (partitions.length && stage === 'hook') {
                coreArrived = true
                await core.promise
              }
            },
          },
        })
        await until(() => deliveries.length === 1)
        await consumer.clientInstanceId({ timeout: 1000 })
        let settled = false
        closing = consumer.disconnect({ timeout: 5000, leaveGroup: true })
        closing.then(() => {
          settled = true
        })
        await until(() => coreArrived && finalArrived)
        const episode = owner.machine.closing
        await expect(consumer.disconnect({ timeout: -1 })).rejects.toBeInstanceOf(
          KafkaJSInvalidConfigurationError
        )
        const second = consumer.disconnect({ timeout: 0, leaveGroup: false })
        expect(owner.machine.closing).toBe(episode)
        final.release()
        await until(() => owner.machine.state === 'CLOSED')
        expect(settled).toBe(false)
        core.release()
        await Promise.all([closing, second])
        expect(events).toEqual(['stop', 'disconnect'])
        expect(deliveries).toEqual(['0'])
        const commits = wire.requests().filter((r) => r.api === 'OffsetCommit')
        expect(commits).toHaveLength(1)
        expect(commits[0].fields.topics[0].partitions[0].offset).toBe('1')
        expect(wire.requests().filter((r) => r.api === 'LeaveGroup')).toHaveLength(1)
        expect(
          wire.requests().filter((r) => r.api === 'PushTelemetry' && r.fields.terminating)
        ).toHaveLength(1)
      } finally {
        core.release()
        final.release()
        if (closing) await closing
        await consumer.disconnect({ timeout: 0 })
        for (const timer of brokerAnswerTimers) clearTimeout(timer)
        wire.assertClean()
        expect(wire.sockets().every((socket) => socket.destroyed)).toBe(true)
        expect(owner.machine.timer).toBeNull()
        expect(owner.machine.waiters.size).toBe(0)
        bind.mockRestore()
      }
    }
  )

  test('disabled disconnects never touch telemetry', async () => {
    const close = jest.spyOn(ClientTelemetry.prototype, 'close')
    for (const kind of kinds) {
      const wire = scripted(kind)
      const client = make(new Kafka(wire.kafkaOptions()), kind)
      await client.connect()
      await client.disconnect({ timeout: 0 })
    }
    expect(close).not.toHaveBeenCalled()
  })

  test.each(kinds)(
    'default-off %s disconnect has the same microtask and event ordering as no telemetry work',
    async (kind) => {
      const capture = async (noWork) => {
        const wire = scripted(kind)
        const client = make(new Kafka(wire.kafkaOptions()), kind)
        await client.connect()
        const gate = held()
        const clusterDisconnect = Cluster.prototype.disconnect
        const clusters = new Set()
        const disconnect = jest
          .spyOn(Cluster.prototype, 'disconnect')
          .mockImplementation(function () {
            clusters.add(this)
            return gate.promise
          })
        const close = noWork
          ? jest.spyOn(TelemetryFactory.prototype, 'close').mockReturnValue(null)
          : null
        let turn = 0
        const trace = []
        client.on(client.events.DISCONNECT, () => trace.push(['event', turn]))
        const done = client.disconnect({ timeout: 0 }).then(() => trace.push(['closed', turn]))
        gate.release()
        for (; turn < 100; turn++) await Promise.resolve()
        await done
        close?.mockRestore()
        disconnect.mockRestore()
        for (const cluster of clusters) await clusterDisconnect.call(cluster, { drainTimeout: 0 })
        wire.assertClean()
        expect(wire.sockets().every((socket) => socket.destroyed)).toBe(true)
        return trace
      }
      expect(await capture(false)).toEqual(await capture(true))
    }
  )

  describe.each([false, true])('producer connect racing a close with telemetry %p', (enabled) => {
    test('a lifecycle retired before connect resumes keeps its result and starts no telemetry', async () => {
      const wire = scripted('producer')
      const resume = jest.spyOn(ClientTelemetry.prototype, 'resume')
      const producer = make(
        new Kafka(wire.kafkaOptions({ enableMetricsPush: enabled })),
        'producer'
      )
      const wait = Generation.prototype.wait
      let closing = null
      jest.spyOn(Generation.prototype, 'wait').mockImplementation(function (operation) {
        const result = wait.call(this, operation)
        result.then(
          () => {
            closing = closing ?? producer.disconnect({ timeout: 0 })
          },
          () => {}
        )
        return result
      })
      await expect(producer.connect()).resolves.toBeUndefined()
      await closing
      expect(resume).not.toHaveBeenCalled()
    })

    test('an ordinary successful connect resumes enabled telemetry and disabled never', async () => {
      const wire = scripted('producer')
      const resume = jest.spyOn(ClientTelemetry.prototype, 'resume')
      const producer = make(
        new Kafka(wire.kafkaOptions({ enableMetricsPush: enabled })),
        'producer'
      )
      try {
        await producer.connect()
        if (enabled) expect(resume).toHaveBeenCalled()
        else expect(resume).not.toHaveBeenCalled()
      } finally {
        await producer.disconnect({ timeout: 0 })
      }
    })
  })

  describe('consumer owner with a private binding', () => {
    const fakeTelemetry = (extra = {}) => ({
      resume: jest.fn(),
      suspend: jest.fn(),
      close: jest.fn(() => null),
      methods: () => ({}),
      setResourceReader: jest.fn(),
      ...extra,
    })
    const stubCluster = () => {
      const instrumentationEmitter = new InstrumentationEventEmitter()
      return {
        instrumentationEmitter,
        committedOffsetsByGroup: new Map(),
        operationsByGroup: new Map(),
        connect: jest.fn(async () => {}),
        disconnect: jest.fn(async () => {}),
        metadata: async () => ({ topicMetadata: [] }),
        addMultipleTargetTopics: async () => {},
        refreshMetadata: async () => {},
        refreshMetadataIfNecessary: async () => {},
        committedOffsets({ groupId }) {
          if (!this.committedOffsetsByGroup.has(groupId))
            this.committedOffsetsByGroup.set(groupId, {})
          return this.committedOffsetsByGroup.get(groupId)
        },
        offsetOperations({ groupId }) {
          if (!this.operationsByGroup.has(groupId)) {
            this.operationsByGroup.set(groupId, new OffsetOperations())
          }
          return this.operationsByGroup.get(groupId)
        },
        defaultOffset: ({ fromBeginning }) => (fromBeginning ? -2 : -1),
        topicId: () => '11121314-1516-1718-191a-1b1c1d1e1f20',
        onTopicRecreated: () => () => {},
        removeBroker: () => {},
      }
    }
    const build = (telemetry, retry = {}) => {
      const cluster = stubCluster()
      return {
        cluster,
        consumer: createConsumer({
          cluster,
          telemetry,
          groupId: 'g',
          logger: newLogger(),
          instrumentationEmitter: cluster.instrumentationEmitter,
          metadataMaxAge: 300000,
          retry: { retries: 0, initialRetryTime: 5, restartOnFailure: async () => true, ...retry },
        }),
      }
    }
    const started = async (telemetry, retry) => {
      const start = jest.spyOn(Runner.prototype, 'start').mockImplementation(async function () {
        this.running = true
      })
      jest.spyOn(Runner.prototype, 'stop').mockResolvedValue()
      const built = build(telemetry, retry)
      await built.consumer.subscribe({ topic: 'topic', fromBeginning: true })
      await built.consumer.run({ eachMessage: async () => {}, autoCommit: false })
      return { ...built, start, runner: start.mock.instances[0] }
    }

    test('connect resumes once the cluster is usable and before CONNECT is delivered', async () => {
      const telemetry = fakeTelemetry()
      const { cluster, consumer } = build(telemetry)
      const order = []
      telemetry.resume.mockImplementation(() => order.push('resume'))
      consumer.on(consumer.events.CONNECT, () => order.push('connect-event'))
      await consumer.connect()
      expect(cluster.connect).toHaveBeenCalledTimes(1)
      expect(order).toEqual(['resume', 'connect-event'])
      await consumer.disconnect()
    })

    test('a crash suspends before any await, sends no final and a restart resumes; stop does neither', async () => {
      const telemetry = fakeTelemetry()
      const { start, runner, consumer } = await started(telemetry)
      expect(telemetry.resume).toHaveBeenCalledTimes(1)
      const crashing = runner.onCrash(
        Object.assign(new Error('temporary failure'), { retriable: true, retryTime: 5 })
      )
      expect(telemetry.suspend).toHaveBeenCalledTimes(1)
      expect(telemetry.close).not.toHaveBeenCalled()
      await crashing
      await until(() => start.mock.calls.length === 2)
      await until(() => telemetry.resume.mock.calls.length === 2)
      await consumer.stop()
      expect(telemetry.suspend).toHaveBeenCalledTimes(1)
      expect(telemetry.close).not.toHaveBeenCalled()
      expect(telemetry.resume).toHaveBeenCalledTimes(2)
      await consumer.disconnect()
      expect(telemetry.close).toHaveBeenCalledTimes(1)
    })

    test.each(
      ['connect', 'join'].flatMap((phase) => [false, true].map((restart) => [phase, restart]))
    )(
      'a real failed Runner %s stays suspended through restart decision and backoff (%p)',
      async (phase, restart) => {
        jest.useFakeTimers()
        const ConsumerGroup = require('../../consumer/consumerGroup')
        const telemetry = new TelemetryFactory({
          kind: 'consumer',
          config: { enableMetricsPush: true, telemetryMaxBytes: 1048576 },
        })
        telemetry.bind({ createTelemetryTransport: () => ({ candidates: () => [] }) })
        const resume = jest.spyOn(telemetry, 'resume')
        const decision = held()
        const deciding = jest.fn(() => decision.promise)
        const start = jest.spyOn(Runner.prototype, 'start')
        jest.spyOn(Runner.prototype, 'stop').mockResolvedValue()
        jest.spyOn(Runner.prototype, 'scheduleFetchManager').mockImplementation(() => {})
        const initialRound = jest.spyOn(Runner.prototype, 'runInitialRound').mockResolvedValue()
        const connecting = jest.spyOn(ConsumerGroup.prototype, 'connect').mockResolvedValue()
        const failure = Object.assign(new Error('initial ' + phase), {
          retriable: true,
          retryTime: 25,
        })
        if (phase === 'connect') connecting.mockRejectedValueOnce(failure)
        else initialRound.mockRejectedValueOnce(failure)
        const { consumer } = build(telemetry, { restartOnFailure: deciding, initialRetryTime: 25 })
        try {
          await consumer.subscribe({ topic: 'topic' })
          const running = consumer.run({ eachMessage: async () => {}, autoCommit: false })
          for (let turn = 0; turn < 100 && !deciding.mock.calls.length; turn++)
            await Promise.resolve()
          expect(deciding).toHaveBeenCalledTimes(1)
          await expect(consumer.clientInstanceId()).rejects.toMatchObject({
            reason: 'not-connected',
          })
          expect(resume).not.toHaveBeenCalled()
          decision.release(restart)
          await running
          expect(resume).not.toHaveBeenCalled()
          await expect(consumer.clientInstanceId()).rejects.toMatchObject({
            reason: 'not-connected',
          })
          await jest.advanceTimersByTimeAsync(24)
          expect(start).toHaveBeenCalledTimes(1)
          if (restart) {
            await jest.advanceTimersByTimeAsync(1)
            expect(start).toHaveBeenCalledTimes(2)
            expect(resume).toHaveBeenCalledTimes(1)
          }
        } finally {
          decision.release(false)
          for (const instance of start.mock.instances) {
            instance.heartbeatManager.shutdown()
            instance.pollTimer.stop()
          }
          await consumer.disconnect({ timeout: 0 })
          jest.useRealTimers()
        }
      }
    )

    test('the first disconnect captures its budget once and joins the held close with core cleanup', async () => {
      const gate = held()
      const telemetry = fakeTelemetry({ close: jest.fn(() => gate.promise) })
      const { cluster, consumer } = build(telemetry)
      await consumer.connect()
      const first = consumer.disconnect({ timeout: 5000 })
      expect(telemetry.close).toHaveBeenCalledTimes(1)
      const [budget] = telemetry.close.mock.calls[0]
      expect(budget.zero).toBe(false)
      expect(budget.remaining()).toBeGreaterThan(0)
      expect(budget.remaining()).toBeLessThanOrEqual(5000)
      expect(Number.isFinite(budget.deadline)).toBe(true)
      let settled = false
      first.then(() => {
        settled = true
      })
      await until(() => cluster.disconnect.mock.calls.length === 1)
      await new Promise((resolve) => setTimeout(resolve, 20))
      expect(settled).toBe(false)
      const second = consumer.disconnect({ timeout: 0 })
      expect(telemetry.close).toHaveBeenCalledTimes(1)
      gate.release()
      await Promise.all([first, second])
    })

    test('equal member strings after a new membership invalidate an older snapshot', async () => {
      const telemetry = fakeTelemetry()
      const { runner } = await started(telemetry)
      const read = telemetry.setResourceReader.mock.calls[0][0]
      const group = runner.consumerGroup
      group.memberId = 'member-1'
      const old = read()
      expect(old.values).toEqual({ group_member_id: 'member-1' })
      expect(old.isCurrent()).toBe(true)
      group.ownership.renew()
      expect(old.isCurrent()).toBe(false)
      const next = read()
      expect(next.values).toEqual({ group_member_id: 'member-1' })
      expect(next.isCurrent()).toBe(true)
      group.memberId = 'member-2'
      expect(next.isCurrent()).toBe(false)
    })

    test('without a run the reader has no member', () => {
      const telemetry = fakeTelemetry()
      build(telemetry)
      expect(telemetry.setResourceReader.mock.calls[0][0]()).toBeNull()
    })
  })

  describe('share owner with a private binding', () => {
    const fakeTelemetry = (extra = {}) => ({
      resume: jest.fn(),
      suspend: jest.fn(),
      close: jest.fn(() => null),
      methods: () => ({}),
      setResourceReader: jest.fn(),
      ...extra,
    })
    const build = (telemetry, cluster = {}) => {
      const runner = new ShareRunner({
        ...validateShareConsumerOptions({ groupId: 'g' }),
        logger: newLogger(),
        instrumentationEmitter: new InstrumentationEventEmitter(),
        requestTimeout: 5000,
        cluster: {
          connect: jest.fn(async () => {}),
          findConnectedBroker: jest.fn(async () => ({})),
          disconnect: jest.fn(async () => {}),
          ...cluster,
        },
        telemetry,
      })
      runner.checkCapability = jest.fn()
      return runner
    }

    test('connect resumes once the member can run and the facade exposes the telemetry methods', async () => {
      const telemetry = fakeTelemetry({ methods: () => ({ clientInstanceId: jest.fn() }) })
      const runner = build(telemetry)
      expect(typeof runner.api().clientInstanceId).toBe('function')
      expect(Object.isFrozen(runner.api())).toBe(true)
      await runner.connect()
      await runner.connect()
      expect(telemetry.resume).toHaveBeenCalledTimes(1)
    })

    test('the first disconnect captures one budget in the monotonic domain and joins the held close', async () => {
      const gate = held()
      const telemetry = fakeTelemetry({ close: jest.fn(() => gate.promise) })
      const runner = build(telemetry)
      await runner.connect()
      const first = runner.disconnect({ timeout: 5000 })
      expect(telemetry.close).toHaveBeenCalledTimes(1)
      const [budget] = telemetry.close.mock.calls[0]
      expect(budget.zero).toBe(false)
      expect(budget.remaining()).toBeGreaterThan(0)
      expect(budget.remaining()).toBeLessThanOrEqual(5000)
      let settled = false
      first.then(() => {
        settled = true
      })
      await until(() => runner.options.cluster.disconnect.mock.calls.length === 1)
      await new Promise((resolve) => setTimeout(resolve, 20))
      expect(settled).toBe(false)
      const second = runner.disconnect({ timeout: 0 })
      expect(telemetry.close).toHaveBeenCalledTimes(1)
      gate.release()
      await Promise.all([first, second])
    })

    test('an invalid disconnect option changes nothing', async () => {
      const telemetry = fakeTelemetry()
      const runner = build(telemetry)
      await runner.connect()
      await expect(runner.disconnect({ timeout: -1 })).rejects.toBeInstanceOf(
        KafkaJSInvalidConfigurationError
      )
      expect(telemetry.close).not.toHaveBeenCalled()
    })

    test('a crash suspends before any cleanup await and never closes', async () => {
      const telemetry = fakeTelemetry()
      const runner = build(telemetry)
      runner.subscription = ['topic']
      runner.superviseCrash = jest.fn(async () => {})
      const run = runner.newRun(async () => {}, false)
      run.firstJoin.promise.catch(() => {})
      jest.spyOn(ShareRun.prototype, 'beginClose').mockReturnValue({ done: Promise.resolve() })
      const crashing = run.crash(new Error('fatal'))
      expect(telemetry.suspend).toHaveBeenCalledTimes(1)
      await crashing
      expect(telemetry.close).not.toHaveBeenCalled()
      run.finishRun()
    })

    test('the reader follows the current run and its issued membership identity', () => {
      const telemetry = fakeTelemetry()
      const runner = build(telemetry)
      const read = telemetry.setResourceReader.mock.calls[0][0]
      expect(read()).toBeNull()
      runner.subscription = ['topic']
      const run = runner.newRun(async () => {}, false)
      const old = read()
      expect(old.values).toEqual({ group_member_id: run.membership.memberId })
      expect(old.isCurrent()).toBe(true)
      run.membership.token = Symbol('membership')
      expect(old.isCurrent()).toBe(false)
      expect(read().isCurrent()).toBe(true)
      run.finishRun()
      expect(read()).toBeNull()
    })
  })
})
