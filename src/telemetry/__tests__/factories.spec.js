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
        owner.suspend()
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
    'enabled %s stores before connect, after close and while stopped without restarting',
    async (kind) => {
      const binding = new TelemetryFactory({
        kind,
        config: { enableMetricsPush: true, telemetryMaxBytes: 1048576 },
      })
      const open = jest.fn()
      binding.bind({ createTelemetryTransport: () => ({ candidates: () => [], open }) })
      const client = binding.methods()
      client.registerMetricForSubscription({ name: 'com.example.total', type: 'sum', value: 1n })
      await binding.close({ zero: true })
      client.registerMetricForSubscription({ name: 'com.example.total', type: 'sum', value: 2n })
      binding.machine.stop()
      client.registerMetricForSubscription({ name: 'com.example.total', type: 'sum', value: 3n })
      client.unregisterMetricFromSubscription({ name: 'com.example.absent' })
      expect(binding.machine.stopped).toBe(true)
      expect(open).not.toHaveBeenCalled()
      expect(() =>
        client.registerMetricForSubscription({ name: 'com.example.total', type: 'sum', value: 2n })
      ).toThrow(KafkaJSInvalidConfigurationError)
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
    await expect(binding.close({ zero: true })).resolves.toBeUndefined()
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
      requestedMetrics: [],
      pushIntervalMs: 1000,
      acceptedCompressionTypes: [0],
    })
    const wire = scripted('producer')
    const shareWire = scripted('shareConsumer')
    wire.answer('GetTelemetrySubscriptions', answer)
    shareWire.answer('GetTelemetrySubscriptions', answer)
    const bind = jest.spyOn(TelemetryFactory.prototype, 'bind')
    const kafka = new Kafka(wire.kafkaOptions({ enableMetricsPush: true }))
    const shareKafka = new Kafka(shareWire.kafkaOptions({ enableMetricsPush: true }))
    const clients = [
      make(kafka, 'producer'),
      make(kafka, 'consumer'),
      make(kafka, 'admin'),
      make(kafka, 'producer'),
      make(shareKafka, 'shareConsumer'),
    ]
    const owners = [...bind.mock.contexts]
    try {
      const ids = []
      for (const client of clients) {
        await client.connect()
        ids.push(await client.clientInstanceId({ timeout: 1000 }))
      }
      expect(new Set(ids).size).toBe(5)
      expect(allocations).toBe(5)
      await clients[0].disconnect({ timeout: 0 })
      await expect(clients[0].clientInstanceId({ timeout: 0 })).resolves.toBe(ids[0])
      await clients[0].connect()
      await expect(clients[0].clientInstanceId({ timeout: 0 })).resolves.toBe(ids[0])
      await until(
        () =>
          wire.requests().filter((r) => r.api === 'GetTelemetrySubscriptions').length === 5 &&
          wire
            .requests()
            .filter((r) => r.api === 'GetTelemetrySubscriptions')
            .at(-1).fields.clientInstanceId === ids[0]
      )
      expect(allocations).toBe(5)
    } finally {
      for (const client of clients) await client.disconnect({ timeout: 0 })
      for (const owner of owners) owner.suspend()
      bind.mockRestore()
    }
  })
})

describe('owner lifecycle joins', () => {
  afterEach(() => jest.restoreAllMocks())

  test.each(kinds)(
    'the first public %s disconnect joins one held telemetry close and never holds core cleanup back',
    async (kind) => {
      const wire = scripted(kind)
      wire.answer('GetTelemetrySubscriptions', () => ({
        clientInstanceId: ID,
        subscriptionId: 1,
        requestedMetrics: [],
        pushIntervalMs: 300000,
        acceptedCompressionTypes: [0],
      }))
      const gate = held()
      const close = jest.spyOn(TelemetryFactory.prototype, 'close').mockReturnValue(gate.promise)
      const disconnect = jest.spyOn(Cluster.prototype, 'disconnect')
      const bind = jest.spyOn(TelemetryFactory.prototype, 'bind')
      const client = make(new Kafka(wire.kafkaOptions({ enableMetricsPush: true })), kind)
      const owner = bind.mock.contexts.at(-1)
      try {
        await client.connect()
        const first = client.disconnect({ timeout: 5000 })
        expect(close).toHaveBeenCalledTimes(1)
        const [budget] = close.mock.calls[0]
        expect(budget.zero).toBe(false)
        expect(Number.isFinite(budget.deadline)).toBe(true)
        expect(budget.remaining()).toBeGreaterThan(0)
        expect(budget.remaining()).toBeLessThanOrEqual(5000)
        let settled = false
        first.then(() => {
          settled = true
        })
        await until(() => disconnect.mock.calls.length > 0)
        await new Promise((resolve) => setTimeout(resolve, 20))
        expect(settled).toBe(false)
        const second = client.disconnect({ timeout: 0 })
        expect(close).toHaveBeenCalledTimes(1)
        gate.release()
        await Promise.all([first, second])
      } finally {
        gate.release()
        owner.suspend()
      }
    }
  )

  test.each(kinds)('a zero timeout reaches the %s telemetry close as zero', async (kind) => {
    const wire = scripted(kind)
    const close = jest.spyOn(TelemetryFactory.prototype, 'close').mockResolvedValue()
    const bind = jest.spyOn(TelemetryFactory.prototype, 'bind')
    const client = make(new Kafka(wire.kafkaOptions({ enableMetricsPush: true })), kind)
    const owner = bind.mock.contexts.at(-1)
    try {
      await client.connect()
      await client.disconnect({ timeout: 0 })
      expect(close).toHaveBeenCalledTimes(1)
      expect(close.mock.calls[0][0].zero).toBe(true)
    } finally {
      owner.suspend()
    }
  })

  test.each(kinds)('a never-timed %s disconnect passes an unbounded budget', async (kind) => {
    const wire = scripted(kind)
    const close = jest.spyOn(TelemetryFactory.prototype, 'close').mockResolvedValue()
    const bind = jest.spyOn(TelemetryFactory.prototype, 'bind')
    const client = make(new Kafka(wire.kafkaOptions({ enableMetricsPush: true })), kind)
    const owner = bind.mock.contexts.at(-1)
    try {
      await client.connect()
      await client.disconnect()
      const [budget] = close.mock.calls[0]
      expect(budget.deadline).toBe(Infinity)
      expect(budget.remaining()).toBe(Infinity)
      expect(budget.zero).toBe(false)
    } finally {
      owner.suspend()
    }
  })

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
      const start = jest.spyOn(Runner.prototype, 'start').mockResolvedValue()
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
