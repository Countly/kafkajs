const TelemetryMetrics = require('../metrics')
const { PublicTelemetryRound } = require('../../../testHelpers/lifecycle')
const { KafkaJSInvalidConfigurationError } = require('../../errors')

/** @param {object} metrics collector @returns {object} selected current sample */
const sample = (metrics) => {
  const s = { revision: 1n, requestedMetrics: ['*'], deltaTemporality: true }
  metrics.select(s)
  const snapshot = metrics.snapshot(s)
  metrics.abandon(snapshot)
  return snapshot
}

/** @param {string} [name] series name @returns {object} legal Gauge descriptor */
const gauge = (name = 'com.example.value') => ({ name, type: 'gauge', value: 0 })

describe('numeric registration is atomic and bounded', () => {
  test.each(['producer', 'consumer', 'admin', 'shareConsumer'])(
    'numeric snapshots work for %s',
    (kind) => {
      const metrics = new TelemetryMetrics({ kind })
      const metric = {
        name: 'com.example.total',
        type: 'sum',
        value: 3n,
        attributes: { z: 'z', a: 'a' },
      }
      metrics.register(metric)
      metrics.register({ ...metric, value: 5n, attributes: { a: 'a', z: 'z' } })
      expect(
        sample(metrics)
          .points.filter((p) => p.name === metric.name)
          .map((p) => ({ value: p.value, attributes: p.attributes }))
      ).toEqual([
        {
          value: 5n,
          attributes: [
            { key: 'a', value: 'a' },
            { key: 'z', value: 'z' },
          ],
        },
      ])
      metrics.unregister(metric)
      metrics.unregister(metric)
      expect(sample(metrics).points.some((p) => p.name === metric.name)).toBe(false)
    }
  )

  test.each([
    null,
    [],
    Promise.resolve(0),
    {},
    { name: 'nodot', type: 'gauge', value: 0 },
    { name: 'org.apache.kafka.fake', type: 'gauge', value: 0 },
    { name: 'com.bad-name', type: 'gauge', value: 0 },
    { name: 'a.' + 'a'.repeat(199), type: 'gauge', value: 0 },
    { ...gauge(), value: NaN },
    { ...gauge(), value: Infinity },
    { ...gauge(), value: '1' },
    { ...gauge(), value: null },
    { ...gauge(), value: 1n },
    { ...gauge(), value: () => 1 },
    { ...gauge(), value: Promise.resolve(1) },
    { ...gauge(), type: 'counter' },
    { ...gauge(), type: 'sum', value: 1 },
    { ...gauge(), type: 'sum', value: -1n },
    { ...gauge(), type: 'sum', value: 9223372036854775808n },
    { ...gauge(), unit: '' },
    { ...gauge(), unit: 'é' },
    { ...gauge(), unit: 'x'.repeat(17) },
    { ...gauge(), attributes: null },
    { ...gauge(), attributes: [] },
    { ...gauge(), attributes: { 'bad.key': '' } },
    { ...gauge(), attributes: { a: 1 } },
    { ...gauge(), attributes: { a: 'é'.repeat(129) } },
    {
      ...gauge(),
      attributes: Object.fromEntries(Array.from({ length: 9 }, (_, i) => ['a' + i, ''])),
    },
  ])('rejects invalid descriptors without storage mutation %#', (metric) => {
    const metrics = new TelemetryMetrics({ kind: 'producer' })
    metrics.register(gauge('com.example.keep'))
    expect(() => metrics.register(metric)).toThrow(KafkaJSInvalidConfigurationError)
    expect(
      sample(metrics)
        .points.filter((p) => p.name.startsWith('com.'))
        .map((p) => p.name)
    ).toEqual(['com.example.keep'])
  })

  test.each([
    'client_rack',
    'group_id',
    'group_instance_id',
    'group_member_id',
    'transactional_id',
    'client_instance_id',
    'client_id',
    'client_software_name',
    'client_software_version',
    'client_source_address',
    'client_source_port',
    'principal',
    'node_id',
  ])('rejects reserved attribute %s', (key) => {
    const metrics = new TelemetryMetrics({ kind: 'producer' })
    expect(() => metrics.register({ ...gauge(), attributes: { [key]: 'spoof' } })).toThrow(
      KafkaJSInvalidConfigurationError
    )
  })

  test('metadata is copied once and sampling never invokes application accessors', () => {
    const metrics = new TelemetryMetrics({ kind: 'producer' })
    const reads = jest.fn(() => 1)
    const attrs = { a: 'initial' }
    const descriptor = {
      ...gauge(),
      attributes: attrs,
      get value() {
        return reads()
      },
    }
    metrics.register(descriptor)
    attrs.a = 'mutated'
    Object.defineProperty(descriptor, 'value', {
      get: () => {
        throw new Error('secret')
      },
    })
    expect(sample(metrics).points.find((p) => p.name === descriptor.name)).toMatchObject({
      value: 1,
      attributes: [{ key: 'a', value: 'initial' }],
    })
    expect(reads).toHaveBeenCalledTimes(1)
  })

  test('throwing proxy and getter produce fixed errors without logging or causes', () => {
    const logger = { warn: jest.fn() }
    const metrics = new TelemetryMetrics({ kind: 'producer', logger })
    const secret = {
      get message() {
        throw new Error('do not inspect')
      },
    }
    for (const metric of [
      new Proxy(
        {},
        {
          ownKeys: () => {
            throw secret
          },
          getOwnPropertyDescriptor: () => {
            throw secret
          },
        }
      ),
      {
        get name() {
          throw secret
        },
      },
    ]) {
      let error
      try {
        metrics.register(metric)
      } catch (e) {
        error = e
      }
      expect(error).toBeInstanceOf(KafkaJSInvalidConfigurationError)
      expect(error.message).toBe('Invalid telemetry metric configuration')
      expect(error.cause).toBeUndefined()
    }
    expect(logger.warn).not.toHaveBeenCalled()
  })

  test('name type and unit remain immutable until its last identity is removed', () => {
    const metrics = new TelemetryMetrics({ kind: 'producer' })
    const metric = { name: 'com.example.total', type: 'sum', value: 3n }
    metrics.register(metric)
    expect(() => metrics.register({ ...metric, value: 2n })).toThrow(
      KafkaJSInvalidConfigurationError
    )
    expect(() =>
      metrics.register({ ...metric, type: 'gauge', value: 3, attributes: { other: 'x' } })
    ).toThrow(KafkaJSInvalidConfigurationError)
    expect(() => metrics.register({ ...metric, unit: 'ms', attributes: { other: 'x' } })).toThrow(
      KafkaJSInvalidConfigurationError
    )
    expect(sample(metrics).points.find((p) => p.name === metric.name).value).toBe(3n)
    metrics.unregister(metric)
    metrics.register({ ...gauge(metric.name), unit: 'ms' })
    expect(sample(metrics).points.find((p) => p.name === metric.name).unit).toBe('ms')
  })

  test('capacity permits updates and refuses a new identity atomically', () => {
    const metrics = new TelemetryMetrics({
      kind: 'producer',
      clock: { wallMs: () => 1000, monotonicNs: () => 0n },
    })
    for (let i = 0; i < 128; i++) metrics.register(gauge('com.example.' + i))
    const beforeUpdate = sample(metrics).points.filter((p) => p.name.startsWith('com.'))
    metrics.register({ ...gauge('com.example.0'), value: -2.5 })
    const afterUpdate = sample(metrics).points.filter((p) => p.name.startsWith('com.'))
    expect(afterUpdate).toEqual(
      beforeUpdate.map((p) => (p.name === 'com.example.0' ? { ...p, value: -2.5 } : p))
    )
    expect(() => metrics.register(gauge('com.example.extra'))).toThrow(
      expect.objectContaining({ reason: 'limit' })
    )
    const afterRefusal = sample(metrics).points.filter((p) => p.name.startsWith('com.'))
    expect(afterRefusal).toHaveLength(128)
    expect(afterRefusal).toEqual(afterUpdate)
    metrics.unregister(gauge('com.example.1'))
    metrics.register(gauge('com.example.extra'))
  })

  test('accepts the largest legal descriptor and prototype-safe identity keys', () => {
    const metrics = new TelemetryMetrics({ kind: 'producer' })
    const attributes = Object.fromEntries(
      Array.from({ length: 8 }, (_, i) => [('a' + i).padEnd(63, 'x'), 'é'.repeat(128)])
    )
    const name = 'a.' + 'x'.repeat(198)
    metrics.register({
      name,
      unit: 'u'.repeat(16),
      attributes,
      type: 'sum',
      value: 9223372036854775807n,
    })
    expect(sample(metrics).points.find((p) => p.name === name).value).toBe(9223372036854775807n)
    metrics.register({
      ...gauge('com.example.proto'),
      attributes: Object.fromEntries([['__proto__', 'data']]),
    })
    expect(sample(metrics).points.find((p) => p.name === 'com.example.proto').attributes).toEqual([
      { key: '__proto__', value: 'data' },
    ])
    expect({}.data).toBeUndefined()
  })

  test('unregistration ignores extra getters and inherited fields', () => {
    const metrics = new TelemetryMetrics({ kind: 'producer' })
    metrics.register(gauge())
    metrics.unregister({
      name: gauge().name,
      get value() {
        throw new Error('unused')
      },
      get unit() {
        throw new Error('unused')
      },
    })
    expect(sample(metrics).points.some((p) => p.name === gauge().name)).toBe(false)
    expect(() => metrics.register(Object.create(gauge()))).toThrow(KafkaJSInvalidConfigurationError)
  })

  test.each([-0, 0, -2.5])('retains explicitly registered Gauge %s', (value) => {
    const metrics = new TelemetryMetrics({ kind: 'producer' })
    metrics.register({ ...gauge(), value })
    expect(
      Object.is(sample(metrics).points.find((p) => p.name === gauge().name).value, value)
    ).toBe(true)
  })
  test('removing one attributes set retains the name declaration until the last is gone', () => {
    const metrics = new TelemetryMetrics({ kind: 'producer' })
    const a = { ...gauge(), attributes: { a: '' } }
    const b = { ...gauge(), attributes: { b: 'x' } }
    metrics.register(a)
    metrics.register(b)
    metrics.unregister(a)
    expect(() => metrics.register({ ...a, unit: 'ms' })).toThrow(KafkaJSInvalidConfigurationError)
    metrics.unregister(b)
    metrics.register({ ...a, unit: 'ms' })
  })

  test('callbacks and inherited metadata are never collected as numeric values', () => {
    const metrics = new TelemetryMetrics({ kind: 'producer' })
    const callback = jest.fn()
    expect(() => metrics.register({ ...gauge(), value: callback })).toThrow(
      KafkaJSInvalidConfigurationError
    )
    expect(callback).not.toHaveBeenCalled()
    const attrs = Object.assign(Object.create({ inherited: 'private' }), { own: '' })
    metrics.register({ ...gauge(), attributes: attrs })
    expect(sample(metrics).points.find((p) => p.name === gauge().name).attributes).toEqual([
      { key: 'own', value: '' },
    ])
    expect(() => metrics.unregister({ name: 'invalid' })).toThrow(KafkaJSInvalidConfigurationError)
  })
})

describe('public final reentrancy', () => {
  test.each(['producer', 'consumer', 'admin', 'shareConsumer'])(
    '%s refuses an old final after a reentrant unregister and register',
    async (kind) => {
      const identity = { name: 'com.example.total' }
      const round = new PublicTelemetryRound({ kind, enforceRequestTimeout: true })
      try {
        await round.wait(() => round.start())
        await round.ready()
        const before = round.capture()
        round.atFinalPrewrite(() => {
          round.client.unregisterMetricFromSubscription(identity)
          round.client.registerMetricForSubscription({ ...identity, type: 'sum', value: 11n })
        })
        await round.wait(() => round.beginClose(round.closeOptions(20)))
        expect(round.finalFrames()).toEqual([])
        expect(round.writtenFor(round.finalSnapshotIdentity())).toBe(0)
        await round.wait(() => round.client.connect())
        await round.ready()
        const point = round.decodedLatestPoint(identity)
        expect(point.value).toBe(11n)
        expect(point.metricRevision).not.toBe(before.metricRevision)
        expect(point.origin).toBe(round.replacementMetricOrigin())
      } finally {
        await round.dispose()
      }
    },
    60000
  )
})
