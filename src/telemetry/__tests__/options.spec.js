const { Kafka, logLevel, KafkaJSInvalidConfigurationError } = require('../../..')
const { snapshotOptions, TelemetryFactory } = require('../factory')
const TelemetryMetrics = require('../metrics')
const ClientTelemetry = require('../client')

const root = (options = {}) =>
  new Kafka({ brokers: ['localhost:9092'], logLevel: logLevel.NOTHING, ...options })

/** Each public factory, called with the given extra options. */
const factories = {
  producer: (kafka, extra) => kafka.producer({ idempotent: false, ...extra }),
  consumer: (kafka, extra) => kafka.consumer({ groupId: 'g', ...extra }),
  admin: (kafka, extra) => kafka.admin(extra),
  shareConsumer: (kafka, extra) => kafka.shareConsumer({ groupId: 'g', ...extra }),
}
const kinds = Object.keys(factories)

describe('telemetry option snapshots', () => {
  test('omission inherits and explicit false overrides true', () => {
    const source = { enableMetricsPush: true, telemetryMaxBytes: 5 }
    const parent = snapshotOptions(source, []).config
    source.enableMetricsPush = false
    source.telemetryMaxBytes = 6
    expect(parent).toEqual({ enableMetricsPush: true, telemetryMaxBytes: 5 })
    expect(snapshotOptions({ enableMetricsPush: false }, [], parent).config).toEqual({
      enableMetricsPush: false,
      telemetryMaxBytes: 5,
    })
    expect(snapshotOptions({ telemetryMaxBytes: 1 }, [], parent).config).toEqual({
      enableMetricsPush: true,
      telemetryMaxBytes: 1,
    })
    expect(snapshotOptions({}, []).config).toEqual({
      enableMetricsPush: false,
      telemetryMaxBytes: 1048576,
    })
    expect(Object.isFrozen(parent)).toBe(true)
  })

  test.each([1, 1048576])('accepts the integer byte limit %p', (value) => {
    expect(snapshotOptions({ telemetryMaxBytes: value }, []).config.telemetryMaxBytes).toBe(value)
  })

  test('copies only the requested known fields once, including inherited ones', () => {
    const reads = []
    const source = Object.create({ inherited: 'value' })
    for (const name of ['known', 'enableMetricsPush', 'telemetryMaxBytes', 'unknown']) {
      Object.defineProperty(source, name, {
        enumerable: true,
        get() {
          reads.push(name)
          return undefined
        },
      })
    }
    const { options } = snapshotOptions(source, ['known', 'inherited'])
    expect(options.inherited).toBe('value')
    expect(reads.sort()).toEqual(['enableMetricsPush', 'known', 'telemetryMaxBytes'])
  })

  test('a nullish source keeps its native destructuring failure', () => {
    expect(() => snapshotOptions(undefined, [])).toThrow(TypeError)
    expect(() => snapshotOptions(null, [])).toThrow(TypeError)
  })

  describe.each(kinds)('%s factory', (kind) => {
    test.each([null, 0, 1, 'true', {}, [], NaN])(
      'rejects the nonboolean opt-in %p before constructing state',
      (value) => {
        const kafka = root({ enableMetricsPush: true })
        const bind = jest.spyOn(TelemetryFactory.prototype, 'bind')
        try {
          expect(() => factories[kind](kafka, { enableMetricsPush: value })).toThrow(
            KafkaJSInvalidConfigurationError
          )
          expect(bind).not.toHaveBeenCalled()
        } finally {
          bind.mockRestore()
        }
      }
    )

    test.each([null, 0, -1, 0.5, NaN, Infinity, -Infinity, 1048577, '1', 1n, true, {}])(
      'validates the byte limit %p even while disabled',
      (value) => {
        const bind = jest.spyOn(TelemetryFactory.prototype, 'bind')
        try {
          expect(() => factories[kind](root(), { telemetryMaxBytes: value })).toThrow(
            KafkaJSInvalidConfigurationError
          )
          expect(bind).not.toHaveBeenCalled()
        } finally {
          bind.mockRestore()
        }
      }
    )

    test('a child override beats the Kafka default in both directions', () => {
      const bind = jest.spyOn(TelemetryFactory.prototype, 'bind')
      try {
        factories[kind](root({ enableMetricsPush: true, telemetryMaxBytes: 7 }), {})
        factories[kind](root({ enableMetricsPush: true }), { enableMetricsPush: false })
        factories[kind](root({ enableMetricsPush: false }), {
          enableMetricsPush: true,
          telemetryMaxBytes: 9,
        })
        const [inherited, disabled, enabled] = bind.mock.contexts.map((owner) => owner.config)
        expect(inherited).toEqual({ enableMetricsPush: true, telemetryMaxBytes: 7 })
        expect(disabled).toEqual({ enableMetricsPush: false, telemetryMaxBytes: 1048576 })
        expect(enabled).toEqual({ enableMetricsPush: true, telemetryMaxBytes: 9 })
      } finally {
        bind.mockRestore()
      }
    })

    test('a later mutation of the caller options cannot change the telemetry configuration', () => {
      const extra = { enableMetricsPush: true, telemetryMaxBytes: 3 }
      const bind = jest.spyOn(TelemetryFactory.prototype, 'bind')
      try {
        factories[kind](root(), extra)
        extra.enableMetricsPush = false
        extra.telemetryMaxBytes = 4
        expect(bind.mock.contexts[0].config).toEqual({
          enableMetricsPush: true,
          telemetryMaxBytes: 3,
        })
      } finally {
        bind.mockRestore()
      }
    })
  })

  test('invalid Kafka-wide values reject before any client exists', () => {
    for (const enableMetricsPush of [null, 1, 'true']) {
      expect(() => root({ enableMetricsPush })).toThrow(KafkaJSInvalidConfigurationError)
    }
    for (const telemetryMaxBytes of [null, 0, -1, 0.5, 1048577, '1']) {
      expect(() => root({ enableMetricsPush: false, telemetryMaxBytes })).toThrow(
        KafkaJSInvalidConfigurationError
      )
    }
  })

  test('throwing getters and proxies expose only a fixed configuration failure', () => {
    let reads = 0
    const access = {
      get enableMetricsPush() {
        reads++
        throw Error('private-value')
      },
    }
    const proxy = new Proxy(
      {},
      {
        get() {
          throw Error('private-value')
        },
      }
    )
    const register = jest.spyOn(TelemetryMetrics.prototype, 'register')
    const machine = jest.spyOn(ClientTelemetry.prototype, 'resume')
    const factory = jest.spyOn(TelemetryFactory.prototype, 'bind')
    try {
      for (const options of [access, proxy]) {
        expect(() => snapshotOptions(options, [])).toThrow('Invalid client telemetry options')
        expect(() => new Kafka(options)).toThrow(KafkaJSInvalidConfigurationError)
        for (const kind of kinds) {
          const call = () =>
            kind === 'producer'
              ? root().producer(options)
              : kind === 'consumer'
                ? root().consumer(options)
                : kind === 'admin'
                  ? root().admin(options)
                  : root().shareConsumer(options)
          expect(call).toThrow(KafkaJSInvalidConfigurationError)
        }
      }
      expect(reads).toBe(6)
      expect(register).not.toHaveBeenCalled()
      expect(machine).not.toHaveBeenCalled()
      expect(factory).not.toHaveBeenCalled()
    } finally {
      register.mockRestore()
      machine.mockRestore()
      factory.mockRestore()
    }
  })

  test('a throwing share getter outside telemetry keeps its identity', () => {
    const cause = new Error('getter')
    expect(() =>
      root().shareConsumer({
        groupId: 'g',
        get maxRecords() {
          throw cause
        },
      })
    ).toThrow(cause)
  })

  test('disabled clients allocate no collector, machine, adapter or timer', async () => {
    const bind = jest.spyOn(TelemetryFactory.prototype, 'bind')
    const resume = jest.spyOn(ClientTelemetry.prototype, 'resume')
    try {
      const kafka = root()
      for (const kind of kinds) {
        const client = factories[kind](kafka, {})
        await expect(client.clientInstanceId()).rejects.toMatchObject({ reason: 'disabled' })
      }
      expect(bind.mock.contexts).toHaveLength(kinds.length)
      for (const owner of bind.mock.contexts) {
        expect(owner.metrics).toBeNull()
        expect(owner.machine).toBeNull()
      }
      expect(resume).not.toHaveBeenCalled()
    } finally {
      bind.mockRestore()
      resume.mockRestore()
    }
  })

  test('valid old factory call forms and inherited fields remain available', () => {
    const kafka = root()
    expect(kafka.producer()).toHaveProperty('send')
    expect(kafka.admin()).toHaveProperty('listTopics')
    expect(kafka.producer(7)).toHaveProperty('send')
    expect(kafka.admin(Object.assign(() => {}, { enableMetricsPush: false }))).toHaveProperty(
      'listTopics'
    )
    expect(kafka.consumer(Object.create({ groupId: 'inherited' }))).toHaveProperty('run')
    expect(kafka.shareConsumer(Object.create({ groupId: 'inherited' }))).toHaveProperty('run')
    expect(() =>
      snapshotOptions({ enableMetricsPush: undefined, telemetryMaxBytes: undefined }, [])
    ).not.toThrow()
    expect(() => kafka.producer(null)).toThrow(TypeError)
    expect(() => kafka.admin(null)).toThrow(TypeError)
    expect(() => kafka.consumer(null)).toThrow(TypeError)
  })

  test('telemetry snapshots preserve inherited producer and consumer option validation', () => {
    expect(() => root().producer({ metadataMaxIdle: 1 })).toThrow(KafkaJSInvalidConfigurationError)
    expect(() => root().consumer({ groupId: 'g', fetchSessions: 'false' })).toThrow(
      'fetchSessions must be a boolean'
    )
    expect(() => root().consumer({ groupId: 'g', rackAwareAssignment: 'false' })).toThrow(
      'rackAwareAssignment must be a boolean'
    )
  })

  test.each([false, true])('forwards rackAwareAssignment %p to custom assigners', (rackAware) => {
    const seen = []
    const assigner = ({ rackAware: value }) => {
      seen.push(value)
      return {
        name: 'probe',
        version: 1,
        assign: async () => [],
        protocol: () => ({ name: 'probe', metadata: Buffer.alloc(0) }),
      }
    }
    for (const enableMetricsPush of [false, true]) {
      root().consumer({
        groupId: 'g',
        partitionAssigners: [assigner],
        rackAwareAssignment: rackAware,
        enableMetricsPush,
      })
    }
    expect(seen).toEqual([rackAware, rackAware])
  })
})
