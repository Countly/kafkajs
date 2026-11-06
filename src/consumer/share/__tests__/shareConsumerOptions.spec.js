jest.mock('../../../cluster')
const { Kafka } = require('../../../../index')
const Cluster = require('../../../cluster')
const { validateShareConsumerOptions } = require('..')
const { KafkaJSInvalidConfigurationError, KafkaJSNonRetriableError } = require('../../../errors')
const { events } = require('../instrumentationEvents')

const numeric = [
  ['maxRecords', 1, 500],
  ['minBytes', 0, 1],
  ['maxBytes', 1, 10485760],
  ['maxWaitTimeInMs', 0, 500],
  ['metadataMaxAge', 1, 300000],
  ['maxPollInterval', 1, 300000],
]
const rejected = [
  ['fromBeginning', 'share.auto.offset.reset'],
  ['readUncommitted', 'share.isolation.level'],
  ['groupInstanceId', 'no static membership'],
  ['partitionAssigners', 'broker assigns'],
  ['sessionTimeout', 'share.session.timeout.ms'],
  ['rebalanceTimeout', 'share.session.timeout.ms'],
  ['heartbeatInterval', 'share.heartbeat.interval.ms'],
  ['groupProtocol', 'broker assigns'],
  ['groupRemoteAssignor', 'broker assigns'],
  ['maxBytesPerPartition', 'no per-partition limit'],
  ['maxPrefetchBatches', 'prefetch is not offered'],
]

describe('Share consumer option validation', () => {
  beforeEach(() => jest.clearAllMocks())
  test('normalizes defaults without constructing client state', () => {
    expect(validateShareConsumerOptions({ groupId: 'g' })).toEqual({
      groupId: 'g',
      acknowledgementMode: 'implicit',
      acquireMode: 'record_limit',
      maxRecords: 500,
      minBytes: 1,
      maxBytes: 10485760,
      maxWaitTimeInMs: 500,
      metadataMaxAge: 300000,
      maxPollInterval: 300000,
      rackId: '',
      retry: { retries: 5 },
      allowAutoTopicCreation: true,
      checkCrcs: true,
      maxInFlightRequests: 5,
    })
    expect(Cluster).not.toHaveBeenCalled()
  })
  for (const [name, minimum] of numeric) {
    test.each([0, 1, 2147483647, 2147483648, -1, 1.5, NaN, Infinity, '5', null])(
      `${name} validates %p before constructing a cluster`,
      (value) => {
        const kafka = new Kafka({ brokers: [] })
        const build = () => kafka.shareConsumer({ groupId: 'g', [name]: value })
        const accepted = Number.isInteger(value) && value >= minimum && value <= 2147483647
        if (accepted) {
          expect(build).not.toThrow()
          expect(Cluster).toHaveBeenCalledTimes(1)
        } else {
          expect(build).toThrow(KafkaJSInvalidConfigurationError)
          expect(Cluster).not.toHaveBeenCalled()
        }
      }
    )
  }
  test.each(rejected)('%s names its equivalent and constructs no cluster', (name, equivalent) => {
    const kafka = new Kafka({ brokers: [] })
    for (const value of [null, false, 0, '', {}]) {
      expect(() => kafka.shareConsumer({ groupId: 'g', [name]: value })).toThrow(equivalent)
      expect(() => kafka.shareConsumer({ groupId: 'g', [name]: value })).toThrow(
        KafkaJSInvalidConfigurationError
      )
    }
    expect(Cluster).not.toHaveBeenCalled()
    expect(() => kafka.shareConsumer({ groupId: 'g', [name]: undefined })).not.toThrow()
  })
  test.each([undefined, null, [], 'g', 5])('rejects invalid constructor shape %p', (input) => {
    expect(() => validateShareConsumerOptions(input)).toThrow(KafkaJSInvalidConfigurationError)
  })
  test.each([undefined, '', 1, null])('requires a non-empty group id %p', (groupId) => {
    expect(() => validateShareConsumerOptions({ groupId })).toThrow(
      KafkaJSInvalidConfigurationError
    )
  })
  test.each([
    ['acknowledgementMode', 'manual'],
    ['acquireMode', 'strict'],
    ['rackId', 5],
    ['allowAutoTopicCreation', 0],
    ['checkCrcs', 'true'],
  ])('rejects invalid %s', (key, value) => {
    expect(() => validateShareConsumerOptions({ groupId: 'g', [key]: value })).toThrow(
      KafkaJSInvalidConfigurationError
    )
  })
  test.each([0, -1, 1.5, '5', NaN, Infinity])(
    'uses the consumer error for invalid in-flight limit %p',
    (value) => {
      expect(() =>
        validateShareConsumerOptions({ groupId: 'g', maxInFlightRequests: value })
      ).toThrow(KafkaJSNonRetriableError)
    }
  )
  test.each([null, 1, 2147483648])('accepts the consumer in-flight domain %p', (value) => {
    expect(
      validateShareConsumerOptions({ groupId: 'g', maxInFlightRequests: value }).maxInFlightRequests
    ).toBe(value)
  })
  test('validates the merged inherited retry before constructing a cluster', () => {
    const kafka = new Kafka({ brokers: [], retry: { maxTotalTime: 0 } })
    expect(() => kafka.shareConsumer({ groupId: 'g' })).toThrow(KafkaJSInvalidConfigurationError)
    expect(Cluster).not.toHaveBeenCalled()
    expect(() => kafka.shareConsumer({ groupId: 'g', retry: { maxTotalTime: 1000 } })).not.toThrow()
  })
  test('reads a known getter once and preserves a thrown getter identity', () => {
    const kafka = new Kafka({ brokers: [] })
    const cause = new Error('getter')
    const getter = jest.fn(() => {
      throw cause
    })
    expect(() =>
      kafka.shareConsumer({
        groupId: 'g',
        get maxRecords() {
          return getter()
        },
      })
    ).toThrow(cause)
    expect(getter).toHaveBeenCalledTimes(1)
    expect(Cluster).not.toHaveBeenCalled()
  })
  test('ignores unknown getters and copies retry before constructing client state', () => {
    const retry = { retries: 8 }
    const input = {
      groupId: 'g',
      retry,
      get unknownOption() {
        throw new Error('unknown')
      },
    }
    const options = validateShareConsumerOptions(input)
    retry.retries = 99
    expect(options.retry.retries).toBe(8)
  })
  test.each([0, -1, 1.5, Infinity, '5', 2147483648])(
    'checks inherited retry deadline %p',
    (value) => {
      expect(() =>
        validateShareConsumerOptions({ groupId: 'g', retry: { maxTotalTime: value } })
      ).toThrow(KafkaJSInvalidConfigurationError)
    }
  )
})

describe('public share option contract', () => {
  beforeEach(() => jest.clearAllMocks())
  const build = () => new Kafka({ brokers: [] }).shareConsumer({ groupId: 'g' })

  test('keeps the public surface additive', () => {
    const share = build()
    expect(Object.keys(share).sort()).toEqual([
      'acquisitionLockTimeoutMs',
      'assignment',
      'connect',
      'disconnect',
      'events',
      'logger',
      'on',
      'run',
      'stop',
      'subscribe',
      'subscription',
    ])
    expect(Object.isFrozen(share.events)).toBe(true)
    expect(Object.keys(share.events)).toHaveLength(18)
    expect(Object.keys(share.events).sort()).toEqual(Object.keys(events).sort())
    const root = require('../../../../index')
    for (const internal of ['ShareRunner', 'ShareRun', 'ShareHandlerCall', 'ShareCloseEpisode'])
      expect(root[internal]).toBeUndefined()
    expect(typeof root.Kafka.prototype.shareConsumer).toBe('function')
  })

  test('returns idempotent listener removers and rejects unknown names synchronously', () => {
    const share = build()
    const remove = share.on(share.events.CONNECT, () => {})
    expect(typeof remove).toBe('function')
    remove()
    expect(() => remove()).not.toThrow()
    for (const name of ['unknown', 'consumer.fetch', 'share_consumer.missing']) {
      expect(() => share.on(name, () => {})).toThrow(KafkaJSNonRetriableError)
    }
  })

  test.each([
    [{ topics: [] }, 'an empty list'],
    [{ topics: [''] }, 'an empty name'],
    [{ topics: ['a', 'a'] }, 'a duplicate'],
    [{ topics: [5] }, 'a number'],
    [{ topics: [/a/] }, 'a regular expression'],
    [{ topic: 'a' }, 'the singular topic form'],
    [{ topics: ['a'], regex: /a/ }, 'a regex key'],
    [{ topics: ['a'], fromBeginning: false }, 'fromBeginning false'],
    [{ topics: ['a'], offsetReset: 'earliest' }, 'an offset reset'],
    [undefined, 'no argument'],
  ])('rejects %p (%s) before any state or request', async (input) => {
    const share = build()
    await expect(share.subscribe(input)).rejects.toThrow(KafkaJSInvalidConfigurationError)
    expect(share.subscription()).toEqual([])
    expect(Cluster).toHaveBeenCalledTimes(1)
    expect(Cluster.mock.instances[0].connect).not.toHaveBeenCalled()
  })

  test('names the group configuration for the rejected subscription keys', async () => {
    const share = build()
    for (const key of ['fromBeginning', 'offsetReset']) {
      await expect(share.subscribe({ topics: ['a'], [key]: true })).rejects.toThrow(
        'share.auto.offset.reset'
      )
    }
  })

  test('requires exactly one handler function and a subscription before running', async () => {
    const share = build()
    const handler = async () => {}
    await expect(share.run({ eachMessage: handler })).rejects.toThrow('subscribe() must be called')
    for (const config of [
      undefined,
      {},
      { eachMessage: handler, eachBatch: handler },
      { eachMessage: 'handler' },
      { eachBatch: null },
    ]) {
      await expect(share.run(config)).rejects.toThrow(KafkaJSInvalidConfigurationError)
    }
    expect(Cluster.mock.instances[0].connect).not.toHaveBeenCalled()
  })

  test.each([0, 1, 2147483647])('accepts an object disconnect timeout of %p', async (timeout) => {
    await expect(build().disconnect({ timeout })).resolves.toBeUndefined()
  })

  test.each([2147483648, -1, 1.5, NaN, Infinity, '5', null])(
    'rejects the disconnect timeout %p before any close mutation',
    async (timeout) => {
      const share = build()
      await expect(share.disconnect({ timeout })).rejects.toThrow(KafkaJSInvalidConfigurationError)
      expect(Cluster.mock.instances[0].disconnect).not.toHaveBeenCalled()
    }
  )

  test.each(['SIGTERM', 5, null, [], () => {}, { unknown: true }])(
    'ignores the non-options disconnect argument %p',
    async (argument) => {
      await expect(build().disconnect(argument)).resolves.toBeUndefined()
    }
  )

  test('starts the lock duration and assignment snapshots empty', () => {
    const share = build()
    expect(share.assignment()).toEqual([])
    expect(share.acquisitionLockTimeoutMs()).toBeNull()
    expect(share.subscription()).toEqual([])
  })

  test('requires a finite request timeout for the share factory only', () => {
    const kafka = new Kafka({ brokers: [], requestTimeout: 2147483648 })
    expect(() => kafka.shareConsumer({ groupId: 'g' })).toThrow(KafkaJSInvalidConfigurationError)
    expect(Cluster).not.toHaveBeenCalled()
  })
})
