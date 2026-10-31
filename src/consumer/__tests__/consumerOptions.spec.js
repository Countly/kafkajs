const { Kafka } = require('../../../index')
const createConsumer = require('../index')
const ConsumerGroupProtocol = require('../groupProtocol/consumer')
const {
  KafkaJSNonRetriableError,
  KafkaJSProtocolError,
  KafkaJSNumberOfRetriesExceeded,
} = require('../../errors')
const InstrumentationEventEmitter = require('../../instrumentation/emitter')
const OffsetOperations = require('../offsetManager/offsetOperations')
const { createErrorFromCode } = require('../../protocol/error')
const { newLogger, waitFor } = require('testHelpers')

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms))

const kafka = () =>
  new Kafka({
    clientId: 'options-test',
    brokers: ['localhost:9092'],
    logCreator: newLogger.logCreator,
  })

/** Everything `createConsumer` and its `subscribe`/`onCrash` paths touch, and nothing else. */
const fakeCluster = () => ({
  connect: jest.fn(async () => {}),
  disconnect: jest.fn(async () => {}),
  refreshMetadata: jest.fn(async ({ onMetadata } = {}) =>
    onMetadata?.({ topicMetadata: [{ topic: 'events-a' }, { topic: 'other' }] })
  ),
  useConsumerMetadataScope: jest.fn(),
  releaseConsumerMetadataScope: jest.fn(),
  resetTargetTopics: jest.fn(async () => {}),
  refreshMetadataIfNecessary: jest.fn(async () => {}),
  removeBroker: jest.fn(),
  isConnected: () => true,
  addMultipleTargetTopics: jest.fn(async () => {}),
  metadata: jest.fn(async () => ({ topicMetadata: [{ topic: 'events-a' }, { topic: 'other' }] })),
  onTopicRecreated: () => () => {},
  offsetOperations: () => new OffsetOperations(),
  retry: {},
})

/** A consumer over a stubbed cluster, for the cases where a subscription must actually succeed. */
const offlineConsumer = (config = {}, cluster = fakeCluster()) => ({
  cluster,
  consumer: createConsumer({
    cluster,
    groupId: 'group-a',
    logger: newLogger(),
    metadataMaxAge: 300000,
    ...config,
  }),
})

describe('Consumer > configuration > groupProtocol', () => {
  it('constructs with the consumer protocol and nothing else set', () => {
    expect(() => kafka().consumer({ groupId: 'group-a', groupProtocol: 'consumer' })).not.toThrow()
  })

  it('defaults to classic', () => {
    expect(() => kafka().consumer({ groupId: 'group-a' })).not.toThrow()
  })

  it('rejects an unknown value', () => {
    expect(() => kafka().consumer({ groupId: 'group-a', groupProtocol: 'next' })).toThrow(
      'groupProtocol must be one of "classic" or "consumer"'
    )
  })

  it.each([
    ['sessionTimeout', { sessionTimeout: 45000 }, 'sessionTimeout'],
    ['heartbeatInterval', { heartbeatInterval: 3000 }, 'heartbeatInterval'],
    ['partitionAssigners', { partitionAssigners: [] }, 'partitionAssigners'],
  ])('rejects an explicit %s under the consumer protocol', (_name, option, key) => {
    expect(() =>
      kafka().consumer({ groupId: 'group-a', groupProtocol: 'consumer', ...option })
    ).toThrow(`${key} cannot be set when groupProtocol=consumer`)
  })

  it('names every offending key, in Java order', () => {
    expect(() =>
      kafka().consumer({
        groupId: 'group-a',
        groupProtocol: 'consumer',
        sessionTimeout: 30000,
        heartbeatInterval: 3000,
        partitionAssigners: [],
      })
    ).toThrow(
      'partitionAssigners, heartbeatInterval, sessionTimeout cannot be set when groupProtocol=consumer'
    )
  })

  it('accepts rebalanceTimeout alongside the consumer protocol', () => {
    expect(() =>
      kafka().consumer({ groupId: 'group-a', groupProtocol: 'consumer', rebalanceTimeout: 60000 })
    ).not.toThrow()
  })

  it('still rejects sessionTimeout when rebalanceTimeout is also given', () => {
    expect(() =>
      kafka().consumer({
        groupId: 'group-a',
        groupProtocol: 'consumer',
        rebalanceTimeout: 60000,
        sessionTimeout: 30000,
      })
    ).toThrow('sessionTimeout cannot be set when groupProtocol=consumer')
  })

  it('rejects groupRemoteAssignor under the classic protocol', () => {
    expect(() => kafka().consumer({ groupId: 'group-a', groupRemoteAssignor: 'uniform' })).toThrow(
      'groupRemoteAssignor cannot be set when groupProtocol=classic'
    )
  })

  it('accepts it under the consumer protocol, and rejects a non-string', () => {
    expect(() =>
      kafka().consumer({
        groupId: 'group-a',
        groupProtocol: 'consumer',
        groupRemoteAssignor: 'uniform',
      })
    ).not.toThrow()

    expect(() =>
      kafka().consumer({ groupId: 'group-a', groupProtocol: 'consumer', groupRemoteAssignor: '' })
    ).toThrow('Argument "groupRemoteAssignor" must be a non-empty string')
  })

  // `null` IS the documented default ("let the coordinator pick its first configured
  // assignor"), so writing it down must not make a consumer unconstructible under either
  // protocol - and a `null` threaded from an optional config value must not either.
  it.each([
    ['classic', {}],
    ['consumer', { groupProtocol: 'consumer' }],
  ])('treats a null groupRemoteAssignor as "not set" under %s', (_protocol, extra) => {
    expect(() =>
      kafka().consumer({ groupId: 'group-a', groupRemoteAssignor: null, ...extra })
    ).not.toThrow()
  })

  it('asks the coordinator for no named assignor when the value is null', () => {
    const membership = new ConsumerGroupProtocol({
      cluster: fakeCluster(),
      groupId: 'group-a',
      logger: newLogger(),
      serverAssignor: null,
    })

    expect(membership.serverAssignor).toBeNull()
    expect(membership.groupProtocol).toEqual('')
  })

  // The rule the object-literal rebuild in src/index.js would otherwise break.
  it('treats an undefined value as "not set"', () => {
    expect(() =>
      createConsumer({
        // A stub rather than `{}`: the factory subscribes to the cluster's topic-recreation event
        // at construction, so the case would fail on a missing method instead of on validation.
        cluster: fakeCluster(),
        groupId: 'group-a',
        logger: newLogger(),
        groupProtocol: 'consumer',
        sessionTimeout: undefined,
        heartbeatInterval: undefined,
        partitionAssigners: undefined,
        metadataMaxAge: 300000,
      })
    ).not.toThrow()
  })
})

describe('Consumer > configuration > maxPrefetchBatches', () => {
  it.each([0, 1])('accepts %s', (value) => {
    expect(() => kafka().consumer({ groupId: 'group-a', maxPrefetchBatches: value })).not.toThrow()
  })

  it.each([2, -1, 1.5, '1'])('rejects %p', (value) => {
    expect(() => kafka().consumer({ groupId: 'group-a', maxPrefetchBatches: value })).toThrow(
      'maxPrefetchBatches must be 0 or 1; deeper prefetch is not supported yet'
    )
  })

  it('is absent from a classic config without effect', () => {
    expect(() => kafka().consumer({ groupId: 'group-a' })).not.toThrow()
  })
})

describe('Consumer > subscribe > regex', () => {
  const consumer = (config = {}) =>
    kafka().consumer({ groupId: 'group-a', groupProtocol: 'consumer', ...config })

  it('accepts a non-empty string under the consumer protocol', async () => {
    await expect(consumer().subscribe({ regex: 'events-.*' })).resolves.toBeUndefined()
  })

  it('rejects a regex under the classic protocol', async () => {
    await expect(
      kafka().consumer({ groupId: 'group-a' }).subscribe({ regex: 'events-.*' })
    ).rejects.toThrow("A regex subscription requires groupProtocol: 'consumer'")
  })

  it.each([[''], [42], [/events-.*/]])('rejects %p', async (regex) => {
    await expect(consumer().subscribe({ regex })).rejects.toThrow(
      'Argument "regex" must be a non-empty string'
    )
  })

  it('rejects a regex combined with topics in the same call', async () => {
    await expect(consumer().subscribe({ regex: 'events-.*', topics: ['a'] })).rejects.toThrow(
      'Subscribe to topics or to a regex, not both'
    )
  })

  it('rejects a regex after a name subscription, and a name after a regex', async () => {
    // The first subscription here SUCCEEDS, so this case goes through the stubbed cluster: the
    // real client would reach addMultipleTargetTopics -> refreshMetadata -> localhost:9092.
    const { consumer: withNames } = offlineConsumer({ groupProtocol: 'consumer' })
    await withNames.subscribe({ topics: ['events-a'] })
    await expect(withNames.subscribe({ regex: 'events-.*' })).rejects.toThrow(
      'Subscribe to topics or to a regex, not both'
    )

    const withRegex = consumer()
    await withRegex.subscribe({ regex: 'events-.*' })
    await expect(withRegex.subscribe({ topics: ['a'] })).rejects.toThrow(
      'Subscribe to topics or to a regex, not both'
    )
  })

  it('registers no target topic for a regex subscription', async () => {
    const { consumer: c, cluster } = offlineConsumer({ groupProtocol: 'consumer' })

    await c.subscribe({ regex: 'events-.*', fromBeginning: true })

    // The names are registered by growSubscription once the coordinator resolves the pattern, not
    // here - so a regex-only consumer legitimately starts with an empty target-topic set.
    expect(cluster.addMultipleTargetTopics).not.toHaveBeenCalled()
  })

  it('keeps client-side RegExp matching in topics under the consumer protocol', async () => {
    const { consumer: c, cluster } = offlineConsumer({ groupProtocol: 'consumer' })

    await c.subscribe({ topics: [/^events-/] })

    // A JavaScript RegExp in `topics` is evaluated against an all-topic refresh and re-evaluated at
    // later refreshes, unrelated to the broker-evaluated `regex` form: it names no target topic.
    expect(cluster.refreshMetadata).toHaveBeenCalledWith(
      expect.objectContaining({
        metadataScope: expect.any(Object),
        onMetadata: expect.any(Function),
      })
    )
    expect(cluster.useConsumerMetadataScope).toHaveBeenCalledTimes(1)
    expect(cluster.resetTargetTopics).toHaveBeenCalledWith([], {
      isCurrent: expect.any(Function),
    })
    expect(cluster.addMultipleTargetTopics).not.toHaveBeenCalled()
  })

  it('leaves broker-regex text untouched and exclusive with names and client patterns', async () => {
    const { consumer: c, cluster } = offlineConsumer({ groupProtocol: 'consumer' })
    await c.subscribe({ topics: [/^events-/] })
    const before = cluster.refreshMetadata.mock.calls.length

    for (const mixed of [{ regex: 'events-(' }, { regex: 'events-(', topics: ['a'] }]) {
      await expect(c.subscribe(mixed)).rejects.toThrow(
        'Subscribe to topics or to a regex, not both'
      )
    }
    expect(cluster.refreshMetadata).toHaveBeenCalledTimes(before)
    expect(cluster.useConsumerMetadataScope).toHaveBeenCalledTimes(1)

    const regexOnly = offlineConsumer({ groupProtocol: 'consumer' })
    await regexOnly.consumer.subscribe({ regex: 'events-(' })
    await expect(regexOnly.consumer.subscribe({ topics: [/^events-/] })).rejects.toThrow(
      'Subscribe to topics or to a regex, not both'
    )
    expect(regexOnly.cluster.useConsumerMetadataScope).not.toHaveBeenCalled()
  })

  it('keeps the declarations and the cluster scope as they were when the refresh fails', async () => {
    const { consumer: c, cluster } = offlineConsumer()
    cluster.refreshMetadata.mockRejectedValueOnce(new Error('metadata unavailable'))

    await expect(c.subscribe({ topics: [/^events-/] })).rejects.toThrow('metadata unavailable')
    expect(cluster.releaseConsumerMetadataScope).toHaveBeenCalledTimes(1)

    await c.subscribe({ topics: ['events-a'] })
    expect(cluster.addMultipleTargetTopics).toHaveBeenCalledWith(['events-a'], {
      fence: expect.any(Object),
    })
  })

  it('rejects an invalid reset policy before any declaration or scope changes', async () => {
    const { consumer: c, cluster } = offlineConsumer()

    await expect(c.subscribe({ topics: [/^events-/], offsetReset: 'sideways' })).rejects.toThrow()
    expect(cluster.useConsumerMetadataScope).not.toHaveBeenCalled()
    expect(cluster.refreshMetadata).not.toHaveBeenCalled()
  })

  it('checks the combination before the protocol and the type', async () => {
    // Order matters: this call is wrong three ways, and the message must be the first rule's.
    await expect(
      kafka()
        .consumer({ groupId: 'group-a' })
        .subscribe({ regex: '', topics: ['a'] })
    ).rejects.toThrow('Subscribe to topics or to a regex, not both')
  })

  it('keeps the existing message for a call with neither', async () => {
    await expect(consumer().subscribe({})).rejects.toThrow('Missing required argument "topics"')
  })
})

describe('Consumer > onCrash > a fatal membership error', () => {
  /**
   * Drives `onCrash` the way `crash.spec.js` does: the runner dies on start because the cluster's
   * connect throws, and the CRASH payload is what the case reads.
   */
  const crashWith = async (error, { groupProtocol = 'consumer', restartOnFailure } = {}) => {
    const cluster = fakeCluster()
    const instrumentationEmitter = new InstrumentationEventEmitter()
    const crashes = []
    const consumer = createConsumer({
      cluster,
      groupId: `fatal-${groupProtocol}`,
      logger: newLogger(),
      instrumentationEmitter,
      metadataMaxAge: 300000,
      groupProtocol,
      retry: { retries: 0, initialRetryTime: 20, restartOnFailure },
    })
    consumer.on(consumer.events.CRASH, (e) => crashes.push(e.payload))

    cluster.connect.mockImplementation(async () => {
      throw error
    })

    await consumer.subscribe({ topics: ['events-a'] })
    await consumer.run({ eachMessage: async () => {} })

    return { consumer, cluster, crashes }
  }

  const UNSUPPORTED_ASSIGNOR = 112
  const GROUP_MAX_SIZE_REACHED = 81
  const UNRELEASED_INSTANCE_ID = 111
  const INVALID_REGULAR_EXPRESSION = 128

  it.each([
    UNSUPPORTED_ASSIGNOR,
    GROUP_MAX_SIZE_REACHED,
    UNRELEASED_INSTANCE_ID,
    INVALID_REGULAR_EXPRESSION,
  ])(
    'emits CRASH with restart: false for code %i even when restartOnFailure returns true',
    async (code) => {
      const { consumer, cluster, crashes } = await crashWith(
        new KafkaJSProtocolError(createErrorFromCode(code)),
        { restartOnFailure: async () => true }
      )

      expect(crashes).toHaveLength(1)
      expect(crashes[0].restart).toBe(false)
      expect(cluster.connect).toHaveBeenCalledTimes(1)

      // No restart timer was armed: nothing reconnects after the crash.
      await sleep(200)
      expect(cluster.connect).toHaveBeenCalledTimes(1)

      await consumer.disconnect()
    }
  )

  it('does the same for a cluster that does not implement the consumer protocol', async () => {
    const unsupported = new KafkaJSNonRetriableError(
      'The cluster does not support the new CONSUMER group protocol'
    )
    unsupported.type = 'CONSUMER_PROTOCOL_UNSUPPORTED'

    const { consumer, crashes } = await crashWith(unsupported, {
      restartOnFailure: async () => true,
    })

    expect(crashes[0].restart).toBe(false)
    await consumer.disconnect()
  })

  // The error contract already called 30 and 42 fatal for a consumer-protocol member; the
  // runner and this override were gated on the narrower exported predicate, so such a member
  // crashed and was then restarted straight back into the same failure for ever.
  it.each([
    [30, 'GROUP_AUTHORIZATION_FAILED'],
    [42, 'INVALID_REQUEST'],
  ])(
    'emits CRASH with restart: false for code %i (%s) under the consumer protocol',
    async (code) => {
      const { consumer, cluster, crashes } = await crashWith(
        new KafkaJSProtocolError(createErrorFromCode(code)),
        { restartOnFailure: async () => true }
      )

      expect(crashes).toHaveLength(1)
      expect(crashes[0].restart).toBe(false)

      await sleep(200)
      expect(cluster.connect).toHaveBeenCalledTimes(1)

      await consumer.disconnect()
    }
  )

  it('leaves a classic member answered 30 restarting, exactly as it did', async () => {
    const { consumer, cluster, crashes } = await crashWith(
      new KafkaJSProtocolError(createErrorFromCode(30)),
      { groupProtocol: 'classic', restartOnFailure: async () => true }
    )

    expect(crashes).toHaveLength(1)
    expect(crashes[0].restart).toBe(true)
    await waitFor(() => cluster.connect.mock.calls.length > 1, { delay: 10, maxWait: 2000 })

    await consumer.disconnect()
  })

  it('leaves the classic path alone: the same code still honours restartOnFailure', async () => {
    const { consumer, cluster, crashes } = await crashWith(
      new KafkaJSProtocolError(createErrorFromCode(GROUP_MAX_SIZE_REACHED)),
      { groupProtocol: 'classic', restartOnFailure: async () => true }
    )

    expect(crashes).toHaveLength(1)
    expect(crashes[0].restart).toBe(true)
    await waitFor(() => cluster.connect.mock.calls.length > 1, { delay: 10, maxWait: 2000 })

    await consumer.disconnect()
  })

  it('still restarts a retriable error', async () => {
    const { consumer, cluster, crashes } = await crashWith(
      new KafkaJSNumberOfRetriesExceeded(new Error('boom'), { retryCount: 5 })
    )

    expect(crashes).toHaveLength(1)
    expect(crashes[0].restart).toBe(true)
    await waitFor(() => cluster.connect.mock.calls.length > 1, { delay: 10, maxWait: 2000 })

    await consumer.disconnect()
  })
})

describe('Consumer > configuration > pass-through', () => {
  /**
   * Starts a consumer with `ConsumerGroup` and `Runner` subclassed inside an isolated module
   * registry, and returns the options each was constructed with. The cluster's `connect` throws, so
   * the runner crashes right after both objects exist and nothing is left running.
   * @param {object} [config] extra consumer options
   * @param {object|object[]} [subscription] the subscription to make before running; a list is
   *   made in order, and each rejection is collected in `rejections`
   * @returns {Promise<{ group: object, runner: object, rejections: Error[] }>}
   */
  let group = null
  let runner = null

  const capturedOptions = async (config = {}, subscription = { topics: ['events-a'] }) => {
    const cluster = fakeCluster()
    const rejections = []
    cluster.connect.mockImplementation(async () => {
      throw new KafkaJSNonRetriableError('nothing to connect to')
    })

    group = null
    runner = null

    await jest.isolateModulesAsync(async () => {
      const RealConsumerGroup = require('../consumerGroup')
      const RealRunner = require('../runner')

      jest.doMock(
        '../consumerGroup',
        () =>
          class extends RealConsumerGroup {
            constructor(options) {
              super(options)
              group = options
            }
          }
      )
      jest.doMock(
        '../runner',
        () =>
          class extends RealRunner {
            constructor(options) {
              super(options)
              runner = options
            }
          }
      )

      const consumer = require('../index')({
        cluster,
        groupId: 'group-a',
        logger: newLogger(),
        metadataMaxAge: 300000,
        ...config,
      })

      for (const each of [].concat(subscription)) {
        await consumer.subscribe(each).catch((error) => rejections.push(error))
      }
      await consumer.run({ eachMessage: async () => {} })
      await consumer.disconnect()
    })

    return { group, runner, rejections, cluster }
  }

  it('hands the group and the runner the reconciled heartbeat interval', async () => {
    const { group, runner } = await capturedOptions({ heartbeatInterval: 5000 })

    expect(group.heartbeatInterval).toEqual(5000)
    expect(runner.heartbeatInterval).toEqual(5000)
  })

  it('hands the group the protocol options and the runner the prefetch depth', async () => {
    const { group, runner } = await capturedOptions(
      { groupProtocol: 'consumer', groupRemoteAssignor: 'uniform', maxPrefetchBatches: 1 },
      { regex: 'events-.*', fromBeginning: true }
    )

    expect(group.groupProtocol).toEqual('consumer')
    expect(group.groupRemoteAssignor).toEqual('uniform')
    expect(group.subscribedTopicRegex).toEqual('events-.*')
    expect(group.regexSubscription).toEqual({
      fromBeginning: true,
      resetPolicy: { kind: 'earliest' },
    })
    expect(group.maxPrefetchBatches).toEqual(1)
    expect(runner.maxPrefetchBatches).toEqual(1)
  })

  it('hands the group the decompression bound, 1 GiB unless the consumer sets one', async () => {
    const defaults = await capturedOptions()
    const lowered = await capturedOptions({ maxDecompressedBatchBytes: 4194304 })

    expect(defaults.group.maxDecompressedBatchBytes).toEqual(1073741824)
    expect(lowered.group.maxDecompressedBatchBytes).toEqual(4194304)
  })

  it('builds no client assigner under the consumer protocol', async () => {
    const { group } = await capturedOptions({ groupProtocol: 'consumer' })

    expect(group.assigners).toEqual([])
    expect(group.rebalanceProtocol).toBeUndefined()
  })

  it('keeps the classic defaults when nothing new is set', async () => {
    const { group, runner } = await capturedOptions()

    expect(group.groupProtocol).toEqual('classic')
    expect(group.groupRemoteAssignor).toBeNull()
    expect(group.subscribedTopicRegex).toBeNull()
    expect(group.maxPrefetchBatches).toEqual(0)
    expect(runner.maxPrefetchBatches).toEqual(0)
    expect(group.assigners).toHaveLength(1)
  })

  it('invalid reset policy leaves targets and prior subscription unchanged', async () => {
    const policy = { offsetReset: 'by_duration:PT5S' }
    const { group, rejections, cluster } = await capturedOptions({}, [
      { topics: ['events-a'], ...policy },
      { topics: ['events-b'], offsetReset: 'by_duration:P1W' },
      { topics: ['events-a'], offsetReset: 'none', fromBeginning: true },
      { topics: ['events-a'], fromBeginning: 'yes' },
    ])

    expect(rejections).toHaveLength(3)
    expect(rejections.map((error) => error.name)).toEqual(Array(3).fill('KafkaJSNonRetriableError'))
    expect(cluster.addMultipleTargetTopics).toHaveBeenCalledTimes(1)
    expect(group.topics).toEqual(['events-a'])
    expect(group.topicConfigurations['events-a']).toEqual({
      fromBeginning: false,
      resetPolicy: { kind: 'by_duration', durationMs: 5000 },
    })
    expect(policy).toEqual({ offsetReset: 'by_duration:PT5S' })
  })

  it('last successful subscription policy wins per topic', async () => {
    const { group } = await capturedOptions({}, [
      { topics: ['events-a', 'events-b'], fromBeginning: true },
      { topics: ['events-a'], offsetReset: 'none' },
      { topics: ['events-b'], offsetReset: 'latest' },
    ])

    expect(group.topicConfigurations['events-a']).toEqual({
      fromBeginning: false,
      resetPolicy: { kind: 'none' },
    })
    expect(group.topicConfigurations['events-b']).toEqual({
      fromBeginning: false,
      resetPolicy: { kind: 'latest' },
    })
  })

  it('hands a pattern consumer its declarations even when no topic matches', async () => {
    const { group, rejections } = await capturedOptions({}, { topics: [/^nothing-/] })

    expect(rejections).toEqual([])
    expect(group.topics).toEqual([])
    expect(group.subscription.hasPatterns()).toBe(true)
  })

  it('stores the policy for the regex forms too', async () => {
    const regex = await capturedOptions(
      { groupProtocol: 'consumer' },
      { regex: 'events-.*', offsetReset: 'by_duration:PT1M' }
    )
    expect(regex.group.regexSubscription).toEqual({
      fromBeginning: false,
      resetPolicy: { kind: 'by_duration', durationMs: 60000 },
    })
    const pattern = await capturedOptions({}, { topics: [/^events-/], offsetReset: 'earliest' })
    expect(pattern.group.topicConfigurations['events-a']).toEqual({
      fromBeginning: true,
      resetPolicy: { kind: 'earliest' },
    })
  })
})
