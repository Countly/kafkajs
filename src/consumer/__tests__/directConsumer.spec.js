const Kafka = require('../../index')
const ManualGroupProtocol = require('../groupProtocol/manual')
const Runner = require('../runner')
const InstrumentationEventEmitter = require('../../instrumentation/emitter')
const { newLogger, waitFor } = require('testHelpers')
const {
  KafkaJSConnectionError,
  KafkaJSNonRetriableError,
  KafkaJSNumberOfRetriesExceeded,
} = require('../../errors')

/** @returns {object} a local protocol with a controllable captured run */
const create = (groupId) => {
  let current = true
  const cluster = { findGroupCoordinator: jest.fn() }
  const protocol = new ManualGroupProtocol({
    cluster,
    groupId,
    topicPartitions: [{ topic: 'records', partitions: [1, 0] }],
    runIdentity: { isCurrent: () => current },
  })
  return {
    protocol,
    cluster,
    retire: () => {
      current = false
    },
  }
}

describe('Manual consumer construction and local protocol', () => {
  test('allows omitted and undefined group IDs at construction', () => {
    const kafka = new Kafka({ brokers: ['unused:9092'] })
    expect(() => kafka.consumer({})).not.toThrow()
    expect(() => kafka.consumer({ groupId: undefined })).not.toThrow()
  })
  test.each([null, '', 1, false, {}, []])(
    'refuses an invalid group ID %p at construction',
    (groupId) => {
      const kafka = new Kafka({ brokers: ['unused:9092'] })
      expect(() => kafka.consumer({ groupId })).toThrow(
        'Consumer groupId must be a non-empty string.'
      )
    }
  )
  test('does not start membership traffic or timers across its entire lifecycle', async () => {
    jest.useFakeTimers()
    try {
      const { protocol, cluster } = create('offset-group')
      const timers = jest.getTimerCount()
      expect(protocol.kind).toBe('manual')
      await protocol.connect()
      protocol.attach({})
      expect(await protocol.rebalance()).toEqual({
        assignment: { records: [0, 1] },
        userData: null,
      })
      protocol.setOwned([{ topic: 'records', partitions: [0, 1] }])
      await protocol.heartbeat({ interval: 0 })
      protocol.onMembershipError(new Error('refused'))
      expect(protocol.needsReconciliation()).toBe(false)
      expect(protocol.generationId).toBe(-1)
      expect(protocol.memberId).toBe('')
      expect(protocol.groupInstanceId).toBeNull()
      expect(protocol.joinCount).toBe(0)
      await protocol.leave({ leaveGroup: true })
      expect(protocol.owned()).toEqual([])
      expect(cluster.findGroupCoordinator).not.toHaveBeenCalled()
      expect(jest.getTimerCount()).toBe(timers)
      await expect(protocol.rebalance()).rejects.toMatchObject({ type: 'CONSUMER_STOPPING' })
    } finally {
      jest.useRealTimers()
    }
  })
  test('keeps no-group offset discovery entirely local', async () => {
    const { protocol, cluster } = create()
    await expect(protocol.liveCoordinator()).rejects.toThrow(
      'Consumer groupId must be a non-empty string.'
    )
    expect(cluster.findGroupCoordinator).not.toHaveBeenCalled()
  })
  test('installs the discovered coordinator and reuses it while connected', async () => {
    const { protocol, cluster } = create('offset-group')
    const coordinator = { isConnected: () => true }
    cluster.findGroupCoordinator.mockResolvedValue(coordinator)
    expect(await protocol.liveCoordinator()).toBe(coordinator)
    expect(await protocol.liveCoordinator()).toBe(coordinator)
    expect(protocol.coordinator).toBe(coordinator)
    expect(cluster.findGroupCoordinator).toHaveBeenCalledTimes(1)
    expect(cluster.findGroupCoordinator).toHaveBeenCalledWith({ groupId: 'offset-group' })
  })
  test('answers the membership queries as a local, leaderless eager protocol', () => {
    const { protocol } = create('offset-group')
    expect(protocol.forgetMemberId(3)).toBe(false)
    expect(protocol.isLeader()).toBe(false)
    expect(protocol.rebalanceProtocol()).toBe('eager')
    expect(protocol.heartbeatInterval()).toBe(Infinity)
    expect(protocol.warnIfStaticMembershipInactive()).toBeUndefined()
    expect(protocol.notifyAssignment({})).toBeUndefined()
    expect(protocol.clearAssignerState()).toBeUndefined()
  })
  test.each([
    ['leaveForPollExpiry', (protocol) => protocol.leaveForPollExpiry()],
    ['resetGeneration', (protocol) => protocol.resetGeneration()],
  ])('%s retires the run and drops ownership', async (_, retireWith) => {
    const { protocol } = create('offset-group')
    protocol.setOwned([{ topic: 'records', partitions: [0] }])
    await retireWith(protocol)
    expect(protocol.owned()).toEqual([])
    await expect(protocol.connect()).rejects.toMatchObject({ type: 'CONSUMER_STOPPING' })
    await expect(protocol.heartbeat()).rejects.toMatchObject({ type: 'CONSUMER_STOPPING' })
  })
  test('never installs a held coordinator after run retirement', async () => {
    const { protocol, cluster, retire } = create('offset-group')
    let answer
    cluster.findGroupCoordinator.mockReturnValue(
      new Promise((resolve) => {
        answer = resolve
      })
    )
    const pending = protocol.liveCoordinator()
    retire()
    answer({ isConnected: () => true })
    await expect(pending).rejects.toMatchObject({ type: 'CONSUMER_STOPPING' })
    expect(protocol.coordinator).toBeNull()
  })
  test('sorts and copies prototype-looking topics and partition arrays', async () => {
    const input = [
      { topic: '__proto__', partitions: [10, 2] },
      { topic: 'constructor', partitions: [0] },
    ]
    const protocol = new ManualGroupProtocol({
      cluster: {},
      topicPartitions: input,
      runIdentity: { isCurrent: () => true },
    })
    input[0].partitions.push(4)
    const result = await protocol.rebalance()
    expect(Object.getPrototypeOf(result.assignment)).toBeNull()
    expect(Object.getOwnPropertyDescriptor(result.assignment, '__proto__').value).toEqual([2, 10])
    protocol.setOwned([{ topic: '__proto__', partitions: [10, 2] }])
    const copy = protocol.owned()
    copy[0].partitions.push(9)
    expect(protocol.owned()).toEqual([{ topic: '__proto__', partitions: [2, 10] }])
    expect(protocol.diffAssignment([{ topic: '__proto__', partitions: [2, 11] }])).toEqual({
      added: [{ topic: '__proto__', partitions: [11] }],
      revoked: [{ topic: '__proto__', partitions: [10] }],
    })
    protocol.narrowOwned([{ topic: '__proto__', partitions: [2, 11] }])
    expect(protocol.owned()).toEqual([{ topic: '__proto__', partitions: [2] }])
  })
  test.each([
    ['undefined', undefined],
    ['null', null],
    ['empty object', {}],
    ['empty array', []],
    ['null entry', [null]],
    ['empty topic', [{ topic: '', partitions: [0] }]],
    ['no partitions', [{ topic: 'a', partitions: [] }]],
    ['missing partitions', [{ topic: 'a' }]],
    ['missing topic', [{ partitions: [0] }]],
    ['NaN partition', [{ topic: 'a', partitions: [NaN] }]],
    ['Infinity partition', [{ topic: 'a', partitions: [Infinity] }]],
    ['negative partition', [{ topic: 'a', partitions: [-1] }]],
    ['fractional partition', [{ topic: 'a', partitions: [0.5] }]],
    ['partition above int32', [{ topic: 'a', partitions: [2147483648] }]],
    ['string partition', [{ topic: 'a', partitions: ['0'] }]],
    ['repeated partition', [{ topic: 'a', partitions: [0, 0] }]],
    [
      'repeated entry',
      [
        { topic: 'a', partitions: [0] },
        { topic: 'a', partitions: [0] },
      ],
    ],
  ])('rejects malformed or duplicate assignment: %s', (_, input) => {
    expect(() => ManualGroupProtocol.validateAssignment(input)).toThrow(KafkaJSNonRetriableError)
  })
  test('accepts integer partition boundaries and repeated topics with distinct pairs', () => {
    expect(() =>
      ManualGroupProtocol.validateAssignment([
        { topic: 'a', partitions: [0] },
        { topic: 'a', partitions: [2147483647] },
      ])
    ).not.toThrow()
  })
})

describe('Manual run outage policy', () => {
  /**
   * Builds a runner over a stand-in group of the given kind; no broker or membership is involved.
   * @param {'classic'|'consumer'|'manual'} kind the assignment protocol
   * @returns {{ runner: Runner, onCrash: jest.Mock }} the runner and its crash handler
   */
  const createRunner = (kind) => {
    const onCrash = jest.fn()
    const runner = new Runner({
      consumerGroup: { kind, attach: jest.fn(), getNodeIds: () => [] },
      onCrash,
      instrumentationEmitter: new InstrumentationEventEmitter(),
      logger: newLogger(),
      eachBatch: jest.fn(),
      concurrency: 1,
      heartbeatInterval: 3000,
      maxPollInterval: 300000,
      retry: { retries: 0, initialRetryTime: 5 },
    })
    return { runner, onCrash }
  }

  test.each([
    ['classic', true],
    ['consumer', true],
    ['manual', false],
  ])('a %s run rides out its own request outages: %p', (kind, rides) => {
    expect(createRunner(kind).runner.ridesOutages()).toBe(rides)
  })

  test.each([
    ['classic', false],
    ['manual', true],
  ])('a lost fetch connection on a %s run reaches the crash handler: %p', async (kind, crashes) => {
    const { runner, onCrash } = createRunner(kind)
    const lost = runner.markOwnRequest(new KafkaJSConnectionError('connection lost'), 'fetch')
    let attempts = 0
    runner.running = true
    runner.fetchManager = {
      start: jest.fn(async () => {
        attempts += 1
        if (attempts === 1) throw lost
        runner.running = false
      }),
      stop: jest.fn(async () => {}),
    }
    runner.scheduleFetchManager()
    await waitFor(() => !runner.consuming)
    expect(onCrash).toHaveBeenCalledTimes(crashes ? 1 : 0)
    if (crashes) {
      expect(onCrash.mock.calls[0][0]).toBeInstanceOf(KafkaJSNumberOfRetriesExceeded)
      expect(onCrash.mock.calls[0][0].cause).toBe(lost)
    }
  })
})
