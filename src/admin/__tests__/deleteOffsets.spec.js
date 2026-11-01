const createAdmin = require('..')
const { KafkaJSInvalidConfigurationError } = require('../../errors')
const logger = { namespace: () => logger, debug() {}, info() {}, warn() {}, error() {} }
const topics = [{ topic: 't', partitions: [0, 1] }]
const retry = { retries: 1, initialRetryTime: 1, factor: 0 }
describe('delete group offsets', () => {
  it('keeps mixed partition errors as caller-ordered data', async () => {
    const broker = {
      offsetDelete: jest.fn(async () => ({
        errorCode: 0,
        topics: [
          {
            topic: 't',
            partitions: [
              { partition: 1, errorCode: 3 },
              { partition: 0, errorCode: 0 },
            ],
          },
        ],
      })),
    }
    const cluster = { findGroupCoordinator: jest.fn(async () => broker) }
    const result = await createAdmin({ cluster, logger, retry }).deleteOffsets({
      groupId: 'g',
      topics,
    })
    expect(
      result.topics[0].partitions.map((p) => [p.partition, p.errorCode, p.error?.code ?? null])
    ).toEqual([
      [0, 0, null],
      [1, 3, 3],
    ])
    expect(broker.offsetDelete).toHaveBeenCalledTimes(1)
  })
  it.each([30, 69, 24, 68])('throws group refusal %i', async (errorCode) => {
    const broker = { offsetDelete: async () => ({ errorCode, topics: [] }) }
    const cluster = { findGroupCoordinator: async () => broker }
    await expect(
      createAdmin({ cluster, logger, retry }).deleteOffsets({ groupId: 'g', topics })
    ).rejects.toMatchObject({ name: 'KafkaJSProtocolError', code: errorCode })
  })
  it('relocates only the unresolved partition and preserves completed data', async () => {
    const first = {
      offsetDelete: jest.fn(async () => ({
        errorCode: 0,
        topics: [
          {
            topic: 't',
            partitions: [
              { partition: 0, errorCode: 0 },
              { partition: 1, errorCode: 16 },
            ],
          },
        ],
      })),
    }
    const second = {
      offsetDelete: jest.fn(async () => ({
        errorCode: 0,
        topics: [{ topic: 't', partitions: [{ partition: 1, errorCode: 0 }] }],
      })),
    }
    const cluster = {
      findGroupCoordinator: jest.fn().mockResolvedValueOnce(first).mockResolvedValueOnce(second),
    }
    const result = await createAdmin({ cluster, logger, retry }).deleteOffsets({
      groupId: 'g',
      topics,
    })
    expect(result.topics[0].partitions.map((p) => p.errorCode)).toEqual([0, 0])
    expect(second.offsetDelete).toHaveBeenCalledWith({
      groupId: 'g',
      topics: [{ topic: 't', partitions: [{ partition: 1 }] }],
    })
    expect(cluster.findGroupCoordinator).toHaveBeenCalledTimes(2)
  })
  it.each(['', null, 'bad\0name'])('refuses invalid group %p before discovery', async (groupId) => {
    const cluster = { findGroupCoordinator: jest.fn() }
    await expect(
      createAdmin({ cluster, logger, retry }).deleteOffsets({ groupId, topics })
    ).rejects.toBeInstanceOf(KafkaJSInvalidConfigurationError)
    expect(cluster.findGroupCoordinator).not.toHaveBeenCalled()
  })
})

describe('delete offsets wire seam', () => {
  it('sends classic OffsetDelete only to the located coordinator', async () => {
    const { Kafka } = require('../../..')
    const { createScriptedCluster } = require('../../../testHelpers/scriptedCluster')
    const scripted = createScriptedCluster({
      nodes: [{ id: 0, host: 'node0', port: 9092 }],
      profile: 'offsets',
    })
    scripted.answer('FindCoordinator', ({ coordinatorKeys }) => ({
      coordinators: coordinatorKeys.map((key) => ({
        key,
        nodeId: 0,
        host: 'node0',
        port: 9092,
        errorCode: 0,
        errorMessage: null,
      })),
    }))
    scripted.answer('OffsetDelete', () => ({
      errorCode: 0,
      topics: [
        {
          topic: 't',
          partitions: [
            { partition: 0, errorCode: 0 },
            { partition: 1, errorCode: 3 },
          ],
        },
      ],
    }))
    const admin = new Kafka(scripted.kafkaOptions({ retry })).admin()
    try {
      await admin.connect()
      const result = await admin.deleteOffsets({ groupId: 'g', topics })
      expect(result.topics[0].partitions.map((p) => p.errorCode)).toEqual([0, 3])
      const sent = scripted.requests().filter((r) => r.api === 'OffsetDelete')
      expect(sent).toHaveLength(1)
      expect(sent[0]).toMatchObject({
        node: 0,
        version: 0,
        fields: {
          groupId: 'g',
          topics: [{ topic: 't', partitions: [{ partition: 0 }, { partition: 1 }] }],
        },
      })
    } finally {
      await admin.disconnect()
      scripted.assertClean()
    }
  })
})

describe('offset deletion capabilities', () => {
  it('names the client and absent broker ranges before sending', async () => {
    const Broker = require('../../broker')
    const target = { versions: {}, lookupRequest: jest.fn() }
    await expect(
      Broker.prototype.offsetDelete.call(target, { groupId: 'g', topics: [] })
    ).rejects.toMatchObject({
      name: 'KafkaJSServerDoesNotSupportApiKey',
      message: expect.stringMatching(/client implements v0-v0; broker supports absent/),
    })
    expect(target.lookupRequest).not.toHaveBeenCalled()
  })
})
describe('offset deletion validation and exhaustion', () => {
  it.each([
    null,
    [],
    {},
    { groupId: '', topics },
    { groupId: 'g', topics: [] },
    { groupId: 'g', topics: [{ topic: 't', partitions: [] }] },
    { groupId: 'g', topics: [topics[0], topics[0]] },
    ...[-1, 2147483648, 1.5, '0', null].map((partition) => ({
      groupId: 'g',
      topics: [{ topic: 't', partitions: [partition] }],
    })),
    { groupId: 'g', topics: [{ topic: 't', partitions: [0, 0] }] },
  ])('refuses invalid selection %p without discovery', async (options) => {
    const cluster = { findGroupCoordinator: jest.fn() }
    await expect(
      createAdmin({ cluster, logger, retry }).deleteOffsets(options)
    ).rejects.toBeInstanceOf(KafkaJSInvalidConfigurationError)
    expect(cluster.findGroupCoordinator).not.toHaveBeenCalled()
  })
  it('accepts partition bounds and retains loading cells when retries are exhausted', async () => {
    const coordinator = {
      offsetDelete: jest.fn(async () => ({
        errorCode: 0,
        topics: [
          {
            topic: 't',
            partitions: [
              { partition: 0, errorCode: 0 },
              { partition: 2147483647, errorCode: 14 },
            ],
          },
        ],
      })),
    }
    const result = await createAdmin({
      logger,
      retry: { retries: 0 },
      cluster: { findGroupCoordinator: async () => coordinator },
    }).deleteOffsets({ groupId: 'g', topics: [{ topic: 't', partitions: [0, 2147483647] }] })
    expect(result.topics[0].partitions.map((p) => p.errorCode)).toEqual([0, 14])
    expect(coordinator.offsetDelete).toHaveBeenCalledTimes(1)
  })
})

describe('offset invocation ownership', () => {
  beforeEach(() => jest.useFakeTimers())
  afterEach(() => {
    expect(jest.getTimerCount()).toBe(0)
    jest.useRealTimers()
  })
  it.each(
    ['deleteOffsets', 'fetchOffsetsForGroups'].flatMap((method) =>
      ['lookup', 'send', 'retry'].flatMap((stage) =>
        [false, true].map((withheld) => ({ method, stage, withheld }))
      )
    )
  )(
    'expires $method during $stage with timer withheld=$withheld',
    async ({ method, stage, withheld }) => {
      let rejectHeld
      const held = new Promise((resolve, reject) => {
        rejectHeld = reject
      })
      held.catch(() => {})
      const success =
        method === 'deleteOffsets'
          ? { errorCode: 0, topics: [{ topic: 't', partitions: [{ partition: 0, errorCode: 0 }] }] }
          : {
              groups: [
                {
                  groupId: 'g',
                  errorCode: 0,
                  topics: [
                    { topic: 't', partitions: [{ partition: 0, offset: '10', errorCode: 0 }] },
                  ],
                },
              ],
            }
      const loading =
        method === 'deleteOffsets'
          ? { errorCode: 14, topics: [] }
          : { groups: [{ groupId: 'g', errorCode: 14, topics: [] }] }
      const send = jest.fn(async () =>
        stage === 'send' ? held : stage === 'retry' ? loading : success
      )
      const target = {
        offsetDelete: send,
        offsetFetch: send,
        lookupRequestWithMax: () => () => ({ request: { apiVersion: 9 } }),
      }
      const cluster = {
        findGroupCoordinator: jest.fn(async () => (stage === 'lookup' ? held : target)),
      }
      const admin = createAdmin({
        logger,
        cluster,
        retry: { retries: 1, initialRetryTime: 100, factor: 0 },
      })
      const options =
        method === 'deleteOffsets'
          ? { groupId: 'g', topics: [{ topic: 't', partitions: [0] }] }
          : { groups: [{ groupId: 'g', topics: [{ topic: 't', partitions: [0] }] }] }
      const result = admin[method]({ ...options, apiTimeout: 10 }).then(
        (value) => ({ value }),
        (error) => ({ error })
      )
      for (let i = 0; i < 60; i++) await Promise.resolve()
      const calls = send.mock.calls.length
      if (withheld) {
        jest.setSystemTime(Date.now() + 11)
        // Fake performance time advances without delivering timers through its explicit clock.
        const now = jest.spyOn(performance, 'now').mockReturnValue(performance.now() + 11)
        rejectHeld(new Error('late transport refusal'))
        await jest.advanceTimersByTimeAsync(0)
        if (stage === 'retry') await jest.advanceTimersByTimeAsync(10)
        now.mockRestore()
      } else {
        await jest.advanceTimersByTimeAsync(10)
        rejectHeld(new Error('late transport refusal'))
      }
      const outcome = await result
      expect(outcome.error).toMatchObject({
        name: 'KafkaJSTimeout',
        message: expect.stringContaining(method),
      })
      expect(outcome.error.message).toContain('outcome is unknown')
      for (let i = 0; i < 30; i++) await Promise.resolve()
      expect(send).toHaveBeenCalledTimes(calls)
      expect(cluster.findGroupCoordinator).toHaveBeenCalledTimes(1)
      const peer = createAdmin({
        logger,
        cluster: {
          findGroupCoordinator: async () => ({
            ...target,
            offsetDelete: async () => success,
            offsetFetch: async () => success,
          }),
        },
        retry,
      })
      await expect(peer[method](options)).resolves.toBeDefined()
    }
  )
})

describe('offset queue deadline isolation', () => {
  beforeEach(() => jest.useFakeTimers())
  afterEach(() => {
    jest.restoreAllMocks()
    jest.useRealTimers()
  })
  it.each(
    ['deleteOffsets', 'fetchOffsetsForGroups'].flatMap((method) =>
      [false, true].map((withheld) => ({ method, withheld }))
    )
  )(
    'retires only $method behind a throttle with withheld=$withheld',
    async ({ method, withheld }) => {
      const Queue = require('../../network/requestQueue')
      const { currentDeadline } = require('../deadline')
      const queue = new Queue({
        logger,
        requestTimeout: 500,
        enforceRequestTimeout: true,
        isConnected: () => true,
      })
      queue.scheduleRequestTimeoutCheck()
      queue.maybeThrottle(100)
      const sent = jest.fn()
      const peerSent = jest.fn()
      const peer = new Promise((resolve, reject) =>
        queue.push({
          entry: { correlationId: 2, apiName: 'Metadata', resolve, reject },
          expectResponse: true,
          sendRequest: peerSent,
        })
      )
      const now = jest.spyOn(performance, 'now').mockReturnValue(0)
      let scope
      const send = () =>
        new Promise((resolve, reject) => {
          scope = currentDeadline()
          queue.push({
            entry: { correlationId: 1, apiName: method, resolve, reject },
            expectResponse: true,
            sendRequest: sent,
            guard: scope.guard,
          })
        })
      const target = {
        offsetDelete: send,
        offsetFetch: send,
        lookupRequestWithMax: () => () => ({ request: { apiVersion: 9 } }),
      }
      const admin = createAdmin({
        logger,
        cluster: { findGroupCoordinator: async () => target },
        retry,
      })
      const options =
        method === 'deleteOffsets'
          ? { groupId: 'g', topics: [{ topic: 't', partitions: [0] }] }
          : { groups: [{ groupId: 'g', topics: [{ topic: 't', partitions: [0] }] }] }
      const result = admin[method]({ ...options, apiTimeout: 5 }).then(
        (value) => ({ value }),
        (error) => ({ error })
      )
      try {
        for (let i = 0; i < 60; i++) await Promise.resolve()
        expect(queue.pending).toHaveLength(2)
        if (withheld) {
          now.mockReturnValue(6)
          expect(scope.refusal()).toMatchObject({ name: 'KafkaJSTimeout' })
        } else await jest.advanceTimersByTimeAsync(5)
        const outcome = await result
        expect(outcome.error).toBe(scope.timeout())
        expect(sent).not.toHaveBeenCalled()
        expect(queue.pending.map((entry) => entry.correlationId)).toEqual([2])
        expect(scope.cleanups.size).toBe(0)
        await jest.advanceTimersByTimeAsync(100)
        expect(peerSent).toHaveBeenCalledTimes(1)
        queue.fulfillRequest({ correlationId: 2, payload: Buffer.alloc(0), size: 0 })
        await peer
        expect(queue.pending).toHaveLength(0)
        expect(queue.inflight.size).toBe(0)
      } finally {
        queue.settleAll(new Error('fixture complete'))
        queue.destroy()
        expect(jest.getTimerCount()).toBe(0)
      }
    }
  )
})
