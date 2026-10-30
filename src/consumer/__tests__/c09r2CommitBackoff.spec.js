const Runner = require('../runner')
const OffsetManager = require('../offsetManager')
const InstrumentationEventEmitter = require('../../instrumentation/emitter')
const { ConsumerOperation } = require('../groupProtocol/interface')
const { KafkaJSProtocolError, KafkaJSDeliveryTimeout } = require('../../errors')
const { createErrorFromCode } = require('../../protocol/error')
const { newLogger } = require('testHelpers')

/**
 * A real Runner and OffsetManager over a coordinator that fails its first commit retriably.
 * @returns {{runner: Runner, commit: jest.Mock, offsets: object}} the rig
 */
const setup = () => {
  const commit = jest
    .fn()
    .mockRejectedValueOnce(new KafkaJSProtocolError(createErrorFromCode(14)))
    .mockResolvedValue({})
  const cache = { t: { 0: '0' } }
  const manager = new OffsetManager({
    cluster: { committedOffsets: () => cache, topicId: () => undefined },
    coordinator: { isConnected: () => true, offsetCommit: commit },
    memberAssignment: { t: [0] },
    autoCommit: true,
    topicConfigurations: { t: {} },
    instrumentationEmitter: new InstrumentationEventEmitter(),
    groupId: 'g',
    generationId: 1,
    memberId: 'm',
    logger: newLogger(),
  })
  const group = {
    groupId: 'g',
    memberId: 'm',
    kind: 'classic',
    offsetManager: manager,
    attach: () => {},
    getNodeIds: () => [],
    commitOffsets: (...args) => manager.commitOffsets(...args),
    heartbeatInterval: () => 3000,
  }
  const runner = new Runner({
    logger: newLogger(),
    consumerGroup: group,
    instrumentationEmitter: new InstrumentationEventEmitter(),
    concurrency: 1,
    eachBatch: async () => {},
    heartbeatInterval: 3000,
    maxPollInterval: 300000,
    retry: { retries: 5, initialRetryTime: 1000, factor: 0 },
    onCrash: () => {},
  })
  runner.running = true
  const offsets = { topics: [{ topic: 't', partitions: [{ partition: 0, offset: '1' }] }] }
  return { runner, commit, offsets }
}

describe('a natural commit call retired during its retry backoff', () => {
  beforeEach(() => jest.useFakeTimers())
  afterEach(() => {
    jest.useRealTimers()
  })

  it('removes the backoff timer and rejects with the retirement reason', async () => {
    const { runner, commit, offsets } = setup()
    const operation = new ConsumerOperation()
    const committing = runner.commitOffsets(offsets, null, { operation }).catch((error) => error)
    await jest.advanceTimersByTimeAsync(0)
    expect(commit).toHaveBeenCalledTimes(1)
    expect(jest.getTimerCount()).toBe(1)
    const reason = new KafkaJSDeliveryTimeout('closed')
    operation.retire(reason)
    expect(jest.getTimerCount()).toBe(0)
    expect(await committing).toBe(reason)
    await jest.advanceTimersByTimeAsync(5000)
    expect(commit).toHaveBeenCalledTimes(1)
  })

  it('an unretired call retries after the backoff and succeeds (control)', async () => {
    const { runner, commit, offsets } = setup()
    const operation = new ConsumerOperation()
    const committing = runner.commitOffsets(offsets, null, { operation })
    await jest.advanceTimersByTimeAsync(1000)
    await committing
    expect(commit).toHaveBeenCalledTimes(2)
    expect(jest.getTimerCount()).toBe(0)
    operation.finish()
  })

  it('a call without any operation keeps the ordinary retrier', async () => {
    const { runner, commit, offsets } = setup()
    const committing = runner.commitOffsets(offsets, null)
    await jest.advanceTimersByTimeAsync(1000)
    await committing
    expect(commit).toHaveBeenCalledTimes(2)
  })
})
