const Runner = require('../runner')
const Batch = require('../batch')
const OffsetManager = require('../offsetManager')
const InstrumentationEventEmitter = require('../../instrumentation/emitter')
const { createLogger } = require('../../loggers')

/**
 * Real batch/Runner/OffsetManager path over a counted coordinator method, with no sockets.
 * @param {object} [options] cadence knobs and autoCommit
 * @returns {{runner:Runner,manager:OffsetManager,commit:Function,processed:Function,batch:Batch}}
 */
const setup = ({ interval = null, threshold = null, autoCommit = true } = {}) => {
  const commit = jest.fn(async () => ({}))
  const cache = { t: { 0: '0' } }
  const coordinator = { isConnected: () => true, offsetCommit: commit }
  const logger = createLogger({
    level: 0,
    logCreator: require('../../loggers/console'),
  })
  const manager = new OffsetManager({
    cluster: { committedOffsets: () => cache, topicId: () => undefined },
    coordinator,
    memberAssignment: { t: [0] },
    autoCommit,
    autoCommitInterval: interval,
    autoCommitThreshold: threshold,
    topicConfigurations: { t: {} },
    instrumentationEmitter: new InstrumentationEventEmitter(),
    groupId: 'g',
    generationId: 1,
    memberId: 'm',
    logger,
  })
  manager.committedOffsets().t[0] = '0'
  const group = {
    groupId: 'g',
    memberId: 'm',
    kind: 'classic',
    offsetManager: manager,
    attach: () => {},
    getNodeIds: () => [],
    isAssigned: () => true,
    hasSeekOffset: () => false,
    resolveOffset: (offset) => manager.resolveOffset(offset),
    commitOffsets: (...args) => manager.commitOffsets(...args),
    commitOffsetsIfNecessary: (...args) => manager.commitOffsetsIfNecessary(...args),
    heartbeat: async () => {},
    heartbeatInterval: () => 3000,
    rebalanceProtocol: () => 'eager',
  }
  const processed = jest.fn(async () => {})
  const runner = new Runner({
    logger,
    consumerGroup: group,
    instrumentationEmitter: new InstrumentationEventEmitter(),
    concurrency: 1,
    eachBatch: processed,
    autoCommit,
    heartbeatInterval: 3000,
    maxPollInterval: 300000,
    onCrash: (error) => {
      throw error
    },
  })
  runner.running = true
  const batch = new Batch('t', '0', {
    partition: 0,
    highWatermark: '1',
    messages: [{ offset: '0', value: Buffer.from('v') }],
  })
  return { runner, manager, commit, processed, batch }
}

describe('batch-end automatic commit opportunities', () => {
  let h
  beforeEach(() => jest.useFakeTimers())
  afterEach(() => {
    if (h) {
      h.runner.running = false
      h.runner.heartbeatManager.shutdown()
      h.runner.pollTimer.stop()
    }
    expect(jest.getTimerCount()).toBe(0)
    jest.useRealTimers()
    h = null
  })
  test.each([
    ['interval', 60000, null],
    ['threshold', null, 100],
    ['both', 60000, 100],
  ])('batch completion preserves a configured %s', async (_name, interval, threshold) => {
    h = setup({ interval, threshold })
    await h.runner.handleBatch(h.batch)
    expect(h.processed).toHaveBeenCalledTimes(1)
    expect(h.manager.resolvedOffsetAt('t', 0)).toBe('1')
    expect(h.commit).toHaveBeenCalledTimes(0)
  })
  test('no knobs retains one batch-end commit', async () => {
    h = setup()
    await h.runner.handleBatch(h.batch)
    expect(h.commit).toHaveBeenCalledTimes(1)
    expect(h.commit.mock.calls[0][0].topics[0].partitions[0].offset).toBe('1')
  })
  test('threshold alone can satisfy both configured conditions', async () => {
    h = setup({ interval: 60000, threshold: 1 })
    await h.runner.handleBatch(h.batch)
    expect(h.commit).toHaveBeenCalledTimes(1)
  })
  test('interval alone can satisfy both configured conditions', async () => {
    h = setup({ interval: 1000, threshold: 100 })
    await jest.advanceTimersByTimeAsync(1000)
    await h.runner.handleBatch(h.batch)
    expect(h.commit).toHaveBeenCalledTimes(1)
  })
  test('idle time alone does not schedule a commit but a later check does', async () => {
    h = setup({ interval: 1000 })
    await h.runner.handleBatch(h.batch)
    expect(h.commit).toHaveBeenCalledTimes(0)
    await jest.advanceTimersByTimeAsync(5000)
    expect(h.commit).toHaveBeenCalledTimes(0)
    await h.runner.autoCommitOffsetsIfNecessary(null)
    expect(h.commit).toHaveBeenCalledTimes(1)
  })
  test.each([
    ['an interval after an idle gap leaves the tail of a burst uncommitted', 1000, '1'],
    ['no knobs commits every batch end, so the tail settles', null, '2'],
  ])('%s', async (_name, interval, settled) => {
    h = setup({ interval })
    await jest.advanceTimersByTimeAsync(1000)
    const tail = new Batch('t', '0', {
      partition: 0,
      highWatermark: '2',
      messages: [{ offset: '1', value: Buffer.from('v') }],
    })
    await h.runner.handleBatch(h.batch)
    await h.runner.handleBatch(tail)
    await jest.advanceTimersByTimeAsync(60000)
    expect(h.manager.resolvedOffsetAt('t', 0)).toBe('2')
    const last = h.commit.mock.calls.at(-1)[0].topics[0].partitions[0].offset
    expect(last).toBe(settled)
  })
  test('disabled automatic commit suppresses both paths', async () => {
    h = setup({ threshold: 1, autoCommit: false })
    await h.runner.handleBatch(h.batch)
    await h.runner.autoCommitOffsets(null)
    expect(h.commit).not.toHaveBeenCalled()
  })
  test('the force helper remains available before release', async () => {
    h = setup({ interval: 60000 })
    await h.runner.handleBatch(h.batch)
    expect(h.commit).not.toHaveBeenCalled()
    await h.runner.autoCommitOffsets(null)
    expect(h.commit).toHaveBeenCalledTimes(1)
  })
  test('classic final progress bypasses an unelapsed cadence once', async () => {
    const { ConsumerOperation } = require('../groupProtocol/interface')
    h = setup({ interval: 60000 })
    await h.runner.handleBatch(h.batch)
    expect(h.commit).not.toHaveBeenCalled()
    const operation = new ConsumerOperation()
    try {
      await h.runner.commitFinalOffsets(operation)
      await h.runner.commitFinalOffsets(operation)
    } finally {
      operation.finish()
    }
    expect(h.commit).toHaveBeenCalledTimes(1)
    expect(h.commit.mock.calls[0][0].topics[0].partitions[0].offset).toBe('1')
  })
  test.each(['disabled', 'lost', 'no-group', 'manual', 'consumer', 'zero', 'empty'])(
    'final attempt skips %s eligibility',
    async (condition) => {
      const { ConsumerOperation } = require('../groupProtocol/interface')
      h = setup({ interval: 60000, autoCommit: condition !== 'disabled' })
      if (condition !== 'empty') await h.runner.handleBatch(h.batch)
      const group = h.runner.consumerGroup
      if (condition === 'lost')
        group.ownership = { token: () => 1, refusal: () => new Error('lost') }
      if (condition === 'no-group') group.groupId = null
      if (condition === 'manual' || condition === 'consumer') group.kind = condition
      const operation = new ConsumerOperation({
        deadlineAt: condition === 'zero' ? performance.now() : Infinity,
      })
      try {
        await h.runner.commitFinalOffsets(operation)
      } finally {
        operation.finish()
      }
      expect(h.commit).not.toHaveBeenCalled()
    }
  )
})

describe('per-message automatic commit check', () => {
  let h
  beforeEach(() => jest.useFakeTimers())
  afterEach(() => {
    if (h) {
      h.runner.running = false
      h.runner.heartbeatManager.shutdown()
      h.runner.pollTimer.stop()
    }
    expect(jest.getTimerCount()).toBe(0)
    jest.useRealTimers()
    h = null
  })
  test.each([
    ['an unelapsed interval', { interval: 60000 }],
    ['no interval and no threshold', {}],
  ])('%s returns before the ownership check', (_name, knobs) => {
    h = setup(knobs)
    const refusal = jest.spyOn(h.runner, 'commitRefusal')
    expect(h.runner.autoCommitOffsetsIfNecessary(null)).toBeUndefined()
    expect(refusal).not.toHaveBeenCalled()
    expect(h.commit).not.toHaveBeenCalled()
  })
  test('an elapsed interval checks ownership, then commits', async () => {
    h = setup({ interval: 1000 })
    await h.runner.handleBatch(h.batch)
    expect(h.commit).not.toHaveBeenCalled()
    await jest.advanceTimersByTimeAsync(1000)
    const refusal = jest.spyOn(h.runner, 'commitRefusal')
    await h.runner.autoCommitOffsetsIfNecessary(null)
    expect(refusal).toHaveBeenCalled()
    expect(h.commit).toHaveBeenCalledTimes(1)
    expect(h.commit.mock.calls[0][0].topics[0].partitions[0].offset).toBe('1')
  })
  test('an elapsed interval refused by ownership sends nothing', async () => {
    h = setup({ interval: 1000 })
    await h.runner.handleBatch(h.batch)
    await jest.advanceTimersByTimeAsync(1000)
    const lastCommit = h.manager.lastCommit
    h.runner.consumerGroup.ownership = { token: () => 1, refusal: () => new Error('lost') }
    expect(h.runner.autoCommitOffsetsIfNecessary(null)).toBeUndefined()
    expect(h.commit).not.toHaveBeenCalled()
    expect(h.manager.lastCommit).toBe(lastCommit)
  })
  test('a configured threshold keeps the ownership check first', async () => {
    h = setup({ threshold: 100 })
    const refusal = jest.spyOn(h.runner, 'commitRefusal')
    await h.runner.autoCommitOffsetsIfNecessary(null)
    expect(refusal).toHaveBeenCalledTimes(1)
    expect(h.commit).not.toHaveBeenCalled()
  })
  test('disabled automatic commit attempts nothing once the interval elapsed', async () => {
    h = setup({ interval: 1000, autoCommit: false })
    await h.runner.handleBatch(h.batch)
    await jest.advanceTimersByTimeAsync(1000)
    expect(h.runner.autoCommitOffsetsIfNecessary(null)).toBeUndefined()
    expect(h.commit).not.toHaveBeenCalled()
  })
})
