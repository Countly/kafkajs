const { Kafka } = require('../../../../index')
const Runner = require('../../runner')
const Cluster = require('../../../cluster')
const Connection = require('../../../network/connection')
const ConnectionPool = require('../../../network/connectionPool')
const IdleWait = require('../../idleWait')
const ManualGroupProtocol = require('../../groupProtocol/manual')
const { Schedule } = require('../../../../testHelpers/schedule')
const { createScriptedCluster, groupScript } = require('../../../../testHelpers/scriptedCluster')
const { TOPIC, prepareRecordBatches } = require('./fetchHarness')
const { KafkaJSCommitFailedError } = require('../../../errors')
const { pump } = require('./lifecycleCase')

const MEMBERSHIP_APIS = [
  'JoinGroup',
  'SyncGroup',
  'Heartbeat',
  'LeaveGroup',
  'ConsumerGroupHeartbeat',
]
const GROUP_APIS = ['FindCoordinator', 'OffsetFetch', 'OffsetCommit']

/**
 * Drives one manual-assignment lifecycle cell through the public factories and the real wire
 * stack. Spies hold boundaries that already exist; nothing here adds a production hook. The cell
 * passes only when the held operation is refused or harmless, no membership frame was ever
 * written, a consumer without a group wrote no coordinator or offset frame, and every ledger,
 * timer and socket is back at its baseline after the final disconnect.
 * @param {{ event: string, boundary: string, protocol: 'manual', membership: 'grouped'|'no-group' }} cell
 * @returns {Promise<void>}
 * @throws {Error} when a boundary never arrives or a post-condition fails
 */
const runManualLifecycleCell = async (cell) => {
  const grouped = cell.membership === 'grouped'
  const schedule = new Schedule()
  const held = schedule.hold({ boundary: cell.boundary })
  const release = () => {
    if (held.settle) held.release()
  }
  const scripted = groupScript(
    createScriptedCluster({ nodes: [{ id: 0, host: 'node', port: 9092 }], schedule: undefined }),
    { batches: cell.boundary === 'idle-wait' ? 0 : 1 }
  )
  const errors = []
  const unhandled = (error) => errors.push(error)
  const baseline = {
    timers: jest.getTimerCount(),
    unhandled: process.listenerCount('unhandledRejection'),
  }
  process.on('unhandledRejection', unhandled)
  const runners = []
  const spies = []
  const cleanup = []
  const pending = []
  let consumer
  let enabled = false
  let used = false
  let operation
  const settled = (promise) => {
    const state = { done: false, error: null }
    state.promise = promise.then(
      (value) => {
        state.done = true
        return value
      },
      (error) => {
        state.done = true
        state.error = error
      }
    )
    pending.push(state)
    return state
  }
  const at = async () => {
    if (!enabled || used) return
    used = true
    await schedule.run({ boundary: cell.boundary }, () => undefined)
  }
  const wrap = (target, method, select = () => true) => {
    const original = target[method]
    spies.push(
      jest.spyOn(target, method).mockImplementation(async function (...args) {
        if (select(...args)) await at()
        return original.apply(this, args)
      })
    )
  }
  const hold = (selector) => {
    const gate = scripted.schedule.hold(selector)
    cleanup.push(() => {
      if (gate.settle) gate.release()
    })
    return gate
  }
  const capture = Runner.prototype.start
  spies.push(
    jest.spyOn(Runner.prototype, 'start').mockImplementation(function (...args) {
      runners.push(this)
      return capture.apply(this, args)
    })
  )
  const socketBaseline = () => scripted.sockets().filter((socket) => !socket.destroyed).length
  const beforeSockets = socketBaseline()
  const handler = async () => {
    if (cell.boundary === 'in-handler') await at()
  }
  const commitOnce = () =>
    settled(consumer.commitOffsets([{ topic: TOPIC, partition: 0, offset: '1' }]))
  let failed = false
  try {
    await prepareRecordBatches()
    consumer = new Kafka(
      scripted.kafkaOptions({
        maxInFlightRequests: 1,
        requestTimeout: 20000,
        retry: { retries: 1, initialRetryTime: 10, maxRetryTime: 10, factor: 0 },
      })
    ).consumer({
      ...(grouped ? { groupId: 'manual-lifecycle' } : {}),
      maxInFlightRequests: 1,
      maxWaitTimeInMs: 20,
    })
    if (cell.boundary === 'before-handler') wrap(Runner.prototype, 'handleBatch')
    if (cell.boundary === 'manual:apply') wrap(ManualGroupProtocol.prototype, 'rebalance')
    if (cell.boundary === 'resolution:list-offsets') hold({ api: 'ListOffsets' }).arrived().then(at)
    if (
      ['before-handler', 'in-handler', 'manual:apply', 'resolution:list-offsets'].includes(
        cell.boundary
      )
    )
      enabled = true

    await consumer.connect()
    await consumer.assign([{ topic: TOPIC, partitions: [0] }], { offsetReset: 'earliest' })
    const starting = settled(consumer.run({ autoCommit: false, eachBatch: handler }))
    await pump(() => runners.length > 0, 'runner capture')
    if (cell.boundary !== 'manual:apply' && cell.boundary !== 'resolution:list-offsets') {
      await pump(
        () => starting.done && runners[0].consumerGroup.offsetManager != null,
        'first acquisition'
      )
    }
    if (starting.error && cell.boundary !== 'manual:apply') throw starting.error
    const group = () => runners[runners.length - 1].consumerGroup

    if (
      !['before-handler', 'in-handler', 'manual:apply', 'resolution:list-offsets'].includes(
        cell.boundary
      )
    ) {
      enabled = true
      if (cell.boundary === 'idle-wait') {
        wrap(IdleWait.prototype, 'wait')
        consumer.pause([{ topic: TOPIC, partitions: [0] }])
      }
      if (cell.boundary === 'position:answer-held') {
        hold({ api: 'ListOffsets' }).arrived().then(at)
        consumer.seek({ topic: TOPIC, partition: 0, offset: '-2' })
        operation = settled(consumer.position({ topic: TOPIC, partition: 0 }))
      }
      if (cell.boundary === 'commit:coordinator-lookup') {
        await pump(() => scripted.requests().some(({ api }) => api === 'Fetch'), 'the first fetch')
        group().offsetManager.coordinator = { isConnected: () => false }
        wrap(Cluster.prototype, 'findGroupCoordinator')
      }
      if (cell.boundary === 'commit:pool-connect') wrap(ConnectionPool.prototype, 'getConnection')
      if (cell.boundary === 'commit:auth-encode') {
        const original = Connection.prototype.send
        spies.push(
          jest.spyOn(Connection.prototype, 'send').mockImplementation(function (protocol) {
            if (protocol.request.apiName === 'OffsetCommit') {
              const encode = protocol.request.encode
              protocol = {
                ...protocol,
                request: {
                  ...protocol.request,
                  encode: async (...args) => {
                    await at()
                    return encode(...args)
                  },
                },
              }
            }
            return original.call(this, protocol)
          })
        )
      }
      if (cell.boundary === 'commit:answer-held') hold({ api: 'OffsetCommit' }).arrived().then(at)
      if (cell.boundary === 'commit:retry-backoff') {
        scripted.schedule.once(
          { api: 'OffsetCommit' },
          { topics: [{ topic: TOPIC, partitions: [{ partition: 0, errorCode: 14 }] }] }
        )
        const runner = runners[runners.length - 1]
        const old = runner.commitRetrier
        runner.commitRetrier = (work) =>
          old(async (...args) => {
            if (args[1] > 0) await at()
            return work(...args)
          })
      }
      if (cell.boundary === 'commit:pending-queue') {
        const blocker = hold({ api: 'OffsetFetch' })
        settled(consumer.committed([{ topic: TOPIC, partition: 0 }]))
        await pump(() => blocker.call != null, 'occupying the coordinator connection')
        operation = commitOnce()
        await pump(
          () =>
            Object.values(group().cluster.brokerPool.brokers).some((broker) =>
              broker.connectionPool.pool.some(
                (connection) => connection.requestQueue.pending.length > 0
              )
            ),
          'actual pending queue'
        )
        settled(at())
      } else if (cell.boundary.startsWith('commit:')) {
        operation = commitOnce()
      }
    }

    await pump(() => held.call != null, cell.boundary)
    const groups = () => runners.map((runner) => runner.consumerGroup)
    let event
    const replacement = () =>
      consumer.run({ autoCommit: false, eachBatch: async () => {} }).catch(() => {})
    if (cell.event === 'stop') event = settled(consumer.stop())
    if (cell.event === 'disconnect') event = settled(consumer.disconnect())
    if (cell.event === 'force') event = settled(consumer.disconnect({ timeout: 0 }))
    if (cell.event === 'unsubscribe') event = settled(consumer.unsubscribe())
    if (cell.event === 'crash') {
      event = settled(
        runners[runners.length - 1].onCrash(
          Object.assign(new Error('fetch failure'), { retriable: true, retryTime: 1 })
        )
      )
    }
    if (cell.event === 'reassign') {
      event = settled(
        (async () => {
          await consumer.stop()
          await consumer.assign([{ topic: TOPIC, partitions: [0] }])
          await replacement()
        })()
      )
    }
    if (cell.event === 'rebuild') event = settled(group().applyManualAssignment())
    release()
    cleanup.forEach((fn) => fn())
    await pump(() => event.done, 'event completion')
    if (event.error && !(event.error.type === 'CONSUMER_STOPPING')) throw event.error
    if (operation) {
      await pump(() => operation.done, 'held operation completion')
      if (
        operation.error &&
        operation.error.type !== 'CONSUMER_STOPPING' &&
        !(operation.error instanceof KafkaJSCommitFailedError)
      )
        throw operation.error
    }
    if (starting.error) throw starting.error
    const stopping = settled(consumer.disconnect())
    await pump(() => stopping.done, 'terminal disconnect')
    if (stopping.error) throw stopping.error
    // The scripted broker holds an empty Fetch for its wait; client timers never outlive the disconnect.
    await pump(() => jest.getTimerCount() === baseline.timers, 'timers drained')
    const frames = scripted.requests().map((record) => record.api)
    expect(frames.filter((api) => MEMBERSHIP_APIS.includes(api))).toEqual([])
    if (!grouped) expect(frames.filter((api) => GROUP_APIS.includes(api))).toEqual([])
    for (const manager of groups()) expect(manager.operations.size()).toBe(0)
    expect(socketBaseline()).toBe(beforeSockets)
    expect(errors).toEqual([])
    scripted.assertClean()
  } catch (error) {
    failed = true
    throw error
  } finally {
    const cleanupErrors = []
    const attempt = async (work) => {
      try {
        await work()
      } catch (error) {
        cleanupErrors.push(error)
      }
    }
    await attempt(release)
    for (const work of cleanup) await attempt(work)
    await attempt(async () => {
      if (!consumer) return
      const closing = settled(consumer.disconnect())
      await pump(() => closing.done, 'finally disconnect')
      if (closing.error !== null) throw closing.error
    })
    for (const spy of spies.reverse()) await attempt(() => spy.mockRestore())
    await attempt(() => process.removeListener('unhandledRejection', unhandled))
    if (!failed) {
      expect(cleanupErrors).toEqual([])
      expect(process.listenerCount('unhandledRejection')).toBe(baseline.unhandled)
      expect(jest.getTimerCount()).toBe(baseline.timers)
    }
  }
}

module.exports = { runManualLifecycleCell }
