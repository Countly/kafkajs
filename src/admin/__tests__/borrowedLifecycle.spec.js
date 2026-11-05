const mockMembers = []
const mockSiblings = []
let mockStage
let mockGate

jest.mock('../../consumer', () => {
  const create = jest.requireActual('../../consumer')
  return (options) => {
    const consumer = create(options)
    if (options.ownsCluster !== false) {
      mockSiblings.push({ consumer, cluster: options.cluster })
      return consumer
    }
    const on = consumer.on
    consumer.on = (event, listener) =>
      on(event, async (...args) => {
        if (mockStage === 'fetch' && event === consumer.events.FETCH) await mockGate.wait()
        return listener(...args)
      })
    const subscribe = consumer.subscribe
    consumer.subscribe = async (...args) => {
      if (mockStage === 'subscribe') await mockGate.wait()
      return subscribe(...args)
    }
    consumer.describeGroup = async () => {
      if (mockStage === 'describe') await mockGate.wait()
      return { state: 'Empty', members: [] }
    }
    mockMembers.push({ consumer, cluster: options.cluster })
    return consumer
  }
})

jest.mock('../../consumer/disposal', () => {
  const original = jest.requireActual('../../consumer/disposal')
  return {
    ...original,
    disposeConsumer: async (consumer) => {
      if (mockStage === 'dispose') await mockGate.wait()
      return original.disposeConsumer(consumer)
    },
  }
})

const { Kafka } = require('../../../index')
const Runner = require('../../consumer/runner')
const { createScriptedCluster, groupScript } = require('../../../testHelpers/scriptedCluster')
const { Schedule } = require('../../../testHelpers/schedule')
const { TOPIC } = require('../../consumer/__tests__/fixtures/fetchHarness')
const { CLUSTER_TOPIC_RECREATED } = require('../../cluster/instrumentationEvents')

/** A manually released boundary; every waiter shares the same terminal promise. */
class Gate {
  /** Creates an unarrived boundary. */
  constructor() {
    this.arrived = false
    this.promise = new Promise((resolve) => {
      this.release = resolve
    })
  }

  /** @returns {Promise<void>} marks actual arrival before blocking the continuation */
  wait() {
    this.arrived = true
    return this.promise
  }
}

/**
 * Pumps real event-loop turns until an observed boundary, with no timer-based arrival assumption.
 * @param {() => boolean} predicate arrival predicate
 * @returns {Promise<void>}
 * @throws {Error} if the boundary never arrives
 */
const until = async (predicate) => {
  for (let index = 0; index < 15000 && !predicate(); index++) {
    await new Promise((resolve) => setTimeout(resolve, 1))
  }
  expect(predicate()).toBe(true)
}

/** @param {Promise<unknown>} promise operation @returns {object} immediately observed outcome */
const observe = (promise) => {
  const result = { settled: false }
  result.promise = promise.then(
    (value) => Object.assign(result, { settled: true, value }),
    (error) => Object.assign(result, { settled: true, error })
  )
  return result
}

describe('borrowed admin consumer boundaries', () => {
  test.each(['subscribe', 'describe', 'join', 'commit', 'fetch', 'stop', 'dispose'])(
    'natural close drains pending %s without releasing a sibling registry holder',
    async (stage) => {
      mockMembers.length = 0
      mockSiblings.length = 0
      mockStage = stage
      mockGate = new Gate()
      const schedule = new Schedule()
      const wire = groupScript(
        createScriptedCluster({
          nodes: [{ id: 0, host: 'node', port: 9092 }],
          schedule,
        }),
        { committed: { 0: '13' } }
      )
      const api = { join: 'JoinGroup', commit: 'OffsetCommit' }[stage]
      const wireGate = api ? schedule.hold({ api }) : null
      const stop = Runner.prototype.stop
      const spy = jest.spyOn(Runner.prototype, 'stop').mockImplementation(async function (...args) {
        if (stage === 'stop') await mockGate.wait()
        return stop.apply(this, args)
      })
      const kafka = new Kafka(wire.kafkaOptions())
      const admin = kafka.admin()
      const sibling = kafka.consumer({ groupId: 'held-group' })
      let close
      let call
      try {
        await admin.connect()
        await sibling.subscribe({ topic: TOPIC })
        mockSiblings[0].cluster.offsetOperations({ groupId: 'held-group' })
        call = observe(
          admin.setOffsets({
            groupId: 'held-group',
            topic: TOPIC,
            partitions: [{ partition: 0, offset: '-1' }],
          })
        )
        await until(() => {
          if (call.error) throw call.error
          return wireGate ? Boolean(wireGate.call) : mockGate.arrived
        }).catch((error) => {
          expect(wire.requests().map(({ api }) => api)).toContain(api || 'OffsetCommit')
          throw error
        })
        const cluster = mockMembers[0].cluster
        const registry = cluster.groupRegistry
        const entry = registry.entries.get('held-group')
        expect(entry).toBeDefined()
        const holders = entry.holders.size
        expect(holders).toBeGreaterThan(1)
        close = observe(admin.disconnect())
        await new Promise((resolve) => setTimeout(resolve, 1))
        expect(close.settled).toBe(false)
        expect(call.settled).toBe(false)
        expect(cluster.isConnected()).toBe(true)
        expect(registry.entries.get('held-group')).toBe(entry)
        mockGate.release()
        if (wireGate) wireGate.release()
        await call.promise
        expect(call.error).toBeUndefined()
        await close.promise
        expect(close.error).toBeUndefined()
        expect(registry.entries.get('held-group')).toBe(entry)
        expect(entry.holders.size).toBeLessThan(holders)
        expect(entry.holders.size).toBeGreaterThan(0)
        expect(cluster.instrumentationEmitter.emitter.listenerCount(CLUSTER_TOPIC_RECREATED)).toBe(
          0
        )
        await sibling.disconnect()
        expect(registry.entries.has('held-group')).toBe(false)
      } finally {
        mockGate.release()
        if (wireGate?.settle) wireGate.release()
        if (call) await call.promise
        spy.mockRestore()
        await sibling.disconnect()
        await admin.disconnect({ timeout: 0 })
        wire.assertClean()
      }
    },
    30000
  )
})
