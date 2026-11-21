const mockSiblings = []

jest.mock('../../consumer', () => {
  const create = jest.requireActual('../../consumer')
  return (options) => {
    const consumer = create(options)
    mockSiblings.push({ consumer, cluster: options.cluster })
    return consumer
  }
})

const { Kafka } = require('../../../index')
const Broker = require('../../broker')
const { createScriptedCluster, groupScript } = require('../../../testHelpers/scriptedCluster')
const { Schedule } = require('../../../testHelpers/schedule')
const { TOPIC } = require('../../consumer/__tests__/fixtures/fetchHarness')
const { describeGroupAs } = require('./fixtures/describeGroupAs')

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

describe('admin setOffsets close boundaries', () => {
  test.each(['describe', 'commit'])(
    'natural close drains a pending %s and leaves a sibling registry holder alone',
    async (stage) => {
      mockSiblings.length = 0
      const schedule = new Schedule()
      const wire = groupScript(
        createScriptedCluster({ nodes: [{ id: 0, host: 'node', port: 9092 }], schedule }),
        { committed: { 0: '13' } }
      )
      const described = describeGroupAs()
      let releaseDescribe
      let describeArrived = false
      if (stage === 'describe') {
        const answer = Broker.prototype.consumerGroupDescribe.getMockImplementation()
        jest.spyOn(Broker.prototype, 'consumerGroupDescribe').mockImplementation(function (...a) {
          describeArrived = true
          return new Promise((resolve) => (releaseDescribe = resolve)).then(() =>
            answer.apply(this, a)
          )
        })
      }
      const wireGate = stage === 'commit' ? schedule.hold({ api: 'OffsetCommit' }) : null
      const kafka = new Kafka(wire.kafkaOptions())
      const admin = kafka.admin()
      const sibling = kafka.consumer({ groupId: 'held-group' })
      let close
      let call
      try {
        await admin.connect()
        await sibling.subscribe({ topic: TOPIC })
        const registry = mockSiblings[0].cluster.groupRegistry
        mockSiblings[0].cluster.offsetOperations({ groupId: 'held-group' })
        const entry = registry.entries.get('held-group')
        const holders = entry.holders.size
        call = observe(
          admin.setOffsets({
            groupId: 'held-group',
            topic: TOPIC,
            partitions: [{ partition: 0, offset: '-1' }],
          })
        )
        await until(() => {
          if (call.error) throw call.error
          return wireGate ? Boolean(wireGate.call) : describeArrived
        })
        expect(registry.entries.get('held-group')).toBe(entry)
        expect(entry.holders.size).toBe(holders)
        close = observe(admin.disconnect())
        await new Promise((resolve) => setTimeout(resolve, 1))
        expect(close.settled).toBe(false)
        expect(call.settled).toBe(false)
        if (releaseDescribe) releaseDescribe()
        if (wireGate) wireGate.release()
        await call.promise
        expect(call.error).toBeUndefined()
        await close.promise
        expect(close.error).toBeUndefined()
        expect(registry.entries.get('held-group')).toBe(entry)
        expect(entry.holders.size).toBe(holders)
        expect(wire.requests().filter(({ api }) => api === 'OffsetCommit')).toHaveLength(1)
        await sibling.disconnect()
        expect(registry.entries.has('held-group')).toBe(false)
      } finally {
        if (releaseDescribe) releaseDescribe()
        if (wireGate?.settle) wireGate.release()
        if (call) await call.promise
        described.restore()
        jest.restoreAllMocks()
        await sibling.disconnect()
        await admin.disconnect({ timeout: 0 })
        wire.assertClean()
      }
    },
    30000
  )
})
