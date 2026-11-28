const { Kafka } = require('../../../index')
const { createScriptedCluster, producerScript } = require('../../../testHelpers/scriptedCluster')
const { TOPIC } = require('../../consumer/__tests__/fixtures/fetchHarness')

const SEED = { id: 0, host: 'node0', port: 9092 }
const COORDINATOR = { id: 1, host: 'node1', port: 9092 }
/** Node 1's address after it moved; the scripted cluster calls this endpoint node 2. */
const MOVED = { id: 2, host: 'node1-moved', port: 9092 }
const NOT_COORDINATOR = 16
const COORDINATOR_APIS = ['InitProducerId', 'AddPartitionsToTxn', 'EndTxn']

/**
 * What a promise settled with, so a rejection is a value to look at.
 * @param {Promise<unknown>} promise
 * @returns {Promise<{ value?: unknown, error?: Error }>}
 */
const outcome = (promise) =>
  promise.then(
    (value) => ({ value }),
    (error) => ({ error })
  )

describe('Producer > a transaction coordinator that keeps its node id and moves to a new address', () => {
  it('sends the next transaction to the new address while the old one answers NOT_COORDINATOR', async () => {
    const seam = producerScript(
      createScriptedCluster({ nodes: [SEED, COORDINATOR, MOVED], profile: 'tv1' })
    )
    let moved = false
    const listed = seam.answerers.get('Metadata')
    seam.answer('Metadata', (...args) => {
      const answer = listed(...args)
      return { ...answer, brokers: answer.brokers.filter(({ nodeId }) => nodeId !== MOVED.id) }
    })
    seam.answer('FindCoordinator', ({ coordinatorKeys }) => ({
      coordinators: coordinatorKeys.map((key) => ({
        key,
        nodeId: COORDINATOR.id,
        host: moved ? MOVED.host : COORDINATOR.host,
        port: COORDINATOR.port,
        errorCode: 0,
        errorMessage: null,
      })),
    }))
    const added = seam.answerers.get('AddPartitionsToTxn')
    seam.answer('AddPartitionsToTxn', (fields, record) => {
      const answer = added(fields, record)
      if (!moved || record.node !== COORDINATOR.id) return answer
      return {
        errors: answer.errors.map(({ topic, partitionErrors }) => ({
          topic,
          partitionErrors: partitionErrors.map(({ partition }) => ({
            partition,
            errorCode: NOT_COORDINATOR,
          })),
        })),
      }
    })
    seam.answer('EndTxn', (_fields, { node }) =>
      moved && node === COORDINATOR.id ? { errorCode: NOT_COORDINATOR } : {}
    )
    const producer = new Kafka(
      seam.kafkaOptions({
        brokers: [`${SEED.host}:${SEED.port}`],
        retry: { retries: 1, initialRetryTime: 1, maxRetryTime: 5 },
      })
    ).producer({ transactionalId: 'moved-coordinator', maxInFlightRequests: 1 })
    let movedAt = 0
    /** @returns {Array<[string, number]>} each coordinator request since the move, with its node */
    const coordinatorWire = () =>
      seam
        .requests()
        .slice(movedAt)
        .filter(({ api }) => COORDINATOR_APIS.includes(api))
        .map(({ api, node }) => [api, node])

    try {
      await producer.connect()
      const first = await producer.transaction()
      await first.send({ topic: TOPIC, messages: [{ value: 'a' }] })
      await first.commit()
      moved = true
      movedAt = seam.requests().length

      const second = await producer.transaction()
      const sent = await outcome(second.send({ topic: TOPIC, messages: [{ value: 'b' }] }))

      expect(coordinatorWire().filter(([, node]) => node !== MOVED.id)).toEqual([])
      expect(sent.error).toBeUndefined()
      await second.commit()
      const wire = coordinatorWire()
      expect(wire.filter(([, node]) => node !== MOVED.id)).toEqual([])
      expect(wire.map(([api]) => api)).toEqual(
        expect.arrayContaining(['AddPartitionsToTxn', 'EndTxn'])
      )
      seam.assertClean()
    } finally {
      await producer.disconnect()
    }
  })
})
