const PartitionSequencer = require('../partitionSequencer')
const { newLogger } = require('testHelpers')

const TOPIC = 'topic-a'
const LEADERS = { [TOPIC]: { 1: [0] } }
const GATE = {
  now: 0,
  producerIdValid: true,
  producerId: '1000',
  producerEpoch: 0,
  isPartitionInTransaction: () => true,
}

const createBatch = (id) => ({
  id,
  topic: TOPIC,
  partition: 0,
  messages: [{ value: `record-${id}` }],
  acks: -1,
  timeout: 30000,
  compression: 0,
  firstSequence: null,
  producerId: null,
  producerEpoch: null,
  attempt: 0,
  settlers: [],
  backoffUntil: 0,
  createdAt: 0,
  sequenceReset: false,
})

describe('Producer > PartitionSequencer > partition queue', () => {
  let shift

  afterEach(() => {
    shift?.mockRestore()
  })

  it('drains a queue far past the size where Array#shift stops being cheap, in order, without Array#shift', () => {
    const sequencer = new PartitionSequencer({ logger: newLogger() })
    const queued = Array.from({ length: 30000 }, (_, id) => createBatch(id))
    sequencer.enqueue(queued)
    shift = jest.spyOn(Array.prototype, 'shift')

    const drained = []
    for (let ready = sequencer.drain(1, LEADERS, GATE); ready.length > 0;) {
      drained.push(...ready)
      ready.forEach((batch) => sequencer.complete(batch, { baseOffset: batch.id }))
      ready = sequencer.drain(1, LEADERS, GATE)
    }

    expect(drained.map(({ id }) => id)).toEqual(queued.map(({ id }) => id))
    expect(shift).not.toHaveBeenCalled()
    expect(sequencer.entry(TOPIC, 0).queued).toEqual([])
  })

  it('puts a failed batch back ahead of the batches that were never sent, deep in a long queue', () => {
    const sequencer = new PartitionSequencer({ logger: newLogger() })
    const queued = Array.from({ length: 2000 }, (_, id) => createBatch(id))
    sequencer.enqueue(queued)
    const [first] = sequencer.drain(1, LEADERS, GATE)

    sequencer.requeue(first, 0)

    expect(sequencer.entry(TOPIC, 0).queued.map(({ id }) => id)).toEqual(queued.map(({ id }) => id))
    expect(sequencer.drain(1, LEADERS, GATE)).toEqual([first])
  })
})
