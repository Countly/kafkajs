const Accumulator = require('../accumulator')
const PartitionSequencer = require('../partitionSequencer')
const { Admission } = require('../admission')

const TOPIC = 'cut-topic'
const record = (i) => ({ value: `${i}`.padEnd(100, 'x') })
const tick = () => new Promise((resolve) => setImmediate(resolve))

/**
 * An accumulator that hands every batch of `sends` on together, one send per append, and keeps
 * them. All records go to partition 0.
 */
const held = async (...sends) => {
  const handed = []
  const admission = new Admission({ capacity: 1000000, maxBlockMs: 0 })
  const accumulator = new Accumulator({
    linger: 20,
    batchSize: 16384,
    admission,
    partition: async ({ records }) =>
      records.map(({ topic, messages }) => ({ topic, partition: 0, messages })),
    sendPartitioned: async ({ batches }) => {
      handed.push(...batches)
    },
  })
  const results = sends.map((messages) =>
    accumulator.append({ acks: -1, topicMessages: [{ topic: TOPIC, messages }] })
  )
  await new Promise((resolve) => setTimeout(resolve, 40))
  return { accumulator, admission, handed, results }
}

describe('Producer > Accumulator.cut', () => {
  describe('a batch whose caller owns nothing', () => {
    const outside = () => {
      const seen = { resolved: [], rejected: [] }
      return {
        seen,
        settler: {
          resolve: (value) => seen.resolved.push(value),
          reject: (error) => seen.rejected.push(error),
        },
      }
    }

    test('is divided with its records, and each callback runs once both halves have called it', () => {
      const calls = []
      const messages = [record(0), record(1), record(2)]
      const [head, tail] = Accumulator.cut(
        {
          messages,
          settlers: [],
          retire: () => calls.push('retire'),
          detach: () => calls.push('detach'),
        },
        1
      )

      expect([head.messages.length, tail.messages.length]).toEqual([1, 2])
      expect([head.recordCount, tail.recordCount]).toEqual([1, 2])
      expect(messages).toHaveLength(0)
      head.detach()
      head.detach()
      expect(calls).toEqual([])
      expect(head.messages).toHaveLength(0)
      tail.detach()
      expect(calls).toEqual(['detach'])
      expect(tail.messages).toHaveLength(0)
      head.retire()
      head.retire()
      expect(calls).toEqual(['detach'])
      tail.retire()
      expect(calls).toEqual(['detach', 'retire'])
    })

    test('is divided without callbacks', () => {
      const [head, tail] = Accumulator.cut({ messages: [record(0), record(1)], settlers: [] }, 1)

      expect(() => {
        head.detach()
        head.retire()
        tail.detach()
        tail.retire()
      }).not.toThrow()
      expect([head.messages.length, tail.messages.length]).toEqual([0, 0])
    })

    test('settles an outside settler with the first half’s value once both halves have, and ignores a half’s second answer', () => {
      const { seen, settler } = outside()
      const [head, tail] = Accumulator.cut(
        { messages: [record(0), record(1)], settlers: [settler] },
        1
      )
      const first = { topicName: TOPIC, partition: 0, baseOffset: '10' }
      const second = { topicName: TOPIC, partition: 0, baseOffset: '11' }

      head.settlers[0].resolve(first)
      head.settlers[0].resolve({ ...first, baseOffset: '99' })
      expect(seen.resolved).toEqual([])
      tail.settlers[0].resolve(second)
      tail.settlers[0].resolve({ ...second, baseOffset: '98' })

      expect(seen).toEqual({ resolved: [first], rejected: [] })
    })

    test('rejects an outside settler with the first error either half was given', () => {
      const { seen, settler } = outside()
      const [head, tail] = Accumulator.cut(
        { messages: [record(0), record(1)], settlers: [settler] },
        1
      )
      const failure = new Error('head failed')

      head.settlers[0].reject(failure)
      head.settlers[0].reject(new Error('again'))
      tail.settlers[0].resolve({ topicName: TOPIC, partition: 0, baseOffset: '10' })

      expect(seen).toEqual({ resolved: [], rejected: [failure] })
    })
  })

  describe('a batch of several sends', () => {
    test('cut at a send boundary keeps each send with its records, charges nothing twice and gives every byte back', async () => {
      const { admission, handed, results } = await held(
        [record(0), record(1)],
        [record(2), record(3)]
      )
      const [batch] = handed
      expect(handed).toHaveLength(1)
      expect(batch.settlers.map((settler) => settler.index)).toEqual([0, 2, undefined])

      const [head, tail] = Accumulator.cut(batch, 2)

      expect([head.messages.length, tail.messages.length]).toEqual([2, 2])
      expect(head.settlers.map((settler) => settler.index)).toEqual([0, undefined])
      expect(tail.settlers.map((settler) => settler.index)).toEqual([0, undefined])
      head.settlers.forEach((settler) =>
        settler.resolve({ topicName: TOPIC, partition: 0, baseOffset: '10' })
      )
      tail.settlers.forEach((settler) =>
        settler.resolve({ topicName: TOPIC, partition: 0, baseOffset: '12' })
      )

      await expect(results[0]).resolves.toEqual([
        { topicName: TOPIC, partition: 0, baseOffset: '10' },
      ])
      await expect(results[1]).resolves.toEqual([
        { topicName: TOPIC, partition: 0, baseOffset: '12' },
      ])
      head.retire()
      tail.retire()
      batch.retire()
      expect([admission.reserved, admission.debts]).toEqual([0, 0])
    })

    test('a settler made without an origin reports the send’s first record at its own position', async () => {
      const { handed } = await held([record(0)])
      const Settler = handed[0].settlers[0].constructor
      const delivered = []
      const send = { share() {}, delivered: (...args) => delivered.push(args) }

      new Settler(send, 3).resolve({ topicName: TOPIC, partition: 0, baseOffset: '10' })

      expect(delivered).toEqual([[{ topicName: TOPIC, partition: 0, baseOffset: '13' }, 0]])
    })

    test('keeps a partition counted when a tracker is cut after its count was cleared', async () => {
      const { accumulator, handed } = await held([record(0), record(1)])
      const tracker = handed[0].settlers.find((settler) => typeof settler.cut === 'function')
      tracker.resolve()

      const [head, tail] = tracker.cut()
      let closed = false
      accumulator.close().then(() => {
        closed = true
      })
      await tick()
      expect(closed).toBe(false)

      head.resolve()
      tail.resolve()
      await tick()
      expect(closed).toBe(true)
    })
  })
})

describe('Producer > a batch emptied in place', () => {
  const gate = () => ({
    now: Date.now(),
    producerIdValid: true,
    producerId: '1',
    producerEpoch: 0,
    isPartitionInTransaction: () => true,
  })

  test('keeps its record count for sequence retirement and cannot return a range twice', () => {
    const sequencer = new PartitionSequencer({
      logger: { debug() {}, info() {}, warn() {}, error() {} },
    })
    const child = {
      topic: TOPIC,
      partition: 0,
      messages: [record(0), record(1)],
      recordCount: 2,
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
    }
    sequencer.enqueue([child])
    sequencer.drain(0, { [TOPIC]: { 0: [0] } }, gate())
    expect(sequencer.nextSequence(TOPIC, 0)).toBe(2)

    child.messages.length = 0
    sequencer.rebase(child)
    expect(sequencer.nextSequence(TOPIC, 0)).toBe(0)
    sequencer.fail(child)
    sequencer.fail(child)
    expect(sequencer.nextSequence(TOPIC, 0)).toBe(0)
  })
})
