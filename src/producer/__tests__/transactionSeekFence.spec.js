const Cluster = require('../../cluster')
const { producerScript } = require('../../../testHelpers/scriptedCluster')
const { CloseRig, GROUP, TOPIC, gate } = require('../../consumer/__tests__/fixtures/closeRig')
const TransactionOffsets = require('../state/transactionOffsets')
const { KafkaJSTransactionAbortableError, KafkaJSCommitFailedError } = require('../../errors')

/**
 * Runs work inside one consumer handler's async context: the handler serves queued work until
 * `end`, so every call made through `run` carries that handler's batch identity.
 */
class Handler {
  constructor() {
    this.tasks = []
    this.wake = gate()
    this.entered = gate()
    this.ended = false
  }

  /** Called by the handler itself; returns once `end` was called. */
  async serve() {
    this.entered.resolve()
    for (;;) {
      for (const task of this.tasks.splice(0)) {
        Promise.resolve().then(task.work).then(task.resolve, task.reject)
      }
      if (this.ended) return
      this.wake = gate()
      await this.wake.promise
    }
  }

  /**
   * @param {() => Promise<unknown>} work
   * @returns {Promise<unknown>} the work's result, the work having run in the handler's context
   */
  run(work) {
    return new Promise((resolve, reject) => {
      this.tasks.push({ work, resolve, reject })
      this.wake.resolve()
    })
  }

  end() {
    this.ended = true
    this.wake.resolve()
  }
}

/**
 * Holds every answer of one API until released.
 * @returns {{ arrived: Promise<object>, release: () => void }} `arrived` settles with the first
 *   held request's fields, after its bytes were written
 */
const holdAnswer = (cluster, api) => {
  const original = cluster.answerers.get(api)
  const arrived = gate()
  const opened = gate()
  cluster.answer(api, async (fields, record) => {
    arrived.resolve(fields)
    await opened.promise
    return original ? original(fields, record) : undefined
  })
  return { arrived: arrived.promise, release: opened.resolve }
}

const offsetsOf = (...cells) => [
  { topic: TOPIC, partitions: cells.map(([partition, offset]) => ({ partition, offset })) },
]

/** @returns {string[][]} each TxnOffsetCommit written, as `topic|partition|offset` cells */
const offsetWrites = (rig) =>
  rig
    .frames('TxnOffsetCommit')
    .map(({ fields }) =>
      fields.topics.flatMap(({ topic, partitions }) =>
        partitions.map(({ partition, offset }) => `${topic}|${partition}|${offset}`)
      )
    )

/**
 * @param {number} count
 * @returns {Promise<void>} settles after `count` event-loop turns
 */
const turns = async (count) => {
  for (let i = 0; i < count; i++) await new Promise((resolve) => setImmediate(resolve))
}

/** @returns {boolean[]} the `transactionResult` of each EndTxn written */
const endResults = (rig) => rig.frames('EndTxn').map(({ fields }) => fields.transactionResult)

const expectFenced = (error) => {
  expect(error).toBeInstanceOf(KafkaJSTransactionAbortableError)
  expect(error.retriable).toBe(false)
  expect(error.category).toBe('abortable')
  expect(error.cause).toBeInstanceOf(KafkaJSCommitFailedError)
  expect(error.cause.reason).toBe('stale-assignment')
}

describe.each(['tv1', 'tv2'])('Producer > transactional seek fence (%s)', (profile) => {
  let rigs, producers, handlers, ledgers

  /**
   * A consumer whose first batch of partition 0 is held in a `Handler`.
   * @returns {Promise<{ rig: CloseRig, consumer: object, handler: Handler }>}
   */
  const openInput = async () => {
    const rig = await CloseRig.open({ profile, partitions: 2 })
    rigs.push(rig)
    producerScript(rig.cluster, { partitions: 2 }).answer(
      'InitProducerId',
      ({ producerEpoch = -1 }) => ({
        producerId: '1000',
        producerEpoch: Math.max(0, producerEpoch + 1),
      })
    )
    const handler = new Handler()
    handlers.push(handler)
    const consumer = rig.consumer()
    await consumer.connect()
    await consumer.subscribe({ topic: TOPIC, fromBeginning: true })
    let first = true
    await consumer.run({
      autoCommit: false,
      eachBatch: async ({ batch }) => {
        if (batch.partition !== 0 || !first) return
        first = false
        await handler.serve()
      },
    })
    await handler.entered.promise
    return { rig, consumer, handler }
  }

  const openProducer = async (rig, options = {}) => {
    const producer = rig.kafka.producer({
      transactionalId: `seek-fence-${profile}`,
      maxInFlightRequests: 1,
      ...options,
    })
    producers.push(producer)
    await producer.connect()
    return producer
  }

  /** @returns {Map|null|undefined} the input dependencies of the one transactional offset state */
  const inputs = () => [...ledgers][0].inputs

  const seekBack = (consumer) => consumer.seek({ topic: TOPIC, partition: 0, offset: '0' })

  /** Opens a transaction that already holds an output record, sent from the handler. */
  const begin = async ({ rig, handler }) => {
    const producer = await openProducer(rig)
    const transaction = await producer.transaction()
    await handler.run(() => transaction.send({ topic: TOPIC, messages: [{ value: 'out' }] }))
    return { producer, transaction }
  }

  beforeEach(() => {
    rigs = []
    producers = []
    handlers = []
    ledgers = new Set()
    const snapshot = TransactionOffsets.prototype.snapshot
    jest.spyOn(TransactionOffsets.prototype, 'snapshot').mockImplementation(function (...args) {
      ledgers.add(this)
      return snapshot.apply(this, args)
    })
  })

  afterEach(async () => {
    jest.restoreAllMocks()
    for (const handler of handlers) handler.end()
    for (const producer of producers) await producer.disconnect().catch(() => {})
    for (const rig of rigs) await rig.dispose()
  })

  it('an applied seek refuses an old handler offset call before any of it is written', async () => {
    const input = await openInput()
    const { transaction } = await begin(input)
    seekBack(input.consumer)

    const error = await input.handler
      .run(() =>
        transaction.sendOffsets({ consumerGroupId: GROUP, topics: offsetsOf([0, '1'], [1, '7']) })
      )
      .catch((e) => e)

    expectFenced(error)
    expect(input.rig.frames('AddOffsetsToTxn')).toHaveLength(0)
    expect(offsetWrites(input.rig)).toEqual([])
    await expect(transaction.commit()).rejects.toBe(error)
    await transaction.abort()
    expect(endResults(input.rig)).toEqual([false])
    expect(inputs()).toBeNull()
  })

  it('a stale handler call is refused at admission while an earlier offset call is in flight', async () => {
    const input = await openInput()
    const { transaction } = await begin(input)
    const held = holdAnswer(input.rig.cluster, 'TxnOffsetCommit')
    const earlier = transaction.sendOffsets({ consumerGroupId: GROUP, topics: offsetsOf([1, '7']) })
    await held.arrived
    seekBack(input.consumer)

    const stale = input.handler
      .run(() => transaction.sendOffsets({ consumerGroupId: GROUP, topics: offsetsOf([0, '1']) }))
      .catch((e) => e)
    // The earlier call stays held, so only a refusal at admission can settle the stale one.
    const error = await Promise.race([stale, turns(50).then(() => 'still queued')])
    expectFenced(error)
    held.release()
    await earlier
    expect(offsetWrites(input.rig)).toEqual([[`${TOPIC}|1|7`]])
    await expect(transaction.commit()).rejects.toBe(error)
    await transaction.abort()
    expect(endResults(input.rig)).toEqual([false])
  })

  it('a stale input refuses the commit before its coordinator is looked up', async () => {
    const input = await openInput()
    const { transaction } = await begin(input)
    await input.handler.run(() =>
      transaction.sendOffsets({ consumerGroupId: GROUP, topics: offsetsOf([0, '1']) })
    )
    seekBack(input.consumer)
    const lookups = () =>
      input.rig.frames('FindCoordinator').filter(({ fields }) => fields.keyType === 1).length
    const before = lookups()
    expect(before).toBeGreaterThan(0)
    // An unreachable transaction coordinator would leave a looked-up commit pending and retriable.
    const original = input.rig.cluster.answerers.get('FindCoordinator')
    input.rig.cluster.answer('FindCoordinator', (fields, record) =>
      fields.keyType === 1
        ? {
            coordinators: fields.coordinatorKeys.map((key) => ({
              key,
              nodeId: -1,
              host: '',
              port: -1,
              errorCode: 15,
              errorMessage: null,
            })),
          }
        : original(fields, record)
    )

    const error = await transaction.commit().catch((e) => e)
    expectFenced(error)
    expect(lookups()).toBe(before)
    expect(endResults(input.rig)).toEqual([])
    input.rig.cluster.answer('FindCoordinator', original)
    await transaction.abort()
    expect(endResults(input.rig)).toEqual([false])
  })

  it('a seek before the TxnOffsetCommit is written refuses the whole call', async () => {
    const input = await openInput()
    const { transaction } = await begin(input)
    // tv1 holds the registration's answer; tv2 holds the group coordinator lookup.
    const held = holdAnswer(
      input.rig.cluster,
      profile === 'tv1' ? 'AddOffsetsToTxn' : 'FindCoordinator'
    )
    const sent = input.handler
      .run(() =>
        transaction.sendOffsets({ consumerGroupId: GROUP, topics: offsetsOf([0, '1'], [1, '7']) })
      )
      .catch((e) => e)
    await held.arrived
    seekBack(input.consumer)
    held.release()

    expectFenced(await sent)
    expect(offsetWrites(input.rig)).toEqual([])
    expect(input.rig.frames('AddOffsetsToTxn')).toHaveLength(profile === 'tv1' ? 1 : 0)
    await expect(transaction.commit()).rejects.toBeInstanceOf(KafkaJSTransactionAbortableError)
    await transaction.abort()
    expect(endResults(input.rig)).toEqual([false])
    expect(inputs()).toBeNull()
  })

  it('a seek after the TxnOffsetCommit was written refuses the commit', async () => {
    const input = await openInput()
    const { transaction } = await begin(input)
    const held = holdAnswer(input.rig.cluster, 'TxnOffsetCommit')
    const sent = input.handler.run(() =>
      transaction.sendOffsets({ consumerGroupId: GROUP, topics: offsetsOf([0, '1'], [1, '7']) })
    )
    await held.arrived
    seekBack(input.consumer)
    held.release()
    await sent

    expect(offsetWrites(input.rig)).toEqual([[`${TOPIC}|0|1`, `${TOPIC}|1|7`]])
    const error = await transaction.commit().catch((e) => e)
    expectFenced(error)
    expect(endResults(input.rig)).toEqual([])
    await transaction.abort()
    expect(endResults(input.rig)).toEqual([false])
    expect(inputs()).toBeNull()
  })

  it('a seek while the commit looks up its coordinator refuses the first EndTxn write', async () => {
    const input = await openInput()
    const { transaction } = await begin(input)
    await input.handler.run(() =>
      transaction.sendOffsets({ consumerGroupId: GROUP, topics: offsetsOf([0, '1']) })
    )
    const lookup = holdAnswer(input.rig.cluster, 'FindCoordinator')
    const committed = transaction.commit().catch((e) => e)
    await lookup.arrived
    seekBack(input.consumer)
    lookup.release()

    expectFenced(await committed)
    expect(endResults(input.rig)).toEqual([])
    await transaction.abort()
    expect(endResults(input.rig)).toEqual([false])
    expect(inputs()).toBeNull()
  })

  it('a seek after the commit EndTxn was written keeps the commit', async () => {
    const input = await openInput()
    const { producer, transaction } = await begin(input)
    const published = jest.spyOn(Cluster.prototype, 'markOffsetAsCommitted')
    await input.handler.run(() =>
      transaction.sendOffsets({
        consumerGroupId: GROUP,
        topics: offsetsOf([0, '1'], [1, '7']),
      })
    )
    const held = holdAnswer(input.rig.cluster, 'EndTxn')
    const committed = transaction.commit()
    expect((await held.arrived).transactionResult).toBe(true)
    seekBack(input.consumer)
    held.release()

    await committed
    expect(endResults(input.rig)).toEqual([true])
    expect(inputs()).toBeNull()
    // The broker committed both offsets, so the shared cache records both: a local seek fences
    // only its own manager's work, never a commit's accepted value.
    expect(published.mock.calls.map(([offsets]) => offsets)).toEqual([
      { groupId: GROUP, topic: TOPIC, partition: 0, offset: '1' },
      { groupId: GROUP, topic: TOPIC, partition: 1, offset: '7' },
    ])
    const next = await producer.transaction()
    await next.abort()
  })

  it('a seek after an unknown commit outcome cannot turn its retry into an abort', async () => {
    const input = await openInput()
    const { transaction } = await begin(input)
    await input.handler.run(() =>
      transaction.sendOffsets({ consumerGroupId: GROUP, topics: offsetsOf([0, '1']) })
    )
    // COORDINATOR_LOAD_IN_PROGRESS on every attempt: the outcome stays unknown.
    input.rig.cluster.answer('EndTxn', () => ({ errorCode: 14 }))
    const unknown = await transaction.commit().catch((e) => e)
    expect(unknown.retriable).toBe(true)
    seekBack(input.consumer)
    input.rig.cluster.answer('EndTxn', () => undefined)

    await transaction.commit()
    const ends = input.rig.frames('EndTxn').map(({ fields }) => fields)
    expect(ends.length).toBeGreaterThan(1)
    expect(
      new Set(ends.map((f) => `${f.producerId}/${f.producerEpoch}/${f.transactionResult}`))
    ).toEqual(new Set([`${ends[0].producerId}/${ends[0].producerEpoch}/true`]))
    expect(inputs()).toBeNull()
  })

  it('a seek after a confirmed commit leaves it unchanged', async () => {
    const input = await openInput()
    const { producer, transaction } = await begin(input)
    await input.handler.run(() =>
      transaction.sendOffsets({ consumerGroupId: GROUP, topics: offsetsOf([0, '1']) })
    )
    await transaction.commit()
    seekBack(input.consumer)

    const next = await producer.transaction()
    await next.sendOffsets({ consumerGroupId: GROUP, topics: offsetsOf([0, '0']) })
    await next.commit()
    expect(endResults(input.rig)).toEqual([true, true])
    expect(inputs()).toBeNull()
  })

  it('explicit offsets outside a handler stay permitted after a seek', async () => {
    const input = await openInput()
    const { transaction } = await begin(input)
    seekBack(input.consumer)

    await transaction.sendOffsets({ consumerGroupId: GROUP, topics: offsetsOf([0, '1']) })
    await transaction.sendOffsets({
      consumerGroupMetadata: input.consumer.groupMetadata(),
      topics: offsetsOf([0, '2']),
    })
    expect(inputs() ?? null).toBeNull()
    await transaction.commit()
    expect(offsetWrites(input.rig)).toEqual([[`${TOPIC}|0|1`], [`${TOPIC}|0|2`]])
    expect(endResults(input.rig)).toEqual([true])
  })

  it('an old handler may still send offsets of another partition or another group', async () => {
    const input = await openInput()
    const { transaction } = await begin(input)
    seekBack(input.consumer)

    await input.handler.run(async () => {
      await transaction.sendOffsets({ consumerGroupId: GROUP, topics: offsetsOf([1, '7']) })
      await transaction.sendOffsets({ consumerGroupId: 'other-group', topics: offsetsOf([0, '1']) })
    })
    await transaction.commit()
    expect(offsetWrites(input.rig)).toEqual([[`${TOPIC}|1|7`], [`${TOPIC}|0|1`]])
    expect(endResults(input.rig)).toEqual([true])
  })

  it('a later offset for the same partition cannot discard an older input', async () => {
    const input = await openInput()
    const { transaction } = await begin(input)
    await input.handler.run(() =>
      transaction.sendOffsets({ consumerGroupId: GROUP, topics: offsetsOf([0, '1']) })
    )
    await transaction.sendOffsets({ consumerGroupId: GROUP, topics: offsetsOf([0, '9']) })
    seekBack(input.consumer)

    expectFenced(await transaction.commit().catch((e) => e))
    expect(endResults(input.rig)).toEqual([])
    await transaction.abort()
    expect(endResults(input.rig)).toEqual([false])
  })

  it('several input consumers keep their own origins until the commit', async () => {
    const a = await openInput()
    const b = await openInput()
    const { producer, transaction } = await begin(a)
    await a.handler.run(() =>
      transaction.sendOffsets({ consumerGroupId: GROUP, topics: offsetsOf([0, '1']) })
    )
    seekBack(b.consumer)
    await transaction.commit()
    expect(endResults(a.rig)).toEqual([true])

    const next = await producer.transaction()
    await a.handler.run(() =>
      next.sendOffsets({ consumerGroupId: GROUP, topics: offsetsOf([0, '1']) })
    )
    const c = await openInput()
    await c.handler.run(() =>
      next.sendOffsets({ consumerGroupId: GROUP, topics: offsetsOf([0, '2']) })
    )
    expect(inputs().size).toBe(2)
    seekBack(c.consumer)

    expectFenced(await next.commit().catch((e) => e))
    await next.abort()
    expect(endResults(a.rig)).toEqual([true, false])
    expect(inputs()).toBeNull()
  })

  it('disconnecting the producer drops the input dependencies of its open transaction', async () => {
    const input = await openInput()
    const { producer, transaction } = await begin(input)
    await input.handler.run(() =>
      transaction.sendOffsets({ consumerGroupId: GROUP, topics: offsetsOf([0, '1']) })
    )
    expect(inputs().size).toBe(1)

    await producer.disconnect()
    expect(inputs()).toBeNull()
  })

  it('a send that is not transactional does no input fence work', async () => {
    const input = await openInput()
    const enlist = jest.spyOn(TransactionOffsets.prototype, 'enlist')
    const supersededInput = jest.spyOn(TransactionOffsets.prototype, 'supersededInput')
    const producer = await openProducer(input.rig, { transactionalId: undefined })
    seekBack(input.consumer)

    await input.handler.run(() => producer.send({ topic: TOPIC, messages: [{ value: 'out' }] }))
    expect(input.rig.frames('Produce')).toHaveLength(1)
    expect(enlist).not.toHaveBeenCalled()
    expect(supersededInput).not.toHaveBeenCalled()
  })
})
