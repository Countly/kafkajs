const ConsumerGroup = require('../consumerGroup')
const { TOPIC, CloseRig, batchAt, gate } = require('./fixtures/closeRig')

const CELL = { topic: TOPIC, partition: 0 }
const WARNING = 'Seek failed; the partition continues from its position before the seek'

/**
 * Captures the consumer group of the run when it applies its first assignment.
 * @returns {{ group: () => ConsumerGroup, restore: () => void }}
 */
const captureGroup = () => {
  const apply = ConsumerGroup.prototype.applyRebalanceResult
  let captured = null
  const spy = jest
    .spyOn(ConsumerGroup.prototype, 'applyRebalanceResult')
    .mockImplementation(function (...args) {
      captured = this
      return apply.apply(this, args)
    })
  return { group: () => captured, restore: () => spy.mockRestore() }
}

/**
 * Answers the next OffsetCommit that carries `offset` with `errorCode` for every cell, recording
 * nothing; every other commit is answered as usual.
 * @param {CloseRig} rig
 * @param {string} offset
 * @param {number} errorCode
 * @returns {void}
 */
const rejectCommitOf = (rig, offset, errorCode) => {
  const answer = rig.cluster.answerers.get('OffsetCommit')
  let rejected = false
  rig.cluster.answer('OffsetCommit', (fields, record) => {
    if (rejected || String(fields.topics[0].partitions[0].offset) !== offset)
      return answer(fields, record)
    rejected = true
    return {
      topics: fields.topics.map((topic) => ({
        ...topic,
        partitions: topic.partitions.map(({ partition }) => ({ partition, errorCode })),
      })),
    }
  })
}

/**
 * A new group with nothing committed: the reset policy places the partition at 2, the first batch
 * (record 2) is resolved and its handler held, so the position is 3 and nothing is committed.
 * Records 3 and 4 then arrive, which moves the log end to 5, and the application seeks back to 2.
 * The schedule makes that seek fail; the outcome is what the consumer delivers next.
 * @param {object} options
 * @param {'classic'|'consumer'} options.protocol
 * @param {boolean} options.fromBeginning the reset policy, `earliest` when true
 * @param {(rig: CloseRig, consumer: object, group: ConsumerGroup) => Promise<{ failure: Error|null }>}
 *   options.fail arms the failure and seeks, returning what `position()` raised, or null on the
 *   fetch-loop path
 * @returns {Promise<object>} the failure, the position after it, the next delivery, the
 *   OffsetFetch and ListOffsets frames the failure caused, every OffsetCommit offset and the WARN
 *   messages the consumer group logged
 */
const failedSeek = async ({ protocol, fromBeginning, fail }) => {
  const rig = await CloseRig.open({ protocol, last: -1, retry: { maxTotalTime: 300 } })
  rig.lookup()
  const captured = captureGroup()
  const entered = gate()
  const releaseHandler = gate()
  const next = gate()
  const finished = gate()
  let end = 3
  const data = {}
  for (const offset of [2, 3, 4, 5]) data[offset] = await batchAt(offset)
  rig.cluster.answer('ListOffsets', ({ topics }) => ({
    topics: topics.map(({ name, partitions }) => ({
      name,
      partitions: partitions.map(({ partitionIndex, timestamp }) => ({
        partitionIndex,
        errorCode: 0,
        timestamp: '-1',
        offset: String(timestamp) === '-2' ? '2' : String(end - 1),
        leaderEpoch: 5,
      })),
    })),
  }))
  rig.cluster.answer('Fetch', ({ topics }) => ({
    partitions: topics[0].partitions.map(({ partition, fetchOffset }) => ({
      partition,
      errorCode: 0,
      records: data[fetchOffset] ?? false,
      abortedTransactions: [],
    })),
  }))
  try {
    const consumer = rig.consumer()
    let delivered = 0
    await consumer.connect()
    await consumer.subscribe({ topic: TOPIC, fromBeginning })
    await consumer.run({
      eachBatchAutoResolve: false,
      eachBatch: async ({ batch, resolveOffset }) => {
        if (++delivered === 1) {
          resolveOffset(batch.lastOffset())
          entered.resolve(batch.firstOffset())
          await releaseHandler.promise
        } else {
          next.resolve(batch.firstOffset())
          await finished.promise
        }
      },
    })
    expect(await entered.promise).toBe('2')
    expect(await consumer.position(CELL)).toBe('3')
    const warn = jest.spyOn(captured.group().logger, 'warn')
    end = 6
    const lookups = {
      fetch: rig.frames('OffsetFetch').length,
      list: rig.frames('ListOffsets').length,
    }
    const joinsBefore = joins(rig)

    const { failure } = await fail(rig, consumer, captured.group())
    const position = failure ? await consumer.position(CELL) : null
    releaseHandler.resolve()
    const delivery = await next.promise
    return {
      failure: failure && (failure.type || failure.name),
      position,
      delivery,
      offsetFetches: rig.frames('OffsetFetch').length - lookups.fetch,
      listOffsets: rig.frames('ListOffsets').length - lookups.list,
      joins: joins(rig) - joinsBefore,
      commits: rig
        .frames('OffsetCommit')
        .map(({ fields }) => String(fields.topics[0].partitions[0].offset)),
      warnings: warn.mock.calls
        .map(([message]) => message)
        .filter((message) => message === WARNING),
    }
  } finally {
    captured.restore()
    releaseHandler.resolve()
    finished.resolve()
    await rig.dispose()
  }
}

/**
 * @param {CloseRig} rig
 * @returns {number} the group joins so far: JoinGroup frames, or heartbeats at member epoch 0
 */
const joins = (rig) =>
  rig.protocol === 'consumer'
    ? rig.frames('ConsumerGroupHeartbeat').filter(({ fields }) => fields.memberEpoch === 0).length
    : rig.frames('JoinGroup').length

/** The seek's OffsetCommit is answered REQUEST_TIMED_OUT, which causes no rejoin. */
const rejected = (viaPosition) => async (rig, consumer) => {
  rejectCommitOf(rig, '2', 7)
  consumer.seek({ ...CELL, offset: '2' })
  if (!viaPosition) return { failure: null }
  return {
    failure: await consumer.position(CELL).then(
      () => null,
      (error) => error
    ),
  }
}

/**
 * The coordinator reads disconnected and its lookup, held, outlives `position()`'s deadline. The
 * rig's lookup switch is installed before the consumer connects, so the coordinator it handed out
 * can be made to read disconnected.
 */
const lookupTimedOut = async (rig, consumer) => {
  const lookup = rig.lookup()
  lookup.makeStale()
  const held = lookup.hold()
  consumer.seek({ ...CELL, offset: '2' })
  const seeking = consumer.position(CELL).then(
    () => null,
    (error) => error
  )
  await held.entered
  const failure = await seeking
  const answering = lookup.spy.mock.results.at(-1).value
  held.release()
  await answering
  return { failure }
}

/**
 * Makes the group's first `Seek offset` DEBUG run `effect`, then throw `failure`.
 * @param {ConsumerGroup} group
 * @param {Error} failure
 * @param {() => void} [effect]
 * @returns {void}
 */
const throwFromSeekDebug = (group, failure, effect = () => {}) => {
  const debug = group.logger.debug
  let thrown = false
  jest.spyOn(group.logger, 'debug').mockImplementation((message, ...extra) => {
    if (thrown || message !== 'Seek offset') return debug(message, ...extra)
    thrown = true
    effect()
    throw failure
  })
}

describe.each(['classic', 'consumer'])('a failed seek, %s protocol', (protocol) => {
  describe.each([
    ['latest', false],
    ['earliest', true],
  ])(
    'under the %s reset policy, on a partition with no committed offset',
    (policy, fromBeginning) => {
      test('rejected by the broker, keeps the position before the seek and looks nothing up', async () => {
        const outcome = await failedSeek({ protocol, fromBeginning, fail: rejected(true) })
        expect(outcome).toEqual({
          failure: 'REQUEST_TIMED_OUT',
          position: '3',
          delivery: '3',
          offsetFetches: 0,
          listOffsets: 0,
          joins: 0,
          commits: ['2'],
          warnings: [WARNING],
        })
      })

      test('rejected by the broker on the fetch-loop path, delivers from the position before the seek', async () => {
        const outcome = await failedSeek({ protocol, fromBeginning, fail: rejected(false) })
        expect(outcome).toMatchObject({
          delivery: '3',
          offsetFetches: 0,
          listOffsets: 0,
          joins: 0,
          commits: ['2'],
          warnings: [WARNING],
        })
      })

      test('whose coordinator lookup outlived the deadline before the write, keeps the position before the seek', async () => {
        const outcome = await failedSeek({ protocol, fromBeginning, fail: lookupTimedOut })
        expect(outcome).toEqual({
          failure: 'KafkaJSDeliveryTimeout',
          position: '3',
          delivery: '3',
          offsetFetches: 0,
          listOffsets: 0,
          joins: 0,
          commits: [],
          warnings: [WARNING],
        })
      })
    }
  )

  test('whose DEBUG log threw after it was popped, surfaces that error and fetches from the position before the seek', async () => {
    const failure = new Error('seek log failed')
    let raised
    const outcome = await failedSeek({
      protocol,
      fromBeginning: false,
      fail: async (rig, consumer, group) => {
        throwFromSeekDebug(group, failure)
        consumer.seek({ ...CELL, offset: '2' })
        raised = await consumer.position(CELL).then(
          () => null,
          (error) => error
        )
        return { failure: raised }
      },
    })
    expect(raised).toBe(failure)
    expect(outcome).toEqual({
      failure: 'Error',
      position: '3',
      delivery: '3',
      offsetFetches: 0,
      listOffsets: 0,
      joins: 0,
      commits: [],
      warnings: [WARNING],
    })
  })

  test('superseded by a seek admitted from its own DEBUG log, is never written and the newer seek applies', async () => {
    const outcome = await failedSeek({
      protocol,
      fromBeginning: false,
      fail: async (rig, consumer, group) => {
        const debug = group.logger.debug
        let admitted = false
        jest.spyOn(group.logger, 'debug').mockImplementation((message, ...extra) => {
          if (!admitted && message === 'Seek offset') {
            admitted = true
            consumer.seek({ ...CELL, offset: '4' })
          }
          return debug(message, ...extra)
        })
        consumer.seek({ ...CELL, offset: '2' })
        return {
          failure: await consumer.position(CELL).then(
            () => null,
            (error) => error
          ),
        }
      },
    })
    expect(outcome).toMatchObject({ failure: null, commits: ['4'], warnings: [] })
  })

  describe.each([
    ['latest', '-1'],
    ['earliest', '-2'],
  ])('a %s seek superseded by a seek admitted from its own DEBUG log', (boundary, sentinel) => {
    const admitFromDebug = (consumer, group) => {
      const debug = group.logger.debug
      let admitted = false
      jest.spyOn(group.logger, 'debug').mockImplementation((message, ...extra) => {
        if (!admitted && message === 'Seek offset') {
          admitted = true
          consumer.seek({ ...CELL, offset: '4' })
        }
        return debug(message, ...extra)
      })
    }

    test('is not applied, and the partition keeps its position when the newer seek is rejected', async () => {
      const outcome = await failedSeek({
        protocol,
        fromBeginning: false,
        fail: async (rig, consumer, group) => {
          rejectCommitOf(rig, '4', 7)
          admitFromDebug(consumer, group)
          consumer.seek({ ...CELL, offset: sentinel })
          return {
            failure: await consumer.position(CELL).then(
              () => null,
              (error) => error
            ),
          }
        },
      })
      expect(outcome).toEqual({
        failure: 'REQUEST_TIMED_OUT',
        position: '3',
        delivery: '3',
        offsetFetches: 0,
        listOffsets: 0,
        joins: 0,
        commits: ['4'],
        warnings: [WARNING],
      })
    })
  })

  test('rejected with a membership error, ends the acquisition through the normal rejoin', async () => {
    const outcome = await failedSeek({
      protocol,
      fromBeginning: false,
      fail: async (rig, consumer) => {
        rejectCommitOf(rig, '2', 25)
        consumer.seek({ ...CELL, offset: '2' })
        return { failure: null }
      },
    })
    expect(outcome.joins).toBe(1)
  })
})
