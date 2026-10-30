const { CloseRig, TOPIC, batchAt, pause, until } = require('./fixtures/closeRig')
const { RecreationRig } = require('./fixtures/recreationRig')

const BOUND_MS = 300
const NEW = ['value-0', 'value-1', 'value-2']

describe.each([true, false])('a recreated topic across a close, with enforcement %p', (enforce) => {
  /** @type {RecreationRig} */
  let rec
  afterEach(async () => {
    await rec.dispose()
  })
  const open = async () => {
    rec = await RecreationRig.open({ enforce })
  }

  test('held ListOffsets: the close commits nothing and the late answer starts nothing', async () => {
    await open()
    const { consumer, delivered } = await rec.start()
    const list = rec.rig.hold('ListOffsets')
    await rec.recreate()
    await until(() => list.hits === 1, 'the held reset')

    await consumer.disconnect({ timeout: BOUND_MS })
    list.release()
    await pause(150)

    expect(rec.rig.frames('OffsetCommit')).toHaveLength(0)
    expect(delivered.filter((value) => NEW.includes(value))).toEqual([])
    expect(rec.offset).toBe('5')
  })

  test('held OffsetFetch then ListOffsets: the close commits nothing and nothing is delivered after it', async () => {
    await open()
    const { consumer, delivered } = await rec.start()
    const fetch = rec.rig.hold('OffsetFetch')
    await rec.recreate()
    await until(() => fetch.hits === 1, 'the held committed read')

    const closing = consumer.disconnect({ timeout: BOUND_MS })
    fetch.release()
    await closing
    await pause(150)

    expect(rec.rig.frames('OffsetCommit')).toHaveLength(0)
    expect(delivered.some((value) => NEW.includes(value))).toBe(false)
    expect(rec.offset).toBe('5')
  })

  test.each([
    ['ListOffsets', 'held ListOffsets'],
    ['OffsetFetch', 'held OffsetFetch then ListOffsets'],
  ])('stop during %s keeps the reset owed, so the same consumer resets again', async (api) => {
    await open()
    const { consumer } = await rec.start()
    const held = rec.rig.hold(api)
    await rec.recreate()
    await until(() => held.hits === 1, 'the held read')

    const stopping = consumer.stop()
    held.release()
    await stopping
    expect(rec.rig.frames('OffsetCommit')).toHaveLength(0)

    const again = []
    const resets = rec.rig.frames('ListOffsets').length
    await rec.rerun(consumer, again)
    await until(() => again.length >= 3, 'the replacement deliveries')
    expect(again.slice(0, 3)).toEqual(NEW)
    expect(rec.rig.frames('ListOffsets').length).toBeGreaterThan(resets)
    expect(rec.offset).toBe('5')
  })

  describe('an assignment rebuild while the reset is owed', () => {
    /**
     * @param {object} consumer the running consumer
     * @param {number} joins JoinGroup frames sent before the rebuild
     * @returns {Promise<void>} the eager round after `enforceRebalance` has re-joined
     */
    const rebuilt = async (consumer, joins) => {
      await consumer.enforceRebalance('rebuild')
      await until(() => rec.rig.frames('JoinGroup').length > joins, 'the rebuild join')
    }
    const count = (delivered, value) => delivered.filter((seen) => seen === value).length

    test.each([
      ['ListOffsets', 'a held ListOffsets'],
      ['OffsetFetch', 'a held OffsetFetch then ListOffsets'],
    ])(
      '%s: the rebuilt manager resets again and never resumes the old incarnation',
      async (api) => {
        await open()
        const { consumer, delivered } = await rec.start()
        const held = rec.rig.hold(api)
        await rec.recreate()
        await until(() => held.hits === 1, 'the held read')
        const joins = rec.rig.frames('JoinGroup').length

        const rebuilding = rebuilt(consumer, joins)
        held.release()
        await rebuilding
        await until(() => NEW.every((value) => delivered.includes(value)), 'the new records')
        expect(count(delivered, 'value-5')).toBe(1)
        expect(rec.rig.frames('ListOffsets').length).toBeGreaterThan(0)
      }
    )

    /** @returns {Promise<{consumer: object, delivered: string[], joins: number}>} the new incarnation consumed */
    const consumedNew = async () => {
      await open()
      const started = await rec.start()
      await rec.recreate()
      await until(() => NEW.every((value) => started.delivered.includes(value)), 'the new records')
      return { ...started, joins: rec.rig.frames('JoinGroup').length }
    }

    test('a failed pre-release commit keeps the debt, so the rebuilt manager resets again', async () => {
      const { consumer, delivered, joins } = await consumedNew()
      rec.failWith = 15
      await rebuilt(consumer, joins)
      await until(() => count(delivered, 'value-0') === 2, 'the repeated reset')
      expect(count(delivered, 'value-5')).toBe(1)
      expect(rec.offset).toBe('5')
    })

    test('a confirmed pre-release commit settles the debt, so the rebuilt manager resumes after it', async () => {
      const { consumer, delivered, joins } = await consumedNew()
      const resets = rec.rig.frames('ListOffsets').length
      await rebuilt(consumer, joins)
      await pause(250)
      expect(rec.offset).toBe('3')
      expect(count(delivered, 'value-0')).toBe(1)
      expect(rec.rig.frames('ListOffsets')).toHaveLength(resets)
    })
  })

  describe('a reset already resolved before the stop', () => {
    /** @returns {Promise<{consumer: object, delivered: string[]}>} a consumer that consumed the new incarnation */
    const consumedNew = async () => {
      await open()
      const started = await rec.start()
      await rec.recreate()
      await until(() => NEW.every((value) => started.delivered.includes(value)), 'the new records')
      return started
    }

    test('a failed final commit leaves the debt, so the same consumer resets again', async () => {
      const { consumer } = await consumedNew()
      rec.failWith = 15
      await consumer.stop()
      expect(rec.rig.frames('OffsetCommit')).toHaveLength(1)
      expect(rec.offset).toBe('5')

      const again = []
      await rec.rerun(consumer, again)
      await until(() => again.length >= 3, 'the replacement deliveries')
      expect(again.slice(0, 3)).toEqual(NEW)
    })

    test('a confirmed final commit settles the debt, so the same consumer resumes after it', async () => {
      const { consumer } = await consumedNew()
      const resets = rec.rig.frames('ListOffsets').length
      await consumer.stop()
      expect(rec.offset).toBe('3')

      const again = []
      await rec.rerun(consumer, again)
      await pause(200)
      expect(again).toEqual([])
      expect(rec.rig.frames('ListOffsets')).toHaveLength(resets)
    })

    test('a written final commit the deadline left unconfirmed is what a new consumer reads', async () => {
      const { consumer } = await consumedNew()
      const commit = rec.rig.hold('OffsetCommit')
      await consumer.disconnect({ timeout: BOUND_MS })
      expect(commit.hits).toBe(1)

      const fetches = rec.rig.frames('Fetch').length
      const next = await rec.rig.started({ delivered: 0 })
      await until(() => rec.rig.frames('Fetch').length > fetches, 'the first fetch')
      const [first] = rec.rig.frames('Fetch').slice(fetches)
      expect(String(first.fields.topics[0].partitions[0].fetchOffset)).toBe('3')
      expect(rec.offset).toBe('3')
      expect(next.delivered).toEqual([])
    })
  })
})

describe.each([true, false])(
  'a halted partition across a close, with enforcement %p',
  (enforce) => {
    /** @type {CloseRig} */
    let rig
    afterEach(async () => {
      await rig.dispose()
    })

    /**
     * A one-partition log of the valid record 0 and a corrupt batch at 1, so the unreadable boundary
     * is 1, with a consumer whose cadence defers every batch-end commit.
     * @returns {Promise<{ consumer: object, halts: object[], lookup: object }>} the halted consumer
     */
    const halted = async () => {
      rig = await CloseRig.open({ enforce, last: -1 })
      const lookup = rig.lookup()
      const prefix = Buffer.concat([await batchAt(0), await batchAt(1, true)])
      rig.cluster.answer('Fetch', async ({ maxWaitMs, topics }) => {
        const [{ partition, fetchOffset }] = topics[0].partitions
        if (String(fetchOffset) === '0')
          return { partitions: [{ partition, errorCode: 0, records: prefix }] }
        await pause(maxWaitMs)
        return { partitions: [{ partition, errorCode: 0, records: false }] }
      })
      const consumer = rig.consumer()
      const halts = []
      consumer.on(consumer.events.PARTITION_HALTED, ({ payload }) => halts.push(payload))
      await consumer.connect()
      await consumer.subscribe({ topic: TOPIC, fromBeginning: true })
      await consumer.run({
        autoCommit: true,
        autoCommitInterval: 60000,
        eachMessage: async () => {},
      })
      await until(() => halts.length === 1, 'the halt')
      return { consumer, halts, lookup }
    }
    /** @param {object[]} frames OffsetCommit frames @returns {number[]} every offset they carry */
    const offsetsOf = (frames) =>
      frames.flatMap(({ fields }) =>
        fields.topics.flatMap(({ partitions }) => partitions.map(({ offset }) => Number(offset)))
      )

    test('a final commit unsent at the close deadline leaves the episode as it was', async () => {
      const { consumer, halts, lookup } = await halted()
      lookup.makeStale()
      const held = lookup.hold()
      await consumer.disconnect({ timeout: BOUND_MS })
      held.release()
      await pause(150)
      expect(rig.frames('OffsetCommit')).toHaveLength(0)
      expect(halts).toHaveLength(1)
    })

    test('a final commit written before the close deadline never passes the boundary and adds no episode', async () => {
      const { consumer, halts } = await halted()
      const commit = rig.hold('OffsetCommit')
      await consumer.disconnect({ timeout: BOUND_MS })
      expect(commit.hits).toBe(1)
      commit.release()
      await pause(150)
      expect(offsetsOf(rig.frames('OffsetCommit'))).toEqual([1])
      expect(halts).toHaveLength(1)
    })

    test('a replacement consumer starts from the confirmed boundary without a reset', async () => {
      const { consumer } = await halted()
      await consumer.disconnect({ timeout: 5000 })
      const [boundary] = offsetsOf(rig.frames('OffsetCommit'))
      expect(boundary).toBe(1)
      const original = rig.cluster.answerers.get('OffsetFetch')
      rig.cluster.answer('OffsetFetch', (fields, record) => {
        const answer = original(fields, record)
        for (const entry of answer.topics)
          for (const cell of entry.partitions) cell.offset = String(boundary)
        return answer
      })
      const fetches = rig.frames('Fetch').length
      const resets = rig.frames('ListOffsets').length

      const next = rig.consumer()
      await next.connect()
      await next.subscribe({ topic: TOPIC, fromBeginning: true })
      await next.run({ autoCommit: true, autoCommitInterval: 60000, eachMessage: async () => {} })
      await until(() => rig.frames('Fetch').length > fetches, 'the first fetch')
      const [first] = rig.frames('Fetch').slice(fetches)
      expect(String(first.fields.topics[0].partitions[0].fetchOffset)).toBe('1')
      expect(rig.frames('ListOffsets')).toHaveLength(resets)
    })

    test('an explicit commit above the boundary keeps the explicit-offset rule apart from automatic progress', async () => {
      const { consumer } = await halted()
      await consumer.commitOffsets([{ topic: TOPIC, partition: 0, offset: '9' }])
      expect(offsetsOf(rig.frames('OffsetCommit'))).toEqual([9])
      await consumer.disconnect({ timeout: 5000 })
      const automatic = offsetsOf(rig.frames('OffsetCommit').slice(1))
      expect(automatic.every((offset) => offset <= 1)).toBe(true)
    })
  }
)

describe.each([true, false])(
  'a none-policy partition across a close, with enforcement %p',
  (enforce) => {
    /** @type {CloseRig} */
    let rig
    afterEach(async () => {
      await rig.dispose()
    })

    test('the final commit names only the healthy sibling and no reset is requested for the none partition', async () => {
      rig = await CloseRig.open({
        enforce,
        last: -1,
        partitions: 2,
        committed: { 0: '-1', 1: '4' },
        log: { '1|4': await batchAt(4) },
      })
      const consumer = rig.consumer()
      const halts = []
      const delivered = []
      consumer.on(consumer.events.PARTITION_HALTED, ({ payload }) => halts.push(payload))
      await consumer.connect()
      await consumer.subscribe({ topics: [TOPIC], offsetReset: 'none' })
      await consumer.run({
        autoCommit: true,
        autoCommitInterval: 60000,
        eachMessage: async ({ partition, message }) => {
          delivered.push([partition, message.offset])
        },
      })
      await until(() => halts.length === 1 && delivered.length === 1, 'the halt and the sibling')

      await consumer.disconnect({ timeout: 5000 })

      const sent = rig
        .frames('OffsetCommit')
        .flatMap(({ fields }) => fields.topics.flatMap(({ partitions }) => partitions))
      expect(sent).toEqual([expect.objectContaining({ partition: 1, offset: '5' })])
      const listed = rig
        .frames('ListOffsets')
        .flatMap(({ fields }) => fields.topics.flatMap(({ partitions }) => partitions))
      expect(listed.filter(({ partitionIndex }) => partitionIndex === 0)).toEqual([])
      expect(halts).toHaveLength(1)
    })
  }
)

describe.each([true, false])('a consumer-protocol close with enforcement %p', (enforce) => {
  /** @type {CloseRig} */
  let rig
  afterEach(async () => {
    await rig.dispose()
  })
  const open = async () => {
    rig = await CloseRig.open({ protocol: 'consumer', enforce })
  }
  /** @returns {object[]} the heartbeats that left the group (member epoch below zero) */
  const leaves = () =>
    rig.frames('ConsumerGroupHeartbeat').filter(({ fields }) => fields.memberEpoch < 0)

  test('the one final attempt commits once, before a leave that names no partition', async () => {
    await open()
    const { consumer, events } = await rig.started()
    await consumer.disconnect()
    expect(rig.frames('OffsetCommit')).toHaveLength(1)
    expect(leaves()).toHaveLength(1)
    const order = rig.cluster.requests().map(({ api }) => api)
    expect(order.lastIndexOf('OffsetCommit')).toBeLessThan(
      order.lastIndexOf('ConsumerGroupHeartbeat')
    )
    expect(events).toEqual(['STOP', 'DISCONNECT'])
  })

  test('a final commit held at its lookup is unsent after the deadline and the leave is not written', async () => {
    await open()
    const lookup = rig.lookup()
    const { consumer } = await rig.started()
    lookup.makeStale()
    const held = lookup.hold()
    await consumer.disconnect({ timeout: BOUND_MS })
    held.release()
    await pause(150)
    expect(rig.frames('OffsetCommit')).toHaveLength(0)
    expect(leaves()).toHaveLength(0)
  })

  test('a written final commit is published nowhere after the deadline', async () => {
    await open()
    const { consumer } = await rig.started()
    const committed = []
    consumer.on(consumer.events.COMMIT_OFFSETS, ({ payload }) => committed.push(payload))
    const commit = rig.hold('OffsetCommit')
    await consumer.disconnect({ timeout: BOUND_MS })
    expect(commit.hits).toBe(1)
    commit.release()
    await pause(150)
    expect(committed).toEqual([])
    expect(leaves()).toHaveLength(0)
  })

  test('a replacement consumer after the forced close starts and consumes', async () => {
    await open()
    const { consumer } = await rig.started()
    rig.hold('OffsetCommit')
    await consumer.disconnect({ timeout: BOUND_MS })
    const next = await rig.started({ delivered: 3 })
    expect(next.delivered).toHaveLength(3)
  })
})
