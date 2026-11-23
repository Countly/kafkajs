const diagnostics = require('node:diagnostics_channel')
const channels = require('../../../instrumentation/channels')
const {
  A,
  B,
  acquire,
  acquired,
  ackAnswer,
  deferred,
  echo,
  fetchAnswer,
  flush,
  part,
  recordsHex,
  shareClient,
  until,
  ShareClock,
} = require('../../../../testHelpers/shareHarness')
const MetadataCache = require('../../../cluster/metadataCache')

const C = '20112233-4455-6677-8899-aabbccddeeff'
const LEADER = { leader: 0, epoch: 1 }

/**
 * @param {Array<[string, string]>} ids Topic name and id pairs; a name may be `__proto__`.
 * @returns {Record<string, {topicId: string, partitions: object[]}>} Metadata for one partition each,
 *   built with own properties so a `__proto__` name stays an ordinary key.
 */
const topicsOf = (ids) =>
  Object.fromEntries(ids.map(([name, topicId]) => [name, { topicId, partitions: [LEADER] }]))

/** @param {object[]} acks Acknowledge payloads. @returns {Array<Array>} Range, type and error reason of each. */
const outcomes = (acks) =>
  acks.map(({ firstOffset, lastOffset, type, error }) => [
    firstOffset,
    lastOffset,
    type,
    error ? error.reason : null,
  ])

describe('public share subscription and run lifecycle', () => {
  let h
  let extra
  /** Releases of held application handlers, run in cleanup even after a failed assertion. */
  const releases = []
  afterEach(async () => {
    for (const release of releases.splice(0)) release()
    await extra?.disconnect({ timeout: 0 })
    extra = undefined
    await h?.close()
    h = undefined
  })
  /** @returns {object[]} The join frames (ShareGroupHeartbeat at epoch 0). */
  const joins = () => h.frames(76).filter(({ fields }) => fields.memberEpoch === 0)

  test('replaces subscription snapshots without exposing stored state', async () => {
    h = shareClient({
      topics: topicsOf([
        ['topic-a', A],
        ['topic-b', B],
        ['topic-c', C],
      ]),
    })
    const { consumer } = h
    expect(consumer.subscription()).toEqual([])
    const input = ['topic-a', 'topic-b']
    await consumer.subscribe({ topics: input })
    const first = consumer.subscription()
    expect(first).toEqual(['topic-a', 'topic-b'])
    first.push('mutated')
    input.push('late')
    input[0] = 'changed'
    expect(consumer.subscription()).toEqual(['topic-a', 'topic-b'])
    expect(consumer.subscription()).not.toBe(consumer.subscription())
    await consumer.run({ eachMessage: async () => {} })
    await consumer.stop()
    expect(consumer.subscription()).toEqual(['topic-a', 'topic-b'])

    const addition = h.hold({
      api: 'Metadata',
      match: ({ fields }) => fields.topics?.some((topic) => topic.name === 'topic-c'),
    })
    const replacement = ['topic-c']
    const pending = consumer.subscribe({ topics: replacement })
    await addition.arrived()
    replacement[0] = 'mutated'
    replacement.push('topic-d')
    expect(consumer.subscription()).toEqual(['topic-a', 'topic-b'])
    addition.release()
    await pending
    expect(consumer.subscription()).toEqual(['topic-c'])
    const requested = h.cluster
      .requests()
      .filter(({ api }) => api === 'Metadata')
      .flatMap(({ fields }) => (fields.topics ?? []).map((topic) => topic.name))
      .filter((name) => name)
    expect([...new Set(requested)].sort()).toEqual(['topic-a', 'topic-b', 'topic-c'])

    await consumer.run({ eachMessage: async () => {} })
    expect(joins().map(({ fields }) => fields.subscribedTopicNames)).toEqual([
      ['topic-a', 'topic-b'],
      ['topic-c'],
    ])
  })

  test('subscribes to names that are also object prototype keys', async () => {
    h = shareClient({
      topics: topicsOf([
        ['__proto__', A],
        ['constructor', B],
        ['topic-a', C],
      ]),
    })
    const { consumer } = h
    await consumer.subscribe({ topics: ['__proto__', 'constructor', 'topic-a'] })
    expect(consumer.subscription()).toEqual(['__proto__', 'constructor', 'topic-a'])
    await consumer.run({ eachMessage: async () => {} })
    expect(joins().map(({ fields }) => fields.subscribedTopicNames)).toEqual([
      ['__proto__', 'constructor', 'topic-a'],
    ])
    expect(
      consumer
        .assignment()
        .map(({ topic, partitions }) => [topic, partitions])
        .sort()
    ).toEqual([
      ['__proto__', [0]],
      ['constructor', [0]],
      ['topic-a', [0]],
    ])
    await until(() => h.frames(78).length > 0, 'the opening fetch')
    expect(
      h
        .frames(78)[0]
        .fields.topics.map(({ topicId }) => topicId)
        .sort()
    ).toEqual([A, B, C])
    expect(Object.keys(Object.prototype)).toEqual([])
    expect('partitions' in {}).toBe(false)
  })

  test('requires exactly one function and a subscription, and one run at a time', async () => {
    h = shareClient()
    const { consumer } = h
    const handler = async () => {}
    const frames = () => h.frames(76, 78, 79).length
    await expect(consumer.run({ eachMessage: handler })).rejects.toThrow(
      'subscribe() must be called before run()'
    )
    await consumer.subscribe({ topics: ['topic-a'] })
    const requests = h.cluster.requests().length
    const timer = jest.spyOn(global, 'setTimeout')
    for (const config of [
      undefined,
      {},
      { eachMessage: handler, eachBatch: handler },
      { eachMessage: 'handler' },
      { eachBatch: null },
    ]) {
      await expect(consumer.run(config)).rejects.toThrow(
        'run requires exactly one of eachMessage or eachBatch'
      )
    }
    expect(timer).not.toHaveBeenCalled()
    timer.mockRestore()
    expect(h.cluster.requests()).toHaveLength(requests)
    expect(frames()).toBe(0)

    const held = deferred()
    const started = deferred()
    h.script('ShareFetch', [fetchAnswer([acquire(0, 0, ['a'])])])
    await consumer.run({
      eachMessage: async () => {
        started.resolve()
        await held.promise
      },
    })
    await started.promise
    const message = 'run() cannot start while another run, close or subscribe is active'
    await expect(consumer.run({ eachMessage: handler })).rejects.toThrow(message)
    const stopping = consumer.stop()
    await expect(consumer.run({ eachMessage: handler })).rejects.toThrow(message)
    held.resolve()
    await stopping
    await consumer.run({ eachMessage: handler })
    expect(joins()).toHaveLength(2)
    expect(joins()[1].fields.memberId).not.toBe(joins()[0].fields.memberId)
  })

  test('starts lock duration and assignment snapshots empty and replaces them per run', async () => {
    h = shareClient()
    const { consumer } = h
    expect(consumer.assignment()).toEqual([])
    expect(consumer.acquisitionLockTimeoutMs()).toBeNull()
    await consumer.subscribe({ topics: ['topic-a'] })
    h.script('ShareFetch', [
      fetchAnswer([acquire(0, 0, ['a'])], { acquisitionLockTimeoutMs: 1000 }),
    ])
    h.script('ShareAcknowledge', [
      ackAnswer([{ partition: 0 }], { acquisitionLockTimeoutMs: 2000 }),
    ])
    const afterRenewal = h.hold({
      api: 'ShareFetch',
      match: ({ fields }) => fields.shareSessionEpoch === 2,
    })
    const durations = []
    await consumer.run({
      eachMessage: async ({ renew }) => {
        durations.push(consumer.acquisitionLockTimeoutMs(), await renew())
      },
    })
    await until(() => durations.length === 2, 'the renewal')
    await until(() => afterRenewal.call !== null, 'the fetch after the renewal')
    expect(durations).toEqual([1000, 2000])
    expect(consumer.acquisitionLockTimeoutMs()).toBe(2000)
    const snapshot = consumer.assignment()
    expect(snapshot).toEqual([{ topic: 'topic-a', partitions: [0] }])
    snapshot[0].partitions.push(9)
    snapshot[0].topic = 'changed'
    snapshot.push({ topic: 'other', partitions: [] })
    expect(consumer.assignment()).toEqual([{ topic: 'topic-a', partitions: [0] }])
    expect(consumer.assignment()[0]).not.toBe(consumer.assignment()[0])

    await consumer.stop()
    expect(consumer.assignment()).toEqual([])
    expect(consumer.acquisitionLockTimeoutMs()).toBe(2000)
    afterRenewal.release()
    await flush()

    h.script('ShareFetch', [
      fetchAnswer([acquire(0, 0, ['b'])], { acquisitionLockTimeoutMs: 3000 }),
    ])
    const fetch = h.hold({ api: 'ShareFetch' })
    h.hold({ api: 'ShareFetch', match: ({ fields }) => fields.shareSessionEpoch === 1 })
    await consumer.run({ eachMessage: async () => {} })
    await fetch.arrived()
    expect(consumer.acquisitionLockTimeoutMs()).toBeNull()
    expect(consumer.assignment()).toEqual([{ topic: 'topic-a', partitions: [0] }])
    fetch.release()
    await until(() => consumer.acquisitionLockTimeoutMs() === 3000, 'the new run duration')
  })

  test('removes one client listener without touching another client or the channel table', async () => {
    h = shareClient()
    extra = h.client.shareConsumer({ groupId: 'second' })
    const tableBefore = Object.keys(channels.shareConsumer)
    const channel = diagnostics.channel('kafkajs.share_consumer.connect')
    const subscribersBefore = channel.hasSubscribers
    const removed = []
    const kept = []
    const remove = h.consumer.on(h.consumer.events.CONNECT, (event) => removed.push(event.type))
    extra.on(extra.events.CONNECT, (event) => kept.push(event.type))
    remove()
    remove()
    await Promise.all([h.consumer.connect(), extra.connect()])
    expect(removed).toEqual([])
    expect(kept).toEqual(['share_consumer.connect'])
    expect(Object.keys(channels.shareConsumer)).toEqual(tableBefore)
    expect(channel.hasSubscribers).toBe(subscribersBefore)
    expect(() => h.consumer.on('consumer.connect', () => {})).toThrow('Event name should be one of')
  })

  test('retains only current topic identities after assignment changes', async () => {
    h = shareClient({ interval: 5 })
    const { consumer } = h
    const assignments = []
    const acks = []
    consumer.on(consumer.events.ASSIGNMENT_CHANGED, ({ payload }) => assignments.push(payload))
    consumer.on(consumer.events.ACKNOWLEDGE, ({ payload }) => acks.push(payload))
    await consumer.subscribe({ topics: ['topic-a'] })
    h.script('ShareFetch', [
      fetchAnswer([acquire(0, 0, ['a0', 'a1', 'a2'])]),
      fetchAnswer([
        part({
          topicId: B,
          partition: 0,
          acquire: [acquired(10, 10)],
          records: recordsHex(10, ['b0']),
        }),
      ]),
    ])
    const held = deferred()
    releases.push(held.resolve)
    const handled = []
    await consumer.run({
      eachMessage: async ({ topic, message }) => {
        handled.push(`${topic}:${message.value.toString()}`)
        if (message.value.toString() === 'a0') await held.promise
      },
    })
    await until(() => handled.length === 1, 'the first handler')

    h.table.set('topic-a', { topicId: B, partitions: [LEADER] })
    const resolution = h.hold({
      api: 'Metadata',
      match: ({ fields }) => fields.topics?.some((topic) => topic.topicId === B),
    })
    const beat = h.hold({
      api: 'ShareGroupHeartbeat',
      match: ({ fields }) => fields.memberEpoch > 0,
    })
    const call = await beat.arrived()
    beat.release({
      errorCode: 0,
      errorMessage: null,
      memberId: call.fields.memberId,
      memberEpoch: 2,
      heartbeatIntervalMs: 5,
      assignment: { topicPartitions: [{ topicId: B, partitions: [0] }] },
    })
    await resolution.arrived()
    await until(() => assignments.length === 2, 'the revocation of the old identity')
    expect(assignments[1]).toMatchObject({
      memberEpoch: 2,
      assignment: [],
      added: [],
      removed: [{ topic: 'topic-a', partitions: [0] }],
    })
    expect(consumer.assignment()).toEqual([])
    await flush()
    expect(
      h.frames(78).every(({ fields }) => fields.topics.every(({ topicId }) => topicId !== B))
    ).toBe(true)
    expect(handled).toEqual(['topic-a:a0'])

    resolution.release()
    await until(() => assignments.length === 3, 'the publication of the new identity')
    expect(assignments[2]).toMatchObject({
      memberEpoch: 2,
      assignment: [{ topic: 'topic-a', partitions: [0] }],
      added: [{ topic: 'topic-a', partitions: [0] }],
      removed: [],
    })
    await flush()
    expect(handled).toEqual(['topic-a:a0'])
    expect(
      h.frames(78).every(({ fields }) => fields.topics.every(({ topicId }) => topicId !== B))
    ).toBe(true)

    held.resolve()
    await until(() => handled.length === 2, 'the handler of the new identity')
    expect(
      h.frames(78).some(({ fields }) => fields.topics.some(({ topicId }) => topicId === B))
    ).toBe(true)
    const finals = h.frames(79).filter(({ fields }) => fields.shareSessionEpoch === -1)
    const openings = h.frames(78).filter(({ fields }) => fields.shareSessionEpoch === 0)
    expect(finals).toHaveLength(1)
    expect(openings).toHaveLength(2)
    expect(openings[1].order).toBeGreaterThan(finals[0].answeredOrder)
    await until(() => acks.length === 3, 'the settled acknowledgements')
    expect(handled).toEqual(['topic-a:a0', 'topic-a:b0'])
    expect(outcomes(acks).sort((x, y) => Number(x[0]) - Number(y[0]))).toEqual([
      ['0', '0', 'accept', null],
      ['1', '2', 'release', null],
      ['10', '10', 'accept', null],
    ])
  })

  let assignments
  /** The fake clock of the tests that install one; null while real timers run. */
  let clock = null
  /**
   * Waits for a stage: on real timers as `until` does, on the fake clock by moving it 5 ms at a
   * time (one heartbeat interval) until the stage is reached, at most two fake seconds.
   * @param {() => boolean} predicate The reached-stage condition.
   * @param {string} what What is awaited, named by the failure.
   * @returns {Promise<void>}
   * @throws {Error} When the stage is not reached.
   */
  const reach = async (predicate, what) => {
    if (!clock) return until(predicate, what)
    for (let step = 0; step < 400 && !predicate(); step++) await clock.advance(5)
    if (!predicate()) throw new Error(`${what} did not happen`)
  }
  /**
   * Starts a subscribed client that records its assignment changes.
   * @param {object} [topics] The metadata the wire serves, by topic name.
   * @param {object} [options] Client options.
   * @param {number} [options.nodes=1] Node count.
   * @param {string[]} [options.subscribe=['topic-a']] The subscribed names.
   * @returns {Promise<void>}
   */
  const start = async (topics, { nodes = 1, subscribe = ['topic-a'] } = {}) => {
    h = shareClient({ interval: 5, nodes, ...(topics && { topics }) })
    assignments = []
    const { consumer } = h
    consumer.on(consumer.events.ASSIGNMENT_CHANGED, ({ payload }) => assignments.push(payload))
    await consumer.subscribe({ topics: subscribe })
  }
  /**
   * Answers the next steady heartbeat with a new assignment, then waits until it was applied.
   * @param {number} memberEpoch The answer's member epoch.
   * @param {Array<{topicId: string, partitions: number[]}>} topicPartitions The assignment.
   * @returns {Promise<void>}
   */
  const assignNext = async (memberEpoch, topicPartitions) => {
    const count = assignments.length
    const beat = h.hold({
      api: 'ShareGroupHeartbeat',
      match: ({ fields }) => fields.memberEpoch > 0,
    })
    await reach(() => beat.call !== null, `the heartbeat for epoch ${memberEpoch}`)
    beat.release({
      errorCode: 0,
      errorMessage: null,
      memberId: beat.call.fields.memberId,
      memberEpoch,
      heartbeatIntervalMs: 5,
      assignment: { topicPartitions },
    })
    await reach(() => assignments.length === count + 1, `the assignment at epoch ${memberEpoch}`)
  }

  describe('a node whose last partition is revoked and restored', () => {
    /** @returns {number[]} The session epoch of every fetch, oldest first. */
    const fetchEpochs = () => h.frames(78).map(({ fields }) => fields.shareSessionEpoch)
    /** @returns {object[]} The final acknowledgements (epoch -1), oldest first. */
    const finals = () => h.frames(79).filter(({ fields }) => fields.shareSessionEpoch === -1)

    test('runs no second handler and opens no second session while its handler runs', async () => {
      await start()
      h.script('ShareFetch', [
        fetchAnswer([acquire(0, 0, ['a0'])]),
        fetchAnswer([acquire(0, 5, ['a5'])]),
      ])
      const held = deferred()
      releases.push(held.resolve)
      const handled = []
      await h.consumer.run({
        eachMessage: async ({ message }) => {
          handled.push(message.offset)
          if (message.offset === '0') await held.promise
        },
      })
      await until(() => handled.length === 1, 'the held handler')
      await assignNext(2, [])
      await assignNext(3, [{ topicId: A, partitions: [0] }])
      await flush()
      expect(fetchEpochs()).toEqual([0])
      expect(handled).toEqual(['0'])

      held.resolve()
      await until(() => handled.length === 2, 'the delivery of the new session')
      expect(handled).toEqual(['0', '5'])
      expect(fetchEpochs().slice(0, 2)).toEqual([0, 0])
      expect(finals()).toHaveLength(1)
      expect(finals()[0].fields.topics).toEqual([
        {
          topicId: A,
          partitions: [
            {
              partitionIndex: 0,
              acknowledgementBatches: [
                { firstOffset: '0', lastOffset: '0', acknowledgeTypes: [1] },
              ],
            },
          ],
        },
      ])
      expect(h.frames(78)[1].order).toBeGreaterThan(finals()[0].answeredOrder)
    })

    test('opens no second session while its fetch is in flight', async () => {
      await start()
      const fetch = h.hold({
        api: 'ShareFetch',
        match: ({ fields }) => fields.shareSessionEpoch === 1,
      })
      await h.consumer.run({ eachMessage: async () => {} })
      await fetch.arrived()
      await assignNext(2, [])
      await assignNext(3, [{ topicId: A, partitions: [0] }])
      await flush()
      expect(fetchEpochs()).toEqual([0, 1])

      fetch.release(fetchAnswer([]))
      await until(() => h.frames(78).length === 3, 'the opening fetch of the new session')
      expect(fetchEpochs().slice(0, 3)).toEqual([0, 1, 0])
      expect(finals()).toHaveLength(1)
      expect(h.frames(78)[2].order).toBeGreaterThan(finals()[0].answeredOrder)
    })

    test('opens a new session only after the final of the ended one was answered', async () => {
      await start()
      await h.consumer.run({ eachMessage: async () => {} })
      await until(() => h.frames(78).length > 0, 'the opening fetch')
      const final = h.hold({
        api: 'ShareAcknowledge',
        match: ({ fields }) => fields.shareSessionEpoch === -1,
      })
      await assignNext(2, [])
      await final.arrived()
      const before = h.frames(78).length
      await assignNext(3, [{ topicId: A, partitions: [0] }])
      await flush()
      expect(h.frames(78)).toHaveLength(before)

      final.release(ackAnswer([]))
      await until(() => h.frames(78).length === before + 1, 'the opening fetch of the new session')
      const [closing] = h.frames(79)
      const opening = h.frames(78)[before]
      expect(h.frames(79)).toHaveLength(1)
      expect(closing.fields.shareSessionEpoch).toBe(-1)
      expect(opening.fields.shareSessionEpoch).toBe(0)
      expect(opening.fields.topics).toEqual([
        { topicId: A, partitions: [{ partitionIndex: 0, acknowledgementBatches: [] }] },
      ])
      expect(opening.order).toBeGreaterThan(closing.answeredOrder)
    })
  })

  describe('an assigned partition without a cached leader', () => {
    /** Releases of the application handlers a test holds; cleanup runs them even after a failure. */
    const held = []
    beforeEach(() => {
      clock = new ShareClock().install()
    })
    afterEach(async () => {
      for (const release of held.splice(0)) release()
      try {
        await h?.close()
      } finally {
        h = undefined
        jest.restoreAllMocks()
        clock.restore()
        clock = null
      }
    })
    /** @returns {{promise: Promise<void>, resolve: () => void}} A handler gate the cleanup releases. */
    const hold = () => {
      const gate = deferred()
      held.push(gate.resolve)
      return gate
    }
    /**
     * Moves the fake clock `ms` in 5 ms steps, so every heartbeat on the way is written and answered.
     * @param {number} ms Fake milliseconds.
     * @returns {Promise<void>}
     */
    const pass = async (ms) => {
      for (let moved = 0; moved < ms; moved += 5) await clock.advance(5)
    }
    /** @returns {number} The Metadata requests written so far. */
    const metadataCount = () => h.cluster.requests().filter(({ api }) => api === 'Metadata').length
    /**
     * @param {{fields: object}} call A Metadata request.
     * @returns {boolean} Whether it asks for topics by id only, as the route refresh does.
     */
    const byId = ({ fields }) =>
      (fields.topics ?? []).length > 0 && fields.topics.every(({ name }) => !name)
    /**
     * @param {object} fields A ShareFetch request's fields.
     * @param {number} partition A partition of topic A.
     * @returns {boolean} Whether the request lists that partition.
     */
    const lists = (fields, partition) =>
      fields.topics.some(
        ({ topicId, partitions }) =>
          topicId === A && partitions.some(({ partitionIndex }) => partitionIndex === partition)
      )

    test('is fetched once a refresh names the leader of a partition added while running', async () => {
      await start()
      const crashes = []
      h.consumer.on(h.consumer.events.CRASH, ({ payload }) => crashes.push(payload.error))
      let given = false
      h.cluster.answer('ShareFetch', (fields) => {
        if (given || !lists(fields, 1)) return echo('ShareFetch', fields)
        given = true
        return fetchAnswer([acquire(1, 0, ['p1'])])
      })
      const handled = []
      await h.consumer.run({
        eachMessage: async ({ partition, message }) =>
          handled.push(`${partition}:${message.offset}`),
      })
      await reach(() => h.frames(78).length > 0, 'the opening fetch')
      h.table.set('topic-a', { topicId: A, partitions: [LEADER, LEADER] })
      const before = metadataCount()
      await assignNext(2, [{ topicId: A, partitions: [0, 1] }])
      await reach(() => handled.length === 1, 'the delivery of the added partition')
      expect(handled).toEqual(['1:0'])
      expect(metadataCount()).toBeGreaterThan(before)
      const routes = h.cluster.requests().filter((call) => call.api === 'Metadata' && byId(call))
      expect(routes.map(({ fields }) => fields.topics.map(({ topicId }) => topicId))).toEqual([[A]])
      const added = h.frames(78).find(({ fields }) => lists(fields, 1))
      expect(added.fields.shareSessionEpoch).toBeGreaterThan(0)
      expect(h.frames(78).filter(({ fields }) => fields.shareSessionEpoch === 0)).toHaveLength(1)
      expect(crashes).toEqual([])
    })

    test('is fetched again once one refresh at a time, under the backoff, finds a leader for a partition the broker answered LEADER_NOT_AVAILABLE', async () => {
      await start()
      let lose = false
      let restored = false
      h.cluster.answer('ShareFetch', (fields) => {
        if (lose && fields.shareSessionEpoch > 0) {
          lose = false
          return fetchAnswer([part({ partition: 0, code: 6 })])
        }
        if (restored && fields.shareSessionEpoch === 0) {
          restored = false
          return fetchAnswer([acquire(0, 0, ['a'])])
        }
        return echo('ShareFetch', fields)
      })
      const handled = []
      await h.consumer.run({ eachMessage: async ({ message }) => handled.push(message.offset) })
      await reach(() => h.frames(78).length > 0, 'the opening fetch')
      const routes = () =>
        h.cluster.requests().filter((call) => call.api === 'Metadata' && byId(call))
      const first = h.hold({ api: 'Metadata', match: byId })
      h.table.set('topic-a', { topicId: A, partitions: [{ leader: -1, epoch: 2, code: 5 }] })
      lose = true
      await reach(() => first.call !== null, 'the route refresh')
      await pass(300)
      expect(routes()).toHaveLength(1)
      const finals = h.frames(79).filter(({ fields }) => fields.shareSessionEpoch === -1)
      expect(finals).toHaveLength(1)
      const fetches = h.frames(78).length

      const second = h.hold({ api: 'Metadata', match: byId })
      first.release()
      await flush()
      const answeredAt = performance.now()
      await pass(90)
      expect(second.call).toBeNull()
      await reach(() => second.call !== null, 'the retried route refresh')
      expect(performance.now() - answeredAt).toBeGreaterThanOrEqual(100)
      expect(performance.now() - answeredAt).toBeLessThanOrEqual(110)
      expect(h.frames(78)).toHaveLength(fetches)

      h.table.set('topic-a', { topicId: A, partitions: [{ leader: 0, epoch: 3 }] })
      restored = true
      second.release()
      await reach(() => handled.length === 1, 'the delivery once the partition has a leader')
      expect(handled).toEqual(['0'])
      expect(routes()).toHaveLength(2)
      expect(h.frames(78).filter(({ fields }) => fields.shareSessionEpoch === 0)).toHaveLength(2)
    })

    test('is fetched while another subscribed topic was deleted, and the deleted topic is never asked for by name', async () => {
      await start(
        topicsOf([
          ['topic-a', A],
          ['topic-b', B],
        ]),
        { subscribe: ['topic-a', 'topic-b'] }
      )
      let given = false
      h.cluster.answer('ShareFetch', (fields) => {
        const listed = fields.topics.some(
          ({ topicId, partitions }) =>
            topicId === B && partitions.some(({ partitionIndex }) => partitionIndex === 1)
        )
        if (given || !listed) return echo('ShareFetch', fields)
        given = true
        return fetchAnswer([acquire(1, 0, ['b1'], { topicId: B })])
      })
      const handled = []
      await h.consumer.run({
        eachMessage: async ({ topic, partition, message }) =>
          handled.push(`${topic}:${partition}:${message.offset}`),
      })
      await reach(() => h.frames(78).length > 0, 'the opening fetch')
      h.table.delete('topic-a')
      h.table.set('topic-b', { topicId: B, partitions: [LEADER, LEADER] })
      const from = h.cluster.requests().length
      await assignNext(2, [{ topicId: B, partitions: [0, 1] }])
      await reach(() => handled.length === 1, 'the delivery of the added partition')
      expect(handled).toEqual(['topic-b:1:0'])
      const asked = h.cluster
        .requests()
        .slice(from)
        .filter(({ api }) => api === 'Metadata')
      expect(asked.length).toBeGreaterThan(0)
      expect(
        asked.flatMap(({ fields }) => (fields.topics ?? []).map(({ name }) => name))
      ).not.toContain('topic-a')
    })

    test('retires the old node acquisition before another node fetches a partition whose leader was unknown in between', async () => {
      const topics = { 'topic-a': { topicId: A, partitions: [LEADER, { leader: 1, epoch: 1 }] } }
      await start(topics, { nodes: 2 })
      let given = false
      let lose = false
      h.cluster.answer('ShareFetch', (fields, call) => {
        if (call.node === 0 && !given && fields.shareSessionEpoch === 0) {
          given = true
          return fetchAnswer([acquire(0, 0, ['a'])])
        }
        if (call.node === 1 && lose && fields.shareSessionEpoch > 0) {
          lose = false
          return fetchAnswer([part({ partition: 1, code: 6 })])
        }
        return echo('ShareFetch', fields)
      })
      const gate = hold()
      const decisions = []
      let started = false
      await h.consumer.run({
        eachMessage: async ({ partition, acknowledge }) => {
          if (partition !== 0) return
          started = true
          await gate.promise
          try {
            acknowledge('accept')
            decisions.push('accepted')
          } catch (error) {
            decisions.push(error.message)
          }
        },
      })
      await reach(() => started, 'the held handler on node 0')
      /** @returns {boolean} Whether node 1 has fetched partition 0. */
      const movedFetch = () =>
        h.frames(78).some(({ node, fields }) => node === 1 && lists(fields, 0))
      const before = metadataCount()
      h.table.set('topic-a', {
        topicId: A,
        partitions: [
          { leader: -1, epoch: 2, code: 5 },
          { leader: 1, epoch: 1 },
        ],
      })
      lose = true
      await reach(() => !lose && metadataCount() > before, 'the refresh that finds no leader')
      await pass(20)
      expect(movedFetch()).toBe(false)

      h.table.set('topic-a', {
        topicId: A,
        partitions: [
          { leader: 1, epoch: 3 },
          { leader: 1, epoch: 1 },
        ],
      })
      lose = true
      await reach(movedFetch, 'node 1 fetching partition 0')
      gate.resolve()
      await reach(() => decisions.length === 1, 'the decision of the old handler')
      expect(decisions).toEqual(['Handler call is no longer current'])
      /** @returns {object[]} The final acknowledgements of node 0. */
      const finals = () =>
        h.frames(79).filter(({ node, fields }) => node === 0 && fields.shareSessionEpoch === -1)
      await reach(() => finals().length === 1, 'the final of node 0')
      expect(finals()[0].fields.topics).toEqual([])
    })

    test('a stop that waits for a running handler ends the refresh in flight: its late answer installs nothing and no refresh follows', async () => {
      await start()
      h.script('ShareFetch', [fetchAnswer([acquire(0, 0, ['a0'])])])
      const gate = hold()
      const handled = []
      await h.consumer.run({
        eachMessage: async ({ partition, message }) => {
          handled.push(`${partition}:${message.offset}`)
          await gate.promise
        },
      })
      await reach(() => handled.length === 1, 'the held handler')
      h.table.set('topic-a', { topicId: A, partitions: [LEADER, LEADER] })
      const refresh = h.hold({ api: 'Metadata' })
      await assignNext(2, [{ topicId: A, partitions: [0, 1] }])
      await reach(() => refresh.call !== null, 'the route refresh')
      const installs = [
        jest.spyOn(MetadataCache.prototype, 'update'),
        jest.spyOn(MetadataCache.prototype, 'updateTopicsById'),
      ]
      let stopped = false
      const stopping = h.consumer.stop().then(() => {
        stopped = true
      })
      await flush()
      const count = metadataCount()
      refresh.release()
      await pass(500)
      expect(stopped).toBe(false)
      expect(installs.map((spy) => spy.mock.calls.length)).toEqual([0, 0])
      expect(metadataCount()).toBe(count)
      expect(h.frames(78).some(({ fields }) => lists(fields, 1))).toBe(false)

      gate.resolve()
      await reach(() => stopped, 'the stop')
      await stopping
      expect(handled).toEqual(['0:0'])
      expect(h.frames(78).some(({ fields }) => lists(fields, 1))).toBe(false)
    })
  })
})
