const diagnostics = require('node:diagnostics_channel')
const channels = require('../../../instrumentation/channels')
const {
  A,
  B,
  acquire,
  acquired,
  ackAnswer,
  deferred,
  fetchAnswer,
  flush,
  part,
  recordsHex,
  shareClient,
  until,
} = require('../../../../testHelpers/shareHarness')

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
  afterEach(async () => {
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
    await until(() => handled.length === 2, 'the handler of the new identity')
    expect(
      h.frames(78).some(({ fields }) => fields.topics.some(({ topicId }) => topicId === B))
    ).toBe(true)

    held.resolve()
    await until(() => acks.length === 3, 'the settled acknowledgements')
    expect(handled).toEqual(['topic-a:a0', 'topic-a:b0'])
    expect(outcomes(acks).sort((x, y) => Number(x[0]) - Number(y[0]))).toEqual([
      ['0', '0', 'accept', null],
      ['1', '2', 'release', null],
      ['10', '10', 'accept', null],
    ])
  })
})
