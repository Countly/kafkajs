const {
  acquire,
  acquired,
  deferred,
  fetchAnswer,
  flush,
  part,
  recordsHex,
  shareClient,
  until,
} = require('../../../../testHelpers/shareHarness')

describe('public share token contract', () => {
  let h
  afterEach(async () => {
    await h?.close()
    h = undefined
  })
  /** @param {object} [options] `shareClient` options. @returns {Promise<object>} A subscribed client. */
  const start = async (options) => {
    h = shareClient(options)
    h.seen = { acks: [], joins: [], beats: [], crashes: [] }
    const { events, on } = h.consumer
    on(events.ACKNOWLEDGE, ({ payload }) => h.seen.acks.push(payload))
    on(events.GROUP_JOIN, ({ payload }) => h.seen.joins.push(payload))
    on(events.HEARTBEAT, ({ payload }) => h.seen.beats.push(payload))
    on(events.CRASH, ({ payload }) => h.seen.crashes.push(payload))
    await h.consumer.subscribe({ topics: ['topic-a'] })
    return h
  }
  /** @returns {object[]} Every acknowledgement batch the fetches and acknowledgements carried. */
  const sent = () =>
    h
      .frames(78, 79)
      .flatMap(({ fields }) =>
        fields.topics.flatMap((t) => t.partitions.flatMap((p) => p.acknowledgementBatches))
      )

  test('an old callback throws while a replacement acquisition of the same offset works', async () => {
    await start()
    h.script('ShareFetch', [
      fetchAnswer([acquire(0, 0, ['a'])]),
      fetchAnswer([acquire(0, 0, ['a'], { acquire: [acquired(0, 0, 2)] })]),
    ])
    const calls = []
    await h.consumer.run({
      eachMessage: async ({ message, acknowledge }) => {
        const attempt = { acknowledge, count: message.deliveryCount, error: null }
        if (calls.length === 1) {
          try {
            calls[0].acknowledge('reject')
          } catch (error) {
            attempt.error = error
          }
          acknowledge('reject')
        }
        calls.push(attempt)
      },
    })
    await until(() => calls.length === 2, 'both deliveries')
    expect(calls.map(({ count }) => count)).toEqual([1, 2])
    expect(calls[1].error).toMatchObject({ name: 'KafkaJSNonRetriableError' })
    await until(() => sent().length === 2, 'both acknowledgements')
    expect(sent()).toEqual([
      { firstOffset: '0', lastOffset: '0', acknowledgeTypes: [1] },
      { firstOffset: '0', lastOffset: '0', acknowledgeTypes: [3] },
    ])
  })

  test('keeps a running handler out of the next membership after a fence', async () => {
    await start()
    h.script('ShareFetch', [fetchAnswer([acquire(0, 0, ['a'])])])
    h.script('ShareGroupHeartbeat', [undefined, { errorCode: 25 }])
    const gate = deferred()
    const saved = []
    await h.consumer.run({
      eachMessage: async ({ acknowledge }) => {
        saved.push(acknowledge)
        await gate.promise
      },
    })
    await until(() => saved.length === 1, 'the running handler')
    await until(() => h.seen.joins.length === 2, 'the rejoin after the fence')
    expect(() => saved[0]('reject')).toThrow('no longer current')
    gate.resolve()
    await flush()
    expect(sent().filter((batch) => batch.acknowledgeTypes.includes(1))).toHaveLength(0)
    const openings = h.frames(78).filter(({ fields }) => fields.shareSessionEpoch === 0)
    expect(openings.length).toBeGreaterThanOrEqual(2)
    expect(h.seen.crashes).toHaveLength(0)
  })

  test('publishes no heartbeat for a stale answer and one group join per membership', async () => {
    await start()
    const stale = { memberEpoch: 0, heartbeatIntervalMs: 50, assignment: null }
    h.script('ShareGroupHeartbeat', [undefined, undefined, stale, undefined])
    await h.consumer.run({ eachMessage: async () => {} })
    await until(() => h.frames(76).length >= 6, 'six heartbeats')
    await flush()
    expect(h.seen.joins).toHaveLength(1)
    expect(h.seen.beats.length).toBeGreaterThanOrEqual(3)
    expect(h.seen.beats.every(({ memberEpoch }) => memberEpoch === 1)).toBe(true)
    expect(h.seen.beats[0]).toEqual({
      groupId: 'group',
      memberId: expect.any(String),
      memberEpoch: 1,
      heartbeatIntervalMs: 50,
    })
  })

  test('separates two answers that repeat an offset and a send time', async () => {
    await start()
    const records = recordsHex(0, ['a'])
    const answer = (count) =>
      fetchAnswer([part({ partition: 0, acquire: [acquired(0, 0, count)], records })])
    h.script('ShareFetch', [answer(1), answer(2)])
    const counts = []
    await h.consumer.run({
      eachMessage: async ({ message }) => {
        counts.push(message.deliveryCount)
      },
    })
    await until(() => counts.length === 2, 'both deliveries')
    expect(counts).toEqual([1, 2])
    await until(() => h.seen.acks.length === 2, 'both settlements')
    expect(h.seen.acks.map(({ type, error }) => [type, error])).toEqual([
      ['accept', null],
      ['accept', null],
    ])
  })
})
