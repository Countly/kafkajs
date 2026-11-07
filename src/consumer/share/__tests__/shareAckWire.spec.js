const {
  A,
  acquire,
  acquired,
  fetchAnswer,
  part,
  recordsHex,
  shareClient,
  until,
} = require('../../../../testHelpers/shareHarness')
const { KafkaJSShareAcknowledgementError } = require('../../../errors')

describe('public share range contract', () => {
  let h
  afterEach(async () => {
    await h?.close()
    h = undefined
  })
  /** @param {object} [options] `shareClient` options. @returns {Promise<object>} A subscribed client. */
  const start = async (options) => {
    h = shareClient(options)
    h.acks = []
    h.consumer.on(h.consumer.events.ACKNOWLEDGE, ({ payload }) => h.acks.push(payload))
    await h.consumer.subscribe({ topics: ['topic-a'] })
    return h
  }
  /** @returns {object[]} Every acknowledgement batch the fetches carried, in order. */
  const sent = () =>
    h
      .frames(78)
      .flatMap(({ fields }) =>
        fields.topics.flatMap((t) => t.partitions.flatMap((p) => p.acknowledgementBatches))
      )
  const FIRST = '9007199254740993'
  const LAST = '9007199255740992'

  test('sends and reports a million absent offsets as one range', async () => {
    await start()
    h.script('ShareFetch', [
      fetchAnswer([part({ partition: 0, acquire: [acquired(FIRST, LAST)] })]),
    ])
    await h.consumer.run({ eachMessage: async () => {} })
    await until(() => sent().length > 0, 'the piggybacked gap range')
    expect(sent()).toEqual([{ firstOffset: FIRST, lastOffset: LAST, acknowledgeTypes: [0] }])
    await until(() => h.acks.length === 1, 'the settled gap')
    expect(h.acks).toEqual([
      {
        topic: 'topic-a',
        partition: 0,
        firstOffset: FIRST,
        lastOffset: LAST,
        type: 'gap',
        error: null,
      },
    ])
    expect(Object.keys(h.acks[0])).not.toContain('offsets')
  })

  test('keeps broker failures compact through the public error', async () => {
    await start()
    h.script('ShareFetch', [
      fetchAnswer([part({ partition: 0, acquire: [acquired(FIRST, LAST)] })]),
      fetchAnswer([part({ partition: 0, ack: 121 })]),
    ])
    await h.consumer.run({ eachMessage: async () => {} })
    await until(() => h.acks.length === 1, 'the failed gap settlement')
    const [event] = h.acks
    expect(event).toMatchObject({ firstOffset: FIRST, lastOffset: LAST, type: 'gap' })
    expect(event.error).toBeInstanceOf(KafkaJSShareAcknowledgementError)
    expect(event.error).toMatchObject({
      reason: 'rejected-by-broker',
      retriable: false,
      topic: 'topic-a',
      partition: 0,
      ranges: [{ firstOffset: FIRST, lastOffset: LAST }],
    })
    expect(event.error.cause.code).toBe(121)
  })

  test.each([
    ['9007199254740993', ['9007199254740993']],
    ['9223372036854775807', ['9223372036854775807']],
  ])('preserves int64 endpoints through bytes and results (%s)', async (offset) => {
    await start()
    h.script('ShareFetch', [
      fetchAnswer([
        part({
          partition: 0,
          acquire: [acquired(offset, offset)],
          records: recordsHex(BigInt(offset), ['v']),
        }),
      ]),
    ])
    const seen = []
    await h.consumer.run({ eachMessage: async ({ message }) => seen.push(message.offset) })
    await until(() => sent().length > 0, 'the piggybacked acknowledgement')
    expect(seen).toEqual([offset])
    expect(sent()).toEqual([{ firstOffset: offset, lastOffset: offset, acknowledgeTypes: [1] }])
    await until(() => h.acks.length === 1, 'the settled acknowledgement')
    expect(h.acks[0]).toMatchObject({ firstOffset: offset, lastOffset: offset, error: null })
  })

  test('fails omitted partitions and ignores consumed or stale replies', async () => {
    await start({
      topics: {
        'topic-a': {
          topicId: A,
          partitions: [
            { leader: 0, epoch: 1 },
            { leader: 0, epoch: 1 },
          ],
        },
      },
    })
    h.script('ShareFetch', [
      fetchAnswer([acquire(0, 0, ['a']), acquire(1, 0, ['b'])]),
      () => fetchAnswer([part({ partition: 0 })]),
    ])
    await h.consumer.run({ eachMessage: async () => {} })
    await until(() => h.acks.length === 2, 'both settlements')
    const byPartition = Object.fromEntries(h.acks.map((event) => [event.partition, event]))
    expect(byPartition[0]).toMatchObject({ type: 'accept', error: null })
    expect(byPartition[1].type).toBe('accept')
    expect(byPartition[1].error).toBeInstanceOf(KafkaJSShareAcknowledgementError)
    expect(byPartition[1].error.ranges).toEqual([{ firstOffset: '0', lastOffset: '0' }])
    await until(() => h.frames(78).length >= 4, 'later quiet fetches')
    expect(h.acks).toHaveLength(2)
  })
})
