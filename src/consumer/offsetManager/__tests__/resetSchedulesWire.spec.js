const { createOffsetsOnTheWire } = require('./fixtures/offsetsOnTheWire')
const { TOPIC, OLD_ID, NEW_ID, groupOver } = require('./fixtures/scheduledOffsetManager')

/**
 * The requests of one API, in the order they were sent.
 *
 * @param {object} wire the fixture of `createOffsetsOnTheWire`
 * @param {string} api the protocol's API name, for example `'ListOffsets'`
 * @returns {object[]} the matching request records
 */
const sent = (wire, api) => wire.requests().filter((request) => request.api === api)

/**
 * The timestamps one ListOffsets request asked for, one per partition.
 *
 * @param {object} request a ListOffsets request record
 * @returns {string[]} the timestamps, in request order
 */
const timestampsOf = (request) =>
  request.fields.topics.flatMap(({ partitions }) => partitions.map(({ timestamp }) => timestamp))

/**
 * The wire repeats of the schedules that cross a recreation or a seek, with the real cluster,
 * broker, connection and codecs between the offset manager and the held answers. What they assert
 * comes from the decoded request log and from the manager's state, so it does not depend on the
 * order in which answers were released.
 */
describe('Consumer > OffsetManager > reset schedules on the wire', () => {
  let wire

  afterEach(async () => {
    const current = wire
    wire = undefined
    if (current) {
      try {
        current.scripted.assertClean()
      } finally {
        await current.disconnect()
      }
    }
  })

  test('P05 a recreation during ListOffsets, while the id was unknown, drops the old answer and the next resolution reaches the reset', async () => {
    wire = await createOffsetsOnTheWire({ committed: '-1' })
    wire.obligations.owe(TOPIC, OLD_ID)
    const listOffsets = wire.schedule.hold({ api: 'ListOffsets' })
    const resolving = wire.manager.resolveOffsets()
    await listOffsets.arrived()

    await wire.setTopicId(OLD_ID)
    await wire.recreate(NEW_ID)
    listOffsets.release(wire.answers.listOffsets({ 0: '500' }))
    await expect(resolving).resolves.toBe(false)
    expect(wire.cache[TOPIC]).toEqual({})

    await wire.manager.resolveOffsets()

    expect(wire.next()).toEqual('0')
    expect(wire.committedAt()).toEqual('0')
    expect(wire.obligations.size()).toBe(1)
    expect(wire.requests().map(({ api }) => api)).toEqual([
      'OffsetFetch',
      'ListOffsets',
      'OffsetFetch',
      'ListOffsets',
    ])
    expect(timestampsOf(sent(wire, 'ListOffsets')[1])).toEqual(['-2'])
  })

  test('P15 an old OffsetFetch landing after the new incarnation resolved does not move its position', async () => {
    wire = await createOffsetsOnTheWire({ topicId: OLD_ID })
    const old = wire.schedule.hold({ api: 'OffsetFetch' })
    const stale = wire.manager.resolveOffsets()
    await old.arrived()

    await wire.recreate(NEW_ID)
    await wire.manager.resolveOffsets()
    expect(wire.committedAt()).toEqual('0')
    old.release(wire.answers.offsetFetch({ 0: '99' }))
    await expect(stale).resolves.toBe(false)

    expect(wire.next()).toEqual('0')
    expect(wire.committedAt()).toEqual('0')
    expect(wire.owed()).toBe(true)
    expect(wire.requests().map(({ api }) => api)).toEqual([
      'OffsetFetch',
      'OffsetFetch',
      'ListOffsets',
    ])
  })

  test('P16 a known recreation during ListOffsets drops the old answer', async () => {
    wire = await createOffsetsOnTheWire({ topicId: OLD_ID, committed: '-1' })
    const listOffsets = wire.schedule.hold({ api: 'ListOffsets' })
    const stale = wire.manager.resolveOffsets()
    await listOffsets.arrived()

    await wire.recreate(NEW_ID)
    listOffsets.release(wire.answers.listOffsets({ 0: '500' }))
    await expect(stale).resolves.toBe(false)
    expect(wire.cache[TOPIC]).toEqual({})
    expect(wire.owed()).toBe(true)

    await wire.manager.resolveOffsets()

    expect(wire.next()).toEqual('0')
    expect(wire.committedAt()).toEqual('0')
    expect(wire.owed()).toBe(true)
    expect(sent(wire, 'ListOffsets')).toHaveLength(2)
  })

  test('N03 a ListOffsets sent with an unknown id does not overwrite the reset a rejoined manager made', async () => {
    wire = await createOffsetsOnTheWire({ committed: '-1' })
    const old = wire.schedule.hold({ api: 'ListOffsets' })
    const stale = wire.manager.resolveOffsets()
    await old.arrived()

    await wire.setTopicId(OLD_ID)
    await wire.recreate(NEW_ID)
    const rejoined = wire.rebuild()
    await rejoined.resolveOffsets()
    old.release(wire.answers.listOffsets({ 0: '500' }))
    await expect(stale).resolves.toBe(false)

    expect(rejoined.nextOffset(TOPIC, 0).toString()).toEqual('0')
    expect(wire.committedAt()).toEqual('0')
    expect(wire.obligations.size()).toBe(1)
    expect(sent(wire, 'ListOffsets')).toHaveLength(2)
  })

  test.each([
    ['the seek is answered first', ['commit', 'listOffsets']],
    ['the resolver is answered first', ['listOffsets', 'commit']],
  ])(
    'N11 a seek applied through the group while the sole resolver waits is not overwritten when %s',
    async (_, order) => {
      wire = await createOffsetsOnTheWire({ topicId: NEW_ID, autoCommit: true })
      await wire.recreate(OLD_ID)
      await wire.recreate(NEW_ID)
      const group = groupOver(wire)
      const listOffsets = wire.schedule.hold({ api: 'ListOffsets' })
      const commit = wire.schedule.hold({ api: 'OffsetCommit' })
      const resolving = wire.manager.resolveOffsets()
      await listOffsets.arrived()
      group.seek({ topic: TOPIC, partition: 0, offset: '500' })
      const round = group.seekOffsets([{ topic: TOPIC, partitions: [0] }])
      await commit.arrived()

      const release = {
        commit: () => commit.release(),
        listOffsets: () => listOffsets.release(wire.answers.listOffsets({ 0: '0' })),
      }
      for (const which of order) {
        release[which]()
        await new Promise(setImmediate)
      }
      await Promise.all([round, resolving])

      expect(wire.next()).toEqual('500')
      expect(wire.committedAt()).toEqual('500')
      expect(wire.owed()).toBe(false)
      expect(sent(wire, 'OffsetCommit')[0].fields.topics[0].partitions[0].offset).toEqual('500')
      expect(wire.requests().every(({ version }) => version === 9)).toBe(true)
    }
  )
})
