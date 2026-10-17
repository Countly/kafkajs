const {
  TOPIC,
  createScheduledManager,
  scheduleOffsets,
  offsetFetchAnswer,
  listOffsetsAnswer,
} = require('./fixtures/scheduledOffsetManager')
const {
  createFetchGroup,
  fetchResponse,
  TOPIC_ID,
} = require('../../__tests__/fixtures/fetchHarness')
const apiKeys = require('../../../protocol/requests/apiKeys')

/** Lets every promise continuation and I/O callback that is already queued run. */
const flush = async (turns = 10) => {
  for (let turn = 0; turn < turns; turn++) {
    await new Promise(setImmediate)
  }
}

/**
 * Schedules of resolving, seeking and committing offsets that interleave with a topic's recreation,
 * each held in the order it names by a `Schedule`. A case reads the offset the next fetch starts at,
 * the committed offset the group cache holds and whether the recreation's reset is still owed, each
 * apart, wherever the schedule exposes that observation.
 *
 * The cases below fail today, and each is marked `it.failing` until the work that fixes it removes
 * the mark: the title starts with the schedule's id.
 */
describe('Consumer > OffsetManager > schedules that interleave a resolution with a later operation', () => {
  const fetchGroups = []

  /**
   * A fetch group the case ends by disconnecting, whether it passed or failed.
   * @param {Parameters<typeof createFetchGroup>[0]} options
   * @returns {ReturnType<typeof createFetchGroup>}
   */
  const openFetchGroup = async (options) => {
    const fetchGroup = await createFetchGroup(options)
    fetchGroups.push(fetchGroup)
    return fetchGroup
  }

  afterEach(async () => {
    jest.useRealTimers()
    jest.restoreAllMocks()
    await Promise.all(fetchGroups.splice(0).map(({ broker }) => broker.disconnect()))
  })

  it.failing(
    'P03 a resolver that starts after the first one took the reset does not seed the committed offset the broker still holds',
    async () => {
      const group = createScheduledManager()
      group.recreate(TOPIC, 'id-1')
      const listOffsets = group.schedule.hold({ api: 'ListOffsets' })
      const first = group.manager.resolveOffsets()
      await listOffsets.arrived()

      const second = group.manager.resolveOffsets()
      await flush()

      expect(group.committed(TOPIC, 0)).not.toBe('99')
      expect(group.position(TOPIC, 0)).not.toBe('99')
      listOffsets.release(listOffsetsAnswer({ 0: '0' }))
      await Promise.all([first, second])
      expect(group.position(TOPIC, 0)).toBe('0')
      expect(group.committed(TOPIC, 0)).toBe('0')
      expect(group.resetOwed(TOPIC)).toBe(false)
    }
  )

  it.failing(
    'P11 a ListOffsets that fails leaves the reset owed, so the retry still resets',
    async () => {
      const group = createScheduledManager()
      group.recreate(TOPIC, 'id-1')
      group.schedule.once({ api: 'ListOffsets' }, new Error('ListOffsets failed'))

      await expect(group.manager.resolveOffsets()).rejects.toThrow('ListOffsets failed')
      expect(group.committed(TOPIC, 0)).toBeUndefined()
      expect(group.resetOwed(TOPIC)).toBe(true)
      await group.manager.resolveOffsets()

      expect(group.position(TOPIC, 0)).toBe('0')
      expect(group.committed(TOPIC, 0)).toBe('0')
      expect(group.resetOwed(TOPIC)).toBe(false)
    }
  )

  it.failing(
    'N05 the broker whose answer was dropped fetches at once, not after the empty-round wait',
    async () => {
      jest.useFakeTimers({ doNotFake: ['setImmediate', 'nextTick', 'queueMicrotask'] })
      const fetchGroup = await openFetchGroup({
        partitions: [0, 1],
        knownNodes: [1, 2],
        partitionLeaders: { 0: 1, 1: 2 },
        position: '-1',
      })
      const { schedule, resetOwed } = scheduleOffsets(fetchGroup)
      // Each broker's Fetch carries only the partitions it leads, and so does the answer to it.
      fetchGroup.setResponse(() =>
        fetchResponse(
          fetchGroup.fetchRequestPartitions().map((partition) => ({ partition, errorCode: 0 }))
        )
      )
      fetchGroup.offsetManager.pendingTopicResets.set(TOPIC, TOPIC_ID)
      const [ownerFetch, otherFetch, listOffsets] = [
        schedule.hold({ api: 'OffsetFetch' }),
        schedule.hold({ api: 'OffsetFetch' }),
        schedule.hold({ api: 'ListOffsets' }),
      ]

      const owner = fetchGroup.group.fetch(1)
      await ownerFetch.arrived()
      const other = fetchGroup.group.fetch(2)
      await otherFetch.arrived()
      ownerFetch.release(offsetFetchAnswer({ 0: '99', 1: '99' }))
      await listOffsets.arrived()
      expect(resetOwed(TOPIC)).toBe(false)

      try {
        otherFetch.release(offsetFetchAnswer({ 0: '99', 1: '99' }))
        let otherSettled = false
        other.then(() => {
          otherSettled = true
        })
        await flush()

        expect(otherSettled).toBe(true)
        expect(fetchGroup.fetchRequestPartitions()).toContain(1)
      } finally {
        listOffsets.release(listOffsetsAnswer({ 0: '0', 1: '0' }))
        jest.advanceTimersByTime(fetchGroup.group.maxWaitTime)
        await Promise.allSettled([owner, other])
      }
      expect(fetchGroup.requests().filter(({ apiKey }) => apiKey === apiKeys.Fetch)).toHaveLength(2)
    }
  )

  it.failing(
    'N08 a seek with auto-commit that lands while the only resolver waits for ListOffsets is not overwritten',
    async () => {
      const group = createScheduledManager({ autoCommit: true })
      group.recreate(TOPIC, 'id-1')
      const listOffsets = group.schedule.hold({ api: 'ListOffsets' })
      const resolver = group.manager.resolveOffsets()
      await listOffsets.arrived()

      await expect(group.manager.seek({ topic: TOPIC, partition: 0, offset: '500' })).resolves.toBe(
        true
      )
      listOffsets.release(listOffsetsAnswer({ 0: '0' }))
      await resolver

      expect(group.committed(TOPIC, 0)).toBeUndefined()
      await group.manager.resolveOffsets()
      expect(group.position(TOPIC, 0)).toBe('500')
      expect(group.committed(TOPIC, 0)).toBe('500')
      expect(group.brokerOffsets.get(`${TOPIC}:0`)).toBe('500')
      expect(group.resetOwed(TOPIC)).toBe(false)
    }
  )

  it.failing(
    'N09 a commit of 50 that lands while the only resolver waits for ListOffsets is not overwritten',
    async () => {
      const group = createScheduledManager()
      group.recreate(TOPIC, 'id-1')
      const listOffsets = group.schedule.hold({ api: 'ListOffsets' })
      const resolver = group.manager.resolveOffsets()
      await listOffsets.arrived()

      await group.manager.commitOffsets({
        topics: [{ topic: TOPIC, partitions: [{ partition: 0, offset: '50' }] }],
      })
      listOffsets.release(listOffsetsAnswer({ 0: '0' }))
      await resolver

      expect(group.committed(TOPIC, 0)).toBe('50')
      expect(group.position(TOPIC, 0)).toBe('50')
      expect(group.brokerOffsets.get(`${TOPIC}:0`)).toBe('50')
      expect(group.resetOwed(TOPIC)).toBe(false)
    }
  )

  it.failing(
    'N11 the same seek through a consumer group: the stale answer lands after the seek cleared the partition, before the pass that resolves it',
    async () => {
      const fetchGroup = await openFetchGroup({
        partitions: [0],
        position: '-1',
        autoCommit: true,
      })
      const { schedule, brokerOffsets, resetOwed, position } = scheduleOffsets(fetchGroup)
      const { group, offsetManager } = fetchGroup
      offsetManager.pendingTopicResets.set(TOPIC, TOPIC_ID)
      const assigned = [{ topic: TOPIC, partitions: [0] }]
      const listOffsets = schedule.hold({ api: 'ListOffsets' })
      const resolver = group.seekOffsets(assigned)
      await listOffsets.arrived()

      // The stale answer lands after the seek has cleared the partition and before the pass that
      // resolves it again starts.
      const resolveOffsets = offsetManager.resolveOffsets.bind(offsetManager)
      jest.spyOn(offsetManager, 'resolveOffsets').mockImplementation(async (seeked) => {
        listOffsets.release(listOffsetsAnswer({ 0: '0' }))
        await flush(3)
        return resolveOffsets(seeked)
      })
      group.seek({ topic: TOPIC, partition: 0, offset: '500' })
      await group.seekOffsets(assigned)
      await resolver

      expect(position(TOPIC, 0)).toBe('500')
      expect(offsetManager.committedOffsets()[TOPIC][0]).toBe('500')
      expect(brokerOffsets.get(`${TOPIC}:0`)).toBe('500')
      expect(resetOwed(TOPIC)).toBe(false)
    }
  )

  it.failing(
    'L06 a quiescent consumer retains nothing for 200 distinct recreated topics, before it disconnects',
    async () => {
      const topics = Array.from({ length: 200 }, (_, index) => `recreated-${index}`)
      const group = createScheduledManager({ topics })
      topics.forEach((topic) => group.recreate(topic, 'id-1'))

      await group.manager.resolveOffsets()
      expect(topics.filter((topic) => group.committed(topic, 0) !== '0')).toEqual([])
      expect(topics.filter((topic) => group.resetOwed(topic))).toEqual([])
      group.manager.updateAssignment({
        revoked: topics.map((topic) => ({ topic, partitions: [0] })),
        memberAssignment: {},
        generationId: 2,
        memberId: 'member-1',
        coordinator: group.coordinator,
      })

      expect(group.retainedEntries()).toBe(0)
    }
  )
})
