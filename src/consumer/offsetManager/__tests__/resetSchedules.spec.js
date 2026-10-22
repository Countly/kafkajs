const OffsetManager = require('../index')
const OffsetOperations = require('../offsetOperations')
const ResetObligations = require('../resetObligations')
const createConsumer = require('../../index')
const Runner = require('../../runner')
const { createFetchGroup, fetchResponse } = require('../../__tests__/fixtures/fetchHarness')
const InstrumentationEventEmitter = require('../../../instrumentation/emitter')
const { newLogger } = require('testHelpers')
const {
  TOPIC,
  GROUP_ID,
  OLD_ID,
  NEW_ID,
  offsetFetchAnswer,
  listOffsetsAnswer,
  createScheduledOffsetManager,
  groupOver,
} = require('./fixtures/scheduledOffsetManager')

const commitOf = (partition, offset) => ({
  topics: [{ topic: TOPIC, partitions: [{ partition, offset }] }],
})

/**
 * Every schedule below asserts three things apart: the offset the next fetch starts from (the
 * resolved position), the committed offset in the shared cache, and whether the reset obligation
 * is still owed.
 */
describe('Consumer > OffsetManager > reset schedules', () => {
  describe('a topic recreated while the manager resolves', () => {
    test('P01 an id learned during ListOffsets does not discard the reset', async () => {
      const ctx = createScheduledOffsetManager()
      ctx.obligations.owe(TOPIC, NEW_ID)
      const listOffsets = ctx.schedule.hold({ api: 'ListOffsets' })
      const resolving = ctx.manager.resolveOffsets()
      await listOffsets.arrived()

      ctx.setTopicId(NEW_ID)
      listOffsets.release(listOffsetsAnswer({ 0: '0' }))
      await resolving

      expect(ctx.next()).toEqual('0')
      expect(ctx.committedAt()).toEqual('0')
      expect(ctx.owed()).toBe(false)
      expect(ctx.obligations.size()).toBe(0)
    })

    test('P02 a cache cleared during ListOffsets and repopulated with the same id does not discard the reset', async () => {
      const ctx = createScheduledOffsetManager({ topicId: NEW_ID })
      ctx.obligations.owe(TOPIC, NEW_ID)
      const listOffsets = ctx.schedule.hold({ api: 'ListOffsets' })
      const resolving = ctx.manager.resolveOffsets()
      await listOffsets.arrived()

      ctx.setTopicId(undefined)
      listOffsets.release(listOffsetsAnswer({ 0: '0' }))
      await resolving
      ctx.setTopicId(NEW_ID)
      await ctx.manager.resolveOffsets()

      expect(ctx.next()).toEqual('0')
      expect(ctx.committedAt()).toEqual('0')
      expect(ctx.obligations.size()).toBe(0)
    })

    test('P03 a resolver that starts after another has passed the reset does not seed the old offset', async () => {
      const ctx = createScheduledOffsetManager({ topicId: NEW_ID })
      ctx.obligations.owe(TOPIC, NEW_ID)
      const first = ctx.schedule.hold({ api: 'ListOffsets' })
      const second = ctx.schedule.hold({ api: 'ListOffsets' })

      const one = ctx.manager.resolveOffsets()
      await first.arrived()
      const two = ctx.manager.resolveOffsets()
      await second.arrived()
      first.release(listOffsetsAnswer({ 0: '0' }))
      second.release(listOffsetsAnswer({ 0: '0' }))
      await Promise.all([one, two])

      expect(ctx.next()).toEqual('0')
      expect(ctx.committedAt()).toEqual('0')
      expect(ctx.owed()).toBe(false)
    })

    test('P04 a first id learned during OffsetFetch is not a recreation: the committed offset is used at once', async () => {
      const ctx = createScheduledOffsetManager()
      const offsetFetch = ctx.schedule.hold({ api: 'OffsetFetch' })
      const resolving = ctx.manager.resolveOffsets()
      await offsetFetch.arrived()

      ctx.setTopicId(NEW_ID)
      offsetFetch.release(offsetFetchAnswer({ 0: '99' }))
      await resolving

      expect(ctx.next()).toEqual('99')
      expect(ctx.committedAt()).toEqual('99')
      expect(ctx.cluster.fetchTopicsOffset).not.toHaveBeenCalled()
      expect(ctx.owed()).toBe(false)
    })

    test('P05 a recreation during ListOffsets, while the id was unknown, drops the old answer and the next resolution reaches the reset', async () => {
      const ctx = createScheduledOffsetManager({ committed: '-1' })
      ctx.obligations.owe(TOPIC, OLD_ID)
      const listOffsets = ctx.schedule.hold({ api: 'ListOffsets' })
      const resolving = ctx.manager.resolveOffsets()
      await listOffsets.arrived()

      ctx.setTopicId(OLD_ID)
      ctx.recreate(NEW_ID)
      listOffsets.release(listOffsetsAnswer({ 0: '500' }))

      await expect(resolving).resolves.toBe(false)
      expect(ctx.cache[TOPIC]).toEqual({})
      expect(ctx.obligations.current(TOPIC)).toEqual(NEW_ID)

      await ctx.manager.resolveOffsets()

      expect(ctx.next()).toEqual('0')
      expect(ctx.committedAt()).toEqual('0')
      expect(ctx.obligations.size()).toBe(0)
    })
  })

  describe('resolvers racing a seek, a commit or consumed progress', () => {
    /**
     * Two resolvers of one recreated topic, both started before either has been answered. The
     * first is answered and completed; the second completes when the case calls `finish`, after
     * whatever the case does in between.
     */
    const staggered = async (ctx) => {
      const first = ctx.schedule.hold({ api: 'OffsetFetch' })
      const second = ctx.schedule.hold({ api: 'OffsetFetch' })
      const listOffsets = ctx.schedule.hold({ api: 'ListOffsets' })
      const one = ctx.manager.resolveOffsets()
      await first.arrived()
      const two = ctx.manager.resolveOffsets()
      await second.arrived()

      first.release(offsetFetchAnswer({ 0: '99' }))
      await listOffsets.arrived()
      listOffsets.release(listOffsetsAnswer({ 0: '0' }))
      await one

      return {
        finish: () => {
          second.release(offsetFetchAnswer({ 0: '99' }))
          return two
        },
      }
    }

    test('P06 a seek applied between two resolvers survives the second one', async () => {
      const ctx = createScheduledOffsetManager({ topicId: NEW_ID, autoCommit: true })
      ctx.recreate(NEW_ID)
      const { finish } = await staggered(ctx)

      await ctx.manager.seek({ topic: TOPIC, partition: 0, offset: '500' })
      await expect(finish()).resolves.toBe(false)
      await ctx.manager.resolveOffsets()

      expect(ctx.next()).toEqual('500')
      expect(ctx.committedAt()).toEqual('500')
      expect(ctx.owed()).toBe(false)
      expect(ctx.cluster.fetchTopicsOffset).toHaveBeenCalledTimes(1)
    })

    test('P08 a commit applied between two resolvers survives the second one', async () => {
      const ctx = createScheduledOffsetManager({ topicId: NEW_ID })
      ctx.recreate(NEW_ID)
      const { finish } = await staggered(ctx)

      await ctx.manager.commitOffsets(commitOf(0, '50'))
      await expect(finish()).resolves.toBe(false)

      expect(ctx.next()).toEqual('50')
      expect(ctx.committedAt()).toEqual('50')
      expect(ctx.owed()).toBe(false)
    })

    test('P09 progress consumed between two resolvers survives the second one', async () => {
      const ctx = createScheduledOffsetManager({ topicId: NEW_ID })
      ctx.recreate(NEW_ID)
      const { finish } = await staggered(ctx)

      ctx.manager.resolveOffset({ topic: TOPIC, partition: 0, offset: '49' })
      await expect(finish()).resolves.toBe(true)

      expect(ctx.next()).toEqual('50')
      expect(ctx.committedAt()).toEqual('0')
      expect(ctx.owed()).toBe(false)
    })

    test('P10 a seek without auto-commit between two resolvers survives the second one', async () => {
      const ctx = createScheduledOffsetManager({ topicId: NEW_ID, autoCommit: false })
      ctx.recreate(NEW_ID)
      const { finish } = await staggered(ctx)

      await ctx.manager.seek({ topic: TOPIC, partition: 0, offset: '500' })
      await expect(finish()).resolves.toBe(false)

      expect(ctx.next()).toEqual('500')
      expect(ctx.committedAt()).toEqual('0')
      expect(ctx.owed()).toBe(false)
    })
  })

  describe('failures and rejoins', () => {
    test('P11 a failed ListOffsets keeps the reset owed and the retry still resets', async () => {
      const ctx = createScheduledOffsetManager({ topicId: NEW_ID })
      ctx.recreate(NEW_ID)
      ctx.schedule.once({ api: 'ListOffsets' }, new Error('list offsets unavailable'))

      await expect(ctx.manager.resolveOffsets()).rejects.toThrow('list offsets unavailable')
      expect(ctx.cache[TOPIC]).toEqual({})
      expect(ctx.owed()).toBe(true)

      await ctx.manager.resolveOffsets()

      expect(ctx.next()).toEqual('0')
      expect(ctx.committedAt()).toEqual('0')
      expect(ctx.owed()).toBe(false)
      expect(ctx.cluster.fetchTopicsOffset).toHaveBeenCalledTimes(2)
    })

    test('P12 a commit after a failed ListOffsets leaves no dormant reset', async () => {
      const ctx = createScheduledOffsetManager({ topicId: NEW_ID })
      ctx.recreate(NEW_ID)
      ctx.schedule.once({ api: 'ListOffsets' }, new Error('list offsets unavailable'))
      await expect(ctx.manager.resolveOffsets()).rejects.toThrow()

      await ctx.manager.commitOffsets(commitOf(0, '50'))
      await ctx.manager.resolveOffsets()

      expect(ctx.next()).toEqual('50')
      expect(ctx.committedAt()).toEqual('50')
      expect(ctx.obligations.size()).toBe(0)
    })

    test('P13 the position stays at the commit after a failed ListOffsets and a rejoin', async () => {
      const ctx = createScheduledOffsetManager({ topicId: NEW_ID })
      ctx.recreate(NEW_ID)
      ctx.schedule.once({ api: 'ListOffsets' }, new Error('list offsets unavailable'))
      await expect(ctx.manager.resolveOffsets()).rejects.toThrow()
      await ctx.manager.commitOffsets(commitOf(0, '50'))
      await ctx.manager.resolveOffsets()

      const rejoined = ctx.rebuild()
      await rejoined.resolveOffsets()

      expect(rejoined.nextOffset(TOPIC, 0).toString()).toEqual('50')
      expect(ctx.committedAt()).toEqual('50')
      expect(ctx.obligations.size()).toBe(0)
    })

    test('P14 a reset that a known topic id contradicts is disposed of without resetting', async () => {
      const ctx = createScheduledOffsetManager({ topicId: NEW_ID })
      ctx.obligations.owe(TOPIC, OLD_ID)

      await ctx.manager.resolveOffsets()

      expect(ctx.next()).toEqual('99')
      expect(ctx.committedAt()).toEqual('99')
      expect(ctx.obligations.size()).toBe(0)
      expect(ctx.cluster.fetchTopicsOffset).not.toHaveBeenCalled()
    })
  })

  describe('answers that outlive the incarnation they describe', () => {
    test('P15 an old OffsetFetch landing after the new incarnation resolved does not move its position', async () => {
      const ctx = createScheduledOffsetManager({ topicId: OLD_ID })
      const old = ctx.schedule.hold({ api: 'OffsetFetch' })
      const stale = ctx.manager.resolveOffsets()
      await old.arrived()

      ctx.recreate(NEW_ID)
      await ctx.manager.resolveOffsets()
      expect(ctx.committedAt()).toEqual('0')

      old.release(offsetFetchAnswer({ 0: '99' }))
      await expect(stale).resolves.toBe(false)

      expect(ctx.next()).toEqual('0')
      expect(ctx.committedAt()).toEqual('0')
      expect(ctx.owed()).toBe(false)
    })

    test('P16 a known recreation during ListOffsets drops the old answer', async () => {
      const ctx = createScheduledOffsetManager({ topicId: OLD_ID, committed: '-1' })
      const listOffsets = ctx.schedule.hold({ api: 'ListOffsets' })
      const stale = ctx.manager.resolveOffsets()
      await listOffsets.arrived()

      ctx.recreate(NEW_ID)
      listOffsets.release(listOffsetsAnswer({ 0: '500' }))
      await expect(stale).resolves.toBe(false)
      expect(ctx.cache[TOPIC]).toEqual({})
      expect(ctx.owed()).toBe(true)

      await ctx.manager.resolveOffsets()

      expect(ctx.next()).toEqual('0')
      expect(ctx.committedAt()).toEqual('0')
      expect(ctx.owed()).toBe(false)
    })

    test('P17 an old OffsetFetch before a fresh resolution leaves the new reset to be applied', async () => {
      const ctx = createScheduledOffsetManager({ topicId: OLD_ID })
      const old = ctx.schedule.hold({ api: 'OffsetFetch' })
      const stale = ctx.manager.resolveOffsets()
      await old.arrived()

      ctx.recreate(NEW_ID)
      old.release(offsetFetchAnswer({ 0: '99' }))
      await expect(stale).resolves.toBe(false)
      expect(ctx.cache[TOPIC]).toEqual({})
      expect(ctx.owed()).toBe(true)
      expect(ctx.cluster.fetchTopicsOffset).not.toHaveBeenCalled()

      await ctx.manager.resolveOffsets()

      expect(ctx.next()).toEqual('0')
      expect(ctx.committedAt()).toEqual('0')
      expect(ctx.owed()).toBe(false)
    })
  })

  describe('a sibling resolver settles the reset while an older request is in flight', () => {
    test('N01 an OffsetFetch sent with an unknown id does not overwrite the reset a sibling made', async () => {
      const ctx = createScheduledOffsetManager()
      const old = ctx.schedule.hold({ api: 'OffsetFetch' })
      const stale = ctx.manager.resolveOffsets()
      await old.arrived()

      ctx.setTopicId(OLD_ID)
      ctx.recreate(NEW_ID)
      await ctx.manager.resolveOffsets()
      old.release(offsetFetchAnswer({ 0: '99' }))

      await expect(stale).resolves.toBe(false)
      expect(ctx.next()).toEqual('0')
      expect(ctx.committedAt()).toEqual('0')
      expect(ctx.obligations.size()).toBe(0)
    })

    test('N02 a ListOffsets sent with an unknown id does not overwrite the reset a sibling made', async () => {
      const ctx = createScheduledOffsetManager({ committed: '-1' })
      const old = ctx.schedule.hold({ api: 'ListOffsets' })
      const stale = ctx.manager.resolveOffsets()
      await old.arrived()

      ctx.setTopicId(OLD_ID)
      ctx.recreate(NEW_ID)
      await ctx.manager.resolveOffsets()
      old.release(listOffsetsAnswer({ 0: '500' }))

      await expect(stale).resolves.toBe(false)
      expect(ctx.next()).toEqual('0')
      expect(ctx.committedAt()).toEqual('0')
      expect(ctx.obligations.size()).toBe(0)
    })

    test('N03 the same, when the sibling that reset the topic is a manager built by a rejoin', async () => {
      const ctx = createScheduledOffsetManager({ committed: '-1' })
      const old = ctx.schedule.hold({ api: 'ListOffsets' })
      const stale = ctx.manager.resolveOffsets()
      await old.arrived()

      ctx.setTopicId(OLD_ID)
      ctx.recreate(NEW_ID)
      const rejoined = ctx.rebuild()
      await rejoined.resolveOffsets()
      old.release(listOffsetsAnswer({ 0: '500' }))

      await expect(stale).resolves.toBe(false)
      expect(rejoined.nextOffset(TOPIC, 0).toString()).toEqual('0')
      expect(ctx.committedAt()).toEqual('0')
      expect(ctx.obligations.size()).toBe(0)
    })

    test('N04 a resolver that finishes first keeps its reset when a sibling started with it finishes later', async () => {
      const ctx = createScheduledOffsetManager({ topicId: NEW_ID })
      ctx.recreate(NEW_ID)

      await Promise.all([ctx.manager.resolveOffsets(), ctx.manager.resolveOffsets()])

      expect(ctx.next()).toEqual('0')
      expect(ctx.committedAt()).toEqual('0')
      expect(ctx.owed()).toBe(false)
    })

    test('N06 six overlapping resolvers settle on the reset', async () => {
      const ctx = createScheduledOffsetManager({ topicId: NEW_ID })
      ctx.recreate(NEW_ID)

      await Promise.all(Array.from({ length: 6 }, () => ctx.manager.resolveOffsets()))

      expect(ctx.next()).toEqual('0')
      expect(ctx.committedAt()).toEqual('0')
      expect(ctx.obligations.size()).toBe(0)
    })

    test('N07 an OffsetFetch answered across a recreation, with the id unknown, is dropped and the next resolution resets', async () => {
      const ctx = createScheduledOffsetManager()
      ctx.obligations.owe(TOPIC, OLD_ID)
      const old = ctx.schedule.hold({ api: 'OffsetFetch' })
      const stale = ctx.manager.resolveOffsets()
      await old.arrived()

      ctx.recreate(NEW_ID)
      old.release(offsetFetchAnswer({ 0: '99' }))
      await expect(stale).resolves.toBe(false)
      expect(ctx.cache[TOPIC]).toEqual({})
      expect(ctx.obligations.current(TOPIC)).toEqual(NEW_ID)

      await ctx.manager.resolveOffsets()

      expect(ctx.next()).toEqual('0')
      expect(ctx.committedAt()).toEqual('0')
      expect(ctx.obligations.size()).toBe(0)
    })
  })

  describe('the only resolver racing a newer operation', () => {
    /** The sole resolver of a recreated topic, held at ListOffsets. */
    const soleResolver = async (options) => {
      const ctx = createScheduledOffsetManager({ topicId: NEW_ID, ...options })
      ctx.recreate(NEW_ID)
      const listOffsets = ctx.schedule.hold({ api: 'ListOffsets' })
      const resolving = ctx.manager.resolveOffsets()
      await listOffsets.arrived()

      return {
        ctx,
        finish: () => {
          listOffsets.release(listOffsetsAnswer({ 0: '0' }))
          return resolving
        },
      }
    }

    test('N08 a later seek with auto-commit wins over the resolver', async () => {
      const { ctx, finish } = await soleResolver({ autoCommit: true })

      await ctx.manager.seek({ topic: TOPIC, partition: 0, offset: '500' })
      await expect(finish()).resolves.toBe(false)
      await ctx.manager.resolveOffsets()

      expect(ctx.next()).toEqual('500')
      expect(ctx.committedAt()).toEqual('500')
      expect(ctx.owed()).toBe(false)
    })

    test('N09 a later commit wins over the resolver', async () => {
      const { ctx, finish } = await soleResolver()

      await ctx.manager.commitOffsets(commitOf(0, '50'))
      await expect(finish()).resolves.toBe(false)

      expect(ctx.next()).toEqual('50')
      expect(ctx.committedAt()).toEqual('50')
      expect(ctx.owed()).toBe(false)
    })

    test('N10 progress consumed meanwhile keeps its position', async () => {
      const { ctx, finish } = await soleResolver()

      ctx.manager.resolveOffset({ topic: TOPIC, partition: 0, offset: '49' })
      await expect(finish()).resolves.toBe(true)

      expect(ctx.next()).toEqual('50')
      expect(ctx.committedAt()).toEqual('0')
      expect(ctx.owed()).toBe(false)
    })
  })

  describe('an id learned or lost around a sibling that already reset the topic', () => {
    test('G01 a sibling that learned the id first settles the reset for the resolver that did not know it', async () => {
      const ctx = createScheduledOffsetManager()
      ctx.obligations.owe(TOPIC, NEW_ID)
      const first = ctx.schedule.hold({ api: 'OffsetFetch' })
      const slow = ctx.manager.resolveOffsets()
      await first.arrived()

      ctx.setTopicId(NEW_ID)
      await ctx.manager.resolveOffsets()
      first.release(offsetFetchAnswer({ 0: '99' }))
      await slow

      expect(ctx.next()).toEqual('0')
      expect(ctx.committedAt()).toEqual('0')
      expect(ctx.obligations.size()).toBe(0)
    })

    test('G02 a later resolution cannot bring back the offset the reset replaced', async () => {
      const ctx = createScheduledOffsetManager()
      ctx.obligations.owe(TOPIC, NEW_ID)
      const first = ctx.schedule.hold({ api: 'OffsetFetch' })
      const slow = ctx.manager.resolveOffsets()
      await first.arrived()
      ctx.setTopicId(NEW_ID)
      await ctx.manager.resolveOffsets()
      first.release(offsetFetchAnswer({ 0: '99' }))
      await slow

      await ctx.manager.resolveOffsets()

      expect(ctx.next()).toEqual('0')
      expect(ctx.committedAt()).toEqual('0')
      expect(ctx.cluster.fetchTopicsOffset).toHaveBeenCalledTimes(1)
      expect(ctx.owed()).toBe(false)
    })

    test('G03 a cleared metadata cache after the sibling reset does not let the old answer through', async () => {
      const ctx = createScheduledOffsetManager({ topicId: NEW_ID })
      ctx.obligations.owe(TOPIC, NEW_ID)
      const first = ctx.schedule.hold({ api: 'OffsetFetch' })
      const slow = ctx.manager.resolveOffsets()
      await first.arrived()

      await ctx.manager.resolveOffsets()
      ctx.setTopicId(undefined)
      first.release(offsetFetchAnswer({ 0: '99' }))
      await slow

      expect(ctx.next()).toEqual('0')
      expect(ctx.committedAt()).toEqual('0')
      expect(ctx.obligations.size()).toBe(0)
    })

    test('G04 two resolvers that start under a new id and an owed reset end on the reset', async () => {
      const ctx = createScheduledOffsetManager({ topicId: NEW_ID })
      ctx.obligations.owe(TOPIC, NEW_ID)
      const one = ctx.schedule.hold({ api: 'OffsetFetch' })
      const two = ctx.schedule.hold({ api: 'OffsetFetch' })
      const resolvingOne = ctx.manager.resolveOffsets()
      await one.arrived()
      const resolvingTwo = ctx.manager.resolveOffsets()
      await two.arrived()

      one.release(offsetFetchAnswer({ 0: '99' }))
      await resolvingOne
      two.release(offsetFetchAnswer({ 0: '99' }))
      await resolvingTwo

      expect(ctx.next()).toEqual('0')
      expect(ctx.committedAt()).toEqual('0')
      expect(ctx.obligations.size()).toBe(0)
    })
  })

  describe('two resolvers under one id both apply the reset', () => {
    test('W1 two resolvers both reach ListOffsets and the second one finds the cell resolved', async () => {
      const ctx = createScheduledOffsetManager({ topicId: NEW_ID })
      ctx.recreate(NEW_ID)
      const first = ctx.schedule.hold({ api: 'ListOffsets' })
      const second = ctx.schedule.hold({ api: 'ListOffsets' })
      const one = ctx.manager.resolveOffsets()
      await first.arrived()
      const two = ctx.manager.resolveOffsets()
      await second.arrived()

      first.release(listOffsetsAnswer({ 0: '0' }))
      second.release(listOffsetsAnswer({ 0: '0' }))
      await Promise.all([one, two])

      expect(ctx.cluster.fetchTopicsOffset).toHaveBeenCalledTimes(2)
      expect(ctx.next()).toEqual('0')
      expect(ctx.cache[TOPIC]).toEqual({ 0: '0' })
      expect(ctx.obligations.size()).toBe(0)
    })

    test('W2 a ListOffsets that fails leaves nothing written and the reset owed, so the next resolution resets again', async () => {
      const ctx = createScheduledOffsetManager({ topicId: NEW_ID })
      ctx.recreate(NEW_ID)
      ctx.schedule.once({ api: 'ListOffsets' }, new Error('list offsets unavailable'))

      await expect(ctx.manager.resolveOffsets()).rejects.toThrow('list offsets unavailable')
      expect(ctx.cache[TOPIC]).toEqual({})
      expect(ctx.owed()).toBe(true)

      await ctx.manager.resolveOffsets()

      expect(ctx.cache[TOPIC]).toEqual({ 0: '0' })
      expect(ctx.obligations.size()).toBe(0)
      expect(ctx.next()).toEqual('0')
    })

    test('W3 an answer that predates a newer recreation is not written and the newer reset stays owed', async () => {
      const ctx = createScheduledOffsetManager()
      ctx.obligations.owe(TOPIC, OLD_ID)
      const listOffsets = ctx.schedule.hold({ api: 'ListOffsets' })
      const resolving = ctx.manager.resolveOffsets()
      await listOffsets.arrived()

      ctx.recreate(NEW_ID)
      listOffsets.release(listOffsetsAnswer({ 0: '0' }))
      await expect(resolving).resolves.toBe(false)
      expect(ctx.cache[TOPIC]).toEqual({})
      expect(ctx.obligations.current(TOPIC)).toEqual(NEW_ID)

      ctx.manager.resetOffsetsForTopic(TOPIC)
      await ctx.manager.resolveOffsets()

      expect(ctx.cache[TOPIC]).toEqual({ 0: '0' })
      expect(ctx.obligations.size()).toBe(0)
      expect(ctx.next()).toEqual('0')
    })
  })

  describe('commits, seeks and barriers ordered by when they began', () => {
    /** A manager whose cell is already resolved to the coordinator's 99. */
    const resolved = async (options) => {
      const ctx = createScheduledOffsetManager({ topicId: NEW_ID, ...options })
      await ctx.manager.resolveOffsets()
      return ctx
    }

    test('C1 a commit that began after the resolver is not suppressed by the resolver completing', async () => {
      const ctx = createScheduledOffsetManager({ topicId: NEW_ID })
      ctx.recreate(NEW_ID)
      const listOffsets = ctx.schedule.hold({ api: 'ListOffsets' })
      const commitAnswer = ctx.schedule.hold({ api: 'OffsetCommit' })
      const resolving = ctx.manager.resolveOffsets()
      await listOffsets.arrived()
      const committing = ctx.manager.commitOffsets(commitOf(0, '50'))
      await commitAnswer.arrived()

      listOffsets.release(listOffsetsAnswer({ 0: '0' }))
      await resolving
      commitAnswer.release({})
      await committing

      expect(ctx.committedAt()).toEqual('50')
      expect(ctx.manager.resolvedOffsetAt(TOPIC, 0)).toBeUndefined()
      expect(ctx.next()).toEqual('50')
      expect(ctx.owed()).toBe(false)
    })

    test('C2 a seek that began earlier cannot clear what a later seek and the consumed progress left', async () => {
      const ctx = await resolved({ autoCommit: true })
      const firstCommit = ctx.schedule.hold({ api: 'OffsetCommit' })
      const earlier = ctx.manager.seek({ topic: TOPIC, partition: 0, offset: '100' })
      await firstCommit.arrived()

      await ctx.manager.seek({ topic: TOPIC, partition: 0, offset: '700' })
      await ctx.manager.resolveOffsets()
      ctx.manager.resolveOffset({ topic: TOPIC, partition: 0, offset: '700' })
      firstCommit.release({})

      await expect(earlier).resolves.toBe(false)
      expect(ctx.next()).toEqual('701')
      expect(ctx.committedAt()).toEqual('700')
      expect(ctx.owed()).toBe(false)
    })

    test('C3 a seek whose commit completes after a recreation is skipped and the reset stays owed', async () => {
      const ctx = await resolved({ autoCommit: true })
      const commitAnswer = ctx.schedule.hold({ api: 'OffsetCommit' })
      const seeking = ctx.manager.seek({ topic: TOPIC, partition: 0, offset: '500' })
      await commitAnswer.arrived()

      ctx.recreate(OLD_ID)
      commitAnswer.release({})

      await expect(seeking).resolves.toBe(false)
      expect(ctx.owed()).toBe(true)

      await ctx.manager.resolveOffsets()

      expect(ctx.next()).toEqual('0')
      expect(ctx.committedAt()).toEqual('0')
      expect(ctx.owed()).toBe(false)
    })

    test('C4 a seek whose commit completes after the partition was revoked and assigned again writes nothing', async () => {
      const ctx = await resolved({ autoCommit: true })
      const commitAnswer = ctx.schedule.hold({ api: 'OffsetCommit' })
      const seeking = ctx.manager.seek({ topic: TOPIC, partition: 0, offset: '500' })
      await commitAnswer.arrived()

      const membership = { generationId: 2, memberId: 'member-a', coordinator: ctx.coordinator }
      ctx.manager.updateAssignment({
        revoked: [{ topic: TOPIC, partitions: [0] }],
        memberAssignment: { [TOPIC]: [] },
        ...membership,
      })
      ctx.manager.updateAssignment({
        revoked: [],
        memberAssignment: { [TOPIC]: [0] },
        ...membership,
      })
      await ctx.manager.resolveOffsets()
      expect(ctx.committedAt()).toEqual('500')
      commitAnswer.release({})

      await expect(seeking).resolves.toBe(false)
      expect(ctx.committedAt()).toEqual('500')
      expect(ctx.next()).toEqual('500')
      expect(ctx.owed()).toBe(false)
    })

    test.each([
      ['a different offset', '60'],
      ['the offset the epoch was committed with', '50'],
    ])(
      'C5 an epoch the coordinator committed is not seeded for a cell a commit has rewritten with %s',
      async (_, committed) => {
        const ctx = createScheduledOffsetManager({ partitions: [0, 1], topicId: NEW_ID })
        const offsetFetch = ctx.schedule.hold({ api: 'OffsetFetch' })
        const listOffsets = ctx.schedule.hold({ api: 'ListOffsets' })
        const resolving = ctx.manager.resolveOffsets()
        await offsetFetch.arrived()
        offsetFetch.release({
          responses: [
            {
              topic: TOPIC,
              partitions: [
                {
                  partition: 0,
                  offset: '50',
                  committedLeaderEpoch: 7,
                  metadata: null,
                  errorCode: 0,
                },
                {
                  partition: 1,
                  offset: '-1',
                  committedLeaderEpoch: -1,
                  metadata: null,
                  errorCode: 0,
                },
              ],
            },
          ],
        })
        await listOffsets.arrived()

        await ctx.manager.commitOffsets(commitOf(0, committed))
        listOffsets.release(listOffsetsAnswer({ 1: '0' }))
        await expect(resolving).resolves.toBe(false)

        expect(ctx.committedAt(0)).toEqual(committed)
        expect(ctx.committedAt(1)).toEqual('0')
        expect(ctx.manager.lastConsumedEpoch(TOPIC, 0)).toEqual(-1)
        expect(ctx.next(0)).toEqual(committed)
        expect(ctx.next(1)).toEqual('0')
        expect(ctx.owed(0)).toBe(false)
        expect(ctx.owed(1)).toBe(false)
      }
    )

    /** Two commits of one cell that both began before either was answered. */
    const twoCommits = async (ctx) => {
      const first = ctx.schedule.hold({ api: 'OffsetCommit' })
      const second = ctx.schedule.hold({ api: 'OffsetCommit' })
      const older = ctx.manager.commitOffsets(commitOf(0, '50'))
      await first.arrived()
      const newer = ctx.manager.commitOffsets(commitOf(0, '60'))
      await second.arrived()

      return {
        answerOlder: async () => {
          first.release({})
          await older
        },
        answerNewer: async () => {
          second.release({})
          await newer
        },
      }
    }

    test('C6 an older commit answered first does not suppress the newer one', async () => {
      const ctx = await resolved()
      const { answerOlder, answerNewer } = await twoCommits(ctx)

      await answerOlder()
      expect(ctx.committedAt()).toEqual('50')
      await answerNewer()

      expect(ctx.committedAt()).toEqual('60')
      expect(ctx.manager.resolvedOffsetAt(TOPIC, 0)).toBeUndefined()
      expect(ctx.next()).toEqual('60')
      expect(ctx.owed()).toBe(false)
    })

    test('C6b an older commit answered after the newer one is skipped', async () => {
      const ctx = await resolved()
      const { answerOlder, answerNewer } = await twoCommits(ctx)

      await answerNewer()
      await answerOlder()

      expect(ctx.committedAt()).toEqual('60')
      expect(ctx.manager.resolvedOffsetAt(TOPIC, 0)).toBeUndefined()
      expect(ctx.next()).toEqual('60')
      expect(ctx.owed()).toBe(false)
    })

    test.each([
      ['in the order they began', [0, 1]],
      ['in the opposite order', [1, 0]],
    ])(
      'C7 two seeks that both began, answered %s, leave the later seek and its consumed progress',
      async (_, order) => {
        const ctx = await resolved({ autoCommit: true })
        const answers = [
          ctx.schedule.hold({ api: 'OffsetCommit' }),
          ctx.schedule.hold({ api: 'OffsetCommit' }),
        ]
        const seeks = [ctx.manager.seek({ topic: TOPIC, partition: 0, offset: '500' })]
        await answers[0].arrived()
        seeks.push(ctx.manager.seek({ topic: TOPIC, partition: 0, offset: '700' }))
        await answers[1].arrived()

        for (const which of order) {
          answers[which].release({})
          await seeks[which]
          if (which === 1) {
            await ctx.manager.resolveOffsets()
            ctx.manager.resolveOffset({ topic: TOPIC, partition: 0, offset: '700' })
          }
        }

        expect(ctx.next()).toEqual('701')
        expect(ctx.committedAt()).toEqual('700')
        expect(ctx.owed()).toBe(false)
      }
    )

    test('C8 an older commit completing between the newer commit beginning and its answer disqualifies nothing', async () => {
      const ctx = await resolved()
      const { answerOlder, answerNewer } = await twoCommits(ctx)

      await answerOlder()
      await answerNewer()

      expect(ctx.committedAt()).toEqual('60')
      expect(ctx.stored[0]).toEqual('60')
      expect(ctx.next()).toEqual('60')
      expect(ctx.owed()).toBe(false)
    })

    test.each([
      ['older first', true],
      ['newer first', false],
    ])(
      'C9 the commit that began last wins even when it is the lower offset (%s)',
      async (_, olderFirst) => {
        const ctx = await resolved()
        const first = ctx.schedule.hold({ api: 'OffsetCommit' })
        const second = ctx.schedule.hold({ api: 'OffsetCommit' })
        const higher = ctx.manager.commitOffsets(commitOf(0, '60'))
        await first.arrived()
        await ctx.manager.seek({ topic: TOPIC, partition: 0, offset: '50' })
        const lower = ctx.manager.commitOffsets(commitOf(0, '50'))
        await second.arrived()

        const steps = [
          async () => {
            first.release({})
            await higher
          },
          async () => {
            second.release({})
            await lower
          },
        ]
        await steps[olderFirst ? 0 : 1]()
        await steps[olderFirst ? 1 : 0]()

        expect(ctx.committedAt()).toEqual('50')
        expect(ctx.next()).toEqual('50')
        expect(ctx.owed()).toBe(false)
      }
    )

    test('C10 a read that began during an unresolved seek is fenced when the seek applies', async () => {
      const ctx = createScheduledOffsetManager({ topicId: NEW_ID, autoCommit: true })
      const commitAnswer = ctx.schedule.hold({ api: 'OffsetCommit' })
      const read = ctx.schedule.hold({ api: 'OffsetFetch' })
      const seeking = ctx.manager.seek({ topic: TOPIC, partition: 0, offset: '500' })
      await commitAnswer.arrived()
      const resolving = ctx.manager.resolveOffsets()
      await read.arrived()

      commitAnswer.release({})
      await seeking
      read.release(offsetFetchAnswer({ 0: '99' }))

      await expect(resolving).resolves.toBe(false)
      expect(ctx.cache[TOPIC]).toEqual({})

      await ctx.manager.resolveOffsets()

      expect(ctx.next()).toEqual('500')
      expect(ctx.committedAt()).toEqual('500')
      expect(ctx.owed()).toBe(false)
    })
  })

  describe('a mutation applied by another participant settles the reset', () => {
    /** Consumer A (the fixture) owes a reset for p0; another participant of the group writes p0. */
    const owingConsumer = (options) => {
      const ctx = createScheduledOffsetManager({ topicId: NEW_ID, ...options })
      ctx.recreate(NEW_ID)
      return ctx
    }

    /** What consumer A does after a rebalance: a manager built afresh resolves from the broker. */
    const resolveAfterRebuild = async (ctx) => {
      const rebuilt = ctx.rebuild()
      await rebuilt.resolveOffsets()
      return rebuilt
    }

    test('C11 a commit by another consumer of the group settles the reset, so no rewind follows', async () => {
      const ctx = owingConsumer()
      const sibling = ctx.build({ obligations: new ResetObligations() })

      await sibling.commitOffsets(commitOf(0, '70'))
      const rebuilt = await resolveAfterRebuild(ctx)

      expect(rebuilt.nextOffset(TOPIC, 0).toString()).toEqual('70')
      expect(ctx.committedAt()).toEqual('70')
      expect(ctx.obligations.size()).toBe(0)
      expect(ctx.cluster.fetchTopicsOffset).not.toHaveBeenCalled()
    })

    test('C12 a confirmed transaction publication settles the reset, so no rewind follows', async () => {
      const ctx = owingConsumer()
      const publication = ctx.operations.begin([{ topic: TOPIC, partition: 0 }], { mutation: true })
      ctx.stored[0] = '70'

      ctx.cache[TOPIC][0] = '70'
      ctx.operations.apply(publication, TOPIC, 0)
      ctx.operations.end(publication)
      const rebuilt = await resolveAfterRebuild(ctx)

      expect(rebuilt.nextOffset(TOPIC, 0).toString()).toEqual('70')
      expect(ctx.committedAt()).toEqual('70')
      expect(ctx.obligations.size()).toBe(0)
      expect(ctx.cluster.fetchTopicsOffset).not.toHaveBeenCalled()
    })

    test('C13 a mutation settles the reset of an unknown topic id, and one made under an older incarnation settles nothing of the newer', async () => {
      const ctx = owingConsumer()
      ctx.setTopicId(undefined)
      const sibling = ctx.build({ obligations: new ResetObligations() })
      await sibling.commitOffsets(commitOf(0, '70'))
      expect(ctx.owed()).toBe(false)

      const older = ctx.operations.begin([{ topic: TOPIC, partition: 0 }], { mutation: true })
      ctx.obligations.owe(TOPIC, OLD_ID)
      ctx.operations.touchTopic(TOPIC)

      expect(ctx.operations.isSuperseded(older, TOPIC, 0)).toBe(true)
      expect(ctx.obligations.current(TOPIC)).toEqual(OLD_ID)
      expect(ctx.owed()).toBe(true)
      ctx.operations.end(older)
      expect(ctx.next()).toEqual('70')
      expect(ctx.committedAt()).toEqual('70')
    })

    test("C14 a consumer that disconnects leaves another participant's open publication fence alone", async () => {
      const ctx = owingConsumer()
      const publication = ctx.operations.begin([{ topic: TOPIC, partition: 0 }], { mutation: true })

      ctx.obligations.clear()

      expect(ctx.obligations.size()).toBe(0)
      expect(ctx.operations.size()).toBe(1)
      expect(ctx.operations.isSuperseded(publication, TOPIC, 0)).toBe(false)

      await ctx.manager.seek({ topic: TOPIC, partition: 0, offset: '5' })
      expect(ctx.operations.isSuperseded(publication, TOPIC, 0)).toBe(true)
      ctx.operations.end(publication)
      expect(ctx.operations.size()).toBe(0)
      expect(ctx.next()).toEqual('5')
      expect(ctx.committedAt()).toBeUndefined()
      expect(ctx.owed()).toBe(false)
    })

    test("C14 a consumer that stops with a pending sentinel leaves another participant's open publication fence alone", async () => {
      const ctx = owingConsumer()
      await ctx.manager.seek({ topic: TOPIC, partition: 0, offset: '-2' })
      const publication = ctx.operations.begin([{ topic: TOPIC, partition: 0 }], { mutation: true })

      groupOver(ctx).onStopRequested()

      expect(ctx.manager.pendingSeeks.size).toBe(0)
      expect(ctx.operations.isSuperseded(publication, TOPIC, 0)).toBe(false)

      ctx.cache[TOPIC][0] = '70'
      ctx.operations.apply(publication, TOPIC, 0)
      ctx.operations.end(publication)

      expect(ctx.operations.size()).toBe(0)
      expect(ctx.committedAt()).toEqual('70')
      expect(ctx.owed()).toBe(false)
    })

    test("C14 a consumer that stops with a pending sentinel leaves another consumer's open commit alone", async () => {
      const ctx = owingConsumer()
      const sibling = ctx.build({ obligations: new ResetObligations() })
      await ctx.manager.seek({ topic: TOPIC, partition: 0, offset: '-2' })
      const answer = ctx.schedule.hold({ api: 'OffsetCommit' })
      const committing = sibling.commitOffsets(commitOf(0, '70'))
      await answer.arrived()

      groupOver(ctx).onStopRequested()
      answer.release({})
      await committing

      expect(ctx.committedAt()).toEqual('70')
      expect(ctx.owed()).toBe(false)
      expect(ctx.operations.size()).toBe(0)
    })

    test('AR1 an aborted transaction publishes nothing, so the resolver in flight applies its answer', async () => {
      const ctx = createScheduledOffsetManager({ topicId: NEW_ID })
      const offsetFetch = ctx.schedule.hold({ api: 'OffsetFetch' })
      const resolving = ctx.manager.resolveOffsets()
      await offsetFetch.arrived()
      const staged = ctx.operations.begin([{ topic: TOPIC, partition: 0 }], { mutation: true })

      ctx.operations.end(staged)
      offsetFetch.release(offsetFetchAnswer({ 0: '99' }))
      await expect(resolving).resolves.toBe(true)

      expect(ctx.next()).toEqual('99')
      expect(ctx.committedAt()).toEqual('99')
      expect(ctx.operations.size()).toBe(0)
      expect(ctx.owed()).toBe(false)
    })

    test('AR2 a publication confirmed after a resolver began is the later applied mutation and supersedes it', async () => {
      const ctx = createScheduledOffsetManager({ topicId: NEW_ID })
      ctx.recreate(NEW_ID)
      const listOffsets = ctx.schedule.hold({ api: 'ListOffsets' })
      const resolving = ctx.manager.resolveOffsets()
      await listOffsets.arrived()
      const publication = ctx.operations.begin([{ topic: TOPIC, partition: 0 }], { mutation: true })

      ctx.cache[TOPIC][0] = '70'
      ctx.operations.apply(publication, TOPIC, 0)
      ctx.operations.end(publication)
      listOffsets.release(listOffsetsAnswer({ 0: '0' }))

      await expect(resolving).resolves.toBe(false)
      expect(ctx.next()).toEqual('70')
      expect(ctx.committedAt()).toEqual('70')
      expect(ctx.owed()).toBe(false)
    })
  })

  describe('retained state follows the outstanding work', () => {
    test('the steady state makes no ledger call: neither resolveOffset nor a resolution with nothing pending opens a ticket', async () => {
      const ctx = createScheduledOffsetManager({ topicId: NEW_ID })
      await ctx.manager.resolveOffsets()
      const begin = jest.spyOn(ctx.operations, 'begin')

      for (let offset = 0; offset < 1000; offset++) {
        ctx.manager.resolveOffset({ topic: TOPIC, partition: 0, offset: String(offset) })
      }
      await ctx.manager.resolveOffsets()

      expect(begin).not.toHaveBeenCalled()
    })

    test('a commit opens one ticket for the cells it writes and leaves none behind', async () => {
      const ctx = createScheduledOffsetManager({ topicId: NEW_ID })
      await ctx.manager.resolveOffsets()
      const begin = jest.spyOn(ctx.operations, 'begin')

      await ctx.manager.commitOffsets(commitOf(0, '50'))

      expect(begin).toHaveBeenCalledTimes(1)
      expect(ctx.operations.size()).toBe(0)
    })

    test('L06 two hundred recreated topic names leave no entry behind, before and after a disconnect and reuse', async () => {
      const names = Array.from({ length: 200 }, (_, index) => `recreated-${index}`)
      const cache = {}
      const operations = new OffsetOperations()
      const start = jest.spyOn(Runner.prototype, 'start').mockResolvedValue()
      const consumer = createConsumer({
        cluster: {
          onTopicRecreated: () => () => {},
          offsetOperations: () => operations,
          addMultipleTargetTopics: async () => {},
          disconnect: async () => {},
        },
        groupId: GROUP_ID,
        logger: newLogger(),
        instrumentationEmitter: new InstrumentationEventEmitter(),
        metadataMaxAge: 300000,
        retry: { retries: 0 },
      })

      try {
        await consumer.subscribe({ topics: names, fromBeginning: true })
        await consumer.run({ eachMessage: async () => {}, autoCommit: false })
        const obligations = start.mock.instances.at(-1).consumerGroup.obligations
        const answerFor = (topics, offset) =>
          topics.map(({ topic, partitions }) => ({
            topic,
            partitions: partitions.map(({ partition }) => ({
              partition,
              offset,
              committedLeaderEpoch: -1,
              metadata: null,
              errorCode: 0,
            })),
          }))
        const manager = new OffsetManager({
          cluster: {
            committedOffsets: () => cache,
            topicId: () => NEW_ID,
            fetchTopicsOffset: async (topics) => answerFor(topics, '0'),
          },
          coordinator: {
            isConnected: () => true,
            offsetFetch: async ({ topics }) => ({ responses: answerFor(topics, '99') }),
          },
          memberAssignment: Object.fromEntries(names.map((name) => [name, [0]])),
          autoCommit: false,
          topicConfigurations: Object.fromEntries(
            names.map((name) => [name, { fromBeginning: true }])
          ),
          instrumentationEmitter: new InstrumentationEventEmitter(),
          groupId: 'test-group',
          generationId: 1,
          memberId: 'member-a',
          operations,
          obligations,
        })

        const recreateAndResolveEverything = async () => {
          for (const name of names) {
            obligations.owe(name, NEW_ID)
            operations.touchTopic(name)
            manager.resetOffsetsForTopic(name)
          }
          expect(obligations.size()).toBe(200)

          await manager.resolveOffsets()

          expect(obligations.size()).toBe(0)
          expect(operations.size()).toBe(0)
          expect(names.every((name) => cache[name][0] === '0')).toBe(true)
          expect(manager.nextOffset(names[0], 0).toString()).toEqual('0')
        }

        await recreateAndResolveEverything()
        manager.updateAssignment({
          revoked: names.map((topic) => ({ topic, partitions: [0] })),
          memberAssignment: {},
          generationId: 2,
          memberId: 'member-a',
          coordinator: manager.coordinator,
        })
        expect(operations.size()).toBe(0)

        expect(obligations.size()).toBe(0)
        await consumer.disconnect()
        expect(obligations.size()).toBe(0)
        expect(operations.size()).toBe(0)
        expect(operations.attached.has(obligations)).toBe(false)
        await consumer.run({ eachMessage: async () => {}, autoCommit: false })
        expect(start.mock.instances.at(-1).consumerGroup.obligations).toBe(obligations)
        expect(operations.attached.has(obligations)).toBe(true)
        manager.updateAssignment({
          revoked: [],
          memberAssignment: Object.fromEntries(names.map((name) => [name, [0]])),
          generationId: 3,
          memberId: 'member-a',
          coordinator: manager.coordinator,
        })
        await recreateAndResolveEverything()
      } finally {
        try {
          await consumer.disconnect()
        } finally {
          start.mockRestore()
        }
      }
    })
  })

  describe('what fences an answer, and what ends a ticket', () => {
    test('a full-topic revoke during ListOffsets drops the answer and preserves the reset for reassignment', async () => {
      const ctx = createScheduledOffsetManager({ topicId: NEW_ID })
      ctx.recreate(NEW_ID)
      const held = ctx.schedule.hold({ api: 'ListOffsets' })
      const resolving = ctx.manager.resolveOffsets()
      await held.arrived()

      const membership = {
        generationId: 2,
        memberId: 'member-a',
        coordinator: ctx.coordinator,
      }
      ctx.manager.updateAssignment({
        revoked: [{ topic: TOPIC, partitions: [0] }],
        memberAssignment: {},
        ...membership,
      })
      held.release(listOffsetsAnswer({ 0: '500' }))

      await expect(resolving).resolves.toBe(false)
      expect(ctx.committedAt()).toBeUndefined()
      expect(ctx.manager.resolvedOffsetAt(TOPIC, 0)).toBeUndefined()
      expect(ctx.owed()).toBe(true)
      expect(ctx.operations.size()).toBe(0)

      ctx.manager.updateAssignment({
        revoked: [],
        memberAssignment: { [TOPIC]: [0] },
        ...membership,
      })
      await expect(ctx.manager.resolveOffsets()).resolves.toBe(true)
      expect(ctx.next()).toEqual('0')
      expect(ctx.committedAt()).toEqual('0')
      expect(ctx.obligations.size()).toBe(0)
      expect(ctx.operations.size()).toBe(0)
    })

    test('an assignment rebuild fences the operations of the manager it replaces', async () => {
      const ctx = createScheduledOffsetManager({ topicId: NEW_ID })
      const offsetFetch = ctx.schedule.hold({ api: 'OffsetFetch' })
      const resolving = ctx.manager.resolveOffsets()
      await offsetFetch.arrived()

      ctx.rebuild()
      offsetFetch.release(offsetFetchAnswer({ 0: '99' }))

      await expect(resolving).resolves.toBe(false)
      expect(ctx.cache[TOPIC]).toEqual({})
    })

    test('a reset of the topic offsets fences a resolution in flight', async () => {
      const ctx = createScheduledOffsetManager({ topicId: NEW_ID })
      const offsetFetch = ctx.schedule.hold({ api: 'OffsetFetch' })
      const resolving = ctx.manager.resolveOffsets()
      await offsetFetch.arrived()

      ctx.manager.resetOffsetsForTopic(TOPIC)
      offsetFetch.release(offsetFetchAnswer({ 0: '99' }))

      await expect(resolving).resolves.toBe(false)
      expect(ctx.cache[TOPIC]).toEqual({})
    })

    test('a topic id that changed while the request was in flight drops the answer even without a recreation event', async () => {
      const ctx = createScheduledOffsetManager({ topicId: OLD_ID })
      const offsetFetch = ctx.schedule.hold({ api: 'OffsetFetch' })
      const resolving = ctx.manager.resolveOffsets()
      await offsetFetch.arrived()

      ctx.setTopicId(NEW_ID)
      offsetFetch.release(offsetFetchAnswer({ 0: '99' }))

      await expect(resolving).resolves.toBe(false)
      expect(ctx.cache[TOPIC]).toEqual({})
    })

    test.each([
      ['a commit', (ctx) => ctx.manager.commitOffsets(commitOf(0, '50'))],
      ['a seek', (ctx) => ctx.manager.seek({ topic: TOPIC, partition: 0, offset: '500' })],
    ])('%s that fails applies nothing and supersedes nothing', async (_, mutation) => {
      const ctx = createScheduledOffsetManager({ topicId: NEW_ID, autoCommit: true })
      ctx.recreate(NEW_ID)
      const listOffsets = ctx.schedule.hold({ api: 'ListOffsets' })
      const resolving = ctx.manager.resolveOffsets()
      await listOffsets.arrived()
      ctx.schedule.once({ api: 'OffsetCommit' }, new Error('commit rejected'))

      await expect(mutation(ctx)).rejects.toThrow('commit rejected')
      listOffsets.release(listOffsetsAnswer({ 0: '0' }))

      await expect(resolving).resolves.toBe(true)
      expect(ctx.committedAt()).toEqual('0')
      expect(ctx.owed()).toBe(false)
    })

    test.each([
      ['a commit', (ctx) => ctx.manager.commitOffsets(commitOf(0, '50')), 'OffsetCommit'],
      [
        'a seek',
        (ctx) => ctx.manager.seek({ topic: TOPIC, partition: 0, offset: '500' }),
        'OffsetCommit',
      ],
      ['a resolution', (ctx) => ctx.manager.resolveOffsets(), 'OffsetFetch'],
    ])('%s that fails ends its ticket', async (_, operation, api) => {
      const ctx = createScheduledOffsetManager({ topicId: NEW_ID, autoCommit: true })
      ctx.schedule.once({ api }, new Error('rejected'))

      await expect(operation(ctx)).rejects.toThrow('rejected')

      expect(ctx.operations.size()).toBe(0)
    })

    test('a seek without auto-commit that throws while resolving its position ends its ticket and settles nothing', async () => {
      const ctx = createScheduledOffsetManager({ topicId: NEW_ID, autoCommit: false })
      ctx.recreate(NEW_ID)

      await expect(
        ctx.manager.seek({ topic: TOPIC, partition: 0, offset: 'not-an-offset' })
      ).rejects.toThrow()

      expect(ctx.operations.size()).toBe(0)
      expect(ctx.owed()).toBe(true)
    })
  })

  describe('through the ConsumerGroup', () => {
    test('P07 a seek applied by the group between two resolvers survives the second one', async () => {
      const ctx = createScheduledOffsetManager({ topicId: NEW_ID, autoCommit: true })
      ctx.recreate(NEW_ID)
      const group = groupOver(ctx)
      const first = ctx.schedule.hold({ api: 'OffsetFetch' })
      const second = ctx.schedule.hold({ api: 'OffsetFetch' })
      const listOffsets = ctx.schedule.hold({ api: 'ListOffsets' })
      const one = ctx.manager.resolveOffsets()
      await first.arrived()
      const two = ctx.manager.resolveOffsets()
      await second.arrived()
      first.release(offsetFetchAnswer({ 0: '99' }))
      await listOffsets.arrived()
      listOffsets.release(listOffsetsAnswer({ 0: '0' }))
      await one

      group.seek({ topic: TOPIC, partition: 0, offset: '500' })
      await group.seekOffsets([{ topic: TOPIC, partitions: [0] }])
      second.release(offsetFetchAnswer({ 0: '99' }))
      await expect(two).resolves.toBe(false)

      expect(ctx.next()).toEqual('500')
      expect(ctx.committedAt()).toEqual('500')
      expect(ctx.owed()).toBe(false)
    })

    test.each(Array.from({ length: 9 }, (_, ticks) => [ticks]))(
      'N11 a seek applied while the sole resolver waits is not overwritten, whichever microtask the resolver answer lands on (%i ticks after the commit answer)',
      async (ticks) => {
        const ctx = createScheduledOffsetManager({ topicId: NEW_ID, autoCommit: true })
        ctx.recreate(NEW_ID)
        const group = groupOver(ctx)
        const listOffsets = ctx.schedule.hold({ api: 'ListOffsets' })
        const resolving = ctx.manager.resolveOffsets()
        await listOffsets.arrived()
        ctx.schedule.once({ api: 'OffsetCommit' }, () => {
          let remaining = ticks
          const land = () =>
            remaining-- > 0
              ? queueMicrotask(land)
              : listOffsets.release(listOffsetsAnswer({ 0: '0' }))
          queueMicrotask(land)
          return {}
        })

        group.seek({ topic: TOPIC, partition: 0, offset: '500' })
        await group.seekOffsets([{ topic: TOPIC, partitions: [0] }])
        await resolving

        expect(ctx.next()).toEqual('500')
        expect(ctx.committedAt()).toEqual('500')
        expect(ctx.owed()).toBe(false)
      }
    )

    test('N05 the broker whose resolution the first one overtook fetches at once, not after an empty fetch wait', async () => {
      const ctx = createScheduledOffsetManager({
        partitions: [0, 1],
        topicId: NEW_ID,
        autoCommit: false,
      })
      ctx.recreate(NEW_ID)
      const harness = await createFetchGroup({
        partitions: [0, 1],
        knownNodes: [0, 1],
        partitionLeaders: { 0: 0, 1: 1 },
      })
      harness.group.offsetManager = ctx.manager
      const [firstFetch, secondFetch] = [
        ctx.schedule.hold({ api: 'OffsetFetch' }),
        ctx.schedule.hold({ api: 'OffsetFetch' }),
      ]
      const firstList = ctx.schedule.hold({ api: 'ListOffsets' })
      jest.useFakeTimers({ doNotFake: ['setImmediate', 'nextTick', 'queueMicrotask'] })

      try {
        const owner = harness.group.fetch(0)
        await firstFetch.arrived()
        const other = harness.group.fetch(1)
        await secondFetch.arrived()
        firstFetch.release(offsetFetchAnswer({ 0: '99', 1: '99' }))
        await firstList.arrived()

        harness.setResponse(fetchResponse([{ partition: 1, errorCode: 0 }]))
        secondFetch.release(offsetFetchAnswer({ 0: '99', 1: '99' }))
        await other

        expect(harness.fetchRequestPartitions()).toEqual([1])
        expect(harness.fetchRequestOffsets()).toEqual(['0'])
        expect(jest.getTimerCount()).toBe(0)

        harness.setResponse(fetchResponse([{ partition: 0, errorCode: 0 }]))
        firstList.release(listOffsetsAnswer({ 0: '0', 1: '0' }))
        await owner
        expect(harness.fetchRequestPartitions()).toEqual([0])
        expect(harness.fetchRequestOffsets()).toEqual(['0'])
        expect(jest.getTimerCount()).toBe(0)
        expect(ctx.committedAt(0)).toEqual('0')
        expect(ctx.committedAt(1)).toEqual('0')
        expect(ctx.obligations.size()).toBe(0)
      } finally {
        jest.useRealTimers()
        await harness.broker.disconnect()
      }
    })

    test('a round whose first resolution dropped an answer fetches at once from the repeat', async () => {
      const ctx = createScheduledOffsetManager({ topicId: NEW_ID })
      const harness = await createFetchGroup({ partitions: [0] })
      harness.group.offsetManager = ctx.manager
      const offsetFetch = ctx.schedule.hold({ api: 'OffsetFetch' })
      jest.useFakeTimers({ doNotFake: ['setImmediate', 'nextTick', 'queueMicrotask'] })

      try {
        const round = harness.group.fetch(0)
        await offsetFetch.arrived()
        ctx.recreate(NEW_ID)
        harness.setResponse(fetchResponse([{ partition: 0, errorCode: 0 }]))
        offsetFetch.release(offsetFetchAnswer({ 0: '99' }))
        await round

        expect(harness.fetchRequestOffsets()).toEqual(['0'])
        expect(ctx.cluster.fetchTopicsOffset).toHaveBeenCalledTimes(1)
        expect(jest.getTimerCount()).toBe(0)
      } finally {
        jest.useRealTimers()
        await harness.broker.disconnect()
      }
    })

    test.each([
      ['every pass drops', [false, false, false, false], 3],
      ['the second pass keeps every answer', [false, true, false], 2],
      ['the first pass keeps every answer', [true, false], 1],
    ])(
      'resolution is repeated at most three times, and only while answers are dropped (%s)',
      async (_, results, calls) => {
        const ctx = createScheduledOffsetManager({ topicId: NEW_ID })
        const group = groupOver(ctx)
        const resolve = jest.spyOn(ctx.manager, 'resolveOffsets')
        results.forEach((result) => resolve.mockResolvedValueOnce(result))

        await group.seekOffsets([{ topic: TOPIC, partitions: [0] }])

        expect(resolve).toHaveBeenCalledTimes(calls)
      }
    )

    test('PT1 the consumer hands one table of obligations and the cluster ledger to its group and the group to its offset manager', async () => {
      const operations = new OffsetOperations()
      const cluster = {
        connect: async () => {},
        disconnect: async () => {},
        addMultipleTargetTopics: async () => {},
        refreshMetadataIfNecessary: async () => {},
        committedOffsets: () => ({}),
        offsetOperations: jest.fn(() => operations),
        onTopicRecreated: () => () => {},
        findTopicPartitionMetadata: () => [{ partitionId: 0 }],
      }
      const start = jest.spyOn(Runner.prototype, 'start').mockResolvedValue()
      try {
        const consumer = createConsumer({
          cluster,
          groupId: GROUP_ID,
          logger: newLogger(),
          instrumentationEmitter: new InstrumentationEventEmitter(),
          metadataMaxAge: 300000,
          retry: { retries: 0 },
        })
        await consumer.subscribe({ topic: TOPIC })
        await consumer.run({ eachMessage: async () => {} })
        const group = start.mock.instances[0].consumerGroup
        group.partitionsPerSubscribedTopic = group.generatePartitionsPerSubscribedTopic()

        await group.applyRebalanceResult({ assignment: { [TOPIC]: [0] } }, { rebuildOffsets: true })

        expect(cluster.offsetOperations).toHaveBeenCalledWith({ groupId: GROUP_ID })
        expect(group.operations).toBe(operations)
        expect(group.offsetManager.operations).toBe(operations)
        expect(group.offsetManager.obligations).toBe(group.obligations)

        group.offsetManager.committedOffsets()[TOPIC][0] = '0'
        expect(group.offsetManager.nextOffset(TOPIC, 0).toString()).toEqual('0')
        expect(group.offsetManager.committedOffsets()[TOPIC][0]).toEqual('0')
        group.obligations.owe(TOPIC, NEW_ID)
        const ticket = operations.begin([{ topic: TOPIC, partition: 0 }], { mutation: true })
        operations.apply(ticket, TOPIC, 0)
        expect(group.obligations.isOwed(TOPIC, 0)).toBe(false)
        operations.end(ticket)
      } finally {
        start.mockRestore()
      }
    })
  })
})
