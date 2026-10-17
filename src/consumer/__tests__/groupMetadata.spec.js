jest.mock('../consumerGroup', () => jest.fn())
jest.mock('../runner', () => jest.fn())

const ConsumerGroup = require('../consumerGroup')
const Runner = require('../runner')
const createConsumer = require('../index')
const InstrumentationEventEmitter = require('../../instrumentation/emitter')
const OffsetOperations = require('../offsetManager/offsetOperations')
const channels = require('../../instrumentation/channels')
const { events } = require('../instrumentationEvents')
const { KafkaJSNonRetriableError } = require('../../errors')
const { newLogger, waitFor } = require('testHelpers')

const GROUP_ID = 'test-group'
const NOT_RUNNING = 'Consumer group was not initialized, consumer#run must be called first'
const eachMessage = async () => {}

/**
 * A consumer whose `ConsumerGroup` is a plain object carrying the five accessors `groupMetadata()`
 * reads, and whose `Runner` does nothing. `join` does what a completed rebalance round does: it
 * moves the membership, calls the runner's `onJoinComplete`, then emits `GROUP_JOIN` on the
 * consumer's instrumentation emitter, in the order the rebalance coordinator does. `crash` hands
 * the runner's `onCrash` a retriable error, as a dying runner does, so the consumer restarts
 * after 1 ms.
 * @param {{ groupProtocol?: 'classic'|'consumer', groupInstanceId?: string|null }} [options]
 * @returns {{ consumer: object, membership: object, join: (fields: object) => void, crash: () => Promise<void> }}
 */
const build = ({ groupProtocol = 'classic', groupInstanceId = null } = {}) => {
  const consumerProtocol = groupProtocol === 'consumer'
  const membership = {
    kind: groupProtocol,
    generationId: consumerProtocol ? 0 : null,
    memberId: consumerProtocol ? 'generated-member-id' : null,
    memberEpoch: consumerProtocol ? 0 : null,
    groupInstanceId,
  }
  let runnerOptions
  ConsumerGroup.mockImplementation(() => membership)
  Runner.mockImplementation((options) => {
    runnerOptions = options
    return { start: async () => {}, stop: async () => {} }
  })

  const instrumentationEmitter = new InstrumentationEventEmitter({ channels: channels.consumer })
  const consumer = createConsumer({
    cluster: {
      onTopicRecreated: () => () => {},
      offsetOperations: () => new OffsetOperations(),
      disconnect: async () => {},
    },
    groupId: GROUP_ID,
    logger: newLogger(),
    instrumentationEmitter,
    groupProtocol,
    groupInstanceId,
  })

  const join = (fields) => {
    Object.assign(membership, fields)
    runnerOptions.onJoinComplete()
    instrumentationEmitter.emit(events.GROUP_JOIN, {
      groupId: GROUP_ID,
      memberId: membership.memberId,
    })
  }

  const crash = () =>
    runnerOptions.onCrash(Object.assign(new Error('crashed'), { retriable: true, retryTime: 1 }))

  return { consumer, membership, join, crash }
}

describe('Consumer > groupMetadata', () => {
  afterEach(() => {
    ConsumerGroup.mockReset()
    Runner.mockReset()
  })

  test.each(['classic', 'consumer'])(
    'throws before run(), as seek() and pause() do (%s)',
    (groupProtocol) => {
      const { consumer } = build({ groupProtocol })

      expect(() => consumer.groupMetadata()).toThrow(KafkaJSNonRetriableError)
      expect(() => consumer.groupMetadata()).toThrow(NOT_RUNNING)
    }
  )

  test.each(['classic', 'consumer'])(
    'before the first join it reports no generation and no member id (%s)',
    async (groupProtocol) => {
      const { consumer } = build({ groupProtocol })
      await consumer.run({ eachMessage })

      expect(consumer.groupMetadata()).toEqual({
        groupId: GROUP_ID,
        generationId: -1,
        memberId: '',
        groupInstanceId: null,
      })
    }
  )

  test('a static member reports its instance id from the start', async () => {
    const { consumer } = build({ groupInstanceId: 'instance-1' })
    await consumer.run({ eachMessage })

    expect(consumer.groupMetadata()).toEqual({
      groupId: GROUP_ID,
      generationId: -1,
      memberId: '',
      groupInstanceId: 'instance-1',
    })
  })

  test('classic: after a join it reports that generation and member id', async () => {
    const { consumer, join } = build()
    await consumer.run({ eachMessage })
    join({ generationId: 5, memberId: 'member-a' })

    expect(consumer.groupMetadata()).toEqual({
      groupId: GROUP_ID,
      generationId: 5,
      memberId: 'member-a',
      groupInstanceId: null,
    })
  })

  test('classic: after a rebalance it reports the new generation', async () => {
    const { consumer, join } = build()
    await consumer.run({ eachMessage })
    join({ generationId: 5, memberId: 'member-a' })
    join({ generationId: 6 })

    expect(consumer.groupMetadata()).toMatchObject({ generationId: 6, memberId: 'member-a' })
  })

  test('classic: a lost generation keeps the last joined pair, so a commit built from it stays fenced', async () => {
    const { consumer, membership, join } = build()
    await consumer.run({ eachMessage })
    join({ generationId: 5, memberId: 'member-a' })

    // What a poll-interval overrun or UNKNOWN_MEMBER_ID does to the live classic membership.
    Object.assign(membership, { generationId: null, memberId: null })

    expect(consumer.groupMetadata()).toMatchObject({ generationId: 5, memberId: 'member-a' })
  })

  test('consumer protocol: generationId is the member epoch, read live', async () => {
    const { consumer, membership, join } = build({ groupProtocol: 'consumer' })
    await consumer.run({ eachMessage })
    join({ memberEpoch: 7, generationId: 3 })

    expect(consumer.groupMetadata()).toEqual({
      groupId: GROUP_ID,
      generationId: 7,
      memberId: 'generated-member-id',
      groupInstanceId: null,
    })

    // A heartbeat that moves the epoch without changing this member's assignment completes no round.
    // The two accessors differ on purpose: only the member epoch may be read under this protocol.
    Object.assign(membership, { memberEpoch: 9, generationId: 4 })

    expect(consumer.groupMetadata().generationId).toEqual(9)
  })

  test('classic: a restarted consumer reports the previous membership until its next join', async () => {
    const { consumer, join } = build()
    await consumer.run({ eachMessage })
    join({ generationId: 5, memberId: 'member-a' })
    await consumer.stop()

    ConsumerGroup.mockImplementation(() => ({
      kind: 'classic',
      generationId: null,
      memberId: null,
      memberEpoch: null,
      groupInstanceId: null,
    }))
    await consumer.run({ eachMessage })

    expect(consumer.groupMetadata()).toMatchObject({ generationId: 5, memberId: 'member-a' })
  })

  test('classic: a consumer restarted after a crash reports the previous membership until its next join', async () => {
    const { consumer, join, crash } = build()
    await consumer.run({ eachMessage })
    join({ generationId: 5, memberId: 'member-a' })

    ConsumerGroup.mockImplementation(() => ({
      kind: 'classic',
      generationId: null,
      memberId: null,
      memberEpoch: null,
      groupInstanceId: null,
    }))
    await crash()
    await waitFor(() => ConsumerGroup.mock.calls.length === 2)

    expect(consumer.groupMetadata()).toMatchObject({ generationId: 5, memberId: 'member-a' })
  })

  test('consumer protocol: a restarted consumer reports its new membership live', async () => {
    const { consumer, join } = build({ groupProtocol: 'consumer' })
    await consumer.run({ eachMessage })
    join({ memberEpoch: 7 })
    await consumer.stop()

    ConsumerGroup.mockImplementation(() => ({
      kind: 'consumer',
      generationId: 0,
      memberId: 'second-member-id',
      memberEpoch: 0,
      groupInstanceId: null,
    }))
    await consumer.run({ eachMessage })

    expect(consumer.groupMetadata()).toMatchObject({
      generationId: 0,
      memberId: 'second-member-id',
    })
  })

  test.each(['classic', 'consumer'])(
    'a static member keeps its instance id after a join (%s)',
    async (groupProtocol) => {
      const { consumer, join } = build({ groupProtocol, groupInstanceId: 'instance-1' })
      await consumer.run({ eachMessage })
      join({ generationId: 5, memberId: 'member-a', memberEpoch: 5 })

      expect(consumer.groupMetadata().groupInstanceId).toEqual('instance-1')
    }
  )

  test('each call returns a new plain snapshot that a later rebalance does not change', async () => {
    const { consumer, join } = build()
    await consumer.run({ eachMessage })
    join({ generationId: 5, memberId: 'member-a' })

    const snapshot = consumer.groupMetadata()
    join({ generationId: 6 })

    expect(snapshot.generationId).toEqual(5)
    expect(consumer.groupMetadata()).not.toBe(snapshot)
    expect(Object.getPrototypeOf(snapshot)).toBe(Object.prototype)
    expect(JSON.parse(JSON.stringify(snapshot))).toEqual(snapshot)
  })

  test('a GROUP_JOIN listener of the application still receives the event', async () => {
    const { consumer, join } = build()
    const payloads = []
    consumer.on(consumer.events.GROUP_JOIN, ({ payload }) => payloads.push(payload))
    await consumer.run({ eachMessage })
    join({ generationId: 5, memberId: 'member-a' })

    expect(payloads).toEqual([{ groupId: GROUP_ID, memberId: 'member-a' }])
  })

  describe('observers of GROUP_JOIN', () => {
    const channel = channels.consumer[events.GROUP_JOIN]
    let unsubscribe

    /**
     * Records what `groupMetadata()` returns inside each `GROUP_JOIN` delivery, once through the
     * `kafkajs.consumer.group_join` channel and once through an `on()` listener.
     * @param {object} consumer
     * @returns {{ channel: object[], listener: object[] }}
     */
    const observe = (consumer) => {
      const seen = { channel: [], listener: [] }
      const subscriber = () => seen.channel.push(consumer.groupMetadata())
      channel.subscribe(subscriber)
      unsubscribe = () => channel.unsubscribe(subscriber)
      consumer.on(consumer.events.GROUP_JOIN, () => seen.listener.push(consumer.groupMetadata()))
      return seen
    }

    afterEach(() => unsubscribe && unsubscribe())

    test('classic: a first join and a rejoin give both kinds of observer the new pair', async () => {
      const { consumer, join } = build()
      const seen = observe(consumer)
      await consumer.run({ eachMessage })

      join({ generationId: 5, memberId: 'member-a' })
      join({ generationId: 6 })

      const pairs = (snapshots) =>
        snapshots.map(({ generationId, memberId }) => ({ generationId, memberId }))
      const expected = [
        { generationId: 5, memberId: 'member-a' },
        { generationId: 6, memberId: 'member-a' },
      ]
      expect(pairs(seen.channel)).toEqual(expected)
      expect(pairs(seen.listener)).toEqual(expected)
    })

    test('classic: a lost membership keeps the observed pair until the next join replaces it', async () => {
      const { consumer, membership, join } = build()
      const seen = observe(consumer)
      await consumer.run({ eachMessage })
      join({ generationId: 5, memberId: 'member-a' })

      Object.assign(membership, { generationId: null, memberId: null })
      expect(consumer.groupMetadata()).toMatchObject({ generationId: 5, memberId: 'member-a' })

      join({ generationId: 7, memberId: 'member-b' })

      expect(seen.channel[1]).toMatchObject({ generationId: 7, memberId: 'member-b' })
      expect(seen.listener[1]).toMatchObject({ generationId: 7, memberId: 'member-b' })
      expect(consumer.groupMetadata()).toMatchObject({ generationId: 7, memberId: 'member-b' })
    })

    test('consumer protocol: a first join gives both kinds of observer the live member epoch', async () => {
      const { consumer, join } = build({ groupProtocol: 'consumer' })
      const seen = observe(consumer)
      await consumer.run({ eachMessage })

      join({ memberEpoch: 3 })

      for (const snapshot of [seen.channel[0], seen.listener[0]]) {
        expect(snapshot).toMatchObject({ generationId: 3, memberId: 'generated-member-id' })
      }
    })
  })

  test.each(['classic', 'consumer'])('throws again after stop() (%s)', async (groupProtocol) => {
    const { consumer, join } = build({ groupProtocol })
    await consumer.run({ eachMessage })
    join({ generationId: 5, memberId: 'member-a', memberEpoch: 5 })
    await consumer.stop()

    // A round that completes after stop() finds no membership to read.
    join({ generationId: 6 })

    expect(() => consumer.groupMetadata()).toThrow(NOT_RUNNING)
  })
})
