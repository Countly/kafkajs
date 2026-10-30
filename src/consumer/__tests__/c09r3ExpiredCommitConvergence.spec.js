const Runner = require('../runner')
const { KafkaJSDeliveryTimeout } = require('../../errors')
const { CloseRig, until, pause } = require('./fixtures/closeRig')
const { TOPIC, TOPIC_ID, prepareRecordBatches } = require('./fixtures/fetchHarness')

jest.setTimeout(30000)

const COMMIT = { topic: TOPIC, partition: 0, offset: '7' }
const FATAL = [30, 42, 111]
const answerWith = (errorCode) => ({
  topics: [{ topicId: TOPIC_ID, partitions: [{ partition: 0, errorCode }] }],
})

/**
 * A real consumer-protocol consumer over the scripted cluster, whose public commit is held after
 * it was written, expires, and is then answered. The group, offset manager, ownership and rebalance
 * coordinator are the production ones; only the broker's answer is scripted.
 */
class ExpiredCommit {
  /** @param {CloseRig} rig @param {object} consumer @param {Runner} runner */
  constructor(rig, consumer, runner) {
    this.rig = rig
    this.consumer = consumer
    this.runner = runner
    this.group = runner.consumerGroup
    this.manager = this.group.offsetManager
    this.ownership = this.group.ownership
    this.crashes = []
    this.commitEvents = []
    consumer.on(consumer.events.CRASH, ({ payload }) => this.crashes.push(payload))
    consumer.on(consumer.events.COMMIT_OFFSETS, () => this.commitEvents.push(1))
  }

  /** @returns {Promise<ExpiredCommit>} a started member with a short public-call deadline */
  static async open() {
    const rig = await CloseRig.open({ protocol: 'consumer' })
    const start = jest.spyOn(Runner.prototype, 'start')
    const { consumer } = await rig.started({
      consumer: { retry: { maxTotalTime: 400, retries: 1, initialRetryTime: 10 } },
      run: { autoCommit: false },
    })
    return new ExpiredCommit(rig, consumer, start.mock.instances[0])
  }

  /** @returns {object[]} the OffsetCommit frames written so far */
  commits() {
    return this.rig.frames('OffsetCommit')
  }

  /** @returns {object[]} the heartbeats that ask for a fresh membership (epoch 0) */
  rejoins() {
    return this.rig
      .frames('ConsumerGroupHeartbeat')
      .filter(({ fields }) => fields.memberEpoch === 0)
  }

  /**
   * Writes one public commit, lets its own deadline expire, moves the member epoch on within the
   * same membership token, then answers the held request with `errorCode`.
   * @param {number} errorCode what the coordinator answers
   * @param {{ replaceToken?: boolean }} [options] `replaceToken` begins a new membership first
   * @returns {Promise<{ error: Error, token: number }>} the caller's outcome and the sending token
   */
  async answerAfterExpiry(errorCode, { replaceToken = false } = {}) {
    const held = this.rig.cluster.schedule.hold({ api: 'OffsetCommit' })
    const token = this.ownership.token()
    const committing = this.consumer.commitOffsets([COMMIT]).catch((error) => error)
    await held.arrived()
    const error = await committing
    const { groupMembership } = this.group
    groupMembership.memberEpoch += 1
    if (replaceToken) this.ownership.renew()
    held.release(answerWith(errorCode))
    return { error, token }
  }

  /** @returns {Promise<void>} releases every hold and disconnects */
  async dispose() {
    jest.restoreAllMocks()
    await this.rig.dispose()
  }
}

describe('a consumer-protocol commit answered after its public call expired', () => {
  /** @type {ExpiredCommit} */
  let member
  beforeAll(() => prepareRecordBatches())
  beforeEach(async () => {
    member = await ExpiredCommit.open()
  })
  afterEach(async () => {
    await member.dispose()
  })

  /** Asserts what an expired call leaves behind, whatever the answer was. */
  const expectNothingPublished = async (error) => {
    expect(error).toBeInstanceOf(KafkaJSDeliveryTimeout)
    expect(error.message).toContain('retry.maxTotalTime')
    await pause(100)
    expect(member.commits()).toHaveLength(1)
    expect(member.commitEvents).toEqual([])
    expect(member.manager.operations.size()).toBe(0)
  }

  test('code 69 keeps the caller timeout, publishes nothing, and the coordinator fences and rejoins', async () => {
    const rejoin = jest.spyOn(member.runner.rebalanceCoordinator, 'requestRejoin')
    const cached = member.manager.committedOffsets()[TOPIC][0]
    const rejoinsBefore = member.rejoins().length
    const { error, token } = await member.answerAfterExpiry(69)

    await until(() => rejoin.mock.calls.length > 0, 'the coordinator asked to rejoin')
    expect(rejoin).toHaveBeenCalledWith(
      expect.objectContaining({ kind: 'lost', error: expect.objectContaining({ code: 69 }) })
    )
    await until(() => member.rejoins().length > rejoinsBefore, 'the member rejoining at epoch 0')
    await until(() => member.ownership.token() === token + 1, 'the replacement membership')
    await expectNothingPublished(error)
    expect(member.manager.committedOffsets()[TOPIC][0]).toBe(cached)
    expect(member.ownership.permanent).toBeNull()
    expect(member.crashes).toEqual([])
  })

  test.each(FATAL)(
    'fatal code %i keeps the caller timeout, publishes nothing, and the coordinator abandons the group',
    async (code) => {
      const lastCommit = member.manager.lastCommit
      const starts = Runner.prototype.start.mock.calls.length
      const rejoinsBefore = member.rejoins().length
      const { error, token } = await member.answerAfterExpiry(code)

      await until(() => member.crashes.length > 0, 'the crash of the abandoned run')
      await expectNothingPublished(error)
      expect(member.crashes[0]).toMatchObject({ restart: false })
      expect(member.crashes[0].error.code).toBe(code)
      expect(member.ownership.permanent.code).toBe(code)
      expect(member.ownership.renew()).toBe(token)
      expect(member.group.groupMembership.state).toBe('FATAL')
      await pause(150)
      expect(member.rejoins()).toHaveLength(rejoinsBefore)
      expect(Runner.prototype.start.mock.calls).toHaveLength(starts)
      expect(member.manager.lastCommit).toBe(lastCommit)
      expect(member.manager.committedOffsets()[TOPIC]?.[0]).not.toBe(COMMIT.offset)
    }
  )

  test('an answer about a replaced membership token drives no rejoin and no abandonment (control)', async () => {
    const rejoin = jest.spyOn(member.runner.rebalanceCoordinator, 'requestRejoin')
    const rejoinsBefore = member.rejoins().length
    const { error, token } = await member.answerAfterExpiry(30, { replaceToken: true })

    await expectNothingPublished(error)
    await pause(150)
    expect(member.ownership.token()).toBe(token + 1)
    expect(member.ownership.refusal(member.ownership.token())).toBeNull()
    expect(member.ownership.permanent).toBeNull()
    expect(rejoin).not.toHaveBeenCalled()
    expect(member.crashes).toEqual([])
    expect(member.rejoins()).toHaveLength(rejoinsBefore)
  })
})
