const Runner = require('../runner')
const { KafkaJSDeliveryTimeout } = require('../../errors')
const { consumerStoppingError } = require('../rebalanceErrorContract')
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

  /**
   * @param {{ run?: object }} [options] extra `consumer.run` options
   * @returns {Promise<ExpiredCommit>} a started member with a short public-call deadline
   */
  static async open({ run = {} } = {}) {
    const rig = await CloseRig.open({ protocol: 'consumer' })
    const start = jest.spyOn(Runner.prototype, 'start')
    const { consumer } = await rig.started({
      consumer: { retry: { maxTotalTime: 400, retries: 1, initialRetryTime: 10 } },
      run: { autoCommit: false, ...run },
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
   * @param {{ replaceToken?: boolean, beforeAnswer?: () => Promise<void> }} [options]
   *   `replaceToken` begins a new membership first; `beforeAnswer` runs once the call has expired
   * @returns {Promise<{ error: Error, token: number }>} the caller's outcome and the sending token
   */
  async answerAfterExpiry(errorCode, { replaceToken = false, beforeAnswer } = {}) {
    const held = this.rig.cluster.schedule.hold({ api: 'OffsetCommit' })
    const token = this.ownership.token()
    const committing = this.consumer.commitOffsets([COMMIT]).catch((error) => error)
    await held.arrived()
    const error = await committing
    await beforeAnswer?.()
    const { groupMembership } = this.group
    groupMembership.memberEpoch += 1
    if (replaceToken) this.ownership.renew()
    held.release(answerWith(errorCode))
    return { error, token }
  }

  /**
   * Writes one public commit, lets its own deadline expire, retires the run with `retire`, then
   * answers the held request with `errorCode`.
   * @param {number} errorCode what the coordinator answers
   * @param {(member: ExpiredCommit) => Promise<void>} retire stops or force-closes the run
   * @returns {Promise<Error>} the caller's outcome
   */
  async answerAfterRetirement(errorCode, retire) {
    const held = this.rig.cluster.schedule.hold({ api: 'OffsetCommit' })
    const committing = this.consumer.commitOffsets([COMMIT]).catch((error) => error)
    await held.arrived()
    const error = await committing
    await retire(this)
    held.release(answerWith(errorCode))
    return error
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

  test('a heartbeat answer built before the fence cannot complete the rejoin it overlaps', async () => {
    const { schedule } = member.rig.cluster
    const { groupMembership } = member.group
    const rejoinsBefore = member.rejoins().length
    let stale
    let staleCall
    await member.answerAfterExpiry(69, {
      beforeAnswer: async () => {
        stale = schedule.hold({ api: 'ConsumerGroupHeartbeat' })
        staleCall = await stale.arrived()
      },
    })

    await until(() => groupMembership.state === 'JOINING', 'the member joining anew')
    stale.release({
      memberId: staleCall.fields.memberId,
      memberEpoch: staleCall.fields.memberEpoch,
      heartbeatIntervalMs: 100,
      assignment: null,
    })

    await until(() => member.rejoins().length > rejoinsBefore, 'the member rejoining at epoch 0')
    await until(() => groupMembership.state === 'STABLE', 'the member stable again')
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

  test('an answer about a replaced membership token is not learned at all (control)', async () => {
    const lose = jest.spyOn(member.ownership, 'lose')
    const { error } = await member.answerAfterExpiry(69, { replaceToken: true })

    await expectNothingPublished(error)
    await pause(150)
    expect(lose).not.toHaveBeenCalled()
  })

  test('a close callback whose commit expired learns nothing once the run is retired', async () => {
    await member.dispose()
    const commits = []
    const listener = {
      onPartitionsRevoked: async ({ partitions }) => {
        if (partitions.length > 0)
          commits.push(member.consumer.commitOffsets([COMMIT]).catch((e) => e))
        await Promise.all(commits)
      },
    }
    member = await ExpiredCommit.open({ run: { rebalanceListener: listener } })
    const lose = jest.spyOn(member.ownership, 'lose')
    const held = member.rig.cluster.schedule.hold({ api: 'OffsetCommit' })
    const leave = member.rig.cluster.schedule.hold({ api: 'ConsumerGroupHeartbeat' })
    const stopping = member.consumer.stop()
    await held.arrived()
    expect(await commits[0]).toBeInstanceOf(KafkaJSDeliveryTimeout)
    member.runner.retire(consumerStoppingError())
    held.release(answerWith(69))
    await pause(200)
    expect(lose).not.toHaveBeenCalled()
    leave.release()
    await stopping.catch(() => {})
  })

  describe('when the run was retired before the late answer arrives', () => {
    const stopped = (m) => m.consumer.stop()
    const retired = async (m) => m.runner.retire(consumerStoppingError())
    const forceClosed = async (m) => {
      const leave = m.rig.cluster.schedule.hold({ api: 'ConsumerGroupHeartbeat' })
      await m.consumer.disconnect({ timeout: 100 })
      expect(m.runner.retirement).not.toBeNull()
      leave.release()
    }

    test.each([
      ['a stop', stopped],
      ['the run retirement a forced close makes', retired],
      ['a forced disconnect, its transport gone (control)', forceClosed],
    ])(
      'after %s a late answer is learned from nothing and published nowhere',
      async (_, retire) => {
        const lose = jest.spyOn(member.ownership, 'lose')
        const rejoin = jest.spyOn(member.runner.rebalanceCoordinator, 'requestRejoin')
        const error = await member.answerAfterRetirement(69, retire)

        expect(error).toBeInstanceOf(KafkaJSDeliveryTimeout)
        await pause(200)
        expect(lose).not.toHaveBeenCalled()
        expect(rejoin).not.toHaveBeenCalled()
        expect(member.crashes).toEqual([])
        expect(member.commitEvents).toEqual([])
      }
    )
  })
})
