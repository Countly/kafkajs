const { Kafka, logLevel } = require('../../../index')
const apiKeys = require('../../protocol/requests/apiKeys')
const { roundRobin } = require('../assigners')
const waitFor = require('../../utils/waitFor')
const { createTopic, plainTextBrokers, secureRandom } = require('testHelpers')
const { WireTranscript } = require('./fixtures/rackWire')

const live = process.env.SYNCGROUP_SLOW_E2E === '1' ? test : test.skip

const ASSIGNMENT_DELAY_MS = 70000
const REBALANCE_TIMEOUT_MS = 120000
/** Above the assignment delay, so the leader is not evicted while it computes. */
const SESSION_TIMEOUT_MS = 90000
const STEP_MS = 60000
const CLOSE_TIMEOUT_MS = 10000
const CLOSE_ALLOWANCE_MS = 2000

/**
 * One public member that records its request frames, group joins, request timeouts and crashes.
 */
class Member {
  /**
   * @param {object} options
   * @param {string} options.name the member label
   * @param {string} options.groupId
   * @param {(name: string) => import('types').PartitionAssigner} options.assigner
   */
  constructor({ name, groupId, assigner }) {
    this.name = name
    this.wire = new WireTranscript()
    this.joins = []
    this.timeouts = []
    this.crashes = []
    this.consumer = new Kafka({
      clientId: `e2e-syncgroup-slow-${name}`,
      brokers: plainTextBrokers(),
      logLevel: logLevel.NOTHING,
      socketFactory: this.wire.socketFactory,
    }).consumer({
      groupId,
      rebalanceTimeout: REBALANCE_TIMEOUT_MS,
      sessionTimeout: SESSION_TIMEOUT_MS,
      partitionAssigners: [assigner(name)],
    })
    const { events } = this.consumer
    this.removers = [
      this.consumer.on(events.GROUP_JOIN, ({ payload }) => this.joins.push(payload)),
      this.consumer.on(events.REQUEST_TIMEOUT, ({ payload }) => this.timeouts.push(payload)),
      this.consumer.on(events.CRASH, ({ payload }) => this.crashes.push(payload.error)),
    ]
  }

  /**
   * @param {number} apiKey
   * @returns {number} the request frames of that API this member wrote, retries included
   */
  written(apiKey) {
    return this.wire.exchanges.filter((x) => x.apiKey === apiKey).length
  }

  /** @returns {object|undefined} the SyncGroup exchange still waiting for its answer */
  parkedSync() {
    return this.wire.exchanges.find((x) => x.apiKey === apiKeys.SyncGroup && !x.answer)
  }

  /**
   * Connects, subscribes and starts the run; resolves once the run has joined.
   * @param {string} topic
   * @returns {Promise<void>}
   */
  async start(topic) {
    await this.consumer.connect()
    await this.consumer.subscribe({ topic, fromBeginning: true })
    await this.consumer.run({ eachMessage: async () => {} })
  }

  /**
   * Closes within the bounded close and detaches the listeners.
   * @returns {Promise<number>} the milliseconds the close took
   */
  async close() {
    const start = Date.now()
    try {
      await this.consumer.disconnect({ timeout: CLOSE_TIMEOUT_MS })
    } finally {
      this.removers.forEach((remove) => remove())
    }
    return Date.now() - start
  }
}

describe('SyncGroup behind a slow leader (live)', () => {
  live(
    "a follower's SyncGroup parked 70 s behind a slow leader completes with rebalanceTimeout 120000",
    async () => {
      const topic = `e2e-syncgroup-slow-${secureRandom()}`
      const groupId = `e2e-syncgroup-slow-${secureRandom()}`
      await createTopic({ topic, partitions: 2 })

      const slowBy = []
      let release
      const released = new Promise((resolve) => {
        release = resolve
      })
      let releaseTimer
      /**
       * The round-robin deal, held until released once the group has two members.
       * @param {string} name the member the assigner runs in
       * @returns {import('types').PartitionAssigner}
       */
      const slowLeader = (name) => (config) => ({
        ...roundRobin(config),
        name: 'SlowLeaderAssigner',
        async assign(group) {
          if (group.members.length === 2) {
            slowBy.push(name)
            await released
          }
          return roundRobin(config).assign(group)
        },
      })

      const first = new Member({ name: 'first', groupId, assigner: slowLeader })
      const second = new Member({ name: 'second', groupId, assigner: slowLeader })
      const members = [first, second]
      let secondStarted = Promise.resolve()
      try {
        await first.start(topic)
        expect(first.joins).toHaveLength(1)

        secondStarted = second.start(topic)
        secondStarted.catch(() => {})
        let follower
        await waitFor(
          () => {
            follower = members.find(({ name }) => slowBy.length === 1 && name !== slowBy[0])
            return follower?.parkedSync() != null
          },
          { delay: 100, maxWait: STEP_MS, timeoutMessage: 'No follower SyncGroup reached the wire' }
        )
        const parked = follower.parkedSync()
        await new Promise((resolve) => {
          releaseTimer = setTimeout(
            resolve,
            Math.max(0, parked.at + ASSIGNMENT_DELAY_MS - performance.now())
          )
        })
        release()
        await secondStarted
        await waitFor(() => first.joins.length === 2 && second.joins.length === 1, {
          delay: 100,
          maxWait: STEP_MS,
          timeoutMessage: 'The members never completed the two-member generation',
        })

        expect(parked.answer).toBeDefined()
        expect(parked.answer.at - parked.at).toBeGreaterThan(65000)
        expect(slowBy).toHaveLength(1)
        // Dynamic members first ask for an id (MEMBER_ID_REQUIRED), then join; the first rejoins once.
        expect(first.written(apiKeys.JoinGroup)).toBe(3)
        expect(second.written(apiKeys.JoinGroup)).toBe(2)
        expect(first.written(apiKeys.SyncGroup)).toBe(2)
        expect(second.written(apiKeys.SyncGroup)).toBe(1)
        for (const member of members) {
          expect(member.timeouts).toEqual([])
          expect(member.crashes).toEqual([])
        }

        const leader = members.find(({ name }) => name === slowBy[0]).joins.at(-1)
        const followed = follower.joins.at(-1)
        expect(leader.isLeader).toBe(true)
        expect(leader.memberId).toBe(leader.leaderId)
        expect(followed.isLeader).toBe(false)
        expect(followed.leaderId).toBe(leader.memberId)
        expect(followed.memberId).not.toBe(leader.memberId)
      } finally {
        clearTimeout(releaseTimer)
        release()
        await secondStarted.catch(() => {})
        const took = await Promise.all(members.map((member) => member.close()))
        for (const ms of took) expect(ms).toBeLessThanOrEqual(CLOSE_TIMEOUT_MS + CLOSE_ALLOWANCE_MS)
      }
    },
    240000
  )
})
