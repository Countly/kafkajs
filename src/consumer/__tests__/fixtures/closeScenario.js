const { Kafka, logLevel } = require('../../../../index')
const defaultSocketFactory = require('../../../network/socketFactory')
const apiKeys = require('../../../protocol/requests/apiKeys')
const { plainTextBrokers, secureRandom } = require('testHelpers')
const { createChaosSocketFactory, FrameFilter } = require('../../../../testHelpers/chaosSocket')
const { NEVER, STOP_GRACE_MS, bounded } = require('../../../producer/__tests__/fixtures/bounded')
const { apiName } = require('./frameCounter')

const STEP_MS = 30000
const CLEANUP_BUDGET_MS = 15000
const MISSING_TOPIC = 'UNKNOWN_TOPIC_OR_PARTITION'

/** @param {string} api the protocol's API name @returns {number} its key */
const keyOf = (api) => apiKeys[api === 'FindCoordinator' ? 'GroupCoordinator' : api]

/**
 * A socket factory that counts every request frame the client writes and can stop chosen APIs'
 * frames before they leave the process, or lose their answers, so a case can hold a coordinator
 * exchange without touching the other APIs on the same connection.
 */
class FaultWire {
  constructor() {
    this.chaos = createChaosSocketFactory(defaultSocketFactory())
    /** @type {Map<string, number>} frames the client wrote, by API */
    this.written = new Map()
    /** @type {Map<string, number>} frames stopped before the wire, by API */
    this.stopped = new Map()
    /** @type {Set<string>} the APIs whose requests never leave the process */
    this.holding = new Set()
    this.factory = (options) => this.wrap(this.chaos(options))
  }

  /** @param {string} api @returns {number} frames of the API the client wrote, stopped ones included */
  count(api) {
    return this.written.get(api) ?? 0
  }

  /** @param {string} api @returns {number} frames of the API that never reached the wire */
  stoppedCount(api) {
    return this.stopped.get(api) ?? 0
  }

  /**
   * @param {string[]} apis the APIs whose later requests are stopped before the wire
   * @returns {void}
   */
  hold(apis) {
    for (const api of apis) this.holding.add(api)
  }

  /** @returns {void} later requests are written again */
  release() {
    this.holding.clear()
  }

  /**
   * Loses the next answer of `api` on every connection, the client's request having been written.
   * @param {string} api
   * @returns {() => void} stops arming connections opened later
   */
  loseAnswers(api) {
    return this.chaos.arm({}, (socket) => socket.dropResponses(1, { apiKey: keyOf(api) }))
  }

  /**
   * @param {object} socket the chaos socket the client will use
   * @returns {object} the same socket, counting and filtering the frames written through it
   */
  wrap(socket) {
    const write = socket.write.bind(socket)
    const filter = new FrameFilter({
      headerBytes: 12,
      decide: (header) => {
        const api = apiName(header.readInt16BE(4))
        this.written.set(api, this.count(api) + 1)
        if (!this.holding.has(api)) return false
        this.stopped.set(api, this.stoppedCount(api) + 1)
        return true
      },
    })
    socket.write = (data, encoding) => {
      for (const chunk of filter.push(Buffer.from(data, encoding))) write(chunk)
      return true
    }
    return socket
  }
}

/**
 * One topic with a fixed corpus and the case's own admin, producer and group. Every client it
 * makes is stopped by the case's cleanup stack, in separate bounded steps.
 */
class LiveTopic {
  /**
   * @param {object} options
   * @param {AbortSignal} options.scope the case scope
   * @param {import('../../../producer/__tests__/fixtures/liveProbe').CleanupStack} options.cleanup
   * @param {string} options.prefix names the topic and the group
   */
  constructor({ scope, cleanup, prefix }) {
    this.scope = scope
    this.cleanup = cleanup
    this.id = secureRandom()
    this.topic = `e2e-p9c-${prefix}-${this.id}`
    this.groupId = `e2e-p9c-${prefix}-group-${this.id}`
    this.brokers = plainTextBrokers()
    this.admin = new Kafka({
      clientId: `e2e-p9c-${prefix}-admin-${this.id}`,
      brokers: this.brokers,
      logLevel: logLevel.NOTHING,
    }).admin()
    /** @type {Array<{topic: string, partition: number, value: string}>} what the broker acknowledged */
    this.produced = []
    /** @type {import('../../../../types').Producer|null} the case's own producer, once prepared */
    this.producer = null
  }

  /** @template T @param {string} label @param {() => Promise<T>} op @returns {Promise<T>} */
  step(label, op) {
    return bounded(this.scope, STEP_MS, op, { label })
  }

  /**
   * Creates the topic, produces `count` records on partition 0 and commits `committed` for the
   * group, each as its own bounded step, and registers the topic deletion and the admin
   * disconnect as separate cleanups.
   * @param {object} options
   * @param {number} options.count records to produce
   * @param {number} [options.committed] the group's committed next offset before the case starts
   * @returns {Promise<void>}
   */
  async prepare({ count, committed }) {
    const { cleanup, admin, topic } = this
    cleanup.add('admin disconnect', () => bounded(NEVER, STOP_GRACE_MS, () => admin.disconnect()))
    cleanup.add('topic deletion', (signal) => this.deleteTopic(signal))
    await this.step('The admin connect', () => admin.connect())
    await this.step('The topic creation', () =>
      admin.createTopics({ topics: [{ topic, numPartitions: 1, replicationFactor: 1 }] })
    )
    const producer = new Kafka({
      clientId: `e2e-p9c-producer-${this.id}`,
      brokers: this.brokers,
      logLevel: logLevel.NOTHING,
    }).producer()
    this.producer = producer
    cleanup.add('producer disconnect', () =>
      bounded(NEVER, STOP_GRACE_MS, () => producer.disconnect())
    )
    await this.step('The producer connect', () => producer.connect())
    const messages = Array.from({ length: count }, (_, index) => ({
      partition: 0,
      value: `value-${index}`,
    }))
    await this.step('The corpus production', () => producer.send({ topic, messages }))
    for (const { value } of messages) this.produced.push({ topic, partition: 0, value })
    if (committed !== undefined) {
      await this.step('The initial commit', () =>
        admin.setOffsets({
          groupId: this.groupId,
          topic,
          partitions: [{ partition: 0, offset: String(committed) }],
        })
      )
    }
  }

  /**
   * @param {AbortSignal} signal the cleanup's own
   * @returns {Promise<void>}
   * @throws {Error} a deletion failure other than a missing topic
   */
  async deleteTopic(signal) {
    try {
      await bounded(signal, CLEANUP_BUDGET_MS, () =>
        this.admin.deleteTopics({ topics: [this.topic], timeout: 10000 })
      )
    } catch (error) {
      if (error.type !== MISSING_TOPIC) throw error
    }
  }

  /**
   * An independent read of the group's committed offset, through the case's admin.
   * @returns {Promise<number>} the committed next offset of partition 0, or -1 for none
   */
  async brokerOffset() {
    const [{ partitions }] = await this.step('The independent offset read', () =>
      this.admin.fetchOffsets({ groupId: this.groupId, topics: [this.topic] })
    )
    return Number(partitions.find(({ partition }) => partition === 0).offset)
  }

  /**
   * @returns {Promise<number>} how many members the group has, read independently
   */
  async memberCount() {
    const { groups } = await this.step('The independent group read', () =>
      this.admin.describeGroups([this.groupId])
    )
    return groups[0].members.length
  }
}

module.exports = { FaultWire, LiveTopic, STEP_MS, CLEANUP_BUDGET_MS, keyOf }
