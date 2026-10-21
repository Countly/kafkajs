const { Kafka, logLevel } = require('../../../../index')
const {
  createScriptedCluster,
  groupScript,
  consumerProtocolScript,
  producerScript,
} = require('../../../../testHelpers/scriptedCluster')
const { prepareRecordBatches, TOPIC_ID } = require('./fetchHarness')
const Encoder = require('../../../protocol/encoder')
const { plainTextBrokers } = require('testHelpers')

/** The names an ordinary object inherits or treats specially. */
const NAMES = ['__proto__', 'constructor', 'toString', 'hasOwnProperty', 'valueOf']
const GROUP = 'prototype-topics'
const STEP_MS = 5
const BUDGET_MS = 15000
/** The topic a growing member is assigned first, and its id; the named topic is added later. */
const GROWTH_TOPIC = 'ordinary-topic'
const GROWTH_TOPIC_ID = 'b1b2b3b4-b5b6-b7b8-b9ba-bbbcbdbebfc0'
/** A Fetch v18 answer that holds no topic: nothing of the named topic was asked for. */
const NO_TOPICS = new Encoder()
  .writeInt32(0)
  .writeInt16(0)
  .writeInt32(0)
  .writeUVarIntArray([])
  .writeTaggedFields().buffer

/** @returns {object} every own property descriptor of `Object.prototype`, as it is now */
const prototypeDescriptors = () => Object.getOwnPropertyDescriptors(Object.prototype)

/**
 * @param {unknown} value
 * @returns {boolean} whether no object inside `value`, followed through plain objects and arrays,
 *   lacks a prototype: a value handed to an application must be one it can call methods on
 */
const ordinary = (value) => {
  if (value === null || typeof value !== 'object') return true
  const prototype = Object.getPrototypeOf(value)
  if (prototype === null) return false
  if (prototype !== Object.prototype && prototype !== Array.prototype) return true
  return Object.values(value).every(ordinary)
}

/**
 * @param {() => boolean} predicate
 * @param {string} label
 * @returns {Promise<void>}
 * @throws {Error} naming `label` when the predicate is still false after the budget
 */
const until = async (predicate, label) => {
  const deadline = Date.now() + BUDGET_MS
  while (!predicate()) {
    if (Date.now() > deadline) throw new Error(`Did not happen in time: ${label}`)
    await new Promise((resolve) => setTimeout(resolve, STEP_MS))
  }
}

/**
 * Throws unless `Object.prototype` has exactly the own properties it had in `before`, with the
 * same descriptors, and no partition number among them (the key a polluting write would add).
 * @param {object} before what {@link prototypeDescriptors} returned before the round trip
 * @returns {void}
 */
const expectPrototypeUntouched = (before) => {
  expect(prototypeDescriptors()).toEqual(before)
  expect(Object.hasOwn(Object.prototype, '0')).toBe(false)
}

/**
 * One client of a cluster whose only topic has a name an ordinary object inherits: on the fake
 * lane a scripted cluster that answers for that topic, on the docker lane the compose cluster.
 * Every client it makes is closed by `close`.
 */
class PrototypeCase {
  /**
   * @param {string} name the topic's name
   * @param {'fake'|'docker'} lane
   * @param {object} [options]
   * @param {'classic'|'consumer'} [options.protocol='classic'] the consumer's group protocol
   * @param {string} [options.rackId] the consumer's rack; the fake broker then names itself the
   *   partition's preferred read replica
   * @param {Function} [options.assigner] the one built-in partition assigner the consumer uses
   * @param {boolean} [options.grow=false] fake lane, consumer group protocol: the member is first
   *   assigned `ordinary-topic` alone, and once that assignment is applied the next heartbeat adds
   *   the named topic to it
   */
  constructor(name, lane, { protocol = 'classic', rackId, assigner, grow = false } = {}) {
    this.name = name
    this.lane = lane
    this.protocol = protocol
    this.rackId = rackId
    this.assigner = assigner
    this.grow = grow
    this.clients = []
    this.delivered = []
    this.joins = []
    this.assigned = []
    this.commits = []
  }

  /** @returns {Promise<Kafka>} a client of the lane's cluster */
  async kafka() {
    if (this.lane === 'fake') {
      await prepareRecordBatches()
      const consumer = this.protocol === 'consumer'
      this.scripted = (consumer ? consumerProtocolScript : groupScript)(
        producerScript(
          createScriptedCluster({
            nodes: [{ id: 0, host: 'node', port: 9092 }],
            profile: consumer ? 'consumer' : 'tv2',
          }),
          { topic: this.name }
        ),
        { topic: this.name, batches: 1 }
      )
      this.scripted
        .answer('InitProducerId', ({ producerEpoch }) => ({
          producerId: '1000',
          producerEpoch: producerEpoch < 0 ? 0 : producerEpoch + 1,
        }))
        .answer('SyncGroup', ({ memberId, groupAssignment }) => ({
          // As a broker does: the member gets what the leader's assigner gave it, or nothing.
          memberAssignment:
            groupAssignment.find((entry) => entry.memberId === memberId)?.memberAssignment ?? '',
        }))
        .answer('Fetch', async ({ maxWaitMs, topics }) => {
          // The named topic's log holds one record at offset 0: a fetch from 0 reads it, any
          // other fetch waits for more. A growing member's first topic holds no record.
          const own = topics.find(({ topicId }) => topicId === TOPIC_ID)
          const [{ partition, fetchOffset } = {}] = own?.partitions ?? []
          if (fetchOffset !== '0') await new Promise((resolve) => setTimeout(resolve, maxWaitMs))
          if (own === undefined) return NO_TOPICS
          const preferredReadReplica = this.rackId === undefined ? -1 : 0
          return {
            partitions: [
              { partition, errorCode: 0, records: fetchOffset === '0', preferredReadReplica },
            ],
          }
        })
      if (this.grow) this.growing()
      return new Kafka(this.scripted.kafkaOptions({ requestTimeout: 20000 }))
    }

    const kafka = new Kafka({ brokers: plainTextBrokers(), logLevel: logLevel.NOTHING })
    const admin = await this.connected(kafka.admin())
    await admin.deleteTopics({ topics: [this.name] }).catch(() => {})
    await admin.createTopics({
      waitForLeaders: true,
      topics: [{ topic: this.name, numPartitions: 1, replicationFactor: 1 }],
    })
    const producer = await this.connected(kafka.producer())
    await producer.send({ topic: this.name, messages: [{ key: 'key', value: 'value' }] })
    this.admin = admin
    return kafka
  }

  /**
   * Scripts the growth: Metadata lists `ordinary-topic` beside the named topic, the joining
   * heartbeat assigns `ordinary-topic` alone, and the first heartbeat after that assignment was
   * reported adds the named topic, so the member takes it on through the incremental apply.
   * @private
   * @returns {void}
   */
  growing() {
    const partition = (leaderId) => ({
      errorCode: 0,
      partitionIndex: 0,
      leaderId,
      leaderEpoch: 5,
      replicas: [leaderId],
      isr: [leaderId],
      offlineReplicas: [],
    })
    const row = (name, topicId) => ({
      errorCode: 0,
      name,
      topicId,
      isInternal: false,
      topicAuthorizedOperations: -2147483648,
      partitions: [partition(0)],
    })
    const first = [{ topicId: GROWTH_TOPIC_ID, partitions: [0] }]
    this.scripted
      .answer('Metadata', () => ({
        brokers: [{ nodeId: 0, host: 'node', port: 9092, rack: null }],
        controllerId: 0,
        topics: [row(GROWTH_TOPIC, GROWTH_TOPIC_ID), row(this.name, TOPIC_ID)],
      }))
      .answer('ConsumerGroupHeartbeat', ({ memberId, memberEpoch }) => {
        const grown = memberEpoch >= 1 && this.assigned.length > 0
        return {
          memberId,
          memberEpoch: memberEpoch === 0 ? 1 : grown ? 2 : memberEpoch,
          heartbeatIntervalMs: 100,
          assignment:
            memberEpoch === 0
              ? { topicPartitions: first }
              : grown && memberEpoch === 1
                ? { topicPartitions: [...first, { topicId: TOPIC_ID, partitions: [0] }] }
                : null,
        }
      })
  }

  /** @template T @param {T} client @returns {Promise<T>} the client, connected and owned */
  async connected(client) {
    this.clients.push(client)
    await client.connect()
    return client
  }

  /**
   * Subscribes a consumer to the topic and runs it until it delivered the first record.
   * @param {Kafka} kafka
   * @returns {Promise<object>} the consumer
   */
  async consume(kafka) {
    const consumer = await this.connected(
      kafka.consumer({
        groupId: GROUP,
        maxWaitTimeInMs: 20,
        ...(this.protocol === 'consumer' ? { groupProtocol: 'consumer' } : {}),
        ...(this.rackId === undefined ? {} : { rackId: this.rackId }),
        ...(this.assigner === undefined ? {} : { partitionAssigners: [this.assigner] }),
      })
    )
    consumer.on(consumer.events.GROUP_JOIN, ({ payload }) => this.joins.push(payload))
    consumer.on(consumer.events.PARTITIONS_ASSIGNED, ({ payload }) => this.assigned.push(payload))
    consumer.on(consumer.events.COMMIT_OFFSETS, ({ payload }) => this.commits.push(payload))
    await consumer.subscribe({
      topics: this.grow ? [GROWTH_TOPIC, this.name] : [this.name],
      fromBeginning: true,
    })
    await consumer.run({
      autoCommit: false,
      eachMessage: async ({ topic, partition, message }) => {
        this.delivered.push({ topic, partition, offset: message.offset })
      },
    })
    await until(() => this.delivered.length > 0, 'the first record')
    return consumer
  }

  async close() {
    for (const client of this.clients.reverse()) {
      await client.disconnect().catch(() => {})
    }
    if (this.lane === 'docker' && this.admin) {
      const kafka = new Kafka({ brokers: plainTextBrokers(), logLevel: logLevel.NOTHING })
      const admin = kafka.admin()
      await admin.connect()
      await admin.deleteTopics({ topics: [this.name] }).catch(() => {})
      await admin.disconnect()
    }
    this.scripted?.assertClean()
  }
}

module.exports = {
  NAMES,
  GROUP,
  GROWTH_TOPIC,
  PrototypeCase,
  prototypeDescriptors,
  expectPrototypeUntouched,
  ordinary,
  until,
}
