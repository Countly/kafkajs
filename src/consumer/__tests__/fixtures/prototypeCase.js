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
const { withDeadline, disconnectAll } = require('./boundedCleanup')

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
 * Polls a condition within the delivery budget, clearing the timer on cancellation.
 * @param {() => boolean|Promise<boolean>} predicate
 * @param {string} label
 * @param {AbortSignal} [signal] retirement signal owned by the case
 * @param {number} [budgetMs=15000] predicate deadline
 * @param {number} [stepMs=5] polling interval
 * @returns {Promise<void>}
 * @throws {Error} when retired or the condition misses its deadline
 */
const until = async (predicate, label, signal, budgetMs = BUDGET_MS, stepMs = STEP_MS) => {
  const deadline = Date.now() + budgetMs
  for (;;) {
    signal?.throwIfAborted()
    if (await predicate()) return
    signal?.throwIfAborted()
    if (Date.now() > deadline) throw new Error(`Did not happen in time: ${label}`)
    await new Promise((resolve, reject) => {
      const finish = () => {
        clearTimeout(timer)
        signal?.removeEventListener('abort', abort)
        resolve()
      }
      const abort = () => {
        clearTimeout(timer)
        signal.removeEventListener('abort', abort)
        reject(signal.reason)
      }
      const timer = setTimeout(finish, stepMs)
      signal?.addEventListener('abort', abort, { once: true })
      if (signal?.aborted) abort()
    })
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
   * @param {string} [options.identity] full Jest row identity used in failure evidence
   * @param {boolean} [options.grow=false] fake lane, consumer group protocol: the member is first
   *   assigned `ordinary-topic` alone, and once that assignment is applied the next heartbeat adds
   *   the named topic to it
   */
  constructor(name, lane, { protocol = 'classic', rackId, assigner, grow = false, identity } = {}) {
    this.name = name
    this.lane = lane
    this.protocol = protocol
    this.rackId = rackId
    this.assigner = assigner
    this.grow = grow
    this.identity = identity || `${lane}/${name}/${protocol}`
    this.controller = new AbortController()
    this.pending = new Set()
    this.stages = []
    this.events = []
    this.labels = new Map()
    this.bodyDone = false
    this.failure = null
    this.clients = []
    this.delivered = []
    this.joins = []
    this.assigned = []
    this.commits = []
  }

  /**
   * Prepares a fresh single-partition topic before producing its seed record on the live lane.
   * @returns {Promise<Kafka>} a client of the lane's cluster
   * @throws {Error} on retirement, setup failure or unexpected partition metadata
   */
  async kafka() {
    if (this.lane === 'fake') {
      await this.stage('record batches', () => prepareRecordBatches())
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

    this.controller.signal.throwIfAborted()
    const kafka = new Kafka({ brokers: plainTextBrokers(), logLevel: logLevel.NOTHING })
    const admin = kafka.admin()
    this.admin = admin
    await this.connected(admin, 'admin')
    await this.stage('deleteTopics', () => this.deleteTopic(admin, this.controller.signal))
    await this.stage('createTopics/leaders', () =>
      admin.createTopics({
        waitForLeaders: true,
        topics: [{ topic: this.name, numPartitions: 1, replicationFactor: 1 }],
      })
    )
    await this.stage('verify one partition', async () => {
      const { topics } = await admin.fetchTopicMetadata({ topics: [this.name] })
      const topic = topics.find(({ name }) => name === this.name)
      if (topic?.partitions.length !== 1 || topic.partitions[0].partitionId !== 0) {
        throw new Error(`Expected exactly partition 0 for ${this.name}: ${JSON.stringify(topics)}`)
      }
    })
    const producer = await this.connected(kafka.producer(), 'producer')
    await this.stage('producer send', () =>
      producer.send({ topic: this.name, messages: [{ key: 'key', value: 'value' }] })
    )
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

  /**
   * Retires the case before teardown so no suspended wait can advance into another stage.
   * @returns {void}
   */
  retire() {
    this.controller.abort(new Error(`Case retired: ${this.identity}`))
  }

  /**
   * Executes the row with failure tracking, including assertion failures outside a client stage.
   * @param {() => Promise<unknown>} body the row's callback, bound to this instance
   * @returns {Promise<unknown>} the callback result
   * @throws {Error} the original failure, retained for the teardown report
   */
  async execute(body) {
    try {
      return await body()
    } catch (error) {
      this.failure = error
      throw error
    } finally {
      this.bodyDone = true
    }
  }

  /**
   * Records a stage and fences its continuation on retirement. Actual work stays tracked until
   * settled: cancellation alone does not prove a pending connect or request has stopped.
   * @template T
   * @param {string} label operation identity
   * @param {() => Promise<T>|T} operation work to start while the case is active
   * @returns {Promise<T>} the operation result
   * @throws {Error} on retirement or operation failure
   */
  async stage(label, operation) {
    const signal = this.controller.signal
    signal.throwIfAborted()
    const entry = { label, started: new Date().toISOString() }
    this.stages.push(entry)
    const work = Promise.resolve().then(() => {
      signal.throwIfAborted()
      return operation()
    })
    this.pending.add(work)
    work.then(
      () => this.pending.delete(work),
      () => this.pending.delete(work)
    )
    let abort
    try {
      const result = await Promise.race([
        work,
        new Promise((_, reject) => {
          abort = () => reject(signal.reason)
          signal.addEventListener('abort', abort, { once: true })
          if (signal.aborted) abort()
        }),
      ])
      signal.throwIfAborted()
      entry.completed = new Date().toISOString()
      return result
    } catch (error) {
      entry.failed = new Date().toISOString()
      entry.error = error.message
      throw error
    } finally {
      signal.removeEventListener('abort', abort)
    }
  }

  /**
   * Polls a row-owned condition; retirement cancels its timer and rejects immediately.
   * @param {() => boolean|Promise<boolean>} predicate
   * @param {string} label
   * @param {number} [budgetMs=15000] predicate deadline
   * @param {number} [stepMs=5] polling interval
   * @returns {Promise<void>}
   * @throws {Error} on retirement, predicate rejection or a missed delivery deadline
   */
  until(predicate, label, budgetMs = BUDGET_MS, stepMs = STEP_MS) {
    return this.stage(label, () =>
      until(predicate, label, this.controller.signal, budgetMs, stepMs)
    )
  }

  /**
   * Owns the client before any fallible wiring or connect, and records relevant client events.
   * @template T
   * @param {T} client newly allocated client
   * @param {string} [label='producer'] client identity in stage and event evidence
   * @returns {Promise<T>} the connected client
   * @throws {Error} on retirement, event wiring failure or connect failure
   */
  async connected(client, label = 'producer') {
    this.controller.signal.throwIfAborted()
    this.clients.push(client)
    this.labels.set(client, label)
    return this.stage(`${label} connect`, async () => {
      for (const name of ['REQUEST_TIMEOUT', 'CRASH', 'GROUP_JOIN']) {
        const type = client.events?.[name]
        if (type) {
          client.on(type, ({ timestamp, payload }) => {
            this.events.push({
              label,
              type,
              timestamp,
              observed: new Date().toISOString(),
              payload,
            })
          })
        }
      }
      await client.connect()
      return client
    })
  }

  /**
   * Deletes only an existing topic, then observes its absence before any recreation can start.
   * All other errors propagate; teardown supplies its own abort signal and failing deadline.
   * @param {object} admin connected, owned admin
   * @param {AbortSignal} signal polling cancellation
   * @returns {Promise<void>}
   * @throws {Error} on an admin failure, retirement or a missed deletion deadline
   */
  async deleteTopic(admin, signal) {
    signal.throwIfAborted()
    if ((await admin.listTopics()).includes(this.name)) {
      signal.throwIfAborted()
      await admin.deleteTopics({ topics: [this.name] })
    }
    await until(
      async () => !(await admin.listTopics()).includes(this.name),
      'topic deletion',
      signal,
      BUDGET_MS,
      50
    )
  }

  /**
   * Formats retained evidence without emitting output on a successful row.
   * @returns {string} timestamped evidence naming the first incomplete stage
   */
  report() {
    const incomplete = this.stages.find(({ completed }) => !completed)
    return JSON.stringify(
      {
        case: this.identity,
        incompleteStage: incomplete?.label || (this.bodyDone ? 'case assertions' : 'case body'),
        failure: this.failure?.stack,
        stages: this.stages,
        events: this.events,
      },
      (key, value) =>
        value instanceof Error ? { ...value, message: value.message, stack: value.stack } : value,
      2
    )
  }

  /**
   * Subscribes a consumer to the topic and runs it until it delivered the first record.
   * @param {Kafka} kafka
   * @returns {Promise<object>} the consumer
   * @throws {Error} on retirement, consumer failure or a missed delivery deadline
   */
  async consume(kafka) {
    this.controller.signal.throwIfAborted()
    const consumer = await this.connected(
      kafka.consumer({
        groupId: GROUP,
        maxWaitTimeInMs: 20,
        ...(this.protocol === 'consumer' ? { groupProtocol: 'consumer' } : {}),
        ...(this.rackId === undefined ? {} : { rackId: this.rackId }),
        ...(this.assigner === undefined ? {} : { partitionAssigners: [this.assigner] }),
      }),
      'consumer'
    )
    consumer.on(consumer.events.GROUP_JOIN, ({ payload }) => this.joins.push(payload))
    consumer.on(consumer.events.PARTITIONS_ASSIGNED, ({ payload }) => this.assigned.push(payload))
    consumer.on(consumer.events.COMMIT_OFFSETS, ({ payload }) => this.commits.push(payload))
    await this.stage('consumer subscribe', () =>
      consumer.subscribe({
        topics: this.grow ? [GROWTH_TOPIC, this.name] : [this.name],
        fromBeginning: true,
      })
    )
    await this.stage('consumer run', () =>
      consumer.run({
        autoCommit: false,
        eachMessage: async ({ topic, partition, message }) => {
          if (!this.controller.signal.aborted) {
            this.delivered.push({ topic, partition, offset: message.offset })
          }
        },
      })
    )
    await this.until(() => this.delivered.length > 0, 'first delivery')
    return consumer
  }

  /**
   * Retires waits, bounds quiescence and every disconnect, and deletes only after data clients
   * close. Every allocation is attempted even if another fails. A failed cleanup must prevent
   * later rows from starting, since a deadline cannot prove that outstanding work has stopped.
   * @returns {Promise<void>}
   * @throws {AggregateError} all cleanup failures, including missed deadlines
   */
  async close() {
    this.retire()
    const failures = []
    const cleanup = async (label, operation) => {
      const entry = { label, started: new Date().toISOString() }
      this.stages.push(entry)
      try {
        await operation()
        entry.completed = new Date().toISOString()
      } catch (error) {
        entry.failed = new Date().toISOString()
        entry.error = error.message
        failures.push(error)
      }
    }
    await cleanup('pending operations', () =>
      withDeadline(Promise.allSettled([...this.pending]), BUDGET_MS)
    )
    const data = this.clients.filter((client) => client !== this.admin).reverse()
    for (const client of data) {
      await cleanup(`${this.labels.get(client)} disconnect`, () =>
        disconnectAll([client], BUDGET_MS)
      )
    }
    if (this.lane === 'docker' && this.admin && failures.length === 0) {
      const deletion = new AbortController()
      await cleanup('teardown deleteTopics', async () => {
        try {
          await withDeadline(this.deleteTopic(this.admin, deletion.signal), BUDGET_MS)
        } finally {
          deletion.abort(new Error(`Deletion retired: ${this.identity}`))
        }
      })
    }
    if (this.admin) {
      await cleanup('admin disconnect', () => disconnectAll([this.admin], BUDGET_MS))
    }
    await cleanup('scripted requests', () => this.scripted?.assertClean())
    if (failures.length) {
      throw new AggregateError(failures, failures.map((error) => error.message).join('; '))
    }
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
