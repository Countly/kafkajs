const crypto = require('crypto')
const fs = require('fs')
const path = require('path')
const { Kafka, logLevel } = require('../../../index')
const { createCluster, plainTextBrokers, secureRandom, writeEvidence } = require('testHelpers')
const apiKeys = require('../../protocol/requests/apiKeys')
const { range } = require('../assigners')
const { WireTranscript, bindTopic } = require('./fixtures/rackWire')
const { CleanupStack, describeError } = require('../../producer/__tests__/fixtures/liveProbe')
const {
  NEVER,
  STOP_GRACE_MS,
  bounded,
  caseTimeout,
  delay,
  liveCase,
  pollUntil,
} = require('../../producer/__tests__/fixtures/bounded')

// Gated: it needs the three-rack cluster. Run alone through the guarded live entry:
// `npm run test:e2e:rack-aware` is the developer row.
const describeRack = process.env.RACK_AWARE_E2E === '1' ? describe : describe.skip

const EVIDENCE = 'rack-aware'
const VECTOR = path.join(
  __dirname,
  '..',
  'assigners',
  'fixtures',
  'range',
  'rack_discriminating_java_rack.json'
)
const CASE_BUDGET_MS = 150000
const STEP_MS = 30000
const ASSIGNMENT_BUDGET_MS = 30000
const CONSUME_BUDGET_MS = 30000
const HINT_BUDGET_MS = 30000
const POLL_MS = 100
const QUIET_MS = 1000
const CLEANUP_BUDGET_MS = 20000
const RECORDS_PER_PARTITION = 3
const BROKER_RACKS = { 1: 'rack-a', 2: 'rack-b', 3: 'rack-c' }
const MISSING_TOPIC = 'UNKNOWN_TOPIC_OR_PARTITION'
const SYNC_GROUP = apiKeys.SyncGroup

/** Two replicas per partition, the replica sets of the independent Java vector on nodes 1 to 3. */
const NONUNIFORM = [
  { partition: 0, replicas: [2, 3] },
  { partition: 1, replicas: [2, 3] },
  { partition: 2, replicas: [1, 3] },
  { partition: 3, replicas: [1, 3] },
]

/** Every partition on every rack, so no partition lacks any rack of the union. */
const UNIFORM = [
  { partition: 0, replicas: [1, 2, 3] },
  { partition: 1, replicas: [2, 3, 1] },
  { partition: 2, replicas: [3, 1, 2] },
  { partition: 3, replicas: [1, 2, 3] },
]

/** Three partitions, three replicas each, one leader on each rack. */
const ROTATED = [
  { partition: 0, replicas: [1, 2, 3] },
  { partition: 1, replicas: [2, 3, 1] },
  { partition: 2, replicas: [3, 1, 2] },
]

/** Plain range for four partitions and two ordered members: what every no-op control must give. */
const PLAIN_RANGE = [
  [0, 1],
  [2, 3],
]

/** The plain-range assignment bytes of the vector's topic, which the previous release also wrote. */
const FLAG_OFF_BYTES = [
  '0003000000010003742d6100000002000000000000000100000000',
  '0003000000010003742d6100000002000000020000000300000000',
]

/**
 * One raw client or admin operation, bounded by the case scope.
 * @template T
 * @param {AbortSignal} scope the case scope
 * @param {string} label names the operation in its time-out message
 * @param {(child: AbortSignal) => Promise<T>} op
 * @returns {Promise<T>}
 */
const step = (scope, label, op) => bounded(scope, STEP_MS, op, { label })

/**
 * @param {Array<{ partition: number, replicas: number[] }>} partitions
 * @param {string} name names the corpus, so no value repeats across episodes
 * @returns {Array<{ partition: number, value: string }>} the records of one corpus
 */
const corpusOf = (partitions, name) =>
  partitions.flatMap(({ partition }) =>
    Array.from({ length: RECORDS_PER_PARTITION }, (_, n) => ({
      partition,
      value: `${name}-${partition}-${n}`,
    }))
  )

/**
 * Deletes a topic as a cleanup. Only a confirmed missing topic is success, so a topic the case
 * never created is not a failure.
 * @param {Kafka} kafka
 * @param {string} topic
 * @param {AbortSignal} signal the cleanup's own, which aborts when its allowance ends
 * @returns {Promise<void>}
 * @throws {Error} a deletion failure other than a missing topic
 */
const deleteTopic = async (kafka, topic, signal) => {
  const admin = kafka.admin()
  try {
    await bounded(signal, STEP_MS, () => admin.connect(), { label: 'The cleanup admin connect' })
    await bounded(signal, STEP_MS, () => admin.deleteTopics({ topics: [topic], timeout: 10000 }), {
      label: 'The topic deletion',
    })
  } catch (error) {
    if (error.type !== MISSING_TOPIC) throw error
  } finally {
    await bounded(NEVER, STOP_GRACE_MS, () => admin.disconnect(), {
      label: 'The cleanup admin disconnect',
    })
  }
}

/**
 * One consumer group over one topic on the racks cluster: the topic, its members with their racks
 * and static instance ids, the wire each member's sockets show, and what the handlers were given.
 * It keeps protocol work only: every wait goes through the case scope, and every client it makes is
 * registered for cleanup before the operation that can make it partially.
 */
class RackEpisode {
  /**
   * @param {AbortSignal} scope the case scope
   * @param {CleanupStack} cleanup the case's cleanup, which releases whatever `close` did not
   * @param {string} id the case's id
   */
  constructor(scope, cleanup, id) {
    this.scope = scope
    this.cleanup = cleanup
    this.id = id
    this.brokers = plainTextBrokers()
    /** @type {Array<{ instanceId: string, rackId: string|undefined, consumer: object,
     *   wire: WireTranscript, joins: object[], crashes: object[] }>} ordered by instance id */
    this.members = []
    /** @type {Array<{ member: number, topic: string, partition: number, offset: string,
     *   value: string }>} */
    this.handled = []
    /** @type {Array<{ topic: string, partition: number, value: string }>} what the send acknowledged */
    this.acknowledged = []
    /** @type {string} the step the episode reached, for the evidence of a failed case */
    this.stage = 'created'
    /** @type {Error|null} what ended the episode's work */
    this.error = null
    /** @type {object|null} what the episode's work returned */
    this.result = null
    /** @type {Record<string, number[]>|null} the settled ownership */
    this.owned = null
    /** @type {Record<number, string>|null} the offsets the broker held at `close` */
    this.committed = null
    this.placement = null
    /** @type {Record<string, unknown>} what the case selected, kept as it is chosen */
    this.facts = {}
    this.closed = false
  }

  /**
   * Creates the topic with an explicit replica assignment, verifies the cluster's actual racks,
   * replica sets and full ISR, then starts one member per entry of `members`, each joining before
   * the next starts.
   * @param {object} options
   * @param {boolean} options.rackAwareAssignment the public option under test
   * @param {string} options.topic
   * @param {Array<{ partition: number, replicas: number[] }>} options.replicas replica node ids per
   *   partition, the first being the preferred leader
   * @param {string} options.groupId
   * @param {Array<{ rackId?: string }>} options.members in instance-id order; no rack is a member
   *   without one
   * @returns {Promise<void>}
   * @throws {Error} when the cluster's racks or replica sets are not the ones asked for
   */
  async open({ rackAwareAssignment, topic, replicas, groupId, members }) {
    const { scope, cleanup, id } = this
    this.stage = 'opening'
    this.topic = topic
    this.groupId = groupId
    this.partitions = replicas.map(({ partition }) => partition)
    this.kafka = new Kafka({
      clientId: `e2e-p9c-rack-aware-${id}`,
      brokers: this.brokers,
      logLevel: logLevel.NOTHING,
    })
    const admin = this.kafka.admin()
    cleanup.add('admin', () => admin.disconnect())
    await step(scope, 'The admin connect', () => admin.connect())
    this.admin = admin
    this.producer = this.kafka.producer()
    cleanup.add('producer', () => this.producer.disconnect())
    await step(scope, 'The producer connect', () => this.producer.connect())
    cleanup.add(`topic ${topic}`, (signal) => deleteTopic(this.kafka, topic, signal))
    await step(scope, 'The topic creation', () =>
      admin.createTopics({
        waitForLeaders: true,
        timeout: 20000,
        topics: [{ topic, replicaAssignment: replicas }],
      })
    )
    this.cluster = createCluster({}, this.brokers)
    cleanup.add('cluster', () => this.cluster.disconnect())
    await step(scope, 'The cluster connect', () => this.cluster.connect())
    await step(scope, 'The target topic', () => this.cluster.addTargetTopic(topic))
    this.stage = 'verifying the placement'
    await this.verify(replicas)

    for (const [index, { rackId }] of members.entries()) {
      await this.start(index, { rackId, rackAwareAssignment })
    }
  }

  /**
   * Waits for full ISR, then checks the live racks and replica sets against the ones asked for.
   * @param {Array<{ partition: number, replicas: number[] }>} replicas
   * @returns {Promise<void>}
   * @throws {Error} naming the difference
   */
  async verify(replicas) {
    const rows = () => this.cluster.findTopicPartitionMetadata(this.topic)
    await pollUntil(
      this.scope,
      STEP_MS,
      async () => {
        await this.cluster.refreshMetadata()
        return (
          rows().length === replicas.length &&
          rows().every(({ leader, replicas: all, isr }) => leader >= 0 && isr.length === all.length)
        )
      },
      { delayMs: POLL_MS, label: 'The full in-sync replica sets' }
    )
    const { brokers } = this.cluster.brokerPool.metadata
    const racks = Object.fromEntries(brokers.map(({ nodeId, rack }) => [nodeId, rack]))
    if (JSON.stringify(racks) !== JSON.stringify(BROKER_RACKS)) {
      throw new Error(
        `The broker racks are ${JSON.stringify(racks)}, not ${JSON.stringify(BROKER_RACKS)}`
      )
    }
    const asked = new Map(replicas.map(({ partition, replicas: ids }) => [partition, ids]))
    for (const { partitionId, replicas: actual } of rows()) {
      const want = asked.get(partitionId)
      if (!want || [...actual].sort().join() !== [...want].sort().join()) {
        throw new Error(`Partition ${partitionId} has replicas ${actual}, not ${want}`)
      }
    }
    this.placement = {
      racks,
      partitions: rows()
        .map(({ partitionId, leader, replicas: all, isr, offlineReplicas }) => ({
          partitionId,
          leader,
          replicas: [...all],
          isr: [...isr],
          offlineReplicas: [...(offlineReplicas ?? [])],
        }))
        .sort((a, b) => a.partitionId - b.partitionId),
    }
    this.endpoints = new Map(brokers.map(({ nodeId, host, port }) => [`${host}:${port}`, nodeId]))
  }

  /**
   * Starts one member with the range assigner, a static instance id and autoCommit on, and waits
   * for its first join.
   * @param {number} index the member's place in the instance-id order
   * @param {{ rackId?: string, rackAwareAssignment: boolean }} options
   * @returns {Promise<void>}
   */
  async start(index, { rackId, rackAwareAssignment }) {
    const { scope, cleanup, id } = this
    this.stage = `starting member ${index + 1}`
    const wire = new WireTranscript()
    for (const [endpoint, node] of this.endpoints) wire.nodes.set(endpoint, node)
    const kafka = new Kafka({
      clientId: `e2e-p9c-rack-aware-${id}-m${index + 1}`,
      brokers: this.brokers,
      logLevel: logLevel.NOTHING,
      socketFactory: wire.socketFactory,
    })
    const consumer = kafka.consumer({
      groupId: this.groupId,
      groupProtocol: 'classic',
      groupInstanceId: `${this.groupId}-m${index + 1}`,
      partitionAssigners: [range],
      rackAwareAssignment,
      ...(rackId ? { rackId } : {}),
      maxWaitTimeInMs: 500,
      heartbeatInterval: 1000,
      sessionTimeout: 30000,
    })
    const member = {
      instanceId: `${this.groupId}-m${index + 1}`,
      rackId,
      consumer,
      wire,
      joins: [],
      crashes: [],
    }
    this.members.push(member)
    const listeners = []
    cleanup.add(`consumer ${index + 1} listeners`, () =>
      listeners.splice(0).forEach((off) => off())
    )
    cleanup.add(`consumer ${index + 1}`, () => consumer.disconnect())
    listeners.push(
      consumer.on(consumer.events.GROUP_JOIN, ({ payload }) => member.joins.push(payload)),
      consumer.on(consumer.events.CRASH, ({ payload }) => member.crashes.push(payload.error))
    )
    await step(scope, 'The consumer connect', () => consumer.connect())
    await step(scope, 'The consumer subscription', () =>
      consumer.subscribe({ topic: this.topic, fromBeginning: true })
    )
    await step(scope, 'The consumer start', () =>
      consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
          if (scope.aborted) return
          this.handled.push({
            member: index,
            topic,
            partition,
            offset: message.offset,
            value: message.value.toString(),
          })
        },
      })
    )
    await pollUntil(scope, ASSIGNMENT_BUDGET_MS, () => member.joins.length > 0, {
      delayMs: POLL_MS,
      label: 'The first group join',
    })
  }

  /**
   * @returns {Map<string, number[]>} each member's partitions of the topic from its last complete
   *   `GROUP_JOIN`, sorted, by instance id
   */
  assignment() {
    return new Map(
      this.members.map(({ instanceId, joins }) => [
        instanceId,
        [...(joins.at(-1)?.memberAssignment?.[this.topic] ?? [])].sort((a, b) => a - b),
      ])
    )
  }

  /**
   * Waits until the members own every partition exactly once, then for one quiet period to show
   * that no later round changes it.
   * @returns {Promise<Record<string, number[]>>} the partitions of each member, by instance id
   * @throws {Error} when the members do not settle exclusively within the wait
   */
  async settled() {
    this.stage = 'settling the ownership'
    const exclusive = () => {
      const owned = [...this.assignment().values()].flat().sort((a, b) => a - b)
      return owned.join() === [...this.partitions].sort((a, b) => a - b).join()
    }
    await pollUntil(this.scope, ASSIGNMENT_BUDGET_MS, exclusive, {
      delayMs: POLL_MS,
      label: 'The exclusive ownership',
    })
    await delay(this.scope, QUIET_MS)
    if (!exclusive()) throw new Error('The ownership changed during the quiet period')
    this.owned = Object.fromEntries(this.assignment())
    return this.owned
  }

  /**
   * Sends one corpus with acks -1 once the group is stable.
   * @param {string} name names the corpus
   * @returns {Promise<Array<{ topic: string, partition: number, value: string }>>} the records
   *   the send acknowledged, kept as `acknowledged`
   */
  async produce(name) {
    this.stage = 'sending'
    const messages = corpusOf(
      this.partitions.map((partition) => ({ partition })),
      name
    )
    await step(this.scope, 'The send', () =>
      this.producer.send({ topic: this.topic, acks: -1, messages })
    )
    this.acknowledged = messages.map(({ partition, value }) => ({
      topic: this.topic,
      partition,
      value,
    }))
    return this.acknowledged
  }

  /**
   * Waits for every record to be handled, then for one quiet period to show that none repeats.
   * @param {object[]} expected the records sent
   * @returns {Promise<void>}
   * @throws {Error} when the handlers do not receive every record within the wait
   */
  async delivered(expected) {
    this.stage = 'waiting for the deliveries'
    await pollUntil(this.scope, CONSUME_BUDGET_MS, () => this.handled.length >= expected.length, {
      delayMs: POLL_MS,
      label: 'The deliveries',
    })
    await delay(this.scope, QUIET_MS)
  }

  /**
   * @returns {Promise<object[]>} for each member the frames it wrote, with the node each went to,
   *   and the preferred read replicas its Fetch answers named; evidence for the case
   */
  async transcript() {
    return Promise.all(
      this.members.map(async ({ instanceId, rackId, wire }) => ({
        instanceId,
        rackId,
        frames: wire.exchanges
          .slice(-300)
          .map(({ connection, endpoint, apiKey, apiVersion, correlationId, at }) => ({
            connection,
            endpoint,
            node: wire.nodes.get(endpoint),
            apiKey,
            apiVersion,
            correlationId,
            at,
          })),
        hints: (await wire.fetchHints()).map(({ exchange, topic, topicId, ...hint }) => ({
          ...hint,
          topic,
          topicId,
          correlationId: exchange.correlationId,
        })),
      }))
    )
  }

  /**
   * Stops every member and reads the offsets the broker holds for the group. Releasing the clients
   * and the listeners is also registered for cleanup, so a failure before this point still
   * releases them.
   * @returns {Promise<Record<number, string>>} the committed offset of each partition, '-1' when none
   */
  async close() {
    if (this.closed) return this.committed
    this.closed = true
    this.stage = 'closing'
    for (const { consumer } of this.members) {
      await step(this.scope, 'The consumer stop', () => consumer.disconnect())
    }
    const [{ partitions }] = await step(this.scope, 'The offset read', () =>
      this.admin.fetchOffsets({ groupId: this.groupId, topics: [this.topic] })
    )
    this.committed = Object.fromEntries(
      partitions.map(({ partition, offset }) => [partition, String(offset)])
    )
    this.stage = 'closed'
    return this.committed
  }

  /**
   * What the episode retained, whatever point it reached. A part that cannot be read is reported
   * as unavailable, so the row is always written.
   * @returns {Promise<object>} the stage, the error, and the placement, ownership, deliveries,
   *   commits and wire the episode holds
   */
  async evidence() {
    let transcript
    try {
      transcript = await this.transcript()
    } catch (error) {
      transcript = { unavailable: describeError(error) }
    }
    return {
      stage: this.stage,
      error: this.error ? describeError(this.error) : null,
      placement: this.placement,
      owned: this.owned,
      assignment: Object.fromEntries(this.assignment()),
      acknowledged: this.acknowledged,
      handled: this.handled,
      committed: this.committed,
      transcript,
    }
  }

  /**
   * @returns {Record<number, number>} how many records each partition's handler was given
   */
  processed() {
    const counts = {}
    for (const { partition } of this.handled) counts[partition] = (counts[partition] ?? 0) + 1
    return counts
  }
}

/**
 * Runs one case body with its own cleanup, which runs under its own allowance after the body,
 * however the body ends.
 * @param {AbortSignal} scope the case scope
 * @param {(context: { id: string, cleanup: CleanupStack }) => Promise<void>} body
 * @returns {Promise<void>}
 */
const withCase = (scope, body) => {
  const cleanup = new CleanupStack()
  return cleanup.run(() => body({ id: secureRandom(), cleanup }), { budgetMs: CLEANUP_BUDGET_MS })
}

/**
 * Runs the flag-off control and the flag-on arm together, so the case spends one group's time. Each
 * episode is registered in `runs` before its work starts and keeps its own result or error, so a
 * failure of either never discards the other's evidence. The first failure surfaces only once both
 * have ended, so nothing is left running when cleanup starts.
 * @param {Array<{ flag: string, episode: RackEpisode }>} runs the case's episodes, appended to
 * @param {{ scope: AbortSignal, cleanup: CleanupStack, id: string }} context
 * @param {(rackAwareAssignment: boolean, episode: RackEpisode) => Promise<object>} work its return
 *   value becomes the episode's `result`
 * @returns {Promise<{ off: RackEpisode, on: RackEpisode }>} the episodes, both finished
 * @throws {Error} the first failure
 */
const bothFlags = async (runs, { scope, cleanup, id }, work) => {
  const entries = ['off', 'on'].map((flag) => ({
    flag,
    episode: new RackEpisode(scope, cleanup, id),
  }))
  runs.push(...entries)
  await Promise.allSettled(
    entries.map(async ({ flag, episode }) => {
      try {
        episode.result = await work(flag === 'on', episode)
      } catch (error) {
        episode.error = error
        throw error
      }
    })
  )
  const failed = entries.find(({ episode }) => episode.error)
  if (failed) throw failed.episode.error
  return { off: entries[0].episode, on: entries[1].episode }
}

/**
 * @param {string} name the case
 * @param {boolean} flag
 * @returns {string} the label of one episode in topic and group names
 */
const labelOf = (name, flag) => `${name}-${flag ? 'on' : 'off'}`

/**
 * Writes one evidence row for each episode in `runs` from what the episode retained.
 * @param {Array<{ flag: string, episode: RackEpisode }>} runs
 * @param {(flag: string, episode: RackEpisode) => object} rowOf the case's own fields of a row
 * @param {Error|null} failure the case's own failure, when it has one
 * @returns {Promise<Error|null>} the first failed write, which never throws
 */
const writeRows = async (runs, rowOf, failure) => {
  let reporting = null
  for (const { flag, episode } of runs) {
    try {
      writeEvidence(EVIDENCE, {
        scenario: EVIDENCE,
        flag,
        groupId: episode.groupId,
        ...rowOf(flag, episode),
        ...(await episode.evidence()),
        caseError: failure ? describeError(failure) : null,
      })
    } catch (error) {
      reporting ??= error
    }
  }
  return reporting
}

/**
 * Runs a case body and then, however it ended, writes the evidence rows of `runs`. A failed write
 * never replaces the body's own failure; with none, it is thrown, so a lost row is not read as a
 * pass.
 * @param {Array<{ flag: string, episode: RackEpisode }>} runs filled by `bothFlags` as the body runs
 * @param {(flag: string, episode: RackEpisode) => object} rowOf the case's own fields of a row
 * @param {() => Promise<void>} body
 * @returns {Promise<void>}
 * @throws {Error} the body's failure, or the first failed evidence write
 */
const withRows = async (runs, rowOf, body) => {
  let failure = null
  let reporting = null
  try {
    await body()
  } catch (error) {
    failure = error
    throw error
  } finally {
    reporting = await writeRows(runs, rowOf, failure)
  }
  if (reporting) throw reporting
}

/**
 * @param {object[]} records `{ topic, partition, value }` identities, possibly with more fields
 * @returns {string[]} the identities sorted, duplicates kept
 */
const identitiesOf = (records) =>
  records.map(({ topic, partition, value }) => `${topic}|${partition}|${value}`).sort()

/**
 * @param {RackEpisode} episode a closed episode
 * @param {object[]} expected the records the send acknowledged
 * @returns {void} asserts every acknowledged `(topic, partition, value)` was handled once, by the
 *   member that owns its partition, and that the broker's commits never passed what the handlers
 *   processed
 */
const expectExactlyOnce = (episode, expected) => {
  expect(identitiesOf(episode.handled)).toEqual(identitiesOf(expected))
  const owned = episode.assignment()
  for (const { member, partition } of episode.handled) {
    expect(owned.get(episode.members[member].instanceId)).toContain(partition)
  }
  const processed = episode.processed()
  for (const [partition, offset] of Object.entries(episode.committed)) {
    expect(Number(offset)).toBeLessThanOrEqual(processed[partition] ?? 0)
  }
  for (const member of episode.members) expect(member.crashes).toEqual([])
}

describeRack('Consumer > rack-aware range assignment (live)', () => {
  test(
    'matches the Java range vector on nonuniform replica racks',
    liveCase(CASE_BUDGET_MS, (scope) =>
      withCase(scope, async ({ id, cleanup }) => {
        const file = fs.readFileSync(VECTOR)
        const vector = JSON.parse(file)
        const identity = {
          caseId: vector.caseId,
          sha256: crypto.createHash('sha256').update(file).digest('hex'),
        }
        const replicas = vector.topics['t-a'].map(({ partition, replicas: ids }) => ({
          partition,
          replicas: ids.map((nodeId) => nodeId + 1),
        }))
        expect(replicas).toEqual(NONUNIFORM)
        const members = vector.members.map(({ rackId }) => ({ rackId }))
        const expected = {
          off: [
            { partitions: [0, 1], bytes: FLAG_OFF_BYTES[0] },
            { partitions: [2, 3], bytes: FLAG_OFF_BYTES[1] },
          ],
          on: vector.members.map(({ memberId }) => ({
            partitions: vector.expected[memberId]['t-a'],
            bytes: vector.expectedAssignmentBytesHex[memberId],
          })),
        }

        const runs = []
        const rowOf = (flag) => ({
          case: 'java-vector',
          vector: identity,
          config: { assignor: 'range', rackAwareAssignment: flag === 'on', members },
          replay: {
            log: 'twelve acknowledged records, three on each of partitions 0 to 3',
            offsets: 'broker commits never exceed the processed positions',
            handlers: 'each acknowledged record once, by the owner of its partition',
          },
        })
        await withRows(runs, rowOf, async () => {
          const episodes = await bothFlags(runs, { scope, cleanup, id }, async (flag, episode) => {
            const label = labelOf('vector', flag)
            await episode.open({
              rackAwareAssignment: flag,
              topic: `e2e-p9c-rack-aware-${label}-${id}`,
              groupId: `e2e-p9c-rack-aware-group-${label}-${id}`,
              replicas,
              members,
            })
            const owned = await episode.settled()
            const sent = await episode.produce(label)
            await episode.delivered(sent)
            await episode.close()
            return { owned, sent }
          })

          for (const [name, episode] of Object.entries(episodes)) {
            const { owned, sent } = episode.result
            const want = expected[name]
            const instances = [...episode.assignment().keys()]
            const sync = episode.members.map(({ wire }) => wire.lastAnswered(SYNC_GROUP))
            expect(owned).toEqual(
              Object.fromEntries(instances.map((instance, i) => [instance, want[i].partitions]))
            )
            for (const partitions of Object.values(owned)) expect(partitions).toHaveLength(2)
            for (const [i, answer] of sync.entries()) {
              const bytes = Buffer.from(bindTopic(want[i].bytes, episode.topic), 'hex')
              expect(answer.answer.frame.includes(bytes)).toBe(true)
            }
            expectExactlyOnce(episode, sent)
          }
        })
      })
    ),
    caseTimeout(CASE_BUDGET_MS)
  )

  test(
    'uses an online in-rack follower on the next Fetch with either assignment flag',
    liveCase(CASE_BUDGET_MS, (scope) =>
      withCase(scope, async ({ id, cleanup }) => {
        const runs = []
        const rowOf = (flag, episode) => {
          const { picked, follower, hint, next, local, eligible } = episode.facts
          const [member] = episode.members
          return {
            case: 'follower-fetch',
            config: { assignor: 'range', rackAwareAssignment: flag === 'on', rackId: 'rack-a' },
            ineligible: local?.map(({ partitionId }) => partitionId) ?? null,
            eligible: eligible?.map(({ partitionId }) => partitionId) ?? null,
            selected: picked
              ? {
                  partition: picked.partitionId,
                  follower,
                  answerCorrelation: hint?.exchange.correlationId,
                  answerNode: hint?.node,
                  nextCorrelation: next?.correlationId,
                  nextNode: next ? member.wire.nodeOf(next) : undefined,
                }
              : null,
            replay: {
              log: 'nine acknowledged records, three on each of partitions 0 to 2',
              offsets: 'broker commits never exceed the processed positions',
              handlers: 'each acknowledged record once during stable membership',
            },
          }
        }
        await withRows(runs, rowOf, async () => {
          const episodes = await bothFlags(runs, { scope, cleanup, id }, async (flag, episode) => {
            const label = labelOf('follower', flag)
            await episode.open({
              rackAwareAssignment: flag,
              topic: `e2e-p9c-rack-aware-${label}-${id}`,
              groupId: `e2e-p9c-rack-aware-group-${label}-${id}`,
              replicas: ROTATED,
              members: [{ rackId: 'rack-a' }],
            })
            const { racks, partitions } = episode.placement
            const local = partitions.filter(({ leader }) => racks[leader] === 'rack-a')
            const eligible = partitions.filter(
              ({ leader, replicas, isr, offlineReplicas }) =>
                racks[leader] !== 'rack-a' &&
                isr.length === replicas.length &&
                replicas.some(
                  (node) =>
                    racks[node] === 'rack-a' &&
                    isr.includes(node) &&
                    !offlineReplicas.includes(node)
                )
            )
            Object.assign(episode.facts, { local, eligible })
            if (eligible.length === 0) {
              throw new Error(
                `No partition has a leader outside rack-a and an online in-rack follower: ${JSON.stringify(partitions)}`
              )
            }
            const [picked] = eligible
            const follower = picked.replicas.find((node) => racks[node] === 'rack-a')
            Object.assign(episode.facts, { picked, follower })
            await episode.settled()
            const sent = await episode.produce(label)
            await episode.delivered(sent)

            episode.stage = 'waiting for the preferred read replica'
            const [{ wire }] = episode.members
            await pollUntil(
              scope,
              HINT_BUDGET_MS,
              async () => {
                episode.facts.hint = (await wire.fetchHints()).find(
                  ({ partition, preferredReadReplica }) =>
                    partition === picked.partitionId && preferredReadReplica === follower
                )
                return Boolean(episode.facts.hint)
              },
              { delayMs: POLL_MS, label: 'The preferred read replica answer' }
            )
            episode.stage = 'waiting for the next Fetch'
            await pollUntil(
              scope,
              HINT_BUDGET_MS,
              () => {
                episode.facts.next = wire.nextFetch(episode.facts.hint)
                return Boolean(episode.facts.next)
              },
              { delayMs: POLL_MS, label: 'The next Fetch of the hinted partition' }
            )
            await episode.close()
            return { sent }
          })

          for (const episode of Object.values(episodes)) {
            const { follower, hint, next } = episode.facts
            const [{ wire }] = episode.members
            const asked = wire.askedTopic(next, hint)
            expect(hint.preferredReadReplica).toBe(follower)
            expect(wire.nodeOf(next)).toBe(follower)
            expect(next.fetch.rackId).toBe('rack-a')
            expect(next.fetch.rackId).toBe(hint.exchange.fetch.rackId)
            expect(asked.topicId ?? asked.topic).toBe(hint.topicId ?? hint.topic)
            expectExactlyOnce(episode, episode.result.sent)
          }
        })
      })
    ),
    caseTimeout(CASE_BUDGET_MS)
  )

  test(
    'keeps ownership unchanged without usable consumer or intersecting replica racks',
    liveCase(CASE_BUDGET_MS, (scope) =>
      withCase(scope, async ({ id, cleanup }) => {
        const controls = [
          { name: 'missing', replicas: NONUNIFORM, members: [{}, {}] },
          {
            name: 'disjoint',
            replicas: NONUNIFORM,
            members: [{ rackId: 'rack-x' }, { rackId: 'rack-y' }],
          },
          {
            name: 'uniform',
            replicas: UNIFORM,
            members: [{ rackId: 'rack-a' }, { rackId: 'rack-b' }],
          },
        ]

        const runs = []
        const rowOf = (flag, { facts }) => ({
          case: `no-op-${facts.control}`,
          config: { assignor: 'range', rackAwareAssignment: flag === 'on', members: facts.members },
          replay: {
            log: 'twelve acknowledged records, three on each of partitions 0 to 3',
            offsets: 'broker commits never exceed the processed positions',
            handlers: 'each acknowledged record once, by the owner of its partition',
          },
        })
        await withRows(runs, rowOf, async () => {
          for (const { name, replicas, members } of controls) {
            const episodes = await bothFlags(
              runs,
              { scope, cleanup, id },
              async (flag, episode) => {
                Object.assign(episode.facts, { control: name, members })
                const label = labelOf(name, flag)
                await episode.open({
                  rackAwareAssignment: flag,
                  topic: `e2e-p9c-rack-aware-${label}-${id}`,
                  groupId: `e2e-p9c-rack-aware-group-${label}-${id}`,
                  replicas,
                  members,
                })
                const owned = await episode.settled()
                const sent = await episode.produce(name)
                await episode.delivered(sent)
                await episode.close()
                return { owned, sent }
              }
            )

            for (const episode of [episodes.off, episodes.on]) {
              expect(Object.values(episode.result.owned)).toEqual(PLAIN_RANGE)
              expectExactlyOnce(episode, episode.result.sent)
            }
            expect(Object.values(episodes.on.result.owned)).toEqual(
              Object.values(episodes.off.result.owned)
            )
          }
        })
      })
    ),
    caseTimeout(CASE_BUDGET_MS)
  )
})
