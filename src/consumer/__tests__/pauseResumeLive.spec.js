const { Kafka, logLevel } = require('../../../index')
const { cooperativeSticky } = require('../assigners')
const { plainTextBrokers, secureRandom, writeEvidence } = require('testHelpers')
const { CleanupStack } = require('../../producer/__tests__/fixtures/liveProbe')
const {
  NEVER,
  STOP_GRACE_MS,
  bounded,
  caseTimeout,
  delay,
  liveCase,
  pollUntil,
} = require('../../producer/__tests__/fixtures/bounded')

// Gated: it needs a real cluster. Run alone: `npm run test:e2e:pause-resume`.
const describePauseResume = process.env.PAUSE_RESUME_E2E === '1' ? describe : describe.skip

const EVIDENCE = 'pause-resume'
const CASE_BUDGET_MS = 60000
const PARTITIONS = 4
const MAX_WAIT_MS = 5000
const RESUME_BOUND_MS = 1000
const QUIET_MS = 2000
const WAIT_MS = 30000
const RECORDS_PER_PARTITION = 3
const CLEANUP_BUDGET_MS = 8000
const MISSING_TOPIC = 'UNKNOWN_TOPIC_OR_PARTITION'
const TOPIC_EXISTS = 'TOPIC_ALREADY_EXISTS'
const NOT_READY = [MISSING_TOPIC, 'LEADER_NOT_AVAILABLE']

/**
 * One raw client or admin operation, bounded by the case scope. It refuses to start under an
 * aborted scope, and an answer that arrives after the abort is dropped. An operation is one raw
 * call: a body that starts a second one is two steps, so no continuation of an aborted scope
 * issues further I/O.
 * @template T
 * @param {AbortSignal} scope the case scope
 * @param {string} label names the operation in its time-out message
 * @param {(child: AbortSignal) => Promise<T>} op
 * @returns {Promise<T>}
 */
const step = (scope, label, op) => bounded(scope, WAIT_MS, op, { label })

/** One consumer of the topic, recording what its handler was given and told. */
class Member {
  /**
   * @param {object} options
   * @param {Kafka} options.kafka
   * @param {string} options.groupId
   * @param {string} options.topic
   * @param {'classic'|'consumer'} [options.groupProtocol]
   * @param {object[]} [options.partitionAssigners]
   */
  constructor({ kafka, groupId, topic, groupProtocol = 'classic', partitionAssigners }) {
    this.topic = topic
    this.consumer = kafka.consumer({
      groupId,
      groupProtocol,
      maxWaitTimeInMs: MAX_WAIT_MS,
      ...(partitionAssigners && { partitionAssigners }),
    })
    /** @type {Array<{ partition: number, value: string, at: number }>} */
    this.deliveries = []
    this.recreated = []
    this.crashes = []
  }

  /**
   * Joins the group and delivers with manual commits, committing only what the handler processed.
   * A delivery that arrives after the scope aborted is neither recorded nor committed.
   * @param {AbortSignal} scope
   * @returns {Promise<void>}
   */
  async start(scope) {
    const { consumer } = this
    let joined = false
    consumer.on(consumer.events.GROUP_JOIN, () => {
      joined = true
    })
    consumer.on(consumer.events.TOPIC_RECREATED, ({ payload }) => this.recreated.push(payload))
    consumer.on(consumer.events.CRASH, ({ payload }) => this.crashes.push(payload.error))
    await step(scope, 'The consumer connect', () => consumer.connect())
    await step(scope, 'The consumer subscription', () =>
      consumer.subscribe({ topic: this.topic, fromBeginning: true })
    )
    await step(scope, 'The consumer start', () =>
      consumer.run({
        autoCommit: false,
        eachMessage: async ({ topic, partition, message }) => {
          if (scope.aborted) return
          this.deliveries.push({
            partition,
            value: message.value.toString(),
            at: performance.now(),
          })
          await step(scope, 'The offset commit', () =>
            consumer.commitOffsets([
              { topic, partition, offset: String(Number(message.offset) + 1) },
            ])
          )
        },
      })
    )
    await pollUntil(scope, WAIT_MS, () => joined, { label: 'The group join' })
  }

  /**
   * @param {number} [partition] only this partition's, when given
   * @returns {string[]} the values the handler was given
   */
  values(partition) {
    return this.deliveries
      .filter((entry) => partition === undefined || entry.partition === partition)
      .map(({ value }) => value)
  }
}

/**
 * Sends `records` marked records to each listed partition.
 * @param {AbortSignal} scope the case scope
 * @param {object} producer
 * @param {string} topic
 * @param {string} marker names the corpus, so no value repeats across episodes
 * @param {number[]} partitions
 * @param {number} [records]
 * @returns {Promise<void>}
 */
const produce = (scope, producer, topic, marker, partitions, records = 1) =>
  step(scope, 'The send', () =>
    producer.send({
      topic,
      acks: -1,
      messages: partitions.flatMap((partition) =>
        Array.from({ length: records }, (_, n) => ({
          partition,
          value: `${marker}-${partition}-${n}`,
        }))
      ),
    })
  )

/**
 * @param {AbortSignal} scope the case scope
 * @param {object} admin a connected admin client
 * @param {string} groupId
 * @param {string} topic
 * @returns {Promise<Record<number, string>>} the committed offset of each partition, '-1' when none
 */
const committedOffsets = async (scope, admin, groupId, topic) => {
  const [{ partitions }] = await step(scope, 'The offset read', () =>
    admin.fetchOffsets({ groupId, topics: [topic] })
  )
  return Object.fromEntries(partitions.map(({ partition, offset }) => [partition, String(offset)]))
}

/**
 * Deletes a topic as a cleanup. Only a confirmed missing topic is success, since the recreation
 * arm may already have replaced it; any other failure is the cleanup's failure and taints the run.
 * The connect and the deletion are separate steps of the cleanup's own scope, so a connect that
 * answers after the allowance ended starts no deletion. The disconnect closes what was opened and
 * is bounded by the stop grace, not by the allowance that may already be spent.
 * @param {Kafka} kafka
 * @param {string} topic
 * @param {AbortSignal} signal the cleanup's own, which aborts when its allowance ends
 * @returns {Promise<void>}
 * @throws {Error} a deletion failure other than a missing topic
 */
const deleteTopic = async (kafka, topic, signal) => {
  const admin = kafka.admin()
  try {
    await bounded(signal, CLEANUP_BUDGET_MS, () => admin.connect(), {
      label: 'The cleanup admin connect',
    })
    await bounded(
      signal,
      CLEANUP_BUDGET_MS,
      () => admin.deleteTopics({ topics: [topic], timeout: 10000 }),
      {
        label: 'The topic deletion',
      }
    )
  } catch (error) {
    if (error.type !== MISSING_TOPIC) throw error
  } finally {
    await bounded(NEVER, STOP_GRACE_MS, () => admin.disconnect(), {
      label: 'The cleanup admin disconnect',
    })
  }
}

/**
 * Creates a topic through the case's own admin and waits until every partition has a leader. Each
 * request is one step of the case scope, so nothing is created or polled once the scope aborted.
 * @param {AbortSignal} scope the case scope
 * @param {object} admin a connected admin client
 * @param {string} topic
 * @param {number} partitions
 * @returns {Promise<void>}
 */
const createOwnedTopic = async (scope, admin, topic, partitions) => {
  await step(scope, 'The topic creation', () =>
    admin.createTopics({
      waitForLeaders: true,
      timeout: 20000,
      topics: [{ topic, numPartitions: partitions, replicationFactor: 2 }],
    })
  )
  await pollUntil(
    scope,
    WAIT_MS,
    async () => {
      try {
        const { topics } = await admin.fetchTopicMetadata({ topics: [topic] })
        const found = topics[0]?.partitions ?? []
        return found.length === partitions && found.every(({ leader }) => leader >= 0)
      } catch (error) {
        if (NOT_READY.includes(error.type)) return false
        throw error
      }
    },
    { label: 'The topic leaders' }
  )
}

/**
 * Runs one case body with a connected producer and admin, and everything closed afterwards.
 * Every owned topic and client is registered for cleanup before the operation that can create it
 * partially, and the cleanup runs under its own allowance after the body, however the body ends.
 * @param {AbortSignal} scope
 * @param {(context: object) => Promise<void>} body
 * @returns {Promise<void>}
 */
const withClient = async (scope, body) => {
  const id = secureRandom()
  const brokers = plainTextBrokers()
  const kafka = new Kafka({
    clientId: `e2e-pause-resume-${id}`,
    brokers,
    logLevel: logLevel.NOTHING,
  })
  const cleanup = new CleanupStack()
  await cleanup.run(
    async () => {
      const admin = kafka.admin()
      cleanup.add('admin', () => admin.disconnect())
      await step(scope, 'The admin connect', () => admin.connect())
      /** @returns {Promise<string>} a topic this case owns, deleted at cleanup even if creation stalls */
      const ownTopic = async (name, partitions) => {
        const created = `e2e-pause-resume-${name}${id}`
        cleanup.add(`topic ${created}`, (signal) => deleteTopic(kafka, created, signal))
        await createOwnedTopic(scope, admin, created, partitions)
        return created
      }
      const topic = await ownTopic('', PARTITIONS)
      const producer = kafka.producer()
      cleanup.add('producer', () => producer.disconnect())
      await step(scope, 'The producer connect', () => producer.connect())
      const member = (options) => {
        const created = new Member({ kafka, topic, groupId: `group-${id}`, ...options })
        cleanup.add('consumer', () => created.consumer.disconnect())
        return created
      }
      await body({ id, kafka, topic, producer, admin, member, ownTopic, cleanup, scope })
    },
    { budgetMs: CLEANUP_BUDGET_MS }
  )
}

/**
 * Every partition is paused, so the idle wait is running when a corpus arrives. Nothing is
 * delivered while paused; after `resume()` the first record reaches the handler in under one
 * second although the idle wait would have lasted `maxWaitTimeInMs`.
 */
const resumeLatency = async (scope) =>
  withClient(scope, async ({ id, topic, producer, admin, member }) => {
    const all = [0, 1, 2, 3]
    const consumer = member()
    await consumer.start(scope)
    consumer.consumer.pause([{ topic }])
    await delay(scope, MAX_WAIT_MS + 1000)

    await produce(scope, producer, topic, `paused-${id}`, all, RECORDS_PER_PARTITION)
    await delay(scope, QUIET_MS)
    const silent = consumer.deliveries.length === 0

    const resumedAt = performance.now()
    consumer.consumer.resume([{ topic }])
    await pollUntil(
      scope,
      WAIT_MS,
      () => consumer.deliveries.length >= PARTITIONS * RECORDS_PER_PARTITION
    )
    const latencyMs = consumer.deliveries[0].at - resumedAt
    await pollUntil(scope, WAIT_MS, async () =>
      Object.values(await committedOffsets(scope, admin, `group-${id}`, topic)).every(
        (offset) => offset === String(RECORDS_PER_PARTITION)
      )
    )
    const committed = await committedOffsets(scope, admin, `group-${id}`, topic)

    writeEvidence(EVIDENCE, {
      scenario: 'pause-resume',
      case: 'resume-latency',
      config: { maxWaitTimeInMs: MAX_WAIT_MS, partitions: PARTITIONS, autoCommit: false },
      latencyMs,
      silentWhilePaused: silent,
      committed,
      handlers: consumer.values(),
    })
    expect(silent).toBe(true)
    expect(latencyMs).toBeLessThan(RESUME_BOUND_MS)
    for (const partition of all) {
      expect(consumer.values(partition)).toEqual(
        Array.from({ length: RECORDS_PER_PARTITION }, (_, n) => `paused-${id}-${partition}-${n}`)
      )
    }
    expect(consumer.crashes).toEqual([])
  })

/**
 * Partition 0 stays paused through an ordinary rebalance: a second member of the group joins on
 * another topic, so the group rebalances (the first member revokes and is reassigned the same
 * partitions) and partition 0 must stay silent while another partition delivers.
 * @param {string} name evidence case name
 * @param {object} options passed to both members
 * @returns {(scope: AbortSignal) => Promise<void>}
 */
const rebalanceSurvival = (name, options) => async (scope) =>
  withClient(scope, async ({ id, topic, producer, admin, member, ownTopic }) => {
    const other = await ownTopic('other-', 1)
    const marker = `${name}-${id}`
    const first = member(options)
    await first.start(scope)
    first.consumer.pause([{ topic, partitions: [0] }])
    await delay(scope, MAX_WAIT_MS + 1000)

    const second = member(options)
    second.topic = other
    await second.start(scope)
    const { groups } = await step(scope, 'The group description', () =>
      admin.describeGroups([`group-${id}`])
    )
    const members = groups[0].members.length
    await produce(scope, producer, topic, marker, [0, 1], RECORDS_PER_PARTITION)
    await pollUntil(scope, WAIT_MS, () => first.values(1).length >= RECORDS_PER_PARTITION)
    await delay(scope, QUIET_MS)
    await pollUntil(scope, WAIT_MS, async () => {
      const offsets = await committedOffsets(scope, admin, `group-${id}`, topic)
      return offsets[1] === String(RECORDS_PER_PARTITION)
    })
    const committed = await committedOffsets(scope, admin, `group-${id}`, topic)

    writeEvidence(EVIDENCE, {
      scenario: 'pause-resume',
      case: name,
      config: { ...options, partitionAssigners: options.partitionAssigners?.length },
      members,
      paused: first.consumer.paused(),
      committed,
      handlers: first.values(),
    })
    expect(members).toBe(2)
    expect(first.values(0)).toEqual([])
    expect(first.values(1)).toEqual(
      Array.from({ length: RECORDS_PER_PARTITION }, (_, n) => `${marker}-1-${n}`)
    )
    expect(committed[0]).toBe('-1')
    expect(first.consumer.paused()).toEqual([{ topic, partitions: [0] }])
    expect(first.crashes).toEqual([])
  })

describePauseResume('Consumer > pause and resume (live)', () => {
  test(
    'resumes a fully paused consumer in under one second',
    liveCase(CASE_BUDGET_MS, resumeLatency),
    caseTimeout(CASE_BUDGET_MS)
  )

  test(
    'keeps a paused partition silent through an eager classic rebalance',
    liveCase(CASE_BUDGET_MS, rebalanceSurvival('eager', {})),
    caseTimeout(CASE_BUDGET_MS)
  )

  test(
    'keeps a paused partition silent through a cooperative classic rebalance',
    liveCase(
      CASE_BUDGET_MS,
      rebalanceSurvival('cooperative', { partitionAssigners: [cooperativeSticky] })
    ),
    caseTimeout(CASE_BUDGET_MS)
  )

  test(
    'keeps a paused partition silent through a consumer protocol rebalance',
    liveCase(CASE_BUDGET_MS, rebalanceSurvival('consumer', { groupProtocol: 'consumer' })),
    caseTimeout(CASE_BUDGET_MS)
  )

  test(
    'retires a pause when its topic is deleted and recreated',
    liveCase(CASE_BUDGET_MS, async (scope) =>
      withClient(scope, async ({ id, topic, producer, admin, member }) => {
        const consumer = member()
        await consumer.start(scope)
        consumer.consumer.pause([{ topic, partitions: [0] }])
        await delay(scope, MAX_WAIT_MS + 1000)

        await step(scope, 'The topic deletion', () =>
          admin.deleteTopics({ topics: [topic], timeout: 10000 })
        )
        await pollUntil(
          scope,
          WAIT_MS,
          async () => consumer.recreated.length > 0 || !(await admin.listTopics()).includes(topic)
        )
        await step(scope, 'The topic recreation', () =>
          admin
            .createTopics({
              topics: [{ topic, numPartitions: PARTITIONS }],
              waitForLeaders: true,
            })
            .catch((error) => {
              if (error.type !== TOPIC_EXISTS) throw error
            })
        )
        await pollUntil(scope, WAIT_MS, () => consumer.recreated.length > 0)
        const marker = `recreated-${id}`
        await produce(scope, producer, topic, marker, [0, 1], RECORDS_PER_PARTITION)
        await pollUntil(scope, WAIT_MS, () => consumer.values(0).length >= RECORDS_PER_PARTITION)
        await pollUntil(scope, WAIT_MS, async () => {
          const offsets = await committedOffsets(scope, admin, `group-${id}`, topic)
          return offsets[0] === String(RECORDS_PER_PARTITION)
        })
        const committed = await committedOffsets(scope, admin, `group-${id}`, topic)

        writeEvidence(EVIDENCE, {
          scenario: 'pause-resume',
          case: 'recreation',
          recreated: consumer.recreated.map(({ previousTopicId, topicId }) => ({
            previousTopicId,
            topicId,
          })),
          committed,
          handlers: consumer.values(),
        })
        expect(consumer.recreated).toHaveLength(1)
        expect(consumer.values(0)).toEqual(
          Array.from({ length: RECORDS_PER_PARTITION }, (_, n) => `${marker}-0-${n}`)
        )
        expect(consumer.crashes).toEqual([])
      })
    ),
    caseTimeout(CASE_BUDGET_MS)
  )
})
