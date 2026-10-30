const { Kafka, logLevel } = require('../../../index')
const defaultSocketFactory = require('../../network/socketFactory')
const { roundRobin, cooperativeSticky } = require('../assigners')
const { plainTextBrokers, secureRandom, writeEvidence, wireVersionsOf } = require('testHelpers')
const { createChaosSocketFactory } = require('../../../testHelpers/chaosSocket')
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
const { CommitEpisodeProbe } = require('./fixtures/commitEpisodeProbe')
const { traceFetches } = require('./fixtures/fetchTrace')
const { exampleTranscript, judge, verifyListenerEvidence } = require('./fixtures/listenerOracle')

// Gated: it needs a real cluster. Run alone: `npm run test:e2e:rebalance-listener`.
const GATED = process.env.REBALANCE_LISTENER_E2E === '1'
const describeListener = GATED ? describe : describe.skip

const EVIDENCE = 'rebalance-listener'
const ARM_BUDGET_MS = 90000
const WAIT_MS = 30000
const CLEANUP_BUDGET_MS = 15000
const PARTITIONS = 6
const RECORDS_PER_PARTITION = 100
const HEAD_PER_PARTITION = 50
const TAIL_PER_PARTITION = 10
const FINAL_AUTO_COMMIT_THRESHOLD = 1000000
const HANDLER_PAUSE_MS = 10
const SESSION_TIMEOUT_MS = 30000
const REBALANCE_TIMEOUT_MS = 60000
const MAX_WAIT_MS = 500
const MISSING_TOPIC = 'UNKNOWN_TOPIC_OR_PARTITION'

const ARMS = [
  [
    'classic eager',
    { protocol: 'classic', assigner: roundRobin, sessionTimeout: SESSION_TIMEOUT_MS },
  ],
  [
    'classic cooperative',
    { protocol: 'classic', assigner: cooperativeSticky, sessionTimeout: SESSION_TIMEOUT_MS },
  ],
  ['consumer protocol', { protocol: 'consumer' }],
]

/**
 * One raw client or admin operation, bounded by the case scope.
 * @template T
 * @param {AbortSignal} scope the case scope
 * @param {string} label names the operation in its time-out message
 * @param {(child: AbortSignal) => Promise<T>} op
 * @returns {Promise<T>}
 */
const step = (scope, label, op) => bounded(scope, WAIT_MS, op, { label })

/**
 * Deletes a topic as a cleanup. Only a confirmed missing topic is success; the connect, the
 * deletion and the disconnect are separate bounded steps of the cleanup's own scope.
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
      { label: 'The topic deletion' }
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
 * One two-member run of the awaited-listener scenario over public clients: the topic, the fixed
 * corpus, the external offset map, both members with their callbacks, and everything the oracle
 * reads, recorded with an independent ordinal clock.
 */
class ListenerScenario {
  /**
   * @param {object} options
   * @param {AbortSignal} options.scope the case scope
   * @param {CleanupStack} options.cleanup the case's cleanup stack
   * @param {string} options.label the arm
   * @param {object} options.arm the arm's fixed settings
   */
  constructor({ scope, cleanup, label, arm }) {
    this.scope = scope
    this.cleanup = cleanup
    this.label = label
    this.arm = arm
    this.id = secureRandom()
    this.topic = `e2e-p9c-listener-${this.id}`
    this.groupId = `e2e-p9c-listener-group-${this.id}`
    this.brokers = plainTextBrokers()
    this.kafka = new Kafka({
      clientId: `e2e-p9c-listener-${this.id}`,
      brokers: this.brokers,
      logLevel: logLevel.NOTHING,
    })
    this.clock = 0
    this.incarnation = null
    this.externalMap = new Map()
    this.produced = []
    this.deliveries = []
    this.resolved = new Map()
    this.hooks = []
    this.fetches = []
    this.episodes = []
    this.seeks = []
    this.members = []
    this.requests = []
    this.error = null
    this.fetchTrace = null
  }

  /** @returns {number} the next ordinal of the scenario's own monotonic clock */
  tick() {
    this.clock += 1
    return this.clock
  }

  /**
   * Creates the topic through the case's own admin, records its UUID and produces the head of the
   * corpus; the tail follows once the second member owns partitions, so it always has records
   * left to deliver without any handler blocking.
   * @returns {Promise<void>}
   */
  async prepare() {
    const { scope, cleanup, kafka, topic } = this
    this.admin = kafka.admin()
    cleanup.add('admin', () => this.admin.disconnect())
    await step(scope, 'The admin connect', () => this.admin.connect())
    this.producer = kafka.producer()
    cleanup.add('producer', () => this.producer.disconnect())
    await step(scope, 'The producer connect', () => this.producer.connect())
    cleanup.add(`topic ${topic}`, (signal) => deleteTopic(kafka, topic, signal))
    await step(scope, 'The topic creation', () =>
      this.admin.createTopics({
        waitForLeaders: true,
        timeout: 20000,
        topics: [{ topic, numPartitions: PARTITIONS, replicationFactor: 2 }],
      })
    )
    await pollUntil(
      scope,
      WAIT_MS,
      async () => {
        const { topics } = await this.admin.fetchTopicMetadata({ topics: [topic] })
        const found = topics[0]
        this.incarnation = found?.topicId ?? null
        return (
          this.incarnation != null &&
          found.partitions.length === PARTITIONS &&
          found.partitions.every(({ leader }) => leader >= 0)
        )
      },
      { label: 'The topic leaders' }
    )
    const spy = traceFetches({ topic, sink: this.fetches, tick: () => this.tick() })
    cleanup.add('fetch trace', () => spy.mockRestore())

    await this.produce(0, HEAD_PER_PARTITION)
  }

  /**
   * Produces records `from` to `to` (exclusive) of every partition and keeps what the broker
   * acknowledged as the corpus. The first call also fixes each partition's starting offset.
   * @param {number} from first record index of each partition
   * @param {number} to one past the last record index of each partition
   * @returns {Promise<void>}
   */
  async produce(from, to) {
    const { scope, topic } = this
    for (let partition = 0; partition < PARTITIONS; partition++) {
      const messages = Array.from({ length: to - from }, (_, i) => ({
        partition,
        key: JSON.stringify({ incarnation: this.incarnation, partition, index: from + i }),
        value: `value-${partition}-${from + i}`,
      }))
      const [ack] = await step(scope, `The production to partition ${partition}`, () =>
        this.producer.send({ topic, messages, acks: -1 })
      )
      messages.forEach(({ value }, i) => {
        this.produced.push({
          topic,
          incarnation: this.incarnation,
          partition,
          offset: Number(ack.baseOffset) + i,
          value,
        })
      })
      if (from === 0) this.externalMap.set(partition, Number(ack.baseOffset))
    }
  }

  /**
   * Reads the broker's committed offsets of the group through the case's admin, on a connection
   * the members do not share.
   * @returns {Promise<Map<number, number|null>>} the offset of each partition, null when none
   */
  async brokerOffsets() {
    const [{ partitions }] = await step(this.scope, 'The broker offset read', () =>
      this.admin.fetchOffsets({ groupId: this.groupId, topics: [this.topic] })
    )
    return new Map(
      partitions.map(({ partition, offset }) => [
        partition,
        offset === '-1' ? null : Number(offset),
      ])
    )
  }

  /**
   * Starts one member whose callbacks load and save offsets through the external map.
   * @param {string} name the member label
   * @param {number} [autoCommitThreshold] set on the member whose close must still hold progress
   * @returns {Promise<object>} the member
   */
  async startMember(name, autoCommitThreshold) {
    const { scope, cleanup, topic } = this
    const clientId = `e2e-p9c-listener-${this.id}-${name}`
    const chaos = createChaosSocketFactory(defaultSocketFactory())
    const probe = new CommitEpisodeProbe(chaos, () => this.tick())
    const kafka = new Kafka({
      clientId,
      brokers: this.brokers,
      logLevel: logLevel.NOTHING,
      socketFactory: probe.socketFactory,
    })
    const { protocol, assigner, sessionTimeout } = this.arm
    const consumer = kafka.consumer({
      groupId: this.groupId,
      maxWaitTimeInMs: MAX_WAIT_MS,
      ...(protocol === 'consumer'
        ? { groupProtocol: 'consumer' }
        : {
            partitionAssigners: [assigner],
            sessionTimeout,
            rebalanceTimeout: REBALANCE_TIMEOUT_MS,
          }),
    })
    const member = {
      name,
      clientId,
      consumer,
      probe,
      assignedOnce: false,
      memberId: null,
      finalProgress: autoCommitThreshold !== undefined,
    }
    this.members.push(name)
    cleanup.add(`consumer ${name}`, () => consumer.disconnect())
    probe.releaseOnCleanup(cleanup, name)
    for (const [event, listener] of [
      [consumer.events.COMMIT_OFFSETS, ({ payload }) => probe.recordCommit(payload)],
      [consumer.events.REQUEST, ({ payload }) => this.requests.push(payload)],
    ]) {
      const remove = consumer.on(event, listener)
      cleanup.add(`listener ${name} ${event}`, () => remove())
    }

    await step(scope, `The ${name} connect`, () => consumer.connect())
    await step(scope, `The ${name} subscribe`, () =>
      consumer.subscribe({ topic, fromBeginning: true })
    )
    const starting = step(scope, `The ${name} run`, () =>
      consumer.run({
        autoCommit: true,
        autoCommitThreshold,
        partitionsConsumedConcurrently: 2,
        rebalanceListener: this.listenerOf(member),
        eachMessage: async ({ topic: delivered, partition, message }) => {
          const ordinal = this.tick()
          this.deliveries.push({
            topic: delivered,
            incarnation: JSON.parse(message.key.toString()).incarnation,
            partition,
            offset: Number(message.offset),
            value: message.value.toString(),
            member: name,
            ordinal,
          })
          await delay(scope, HANDLER_PAUSE_MS)
          this.resolved.set(`${name}|${partition}`, Number(message.offset) + 1)
        },
      })
    )
    member.starting = starting
    return member
  }

  /**
   * The member's callbacks. Entry and return are recorded by a wrapper, after the whole callback
   * has settled, on the scenario's ordinal clock.
   * @param {object} member
   * @returns {object} a public listener
   */
  listenerOf(member) {
    const wrap = (kind, body) => async (payload) => {
      this.scope.throwIfAborted()
      const stage = this.stageOf(member, kind)
      const invocation = {
        kind,
        stage,
        member: member.name,
        enteredOrdinal: this.tick(),
        returnedOrdinal: null,
        partitions: payload.partitions.flatMap(({ partitions }) => partitions),
      }
      this.hooks.push(invocation)
      member.memberId = payload.memberId ?? member.memberId
      try {
        await body(payload, invocation)
      } finally {
        invocation.returnedOrdinal = this.tick()
      }
    }
    return {
      onPartitionsAssigned: wrap('assigned', async ({ partitions }) => {
        for (const partition of partitions.flatMap((entry) => entry.partitions)) {
          const cell = { topic: this.topic, partition }
          const [{ offset, metadata }] = await this.operate('The assigned committed read', () =>
            member.consumer.committed([cell])
          )
          const start = this.externalMap.get(partition) ?? 0
          const from = offset == null || offset === '-1' ? start : Number(offset)
          await this.operate('The assigned seek', () =>
            member.consumer.seek({ ...cell, offset: String(start) })
          )
          await this.operate('The assigned position read', () => member.consumer.position(cell))
          this.seeks.push({ partition, from, to: start, read: { offset, metadata } })
        }
      }),
      onPartitionsRevoked: wrap('revoked', async ({ partitions }, invocation) => {
        const cells = partitions.flatMap(({ partitions: ids }) => ids)
        await this.saveEpisodes(member, invocation, cells)
      }),
      onPartitionsLost: wrap('lost', async () => {}),
    }
  }

  /**
   * Runs one operation of a callback under the case scope. None starts once the case has expired
   * or ended, and an answer that arrives after that is dropped, so a late callback changes nothing
   * the case owns.
   * @template T
   * @param {string} label names the operation in its time-out message
   * @param {() => T|Promise<T>} op the client call or the write to the case's own state
   * @returns {Promise<T>} what `op` returned in time
   * @throws {Error} `op`'s failure, its time-out or the reason the case ended
   */
  async operate(label, op) {
    const result = await step(this.scope, label, async () => op())
    this.scope.throwIfAborted()
    return result
  }

  /**
   * @param {object} member
   * @param {'assigned'|'revoked'|'lost'} kind
   * @returns {'initial'|'round'|'close'} the stage the invocation belongs to
   */
  stageOf(member, kind) {
    if (member.closing) return 'close'
    if (kind === 'assigned' && !member.assignedOnce) {
      member.assignedOnce = true
      return 'initial'
    }
    if (kind === 'revoked' && !member.revokedOnce) {
      member.revokedOnce = true
      return 'initial'
    }
    return 'round'
  }

  /**
   * Saves each revoked cell's quiescent position, reads the broker independently before the
   * callback returns, and records one episode per cell. At close the final commit response is held
   * after the broker acknowledged it, and the broker is read again while the member still waits.
   * A close cell whose saved position is ahead of the broker's offset is expected to commit.
   * @param {object} member
   * @param {object} invocation
   * @param {number[]} cells the revoked partitions
   * @returns {Promise<void>}
   */
  async saveEpisodes(member, invocation, cells) {
    const { topic } = this
    const saved = new Map()
    const processed = new Map()
    for (const partition of cells) {
      processed.set(partition, this.processedBy(member.name, partition))
      const position = Number(
        await this.operate('The revoked position read', () =>
          member.consumer.position({ topic, partition })
        )
      )
      saved.set(partition, position)
      await this.operate('The external map write', () => this.externalMap.set(partition, position))
    }
    const observed = await this.brokerOffsets()
    const closing = invocation.stage === 'close'
    const expected = new Set(
      closing ? cells.filter((cell) => saved.get(cell) > (observed.get(cell) ?? 0)) : []
    )
    if (closing && member.finalProgress && expected.size === 0) {
      this.error ??= `The ${member.name} close callback found no cell ahead of the broker`
    }
    if (closing) member.probe.arm()
    invocation.pending = { cells, saved, processed, observed, expected }
    member.episodesPending = [...(member.episodesPending ?? []), invocation]
  }

  /**
   * Completes the episodes of every revoked invocation of a member: for a close, the held final
   * commit response is read against the broker before it is released.
   * @param {object} member
   * @returns {Promise<void>}
   */
  async completeEpisodes(member) {
    for (const invocation of member.episodesPending ?? []) {
      const { cells, saved, processed, observed, expected } = invocation.pending
      const closing = invocation.stage === 'close'
      let brokerRead = observed
      let held = false
      if (closing) {
        held = await member.probe.heldResponse(this.scope)
        brokerRead = await this.brokerOffsets()
      }
      for (const partition of cells) {
        let correlation = null
        let eligible = false
        const mustCommit = closing && expected.has(partition)
        // A cell the member delivered nothing from has no offset to commit.
        if (mustCommit ? held : !closing && processed.get(partition) > 0) {
          try {
            correlation = this.correlated(member, partition, invocation, closing)
            eligible = true
          } catch (error) {
            this.error ??= describeError(error)
          }
        }
        this.episodes.push({
          id: `${member.name}-${invocation.stage}-${partition}-${invocation.enteredOrdinal}`,
          member: member.name,
          memberId: member.memberId,
          partition,
          incarnation: this.incarnation,
          saved: saved.get(partition),
          processed: processed.get(partition),
          brokerObserved: brokerRead.get(partition) ?? null,
          eligible,
          expected: mustCommit,
          correlation,
        })
      }
      if (closing) member.probe.release()
    }
    member.episodesPending = []
  }

  /**
   * @param {object} member
   * @param {number} partition
   * @param {object} invocation the revoked callback's record
   * @param {boolean} closing whether the commit is the final one after the callback
   * @returns {object} the correlated commit fields, with the topic and incarnation of the run
   */
  correlated(member, partition, invocation, closing) {
    const found = member.probe.correlate(
      closing
        ? { partition, after: invocation.returnedOrdinal, before: Infinity, single: true }
        : { partition, after: 0, before: invocation.enteredOrdinal, single: false }
    )
    return {
      apiKey: found.apiKey,
      correlationId: found.correlationId,
      groupId: found.groupId,
      memberId: found.memberId,
      topic: found.topic,
      incarnation: this.incarnation,
      partition,
    }
  }

  /**
   * @param {string} member the label
   * @param {number} partition
   * @returns {number} one past the highest offset this member's handler entered for the partition
   */
  processedBy(member, partition) {
    const offsets = this.deliveries
      .filter((d) => d.member === member && d.partition === partition)
      .map((d) => d.offset)
    return offsets.length ? Math.max(...offsets) + 1 : 0
  }

  /**
   * Closes a member and finishes its close episodes.
   * @param {object} member
   * @returns {Promise<void>}
   */
  async closeMember(member) {
    member.closing = true
    const closing = member.consumer.disconnect()
    closing.catch(() => {})
    await pollUntil(
      this.scope,
      WAIT_MS,
      () =>
        this.hooks.some(
          (h) => h.member === member.name && h.stage === 'close' && h.returnedOrdinal
        ),
      { label: `The ${member.name} close callback` }
    ).catch((error) => {
      this.error ??= describeError(error)
    })
    await this.completeEpisodes(member)
    await step(this.scope, `The ${member.name} disconnect`, () => closing)
  }

  /**
   * @param {string} name the member label
   * @returns {boolean} whether, after its first-member peer's close callback returned, the member's
   *   completed callbacks leave it owning every partition
   */
  ownsAllAfterPeerClose(name) {
    if (!this.hooks.some((h) => h.stage === 'close' && h.member !== name && h.returnedOrdinal)) {
      return false
    }
    const owned = new Set()
    for (const hook of this.hooks) {
      if (hook.member !== name || !hook.returnedOrdinal) continue
      for (const partition of hook.partitions) {
        if (hook.kind === 'assigned') owned.add(partition)
        else owned.delete(partition)
      }
    }
    return owned.size === PARTITIONS
  }

  /**
   * @param {string} name the member label
   * @returns {boolean} whether the member's handlers returned for the last acknowledged record of
   *   every partition
   */
  resolvedAll(name) {
    for (let partition = 0; partition < PARTITIONS; partition++) {
      const last = Math.max(
        ...this.produced.filter((p) => p.partition === partition).map((p) => p.offset)
      )
      if ((this.resolved.get(`${name}|${partition}`) ?? 0) < last + 1) return false
    }
    return true
  }

  /** @returns {number} how many distinct records of the corpus were delivered at least once */
  coverage() {
    return new Set(this.deliveries.map((d) => `${d.partition}|${d.offset}`)).size
  }

  /**
   * @returns {object} the transcript the oracle judges, assembled from what was recorded
   */
  transcript() {
    const label = (clientId) => clientId.slice(clientId.lastIndexOf('-') + 1)
    return {
      topic: this.topic,
      incarnation: this.incarnation,
      members: this.members,
      produced: this.produced,
      deliveries: this.deliveries,
      hooks: this.hooks,
      fetches: this.fetches
        .filter(({ clientId }) => this.members.includes(label(clientId)))
        .map(({ clientId, partition, ordinal }) => ({
          member: label(clientId),
          partition,
          ordinal,
        })),
      episodes: this.episodes,
      seeks: this.seeks,
      replays: [],
      requireExpected: true,
    }
  }
}

/**
 * One arm: two members join and leave in turn over a fixed corpus whose offsets live in an
 * external map. The evidence row is written in a `finally`, so a case that fails or times out still
 * records what it saw, and the primary failure stays the thrown error.
 * @param {string} label the arm title prefix
 * @param {object} arm the arm's fixed settings
 * @returns {(scope: AbortSignal) => Promise<void>}
 */
const armBody = (label, arm) => async (scope) => {
  const cleanup = new CleanupStack()
  const scenario = new ListenerScenario({ scope, cleanup, label, arm })
  const state = { stage: 'starting', enforcement: null }
  let verdict = null
  await cleanup.run(
    async () => {
      try {
        await scenario.prepare()
        state.stage = 'first member'
        const first = await scenario.startMember('first')
        await first.starting
        await pollUntil(scope, WAIT_MS, () => scenario.deliveries.length >= PARTITIONS, {
          label: 'The first deliveries',
        })

        state.stage = 'second member'
        const second = await scenario.startMember('second', FINAL_AUTO_COMMIT_THRESHOLD)
        await second.starting
        await pollUntil(
          scope,
          WAIT_MS,
          () =>
            scenario.hooks.some(
              (h) =>
                h.member === 'second' &&
                h.kind === 'assigned' &&
                h.returnedOrdinal &&
                h.partitions.length > 0
            ),
          { label: 'The second member assignment' }
        )
        await scenario.produce(HEAD_PER_PARTITION, RECORDS_PER_PARTITION)
        await pollUntil(
          scope,
          WAIT_MS,
          () => scenario.deliveries.some((d) => d.member === 'second'),
          {
            label: 'The second member deliveries',
          }
        )

        state.stage = 'enforcement'
        if (arm.protocol === 'classic') {
          await step(scope, 'The explicit enforcement', () =>
            first.consumer.enforceRebalance('listener-live')
          )
          state.enforcement = 'accepted'
        } else {
          await step(scope, 'The refused enforcement', () =>
            first.consumer.enforceRebalance('listener-live').then(
              () => {
                throw new Error('The consumer protocol accepted enforceRebalance')
              },
              (error) => {
                state.enforcement = error.name
              }
            )
          )
        }

        state.stage = 'first member leaves'
        await scenario.closeMember(first)
        state.stage = 'final progress'
        await pollUntil(scope, WAIT_MS, () => scenario.ownsAllAfterPeerClose('second'), {
          label: 'The second member reacquisition',
        })
        await scenario.produce(RECORDS_PER_PARTITION, RECORDS_PER_PARTITION + TAIL_PER_PARTITION)
        await pollUntil(
          scope,
          ARM_BUDGET_MS,
          () => scenario.coverage() >= scenario.produced.length && scenario.resolvedAll('second'),
          { label: 'The whole corpus and the second member progress' }
        )
        state.stage = 'second member leaves'
        await scenario.closeMember(second)

        state.stage = 'judging'
        verifyListenerEvidence(scenario.transcript())
        expect(scenario.seeks.length).toBeGreaterThan(0)
        for (const { read } of scenario.seeks) expect(read).toHaveProperty('metadata')
        expect(scenario.produced).toHaveLength(
          PARTITIONS * (RECORDS_PER_PARTITION + TAIL_PER_PARTITION)
        )
        expect(scenario.error).toBeNull()
        verdict = 'accepted'
        state.stage = 'passed'
      } catch (error) {
        state.error = describeError(error)
        verdict = judge(scenario.transcript())
        throw error
      } finally {
        writeEvidence(EVIDENCE, {
          scenario: 'rebalance-listener',
          case: label,
          config: {
            protocol: arm.protocol,
            partitions: PARTITIONS,
            records: PARTITIONS * (RECORDS_PER_PARTITION + TAIL_PER_PARTITION),
            members: 2,
            autoCommit: true,
            finalAutoCommitThreshold: FINAL_AUTO_COMMIT_THRESHOLD,
            sessionTimeout: arm.sessionTimeout ?? null,
            rebalanceTimeout: arm.protocol === 'classic' ? REBALANCE_TIMEOUT_MS : null,
            budgetMs: ARM_BUDGET_MS,
          },
          replay: {
            log: 'n/a: a fixed log of 660 records, acknowledged offsets kept apart from deliveries',
            offsets: 'every successful eligible episode equals its own saved quiescent position',
            handlers:
              'at least once by incarnation, partition and offset; repeats only in a declared interval',
          },
          wireVersions: wireVersionsOf(scenario.requests),
          state,
          incarnation: scenario.incarnation,
          topic: scenario.topic,
          acknowledged: scenario.produced.length,
          delivered: scenario.deliveries.length,
          covered: scenario.coverage(),
          hooks: scenario.hooks.map(({ pending, ...fields }) => fields),
          seeks: scenario.seeks,
          episodes: scenario.episodes,
          verdict,
        })
      }
    },
    { budgetMs: CLEANUP_BUDGET_MS * 4 }
  )
}

describeListener('Consumer > awaited rebalance listeners (live)', () => {
  test.each(ARMS)(
    '%s: awaited hooks load and save offsets without early delivery',
    liveCase(ARM_BUDGET_MS, async (scope, label, arm) => armBody(label, arm)(scope)),
    caseTimeout(ARM_BUDGET_MS)
  )

  /**
   * @param {(transcript: object) => void} mutate breaks one thing in an accepted transcript
   * @param {RegExp} reason what the oracle must say
   * @returns {void}
   */
  const rejects = (mutate, reason) => {
    expect(judge(exampleTranscript())).toEqual([])
    const broken = exampleTranscript()
    mutate(broken)
    expect(() => verifyListenerEvidence(broken)).toThrow(reason)
  }

  test('control: a delivery before its assigned hook returned is rejected', () => {
    rejects((t) => {
      t.deliveries[0].ordinal = 5
    }, /before its assigned callback returned/)
    rejects((t) => {
      t.fetches[0].ordinal = 5
    }, /fetched before its assigned callback returned/)
  })

  test('control: a saved position from another episode is rejected', () => {
    rejects((t) => {
      t.episodes[0].correlation.partition = 1
    }, /correlates another episode's commit/)
    rejects((t) => {
      t.episodes[1].id = t.episodes[0].id
    }, /recorded twice/)
  })

  test('control: a broker commit that differs from the saved final position is rejected', () => {
    rejects((t) => {
      t.episodes[0].brokerObserved = 2
    }, /saved 3 but the broker held 2/)
    rejects((t) => {
      t.episodes[0].saved = 5
      t.episodes[0].brokerObserved = 5
    }, /passes the processed/)
  })

  test('control: a replay outside the declared interval is rejected', () => {
    rejects((t) => {
      t.deliveries.push({ ...t.deliveries[0], ordinal: 150 })
    }, /outside every declared replay/)
    const declared = exampleTranscript()
    declared.deliveries.push({ ...declared.deliveries[0], ordinal: 150 })
    declared.seeks.push({ partition: 0, from: 3, to: 0 })
    expect(judge(declared)).toEqual([])
  })

  test('control: a record or commit from another topic incarnation is rejected', () => {
    rejects((t) => {
      t.deliveries[1].incarnation = 'topic-b'
    }, /another incarnation/)
    rejects((t) => {
      t.deliveries[1].topic = 'other'
    }, /of another topic/)
    rejects((t) => {
      t.episodes[0].correlation.incarnation = 'topic-b'
    }, /correlates another episode's commit/)
  })

  test('control: a suppressed initial assigned invocation is rejected', () => {
    rejects((t) => {
      t.hooks = t.hooks.filter((hook) => !(hook.kind === 'assigned' && hook.member === 'm1'))
    }, /never had its initial assigned callback invoked/)
  })

  test('control: a suppressed graceful revoked invocation is rejected', () => {
    rejects((t) => {
      t.hooks = t.hooks.filter((hook) => !(hook.stage === 'close' && hook.member === 'm2'))
    }, /never had its graceful revoked callback invoked/)
  })

  test('control: a missing corpus key or broker offset is rejected', () => {
    rejects((t) => {
      t.deliveries.pop()
    }, /never delivered/)
    rejects((t) => {
      t.deliveries[0].value = 'value-0-9'
    }, /is not in the corpus/)
    rejects((t) => {
      t.deliveries[0].offset = 7
    }, /at offset 7, acknowledged at 0/)
    rejects((t) => {
      t.episodes[0].brokerObserved = null
    }, /no broker-observed offset/)
  })
})
