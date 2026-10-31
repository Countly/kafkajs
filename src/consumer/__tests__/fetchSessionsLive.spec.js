const {
  reassignPreferredReplica,
  secureRandom,
  triggerPreferredElection,
  waitForReassignmentToComplete,
  writeEvidence,
} = require('testHelpers')
const { BrokerBouncer, NODES } = require('../../../testHelpers/leaderElection')
const { createChaosSocketFactory } = require('../../../testHelpers/chaosSocket')
const defaultSocketFactory = require('../../network/socketFactory')
const {
  CleanupStack,
  COORDINATOR_TYPES,
  coordinatorOf,
  describeError,
} = require('../../producer/__tests__/fixtures/liveProbe')
const {
  bounded,
  caseTimeout,
  liveCase,
  pollUntil,
} = require('../../producer/__tests__/fixtures/bounded')
const { runWithEvidence } = require('./fixtures/evidenceCase')
const {
  FETCH,
  MODES,
  POLL_MS,
  RECREATE_PARTITIONS,
  SessionRig,
  STEP_MS,
  closeProblems,
  commitProblems,
  connectionOf,
  corpusProblems,
  followerProblems,
  lostOnConnection,
  monotonicOf,
  moveProblems,
  sessionFacts,
  sessionProblems,
  step,
  summarize,
} = require('./fixtures/sessionRig')

// Gated: it moves leaders, stops a broker and injects faults into sockets. Run alone through the
// guarded live entry: `npm run test:e2e:fetch-sessions` is the developer row. Every consumer here
// forces `fetchSessions: true`, whatever the default is.
const describeSessions = process.env.FETCH_SESSIONS_E2E === '1' ? describe : describe.skip

const EVIDENCE = 'fetch-sessions'
const PREFIX = 'e2e-p9c-fetch-sessions'
const CASE_BUDGET_MS = 120000
const CLEANUP_BUDGET_MS = 30000
const PARTITIONS = 20
const RECORDS = 5
const OUTAGE_HOLD_MS = 10000
const CLOSE_BUDGET_MS = 10000
const CLOSE_GRACE_MS = 2000
const PARTITION_LIST = Array.from({ length: PARTITIONS }, (_, partition) => partition)
const RECREATE_PARTITION_LIST = Array.from(
  { length: RECREATE_PARTITIONS },
  (_, partition) => partition
)

/**
 * Runs one case body with its own rig and cleanup, and writes the case's evidence row between the
 * two however the body ended, so the row exists before any cleanup runs.
 * @param {string} name the case
 * @param {AbortSignal} scope the case scope
 * @param {(rig: SessionRig) => Promise<void>} body
 * @returns {Promise<void>}
 * @throws {Error} the body's failure, a failed evidence write, or the failed cleanups
 */
const runCase = (name, scope, body) => {
  const cleanup = new CleanupStack()
  const rig = new SessionRig(scope, cleanup, secureRandom(), PREFIX)
  return runWithEvidence(
    cleanup,
    async (failure) => {
      try {
        writeEvidence(EVIDENCE, {
          scenario: EVIDENCE,
          case: name,
          caseError: failure ? describeError(failure) : null,
          ...(await rig.evidence()),
        })
        return null
      } catch (error) {
        return error
      }
    },
    () => body(rig),
    CLEANUP_BUDGET_MS
  )
}

/**
 * @param {AbortSignal} scope the case scope
 * @param {string} groupId
 * @returns {Promise<number>} the node id of the group's coordinator
 */
const coordinatorNode = async (scope, groupId) => {
  let coordinator
  await pollUntil(
    scope,
    STEP_MS,
    async () => {
      try {
        coordinator = await coordinatorOf(groupId, COORDINATOR_TYPES.GROUP)
        return true
      } catch {
        return false
      }
    },
    { delayMs: POLL_MS, label: 'The group coordinator lookup' }
  )
  return Object.values(NODES).find(({ port }) => port === coordinator.port).nodeId
}

/**
 * @param {SessionRig} rig
 * @param {string} topic
 * @returns {Promise<Map<number, number>>} the leader of each partition now
 */
const leaders = async (rig, topic) => {
  const { topics } = await step(rig.scope, 'The topic metadata', () =>
    rig.admin.fetchTopicMetadata({ topics: [topic] })
  )
  return new Map(topics[0].partitions.map(({ partitionId, leader }) => [partitionId, leader]))
}

/**
 * Waits until a frame matching `predicate` shows on the member's wire.
 * @param {SessionRig} rig
 * @param {object} member
 * @param {(observation: object) => boolean} predicate
 * @param {string} label names the wait
 * @returns {Promise<object>} the first matching frame
 */
const frameWhere = async (rig, member, predicate, label) => {
  let found
  await pollUntil(
    rig.scope,
    STEP_MS,
    async () => {
      found = (await member.wire.observations()).find(predicate)
      return Boolean(found)
    },
    { delayMs: POLL_MS, label }
  )
  return found
}

describeSessions('Consumer > Fetch sessions (live)', () => {
  const bouncer = new BrokerBouncer()

  test(
    'records the full Fetch limit for 1000 and 2001 partitions on one node',
    liveCase(CASE_BUDGET_MS, (scope) =>
      runCase('limit-probe', scope, async (rig) => {
        await rig.open()
        const [leader] = rig.nodeIds
        const outcomes = []
        rig.facts.outcomes = outcomes
        for (const count of [1000, 2001]) {
          const topic = `${PREFIX}-limit-${count}-${rig.id}`
          const partitions = Array.from({ length: count }, (_, partition) => partition)
          await rig.createTopic({
            topic,
            replicaAssignment: partitions.map((partition) => ({ partition, replicas: [leader] })),
          })
          const member = await rig.startMember({
            name: `limit-${count}`,
            mode: 'manual assignment',
            topic,
            partitions,
          })
          const attempts = async () =>
            (await member.wire.observations()).filter(
              (o) => o.sessionEpoch === 0 && o.cells.length === count
            )
          await pollUntil(scope, STEP_MS, async () => (await attempts()).length > 0, {
            delayMs: POLL_MS,
            label: `The full ${count}-cell request`,
          })
          await pollUntil(
            scope,
            STEP_MS,
            async () => {
              const tried = await attempts()
              return tried[0].answer !== null || tried.length > 1 || member.crashes.length > 0
            },
            { delayMs: POLL_MS, label: `The outcome of the ${count}-cell request` }
          )
          const [first, ...retries] = await attempts()
          outcomes.push({
            count,
            node: first.node,
            version: first.version,
            cells: first.cells.length,
            sessionId: first.sessionId,
            sessionEpoch: first.sessionEpoch,
            correlationId: first.correlationId,
            answer: first.answer && {
              errorCode: first.answer.errorCode,
              sessionId: first.answer.sessionId,
              rows: first.answer.rows,
            },
            retries: retries.length,
            crashes: member.crashes.map((error) => `${error.name}: ${error.message}`),
          })
          await step(scope, 'The probe consumer stop', () => member.consumer.disconnect())
        }
        expect(outcomes.map(({ count, cells }) => [count, cells])).toEqual([
          [1000, 1000],
          [2001, 2001],
        ])
        for (const { sessionId, sessionEpoch } of outcomes) {
          expect([sessionId, sessionEpoch]).toEqual([0, 0])
        }
      })
    ),
    caseTimeout(CASE_BUDGET_MS)
  )

  for (const mode of Object.keys(MODES)) {
    test(
      `delivers a fresh corpus once through incremental and sparse replies (${mode})`,
      liveCase(CASE_BUDGET_MS, (scope) =>
        runCase(`corpus ${mode}`, scope, async (rig) => {
          await rig.open()
          const slug = mode.replaceAll(' ', '-')
          const topic = `${PREFIX}-corpus-${slug}-${rig.id}`
          const groupId = `${PREFIX}-corpus-${slug}-group-${rig.id}`
          await rig.createTopic({ topic, partitions: PARTITIONS })
          const member = await rig.startMember({
            name: 'member',
            mode,
            topic,
            partitions: PARTITION_LIST,
            groupId,
          })
          const first = await rig.produce('first', topic, PARTITION_LIST, RECORDS)
          await rig.delivered(member, first)
          const second = await rig.produce('second', topic, [0, 1, 2], 2)
          await rig.delivered(member, second)
          await rig.quiet()

          const observations = await member.wire.observations()
          const facts = sessionFacts(observations)
          rig.facts.sessions = summarize(facts)
          const problems = {
            corpus: corpusProblems([...first, ...second], member.handled),
            commits: member.group
              ? commitProblems(member.handled, await rig.committed(groupId, topic))
              : [],
            sessions: sessionProblems(facts),
            crashes: member.crashes.map((error) => error.message),
          }
          rig.facts.problems = problems
          expect(problems).toEqual({ corpus: [], commits: [], sessions: [], crashes: [] })

          expect(
            sessionProblems(sessionFacts(observations.filter((o) => o.sessionEpoch <= 0)))
          ).not.toEqual([])
          expect(corpusProblems([...first, ...second], member.handled.slice(1))).not.toEqual([])
        })
      ),
      caseTimeout(CASE_BUDGET_MS)
    )
  }

  test(
    'serves pause, resume and a declared seek through sparse replies',
    liveCase(CASE_BUDGET_MS, (scope) =>
      runCase('pause-seek', scope, async (rig) => {
        await rig.open()
        const topic = `${PREFIX}-pause-${rig.id}`
        const groupId = `${PREFIX}-pause-group-${rig.id}`
        const topicId = await rig.createTopic({ topic, partitions: PARTITIONS })
        const member = await rig.startMember({
          name: 'member',
          mode: 'classic eager dynamic',
          topic,
          partitions: PARTITION_LIST,
          groupId,
        })
        const first = await rig.produce('first', topic, PARTITION_LIST, RECORDS)
        await rig.delivered(member, first)

        const key = `${topicId}|3`
        const pausedAt = performance.now()
        await step(scope, 'The pause', () => member.consumer.pause([{ topic, partitions: [3] }]))
        const forgotten = await frameWhere(
          rig,
          member,
          (o) => o.at > pausedAt && o.forgotten.includes(key),
          'The removal of the paused partition'
        )
        const held = await rig.produce('held', topic, [3, 4], 2)
        await rig.delivered(
          member,
          held.filter((r) => r.partition === 4)
        )
        await rig.quiet()
        expect(
          member.handled.filter((r) => r.partition === 3 && r.value.startsWith('held'))
        ).toEqual([])

        const resumedAt = performance.now()
        member.consumer.resume([{ topic, partitions: [3] }])
        await rig.delivered(
          member,
          held.filter((r) => r.partition === 3)
        )
        const readded = await frameWhere(
          rig,
          member,
          (o) => o.at > resumedAt && o.cells.includes(key),
          'The addition of the resumed partition'
        )
        expect(readded.at).toBeGreaterThan(forgotten.at)

        const replay = member.handled.filter((r) => r.partition === 4 && Number(r.offset) >= 2)
        const replayKeys = new Set(replay.map((r) => `${r.topic}|${r.partition}|${r.value}`))
        const before = member.handled.filter((r) => r.partition === 4).length
        member.consumer.seek({ topic, partition: 4, offset: '2' })
        await pollUntil(
          scope,
          STEP_MS,
          () => member.handled.filter((r) => r.partition === 4).length >= before + replay.length,
          { delayMs: POLL_MS, label: 'The declared replay' }
        )
        await rig.quiet()
        rig.facts.replay = { count: replay.length, from: 2 }

        const problems = {
          corpus: corpusProblems([...first, ...held], member.handled, (k) => replayKeys.has(k)),
          commits: commitProblems(member.handled, await rig.committed(groupId, topic)),
          undeclared: corpusProblems([...first, ...held], member.handled),
        }
        rig.facts.problems = problems
        expect(problems.corpus).toEqual([])
        expect(problems.commits).toEqual([])
        expect(problems.undeclared.length).toBeGreaterThan(0)
        expect(member.handled.filter((r) => r.partition === 4)).toHaveLength(before + replay.length)
      })
    ),
    caseTimeout(CASE_BUDGET_MS)
  )

  test(
    'forgets the final partition once and wakes on resume',
    liveCase(CASE_BUDGET_MS, (scope) =>
      runCase('final-partition', scope, async (rig) => {
        await rig.open()
        const topic = `${PREFIX}-final-${rig.id}`
        const topicId = await rig.createTopic({ topic, partitions: 1 })
        const member = await rig.startMember({
          name: 'member',
          mode: 'manual assignment',
          topic,
          partitions: [0],
        })
        const first = await rig.produce('first', topic, [0], 3)
        await rig.delivered(member, first)

        const key = `${topicId}|0`
        const pausedAt = performance.now()
        await step(scope, 'The pause', () => member.consumer.pause([{ topic, partitions: [0] }]))
        await frameWhere(
          rig,
          member,
          (o) => o.at > pausedAt && o.forgotten.includes(key),
          'The removal of the final partition'
        )
        await rig.quiet()
        const idle = (await member.wire.observations()).length
        await rig.quiet()
        await rig.quiet()
        const observations = await member.wire.observations()
        const removals = observations.filter((o) => o.forgotten.includes(key))
        rig.facts.final = { removals: removals.length, idleFrames: observations.length - idle }
        expect(removals).toHaveLength(1)
        expect(observations).toHaveLength(idle)

        const later = await rig.produce('later', topic, [0], 2)
        await rig.quiet()
        expect(member.handled.filter((r) => r.value.startsWith('later'))).toEqual([])
        const resumedAt = performance.now()
        member.consumer.resume([{ topic, partitions: [0] }])
        await rig.delivered(member, later)
        await frameWhere(
          rig,
          member,
          (o) => o.at > resumedAt && o.cells.includes(key),
          'The addition of the resumed partition'
        )
        expect(corpusProblems([...first, ...later], member.handled)).toEqual([])
      })
    ),
    caseTimeout(CASE_BUDGET_MS)
  )

  test(
    'moves the effective session to a new leader',
    liveCase(CASE_BUDGET_MS, (scope) =>
      runCase('leader-move', scope, async (rig) => {
        await rig.open()
        const [oldNode, newNode] = rig.nodeIds
        const topic = `${PREFIX}-move-${rig.id}`
        const groupId = `${PREFIX}-move-group-${rig.id}`
        const topicId = await rig.createTopic({
          topic,
          replicaAssignment: [{ partition: 0, replicas: [oldNode, newNode] }],
        })
        Object.assign(rig.facts, { oldNode, newNode })
        const member = await rig.startMember({
          name: 'member',
          mode: 'classic eager dynamic',
          topic,
          partitions: [0],
          groupId,
        })
        const pre = await rig.produce('pre', topic, [0], 3)
        await rig.delivered(member, pre)

        const movedAt = performance.now()
        reassignPreferredReplica(topic, [newNode, oldNode], newNode)
        await step(scope, 'The reassignment', () => waitForReassignmentToComplete())
        triggerPreferredElection(topic)
        await pollUntil(
          scope,
          STEP_MS,
          async () => (await leaders(rig, topic)).get(0) === newNode,
          {
            delayMs: POLL_MS,
            label: 'The new leader',
          }
        )
        const post = await rig.produce('post', topic, [0], 3)
        await rig.delivered(member, post)
        await rig.quiet()

        const observations = await member.wire.observations()
        const key = `${topicId}|0`
        const evidence = { observations, key, movedAt, oldNode, newNode }
        const problems = {
          move: moveProblems(evidence),
          corpus: corpusProblems([...pre, ...post], member.handled),
          commits: commitProblems(member.handled, await rig.committed(groupId, topic)),
        }
        rig.facts.problems = problems
        rig.facts.forgotAtOld = observations.some(
          (o) => o.node === oldNode && o.at > movedAt && o.forgotten.includes(key)
        )
        expect(problems).toEqual({ move: [], corpus: [], commits: [] })
        expect(moveProblems({ ...evidence, movedAt: Infinity })).not.toEqual([])
        expect(
          moveProblems({
            ...evidence,
            observations: observations.map((o) => ({ ...o, node: oldNode })),
          })
        ).not.toEqual([])
      })
    ),
    caseTimeout(CASE_BUDGET_MS)
  )

  test(
    'moves the next effective Fetch to an eligible in-rack follower with either assignment flag',
    liveCase(CASE_BUDGET_MS, (scope) =>
      runCase('follower', scope, async (rig) => {
        await rig.open()
        const [leader, follower, third] = rig.nodeIds
        const rackId = rig.racks.get(follower)
        if (!rackId || rig.racks.get(leader) === rackId) {
          throw new Error(
            'The cluster has no in-rack follower: run this scenario on the racks compose'
          )
        }
        rig.facts.placement = { leader, follower, third, racks: [...rig.racks] }
        const arms = []
        rig.facts.arms = arms
        for (const rackAwareAssignment of [false, true]) {
          const label = rackAwareAssignment ? 'on' : 'off'
          const topic = `${PREFIX}-follower-${label}-${rig.id}`
          const groupId = `${PREFIX}-follower-${label}-group-${rig.id}`
          const topicId = await rig.createTopic({
            topic,
            replicaAssignment: [{ partition: 0, replicas: [leader, follower, third] }],
          })
          const member = await rig.startMember({
            name: `follower-${label}`,
            mode: 'classic eager dynamic',
            topic,
            partitions: [0],
            groupId,
            rackId,
            rackAwareAssignment,
          })
          const pre = await rig.produce(`pre-${label}`, topic, [0], 3)
          await rig.delivered(member, pre)

          const key = `${topicId}|0`
          let hint
          await pollUntil(
            scope,
            STEP_MS,
            async () => {
              const answers = await member.wire.answers()
              const named = answers.find(({ decoded }) =>
                decoded.responses.some(
                  (t) =>
                    (t.topicId ?? t.topicName) === topicId &&
                    t.partitions.some((p) => p.preferredReadReplica === follower)
                )
              )
              hint = named && {
                node: member.wire.nodeOf(named.exchange),
                at: named.exchange.answer.at,
              }
              return Boolean(named)
            },
            { delayMs: POLL_MS, label: 'The preferred read replica hint' }
          )
          const post = await rig.produce(`post-${label}`, topic, [0], 3)
          await rig.delivered(member, post)
          await rig.quiet()

          const observations = await member.wire.observations()
          const evidence = { observations, hint, key, follower, rackId }
          const problems = {
            follower: followerProblems(evidence),
            corpus: corpusProblems([...pre, ...post], member.handled),
          }
          arms.push({ rackAwareAssignment, hint, problems })
          expect(problems).toEqual({ follower: [], corpus: [] })
          expect(hint.node).toBe(leader)
          expect(
            followerProblems({
              ...evidence,
              observations: observations.map((o) => (o.at > hint.at ? { ...o, node: leader } : o)),
            })
          ).not.toEqual([])
          await step(scope, 'The follower consumer stop', () => member.consumer.disconnect())
        }
      })
    ),
    caseTimeout(CASE_BUDGET_MS)
  )

  test(
    'recreates a topic under the same name and a new topic ID',
    liveCase(CASE_BUDGET_MS, (scope) =>
      runCase('recreation', scope, async (rig) => {
        await rig.open()
        const topic = `${PREFIX}-recreate-${rig.id}`
        const groupId = `${PREFIX}-recreate-group-${rig.id}`
        const oldId = await rig.createTopic({ topic, partitions: RECREATE_PARTITIONS })
        const member = await rig.startMember({
          name: 'member',
          mode: 'classic eager dynamic',
          topic,
          partitions: RECREATE_PARTITION_LIST,
          groupId,
        })
        const first = await rig.produce('first', topic, RECREATE_PARTITION_LIST, RECORDS)
        await rig.delivered(member, first)

        const recreatedAt = performance.now()
        await step(scope, 'The topic deletion', () =>
          rig.admin.deleteTopics({ topics: [topic], timeout: 20000 })
        )
        await step(scope, 'The topic recreation', () =>
          rig.admin.createTopics({
            waitForLeaders: true,
            timeout: 20000,
            topics: [{ topic, numPartitions: RECREATE_PARTITIONS, replicationFactor: 1 }],
          })
        )
        let newId
        await pollUntil(
          scope,
          STEP_MS,
          async () => {
            newId = await rig.topicId(topic)
            return newId !== undefined && newId !== oldId
          },
          { delayMs: POLL_MS, label: 'The new topic ID' }
        )
        Object.assign(rig.facts, { oldId, newId })
        const second = await rig.produce('second', topic, RECREATE_PARTITION_LIST, RECORDS)
        await rig.delivered(member, second)
        await rig.quiet()

        const observations = await member.wire.observations()
        const after = observations.filter((o) => o.at > recreatedAt)
        const newRecords = member.handled.filter((r) => r.value.startsWith('second'))
        const problems = {
          corpus: corpusProblems([...first, ...second], member.handled, undefined, (r) =>
            r.value.startsWith('second') ? 1 : 0
          ),
          positions: newRecords
            .filter((r) => Number(r.offset) !== Number(r.value.split('-').at(-1)))
            .map((r) => `${r.partition} ${r.value} at offset ${r.offset}`),
          commits: commitProblems(member.handled, await rig.committed(groupId, topic)),
          newIncarnation: after.some((o) => o.cells.some((c) => c.startsWith(`${newId}|`)))
            ? []
            : ['no request carried the new topic ID'],
          crashes: member.crashes.map((error) => error.message),
        }
        const committed = [...(await rig.committed(groupId, topic)).values()]
        rig.facts.problems = problems
        rig.facts.committed = committed
        rig.facts.forgotOldId = after.some((o) =>
          o.forgotten.some((k) => k.startsWith(`${oldId}|`))
        )
        expect(problems).toEqual({
          corpus: [],
          positions: [],
          commits: [],
          newIncarnation: [],
          crashes: [],
        })
        expect(committed.every((offset) => offset === RECORDS)).toBe(true)
      })
    ),
    caseTimeout(CASE_BUDGET_MS)
  )

  test(
    'rides out a one-broker outage',
    liveCase(CASE_BUDGET_MS, (scope) =>
      runCase('outage', scope, async (rig) => {
        await rig.open()
        const topic = `${PREFIX}-outage-${rig.id}`
        const groupId = `${PREFIX}-outage-group-${rig.id}`
        await rig.createTopic({ topic, partitions: PARTITIONS })
        const coordinator = await coordinatorNode(scope, groupId)
        const stopped = rig.nodeIds.filter((nodeId) => nodeId !== coordinator).at(-1)
        const service = Object.keys(NODES).find((name) => NODES[name].nodeId === stopped)
        Object.assign(rig.facts, { coordinator, stopped, service })
        const member = await rig.startMember({
          name: 'member',
          mode: 'classic eager dynamic',
          topic,
          partitions: PARTITION_LIST,
          groupId,
        })
        const pre = await rig.produce('pre', topic, PARTITION_LIST, RECORDS)
        await rig.delivered(member, pre)
        const reachable = [...(await leaders(rig, topic))]
          .filter(([, leader]) => leader !== stopped)
          .map(([partition]) => partition)

        const outage = bouncer.step(
          { service, kind: 'stop', holdMs: OUTAGE_HOLD_MS },
          { admin: rig.admin, topics: rig.topics, signal: scope }
        )
        outage.catch(() => {})
        rig.cleanup.add('stopped broker restore', () => outage.then(() => undefined))
        const during = await rig.produce('during', topic, reachable, 2)
        await rig.delivered(member, during)
        const down = await bounded(scope, Infinity, () => outage, { label: 'The outage' })
        rig.facts.outage = down

        const post = await rig.produce('post', topic, PARTITION_LIST, 2)
        await rig.delivered(member, post)
        await rig.quiet()

        const committedAtLoss = new Map()
        for (const { topic: from, partition, offset, at } of member.commits) {
          if (at <= down.tDown) committedAtLoss.set(`${from}|${partition}`, offset)
        }
        const replays = (key) => {
          const [from, partition, value] = key.split('|')
          const record = member.handled.find(
            (r) => r.topic === from && String(r.partition) === partition && r.value === value
          )
          return Number(record.offset) >= (committedAtLoss.get(`${from}|${partition}`) ?? 0)
        }
        const observations = await member.wire.observations()
        const downAt = monotonicOf(down.tDown)
        const resumed = observations.some(
          (o) => o.node === stopped && o.at > downAt && o.sessionEpoch === 0
        )
        const problems = {
          corpus: corpusProblems([...pre, ...during, ...post], member.handled, replays),
          commits: commitProblems(member.handled, await rig.committed(groupId, topic)),
          resumed: resumed
            ? []
            : ['the stopped node was never sent a full request after the outage'],
        }
        rig.facts.problems = problems
        expect(problems).toEqual({ corpus: [], commits: [], resumed: [] })
      })
    ),
    caseTimeout(CASE_BUDGET_MS)
  )

  test(
    'rides out dropped, truncated and blackholed replies',
    liveCase(CASE_BUDGET_MS, (scope) =>
      runCase('socket-faults', scope, async (rig) => {
        await rig.open()
        const topic = `${PREFIX}-faults-${rig.id}`
        const groupId = `${PREFIX}-faults-group-${rig.id}`
        await rig.createTopic({ topic, partitions: PARTITIONS })
        const chaos = createChaosSocketFactory(defaultSocketFactory())
        const member = await rig.startMember({
          name: 'member',
          mode: 'classic eager dynamic',
          topic,
          partitions: PARTITION_LIST,
          groupId,
          requestTimeout: 5000,
          innerSocketFactory: chaos,
        })
        const [node] = rig.nodeIds
        const [host, port] = rig.endpoints.get(node).split(':')
        const select = { host, port: Number(port) }
        const corpus = await rig.produce('pre', topic, PARTITION_LIST, 2)
        await rig.delivered(member, corpus)
        const faults = {
          dropped: (socket) => socket.dropResponses(1, { apiKey: FETCH, thenClose: true }),
          truncated: (socket) => socket.truncateAndClose(40),
          blackholed: (socket) => socket.blackhole(),
        }
        rig.facts.faults = {}
        const expected = [...corpus]
        for (const [name, apply] of Object.entries(faults)) {
          const armed = connectionOf(await member.wire.observations(), node)
          // Arms the sockets open now and none opened later, so the reconnect is clean.
          chaos.arm(select, apply)()
          const unanswered = await frameWhere(
            rig,
            member,
            lostOnConnection(node, armed),
            `The ${name} fault on node ${node}`
          )
          const records = await rig.produce(name, topic, PARTITION_LIST, 2)
          expected.push(...records)
          await rig.delivered(member, records)
          const resumed = await frameWhere(
            rig,
            member,
            (o) => o.node === node && o.at > unanswered.at && o.sessionEpoch === 0,
            `The full request after the ${name} fault`
          )
          rig.facts.faults[name] = {
            unansweredAt: unanswered.at,
            resumedAt: resumed.at,
            sameConnection: resumed.connection === unanswered.connection,
          }
        }
        await rig.quiet()
        const problems = {
          corpus: corpusProblems(expected, member.handled),
          commits: commitProblems(member.handled, await rig.committed(groupId, topic)),
          crashes: member.crashes.map((error) => error.message),
        }
        rig.facts.problems = problems
        expect(problems).toEqual({ corpus: [], commits: [], crashes: [] })
      })
    ),
    caseTimeout(CASE_BUDGET_MS)
  )

  test(
    'closes positive-ID sessions after the fetchers stop within the close budget',
    liveCase(CASE_BUDGET_MS, (scope) =>
      runCase('close', scope, async (rig) => {
        await rig.open()
        const topic = `${PREFIX}-close-${rig.id}`
        const groupId = `${PREFIX}-close-group-${rig.id}`
        await rig.createTopic({ topic, partitions: PARTITIONS })
        const member = await rig.startMember({
          name: 'member',
          mode: 'classic eager dynamic',
          topic,
          partitions: PARTITION_LIST,
          groupId,
        })
        const corpus = await rig.produce('corpus', topic, PARTITION_LIST, RECORDS)
        await rig.delivered(member, corpus)
        await rig.quiet()

        const before = await member.wire.observations()
        const startedAt = performance.now()
        await step(scope, 'The bounded close', () =>
          member.consumer.disconnect({ timeout: CLOSE_BUDGET_MS })
        )
        const endedAt = performance.now()
        await rig.quiet()
        const after = await member.wire.observations()
        const evidence = {
          before,
          after,
          startedAt,
          endedAt,
          budgetMs: CLOSE_BUDGET_MS + CLOSE_GRACE_MS,
        }
        const problems = closeProblems(evidence)
        rig.facts.close = {
          elapsedMs: endedAt - startedAt,
          closes: sessionFacts(after).closes.length,
        }
        rig.facts.problems = problems
        expect(problems).toEqual([])
        expect(sessionFacts(after).closes.length).toBeGreaterThan(0)
        expect(
          closeProblems({ ...evidence, endedAt: startedAt + 10 * CLOSE_BUDGET_MS })
        ).not.toEqual([])
        expect(closeProblems({ ...evidence, after: before })).not.toEqual([])
        expect(corpusProblems(corpus, member.handled)).toEqual([])
      })
    ),
    caseTimeout(CASE_BUDGET_MS)
  )
})
