const { writeEvidence } = require('testHelpers')
const { BrokerBouncer, NODES } = require('../../../testHelpers/leaderElection')
const {
  caseTimeout,
  delay,
  liveCase,
  pollUntil,
} = require('../../producer/__tests__/fixtures/bounded')
const {
  JournalMember,
  LiveCorpus,
  STEP_MS,
  missingFrom,
  step,
  replaysBelowFloor,
  runArm,
  untilCovered,
  untilQuiet,
} = require('./fixtures/liveGroupRig')

// Gated: it needs the three-broker cluster, and its outage arms stop and kill one broker at a
// time. Run alone, as the only cluster user: `npm run test:e2e:group-faults`.
const GATED = process.env.GROUP_FAULTS_E2E === '1'
const describeGroupFaults = GATED ? describe : describe.skip

const EVIDENCE = 'group-faults'
/** The arm titles, exactly as the guard's required-title list names them. */
const TITLES = {
  duplicate: 'a duplicate static instance fences exactly one member without rebalancing its peers',
  returns: 'a static member returns within its session without rebalancing peers',
  short: 'short coordinator outages preserve membership and commits resume',
  long: 'a long coordinator outage recovers with at most one membership change',
}
const ARM_BUDGET_MS = 300000
const SESSION_MS = 30000
const CONSUMER_SESSION_MS = 6000
const SHORT_FRACTION = 0.8
const MIN_ISR = [{ name: 'min.insync.replicas', value: '2' }]
const RECORDS = 60
const MORE_RECORDS = 30
const OBSERVE_MS = 20000
const QUIET_MS = 5000
const RETURN_AFTER_MS = 5000
const SHORT_HOLD_MS = 2000
const LONG_HOLD_MS = 45000
const OUTAGE_ALLOWANCE_MS = 150000
// No interval: it commits only when a later batch arrives, which strands the tail of a burst.
const AUTO_COMMIT = { autoCommit: true }

/**
 * @param {number} nodeId the broker's node id
 * @returns {string} its compose service
 */
const serviceOf = (nodeId) => Object.entries(NODES).find(([, node]) => node.nodeId === nodeId)[0]

const REPLAY = {
  log: 'acknowledged records by topic, partition and value; no fixed-log claim',
  offsets: 'broker-confirmed committed offsets read through the case admin',
  handlers: 'delivered records; a replay starts at the last confirmed commit and never below it',
}

/**
 * @param {AbortSignal} scope the case scope
 * @param {string} title the arm's title
 * @param {object} config what the row records
 * @param {Function} body the arm
 * @returns {Promise<void>}
 */
const runGroupFaultArm = (scope, title, config, body) =>
  runArm({ evidence: EVIDENCE, title, config, budgetMs: ARM_BUDGET_MS, replay: REPLAY, body })

/**
 * @param {object} ctx
 * @param {AbortSignal} ctx.scope
 * @param {import('../../producer/__tests__/fixtures/liveProbe').CleanupStack} ctx.cleanup
 * @param {LiveCorpus} ctx.corpus
 * @returns {(name: string, options?: object) => JournalMember} makes a classic member of the case's group
 */
const classicMembers =
  ({ scope, cleanup, corpus }) =>
  (name, consumer = {}) =>
    new JournalMember({
      scope,
      cleanup,
      corpus,
      name,
      groupId: corpus.groupId,
      consumer: { sessionTimeout: SESSION_MS, ...consumer },
      run: AUTO_COMMIT,
    })

/**
 * Starts every member and waits until the group delivered the corpus and stayed quiet.
 * @param {AbortSignal} scope
 * @param {LiveCorpus} corpus
 * @param {JournalMember[]} members
 * @returns {Promise<void>}
 */
const settle = async (scope, corpus, members) => {
  for (const member of members) await member.start()
  await untilCovered(scope, corpus, members)
  await untilQuiet(scope, members, QUIET_MS)
}

/**
 * @param {JournalMember[]} members
 * @returns {Array<{partition: number, offset: string, seq: number}>} every member's deliveries
 */
const deliveriesOf = (members) => members.flatMap(({ delivered }) => delivered)

/**
 * How a group's outage compares with its liveness window: an outage is short for a group when it
 * lasts less than 0.8 of the group's session timeout, measured through restored communication.
 * @param {number} outageMs
 * @param {number} windowMs
 * @returns {'short'|'long'}
 */
const armKind = (outageMs, windowMs) => (outageMs < SHORT_FRACTION * windowMs ? 'short' : 'long')

/**
 * Takes the coordinator of a classic group and of a consumer-protocol group down, one broker
 * only, and judges each group's membership against the outage window.
 * @param {object} ctx
 * @param {AbortSignal} ctx.scope
 * @param {import('../../producer/__tests__/fixtures/liveProbe').CleanupStack} ctx.cleanup
 * @param {LiveCorpus} ctx.corpus
 * @param {object} ctx.observed the evidence the arm accumulates
 * @param {{kind: 'stop'|'kill', holdMs: number}} ctx.outage
 * @param {number} ctx.moreRecords records produced after the outage
 * @param {string} ctx.title the arm's title, which keys the evidence rows
 * @returns {Promise<{classic: JournalMember[], consumer: JournalMember[], window: object}>}
 */
const outageEpisode = async ({ scope, cleanup, corpus, observed, outage, moreRecords, title }) => {
  const make = classicMembers({ scope, cleanup, corpus })
  const nodeId = await corpus.coordinatorOf(corpus.groupId)
  const consumerGroup = await corpus.groupOn(nodeId, `${corpus.groupId}-consumer`)
  observed.coordinator = { nodeId, service: serviceOf(nodeId), consumerGroup }
  writeEvidence(EVIDENCE, {
    scenario: 'group-faults',
    case: title,
    stage: 'before-outage',
    config: { outage, sessionTimeout: SESSION_MS, consumerSessionMs: CONSUMER_SESSION_MS },
    observed: { coordinator: observed.coordinator, issuedAfter: Date.now() },
  })
  const staticId = `${corpus.id}-static`
  const classic = [make('classic-static', { groupInstanceId: staticId }), make('classic-dynamic')]
  const consumer = ['a', 'b'].map(
    (name) =>
      new JournalMember({
        scope,
        cleanup,
        corpus,
        name: `consumer-${name}`,
        groupId: consumerGroup,
        consumer: { groupProtocol: 'consumer' },
        run: AUTO_COMMIT,
      })
  )
  await corpus.produce(RECORDS)
  await settle(scope, corpus, classic)
  await settle(scope, corpus, consumer)
  const floors = {
    classic: await corpus.committed(corpus.groupId),
    consumer: await corpus.committed(consumerGroup),
  }
  const afterSeq = corpus.tick()

  const bouncer = new BrokerBouncer()
  cleanup.add('every broker answers', () => bouncer.assertAllUp())
  const issuedAt = Date.now()
  const returned = await step(
    scope,
    'The coordinator outage',
    () =>
      bouncer.step(
        { service: observed.coordinator.service, ...outage },
        { admin: corpus.admin, topics: [corpus.topic], signal: scope }
      ),
    outage.holdMs + OUTAGE_ALLOWANCE_MS
  )
  const window = { issuedAt, ...returned, outageMs: returned.tRestored - issuedAt }
  await untilQuiet(scope, [...classic, ...consumer], QUIET_MS)
  await corpus.produce(moreRecords)
  await untilCovered(scope, corpus, classic)
  await untilCovered(scope, corpus, consumer)

  observed.window = {
    ...window,
    classic: armKind(window.outageMs, SESSION_MS),
    consumer: armKind(window.outageMs, CONSUMER_SESSION_MS),
  }
  observed.membership = Object.fromEntries(
    [...classic, ...consumer].map((member) => [
      member.name,
      member.membershipBetween(issuedAt).length,
    ])
  )
  observed.replays = {
    classic: replaysBelowFloor(deliveriesOf(classic), floors.classic, afterSeq),
    consumer: replaysBelowFloor(deliveriesOf(consumer), floors.consumer, afterSeq),
  }
  observed.staticSurvived = (await corpus.members(corpus.groupId)).some(
    ({ groupInstanceId }) => groupInstanceId === staticId
  )
  return { classic, consumer, consumerGroup, floors }
}

/**
 * Commits each member's delivered next offsets through the public commit path; only for the stable short arm.
 * @param {JournalMember[]} members
 * @returns {Promise<void>}
 */
const commitHandled = async (members) => {
  for (const member of members) {
    const topics = new Map()
    for (const { topic, partition, offset } of member.delivered) {
      if (!topics.has(topic)) topics.set(topic, new Map())
      const partitions = topics.get(topic)
      const next = BigInt(offset) + 1n
      if (next > (partitions.get(partition) ?? -1n)) partitions.set(partition, next)
    }
    const offsets = [...topics].flatMap(([topic, partitions]) =>
      [...partitions].map(([partition, offset]) => ({ topic, partition, offset: String(offset) }))
    )
    if (offsets.length) await member.client.commitOffsets(offsets)
  }
}

/**
 * Waits until every partition's broker-confirmed commit of a group reached what its members handled.
 * @param {AbortSignal} scope
 * @param {LiveCorpus} corpus
 * @param {string} groupId
 * @param {JournalMember[]} members
 * @returns {Promise<Map<number, number>>} the committed next offsets
 */
const untilCommitted = async (scope, corpus, groupId, members) => {
  const handled = new Map()
  for (const { partition, offset } of deliveriesOf(members))
    handled.set(partition, Math.max(handled.get(partition) ?? 0, Number(offset) + 1))
  let committed = new Map()
  await pollUntil(
    scope,
    STEP_MS * 2,
    async () => {
      committed = await corpus.committed(groupId)
      return [...handled].every(([partition, next]) => (committed.get(partition) ?? -1) >= next)
    },
    { label: 'The broker-confirmed commits', delayMs: 1000 }
  )
  return committed
}

describeGroupFaults('Consumer > classic group faults (live)', () => {
  const add = (title, body) =>
    test(title, liveCase(ARM_BUDGET_MS, body), caseTimeout(ARM_BUDGET_MS))

  add(TITLES.duplicate, (scope) =>
    runGroupFaultArm(
      scope,
      TITLES.duplicate,
      { sessionTimeout: SESSION_MS, partitions: 3, replicationFactor: 3, minIsr: 2 },
      async ({ cleanup, observed }) => {
        const corpus = new LiveCorpus({
          scope,
          cleanup,
          prefix: 'static-duplicate',
          configEntries: MIN_ISR,
          producer: { idempotent: true, maxInFlightRequests: 1 },
        })
        await corpus.prepare()
        await corpus.produce(RECORDS)
        const make = classicMembers({ scope, cleanup, corpus })
        const staticId = `${corpus.id}-one`
        const first = make('static-one', { groupInstanceId: staticId })
        const control = make('static-two', { groupInstanceId: `${corpus.id}-two` })
        const peer = make('dynamic')
        await settle(scope, corpus, [first, control, peer])
        observed.control = {
          crashes: [first, control, peer].map(({ crashes }) => crashes.length),
          members: (await corpus.members(corpus.groupId)).length,
        }
        expect(observed.control).toEqual({ crashes: [0, 0, 0], members: 3 })

        const floors = await corpus.committed()
        const afterSeq = corpus.tick()
        const joinedAt = Date.now()
        const duplicate = make('static-one-duplicate', { groupInstanceId: staticId })
        await duplicate.start()
        await pollUntil(
          scope,
          STEP_MS * 2,
          () => [first, duplicate].some(({ crashes }) => crashes.length > 0),
          { label: 'The fence', delayMs: 250 }
        )
        await delay(scope, OBSERVE_MS)
        const all = [first, control, peer, duplicate]
        const fenced = all.flatMap((member) => member.crashesOf('FENCED_INSTANCE_ID'))
        observed.fenced = all.map(({ name, crashes }) => ({ name, crashes }))
        observed.peerMembership = [control, peer].map(
          (member) => member.membershipBetween(joinedAt).length
        )
        expect(all.flatMap(({ crashes }) => crashes)).toHaveLength(1)
        expect(fenced).toHaveLength(1)
        expect(fenced[0].restart).toBe(false)
        expect(observed.peerMembership).toEqual([0, 0])

        await corpus.produce(MORE_RECORDS)
        const live = all.filter(({ crashes }) => crashes.length === 0)
        await untilCovered(scope, corpus, all)
        observed.replays = replaysBelowFloor(deliveriesOf(all), floors, afterSeq)
        observed.missing = missingFrom(corpus.acknowledged, deliveriesOf(all))
        expect(observed.replays).toEqual([])
        expect(observed.missing).toEqual([])
        expect(live).toHaveLength(3)
        return corpus
      }
    )
  )

  add(TITLES.returns, (scope) =>
    runGroupFaultArm(
      scope,
      TITLES.returns,
      { sessionTimeout: SESSION_MS, returnAfterMs: RETURN_AFTER_MS, partitions: 3 },
      async ({ cleanup, observed }) => {
        const corpus = new LiveCorpus({
          scope,
          cleanup,
          prefix: 'static-return',
          configEntries: MIN_ISR,
          producer: { idempotent: true, maxInFlightRequests: 1 },
        })
        await corpus.prepare()
        await corpus.produce(RECORDS)
        const make = classicMembers({ scope, cleanup, corpus })
        const staticId = `${corpus.id}-returning`
        const leaving = make('static-before', { groupInstanceId: staticId })
        const peers = [make('dynamic-one'), make('dynamic-two')]
        await settle(scope, corpus, [leaving, ...peers])
        const before = (await corpus.members(corpus.groupId)).find(
          ({ groupInstanceId }) => groupInstanceId === staticId
        )
        const floors = await corpus.committed()
        const afterSeq = corpus.tick()

        const goneAt = Date.now()
        await leaving.stop({ leaveGroup: false })
        await delay(scope, RETURN_AFTER_MS)
        const returning = make('static-after', { groupInstanceId: staticId })
        await returning.start()
        await untilQuiet(scope, [returning, ...peers], QUIET_MS)
        const after = (await corpus.members(corpus.groupId)).find(
          ({ groupInstanceId }) => groupInstanceId === staticId
        )
        observed.assignment = { before: before.partitions, after: after.partitions }
        observed.session = { goneAt, returnedAfterMs: Date.now() - goneAt, sessionMs: SESSION_MS }
        observed.peerMembership = peers.map((member) => member.membershipBetween(goneAt).length)
        expect(after.partitions).toEqual(before.partitions)
        expect(observed.peerMembership).toEqual([0, 0])

        await corpus.produce(MORE_RECORDS)
        const all = [leaving, ...peers, returning]
        await untilCovered(scope, corpus, all)
        observed.replays = replaysBelowFloor(deliveriesOf(all), floors, afterSeq)
        expect(observed.replays).toEqual([])
        expect(missingFrom(corpus.acknowledged, deliveriesOf(all))).toEqual([])
        return corpus
      }
    )
  )

  add(TITLES.short, (scope) =>
    runGroupFaultArm(
      scope,
      TITLES.short,
      {
        sessionTimeout: SESSION_MS,
        consumerSessionMs: CONSUMER_SESSION_MS,
        holdMs: SHORT_HOLD_MS,
        kinds: ['stop', 'kill'],
      },
      async ({ cleanup, observed }) => {
        observed.outages = []
        let corpus = null
        for (const kind of ['stop', 'kill']) {
          corpus = new LiveCorpus({
            scope,
            cleanup,
            prefix: `coordinator-short-${kind}`,
            configEntries: MIN_ISR,
            producer: { idempotent: true, maxInFlightRequests: 1 },
          })
          await corpus.prepare()
          const episode = {}
          observed.outages.push(episode)
          const run = await outageEpisode({
            scope,
            cleanup,
            corpus,
            observed: episode,
            outage: { kind, holdMs: SHORT_HOLD_MS },
            moreRecords: MORE_RECORDS,
            title: TITLES.short,
          })
          episode.kind = kind
          expect(episode.window.classic).toBe('short')
          for (const member of run.classic)
            expect(member.membershipBetween(episode.window.issuedAt)).toEqual([])
          if (episode.window.consumer === 'short')
            for (const member of run.consumer)
              expect(member.membershipBetween(episode.window.issuedAt)).toEqual([])
          expect(episode.staticSurvived).toBe(true)
          expect(episode.replays).toEqual({ classic: [], consumer: [] })
          await step(scope, 'The handled-offset commit boundary', () => commitHandled(run.classic))
          const committed = await untilCommitted(scope, corpus, corpus.groupId, run.classic)
          for (const [partition, next] of committed)
            expect(next).toBeGreaterThan(run.floors.classic.get(partition))
          for (const member of [...run.classic, ...run.consumer]) await member.stop()
        }
        return corpus
      }
    )
  )

  add(TITLES.long, (scope) =>
    runGroupFaultArm(
      scope,
      TITLES.long,
      { sessionTimeout: SESSION_MS, consumerSessionMs: CONSUMER_SESSION_MS, holdMs: LONG_HOLD_MS },
      async ({ cleanup, observed }) => {
        const corpus = new LiveCorpus({
          scope,
          cleanup,
          prefix: 'coordinator-long',
          configEntries: MIN_ISR,
          producer: { idempotent: true, maxInFlightRequests: 1 },
        })
        await corpus.prepare()
        const run = await outageEpisode({
          scope,
          cleanup,
          corpus,
          observed,
          outage: { kind: 'kill', holdMs: LONG_HOLD_MS },
          moreRecords: MORE_RECORDS,
          title: TITLES.long,
        })
        const all = [...run.classic, ...run.consumer]
        expect(observed.window.outageMs).toBeGreaterThanOrEqual(LONG_HOLD_MS)
        for (const member of all) {
          const joins = member
            .membershipBetween(observed.window.issuedAt)
            .filter(({ kind }) => kind === 'GROUP_JOIN')
          expect(joins.length).toBeLessThanOrEqual(1)
          expect(member.crashes).toEqual([])
        }
        for (const member of all) {
          const lost = member.journal.filter(({ kind }) => kind === 'PARTITIONS_LOST')
          for (const { at } of lost) {
            const rejoin = member.journal.find(
              ({ kind, at: when }) => kind === 'GROUP_JOIN' && when > at
            )
            const wrote = member.journal.filter(
              ({ kind, at: when }) =>
                kind === 'COMMIT_OFFSETS' && when > at && (!rejoin || when < rejoin.at)
            )
            expect(wrote).toEqual([])
          }
        }
        const owners = (await corpus.members(corpus.groupId)).flatMap(
          ({ partitions }) => partitions
        )
        expect(new Set(owners).size).toBe(owners.length)
        expect(observed.replays).toEqual({ classic: [], consumer: [] })
        await untilCommitted(scope, corpus, corpus.groupId, run.classic)
        for (const group of [run.classic, run.consumer])
          expect(missingFrom(corpus.acknowledged, deliveriesOf(group))).toEqual([])
        return corpus
      }
    )
  )
})
