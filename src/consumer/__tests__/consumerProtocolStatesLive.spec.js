const fs = require('fs')
const path = require('path')
const { execFile } = require('child_process')
const { promisify } = require('util')
const { Kafka, logLevel } = require('../../../index')
const {
  createCluster,
  plainTextBrokers,
  secureRandom,
  writeEvidence,
  wireVersionsOf,
} = require('testHelpers')
const InstrumentationEventEmitter = require('../../instrumentation/emitter')
const { NETWORK_REQUEST } = require('../../network/instrumentationEvents')
const { generateMemberId } = require('../groupProtocol/memberId')
const {
  bounded,
  caseTimeout,
  delay,
  liveCase,
  pollUntil,
} = require('../../producer/__tests__/fixtures/bounded')
const { CleanupStack } = require('../../producer/__tests__/fixtures/liveProbe')
const { gateNonce } = require('./fixtures/gateNonce')

// Gated: it creates real KIP-848 groups and arms the state fixture's assignment gate. Run alone
// through the guarded live entry: `npm run test:e2e:consumer-protocol-states` is the developer row.
const describeStates = process.env.CONSUMER_PROTOCOL_STATES_E2E === '1' ? describe : describe.skip

const EVIDENCE = 'consumer-protocol-states'
const FIXTURE = path.join(__dirname, 'fixtures', 'consumerProtocolStates.json')
const CAPTURE_BUDGET_MS = 120000
const NAMED_BUDGET_MS = 120000
const DESCRIBE_BUDGET_MS = 2000
const GATE_BUDGET_MS = 15000
const TURN_MS = 200
const POLL_MS = 100
const WINDOW_SAMPLES = 2
const STABLE_BUDGET_MS = 30000
const EMPTY_SUBSET_BUDGET_MS = 30000
const CREATE_BUDGET_MS = 20000
const DELIVERY_BUDGET_MS = 30000
const TEARDOWN_BUDGET_MS = 20000
const HEARTBEAT_APIS = new Set(['ConsumerGroupHeartbeat', 'ConsumerGroupDescribe'])
const MISSING_TOPIC = 'UNKNOWN_TOPIC_OR_PARTITION'
const NOT_READY = [MISSING_TOPIC, 'LEADER_NOT_AVAILABLE']

const run = promisify(execFile)

/**
 * @param {object} group one described group
 * @param {string} memberId
 * @returns {{ current: number[], target: number[] }|undefined} the member's current and target
 *   partitions, flattened over topics
 */
const partitionsOf = (group, memberId) => {
  const member = group.members.find((candidate) => candidate.memberId === memberId)
  if (!member) return undefined
  const flat = ({ topicPartitions }) => topicPartitions.flatMap(({ partitions }) => partitions)
  return { current: flat(member.assignment), target: flat(member.targetAssignment) }
}

/**
 * The window predicate of the Reconciling capture: a live owner still holds a partition the target
 * hands to another member, and that member has not received it.
 * @param {object} group one Describe sample
 * @param {string} owner member that withholds
 * @param {string} taker member the partition is moving to
 * @returns {number|null} the partition in transfer, or null when the sample is not the window
 */
const pendingTransfer = (group, owner, taker) => {
  if (group.errorCode !== 0 || group.groupState !== 'Reconciling') return null
  if (group.groupEpoch !== group.assignmentEpoch) return null
  const from = partitionsOf(group, owner)
  const to = partitionsOf(group, taker)
  if (!from || !to) return null
  const moving = from.current.find(
    (partition) => !from.target.includes(partition) && to.target.includes(partition)
  )
  return moving !== undefined && !to.current.includes(moving) ? moving : null
}

/**
 * The window predicate of the Assigning capture: the committed group epoch is ahead of the last
 * target assignment while Describe still answers.
 * @param {object} group one Describe sample
 * @returns {boolean} whether the sample is inside the epoch gap
 */
const assigningGap = (group) =>
  group.errorCode === 0 &&
  group.groupState === 'Assigning' &&
  group.groupEpoch > group.assignmentEpoch

/**
 * @param {Array<{ at: number }>} samples window samples in order
 * @returns {boolean} whether they span two serviceable Describe answers at least one heartbeat
 *   turn apart inside the same pending transfer
 */
const windowProof = (samples) =>
  samples.length >= WINDOW_SAMPLES && samples.at(-1).at - samples[0].at >= TURN_MS

/**
 * Creates a topic through the case's own admin and waits until every partition has a leader. No
 * request starts once the scope has aborted.
 * @param {AbortSignal} scope
 * @param {object} admin a connected admin client
 * @param {{ topic: string, partitions: number, replicas: number }} spec
 * @returns {Promise<number>} the time the creation request resolved, before the readiness poll
 * @throws {Error} a creation failure, or the reason the scope aborted
 */
const createOwnedTopic = async (scope, admin, { topic, partitions, replicas }) => {
  await bounded(
    scope,
    Infinity,
    () =>
      admin.createTopics({
        waitForLeaders: true,
        timeout: CREATE_BUDGET_MS,
        topics: [{ topic, numPartitions: partitions, replicationFactor: replicas }],
      }),
    { label: 'The topic creation' }
  )
  const createdAt = Date.now()
  await pollUntil(
    scope,
    Infinity,
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
    { delayMs: POLL_MS, label: 'The topic leaders' }
  )
  return createdAt
}

/**
 * Deletes a topic through the case's own admin as one cleanup step. A topic that is already gone
 * is success.
 * @param {AbortSignal} signal the cleanup's own, which aborts when its allowance ends
 * @param {object} admin a connected admin client
 * @param {string} topic
 * @returns {Promise<void>}
 * @throws {Error} a deletion failure other than a missing topic
 */
const deleteOwnedTopic = async (signal, admin, topic) => {
  try {
    await bounded(signal, Infinity, () => admin.deleteTopics({ topics: [topic], timeout: 10000 }), {
      label: 'The topic deletion',
    })
  } catch (error) {
    if (error.type !== MISSING_TOPIC) throw error
  }
}

/** One raw KIP-848 member: its stable id, epoch and what it reports as owned. */
class RawMember {
  /** @param {string} name label for evidence and failures */
  constructor(name) {
    this.name = name
    this.memberId = generateMemberId()
    this.epoch = 0
    this.owned = []
    this.withhold = false
  }

  /**
   * Adopts an answer: the epoch, and the assignment as the new ownership unless this member
   * withholds revocations, in which case it keeps every partition it already reported.
   * @param {{ memberEpoch: number, assignment: object|null }} response one heartbeat answer
   * @returns {void}
   */
  accept({ memberEpoch, assignment }) {
    this.epoch = memberEpoch
    if (!assignment) return
    const next = assignment.topicPartitions.map(({ topicId, partitions }) => ({
      topicId,
      partitions: [...partitions],
    }))
    this.owned = this.withhold ? mergeOwned(this.owned, next) : next
  }
}

/**
 * @param {Array<{ topicId: string, partitions: number[] }>} left
 * @param {Array<{ topicId: string, partitions: number[] }>} right
 * @returns {Array<{ topicId: string, partitions: number[] }>} the union per topic id
 */
const mergeOwned = (left, right) => {
  const byTopic = new Map()
  for (const { topicId, partitions } of [...left, ...right]) {
    byTopic.set(topicId, new Set([...(byTopic.get(topicId) ?? []), ...partitions]))
  }
  return [...byTopic].map(([topicId, partitions]) => ({ topicId, partitions: [...partitions] }))
}

/**
 * Fixture-local capture of one broker-held group state. It owns the raw cluster, two generated
 * stable member ids, one group, one nonce and the transcript, and creates no production API. The
 * case's scope is the only deadline: every raw call, gate operation and wait runs under it, and an
 * expiry, abort or failed cleanup taints the run through the shared live-run contract.
 */
class StateCapture {
  /**
   * @param {object} options
   * @param {string} options.version broker version the arm runs against
   * @param {string} options.groupId
   * @param {string} options.topic
   * @param {string} options.nonce one bounded alphanumeric gate identity
   * @param {AbortSignal} options.scope the case's scope
   */
  constructor({ version, groupId, topic, nonce, scope }) {
    this.version = version
    this.groupId = groupId
    this.topic = topic
    this.nonce = nonce
    this.scope = scope
    this.cluster = null
    this.removeListener = null
    this.armed = false
    this.coordinator = null
    this.topicId = null
    this.transcript = []
    this.members = { A: new RawMember('A'), B: new RawMember('B'), C: new RawMember('C') }
  }

  /**
   * Connects the raw cluster, finds the group coordinator and reads the topic id. The cluster is
   * assigned before it connects, so `register`'s cleanup always sees it.
   * @returns {Promise<void>}
   * @throws {Error} when the case's scope aborts, or a lookup fails
   */
  async open() {
    const emitter = new InstrumentationEventEmitter()
    this.removeListener = emitter.addListener(NETWORK_REQUEST, ({ payload }) => this.note(payload))
    this.cluster = createCluster({ logLevel: logLevel.NOTHING, instrumentationEmitter: emitter })
    await bounded(this.scope, Infinity, () => this.cluster.connect())
    await bounded(this.scope, Infinity, () => this.cluster.addTargetTopic(this.topic))
    this.topicId = this.cluster.topicId(this.topic)
    if (!this.topicId) throw new Error(`No topic id for ${this.topic}`)
    this.coordinator = await bounded(this.scope, Infinity, () =>
      this.cluster.findGroupCoordinator({ groupId: this.groupId })
    )
  }

  /**
   * Records one Heartbeat or Describe request the raw cluster sent, with its correlation id.
   * @private
   * @param {{ apiName: string, apiVersion: number, correlationId: number, size: number }} payload
   * @returns {void}
   */
  note({ apiName, apiVersion, correlationId, size }) {
    if (HEARTBEAT_APIS.has(apiName)) {
      this.transcript.push({ apiName, apiVersion, correlationId, size, at: Date.now() })
    }
  }

  /**
   * One ConsumerGroupHeartbeat: the full identity and subscription on a join, literal ownership on
   * every update.
   * @param {RawMember} member
   * @param {{ epoch?: number, topicPartitions?: object[] }} [options] overrides over the member's
   *   own epoch and reported ownership
   * @returns {Promise<object>} the decoded answer
   * @throws {Error} on a broker error or when the scope aborts
   */
  async heartbeat(member, { epoch = member.epoch, topicPartitions = member.owned } = {}) {
    const joining = epoch === 0
    const response = await bounded(this.scope, Infinity, () =>
      this.coordinator.consumerGroupHeartbeat({
        groupId: this.groupId,
        memberId: member.memberId,
        memberEpoch: epoch,
        rebalanceTimeoutMs: joining ? 60000 : -1,
        subscribedTopicNames: joining ? [this.topic] : null,
        topicPartitions: joining ? [] : topicPartitions,
      })
    )
    member.accept(response)
    return response
  }

  /**
   * One foreground heartbeat turn for each member, in order.
   * @param {RawMember[]} members
   * @returns {Promise<void>}
   */
  async turn(members) {
    for (const member of members) await this.heartbeat(member)
  }

  /**
   * Describes the group once, at most 2000 ms.
   * @returns {Promise<{ group: object, elapsedMs: number }>} exactly one matching group
   * @throws {Error} unless the answer holds one errorCode-0 group with this id
   */
  async describe() {
    const startedAt = performance.now()
    const { groups } = await bounded(
      this.scope,
      DESCRIBE_BUDGET_MS,
      () => this.coordinator.consumerGroupDescribe({ groupIds: [this.groupId] }),
      { label: 'Describe' }
    )
    const matching = groups.filter(({ groupId }) => groupId === this.groupId)
    if (matching.length !== 1 || matching[0].errorCode !== 0) {
      throw new Error(`Describe did not return one serviceable group: ${JSON.stringify(groups)}`)
    }
    return { group: matching[0], elapsedMs: performance.now() - startedAt }
  }

  /**
   * Runs one operation of the existing guard's gate through a foreground child, never touching a
   * container directly. Starts none under an aborted scope; a cleanup passes its own signal
   * instead, which bounds the child and lets release and retire run after the case aborted.
   * @param {'arm'|'release'|'read'|'retire'} action
   * @param {{ signal?: AbortSignal }} [options] the cleanup's signal
   * @returns {Promise<object[]>} the fixture's events for this group and nonce
   * @throws {Error} when the guard environment is absent, the child fails, or the scope aborted
   */
  async gate(action, { signal } = {}) {
    const { P9_CLUSTER_GUARD: guard, P9_CLUSTER_MANIFEST: manifest } = process.env
    if (!guard || !manifest) {
      throw new Error('The state gate needs P9_CLUSTER_GUARD and P9_CLUSTER_MANIFEST')
    }
    if (!signal) this.scope.throwIfAborted()
    if (action === 'arm') this.armed = true
    const { stdout } = await run(
      process.execPath,
      [guard, 'gate', manifest, action, this.groupId, this.nonce],
      { timeout: GATE_BUDGET_MS, killSignal: 'SIGKILL', signal: signal ?? this.scope }
    )
    return JSON.parse(stdout.trim() || '[]')
  }

  /**
   * Heartbeats every member until `done` holds, one turn at a time, no more than 500 ms apart.
   * @param {RawMember[]} members
   * @param {() => boolean|Promise<boolean>} done
   * @param {number} ms budget of this wait inside the case
   * @param {string} label
   * @returns {Promise<void>}
   * @throws {Error} `<label> did not settle within <ms> ms`
   */
  async drive(members, done, ms, label) {
    await pollUntil(
      this.scope,
      ms,
      async () => {
        await this.turn(members)
        return done()
      },
      { delayMs: TURN_MS, label }
    )
  }

  /**
   * Joins the members one after another and drives them until the group is Stable with every
   * partition owned exactly once.
   * @param {RawMember[]} members
   * @param {number} partitions the topic's partition count
   * @returns {Promise<object>} the Stable sample
   */
  async stabilise(members, partitions) {
    let sample
    await this.drive(
      members,
      async () => {
        ;({ group: sample } = await this.describe())
        const owned = members.flatMap(
          (member) => partitionsOf(sample, member.memberId)?.current ?? []
        )
        return (
          sample.groupState === 'Stable' &&
          sample.groupEpoch === sample.assignmentEpoch &&
          owned.length === partitions &&
          new Set(owned).size === partitions
        )
      },
      STABLE_BUDGET_MS,
      'The group to become Stable'
    )
    return sample
  }

  /**
   * Reconciling: A holds both partitions, B joins to take one, and A keeps reporting its old
   * ownership so only the revoked partition's acknowledgement is withheld.
   * @returns {Promise<object>} samples, the partition in transfer, the control and the transcript
   * @throws {Error} when the window is never observed, or the control accepts the wrong window
   */
  async captureReconciling() {
    const { A, B } = this.members
    await this.drive(
      [A],
      () => A.owned[0]?.partitions.length === 2,
      STABLE_BUDGET_MS,
      'A to own both'
    )
    await this.turn([A])
    A.withhold = true
    const samples = []
    let moving = null
    await this.drive(
      [A, B],
      async () => {
        const { group, elapsedMs } = await this.describe()
        const partition = pendingTransfer(group, A.memberId, B.memberId)
        if (partition !== null) {
          moving ??= partition
          samples.push({ at: Date.now(), elapsedMs, partition, group })
        }
        return windowProof(samples)
      },
      CAPTURE_BUDGET_MS / 2,
      'The pending-owner window'
    )
    A.withhold = false
    A.owned = A.owned.map(({ topicId, partitions }) => ({
      topicId,
      partitions: partitions.filter((partition) => partition !== moving),
    }))
    const stable = await this.stabilise([A, B], 2)
    return { moving, samples, stable, transcript: this.transcript }
  }

  /**
   * The negative control of the Reconciling window: the same two-member transfer with an owner
   * that acknowledges its revocation at the first heartbeat. Describe is sampled until the group is
   * Stable, and the window proof must reject what it saw.
   * @returns {Promise<Array<{ at: number }>>} the pending-owner samples the control saw
   * @throws {Error} when the control group never becomes Stable
   */
  async controlImmediate() {
    const { A, B } = this.members
    await this.drive(
      [A],
      () => A.owned[0]?.partitions.length === 2,
      STABLE_BUDGET_MS,
      'A to own both'
    )
    const samples = []
    await this.drive(
      [A, B],
      async () => {
        const { group } = await this.describe()
        if (pendingTransfer(group, A.memberId, B.memberId) !== null)
          samples.push({ at: Date.now() })
        return (
          group.groupState === 'Stable' && partitionsOf(group, B.memberId)?.current.length === 1
        )
      },
      STABLE_BUDGET_MS,
      'The control group to become Stable'
    )
    return samples
  }

  /**
   * Assigning: after a Stable baseline the gate is armed for exactly this group and nonce, a third
   * member joins to force an epoch bump, and Describe must independently show the epoch gap.
   * @returns {Promise<object>} the gate events, Describe samples, release and convergence
   * @throws {Error} when the gate is never reached, the gap is not seen by Describe, or the group
   *   does not converge before the gate's own expiry
   */
  async captureAssigning() {
    const { A, B, C } = this.members
    await this.stabilise([A, B], 2)
    await this.gate('arm')
    const samples = []
    let arrived = null
    await this.drive(
      [A, B, C],
      async () => {
        arrived ??= (await this.gate('read')).find(
          ({ status, groupEpoch, assignmentEpoch }) =>
            status === 'arrived' && groupEpoch > assignmentEpoch
        )
        if (!arrived) return false
        const { group, elapsedMs } = await this.describe()
        if (assigningGap(group)) samples.push({ at: Date.now(), elapsedMs, group })
        return samples.length >= WINDOW_SAMPLES
      },
      CAPTURE_BUDGET_MS / 2,
      'The committed epoch gap'
    )
    const releasedAt = Date.now()
    const events = await this.gate('release')
    await this.drive([A, B, C], () => false, TURN_MS, 'one heartbeat to retrigger').catch(() => {})
    const stable = await this.stabilise([A, B, C], 2)
    const final = await this.gate('read')
    return { arrived, samples, releasedAt, events: final.length ? final : events, stable }
  }

  /**
   * Registers this capture's cleanup before anything is acquired. Each step is attempted on its
   * own allowance and tolerates a capture that never got that far; they run in reverse: gate,
   * member leave, then the cluster.
   * @param {CleanupStack} cleanup
   * @returns {void}
   */
  register(cleanup) {
    cleanup.add(`cluster ${this.groupId}`, () => this.disconnect())
    cleanup.add(`member leave ${this.groupId}`, (signal) => this.leave(signal))
    cleanup.add(`gate retire ${this.groupId}`, (signal) => this.retireGate(signal))
  }

  /**
   * Leaves every member that joined, one bounded step per member. A broker refusal is ignored: the
   * member may already be fenced.
   * @param {AbortSignal} signal the cleanup's own
   * @returns {Promise<void>}
   * @throws {Error} the reason the signal aborted with
   */
  async leave(signal) {
    for (const member of Object.values(this.members)) {
      if (member.epoch <= 0) continue
      await bounded(
        signal,
        Infinity,
        () =>
          this.coordinator
            .consumerGroupHeartbeat({
              groupId: this.groupId,
              memberId: member.memberId,
              memberEpoch: -1,
            })
            .catch(() => {}),
        { label: 'The member leave' }
      )
    }
  }

  /**
   * Stops listening and closes the cluster, if one was ever created.
   * @returns {Promise<void>}
   */
  async disconnect() {
    this.removeListener?.()
    await this.cluster?.disconnect()
  }

  /**
   * Releases and retires the gate when it was ever armed. A failed release does not stop the
   * retire.
   * @param {AbortSignal} signal the cleanup's own
   * @returns {Promise<void>}
   * @throws {Error} a failed retire
   */
  async retireGate(signal) {
    if (!this.armed) return
    await this.gate('release', { signal }).catch(() => {})
    await this.gate('retire', { signal })
  }
}

/**
 * @param {string} version the broker version
 * @returns {object} the fixture's entry for it
 * @throws {Error} when the version-keyed fixture has no entry for the version
 */
const fixtureEntry = (version) => {
  const fixture = JSON.parse(fs.readFileSync(FIXTURE, 'utf8'))
  if (!fixture[version]) throw new Error(`No state fixture entry for Kafka ${version}`)
  return fixture[version]
}

/**
 * Runs one capture arm: registers every cleanup first, then creates the topic through the case's
 * own admin, takes the capture, runs its controls and writes evidence. The Reconciling arm runs
 * its immediate-acknowledgement control on a separate group of the same case.
 * @param {AbortSignal} scope the case's scope
 * @param {'reconciling'|'assigning'} kind
 * @returns {Promise<void>}
 */
const runCapture = async (scope, kind) => {
  const version = process.env.KAFKA_VERSION
  if (!version) throw new Error('KAFKA_VERSION names the broker the arm runs against')
  fixtureEntry(version)
  const id = secureRandom()
  const topic = `e2e-p9c-states-${kind}-${id}`
  const groupId = `e2e-p9c-states-${kind}-group-${id}`
  const kafka = new Kafka({
    clientId: `e2e-p9c-states-${kind}-${id}`,
    brokers: plainTextBrokers(),
    logLevel: logLevel.NOTHING,
  })
  const admin = kafka.admin()
  const capture = new StateCapture({ version, groupId, topic, nonce: gateNonce(id), scope })
  const control =
    kind === 'reconciling'
      ? new StateCapture({
          version,
          groupId: `${groupId}-control`,
          topic,
          nonce: gateNonce(id, 'c'),
          scope,
        })
      : null
  let topicStarted = false
  const cleanup = new CleanupStack()
  cleanup.add('admin', () => admin.disconnect())
  cleanup.add('topic deletion', (signal) =>
    topicStarted ? deleteOwnedTopic(signal, admin, topic) : undefined
  )
  capture.register(cleanup)
  control?.register(cleanup)
  await cleanup.run(
    async () => {
      await bounded(scope, Infinity, () => admin.connect(), { label: 'The admin connect' })
      topicStarted = true
      await createOwnedTopic(scope, admin, { topic, partitions: 2, replicas: 2 })
      await capture.open()
      const result =
        kind === 'reconciling'
          ? await capture.captureReconciling()
          : await capture.captureAssigning()
      let controls
      if (kind === 'reconciling') {
        await control.open()
        const immediate = await control.controlImmediate()
        controls = {
          immediateRejected: !windowProof(immediate),
          noTransferRejected:
            pendingTransfer(
              result.stable,
              capture.members.A.memberId,
              capture.members.B.memberId
            ) === null,
        }
      } else {
        controls = {
          gateReached: Boolean(result.arrived),
          gapClearedAfterRelease: !assigningGap(result.stable),
        }
      }
      scope.throwIfAborted()
      writeEvidence(EVIDENCE, {
        scenario: EVIDENCE,
        case: kind,
        observedBrokerVersion: version,
        groupId,
        nonce: capture.nonce,
        samples: result.samples.map(({ at, elapsedMs, group }) => ({
          at,
          elapsedMs,
          groupState: group.groupState,
          groupEpoch: group.groupEpoch,
          assignmentEpoch: group.assignmentEpoch,
          members: group.members.map(({ memberId, assignment, targetAssignment }) => ({
            memberId,
            current: assignment.topicPartitions,
            target: targetAssignment.topicPartitions,
          })),
        })),
        gateEvents: result.events ?? [],
        transcript: capture.transcript,
        controls,
        wireVersions: wireVersionsOf(
          capture.transcript.map(({ apiName, apiVersion }) => ({ apiName, apiVersion }))
        ),
      })
      expect(result.samples.length).toBeGreaterThanOrEqual(WINDOW_SAMPLES)
      expect(result.stable.groupState).toBe('Stable')
      expect(result.stable.groupEpoch).toBe(result.stable.assignmentEpoch)
      if (kind === 'reconciling') {
        expect(result.moving).not.toBeNull()
        expect(controls).toEqual({ immediateRejected: true, noTransferRejected: true })
        for (const { group } of result.samples) {
          expect(group.groupState).toBe('Reconciling')
          expect(group.groupEpoch).toBe(group.assignmentEpoch)
        }
      } else {
        expect(controls).toEqual({ gateReached: true, gapClearedAfterRelease: true })
        expect(result.arrived).toMatchObject({ status: 'arrived', groupId, nonce: capture.nonce })
        expect(result.arrived.groupEpoch).toBeGreaterThan(result.arrived.assignmentEpoch)
        for (const { group } of result.samples) {
          expect(group.groupState).toBe('Assigning')
          expect(group.groupEpoch).toBeGreaterThan(group.assignmentEpoch)
        }
        expect(result.events.map(({ status }) => status)).toEqual(
          expect.arrayContaining(['arrived', 'released'])
        )
        expect(result.events.map(({ status }) => status)).not.toContain('expired')
      }
    },
    { budgetMs: TEARDOWN_BUDGET_MS }
  )
}

describeStates('Consumer > consumer-protocol group states (live)', () => {
  test(
    'captures reconciliation while a live owner withholds revocation',
    liveCase(CAPTURE_BUDGET_MS, (scope) => runCapture(scope, 'reconciling')),
    caseTimeout(CAPTURE_BUDGET_MS)
  )

  test(
    'captures assigning in the committed epoch gap while Describe remains serviceable',
    liveCase(CAPTURE_BUDGET_MS, (scope) => runCapture(scope, 'assigning')),
    caseTimeout(CAPTURE_BUDGET_MS)
  )

  test(
    'delivers a named topic created after subscription within thirty seconds',
    liveCase(NAMED_BUDGET_MS, async (scope) => {
      const id = secureRandom()
      const topic = `e2e-p9c-states-named-${id}`
      const groupId = `e2e-p9c-states-named-group-${id}`
      const brokers = plainTextBrokers()
      const requests = []
      const handled = []
      const cleanup = new CleanupStack()

      await cleanup.run(
        async () => {
          const kafka = new Kafka({
            clientId: `e2e-p9c-states-${id}`,
            brokers,
            logLevel: logLevel.NOTHING,
          })
          const consumer = kafka.consumer({
            groupId,
            groupProtocol: 'consumer',
            allowAutoTopicCreation: false,
            metadataMaxAge: 300000,
            maxWaitTimeInMs: 500,
          })
          const producer = kafka.producer({ idempotent: true })
          const admin = kafka.admin()
          let topicStarted = false
          let joins = 0
          const removers = []
          cleanup.add('admin', () => admin.disconnect())
          cleanup.add('topic deletion', (signal) =>
            topicStarted ? deleteOwnedTopic(signal, admin, topic) : undefined
          )
          cleanup.add('producer', () => producer.disconnect())
          cleanup.add('consumer', () => consumer.disconnect())
          cleanup.add('listeners', () => removers.forEach((remove) => remove()))
          removers.push(
            consumer.on(consumer.events.GROUP_JOIN, () => (joins += 1)),
            consumer.on(consumer.events.REQUEST, ({ payload }) => requests.push(payload))
          )

          await bounded(scope, Infinity, () => admin.connect(), { label: 'The admin connect' })
          await bounded(scope, Infinity, () => consumer.connect())
          await bounded(scope, Infinity, () => consumer.subscribe({ topic, fromBeginning: true }))
          await bounded(scope, Infinity, () =>
            consumer.run({
              autoCommit: false,
              eachMessage: async ({ partition, message }) => {
                handled.push(`${partition}:${message.offset}`)
              },
            })
          )
          await pollUntil(scope, EMPTY_SUBSET_BUDGET_MS, () => joins > 0, {
            delayMs: POLL_MS,
            label: 'The stable empty subset',
          })

          topicStarted = true
          const createdAt = await bounded(
            scope,
            CREATE_BUDGET_MS,
            (inner) => createOwnedTopic(inner, admin, { topic, partitions: 1, replicas: 1 }),
            { label: 'The topic creation' }
          )
          await bounded(
            scope,
            createdAt + DELIVERY_BUDGET_MS - Date.now(),
            async (inner) => {
              await producer.connect()
              inner.throwIfAborted()
              await producer.send({
                topic,
                acks: -1,
                messages: [0, 1, 2].map((index) => ({ partition: 0, value: `named-${index}` })),
              })
              await pollUntil(inner, Infinity, () => handled.length >= 3, {
                delayMs: POLL_MS,
                label: 'The named topic delivery',
              })
            },
            { label: 'The named topic delivery' }
          )
          const deliveredAfterMs = Date.now() - createdAt
          await delay(scope, TURN_MS)
          const [{ partitions }] = await bounded(scope, Infinity, () =>
            admin.fetchOffsets({ groupId, topics: [topic] })
          )
          const committed = partitions.map(({ partition, offset }) => [partition, String(offset)])
          const commits = requests.filter(({ apiName }) => apiName === 'OffsetCommit')

          writeEvidence(EVIDENCE, {
            scenario: EVIDENCE,
            case: 'named-topic',
            groupId,
            config: { metadataMaxAge: 300000, autoCommit: false, allowAutoTopicCreation: false },
            replay: {
              log: 'three acknowledged records at partition 0 offsets 0 to 2',
              offsets: 'broker commits none',
              handlers: 'exactly the three ids, once each',
            },
            wireVersions: wireVersionsOf(requests),
            handled,
            deliveredAfterMs,
            committed,
          })

          expect([...handled].sort()).toEqual(['0:0', '0:1', '0:2'])
          expect(deliveredAfterMs).toBeLessThanOrEqual(DELIVERY_BUDGET_MS)
          expect(commits).toEqual([])
          expect(committed).toEqual([[0, '-1']])
        },
        { budgetMs: TEARDOWN_BUDGET_MS }
      )
    }),
    caseTimeout(NAMED_BUDGET_MS)
  )
})
