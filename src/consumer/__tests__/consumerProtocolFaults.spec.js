const Protocol = require('../groupProtocol/consumer')
const GroupOwnership = require('../groupOwnership')
const Emitter = require('../../instrumentation/emitter')
const { KafkaJSProtocolError, KafkaJSServerDoesNotSupportApiKey } = require('../../errors')
const { createErrorFromCode } = require('../../protocol/error')
const ID = '01020304-0506-0708-090a-0b0c0d0e0f10'
const STEPS = Object.freeze([
  ['before join', 'JOINING'],
  ['target unapplied', 'RECONCILING'],
  ['during apply', 'RECONCILING'],
  ['during acknowledgement', 'ACKNOWLEDGING'],
])
const FAULTS = Object.freeze([
  [16, 'retry-now'],
  [15, 'retry-now'],
  [14, 'backoff'],
  [25, 'FENCED'],
  [110, 'FENCED'],
  [35, 'FATAL'],
  ['missing', 'FATAL'],
  [128, 'FATAL'],
  [81, 'FATAL'],
  [29, 'stay'],
  ['disconnect', 'backoff'],
  ['timeout', 'backoff'],
  [112, 'FATAL'],
  [111, 'FATAL'],
])
const CELLS = STEPS.flatMap(([step, state]) =>
  FAULTS.map(([fault, outcome]) => [step, state, fault, outcome])
)
/** @param {number|string} fault broker error or transport event @returns {Error} independent scripted error */
const failure = (fault) => {
  if (fault === 'missing')
    return new KafkaJSServerDoesNotSupportApiKey('missing API', {
      apiKey: 68,
      apiName: 'ConsumerGroupHeartbeat',
    })
  if (typeof fault === 'number') return new KafkaJSProtocolError(createErrorFromCode(fault))
  return Object.assign(new Error(fault), {
    name: fault === 'timeout' ? 'KafkaJSRequestTimeoutError' : 'KafkaJSConnectionError',
  })
}
/** @param {string} state phase held by the test @param {Error} error response error
 * @returns {object} real membership, ownership guard and one scripted broker */
const build = (state, error) => {
  const ownership = new GroupOwnership()
  const log = {
    namespace() {
      return this
    },
    debug: jest.fn(),
    error: jest.fn(),
    warn: jest.fn(),
    info: jest.fn(),
  }
  const broker = {
    consumerGroupHeartbeat: jest.fn(async () => {
      throw error
    }),
  }
  const cluster = {
    findGroupCoordinator: jest.fn(async () => broker),
    topicName: () => 'named',
    refreshMetadataForTopicIds: jest.fn(async () => ({ resolved: [ID], unresolved: [] })),
  }
  const p = new Protocol({
    cluster,
    ownership,
    groupId: 'faults',
    rebalanceTimeout: 30000,
    instrumentationEmitter: new Emitter(),
    logger: log,
    retry: { initialRetryTime: 300, factor: 0 },
  })
  p.state = state
  p.memberEpoch = state === 'JOINING' ? 0 : 7
  p.coordinator = broker
  p.replaceTarget({ topicPartitions: [{ topicId: ID, partitions: [0] }] })
  p.ownedPartitions = state === 'ACKNOWLEDGING' ? [{ topic: 'named', partitions: [0] }] : []
  const manager = { requestTick: jest.fn() }
  p.attach({ heartbeatManager: manager, rebalanceCoordinator: { requestRejoin: jest.fn() } })
  return { p, ownership, broker, cluster, manager, log }
}

describe('Consumer > heartbeat fault matrix', () => {
  beforeEach(() => jest.useFakeTimers())
  afterEach(() => {
    jest.restoreAllMocks()
    jest.useRealTimers()
  })
  test.each(CELLS)(
    '%s in %s with fault %s has outcome %s',
    async (_step, state, fault, outcome) => {
      const h = build(state, failure(fault))
      const memberId = h.p.memberId
      const epoch = h.p.memberEpoch
      const token = h.ownership.token()
      await expect(
        h.p.sendHeartbeat({ sendAllFields: state === 'JOINING' })
      ).rejects.toBeInstanceOf(Error)
      expect(h.broker.consumerGroupHeartbeat).toHaveBeenCalledTimes(1)
      expect(h.p.memberId).toBe(memberId)
      expect(h.p.memberEpoch).toBe(epoch)
      const expected = ['FENCED', 'FATAL'].includes(outcome)
        ? outcome
        : state === 'ACKNOWLEDGING'
          ? 'STABLE'
          : state
      expect(h.p.state).toBe(expected)
      if (outcome === 'FENCED' || outcome === 'FATAL') {
        expect(h.ownership.refusal(token)).not.toBeNull()
        expect(h.p.target).toBeNull()
      } else expect(h.ownership.refusal(token)).toBeNull()
      if (outcome === 'retry-now') {
        expect(h.p.coordinator).toBeNull()
        expect(h.manager.requestTick).toHaveBeenCalledTimes(1)
        expect(h.p.backoff.isDue(Date.now())).toBe(true)
      }
      if (outcome === 'backoff') expect(h.p.backoff.remaining(Date.now())).toBe(300)
      if (outcome === 'stay') expect(h.log.error).toHaveBeenCalledTimes(1)
      if (outcome === 'FATAL') {
        await expect(h.p.rebalance({ beforeSync() {} })).rejects.toBe(h.p.fatalError)
        expect(h.broker.consumerGroupHeartbeat).toHaveBeenCalledTimes(1)
      } else {
        h.broker.consumerGroupHeartbeat.mockResolvedValue({
          memberEpoch: 7,
          heartbeatIntervalMs: 500,
          assignment: null,
        })
        if (outcome === 'FENCED') {
          h.ownership.renew()
          h.p.state = 'JOINING'
          h.p.memberEpoch = 0
        }
        await h.p.sendHeartbeat({ sendAllFields: h.p.state === 'JOINING' })
        expect(h.p.memberEpoch).toBe(7)
        expect(h.broker.consumerGroupHeartbeat).toHaveBeenCalledTimes(2)
      }
      expect(jest.getTimerCount()).toBe(0)
    }
  )
  it('logs an authorization message once until the distinct message changes', async () => {
    const h = build('RECONCILING', failure(29))
    for (let i = 0; i < 3; i++) h.p.classifyHeartbeatError(failure(29))
    expect(h.log.error).toHaveBeenCalledTimes(1)
    const changed = failure(29)
    changed.message = 'another topic'
    h.p.classifyHeartbeatError(changed)
    expect(h.log.error).toHaveBeenCalledTimes(2)
  })
})

describe('Consumer > heartbeat answered after the member began joining anew', () => {
  /** Holds a JOINING heartbeat, fences the member, starts a new join, then returns the join state. */
  const heldAcrossRejoin = async () => {
    const h = build('JOINING', failure(25))
    let answer
    let reject
    h.broker.consumerGroupHeartbeat.mockImplementationOnce(
      () =>
        new Promise((resolve, rejectCall) => {
          answer = resolve
          reject = rejectCall
        })
    )
    const heartbeat = h.p.sendHeartbeat({ sendAllFields: true })
    h.p.onMembershipError(failure(25))
    const joining = h.p.rebalance({ beforeSync() {} })
    expect(h.p.state).toBe('JOINING')
    return { h, heartbeat, joining, answer, reject }
  }

  test('a join response cannot settle a later join after UNKNOWN_MEMBER_ID', async () => {
    const { h, heartbeat, joining, answer } = await heldAcrossRejoin()
    answer({ memberEpoch: 7, heartbeatIntervalMs: 100, assignment: null })
    await heartbeat
    const epoch = h.p.memberEpoch
    h.p.transitionToFenced(failure(25))
    await joining.catch(() => {})
    expect(epoch).toBe(0)
  })

  test('a join error cannot fence a later join after UNKNOWN_MEMBER_ID', async () => {
    const { h, heartbeat, joining, reject } = await heldAcrossRejoin()
    reject(failure(25))
    await heartbeat.catch(() => {})
    const state = h.p.state
    h.p.transitionToFenced(failure(25))
    await joining.catch(() => {})
    expect(state).toBe('JOINING')
  })

  test('progress inside one membership keeps its answer', async () => {
    const h = build('RECONCILING', failure(25))
    const revision = h.p.membershipRevision
    h.broker.consumerGroupHeartbeat.mockResolvedValue({
      memberEpoch: 8,
      heartbeatIntervalMs: 100,
      assignment: null,
    })
    await h.p.sendHeartbeat({})
    expect(h.p.membershipRevision).toBe(revision)
    expect(h.p.memberEpoch).toBe(8)
  })
})
const ConsumerGroup = require('../consumerGroup')
const RebalanceCoordinator = require('../rebalanceCoordinator')
const OffsetManager = require('../offsetManager')
const { stampGeneration, stampOwnership, stampOffsetCommit } = require('../rebalanceErrorContract')

/** @param {'consumer'|'classic'} kind protocol
 * @returns {object} real ownership wiring, coordinator and captured commit sender without sockets */
const commitEpisode = (kind) => {
  const log = {
    namespace() {
      return this
    },
    debug() {},
    info() {},
    warn() {},
    error() {},
  }
  const group = new ConsumerGroup({
    cluster: {},
    groupId: 'epoch-loss',
    groupProtocol: kind,
    topics: [],
    topicConfigurations: {},
    assigners: [],
    logger: log,
    instrumentationEmitter: new Emitter(),
    rebalanceTimeout: 30000,
    retry: {},
    sessionTimeout: 45000,
    heartbeatInterval: 3000,
  })
  if (kind === 'consumer') {
    group.groupMembership.state = 'STABLE'
    group.groupMembership.memberEpoch = 7
  } else group.groupMembership.generationId = 7
  const fetchManager = { stop: jest.fn(async () => {}) }
  const coordinator = new RebalanceCoordinator({
    consumerGroup: group,
    runner: { stopRequested: false },
    fetchManager,
    heartbeatManager: {},
    pollTimer: {},
    instrumentationEmitter: new Emitter(),
    logger: log,
    groupId: 'epoch-loss',
  })
  group.rebalanceCoordinator = coordinator
  let reject
  const held = new Promise((_yes, no) => {
    reject = no
  })
  const broker = { offsetCommit: jest.fn(() => held) }
  const sender = {
    ownership: group.ownership,
    membershipProtocol: kind,
    membership: () => ({ revision: 0 }),
    getCoordinator: async () => broker,
    withTopicIds: (topics) => topics,
    onCommitError: (error) =>
      coordinator.requestRejoin({ kind: 'lost', reason: 'membership-lost', error }),
  }
  return { group, coordinator, sender, broker, reject, fetchManager }
}

describe('Consumer > membership loss across epoch changes', () => {
  test.each([69, 30, 42, 111])(
    'retains a current consumer-token commit loss %i after an epoch bump',
    async (code) => {
      const h = commitEpisode('consumer')
      const token = h.group.ownership.token()
      const commit = OffsetManager.prototype.sendOffsetCommit.call(h.sender, {
        groupId: 'epoch-loss',
        groupGenerationId: 7,
        memberId: 'm',
        topics: [],
      })
      const refused = commit.catch((error) => error)
      await Promise.resolve()
      await Promise.resolve()
      expect(h.broker.offsetCommit).toHaveBeenCalledTimes(1)
      h.group.groupMembership.memberEpoch = 8
      h.reject(new KafkaJSProtocolError(createErrorFromCode(code)))
      await expect(refused).resolves.toMatchObject({ name: 'KafkaJSCommitFailedError' })
      expect(h.group.ownership.refusal(token)).not.toBeNull()
      const verdict = h.coordinator.takeVerdict()
      expect(verdict).toMatchObject({ kind: 'lost', error: { code } })
      expect(h.coordinator.isStaleGenerationVerdict(verdict)).toBe(false)
      expect(h.fetchManager.stop).toHaveBeenCalledTimes(1)
    }
  )
  test.each([69, 30, 42, 111])(
    'drops a replaced-token consumer loss %i without poisoning the new owner',
    async (code) => {
      const h = commitEpisode('consumer')
      const old = h.group.ownership.token()
      h.group.ownership.renew()
      const error = stampOwnership(
        stampGeneration(stampOffsetCommit(new KafkaJSProtocolError(createErrorFromCode(code))), 7),
        old
      )
      h.coordinator.requestRejoin({ kind: 'lost', reason: 'membership-lost', error })
      expect(h.coordinator.takeVerdict()).toBeNull()
      expect(h.group.ownership.refusal(h.group.ownership.token())).toBeNull()
      expect(h.fetchManager.stop).not.toHaveBeenCalled()
    }
  )
  test.each([25, 69])(
    'keeps stale classic generation loss %i consistent at both admission sites',
    (code) => {
      const h = commitEpisode('classic')
      const error = stampGeneration(
        stampOffsetCommit(new KafkaJSProtocolError(createErrorFromCode(code))),
        6
      )
      const verdict = { kind: 'lost', reason: 'membership-lost', error }
      expect(h.group.ownership.isStale(error)).toBe(true)
      expect(h.coordinator.isStaleGenerationVerdict(verdict)).toBe(true)
      h.coordinator.requestRejoin(verdict)
      expect(h.group.ownership.refusal(h.group.ownership.token())).toBeNull()
      expect(h.coordinator.takeVerdict()).toBeNull()
    }
  )
})

const RebalanceAssertions = require('./fixtures/rebalanceAssertions')
describe('Consumer > bounce assertion controls', () => {
  test.each(['classic', 'consumer'])(
    'fails a short rebalance for %s and a second long membership change',
    (group) => {
      const short = { arm: 'short', kind: 'short', violation: true }
      const long = { arm: 'long', kind: 'long', violation: true }
      const valid = { arm: 'healthy', kind: 'short', violation: false }
      expect(RebalanceAssertions.shortArmStop({ [group]: short })).toEqual([group, short])
      expect(RebalanceAssertions.shortArmStop({ [group]: long })).toBeUndefined()
      expect(RebalanceAssertions.shortArmStop({ [group]: valid })).toBeUndefined()
      const verdict = { classic: { rebalances: [] }, consumer: { rebalances: [] } }
      verdict[group].rebalances = [valid, short, long]
      expect(RebalanceAssertions.violations(verdict)).toEqual([short, long])
    }
  )
  it('starts attribution at command issue rather than the stop command return', () => {
    const window = RebalanceAssertions.issueWindow(
      { tDown: 7000, tRestored: 8000, outageMs: 1000 },
      1000
    )
    expect(window).toMatchObject({
      tDown: 1000,
      commandReturnedAt: 7000,
      tRestored: 8000,
      outageMs: 7000,
    })
    expect(window.tDown <= 3000 && window.tRestored >= 3000).toBe(true)
    expect(() => RebalanceAssertions.issueWindow({ tDown: 7000, tRestored: 8000 }, 9000)).toThrow(
      'Invalid outage timestamps'
    )
  })
})

const { Kafka } = require('../../../index')
const Runner = require('../runner')
const sleep = require('../../utils/sleep')
const { socketCounts } = require('../../../testHelpers/fakeSocket')
const {
  createScriptedCluster,
  consumerProtocolScript,
} = require('../../../testHelpers/scriptedCluster')
const { TOPIC, TOPIC_ID, prepareRecordBatches } = require('./fixtures/fetchHarness')
const { RecordBatch } = require('../../protocol/recordBatch/v0')
const RecordEncoder = require('../../protocol/recordBatch/record/v0')

const HEARTBEAT = 'ConsumerGroupHeartbeat'
const NIL_TOPIC_ID = '00000000-0000-0000-0000-000000000000'
const REQUEST_TIMEOUT_MS = 1500
const WAIT_MS = 10000
const RETRY = { initialRetryTime: 300, maxRetryTime: 1000, multiplier: 2, factor: 0, retries: 5 }
const EPISODE_EVENTS = [
  'GROUP_JOIN',
  'PARTITIONS_ASSIGNED',
  'PARTITIONS_REVOKED',
  'PARTITIONS_LOST',
  'CRASH',
]

/**
 * Polls until `predicate` holds.
 * @param {() => boolean} predicate
 * @param {string} label what the wait is for, named in the failure
 * @param {number} [ms] the wait budget
 * @returns {Promise<void>}
 * @throws {Error} when the condition does not hold within the wait budget
 */
const until = async (predicate, label, ms = WAIT_MS) => {
  const deadline = Date.now() + ms
  while (!predicate()) {
    if (Date.now() > deadline) throw new Error(`Episode wait expired: ${label}`)
    await sleep(5)
  }
}

/**
 * One episode of the seam matrix: two real consumers on the consumer protocol over a scripted
 * cluster whose coordinator answers from a fixed script. Member A takes the fault at a held
 * boundary; member B is never faulted and proves ownership stays exclusive. The coordinator and the
 * Fetch answers live on different nodes, so a held or failed call on one never kills the other.
 * Nothing here is a production export or a general simulator.
 */
class FaultEpisode {
  /**
   * Builds and starts an episode, holding A at the named boundary before B joins.
   * @param {object} cell
   * @param {string} cell.step a boundary name from STEPS, or `steady` to park nothing
   * @param {number|string|null} [cell.fault] broker error code or transport fault name
   * @param {boolean} [cell.holdHandler] park A's first handler call until `releaseHandler`
   * @param {(request: object, member: 'A'|'B') => object|undefined} [cell.answer] replaces the
   *   coordinator's answer to a heartbeat when it returns one
   * @param {number} [cell.waitMs] how long each recovery wait may take
   * @param {boolean} [cell.eager] releases the held apply at once, without waiting for the member
   *   to find its coordinator again
   * @param {boolean} [cell.hideIds] answers Metadata by name without the topic id, so the first
   *   assignment names a topic id the client has never seen
   * @returns {Promise<FaultEpisode>} the episode, with A parked at the boundary
   * @throws {Error} when the boundary is never reached
   */
  static async open({ step, fault = null, ...options }) {
    const episode = new FaultEpisode(step, fault, options)
    try {
      await episode.start()
    } catch (error) {
      await episode.close().catch(() => {})
      throw error
    }
    return episode
  }

  /**
   * @param {string} step boundary name from STEPS, or `steady`
   * @param {number|string|null} fault broker error code or transport fault name
   * @param {{ holdHandler?: boolean, answer?: Function, hideIds?: boolean, waitMs?: number,
   *   eager?: boolean }} [options] see `open`
   */
  constructor(
    step,
    fault,
    { holdHandler = false, answer = null, hideIds = false, waitMs = WAIT_MS, eager = false } = {}
  ) {
    this.waitMs = waitMs
    this.eager = eager
    this.step = step
    this.fault = fault
    this.holdHandler = holdHandler
    this.hideIds = hideIds
    this.entriesAtLoss = null
    this.losses = []
    this.script = answer
    this.consumers = {}
    this.running = []
    this.runners = []
    this.spies = []
    this.gates = []
    this.late = []
    this.unhandledErrors = []
    this.timeline = []
    this.states = []
    this.entries = []
    this.applies = 0
    this.ownedBy = { A: new Set(), B: new Set() }
    this.crashes = []
    this.joins = 0
    this.classified = 0
    this.aMemberId = null
    this.applyGate = null
    this.applyHeld = false
    this.gate = null
    this.handlerRelease = null
    this.epochs = new Map()
    this.widened = step !== 'during apply'
    // During apply A is assigned a partition it has not seen, so its apply refreshes metadata.
    this.partitionOf = step === 'during apply' ? { A: 1, B: 0 } : { A: 0, B: 1 }
    this.unhandledListener = (error) => this.unhandledErrors.push(error)
    this.cluster = consumerProtocolScript(
      createScriptedCluster({
        nodes: [
          { id: 1, host: 'leader', port: 9092 },
          { id: 0, host: 'coordinator', port: 9093 },
        ],
        profile: 'consumer',
      }),
      { partitions: 2, batches: 1, coordinator: 0 }
    )
    this.schedule = this.cluster.schedule
    this.cluster.answer(HEARTBEAT, (request) => this.coordinatorAnswer(request))
    this.cluster.answer('Fetch', (request) => this.fetchAnswer(request))
    const metadata = this.cluster.answerers.get('Metadata')
    this.cluster.answer('Metadata', (fields, record) => {
      const answer = metadata(fields, record)
      if (this.hideIds && fields.topics?.every(({ name }) => name)) {
        return {
          ...answer,
          topics: answer.topics.map((topic) => ({ ...topic, topicId: NIL_TOPIC_ID })),
        }
      }
      if (this.widened) return answer
      return {
        ...answer,
        topics: answer.topics.map((topic) => ({
          ...topic,
          partitions: topic.partitions.slice(0, 1),
        })),
      }
    })
  }

  /**
   * @private
   * @param {string} memberId
   * @returns {'A'|'B'} the member, A being the first to heartbeat
   */
  nameOf(memberId) {
    const order = [
      ...new Set(
        this.cluster
          .requests()
          .filter(({ api }) => api === HEARTBEAT)
          .map(({ fields }) => fields.memberId)
      ),
    ]
    return order.indexOf(memberId) === 0 ? 'A' : 'B'
  }

  /**
   * The scripted coordinator: members take their partition in order of first appearance, a join is
   * answered with the next epoch and that assignment, an update echoes its epoch with no new
   * assignment, and a leave echoes its epoch.
   * @param {{ memberId: string, memberEpoch: number }} request the decoded heartbeat
   * @returns {object} response fields
   */
  coordinatorAnswer(request) {
    const { memberId, memberEpoch } = request
    const name = this.nameOf(memberId)
    const scripted = this.script?.(request, name)
    if (scripted) return scripted
    if (memberEpoch < 0) return { memberId, memberEpoch }
    if (memberEpoch > 0)
      return { memberId, memberEpoch, heartbeatIntervalMs: 100, assignment: null }
    const epoch = (this.epochs.get(memberId) || 0) + 1
    this.epochs.set(memberId, epoch)
    return {
      memberId,
      memberEpoch: epoch,
      heartbeatIntervalMs: 100,
      assignment: {
        topicPartitions: [{ topicId: TOPIC_ID, partitions: [this.partitionOf[name]] }],
      },
    }
  }

  /**
   * Answers a Fetch with records 0..2 once per partition, then holds for the wait asked for.
   * @param {{ maxWaitMs: number, topics: object[] }} request the decoded Fetch
   * @returns {Promise<object>} response fields
   */
  async fetchAnswer({ maxWaitMs, topics }) {
    const [{ partition, fetchOffset }] = topics[0].partitions
    if (fetchOffset !== '0') {
      await sleep(maxWaitMs)
      return { partitions: [{ partition, errorCode: 0, records: false }] }
    }
    const batch = await RecordBatch({
      firstOffset: fetchOffset,
      firstTimestamp: 1700000000000,
      maxTimestamp: 1700000000000,
      records: [0, 1, 2].map((offsetDelta) =>
        RecordEncoder({ offsetDelta, key: Buffer.from('key'), value: Buffer.from('value') })
      ),
    })
    return { partitions: [{ partition, errorCode: 0, records: Buffer.from(batch.buffer) }] }
  }

  /**
   * @private
   * @param {{ fields: { memberId: string } }} call a scheduled heartbeat
   * @returns {boolean} whether it is member A's: the first member ever to heartbeat
   */
  isA(call) {
    this.aMemberId ??= this.cluster.requests().find(({ api }) => api === HEARTBEAT).fields.memberId
    return call.fields.memberId === this.aMemberId
  }

  /** @private @returns {Promise<void>} builds the cluster and starts A, which parks at the boundary */
  async start() {
    await prepareRecordBatches()
    process.on('unhandledRejection', this.unhandledListener)
    this.baseline = { listeners: process.listenerCount('unhandledRejection') - 1 }
    const capture = Runner.prototype.start
    const episode = this
    this.spies.push(
      jest.spyOn(Runner.prototype, 'start').mockImplementation(function (...args) {
        episode.runners.push(this)
        if (episode.runners.length === 1) episode.watchA(this)
        return capture.apply(this, args)
      })
    )
    this.armBoundary()
    this.kafka = new Kafka(
      this.cluster.kafkaOptions({
        brokers: ['leader:9092'],
        retry: RETRY,
        requestTimeout: REQUEST_TIMEOUT_MS,
      })
    )
    await this.startMember('A')
  }

  /**
   * Waits until A is parked at the declared boundary, then starts B, which is never faulted.
   * @param {string} [step] the boundary, which must be the episode's own
   * @returns {Promise<void>}
   * @throws {Error} when the boundary is never reached
   */
  async holdBoundary(step = this.step) {
    if (step !== this.step) throw new Error(`The episode holds ${this.step}, not ${step}`)
    await this.reachBoundary()
    await this.startMember('B')
  }

  /**
   * Hooks member A's real protocol: state transitions, classification, ownership loss and the
   * apply entry.
   * @private
   * @param {Runner} runner A's runner
   * @returns {void}
   */
  watchA(runner) {
    const group = runner.consumerGroup
    const protocol = group.groupMembership
    this.protocol = protocol
    this.group = group
    const transitionTo = protocol.transitionTo.bind(protocol)
    this.spies.push(
      jest.spyOn(protocol, 'transitionTo').mockImplementation((state) => {
        transitionTo(state)
        this.states.push(protocol.state)
        if (protocol.state === 'FATAL') this.cluster.mark('A-terminal')
      })
    )
    const classify = protocol.classifyHeartbeatError.bind(protocol)
    this.spies.push(
      jest.spyOn(protocol, 'classifyHeartbeatError').mockImplementation((error) => {
        const result = classify(error)
        if (this.holdsApply() && ['FENCED', 'FATAL'].includes(protocol.state)) this.applyGate?.()
        this.classified += 1
        return result
      })
    )
    const lose = group.ownership.lose.bind(group.ownership)
    this.spies.push(
      jest.spyOn(group.ownership, 'lose').mockImplementation((cause, token) => {
        const { current, states } = group.ownership
        if (token === current && !states.get(token).cause) {
          this.cluster.mark('A-learned')
          this.losses.push({ order: this.cluster.marks.at(-1).order, epoch: protocol.memberEpoch })
        }
        return lose(cause, token)
      })
    )
    const { cluster } = group
    const refresh = cluster.refreshMetadata.bind(cluster)
    let inApply = false
    this.spies.push(
      jest.spyOn(cluster, 'refreshMetadata').mockImplementation(async (...args) => {
        if (inApply && this.step === 'during apply' && !this.applyHeld) {
          this.applyHeld = true
          await new Promise((resolve) => {
            this.applyGate = resolve
          })
        }
        return refresh(...args)
      })
    )
    if (this.step === 'during acknowledgement') {
      // The heartbeat timer alone sends the held acknowledgement, so the fault has one sender.
      this.spies.push(jest.spyOn(runner, 'heartbeat').mockImplementation(async () => {}))
    }
    const apply = group.applyRebalanceResult.bind(group)
    this.spies.push(
      jest.spyOn(group, 'applyRebalanceResult').mockImplementation(async (...args) => {
        this.applies += 1
        if (this.step === 'target unapplied' && !this.applyHeld) {
          this.applyHeld = true
          await new Promise((resolve) => {
            this.applyGate = resolve
          })
        }
        inApply = true
        try {
          return await apply(...args)
        } finally {
          inApply = false
        }
      })
    )
  }

  /** @private @returns {void} installs the schedule rule that parks A at a wire boundary */
  armBoundary() {
    const heartbeat = (extra) => ({
      api: HEARTBEAT,
      match: (call) => this.isA(call) && extra(call),
    })
    if (this.step === 'before join') {
      this.gate = this.schedule.hold(heartbeat(({ fields }) => fields.memberEpoch === 0))
    } else if (this.step === 'during acknowledgement') {
      this.gate = this.schedule.hold(
        heartbeat(({ fields }) => fields.memberEpoch > 0 && fields.topicPartitions?.length > 0)
      )
    }
    if (this.gate) this.gates.push(this.gate)
  }

  /** @private @returns {Promise<void>} waits until A is parked at the declared boundary */
  async reachBoundary() {
    if (this.step === 'steady') {
      await until(() => this.states.at(-1) === 'STABLE', 'member A to be stable')
      if (this.holdHandler) await until(() => this.entries.length === 1, 'the first handler call')
      return
    }
    await until(() => this.applyHeld === true || this.gate, 'the boundary')
    if (this.gate) await this.gate.arrived()
  }

  /**
   * @private
   * @param {'A'|'B'} name member name
   * @returns {Promise<void>} connects and starts the member without awaiting its first join
   */
  async startMember(name) {
    const consumer = this.kafka.consumer({
      groupId: 'fault-episode',
      groupProtocol: 'consumer',
      maxWaitTimeInMs: 20,
      retry: RETRY,
    })
    this.consumers[name] = consumer
    for (const key of EPISODE_EVENTS) {
      consumer.on(consumer.events[key], ({ payload }) => this.record(name, key, payload))
    }
    await consumer.connect()
    await consumer.subscribe({ topic: TOPIC, fromBeginning: true })
    this.running.push(
      consumer
        .run({
          autoCommit: true,
          autoCommitThreshold: 1,
          eachMessage: async ({ message }) => {
            if (name !== 'A') return
            this.entries.push(message.offset)
            if (this.holdHandler && this.entries.length === 1) {
              await new Promise((resolve) => {
                this.handlerRelease = resolve
              })
            }
          },
        })
        .catch((error) => this.unhandledErrors.push(error))
    )
    if (name === 'B') await until(() => this.ownedBy.B.size > 0, 'member B to own its partition')
  }

  /**
   * Records one member event, keeping ownership sets and the ordered timeline.
   * @private
   * @param {'A'|'B'} name member
   * @param {string} key event name
   * @param {object} payload event payload
   * @returns {void}
   */
  record(name, key, payload) {
    const owned = this.ownedBy[name]
    const partitions = (payload.partitions ?? []).flatMap((entry) => entry.partitions)
    this.timeline.push({ name, key, partitions, at: Date.now() })
    if (key === 'GROUP_JOIN' && name === 'A') this.joins += 1
    if (key === 'CRASH' && name === 'A') this.crashes.push(payload)
    if (key === 'PARTITIONS_LOST' && name === 'A') this.entriesAtLoss ??= this.entries.length
    if (key === 'PARTITIONS_ASSIGNED') partitions.forEach((partition) => owned.add(partition))
    if (key === 'PARTITIONS_REVOKED' || key === 'PARTITIONS_LOST') owned.clear()
  }

  /**
   * Delivers exactly one fault at the held boundary: a response for a broker code, a destroyed
   * socket for a disconnect, no answer until the client's own timeout for a timeout, and an
   * advertisement without the API for a missing one. Returns once the client classified it.
   * @param {number|string} [fault] the fault; defaults to the episode's
   * @returns {Promise<void>}
   * @throws {Error} when the held request never receives it or the client never classifies it
   */
  async answerFault(fault = this.fault) {
    this.widened = true
    if (fault === 'missing') this.cluster.advertised.delete(HEARTBEAT)
    const outcome = this.faultOutcome(fault)
    const before = this.classified
    if (this.gate && ['before join', 'during acknowledgement'].includes(this.step)) {
      if (outcome instanceof Error) this.gate.fail(outcome)
      else if (outcome === 'silent') {
        await sleep(REQUEST_TIMEOUT_MS + 150)
        this.gate.release({})
      } else this.gate.release(outcome)
    } else if (fault !== 'missing') {
      let delivered = false
      const mine = (call) => this.isA(call) && call.fields.memberEpoch > 0
      this.schedule.once({ api: HEARTBEAT, match: mine }, () => {
        delivered = true
        if (outcome === 'silent') return new Promise((resolve) => this.late.push(resolve))
        return outcome
      })
      await until(() => delivered, 'the fault to reach a heartbeat', this.waitMs)
    }
    const expectedState = FAULTS.find(([name]) => name === fault)?.[1]
    await until(
      () =>
        this.classified > before &&
        (!['FENCED', 'FATAL'].includes(expectedState) || this.states.includes(expectedState)),
      'the client to classify the injected fault',
      this.waitMs
    )
  }

  /**
   * @private
   * @param {number|string} fault
   * @returns {Error|'silent'|object} what the coordinator does instead of its normal answer
   */
  faultOutcome(fault) {
    if (fault === 'timeout') return 'silent'
    if (fault === 'disconnect' || fault === 'missing') {
      return Object.assign(new Error(`scripted ${fault}`), { code: 'ECONNRESET' })
    }
    return { errorCode: fault, errorMessage: `scripted ${fault}`, memberEpoch: 0, memberId: null }
  }

  /**
   * Waits for a condition on the episode, within its recovery budget.
   * @param {() => boolean} predicate
   * @param {string} label
   * @returns {Promise<void>}
   * @throws {Error} when the condition does not hold in time
   */
  settled(predicate, label) {
    return until(predicate, label, this.waitMs)
  }

  /** @private @returns {boolean} whether the step parks A's apply, in front of or inside it */
  holdsApply() {
    return this.step === 'target unapplied' || this.step === 'during apply'
  }

  /**
   * Lets the parked handler of A return.
   * @returns {void}
   */
  releaseHandler() {
    this.handlerRelease?.()
  }

  /**
   * Lets every held call go and waits for the member to settle: terminal faults end in one CRASH,
   * the others in STABLE with the complete exclusive partition map.
   * @param {boolean} [terminal] whether the cell ends the membership
   * @returns {Promise<void>}
   * @throws {Error} when the member does not settle within the wait budget
   */
  async releaseAndRecover(terminal = false) {
    if (!this.eager && !terminal && !['FENCED', 'FATAL'].includes(this.states.at(-1))) {
      await until(
        () => this.protocol.coordinator != null,
        'the coordinator to be found again',
        this.waitMs
      )
    }
    this.widened = true
    try {
      this.applyGate?.()
      for (const gate of this.gates) if (gate.settle) gate.release()
    } finally {
      for (const release of this.late) release({})
    }
    if (terminal) {
      await until(() => this.crashes.length > 0, 'the terminal crash', this.waitMs)
      await sleep(400)
    } else {
      await until(
        () =>
          this.states.at(-1) === 'STABLE' &&
          this.ownedBy.A.has(this.partitionOf.A) &&
          this.ownedBy.B.has(this.partitionOf.B),
        'member A to recover to STABLE',
        this.waitMs
      )
    }
  }

  /**
   * @returns {{ states: string[], wire: object, owners: object, commits: object,
   *   unhandled: Error[], resources: object, rounds: number, crashes: number, entries: string[],
   *   entriesAtLoss: number|null, applies: number, lost: number }} what the episode observed so far
   */
  facts() {
    const [terminal] = this.cluster.marks
      .filter((mark) => mark.label === 'A-terminal')
      .map(({ order }) => order)
    const requests = this.cluster.requests()
    const ofA = ({ fields }) => fields?.memberId === this.aMemberId
    const commits = requests.filter(({ api, fields }) => api === 'OffsetCommit' && ofA({ fields }))
    const owners = new Map()
    const overlaps = []
    for (const { name, key, partitions } of this.timeline) {
      for (const partition of partitions) {
        const holder = owners.get(partition)
        if (key === 'PARTITIONS_ASSIGNED') {
          if (holder && holder !== name) overlaps.push({ partition, holder, name })
          owners.set(partition, name)
        } else if (holder === name) owners.delete(partition)
      }
    }
    return {
      states: [...this.states],
      wire: {
        afterTerminal:
          terminal === undefined
            ? []
            : requests.filter((r) => r.api === HEARTBEAT && ofA(r) && r.order > terminal),
      },
      owners: { overlaps, final: { A: [...this.ownedBy.A], B: [...this.ownedBy.B] } },
      commits: {
        // A commit under the lost membership's epoch; the renewed membership commits under its own.
        afterLearnedLoss: commits.filter((request) =>
          this.losses.some(
            ({ order, epoch }) => request.order > order && request.fields.groupGenerationId <= epoch
          )
        ),
        all: commits,
      },
      unhandled: [...this.unhandledErrors],
      resources: { sockets: socketCounts(this.cluster.fake.state) },
      rounds: this.joins,
      crashes: this.crashes.length,
      entries: [...this.entries],
      entriesAtLoss: this.entriesAtLoss,
      applies: this.applies,
      lost: this.timeline.filter(({ name, key }) => name === 'A' && key === 'PARTITIONS_LOST')
        .length,
    }
  }

  /**
   * Stops both members, frees every held call, restores spies and listeners and checks the
   * episode left nothing behind.
   * @returns {Promise<void>}
   * @throws {Error} when a socket, listener or the scripted cluster is left dirty
   */
  async close() {
    const problems = []
    this.widened = true
    this.applyGate?.()
    this.handlerRelease?.()
    for (const gate of this.gates) if (gate.settle) gate.release()
    for (const release of this.late) release({})
    await Promise.all(
      Object.values(this.consumers).map((consumer) =>
        consumer.disconnect().catch((error) => problems.push(error))
      )
    )
    await Promise.all(this.running)
    for (const spy of this.spies) spy.mockRestore()
    process.off('unhandledRejection', this.unhandledListener)
    // A missing API is refused by the scripted cluster by name; that refusal is the fault.
    const refused = ({ message }) =>
      this.fault === 'missing' && /advertises no version of it/.test(message)
    const failure = this.cluster.failures().find((error) => !refused(error))
    if (failure) problems.push(failure)
    const { created, destroyed } = socketCounts(this.cluster.fake.state)
    if (created !== destroyed) {
      const open = this.cluster
        .sockets()
        .filter((socket) => !socket.destroyed)
        .map(({ endpoint }) => `${endpoint.host}:${endpoint.port}`)
      problems.push(
        new Error(
          `${created - destroyed} sockets left open: ${open.join(', ')}; runners ${this.runners.length}; states ${this.states.join('>')}`
        )
      )
    }
    if (process.listenerCount('unhandledRejection') !== this.baseline?.listeners) {
      problems.push(new Error('unhandledRejection listener left behind'))
    }
    if (problems.length) throw problems[0]
  }
}

const NORMAL = Object.freeze(['JOINING', 'RECONCILING', 'ACKNOWLEDGING', 'STABLE'])
const REJOIN = Object.freeze(['JOINING', 'RECONCILING', 'ACKNOWLEDGING', 'STABLE'])
// Declared per step and outcome class from the fault table, before any episode ran. A fault
// answered while the join, the apply or the acknowledgement is held ends that phase; a fence then
// renews membership through one more JOINING round, a fatal answer ends in FATAL and one CRASH.
const SEQUENCES = Object.freeze({
  'before join': {
    healthy: NORMAL,
    FENCED: ['JOINING', 'FENCED', ...REJOIN],
    FATAL: ['JOINING', 'FATAL'],
  },
  'target unapplied': {
    healthy: NORMAL,
    FENCED: ['JOINING', 'RECONCILING', 'FENCED', ...REJOIN],
    FATAL: ['JOINING', 'RECONCILING', 'FATAL'],
  },
  'during apply': {
    healthy: NORMAL,
    FENCED: ['JOINING', 'RECONCILING', 'FENCED', ...REJOIN],
    FATAL: ['JOINING', 'RECONCILING', 'FATAL'],
  },
  'during acknowledgement': {
    healthy: NORMAL,
    FENCED: ['JOINING', 'RECONCILING', 'ACKNOWLEDGING', 'FENCED', ...REJOIN],
    FATAL: ['JOINING', 'RECONCILING', 'ACKNOWLEDGING', 'FATAL'],
  },
})
// A missing API is learned on reconnect, after the held acknowledgement was already generated.
const SEQUENCE_EXCEPTIONS = Object.freeze({
  'during acknowledgement|missing': ['JOINING', 'RECONCILING', 'ACKNOWLEDGING', 'STABLE', 'FATAL'],
})
const HELD_APPLY_STEPS = new Set(['target unapplied', 'during apply'])
const outcomeOf = (fault) => FAULTS.find(([name]) => name === fault)[1]
const classOf = (fault) =>
  ['FENCED', 'FATAL'].includes(outcomeOf(fault)) ? outcomeOf(fault) : 'healthy'
const expectedSequence = (step, fault) =>
  SEQUENCE_EXCEPTIONS[`${step}|${fault}`] ?? SEQUENCES[step][classOf(fault)]
const isTerminal = (fault) => outcomeOf(fault) === 'FATAL'
// These cells release the held apply inside error classification, before verdict delivery.
// They must refuse that apply and then recover or terminate through the retained LOST verdict.
const isInterruptedApplyCell = (step, fault) =>
  HELD_APPLY_STEPS.has(step) && classOf(fault) !== 'healthy'
const EPISODE_CELLS = CELLS.map(([step, , fault]) => [step, fault])

/**
 * Runs `body` on one episode and always closes it, keeping the body's own failure over a cleanup
 * failure.
 * @param {object} options passed to `FaultEpisode.open`
 * @param {(episode: FaultEpisode) => Promise<void>} body
 * @returns {Promise<void>}
 */
const withEpisode = async (options, body) => {
  const episode = await FaultEpisode.open(options)
  let primary
  try {
    await body(episode)
  } catch (error) {
    primary = error
    throw error
  } finally {
    await episode.close().catch((cleanup) => {
      if (!primary) throw cleanup
      primary.cleanupError = cleanup
    })
  }
}

/**
 * The assertion contract of one table cell.
 * @param {string} step
 * @param {number|string} fault
 * @param {object} [options] extra episode options
 * @returns {Promise<void>}
 */
const runCell = (step, fault, options = {}) =>
  withEpisode({ step, fault, ...options }, async (episode) => {
    await episode.holdBoundary(step)
    await episode.answerFault(fault)
    await episode.releaseAndRecover(isTerminal(fault))
    const facts = episode.facts()
    expect(facts.states).toEqual(expectedSequence(step, fault))
    expect(facts.owners.overlaps).toEqual([])
    expect(facts.commits.afterLearnedLoss).toEqual([])
    expect(facts.wire.afterTerminal).toEqual([])
    expect(facts.unhandled).toEqual([])
    expect(facts.rounds).toBeLessThanOrEqual(6)
    expect(facts.crashes).toBe(isTerminal(fault) ? 1 : 0)
    if (!isTerminal(fault)) {
      expect(facts.owners.final).toEqual({ A: [episode.partitionOf.A], B: [episode.partitionOf.B] })
    }
  })

describe('Consumer > seam fault episodes', () => {
  afterEach(() => jest.restoreAllMocks())

  test.each(EPISODE_CELLS.filter(([step, fault]) => !isInterruptedApplyCell(step, fault)))(
    '%s with fault %s follows its declared state sequence',
    (step, fault) => runCell(step, fault),
    60000
  )

  test.each(EPISODE_CELLS.filter(([step, fault]) => isInterruptedApplyCell(step, fault)))(
    '%s with fault %s recovers like the other boundaries',
    (step, fault) => runCell(step, fault, { waitMs: 3000 }),
    60000
  )

  test.each([
    ['target unapplied', 'disconnect'],
    ['target unapplied', 16],
  ])(
    'applies %s while fault %s has left the coordinator unknown',
    (step, fault) =>
      withEpisode({ step, fault, eager: true }, async (episode) => {
        await episode.holdBoundary()
        let releaseLookup
        const lookupGate = new Promise((resolve) => {
          releaseLookup = resolve
        })
        const find = episode.group.cluster.findGroupCoordinator.bind(episode.group.cluster)
        episode.spies.push(
          jest
            .spyOn(episode.group.cluster, 'findGroupCoordinator')
            .mockImplementation(async (...args) => {
              await lookupGate
              return find(...args)
            })
        )
        let readWithNoCoordinator = false
        const get = OffsetManager.prototype.getCoordinator
        episode.spies.push(
          jest.spyOn(OffsetManager.prototype, 'getCoordinator').mockImplementation(function (
            ...args
          ) {
            if (this.ownership === episode.group.ownership && this.coordinator == null) {
              readWithNoCoordinator = true
            }
            return get.apply(this, args)
          })
        )
        try {
          await episode.answerFault()
          expect(episode.protocol.coordinator).toBeNull()
          episode.applyGate()
          await episode.settled(
            () => readWithNoCoordinator || episode.crashes.length > 0,
            'offset discovery with no cached coordinator'
          )
          expect(readWithNoCoordinator).toBe(true)
          releaseLookup()
          await episode.settled(
            () => episode.facts().commits.all.length > 0 || episode.crashes.length > 0,
            'a recovered commit or a crash'
          )
          expect(
            episode.crashes.map(({ error, restart }) => ({ message: error.message, restart }))
          ).toEqual([])
          await episode.releaseAndRecover()
          expect(episode.facts().states).toEqual(NORMAL)
          expect(episode.facts().owners.overlaps).toEqual([])
        } finally {
          releaseLookup()
        }
      }),
    60000
  )

  test(
    'retains membership when Runner sends an acknowledgement answered with 29',
    () =>
      withEpisode({ step: 'target unapplied', fault: 29 }, async (episode) => {
        await episode.holdBoundary()
        const runner = episode.runners[0]
        runner.heartbeatManager.disable()
        const ack = episode.schedule.hold({
          api: HEARTBEAT,
          match: (call) =>
            episode.isA(call) &&
            call.fields.memberEpoch > 0 &&
            call.fields.topicPartitions?.length > 0,
        })
        episode.gates.push(ack)
        let outcome
        const heartbeat = runner.heartbeat.bind(runner)
        episode.spies.push(
          jest.spyOn(runner, 'heartbeat').mockImplementation(async () => {
            try {
              await heartbeat()
              outcome = 'resolved'
            } catch (error) {
              outcome = error
              throw error
            }
          })
        )
        episode.applyGate()
        await ack.arrived()
        const token = episode.group.ownership.token()
        ack.release(episode.faultOutcome(29))
        await episode.settled(
          () => outcome === 'resolved' || episode.crashes.length > 0,
          'the Runner heartbeat outcome'
        )
        expect(outcome).toBe('resolved')
        expect(episode.crashes).toEqual([])
        expect(episode.group.ownership.token()).toBe(token)
        expect(episode.group.ownership.refusal(token)).toBeNull()
        expect(episode.facts().states).toEqual(NORMAL)
        runner.heartbeatManager.enable()
        await episode.releaseAndRecover()
        await episode.settled(() => episode.entries.length >= 3, 'continued delivery')
        expect(episode.facts().rounds).toBe(1)
        expect(episode.facts().lost).toBe(0)
      }),
    60000
  )

  test(
    'stops a fenced handler before its next record or commit',
    () =>
      withEpisode({ step: 'steady', holdHandler: true }, async (episode) => {
        await episode.holdBoundary('steady')
        await episode.answerFault(110)
        const commit = episode.consumers.A.commitOffsets([
          { topic: TOPIC, partition: 0, offset: '1' },
        ]).catch((error) => error)
        episode.releaseHandler()
        await episode.releaseAndRecover()
        const facts = episode.facts()
        expect(await commit).toMatchObject({ name: 'KafkaJSCommitFailedError' })
        expect(facts.entriesAtLoss).toBe(1)
        expect(facts.entries.slice(0, facts.entriesAtLoss)).toEqual(['0'])
        expect(facts.commits.afterLearnedLoss).toEqual([])
        expect(facts.lost).toBe(1)
        expect(facts.rounds).toBeGreaterThanOrEqual(2)
        expect(facts.states.at(-1)).toBe('STABLE')
        expect(facts.unhandled).toEqual([])
      }),
    60000
  )

  const identity = ({ memberId }, partitions) => ({
    memberId,
    memberEpoch: 1,
    heartbeatIntervalMs: 100,
    assignment: { topicPartitions: partitions.length ? [{ topicId: TOPIC_ID, partitions }] : [] },
  })

  test(
    'accepts an empty server assignment without inventing ownership',
    () =>
      withEpisode(
        {
          step: 'steady',
          answer: (request, member) =>
            request.memberEpoch === 0 ? identity(request, member === 'A' ? [] : [0, 1]) : undefined,
        },
        async (episode) => {
          await episode.holdBoundary('steady')
          await episode.settled(() => episode.ownedBy.B.size === 2, 'member B to own both')
          const facts = episode.facts()
          expect(facts.owners.final).toEqual({ A: [], B: [0, 1] })
          expect(facts.applies).toBe(1)
          expect(facts.states.at(-1)).toBe('STABLE')
          expect(
            episode.timeline.filter(
              ({ name, key }) => name === 'A' && key === 'PARTITIONS_ASSIGNED'
            )
          ).toEqual([])
          expect(facts.owners.overlaps).toEqual([])
          expect(facts.unhandled).toEqual([])
        }
      ),
    60000
  )

  test('keeps an equal server assignment without duplicate application', () => {
    let repeated = false
    return withEpisode(
      {
        step: 'steady',
        answer: (request, member) => {
          if (member !== 'A' || request.memberEpoch !== 1 || repeated) return undefined
          repeated = true
          return { ...identity(request, [0]), memberEpoch: 2 }
        },
      },
      async (episode) => {
        await episode.holdBoundary('steady')
        await episode.settled(() => repeated, 'the repeated assignment to be answered')
        await episode.settled(
          () =>
            episode.cluster
              .requests()
              .some(
                ({ api, fields }) => api === 'ConsumerGroupHeartbeat' && fields.memberEpoch === 2
              ),
          'a heartbeat under the new epoch'
        )
        const facts = episode.facts()
        expect(facts.applies).toBe(1)
        expect(
          episode.timeline.filter(({ name, key }) => name === 'A' && key === 'PARTITIONS_ASSIGNED')
        ).toHaveLength(1)
        expect(facts.owners.final.A).toEqual([0])
        expect(facts.states.at(-1)).toBe('STABLE')
        expect(facts.unhandled).toEqual([])
      }
    )
  }, 60000)

  test(
    'names an unknown server topic before applying it',
    () =>
      withEpisode({ step: 'steady', hideIds: true }, async (episode) => {
        await episode.holdBoundary('steady')
        const requests = episode.cluster.requests()
        const named = requests.find(
          ({ api, fields }) =>
            api === 'Metadata' && fields.topics?.some(({ topicId }) => topicId === TOPIC_ID)
        )
        const acknowledged = requests.find(
          ({ api, fields }) =>
            api === 'ConsumerGroupHeartbeat' &&
            fields.memberEpoch > 0 &&
            fields.topicPartitions?.length
        )
        const facts = episode.facts()
        expect(named).toBeDefined()
        expect(named.order).toBeLessThan(acknowledged.order)
        expect(facts.applies).toBe(1)
        expect(facts.owners.final.A).toEqual([0])
        expect(facts.states.at(-1)).toBe('STABLE')
        expect(facts.unhandled).toEqual([])
      }),
    60000
  )
})

const { KafkaJSCommitFailedError } = require('../../errors')

/**
 * @param {object} options
 * @param {number} options.epoch the member epoch the heartbeats have installed
 * @param {Function} options.offsetCommit the broker double's commit
 * @returns {{ group: ConsumerGroup, manager: OffsetManager, membership: object }} a real
 *   consumer-protocol group with its real offset manager, an owned partition and a resolved offset
 */
const staleEpochHarness = ({ epoch, offsetCommit }) => {
  const h = commitEpisode('consumer')
  const membership = { epoch }
  h.group.groupMembership.memberEpoch = epoch
  const coordinator = { isConnected: () => true, offsetCommit }
  h.group.groupMembership.coordinator = coordinator
  const manager = new OffsetManager({
    cluster: {
      committedOffsets: () => ({}),
      topicId: () => undefined,
      findGroupCoordinator: async () => coordinator,
    },
    coordinator,
    memberAssignment: { topic: [0] },
    autoCommit: true,
    autoCommitInterval: null,
    autoCommitThreshold: null,
    topicConfigurations: { topic: {} },
    instrumentationEmitter: new Emitter(),
    groupId: 'epoch-loss',
    generationId: epoch,
    memberId: 'm',
    membershipProtocol: 'consumer',
    ownership: h.group.ownership,
    membership: () => ({
      generationId: h.group.groupMembership.memberEpoch,
      memberId: 'm',
      coordinator,
      revision: 0,
    }),
    staleEpochTimeout: 5000,
    logger: h.group.logger,
  })
  h.group.offsetManager = manager
  h.group.autoCommit = true
  manager.resolveOffset({ topic: 'topic', partition: 0, offset: '5' })
  return { group: h.group, manager, membership }
}

const staleEpoch = () => new KafkaJSProtocolError(createErrorFromCode(113))

describe('Consumer > stale member epoch commits through the real sender', () => {
  test('skips a stale-epoch automatic commit', async () => {
    const offsetCommit = jest.fn(async () => {
      throw staleEpoch()
    })
    const { manager } = staleEpochHarness({ epoch: 7, offsetCommit })
    manager.lastCommit = 0
    await expect(manager.commitOffsets({}, { auto: true })).resolves.toBeUndefined()
    await new Promise((resolve) => setTimeout(resolve, 150))
    expect(offsetCommit).toHaveBeenCalledTimes(1)
    expect(manager.lastCommit).toBeGreaterThan(0)
  })

  test('rejects a stale-epoch explicit commit', async () => {
    const offsetCommit = jest.fn(async () => {
      throw staleEpoch()
    })
    const { manager } = staleEpochHarness({ epoch: 7, offsetCommit })
    const caught = await manager.commitOffsets().catch((error) => error)
    expect(caught).toMatchObject({ type: 'STALE_MEMBER_EPOCH', retriable: false })
    expect(offsetCommit).toHaveBeenCalledTimes(1)
  })

  test('waits for a newer epoch before pre-release commit', async () => {
    const offsetCommit = jest.fn(async ({ groupGenerationId }) => {
      if (groupGenerationId === 7) throw staleEpoch()
    })
    const { group, manager } = staleEpochHarness({ epoch: 7, offsetCommit })
    const releasing = group.commitBeforeRelease([{ topic: 'topic', partitions: [0] }])
    await new Promise((resolve) => setTimeout(resolve, 350))
    expect(offsetCommit).toHaveBeenCalledTimes(1)
    group.groupMembership.memberEpoch = 8
    await releasing
    expect(offsetCommit).toHaveBeenCalledTimes(2)
    expect(offsetCommit.mock.calls[1][0]).toMatchObject({ groupGenerationId: 8 })
    group.ownership.lose(new Error('lost'), group.ownership.token())
    await expect(manager.commitOffsets()).rejects.toBeInstanceOf(KafkaJSCommitFailedError)
    expect(offsetCommit).toHaveBeenCalledTimes(2)
  })
})
