const { isDeepStrictEqual } = require('util')
const { Kafka, logLevel } = require('../../../../index')
const { readLog } = require('../../../../testHelpers/rawLog')
const {
  SHARE_TOPIC_PREFIX,
  shareTopicName,
  setShareGroupConfigs,
  shareRecordIdentity,
  verifierMember,
  assertShareCleanup,
} = require('../../../../testHelpers/shareFixtures')
const {
  createResourceProbe,
  createTopic,
  plainTextBrokers,
  secureRandom,
  wireVersionsOf,
  writeEvidence,
} = require('testHelpers')
const defaultSocketFactory = require('../../../network/socketFactory')
const RequestQueue = require('../../../network/requestQueue')
const ALTER_CONFIG_OP_TYPES = require('../../../protocol/alterConfigOpTypes')
const CONFIG_RESOURCE_TYPES = require('../../../protocol/configResourceTypes')
const CONFIG_SOURCE = require('../../../protocol/configSource')
const { ShareAckLedger } = require('../shareAckLedger')
const {
  NEVER,
  bounded,
  caseTimeout,
  liveCase,
  liveRun,
  pollUntil,
} = require('../../../producer/__tests__/fixtures/bounded')
const { CleanupStack, describeError } = require('../../../producer/__tests__/fixtures/liveProbe')

const enabled = process.env.SHARE_ISOLATION_E2E === '1' ? describe : describe.skip

const TITLE = 'separates committed visibility and waits for an open transaction'
const EVIDENCE = 'share-isolation'
const PROFILE = 'tv2'
const CASE_BUDGET_MS = 110000
const HOOK_BUDGET_MS = 10000
const STEP_MS = 20000
const DELIVERY_MS = 30000
const CLEANUP_MS = 25000
const PROOF_MS = 10000
const TIMESTAMP = '1700000000000'
const COMMIT_MARKER = 1
const ABORT_MARKER = 0
/** The share-group config keys this case sets, so cleanup can remove exactly them. */
const GROUP_KEYS = ['share.auto.offset.reset', 'share.isolation.level']
const LEDGER_METHODS = [
  'canStart',
  'decide',
  'complete',
  'completeRange',
  'dropExpired',
  'needsFlush',
  'prepare',
  'settle',
  'invalidate',
]

/**
 * @param {Error} error a failed admin call
 * @returns {boolean} whether it only says the group coordinator is loading or moving (14, 15, 16)
 */
const moving = (error) => [14, 15, 16].includes(error?.code ?? error?.cause?.code)

/**
 * @param {number} index
 * @returns {string} the index as four digits
 */
const pad = (index) => String(index).padStart(4, '0')

/**
 * The fixed records of one block: key `<stem>:<index>`, value `<stem>:value:<index>`, the old fixed
 * timestamp, partition 0. Nothing here comes from what a consumer received.
 * @param {string} stem names the block
 * @param {number} count how many records
 * @returns {Array<{partition: number, key: Buffer, value: Buffer, timestamp: string}>}
 */
const block = (stem, count) =>
  Array.from({ length: count }, (_, index) => ({
    partition: 0,
    key: Buffer.from(`${stem}:${pad(index)}`),
    value: Buffer.from(`${stem}:value:${index}`),
    timestamp: TIMESTAMP,
  }))

/**
 * Counts sockets through the factory a client was built with, so a probe can compare open sockets.
 */
class SocketCounter {
  /** Starts with no sockets. */
  constructor() {
    /** @type {import('net').Socket[]} */
    this.sockets = []
  }

  /**
   * @returns {Function} a socket factory that remembers every socket it creates
   */
  factory() {
    const create = defaultSocketFactory()
    return (options) => {
      const socket = create(options)
      this.sockets.push(socket)
      return socket
    }
  }

  /**
   * @returns {{ created: number, destroyed: number }} sockets created and how many are destroyed
   */
  counts() {
    return {
      created: this.sockets.length,
      destroyed: this.sockets.filter((socket) => socket.destroyed).length,
    }
  }
}

/**
 * Reads the product's own acknowledgement ledgers and request queues through call-through spies on
 * their prototypes: the spy runs the original with its own `this` and returns its result, and only
 * remembers which instance it saw. Nothing is altered or answered.
 */
class ResourceGauge {
  /** Starts with no spy installed. */
  constructor() {
    /** @type {Set<ShareAckLedger>} */
    this.ledgers = new Set()
    /** @type {Set<RequestQueue>} */
    this.queues = new Set()
    this.restores = []
  }

  /**
   * Installs the spies.
   * @returns {void}
   */
  install() {
    this.watch(ShareAckLedger.prototype, LEDGER_METHODS, this.ledgers)
    this.watch(RequestQueue.prototype, ['push'], this.queues)
  }

  /**
   * @param {object} prototype class prototype to spy on
   * @param {string[]} names methods that remember their receiver
   * @param {Set<object>} seen where receivers are remembered
   * @returns {void}
   */
  watch(prototype, names, seen) {
    for (const name of names) {
      const original = prototype[name]
      prototype[name] = function (...args) {
        seen.add(this)
        return original.apply(this, args)
      }
      this.restores.push(() => {
        prototype[name] = original
      })
    }
  }

  /**
   * Restores every original method and forgets what was seen.
   * @returns {void}
   */
  uninstall() {
    for (const restore of this.restores.splice(0).reverse()) restore()
    this.ledgers.clear()
    this.queues.clear()
  }

  /**
   * @returns {number} requests pending or in flight on every queue seen
   */
  queueDepth() {
    return [...this.queues].reduce(
      (total, queue) => total + queue.pending.length + queue.inflight.size,
      0
    )
  }

  /**
   * @returns {number} entries every ledger seen still holds
   */
  ledgerSize() {
    return [...this.ledgers].reduce((total, ledger) => total + ledger.size, 0)
  }
}

/**
 * Reduces a probe snapshot to the integers the evidence verifier compares.
 * @param {object} snapshot a `createResourceProbe` snapshot with every source
 * @returns {{openSockets: number, timers: number, listeners: number, queueDepth: number, ledgerSize: number}}
 */
const summarize = (snapshot) => ({
  openSockets: snapshot.sockets.open,
  timers: snapshot.activeResources.Timeout ?? 0,
  listeners: Object.values(snapshot.listeners).reduce((total, count) => total + count, 0),
  queueDepth: snapshot.queueDepth,
  ledgerSize: snapshot.ledgerSize,
})

/**
 * One share member with everything it observed, kept per fact and never reconstructed from a count.
 */
class Member {
  /**
   * @param {object} options
   * @param {Kafka} options.kafka the client factory, whose sockets the case counts
   * @param {string} options.label short name in evidence
   * @param {string} options.groupId the share group
   * @param {object} [options.config] share consumer options beyond the group
   */
  constructor({ kafka, label, groupId, config = {} }) {
    this.label = label
    this.groupId = groupId
    this.client = kafka.shareConsumer({ groupId, maxRecords: 100, maxWaitTimeInMs: 250, ...config })
    /** @type {Array<{identity: string, deliveryCount: number, offset: string, at: number}>} */
    this.entries = []
    this.acks = []
    this.requests = []
    this.assignments = []
    this.crashes = []
    this.removers = []
  }

  /**
   * Starts observing; call before `start`, so no event can be missed.
   * @returns {void}
   */
  listen() {
    const { events } = this.client
    const keep = (name, list) =>
      this.removers.push(
        this.client.on(events[name], ({ payload }) => list.push({ ...payload, at: Date.now() }))
      )
    keep('ACKNOWLEDGE', this.acks)
    keep('REQUEST', this.requests)
    keep('ASSIGNMENT_CHANGED', this.assignments)
    keep('CRASH', this.crashes)
  }

  /**
   * Connects, subscribes and starts the run, each step bounded.
   * @param {AbortSignal} scope
   * @param {string} topic
   * @returns {Promise<void>}
   * @throws {Error} a failed or slow step
   */
  async start(scope, topic) {
    await bounded(scope, STEP_MS, () => this.client.connect())
    await bounded(scope, STEP_MS, () => this.client.subscribe({ topics: [topic] }))
    await bounded(scope, STEP_MS, () =>
      this.client.run({
        eachMessage: async ({ topic: name, partition, message }) => {
          this.entries.push({
            identity: shareRecordIdentity({ ...message, topic: name, partition }),
            deliveryCount: message.deliveryCount,
            offset: message.offset,
            at: Date.now(),
          })
        },
      })
    )
  }

  /**
   * @returns {string[]} every delivered identity, in delivery order, one element per delivery
   */
  identities() {
    return this.entries.map(({ identity }) => identity)
  }

  /**
   * Offsets the member's accepted gap acknowledgements cover.
   * @returns {Set<string>} decimal offsets
   */
  gapOffsets() {
    const covered = new Set()
    for (const { type, error, firstOffset, lastOffset } of this.acks) {
      if (type !== 'gap' || error !== null) continue
      for (let offset = BigInt(firstOffset); offset <= BigInt(lastOffset); offset++) {
        covered.add(String(offset))
      }
    }
    return covered
  }

  /**
   * @returns {void} removes every listener this member added
   */
  unlisten() {
    for (const remove of this.removers.splice(0)) remove()
  }
}

/**
 * Deletes a topic and waits until the broker no longer lists it.
 * @param {AbortSignal} scope
 * @param {object} admin a connected admin client
 * @param {string} topic
 * @returns {Promise<void>}
 * @throws {Error} a delete other than "unknown topic", or the topic still listed at the bound
 */
const dropTopic = async (scope, admin, topic) => {
  try {
    await bounded(scope, STEP_MS, () => admin.deleteTopics({ topics: [topic], timeout: 15000 }))
  } catch (error) {
    if (error.type !== 'UNKNOWN_TOPIC_OR_PARTITION') throw error
  }
  await pollUntil(scope, STEP_MS, async () => !(await admin.listTopics()).includes(topic), {
    delayMs: 500,
    label: `Deleting ${topic}`,
  })
}

/**
 * Deletes a group and waits until the broker no longer lists it. A member that has just left keeps
 * the group non-empty for a moment (68), which is retried; "not found" counts only once a listing
 * confirms the absence.
 * @param {AbortSignal} scope
 * @param {object} admin a connected admin client
 * @param {string} group
 * @returns {Promise<void>}
 * @throws {Error} any other group error, or the group still listed at the bound
 */
const dropGroup = (scope, admin, group) =>
  pollUntil(
    scope,
    STEP_MS,
    async () => {
      try {
        await admin.deleteGroups([group])
      } catch (error) {
        const code = error.groups?.find(({ groupId }) => groupId === group)?.errorCode
        if (code === 68) return false
        if (code !== 69) throw error
      }
      const { groups } = await admin.listGroups()
      return !groups.some(({ groupId }) => groupId === group)
    },
    { delayMs: 500, label: `Deleting ${group}` }
  )

/**
 * Removes the dynamic GROUP configs a case set and verifies none is left.
 * @param {AbortSignal} scope
 * @param {object} admin a connected admin client
 * @param {string} group
 * @param {string[]} names the owned config keys
 * @returns {Promise<void>}
 * @throws {Error} a refused change, or an owned override still present
 */
const resetGroupConfig = async (scope, admin, group, names) => {
  const resource = { type: CONFIG_RESOURCE_TYPES.GROUP, name: group }
  const configEntries = names.map((name) => ({ name, operation: ALTER_CONFIG_OP_TYPES.DELETE }))
  const { resources } = await bounded(scope, STEP_MS, () =>
    admin.incrementalAlterConfigs({ resources: [{ ...resource, configEntries }] })
  )
  if (resources[0].errorCode !== 0) {
    throw new Error(`Group ${group} refused a config reset: error ${resources[0].errorCode}`)
  }
  await pollUntil(
    scope,
    STEP_MS,
    async () => {
      const read = await admin.describeConfigs({
        resources: [{ ...resource, configNames: names }],
      })
      return read.resources[0].configEntries.every(
        ({ configSource }) => configSource !== CONFIG_SOURCE.GROUP_CONFIG
      )
    },
    { delayMs: 500, label: `Resetting ${group}` }
  )
}

/**
 * The live case: committed and read_committed share groups over one partition that holds committed,
 * aborted and plain records, then an open transaction.
 */
class IsolationCase {
  /** Names every owned resource and builds the clients; nothing is created on the cluster yet. */
  constructor() {
    this.nonce = secureRandom()
    this.topic = shareTopicName('isolation')
    this.groups = {
      R: `${SHARE_TOPIC_PREFIX}isolation-r-${this.nonce}`,
      C: `${SHARE_TOPIC_PREFIX}isolation-c-${this.nonce}`,
    }
    this.sockets = new SocketCounter()
    this.gauge = new ResourceGauge()
    this.probe = createResourceProbe({
      emitters: { process },
      sockets: () => this.sockets.counts(),
      queueDepth: () => this.gauge.queueDepth(),
      ledgerSize: () => this.gauge.ledgerSize(),
      activeResources: true,
    })
    this.kafkaOptions = {
      brokers: plainTextBrokers(),
      logLevel: logLevel.NOTHING,
    }
    this.kafka = new Kafka({
      ...this.kafkaOptions,
      clientId: `share-isolation-${this.nonce}`,
      socketFactory: this.sockets.factory(),
    })
    this.admin = new Kafka({
      ...this.kafkaOptions,
      clientId: `share-isolation-admin-${this.nonce}`,
    }).admin()
    this.stack = new CleanupStack()
    this.expected = { R: [], C: [] }
    this.log = []
    this.broker = []
    this.ending = false
  }

  /**
   * Appends one evidence row stamped with the case, profile, nonce, group aliases and budgets.
   * @param {string} event the row's event name
   * @param {object} [fields] what was measured
   * @returns {void}
   */
  row(event, fields = {}) {
    writeEvidence(EVIDENCE, {
      event,
      case: TITLE,
      profile: PROFILE,
      scenario: this.nonce,
      groups: this.groups,
      budgets: { caseMs: CASE_BUDGET_MS, stepMs: STEP_MS, deliveryMs: DELIVERY_MS },
      ...fields,
    })
  }

  /**
   * Runs the whole case: bounded setup, the isolation action, the final oracles, strict cleanup
   * and, only if all of it held, one completion row.
   * @param {AbortSignal} scope the case scope
   * @returns {Promise<void>}
   * @throws {Error} the first failure; cleanup failures ride on it
   */
  async run(scope) {
    const outer = new CleanupStack()
    outer.add('release resource gauge', () => this.gauge.uninstall())
    outer.add('disconnect admin', () => this.admin.disconnect())
    let pre
    let probeBase
    let after
    try {
      await outer.run(
        async () => {
          this.gauge.install()
          pre = await this.sample(scope)
          await bounded(scope, STEP_MS, () => this.admin.connect())
          await bounded(scope, STEP_MS, async () => {
            await this.admin.describeCluster()
            await this.admin.listGroups()
          })
          probeBase = await this.sample(scope)
          this.row('resource-baseline', { resources: summarize(probeBase) })
          await this.stack.run(
            async () => {
              await this.setup(scope)
              await this.isolate(scope)
              await this.finish(scope)
            },
            { budgetMs: CLEANUP_MS }
          )
          after = await assertShareCleanup(
            this.admin,
            Object.values(this.groups),
            [this.topic],
            probeBase,
            this.probe,
            { signal: scope }
          )
        },
        { budgetMs: PROOF_MS }
      )
      await this.settleProcess(scope, pre)
      this.complete(probeBase, after)
    } catch (error) {
      this.row('case-failed', { status: 'failed', error: describeError(error) })
      throw error
    }
  }

  /**
   * One resource sample, taken under the same bounds `assertShareCleanup` samples under, so the
   * timers a bound itself holds appear in the baseline too.
   * @param {AbortSignal} scope
   * @returns {Promise<object>} a probe snapshot
   */
  sample(scope) {
    return bounded(scope, PROOF_MS, (inner) =>
      bounded(inner, Infinity, () => this.probe.snapshot())
    )
  }

  /**
   * After the admin client is gone, the process holds the handles it held before the case began.
   * Timers are left out: the poll's own bounds hold some.
   * @param {AbortSignal} scope
   * @param {object} base the sample taken before the admin client connected
   * @returns {Promise<void>}
   * @throws {Error} event-loop handles still differ at the bound
   */
  settleProcess(scope, base) {
    const handles = ({ Timeout: _timers, ...rest }) => rest
    return pollUntil(
      scope,
      PROOF_MS,
      async () =>
        isDeepStrictEqual(
          handles((await this.sample(scope)).activeResources),
          handles(base.activeResources)
        ),
      { delayMs: 250, label: 'Event-loop handles returning to their baseline' }
    )
  }

  /**
   * Creates the topic and groups, reads the configs back and starts both members. Every owned
   * resource is registered for deletion before it is created.
   * @param {AbortSignal} scope
   * @returns {Promise<void>}
   * @throws {Error} a refused create, a config not read back, or a member that never got partition 0
   */
  async setup(scope) {
    const { stack, admin, topic } = this
    stack.add('delete topic', (signal) => dropTopic(signal, admin, topic))
    for (const group of Object.values(this.groups)) {
      stack.add(`delete group ${group}`, (signal) => dropGroup(signal, admin, group))
      stack.add(`reset config of ${group}`, (signal) =>
        resetGroupConfig(signal, admin, group, GROUP_KEYS)
      )
    }
    await bounded(scope, STEP_MS, () =>
      createTopic({
        topic,
        partitions: 1,
        replicas: 1,
        config: [
          { name: 'retention.ms', value: '-1' },
          { name: 'cleanup.policy', value: 'delete' },
        ],
      })
    )
    await bounded(scope, STEP_MS, () =>
      setShareGroupConfigs(admin, this.groups.R, { 'share.auto.offset.reset': 'earliest' })
    )
    await bounded(scope, STEP_MS, () =>
      setShareGroupConfigs(admin, this.groups.C, {
        'share.auto.offset.reset': 'earliest',
        'share.isolation.level': 'read_committed',
      })
    )
    const read = await bounded(scope, STEP_MS, () =>
      admin.describeConfigs({
        resources: [
          {
            type: CONFIG_RESOURCE_TYPES.GROUP,
            name: this.groups.R,
            configNames: ['share.isolation.level'],
          },
        ],
      })
    )
    this.defaultIsolation = read.resources[0].configEntries[0].configValue
    expect(this.defaultIsolation).toBe('read_uncommitted')
    this.row('config-readback', {
      configs: { R: { 'share.isolation.level': this.defaultIsolation } },
    })

    this.members = {
      R: new Member({ kafka: this.kafka, label: 'R', groupId: this.groups.R }),
      C: new Member({ kafka: this.kafka, label: 'C', groupId: this.groups.C }),
    }
    for (const member of Object.values(this.members)) {
      member.listen()
      stack.add(`remove ${member.label} listeners`, () => member.unlisten())
      stack.add(`disconnect ${member.label}`, () => member.client.disconnect({ timeout: 5000 }))
    }
    await Promise.all(Object.values(this.members).map((member) => member.start(scope, topic)))
    await this.awaitAssigned(scope)
  }

  /**
   * Waits until each group's one member is assigned partition 0, as its own events, its public
   * `assignment()` and the broker's description all say. Waiting for `run()` alone is not enough.
   * @param {AbortSignal} scope
   * @returns {Promise<void>}
   * @throws {Error} an assignment that never agrees
   */
  async awaitAssigned(scope) {
    const wanted = [{ topic: this.topic, partitions: [0] }]
    await pollUntil(
      scope,
      STEP_MS,
      async () => {
        for (const member of Object.values(this.members)) {
          const last = member.assignments.at(-1)
          if (!last || !isDeepStrictEqual(member.client.assignment(), wanted)) return false
          let members
          try {
            members = (await this.admin.describeShareGroups([member.groupId])).groups[0].members
          } catch (error) {
            if (moving(error)) return false
            throw error
          }
          const [described] = members
          if (
            members.length !== 1 ||
            described.memberId !== last.memberId ||
            described.clientId !== `share-isolation-${this.nonce}` ||
            !isDeepStrictEqual(
              described.assignment.map(({ topic, partitions }) => ({ topic, partitions })),
              wanted
            )
          ) {
            return false
          }
        }
        return true
      },
      { delayMs: 250, label: 'Both members getting partition 0' }
    )
    this.row('assignment', {
      members: Object.fromEntries(
        Object.entries(this.members).map(([label, member]) => [
          label,
          {
            memberId: member.assignments.at(-1).memberId,
            epoch: member.assignments.at(-1).memberEpoch,
          },
        ])
      ),
    })
  }

  /**
   * Produces one block and returns the identities it was appended under, from the broker's own base
   * offset for it.
   * @param {AbortSignal} scope
   * @param {{ send: Function }} sender a producer or an open transaction
   * @param {string} stem
   * @param {number} expectedBase the offset the block must start at, from the case's fixed layout
   * @returns {Promise<string[]>} identities in append order
   * @throws {Error} an append that did not land at the offset it was expected at
   */
  async append(scope, sender, stem, expectedBase) {
    const messages = block(stem, 10)
    const [meta] = await bounded(scope, STEP_MS, () =>
      sender.send({ topic: this.topic, acks: -1, messages })
    )
    expect(meta.errorCode).toBe(0)
    expect(meta.baseOffset).toBe(String(expectedBase))
    return messages.map((message, index) =>
      shareRecordIdentity({
        topic: this.topic,
        partition: 0,
        offset: String(BigInt(meta.baseOffset) + BigInt(index)),
        key: message.key,
        value: message.value,
      })
    )
  }

  /**
   * The case's action: committed, aborted and plain records, then an open transaction that R sees
   * and C must not until it commits.
   * @param {AbortSignal} scope
   * @returns {Promise<void>}
   * @throws {Error} an oracle that does not hold
   */
  async isolate(scope) {
    const { stack } = this
    const plain = this.kafka.producer({ idempotent: true, maxInFlightRequests: 1 })
    const transactional = this.kafka.producer({
      transactionalId: `${SHARE_TOPIC_PREFIX}isolation-tx-${this.nonce}`,
      idempotent: true,
      maxInFlightRequests: 1,
    })
    stack.add('disconnect plain producer', () => plain.disconnect())
    stack.add('disconnect transactional producer', () => transactional.disconnect())
    await bounded(scope, STEP_MS, () => plain.connect())
    await bounded(scope, STEP_MS, () => transactional.connect())

    const committed = await this.transact(scope, transactional, 'commit', 0, 'commit')
    const aborted = await this.transact(scope, transactional, 'abort', 11, 'abort')
    const visible = await this.append(scope, plain, 'plain', 22)
    this.row('produced', { arm: 'initial', end: '32', markers: { 10: 'commit', 21: 'abort' } })

    const initialR = [...committed, ...aborted, ...visible]
    const initialC = [...committed, ...visible]
    await this.awaitDelivered(scope, 'R', initialR.length)
    await this.awaitDelivered(scope, 'C', initialC.length)
    const initialLog = await this.rawLog(scope)
    expect(initialLog.end).toBe('32')
    expect(initialLog.markers).toEqual([
      { offset: '10', type: COMMIT_MARKER },
      { offset: '21', type: ABORT_MARKER },
    ])
    expect([...initialLog.data].sort()).toEqual([...initialR].sort())
    expect([...this.members.R.identities()].sort()).toEqual([...initialR].sort())
    expect([...this.members.C.identities()].sort()).toEqual([...initialC].sort())
    await this.awaitStart(scope, '32')
    await this.awaitGaps(scope, ['10', '21'])

    const opening = await bounded(scope, STEP_MS, () => transactional.transaction())
    stack.add('abort an open transaction', async () => {
      if (opening.isActive()) await opening.abort()
    })
    const open = await this.append(scope, opening, 'open', 32)
    this.row('produced', { arm: 'open', end: '42' })
    await this.awaitDelivered(scope, 'R', initialR.length + open.length)
    const rFullAt = Date.now()
    await pollUntil(
      scope,
      DELIVERY_MS,
      () => {
        this.expectNoOpenYet(open)
        return this.members.C.requests.some(
          ({ apiName, sentAt }) => apiName === 'ShareFetch' && sentAt > rFullAt
        )
      },
      { delayMs: 100, label: 'A complete read_committed fetch after the open records reached R' }
    )
    this.expectNoOpenYet(open)
    this.ending = true
    await bounded(scope, STEP_MS, () => opening.commit())
    await this.awaitDelivered(scope, 'C', initialC.length + open.length)

    this.expected.R = [...initialR, ...open]
    this.expected.C = [...initialC, ...open]
    this.openIdentities = open
  }

  /**
   * C must hold back the open records until the commit is requested.
   * @param {string[]} open identities of the open transaction's records
   * @returns {void}
   * @throws {Error} an open identity delivered to C before the commit was requested
   */
  expectNoOpenYet(open) {
    if (this.ending) return
    const early = this.members.C.identities().filter((identity) => open.includes(identity))
    expect(early).toEqual([])
  }

  /**
   * Sends one block in a transaction and ends it as asked.
   * @param {AbortSignal} scope
   * @param {{ transaction: Function }} producer a transactional producer
   * @param {string} stem
   * @param {number} base the offset the block must land at
   * @param {'commit'|'abort'} how
   * @returns {Promise<string[]>} identities of the block
   * @throws {Error} a failed step
   */
  async transact(scope, producer, stem, base, how) {
    const transaction = await bounded(scope, STEP_MS, () => producer.transaction())
    const identities = await this.append(scope, transaction, stem, base)
    await bounded(scope, STEP_MS, () =>
      how === 'commit' ? transaction.commit() : transaction.abort()
    )
    return identities
  }

  /**
   * Waits until a member has been handed `count` records in all.
   * @param {AbortSignal} scope
   * @param {'R'|'C'} label
   * @param {number} count
   * @returns {Promise<void>}
   * @throws {Error} a crash, a delivery past `count`, or too few deliveries at the bound
   */
  async awaitDelivered(scope, label, count) {
    const member = this.members[label]
    await pollUntil(
      scope,
      DELIVERY_MS,
      () => {
        expect(member.crashes).toEqual([])
        expect(member.entries.length).toBeLessThanOrEqual(count)
        return member.entries.length === count
      },
      { delayMs: 100, label: `${label} receiving ${count} records` }
    )
  }

  /**
   * Waits until R's accepted gap acknowledgements cover every control marker, and checks that no
   * acknowledgement either member reported failed.
   * @param {AbortSignal} scope
   * @param {string[]} markers decimal marker offsets
   * @returns {Promise<void>}
   * @throws {Error} a marker never acknowledged as a gap, or a failed acknowledgement
   */
  async awaitGaps(scope, markers) {
    await pollUntil(
      scope,
      STEP_MS,
      () => markers.every((m) => this.members.R.gapOffsets().has(m)),
      {
        delayMs: 100,
        label: 'R acknowledging the control markers as gaps',
      }
    )
    for (const { acks } of Object.values(this.members)) {
      expect(acks.filter(({ error }) => error !== null)).toEqual([])
    }
  }

  /**
   * Waits until both groups' share-partition start offset equals `end`, as the named and the
   * all-topics listings agree, with error 0 and a lag that is 0 or explicitly unknown (`null`).
   * @param {AbortSignal} scope
   * @param {string} end decimal log end
   * @returns {Promise<void>}
   * @throws {Error} a start offset that never reaches `end`, or disagreeing listings
   */
  async awaitStart(scope, end) {
    for (const group of Object.values(this.groups)) {
      await pollUntil(
        scope,
        DELIVERY_MS,
        async () => {
          try {
            const [named] = await this.admin.listShareGroupOffsets({
              groupId: group,
              topics: [{ topic: this.topic, partitions: [0] }],
            })
            const [every] = await this.admin.listShareGroupOffsets({ groupId: group })
            const [row] = named.partitions
            expect(every.partitions[0].startOffset).toBe(row.startOffset)
            return (
              row.errorCode === 0 &&
              row.startOffset === end &&
              (row.lag === '0' || row.lag === null)
            )
          } catch (error) {
            if (moving(error)) return false
            throw error
          }
        },
        { delayMs: 250, label: `${group} reaching start offset ${end}` }
      )
    }
  }

  /**
   * Reads the partition straight from its leader.
   * @param {AbortSignal} scope
   * @returns {Promise<{end: string, data: string[], markers: Array<{offset: string, type: number}>}>}
   *   data identities and control markers, separately
   */
  async rawLog(scope) {
    const log = await bounded(scope, STEP_MS, () =>
      readLog({ brokers: plainTextBrokers(), topic: this.topic })
    )
    const { endOffset, records } = log.get(0)
    return {
      end: endOffset,
      data: records
        .filter(({ batchContext }) => !batchContext.isControlBatch)
        .map(({ offset, key, value }) =>
          shareRecordIdentity({ topic: this.topic, partition: 0, offset, key, value })
        ),
      markers: records
        .filter(({ batchContext }) => batchContext.isControlBatch)
        .map(({ offset, key }) => ({ offset, type: key.readInt16BE(2) })),
    }
  }

  /**
   * The final oracles, before cleanup: raw log, broker positions and handler multiplicities, each
   * of them separately and per group, then a quiet same-group verifier.
   * @param {AbortSignal} scope
   * @returns {Promise<void>}
   * @throws {Error} an oracle that does not hold
   */
  async finish(scope) {
    const log = await this.rawLog(scope)
    expect(log.end).toBe('43')
    expect(log.markers).toEqual([
      { offset: '10', type: COMMIT_MARKER },
      { offset: '21', type: ABORT_MARKER },
      { offset: '42', type: COMMIT_MARKER },
    ])
    expect([...log.data].sort()).toEqual([...this.expected.R].sort())
    this.log = log.data
    expect(this.members.R.identities()).toHaveLength(40)
    expect(this.members.C.identities()).toHaveLength(30)
    for (const label of ['R', 'C']) {
      const identities = this.members[label].identities()
      expect([...identities].sort()).toEqual([...this.expected[label]].sort())
      expect(new Set(identities).size).toBe(identities.length)
      expect(this.members[label].crashes).toEqual([])
    }
    await this.awaitStart(scope, '43')
    await this.awaitGaps(scope, ['10', '21', '42'])
    this.broker = []
    for (const group of Object.values(this.groups)) {
      const [named] = await bounded(scope, STEP_MS, () =>
        this.admin.listShareGroupOffsets({
          groupId: group,
          topics: [{ topic: this.topic, partitions: [0] }],
        })
      )
      const [row] = named.partitions
      this.broker.push({
        groupId: group,
        topic: this.topic,
        partition: 0,
        errorCode: row.errorCode,
        startOffset: row.startOffset,
        logEnd: log.end,
        lag: row.lag,
      })
    }
    this.row('acknowledgement', {
      gaps: Object.fromEntries(
        Object.entries(this.members).map(([label, member]) => [
          label,
          member.acks
            .filter(({ type }) => type === 'gap')
            .map(({ firstOffset, lastOffset, error }) => ({
              firstOffset,
              lastOffset,
              error: describeError(error),
            })),
        ])
      ),
    })

    for (const member of Object.values(this.members)) {
      await bounded(scope, STEP_MS, () => member.client.disconnect({ timeout: 5000 }))
    }
    for (const group of Object.values(this.groups)) {
      const quiet = await verifierMember(group, this.topic, { kafka: this.kafka, signal: scope })
      expect(quiet).toEqual([])
    }
    this.row('cleanup', { verifier: 'quiet for 5000 ms in both groups' })
  }

  /**
   * Writes the one completion row, only after every assertion and the strict cleanup passed.
   * @param {object} before the baseline snapshot
   * @param {object} after the snapshot after cleanup
   * @returns {void}
   */
  complete(before, after) {
    const requests = Object.values(this.members).flatMap(({ requests: list }) => list)
    this.row('case-complete', {
      status: 'passed',
      tainted: false,
      oracles: {
        log: { expected: this.expected.R, actual: this.log },
        handler: {
          expected: [...this.expected.R, ...this.expected.C],
          actual: [...this.members.R.identities(), ...this.members.C.identities()],
        },
        handlerByGroup: {
          R: { expected: this.expected.R, actual: this.members.R.identities() },
          C: { expected: this.expected.C, actual: this.members.C.identities() },
        },
        broker: this.broker,
      },
      resources: { before: summarize(before), after: summarize(after) },
      cleanup: {
        groupsAbsent: true,
        topicsAbsent: true,
        completedAt: Number(process.hrtime.bigint() / 1000000n),
      },
      wireVersions: wireVersionsOf(requests),
      config: {
        clients: { maxRecords: 100, maxWaitTimeInMs: 250, acknowledgementMode: 'implicit' },
        groups: {
          R: {
            'share.auto.offset.reset': 'earliest',
            'share.isolation.level': this.defaultIsolation,
          },
          C: { 'share.auto.offset.reset': 'earliest', 'share.isolation.level': 'read_committed' },
        },
      },
    })
  }
}

enabled('share isolation on a live broker', () => {
  beforeAll(
    () =>
      bounded(NEVER, HOOK_BUDGET_MS - 2000, () => {
        liveRun.assertClean()
      }),
    HOOK_BUDGET_MS
  )

  it(
    TITLE,
    liveCase(CASE_BUDGET_MS, (scope) => new IsolationCase().run(scope)),
    caseTimeout(CASE_BUDGET_MS)
  )

  afterEach(() => {
    if (!liveRun.takeAdmission()) return
    liveRun.assertClean()
  })
})
