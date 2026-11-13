const fs = require('fs')
const sleep = require('../src/utils/sleep')
const CONFIG_RESOURCE_TYPES = require('../src/protocol/configResourceTypes')
const CONFIG_SOURCE = require('../src/protocol/configSource')
const ALTER_CONFIG_OP_TYPES = require('../src/protocol/alterConfigOpTypes')
const helpers = require('./index')

/** The topics of the share-group scenarios start with this, so leftovers on a cluster are found. */
const SHARE_TOPIC_PREFIX = 'e2e-p9e-'

/** A scenario name: lower-case kebab-case, so it can never change the topic's shape. */
const SCENARIO_NAME = /^[a-z0-9]+(?:-[a-z0-9]+)*$/

/**
 * A fresh topic name for a share-group scenario, `e2e-p9e-<scenario>-<random>`.
 * @param {string} scenario e.g. `delivery`
 * @returns {string}
 * @throws {Error} when `scenario` is not lower-case kebab-case
 */
const shareTopicName = (scenario) => {
  if (typeof scenario !== 'string' || !SCENARIO_NAME.test(scenario)) {
    throw new Error(
      `A share scenario name is lower-case kebab-case, got ${JSON.stringify(scenario)}`
    )
  }

  return `${SHARE_TOPIC_PREFIX}${scenario}-${helpers.secureRandom()}`
}

/**
 * Creates a fresh topic for a share-group scenario through `createTopic`, which returns once every
 * broker serves the topic with a leader for each partition.
 * @param {{ scenario: string, partitions?: number, replicas?: number }} options
 * @returns {Promise<string>} the name of the topic it created
 * @throws {Error} when the scenario name is invalid (before anything is sent), or what
 *   `createTopic` throws
 */
const createShareTopic = async ({ scenario, partitions = 1, replicas = 1 }) => {
  const topic = shareTopicName(scenario)
  await helpers.createTopic({ topic, partitions, replicas })
  return topic
}

/**
 * Which of the wanted configs one `describeConfigs` answer does not show for the resource yet. A
 * value counts only on an entry of that very resource whose source is `GROUP_CONFIG`: a default or
 * a broker-wide value that happens to be equal does not prove the change applied.
 * @param {object} admin a connected admin client
 * @param {{ type: number, name: string }} resource the GROUP resource
 * @param {[string, string][]} wanted config names and their values
 * @returns {Promise<string[]>} `name=value` of each config not read back
 */
const unreadConfigs = async (admin, resource, wanted) => {
  const { resources } = await admin.describeConfigs({
    resources: [{ ...resource, configNames: wanted.map(([name]) => name) }],
  })
  const read = resources.find(
    ({ resourceType, resourceName }) =>
      resourceType === resource.type && resourceName === resource.name
  )
  const entries = new Map((read?.configEntries ?? []).map((entry) => [entry.configName, entry]))

  return wanted
    .filter(([name, value]) => {
      const entry = entries.get(name)
      return !(
        entry &&
        entry.configValue === value &&
        entry.configSource === CONFIG_SOURCE.GROUP_CONFIG
      )
    })
    .map(([name, value]) => `${name}=${value}`)
}

/**
 * Sets configs on a share group's GROUP resource and resolves only once one `describeConfigs`
 * answer reads every value back as a group config. A member that joins before a change applies
 * starts from the old values (`share.auto.offset.reset` is `latest` until the change is read back),
 * so a scenario calls this before its first member joins.
 *
 * It sets exactly the keys it is given, with one `incrementalAlterConfigs` SET each, so a scenario
 * never inherits a lock or isolation level it did not ask for. A broker that refuses the change (a
 * lock below the broker minimum, an unknown value) fails the call at once with the broker's code
 * and message; nothing is read back then. Reads are repeated every `interval` until `timeout`.
 * @param {object} admin a connected admin client (`kafka.admin()`)
 * @param {string} group the share group's id
 * @param {Record<string, string|number|boolean>} kv config names and values, e.g.
 *   `{ 'share.auto.offset.reset': 'earliest', 'share.record.lock.duration.ms': 15000 }`
 * @param {{ timeout?: number, interval?: number }} [options] how long to keep reading back, in ms
 *   (default 10000), and the pause between two reads (default 250)
 * @returns {Promise<void>}
 * @throws {Error} when `kv` names no config; when the broker refuses the change; when a value is
 *   still not read back at the deadline, naming every such `name=value`
 */
const setShareGroupConfigs = async (admin, group, kv, { timeout = 10000, interval = 250 } = {}) => {
  const wanted = Object.entries(kv).map(([name, value]) => [name, String(value)])
  if (wanted.length === 0) {
    throw new Error(`No config to set on group ${group}`)
  }

  const resource = { type: CONFIG_RESOURCE_TYPES.GROUP, name: group }
  const configEntries = wanted.map(([name, value]) => ({
    name,
    operation: ALTER_CONFIG_OP_TYPES.SET,
    value,
  }))
  const { resources } = await admin.incrementalAlterConfigs({
    resources: [{ ...resource, configEntries }],
  })
  const [result] = resources
  if (result.errorCode !== 0) {
    throw new Error(
      `Group ${group} refused ${configEntries.map(({ name }) => name).join(', ')}: error ${
        result.errorCode
      } ${result.errorMessage}`
    )
  }

  const deadline = Date.now() + timeout
  for (;;) {
    const unread = await unreadConfigs(admin, resource, wanted)
    if (unread.length === 0) {
      return
    }

    if (Date.now() >= deadline) {
      throw new Error(`Group ${group} did not read back ${unread.join(', ')} within ${timeout} ms`)
    }

    await sleep(interval)
  }
}

/**
 * Deletes the given group overrides once, then waits for the broker's readback to catch up.
 * @param {object} admin Connected admin client.
 * @param {string} group Owned group id.
 * @param {string[]} names Config names to restore to defaults.
 * @param {{ signal?: AbortSignal }} [options] Enclosing cleanup scope.
 * @returns {Promise<void>} Once every override is absent from the GROUP resource.
 * @throws {Error} On refusal, missing readback or an override remaining at the deadline, naming
 *   every remaining config.
 */
const resetShareGroupConfigs = async (admin, group, names, { signal } = {}) => {
  const { NEVER, bounded, pollUntil } = require('../src/producer/__tests__/fixtures/bounded')
  let remaining = []
  try {
    await bounded(signal || NEVER, 8000, async (scope) => {
      const resource = { type: CONFIG_RESOURCE_TYPES.GROUP, name: group }
      const { resources } = await bounded(scope, Infinity, () =>
        admin.incrementalAlterConfigs({
          resources: [
            {
              ...resource,
              configEntries: names.map((name) => ({
                name,
                operation: ALTER_CONFIG_OP_TYPES.DELETE,
              })),
            },
          ],
        })
      )
      if (resources[0].errorCode !== 0) {
        throw new Error(`Config reset refused: ${resources[0].errorCode}`)
      }
      await pollUntil(
        scope,
        Infinity,
        async () => {
          const read = await admin.describeConfigs({
            resources: [{ ...resource, configNames: names }],
          })
          const row = read.resources.find(
            ({ resourceType, resourceName }) =>
              resourceType === resource.type && resourceName === group
          )
          if (!row || row.errorCode !== 0) {
            throw new Error('Missing successful group config readback')
          }
          remaining = row.configEntries
            .filter(
              ({ configName, configSource }) =>
                names.includes(configName) && configSource === CONFIG_SOURCE.GROUP_CONFIG
            )
            .map(({ configName }) => configName)
          return remaining.length === 0
        },
        { delayMs: 100, label: 'Group config overrides disappearing' }
      )
    })
  } catch (error) {
    if (remaining.length > 0) {
      error.message += `; Group ${group} config overrides remain: ${remaining.join(', ')}`
    }
    throw error
  }
}

/**
 * A stable delivered-record identity, including bytes and the exact decimal broker offset.
 * @param {{ topic: string, partition: number, offset: string, key: Buffer|null, value: Buffer|null }} record
 * @returns {string} a collision-free JSON tuple; delivery count is an observation, not identity
 * @throws {Error} for a missing topic/partition or a nondecimal offset
 */
const shareRecordIdentity = ({ topic, partition, offset, key, value }) => {
  if (
    typeof topic !== 'string' ||
    !Number.isInteger(partition) ||
    partition < 0 ||
    typeof offset !== 'string' ||
    !/^(0|[1-9][0-9]*)$/.test(offset)
  ) {
    throw new Error('A share record needs a topic, partition and decimal offset')
  }
  return JSON.stringify([
    topic,
    partition,
    offset,
    key === null ? null : key.toString('hex'),
    value === null ? null : value.toString('hex'),
  ])
}

/**
 * A synchronous handler journal: each start is fsynced before the handler can block or be killed,
 * so a parent can read exactly which invocations never finished. A finish records that a handler
 * returned, never that the broker accepted its decision.
 */
class ShareHandlerLog {
  /**
   * @param {string} file fresh, exclusive journal path owned by this invocation
   * @param {string} member client label; broker member id is recorded separately in evidence
   * @throws {Error} on an existing file or filesystem failure
   */
  constructor(file, member) {
    this.fd = fs.openSync(file, 'wx', 0o600)
    this.member = member
    this.sequence = 0
    this.held = new Map()
  }

  /**
   * @param {object} record delivered record plus topic and partition
   * @returns {number} original invocation id, unique even on redelivery
   * @throws {Error} on invalid identity, a closed journal or failed durable write
   */
  start(record) {
    const identity = shareRecordIdentity(record)
    const invocation = ++this.sequence
    this.append({ event: 'start', invocation, identity, deliveryCount: record.deliveryCount })
    this.held.set(invocation, identity)
    return invocation
  }

  /**
   * @param {number} invocation the exact start to complete
   * @returns {void}
   * @throws {Error} if unknown/already complete, closed or the write fails
   */
  finish(invocation) {
    if (!this.held.has(invocation)) throw new Error('Unknown handler invocation')
    this.append({ event: 'finish', invocation })
    this.held.delete(invocation)
  }

  /**
   * @param {object} row immutable journal fact, never an acknowledgement success claim
   * @returns {void}
   * @throws {Error} on a closed journal or filesystem failure
   */
  append(row) {
    if (this.fd === null) throw new Error('Handler journal is closed')
    fs.writeSync(this.fd, `${JSON.stringify({ member: this.member, ...row })}\n`)
    fs.fsyncSync(this.fd)
  }

  /**
   * @returns {void} closes the descriptor at most once
   * @throws {Error} on filesystem failure
   */
  close() {
    if (this.fd === null) return
    fs.closeSync(this.fd)
    this.fd = null
  }

  /**
   * Replays journal facts without inferring completion from handler return or broker state.
   * @param {string} file this invocation's child journal
   * @returns {{ rows: object[], held: string[] }} held identities with multiplicity
   * @throws {Error} on malformed, truncated, out-of-order or duplicate-completion input
   */
  static read(file) {
    const text = fs.readFileSync(file, 'utf8')
    if (text && !text.endsWith('\n')) throw new Error('Incomplete handler journal')
    const rows = text
      .split('\n')
      .filter(Boolean)
      .map((row) => JSON.parse(row))
    const held = new Map()
    let sequence = 0
    const member = rows[0]?.member
    for (const row of rows) {
      if (
        !row ||
        typeof member !== 'string' ||
        !member ||
        row.member !== member ||
        !Number.isSafeInteger(row.invocation) ||
        row.invocation < 1
      ) {
        throw new Error('Invalid handler journal order or fact')
      }
      if (row.event === 'start' && row.invocation === sequence + 1) {
        const tuple = JSON.parse(row.identity)
        if (
          !Array.isArray(tuple) ||
          tuple.length !== 5 ||
          !tuple.slice(3).every((bytes) => bytes === null || /^(?:[0-9a-f]{2})*$/.test(bytes)) ||
          !Number.isSafeInteger(row.deliveryCount) ||
          row.deliveryCount < 1 ||
          shareRecordIdentity({
            topic: tuple[0],
            partition: tuple[1],
            offset: tuple[2],
            key: tuple[3] === null ? null : Buffer.from(tuple[3], 'hex'),
            value: tuple[4] === null ? null : Buffer.from(tuple[4], 'hex'),
          }) !== row.identity
        ) {
          throw new Error('Invalid handler journal identity or delivery count')
        }
        sequence = row.invocation
        held.set(row.invocation, row.identity)
      } else if (row.event === 'finish' && held.has(row.invocation)) {
        held.delete(row.invocation)
      } else throw new Error('Invalid handler journal order')
    }
    return { rows, held: [...held.values()] }
  }
}

/**
 * Runs a fresh member in an existing group, starting its observation window only after assignment.
 * It does not set configs, reset offsets, produce data or invent acquisition state. Every await
 * is bounded by the inherited harness. Abort/expiry or failed cleanup taints that spec's run;
 * work ignoring abort may finish later and is contained by the sole guard.
 * @param {string} group existing group id
 * @param {string} topic topic to observe
 * @param {object} [options] injected Kafka and enclosing signal for live callers
 * @param {object} [options.kafka] defaults to the repository's plaintext Kafka configuration
 * @param {AbortSignal} [options.signal] enclosing case scope
 * @param {number} [options.windowMs=5000] post-join observation window
 * @param {Function} [options.record] Verifier sub-step observations on the monotonic clock.
 * @param {Function} [options.onClient] Registers the original client with its enclosing owner.
 * @returns {Promise<object[]>} actual delivered identities and delivery counts
 * @throws {Error} on join, crash, bound or cleanup failure
 */
const verifierMember = async (
  group,
  topic,
  { kafka, signal, windowMs = 5000, record, onClient } = {}
) => {
  const { NEVER, bounded, delay, pollUntil } = require('../src/producer/__tests__/fixtures/bounded')
  const { CleanupStack } = require('../src/producer/__tests__/fixtures/liveProbe')
  const { Kafka, logLevel } = require('../index')
  const owner = signal || NEVER
  const client = (
    kafka || new Kafka({ brokers: helpers.plainTextBrokers(), logLevel: logLevel.NOTHING })
  ).shareConsumer({
    groupId: group,
    maxRecords: 100,
    maxWaitTimeInMs: 250,
    acknowledgementMode: 'implicit',
  })
  onClient?.(client)
  const startedAt = performance.now()
  const note = (step, fields = {}) =>
    record?.({
      status: 'progress',
      groupId: group,
      topic,
      step,
      elapsedMs: performance.now() - startedAt,
      ...fields,
    })
  const cleanup = new CleanupStack()
  let stopping
  const abort = () => {
    note('abort')
    stopping = client.disconnect({ timeout: 0 })
    stopping.catch(() => {})
  }
  owner.addEventListener('abort', abort, { once: true })
  const removers = [() => owner.removeEventListener('abort', abort)]
  const delivered = []
  const crashes = []
  const remove = client.on(client.events.CRASH, ({ payload }) => {
    crashes.push(payload.error)
  })
  removers.push(remove)
  for (const [event, step] of [
    [client.events.GROUP_JOIN, 'joined'],
    [client.events.ASSIGNMENT_CHANGED, 'assignment'],
  ]) {
    if (!event) continue
    const removeEvent = client.on(event, ({ payload }) => note(step, { payload }))
    removers.push(removeEvent)
  }
  const fetches = []
  if (client.events.REQUEST) {
    const removeRequests = client.on(client.events.REQUEST, ({ payload }) => {
      if (payload.apiName === 'ShareFetch') {
        if (fetches.length === 0) note('first fetch', { payload })
        fetches.push(payload)
      }
    })
    removers.push(removeRequests)
  }
  cleanup.add('disconnect verifier', async () => {
    note('disconnect start')
    try {
      await client.disconnect({ timeout: owner.aborted ? 0 : 2000 })
      await stopping
      note('disconnected')
    } finally {
      for (const remove of removers.reverse()) remove()
    }
  })
  return cleanup.run(
    async () => {
      note('connect start')
      await bounded(owner, 10000, () => client.connect())
      note('connected')
      await bounded(owner, 10000, () => client.subscribe({ topics: [topic] }))
      note('join start')
      await bounded(owner, 10000, async (scope) => {
        await client.run({
          eachMessage: async ({ topic: name, partition, message }) => {
            delivered.push({
              identity: shareRecordIdentity({ ...message, topic: name, partition }),
              deliveryCount: message.deliveryCount,
            })
          },
        })
        note('run resolved')
        await pollUntil(
          scope,
          Infinity,
          () => {
            if (crashes.length) throw crashes[0]
            return (
              delivered.length > 0 ||
              (client
                .assignment?.()
                .some((entry) => entry.topic === topic && entry.partitions.length > 0) &&
                (!client.events.REQUEST || fetches.length > 0))
            )
          },
          { label: 'The verifier assignment' }
        )
      })
      const windowStartedAt = Date.now()
      note('window start', { assignment: client.assignment?.(), fetches: fetches.length, windowMs })
      // Timers run on a cached millisecond loop clock and can wake just before a monotonic
      // measurement reaches windowMs; top up until the full window has really elapsed.
      const windowClock = performance.now()
      await delay(owner, windowMs)
      for (let left = windowMs - (performance.now() - windowClock); left > 0;) {
        await delay(owner, Math.ceil(left))
        left = windowMs - (performance.now() - windowClock)
      }
      note('window end', { assignment: client.assignment?.(), fetches: fetches.length, windowMs })
      if (crashes.length) throw crashes[0]
      if (client.events.REQUEST && !fetches.some(({ sentAt }) => sentAt >= windowStartedAt)) {
        throw new Error('The assigned verifier completed no active fetch in its observation window')
      }
      if (
        delivered.length === 0 &&
        !client.assignment?.().some((entry) => entry.topic === topic && entry.partitions.length > 0)
      ) {
        throw new Error('The verifier lost its assignment during the observation window')
      }
      return delivered
    },
    { budgetMs: 3000 }
  )
}

/**
 * Samples after pending close callbacks and the sampling pause have released their resources.
 * Both baseline capture and cleanup call this under one proof deadline.
 * @param {{ snapshot: Function }} probe original owners' resource probe
 * @param {AbortSignal} signal enclosing proof scope
 * @returns {Promise<object>} measured resources, without supplying missing sources
 * @throws {Error} on abort or a failed measurement
 */
const sampleShareCleanup = async (probe, signal) => {
  const { bounded, delay } = require('../src/producer/__tests__/fixtures/bounded')
  await delay(signal, 0)
  return bounded(signal, Infinity, () => probe.snapshot())
}

/**
 * Validates the required measurements and selects the values that cleanup must restore exactly.
 * Cumulative socket totals can grow; the number of open sockets must return to its baseline.
 * @param {object} snapshot Original probe measurement.
 * @returns {object} Comparable resource counts without inferred defaults.
 * @throws {Error} When any required measurement is missing or invalid.
 */
const shareCleanupResources = (snapshot) => {
  for (const key of ['listeners', 'sockets', 'queueDepth', 'ledgerSize', 'activeResources']) {
    if (snapshot?.[key] === undefined) throw new Error(`Missing resource measurement: ${key}`)
  }
  const count = (value) => Number.isSafeInteger(value) && value >= 0
  const counts = (value) =>
    value !== null &&
    typeof value === 'object' &&
    !Array.isArray(value) &&
    Object.values(value).every(count)
  if (
    !counts(snapshot.listeners) ||
    !counts(snapshot.activeResources) ||
    !count(snapshot.queueDepth) ||
    !count(snapshot.ledgerSize) ||
    !['created', 'destroyed', 'open'].every((key) => count(snapshot.sockets?.[key])) ||
    snapshot.sockets.created - snapshot.sockets.destroyed !== snapshot.sockets.open
  ) {
    throw new Error('Invalid resource measurement')
  }
  return {
    listeners: snapshot.listeners,
    sockets: snapshot.sockets.open,
    queueDepth: snapshot.queueDepth,
    ledgerSize: snapshot.ledgerSize,
    activeResources: snapshot.activeResources,
  }
}

/**
 * Captures three equal samples, 50 ms apart, with no foreign network or child-process resources.
 * Uses the same deadline and sampling path as cleanup; it never rebases the after comparison.
 * Topic-propagation connections belong to the fixture and are closed at both sampling points.
 * Call outside any workload-only bound, before connecting clients. The enclosing case deadline
 * must remain active through the cleanup proof.
 * @param {{ snapshot: Function }} probe original owners' resource probe
 * @param {object} [options]
 * @param {AbortSignal} [options.signal] enclosing case scope
 * @param {boolean} [options.cleanupStack=true] whether the final proof runs in a CleanupStack
 * @returns {Promise<object>} measured baseline with the final proof's harness timers
 * @throws {Error} on abort, failed fixture disconnect or measurement
 */
const captureShareCleanupBaseline = async (probe, { signal, cleanupStack = true } = {}) => {
  const { inspect, isDeepStrictEqual } = require('util')
  const { NEVER, bounded, pollUntil } = require('../src/producer/__tests__/fixtures/bounded')
  const { CleanupStack } = require('../src/producer/__tests__/fixtures/liveProbe')
  const owner = signal || NEVER
  let baseline
  const capture = (scope) =>
    bounded(AbortSignal.any([owner, scope]), 10000, async (inner) => {
      await bounded(inner, Infinity, () => helpers.disconnectPropagationBrokers())
      let previous
      let stable = 0
      await pollUntil(
        inner,
        Infinity,
        async (sampleScope) => {
          baseline = await sampleShareCleanup(probe, sampleScope)
          const resources = shareCleanupResources(baseline)
          const active = resources.activeResources
          const foreign =
            (active.TCPSocketWrap ?? 0) > resources.sockets ||
            ['TCPServerWrap', 'TCPConnectWrap', 'ShutdownWrap', 'ProcessWrap'].some(
              (type) => (active[type] ?? 0) > 0
            )
          stable = !foreign && isDeepStrictEqual(resources, previous) ? stable + 1 : 1
          previous = resources
          return !foreign && stable >= 3
        },
        { delayMs: 50, label: 'Share resource baseline becoming quiet' }
      )
      return baseline
    }).catch((error) => {
      // Keep the last raw inventory (including timer owners) when no quiet baseline was possible.
      // CleanupStack copies this message into its named error, so CI's aggregate exposes it too.
      const failure = error instanceof Error ? error : new Error(String(error))
      failure.resourceBaseline = baseline
      failure.message += `; resource baseline ${inspect(
        { lastSample: baseline },
        { depth: null, breakLength: Infinity, sorted: true }
      )}`
      failure.message += require('./socketOwners').describeSocketOwners()
      throw failure
    })
  if (!cleanupStack) return capture(owner)
  const cleanup = new CleanupStack()
  cleanup.add('capture resource baseline', capture)
  await cleanup.run(async () => {}, { budgetMs: 15000 })
  return baseline
}

/**
 * Waits for every owned share group to be empty or absent before deletion.
 * @param {object} admin Connected admin client.
 * @param {string[]} groups Owned share group ids.
 * @param {{ signal?: AbortSignal, record?: Function }} [options] Existing cleanup scope and evidence sink.
 * @returns {Promise<void>} Once no owned group has members.
 * @throws {Error} On missing/refused descriptions, retained membership or expiry.
 */
const waitForShareGroupsEmpty = async (admin, groups, { signal, record } = {}) => {
  if (groups.length === 0) return
  const { NEVER, bounded, pollUntil } = require('../src/producer/__tests__/fixtures/bounded')
  await pollUntil(
    signal || NEVER,
    10000,
    async (scope) => {
      const described = await bounded(scope, Infinity, () =>
        admin.describeShareGroups(groups, { apiTimeout: 6000 })
      )
      let empty = true
      for (const groupId of groups) {
        const matches = described.groups.filter((group) => group.groupId === groupId)
        if (matches.length !== 1)
          throw new Error('Missing owned share group description during cleanup')
        const [group] = matches
        record?.('cleanup-members', { group })
        if (group.errorCode === 69) continue
        if (group.errorCode !== 0) {
          throw new Error(`Share group cleanup description failed: ${group.errorCode}`)
        }
        if (group.state !== 'Empty' || group.members.length !== 0) empty = false
      }
      return empty
    },
    { label: 'Every share member leaving' }
  )
}

/**
 * Proves owned groups absent and topics absent on every advertised broker within the caller's bound.
 * Group listings aggregate all brokers; topic listings are attributed through original request events.
 * @param {object} admin Connected admin client.
 * @param {string[]} groups Owned share group ids.
 * @param {string[]} topics Owned topic names.
 * @param {{ signal: AbortSignal, unrelated: string[], record?: Function }} options Existing cleanup scope and evidence sink.
 * @param {number} [options.delayMs=250] Pause between broker absence observations.
 * @returns {Promise<void>} Once all absence and unrelated-presence checks hold.
 * @throws {Error} On missing evidence, a harmed sibling, retained fixtures or expiry.
 */
const waitForShareFixtureAbsence = async (
  admin,
  groups,
  topics,
  { signal, unrelated, record, delayMs = 250 }
) => {
  const { bounded, pollUntil } = require('../src/producer/__tests__/fixtures/bounded')
  await waitForShareGroupsEmpty(admin, groups, { signal, record })
  const pending = new Set()
  if (topics.length > 0) {
    const { brokers } = await bounded(signal, Infinity, () => admin.describeCluster())
    for (const { host, port } of brokers) pending.add(`${host}:${port}`)
    if (pending.size === 0) throw new Error('No brokers available to prove topic deletion')
  }
  let request
  const remove = topics.length
    ? admin.on(admin.events.REQUEST, ({ payload }) => {
        if (payload.apiName === 'Metadata') request = payload
      })
    : () => {}
  try {
    await pollUntil(
      signal,
      Infinity,
      async (scope) => {
        const { groups: listed } = await bounded(scope, Infinity, () => admin.listGroups())
        const names = new Set(listed.map(({ groupId }) => groupId))
        if (unrelated.some((id) => !names.has(id))) throw new Error('Unrelated group disappeared')
        const remaining = groups.filter((id) => names.has(id))
        record?.('group-deletion', { remaining, listed, unrelated })
        if (topics.length > 0) {
          request = undefined
          const listedTopics = await bounded(scope, Infinity, () => admin.listTopics())
          if (!request) throw new Error('Missing topic listing request evidence')
          const present = topics.filter((name) => listedTopics.includes(name))
          if (present.length) pending.add(request.broker)
          else pending.delete(request.broker)
          record?.('topic-deletion', {
            topics,
            broker: request.broker,
            present,
            pending: [...pending],
          })
        }
        return remaining.length === 0 && pending.size === 0
      },
      { delayMs, label: 'Share fixtures disappearing on every broker' }
    )
  } finally {
    remove()
  }
}

/**
 * Proves owned groups/topics absent and measured owners back at baseline after CleanupStack.
 * Unrelated groups are checked for presence only, never whole-cluster list equality.
 * @param {object} admin still-connected admin used to prove fixture absence
 * @param {string[]} groups owned group ids
 * @param {string[]} topics owned topic names
 * @param {object} baseline client-free snapshot from `captureShareCleanupBaseline`
 * @param {{ snapshot: Function }} probe original owners' resource probe
 * @param {object} [options] unrelated group ids and case signal
 * @param {string[]} [options.unrelated=[]] previously observed unrelated groups
 * @param {AbortSignal} [options.signal] enclosing cleanup scope
 * @param {Function} [options.record] Original cleanup observations for the evidence sink.
 * @param {boolean} [options.disconnectAdmin=false] close admin and fixture propagation
 *   connections before measuring against a client-free baseline
 * @returns {Promise<object>} final measured snapshot
 * @throws {Error} on missing measurement, retained resource, leftover group/topic or harmed sibling
 */
const assertShareCleanup = async (
  admin,
  groups,
  topics,
  baseline,
  probe,
  { unrelated = [], signal, disconnectAdmin = false, record } = {}
) => {
  const { inspect, isDeepStrictEqual } = require('util')
  const { NEVER, bounded, pollUntil } = require('../src/producer/__tests__/fixtures/bounded')
  const keys = ['listeners', 'sockets', 'queueDepth', 'ledgerSize', 'activeResources']
  let after
  let previous
  try {
    return await bounded(signal || NEVER, 10000, async (scope) => {
      await waitForShareFixtureAbsence(admin, groups, topics, { signal: scope, unrelated, record })
      if (disconnectAdmin) {
        await bounded(scope, Infinity, () => admin.disconnect())
        await bounded(scope, Infinity, () => helpers.disconnectPropagationBrokers())
      }
      await pollUntil(
        scope,
        Infinity,
        async (sampleScope) => {
          after = await sampleShareCleanup(probe, sampleScope)
          const resources = shareCleanupResources(after)
          const restored = isDeepStrictEqual(resources, shareCleanupResources(baseline))
          if (!isDeepStrictEqual(resources, previous)) {
            record?.('cleanup-resources', { baseline, after, restored })
            previous = resources
          }
          if (!restored && !disconnectAdmin) {
            throw new Error('Share resources did not return to baseline')
          }
          return restored
        },
        { delayMs: 50, label: 'Share resources returning to baseline' }
      )
      return after
    })
  } catch (error) {
    const diff = Object.fromEntries(
      keys.flatMap((key) => {
        const beforeValue = key === 'sockets' ? baseline?.sockets?.open : baseline?.[key]
        const afterValue = key === 'sockets' ? after?.sockets?.open : after?.[key]
        return isDeepStrictEqual(beforeValue, afterValue)
          ? []
          : [[key, { baseline: beforeValue, lastAfter: afterValue }]]
      })
    )
    const failure = error instanceof Error ? error : new Error(String(error))
    failure.message += `; resource comparison ${inspect(
      { baseline, lastAfter: after, diff },
      { depth: null, breakLength: Infinity, sorted: true }
    )}`
    throw failure
  }
}

module.exports = {
  SHARE_TOPIC_PREFIX,
  shareTopicName,
  createShareTopic,
  setShareGroupConfigs,
  resetShareGroupConfigs,
  shareRecordIdentity,
  ShareHandlerLog,
  verifierMember,
  captureShareCleanupBaseline,
  waitForShareGroupsEmpty,
  waitForShareFixtureAbsence,
  assertShareCleanup,
}
