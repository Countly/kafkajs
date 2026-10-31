const { Kafka, logLevel } = require('../../../index')
const {
  createCluster,
  plainTextBrokers,
  reassignPreferredReplica,
  secureRandom,
  triggerPreferredElection,
  waitForReassignmentToComplete,
  writeEvidence,
} = require('testHelpers')
const { BrokerBouncer, NODES, waitForFullIsr } = require('../../../testHelpers/leaderElection')
const { readLog } = require('../../../testHelpers/rawLog')
const apiKeys = require('../../protocol/requests/apiKeys')
const Decoder = require('../../protocol/decoder')
const { requests } = require('../../protocol/requests')
const { responseHeaderVersion, RESPONSE_HEADER_V1 } = require('../../protocol/headers')
const {
  CleanupStack,
  COORDINATOR_TYPES,
  coordinatorOf,
  describeError,
} = require('../../producer/__tests__/fixtures/liveProbe')
const {
  NEVER,
  STOP_GRACE_MS,
  bounded,
  caseTimeout,
  delay,
  liveCase,
  pollUntil,
} = require('../../producer/__tests__/fixtures/bounded')
const { WireTranscript } = require('./fixtures/rackWire')
const { MOVE_WAIT_MS, ROUND_ALLOWANCE_MS, moveProblems } = require('./fixtures/fetchRoutingOracle')

// Gated: it stops a broker and moves leaders. Run alone through the guarded live entry:
// `npm run test:e2e:fetch-routing` is the developer row.
const describeRouting = process.env.FETCH_ROUTING_E2E === '1' ? describe : describe.skip

const EVIDENCE = 'fetch-routing'
const PREFIX = 'e2e-p9c-fetch-routing'
const CASE_BUDGET_MS = 120000
const STEP_MS = 30000
const CLEANUP_BUDGET_MS = 25000
const DELIVERY_BUDGET_MS = 30000
const POLL_MS = 100
const QUIET_MS = 1000
const RECORDS = 3
/** Case one: the idle long poll, ms. */
const SIBLING_WAIT_MS = 500
/** How long the broker of the unhinted moves stays down at least, ms. */
const OUTAGE_HOLD_MS = 15000

const FETCH = apiKeys.Fetch
const METADATA = apiKeys.Metadata

/**
 * @param {{ topic: string, partition: number, value: string }} record
 * @returns {string} the identity of a record: where it is and what it says
 */
const identity = ({ topic, partition, value }) => `${topic}|${partition}|${value}`

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
 * Deletes topics as a cleanup. Only a confirmed missing topic is success.
 * @param {Kafka} kafka
 * @param {string[]} topics
 * @param {AbortSignal} signal the cleanup's own
 * @returns {Promise<void>}
 * @throws {Error} a deletion failure other than a missing topic
 */
const deleteTopics = async (kafka, topics, signal) => {
  const admin = kafka.admin()
  try {
    await bounded(signal, STEP_MS, () => admin.connect(), { label: 'The cleanup admin connect' })
    await bounded(signal, STEP_MS, () => admin.deleteTopics({ topics, timeout: 10000 }), {
      label: 'The topic deletion',
    })
  } catch (error) {
    if (error.type !== 'UNKNOWN_TOPIC_OR_PARTITION') throw error
  } finally {
    await bounded(NEVER, STOP_GRACE_MS, () => admin.disconnect(), {
      label: 'The cleanup admin disconnect',
    })
  }
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
 * @param {number} coordinatorId the node that coordinates the case's group
 * @returns {{ old: number, first: number, second: number }} a node that is not the coordinator and
 *   the two others; the first is the idle node of a move, the second the busy one
 */
const rolesFor = (coordinatorId) => {
  const ids = Object.values(NODES)
    .map(({ nodeId }) => nodeId)
    .sort((a, b) => a - b)
  const [old] = ids.filter((nodeId) => nodeId !== coordinatorId).reverse()
  const [first, second] = ids.filter((nodeId) => nodeId !== old)
  return { old, first, second }
}

/**
 * Records every frame a client writes and answers it reads, as `WireTranscript` does, and also the
 * Metadata answers and the time each socket closed, so a case can place the moment an old leader
 * was lost and the moment a new one became known.
 */
class RoutingWire extends WireTranscript {
  /** Starts with no closed socket. */
  constructor() {
    super()
    /** @type {Array<{ endpoint: string, at: number }>} when each socket closed */
    this.closes = []
  }

  /**
   * @param {object} socket a socket of the client
   * @param {{ host: string, port: number }} address
   * @returns {object} the socket, also noting when it closes
   */
  observe(socket, address) {
    const observed = super.observe(socket, address)
    const endpoint = `${address.host}:${address.port}`
    observed.once('close', () => this.closes.push({ endpoint, at: performance.now() }))
    return observed
  }

  /**
   * @param {number} connection
   * @param {Buffer} frame one complete answer frame
   * @returns {void}
   */
  read(connection, frame) {
    const exchange = this.open.get(`${connection}:${frame.readInt32BE(4)}`)
    super.read(connection, frame)
    if (exchange?.apiKey === METADATA && !exchange.answer) {
      exchange.answer = { at: performance.now(), frame }
    }
  }

  /**
   * Decodes the answers of one API, once each.
   * @param {number} apiKey `Fetch` or `Metadata`
   * @returns {Promise<Array<{ exchange: object, at: number, decoded: object }>>} oldest first
   */
  async answers(apiKey) {
    const kept = this.exchanges.filter((x) => x.apiKey === apiKey && x.answer)
    return Promise.all(
      kept.map(async (exchange) => {
        exchange.decoded ??= this.decode(exchange)
        return { exchange, at: exchange.answer.at, decoded: await exchange.decoded }
      })
    )
  }

  /**
   * @param {object} exchange an exchange whose answer was kept
   * @returns {Promise<object>} the answer's decoded body
   */
  async decode(exchange) {
    const { response } = requests[exchange.apiKey === FETCH ? 'Fetch' : 'Metadata'].protocol({
      version: exchange.apiVersion,
    })({
      maxWaitTime: 0,
      minBytes: 0,
      maxBytes: 0,
      topics: [],
      topicIds: new Map(),
      rackId: '',
      allowAutoTopicCreation: false,
    })
    const { frame } = exchange.answer
    const header = new Decoder(frame)
    header.readInt32()
    header.readInt32()
    if (responseHeaderVersion(exchange.apiKey, exchange.apiVersion) === RESPONSE_HEADER_V1) {
      header.readTaggedFields()
    }
    return response.decode(frame.subarray(header.offset))
  }
}

/**
 * One client group over a few single-partition topics on the live cluster: the topics with the
 * replicas asked for, one member, what its handlers were given, the commits it confirmed and the
 * wire it showed. Protocol work only: every wait goes through the case scope and every client is
 * registered for cleanup before the operation that can make it partially.
 */
class Episode {
  /**
   * @param {AbortSignal} scope the case scope
   * @param {CleanupStack} cleanup the case's cleanup
   * @param {string} id the case's id
   * @param {string} name names the episode in client ids
   */
  constructor(scope, cleanup, id, name) {
    this.scope = scope
    this.cleanup = cleanup
    this.id = id
    this.name = name
    this.brokers = plainTextBrokers()
    this.wire = new RoutingWire()
    /** @type {Array<{ topic: string, partition: number, offset: string, value: string, at: number }>} */
    this.handled = []
    /** @type {Array<{ topic: string, partition: number, offset: number, at: number }>} */
    this.commits = []
    /** @type {object[]} */
    this.joins = []
    /** @type {object[]} */
    this.rebalances = []
    /** @type {Error[]} */
    this.crashes = []
    /** @type {Array<{ topic: string, partition: number, value: string }>} */
    this.acknowledged = []
    /** @type {string} the step the episode reached, for the evidence of a failed case */
    this.stage = 'created'
    /** @type {Record<string, unknown>} what the case selected and measured */
    this.facts = {}
    /** @type {Map<string, number>|null} the offset the broker holds for each topic at `close` */
    this.committed = null
    this.window = null
  }

  /**
   * Creates the topics with the replicas asked for, one partition each, and learns the cluster's
   * nodes and the topic ids.
   * @param {Array<{ topic: string, replicas: number[] }>} topics
   * @returns {Promise<void>}
   */
  async open(topics) {
    const { scope, cleanup, id, name } = this
    this.stage = 'opening'
    this.topics = topics.map(({ topic }) => topic)
    this.kafka = new Kafka({
      clientId: `${PREFIX}-${name}-${id}`,
      brokers: this.brokers,
      logLevel: logLevel.NOTHING,
    })
    const admin = this.kafka.admin()
    cleanup.add('admin', () => admin.disconnect())
    await step(scope, 'The admin connect', () => admin.connect())
    this.admin = admin
    const producer = this.kafka.producer()
    cleanup.add('producer', () => producer.disconnect())
    await step(scope, 'The producer connect', () => producer.connect())
    this.producer = producer
    cleanup.add('topics', (signal) => deleteTopics(this.kafka, this.topics, signal))
    await step(scope, 'The topic creation', () =>
      admin.createTopics({
        waitForLeaders: true,
        timeout: 20000,
        topics: topics.map(({ topic, replicas }) => ({
          topic,
          replicaAssignment: [{ partition: 0, replicas }],
        })),
      })
    )
    await step(scope, 'The full in-sync replica sets', () => waitForFullIsr(admin, this.topics))
    const { brokers } = await step(scope, 'The cluster read', () => admin.describeCluster())
    for (const { nodeId, host, port } of brokers) this.wire.nodes.set(`${host}:${port}`, nodeId)
    this.cluster = createCluster({}, this.brokers)
    cleanup.add('cluster', () => this.cluster.disconnect())
    await step(scope, 'The cluster connect', () => this.cluster.connect())
    await step(scope, 'The target topics', () => this.cluster.addMultipleTargetTopics(this.topics))
    await step(scope, 'The topic ids', () => this.cluster.refreshMetadata())
    this.ids = new Map(this.topics.map((topic) => [topic, this.cluster.topicId(topic)]))
  }

  /**
   * Starts one classic member over every topic, with autoCommit on, and waits until it owns every
   * partition and has been quiet for one period.
   * @param {object} options
   * @param {string} options.groupId
   * @param {number} options.maxWaitTimeInMs the consumer's Fetch wait
   * @returns {Promise<void>}
   */
  async startConsumer({ groupId, maxWaitTimeInMs }) {
    const { scope, cleanup, id, name } = this
    this.stage = 'starting the consumer'
    this.groupId = groupId
    const kafka = new Kafka({
      clientId: `${PREFIX}-${name}-member-${id}`,
      brokers: this.brokers,
      logLevel: logLevel.NOTHING,
      socketFactory: this.wire.socketFactory,
    })
    const consumer = kafka.consumer({
      groupId,
      groupProtocol: 'classic',
      maxWaitTimeInMs,
      heartbeatInterval: 1000,
      sessionTimeout: 30000,
    })
    this.consumer = consumer
    const off = [
      consumer.on(consumer.events.GROUP_JOIN, ({ payload }) => this.joins.push(payload)),
      consumer.on(consumer.events.REBALANCING, ({ payload }) => this.rebalances.push(payload)),
      consumer.on(consumer.events.CRASH, ({ payload }) => this.crashes.push(payload.error)),
      consumer.on(consumer.events.COMMIT_OFFSETS, ({ payload }) => {
        for (const { topic, partitions } of payload.topics) {
          for (const { partition, offset } of partitions) {
            this.commits.push({ topic, partition, offset: Number(offset), at: Date.now() })
          }
        }
      }),
    ]
    cleanup.add('consumer listeners', () => off.forEach((remove) => remove()))
    cleanup.add('consumer', () => consumer.disconnect())
    await step(scope, 'The consumer connect', () => consumer.connect())
    await step(scope, 'The consumer subscription', () =>
      consumer.subscribe({ topics: this.topics, fromBeginning: true })
    )
    await step(scope, 'The consumer start', () =>
      consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
          if (scope.aborted) return
          this.handled.push({
            topic,
            partition,
            offset: message.offset,
            value: message.value.toString(),
            at: Date.now(),
          })
        },
      })
    )
    const owned = () => {
      const assignment = this.joins.at(-1)?.memberAssignment ?? {}
      return this.topics.every((topic) => (assignment[topic] ?? []).includes(0))
    }
    await pollUntil(scope, STEP_MS, owned, { delayMs: POLL_MS, label: 'The full assignment' })
    const joined = this.joins.length
    await delay(scope, QUIET_MS)
    if (this.joins.length !== joined) throw new Error('The assignment changed while it settled')
  }

  /**
   * Sends `RECORDS` records to partition 0 of each topic with acks -1.
   * @param {string} label names the corpus, so no value repeats
   * @param {string[]} topics
   * @returns {Promise<Array<{ topic: string, partition: number, value: string }>>} the records the
   *   sends acknowledged, kept in `acknowledged`
   */
  async produce(label, topics) {
    this.stage = `sending ${label}`
    const records = []
    for (const topic of topics) {
      const messages = Array.from({ length: RECORDS }, (_, n) => ({
        partition: 0,
        value: `${label}-${topic}-${n}`,
      }))
      await step(this.scope, 'The send', () => this.producer.send({ topic, acks: -1, messages }))
      records.push(...messages.map(({ partition, value }) => ({ topic, partition, value })))
    }
    this.acknowledged.push(...records)
    return records
  }

  /**
   * Waits until every record has been given to a handler.
   * @param {Array<{ topic: string, partition: number, value: string }>} records
   * @returns {Promise<void>}
   * @throws {Error} when a record is not handled within the wait
   */
  async delivered(records) {
    this.stage = 'waiting for the deliveries'
    const have = () => new Set(this.handled.map(identity))
    await pollUntil(
      this.scope,
      DELIVERY_BUDGET_MS,
      () => records.every((r) => have().has(identity(r))),
      {
        delayMs: POLL_MS,
        label: 'The deliveries',
      }
    )
  }

  /**
   * Reads each topic's whole log straight from its leader and checks every record is in it.
   * @param {Array<{ topic: string, partition: number, value: string }>} records
   * @returns {Promise<void>}
   * @throws {Error} naming a record the log does not hold
   */
  async verifyRaw(records) {
    this.stage = 'reading the raw logs'
    for (const topic of new Set(records.map((r) => r.topic))) {
      const log = await step(this.scope, 'The raw log read', () =>
        readLog({ brokers: this.brokers, topic })
      )
      const values = new Set(
        [...log.values()].flatMap(({ records: stored }) => stored.map(({ value }) => String(value)))
      )
      for (const record of records.filter((r) => r.topic === topic)) {
        if (!values.has(record.value)) throw new Error(`The log of ${topic} lacks ${record.value}`)
      }
    }
  }

  /**
   * @param {string} topic
   * @returns {Promise<number>} the leader of its partition 0, -1 when it has none
   */
  async leaderOf(topic) {
    const { topics } = await step(this.scope, 'The topic metadata', () =>
      this.admin.fetchTopicMetadata({ topics: [topic] })
    )
    return topics[0].partitions[0].leader
  }

  /**
   * Starts the window in which no membership event may happen.
   * @returns {void}
   */
  windowStart() {
    this.window = {
      at: Date.now(),
      joins: this.joins.length,
      rebalances: this.rebalances.length,
      crashes: this.crashes.length,
    }
  }

  /**
   * @returns {{ joins: number, rebalances: number, crashes: number }} the membership events since
   *   `windowStart`
   */
  windowEvents() {
    return {
      joins: this.joins.length - this.window.joins,
      rebalances: this.rebalances.length - this.window.rebalances,
      crashes: this.crashes.length - this.window.crashes,
    }
  }

  /**
   * Stops the member and reads the offsets the broker holds for the group.
   * @returns {Promise<Map<string, number>>} the committed next offset of each topic, -1 for none
   */
  async close() {
    this.stage = 'closing'
    await step(this.scope, 'The consumer stop', () => this.consumer.disconnect())
    this.committed = new Map()
    for (const topic of this.topics) {
      const [{ partitions }] = await step(this.scope, 'The offset read', () =>
        this.admin.fetchOffsets({ groupId: this.groupId, topics: [topic] })
      )
      this.committed.set(topic, Number(partitions[0].offset))
    }
    this.stage = 'closed'
    return this.committed
  }

  /**
   * Waits for and measures one leader move of one topic's partition from the wire.
   * @param {object} move
   * @param {'idle'|'busy'} move.kind whether the new node has a long poll outstanding at the move
   * @param {string} move.topic
   * @param {number} move.oldNode
   * @param {number} move.newNode
   * @param {boolean} move.hinted whether the old leader names the new one
   * @param {number} move.since `performance.now()` when the move was issued
   * @returns {Promise<object>} the timestamps, the hint and the counts the bounds read
   */
  async measure({ kind, topic, oldNode, newNode, hinted, since }) {
    this.stage = `measuring the ${hinted ? 'hinted' : 'unhinted'} ${kind} move`
    const { wire, ids } = this
    const endpointOf = (nodeId) => [...wire.nodes].find(([, node]) => node === nodeId)[0]
    const oldEndpoint = endpointOf(oldNode)
    const newEndpoint = endpointOf(newNode)
    const topicId = ids.get(topic)
    const named = (entry) => (topicId ? entry.topicId === topicId : entry.topicName === topic)
    const asksFor = (exchange) => {
      if (exchange.fetchError) throw exchange.fetchError
      return Boolean(
        exchange.fetch?.topics.some(
          (t) =>
            (topicId ? t.topicId === topicId : t.topic === topic) &&
            t.partitions.some((p) => p.partition === 0)
        )
      )
    }
    const entriesOf = (answers) =>
      answers.flatMap(({ exchange, at, decoded }) =>
        decoded.responses
          .filter(named)
          .flatMap(({ partitions }) =>
            partitions.map((partition) => ({ exchange, at, ...partition }))
          )
      )
    let found = null
    await pollUntil(
      this.scope,
      STEP_MS,
      async () => {
        const fetched = entriesOf(await wire.answers(FETCH)).filter(({ at }) => at >= since)
        const hint = fetched.find(
          ({ exchange, errorCode, currentLeader }) =>
            exchange.endpoint === oldEndpoint &&
            errorCode !== 0 &&
            currentLeader?.leaderId === newNode
        )
        let errorAt
        let routeAt
        if (hinted) {
          if (!hint) return false
          errorAt = hint.at
          routeAt = hint.at
        } else {
          const lost = wire.closes.find(
            ({ endpoint, at }) => endpoint === oldEndpoint && at >= since
          )
          if (!lost) return false
          errorAt = lost.at
          const known = (await wire.answers(METADATA)).find(
            ({ at, decoded }) =>
              at >= errorAt &&
              decoded.topicMetadata
                .find((t) => t.topic === topic)
                ?.partitionMetadata.some((p) => p.partitionId === 0 && p.leader === newNode)
          )
          if (!known) return false
          routeAt = known.at
        }
        const next = wire.exchanges.find(
          (x) => x.apiKey === FETCH && x.endpoint === newEndpoint && x.at >= routeAt && asksFor(x)
        )
        if (!next) return false
        const prior = wire.exchanges.filter(
          (x) => x.apiKey === FETCH && x.endpoint === newEndpoint && x.at <= routeAt
        )
        const outstanding = prior.at(-1)
        found = {
          kind,
          hinted,
          topic,
          oldNode,
          newNode,
          errorAt,
          routeAt,
          nextAt: next.at,
          priorFetches: prior.length,
          pollWriteAt: outstanding?.at ?? null,
          pollDoneAt: outstanding?.answer?.at ?? null,
          hint: hint?.currentLeader ?? null,
          anyHint: fetched.some(({ currentLeader }) => currentLeader),
          refreshed: wire.exchanges.some(
            (x) => x.apiKey === METADATA && x.at >= errorAt && x.at <= next.at
          ),
        }
        return true
      },
      { delayMs: POLL_MS, label: `The ${kind} route to the new leader` }
    )
    return found
  }
}

/**
 * @param {Array<{ topic: string, partition: number, value: string }>} expected
 * @param {Array<{ topic: string, partition: number, offset: string, value: string, at: number }>} handled
 * @param {Array<{ topic: string, partition: number, offset: number, at: number }>} commits the
 *   commits the member confirmed
 * @returns {string[]} the records never handled, and every record handled again below a commit
 *   already confirmed when it was handled again
 */
const deliveryProblems = (expected, handled, commits) => {
  const problems = []
  const seen = new Set()
  for (const record of handled) {
    const key = identity(record)
    if (seen.has(key)) {
      const confirmed = Math.max(
        0,
        ...commits
          .filter(({ topic, at }) => topic === record.topic && at <= record.at)
          .map(({ offset }) => offset)
      )
      if (Number(record.offset) < confirmed) {
        problems.push(`${key} was replayed below the confirmed commit ${confirmed}`)
      }
    }
    seen.add(key)
  }
  for (const record of expected) {
    if (!seen.has(identity(record))) problems.push(`${identity(record)} was never handled`)
  }
  return problems
}

/**
 * @param {Array<{ topic: string, offset: string }>} handled
 * @param {Map<string, number>} committed the offset the broker holds for each topic
 * @returns {string[]} each topic whose committed offset passes what its handlers processed
 */
const commitProblems = (handled, committed) =>
  [...committed].flatMap(([topic, offset]) => {
    const processed = Math.max(
      -1,
      ...handled.filter((r) => r.topic === topic).map((r) => Number(r.offset))
    )
    return offset > processed + 1
      ? [`${topic} committed ${offset}, past ${processed + 1} processed`]
      : []
  })

/**
 * @param {object} evidence the sibling case's facts
 * @param {Array<{ topic: string, partition: number, value: string }>} evidence.outage the healthy
 *   records sent while the stopped broker was down
 * @param {object[]} evidence.handled
 * @param {{ tDown: number, tRestored: number }} evidence.window when the broker was down
 * @param {{ joins: number, rebalances: number, crashes: number }} evidence.events the membership
 *   events of the stable window
 * @returns {string[]} how the evidence breaks the case's oracle; empty when it meets it
 */
const siblingProblems = ({ outage, handled, window, events }) => {
  const problems = []
  for (const record of outage) {
    const first = handled.find((h) => identity(h) === identity(record))
    if (!first || first.at < window.tDown || first.at > window.tRestored) {
      problems.push(`${identity(record)} was not handled while the sibling had no leader`)
    }
  }
  for (const [name, count] of Object.entries(events)) {
    if (count !== 0) problems.push(`${count} ${name} in the stable window`)
  }
  return problems
}

/**
 * Takes the episode's old leader's broker down and, under the bouncer's one-down rule, brings it
 * back. The restore is also a cleanup, so a case that ends early still waits for it.
 * @param {BrokerBouncer} bouncer
 * @param {Episode} episode
 * @param {string} service the compose service to stop
 * @returns {Promise<{ tDown: number, tRestored: number, outageMs: number }>} the outage, once the
 *   broker answers again and the in-sync replica sets are full
 */
const startOutage = (bouncer, episode, service) => {
  const outage = bouncer.step(
    { service, kind: 'stop', holdMs: OUTAGE_HOLD_MS },
    { admin: episode.admin, topics: episode.topics, signal: episode.scope }
  )
  outage.catch(() => {})
  episode.cleanup.add('stopped broker restore', () => outage.then(() => undefined))
  return outage
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
 * Runs a case body and then, however it ended, writes the evidence row of each episode, so the
 * rows exist before any cleanup. A failed write never replaces the body's own failure; with none,
 * it is thrown, so a lost row is not read as a pass.
 * @param {string} name the case
 * @param {Episode[]} episodes filled as the body runs
 * @param {() => Promise<void>} body
 * @returns {Promise<void>}
 * @throws {Error} the body's failure, or the first failed evidence write
 */
const withRows = async (name, episodes, body) => {
  let failure = null
  let reporting = null
  try {
    await body()
  } catch (error) {
    failure = error
    throw error
  } finally {
    for (const episode of episodes) {
      try {
        writeEvidence(EVIDENCE, {
          scenario: EVIDENCE,
          case: name,
          episode: episode.name,
          groupId: episode.groupId,
          stage: episode.stage,
          facts: episode.facts,
          acknowledged: episode.acknowledged,
          handled: episode.handled,
          commits: episode.commits,
          committed: episode.committed ? Object.fromEntries(episode.committed) : null,
          events: {
            joins: episode.joins.length,
            rebalances: episode.rebalances.length,
            crashes: episode.crashes.length,
          },
          window: episode.window,
          caseError: failure ? describeError(failure) : null,
        })
      } catch (error) {
        reporting ??= error
      }
    }
  }
  if (reporting) throw reporting
}

describeRouting('Consumer > Fetch routing around unavailable and moved leaders (live)', () => {
  const bouncer = new BrokerBouncer()

  test(
    'healthy partitions progress while a sibling has no leader',
    liveCase(CASE_BUDGET_MS, (scope) =>
      withCase(scope, async ({ id, cleanup }) => {
        const episode = new Episode(scope, cleanup, id, 'sibling')
        await withRows('sibling', [episode], async () => {
          const groupId = `${PREFIX}-sibling-group-${id}`
          const coordinator = await coordinatorNode(scope, groupId)
          const { old: stopped, first: healthy } = rolesFor(coordinator)
          const healthyTopic = `${PREFIX}-sibling-healthy-${id}`
          const stoppedTopic = `${PREFIX}-sibling-stopped-${id}`
          const service = Object.keys(NODES).find((key) => NODES[key].nodeId === stopped)
          Object.assign(episode.facts, { coordinator, stopped, healthy, service })

          await episode.open([
            { topic: healthyTopic, replicas: [healthy] },
            { topic: stoppedTopic, replicas: [stopped] },
          ])
          await episode.startConsumer({ groupId, maxWaitTimeInMs: SIBLING_WAIT_MS })
          const { groups } = await step(scope, 'The group read', () =>
            episode.admin.describeGroups([groupId])
          )
          expect(groups[0].state).toBe('Stable')
          expect(await coordinatorNode(scope, groupId)).toBe(coordinator)

          const pre = await episode.produce('pre', [healthyTopic, stoppedTopic])
          await episode.delivered(pre)
          await episode.verifyRaw(pre)
          episode.windowStart()

          const outage = startOutage(bouncer, episode, service)
          await pollUntil(
            scope,
            STEP_MS,
            async () => (await episode.leaderOf(stoppedTopic)) === -1,
            {
              delayMs: POLL_MS,
              label: 'The partition without a leader',
            }
          )
          const during = await episode.produce('during', [healthyTopic])
          await episode.delivered(during)
          const down = await bounded(scope, Infinity, () => outage, { label: 'The outage' })
          episode.facts.outage = down

          await pollUntil(scope, STEP_MS, async () => (await episode.leaderOf(stoppedTopic)) >= 0, {
            delayMs: POLL_MS,
            label: 'The returned leader',
          })
          const post = await episode.produce('post', [stoppedTopic, healthyTopic])
          await episode.delivered(post)
          const events = episode.windowEvents()
          const everything = [...pre, ...during, ...post]
          await episode.verifyRaw(everything)
          const committed = await episode.close()

          const evidence = {
            outage: during,
            handled: episode.handled,
            window: { tDown: down.tDown, tRestored: down.tRestored },
            events,
          }
          episode.facts.problems = {
            sibling: siblingProblems(evidence),
            delivery: deliveryProblems(everything, episode.handled, episode.commits),
            commits: commitProblems(episode.handled, committed),
          }
          expect(episode.facts.problems).toEqual({ sibling: [], delivery: [], commits: [] })

          const victim = identity(during[0])
          const caughtUp = episode.handled
            .filter((record) => identity(record) !== victim)
            .concat({ ...during[0], offset: '0', at: down.tRestored + 1000 })
          expect(siblingProblems({ ...evidence, handled: caughtUp })).toEqual([
            `${victim} was not handled while the sibling had no leader`,
          ])
          expect(siblingProblems({ ...evidence, events: { ...events, rebalances: 1 } })).toEqual([
            '1 rebalances in the stable window',
          ])
        })
      })
    ),
    caseTimeout(CASE_BUDGET_MS)
  )

  test(
    'hinted and unhinted leader moves meet idle and busy bounds',
    liveCase(CASE_BUDGET_MS, (scope) =>
      withCase(scope, async ({ id, cleanup }) => {
        const episodes = []
        await withRows('leader-moves', episodes, async () => {
          const measured = []
          for (const hinted of [true, false]) {
            const name = hinted ? 'hinted' : 'unhinted'
            const episode = new Episode(scope, cleanup, id, name)
            episodes.push(episode)
            const groupId = `${PREFIX}-${name}-group-${id}`
            const roles = rolesFor(await coordinatorNode(scope, groupId))
            const topics = {
              idle: `${PREFIX}-${name}-idle-${id}`,
              busy: `${PREFIX}-${name}-busy-${id}`,
              poll: `${PREFIX}-${name}-poll-${id}`,
            }
            const service = Object.keys(NODES).find((key) => NODES[key].nodeId === roles.old)
            Object.assign(episode.facts, { hinted, roles, topics, service })

            await episode.open([
              { topic: topics.idle, replicas: [roles.old, roles.first] },
              { topic: topics.busy, replicas: [roles.old, roles.second] },
              { topic: topics.poll, replicas: [roles.second] },
            ])
            await episode.startConsumer({ groupId, maxWaitTimeInMs: MOVE_WAIT_MS })
            const pre = await episode.produce('pre', episode.topics)
            await episode.delivered(pre)
            episode.windowStart()

            const moves = [
              { kind: 'idle', topic: topics.idle, newNode: roles.first },
              { kind: 'busy', topic: topics.busy, newNode: roles.second },
            ]
            const since = performance.now()
            let outage = null
            if (hinted) {
              for (const { topic, newNode } of moves) {
                reassignPreferredReplica(topic, [roles.old, newNode], newNode)
                await step(scope, 'The reassignment', () => waitForReassignmentToComplete())
                triggerPreferredElection(topic)
              }
            } else {
              outage = startOutage(bouncer, episode, service)
            }
            for (const move of moves) {
              measured.push(await episode.measure({ ...move, oldNode: roles.old, hinted, since }))
            }
            episode.facts.moves = measured.filter((m) => m.hinted === hinted)
            if (outage) {
              episode.facts.outage = await bounded(scope, Infinity, () => outage, {
                label: 'The outage',
              })
            }

            const post = await episode.produce('post', [topics.idle, topics.busy])
            await episode.delivered(post)
            const events = episode.windowEvents()
            await episode.verifyRaw([...pre, ...post])
            const committed = await episode.close()
            episode.facts.problems = {
              moves: episode.facts.moves.flatMap(moveProblems),
              delivery: deliveryProblems([...pre, ...post], episode.handled, episode.commits),
              commits: commitProblems(episode.handled, committed),
              events,
            }
          }

          for (const { facts } of episodes) {
            expect(facts.problems).toEqual({
              moves: [],
              delivery: [],
              commits: [],
              events: { joins: 0, rebalances: 0, crashes: 0 },
            })
          }
          for (const move of measured) {
            const late = { ...move, nextAt: move.nextAt + MOVE_WAIT_MS + 2 * ROUND_ALLOWANCE_MS }
            expect(moveProblems(late)).not.toEqual([])
            if (move.hinted) expect(moveProblems({ ...move, hint: null })).not.toEqual([])
            else expect(moveProblems({ ...move, anyHint: true })).not.toEqual([])
          }
        })
      })
    ),
    caseTimeout(CASE_BUDGET_MS)
  )
})
