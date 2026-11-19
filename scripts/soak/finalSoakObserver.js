'use strict'
/**
 * Development-only passive weak observation for the final soak driver.
 *
 * Before any client exists, `PassiveObserver#install` wraps named methods of the product's internal
 * classes. Each wrapper remembers its receiver once through a `WeakRef`, attributes it to the client
 * whose async context created it, runs its hook, and calls the original with the same receiver and
 * arguments: it returns the original's exact result (the same promise for an async method) and lets
 * its error through unchanged. Hooks never throw into the product; a hook failure is counted. No
 * product getter, option, event or channel is added and no semantics change. State is weak or bounded
 * by live work (open tickets, live acquisitions, a fixed ring of recent wire exchanges), so nothing
 * grows with the run's history.
 *
 * `SocketRegistry` supplies each client's public `socketFactory`: it counts the sockets the client
 * creates and, for the share client and the raw reader, splits their bytes into request/response
 * frames for the archived wire evidence.
 */
const { acknowledgementResponse } = require('../e2e/telemetry/shareEvidence')

/** Share acknowledgement type codes on the wire, by name. */
const SHARE_TYPES = Object.freeze({ 1: 'accept', 2: 'release', 3: 'reject' })
/** Completed share exchanges kept per client for binding acquisitions to their raw frames. */
const RECENT_EXCHANGES = 32

/**
 * Normalizes a topic id to Kafka's printed form (22 base64url characters); the protocol decoder
 * renders ids as dashed hex.
 * @param {string|null|undefined} id
 * @returns {string|null}
 */
const normalizeTopicId = (id) => {
  if (typeof id !== 'string') return null
  if (/^[A-Za-z0-9_-]{22}$/.test(id)) return id
  const hex = id.replace(/-/g, '')
  return /^[0-9a-f]{32}$/i.test(hex) ? Buffer.from(hex, 'hex').toString('base64url') : null
}

/** @param {string} key `topic|partition` @returns {{ topic: string, partition: number }} */
const splitKey = (key) => {
  const at = String(key).lastIndexOf('|')
  return { topic: key.slice(0, at), partition: Number(key.slice(at + 1)) }
}

/** @param {Iterable<number>} values @returns {number[]} ascending */
const ascending = (values) => [...values].map(Number).sort((a, b) => a - b)

class PassiveObserver {
  /** The internal methods the driver watches, by class. */
  static WATCHED = Object.freeze({
    Connection: ['connect', 'disconnect'],
    RequestQueue: ['push', 'destroy'],
    ConsumerGroup: [
      'connect',
      'fetch',
      'applyRebalanceResult',
      'releaseRevoked',
      'retainSubscription',
      'growSubscription',
      'invalidateTopic',
      'pruneAssignmentState',
    ],
    Runner: ['processEachMessage', 'processEachBatch'],
    SubscriptionState: ['assign', 'pause', 'resume', 'retainTopics', 'retireTopic', 'clearPauses'],
    GroupOwnership: ['retain', 'lose', 'renew'],
    OffsetOperations: ['begin', 'end'],
    ResetObligations: ['owe', 'discharge', 'settle', 'retain', 'clear'],
    OffsetManager: ['recordCommitted'],
    Broker: ['offsetCommit'],
    ShareRun: ['syncNodes'],
    ShareFetchLoop: ['adopt', 'poll'],
    ShareAckLedger: ['prepare', 'settle', 'invalidate'],
    ClientTelemetry: ['install'],
  })

  /** @returns {Record<string, Function>} the real classes, loaded only when the driver needs them */
  static originals() {
    return {
      Connection: require('../../src/network/connection'),
      RequestQueue: require('../../src/network/requestQueue'),
      ConsumerGroup: require('../../src/consumer/consumerGroup'),
      Runner: require('../../src/consumer/runner'),
      SubscriptionState: require('../../src/consumer/subscriptionState'),
      GroupOwnership: require('../../src/consumer/groupOwnership'),
      OffsetOperations: require('../../src/consumer/offsetManager/offsetOperations'),
      ResetObligations: require('../../src/consumer/offsetManager/resetObligations'),
      OffsetManager: require('../../src/consumer/offsetManager'),
      Broker: require('../../src/broker'),
      ShareRun: require('../../src/consumer/share/shareRun').ShareRun,
      ShareFetchLoop: require('../../src/consumer/share/shareFetchLoop').ShareFetchLoop,
      ShareAckLedger: require('../../src/consumer/share/shareAckLedger').ShareAckLedger,
      ClientTelemetry: require('../../src/telemetry/client'),
    }
  }

  /**
   * @param {object} [options]
   * @param {Record<string, Function>} [options.owners] classes by name; the real ones by default
   * @param {Record<string, string[]>} [options.watched] methods to wrap per class
   * @param {import('node:async_hooks').AsyncLocalStorage} [options.context] the client id store
   * @param {import('node:async_hooks').AsyncLocalStorage} [options.commits] the driver's commit
   *   store, read by the write and publish hooks
   * @param {{ current: () => object|undefined }} [options.handlerContext] the product's batch
   *   identity store; the real one by default
   * @param {{ now: () => number }} [options.clock] the run clock
   * @param {(topic: string) => string|null} [options.topicIds] the driver's topic id table, used only
   *   where the product's own cluster has no id
   * @param {(event: object) => void} [options.onEvent] receives boundary, round, ticket, commit
   *   stage, subscription, share and telemetry events; its errors count as hook errors
   */
  constructor({
    owners = PassiveObserver.originals(),
    watched = PassiveObserver.WATCHED,
    context = null,
    commits = null,
    handlerContext = require('../../src/consumer/handlerContext'),
    clock = { now: () => 0 },
    topicIds = () => null,
    onEvent = () => {},
  } = {}) {
    Object.assign(this, { owners, watched, context, commits, handlerContext, clock, topicIds })
    this.onEvent = onEvent
    this.errors = 0
    this.failures = []
    this.absent = []
    this.refs = {}
    this.seen = new WeakSet()
    this.ids = new WeakMap()
    this.owner = new WeakMap()
    this.origins = new WeakMap()
    this.operations = new WeakMap()
    this.batches = new WeakMap()
    this.identities = new WeakMap()
    this.queueConnection = new WeakMap()
    this.ownershipGroup = new WeakMap()
    this.revoking = new WeakMap()
    this.liveTickets = new Map()
    this.acquisitions = new WeakMap()
    this.ledgers = new WeakMap()
    this.tickets = new WeakMap()
    this.exchanges = new Map()
    this.counters = { id: 1, round: 0, acquisition: 0, loss: 0 }
    this.unknownTopicIds = 0
    this.restores = []
  }

  /**
   * Wraps every watched method that exists. A missing class or method is a missing source, reported
   * by `missing()`, never an empty reading.
   * @returns {void}
   */
  install() {
    for (const [name, methods] of Object.entries(this.watched)) {
      const owner = this.owners[name]
      for (const method of methods) {
        const prototype = typeof owner === 'function' ? owner.prototype : owner
        if (typeof prototype?.[method] !== 'function') this.absent.push(`${name}#${method}`)
        else this.wrap(name, prototype, method)
      }
    }
  }

  /** @returns {string[]} watched sources the product does not have */
  missing() {
    return [...this.absent]
  }

  /**
   * Replaces one method with a transparent wrapper: remember the receiver, run the `before` hook,
   * call the original, run the `after` hook on its result (on the settled value of a promise), and
   * return the original result unchanged.
   * @param {string} name @param {object} prototype @param {string} method
   * @returns {void}
   */
  wrap(name, prototype, method) {
    const original = prototype[method]
    const observer = this
    const hooks = this.hooks()[`${name}#${method}`] ?? {}
    const label = `${name}#${method}`
    const wrapper = function (...args) {
      observer.guard(() => observer.remember(name, this), `${label} remember`)
      const state = hooks.before
        ? observer.guard(() => hooks.before(this, args), `${label} before`)
        : undefined
      const result = original.apply(this, args)
      if (hooks.after) {
        if (result && typeof result.then === 'function')
          result.then(
            (value) =>
              observer.guard(() => hooks.after(this, args, value, state), `${label} after`),
            () => observer.guard(() => hooks.failed?.(this, args, state), `${label} failed`)
          )
        else observer.guard(() => hooks.after(this, args, result, state), `${label} after`)
      }
      return result
    }
    prototype[method] = wrapper
    this.restores.push(() => {
      if (prototype[method] === wrapper) prototype[method] = original
    })
  }

  /**
   * Runs a hook so that a failure never reaches the product, and remembers it: the product call
   * goes on unchanged, but the evidence the hook was to record is incomplete.
   * @param {() => unknown} fn a hook @param {string} hook its identity
   * @returns {unknown} its value, or undefined after counting and naming its error
   */
  guard(fn, hook = 'hook') {
    try {
      return fn()
    } catch (e) {
      this.fail(`${hook}: ${e.message}`)
      return undefined
    }
  }

  /** @param {string} failure counts and keeps a failure, at most fifty until drained @returns {void} */
  fail(failure) {
    this.errors++
    if (this.failures.length < 50) this.failures.push(failure)
  }

  /** @returns {string[]} the hook failures since the last call, by hook identity */
  drainFailures() {
    return this.failures.splice(0)
  }

  /**
   * Drops everything kept for a client that has disconnected: its ring of share exchanges. What the
   * observer holds weakly (receivers, ids, origins) is not kept alive by it.
   * @param {string} client
   * @returns {void}
   */
  forget(client) {
    this.exchanges.delete(client)
  }

  /** @returns {void} puts every original method back */
  restore() {
    for (const undo of this.restores.splice(0).reverse()) undo()
  }

  /** @param {object} event @returns {void} */
  emit(event) {
    this.onEvent(event)
  }

  /** @param {string} name @param {object} receiver remembers it once, with its creating client */
  remember(name, receiver) {
    if (receiver === null || typeof receiver !== 'object' || this.seen.has(receiver)) return
    this.seen.add(receiver)
    this.idOf(receiver, name[0].toLowerCase())
    const client = this.context?.getStore()
    if (client) this.owner.set(receiver, client)
    ;(this.refs[name] ??= []).push(new WeakRef(receiver))
    if (name === 'ConsumerGroup' && receiver.ownership)
      this.ownershipGroup.set(receiver.ownership, receiver)
    if (name === 'Connection' && receiver.requestQueue) {
      this.queueConnection.set(receiver.requestQueue, receiver)
      this.remember('RequestQueue', receiver.requestQueue)
      if (client && !this.owner.has(receiver.requestQueue))
        this.owner.set(receiver.requestQueue, client)
    }
  }

  /** @param {object} receiver @param {string} [prefix] @returns {string} its weak id */
  idOf(receiver, prefix = 'o') {
    if (!this.ids.has(receiver)) this.ids.set(receiver, `${prefix}${this.counters.id++}`)
    return this.ids.get(receiver)
  }

  /** @param {object} receiver @returns {string|null} the client that created it */
  clientOf(receiver) {
    return this.owner.get(receiver) ?? null
  }

  /**
   * @param {string} name a watched class
   * @returns {object[]} its live discovered receivers; dead references are dropped
   */
  live(name) {
    const out = []
    this.refs[name] = (this.refs[name] ?? []).filter((ref) => {
      const value = ref.deref()
      if (value !== undefined) out.push(value)
      return value !== undefined
    })
    return out
  }

  /**
   * @param {unknown} id a topic id as the product stores it (dashed hex or printed)
   * @returns {string|null} the printed form; an id that cannot be represented is counted as unknown
   */
  printed(id) {
    const topicId = normalizeTopicId(String(id))
    if (!topicId) this.unknownTopicIds++
    return topicId
  }

  /** @param {string} client @param {string} name @returns {object[]} the client's live receivers */
  mine(client, name) {
    return this.live(name).filter((o) => this.clientOf(o) === client)
  }

  /**
   * The topic id of a name as the product's own cluster knows it, else the driver's table.
   * @param {string} topic @param {object} [group] the consumer group whose cluster answers
   * @returns {string|null}
   */
  topicIdOf(topic, group) {
    return normalizeTopicId(group?.cluster?.topicId?.(topic) ?? this.topicIds(topic))
  }

  /**
   * @param {string} topic @param {number} partition @param {object} [group]
   * @returns {{ topic: string, topicId: string, partition: number }|null} a Key, or null (counted)
   *   when no id is known; an id is never guessed
   */
  keyOf(topic, partition, group) {
    const topicId = this.topicIdOf(topic, group)
    if (!topicId) {
      this.unknownTopicIds++
      return null
    }
    return { topic, topicId, partition: Number(partition) }
  }

  /**
   * Whether a group's run is still its consumer's current run, by the product's own run identity.
   * A run ends at a stop, a disconnect or a crash, and a restart starts a new one; the consumer
   * then drops the old group, but a weak reference still answers until a collection clears it, so
   * reachability never decides liveness. A group without a run identity counts as current.
   * @param {object} group @returns {boolean}
   */
  isCurrent(group) {
    return group.runIdentity?.isCurrent?.() !== false
  }

  /**
   * The live group that uses `object` as its `field`, preferring the one whose run is current: a
   * consumer's ledger outlives its runs, so a restarted run and its not yet collected predecessor
   * share it. Without a current one (a run stopping) the stopping run's group answers.
   * @param {string} field @param {object} object
   * @returns {object|undefined}
   */
  groupUsing(field, object) {
    const groups = this.live('ConsumerGroup').filter((g) => g[field] === object)
    return groups.find((g) => this.isCurrent(g)) ?? groups[0]
  }

  /** @param {object} group @returns {string} the weak id of the group's current run identity */
  runOf(group) {
    const identity = group?.runIdentity
    const client = this.clientOf(group) ?? 'harness'
    return identity && typeof identity === 'object'
      ? `${client}.${this.idOf(identity, 'r')}`
      : `${client}.r0`
  }

  /** @param {object} group @returns {'eager'|'cooperative'|'consumer'} the negotiated protocol */
  protocolOf(group) {
    if (group.kind === 'consumer') return 'consumer'
    return group.rebalanceProtocol?.() === 'cooperative' ? 'cooperative' : 'eager'
  }

  /**
   * The group's authority over one key as it stands now: used for boundaries and assignment cuts,
   * never for a handler or a commit, which carry their batch's captured authority.
   * @param {object} group @param {string} key `topic|partition`
   * @returns {object} an Authority
   */
  currentAuthority(group, key) {
    const acquired = group.acquired?.get(key)
    return {
      client: this.clientOf(group) ?? 'harness',
      run: this.runOf(group),
      member: String(group.memberId || '-'),
      epoch: Number(group.generationId ?? -1),
      token: Number(group.ownership?.token?.() ?? group.ownership?.current ?? 0),
      assignmentEpoch: Number(group.assignmentEpoch ?? 0),
      acquiredAt: Number(acquired?.acquiredAt ?? 0),
      recreatedAt: Number(acquired?.recreatedAt ?? 0),
    }
  }

  /**
   * The authority a handler runs under: captured when its batch's fetch began (member, generation,
   * membership token and run) and stamped on the batch (assignment epoch and acquisition counters),
   * looked up through the product's own batch identity store. Never the group's current state.
   * @param {object} [identity] the handler's batch identity; the current one by default
   * @returns {object|null} an Authority, or null outside a captured batch
   */
  batchAuthority(identity = this.handlerContext.current()) {
    return (identity && this.identities.get(identity)) ?? null
  }

  /** @returns {Record<string, { before?: Function, after?: Function, failed?: Function }>} */
  hooks() {
    if (this.table) return this.table
    const self = this
    const boundary = (group, event, key, cause) => {
      const k = self.keyOf(splitKey(key).topic, splitKey(key).partition, group)
      if (!k) return
      self.emit({
        type: 'boundary',
        record: {
          client: self.clientOf(group) ?? 'harness',
          group: String(group.groupId),
          run: self.runOf(group),
          member: String(group.memberId || '-'),
          epoch: Number(group.generationId ?? -1),
          protocol: self.protocolOf(group),
          event,
          key: k,
          authority: self.currentAuthority(group, key),
          position: null,
          cause,
        },
      })
    }
    const acquiredSnapshot = (group) =>
      new Map([...(group.acquired ?? [])].map(([k, v]) => [k, v.acquiredAt]))
    this.table = {
      'Connection#connect': {
        after: (conn) => {
          if (conn.requestQueue) self.queueConnection.set(conn.requestQueue, conn)
        },
      },
      'ConsumerGroup#fetch': {
        before: (group) => ({
          client: self.clientOf(group) ?? 'harness',
          run: self.runOf(group),
          member: String(group.memberId || '-'),
          epoch: Number(group.generationId ?? -1),
          token: Number(group.ownership?.token?.() ?? 0),
          acquired: new Map(
            [...(group.acquired ?? [])].map(([k, v]) => [k, [v.acquiredAt, v.recreatedAt]])
          ),
        }),
        after: (group, args, batches, captured) => {
          for (const batch of Array.isArray(batches) ? batches : []) {
            const [acquiredAt, recreatedAt] = captured.acquired.get(
              batch.ownershipKey ?? `${batch.topic}|${batch.partition}`
            ) ?? [0, 0]
            self.batches.set(batch, {
              client: captured.client,
              run: captured.run,
              member: captured.member,
              epoch: captured.epoch,
              token: captured.token,
              assignmentEpoch: Number(batch.assignmentEpoch ?? 0),
              acquiredAt: Number(acquiredAt),
              recreatedAt: Number(recreatedAt),
            })
          }
        },
      },
      'Runner#processEachMessage': {
        before: (runner, [batch]) => self.bindIdentity(batch),
      },
      'Runner#processEachBatch': {
        before: (runner, [batch]) => self.bindIdentity(batch),
      },
      'ConsumerGroup#releaseRevoked': {
        before: (group, [revoked]) => {
          const begun = self.revoking.get(group) ?? new Set()
          self.revoking.set(group, begun)
          const cause = `${self.clientOf(group) ?? 'harness'}.v${++self.counters.round}`
          for (const { topic, partitions } of revoked ?? [])
            for (const partition of partitions) {
              begun.add(`${topic}|${partition}`)
              boundary(group, 'revoke-begin', `${topic}|${partition}`, cause)
            }
        },
      },
      'ConsumerGroup#applyRebalanceResult': {
        before: (group, args) => ({ before: acquiredSnapshot(group), round: args[1]?.round }),
        after: (group, args, value, state) => self.applied(group, state, true, boundary),
        failed: (group, args, state) => self.applied(group, state, false, boundary),
      },
      'GroupOwnership#lose': {
        before: (ownership, [, token]) => ownership.states?.get(token)?.cause ?? null,
        after: (ownership, [, token], value, had) => {
          const group = self.ownershipGroup.get(ownership)
          const now = ownership.states?.get(token)?.cause
          if (!group || had || !now || token !== ownership.current) return
          const cause = `${self.clientOf(group) ?? 'harness'}.l${++self.counters.loss}`
          for (const key of group.acquired?.keys() ?? []) boundary(group, 'lost', key, cause)
        },
      },
      'ConsumerGroup#retainSubscription': {
        after: (group) =>
          self.emit({ type: 'subscription', group, names: [...group.topicsSubscribed] }),
      },
      // A recreation keeps the names and changes an id: publish so the new incarnation's lifetime
      // exists before any pause state of it is stamped.
      'ConsumerGroup#invalidateTopic': {
        after: (group) =>
          self.emit({ type: 'subscription', group, names: [...group.topicsSubscribed] }),
      },
      'SubscriptionState#assign': { after: (state) => self.stamp(state, 'assign') },
      'SubscriptionState#pause': { after: (state) => self.stamp(state, 'pause') },
      'SubscriptionState#resume': { after: (state) => self.stamp(state, 'resume') },
      'SubscriptionState#retainTopics': { after: (state) => self.stamp(state, 'retain') },
      'SubscriptionState#retireTopic': { after: (state) => self.stamp(state, 'retire') },
      'SubscriptionState#clearPauses': { after: (state) => self.stamp(state, 'clear') },
      'OffsetOperations#begin': {
        after: (ledger, args, ticket) => self.ticketBegan(ledger, ticket),
      },
      'OffsetOperations#end': { before: (ledger, [ticket]) => self.ticketEnded(ledger, ticket) },
      'Broker#offsetCommit': {
        before: () => {
          const commit = self.commits?.getStore()
          if (commit) self.emit({ type: 'commit-stage', phase: 'write', commit })
        },
      },
      'OffsetManager#recordCommitted': {
        before: (manager, [ticket, topics]) => {
          const commit = self.commits?.getStore()
          if (!commit) return
          const written = []
          for (const { topic, partitions } of topics ?? [])
            for (const { partition, offset } of partitions)
              if (!manager.operations?.isSuperseded?.(ticket, topic, Number(partition)))
                written.push({ topic, partition: Number(partition), offset: String(offset) })
          self.emit({ type: 'commit-stage', phase: 'publish', commit, written })
        },
      },
      'ShareFetchLoop#adopt': {
        after: (loop, [exchange, topicId, partition, lockDuration], row) =>
          self.adopted(loop, row, { exchange, topicId, partition, lockDuration }),
      },
      'ShareAckLedger#prepare': {
        after: (ledger, args, value) => self.prepared(ledger, value?.ticket),
      },
      'ShareAckLedger#settle': {
        before: (ledger, [ticket, options]) => self.settling(ledger, ticket, options),
      },
      'ShareAckLedger#invalidate': {
        before: (ledger, [, preserve]) => self.tickets.get(preserve)?.offsets ?? null,
        after: (ledger, args, value, kept) => self.retired(ledger, kept),
      },
      'ClientTelemetry#install': {
        after: (telemetry) =>
          self.emit({
            type: 'telemetry-subscription',
            client: self.clientOf(telemetry),
            instanceId: String(telemetry.uuid),
            subscriptionId: String(telemetry.subscription?.subscriptionId),
          }),
      },
    }
    return this.table
  }

  /**
   * Binds the product's batch identity, created just before the handler loop, to the authority
   * captured for that batch.
   * @param {object} batch @returns {void}
   */
  bindIdentity(batch) {
    const identity = this.handlerContext.current()
    const captured = this.batches.get(batch)
    if (identity && captured) this.identities.set(identity, captured)
  }

  /**
   * Turns one applied rebalance result into keyed boundaries and, when it succeeded, one round.
   * @param {object} group @param {{ before: Map, round: object }} state @param {boolean} ok
   * @param {Function} boundary emits one boundary
   * @returns {void}
   */
  applied(group, state, ok, boundary) {
    const after = new Map([...(group.acquired ?? [])].map(([k, v]) => [k, v.acquiredAt]))
    const client = this.clientOf(group) ?? 'harness'
    const round = `${client}.${this.idOf(state.round ?? {}, 'n')}`
    const begun = this.revoking.get(group)
    const revoked = [...state.before.keys()].filter((k) => !after.has(k))
    const acquired = [...after.keys()].filter((k) => state.before.get(k) !== after.get(k))
    for (const key of revoked) {
      if (!begun?.has(key)) boundary(group, 'revoke-begin', key, round)
      begun?.delete(key)
      boundary(group, 'revoke-end', key, round)
    }
    for (const key of acquired) boundary(group, 'acquire', key, round)
    if (!ok) return
    const keys = (list) =>
      list.map((k) => this.keyOf(splitKey(k).topic, splitKey(k).partition, group)).filter(Boolean)
    this.emit({
      type: 'round',
      group,
      record: {
        client,
        group: String(group.groupId),
        run: this.runOf(group),
        round,
        protocol: this.protocolOf(group),
        epoch: Number(group.generationId ?? -1),
        assigned: keys([...after.keys()]),
        revoked: keys(revoked),
        causes: [],
      },
      acquired: keys(acquired),
    })
  }

  /** Gives each pause-state object that has just appeared its fixed origin. */
  stamp(receiver, method) {
    const n = (this.operations.get(receiver) ?? 0) + 1
    this.operations.set(receiver, n)
    const op = `${this.idOf(receiver, 's')}.${method}${n}`
    const states = receiver.subscriptionStatesByTopic ?? {}
    for (const topic of Object.keys(states)) {
      const state = states[topic]
      const origin = this.origins.get(state)
      if (origin) {
        if (method === 'pause' || method === 'resume') origin.lastOperation = op
        continue
      }
      const group = this.groupUsing('subscriptionState', receiver)
      const client = this.clientOf(group ?? receiver)
      this.origins.set(state, {
        stateId: this.idOf(state, 'p'),
        topicId: this.topicIdOf(topic, group),
        life: client && group ? this.lifeOf(client, group, topic) : null,
        client,
        run: group ? this.runOf(group) : null,
        lastOperation: op,
      })
    }
  }

  /**
   * The accepted lifetime a name is in: set by the runtime from accepted publications.
   * @param {string} client @param {object} group @param {string} topic
   * @returns {string|null}
   */
  lifeOf(client, group, topic) {
    return this.lives?.(client, topic, this.topicIdOf(topic, group)) ?? null
  }

  /**
   * Opens a ledger ticket: an id, the keys it covers with the incarnation its owning group knows at
   * this moment, and the owning run and token when a live group of this client uses the ledger.
   * @param {object} ledger @param {object} ticket @returns {void}
   */
  ticketBegan(ledger, ticket) {
    if (!ticket || typeof ticket !== 'object') return
    const group = this.groupUsing('operations', ledger)
    const id = this.idOf(ticket, 't')
    const keys = (ticket.cells ?? [])
      .map(({ topic, partition }) => this.keyOf(topic, partition, group))
      .filter(Boolean)
    const live = this.liveTickets.get(ledger) ?? new Map()
    this.liveTickets.set(ledger, live)
    live.set(ticket, id)
    this.emit({
      type: 'ticket',
      record: {
        ticket: id,
        ledger: this.idOf(ledger, 'l'),
        client: this.clientOf(group ?? ledger) ?? 'harness',
        ownerRun: group ? this.runOf(group) : null,
        phase: 'begin',
        keys,
        token: group ? Number(group.ownership?.token?.() ?? 0) : null,
      },
    })
  }

  /** Closes a ledger ticket once; the product's `end` is idempotent and so is this. */
  ticketEnded(ledger, ticket) {
    const live = this.liveTickets.get(ledger)
    const id = live?.get(ticket)
    if (!id || ticket.ended) return
    live.delete(ticket)
    if (live.size === 0) this.liveTickets.delete(ledger)
    const group = this.groupUsing('operations', ledger)
    this.emit({
      type: 'ticket',
      record: {
        ticket: id,
        ledger: this.idOf(ledger, 'l'),
        client: this.clientOf(group ?? ledger) ?? 'harness',
        ownerRun: group ? this.runOf(group) : null,
        phase: 'end',
        keys: (ticket.cells ?? [])
          .map(({ topic, partition }) => this.keyOf(topic, partition, group))
          .filter(Boolean),
        token: group ? Number(group.ownership?.token?.() ?? 0) : null,
      },
    })
  }

  /**
   * Registers one adopted share acquisition: its id, its held records and decided ranges as the
   * walk that built the real ledger classified them, and the acquired ranges of the raw response
   * that carried it.
   * @param {object} loop @param {object} row the loop's cohort row, holding the real ledger
   * @param {{ exchange: object, topicId: string, partition: object, lockDuration: number }} wire
   * @returns {void}
   */
  adopted(loop, row, { topicId, partition, lockDuration }) {
    if (!row?.ledger) return
    const client = this.clientOf(loop) ?? 'harness'
    const acquisition = `${client}.a${++this.counters.acquisition}`
    const key = {
      topic: row.topic,
      topicId: normalizeTopicId(topicId),
      partition: Number(row.partition),
    }
    if (!key.topicId) {
      this.unknownTopicIds++
      return
    }
    const entries = new Map()
    const deliveries = new Map()
    const { records = [], deliveryCounts = [] } = row.walk?.deliver ?? {}
    records.forEach((record, i) => {
      const offset = String(record.offset)
      entries.set(offset, { state: 'held', automatic: false, first: offset, last: offset })
      deliveries.set(offset, deliveryCounts[i])
    })
    for (const name of ['gaps', 'release', 'reject'])
      for (const range of row.walk?.[name] ?? [])
        entries.set(String(range.firstOffset), {
          state: 'held',
          automatic: true,
          first: String(range.firstOffset),
          last: String(range.lastOffset),
        })
    this.ledgers.set(row.ledger, { acquisition, client, key, entries })
    this.acquisitions.set(row, { acquisition, deliveries })
    const wire = this.wireFor(client, 78, (decoded) =>
      decoded.topics.some(
        (t) =>
          normalizeTopicId(t.topicId) === key.topicId &&
          t.partitions.some((p) => p.partitionIndex === key.partition && p.acquired.length > 0)
      )
    )
    const acquired = (partition.acquiredRecords ?? []).map((r) => ({
      from: String(r.firstOffset),
      to: String(r.lastOffset),
      deliveryCount: Number(r.deliveryCount),
    }))
    this.emit({
      type: 'share-acquire',
      client,
      acquisition,
      key,
      acquired,
      lockMs: Number(lockDuration),
      receivedAt: this.clock.now(),
      wire,
    })
  }

  /** Marks the records a prepared acknowledgement ticket carries as settling. */
  prepared(ledger, ticket) {
    const mirror = this.ledgers.get(ledger)
    if (!mirror || !ticket) return
    const offsets = []
    for (const batch of ticket.acknowledgementBatches ?? []) {
      const first = BigInt(batch.firstOffset)
      const last = BigInt(batch.lastOffset)
      for (const [offset, entry] of mirror.entries) {
        const at = BigInt(offset)
        if (at < first || at > last) continue
        const type =
          batch.acknowledgeTypes[batch.acknowledgeTypes.length === 1 ? 0 : Number(at - first)]
        if (!ticket.isRenewAck) entry.state = 'settling'
        offsets.push({ offset, automatic: entry.automatic, type: SHARE_TYPES[type] ?? null })
      }
    }
    this.tickets.set(ticket, { offsets, renew: ticket.isRenewAck === true })
  }

  /**
   * Settles a ticket's records: the request and response facts for every delivered record it
   * carried, bound to the raw acknowledgement exchange, and their removal from the held state.
   */
  settling(ledger, ticket, options = {}) {
    const mirror = this.ledgers.get(ledger)
    const sent = this.tickets.get(ticket)
    if (!mirror || !sent) return
    this.tickets.delete(ticket)
    if (sent.renew) return
    const match = (decoded) =>
      decoded.topics.some(
        (t) =>
          normalizeTopicId(t.topicId) === mirror.key.topicId &&
          t.partitions.some((p) => p.partitionIndex === mirror.key.partition)
      )
    const wire = this.wireFor(mirror.client, null, match)
    const partition = wire?.decoded.topics
      .find((t) => normalizeTopicId(t.topicId) === mirror.key.topicId)
      ?.partitions.find((p) => p.partitionIndex === mirror.key.partition)
    for (const { offset } of sent.offsets) mirror.entries.delete(offset)
    this.emit({
      type: 'share-settle',
      client: mirror.client,
      acquisition: mirror.acquisition,
      key: mirror.key,
      offsets: sent.offsets.filter((o) => !o.automatic && o.type),
      result: options.result ?? 'ok',
      errorCode: partition ? partition.acknowledgeErrorCode : null,
      wire,
    })
  }

  /** Drops every retained entry of a retired ledger except a preserved ticket's. */
  retired(ledger, kept) {
    const mirror = this.ledgers.get(ledger)
    if (!mirror) return
    const keep = new Set((kept ?? []).map((o) => o.offset))
    for (const offset of [...mirror.entries.keys()])
      if (!keep.has(offset)) mirror.entries.delete(offset)
  }

  /**
   * Stores one completed raw exchange of a client, independently decoded, in a fixed-size ring.
   * @param {string} client @param {{ apiKey: number, response: Buffer, file: string, sha256: string }} exchange
   * @returns {void}
   */
  exchanged(client, { apiKey, response, file, sha256 }) {
    let decoded = null
    try {
      decoded = acknowledgementResponse(apiKey, response)
    } catch (e) {
      this.fail(`exchanged ${client}: ${e.message}`)
      return
    }
    const ring = this.exchanges.get(client) ?? []
    ring.push({ apiKey, decoded, file, sha256 })
    if (ring.length > RECENT_EXCHANGES) ring.shift()
    this.exchanges.set(client, ring)
  }

  /**
   * @param {string} client @param {number|null} apiKey 78 or 79, or either
   * @param {(decoded: object) => boolean} test
   * @returns {{ file: string, sha256: string, decoded: object }|null} the newest matching exchange
   */
  wireFor(client, apiKey, test) {
    const ring = this.exchanges.get(client) ?? []
    for (let i = ring.length - 1; i >= 0; i--) {
      const e = ring[i]
      if ((apiKey === null || e.apiKey === apiKey) && test(e.decoded)) return e
    }
    return null
  }

  /**
   * The acquisition and delivery count of a record a share handler was given.
   * @param {string} client @param {string} topic @param {number} partition @param {string} offset
   * @returns {{ acquisition: string, deliveryCount: number }|null}
   */
  shareDelivery(client, topic, partition, offset) {
    for (const loop of this.mine(client, 'ShareFetchLoop'))
      for (const rows of loop.cohorts?.values() ?? [])
        for (const row of rows) {
          if (row.topic !== topic || Number(row.partition) !== Number(partition)) continue
          const found = this.acquisitions.get(row)
          const deliveryCount = found?.deliveries.get(String(offset))
          if (deliveryCount !== undefined) return { acquisition: found.acquisition, deliveryCount }
        }
    return null
  }

  /**
   * Every pause-state entry of the subscription state of every current run (see `isCurrent`),
   * including empty, resumed-only and pause-all ones, with the origin fixed when the state object
   * first appeared.
   * @returns {object[]} pause-state section entries; entries without an origin id are left out
   *   and counted as unknown topic ids
   */
  pauseStates() {
    const out = []
    const current = new Set(
      this.live('ConsumerGroup')
        .filter((g) => this.isCurrent(g))
        .map((g) => g.subscriptionState)
    )
    for (const receiver of this.live('SubscriptionState')) {
      if (!current.has(receiver)) continue
      const states = receiver.subscriptionStatesByTopic ?? {}
      for (const topic of Object.keys(states)) {
        const state = states[topic]
        const o = this.origins.get(state) ?? {}
        if (!o.topicId || !o.life || !o.run || !o.client) {
          this.unknownTopicIds++
          continue
        }
        out.push({
          client: o.client,
          run: o.run,
          stateId: o.stateId,
          topic,
          originTopicId: o.topicId,
          subscriptionLife: o.life,
          pauseAll: state.pauseAll === true,
          paused: ascending(state.paused),
          resumed: ascending(state.resumed),
          lastOperation: o.lastOperation,
        })
      }
    }
    return out
  }

  /**
   * The process-wide sections of one cut, read synchronously from the original discovered owners.
   * @param {{ read: () => Record<string, Record<string, number[]>> }} roles live async resources
   * @param {SocketRegistry} sockets the process's socket registry
   * @param {number} now the cut's run-clock time
   * @returns {Record<string, object[]>} queues, requests, sockets, listeners, resources and scopes
   */
  processSections(roles, sockets, now) {
    const queues = []
    const requests = []
    const listeners = []
    const scopes = new Map()
    const scopeOf = (client) => {
      const scope = client ?? 'harness'
      if (!scopes.has(scope))
        scopes.set(scope, {
          scope,
          owner: scope,
          type: client ? 'run' : 'harness',
          live: true,
          retiredAt: null,
        })
      return scope
    }
    const wallOffset = Date.now() - now
    const emitter = (target, client) => {
      const id = this.idOf(target)
      const names = target.eventNames()
      if (names.length === 0)
        listeners.push({
          emitter: id,
          client,
          scope: scopeOf(client),
          event: null,
          registrations: [],
        })
      for (const event of names)
        listeners.push({
          emitter: id,
          client,
          scope: scopeOf(client),
          event: String(event),
          registrations: target.rawListeners(event).map((fn) => ({
            listener: this.idOf(fn, 'f'),
            scope: scopeOf(this.clientOf(fn) ?? client),
          })),
        })
    }
    const connections = new Map()
    for (const connection of this.live('Connection')) {
      if (connection.requestQueue) {
        this.queueConnection.set(connection.requestQueue, connection)
        this.remember('RequestQueue', connection.requestQueue)
      }
      if (connection.socket) connections.set(connection.socket, connection)
    }
    for (const queue of this.live('RequestQueue')) {
      const client = this.clientOf(queue)
      const id = this.idOf(queue)
      const connection = this.queueConnection.get(queue)
      queues.push({
        queue: id,
        client: client ?? 'harness',
        connection: connection ? this.idOf(connection) : 'detached',
        scope: scopeOf(client),
        pending: queue.pending.length,
        inflight: queue.inflight.size,
      })
      const request = (r, phase) => {
        const beganAt = Math.max(0, (r.createdAt ?? Date.now()) - wallOffset)
        requests.push({
          request: this.idOf(r, 'q'),
          queue: id,
          apiKey: Number(r.entry?.apiKey ?? 0),
          phase,
          scope: scopeOf(client),
          beganAt,
          deadlineAt:
            phase === 'inflight' && r.sentAt
              ? Math.max(0, r.sentAt - wallOffset + (r.requestTimeout ?? 0))
              : null,
        })
      }
      for (const r of queue.pending) request(r, 'pending')
      for (const r of queue.inflight.values()) request(r, 'inflight')
      emitter(queue, client)
    }
    const socketEntries = []
    for (const { socket, client, node } of sockets.live()) {
      const connection = connections.get(socket)
      socketEntries.push({
        socket: this.idOf(socket, 'k'),
        client: client ?? 'harness',
        scope: scopeOf(client),
        node,
        connection: connection ? this.idOf(connection) : 'detached',
        destroyed: socket.destroyed === true,
      })
      emitter(socket, client)
    }
    const resources = []
    for (const [role, types] of Object.entries(roles.read())) {
      const client = role === 'harness' || role === 'unknown' ? null : role
      for (const [type, ids] of Object.entries(types))
        for (const id of ids)
          resources.push({ resource: `a${id}`, type, scope: scopeOf(client), client })
    }
    return {
      queues,
      requests,
      sockets: socketEntries,
      listeners,
      resources,
      scopes: [...scopes.values()],
    }
  }

  /**
   * The ordinary-consumer sections of one client, read from the backing sources: ConsumerGroup
   * subscription, assignment, acquisition and halt tables, topic configurations, preferred
   * replicas, queued seeks, consumer-protocol grown names and the cluster's target topics;
   * GroupOwnership states; OffsetOperations cells with their live tickets; ResetObligations
   * entries. A source whose field is missing is listed in `missingSources`, never read as empty.
   * Only groups whose run is current are read (see `isCurrent`), so a cut holds at most one run
   * generation of a client: a crashed or stopped run's dropped group is never reported.
   * @param {string} client actual client id
   * @returns {{ sections: Record<string, object[]>, missingSources: string[] }}
   */
  ordinarySections(client) {
    const missingSources = []
    const need = (object, field, label) => {
      if (object == null || !(field in object)) {
        missingSources.push(label)
        return false
      }
      return true
    }
    const subscription = []
    const assignment = []
    const membership = []
    const tables = []
    const cells = []
    const debts = []
    const tableEntry = (table, keys) => tables.push({ client, table, keys })
    const tableKey = (group, topic, partition) => ({
      topic,
      topicId: this.topicIdOf(topic, group),
      partition: partition === null ? null : Number(partition),
    })
    for (const group of this.mine(client, 'ConsumerGroup').filter((g) => this.isCurrent(g))) {
      const run = this.runOf(group)
      const ids = (names) =>
        names.map((topic) => ({ topic, topicId: this.topicIdOf(topic, group) }))
      if (need(group, 'topicsSubscribed', 'ConsumerGroup.topicsSubscribed')) {
        const accepted = ids([...group.topicsSubscribed]).filter((r) => r.topicId)
        const desired = group.subscription?.desired?.names ?? group.topicsSubscribed
        const observed = group.subscription?.observed
        subscription.push({
          client,
          run,
          revision: Number(group.subscription?.applied?.revision ?? 0),
          accepted,
          desired: ids([...desired]),
          observed: observed
            ? [...observed].map(([topic, topicId]) => ({
                topic,
                topicId: topicId == null ? null : this.printed(topicId),
              }))
            : ids([...group.topicsSubscribed]),
        })
      }
      if (need(group, 'ownership', 'ConsumerGroup.ownership'))
        for (const [token, state] of group.ownership.states ?? [])
          membership.push({
            client,
            run,
            token: Number(token),
            current: token === group.ownership.current,
            holders: Number(state.holders),
            refused: state.cause != null,
          })
      if (need(group, 'acquired', 'ConsumerGroup.acquired')) {
        const keys = []
        for (const key of group.acquired.keys()) {
          const { topic, partition } = splitKey(key)
          keys.push(tableKey(group, topic, partition))
          const k = this.keyOf(topic, partition, group)
          if (k) assignment.push({ client, key: k, authority: this.currentAuthority(group, key) })
        }
        tableEntry('acquired', keys)
      }
      if (need(group, 'haltedPartitions', 'ConsumerGroup.haltedPartitions'))
        tableEntry(
          'halted',
          [...group.haltedPartitions.keys()].map((k) => {
            const { topic, partition } = splitKey(k)
            return tableKey(group, topic, partition)
          })
        )
      if (need(group, 'topicConfigurations', 'ConsumerGroup.topicConfigurations'))
        tableEntry(
          'topic-configurations',
          Object.keys(group.topicConfigurations ?? {}).map((t) => tableKey(group, t, null))
        )
      if (
        need(
          group,
          'preferredReadReplicasPerTopicPartition',
          'ConsumerGroup.preferredReadReplicasPerTopicPartition'
        )
      ) {
        const keys = []
        for (const topic of Object.keys(group.preferredReadReplicasPerTopicPartition)) {
          const partitions = Object.keys(group.preferredReadReplicasPerTopicPartition[topic] ?? {})
          if (partitions.length === 0) keys.push(tableKey(group, topic, null))
          for (const p of partitions) keys.push(tableKey(group, topic, p))
        }
        tableEntry('preferred-replicas', keys)
      }
      if (need(group, 'seekOffset', 'ConsumerGroup.seekOffset'))
        tableEntry(
          'queued-seeks',
          [...group.seekOffset.keys()].map((k) => {
            const [topic, partition] = JSON.parse(k)
            return tableKey(group, topic, partition)
          })
        )
      if (group.kind === 'consumer') {
        if (need(group.groupMembership, 'grownTopicNames', 'ConsumerGroupProtocol.grownTopicNames'))
          tableEntry(
            'grown-topic-names',
            [...group.groupMembership.grownTopicNames].map((t) => tableKey(group, t, null))
          )
      } else tableEntry('grown-topic-names', [])
      if (need(group.cluster, 'targetTopics', 'Cluster.targetTopics'))
        tableEntry(
          'cluster-targets',
          [...group.cluster.targetTopics].map((t) => tableKey(group, t, null))
        )
    }
    for (const ledger of this.mine(client, 'OffsetOperations')) {
      if (!need(ledger, 'topics', 'OffsetOperations.topics')) continue
      const live = [...(this.liveTickets.get(ledger) ?? [])]
      for (const [topic, partitions] of ledger.topics)
        for (const [partition, cell] of partitions) {
          const tickets = live
            .filter(([t]) =>
              t.cells?.some((c) => c.topic === topic && c.partition === Number(partition))
            )
            .map(([, id]) => id)
          cells.push({
            ledger: this.idOf(ledger, 'l'),
            topic,
            partition: Number(partition),
            open: Number(cell.open),
            tickets,
          })
        }
    }
    for (const owed of this.mine(client, 'ResetObligations')) {
      if (!need(owed, 'entries', 'ResetObligations.entries')) continue
      for (const [topic, entry] of owed.entries) {
        if (!entry?.stamp) {
          this.unknownTopicIds++
          continue
        }
        const topicId = this.printed(entry.stamp)
        if (!topicId) continue
        debts.push({
          client,
          topic,
          topicId,
          discharged: ascending(entry.discharged ?? []),
        })
      }
    }
    return {
      sections: {
        subscription,
        'pause-state': this.pauseStates().filter((e) => e.client === client),
        assignment,
        'membership-state': membership,
        'operation-cell': cells,
        'reset-debt': debts,
        'other-table': tables,
      },
      missingSources,
    }
  }

  /**
   * The share-state section of one client: the client's current ShareRun (`run`, or null) and its
   * node sessions, the held records and decided ranges of every acquisition ledger of that run's
   * fetch loops, and their cohort rows by acquisition id. A run is current by the client's own
   * check (not retired and still the runner's current run), never by reachability, so a dropped run
   * that is not yet collected stays out after a restart. A run without a runner, or a loop without
   * a membership, counts as current. The held set is reconciled with the real ledger's own record
   * and range counts; a disagreement is reported.
   * @param {string} client actual client id
   * @returns {{ entry: object, mismatches: string[] }}
   */
  shareSections(client) {
    const runs = this.mine(client, 'ShareRun').filter(
      (run) => !run.retired && (!run.runner || run.runner.current === run)
    )
    const memberships = new Set(runs.map((run) => run.membership))
    const sessions = []
    for (const run of runs)
      for (const [nodeId, node] of run.nodes ?? [])
        if (node?.session)
          sessions.push({ node: Number(nodeId), session: Number(node.session.epoch) })
    const records = []
    const ranges = []
    const cohorts = []
    const mismatches = []
    const loops = this.mine(client, 'ShareFetchLoop').filter(
      (loop) => !loop.membership || memberships.has(loop.membership)
    )
    for (const loop of loops)
      for (const rows of loop.cohorts?.values() ?? [])
        for (const row of rows) {
          const mirror = this.ledgers.get(row.ledger)
          if (!mirror) continue
          cohorts.push(mirror.acquisition)
          let held = 0
          let decided = 0
          for (const entry of mirror.entries.values()) {
            if (entry.automatic) {
              decided++
              ranges.push({
                acquisition: mirror.acquisition,
                key: mirror.key,
                from: entry.first,
                to: entry.last,
              })
            } else {
              held++
              records.push({
                acquisition: mirror.acquisition,
                key: mirror.key,
                offset: entry.first,
                state: entry.state === 'settling' ? 'settling' : 'held',
              })
            }
          }
          const counts = row.ledger.counts
          if (counts && (counts.records !== held || counts.ranges !== decided))
            mismatches.push(
              `${mirror.acquisition} holds ${counts.records}/${counts.ranges}, observed ${held}/${decided}`
            )
        }
    const run = runs.length ? `${client}.${this.idOf(runs[0], 'r')}` : null
    return { entry: { client, run, sessions, records, ranges, cohorts }, mismatches }
  }
}

/**
 * Supplies each client's public `socketFactory`. It records every socket the default factory makes
 * (weakly, with its client and node) and, for a client that captures, splits the socket's outgoing
 * and incoming bytes into frames and hands each completed request/response pair of the chosen API
 * keys to `onExchange`. Each request of those keys is also handed to `onSent` as it is written, and
 * each one still unanswered when the socket closes to `onUnanswered`, so a request whose reply was
 * lost leaves a trace. A capturing socket's own `data` listener and `write` wrapper are removed at
 * close, as the existing share wire trace does.
 */
class SocketRegistry {
  /**
   * @param {object} [options]
   * @param {() => Function} [options.base] the default socket factory maker
   * @param {(header: Buffer) => { apiKey: number, correlationId: number }} [options.readHeader]
   */
  constructor({
    base = require('../../src/network/socketFactory'),
    readHeader = (frame) => ({ apiKey: frame.readInt16BE(4), correlationId: frame.readInt32BE(8) }),
  } = {}) {
    this.make = base()
    this.readHeader = readHeader
    this.refs = []
    this.created = 0
  }

  /**
   * @param {string} client the actual client id
   * @param {{ apiKeys: number[], onExchange: (e: { apiKey: number, request: Buffer,
   *   response: Buffer }) => void, onSent?: (r: SentRequest) => void,
   *   onUnanswered?: (r: SentRequest) => void }|null} [capture] `SentRequest` is
   *   `{ node, apiKey, correlationId, request }`
   * @returns {Function} a `socketFactory` for the client's Kafka config
   */
  factory(client, capture = null) {
    return (options) => {
      const socket = this.make(options)
      this.created++
      const node = `${options.host}:${options.port}`
      this.refs.push({ ref: new WeakRef(socket), client, node })
      if (capture) this.capture(socket, capture, node)
      return socket
    }
  }

  /** @returns {{ socket: object, client: string, node: string }[]} sockets not yet collected */
  live() {
    const out = []
    this.refs = this.refs.filter(({ ref, client, node }) => {
      const socket = ref.deref()
      if (socket) out.push({ socket, client, node })
      return socket !== undefined
    })
    return out
  }

  /**
   * Splits a capturing socket's bytes into frames and pairs requests with responses.
   * @param {object} socket @param {object} capture see `factory` @param {string} node host:port
   * @returns {void}
   */
  capture(socket, { apiKeys, onExchange, onSent = () => {}, onUnanswered = () => {} }, node) {
    const pending = new Map()
    const buffers = { out: Buffer.alloc(0), in: Buffer.alloc(0) }
    const take = (direction, chunk) => {
      let bytes = Buffer.concat([buffers[direction], Buffer.from(chunk)])
      while (bytes.length >= 4 && bytes.length >= bytes.readInt32BE(0) + 4) {
        const length = bytes.readInt32BE(0) + 4
        const frame = bytes.subarray(0, length)
        bytes = bytes.subarray(length)
        if (direction === 'out') {
          const { apiKey, correlationId } = this.readHeader(frame)
          if (apiKeys.includes(apiKey)) {
            pending.set(correlationId, { apiKey, request: frame })
            onSent({ node, apiKey, correlationId, request: frame })
          }
        } else {
          const correlationId = frame.readInt32BE(4)
          const request = pending.get(correlationId)
          if (!request) continue
          pending.delete(correlationId)
          onExchange({ ...request, response: Buffer.from(frame) })
        }
      }
      buffers[direction] = Buffer.from(bytes)
    }
    const original = socket.write
    const write = function (chunk, ...rest) {
      take('out', chunk)
      return Reflect.apply(original, this, [chunk, ...rest])
    }
    const receive = (chunk) => take('in', chunk)
    socket.write = write
    socket.on('data', receive)
    socket.once('close', () => {
      socket.removeListener('data', receive)
      if (socket.write === write) socket.write = original
      for (const [correlationId, { apiKey, request }] of pending)
        onUnanswered({ node, apiKey, correlationId, request })
      pending.clear()
    })
  }
}

module.exports = { PassiveObserver, SocketRegistry, splitKey, normalizeTopicId }
