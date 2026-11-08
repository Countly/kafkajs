'use strict'

const assert = require('node:assert/strict')
const { createResourceProbe, compareSnapshots } = require('../../../../testHelpers/resourceProbe')
const {
  PublicTelemetryRound,
  assertTelemetryOutcome,
  TELEMETRY_UUID,
} = require('../../../../testHelpers/lifecycle')
const { BoundaryWitness } = require('../../../../testHelpers/processMemory')
const gzip = require('../../../protocol/message/compression/gzip')
const oauthBearerAuthenticator = require('../../../broker/saslAuthenticator/oauthBearer')

/** The order an immediate reconnect is witnessed in. */
const IMMEDIATE = Object.freeze(['disconnect', 'connect', 'yield'])

/** The share acknowledgement type that accepts a record. */
const ACCEPT = 1

/**
 * Closes the round's client and calls the same object's connect in the public disconnect's own
 * resolution continuation, with no await between them; the composed work is bounded from outside.
 * @param {import('../../../../testHelpers/lifecycle').PublicTelemetryRound} round started round
 * @param {{defer?: boolean}} [options] `defer` awaits once before connect, as a control
 * @returns {Promise<string[]>} the witnessed actions, `IMMEDIATE` when connect came first
 */
const reopenNow = async (round, { defer = false } = {}) => {
  const witness = new BoundaryWitness(() => performance.now())
  await round.wait(() =>
    round.beginClose(round.closeOptions(0)).then(async () => {
      witness.resolved()
      if (defer) await Promise.resolve()
      witness.call('connect')
      return round.client.connect()
    })
  )
  return witness.events.map((event) => event.action)
}

/**
 * Joins one share record's acknowledgement to its actual settlement: the request that carries an
 * Accept for its offset, that request's broker answer, and the client's public acknowledgement
 * outcome for the offset. Keeps scalars only.
 */
class AckProof {
  /**
   * @param {{topic: string, topicId: string, partition: number, offset: number}} acquisition the
   *   record the cycle delivered
   * @returns {AckProof} a proof that has observed nothing
   */
  constructor({ topic, topicId, partition, offset }) {
    Object.assign(this, { topic, topicId, partition, offset: BigInt(offset) })
    this.written = 0
    this.answered = 0
    this.outcome = null
  }

  /**
   * @param {object|null} fields the decoded fields of a ShareFetch or ShareAcknowledge request
   * @returns {boolean} whether it accepts this record's offset
   */
  covers(fields) {
    return Boolean(
      fields?.topics?.some(
        (topic) =>
          topic.topicId === this.topicId &&
          topic.partitions.some(
            (partition) =>
              partition.partitionIndex === this.partition &&
              partition.acknowledgementBatches?.some((batch) => {
                const first = BigInt(batch.firstOffset)
                if (this.offset < first || this.offset > BigInt(batch.lastOffset)) return false
                const types = batch.acknowledgeTypes
                return (
                  (types.length === 1 ? types[0] : types[Number(this.offset - first)]) === ACCEPT
                )
              })
          )
      )
    )
  }

  /**
   * @param {'request'|'answered'|'cancelled'} event the wire notification
   * @param {object} record the request record
   * @returns {void} counts the requests that accept the offset and the answers they got
   */
  wire(event, record) {
    if (!['ShareFetch', 'ShareAcknowledge'].includes(record.api) || !this.covers(record.fields))
      return
    if (event === 'request') this.written += 1
    else if (event === 'answered') this.answered += 1
  }

  /**
   * @param {{topic: string, partition: number, firstOffset: string, lastOffset: string,
   *   type: string, error: Error|null}} payload one public acknowledgement outcome
   * @returns {void} keeps the outcome that covers the offset: its type, or `error`
   */
  settle(payload) {
    if (payload.topic !== this.topic || payload.partition !== this.partition) return
    if (this.offset < BigInt(payload.firstOffset) || this.offset > BigInt(payload.lastOffset))
      return
    this.outcome = payload.error ? 'error' : payload.type
  }

  /** @returns {{written: number, answered: number, outcome: string|null}} the joined facts */
  facts() {
    return { written: this.written, answered: this.answered, outcome: this.outcome }
  }
}

/**
 * Counts opens of the second Connection of every dedicated telemetry pool, at the real pool the
 * private factory builds. Cumulative: a pool that is destroyed and rebuilt keeps its count.
 */
class IndexOneWatch {
  /**
   * @param {import('../../../../testHelpers/lifecycle').PublicTelemetryRound} round started round
   *   whose cluster exists (use `beforeConnect`)
   * @returns {IndexOneWatch} a watch that is not installed yet
   */
  constructor(round) {
    this.round = round
    this.opens = 0
    this.pools = 0
    this.last = null
  }

  /**
   * Wraps the dedicated pool builder for the life of the round; the round restores it.
   * @returns {IndexOneWatch} this watch
   */
  install() {
    const watch = this
    const builder = this.round.cluster.connectionPoolBuilder
    this.round.patch(builder, 'buildTelemetry', (original) => (...args) => {
      const pool = original(...args)
      watch.pools += 1
      watch.last = pool
      const second = pool.pool[1]
      const connect = second.connect
      second.connect = function (...rest) {
        watch.opens += 1
        return connect.apply(this, rest)
      }
      return pool
    })
    return this
  }
}

/**
 * Drives one ordinary action of the round's kind through the real client after each public
 * reconnect, observing the original boundary without replacing a public method: the original
 * receiver, arguments, invocation count and result or rejection object stay the product's.
 */
class KindForeground {
  /**
   * @param {import('../../../../testHelpers/lifecycle').PublicTelemetryRound} round started or
   *   about-to-start round
   * @returns {KindForeground} a driver with no patch installed
   */
  constructor(round) {
    this.round = round
    this.kind = round.kind
    this.batch = null
    this.lastHandled = null
    this.proof = null
    this.nextOffset = 100
    this.shareHandler = async (payload) => {
      this.lastHandled = payload.message.offset
    }
  }

  /**
   * Installs the passive batch capture a consumer needs, and the recording handler a share
   * consumer runs with; call before `round.start`.
   * @returns {KindForeground} this driver
   */
  prepare() {
    if (this.kind === 'shareConsumer') {
      this.round.runOptions = { eachMessage: this.shareHandler }
      return this
    }
    if (this.kind !== 'consumer') return this
    const foreground = this
    const Runner = require('../../../consumer/runner')
    this.round.patch(
      Runner.prototype,
      'processEachMessage',
      (original) =>
        async function (batch) {
          const result = await original.call(this, batch)
          foreground.batch = batch
          return result
        }
    )
    return this
  }

  /**
   * Feeds the current acknowledgement proof from the wire's own notifications and from the share
   * consumer's public acknowledgement outcomes, so no request history is needed; the round removes
   * both listeners when it is disposed.
   * @returns {void}
   */
  watchAcknowledgements() {
    if (this.watching) return
    this.watching = true
    const { round } = this
    round.eventRemovers.push(
      round.wire.watch((event, record) => this.proof?.wire(event, record)),
      round.client.on(round.client.events.ACKNOWLEDGE, (event) => this.proof?.settle(event.payload))
    )
  }

  /**
   * Starts the run a consumer or share consumer needs after a public reconnect; the producer and
   * the admin client have nothing to resume.
   * @returns {Promise<void>} the actual membership exists
   */
  async resume() {
    const { round, kind } = this
    if (kind === 'consumer') {
      const scripted = require('../../../../testHelpers/scriptedCluster')
      const { TOPIC } = require('../../../consumer/__tests__/fixtures/fetchHarness')
      this.batch = null
      scripted.groupScript(round.wire, { batches: 1, brokerAnswerTimers: round.brokerAnswerTimers })
      await round.client.subscribe({ topic: TOPIC, fromBeginning: true })
      await round.client.run({ autoCommit: false, eachMessage: async () => {} })
    } else if (kind === 'shareConsumer') {
      this.watchAcknowledgements()
      await round.client.subscribe({ topics: ['topic-a'] })
      await round.client.run({ eachMessage: this.shareHandler })
      await round.until(() => round.runner.current?.membership.memberEpoch > 0)
    }
  }

  /**
   * Completes one ordinary action and checks the original outcome it produced.
   * @param {number} cycle attempt number, used only to make each action unique
   * @param {{ackError?: number}} [options] for a share consumer, a broker acknowledgement error
   *   code to answer the record's acknowledgement with, as a control
   * @returns {Promise<object>} the object that identifies this action's result
   * @throws {import('node:assert').AssertionError} the original boundary was not reached exactly
   *   once with the product's own receiver, arguments and outcome, or a share record's
   *   acknowledgement was not confirmed
   */
  async act(cycle, options = {}) {
    const { round, kind } = this
    const restoredAt = round.restores.length
    try {
      if (kind === 'producer') return await this.send(cycle)
      if (kind === 'admin') return await this.metadata()
      if (kind === 'consumer') return await this.process()
      return await this.accept(cycle, options)
    } finally {
      while (round.restores.length > restoredAt) round.restores.pop()()
    }
  }

  /** @returns {Promise<object>} the produce result of one unique record */
  async send(cycle) {
    const Accumulator = require('../../../producer/state/accumulator')
    const { TOPIC } = require('../../../consumer/__tests__/fixtures/fetchHarness')
    const { round } = this
    const record = await round.exerciseForeground({
      target: Accumulator.prototype,
      key: 'append',
      tap: true,
      run: () =>
        round.client.send({ topic: TOPIC, messages: [{ value: Buffer.from(`cycle ${cycle}`) }] }),
    })
    assertTelemetryOutcome(record.actual, record.expected, record.log.length)
    assert.equal(record.actual.kind, 'return')
    assert.ok(record.log[0].receiver instanceof Accumulator)
    assert.equal(record.log[0].args[0].topicMessages.length, 1)
    return record.actual.value
  }

  /** @returns {Promise<object>} the metadata result of one ordinary admin request */
  async metadata() {
    const { AdminDeadline } = require('../../../admin/deadline')
    const { TOPIC } = require('../../../consumer/__tests__/fixtures/fetchHarness')
    const { round } = this
    const entered = new Set()
    const record = await round.exerciseForeground({
      target: AdminDeadline.prototype,
      key: 'wait',
      tap: true,
      select: (receiver) => {
        if (entered.has(receiver)) return false
        entered.add(receiver)
        return true
      },
      run: () => round.client.fetchTopicMetadata({ topics: [TOPIC] }),
    })
    assertTelemetryOutcome(record.actual, record.expected, record.log.length)
    assert.equal(record.actual.kind, 'return')
    assert.equal(record.log[0].receiver.method, 'fetchTopicMetadata')
    return record.actual.value
  }

  /** @returns {Promise<object>} the consumed message the real runner handed to the handler */
  async process() {
    const { round } = this
    await round.until(() => this.batch !== null)
    const runner = round.runner
    const batch = this.batch
    const record = await round.exerciseForeground({
      target: runner,
      key: 'eachMessage',
      tap: true,
      run: () => runner.processEachMessage(batch),
    })
    assert.deepEqual(record.actual, { kind: 'return', value: undefined })
    assert.equal(record.expected.kind, 'return')
    assert.equal(record.log[0].receiver, runner)
    assert.equal(record.log[0].args[0].message, batch.messages[0])
    return batch.messages[0]
  }

  /**
   * Delivers one share record through the real run, then requires its Accept to be written,
   * answered by the broker and confirmed by the client's public acknowledgement outcome.
   * @param {number} cycle attempt number, used only to make each record unique
   * @param {{ackError?: number}} [options] answers the acknowledgement with this error code instead
   * @returns {Promise<object>} the share record the real run delivered and acknowledged
   * @throws {import('node:assert').AssertionError} when the acknowledgement was not confirmed
   */
  async accept(cycle, { ackError } = {}) {
    const {
      A,
      part,
      acquired,
      recordsHex,
      fetchAnswer,
      ackAnswer,
    } = require('../../../../testHelpers/shareHarness')
    const { round } = this
    const run = round.runner.current
    const offset = this.nextOffset++
    this.watchAcknowledgements()
    const proof = new AckProof({ topic: 'topic-a', topicId: A, partition: 0, offset })
    this.proof = proof
    const refuse = (answer) => (fields) => (proof.covers(fields) ? answer : undefined)
    const refusals = (answer) => Array.from({ length: 8 }, () => refuse(answer))
    const record = await round.exerciseForeground({
      target: run,
      key: 'handler',
      tap: true,
      run: async () => {
        const delivery = fetchAnswer([
          part({
            topicId: A,
            partition: 0,
            acquire: [acquired(offset, offset)],
            records: recordsHex(offset, [`share cycle ${cycle}`]),
          }),
        ])
        if (ackError === undefined) round.harness.script('ShareFetch', [delivery])
        else {
          round.harness.script('ShareFetch', [
            delivery,
            ...refusals(fetchAnswer([part({ topicId: A, partition: 0, ack: ackError })])),
          ])
          round.harness.script(
            'ShareAcknowledge',
            refusals(ackAnswer([{ topicId: A, partition: 0, code: ackError }]))
          )
        }
        await round.until(() => this.lastHandled === String(offset))
      },
    })
    assert.deepEqual(record.actual, { kind: 'return', value: undefined })
    assert.equal(record.log[0].receiver, run)
    try {
      await round.until(() => proof.outcome !== null && proof.answered > 0)
    } finally {
      this.proof = null
    }
    const facts = proof.facts()
    assert.ok(
      facts.written > 0 && facts.answered > 0 && facts.outcome === 'accept',
      `the acknowledgement of offset ${offset} was not confirmed: ${JSON.stringify(facts)}`
    )
    return record.log[0].args[0].message
  }
}

/**
 * Journals one fixed-shape scalar row per cycle of a bounded hundred-cycle schedule: the witnessed
 * reconnect order, weak-issued IDs of every current owner by role, the dedicated socket's emitter
 * and raw listener IDs, the pending timers by owning role with the clock's issue floor at the
 * cycle's start, and the populations of every capture root of the bounded round. The round
 * releases each completed per-object patch and closed socket before the populations are read, so
 * neither the round nor this ledger keeps a completed owner alive.
 */
class CycleLedger {
  /**
   * @param {import('../../../../testHelpers/lifecycle').PublicTelemetryRound} round started bounded
   *   or default round
   * @param {() => Record<string, object[]>} shell the held shell's owner objects by role: the
   *   one occupied builder, native job or supplier that must stay the same through every cycle
   * @returns {CycleLedger} an empty ledger
   */
  constructor(round, shell) {
    this.round = round
    this.shellProbe = createResourceProbe({ identities: shell })
    this.churnProbe = createResourceProbe({
      identities: () => this.churnOwners(),
      registrations: () => this.registrations(),
    })
    this.current = []
    this.rows = []
    this.base = null
  }

  /** @returns {object[]} dedicated sockets that are still open */
  openSockets() {
    return this.round.openDedicated()
  }

  /** @returns {Record<string, object[]>} what the machine and its queue hold right now */
  churnOwners() {
    const { machine, dedicatedConnection } = this.round
    const queue = dedicatedConnection?.requestQueue
    return {
      snapshots: machine.running?.snapshot ? [machine.running.snapshot] : [],
      operations: machine.running ? [machine.running] : [],
      waiters: [...machine.waiters],
      queue: queue ? [...queue.pending, ...queue.inflight.values()] : [],
      sockets: this.openSockets(),
      handles: machine.handle ? [machine.handle] : [],
      foreground: this.current,
    }
  }

  /** @returns {Record<string, import('events').EventEmitter>} the one open dedicated socket */
  registrations() {
    const [socket] = this.openSockets()
    return socket ? { 'dedicated-socket': socket } : {}
  }

  /**
   * Takes the shell identity reading that later cycles must equal, and the first time also the
   * round's capture populations that later cycles are measured against.
   * @returns {Promise<object>} the baseline shell snapshot
   */
  async baseline() {
    this.base ??= this.round.boundedState()
    return this.shellProbe.snapshot()
  }

  /**
   * @param {bigint} floor the clock's last issued timer at the cycle's start
   * @returns {{floor: string, scheduler: string[], watchdog: string[], waiter: string[],
   *   close: string[], unknown: string[]}} every pending timer under the machine role that owns
   *   it, and under `unknown` when none does
   */
  timers(floor) {
    const { machine, clock, closeTimer } = this.round
    const named = {
      scheduler: machine.timer,
      watchdog: machine.watchdog,
      waiter: machine.waiterTimer,
      close: machine.closing ? closeTimer : null,
    }
    const pending = [...clock.pending.keys()]
    const owned = Object.values(named)
    return {
      floor: String(floor),
      ...Object.fromEntries(
        Object.entries(named).map(([role, id]) => [
          role,
          pending.filter((candidate) => candidate === id).map(String),
        ])
      ),
      unknown: pending.filter((id) => !owned.includes(id)).map(String),
    }
  }

  /**
   * Reads one cycle, then releases what completed and journals the populations.
   * @param {number} cycle attempt number
   * @param {object} foreground the object identifying this cycle's ordinary result
   * @param {{reopen: string[], floor: bigint}} cycleFacts the witnessed reconnect order and the
   *   clock's last issued timer at the cycle's start
   * @returns {Promise<object>} the scalar row, also kept in `rows`
   */
  async record(cycle, foreground, { reopen, floor }) {
    const { round } = this
    this.current = [foreground]
    const row = {
      cycle,
      reopen: [...reopen],
      lifecycle: String(round.machine.lifecycle),
      transport: String(round.machine.transportRevision),
      uuid: await round.client.clientInstanceId({ timeout: 0 }),
      shell: await this.shellProbe.snapshot(),
      churn: await this.churnProbe.snapshot(),
      timers: this.timers(floor),
    }
    this.current = []
    round.releaseCompleted()
    row.state = round.boundedState()
    this.rows.push(row)
    return row
  }
}

/** What a bounded round may hold beyond its held baseline: one replacement lifecycle in flight. */
const SLACK = Object.freeze({
  owners: 2,
  liveSockets: 1,
  wireSockets: 1,
  wireLive: 2,
  handles: 1,
  pending: 1,
  work: 2,
})

/** Owners that live across cycles: the very same IDs at every checkpoint. */
const STABLE_ROLES = Object.freeze(['snapshots', 'operations', 'queue'])

/** Owners each lifecycle replaces: as many at every checkpoint, never a retired one. */
const REPLACED_ROLES = Object.freeze(['sockets', 'handles'])

/** @param {*} a @param {*} b @returns {boolean} whether two JSON values are the same */
const sameJson = (a, b) => JSON.stringify(a) === JSON.stringify(b)

/**
 * @param {object} churn one churn snapshot
 * @returns {number} the largest weak ID it mentions, owners and registrations alike
 */
const largestId = (churn) =>
  Math.max(
    0,
    ...Object.values(churn.identities).flat(),
    ...Object.values(churn.registrations).flatMap((registration) => [
      registration.id,
      ...Object.values(registration.events).flat(),
    ])
  )

/**
 * @param {object} row one journaled cycle
 * @returns {string} the dedicated socket's event names with, per event, its raw listeners named by
 *   the order they first appear in, so a duplicate, missing or moved listener changes it while
 *   fresh IDs of the same shape do not
 */
const listenerShape = (row) => {
  const events = row.churn.registrations['dedicated-socket']?.events ?? {}
  const order = new Map()
  const name = (id) => {
    if (!order.has(id)) order.set(id, order.size)
    return order.get(id)
  }
  return JSON.stringify(
    Object.keys(events)
      .sort()
      .map((event) => [event, events[event].map(name)])
  )
}

/**
 * Judges bounded-capture readings. Each check returns plain sentences, one per violation, so a
 * test can assert exactly which fact failed and a control can assert its specific diagnostic.
 */
class BoundedAudit {
  /**
   * @param {object} state a `boundedState()` reading
   * @param {object} base the reading taken with the held shell
   * @returns {string[]} what the populations show beyond the baseline
   */
  static state(state, base) {
    const problems = []
    const check = (holds, sentence) => holds || problems.push(sentence)
    check(state.restorers === base.restorers, 'the round-wide restorer list changed')
    check(state.ownersCompleted === 0, 'a retired owner is still in the restore table')
    check(state.owners <= base.owners + SLACK.owners, 'live owners exceed their bound')
    check(state.liveSockets <= base.liveSockets + SLACK.liveSockets, 'live sockets exceed bound')
    check(state.wireSockets <= base.wireSockets + SLACK.wireSockets, 'wire sockets exceed bound')
    check(state.wireLive <= base.wireLive + SLACK.wireLive, 'live wire requests exceed bound')
    check(state.handles <= base.handles + SLACK.handles, 'live handles exceed their bound')
    check(state.pending <= base.pending + SLACK.pending, 'pending close work exceeds its bound')
    check(state.work <= base.work + SLACK.work, 'tracked work exceeds its bound')
    check(state.history === 0, 'a history of completed work is retained')
    check(state.retainedBytes === 0, 'captured bytes are retained')
    check(state.writesAfterClose === base.writesAfterClose, 'a write reached a closed socket')
    return problems
  }

  /**
   * Judges one cycle by identity, not by count: the held shell is the same objects; the reconnect
   * ran in the disconnect's continuation; owners that live across cycles (samples, operations,
   * queued requests) are the very ones of the first row; owners each lifecycle replaces (socket,
   * transport handle, foreground result, the socket's emitter and raw listeners) are as many as in
   * the first row and were all issued after the previous row, so no retired one is still held; the
   * socket's events and listener multiplicity repeat the first row's; and the only pending timer is
   * the scheduler's, issued in this cycle.
   * @param {object} row one journaled cycle
   * @param {object} shell the baseline shell snapshot
   * @param {string[]} roles the shell roles that must be present and unchanged
   * @param {object|undefined} first the first row, whose owner shape every cycle repeats
   * @param {object|undefined} previous the row of the cycle before
   * @param {{sockets: number}} expected how many dedicated sockets a checkpoint holds open
   * @returns {string[]} what this cycle shows against the held shell and the per-cycle rules
   */
  static cycle(row, shell, roles, first, previous, expected) {
    const problems = compareSnapshots(shell, row.shell, { identities: roles })
    const check = (holds, sentence) => holds || problems.push(sentence)
    const owners = row.churn.identities
    check(row.uuid === TELEMETRY_UUID, 'the client instance id changed')
    check(
      sameJson(row.reopen, IMMEDIATE),
      'the reconnect did not run in the disconnect continuation'
    )
    check(owners.foreground.length === 1, 'the foreground result is not one object')
    check(owners.waiters.length === 0, 'an id waiter survives')
    check(
      owners.sockets.length === expected.sockets,
      'the open dedicated sockets are not as expected'
    )
    if (first) {
      for (const role of STABLE_ROLES)
        check(sameJson(owners[role], first.churn.identities[role]), `${role} owners changed`)
      for (const role of REPLACED_ROLES)
        check(owners[role].length === first.churn.identities[role].length, `${role} owners changed`)
      check(listenerShape(row) === listenerShape(first), 'dedicated socket listeners changed')
    }
    const registration = row.churn.registrations['dedicated-socket']
    if (registration)
      check(registration.id === owners.sockets[0], 'the listeners are on another emitter')
    if (previous) {
      const floor = largestId(previous.churn)
      for (const role of [...REPLACED_ROLES, 'foreground'])
        check(
          owners[role].every((id) => id > floor),
          `a retired ${role} owner is still held`
        )
      if (registration)
        check(
          [registration.id, ...Object.values(registration.events).flat()].every((id) => id > floor),
          'a retired emitter or listener is still registered'
        )
    }
    const { timers } = row
    check(timers.scheduler.length === 1, 'the scheduler timer is not exactly one')
    check(
      timers.watchdog.length + timers.waiter.length + timers.close.length === 0,
      'an operation timer survives the checkpoint'
    )
    check(timers.unknown.length === 0, 'an unowned timer is pending')
    const pending = [
      ...timers.scheduler,
      ...timers.watchdog,
      ...timers.waiter,
      ...timers.close,
      ...timers.unknown,
    ]
    check(
      pending.every((id) => BigInt(id) > BigInt(timers.floor)),
      'a timer from a retired lifecycle survives'
    )
    return problems
  }

  /**
   * @param {object[]} rows the whole journal
   * @param {number} count how many cycles it must hold
   * @returns {string[]} what the journal shows across all cycles
   */
  static journal(rows, count) {
    const problems = []
    const check = (holds, sentence) => holds || problems.push(sentence)
    check(rows.length === count, `the journal holds ${rows.length} rows, not ${count}`)
    check(new Set(rows.map((row) => row.lifecycle)).size === rows.length, 'a lifecycle repeats')
    check(new Set(rows.map((row) => row.transport)).size === rows.length, 'a transport repeats')
    check(
      new Set(rows.map((row) => row.churn.identities.foreground[0])).size === rows.length,
      'a foreground result repeats'
    )
    rows.forEach((row, index) => {
      if (index > 0 && row.state.socketsIssued < rows[index - 1].state.socketsIssued)
        problems.push(`issued socket numbers fell at cycle ${index}`)
      if (index > 0 && row.state.ownersIssued < rows[index - 1].state.ownersIssued)
        problems.push(`issued owner numbers fell at cycle ${index}`)
      check(
        Object.values(row).every((value) => typeof value !== 'function'),
        'a row keeps a function'
      )
    })
    if (rows.length > 1)
      check(
        rows.at(-1).state.socketsIssued > rows[0].state.socketsIssued,
        'issued socket numbers never advanced'
      )
    return problems
  }
}

/**
 * @param {NativeResultFamily|SupplierFamily} family an opened family
 * @param {object} row one of its journaled rows, or a copy with one fact changed
 * @returns {string[]} every violation the row and its populations show, judged against the first
 *   row and the row of the cycle before
 */
const auditRow = (family, row) => {
  const { rows, base } = family.ledger
  const previous = rows.find((candidate) => candidate.cycle === row.cycle - 1)
  return [
    ...BoundedAudit.cycle(row, family.shell, family.roles, rows[0], previous, family.expected),
    ...BoundedAudit.state(row.state, base),
  ]
}

/**
 * One schedule of public close and immediate reopen cycles while a real native job stays occupied
 * after a held completion, on a bounded round. `outcome` is how the held job settles when the
 * schedule ends: a late success or a late rejection.
 */
class NativeResultFamily {
  /**
   * @param {{kind: string, outcome: 'resolve'|'reject'}} options the public kind and the late result
   * @returns {NativeResultFamily} an unopened schedule
   */
  constructor({ kind, outcome }) {
    this.kind = kind
    this.outcome = outcome
    this.roles = ['builder', 'nativeJob', 'nativeInput', 'nativeResult']
    this.expected = { sockets: 1 }
    this.failNext = false
    this.compressCalls = 0
    this.submitted = null
  }

  /**
   * Starts the client, holds one native job and takes the baselines.
   * @returns {Promise<NativeResultFamily>} this family, at its held state
   */
  async open() {
    const round = (this.round = new PublicTelemetryRound({ kind: this.kind, bounded: true }))
    this.foreground = new KindForeground(round).prepare()
    this.watch = new IndexOneWatch(round)
    round.patch(gzip, 'compress', (original) => (...args) => {
      this.compressCalls += 1
      this.submitted = new WeakRef(args[0].buffer)
      return this.failNext ? Promise.reject(new Error('late codec failure')) : original(...args)
    })
    this.gate = round.hold('S12', { mode: 'normal' })
    await round.wait(() =>
      round.start({
        points: 'large',
        subscription: { acceptedCompressionTypes: [1, 0] },
        beforeConnect: () => this.watch.install(),
      })
    )
    await round.ready()
    this.failNext = this.outcome === 'reject'
    round.arm()
    round.tick()
    await round.wait(() => this.gate.arrived())
    this.failNext = false
    assert.equal(round.codecJobs.length, 1, 'exactly one native job is held')
    this.heldJob = round.codecJobs[0]
    assert.equal(this.heldJob.input, this.submitted.deref(), 'the held input is the payload input')
    this.callsAtHold = this.compressCalls
    this.ledger = new CycleLedger(round, () => ({
      builder: [round.machine.payload],
      nativeJob: round.codecJobs.slice(0, 1),
      nativeInput: round.codecJobs.slice(0, 1).flatMap((job) => (job.input ? [job.input] : [])),
      nativeResult: round.codecJobs.slice(0, 1).map((job) => job.promise),
    }))
    this.shell = await this.ledger.baseline()
    for (const role of this.roles)
      assert.equal(this.shell.identities[role].length, 1, `the shell has one ${role}`)
    return this
  }

  /** @returns {string[]} what shows that the held native job was replaced, settled or joined */
  held() {
    const problems = []
    if (this.round.codecJobs[0] !== this.heldJob) problems.push('the held native job was replaced')
    if (this.heldJob.settled) problems.push('the held native job settled during the schedule')
    if (this.compressCalls !== this.callsAtHold) problems.push('a second native job was admitted')
    return problems
  }

  /** @returns {Record<string, object[]>} objects of the same count as the shell, none of them it */
  decoyShell() {
    return Object.fromEntries(this.roles.map((role) => [role, [{}]]))
  }

  /**
   * Lets the held native job settle now, with the late outcome, instead of at the end.
   * @returns {Promise<void>} the job has settled
   */
  async releaseEarly() {
    this.gate.release()
    await this.round.until(() => this.heldJob.settled)
  }

  /**
   * Runs one public close with an immediate reopen, waits until the replacement's Get is answered
   * and the machine is idle, then runs one ordinary foreground action and journals the row.
   * @param {number} index attempt number
   * @param {{defer?: boolean}} [options] `defer` delays the reconnect by one await, as a control
   * @returns {Promise<object>} the journaled row
   */
  async cycle(index, options) {
    const { round, foreground } = this
    const floor = round.clock.sequence
    const reopen = await reopenNow(round, options)
    await round.wait(() => foreground.resume(), 30000)
    const gets = round.wire.tally('GetTelemetrySubscriptions', 'answered')
    round.tick()
    await round.until(
      () =>
        round.wire.tally('GetTelemetrySubscriptions', 'answered') > gets && !round.machine.running
    )
    const identity = await round.wait(() => foreground.act(index), 30000)
    const row = await this.ledger.record(index, identity, { reopen, floor })
    assert.deepEqual(this.held(), [], 'the held shell stayed occupied')
    return row
  }

  /**
   * @param {object} row a journaled row
   * @returns {string[]} every violation the row and its populations show
   */
  audit(row) {
    return auditRow(this, row)
  }

  /**
   * Finishes the schedule: no pool-index-one open, no new Push or write while the shell is held,
   * then the late result is released and a fresh current accepted Push must follow.
   * @returns {Promise<void>}
   */
  async finish() {
    const { round, gate, heldJob, ledger, shell, watch } = this
    assert.ok(watch.pools > 0, 'dedicated pools were built')
    assert.equal(watch.opens, 0, 'the second connection of a dedicated pool was opened')
    const blocked = round.pushesRequested()
    round.tick()
    await round.until(() => !round.machine.running)
    assert.equal(round.codecJobs.length, 1)
    assert.equal(round.pushesRequested(), blocked, 'a Push was written while the shell was held')
    const replacement = round.capture()
    const writes = round.writeCount
    round.armed = false
    await round.drainReleased(gate)
    assert.equal(heldJob.settled, true, 'the held native job did not settle after release')
    round.assertReplacementUnchanged(replacement)
    assert.equal(round.writeCount, writes, 'a write followed the late completion')
    assert.deepEqual(
      compareSnapshots(shell, await ledger.baseline(), { identities: ['builder'] }),
      []
    )
    assert.equal(round.machine.uuid, TELEMETRY_UUID)
    await round.ready()
    assert.ok(round.pushesRequested() > blocked, 'no fresh Push followed the late completion')
    assert.equal(round.codecJobs.length, 1)
    assert.ok(this.compressCalls > this.callsAtHold)
    assert.equal(watch.opens, 0)
  }

  /** @returns {Promise<void>} restores every patch and joins every owned release */
  async close() {
    await this.round?.dispose()
  }
}

/**
 * One schedule of public close and immediate reopen cycles while the actual authentication
 * supplier of the dedicated connection stays occupied by a held call. `mechanism` is the built-in
 * OAuth path or a custom provider; `outcome` is how the held call settles at the end.
 */
class SupplierFamily {
  /**
   * @param {{kind: string, mechanism: 'built-in OAuth'|'custom provider',
   *   outcome: 'resolve'|'reject'}} options the public kind, mechanism and late result
   * @returns {SupplierFamily} an unopened schedule
   */
  constructor({ kind, mechanism = 'built-in OAuth', outcome }) {
    this.kind = kind
    this.mechanism = mechanism
    this.outcome = outcome
    this.roles = ['supplier', 'pending']
    this.expected = { sockets: 0 }
    this.calls = 0
    this.late = false
  }

  /**
   * Starts the client, holds one supplier call and takes the baselines.
   * @returns {Promise<SupplierFamily>} this family, at its held state
   */
  async open() {
    const round = (this.round = new PublicTelemetryRound({ kind: this.kind, bounded: true }))
    this.foreground = new KindForeground(round).prepare()
    this.watch = new IndexOneWatch(round)
    const supplier = async () => {
      this.calls += 1
      const held = this.late
      if (held) await round.enter('S19b')
      if (held && this.outcome === 'reject') throw new Error('late supplier failure')
      return { value: 'telemetry-token' }
    }
    const sasl = { mechanism: 'oauthbearer', oauthBearerProvider: supplier }
    if (this.mechanism === 'custom provider')
      sasl.authenticationProvider = (args) => oauthBearerAuthenticator(sasl)(args)
    this.gate = round.hold('S19b', { mode: 'normal' })
    await round.wait(() =>
      round.start({ sasl: true, kafka: { sasl }, beforeConnect: () => this.watch.install() })
    )
    await round.ready()
    this.owner = round.dedicatedConnection.authenticationOwner
    this.baselineCalls = this.calls
    round.armRenewal()
    round.arm()
    this.late = true
    round.tick()
    await round.wait(() => this.gate.arrived())
    this.late = false
    assert.equal(this.calls, this.baselineCalls + 1, 'exactly one supplier call is held')
    this.heldPending = this.owner.pending
    assert.notEqual(this.heldPending, null)
    this.ledger = new CycleLedger(round, () => ({
      supplier: [this.owner],
      pending: this.owner.pending ? [this.owner.pending] : [],
    }))
    this.shell = await this.ledger.baseline()
    for (const role of this.roles)
      assert.equal(this.shell.identities[role].length, 1, `the shell has one ${role}`)
    return this
  }

  /** @returns {string[]} what shows that the held supplier call was replaced, settled or joined */
  held() {
    const problems = []
    if (this.owner.pending === null) problems.push('the held supplier shell settled early')
    else if (this.owner.pending !== this.heldPending)
      problems.push('the held supplier shell was replaced')
    if (this.calls !== this.baselineCalls + 1) problems.push('a second supplier call was admitted')
    return problems
  }

  /** @returns {Record<string, object[]>} objects of the same count as the shell, none of them it */
  decoyShell() {
    return Object.fromEntries(this.roles.map((role) => [role, [{}]]))
  }

  /**
   * Lets the held supplier call settle now, with the late outcome, instead of at the end.
   * @returns {Promise<void>} the supplier shell is free
   */
  async releaseEarly() {
    this.gate.release()
    await this.round.until(() => this.owner.pending === null)
  }

  /**
   * @param {string} api wire API name @returns {number} requests of it written so far
   */
  apiCount(api) {
    return this.round.wire.tally(api)
  }

  /**
   * Runs one public close with an immediate reopen, delivers the replacement's setup and waits
   * until that setup was attempted and refused while the held supplier call blocks it, the machine
   * is idle and has scheduled its retry; then runs one ordinary foreground action and journals
   * the row.
   * @param {number} index attempt number
   * @param {{defer?: boolean}} [options] `defer` delays the reconnect by one await, as a control
   * @returns {Promise<object>} the journaled row
   */
  async cycle(index, options) {
    const { round, foreground } = this
    const floor = round.clock.sequence
    const reopen = await reopenNow(round, options)
    await round.wait(() => foreground.resume(), 30000)
    assert.notEqual(round.machine.timer, null, 'the reopened client scheduled no setup')
    const attempts = round.opensAttempted
    round.tick()
    await round.until(
      () =>
        round.opensAttempted > attempts &&
        !round.machine.running &&
        round.machine.timer !== null &&
        this.owner.pending === this.heldPending
    )
    const identity = await round.wait(() => foreground.act(index), 30000)
    const row = await this.ledger.record(index, identity, { reopen, floor })
    assert.deepEqual(this.held(), [], 'the held shell stayed occupied')
    return row
  }

  /**
   * @param {object} row a journaled row
   * @returns {string[]} every violation the row and its populations show
   */
  audit(row) {
    return auditRow(this, row)
  }

  /**
   * Finishes the schedule: one more reopen, then the late call is released, nothing may be
   * written by it, and a fresh current accepted Push must follow on the replacement.
   * @returns {Promise<void>}
   */
  async finish() {
    const { round, gate, foreground, owner, watch } = this
    assert.ok(watch.pools > 0, 'dedicated pools were built')
    assert.equal(watch.opens, 0, 'the second connection of a dedicated pool was opened')
    assert.deepEqual(await reopenNow(round), IMMEDIATE)
    await round.wait(() => foreground.resume(), 30000)
    const idle = round.capture()
    const apis = ['SaslAuthenticate', 'GetTelemetrySubscriptions', 'PushTelemetry']
    const before = Object.fromEntries(apis.map((api) => [api, this.apiCount(api)]))
    const writes = round.writeCount
    round.armed = false
    await round.drainReleased(gate)
    assert.deepEqual(
      Object.fromEntries(apis.map((api) => [api, this.apiCount(api)])),
      before,
      'the late call issued a request'
    )
    assert.equal(round.writeCount, writes, 'a write followed the late call')
    assert.equal(owner.pending, null, 'the held supplier shell did not settle after release')
    round.assertReplacementUnchanged(idle)
    assert.equal(idle.uuid, TELEMETRY_UUID)
    await round.establish()
    assert.ok(this.apiCount('PushTelemetry') > before.PushTelemetry, 'no fresh Push followed')
    assert.equal(round.machine.uuid, TELEMETRY_UUID)
    assert.equal(watch.opens, 0)
  }

  /** @returns {Promise<void>} restores every patch and joins every owned release */
  async close() {
    await this.round?.dispose()
  }
}

module.exports = {
  AckProof,
  BoundedAudit,
  IMMEDIATE,
  CycleLedger,
  IndexOneWatch,
  KindForeground,
  NativeResultFamily,
  SupplierFamily,
  reopenNow,
}
