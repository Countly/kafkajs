'use strict'

const assert = require('node:assert/strict')
const { createResourceProbe } = require('../../../../testHelpers/resourceProbe')
const { assertTelemetryOutcome } = require('../../../../testHelpers/lifecycle')

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
    this.handled = []
    this.nextOffset = 100
    this.shareHandler = async (payload) => {
      this.handled.push(payload.message.offset)
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
      await round.client.subscribe({ topics: ['topic-a'] })
      await round.client.run({ eachMessage: this.shareHandler })
      await round.until(() => round.runner.current?.membership.memberEpoch > 0)
    }
  }

  /**
   * Completes one ordinary action and checks the original outcome it produced.
   * @param {number} cycle attempt number, used only to make each action unique
   * @returns {Promise<object>} the object that identifies this action's result
   * @throws {import('node:assert').AssertionError} the original boundary was not reached exactly
   *   once with the product's own receiver, arguments and outcome
   */
  async act(cycle) {
    const { round, kind } = this
    const restoredAt = round.restores.length
    try {
      if (kind === 'producer') return await this.send(cycle)
      if (kind === 'admin') return await this.metadata()
      if (kind === 'consumer') return await this.process()
      return await this.accept(cycle)
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

  /** @returns {Promise<object>} the share record the real run delivered and acknowledged */
  async accept(cycle) {
    const {
      A,
      part,
      acquired,
      recordsHex,
      fetchAnswer,
    } = require('../../../../testHelpers/shareHarness')
    const { round } = this
    const run = round.runner.current
    const offset = this.nextOffset++
    const acknowledged = () =>
      round.wire
        .requests()
        .filter(
          (row) =>
            ['ShareFetch', 'ShareAcknowledge'].includes(row.api) &&
            row.fields.topics?.some((topic) =>
              topic.partitions.some((partition) => partition.acknowledgementBatches?.length)
            )
        ).length
    const before = acknowledged()
    const record = await round.exerciseForeground({
      target: run,
      key: 'handler',
      tap: true,
      run: async () => {
        round.harness.script('ShareFetch', [
          fetchAnswer([
            part({
              topicId: A,
              partition: 0,
              acquire: [acquired(offset, offset)],
              records: recordsHex(offset, [`share cycle ${cycle}`]),
            }),
          ]),
        ])
        await round.until(() => this.handled.includes(String(offset)))
      },
    })
    assert.deepEqual(record.actual, { kind: 'return', value: undefined })
    assert.equal(record.log[0].receiver, run)
    await round.until(() => acknowledged() > before)
    return record.log[0].args[0].message
  }
}

/**
 * Journals one scalar row per cycle of a hundred-cycle schedule and then releases the fixture's
 * own history of completed work, so a measured owner is never kept alive by the harness. Rows
 * hold numbers, booleans and strings only; the held shell is compared by issued identity.
 */
class CycleLedger {
  /**
   * @param {import('../../../../testHelpers/lifecycle').PublicTelemetryRound} round started round
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
    this.writes = 0
    this.rows = []
  }

  /** @returns {object[]} dedicated sockets that are still open */
  openSockets() {
    return this.round.dedicatedSockets.filter((socket) => !socket.destroyed)
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
   * Takes the shell identity reading that later cycles must equal.
   * @returns {Promise<object>} the baseline shell snapshot
   */
  async baseline() {
    return this.shellProbe.snapshot()
  }

  /**
   * Reads one cycle, then drops the references the fixture itself keeps for completed work.
   * @param {number} cycle attempt number
   * @param {object} foreground the object identifying this cycle's ordinary result
   * @returns {Promise<object>} the scalar row, also kept in `rows`
   */
  async record(cycle, foreground) {
    const { round } = this
    this.current = [foreground]
    const row = {
      cycle,
      lifecycle: String(round.machine.lifecycle),
      transport: String(round.machine.transportRevision),
      uuid: await round.client.clientInstanceId({ timeout: 0 }),
      shell: await this.shellProbe.snapshot(),
      churn: await this.churnProbe.snapshot(),
      timers: [...round.clock.pending.keys()].map(String).sort(),
    }
    this.current = []
    row.afterRelease = this.release()
    this.rows.push(row)
    return row
  }

  /**
   * Drops completed samples, frames, writes, retired handles and closed sockets from the round's
   * own history, keeping its first sample (the held operation's) and every live handle.
   * @returns {Record<string, number>} what the round still holds, as counts
   */
  release() {
    const { round } = this
    this.writes += round.socketWrites.length
    round.snapshots.splice(1)
    round.written.length = 0
    round.abandoned.length = 0
    round.expectedFrames.length = 0
    round.socketWrites.length = 0
    round.trace.frames.length = 0
    round.frameFields.length = 0
    const live = round.builtHandles.filter((handle) => !round.retirements.includes(handle))
    round.builtHandles.splice(0, round.builtHandles.length, ...live)
    round.retirements.length = 0
    round.dedicatedSockets = this.openSockets()
    return {
      snapshots: round.snapshots.length,
      handles: round.builtHandles.length,
      retirements: round.retirements.length,
      frames: round.trace.frames.length,
      sockets: round.dedicatedSockets.length,
    }
  }

  /** @returns {number} writes the dedicated sockets made since the ledger began */
  totalWrites() {
    return this.writes + this.round.socketWrites.length
  }
}

module.exports = { CycleLedger, IndexOneWatch, KindForeground }
