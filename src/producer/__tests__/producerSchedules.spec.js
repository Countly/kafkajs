const zlib = require('zlib')
const { Kafka, CompressionCodecs, CompressionTypes } = require('../../../index')
const Cluster = require('../../cluster')
const { Admission } = require('../state/admission')
const ProducerStateManager = require('../state')
const { KafkaJSRequestTimeoutError } = require('../../errors')
const { createScriptedCluster, producerScript } = require('../../../testHelpers/scriptedCluster')
const { WireOrder } = require('../../../testHelpers/wireOrder')
const { TOPIC } = require('../../consumer/__tests__/fixtures/fetchHarness')
/** Advances transport callbacks. @returns {Promise<void>} completion; @throws Never. */
const turn = () => new Promise((resolve) => setImmediate(resolve))
/** Pumps finite transport turns. @param {Function} ready predicate @returns {Promise<void>} completion @throws {Error} on a missing transition. */
const until = async (ready) => {
  for (let i = 0; i < 200 && !ready(); i++) {
    if (typeof setTimeout.clock === 'object') await jest.advanceTimersByTimeAsync(1)
    else await new Promise((resolve) => setTimeout(resolve, 1))
    await turn()
  }
  expect(ready()).toBe(true)
}
/** Watches settlement. @param {Promise<*>} promise operation @returns {object} state @throws Never. */
const watch = (promise) => {
  const result = { status: 'pending' }
  promise.then(
    (value) => Object.assign(result, { status: 'fulfilled', value }),
    (error) => Object.assign(result, { status: 'rejected', error })
  )
  return result
}

const { interleavings } = require('../../../testHelpers/schedule')
/**
 * Attempts every cleanup action and unconditionally restores global test state.
 * @param {Error|undefined} primary original test failure
 * @param {Function[]} actions independent resource cleanup attempts
 * @param {Function} [restore] final synchronous restoration, even after cleanup failures
 * @returns {Promise<void>} after all actions and restoration were attempted
 * @throws {Error|AggregateError} original with cleanupErrors, or cleanup failures alone
 */
const finish = async (primary, actions, restore = () => {}) => {
  const failures = []
  try {
    for (const action of actions) {
      try {
        await action()
      } catch (error) {
        failures.push(error)
      }
    }
  } finally {
    try {
      restore()
    } catch (error) {
      failures.push(error)
    }
  }
  if (primary) {
    if (failures.length) primary.cleanupErrors = failures
    throw primary
  }
  if (failures.length) throw new AggregateError(failures, 'Resource cleanup failed')
}

describe.each(['default', 'tv1', 'tv2'])('Late answer under %s', (profile) => {
  it.each([0, 46])('keeps the expired caller rejected after answer code %i', async (errorCode) => {
    const cluster = producerScript(
      createScriptedCluster({ nodes: [{ id: 0, host: 'node0', port: 9092 }], profile })
    )
    cluster.answer('InitProducerId', ({ producerEpoch = -1 }) => ({
      producerId: '1000',
      producerEpoch: Math.max(0, producerEpoch + 1),
    }))
    const gate = cluster.schedule.hold({ api: 'Produce' })
    const producer = new Kafka(
      cluster.kafkaOptions({ requestTimeout: 1000, enforceRequestTimeout: false })
    ).producer({
      idempotent: true,
      linger: 0,
      deliveryTimeout: 1000,
      ...(profile === 'default' ? {} : { transactionalId: 'schedule' }),
    })
    let pending
    let result
    let abort
    let aborted
    let primary
    try {
      jest.useFakeTimers({ doNotFake: ['nextTick', 'setImmediate'] })
      await producer.connect()
      const sender = profile === 'default' ? producer : await producer.transaction()
      pending = sender.send({ topic: TOPIC, messages: [{ value: 'late', timestamp: 1000 }] })
      result = watch(pending)
      await until(() => gate.call !== null)
      await jest.advanceTimersByTimeAsync(1000)
      expect(result).toMatchObject({
        status: 'rejected',
        error: { name: 'KafkaJSDeliveryTimeout', outcome: 'unknown' },
      })
      gate.release({
        responses: [
          {
            ...gate.call.fields.topics[0],
            partitions: [
              {
                partition: 0,
                errorCode,
                baseOffset: '10',
                logAppendTime: '-1',
                logStartOffset: '0',
                leader: null,
              },
            ],
          },
        ],
        nodeEndpoints: [],
      })
      await until(() =>
        cluster.timeline().some((e) => e.api === 'Produce' && e.kind === 'response')
      )
      expect(result.status).toBe('rejected')
      expect(WireOrder.of(cluster).produced()).toHaveLength(1)
      WireOrder.of(cluster).assertNoRewrite()
      if (profile !== 'default') {
        await expect(sender.commit()).rejects.toMatchObject({
          name: 'KafkaJSTransactionAbortableError',
          category: 'abortable',
        })
        abort = sender.abort()
        aborted = watch(abort)
        await until(() => aborted.status !== 'pending')
        await abort
      }
    } catch (error) {
      primary = error
    } finally {
      await finish(
        primary,
        [
          () => {
            cluster.schedule.rules.length = 0
          },
          () => {
            if (gate.settle) gate.fail(new Error('Fixture closing'))
          },
          async () => {
            const close = producer.disconnect()
            const closed = watch(close)
            await until(() => closed.status !== 'pending')
            await close
          },
          async () => {
            await until(
              () =>
                (!result || result.status !== 'pending') &&
                (!aborted || aborted.status !== 'pending')
            )
            await Promise.allSettled([pending, abort].filter(Boolean))
          },
          () => cluster.assertClean(),
        ],
        () => jest.useRealTimers()
      )
    }
  })
})
it('enumerates every event order for both queue shapes', () => {
  const orders = interleavings(['answer', 'timeout', 'reconnect', 'expiry'])
  expect(orders).toHaveLength(24)
  expect(new Set(orders.map((order) => order.join(','))).size).toBe(24)
  expect(
    ['inflight-and-queued', 'two-inflight'].flatMap((shape) =>
      orders.map((order) => ({ shape, order }))
    )
  ).toHaveLength(48)
})

const NODES = [{ id: 0, host: 'node0', port: 9092 }]
/** Fake milliseconds per pump step: every timer of a row falls on a multiple of it. */
const STEP = 50
const BACKOFF = 100
const DEADLINE = 1000
/** When a row's second send is made, so that its deadline falls after the first one's. */
const SECOND_SEND_AT = 300
const GROUP = 'group'
const OFFSETS = [{ topic: 'input', partitions: [{ partition: 0, offset: '1' }] }]

/** @returns {{ promise: Promise<*>, resolve: Function, reject: Function }} a promise to settle by hand */
const deferred = () => {
  const handle = {}
  handle.promise = new Promise((resolve, reject) => {
    handle.resolve = resolve
    handle.reject = reject
  })
  return handle
}

/**
 * Prelude of each replay shape: a partition with one batch written and one queued behind it, or
 * two partitions with one batch written each. Rows of shape `script` carry their whole script.
 * @type {Record<string, Command[]>}
 */
const SHAPES = {
  script: [],
  'inflight-and-queued': [
    { op: 'connect' },
    { op: 'send', id: 'm1', partition: 0 },
    { op: 'arrive', attempt: 0 },
    { op: 'advance', ms: SECOND_SEND_AT },
    { op: 'send', id: 'm2', partition: 0 },
  ],
  'two-inflight': [
    { op: 'connect' },
    { op: 'send', id: 'm1', partition: 0 },
    { op: 'arrive', attempt: 0 },
    { op: 'advance', ms: SECOND_SEND_AT },
    { op: 'send', id: 'm2', partition: 1 },
    { op: 'arrive', attempt: 1 },
  ],
}

/**
 * One step of a row, finite data. `connect` (with `bare`: no transaction) and `begin` open the
 * producer and a transaction; `start` begins a `connect` or `transaction` call without awaiting
 * it; `send`, `offsets`, `commit` and `abort` act on the current sender and are watched under
 * `id`; `wait` pumps until a watched call settles; `hold` arms the gate of the next request of an
 * API, and `arrive`/`release` wait for and answer a held request (`attempt` counts Produce
 * requests, `gate` names another API; `errorCode` replaces the broker's answer); `fail` and
 * `destroy` end a held Produce request with a request timeout or by destroying its socket;
 * `advance` runs fake time by `ms` or to `to`; `close` is `disconnect({ timeout })`; `codec` and
 * `encoder` hold and release a compressing codec; `event` is one of `answer`, `timeout`,
 * `reconnect` and `expiry`, each acting on the first Produce request; `finish` answers what is
 * live until every send settled; `end` commits a transaction whose sends all resolved, else
 * aborts it.
 * @typedef {object} Command
 * @property {string} op
 * @property {string} [id]
 * @property {number} [partition]
 * @property {number} [attempt]
 * @property {string} [gate]
 * @property {string} [api]
 * @property {string} [call]
 * @property {string} [event]
 * @property {number} [ms]
 * @property {number} [to]
 * @property {number} [timeout]
 * @property {number} [errorCode]
 * @property {boolean} [bare]
 * @property {boolean} [compressed]
 */

/**
 * A retained schedule: the producer kind and profile, a script of commands and everything it must
 * leave behind, all of it literal. Wire rows are `[api, partition, producer id, epoch, sequence,
 * connection generation]` for Produce and `[api, ...]` for the control requests. Settlements are
 * `[id, 'fulfilled', baseOffset]` or `[id, 'rejected', error name, outcome]`; logs are `[partition,
 * producer id, epoch, sequence, offset]`. A row of shape `inflight-and-queued` or `two-inflight`
 * is its events after the shape's prelude, then `finish` and, for a transaction, `end`.
 * @typedef {object} ReplayRow
 * @property {string} name unique title
 * @property {'idempotent'|'transactional'} kind
 * @property {'default'|'tv1'|'tv2'} profile the scripted cluster's profile
 * @property {'script'|'inflight-and-queued'|'two-inflight'} shape
 * @property {string} fault what the row injects: a fault point or an event order
 * @property {Command[]} commands
 * @property {Array<Array<string|number|number[]>>} expectedWire
 * @property {string[][]} expectedSettlement
 * @property {string} expectedState the producer's last state manager at the end
 * @property {number[][]} expectedLog every record the broker applied, in order
 * @property {string[]} expectedNoops events that found nothing live to act on
 * @property {number[][]} [expectedCommitted] records a read_committed consumer sees
 * @property {string[][]} [expectedPublished] offsets the producer wrote to its local cache
 * @property {Record<string, string>} [expectedBrokerOffsets] offsets the broker committed
 * @property {string[]} [expectedOrder] timeline events that must occur in this order
 * @property {number[]} [expectedSockets] sockets when the close began and at the end
 */

/**
 * The scripted broker's own ledger: it applies each Produce frame on receipt, whether or not its
 * answer is ever delivered, by the idempotence rules (a new epoch restarts at sequence 0, the
 * next sequence appends, a lower one is a duplicate, a higher one is out of order), and applies a
 * transaction's records and staged offsets only when its EndTxn commits. Its log is the physical
 * oracle of every row, independent of anything the client believes.
 */
class BrokerModel {
  /** @param {boolean} transactional whether read_committed visibility waits for EndTxn */
  constructor(transactional) {
    this.transactional = transactional
    this.state = new Map()
    this.log = []
    this.pending = []
    this.committed = []
    this.staged = []
    this.offsets = {}
  }

  /**
   * Applies one batch.
   * @param {number} partition
   * @param {{ producerId: string, producerEpoch: number, baseSequence: number, recordCount: number }} batch
   * @returns {object} the partition's Produce answer: 0, 46 (duplicate, with the original offset),
   *   45 or 47
   */
  produce(partition, { producerId, producerEpoch, baseSequence, recordCount }) {
    const state = this.state.get(partition) ?? { epoch: -1, next: 0, offsets: new Map() }
    this.state.set(partition, state)
    const answer = (errorCode, baseOffset) => ({
      partition,
      errorCode,
      baseOffset: String(baseOffset),
      logAppendTime: '-1',
      logStartOffset: '0',
      leader: null,
    })
    if (producerEpoch < state.epoch) return answer(47, -1)
    if (producerEpoch > state.epoch) {
      if (baseSequence !== 0) return answer(45, -1)
      state.epoch = producerEpoch
      state.next = 0
      state.offsets = new Map()
    }
    if (baseSequence < state.next) return answer(46, state.offsets.get(baseSequence) ?? -1)
    if (baseSequence > state.next) return answer(45, -1)
    const offset = this.log.filter((entry) => entry[0] === partition).length
    const entry = [partition, Number(producerId), producerEpoch, baseSequence, offset]
    state.offsets.set(baseSequence, offset)
    state.next += recordCount
    this.log.push(entry)
    this.pending.push(entry)
    return answer(0, offset)
  }

  /**
   * Stages offsets a TxnOffsetCommit carried until the transaction ends.
   * @param {Array<{ topic: string, partitions: object[] }>} topics
   * @returns {void}
   */
  stage(topics) {
    this.staged.push(...topics)
  }

  /**
   * Ends the transaction at the broker: a commit makes its records and offsets visible.
   * @param {boolean} commit
   * @returns {void}
   */
  end(commit) {
    if (commit) {
      this.committed.push(...this.pending)
      for (const { topic, partitions } of this.staged) {
        for (const { partition, offset } of partitions) {
          this.offsets[`${topic}/${partition}`] = offset
        }
      }
    }
    this.pending = []
    this.staged = []
  }

  /** @returns {number[][]} the records a read_committed consumer sees (all of them when plain) */
  visible() {
    return this.transactional ? this.committed : this.log
  }
}

/**
 * Runs one retained schedule through the public producer over a scripted wire under fake time.
 * Every Produce request is held by the case's own answerer, which has already applied it to the
 * broker model; a command decides when and whether its answer is delivered. Other APIs answer at
 * once unless a `hold` command armed their gate. The case owns the cluster, the producer, the
 * gates and the spies on the real cluster, admission and state managers (additive: each calls the
 * original with its own `this`), and `close()` releases the gates, closes the producer and checks
 * the cluster.
 */
class ProducerScheduleCase {
  /** @param {ReplayRow} row */
  constructor(row) {
    this.row = row
    this.transactional = row.kind === 'transactional'
    this.broker = new BrokerModel(this.transactional)
    this.attempts = []
    this.gates = new Map()
    this.holds = new Map()
    this.noops = []
    this.watched = new Map()
    this.cluster = producerScript(createScriptedCluster({ nodes: NODES, profile: row.profile }), {
      partitions: 2,
    })
    this.cluster.answer('InitProducerId', (fields, record) =>
      this.answered('InitProducerId', record, {
        producerId: '1000',
        producerEpoch: Math.max(0, fields.producerEpoch + 1),
      })
    )
    this.cluster.answer('Produce', (fields, record) => this.produce(fields, record))
    this.cluster.answer('TxnOffsetCommit', (fields, record) => {
      this.broker.stage(fields.topics)
      return this.answered('TxnOffsetCommit', record, {
        topics: fields.topics.map((entry) => ({
          topic: entry.topic,
          partitions: entry.partitions.map(({ partition }) => ({ partition, errorCode: 0 })),
        })),
      })
    })
    this.cluster.answer('EndTxn', (fields, record) => {
      this.broker.end(fields.transactionResult)
      return this.answered(
        'EndTxn',
        record,
        row.profile === 'tv2' ? { producerId: '1000', producerEpoch: fields.producerEpoch + 1 } : {}
      )
    })
    const metadata = this.cluster.answerers.get('Metadata')
    this.cluster.answer('Metadata', (fields, record) =>
      this.answered('Metadata', record, metadata(fields, record))
    )
  }

  /**
   * @param {string} api
   * @param {object} record the request
   * @param {object} response the answer the broker model decided
   * @returns {object|Promise<object>} the answer, or the promise of the gate armed for `api`
   */
  answered(api, record, response) {
    const gate = this.holds.get(api)?.shift()
    if (!gate) return response
    Object.assign(gate, { record, response, arrived: true })
    return gate.promise
  }

  /**
   * Applies a Produce request to the broker model and holds its answer.
   * @param {object} fields the request
   * @param {object} record the request's log record
   * @returns {Promise<object>} the answer, once a command releases it
   */
  produce(fields, record) {
    const responses = fields.topics.map(({ partitions, ...key }) => ({
      ...key,
      partitions: partitions.flatMap(({ partition, batches }) =>
        batches.map((batch) => this.broker.produce(partition, batch))
      ),
    }))
    const attempt = {
      record,
      response: { responses, nodeEndpoints: [] },
      status: 'held',
      ...deferred(),
    }
    this.attempts.push(attempt)
    return attempt.promise
  }

  /** @returns {object} the scripted socket a held Produce request was written on */
  socketOf(attempt) {
    return this.cluster.sockets().filter((socket) => socket.endpoint.port === 9092)[
      attempt.record.generation - 1
    ]
  }

  /** @returns {boolean} whether the attempt is held on a socket that still lives */
  live(attempt) {
    return Boolean(attempt) && attempt.status === 'held' && !this.socketOf(attempt).destroyed
  }

  /**
   * Runs the row to its end and reads what it left.
   * @returns {Promise<object>} the observation
   * @throws {Error} when a command cannot be carried out
   */
  async run() {
    this.start()
    const { shape, commands } = this.row
    const wrapped = shape !== 'script'
    const script = [
      ...SHAPES[shape],
      ...commands,
      ...(wrapped ? [{ op: 'finish' }] : []),
      ...(wrapped && this.transactional ? [{ op: 'end' }] : []),
    ]
    for (const command of script) await this.exec(command)
    if (!this.watched.has('close')) {
      await this.exec({ op: 'close' })
      await this.exec({ op: 'wait', id: 'close' })
    }
    await this.settle()
    return this.observe()
  }

  /** Installs fake time, the spies and the producer, which is not connected yet. */
  start() {
    jest.useFakeTimers({ now: 0, doNotFake: ['nextTick', 'setImmediate'] })
    const publish = Cluster.prototype.markOffsetAsCommitted
    const cluster = this.cluster
    this.spies = {
      pins: jest.spyOn(Cluster.prototype, 'holdTopic'),
      published: jest
        .spyOn(Cluster.prototype, 'markOffsetAsCommitted')
        .mockImplementation(function (...args) {
          cluster.mark('publish')
          return publish.apply(this, args)
        }),
      acquire: jest.spyOn(Admission.prototype, 'acquire'),
      cancel: jest.spyOn(Admission.prototype, 'cancel'),
      initialize: jest.spyOn(ProducerStateManager.prototype, 'initialize'),
    }
    this.producer = new Kafka(
      this.cluster.kafkaOptions({ requestTimeout: 1000, enforceRequestTimeout: false })
    ).producer({
      idempotent: true,
      linger: 0,
      deliveryTimeout: DEADLINE,
      retry: { initialRetryTime: BACKOFF, maxRetryTime: BACKOFF, factor: 0, multiplier: 1 },
      ...(this.transactional ? { transactionalId: 'schedule' } : {}),
    })
    this.sender = this.producer
  }

  /** @returns {string} what changes whenever the wire, the timers or a watched call moves */
  signature() {
    return [
      this.cluster.requests().length,
      this.cluster.sockets().length,
      this.cluster.clock,
      jest.getTimerCount(),
      [...this.watched.values()].filter(({ status }) => status === 'pending').length,
    ].join()
  }

  /** Lets queued work run, with no fake time passing, until nothing moves for three turns. */
  async settle() {
    let stable = 0
    let last = ''
    for (let i = 0; i < 300 && stable < 3; i += 1) {
      await turn()
      await jest.advanceTimersByTimeAsync(0)
      const signature = this.signature()
      stable = signature === last ? stable + 1 : 0
      last = signature
    }
  }

  /**
   * Runs fake time in steps, letting the wire settle after each, so a retry written at its timer
   * sees the clock of its own step.
   * @param {number} ms
   */
  async advance(ms) {
    for (let left = ms; left > 0; left -= STEP) {
      await jest.advanceTimersByTimeAsync(Math.min(STEP, left))
      await this.settle()
    }
  }

  /**
   * Pumps until `promise` settles.
   * @param {Promise<*>} promise
   * @param {boolean} [timed=true] whether fake time may pass meanwhile
   * @returns {Promise<*>} its value
   * @throws {Error} its rejection, or a promise that never settles
   */
  async pumped(promise, timed = true) {
    const result = watch(promise)
    for (let i = 0; i < 400 && result.status === 'pending'; i += 1) {
      if (timed) await this.advance(STEP)
      else await this.settle()
    }
    if (result.status === 'pending') throw new Error('The call did not settle')
    if (result.status === 'rejected') throw result.error
    return result.value
  }

  /**
   * @param {string} id
   * @param {Promise<*>} promise
   * @returns {Promise<void>} once the wire settled
   */
  watching(id, promise) {
    this.watched.set(id, watch(promise))
    return this.settle()
  }

  /**
   * Carries out one command.
   * @param {Command} command
   * @returns {Promise<*>}
   * @throws {Error} for an unknown command, or a held request that never arrives
   */
  async exec(command) {
    const { op } = command
    if (op === 'connect') {
      await this.pumped(this.producer.connect(), false)
      if (this.transactional && !command.bare) {
        this.sender = await this.pumped(this.producer.transaction(), false)
      }
      return undefined
    }
    if (op === 'begin') {
      this.sender = await this.pumped(this.producer.transaction())
      return undefined
    }
    if (op === 'start') return this.watching(command.id, this.producer[command.call]())
    if (op === 'send') {
      return this.watching(
        command.id,
        this.sender.send({
          topic: TOPIC,
          ...(command.compressed ? { compression: CompressionTypes.GZIP } : {}),
          messages: [{ value: command.id, partition: command.partition, timestamp: 1000 }],
        })
      )
    }
    if (op === 'offsets') {
      return this.watching(
        command.id,
        this.sender.sendOffsets({ consumerGroupId: GROUP, topics: OFFSETS })
      )
    }
    if (op === 'commit' || op === 'abort') return this.watching(command.id, this.sender[op]())
    if (op === 'end') return this.end()
    if (op === 'wait') {
      for (let i = 0; i < 100 && this.watched.get(command.id).status === 'pending'; i += 1) {
        await this.advance(STEP)
      }
      return undefined
    }
    if (op === 'close') {
      this.socketsAtClose = this.cluster.sockets().length
      return this.watching('close', this.producer.disconnect({ timeout: command.timeout }))
    }
    if (op === 'hold') {
      const gate = { api: command.api, arrived: false, ...deferred() }
      this.holds.set(command.api, [...(this.holds.get(command.api) ?? []), gate])
      this.gates.set(command.api, gate)
      return undefined
    }
    if (op === 'arrive') return this.arrive(command)
    if (op === 'codec') return this.installCodec()
    if (op === 'encoder') {
      this.encoder.resolve()
      return this.settle()
    }
    if (op === 'release') return this.release(command)
    if (op === 'fail') {
      const attempt = this.attempts[command.attempt]
      if (!this.live(attempt)) return this.noops.push(`fail:${command.attempt}`)
      attempt.status = 'failed'
      attempt.reject(new KafkaJSRequestTimeoutError('Request timed out'))
      return this.settle()
    }
    if (op === 'destroy') {
      const socket = this.socketOf(this.attempts[command.attempt])
      if (socket.destroyed) return this.noops.push(`destroy:${command.attempt}`)
      socket.destroy(Object.assign(new Error('read ECONNRESET'), { code: 'ECONNRESET' }))
      return this.settle()
    }
    if (op === 'advance') {
      return this.advance(command.to === undefined ? command.ms : command.to - Date.now())
    }
    if (op === 'event') return this.event(command.event)
    if (op === 'finish') return this.finish()
    throw new Error(`Unknown command ${op}`)
  }

  /**
   * Waits for a held request, advancing fake time while it has not arrived.
   * @param {Command} command `attempt` or `gate`
   * @returns {Promise<void>}
   * @throws {Error} when it never arrives
   */
  async arrive(command) {
    for (let i = 0; i < 200; i += 1) {
      if (command.gate ? this.gates.get(command.gate)?.arrived : this.attempts[command.attempt]) {
        return
      }
      await this.advance(STEP)
    }
    throw new Error(`${JSON.stringify(command)} never arrived`)
  }

  /**
   * Delivers the answer of a held request: a Produce attempt only while its socket lives (an answer
   * on a destroyed socket is lost), a gate with the answer the broker model decided or its error
   * code replaced.
   * @param {Command} command `attempt` or `gate`
   * @returns {Promise<void>|number}
   */
  release(command) {
    if (command.gate) {
      const gate = this.gates.get(command.gate)
      gate.resolve(
        command.errorCode === undefined
          ? gate.response
          : { ...gate.response, errorCode: command.errorCode }
      )
      return this.settle()
    }
    const attempt = this.attempts[command.attempt]
    if (!this.live(attempt)) return this.noops.push(`release:${command.attempt}`)
    attempt.status = 'released'
    attempt.resolve(attempt.response)
    return this.settle()
  }

  /**
   * One event of an order, acting on the first Produce request.
   * @param {'answer'|'timeout'|'reconnect'|'expiry'} name
   * @returns {Promise<*>}
   */
  event(name) {
    if (name === 'answer') return this.exec({ op: 'release', attempt: 0 })
    if (name === 'timeout') return this.exec({ op: 'fail', attempt: 0 })
    if (name === 'expiry') return this.exec({ op: 'advance', to: DEADLINE })
    return this.exec({ op: 'destroy', attempt: 0 }).then(() => this.advance(BACKOFF))
  }

  /** Holds the compressing codec of GZIP until `encoder` releases it; `close()` restores it. */
  installCodec() {
    this.saved = CompressionCodecs[CompressionTypes.GZIP]
    const codec = this.saved()
    this.encoder = deferred()
    CompressionCodecs[CompressionTypes.GZIP] = () => ({
      ...codec,
      compress: async (...args) => {
        await this.encoder.promise
        return zlib.gzipSync(args[0].buffer)
      },
    })
  }

  /**
   * Answers every live request in arrival order, letting time pass when none is live, until every
   * send settled.
   * @returns {Promise<void>}
   * @throws {Error} when sends stay pending
   */
  async finish() {
    for (let i = 0; i < 60; i += 1) {
      const live = this.attempts.find((attempt) => this.live(attempt))
      if (live) {
        live.status = 'released'
        live.resolve(live.response)
        await this.settle()
      } else if (this.sends().every(({ status }) => status !== 'pending')) {
        return
      } else {
        await this.advance(BACKOFF)
      }
    }
    throw new Error('The sends did not settle')
  }

  /** @returns {object[]} the watched sends */
  sends() {
    return [...this.watched.entries()].filter(([id]) => id.startsWith('m')).map(([, s]) => s)
  }

  /** Commits the transaction when every send resolved, else aborts it. */
  async end() {
    const commit = this.sends().every(({ status }) => status === 'fulfilled')
    await this.exec({ op: commit ? 'commit' : 'abort', id: 'end' })
    await this.exec({ op: 'wait', id: 'end' })
  }

  /** @returns {Array<Array<string|number|number[]>>} the wire rows */
  wireRows() {
    return this.cluster.requests().flatMap((record) => {
      const { api, fields } = record
      if (api === 'Produce') {
        return fields.topics.flatMap((entry) =>
          entry.partitions.flatMap(({ partition, batches }) =>
            batches.map((batch) => [
              api,
              partition,
              Number(batch.producerId),
              batch.producerEpoch,
              batch.baseSequence,
              record.generation,
            ])
          )
        )
      }
      if (api === 'InitProducerId') return [[api, Number(fields.producerId), fields.producerEpoch]]
      if (api === 'AddPartitionsToTxn') return [[api, fields.topics.flatMap((t) => t.partitions)]]
      if (api === 'EndTxn') {
        const result = fields.transactionResult ? 'commit' : 'abort'
        return [[api, result, Number(fields.producerId), fields.producerEpoch]]
      }
      return api === 'AddOffsetsToTxn' || api === 'TxnOffsetCommit' ? [[api]] : []
    })
  }

  /** @returns {string[][]} the settlement of every watched call except the close */
  settled() {
    return [...this.watched.entries()]
      .filter(([id]) => id !== 'close')
      .map(([id, { status, value, error }]) => {
        if (status === 'fulfilled') {
          return [id, status, ...(Array.isArray(value) ? [value[0].baseOffset] : [])]
        }
        return status === 'rejected'
          ? [id, status, error.name, error.outcome ?? null]
          : [id, status]
      })
  }

  /** @returns {object} what the row left: wire, settlements, state, logs, admission and pins */
  observe() {
    const managers = [...new Set(this.spies.initialize.mock.contexts)]
    const entries = managers.flatMap((manager) =>
      [0, 1].map((partition) => manager.sequencer.entry(TOPIC, partition))
    )
    const count = (key) => entries.reduce((sum, entry) => sum + (entry ? entry[key].length : 0), 0)
    const admission = this.spies.acquire.mock.contexts[0] ?? this.spies.cancel.mock.contexts[0]
    const cluster = this.spies.pins.mock.contexts[0]
    return {
      wire: this.wireRows(),
      settled: this.settled(),
      state: managers.at(-1).state(),
      physicalLog: this.broker.log,
      committedLog: this.broker.visible(),
      published: this.spies.published.mock.calls.map(([o]) => [
        o.groupId,
        o.topic,
        o.partition,
        o.offset,
      ]),
      brokerOffsets: this.broker.offsets,
      queued: count('queued'),
      inFlight: count('inFlight'),
      admission: {
        available: admission.available,
        capacity: admission.capacity,
        reserved: admission.reserved,
        waitingBytes: admission.waitingBytes,
        debts: admission.debts,
      },
      pins: [...cluster.topicPins.values()].reduce((sum, pins) => sum + pins, 0),
      sockets: [this.socketsAtClose ?? null, this.cluster.sockets().length],
      noops: this.noops,
    }
  }

  /**
   * Releases every gate still held, closes the producer at once if the row did not, and checks the
   * cluster. Call `restore()` afterwards, even when this throws.
   * @returns {Promise<void>}
   * @throws {Error} the cluster's first failure
   */
  async close() {
    for (const attempt of this.attempts) attempt.resolve(attempt.response)
    for (const gate of this.gates.values()) gate.resolve(gate.response)
    this.encoder?.resolve()
    if (!this.watched.has('close')) await this.producer.disconnect({ timeout: 0 })
    await this.settle()
    this.cluster.assertClean()
  }

  /** Restores the codec, the spies and real time. */
  restore() {
    if (this.saved) CompressionCodecs[CompressionTypes.GZIP] = this.saved
    Object.values(this.spies ?? {}).forEach((spy) => spy.mockRestore())
    jest.useRealTimers()
  }
}

/**
 * Checks one observation against its row and the wire oracle's invariants.
 * @param {ProducerScheduleCase} scenario
 * @param {ReplayRow} row
 * @param {object} observed
 * @returns {void}
 * @throws {Error} the first assertion that fails
 */
const assertRow = (scenario, row, observed) => {
  const wire = WireOrder.of(scenario.cluster)
  expect(observed.wire).toEqual(row.expectedWire)
  expect(observed.settled).toEqual(row.expectedSettlement)
  expect(observed.state).toBe(row.expectedState)
  expect(observed.physicalLog).toEqual(row.expectedLog)
  expect(observed.noops).toEqual(row.expectedNoops)
  if (row.expectedCommitted) expect(observed.committedLog).toEqual(row.expectedCommitted)
  if (row.expectedPublished) expect(observed.published).toEqual(row.expectedPublished)
  if (row.expectedBrokerOffsets) expect(observed.brokerOffsets).toEqual(row.expectedBrokerOffsets)
  if (row.expectedSockets) expect(observed.sockets).toEqual(row.expectedSockets)
  if (row.expectedOrder) wire.assertOrder(row.expectedOrder)
  wire.assertNoRewrite()
  wire.assertSequencesInOrder()
  wire.assertNoStaleGeneration()
  wire.assertEndTxnAfterOffsets()
  expect(observed.queued + observed.inFlight).toBe(0)
  expect(observed.admission.available).toBe(observed.admission.capacity)
  expect(
    observed.admission.reserved + observed.admission.waitingBytes + observed.admission.debts
  ).toBe(0)
  expect(observed.pins).toBe(0)
}

/**
 * Runs the rows, each in its own scenario, preserving the first failure over any cleanup failure.
 * @param {ReplayRow[]} rows
 * @returns {void}
 */
const runRows = (rows) =>
  it.each(rows.map((row) => [row.name, row]))('%s', async (_, row) => {
    const scenario = new ProducerScheduleCase(row)
    let primary
    try {
      assertRow(scenario, row, await scenario.run())
    } catch (error) {
      primary = error
    } finally {
      await finish(primary, [() => scenario.close()], () => scenario.restore())
    }
  })

/**
 * The event-order model of the replay rows below, written from the delivery rules and not from the
 * client. Time: the first send is made at 0 and its deadline is 1000, the second at 300 (deadline
 * 1300), the retry backoff is 100. Events act on the first Produce request: `answer` delivers its
 * held answer if its connection lives (else the answer is lost and the event finds nothing);
 * `timeout` fails it with a request timeout, which destroys its connection; `reconnect` destroys
 * that connection unless already gone, then runs 100 ms; `expiry` runs to 1000. Rules: a request
 * that fails is retried after the backoff with the same producer id, epoch and sequence on a new
 * connection, and the broker, which applied it when it arrived, answers the retry as a duplicate
 * (resolved with offset -1); a batch past its deadline is rejected `unknown` if it was ever
 * written and `unsent` otherwise; an idempotent partition whose written batch expired bumps its
 * epoch locally and restarts at sequence 0 before it numbers its next batch, while another
 * partition keeps its old stamp; a transaction whose batch expired is abort-only, rejects its
 * queued batches and does not retry a request that fails afterwards, and ends by `abort` (TV1 then
 * bumps its epoch with InitProducerId, TV2 adopts the pair EndTxn returns) or, when every send
 * resolved, by `commit`. A transactional row ends with `end`. TV1 sends AddPartitionsToTxn before
 * the first record of a partition.
 */

/** @type {ReplayRow[]} */
// prettier-ignore
const REPLAY_ROWS = [
  {
    name: 'default inflight-and-queued answer>timeout>reconnect>expiry',
    kind: 'idempotent',
    profile: 'default',
    shape: 'inflight-and-queued',
    fault: 'answer>timeout>reconnect>expiry',
    commands: [{ op: 'event', event: 'answer' }, { op: 'event', event: 'timeout' }, { op: 'event', event: 'reconnect' }, { op: 'event', event: 'expiry' }],
    expectedWire: [['InitProducerId', -1, -1], ['Produce', 0, 1000, 0, 0, 1], ['Produce', 0, 1000, 0, 1, 1], ['Produce', 0, 1000, 0, 1, 2]],
    expectedSettlement: [['m1', 'fulfilled', '0'], ['m2', 'fulfilled', '-1']],
    expectedState: 'READY',
    expectedLog: [[0, 1000, 0, 0, 0], [0, 1000, 0, 1, 1]],
    expectedNoops: ['fail:0'],
  },
  {
    name: 'default inflight-and-queued answer>timeout>expiry>reconnect',
    kind: 'idempotent',
    profile: 'default',
    shape: 'inflight-and-queued',
    fault: 'answer>timeout>expiry>reconnect',
    commands: [{ op: 'event', event: 'answer' }, { op: 'event', event: 'timeout' }, { op: 'event', event: 'expiry' }, { op: 'event', event: 'reconnect' }],
    expectedWire: [['InitProducerId', -1, -1], ['Produce', 0, 1000, 0, 0, 1], ['Produce', 0, 1000, 0, 1, 1], ['Produce', 0, 1000, 0, 1, 2]],
    expectedSettlement: [['m1', 'fulfilled', '0'], ['m2', 'fulfilled', '-1']],
    expectedState: 'READY',
    expectedLog: [[0, 1000, 0, 0, 0], [0, 1000, 0, 1, 1]],
    expectedNoops: ['fail:0'],
  },
  {
    name: 'default inflight-and-queued answer>reconnect>timeout>expiry',
    kind: 'idempotent',
    profile: 'default',
    shape: 'inflight-and-queued',
    fault: 'answer>reconnect>timeout>expiry',
    commands: [{ op: 'event', event: 'answer' }, { op: 'event', event: 'reconnect' }, { op: 'event', event: 'timeout' }, { op: 'event', event: 'expiry' }],
    expectedWire: [['InitProducerId', -1, -1], ['Produce', 0, 1000, 0, 0, 1], ['Produce', 0, 1000, 0, 1, 1], ['Produce', 0, 1000, 0, 1, 2]],
    expectedSettlement: [['m1', 'fulfilled', '0'], ['m2', 'fulfilled', '-1']],
    expectedState: 'READY',
    expectedLog: [[0, 1000, 0, 0, 0], [0, 1000, 0, 1, 1]],
    expectedNoops: ['fail:0'],
  },
  {
    name: 'default inflight-and-queued answer>reconnect>expiry>timeout',
    kind: 'idempotent',
    profile: 'default',
    shape: 'inflight-and-queued',
    fault: 'answer>reconnect>expiry>timeout',
    commands: [{ op: 'event', event: 'answer' }, { op: 'event', event: 'reconnect' }, { op: 'event', event: 'expiry' }, { op: 'event', event: 'timeout' }],
    expectedWire: [['InitProducerId', -1, -1], ['Produce', 0, 1000, 0, 0, 1], ['Produce', 0, 1000, 0, 1, 1], ['Produce', 0, 1000, 0, 1, 2]],
    expectedSettlement: [['m1', 'fulfilled', '0'], ['m2', 'fulfilled', '-1']],
    expectedState: 'READY',
    expectedLog: [[0, 1000, 0, 0, 0], [0, 1000, 0, 1, 1]],
    expectedNoops: ['fail:0'],
  },
  {
    name: 'default inflight-and-queued answer>expiry>timeout>reconnect',
    kind: 'idempotent',
    profile: 'default',
    shape: 'inflight-and-queued',
    fault: 'answer>expiry>timeout>reconnect',
    commands: [{ op: 'event', event: 'answer' }, { op: 'event', event: 'expiry' }, { op: 'event', event: 'timeout' }, { op: 'event', event: 'reconnect' }],
    expectedWire: [['InitProducerId', -1, -1], ['Produce', 0, 1000, 0, 0, 1], ['Produce', 0, 1000, 0, 1, 1], ['Produce', 0, 1000, 0, 1, 2]],
    expectedSettlement: [['m1', 'fulfilled', '0'], ['m2', 'fulfilled', '-1']],
    expectedState: 'READY',
    expectedLog: [[0, 1000, 0, 0, 0], [0, 1000, 0, 1, 1]],
    expectedNoops: ['fail:0'],
  },
  {
    name: 'default inflight-and-queued answer>expiry>reconnect>timeout',
    kind: 'idempotent',
    profile: 'default',
    shape: 'inflight-and-queued',
    fault: 'answer>expiry>reconnect>timeout',
    commands: [{ op: 'event', event: 'answer' }, { op: 'event', event: 'expiry' }, { op: 'event', event: 'reconnect' }, { op: 'event', event: 'timeout' }],
    expectedWire: [['InitProducerId', -1, -1], ['Produce', 0, 1000, 0, 0, 1], ['Produce', 0, 1000, 0, 1, 1], ['Produce', 0, 1000, 0, 1, 2]],
    expectedSettlement: [['m1', 'fulfilled', '0'], ['m2', 'fulfilled', '-1']],
    expectedState: 'READY',
    expectedLog: [[0, 1000, 0, 0, 0], [0, 1000, 0, 1, 1]],
    expectedNoops: ['fail:0'],
  },
  {
    name: 'default inflight-and-queued timeout>answer>reconnect>expiry',
    kind: 'idempotent',
    profile: 'default',
    shape: 'inflight-and-queued',
    fault: 'timeout>answer>reconnect>expiry',
    commands: [{ op: 'event', event: 'timeout' }, { op: 'event', event: 'answer' }, { op: 'event', event: 'reconnect' }, { op: 'event', event: 'expiry' }],
    expectedWire: [['InitProducerId', -1, -1], ['Produce', 0, 1000, 0, 0, 1], ['Produce', 0, 1000, 0, 0, 2], ['Produce', 0, 1000, 1, 0, 2]],
    expectedSettlement: [['m1', 'rejected', 'KafkaJSDeliveryTimeout', 'unknown'], ['m2', 'fulfilled', '1']],
    expectedState: 'READY',
    expectedLog: [[0, 1000, 0, 0, 0], [0, 1000, 1, 0, 1]],
    expectedNoops: ['release:0', 'destroy:0'],
  },
  {
    name: 'default inflight-and-queued timeout>answer>expiry>reconnect',
    kind: 'idempotent',
    profile: 'default',
    shape: 'inflight-and-queued',
    fault: 'timeout>answer>expiry>reconnect',
    commands: [{ op: 'event', event: 'timeout' }, { op: 'event', event: 'answer' }, { op: 'event', event: 'expiry' }, { op: 'event', event: 'reconnect' }],
    expectedWire: [['InitProducerId', -1, -1], ['Produce', 0, 1000, 0, 0, 1], ['Produce', 0, 1000, 0, 0, 2], ['Produce', 0, 1000, 1, 0, 2]],
    expectedSettlement: [['m1', 'rejected', 'KafkaJSDeliveryTimeout', 'unknown'], ['m2', 'fulfilled', '1']],
    expectedState: 'READY',
    expectedLog: [[0, 1000, 0, 0, 0], [0, 1000, 1, 0, 1]],
    expectedNoops: ['release:0', 'destroy:0'],
  },
  {
    name: 'default inflight-and-queued timeout>reconnect>answer>expiry',
    kind: 'idempotent',
    profile: 'default',
    shape: 'inflight-and-queued',
    fault: 'timeout>reconnect>answer>expiry',
    commands: [{ op: 'event', event: 'timeout' }, { op: 'event', event: 'reconnect' }, { op: 'event', event: 'answer' }, { op: 'event', event: 'expiry' }],
    expectedWire: [['InitProducerId', -1, -1], ['Produce', 0, 1000, 0, 0, 1], ['Produce', 0, 1000, 0, 0, 2], ['Produce', 0, 1000, 1, 0, 2]],
    expectedSettlement: [['m1', 'rejected', 'KafkaJSDeliveryTimeout', 'unknown'], ['m2', 'fulfilled', '1']],
    expectedState: 'READY',
    expectedLog: [[0, 1000, 0, 0, 0], [0, 1000, 1, 0, 1]],
    expectedNoops: ['destroy:0', 'release:0'],
  },
  {
    name: 'default inflight-and-queued timeout>reconnect>expiry>answer',
    kind: 'idempotent',
    profile: 'default',
    shape: 'inflight-and-queued',
    fault: 'timeout>reconnect>expiry>answer',
    commands: [{ op: 'event', event: 'timeout' }, { op: 'event', event: 'reconnect' }, { op: 'event', event: 'expiry' }, { op: 'event', event: 'answer' }],
    expectedWire: [['InitProducerId', -1, -1], ['Produce', 0, 1000, 0, 0, 1], ['Produce', 0, 1000, 0, 0, 2], ['Produce', 0, 1000, 1, 0, 2]],
    expectedSettlement: [['m1', 'rejected', 'KafkaJSDeliveryTimeout', 'unknown'], ['m2', 'fulfilled', '1']],
    expectedState: 'READY',
    expectedLog: [[0, 1000, 0, 0, 0], [0, 1000, 1, 0, 1]],
    expectedNoops: ['destroy:0', 'release:0'],
  },
  {
    name: 'default inflight-and-queued timeout>expiry>answer>reconnect',
    kind: 'idempotent',
    profile: 'default',
    shape: 'inflight-and-queued',
    fault: 'timeout>expiry>answer>reconnect',
    commands: [{ op: 'event', event: 'timeout' }, { op: 'event', event: 'expiry' }, { op: 'event', event: 'answer' }, { op: 'event', event: 'reconnect' }],
    expectedWire: [['InitProducerId', -1, -1], ['Produce', 0, 1000, 0, 0, 1], ['Produce', 0, 1000, 0, 0, 2], ['Produce', 0, 1000, 1, 0, 2]],
    expectedSettlement: [['m1', 'rejected', 'KafkaJSDeliveryTimeout', 'unknown'], ['m2', 'fulfilled', '1']],
    expectedState: 'READY',
    expectedLog: [[0, 1000, 0, 0, 0], [0, 1000, 1, 0, 1]],
    expectedNoops: ['release:0', 'destroy:0'],
  },
  {
    name: 'default inflight-and-queued timeout>expiry>reconnect>answer',
    kind: 'idempotent',
    profile: 'default',
    shape: 'inflight-and-queued',
    fault: 'timeout>expiry>reconnect>answer',
    commands: [{ op: 'event', event: 'timeout' }, { op: 'event', event: 'expiry' }, { op: 'event', event: 'reconnect' }, { op: 'event', event: 'answer' }],
    expectedWire: [['InitProducerId', -1, -1], ['Produce', 0, 1000, 0, 0, 1], ['Produce', 0, 1000, 0, 0, 2], ['Produce', 0, 1000, 1, 0, 2]],
    expectedSettlement: [['m1', 'rejected', 'KafkaJSDeliveryTimeout', 'unknown'], ['m2', 'fulfilled', '1']],
    expectedState: 'READY',
    expectedLog: [[0, 1000, 0, 0, 0], [0, 1000, 1, 0, 1]],
    expectedNoops: ['destroy:0', 'release:0'],
  },
  {
    name: 'default inflight-and-queued reconnect>answer>timeout>expiry',
    kind: 'idempotent',
    profile: 'default',
    shape: 'inflight-and-queued',
    fault: 'reconnect>answer>timeout>expiry',
    commands: [{ op: 'event', event: 'reconnect' }, { op: 'event', event: 'answer' }, { op: 'event', event: 'timeout' }, { op: 'event', event: 'expiry' }],
    expectedWire: [['InitProducerId', -1, -1], ['Produce', 0, 1000, 0, 0, 1], ['Produce', 0, 1000, 0, 0, 2], ['Produce', 0, 1000, 1, 0, 2]],
    expectedSettlement: [['m1', 'rejected', 'KafkaJSDeliveryTimeout', 'unknown'], ['m2', 'fulfilled', '1']],
    expectedState: 'READY',
    expectedLog: [[0, 1000, 0, 0, 0], [0, 1000, 1, 0, 1]],
    expectedNoops: ['release:0', 'fail:0'],
  },
  {
    name: 'default inflight-and-queued reconnect>answer>expiry>timeout',
    kind: 'idempotent',
    profile: 'default',
    shape: 'inflight-and-queued',
    fault: 'reconnect>answer>expiry>timeout',
    commands: [{ op: 'event', event: 'reconnect' }, { op: 'event', event: 'answer' }, { op: 'event', event: 'expiry' }, { op: 'event', event: 'timeout' }],
    expectedWire: [['InitProducerId', -1, -1], ['Produce', 0, 1000, 0, 0, 1], ['Produce', 0, 1000, 0, 0, 2], ['Produce', 0, 1000, 1, 0, 2]],
    expectedSettlement: [['m1', 'rejected', 'KafkaJSDeliveryTimeout', 'unknown'], ['m2', 'fulfilled', '1']],
    expectedState: 'READY',
    expectedLog: [[0, 1000, 0, 0, 0], [0, 1000, 1, 0, 1]],
    expectedNoops: ['release:0', 'fail:0'],
  },
  {
    name: 'default inflight-and-queued reconnect>timeout>answer>expiry',
    kind: 'idempotent',
    profile: 'default',
    shape: 'inflight-and-queued',
    fault: 'reconnect>timeout>answer>expiry',
    commands: [{ op: 'event', event: 'reconnect' }, { op: 'event', event: 'timeout' }, { op: 'event', event: 'answer' }, { op: 'event', event: 'expiry' }],
    expectedWire: [['InitProducerId', -1, -1], ['Produce', 0, 1000, 0, 0, 1], ['Produce', 0, 1000, 0, 0, 2], ['Produce', 0, 1000, 1, 0, 2]],
    expectedSettlement: [['m1', 'rejected', 'KafkaJSDeliveryTimeout', 'unknown'], ['m2', 'fulfilled', '1']],
    expectedState: 'READY',
    expectedLog: [[0, 1000, 0, 0, 0], [0, 1000, 1, 0, 1]],
    expectedNoops: ['fail:0', 'release:0'],
  },
  {
    name: 'default inflight-and-queued reconnect>timeout>expiry>answer',
    kind: 'idempotent',
    profile: 'default',
    shape: 'inflight-and-queued',
    fault: 'reconnect>timeout>expiry>answer',
    commands: [{ op: 'event', event: 'reconnect' }, { op: 'event', event: 'timeout' }, { op: 'event', event: 'expiry' }, { op: 'event', event: 'answer' }],
    expectedWire: [['InitProducerId', -1, -1], ['Produce', 0, 1000, 0, 0, 1], ['Produce', 0, 1000, 0, 0, 2], ['Produce', 0, 1000, 1, 0, 2]],
    expectedSettlement: [['m1', 'rejected', 'KafkaJSDeliveryTimeout', 'unknown'], ['m2', 'fulfilled', '1']],
    expectedState: 'READY',
    expectedLog: [[0, 1000, 0, 0, 0], [0, 1000, 1, 0, 1]],
    expectedNoops: ['fail:0', 'release:0'],
  },
  {
    name: 'default inflight-and-queued reconnect>expiry>answer>timeout',
    kind: 'idempotent',
    profile: 'default',
    shape: 'inflight-and-queued',
    fault: 'reconnect>expiry>answer>timeout',
    commands: [{ op: 'event', event: 'reconnect' }, { op: 'event', event: 'expiry' }, { op: 'event', event: 'answer' }, { op: 'event', event: 'timeout' }],
    expectedWire: [['InitProducerId', -1, -1], ['Produce', 0, 1000, 0, 0, 1], ['Produce', 0, 1000, 0, 0, 2], ['Produce', 0, 1000, 1, 0, 2]],
    expectedSettlement: [['m1', 'rejected', 'KafkaJSDeliveryTimeout', 'unknown'], ['m2', 'fulfilled', '1']],
    expectedState: 'READY',
    expectedLog: [[0, 1000, 0, 0, 0], [0, 1000, 1, 0, 1]],
    expectedNoops: ['release:0', 'fail:0'],
  },
  {
    name: 'default inflight-and-queued reconnect>expiry>timeout>answer',
    kind: 'idempotent',
    profile: 'default',
    shape: 'inflight-and-queued',
    fault: 'reconnect>expiry>timeout>answer',
    commands: [{ op: 'event', event: 'reconnect' }, { op: 'event', event: 'expiry' }, { op: 'event', event: 'timeout' }, { op: 'event', event: 'answer' }],
    expectedWire: [['InitProducerId', -1, -1], ['Produce', 0, 1000, 0, 0, 1], ['Produce', 0, 1000, 0, 0, 2], ['Produce', 0, 1000, 1, 0, 2]],
    expectedSettlement: [['m1', 'rejected', 'KafkaJSDeliveryTimeout', 'unknown'], ['m2', 'fulfilled', '1']],
    expectedState: 'READY',
    expectedLog: [[0, 1000, 0, 0, 0], [0, 1000, 1, 0, 1]],
    expectedNoops: ['fail:0', 'release:0'],
  },
  {
    name: 'default inflight-and-queued expiry>answer>timeout>reconnect',
    kind: 'idempotent',
    profile: 'default',
    shape: 'inflight-and-queued',
    fault: 'expiry>answer>timeout>reconnect',
    commands: [{ op: 'event', event: 'expiry' }, { op: 'event', event: 'answer' }, { op: 'event', event: 'timeout' }, { op: 'event', event: 'reconnect' }],
    expectedWire: [['InitProducerId', -1, -1], ['Produce', 0, 1000, 0, 0, 1], ['Produce', 0, 1000, 1, 0, 1], ['Produce', 0, 1000, 1, 0, 2]],
    expectedSettlement: [['m1', 'rejected', 'KafkaJSDeliveryTimeout', 'unknown'], ['m2', 'fulfilled', '-1']],
    expectedState: 'READY',
    expectedLog: [[0, 1000, 0, 0, 0], [0, 1000, 1, 0, 1]],
    expectedNoops: ['fail:0'],
  },
  {
    name: 'default inflight-and-queued expiry>answer>reconnect>timeout',
    kind: 'idempotent',
    profile: 'default',
    shape: 'inflight-and-queued',
    fault: 'expiry>answer>reconnect>timeout',
    commands: [{ op: 'event', event: 'expiry' }, { op: 'event', event: 'answer' }, { op: 'event', event: 'reconnect' }, { op: 'event', event: 'timeout' }],
    expectedWire: [['InitProducerId', -1, -1], ['Produce', 0, 1000, 0, 0, 1], ['Produce', 0, 1000, 1, 0, 1], ['Produce', 0, 1000, 1, 0, 2]],
    expectedSettlement: [['m1', 'rejected', 'KafkaJSDeliveryTimeout', 'unknown'], ['m2', 'fulfilled', '-1']],
    expectedState: 'READY',
    expectedLog: [[0, 1000, 0, 0, 0], [0, 1000, 1, 0, 1]],
    expectedNoops: ['fail:0'],
  },
  {
    name: 'default inflight-and-queued expiry>timeout>answer>reconnect',
    kind: 'idempotent',
    profile: 'default',
    shape: 'inflight-and-queued',
    fault: 'expiry>timeout>answer>reconnect',
    commands: [{ op: 'event', event: 'expiry' }, { op: 'event', event: 'timeout' }, { op: 'event', event: 'answer' }, { op: 'event', event: 'reconnect' }],
    expectedWire: [['InitProducerId', -1, -1], ['Produce', 0, 1000, 0, 0, 1], ['Produce', 0, 1000, 1, 0, 1], ['Produce', 0, 1000, 1, 0, 2]],
    expectedSettlement: [['m1', 'rejected', 'KafkaJSDeliveryTimeout', 'unknown'], ['m2', 'fulfilled', '-1']],
    expectedState: 'READY',
    expectedLog: [[0, 1000, 0, 0, 0], [0, 1000, 1, 0, 1]],
    expectedNoops: ['release:0', 'destroy:0'],
  },
  {
    name: 'default inflight-and-queued expiry>timeout>reconnect>answer',
    kind: 'idempotent',
    profile: 'default',
    shape: 'inflight-and-queued',
    fault: 'expiry>timeout>reconnect>answer',
    commands: [{ op: 'event', event: 'expiry' }, { op: 'event', event: 'timeout' }, { op: 'event', event: 'reconnect' }, { op: 'event', event: 'answer' }],
    expectedWire: [['InitProducerId', -1, -1], ['Produce', 0, 1000, 0, 0, 1], ['Produce', 0, 1000, 1, 0, 1], ['Produce', 0, 1000, 1, 0, 2]],
    expectedSettlement: [['m1', 'rejected', 'KafkaJSDeliveryTimeout', 'unknown'], ['m2', 'fulfilled', '-1']],
    expectedState: 'READY',
    expectedLog: [[0, 1000, 0, 0, 0], [0, 1000, 1, 0, 1]],
    expectedNoops: ['destroy:0', 'release:0'],
  },
  {
    name: 'default inflight-and-queued expiry>reconnect>answer>timeout',
    kind: 'idempotent',
    profile: 'default',
    shape: 'inflight-and-queued',
    fault: 'expiry>reconnect>answer>timeout',
    commands: [{ op: 'event', event: 'expiry' }, { op: 'event', event: 'reconnect' }, { op: 'event', event: 'answer' }, { op: 'event', event: 'timeout' }],
    expectedWire: [['InitProducerId', -1, -1], ['Produce', 0, 1000, 0, 0, 1], ['Produce', 0, 1000, 1, 0, 1], ['Produce', 0, 1000, 1, 0, 2]],
    expectedSettlement: [['m1', 'rejected', 'KafkaJSDeliveryTimeout', 'unknown'], ['m2', 'fulfilled', '-1']],
    expectedState: 'READY',
    expectedLog: [[0, 1000, 0, 0, 0], [0, 1000, 1, 0, 1]],
    expectedNoops: ['release:0', 'fail:0'],
  },
  {
    name: 'default inflight-and-queued expiry>reconnect>timeout>answer',
    kind: 'idempotent',
    profile: 'default',
    shape: 'inflight-and-queued',
    fault: 'expiry>reconnect>timeout>answer',
    commands: [{ op: 'event', event: 'expiry' }, { op: 'event', event: 'reconnect' }, { op: 'event', event: 'timeout' }, { op: 'event', event: 'answer' }],
    expectedWire: [['InitProducerId', -1, -1], ['Produce', 0, 1000, 0, 0, 1], ['Produce', 0, 1000, 1, 0, 1], ['Produce', 0, 1000, 1, 0, 2]],
    expectedSettlement: [['m1', 'rejected', 'KafkaJSDeliveryTimeout', 'unknown'], ['m2', 'fulfilled', '-1']],
    expectedState: 'READY',
    expectedLog: [[0, 1000, 0, 0, 0], [0, 1000, 1, 0, 1]],
    expectedNoops: ['fail:0', 'release:0'],
  },
  {
    name: 'default two-inflight answer>timeout>reconnect>expiry',
    kind: 'idempotent',
    profile: 'default',
    shape: 'two-inflight',
    fault: 'answer>timeout>reconnect>expiry',
    commands: [{ op: 'event', event: 'answer' }, { op: 'event', event: 'timeout' }, { op: 'event', event: 'reconnect' }, { op: 'event', event: 'expiry' }],
    expectedWire: [['InitProducerId', -1, -1], ['Produce', 0, 1000, 0, 0, 1], ['Produce', 1, 1000, 0, 0, 1], ['Produce', 1, 1000, 0, 0, 2]],
    expectedSettlement: [['m1', 'fulfilled', '0'], ['m2', 'fulfilled', '-1']],
    expectedState: 'READY',
    expectedLog: [[0, 1000, 0, 0, 0], [1, 1000, 0, 0, 0]],
    expectedNoops: ['fail:0'],
  },
  {
    name: 'default two-inflight answer>timeout>expiry>reconnect',
    kind: 'idempotent',
    profile: 'default',
    shape: 'two-inflight',
    fault: 'answer>timeout>expiry>reconnect',
    commands: [{ op: 'event', event: 'answer' }, { op: 'event', event: 'timeout' }, { op: 'event', event: 'expiry' }, { op: 'event', event: 'reconnect' }],
    expectedWire: [['InitProducerId', -1, -1], ['Produce', 0, 1000, 0, 0, 1], ['Produce', 1, 1000, 0, 0, 1], ['Produce', 1, 1000, 0, 0, 2]],
    expectedSettlement: [['m1', 'fulfilled', '0'], ['m2', 'fulfilled', '-1']],
    expectedState: 'READY',
    expectedLog: [[0, 1000, 0, 0, 0], [1, 1000, 0, 0, 0]],
    expectedNoops: ['fail:0'],
  },
  {
    name: 'default two-inflight answer>reconnect>timeout>expiry',
    kind: 'idempotent',
    profile: 'default',
    shape: 'two-inflight',
    fault: 'answer>reconnect>timeout>expiry',
    commands: [{ op: 'event', event: 'answer' }, { op: 'event', event: 'reconnect' }, { op: 'event', event: 'timeout' }, { op: 'event', event: 'expiry' }],
    expectedWire: [['InitProducerId', -1, -1], ['Produce', 0, 1000, 0, 0, 1], ['Produce', 1, 1000, 0, 0, 1], ['Produce', 1, 1000, 0, 0, 2]],
    expectedSettlement: [['m1', 'fulfilled', '0'], ['m2', 'fulfilled', '-1']],
    expectedState: 'READY',
    expectedLog: [[0, 1000, 0, 0, 0], [1, 1000, 0, 0, 0]],
    expectedNoops: ['fail:0'],
  },
  {
    name: 'default two-inflight answer>reconnect>expiry>timeout',
    kind: 'idempotent',
    profile: 'default',
    shape: 'two-inflight',
    fault: 'answer>reconnect>expiry>timeout',
    commands: [{ op: 'event', event: 'answer' }, { op: 'event', event: 'reconnect' }, { op: 'event', event: 'expiry' }, { op: 'event', event: 'timeout' }],
    expectedWire: [['InitProducerId', -1, -1], ['Produce', 0, 1000, 0, 0, 1], ['Produce', 1, 1000, 0, 0, 1], ['Produce', 1, 1000, 0, 0, 2]],
    expectedSettlement: [['m1', 'fulfilled', '0'], ['m2', 'fulfilled', '-1']],
    expectedState: 'READY',
    expectedLog: [[0, 1000, 0, 0, 0], [1, 1000, 0, 0, 0]],
    expectedNoops: ['fail:0'],
  },
  {
    name: 'default two-inflight answer>expiry>timeout>reconnect',
    kind: 'idempotent',
    profile: 'default',
    shape: 'two-inflight',
    fault: 'answer>expiry>timeout>reconnect',
    commands: [{ op: 'event', event: 'answer' }, { op: 'event', event: 'expiry' }, { op: 'event', event: 'timeout' }, { op: 'event', event: 'reconnect' }],
    expectedWire: [['InitProducerId', -1, -1], ['Produce', 0, 1000, 0, 0, 1], ['Produce', 1, 1000, 0, 0, 1], ['Produce', 1, 1000, 0, 0, 2]],
    expectedSettlement: [['m1', 'fulfilled', '0'], ['m2', 'fulfilled', '-1']],
    expectedState: 'READY',
    expectedLog: [[0, 1000, 0, 0, 0], [1, 1000, 0, 0, 0]],
    expectedNoops: ['fail:0'],
  },
  {
    name: 'default two-inflight answer>expiry>reconnect>timeout',
    kind: 'idempotent',
    profile: 'default',
    shape: 'two-inflight',
    fault: 'answer>expiry>reconnect>timeout',
    commands: [{ op: 'event', event: 'answer' }, { op: 'event', event: 'expiry' }, { op: 'event', event: 'reconnect' }, { op: 'event', event: 'timeout' }],
    expectedWire: [['InitProducerId', -1, -1], ['Produce', 0, 1000, 0, 0, 1], ['Produce', 1, 1000, 0, 0, 1], ['Produce', 1, 1000, 0, 0, 2]],
    expectedSettlement: [['m1', 'fulfilled', '0'], ['m2', 'fulfilled', '-1']],
    expectedState: 'READY',
    expectedLog: [[0, 1000, 0, 0, 0], [1, 1000, 0, 0, 0]],
    expectedNoops: ['fail:0'],
  },
  {
    name: 'default two-inflight timeout>answer>reconnect>expiry',
    kind: 'idempotent',
    profile: 'default',
    shape: 'two-inflight',
    fault: 'timeout>answer>reconnect>expiry',
    commands: [{ op: 'event', event: 'timeout' }, { op: 'event', event: 'answer' }, { op: 'event', event: 'reconnect' }, { op: 'event', event: 'expiry' }],
    expectedWire: [['InitProducerId', -1, -1], ['Produce', 0, 1000, 0, 0, 1], ['Produce', 1, 1000, 0, 0, 1], ['Produce', 0, 1000, 0, 0, 2], ['Produce', 1, 1000, 0, 0, 2]],
    expectedSettlement: [['m1', 'rejected', 'KafkaJSDeliveryTimeout', 'unknown'], ['m2', 'fulfilled', '-1']],
    expectedState: 'READY',
    expectedLog: [[0, 1000, 0, 0, 0], [1, 1000, 0, 0, 0]],
    expectedNoops: ['release:0', 'destroy:0'],
  },
  {
    name: 'default two-inflight timeout>answer>expiry>reconnect',
    kind: 'idempotent',
    profile: 'default',
    shape: 'two-inflight',
    fault: 'timeout>answer>expiry>reconnect',
    commands: [{ op: 'event', event: 'timeout' }, { op: 'event', event: 'answer' }, { op: 'event', event: 'expiry' }, { op: 'event', event: 'reconnect' }],
    expectedWire: [['InitProducerId', -1, -1], ['Produce', 0, 1000, 0, 0, 1], ['Produce', 1, 1000, 0, 0, 1], ['Produce', 0, 1000, 0, 0, 2], ['Produce', 1, 1000, 0, 0, 2]],
    expectedSettlement: [['m1', 'rejected', 'KafkaJSDeliveryTimeout', 'unknown'], ['m2', 'fulfilled', '-1']],
    expectedState: 'READY',
    expectedLog: [[0, 1000, 0, 0, 0], [1, 1000, 0, 0, 0]],
    expectedNoops: ['release:0', 'destroy:0'],
  },
  {
    name: 'default two-inflight timeout>reconnect>answer>expiry',
    kind: 'idempotent',
    profile: 'default',
    shape: 'two-inflight',
    fault: 'timeout>reconnect>answer>expiry',
    commands: [{ op: 'event', event: 'timeout' }, { op: 'event', event: 'reconnect' }, { op: 'event', event: 'answer' }, { op: 'event', event: 'expiry' }],
    expectedWire: [['InitProducerId', -1, -1], ['Produce', 0, 1000, 0, 0, 1], ['Produce', 1, 1000, 0, 0, 1], ['Produce', 0, 1000, 0, 0, 2], ['Produce', 1, 1000, 0, 0, 2]],
    expectedSettlement: [['m1', 'rejected', 'KafkaJSDeliveryTimeout', 'unknown'], ['m2', 'fulfilled', '-1']],
    expectedState: 'READY',
    expectedLog: [[0, 1000, 0, 0, 0], [1, 1000, 0, 0, 0]],
    expectedNoops: ['destroy:0', 'release:0'],
  },
  {
    name: 'default two-inflight timeout>reconnect>expiry>answer',
    kind: 'idempotent',
    profile: 'default',
    shape: 'two-inflight',
    fault: 'timeout>reconnect>expiry>answer',
    commands: [{ op: 'event', event: 'timeout' }, { op: 'event', event: 'reconnect' }, { op: 'event', event: 'expiry' }, { op: 'event', event: 'answer' }],
    expectedWire: [['InitProducerId', -1, -1], ['Produce', 0, 1000, 0, 0, 1], ['Produce', 1, 1000, 0, 0, 1], ['Produce', 0, 1000, 0, 0, 2], ['Produce', 1, 1000, 0, 0, 2]],
    expectedSettlement: [['m1', 'rejected', 'KafkaJSDeliveryTimeout', 'unknown'], ['m2', 'fulfilled', '-1']],
    expectedState: 'READY',
    expectedLog: [[0, 1000, 0, 0, 0], [1, 1000, 0, 0, 0]],
    expectedNoops: ['destroy:0', 'release:0'],
  },
  {
    name: 'default two-inflight timeout>expiry>answer>reconnect',
    kind: 'idempotent',
    profile: 'default',
    shape: 'two-inflight',
    fault: 'timeout>expiry>answer>reconnect',
    commands: [{ op: 'event', event: 'timeout' }, { op: 'event', event: 'expiry' }, { op: 'event', event: 'answer' }, { op: 'event', event: 'reconnect' }],
    expectedWire: [['InitProducerId', -1, -1], ['Produce', 0, 1000, 0, 0, 1], ['Produce', 1, 1000, 0, 0, 1], ['Produce', 0, 1000, 0, 0, 2], ['Produce', 1, 1000, 0, 0, 2]],
    expectedSettlement: [['m1', 'rejected', 'KafkaJSDeliveryTimeout', 'unknown'], ['m2', 'fulfilled', '-1']],
    expectedState: 'READY',
    expectedLog: [[0, 1000, 0, 0, 0], [1, 1000, 0, 0, 0]],
    expectedNoops: ['release:0', 'destroy:0'],
  },
  {
    name: 'default two-inflight timeout>expiry>reconnect>answer',
    kind: 'idempotent',
    profile: 'default',
    shape: 'two-inflight',
    fault: 'timeout>expiry>reconnect>answer',
    commands: [{ op: 'event', event: 'timeout' }, { op: 'event', event: 'expiry' }, { op: 'event', event: 'reconnect' }, { op: 'event', event: 'answer' }],
    expectedWire: [['InitProducerId', -1, -1], ['Produce', 0, 1000, 0, 0, 1], ['Produce', 1, 1000, 0, 0, 1], ['Produce', 0, 1000, 0, 0, 2], ['Produce', 1, 1000, 0, 0, 2]],
    expectedSettlement: [['m1', 'rejected', 'KafkaJSDeliveryTimeout', 'unknown'], ['m2', 'fulfilled', '-1']],
    expectedState: 'READY',
    expectedLog: [[0, 1000, 0, 0, 0], [1, 1000, 0, 0, 0]],
    expectedNoops: ['destroy:0', 'release:0'],
  },
  {
    name: 'default two-inflight reconnect>answer>timeout>expiry',
    kind: 'idempotent',
    profile: 'default',
    shape: 'two-inflight',
    fault: 'reconnect>answer>timeout>expiry',
    commands: [{ op: 'event', event: 'reconnect' }, { op: 'event', event: 'answer' }, { op: 'event', event: 'timeout' }, { op: 'event', event: 'expiry' }],
    expectedWire: [['InitProducerId', -1, -1], ['Produce', 0, 1000, 0, 0, 1], ['Produce', 1, 1000, 0, 0, 1], ['Produce', 0, 1000, 0, 0, 2], ['Produce', 1, 1000, 0, 0, 2]],
    expectedSettlement: [['m1', 'rejected', 'KafkaJSDeliveryTimeout', 'unknown'], ['m2', 'fulfilled', '-1']],
    expectedState: 'READY',
    expectedLog: [[0, 1000, 0, 0, 0], [1, 1000, 0, 0, 0]],
    expectedNoops: ['release:0', 'fail:0'],
  },
  {
    name: 'default two-inflight reconnect>answer>expiry>timeout',
    kind: 'idempotent',
    profile: 'default',
    shape: 'two-inflight',
    fault: 'reconnect>answer>expiry>timeout',
    commands: [{ op: 'event', event: 'reconnect' }, { op: 'event', event: 'answer' }, { op: 'event', event: 'expiry' }, { op: 'event', event: 'timeout' }],
    expectedWire: [['InitProducerId', -1, -1], ['Produce', 0, 1000, 0, 0, 1], ['Produce', 1, 1000, 0, 0, 1], ['Produce', 0, 1000, 0, 0, 2], ['Produce', 1, 1000, 0, 0, 2]],
    expectedSettlement: [['m1', 'rejected', 'KafkaJSDeliveryTimeout', 'unknown'], ['m2', 'fulfilled', '-1']],
    expectedState: 'READY',
    expectedLog: [[0, 1000, 0, 0, 0], [1, 1000, 0, 0, 0]],
    expectedNoops: ['release:0', 'fail:0'],
  },
  {
    name: 'default two-inflight reconnect>timeout>answer>expiry',
    kind: 'idempotent',
    profile: 'default',
    shape: 'two-inflight',
    fault: 'reconnect>timeout>answer>expiry',
    commands: [{ op: 'event', event: 'reconnect' }, { op: 'event', event: 'timeout' }, { op: 'event', event: 'answer' }, { op: 'event', event: 'expiry' }],
    expectedWire: [['InitProducerId', -1, -1], ['Produce', 0, 1000, 0, 0, 1], ['Produce', 1, 1000, 0, 0, 1], ['Produce', 0, 1000, 0, 0, 2], ['Produce', 1, 1000, 0, 0, 2]],
    expectedSettlement: [['m1', 'rejected', 'KafkaJSDeliveryTimeout', 'unknown'], ['m2', 'fulfilled', '-1']],
    expectedState: 'READY',
    expectedLog: [[0, 1000, 0, 0, 0], [1, 1000, 0, 0, 0]],
    expectedNoops: ['fail:0', 'release:0'],
  },
  {
    name: 'default two-inflight reconnect>timeout>expiry>answer',
    kind: 'idempotent',
    profile: 'default',
    shape: 'two-inflight',
    fault: 'reconnect>timeout>expiry>answer',
    commands: [{ op: 'event', event: 'reconnect' }, { op: 'event', event: 'timeout' }, { op: 'event', event: 'expiry' }, { op: 'event', event: 'answer' }],
    expectedWire: [['InitProducerId', -1, -1], ['Produce', 0, 1000, 0, 0, 1], ['Produce', 1, 1000, 0, 0, 1], ['Produce', 0, 1000, 0, 0, 2], ['Produce', 1, 1000, 0, 0, 2]],
    expectedSettlement: [['m1', 'rejected', 'KafkaJSDeliveryTimeout', 'unknown'], ['m2', 'fulfilled', '-1']],
    expectedState: 'READY',
    expectedLog: [[0, 1000, 0, 0, 0], [1, 1000, 0, 0, 0]],
    expectedNoops: ['fail:0', 'release:0'],
  },
  {
    name: 'default two-inflight reconnect>expiry>answer>timeout',
    kind: 'idempotent',
    profile: 'default',
    shape: 'two-inflight',
    fault: 'reconnect>expiry>answer>timeout',
    commands: [{ op: 'event', event: 'reconnect' }, { op: 'event', event: 'expiry' }, { op: 'event', event: 'answer' }, { op: 'event', event: 'timeout' }],
    expectedWire: [['InitProducerId', -1, -1], ['Produce', 0, 1000, 0, 0, 1], ['Produce', 1, 1000, 0, 0, 1], ['Produce', 0, 1000, 0, 0, 2], ['Produce', 1, 1000, 0, 0, 2]],
    expectedSettlement: [['m1', 'rejected', 'KafkaJSDeliveryTimeout', 'unknown'], ['m2', 'fulfilled', '-1']],
    expectedState: 'READY',
    expectedLog: [[0, 1000, 0, 0, 0], [1, 1000, 0, 0, 0]],
    expectedNoops: ['release:0', 'fail:0'],
  },
  {
    name: 'default two-inflight reconnect>expiry>timeout>answer',
    kind: 'idempotent',
    profile: 'default',
    shape: 'two-inflight',
    fault: 'reconnect>expiry>timeout>answer',
    commands: [{ op: 'event', event: 'reconnect' }, { op: 'event', event: 'expiry' }, { op: 'event', event: 'timeout' }, { op: 'event', event: 'answer' }],
    expectedWire: [['InitProducerId', -1, -1], ['Produce', 0, 1000, 0, 0, 1], ['Produce', 1, 1000, 0, 0, 1], ['Produce', 0, 1000, 0, 0, 2], ['Produce', 1, 1000, 0, 0, 2]],
    expectedSettlement: [['m1', 'rejected', 'KafkaJSDeliveryTimeout', 'unknown'], ['m2', 'fulfilled', '-1']],
    expectedState: 'READY',
    expectedLog: [[0, 1000, 0, 0, 0], [1, 1000, 0, 0, 0]],
    expectedNoops: ['fail:0', 'release:0'],
  },
  {
    name: 'default two-inflight expiry>answer>timeout>reconnect',
    kind: 'idempotent',
    profile: 'default',
    shape: 'two-inflight',
    fault: 'expiry>answer>timeout>reconnect',
    commands: [{ op: 'event', event: 'expiry' }, { op: 'event', event: 'answer' }, { op: 'event', event: 'timeout' }, { op: 'event', event: 'reconnect' }],
    expectedWire: [['InitProducerId', -1, -1], ['Produce', 0, 1000, 0, 0, 1], ['Produce', 1, 1000, 0, 0, 1], ['Produce', 1, 1000, 0, 0, 2]],
    expectedSettlement: [['m1', 'rejected', 'KafkaJSDeliveryTimeout', 'unknown'], ['m2', 'fulfilled', '-1']],
    expectedState: 'READY',
    expectedLog: [[0, 1000, 0, 0, 0], [1, 1000, 0, 0, 0]],
    expectedNoops: ['fail:0'],
  },
  {
    name: 'default two-inflight expiry>answer>reconnect>timeout',
    kind: 'idempotent',
    profile: 'default',
    shape: 'two-inflight',
    fault: 'expiry>answer>reconnect>timeout',
    commands: [{ op: 'event', event: 'expiry' }, { op: 'event', event: 'answer' }, { op: 'event', event: 'reconnect' }, { op: 'event', event: 'timeout' }],
    expectedWire: [['InitProducerId', -1, -1], ['Produce', 0, 1000, 0, 0, 1], ['Produce', 1, 1000, 0, 0, 1], ['Produce', 1, 1000, 0, 0, 2]],
    expectedSettlement: [['m1', 'rejected', 'KafkaJSDeliveryTimeout', 'unknown'], ['m2', 'fulfilled', '-1']],
    expectedState: 'READY',
    expectedLog: [[0, 1000, 0, 0, 0], [1, 1000, 0, 0, 0]],
    expectedNoops: ['fail:0'],
  },
  {
    name: 'default two-inflight expiry>timeout>answer>reconnect',
    kind: 'idempotent',
    profile: 'default',
    shape: 'two-inflight',
    fault: 'expiry>timeout>answer>reconnect',
    commands: [{ op: 'event', event: 'expiry' }, { op: 'event', event: 'timeout' }, { op: 'event', event: 'answer' }, { op: 'event', event: 'reconnect' }],
    expectedWire: [['InitProducerId', -1, -1], ['Produce', 0, 1000, 0, 0, 1], ['Produce', 1, 1000, 0, 0, 1], ['Produce', 1, 1000, 0, 0, 2]],
    expectedSettlement: [['m1', 'rejected', 'KafkaJSDeliveryTimeout', 'unknown'], ['m2', 'fulfilled', '-1']],
    expectedState: 'READY',
    expectedLog: [[0, 1000, 0, 0, 0], [1, 1000, 0, 0, 0]],
    expectedNoops: ['release:0', 'destroy:0'],
  },
  {
    name: 'default two-inflight expiry>timeout>reconnect>answer',
    kind: 'idempotent',
    profile: 'default',
    shape: 'two-inflight',
    fault: 'expiry>timeout>reconnect>answer',
    commands: [{ op: 'event', event: 'expiry' }, { op: 'event', event: 'timeout' }, { op: 'event', event: 'reconnect' }, { op: 'event', event: 'answer' }],
    expectedWire: [['InitProducerId', -1, -1], ['Produce', 0, 1000, 0, 0, 1], ['Produce', 1, 1000, 0, 0, 1], ['Produce', 1, 1000, 0, 0, 2]],
    expectedSettlement: [['m1', 'rejected', 'KafkaJSDeliveryTimeout', 'unknown'], ['m2', 'fulfilled', '-1']],
    expectedState: 'READY',
    expectedLog: [[0, 1000, 0, 0, 0], [1, 1000, 0, 0, 0]],
    expectedNoops: ['destroy:0', 'release:0'],
  },
  {
    name: 'default two-inflight expiry>reconnect>answer>timeout',
    kind: 'idempotent',
    profile: 'default',
    shape: 'two-inflight',
    fault: 'expiry>reconnect>answer>timeout',
    commands: [{ op: 'event', event: 'expiry' }, { op: 'event', event: 'reconnect' }, { op: 'event', event: 'answer' }, { op: 'event', event: 'timeout' }],
    expectedWire: [['InitProducerId', -1, -1], ['Produce', 0, 1000, 0, 0, 1], ['Produce', 1, 1000, 0, 0, 1], ['Produce', 1, 1000, 0, 0, 2]],
    expectedSettlement: [['m1', 'rejected', 'KafkaJSDeliveryTimeout', 'unknown'], ['m2', 'fulfilled', '-1']],
    expectedState: 'READY',
    expectedLog: [[0, 1000, 0, 0, 0], [1, 1000, 0, 0, 0]],
    expectedNoops: ['release:0', 'fail:0'],
  },
  {
    name: 'default two-inflight expiry>reconnect>timeout>answer',
    kind: 'idempotent',
    profile: 'default',
    shape: 'two-inflight',
    fault: 'expiry>reconnect>timeout>answer',
    commands: [{ op: 'event', event: 'expiry' }, { op: 'event', event: 'reconnect' }, { op: 'event', event: 'timeout' }, { op: 'event', event: 'answer' }],
    expectedWire: [['InitProducerId', -1, -1], ['Produce', 0, 1000, 0, 0, 1], ['Produce', 1, 1000, 0, 0, 1], ['Produce', 1, 1000, 0, 0, 2]],
    expectedSettlement: [['m1', 'rejected', 'KafkaJSDeliveryTimeout', 'unknown'], ['m2', 'fulfilled', '-1']],
    expectedState: 'READY',
    expectedLog: [[0, 1000, 0, 0, 0], [1, 1000, 0, 0, 0]],
    expectedNoops: ['fail:0', 'release:0'],
  },
  {
    name: 'tv1 inflight-and-queued answer>timeout>reconnect>expiry',
    kind: 'transactional',
    profile: 'tv1',
    shape: 'inflight-and-queued',
    fault: 'answer>timeout>reconnect>expiry',
    commands: [{ op: 'event', event: 'answer' }, { op: 'event', event: 'timeout' }, { op: 'event', event: 'reconnect' }, { op: 'event', event: 'expiry' }],
    expectedWire: [['InitProducerId', -1, -1], ['InitProducerId', -1, -1], ['AddPartitionsToTxn', [0]], ['Produce', 0, 1000, 0, 0, 1], ['Produce', 0, 1000, 0, 1, 1], ['Produce', 0, 1000, 0, 1, 2], ['EndTxn', 'commit', 1000, 0]],
    expectedSettlement: [['m1', 'fulfilled', '0'], ['m2', 'fulfilled', '-1'], ['end', 'fulfilled']],
    expectedState: 'READY',
    expectedLog: [[0, 1000, 0, 0, 0], [0, 1000, 0, 1, 1]],
    expectedNoops: ['fail:0'],
  },
  {
    name: 'tv1 inflight-and-queued answer>timeout>expiry>reconnect',
    kind: 'transactional',
    profile: 'tv1',
    shape: 'inflight-and-queued',
    fault: 'answer>timeout>expiry>reconnect',
    commands: [{ op: 'event', event: 'answer' }, { op: 'event', event: 'timeout' }, { op: 'event', event: 'expiry' }, { op: 'event', event: 'reconnect' }],
    expectedWire: [['InitProducerId', -1, -1], ['InitProducerId', -1, -1], ['AddPartitionsToTxn', [0]], ['Produce', 0, 1000, 0, 0, 1], ['Produce', 0, 1000, 0, 1, 1], ['Produce', 0, 1000, 0, 1, 2], ['EndTxn', 'commit', 1000, 0]],
    expectedSettlement: [['m1', 'fulfilled', '0'], ['m2', 'fulfilled', '-1'], ['end', 'fulfilled']],
    expectedState: 'READY',
    expectedLog: [[0, 1000, 0, 0, 0], [0, 1000, 0, 1, 1]],
    expectedNoops: ['fail:0'],
  },
  {
    name: 'tv1 inflight-and-queued answer>reconnect>timeout>expiry',
    kind: 'transactional',
    profile: 'tv1',
    shape: 'inflight-and-queued',
    fault: 'answer>reconnect>timeout>expiry',
    commands: [{ op: 'event', event: 'answer' }, { op: 'event', event: 'reconnect' }, { op: 'event', event: 'timeout' }, { op: 'event', event: 'expiry' }],
    expectedWire: [['InitProducerId', -1, -1], ['InitProducerId', -1, -1], ['AddPartitionsToTxn', [0]], ['Produce', 0, 1000, 0, 0, 1], ['Produce', 0, 1000, 0, 1, 1], ['Produce', 0, 1000, 0, 1, 2], ['EndTxn', 'commit', 1000, 0]],
    expectedSettlement: [['m1', 'fulfilled', '0'], ['m2', 'fulfilled', '-1'], ['end', 'fulfilled']],
    expectedState: 'READY',
    expectedLog: [[0, 1000, 0, 0, 0], [0, 1000, 0, 1, 1]],
    expectedNoops: ['fail:0'],
  },
  {
    name: 'tv1 inflight-and-queued answer>reconnect>expiry>timeout',
    kind: 'transactional',
    profile: 'tv1',
    shape: 'inflight-and-queued',
    fault: 'answer>reconnect>expiry>timeout',
    commands: [{ op: 'event', event: 'answer' }, { op: 'event', event: 'reconnect' }, { op: 'event', event: 'expiry' }, { op: 'event', event: 'timeout' }],
    expectedWire: [['InitProducerId', -1, -1], ['InitProducerId', -1, -1], ['AddPartitionsToTxn', [0]], ['Produce', 0, 1000, 0, 0, 1], ['Produce', 0, 1000, 0, 1, 1], ['Produce', 0, 1000, 0, 1, 2], ['EndTxn', 'commit', 1000, 0]],
    expectedSettlement: [['m1', 'fulfilled', '0'], ['m2', 'fulfilled', '-1'], ['end', 'fulfilled']],
    expectedState: 'READY',
    expectedLog: [[0, 1000, 0, 0, 0], [0, 1000, 0, 1, 1]],
    expectedNoops: ['fail:0'],
  },
  {
    name: 'tv1 inflight-and-queued answer>expiry>timeout>reconnect',
    kind: 'transactional',
    profile: 'tv1',
    shape: 'inflight-and-queued',
    fault: 'answer>expiry>timeout>reconnect',
    commands: [{ op: 'event', event: 'answer' }, { op: 'event', event: 'expiry' }, { op: 'event', event: 'timeout' }, { op: 'event', event: 'reconnect' }],
    expectedWire: [['InitProducerId', -1, -1], ['InitProducerId', -1, -1], ['AddPartitionsToTxn', [0]], ['Produce', 0, 1000, 0, 0, 1], ['Produce', 0, 1000, 0, 1, 1], ['Produce', 0, 1000, 0, 1, 2], ['EndTxn', 'commit', 1000, 0]],
    expectedSettlement: [['m1', 'fulfilled', '0'], ['m2', 'fulfilled', '-1'], ['end', 'fulfilled']],
    expectedState: 'READY',
    expectedLog: [[0, 1000, 0, 0, 0], [0, 1000, 0, 1, 1]],
    expectedNoops: ['fail:0'],
  },
  {
    name: 'tv1 inflight-and-queued answer>expiry>reconnect>timeout',
    kind: 'transactional',
    profile: 'tv1',
    shape: 'inflight-and-queued',
    fault: 'answer>expiry>reconnect>timeout',
    commands: [{ op: 'event', event: 'answer' }, { op: 'event', event: 'expiry' }, { op: 'event', event: 'reconnect' }, { op: 'event', event: 'timeout' }],
    expectedWire: [['InitProducerId', -1, -1], ['InitProducerId', -1, -1], ['AddPartitionsToTxn', [0]], ['Produce', 0, 1000, 0, 0, 1], ['Produce', 0, 1000, 0, 1, 1], ['Produce', 0, 1000, 0, 1, 2], ['EndTxn', 'commit', 1000, 0]],
    expectedSettlement: [['m1', 'fulfilled', '0'], ['m2', 'fulfilled', '-1'], ['end', 'fulfilled']],
    expectedState: 'READY',
    expectedLog: [[0, 1000, 0, 0, 0], [0, 1000, 0, 1, 1]],
    expectedNoops: ['fail:0'],
  },
  {
    name: 'tv1 inflight-and-queued timeout>answer>reconnect>expiry',
    kind: 'transactional',
    profile: 'tv1',
    shape: 'inflight-and-queued',
    fault: 'timeout>answer>reconnect>expiry',
    commands: [{ op: 'event', event: 'timeout' }, { op: 'event', event: 'answer' }, { op: 'event', event: 'reconnect' }, { op: 'event', event: 'expiry' }],
    expectedWire: [['InitProducerId', -1, -1], ['InitProducerId', -1, -1], ['AddPartitionsToTxn', [0]], ['Produce', 0, 1000, 0, 0, 1], ['Produce', 0, 1000, 0, 0, 2], ['EndTxn', 'abort', 1000, 0], ['InitProducerId', 1000, 0]],
    expectedSettlement: [['m1', 'rejected', 'KafkaJSDeliveryTimeout', 'unknown'], ['m2', 'rejected', 'KafkaJSTransactionAbortableError', null], ['end', 'fulfilled']],
    expectedState: 'READY',
    expectedLog: [[0, 1000, 0, 0, 0]],
    expectedNoops: ['release:0', 'destroy:0'],
  },
  {
    name: 'tv1 inflight-and-queued timeout>answer>expiry>reconnect',
    kind: 'transactional',
    profile: 'tv1',
    shape: 'inflight-and-queued',
    fault: 'timeout>answer>expiry>reconnect',
    commands: [{ op: 'event', event: 'timeout' }, { op: 'event', event: 'answer' }, { op: 'event', event: 'expiry' }, { op: 'event', event: 'reconnect' }],
    expectedWire: [['InitProducerId', -1, -1], ['InitProducerId', -1, -1], ['AddPartitionsToTxn', [0]], ['Produce', 0, 1000, 0, 0, 1], ['Produce', 0, 1000, 0, 0, 2], ['EndTxn', 'abort', 1000, 0], ['InitProducerId', 1000, 0]],
    expectedSettlement: [['m1', 'rejected', 'KafkaJSDeliveryTimeout', 'unknown'], ['m2', 'rejected', 'KafkaJSTransactionAbortableError', null], ['end', 'fulfilled']],
    expectedState: 'READY',
    expectedLog: [[0, 1000, 0, 0, 0]],
    expectedNoops: ['release:0', 'destroy:0'],
  },
  {
    name: 'tv1 inflight-and-queued timeout>reconnect>answer>expiry',
    kind: 'transactional',
    profile: 'tv1',
    shape: 'inflight-and-queued',
    fault: 'timeout>reconnect>answer>expiry',
    commands: [{ op: 'event', event: 'timeout' }, { op: 'event', event: 'reconnect' }, { op: 'event', event: 'answer' }, { op: 'event', event: 'expiry' }],
    expectedWire: [['InitProducerId', -1, -1], ['InitProducerId', -1, -1], ['AddPartitionsToTxn', [0]], ['Produce', 0, 1000, 0, 0, 1], ['Produce', 0, 1000, 0, 0, 2], ['EndTxn', 'abort', 1000, 0], ['InitProducerId', 1000, 0]],
    expectedSettlement: [['m1', 'rejected', 'KafkaJSDeliveryTimeout', 'unknown'], ['m2', 'rejected', 'KafkaJSTransactionAbortableError', null], ['end', 'fulfilled']],
    expectedState: 'READY',
    expectedLog: [[0, 1000, 0, 0, 0]],
    expectedNoops: ['destroy:0', 'release:0'],
  },
  {
    name: 'tv1 inflight-and-queued timeout>reconnect>expiry>answer',
    kind: 'transactional',
    profile: 'tv1',
    shape: 'inflight-and-queued',
    fault: 'timeout>reconnect>expiry>answer',
    commands: [{ op: 'event', event: 'timeout' }, { op: 'event', event: 'reconnect' }, { op: 'event', event: 'expiry' }, { op: 'event', event: 'answer' }],
    expectedWire: [['InitProducerId', -1, -1], ['InitProducerId', -1, -1], ['AddPartitionsToTxn', [0]], ['Produce', 0, 1000, 0, 0, 1], ['Produce', 0, 1000, 0, 0, 2], ['EndTxn', 'abort', 1000, 0], ['InitProducerId', 1000, 0]],
    expectedSettlement: [['m1', 'rejected', 'KafkaJSDeliveryTimeout', 'unknown'], ['m2', 'rejected', 'KafkaJSTransactionAbortableError', null], ['end', 'fulfilled']],
    expectedState: 'READY',
    expectedLog: [[0, 1000, 0, 0, 0]],
    expectedNoops: ['destroy:0', 'release:0'],
  },
  {
    name: 'tv1 inflight-and-queued timeout>expiry>answer>reconnect',
    kind: 'transactional',
    profile: 'tv1',
    shape: 'inflight-and-queued',
    fault: 'timeout>expiry>answer>reconnect',
    commands: [{ op: 'event', event: 'timeout' }, { op: 'event', event: 'expiry' }, { op: 'event', event: 'answer' }, { op: 'event', event: 'reconnect' }],
    expectedWire: [['InitProducerId', -1, -1], ['InitProducerId', -1, -1], ['AddPartitionsToTxn', [0]], ['Produce', 0, 1000, 0, 0, 1], ['Produce', 0, 1000, 0, 0, 2], ['EndTxn', 'abort', 1000, 0], ['InitProducerId', 1000, 0]],
    expectedSettlement: [['m1', 'rejected', 'KafkaJSDeliveryTimeout', 'unknown'], ['m2', 'rejected', 'KafkaJSTransactionAbortableError', null], ['end', 'fulfilled']],
    expectedState: 'READY',
    expectedLog: [[0, 1000, 0, 0, 0]],
    expectedNoops: ['release:0', 'destroy:0'],
  },
  {
    name: 'tv1 inflight-and-queued timeout>expiry>reconnect>answer',
    kind: 'transactional',
    profile: 'tv1',
    shape: 'inflight-and-queued',
    fault: 'timeout>expiry>reconnect>answer',
    commands: [{ op: 'event', event: 'timeout' }, { op: 'event', event: 'expiry' }, { op: 'event', event: 'reconnect' }, { op: 'event', event: 'answer' }],
    expectedWire: [['InitProducerId', -1, -1], ['InitProducerId', -1, -1], ['AddPartitionsToTxn', [0]], ['Produce', 0, 1000, 0, 0, 1], ['Produce', 0, 1000, 0, 0, 2], ['EndTxn', 'abort', 1000, 0], ['InitProducerId', 1000, 0]],
    expectedSettlement: [['m1', 'rejected', 'KafkaJSDeliveryTimeout', 'unknown'], ['m2', 'rejected', 'KafkaJSTransactionAbortableError', null], ['end', 'fulfilled']],
    expectedState: 'READY',
    expectedLog: [[0, 1000, 0, 0, 0]],
    expectedNoops: ['destroy:0', 'release:0'],
  },
  {
    name: 'tv1 inflight-and-queued reconnect>answer>timeout>expiry',
    kind: 'transactional',
    profile: 'tv1',
    shape: 'inflight-and-queued',
    fault: 'reconnect>answer>timeout>expiry',
    commands: [{ op: 'event', event: 'reconnect' }, { op: 'event', event: 'answer' }, { op: 'event', event: 'timeout' }, { op: 'event', event: 'expiry' }],
    expectedWire: [['InitProducerId', -1, -1], ['InitProducerId', -1, -1], ['AddPartitionsToTxn', [0]], ['Produce', 0, 1000, 0, 0, 1], ['Produce', 0, 1000, 0, 0, 2], ['EndTxn', 'abort', 1000, 0], ['InitProducerId', 1000, 0]],
    expectedSettlement: [['m1', 'rejected', 'KafkaJSDeliveryTimeout', 'unknown'], ['m2', 'rejected', 'KafkaJSTransactionAbortableError', null], ['end', 'fulfilled']],
    expectedState: 'READY',
    expectedLog: [[0, 1000, 0, 0, 0]],
    expectedNoops: ['release:0', 'fail:0'],
  },
  {
    name: 'tv1 inflight-and-queued reconnect>answer>expiry>timeout',
    kind: 'transactional',
    profile: 'tv1',
    shape: 'inflight-and-queued',
    fault: 'reconnect>answer>expiry>timeout',
    commands: [{ op: 'event', event: 'reconnect' }, { op: 'event', event: 'answer' }, { op: 'event', event: 'expiry' }, { op: 'event', event: 'timeout' }],
    expectedWire: [['InitProducerId', -1, -1], ['InitProducerId', -1, -1], ['AddPartitionsToTxn', [0]], ['Produce', 0, 1000, 0, 0, 1], ['Produce', 0, 1000, 0, 0, 2], ['EndTxn', 'abort', 1000, 0], ['InitProducerId', 1000, 0]],
    expectedSettlement: [['m1', 'rejected', 'KafkaJSDeliveryTimeout', 'unknown'], ['m2', 'rejected', 'KafkaJSTransactionAbortableError', null], ['end', 'fulfilled']],
    expectedState: 'READY',
    expectedLog: [[0, 1000, 0, 0, 0]],
    expectedNoops: ['release:0', 'fail:0'],
  },
  {
    name: 'tv1 inflight-and-queued reconnect>timeout>answer>expiry',
    kind: 'transactional',
    profile: 'tv1',
    shape: 'inflight-and-queued',
    fault: 'reconnect>timeout>answer>expiry',
    commands: [{ op: 'event', event: 'reconnect' }, { op: 'event', event: 'timeout' }, { op: 'event', event: 'answer' }, { op: 'event', event: 'expiry' }],
    expectedWire: [['InitProducerId', -1, -1], ['InitProducerId', -1, -1], ['AddPartitionsToTxn', [0]], ['Produce', 0, 1000, 0, 0, 1], ['Produce', 0, 1000, 0, 0, 2], ['EndTxn', 'abort', 1000, 0], ['InitProducerId', 1000, 0]],
    expectedSettlement: [['m1', 'rejected', 'KafkaJSDeliveryTimeout', 'unknown'], ['m2', 'rejected', 'KafkaJSTransactionAbortableError', null], ['end', 'fulfilled']],
    expectedState: 'READY',
    expectedLog: [[0, 1000, 0, 0, 0]],
    expectedNoops: ['fail:0', 'release:0'],
  },
  {
    name: 'tv1 inflight-and-queued reconnect>timeout>expiry>answer',
    kind: 'transactional',
    profile: 'tv1',
    shape: 'inflight-and-queued',
    fault: 'reconnect>timeout>expiry>answer',
    commands: [{ op: 'event', event: 'reconnect' }, { op: 'event', event: 'timeout' }, { op: 'event', event: 'expiry' }, { op: 'event', event: 'answer' }],
    expectedWire: [['InitProducerId', -1, -1], ['InitProducerId', -1, -1], ['AddPartitionsToTxn', [0]], ['Produce', 0, 1000, 0, 0, 1], ['Produce', 0, 1000, 0, 0, 2], ['EndTxn', 'abort', 1000, 0], ['InitProducerId', 1000, 0]],
    expectedSettlement: [['m1', 'rejected', 'KafkaJSDeliveryTimeout', 'unknown'], ['m2', 'rejected', 'KafkaJSTransactionAbortableError', null], ['end', 'fulfilled']],
    expectedState: 'READY',
    expectedLog: [[0, 1000, 0, 0, 0]],
    expectedNoops: ['fail:0', 'release:0'],
  },
  {
    name: 'tv1 inflight-and-queued reconnect>expiry>answer>timeout',
    kind: 'transactional',
    profile: 'tv1',
    shape: 'inflight-and-queued',
    fault: 'reconnect>expiry>answer>timeout',
    commands: [{ op: 'event', event: 'reconnect' }, { op: 'event', event: 'expiry' }, { op: 'event', event: 'answer' }, { op: 'event', event: 'timeout' }],
    expectedWire: [['InitProducerId', -1, -1], ['InitProducerId', -1, -1], ['AddPartitionsToTxn', [0]], ['Produce', 0, 1000, 0, 0, 1], ['Produce', 0, 1000, 0, 0, 2], ['EndTxn', 'abort', 1000, 0], ['InitProducerId', 1000, 0]],
    expectedSettlement: [['m1', 'rejected', 'KafkaJSDeliveryTimeout', 'unknown'], ['m2', 'rejected', 'KafkaJSTransactionAbortableError', null], ['end', 'fulfilled']],
    expectedState: 'READY',
    expectedLog: [[0, 1000, 0, 0, 0]],
    expectedNoops: ['release:0', 'fail:0'],
  },
  {
    name: 'tv1 inflight-and-queued reconnect>expiry>timeout>answer',
    kind: 'transactional',
    profile: 'tv1',
    shape: 'inflight-and-queued',
    fault: 'reconnect>expiry>timeout>answer',
    commands: [{ op: 'event', event: 'reconnect' }, { op: 'event', event: 'expiry' }, { op: 'event', event: 'timeout' }, { op: 'event', event: 'answer' }],
    expectedWire: [['InitProducerId', -1, -1], ['InitProducerId', -1, -1], ['AddPartitionsToTxn', [0]], ['Produce', 0, 1000, 0, 0, 1], ['Produce', 0, 1000, 0, 0, 2], ['EndTxn', 'abort', 1000, 0], ['InitProducerId', 1000, 0]],
    expectedSettlement: [['m1', 'rejected', 'KafkaJSDeliveryTimeout', 'unknown'], ['m2', 'rejected', 'KafkaJSTransactionAbortableError', null], ['end', 'fulfilled']],
    expectedState: 'READY',
    expectedLog: [[0, 1000, 0, 0, 0]],
    expectedNoops: ['fail:0', 'release:0'],
  },
  {
    name: 'tv1 inflight-and-queued expiry>answer>timeout>reconnect',
    kind: 'transactional',
    profile: 'tv1',
    shape: 'inflight-and-queued',
    fault: 'expiry>answer>timeout>reconnect',
    commands: [{ op: 'event', event: 'expiry' }, { op: 'event', event: 'answer' }, { op: 'event', event: 'timeout' }, { op: 'event', event: 'reconnect' }],
    expectedWire: [['InitProducerId', -1, -1], ['InitProducerId', -1, -1], ['AddPartitionsToTxn', [0]], ['Produce', 0, 1000, 0, 0, 1], ['EndTxn', 'abort', 1000, 0], ['InitProducerId', 1000, 0]],
    expectedSettlement: [['m1', 'rejected', 'KafkaJSDeliveryTimeout', 'unknown'], ['m2', 'rejected', 'KafkaJSTransactionAbortableError', null], ['end', 'fulfilled']],
    expectedState: 'READY',
    expectedLog: [[0, 1000, 0, 0, 0]],
    expectedNoops: ['fail:0'],
  },
  {
    name: 'tv1 inflight-and-queued expiry>answer>reconnect>timeout',
    kind: 'transactional',
    profile: 'tv1',
    shape: 'inflight-and-queued',
    fault: 'expiry>answer>reconnect>timeout',
    commands: [{ op: 'event', event: 'expiry' }, { op: 'event', event: 'answer' }, { op: 'event', event: 'reconnect' }, { op: 'event', event: 'timeout' }],
    expectedWire: [['InitProducerId', -1, -1], ['InitProducerId', -1, -1], ['AddPartitionsToTxn', [0]], ['Produce', 0, 1000, 0, 0, 1], ['EndTxn', 'abort', 1000, 0], ['InitProducerId', 1000, 0]],
    expectedSettlement: [['m1', 'rejected', 'KafkaJSDeliveryTimeout', 'unknown'], ['m2', 'rejected', 'KafkaJSTransactionAbortableError', null], ['end', 'fulfilled']],
    expectedState: 'READY',
    expectedLog: [[0, 1000, 0, 0, 0]],
    expectedNoops: ['fail:0'],
  },
  {
    name: 'tv1 inflight-and-queued expiry>timeout>answer>reconnect',
    kind: 'transactional',
    profile: 'tv1',
    shape: 'inflight-and-queued',
    fault: 'expiry>timeout>answer>reconnect',
    commands: [{ op: 'event', event: 'expiry' }, { op: 'event', event: 'timeout' }, { op: 'event', event: 'answer' }, { op: 'event', event: 'reconnect' }],
    expectedWire: [['InitProducerId', -1, -1], ['InitProducerId', -1, -1], ['AddPartitionsToTxn', [0]], ['Produce', 0, 1000, 0, 0, 1], ['EndTxn', 'abort', 1000, 0], ['InitProducerId', 1000, 0]],
    expectedSettlement: [['m1', 'rejected', 'KafkaJSDeliveryTimeout', 'unknown'], ['m2', 'rejected', 'KafkaJSTransactionAbortableError', null], ['end', 'fulfilled']],
    expectedState: 'READY',
    expectedLog: [[0, 1000, 0, 0, 0]],
    expectedNoops: ['release:0', 'destroy:0'],
  },
  {
    name: 'tv1 inflight-and-queued expiry>timeout>reconnect>answer',
    kind: 'transactional',
    profile: 'tv1',
    shape: 'inflight-and-queued',
    fault: 'expiry>timeout>reconnect>answer',
    commands: [{ op: 'event', event: 'expiry' }, { op: 'event', event: 'timeout' }, { op: 'event', event: 'reconnect' }, { op: 'event', event: 'answer' }],
    expectedWire: [['InitProducerId', -1, -1], ['InitProducerId', -1, -1], ['AddPartitionsToTxn', [0]], ['Produce', 0, 1000, 0, 0, 1], ['EndTxn', 'abort', 1000, 0], ['InitProducerId', 1000, 0]],
    expectedSettlement: [['m1', 'rejected', 'KafkaJSDeliveryTimeout', 'unknown'], ['m2', 'rejected', 'KafkaJSTransactionAbortableError', null], ['end', 'fulfilled']],
    expectedState: 'READY',
    expectedLog: [[0, 1000, 0, 0, 0]],
    expectedNoops: ['destroy:0', 'release:0'],
  },
  {
    name: 'tv1 inflight-and-queued expiry>reconnect>answer>timeout',
    kind: 'transactional',
    profile: 'tv1',
    shape: 'inflight-and-queued',
    fault: 'expiry>reconnect>answer>timeout',
    commands: [{ op: 'event', event: 'expiry' }, { op: 'event', event: 'reconnect' }, { op: 'event', event: 'answer' }, { op: 'event', event: 'timeout' }],
    expectedWire: [['InitProducerId', -1, -1], ['InitProducerId', -1, -1], ['AddPartitionsToTxn', [0]], ['Produce', 0, 1000, 0, 0, 1], ['EndTxn', 'abort', 1000, 0], ['InitProducerId', 1000, 0]],
    expectedSettlement: [['m1', 'rejected', 'KafkaJSDeliveryTimeout', 'unknown'], ['m2', 'rejected', 'KafkaJSTransactionAbortableError', null], ['end', 'fulfilled']],
    expectedState: 'READY',
    expectedLog: [[0, 1000, 0, 0, 0]],
    expectedNoops: ['release:0', 'fail:0'],
  },
  {
    name: 'tv1 inflight-and-queued expiry>reconnect>timeout>answer',
    kind: 'transactional',
    profile: 'tv1',
    shape: 'inflight-and-queued',
    fault: 'expiry>reconnect>timeout>answer',
    commands: [{ op: 'event', event: 'expiry' }, { op: 'event', event: 'reconnect' }, { op: 'event', event: 'timeout' }, { op: 'event', event: 'answer' }],
    expectedWire: [['InitProducerId', -1, -1], ['InitProducerId', -1, -1], ['AddPartitionsToTxn', [0]], ['Produce', 0, 1000, 0, 0, 1], ['EndTxn', 'abort', 1000, 0], ['InitProducerId', 1000, 0]],
    expectedSettlement: [['m1', 'rejected', 'KafkaJSDeliveryTimeout', 'unknown'], ['m2', 'rejected', 'KafkaJSTransactionAbortableError', null], ['end', 'fulfilled']],
    expectedState: 'READY',
    expectedLog: [[0, 1000, 0, 0, 0]],
    expectedNoops: ['fail:0', 'release:0'],
  },
  {
    name: 'tv1 two-inflight answer>timeout>reconnect>expiry',
    kind: 'transactional',
    profile: 'tv1',
    shape: 'two-inflight',
    fault: 'answer>timeout>reconnect>expiry',
    commands: [{ op: 'event', event: 'answer' }, { op: 'event', event: 'timeout' }, { op: 'event', event: 'reconnect' }, { op: 'event', event: 'expiry' }],
    expectedWire: [['InitProducerId', -1, -1], ['InitProducerId', -1, -1], ['AddPartitionsToTxn', [0]], ['Produce', 0, 1000, 0, 0, 1], ['AddPartitionsToTxn', [1]], ['Produce', 1, 1000, 0, 0, 1], ['Produce', 1, 1000, 0, 0, 2], ['EndTxn', 'commit', 1000, 0]],
    expectedSettlement: [['m1', 'fulfilled', '0'], ['m2', 'fulfilled', '-1'], ['end', 'fulfilled']],
    expectedState: 'READY',
    expectedLog: [[0, 1000, 0, 0, 0], [1, 1000, 0, 0, 0]],
    expectedNoops: ['fail:0'],
  },
  {
    name: 'tv1 two-inflight answer>timeout>expiry>reconnect',
    kind: 'transactional',
    profile: 'tv1',
    shape: 'two-inflight',
    fault: 'answer>timeout>expiry>reconnect',
    commands: [{ op: 'event', event: 'answer' }, { op: 'event', event: 'timeout' }, { op: 'event', event: 'expiry' }, { op: 'event', event: 'reconnect' }],
    expectedWire: [['InitProducerId', -1, -1], ['InitProducerId', -1, -1], ['AddPartitionsToTxn', [0]], ['Produce', 0, 1000, 0, 0, 1], ['AddPartitionsToTxn', [1]], ['Produce', 1, 1000, 0, 0, 1], ['Produce', 1, 1000, 0, 0, 2], ['EndTxn', 'commit', 1000, 0]],
    expectedSettlement: [['m1', 'fulfilled', '0'], ['m2', 'fulfilled', '-1'], ['end', 'fulfilled']],
    expectedState: 'READY',
    expectedLog: [[0, 1000, 0, 0, 0], [1, 1000, 0, 0, 0]],
    expectedNoops: ['fail:0'],
  },
  {
    name: 'tv1 two-inflight answer>reconnect>timeout>expiry',
    kind: 'transactional',
    profile: 'tv1',
    shape: 'two-inflight',
    fault: 'answer>reconnect>timeout>expiry',
    commands: [{ op: 'event', event: 'answer' }, { op: 'event', event: 'reconnect' }, { op: 'event', event: 'timeout' }, { op: 'event', event: 'expiry' }],
    expectedWire: [['InitProducerId', -1, -1], ['InitProducerId', -1, -1], ['AddPartitionsToTxn', [0]], ['Produce', 0, 1000, 0, 0, 1], ['AddPartitionsToTxn', [1]], ['Produce', 1, 1000, 0, 0, 1], ['Produce', 1, 1000, 0, 0, 2], ['EndTxn', 'commit', 1000, 0]],
    expectedSettlement: [['m1', 'fulfilled', '0'], ['m2', 'fulfilled', '-1'], ['end', 'fulfilled']],
    expectedState: 'READY',
    expectedLog: [[0, 1000, 0, 0, 0], [1, 1000, 0, 0, 0]],
    expectedNoops: ['fail:0'],
  },
  {
    name: 'tv1 two-inflight answer>reconnect>expiry>timeout',
    kind: 'transactional',
    profile: 'tv1',
    shape: 'two-inflight',
    fault: 'answer>reconnect>expiry>timeout',
    commands: [{ op: 'event', event: 'answer' }, { op: 'event', event: 'reconnect' }, { op: 'event', event: 'expiry' }, { op: 'event', event: 'timeout' }],
    expectedWire: [['InitProducerId', -1, -1], ['InitProducerId', -1, -1], ['AddPartitionsToTxn', [0]], ['Produce', 0, 1000, 0, 0, 1], ['AddPartitionsToTxn', [1]], ['Produce', 1, 1000, 0, 0, 1], ['Produce', 1, 1000, 0, 0, 2], ['EndTxn', 'commit', 1000, 0]],
    expectedSettlement: [['m1', 'fulfilled', '0'], ['m2', 'fulfilled', '-1'], ['end', 'fulfilled']],
    expectedState: 'READY',
    expectedLog: [[0, 1000, 0, 0, 0], [1, 1000, 0, 0, 0]],
    expectedNoops: ['fail:0'],
  },
  {
    name: 'tv1 two-inflight answer>expiry>timeout>reconnect',
    kind: 'transactional',
    profile: 'tv1',
    shape: 'two-inflight',
    fault: 'answer>expiry>timeout>reconnect',
    commands: [{ op: 'event', event: 'answer' }, { op: 'event', event: 'expiry' }, { op: 'event', event: 'timeout' }, { op: 'event', event: 'reconnect' }],
    expectedWire: [['InitProducerId', -1, -1], ['InitProducerId', -1, -1], ['AddPartitionsToTxn', [0]], ['Produce', 0, 1000, 0, 0, 1], ['AddPartitionsToTxn', [1]], ['Produce', 1, 1000, 0, 0, 1], ['Produce', 1, 1000, 0, 0, 2], ['EndTxn', 'commit', 1000, 0]],
    expectedSettlement: [['m1', 'fulfilled', '0'], ['m2', 'fulfilled', '-1'], ['end', 'fulfilled']],
    expectedState: 'READY',
    expectedLog: [[0, 1000, 0, 0, 0], [1, 1000, 0, 0, 0]],
    expectedNoops: ['fail:0'],
  },
  {
    name: 'tv1 two-inflight answer>expiry>reconnect>timeout',
    kind: 'transactional',
    profile: 'tv1',
    shape: 'two-inflight',
    fault: 'answer>expiry>reconnect>timeout',
    commands: [{ op: 'event', event: 'answer' }, { op: 'event', event: 'expiry' }, { op: 'event', event: 'reconnect' }, { op: 'event', event: 'timeout' }],
    expectedWire: [['InitProducerId', -1, -1], ['InitProducerId', -1, -1], ['AddPartitionsToTxn', [0]], ['Produce', 0, 1000, 0, 0, 1], ['AddPartitionsToTxn', [1]], ['Produce', 1, 1000, 0, 0, 1], ['Produce', 1, 1000, 0, 0, 2], ['EndTxn', 'commit', 1000, 0]],
    expectedSettlement: [['m1', 'fulfilled', '0'], ['m2', 'fulfilled', '-1'], ['end', 'fulfilled']],
    expectedState: 'READY',
    expectedLog: [[0, 1000, 0, 0, 0], [1, 1000, 0, 0, 0]],
    expectedNoops: ['fail:0'],
  },
  {
    name: 'tv1 two-inflight timeout>answer>reconnect>expiry',
    kind: 'transactional',
    profile: 'tv1',
    shape: 'two-inflight',
    fault: 'timeout>answer>reconnect>expiry',
    commands: [{ op: 'event', event: 'timeout' }, { op: 'event', event: 'answer' }, { op: 'event', event: 'reconnect' }, { op: 'event', event: 'expiry' }],
    expectedWire: [['InitProducerId', -1, -1], ['InitProducerId', -1, -1], ['AddPartitionsToTxn', [0]], ['Produce', 0, 1000, 0, 0, 1], ['AddPartitionsToTxn', [1]], ['Produce', 1, 1000, 0, 0, 1], ['Produce', 0, 1000, 0, 0, 2], ['Produce', 1, 1000, 0, 0, 2], ['EndTxn', 'abort', 1000, 0], ['InitProducerId', 1000, 0]],
    expectedSettlement: [['m1', 'rejected', 'KafkaJSDeliveryTimeout', 'unknown'], ['m2', 'fulfilled', '-1'], ['end', 'fulfilled']],
    expectedState: 'READY',
    expectedLog: [[0, 1000, 0, 0, 0], [1, 1000, 0, 0, 0]],
    expectedNoops: ['release:0', 'destroy:0'],
  },
  {
    name: 'tv1 two-inflight timeout>answer>expiry>reconnect',
    kind: 'transactional',
    profile: 'tv1',
    shape: 'two-inflight',
    fault: 'timeout>answer>expiry>reconnect',
    commands: [{ op: 'event', event: 'timeout' }, { op: 'event', event: 'answer' }, { op: 'event', event: 'expiry' }, { op: 'event', event: 'reconnect' }],
    expectedWire: [['InitProducerId', -1, -1], ['InitProducerId', -1, -1], ['AddPartitionsToTxn', [0]], ['Produce', 0, 1000, 0, 0, 1], ['AddPartitionsToTxn', [1]], ['Produce', 1, 1000, 0, 0, 1], ['Produce', 0, 1000, 0, 0, 2], ['Produce', 1, 1000, 0, 0, 2], ['EndTxn', 'abort', 1000, 0], ['InitProducerId', 1000, 0]],
    expectedSettlement: [['m1', 'rejected', 'KafkaJSDeliveryTimeout', 'unknown'], ['m2', 'fulfilled', '-1'], ['end', 'fulfilled']],
    expectedState: 'READY',
    expectedLog: [[0, 1000, 0, 0, 0], [1, 1000, 0, 0, 0]],
    expectedNoops: ['release:0', 'destroy:0'],
  },
  {
    name: 'tv1 two-inflight timeout>reconnect>answer>expiry',
    kind: 'transactional',
    profile: 'tv1',
    shape: 'two-inflight',
    fault: 'timeout>reconnect>answer>expiry',
    commands: [{ op: 'event', event: 'timeout' }, { op: 'event', event: 'reconnect' }, { op: 'event', event: 'answer' }, { op: 'event', event: 'expiry' }],
    expectedWire: [['InitProducerId', -1, -1], ['InitProducerId', -1, -1], ['AddPartitionsToTxn', [0]], ['Produce', 0, 1000, 0, 0, 1], ['AddPartitionsToTxn', [1]], ['Produce', 1, 1000, 0, 0, 1], ['Produce', 0, 1000, 0, 0, 2], ['Produce', 1, 1000, 0, 0, 2], ['EndTxn', 'abort', 1000, 0], ['InitProducerId', 1000, 0]],
    expectedSettlement: [['m1', 'rejected', 'KafkaJSDeliveryTimeout', 'unknown'], ['m2', 'fulfilled', '-1'], ['end', 'fulfilled']],
    expectedState: 'READY',
    expectedLog: [[0, 1000, 0, 0, 0], [1, 1000, 0, 0, 0]],
    expectedNoops: ['destroy:0', 'release:0'],
  },
  {
    name: 'tv1 two-inflight timeout>reconnect>expiry>answer',
    kind: 'transactional',
    profile: 'tv1',
    shape: 'two-inflight',
    fault: 'timeout>reconnect>expiry>answer',
    commands: [{ op: 'event', event: 'timeout' }, { op: 'event', event: 'reconnect' }, { op: 'event', event: 'expiry' }, { op: 'event', event: 'answer' }],
    expectedWire: [['InitProducerId', -1, -1], ['InitProducerId', -1, -1], ['AddPartitionsToTxn', [0]], ['Produce', 0, 1000, 0, 0, 1], ['AddPartitionsToTxn', [1]], ['Produce', 1, 1000, 0, 0, 1], ['Produce', 0, 1000, 0, 0, 2], ['Produce', 1, 1000, 0, 0, 2], ['EndTxn', 'abort', 1000, 0], ['InitProducerId', 1000, 0]],
    expectedSettlement: [['m1', 'rejected', 'KafkaJSDeliveryTimeout', 'unknown'], ['m2', 'fulfilled', '-1'], ['end', 'fulfilled']],
    expectedState: 'READY',
    expectedLog: [[0, 1000, 0, 0, 0], [1, 1000, 0, 0, 0]],
    expectedNoops: ['destroy:0', 'release:0'],
  },
  {
    name: 'tv1 two-inflight timeout>expiry>answer>reconnect',
    kind: 'transactional',
    profile: 'tv1',
    shape: 'two-inflight',
    fault: 'timeout>expiry>answer>reconnect',
    commands: [{ op: 'event', event: 'timeout' }, { op: 'event', event: 'expiry' }, { op: 'event', event: 'answer' }, { op: 'event', event: 'reconnect' }],
    expectedWire: [['InitProducerId', -1, -1], ['InitProducerId', -1, -1], ['AddPartitionsToTxn', [0]], ['Produce', 0, 1000, 0, 0, 1], ['AddPartitionsToTxn', [1]], ['Produce', 1, 1000, 0, 0, 1], ['Produce', 0, 1000, 0, 0, 2], ['Produce', 1, 1000, 0, 0, 2], ['EndTxn', 'abort', 1000, 0], ['InitProducerId', 1000, 0]],
    expectedSettlement: [['m1', 'rejected', 'KafkaJSDeliveryTimeout', 'unknown'], ['m2', 'fulfilled', '-1'], ['end', 'fulfilled']],
    expectedState: 'READY',
    expectedLog: [[0, 1000, 0, 0, 0], [1, 1000, 0, 0, 0]],
    expectedNoops: ['release:0', 'destroy:0'],
  },
  {
    name: 'tv1 two-inflight timeout>expiry>reconnect>answer',
    kind: 'transactional',
    profile: 'tv1',
    shape: 'two-inflight',
    fault: 'timeout>expiry>reconnect>answer',
    commands: [{ op: 'event', event: 'timeout' }, { op: 'event', event: 'expiry' }, { op: 'event', event: 'reconnect' }, { op: 'event', event: 'answer' }],
    expectedWire: [['InitProducerId', -1, -1], ['InitProducerId', -1, -1], ['AddPartitionsToTxn', [0]], ['Produce', 0, 1000, 0, 0, 1], ['AddPartitionsToTxn', [1]], ['Produce', 1, 1000, 0, 0, 1], ['Produce', 0, 1000, 0, 0, 2], ['Produce', 1, 1000, 0, 0, 2], ['EndTxn', 'abort', 1000, 0], ['InitProducerId', 1000, 0]],
    expectedSettlement: [['m1', 'rejected', 'KafkaJSDeliveryTimeout', 'unknown'], ['m2', 'fulfilled', '-1'], ['end', 'fulfilled']],
    expectedState: 'READY',
    expectedLog: [[0, 1000, 0, 0, 0], [1, 1000, 0, 0, 0]],
    expectedNoops: ['destroy:0', 'release:0'],
  },
  {
    name: 'tv1 two-inflight reconnect>answer>timeout>expiry',
    kind: 'transactional',
    profile: 'tv1',
    shape: 'two-inflight',
    fault: 'reconnect>answer>timeout>expiry',
    commands: [{ op: 'event', event: 'reconnect' }, { op: 'event', event: 'answer' }, { op: 'event', event: 'timeout' }, { op: 'event', event: 'expiry' }],
    expectedWire: [['InitProducerId', -1, -1], ['InitProducerId', -1, -1], ['AddPartitionsToTxn', [0]], ['Produce', 0, 1000, 0, 0, 1], ['AddPartitionsToTxn', [1]], ['Produce', 1, 1000, 0, 0, 1], ['Produce', 0, 1000, 0, 0, 2], ['Produce', 1, 1000, 0, 0, 2], ['EndTxn', 'abort', 1000, 0], ['InitProducerId', 1000, 0]],
    expectedSettlement: [['m1', 'rejected', 'KafkaJSDeliveryTimeout', 'unknown'], ['m2', 'fulfilled', '-1'], ['end', 'fulfilled']],
    expectedState: 'READY',
    expectedLog: [[0, 1000, 0, 0, 0], [1, 1000, 0, 0, 0]],
    expectedNoops: ['release:0', 'fail:0'],
  },
  {
    name: 'tv1 two-inflight reconnect>answer>expiry>timeout',
    kind: 'transactional',
    profile: 'tv1',
    shape: 'two-inflight',
    fault: 'reconnect>answer>expiry>timeout',
    commands: [{ op: 'event', event: 'reconnect' }, { op: 'event', event: 'answer' }, { op: 'event', event: 'expiry' }, { op: 'event', event: 'timeout' }],
    expectedWire: [['InitProducerId', -1, -1], ['InitProducerId', -1, -1], ['AddPartitionsToTxn', [0]], ['Produce', 0, 1000, 0, 0, 1], ['AddPartitionsToTxn', [1]], ['Produce', 1, 1000, 0, 0, 1], ['Produce', 0, 1000, 0, 0, 2], ['Produce', 1, 1000, 0, 0, 2], ['EndTxn', 'abort', 1000, 0], ['InitProducerId', 1000, 0]],
    expectedSettlement: [['m1', 'rejected', 'KafkaJSDeliveryTimeout', 'unknown'], ['m2', 'fulfilled', '-1'], ['end', 'fulfilled']],
    expectedState: 'READY',
    expectedLog: [[0, 1000, 0, 0, 0], [1, 1000, 0, 0, 0]],
    expectedNoops: ['release:0', 'fail:0'],
  },
  {
    name: 'tv1 two-inflight reconnect>timeout>answer>expiry',
    kind: 'transactional',
    profile: 'tv1',
    shape: 'two-inflight',
    fault: 'reconnect>timeout>answer>expiry',
    commands: [{ op: 'event', event: 'reconnect' }, { op: 'event', event: 'timeout' }, { op: 'event', event: 'answer' }, { op: 'event', event: 'expiry' }],
    expectedWire: [['InitProducerId', -1, -1], ['InitProducerId', -1, -1], ['AddPartitionsToTxn', [0]], ['Produce', 0, 1000, 0, 0, 1], ['AddPartitionsToTxn', [1]], ['Produce', 1, 1000, 0, 0, 1], ['Produce', 0, 1000, 0, 0, 2], ['Produce', 1, 1000, 0, 0, 2], ['EndTxn', 'abort', 1000, 0], ['InitProducerId', 1000, 0]],
    expectedSettlement: [['m1', 'rejected', 'KafkaJSDeliveryTimeout', 'unknown'], ['m2', 'fulfilled', '-1'], ['end', 'fulfilled']],
    expectedState: 'READY',
    expectedLog: [[0, 1000, 0, 0, 0], [1, 1000, 0, 0, 0]],
    expectedNoops: ['fail:0', 'release:0'],
  },
  {
    name: 'tv1 two-inflight reconnect>timeout>expiry>answer',
    kind: 'transactional',
    profile: 'tv1',
    shape: 'two-inflight',
    fault: 'reconnect>timeout>expiry>answer',
    commands: [{ op: 'event', event: 'reconnect' }, { op: 'event', event: 'timeout' }, { op: 'event', event: 'expiry' }, { op: 'event', event: 'answer' }],
    expectedWire: [['InitProducerId', -1, -1], ['InitProducerId', -1, -1], ['AddPartitionsToTxn', [0]], ['Produce', 0, 1000, 0, 0, 1], ['AddPartitionsToTxn', [1]], ['Produce', 1, 1000, 0, 0, 1], ['Produce', 0, 1000, 0, 0, 2], ['Produce', 1, 1000, 0, 0, 2], ['EndTxn', 'abort', 1000, 0], ['InitProducerId', 1000, 0]],
    expectedSettlement: [['m1', 'rejected', 'KafkaJSDeliveryTimeout', 'unknown'], ['m2', 'fulfilled', '-1'], ['end', 'fulfilled']],
    expectedState: 'READY',
    expectedLog: [[0, 1000, 0, 0, 0], [1, 1000, 0, 0, 0]],
    expectedNoops: ['fail:0', 'release:0'],
  },
  {
    name: 'tv1 two-inflight reconnect>expiry>answer>timeout',
    kind: 'transactional',
    profile: 'tv1',
    shape: 'two-inflight',
    fault: 'reconnect>expiry>answer>timeout',
    commands: [{ op: 'event', event: 'reconnect' }, { op: 'event', event: 'expiry' }, { op: 'event', event: 'answer' }, { op: 'event', event: 'timeout' }],
    expectedWire: [['InitProducerId', -1, -1], ['InitProducerId', -1, -1], ['AddPartitionsToTxn', [0]], ['Produce', 0, 1000, 0, 0, 1], ['AddPartitionsToTxn', [1]], ['Produce', 1, 1000, 0, 0, 1], ['Produce', 0, 1000, 0, 0, 2], ['Produce', 1, 1000, 0, 0, 2], ['EndTxn', 'abort', 1000, 0], ['InitProducerId', 1000, 0]],
    expectedSettlement: [['m1', 'rejected', 'KafkaJSDeliveryTimeout', 'unknown'], ['m2', 'fulfilled', '-1'], ['end', 'fulfilled']],
    expectedState: 'READY',
    expectedLog: [[0, 1000, 0, 0, 0], [1, 1000, 0, 0, 0]],
    expectedNoops: ['release:0', 'fail:0'],
  },
  {
    name: 'tv1 two-inflight reconnect>expiry>timeout>answer',
    kind: 'transactional',
    profile: 'tv1',
    shape: 'two-inflight',
    fault: 'reconnect>expiry>timeout>answer',
    commands: [{ op: 'event', event: 'reconnect' }, { op: 'event', event: 'expiry' }, { op: 'event', event: 'timeout' }, { op: 'event', event: 'answer' }],
    expectedWire: [['InitProducerId', -1, -1], ['InitProducerId', -1, -1], ['AddPartitionsToTxn', [0]], ['Produce', 0, 1000, 0, 0, 1], ['AddPartitionsToTxn', [1]], ['Produce', 1, 1000, 0, 0, 1], ['Produce', 0, 1000, 0, 0, 2], ['Produce', 1, 1000, 0, 0, 2], ['EndTxn', 'abort', 1000, 0], ['InitProducerId', 1000, 0]],
    expectedSettlement: [['m1', 'rejected', 'KafkaJSDeliveryTimeout', 'unknown'], ['m2', 'fulfilled', '-1'], ['end', 'fulfilled']],
    expectedState: 'READY',
    expectedLog: [[0, 1000, 0, 0, 0], [1, 1000, 0, 0, 0]],
    expectedNoops: ['fail:0', 'release:0'],
  },
  {
    name: 'tv1 two-inflight expiry>answer>timeout>reconnect',
    kind: 'transactional',
    profile: 'tv1',
    shape: 'two-inflight',
    fault: 'expiry>answer>timeout>reconnect',
    commands: [{ op: 'event', event: 'expiry' }, { op: 'event', event: 'answer' }, { op: 'event', event: 'timeout' }, { op: 'event', event: 'reconnect' }],
    expectedWire: [['InitProducerId', -1, -1], ['InitProducerId', -1, -1], ['AddPartitionsToTxn', [0]], ['Produce', 0, 1000, 0, 0, 1], ['AddPartitionsToTxn', [1]], ['Produce', 1, 1000, 0, 0, 1], ['EndTxn', 'abort', 1000, 0], ['InitProducerId', 1000, 0]],
    expectedSettlement: [['m1', 'rejected', 'KafkaJSDeliveryTimeout', 'unknown'], ['m2', 'rejected', 'KafkaJSConnectionError', null], ['end', 'fulfilled']],
    expectedState: 'READY',
    expectedLog: [[0, 1000, 0, 0, 0], [1, 1000, 0, 0, 0]],
    expectedNoops: ['fail:0'],
  },
  {
    name: 'tv1 two-inflight expiry>answer>reconnect>timeout',
    kind: 'transactional',
    profile: 'tv1',
    shape: 'two-inflight',
    fault: 'expiry>answer>reconnect>timeout',
    commands: [{ op: 'event', event: 'expiry' }, { op: 'event', event: 'answer' }, { op: 'event', event: 'reconnect' }, { op: 'event', event: 'timeout' }],
    expectedWire: [['InitProducerId', -1, -1], ['InitProducerId', -1, -1], ['AddPartitionsToTxn', [0]], ['Produce', 0, 1000, 0, 0, 1], ['AddPartitionsToTxn', [1]], ['Produce', 1, 1000, 0, 0, 1], ['EndTxn', 'abort', 1000, 0], ['InitProducerId', 1000, 0]],
    expectedSettlement: [['m1', 'rejected', 'KafkaJSDeliveryTimeout', 'unknown'], ['m2', 'rejected', 'KafkaJSConnectionError', null], ['end', 'fulfilled']],
    expectedState: 'READY',
    expectedLog: [[0, 1000, 0, 0, 0], [1, 1000, 0, 0, 0]],
    expectedNoops: ['fail:0'],
  },
  {
    name: 'tv1 two-inflight expiry>timeout>answer>reconnect',
    kind: 'transactional',
    profile: 'tv1',
    shape: 'two-inflight',
    fault: 'expiry>timeout>answer>reconnect',
    commands: [{ op: 'event', event: 'expiry' }, { op: 'event', event: 'timeout' }, { op: 'event', event: 'answer' }, { op: 'event', event: 'reconnect' }],
    expectedWire: [['InitProducerId', -1, -1], ['InitProducerId', -1, -1], ['AddPartitionsToTxn', [0]], ['Produce', 0, 1000, 0, 0, 1], ['AddPartitionsToTxn', [1]], ['Produce', 1, 1000, 0, 0, 1], ['EndTxn', 'abort', 1000, 0], ['InitProducerId', 1000, 0]],
    expectedSettlement: [['m1', 'rejected', 'KafkaJSDeliveryTimeout', 'unknown'], ['m2', 'rejected', 'KafkaJSConnectionError', null], ['end', 'fulfilled']],
    expectedState: 'READY',
    expectedLog: [[0, 1000, 0, 0, 0], [1, 1000, 0, 0, 0]],
    expectedNoops: ['release:0', 'destroy:0'],
  },
  {
    name: 'tv1 two-inflight expiry>timeout>reconnect>answer',
    kind: 'transactional',
    profile: 'tv1',
    shape: 'two-inflight',
    fault: 'expiry>timeout>reconnect>answer',
    commands: [{ op: 'event', event: 'expiry' }, { op: 'event', event: 'timeout' }, { op: 'event', event: 'reconnect' }, { op: 'event', event: 'answer' }],
    expectedWire: [['InitProducerId', -1, -1], ['InitProducerId', -1, -1], ['AddPartitionsToTxn', [0]], ['Produce', 0, 1000, 0, 0, 1], ['AddPartitionsToTxn', [1]], ['Produce', 1, 1000, 0, 0, 1], ['EndTxn', 'abort', 1000, 0], ['InitProducerId', 1000, 0]],
    expectedSettlement: [['m1', 'rejected', 'KafkaJSDeliveryTimeout', 'unknown'], ['m2', 'rejected', 'KafkaJSConnectionError', null], ['end', 'fulfilled']],
    expectedState: 'READY',
    expectedLog: [[0, 1000, 0, 0, 0], [1, 1000, 0, 0, 0]],
    expectedNoops: ['destroy:0', 'release:0'],
  },
  {
    name: 'tv1 two-inflight expiry>reconnect>answer>timeout',
    kind: 'transactional',
    profile: 'tv1',
    shape: 'two-inflight',
    fault: 'expiry>reconnect>answer>timeout',
    commands: [{ op: 'event', event: 'expiry' }, { op: 'event', event: 'reconnect' }, { op: 'event', event: 'answer' }, { op: 'event', event: 'timeout' }],
    expectedWire: [['InitProducerId', -1, -1], ['InitProducerId', -1, -1], ['AddPartitionsToTxn', [0]], ['Produce', 0, 1000, 0, 0, 1], ['AddPartitionsToTxn', [1]], ['Produce', 1, 1000, 0, 0, 1], ['EndTxn', 'abort', 1000, 0], ['InitProducerId', 1000, 0]],
    expectedSettlement: [['m1', 'rejected', 'KafkaJSDeliveryTimeout', 'unknown'], ['m2', 'rejected', 'KafkaJSConnectionError', null], ['end', 'fulfilled']],
    expectedState: 'READY',
    expectedLog: [[0, 1000, 0, 0, 0], [1, 1000, 0, 0, 0]],
    expectedNoops: ['release:0', 'fail:0'],
  },
  {
    name: 'tv1 two-inflight expiry>reconnect>timeout>answer',
    kind: 'transactional',
    profile: 'tv1',
    shape: 'two-inflight',
    fault: 'expiry>reconnect>timeout>answer',
    commands: [{ op: 'event', event: 'expiry' }, { op: 'event', event: 'reconnect' }, { op: 'event', event: 'timeout' }, { op: 'event', event: 'answer' }],
    expectedWire: [['InitProducerId', -1, -1], ['InitProducerId', -1, -1], ['AddPartitionsToTxn', [0]], ['Produce', 0, 1000, 0, 0, 1], ['AddPartitionsToTxn', [1]], ['Produce', 1, 1000, 0, 0, 1], ['EndTxn', 'abort', 1000, 0], ['InitProducerId', 1000, 0]],
    expectedSettlement: [['m1', 'rejected', 'KafkaJSDeliveryTimeout', 'unknown'], ['m2', 'rejected', 'KafkaJSConnectionError', null], ['end', 'fulfilled']],
    expectedState: 'READY',
    expectedLog: [[0, 1000, 0, 0, 0], [1, 1000, 0, 0, 0]],
    expectedNoops: ['fail:0', 'release:0'],
  },
  {
    name: 'tv2 inflight-and-queued answer>timeout>reconnect>expiry',
    kind: 'transactional',
    profile: 'tv2',
    shape: 'inflight-and-queued',
    fault: 'answer>timeout>reconnect>expiry',
    commands: [{ op: 'event', event: 'answer' }, { op: 'event', event: 'timeout' }, { op: 'event', event: 'reconnect' }, { op: 'event', event: 'expiry' }],
    expectedWire: [['InitProducerId', -1, -1], ['InitProducerId', -1, -1], ['Produce', 0, 1000, 0, 0, 1], ['Produce', 0, 1000, 0, 1, 1], ['Produce', 0, 1000, 0, 1, 2], ['EndTxn', 'commit', 1000, 0]],
    expectedSettlement: [['m1', 'fulfilled', '0'], ['m2', 'fulfilled', '-1'], ['end', 'fulfilled']],
    expectedState: 'READY',
    expectedLog: [[0, 1000, 0, 0, 0], [0, 1000, 0, 1, 1]],
    expectedNoops: ['fail:0'],
  },
  {
    name: 'tv2 inflight-and-queued answer>timeout>expiry>reconnect',
    kind: 'transactional',
    profile: 'tv2',
    shape: 'inflight-and-queued',
    fault: 'answer>timeout>expiry>reconnect',
    commands: [{ op: 'event', event: 'answer' }, { op: 'event', event: 'timeout' }, { op: 'event', event: 'expiry' }, { op: 'event', event: 'reconnect' }],
    expectedWire: [['InitProducerId', -1, -1], ['InitProducerId', -1, -1], ['Produce', 0, 1000, 0, 0, 1], ['Produce', 0, 1000, 0, 1, 1], ['Produce', 0, 1000, 0, 1, 2], ['EndTxn', 'commit', 1000, 0]],
    expectedSettlement: [['m1', 'fulfilled', '0'], ['m2', 'fulfilled', '-1'], ['end', 'fulfilled']],
    expectedState: 'READY',
    expectedLog: [[0, 1000, 0, 0, 0], [0, 1000, 0, 1, 1]],
    expectedNoops: ['fail:0'],
  },
  {
    name: 'tv2 inflight-and-queued answer>reconnect>timeout>expiry',
    kind: 'transactional',
    profile: 'tv2',
    shape: 'inflight-and-queued',
    fault: 'answer>reconnect>timeout>expiry',
    commands: [{ op: 'event', event: 'answer' }, { op: 'event', event: 'reconnect' }, { op: 'event', event: 'timeout' }, { op: 'event', event: 'expiry' }],
    expectedWire: [['InitProducerId', -1, -1], ['InitProducerId', -1, -1], ['Produce', 0, 1000, 0, 0, 1], ['Produce', 0, 1000, 0, 1, 1], ['Produce', 0, 1000, 0, 1, 2], ['EndTxn', 'commit', 1000, 0]],
    expectedSettlement: [['m1', 'fulfilled', '0'], ['m2', 'fulfilled', '-1'], ['end', 'fulfilled']],
    expectedState: 'READY',
    expectedLog: [[0, 1000, 0, 0, 0], [0, 1000, 0, 1, 1]],
    expectedNoops: ['fail:0'],
  },
  {
    name: 'tv2 inflight-and-queued answer>reconnect>expiry>timeout',
    kind: 'transactional',
    profile: 'tv2',
    shape: 'inflight-and-queued',
    fault: 'answer>reconnect>expiry>timeout',
    commands: [{ op: 'event', event: 'answer' }, { op: 'event', event: 'reconnect' }, { op: 'event', event: 'expiry' }, { op: 'event', event: 'timeout' }],
    expectedWire: [['InitProducerId', -1, -1], ['InitProducerId', -1, -1], ['Produce', 0, 1000, 0, 0, 1], ['Produce', 0, 1000, 0, 1, 1], ['Produce', 0, 1000, 0, 1, 2], ['EndTxn', 'commit', 1000, 0]],
    expectedSettlement: [['m1', 'fulfilled', '0'], ['m2', 'fulfilled', '-1'], ['end', 'fulfilled']],
    expectedState: 'READY',
    expectedLog: [[0, 1000, 0, 0, 0], [0, 1000, 0, 1, 1]],
    expectedNoops: ['fail:0'],
  },
  {
    name: 'tv2 inflight-and-queued answer>expiry>timeout>reconnect',
    kind: 'transactional',
    profile: 'tv2',
    shape: 'inflight-and-queued',
    fault: 'answer>expiry>timeout>reconnect',
    commands: [{ op: 'event', event: 'answer' }, { op: 'event', event: 'expiry' }, { op: 'event', event: 'timeout' }, { op: 'event', event: 'reconnect' }],
    expectedWire: [['InitProducerId', -1, -1], ['InitProducerId', -1, -1], ['Produce', 0, 1000, 0, 0, 1], ['Produce', 0, 1000, 0, 1, 1], ['Produce', 0, 1000, 0, 1, 2], ['EndTxn', 'commit', 1000, 0]],
    expectedSettlement: [['m1', 'fulfilled', '0'], ['m2', 'fulfilled', '-1'], ['end', 'fulfilled']],
    expectedState: 'READY',
    expectedLog: [[0, 1000, 0, 0, 0], [0, 1000, 0, 1, 1]],
    expectedNoops: ['fail:0'],
  },
  {
    name: 'tv2 inflight-and-queued answer>expiry>reconnect>timeout',
    kind: 'transactional',
    profile: 'tv2',
    shape: 'inflight-and-queued',
    fault: 'answer>expiry>reconnect>timeout',
    commands: [{ op: 'event', event: 'answer' }, { op: 'event', event: 'expiry' }, { op: 'event', event: 'reconnect' }, { op: 'event', event: 'timeout' }],
    expectedWire: [['InitProducerId', -1, -1], ['InitProducerId', -1, -1], ['Produce', 0, 1000, 0, 0, 1], ['Produce', 0, 1000, 0, 1, 1], ['Produce', 0, 1000, 0, 1, 2], ['EndTxn', 'commit', 1000, 0]],
    expectedSettlement: [['m1', 'fulfilled', '0'], ['m2', 'fulfilled', '-1'], ['end', 'fulfilled']],
    expectedState: 'READY',
    expectedLog: [[0, 1000, 0, 0, 0], [0, 1000, 0, 1, 1]],
    expectedNoops: ['fail:0'],
  },
  {
    name: 'tv2 inflight-and-queued timeout>answer>reconnect>expiry',
    kind: 'transactional',
    profile: 'tv2',
    shape: 'inflight-and-queued',
    fault: 'timeout>answer>reconnect>expiry',
    commands: [{ op: 'event', event: 'timeout' }, { op: 'event', event: 'answer' }, { op: 'event', event: 'reconnect' }, { op: 'event', event: 'expiry' }],
    expectedWire: [['InitProducerId', -1, -1], ['InitProducerId', -1, -1], ['Produce', 0, 1000, 0, 0, 1], ['Produce', 0, 1000, 0, 0, 2], ['EndTxn', 'abort', 1000, 0]],
    expectedSettlement: [['m1', 'rejected', 'KafkaJSDeliveryTimeout', 'unknown'], ['m2', 'rejected', 'KafkaJSTransactionAbortableError', null], ['end', 'fulfilled']],
    expectedState: 'READY',
    expectedLog: [[0, 1000, 0, 0, 0]],
    expectedNoops: ['release:0', 'destroy:0'],
  },
  {
    name: 'tv2 inflight-and-queued timeout>answer>expiry>reconnect',
    kind: 'transactional',
    profile: 'tv2',
    shape: 'inflight-and-queued',
    fault: 'timeout>answer>expiry>reconnect',
    commands: [{ op: 'event', event: 'timeout' }, { op: 'event', event: 'answer' }, { op: 'event', event: 'expiry' }, { op: 'event', event: 'reconnect' }],
    expectedWire: [['InitProducerId', -1, -1], ['InitProducerId', -1, -1], ['Produce', 0, 1000, 0, 0, 1], ['Produce', 0, 1000, 0, 0, 2], ['EndTxn', 'abort', 1000, 0]],
    expectedSettlement: [['m1', 'rejected', 'KafkaJSDeliveryTimeout', 'unknown'], ['m2', 'rejected', 'KafkaJSTransactionAbortableError', null], ['end', 'fulfilled']],
    expectedState: 'READY',
    expectedLog: [[0, 1000, 0, 0, 0]],
    expectedNoops: ['release:0', 'destroy:0'],
  },
  {
    name: 'tv2 inflight-and-queued timeout>reconnect>answer>expiry',
    kind: 'transactional',
    profile: 'tv2',
    shape: 'inflight-and-queued',
    fault: 'timeout>reconnect>answer>expiry',
    commands: [{ op: 'event', event: 'timeout' }, { op: 'event', event: 'reconnect' }, { op: 'event', event: 'answer' }, { op: 'event', event: 'expiry' }],
    expectedWire: [['InitProducerId', -1, -1], ['InitProducerId', -1, -1], ['Produce', 0, 1000, 0, 0, 1], ['Produce', 0, 1000, 0, 0, 2], ['EndTxn', 'abort', 1000, 0]],
    expectedSettlement: [['m1', 'rejected', 'KafkaJSDeliveryTimeout', 'unknown'], ['m2', 'rejected', 'KafkaJSTransactionAbortableError', null], ['end', 'fulfilled']],
    expectedState: 'READY',
    expectedLog: [[0, 1000, 0, 0, 0]],
    expectedNoops: ['destroy:0', 'release:0'],
  },
  {
    name: 'tv2 inflight-and-queued timeout>reconnect>expiry>answer',
    kind: 'transactional',
    profile: 'tv2',
    shape: 'inflight-and-queued',
    fault: 'timeout>reconnect>expiry>answer',
    commands: [{ op: 'event', event: 'timeout' }, { op: 'event', event: 'reconnect' }, { op: 'event', event: 'expiry' }, { op: 'event', event: 'answer' }],
    expectedWire: [['InitProducerId', -1, -1], ['InitProducerId', -1, -1], ['Produce', 0, 1000, 0, 0, 1], ['Produce', 0, 1000, 0, 0, 2], ['EndTxn', 'abort', 1000, 0]],
    expectedSettlement: [['m1', 'rejected', 'KafkaJSDeliveryTimeout', 'unknown'], ['m2', 'rejected', 'KafkaJSTransactionAbortableError', null], ['end', 'fulfilled']],
    expectedState: 'READY',
    expectedLog: [[0, 1000, 0, 0, 0]],
    expectedNoops: ['destroy:0', 'release:0'],
  },
  {
    name: 'tv2 inflight-and-queued timeout>expiry>answer>reconnect',
    kind: 'transactional',
    profile: 'tv2',
    shape: 'inflight-and-queued',
    fault: 'timeout>expiry>answer>reconnect',
    commands: [{ op: 'event', event: 'timeout' }, { op: 'event', event: 'expiry' }, { op: 'event', event: 'answer' }, { op: 'event', event: 'reconnect' }],
    expectedWire: [['InitProducerId', -1, -1], ['InitProducerId', -1, -1], ['Produce', 0, 1000, 0, 0, 1], ['Produce', 0, 1000, 0, 0, 2], ['EndTxn', 'abort', 1000, 0]],
    expectedSettlement: [['m1', 'rejected', 'KafkaJSDeliveryTimeout', 'unknown'], ['m2', 'rejected', 'KafkaJSTransactionAbortableError', null], ['end', 'fulfilled']],
    expectedState: 'READY',
    expectedLog: [[0, 1000, 0, 0, 0]],
    expectedNoops: ['release:0', 'destroy:0'],
  },
  {
    name: 'tv2 inflight-and-queued timeout>expiry>reconnect>answer',
    kind: 'transactional',
    profile: 'tv2',
    shape: 'inflight-and-queued',
    fault: 'timeout>expiry>reconnect>answer',
    commands: [{ op: 'event', event: 'timeout' }, { op: 'event', event: 'expiry' }, { op: 'event', event: 'reconnect' }, { op: 'event', event: 'answer' }],
    expectedWire: [['InitProducerId', -1, -1], ['InitProducerId', -1, -1], ['Produce', 0, 1000, 0, 0, 1], ['Produce', 0, 1000, 0, 0, 2], ['EndTxn', 'abort', 1000, 0]],
    expectedSettlement: [['m1', 'rejected', 'KafkaJSDeliveryTimeout', 'unknown'], ['m2', 'rejected', 'KafkaJSTransactionAbortableError', null], ['end', 'fulfilled']],
    expectedState: 'READY',
    expectedLog: [[0, 1000, 0, 0, 0]],
    expectedNoops: ['destroy:0', 'release:0'],
  },
  {
    name: 'tv2 inflight-and-queued reconnect>answer>timeout>expiry',
    kind: 'transactional',
    profile: 'tv2',
    shape: 'inflight-and-queued',
    fault: 'reconnect>answer>timeout>expiry',
    commands: [{ op: 'event', event: 'reconnect' }, { op: 'event', event: 'answer' }, { op: 'event', event: 'timeout' }, { op: 'event', event: 'expiry' }],
    expectedWire: [['InitProducerId', -1, -1], ['InitProducerId', -1, -1], ['Produce', 0, 1000, 0, 0, 1], ['Produce', 0, 1000, 0, 0, 2], ['EndTxn', 'abort', 1000, 0]],
    expectedSettlement: [['m1', 'rejected', 'KafkaJSDeliveryTimeout', 'unknown'], ['m2', 'rejected', 'KafkaJSTransactionAbortableError', null], ['end', 'fulfilled']],
    expectedState: 'READY',
    expectedLog: [[0, 1000, 0, 0, 0]],
    expectedNoops: ['release:0', 'fail:0'],
  },
  {
    name: 'tv2 inflight-and-queued reconnect>answer>expiry>timeout',
    kind: 'transactional',
    profile: 'tv2',
    shape: 'inflight-and-queued',
    fault: 'reconnect>answer>expiry>timeout',
    commands: [{ op: 'event', event: 'reconnect' }, { op: 'event', event: 'answer' }, { op: 'event', event: 'expiry' }, { op: 'event', event: 'timeout' }],
    expectedWire: [['InitProducerId', -1, -1], ['InitProducerId', -1, -1], ['Produce', 0, 1000, 0, 0, 1], ['Produce', 0, 1000, 0, 0, 2], ['EndTxn', 'abort', 1000, 0]],
    expectedSettlement: [['m1', 'rejected', 'KafkaJSDeliveryTimeout', 'unknown'], ['m2', 'rejected', 'KafkaJSTransactionAbortableError', null], ['end', 'fulfilled']],
    expectedState: 'READY',
    expectedLog: [[0, 1000, 0, 0, 0]],
    expectedNoops: ['release:0', 'fail:0'],
  },
  {
    name: 'tv2 inflight-and-queued reconnect>timeout>answer>expiry',
    kind: 'transactional',
    profile: 'tv2',
    shape: 'inflight-and-queued',
    fault: 'reconnect>timeout>answer>expiry',
    commands: [{ op: 'event', event: 'reconnect' }, { op: 'event', event: 'timeout' }, { op: 'event', event: 'answer' }, { op: 'event', event: 'expiry' }],
    expectedWire: [['InitProducerId', -1, -1], ['InitProducerId', -1, -1], ['Produce', 0, 1000, 0, 0, 1], ['Produce', 0, 1000, 0, 0, 2], ['EndTxn', 'abort', 1000, 0]],
    expectedSettlement: [['m1', 'rejected', 'KafkaJSDeliveryTimeout', 'unknown'], ['m2', 'rejected', 'KafkaJSTransactionAbortableError', null], ['end', 'fulfilled']],
    expectedState: 'READY',
    expectedLog: [[0, 1000, 0, 0, 0]],
    expectedNoops: ['fail:0', 'release:0'],
  },
  {
    name: 'tv2 inflight-and-queued reconnect>timeout>expiry>answer',
    kind: 'transactional',
    profile: 'tv2',
    shape: 'inflight-and-queued',
    fault: 'reconnect>timeout>expiry>answer',
    commands: [{ op: 'event', event: 'reconnect' }, { op: 'event', event: 'timeout' }, { op: 'event', event: 'expiry' }, { op: 'event', event: 'answer' }],
    expectedWire: [['InitProducerId', -1, -1], ['InitProducerId', -1, -1], ['Produce', 0, 1000, 0, 0, 1], ['Produce', 0, 1000, 0, 0, 2], ['EndTxn', 'abort', 1000, 0]],
    expectedSettlement: [['m1', 'rejected', 'KafkaJSDeliveryTimeout', 'unknown'], ['m2', 'rejected', 'KafkaJSTransactionAbortableError', null], ['end', 'fulfilled']],
    expectedState: 'READY',
    expectedLog: [[0, 1000, 0, 0, 0]],
    expectedNoops: ['fail:0', 'release:0'],
  },
  {
    name: 'tv2 inflight-and-queued reconnect>expiry>answer>timeout',
    kind: 'transactional',
    profile: 'tv2',
    shape: 'inflight-and-queued',
    fault: 'reconnect>expiry>answer>timeout',
    commands: [{ op: 'event', event: 'reconnect' }, { op: 'event', event: 'expiry' }, { op: 'event', event: 'answer' }, { op: 'event', event: 'timeout' }],
    expectedWire: [['InitProducerId', -1, -1], ['InitProducerId', -1, -1], ['Produce', 0, 1000, 0, 0, 1], ['Produce', 0, 1000, 0, 0, 2], ['EndTxn', 'abort', 1000, 0]],
    expectedSettlement: [['m1', 'rejected', 'KafkaJSDeliveryTimeout', 'unknown'], ['m2', 'rejected', 'KafkaJSTransactionAbortableError', null], ['end', 'fulfilled']],
    expectedState: 'READY',
    expectedLog: [[0, 1000, 0, 0, 0]],
    expectedNoops: ['release:0', 'fail:0'],
  },
  {
    name: 'tv2 inflight-and-queued reconnect>expiry>timeout>answer',
    kind: 'transactional',
    profile: 'tv2',
    shape: 'inflight-and-queued',
    fault: 'reconnect>expiry>timeout>answer',
    commands: [{ op: 'event', event: 'reconnect' }, { op: 'event', event: 'expiry' }, { op: 'event', event: 'timeout' }, { op: 'event', event: 'answer' }],
    expectedWire: [['InitProducerId', -1, -1], ['InitProducerId', -1, -1], ['Produce', 0, 1000, 0, 0, 1], ['Produce', 0, 1000, 0, 0, 2], ['EndTxn', 'abort', 1000, 0]],
    expectedSettlement: [['m1', 'rejected', 'KafkaJSDeliveryTimeout', 'unknown'], ['m2', 'rejected', 'KafkaJSTransactionAbortableError', null], ['end', 'fulfilled']],
    expectedState: 'READY',
    expectedLog: [[0, 1000, 0, 0, 0]],
    expectedNoops: ['fail:0', 'release:0'],
  },
  {
    name: 'tv2 inflight-and-queued expiry>answer>timeout>reconnect',
    kind: 'transactional',
    profile: 'tv2',
    shape: 'inflight-and-queued',
    fault: 'expiry>answer>timeout>reconnect',
    commands: [{ op: 'event', event: 'expiry' }, { op: 'event', event: 'answer' }, { op: 'event', event: 'timeout' }, { op: 'event', event: 'reconnect' }],
    expectedWire: [['InitProducerId', -1, -1], ['InitProducerId', -1, -1], ['Produce', 0, 1000, 0, 0, 1], ['EndTxn', 'abort', 1000, 0]],
    expectedSettlement: [['m1', 'rejected', 'KafkaJSDeliveryTimeout', 'unknown'], ['m2', 'rejected', 'KafkaJSTransactionAbortableError', null], ['end', 'fulfilled']],
    expectedState: 'READY',
    expectedLog: [[0, 1000, 0, 0, 0]],
    expectedNoops: ['fail:0'],
  },
  {
    name: 'tv2 inflight-and-queued expiry>answer>reconnect>timeout',
    kind: 'transactional',
    profile: 'tv2',
    shape: 'inflight-and-queued',
    fault: 'expiry>answer>reconnect>timeout',
    commands: [{ op: 'event', event: 'expiry' }, { op: 'event', event: 'answer' }, { op: 'event', event: 'reconnect' }, { op: 'event', event: 'timeout' }],
    expectedWire: [['InitProducerId', -1, -1], ['InitProducerId', -1, -1], ['Produce', 0, 1000, 0, 0, 1], ['EndTxn', 'abort', 1000, 0]],
    expectedSettlement: [['m1', 'rejected', 'KafkaJSDeliveryTimeout', 'unknown'], ['m2', 'rejected', 'KafkaJSTransactionAbortableError', null], ['end', 'fulfilled']],
    expectedState: 'READY',
    expectedLog: [[0, 1000, 0, 0, 0]],
    expectedNoops: ['fail:0'],
  },
  {
    name: 'tv2 inflight-and-queued expiry>timeout>answer>reconnect',
    kind: 'transactional',
    profile: 'tv2',
    shape: 'inflight-and-queued',
    fault: 'expiry>timeout>answer>reconnect',
    commands: [{ op: 'event', event: 'expiry' }, { op: 'event', event: 'timeout' }, { op: 'event', event: 'answer' }, { op: 'event', event: 'reconnect' }],
    expectedWire: [['InitProducerId', -1, -1], ['InitProducerId', -1, -1], ['Produce', 0, 1000, 0, 0, 1], ['EndTxn', 'abort', 1000, 0]],
    expectedSettlement: [['m1', 'rejected', 'KafkaJSDeliveryTimeout', 'unknown'], ['m2', 'rejected', 'KafkaJSTransactionAbortableError', null], ['end', 'fulfilled']],
    expectedState: 'READY',
    expectedLog: [[0, 1000, 0, 0, 0]],
    expectedNoops: ['release:0', 'destroy:0'],
  },
  {
    name: 'tv2 inflight-and-queued expiry>timeout>reconnect>answer',
    kind: 'transactional',
    profile: 'tv2',
    shape: 'inflight-and-queued',
    fault: 'expiry>timeout>reconnect>answer',
    commands: [{ op: 'event', event: 'expiry' }, { op: 'event', event: 'timeout' }, { op: 'event', event: 'reconnect' }, { op: 'event', event: 'answer' }],
    expectedWire: [['InitProducerId', -1, -1], ['InitProducerId', -1, -1], ['Produce', 0, 1000, 0, 0, 1], ['EndTxn', 'abort', 1000, 0]],
    expectedSettlement: [['m1', 'rejected', 'KafkaJSDeliveryTimeout', 'unknown'], ['m2', 'rejected', 'KafkaJSTransactionAbortableError', null], ['end', 'fulfilled']],
    expectedState: 'READY',
    expectedLog: [[0, 1000, 0, 0, 0]],
    expectedNoops: ['destroy:0', 'release:0'],
  },
  {
    name: 'tv2 inflight-and-queued expiry>reconnect>answer>timeout',
    kind: 'transactional',
    profile: 'tv2',
    shape: 'inflight-and-queued',
    fault: 'expiry>reconnect>answer>timeout',
    commands: [{ op: 'event', event: 'expiry' }, { op: 'event', event: 'reconnect' }, { op: 'event', event: 'answer' }, { op: 'event', event: 'timeout' }],
    expectedWire: [['InitProducerId', -1, -1], ['InitProducerId', -1, -1], ['Produce', 0, 1000, 0, 0, 1], ['EndTxn', 'abort', 1000, 0]],
    expectedSettlement: [['m1', 'rejected', 'KafkaJSDeliveryTimeout', 'unknown'], ['m2', 'rejected', 'KafkaJSTransactionAbortableError', null], ['end', 'fulfilled']],
    expectedState: 'READY',
    expectedLog: [[0, 1000, 0, 0, 0]],
    expectedNoops: ['release:0', 'fail:0'],
  },
  {
    name: 'tv2 inflight-and-queued expiry>reconnect>timeout>answer',
    kind: 'transactional',
    profile: 'tv2',
    shape: 'inflight-and-queued',
    fault: 'expiry>reconnect>timeout>answer',
    commands: [{ op: 'event', event: 'expiry' }, { op: 'event', event: 'reconnect' }, { op: 'event', event: 'timeout' }, { op: 'event', event: 'answer' }],
    expectedWire: [['InitProducerId', -1, -1], ['InitProducerId', -1, -1], ['Produce', 0, 1000, 0, 0, 1], ['EndTxn', 'abort', 1000, 0]],
    expectedSettlement: [['m1', 'rejected', 'KafkaJSDeliveryTimeout', 'unknown'], ['m2', 'rejected', 'KafkaJSTransactionAbortableError', null], ['end', 'fulfilled']],
    expectedState: 'READY',
    expectedLog: [[0, 1000, 0, 0, 0]],
    expectedNoops: ['fail:0', 'release:0'],
  },
  {
    name: 'tv2 two-inflight answer>timeout>reconnect>expiry',
    kind: 'transactional',
    profile: 'tv2',
    shape: 'two-inflight',
    fault: 'answer>timeout>reconnect>expiry',
    commands: [{ op: 'event', event: 'answer' }, { op: 'event', event: 'timeout' }, { op: 'event', event: 'reconnect' }, { op: 'event', event: 'expiry' }],
    expectedWire: [['InitProducerId', -1, -1], ['InitProducerId', -1, -1], ['Produce', 0, 1000, 0, 0, 1], ['Produce', 1, 1000, 0, 0, 1], ['Produce', 1, 1000, 0, 0, 2], ['EndTxn', 'commit', 1000, 0]],
    expectedSettlement: [['m1', 'fulfilled', '0'], ['m2', 'fulfilled', '-1'], ['end', 'fulfilled']],
    expectedState: 'READY',
    expectedLog: [[0, 1000, 0, 0, 0], [1, 1000, 0, 0, 0]],
    expectedNoops: ['fail:0'],
  },
  {
    name: 'tv2 two-inflight answer>timeout>expiry>reconnect',
    kind: 'transactional',
    profile: 'tv2',
    shape: 'two-inflight',
    fault: 'answer>timeout>expiry>reconnect',
    commands: [{ op: 'event', event: 'answer' }, { op: 'event', event: 'timeout' }, { op: 'event', event: 'expiry' }, { op: 'event', event: 'reconnect' }],
    expectedWire: [['InitProducerId', -1, -1], ['InitProducerId', -1, -1], ['Produce', 0, 1000, 0, 0, 1], ['Produce', 1, 1000, 0, 0, 1], ['Produce', 1, 1000, 0, 0, 2], ['EndTxn', 'commit', 1000, 0]],
    expectedSettlement: [['m1', 'fulfilled', '0'], ['m2', 'fulfilled', '-1'], ['end', 'fulfilled']],
    expectedState: 'READY',
    expectedLog: [[0, 1000, 0, 0, 0], [1, 1000, 0, 0, 0]],
    expectedNoops: ['fail:0'],
  },
  {
    name: 'tv2 two-inflight answer>reconnect>timeout>expiry',
    kind: 'transactional',
    profile: 'tv2',
    shape: 'two-inflight',
    fault: 'answer>reconnect>timeout>expiry',
    commands: [{ op: 'event', event: 'answer' }, { op: 'event', event: 'reconnect' }, { op: 'event', event: 'timeout' }, { op: 'event', event: 'expiry' }],
    expectedWire: [['InitProducerId', -1, -1], ['InitProducerId', -1, -1], ['Produce', 0, 1000, 0, 0, 1], ['Produce', 1, 1000, 0, 0, 1], ['Produce', 1, 1000, 0, 0, 2], ['EndTxn', 'commit', 1000, 0]],
    expectedSettlement: [['m1', 'fulfilled', '0'], ['m2', 'fulfilled', '-1'], ['end', 'fulfilled']],
    expectedState: 'READY',
    expectedLog: [[0, 1000, 0, 0, 0], [1, 1000, 0, 0, 0]],
    expectedNoops: ['fail:0'],
  },
  {
    name: 'tv2 two-inflight answer>reconnect>expiry>timeout',
    kind: 'transactional',
    profile: 'tv2',
    shape: 'two-inflight',
    fault: 'answer>reconnect>expiry>timeout',
    commands: [{ op: 'event', event: 'answer' }, { op: 'event', event: 'reconnect' }, { op: 'event', event: 'expiry' }, { op: 'event', event: 'timeout' }],
    expectedWire: [['InitProducerId', -1, -1], ['InitProducerId', -1, -1], ['Produce', 0, 1000, 0, 0, 1], ['Produce', 1, 1000, 0, 0, 1], ['Produce', 1, 1000, 0, 0, 2], ['EndTxn', 'commit', 1000, 0]],
    expectedSettlement: [['m1', 'fulfilled', '0'], ['m2', 'fulfilled', '-1'], ['end', 'fulfilled']],
    expectedState: 'READY',
    expectedLog: [[0, 1000, 0, 0, 0], [1, 1000, 0, 0, 0]],
    expectedNoops: ['fail:0'],
  },
  {
    name: 'tv2 two-inflight answer>expiry>timeout>reconnect',
    kind: 'transactional',
    profile: 'tv2',
    shape: 'two-inflight',
    fault: 'answer>expiry>timeout>reconnect',
    commands: [{ op: 'event', event: 'answer' }, { op: 'event', event: 'expiry' }, { op: 'event', event: 'timeout' }, { op: 'event', event: 'reconnect' }],
    expectedWire: [['InitProducerId', -1, -1], ['InitProducerId', -1, -1], ['Produce', 0, 1000, 0, 0, 1], ['Produce', 1, 1000, 0, 0, 1], ['Produce', 1, 1000, 0, 0, 2], ['EndTxn', 'commit', 1000, 0]],
    expectedSettlement: [['m1', 'fulfilled', '0'], ['m2', 'fulfilled', '-1'], ['end', 'fulfilled']],
    expectedState: 'READY',
    expectedLog: [[0, 1000, 0, 0, 0], [1, 1000, 0, 0, 0]],
    expectedNoops: ['fail:0'],
  },
  {
    name: 'tv2 two-inflight answer>expiry>reconnect>timeout',
    kind: 'transactional',
    profile: 'tv2',
    shape: 'two-inflight',
    fault: 'answer>expiry>reconnect>timeout',
    commands: [{ op: 'event', event: 'answer' }, { op: 'event', event: 'expiry' }, { op: 'event', event: 'reconnect' }, { op: 'event', event: 'timeout' }],
    expectedWire: [['InitProducerId', -1, -1], ['InitProducerId', -1, -1], ['Produce', 0, 1000, 0, 0, 1], ['Produce', 1, 1000, 0, 0, 1], ['Produce', 1, 1000, 0, 0, 2], ['EndTxn', 'commit', 1000, 0]],
    expectedSettlement: [['m1', 'fulfilled', '0'], ['m2', 'fulfilled', '-1'], ['end', 'fulfilled']],
    expectedState: 'READY',
    expectedLog: [[0, 1000, 0, 0, 0], [1, 1000, 0, 0, 0]],
    expectedNoops: ['fail:0'],
  },
  {
    name: 'tv2 two-inflight timeout>answer>reconnect>expiry',
    kind: 'transactional',
    profile: 'tv2',
    shape: 'two-inflight',
    fault: 'timeout>answer>reconnect>expiry',
    commands: [{ op: 'event', event: 'timeout' }, { op: 'event', event: 'answer' }, { op: 'event', event: 'reconnect' }, { op: 'event', event: 'expiry' }],
    expectedWire: [['InitProducerId', -1, -1], ['InitProducerId', -1, -1], ['Produce', 0, 1000, 0, 0, 1], ['Produce', 1, 1000, 0, 0, 1], ['Produce', 0, 1000, 0, 0, 2], ['Produce', 1, 1000, 0, 0, 2], ['EndTxn', 'abort', 1000, 0]],
    expectedSettlement: [['m1', 'rejected', 'KafkaJSDeliveryTimeout', 'unknown'], ['m2', 'fulfilled', '-1'], ['end', 'fulfilled']],
    expectedState: 'READY',
    expectedLog: [[0, 1000, 0, 0, 0], [1, 1000, 0, 0, 0]],
    expectedNoops: ['release:0', 'destroy:0'],
  },
  {
    name: 'tv2 two-inflight timeout>answer>expiry>reconnect',
    kind: 'transactional',
    profile: 'tv2',
    shape: 'two-inflight',
    fault: 'timeout>answer>expiry>reconnect',
    commands: [{ op: 'event', event: 'timeout' }, { op: 'event', event: 'answer' }, { op: 'event', event: 'expiry' }, { op: 'event', event: 'reconnect' }],
    expectedWire: [['InitProducerId', -1, -1], ['InitProducerId', -1, -1], ['Produce', 0, 1000, 0, 0, 1], ['Produce', 1, 1000, 0, 0, 1], ['Produce', 0, 1000, 0, 0, 2], ['Produce', 1, 1000, 0, 0, 2], ['EndTxn', 'abort', 1000, 0]],
    expectedSettlement: [['m1', 'rejected', 'KafkaJSDeliveryTimeout', 'unknown'], ['m2', 'fulfilled', '-1'], ['end', 'fulfilled']],
    expectedState: 'READY',
    expectedLog: [[0, 1000, 0, 0, 0], [1, 1000, 0, 0, 0]],
    expectedNoops: ['release:0', 'destroy:0'],
  },
  {
    name: 'tv2 two-inflight timeout>reconnect>answer>expiry',
    kind: 'transactional',
    profile: 'tv2',
    shape: 'two-inflight',
    fault: 'timeout>reconnect>answer>expiry',
    commands: [{ op: 'event', event: 'timeout' }, { op: 'event', event: 'reconnect' }, { op: 'event', event: 'answer' }, { op: 'event', event: 'expiry' }],
    expectedWire: [['InitProducerId', -1, -1], ['InitProducerId', -1, -1], ['Produce', 0, 1000, 0, 0, 1], ['Produce', 1, 1000, 0, 0, 1], ['Produce', 0, 1000, 0, 0, 2], ['Produce', 1, 1000, 0, 0, 2], ['EndTxn', 'abort', 1000, 0]],
    expectedSettlement: [['m1', 'rejected', 'KafkaJSDeliveryTimeout', 'unknown'], ['m2', 'fulfilled', '-1'], ['end', 'fulfilled']],
    expectedState: 'READY',
    expectedLog: [[0, 1000, 0, 0, 0], [1, 1000, 0, 0, 0]],
    expectedNoops: ['destroy:0', 'release:0'],
  },
  {
    name: 'tv2 two-inflight timeout>reconnect>expiry>answer',
    kind: 'transactional',
    profile: 'tv2',
    shape: 'two-inflight',
    fault: 'timeout>reconnect>expiry>answer',
    commands: [{ op: 'event', event: 'timeout' }, { op: 'event', event: 'reconnect' }, { op: 'event', event: 'expiry' }, { op: 'event', event: 'answer' }],
    expectedWire: [['InitProducerId', -1, -1], ['InitProducerId', -1, -1], ['Produce', 0, 1000, 0, 0, 1], ['Produce', 1, 1000, 0, 0, 1], ['Produce', 0, 1000, 0, 0, 2], ['Produce', 1, 1000, 0, 0, 2], ['EndTxn', 'abort', 1000, 0]],
    expectedSettlement: [['m1', 'rejected', 'KafkaJSDeliveryTimeout', 'unknown'], ['m2', 'fulfilled', '-1'], ['end', 'fulfilled']],
    expectedState: 'READY',
    expectedLog: [[0, 1000, 0, 0, 0], [1, 1000, 0, 0, 0]],
    expectedNoops: ['destroy:0', 'release:0'],
  },
  {
    name: 'tv2 two-inflight timeout>expiry>answer>reconnect',
    kind: 'transactional',
    profile: 'tv2',
    shape: 'two-inflight',
    fault: 'timeout>expiry>answer>reconnect',
    commands: [{ op: 'event', event: 'timeout' }, { op: 'event', event: 'expiry' }, { op: 'event', event: 'answer' }, { op: 'event', event: 'reconnect' }],
    expectedWire: [['InitProducerId', -1, -1], ['InitProducerId', -1, -1], ['Produce', 0, 1000, 0, 0, 1], ['Produce', 1, 1000, 0, 0, 1], ['Produce', 0, 1000, 0, 0, 2], ['Produce', 1, 1000, 0, 0, 2], ['EndTxn', 'abort', 1000, 0]],
    expectedSettlement: [['m1', 'rejected', 'KafkaJSDeliveryTimeout', 'unknown'], ['m2', 'fulfilled', '-1'], ['end', 'fulfilled']],
    expectedState: 'READY',
    expectedLog: [[0, 1000, 0, 0, 0], [1, 1000, 0, 0, 0]],
    expectedNoops: ['release:0', 'destroy:0'],
  },
  {
    name: 'tv2 two-inflight timeout>expiry>reconnect>answer',
    kind: 'transactional',
    profile: 'tv2',
    shape: 'two-inflight',
    fault: 'timeout>expiry>reconnect>answer',
    commands: [{ op: 'event', event: 'timeout' }, { op: 'event', event: 'expiry' }, { op: 'event', event: 'reconnect' }, { op: 'event', event: 'answer' }],
    expectedWire: [['InitProducerId', -1, -1], ['InitProducerId', -1, -1], ['Produce', 0, 1000, 0, 0, 1], ['Produce', 1, 1000, 0, 0, 1], ['Produce', 0, 1000, 0, 0, 2], ['Produce', 1, 1000, 0, 0, 2], ['EndTxn', 'abort', 1000, 0]],
    expectedSettlement: [['m1', 'rejected', 'KafkaJSDeliveryTimeout', 'unknown'], ['m2', 'fulfilled', '-1'], ['end', 'fulfilled']],
    expectedState: 'READY',
    expectedLog: [[0, 1000, 0, 0, 0], [1, 1000, 0, 0, 0]],
    expectedNoops: ['destroy:0', 'release:0'],
  },
  {
    name: 'tv2 two-inflight reconnect>answer>timeout>expiry',
    kind: 'transactional',
    profile: 'tv2',
    shape: 'two-inflight',
    fault: 'reconnect>answer>timeout>expiry',
    commands: [{ op: 'event', event: 'reconnect' }, { op: 'event', event: 'answer' }, { op: 'event', event: 'timeout' }, { op: 'event', event: 'expiry' }],
    expectedWire: [['InitProducerId', -1, -1], ['InitProducerId', -1, -1], ['Produce', 0, 1000, 0, 0, 1], ['Produce', 1, 1000, 0, 0, 1], ['Produce', 0, 1000, 0, 0, 2], ['Produce', 1, 1000, 0, 0, 2], ['EndTxn', 'abort', 1000, 0]],
    expectedSettlement: [['m1', 'rejected', 'KafkaJSDeliveryTimeout', 'unknown'], ['m2', 'fulfilled', '-1'], ['end', 'fulfilled']],
    expectedState: 'READY',
    expectedLog: [[0, 1000, 0, 0, 0], [1, 1000, 0, 0, 0]],
    expectedNoops: ['release:0', 'fail:0'],
  },
  {
    name: 'tv2 two-inflight reconnect>answer>expiry>timeout',
    kind: 'transactional',
    profile: 'tv2',
    shape: 'two-inflight',
    fault: 'reconnect>answer>expiry>timeout',
    commands: [{ op: 'event', event: 'reconnect' }, { op: 'event', event: 'answer' }, { op: 'event', event: 'expiry' }, { op: 'event', event: 'timeout' }],
    expectedWire: [['InitProducerId', -1, -1], ['InitProducerId', -1, -1], ['Produce', 0, 1000, 0, 0, 1], ['Produce', 1, 1000, 0, 0, 1], ['Produce', 0, 1000, 0, 0, 2], ['Produce', 1, 1000, 0, 0, 2], ['EndTxn', 'abort', 1000, 0]],
    expectedSettlement: [['m1', 'rejected', 'KafkaJSDeliveryTimeout', 'unknown'], ['m2', 'fulfilled', '-1'], ['end', 'fulfilled']],
    expectedState: 'READY',
    expectedLog: [[0, 1000, 0, 0, 0], [1, 1000, 0, 0, 0]],
    expectedNoops: ['release:0', 'fail:0'],
  },
  {
    name: 'tv2 two-inflight reconnect>timeout>answer>expiry',
    kind: 'transactional',
    profile: 'tv2',
    shape: 'two-inflight',
    fault: 'reconnect>timeout>answer>expiry',
    commands: [{ op: 'event', event: 'reconnect' }, { op: 'event', event: 'timeout' }, { op: 'event', event: 'answer' }, { op: 'event', event: 'expiry' }],
    expectedWire: [['InitProducerId', -1, -1], ['InitProducerId', -1, -1], ['Produce', 0, 1000, 0, 0, 1], ['Produce', 1, 1000, 0, 0, 1], ['Produce', 0, 1000, 0, 0, 2], ['Produce', 1, 1000, 0, 0, 2], ['EndTxn', 'abort', 1000, 0]],
    expectedSettlement: [['m1', 'rejected', 'KafkaJSDeliveryTimeout', 'unknown'], ['m2', 'fulfilled', '-1'], ['end', 'fulfilled']],
    expectedState: 'READY',
    expectedLog: [[0, 1000, 0, 0, 0], [1, 1000, 0, 0, 0]],
    expectedNoops: ['fail:0', 'release:0'],
  },
  {
    name: 'tv2 two-inflight reconnect>timeout>expiry>answer',
    kind: 'transactional',
    profile: 'tv2',
    shape: 'two-inflight',
    fault: 'reconnect>timeout>expiry>answer',
    commands: [{ op: 'event', event: 'reconnect' }, { op: 'event', event: 'timeout' }, { op: 'event', event: 'expiry' }, { op: 'event', event: 'answer' }],
    expectedWire: [['InitProducerId', -1, -1], ['InitProducerId', -1, -1], ['Produce', 0, 1000, 0, 0, 1], ['Produce', 1, 1000, 0, 0, 1], ['Produce', 0, 1000, 0, 0, 2], ['Produce', 1, 1000, 0, 0, 2], ['EndTxn', 'abort', 1000, 0]],
    expectedSettlement: [['m1', 'rejected', 'KafkaJSDeliveryTimeout', 'unknown'], ['m2', 'fulfilled', '-1'], ['end', 'fulfilled']],
    expectedState: 'READY',
    expectedLog: [[0, 1000, 0, 0, 0], [1, 1000, 0, 0, 0]],
    expectedNoops: ['fail:0', 'release:0'],
  },
  {
    name: 'tv2 two-inflight reconnect>expiry>answer>timeout',
    kind: 'transactional',
    profile: 'tv2',
    shape: 'two-inflight',
    fault: 'reconnect>expiry>answer>timeout',
    commands: [{ op: 'event', event: 'reconnect' }, { op: 'event', event: 'expiry' }, { op: 'event', event: 'answer' }, { op: 'event', event: 'timeout' }],
    expectedWire: [['InitProducerId', -1, -1], ['InitProducerId', -1, -1], ['Produce', 0, 1000, 0, 0, 1], ['Produce', 1, 1000, 0, 0, 1], ['Produce', 0, 1000, 0, 0, 2], ['Produce', 1, 1000, 0, 0, 2], ['EndTxn', 'abort', 1000, 0]],
    expectedSettlement: [['m1', 'rejected', 'KafkaJSDeliveryTimeout', 'unknown'], ['m2', 'fulfilled', '-1'], ['end', 'fulfilled']],
    expectedState: 'READY',
    expectedLog: [[0, 1000, 0, 0, 0], [1, 1000, 0, 0, 0]],
    expectedNoops: ['release:0', 'fail:0'],
  },
  {
    name: 'tv2 two-inflight reconnect>expiry>timeout>answer',
    kind: 'transactional',
    profile: 'tv2',
    shape: 'two-inflight',
    fault: 'reconnect>expiry>timeout>answer',
    commands: [{ op: 'event', event: 'reconnect' }, { op: 'event', event: 'expiry' }, { op: 'event', event: 'timeout' }, { op: 'event', event: 'answer' }],
    expectedWire: [['InitProducerId', -1, -1], ['InitProducerId', -1, -1], ['Produce', 0, 1000, 0, 0, 1], ['Produce', 1, 1000, 0, 0, 1], ['Produce', 0, 1000, 0, 0, 2], ['Produce', 1, 1000, 0, 0, 2], ['EndTxn', 'abort', 1000, 0]],
    expectedSettlement: [['m1', 'rejected', 'KafkaJSDeliveryTimeout', 'unknown'], ['m2', 'fulfilled', '-1'], ['end', 'fulfilled']],
    expectedState: 'READY',
    expectedLog: [[0, 1000, 0, 0, 0], [1, 1000, 0, 0, 0]],
    expectedNoops: ['fail:0', 'release:0'],
  },
  {
    name: 'tv2 two-inflight expiry>answer>timeout>reconnect',
    kind: 'transactional',
    profile: 'tv2',
    shape: 'two-inflight',
    fault: 'expiry>answer>timeout>reconnect',
    commands: [{ op: 'event', event: 'expiry' }, { op: 'event', event: 'answer' }, { op: 'event', event: 'timeout' }, { op: 'event', event: 'reconnect' }],
    expectedWire: [['InitProducerId', -1, -1], ['InitProducerId', -1, -1], ['Produce', 0, 1000, 0, 0, 1], ['Produce', 1, 1000, 0, 0, 1], ['EndTxn', 'abort', 1000, 0]],
    expectedSettlement: [['m1', 'rejected', 'KafkaJSDeliveryTimeout', 'unknown'], ['m2', 'rejected', 'KafkaJSConnectionError', null], ['end', 'fulfilled']],
    expectedState: 'READY',
    expectedLog: [[0, 1000, 0, 0, 0], [1, 1000, 0, 0, 0]],
    expectedNoops: ['fail:0'],
  },
  {
    name: 'tv2 two-inflight expiry>answer>reconnect>timeout',
    kind: 'transactional',
    profile: 'tv2',
    shape: 'two-inflight',
    fault: 'expiry>answer>reconnect>timeout',
    commands: [{ op: 'event', event: 'expiry' }, { op: 'event', event: 'answer' }, { op: 'event', event: 'reconnect' }, { op: 'event', event: 'timeout' }],
    expectedWire: [['InitProducerId', -1, -1], ['InitProducerId', -1, -1], ['Produce', 0, 1000, 0, 0, 1], ['Produce', 1, 1000, 0, 0, 1], ['EndTxn', 'abort', 1000, 0]],
    expectedSettlement: [['m1', 'rejected', 'KafkaJSDeliveryTimeout', 'unknown'], ['m2', 'rejected', 'KafkaJSConnectionError', null], ['end', 'fulfilled']],
    expectedState: 'READY',
    expectedLog: [[0, 1000, 0, 0, 0], [1, 1000, 0, 0, 0]],
    expectedNoops: ['fail:0'],
  },
  {
    name: 'tv2 two-inflight expiry>timeout>answer>reconnect',
    kind: 'transactional',
    profile: 'tv2',
    shape: 'two-inflight',
    fault: 'expiry>timeout>answer>reconnect',
    commands: [{ op: 'event', event: 'expiry' }, { op: 'event', event: 'timeout' }, { op: 'event', event: 'answer' }, { op: 'event', event: 'reconnect' }],
    expectedWire: [['InitProducerId', -1, -1], ['InitProducerId', -1, -1], ['Produce', 0, 1000, 0, 0, 1], ['Produce', 1, 1000, 0, 0, 1], ['EndTxn', 'abort', 1000, 0]],
    expectedSettlement: [['m1', 'rejected', 'KafkaJSDeliveryTimeout', 'unknown'], ['m2', 'rejected', 'KafkaJSConnectionError', null], ['end', 'fulfilled']],
    expectedState: 'READY',
    expectedLog: [[0, 1000, 0, 0, 0], [1, 1000, 0, 0, 0]],
    expectedNoops: ['release:0', 'destroy:0'],
  },
  {
    name: 'tv2 two-inflight expiry>timeout>reconnect>answer',
    kind: 'transactional',
    profile: 'tv2',
    shape: 'two-inflight',
    fault: 'expiry>timeout>reconnect>answer',
    commands: [{ op: 'event', event: 'expiry' }, { op: 'event', event: 'timeout' }, { op: 'event', event: 'reconnect' }, { op: 'event', event: 'answer' }],
    expectedWire: [['InitProducerId', -1, -1], ['InitProducerId', -1, -1], ['Produce', 0, 1000, 0, 0, 1], ['Produce', 1, 1000, 0, 0, 1], ['EndTxn', 'abort', 1000, 0]],
    expectedSettlement: [['m1', 'rejected', 'KafkaJSDeliveryTimeout', 'unknown'], ['m2', 'rejected', 'KafkaJSConnectionError', null], ['end', 'fulfilled']],
    expectedState: 'READY',
    expectedLog: [[0, 1000, 0, 0, 0], [1, 1000, 0, 0, 0]],
    expectedNoops: ['destroy:0', 'release:0'],
  },
  {
    name: 'tv2 two-inflight expiry>reconnect>answer>timeout',
    kind: 'transactional',
    profile: 'tv2',
    shape: 'two-inflight',
    fault: 'expiry>reconnect>answer>timeout',
    commands: [{ op: 'event', event: 'expiry' }, { op: 'event', event: 'reconnect' }, { op: 'event', event: 'answer' }, { op: 'event', event: 'timeout' }],
    expectedWire: [['InitProducerId', -1, -1], ['InitProducerId', -1, -1], ['Produce', 0, 1000, 0, 0, 1], ['Produce', 1, 1000, 0, 0, 1], ['EndTxn', 'abort', 1000, 0]],
    expectedSettlement: [['m1', 'rejected', 'KafkaJSDeliveryTimeout', 'unknown'], ['m2', 'rejected', 'KafkaJSConnectionError', null], ['end', 'fulfilled']],
    expectedState: 'READY',
    expectedLog: [[0, 1000, 0, 0, 0], [1, 1000, 0, 0, 0]],
    expectedNoops: ['release:0', 'fail:0'],
  },
  {
    name: 'tv2 two-inflight expiry>reconnect>timeout>answer',
    kind: 'transactional',
    profile: 'tv2',
    shape: 'two-inflight',
    fault: 'expiry>reconnect>timeout>answer',
    commands: [{ op: 'event', event: 'expiry' }, { op: 'event', event: 'reconnect' }, { op: 'event', event: 'timeout' }, { op: 'event', event: 'answer' }],
    expectedWire: [['InitProducerId', -1, -1], ['InitProducerId', -1, -1], ['Produce', 0, 1000, 0, 0, 1], ['Produce', 1, 1000, 0, 0, 1], ['EndTxn', 'abort', 1000, 0]],
    expectedSettlement: [['m1', 'rejected', 'KafkaJSDeliveryTimeout', 'unknown'], ['m2', 'rejected', 'KafkaJSConnectionError', null], ['end', 'fulfilled']],
    expectedState: 'READY',
    expectedLog: [[0, 1000, 0, 0, 0], [1, 1000, 0, 0, 0]],
    expectedNoops: ['fail:0', 'release:0'],
  },
]

/** @type {ReplayRow[]} the 21 fault and outcome rows: five fault points for each profile, three outcomes for the two transaction profiles */
// prettier-ignore
const FAULT_ROWS = [
  {
    name: 'before write under default',
    kind: 'idempotent',
    profile: 'default',
    shape: 'script',
    fault: 'before write',
    commands: [
      { op: 'connect' },
      { op: 'hold', api: 'Metadata' },
      { op: 'send', id: 'm1', partition: 0 },
      { op: 'arrive', gate: 'Metadata' },
      { op: 'send', id: 'm2', partition: 0 },
      { op: 'close', timeout: 0 },
      { op: 'release', gate: 'Metadata' },
      { op: 'advance', ms: 2000 },
    ],
    expectedWire: [['InitProducerId', -1, -1]],
    expectedSettlement: [['m1', 'rejected', 'KafkaJSProducerClosedError', 'unsent'], ['m2', 'rejected', 'KafkaJSProducerClosedError', 'unsent']],
    expectedState: 'READY',
    expectedLog: [],
    expectedNoops: [],
    expectedCommitted: [],
    expectedSockets: [1, 1],
  },
  {
    name: 'before write under tv1',
    kind: 'transactional',
    profile: 'tv1',
    shape: 'script',
    fault: 'before write',
    commands: [
      { op: 'connect' },
      { op: 'hold', api: 'Metadata' },
      { op: 'send', id: 'm1', partition: 0 },
      { op: 'arrive', gate: 'Metadata' },
      { op: 'send', id: 'm2', partition: 0 },
      { op: 'close', timeout: 0 },
      { op: 'release', gate: 'Metadata' },
      { op: 'advance', ms: 2000 },
    ],
    expectedWire: [['InitProducerId', -1, -1], ['InitProducerId', -1, -1]],
    expectedSettlement: [['m1', 'rejected', 'KafkaJSProducerClosedError', 'unsent'], ['m2', 'rejected', 'KafkaJSProducerClosedError', 'unsent']],
    expectedState: 'ABORTABLE_ERROR',
    expectedLog: [],
    expectedNoops: [],
    expectedCommitted: [],
    expectedSockets: [1, 1],
  },
  {
    name: 'before write under tv2',
    kind: 'transactional',
    profile: 'tv2',
    shape: 'script',
    fault: 'before write',
    commands: [
      { op: 'connect' },
      { op: 'hold', api: 'Metadata' },
      { op: 'send', id: 'm1', partition: 0 },
      { op: 'arrive', gate: 'Metadata' },
      { op: 'send', id: 'm2', partition: 0 },
      { op: 'close', timeout: 0 },
      { op: 'release', gate: 'Metadata' },
      { op: 'advance', ms: 2000 },
    ],
    expectedWire: [['InitProducerId', -1, -1], ['InitProducerId', -1, -1]],
    expectedSettlement: [['m1', 'rejected', 'KafkaJSProducerClosedError', 'unsent'], ['m2', 'rejected', 'KafkaJSProducerClosedError', 'unsent']],
    expectedState: 'ABORTABLE_ERROR',
    expectedLog: [],
    expectedNoops: [],
    expectedCommitted: [],
    expectedSockets: [1, 1],
  },
  {
    name: 'during encoder await under default',
    kind: 'idempotent',
    profile: 'default',
    shape: 'script',
    fault: 'during encoder await',
    commands: [
      { op: 'connect' },
      { op: 'codec' },
      { op: 'send', id: 'm1', partition: 0, compressed: true },
      { op: 'advance', ms: 50 },
      { op: 'advance', to: 1000 },
      { op: 'close', timeout: 0 },
      { op: 'encoder' },
      { op: 'advance', ms: 2000 },
    ],
    expectedWire: [['InitProducerId', -1, -1]],
    expectedSettlement: [['m1', 'rejected', 'KafkaJSDeliveryTimeout', 'unsent']],
    expectedState: 'READY',
    expectedLog: [],
    expectedNoops: [],
    expectedCommitted: [],
    expectedSockets: [1, 1],
  },
  {
    name: 'during encoder await under tv1',
    kind: 'transactional',
    profile: 'tv1',
    shape: 'script',
    fault: 'during encoder await',
    commands: [
      { op: 'connect' },
      { op: 'codec' },
      { op: 'send', id: 'm1', partition: 0, compressed: true },
      { op: 'advance', ms: 50 },
      { op: 'advance', to: 1000 },
      { op: 'close', timeout: 0 },
      { op: 'encoder' },
      { op: 'advance', ms: 2000 },
    ],
    expectedWire: [['InitProducerId', -1, -1], ['InitProducerId', -1, -1], ['AddPartitionsToTxn', [0]]],
    expectedSettlement: [['m1', 'rejected', 'KafkaJSDeliveryTimeout', 'unsent']],
    expectedState: 'ABORTABLE_ERROR',
    expectedLog: [],
    expectedNoops: [],
    expectedCommitted: [],
    expectedSockets: [1, 1],
  },
  {
    name: 'during encoder await under tv2',
    kind: 'transactional',
    profile: 'tv2',
    shape: 'script',
    fault: 'during encoder await',
    commands: [
      { op: 'connect' },
      { op: 'codec' },
      { op: 'send', id: 'm1', partition: 0, compressed: true },
      { op: 'advance', ms: 50 },
      { op: 'advance', to: 1000 },
      { op: 'close', timeout: 0 },
      { op: 'encoder' },
      { op: 'advance', ms: 2000 },
    ],
    expectedWire: [['InitProducerId', -1, -1], ['InitProducerId', -1, -1]],
    expectedSettlement: [['m1', 'rejected', 'KafkaJSDeliveryTimeout', 'unsent']],
    expectedState: 'ABORTABLE_ERROR',
    expectedLog: [],
    expectedNoops: [],
    expectedCommitted: [],
    expectedSockets: [1, 1],
  },
  {
    name: 'after write before answer under default',
    kind: 'idempotent',
    profile: 'default',
    shape: 'script',
    fault: 'after write before answer',
    commands: [
      { op: 'connect' },
      { op: 'send', id: 'm1', partition: 0 },
      { op: 'arrive', attempt: 0 },
      { op: 'advance', to: 1000 },
      { op: 'send', id: 'm3', partition: 0 },
      { op: 'release', attempt: 0 },
      { op: 'finish' },
    ],
    expectedWire: [['InitProducerId', -1, -1], ['Produce', 0, 1000, 0, 0, 1], ['Produce', 0, 1000, 1, 0, 1]],
    expectedSettlement: [['m1', 'rejected', 'KafkaJSDeliveryTimeout', 'unknown'], ['m3', 'fulfilled', '1']],
    expectedState: 'READY',
    expectedLog: [[0, 1000, 0, 0, 0], [0, 1000, 1, 0, 1]],
    expectedNoops: [],
    expectedCommitted: [[0, 1000, 0, 0, 0], [0, 1000, 1, 0, 1]],
  },
  {
    name: 'after write before answer under tv1',
    kind: 'transactional',
    profile: 'tv1',
    shape: 'script',
    fault: 'after write before answer',
    commands: [
      { op: 'connect' },
      { op: 'send', id: 'm1', partition: 0 },
      { op: 'arrive', attempt: 0 },
      { op: 'advance', to: 1000 },
      { op: 'release', attempt: 0 },
      { op: 'commit', id: 'c1' },
      { op: 'abort', id: 'a1' },
      { op: 'wait', id: 'a1' },
      { op: 'begin' },
      { op: 'send', id: 'm3', partition: 0 },
      { op: 'finish' },
      { op: 'commit', id: 'c2' },
      { op: 'wait', id: 'c2' },
    ],
    expectedWire: [['InitProducerId', -1, -1], ['InitProducerId', -1, -1], ['AddPartitionsToTxn', [0]], ['Produce', 0, 1000, 0, 0, 1], ['EndTxn', 'abort', 1000, 0], ['InitProducerId', 1000, 0], ['AddPartitionsToTxn', [0]], ['Produce', 0, 1000, 1, 0, 1], ['EndTxn', 'commit', 1000, 1]],
    expectedSettlement: [['m1', 'rejected', 'KafkaJSDeliveryTimeout', 'unknown'], ['c1', 'rejected', 'KafkaJSTransactionAbortableError', null], ['a1', 'fulfilled'], ['m3', 'fulfilled', '1'], ['c2', 'fulfilled']],
    expectedState: 'READY',
    expectedLog: [[0, 1000, 0, 0, 0], [0, 1000, 1, 0, 1]],
    expectedNoops: [],
    expectedCommitted: [[0, 1000, 1, 0, 1]],
  },
  {
    name: 'after write before answer under tv2',
    kind: 'transactional',
    profile: 'tv2',
    shape: 'script',
    fault: 'after write before answer',
    commands: [
      { op: 'connect' },
      { op: 'send', id: 'm1', partition: 0 },
      { op: 'arrive', attempt: 0 },
      { op: 'advance', to: 1000 },
      { op: 'release', attempt: 0 },
      { op: 'commit', id: 'c1' },
      { op: 'abort', id: 'a1' },
      { op: 'wait', id: 'a1' },
      { op: 'begin' },
      { op: 'send', id: 'm3', partition: 0 },
      { op: 'finish' },
      { op: 'commit', id: 'c2' },
      { op: 'wait', id: 'c2' },
    ],
    expectedWire: [['InitProducerId', -1, -1], ['InitProducerId', -1, -1], ['Produce', 0, 1000, 0, 0, 1], ['EndTxn', 'abort', 1000, 0], ['Produce', 0, 1000, 1, 0, 1], ['EndTxn', 'commit', 1000, 1]],
    expectedSettlement: [['m1', 'rejected', 'KafkaJSDeliveryTimeout', 'unknown'], ['c1', 'rejected', 'KafkaJSTransactionAbortableError', null], ['a1', 'fulfilled'], ['m3', 'fulfilled', '1'], ['c2', 'fulfilled']],
    expectedState: 'READY',
    expectedLog: [[0, 1000, 0, 0, 0], [0, 1000, 1, 0, 1]],
    expectedNoops: [],
    expectedCommitted: [[0, 1000, 1, 0, 1]],
  },
  {
    name: 'reconnect under default',
    kind: 'idempotent',
    profile: 'default',
    shape: 'script',
    fault: 'reconnect',
    commands: [
      { op: 'connect' },
      { op: 'send', id: 'm1', partition: 0 },
      { op: 'arrive', attempt: 0 },
      { op: 'destroy', attempt: 0 },
      { op: 'advance', ms: 100 },
      { op: 'arrive', attempt: 1 },
      { op: 'finish' },
    ],
    expectedWire: [['InitProducerId', -1, -1], ['Produce', 0, 1000, 0, 0, 1], ['Produce', 0, 1000, 0, 0, 2]],
    expectedSettlement: [['m1', 'fulfilled', '-1']],
    expectedState: 'READY',
    expectedLog: [[0, 1000, 0, 0, 0]],
    expectedNoops: [],
    expectedCommitted: [[0, 1000, 0, 0, 0]],
  },
  {
    name: 'reconnect under tv1',
    kind: 'transactional',
    profile: 'tv1',
    shape: 'script',
    fault: 'reconnect',
    commands: [
      { op: 'connect' },
      { op: 'send', id: 'm1', partition: 0 },
      { op: 'arrive', attempt: 0 },
      { op: 'destroy', attempt: 0 },
      { op: 'advance', ms: 100 },
      { op: 'arrive', attempt: 1 },
      { op: 'finish' },
      { op: 'commit', id: 'c1' },
      { op: 'wait', id: 'c1' },
    ],
    expectedWire: [['InitProducerId', -1, -1], ['InitProducerId', -1, -1], ['AddPartitionsToTxn', [0]], ['Produce', 0, 1000, 0, 0, 1], ['Produce', 0, 1000, 0, 0, 2], ['EndTxn', 'commit', 1000, 0]],
    expectedSettlement: [['m1', 'fulfilled', '-1'], ['c1', 'fulfilled']],
    expectedState: 'READY',
    expectedLog: [[0, 1000, 0, 0, 0]],
    expectedNoops: [],
    expectedCommitted: [[0, 1000, 0, 0, 0]],
  },
  {
    name: 'reconnect under tv2',
    kind: 'transactional',
    profile: 'tv2',
    shape: 'script',
    fault: 'reconnect',
    commands: [
      { op: 'connect' },
      { op: 'send', id: 'm1', partition: 0 },
      { op: 'arrive', attempt: 0 },
      { op: 'destroy', attempt: 0 },
      { op: 'advance', ms: 100 },
      { op: 'arrive', attempt: 1 },
      { op: 'finish' },
      { op: 'commit', id: 'c1' },
      { op: 'wait', id: 'c1' },
    ],
    expectedWire: [['InitProducerId', -1, -1], ['InitProducerId', -1, -1], ['Produce', 0, 1000, 0, 0, 1], ['Produce', 0, 1000, 0, 0, 2], ['EndTxn', 'commit', 1000, 0]],
    expectedSettlement: [['m1', 'fulfilled', '-1'], ['c1', 'fulfilled']],
    expectedState: 'READY',
    expectedLog: [[0, 1000, 0, 0, 0]],
    expectedNoops: [],
    expectedCommitted: [[0, 1000, 0, 0, 0]],
  },
  {
    name: 'epoch switch under default',
    kind: 'idempotent',
    profile: 'default',
    shape: 'script',
    fault: 'epoch switch',
    commands: [
      { op: 'connect' },
      { op: 'send', id: 'm1', partition: 0 },
      { op: 'arrive', attempt: 0 },
      { op: 'advance', ms: 300 },
      { op: 'send', id: 'm2', partition: 1 },
      { op: 'arrive', attempt: 1 },
      { op: 'send', id: 'm3', partition: 0 },
      { op: 'advance', to: 1000 },
      { op: 'send', id: 'm4', partition: 0 },
      { op: 'finish' },
      { op: 'send', id: 'm5', partition: 1 },
      { op: 'finish' },
    ],
    expectedWire: [['InitProducerId', -1, -1], ['Produce', 0, 1000, 0, 0, 1], ['Produce', 1, 1000, 0, 0, 1], ['Produce', 0, 1000, 1, 0, 1], ['Produce', 0, 1000, 1, 1, 1], ['Produce', 1, 1000, 1, 0, 1]],
    expectedSettlement: [['m1', 'rejected', 'KafkaJSDeliveryTimeout', 'unknown'], ['m2', 'fulfilled', '0'], ['m3', 'fulfilled', '1'], ['m4', 'fulfilled', '2'], ['m5', 'fulfilled', '1']],
    expectedState: 'READY',
    expectedLog: [[0, 1000, 0, 0, 0], [1, 1000, 0, 0, 0], [0, 1000, 1, 0, 1], [0, 1000, 1, 1, 2], [1, 1000, 1, 0, 1]],
    expectedNoops: [],
    expectedCommitted: [[0, 1000, 0, 0, 0], [1, 1000, 0, 0, 0], [0, 1000, 1, 0, 1], [0, 1000, 1, 1, 2], [1, 1000, 1, 0, 1]],
  },
  {
    name: 'epoch switch under tv1',
    kind: 'transactional',
    profile: 'tv1',
    shape: 'script',
    fault: 'epoch switch',
    commands: [
      { op: 'connect' },
      { op: 'send', id: 'm1', partition: 0 },
      { op: 'arrive', attempt: 0 },
      { op: 'advance', ms: 300 },
      { op: 'send', id: 'm2', partition: 1 },
      { op: 'arrive', attempt: 1 },
      { op: 'send', id: 'm3', partition: 0 },
      { op: 'advance', to: 1000 },
      { op: 'send', id: 'm4', partition: 0 },
      { op: 'finish' },
      { op: 'commit', id: 'c1' },
      { op: 'abort', id: 'a1' },
      { op: 'wait', id: 'a1' },
      { op: 'begin' },
      { op: 'send', id: 'm5', partition: 1 },
      { op: 'send', id: 'm6', partition: 0 },
      { op: 'finish' },
      { op: 'commit', id: 'c2' },
      { op: 'wait', id: 'c2' },
    ],
    expectedWire: [['InitProducerId', -1, -1], ['InitProducerId', -1, -1], ['AddPartitionsToTxn', [0]], ['Produce', 0, 1000, 0, 0, 1], ['AddPartitionsToTxn', [1]], ['Produce', 1, 1000, 0, 0, 1], ['EndTxn', 'abort', 1000, 0], ['InitProducerId', 1000, 0], ['AddPartitionsToTxn', [1]], ['Produce', 1, 1000, 1, 0, 1], ['AddPartitionsToTxn', [0]], ['Produce', 0, 1000, 1, 0, 1], ['EndTxn', 'commit', 1000, 1]],
    expectedSettlement: [['m1', 'rejected', 'KafkaJSDeliveryTimeout', 'unknown'], ['m2', 'fulfilled', '0'], ['m3', 'rejected', 'KafkaJSTransactionAbortableError', null], ['m4', 'rejected', 'KafkaJSTransactionAbortableError', null], ['c1', 'rejected', 'KafkaJSTransactionAbortableError', null], ['a1', 'fulfilled'], ['m5', 'fulfilled', '1'], ['m6', 'fulfilled', '1'], ['c2', 'fulfilled']],
    expectedState: 'READY',
    expectedLog: [[0, 1000, 0, 0, 0], [1, 1000, 0, 0, 0], [1, 1000, 1, 0, 1], [0, 1000, 1, 0, 1]],
    expectedNoops: [],
    expectedCommitted: [[1, 1000, 1, 0, 1], [0, 1000, 1, 0, 1]],
  },
  {
    name: 'epoch switch under tv2',
    kind: 'transactional',
    profile: 'tv2',
    shape: 'script',
    fault: 'epoch switch',
    commands: [
      { op: 'connect' },
      { op: 'send', id: 'm1', partition: 0 },
      { op: 'arrive', attempt: 0 },
      { op: 'advance', ms: 300 },
      { op: 'send', id: 'm2', partition: 1 },
      { op: 'arrive', attempt: 1 },
      { op: 'send', id: 'm3', partition: 0 },
      { op: 'advance', to: 1000 },
      { op: 'send', id: 'm4', partition: 0 },
      { op: 'finish' },
      { op: 'commit', id: 'c1' },
      { op: 'abort', id: 'a1' },
      { op: 'wait', id: 'a1' },
      { op: 'begin' },
      { op: 'send', id: 'm5', partition: 1 },
      { op: 'send', id: 'm6', partition: 0 },
      { op: 'finish' },
      { op: 'commit', id: 'c2' },
      { op: 'wait', id: 'c2' },
    ],
    expectedWire: [['InitProducerId', -1, -1], ['InitProducerId', -1, -1], ['Produce', 0, 1000, 0, 0, 1], ['Produce', 1, 1000, 0, 0, 1], ['EndTxn', 'abort', 1000, 0], ['Produce', 1, 1000, 1, 0, 1], ['Produce', 0, 1000, 1, 0, 1], ['EndTxn', 'commit', 1000, 1]],
    expectedSettlement: [['m1', 'rejected', 'KafkaJSDeliveryTimeout', 'unknown'], ['m2', 'fulfilled', '0'], ['m3', 'rejected', 'KafkaJSTransactionAbortableError', null], ['m4', 'rejected', 'KafkaJSTransactionAbortableError', null], ['c1', 'rejected', 'KafkaJSTransactionAbortableError', null], ['a1', 'fulfilled'], ['m5', 'fulfilled', '1'], ['m6', 'fulfilled', '1'], ['c2', 'fulfilled']],
    expectedState: 'READY',
    expectedLog: [[0, 1000, 0, 0, 0], [1, 1000, 0, 0, 0], [1, 1000, 1, 0, 1], [0, 1000, 1, 0, 1]],
    expectedNoops: [],
    expectedCommitted: [[1, 1000, 1, 0, 1], [0, 1000, 1, 0, 1]],
  },
  {
    name: 'commit under tv1',
    kind: 'transactional',
    profile: 'tv1',
    shape: 'script',
    fault: 'commit',
    commands: [
      { op: 'connect' },
      { op: 'send', id: 'm1', partition: 0 },
      { op: 'arrive', attempt: 0 },
      { op: 'release', attempt: 0 },
      { op: 'hold', api: 'TxnOffsetCommit' },
      { op: 'offsets', id: 'o1' },
      { op: 'arrive', gate: 'TxnOffsetCommit' },
      { op: 'commit', id: 'c1' },
      { op: 'advance', ms: 100 },
      { op: 'release', gate: 'TxnOffsetCommit' },
      { op: 'wait', id: 'c1' },
      { op: 'begin' },
      { op: 'send', id: 'm2', partition: 0 },
      { op: 'finish' },
      { op: 'commit', id: 'c2' },
      { op: 'wait', id: 'c2' },
    ],
    expectedWire: [['InitProducerId', -1, -1], ['InitProducerId', -1, -1], ['AddPartitionsToTxn', [0]], ['Produce', 0, 1000, 0, 0, 1], ['AddOffsetsToTxn'], ['TxnOffsetCommit'], ['EndTxn', 'commit', 1000, 0], ['AddPartitionsToTxn', [0]], ['Produce', 0, 1000, 0, 1, 1], ['EndTxn', 'commit', 1000, 0]],
    expectedSettlement: [['m1', 'fulfilled', '0'], ['o1', 'fulfilled'], ['c1', 'fulfilled'], ['m2', 'fulfilled', '1'], ['c2', 'fulfilled']],
    expectedState: 'READY',
    expectedLog: [[0, 1000, 0, 0, 0], [0, 1000, 0, 1, 1]],
    expectedNoops: [],
    expectedCommitted: [[0, 1000, 0, 0, 0], [0, 1000, 0, 1, 1]],
    expectedPublished: [['group', 'input', 0, '1']],
    expectedBrokerOffsets: { 'input/0': '1' },
    expectedOrder: ['TxnOffsetCommit response', 'EndTxn request', 'EndTxn response', 'publish'],
  },
  {
    name: 'commit under tv2',
    kind: 'transactional',
    profile: 'tv2',
    shape: 'script',
    fault: 'commit',
    commands: [
      { op: 'connect' },
      { op: 'send', id: 'm1', partition: 0 },
      { op: 'arrive', attempt: 0 },
      { op: 'release', attempt: 0 },
      { op: 'hold', api: 'TxnOffsetCommit' },
      { op: 'offsets', id: 'o1' },
      { op: 'arrive', gate: 'TxnOffsetCommit' },
      { op: 'commit', id: 'c1' },
      { op: 'advance', ms: 100 },
      { op: 'release', gate: 'TxnOffsetCommit' },
      { op: 'wait', id: 'c1' },
      { op: 'begin' },
      { op: 'send', id: 'm2', partition: 0 },
      { op: 'finish' },
      { op: 'commit', id: 'c2' },
      { op: 'wait', id: 'c2' },
    ],
    expectedWire: [['InitProducerId', -1, -1], ['InitProducerId', -1, -1], ['Produce', 0, 1000, 0, 0, 1], ['TxnOffsetCommit'], ['EndTxn', 'commit', 1000, 0], ['Produce', 0, 1000, 1, 0, 1], ['EndTxn', 'commit', 1000, 1]],
    expectedSettlement: [['m1', 'fulfilled', '0'], ['o1', 'fulfilled'], ['c1', 'fulfilled'], ['m2', 'fulfilled', '1'], ['c2', 'fulfilled']],
    expectedState: 'READY',
    expectedLog: [[0, 1000, 0, 0, 0], [0, 1000, 1, 0, 1]],
    expectedNoops: [],
    expectedCommitted: [[0, 1000, 0, 0, 0], [0, 1000, 1, 0, 1]],
    expectedPublished: [['group', 'input', 0, '1']],
    expectedBrokerOffsets: { 'input/0': '1' },
    expectedOrder: ['TxnOffsetCommit response', 'EndTxn request', 'EndTxn response', 'publish'],
  },
  {
    name: 'abort under tv1',
    kind: 'transactional',
    profile: 'tv1',
    shape: 'script',
    fault: 'abort',
    commands: [
      { op: 'connect' },
      { op: 'send', id: 'm1', partition: 0 },
      { op: 'arrive', attempt: 0 },
      { op: 'release', attempt: 0 },
      { op: 'offsets', id: 'o1' },
      { op: 'abort', id: 'a1' },
      { op: 'wait', id: 'a1' },
      { op: 'begin' },
      { op: 'send', id: 'm2', partition: 0 },
      { op: 'finish' },
      { op: 'commit', id: 'c2' },
      { op: 'wait', id: 'c2' },
    ],
    expectedWire: [['InitProducerId', -1, -1], ['InitProducerId', -1, -1], ['AddPartitionsToTxn', [0]], ['Produce', 0, 1000, 0, 0, 1], ['AddOffsetsToTxn'], ['TxnOffsetCommit'], ['EndTxn', 'abort', 1000, 0], ['AddPartitionsToTxn', [0]], ['Produce', 0, 1000, 0, 1, 1], ['EndTxn', 'commit', 1000, 0]],
    expectedSettlement: [['m1', 'fulfilled', '0'], ['o1', 'fulfilled'], ['a1', 'fulfilled'], ['m2', 'fulfilled', '1'], ['c2', 'fulfilled']],
    expectedState: 'READY',
    expectedLog: [[0, 1000, 0, 0, 0], [0, 1000, 0, 1, 1]],
    expectedNoops: [],
    expectedCommitted: [[0, 1000, 0, 1, 1]],
    expectedPublished: [],
    expectedBrokerOffsets: {  },
  },
  {
    name: 'abort under tv2',
    kind: 'transactional',
    profile: 'tv2',
    shape: 'script',
    fault: 'abort',
    commands: [
      { op: 'connect' },
      { op: 'send', id: 'm1', partition: 0 },
      { op: 'arrive', attempt: 0 },
      { op: 'release', attempt: 0 },
      { op: 'offsets', id: 'o1' },
      { op: 'abort', id: 'a1' },
      { op: 'wait', id: 'a1' },
      { op: 'begin' },
      { op: 'send', id: 'm2', partition: 0 },
      { op: 'finish' },
      { op: 'commit', id: 'c2' },
      { op: 'wait', id: 'c2' },
    ],
    expectedWire: [['InitProducerId', -1, -1], ['InitProducerId', -1, -1], ['Produce', 0, 1000, 0, 0, 1], ['TxnOffsetCommit'], ['EndTxn', 'abort', 1000, 0], ['Produce', 0, 1000, 1, 0, 1], ['EndTxn', 'commit', 1000, 1]],
    expectedSettlement: [['m1', 'fulfilled', '0'], ['o1', 'fulfilled'], ['a1', 'fulfilled'], ['m2', 'fulfilled', '1'], ['c2', 'fulfilled']],
    expectedState: 'READY',
    expectedLog: [[0, 1000, 0, 0, 0], [0, 1000, 1, 0, 1]],
    expectedNoops: [],
    expectedCommitted: [[0, 1000, 1, 0, 1]],
    expectedPublished: [],
    expectedBrokerOffsets: {  },
  },
  {
    name: 'unknown commit under tv1',
    kind: 'transactional',
    profile: 'tv1',
    shape: 'script',
    fault: 'unknown commit',
    commands: [
      { op: 'connect' },
      { op: 'send', id: 'm1', partition: 0 },
      { op: 'arrive', attempt: 0 },
      { op: 'release', attempt: 0 },
      { op: 'offsets', id: 'o1' },
      { op: 'hold', api: 'EndTxn' },
      { op: 'commit', id: 'c1' },
      { op: 'arrive', gate: 'EndTxn' },
      { op: 'close', timeout: 10 },
      { op: 'advance', ms: 10 },
      { op: 'release', gate: 'EndTxn' },
      { op: 'advance', ms: 1000 },
    ],
    expectedWire: [['InitProducerId', -1, -1], ['InitProducerId', -1, -1], ['AddPartitionsToTxn', [0]], ['Produce', 0, 1000, 0, 0, 1], ['AddOffsetsToTxn'], ['TxnOffsetCommit'], ['EndTxn', 'commit', 1000, 0]],
    expectedSettlement: [['m1', 'fulfilled', '0'], ['o1', 'fulfilled'], ['c1', 'rejected', 'KafkaJSProducerClosedError', 'unknown']],
    expectedState: 'COMMITTING',
    expectedLog: [[0, 1000, 0, 0, 0]],
    expectedNoops: [],
    expectedCommitted: [[0, 1000, 0, 0, 0]],
    expectedPublished: [],
    expectedBrokerOffsets: { 'input/0': '1' },
    expectedSockets: [1, 1],
  },
  {
    name: 'unknown commit under tv2',
    kind: 'transactional',
    profile: 'tv2',
    shape: 'script',
    fault: 'unknown commit',
    commands: [
      { op: 'connect' },
      { op: 'send', id: 'm1', partition: 0 },
      { op: 'arrive', attempt: 0 },
      { op: 'release', attempt: 0 },
      { op: 'offsets', id: 'o1' },
      { op: 'hold', api: 'EndTxn' },
      { op: 'commit', id: 'c1' },
      { op: 'arrive', gate: 'EndTxn' },
      { op: 'close', timeout: 10 },
      { op: 'advance', ms: 10 },
      { op: 'release', gate: 'EndTxn' },
      { op: 'advance', ms: 1000 },
    ],
    expectedWire: [['InitProducerId', -1, -1], ['InitProducerId', -1, -1], ['Produce', 0, 1000, 0, 0, 1], ['TxnOffsetCommit'], ['EndTxn', 'commit', 1000, 0]],
    expectedSettlement: [['m1', 'fulfilled', '0'], ['o1', 'fulfilled'], ['c1', 'rejected', 'KafkaJSProducerClosedError', 'unknown']],
    expectedState: 'COMMITTING',
    expectedLog: [[0, 1000, 0, 0, 0]],
    expectedNoops: [],
    expectedCommitted: [[0, 1000, 0, 0, 0]],
    expectedPublished: [],
    expectedBrokerOffsets: { 'input/0': '1' },
    expectedSockets: [1, 1],
  },
]

/** @type {ReplayRow[]} a held InitProducerId, a forced close, a replacement connect, then the old answer */
// prettier-ignore
const LATE_FENCE_ROWS = [
  {
    name: 'a late 47 after the replacement connected under default',
    kind: 'idempotent',
    profile: 'default',
    shape: 'script',
    fault: 'close-first late 47',
    commands: [
      { op: 'hold', api: 'InitProducerId' },
      { op: 'start', id: 'i0', call: 'connect' },
      { op: 'arrive', gate: 'InitProducerId' },
      { op: 'close', timeout: 0 },
      { op: 'connect' },
      { op: 'release', gate: 'InitProducerId', errorCode: 47 },
      { op: 'advance', ms: 500 },
      { op: 'send', id: 'm1', partition: 0 },
      { op: 'finish' },
    ],
    expectedWire: [['InitProducerId', -1, -1], ['InitProducerId', -1, -1], ['Produce', 0, 1000, 0, 0, 2]],
    expectedSettlement: [['i0', 'rejected', 'KafkaJSProducerClosedError', 'unknown'], ['m1', 'fulfilled', '0']],
    expectedState: 'READY',
    expectedLog: [[0, 1000, 0, 0, 0]],
    expectedNoops: [],
    expectedCommitted: [[0, 1000, 0, 0, 0]],
    expectedPublished: [],
    expectedSockets: [1, 2],
  },
  {
    name: 'a late 90 after the replacement connected under default',
    kind: 'idempotent',
    profile: 'default',
    shape: 'script',
    fault: 'close-first late 90',
    commands: [
      { op: 'hold', api: 'InitProducerId' },
      { op: 'start', id: 'i0', call: 'connect' },
      { op: 'arrive', gate: 'InitProducerId' },
      { op: 'close', timeout: 0 },
      { op: 'connect' },
      { op: 'release', gate: 'InitProducerId', errorCode: 90 },
      { op: 'advance', ms: 500 },
      { op: 'send', id: 'm1', partition: 0 },
      { op: 'finish' },
    ],
    expectedWire: [['InitProducerId', -1, -1], ['InitProducerId', -1, -1], ['Produce', 0, 1000, 0, 0, 2]],
    expectedSettlement: [['i0', 'rejected', 'KafkaJSProducerClosedError', 'unknown'], ['m1', 'fulfilled', '0']],
    expectedState: 'READY',
    expectedLog: [[0, 1000, 0, 0, 0]],
    expectedNoops: [],
    expectedCommitted: [[0, 1000, 0, 0, 0]],
    expectedPublished: [],
    expectedSockets: [1, 2],
  },
  {
    name: 'a late 47 after the replacement connected under tv1',
    kind: 'transactional',
    profile: 'tv1',
    shape: 'script',
    fault: 'close-first late 47',
    commands: [
      { op: 'connect', bare: true },
      { op: 'hold', api: 'InitProducerId' },
      { op: 'start', id: 'i0', call: 'transaction' },
      { op: 'arrive', gate: 'InitProducerId' },
      { op: 'close', timeout: 0 },
      { op: 'connect' },
      { op: 'release', gate: 'InitProducerId', errorCode: 47 },
      { op: 'advance', ms: 500 },
      { op: 'send', id: 'm1', partition: 0 },
      { op: 'finish' },
      { op: 'commit', id: 'c1' },
      { op: 'wait', id: 'c1' },
    ],
    expectedWire: [['InitProducerId', -1, -1], ['InitProducerId', -1, -1], ['InitProducerId', -1, -1], ['AddPartitionsToTxn', [0]], ['Produce', 0, 1000, 0, 0, 2], ['EndTxn', 'commit', 1000, 0]],
    expectedSettlement: [['i0', 'rejected', 'KafkaJSProducerClosedError', 'unknown'], ['m1', 'fulfilled', '0'], ['c1', 'fulfilled']],
    expectedState: 'READY',
    expectedLog: [[0, 1000, 0, 0, 0]],
    expectedNoops: [],
    expectedCommitted: [[0, 1000, 0, 0, 0]],
    expectedPublished: [],
    expectedSockets: [1, 2],
  },
  {
    name: 'a late 90 after the replacement connected under tv1',
    kind: 'transactional',
    profile: 'tv1',
    shape: 'script',
    fault: 'close-first late 90',
    commands: [
      { op: 'connect', bare: true },
      { op: 'hold', api: 'InitProducerId' },
      { op: 'start', id: 'i0', call: 'transaction' },
      { op: 'arrive', gate: 'InitProducerId' },
      { op: 'close', timeout: 0 },
      { op: 'connect' },
      { op: 'release', gate: 'InitProducerId', errorCode: 90 },
      { op: 'advance', ms: 500 },
      { op: 'send', id: 'm1', partition: 0 },
      { op: 'finish' },
      { op: 'commit', id: 'c1' },
      { op: 'wait', id: 'c1' },
    ],
    expectedWire: [['InitProducerId', -1, -1], ['InitProducerId', -1, -1], ['InitProducerId', -1, -1], ['AddPartitionsToTxn', [0]], ['Produce', 0, 1000, 0, 0, 2], ['EndTxn', 'commit', 1000, 0]],
    expectedSettlement: [['i0', 'rejected', 'KafkaJSProducerClosedError', 'unknown'], ['m1', 'fulfilled', '0'], ['c1', 'fulfilled']],
    expectedState: 'READY',
    expectedLog: [[0, 1000, 0, 0, 0]],
    expectedNoops: [],
    expectedCommitted: [[0, 1000, 0, 0, 0]],
    expectedPublished: [],
    expectedSockets: [1, 2],
  },
  {
    name: 'a late 47 after the replacement connected under tv2',
    kind: 'transactional',
    profile: 'tv2',
    shape: 'script',
    fault: 'close-first late 47',
    commands: [
      { op: 'connect', bare: true },
      { op: 'hold', api: 'InitProducerId' },
      { op: 'start', id: 'i0', call: 'transaction' },
      { op: 'arrive', gate: 'InitProducerId' },
      { op: 'close', timeout: 0 },
      { op: 'connect' },
      { op: 'release', gate: 'InitProducerId', errorCode: 47 },
      { op: 'advance', ms: 500 },
      { op: 'send', id: 'm1', partition: 0 },
      { op: 'finish' },
      { op: 'commit', id: 'c1' },
      { op: 'wait', id: 'c1' },
    ],
    expectedWire: [['InitProducerId', -1, -1], ['InitProducerId', -1, -1], ['InitProducerId', -1, -1], ['Produce', 0, 1000, 0, 0, 2], ['EndTxn', 'commit', 1000, 0]],
    expectedSettlement: [['i0', 'rejected', 'KafkaJSProducerClosedError', 'unknown'], ['m1', 'fulfilled', '0'], ['c1', 'fulfilled']],
    expectedState: 'READY',
    expectedLog: [[0, 1000, 0, 0, 0]],
    expectedNoops: [],
    expectedCommitted: [[0, 1000, 0, 0, 0]],
    expectedPublished: [],
    expectedSockets: [1, 2],
  },
  {
    name: 'a late 90 after the replacement connected under tv2',
    kind: 'transactional',
    profile: 'tv2',
    shape: 'script',
    fault: 'close-first late 90',
    commands: [
      { op: 'connect', bare: true },
      { op: 'hold', api: 'InitProducerId' },
      { op: 'start', id: 'i0', call: 'transaction' },
      { op: 'arrive', gate: 'InitProducerId' },
      { op: 'close', timeout: 0 },
      { op: 'connect' },
      { op: 'release', gate: 'InitProducerId', errorCode: 90 },
      { op: 'advance', ms: 500 },
      { op: 'send', id: 'm1', partition: 0 },
      { op: 'finish' },
      { op: 'commit', id: 'c1' },
      { op: 'wait', id: 'c1' },
    ],
    expectedWire: [['InitProducerId', -1, -1], ['InitProducerId', -1, -1], ['InitProducerId', -1, -1], ['Produce', 0, 1000, 0, 0, 2], ['EndTxn', 'commit', 1000, 0]],
    expectedSettlement: [['i0', 'rejected', 'KafkaJSProducerClosedError', 'unknown'], ['m1', 'fulfilled', '0'], ['c1', 'fulfilled']],
    expectedState: 'READY',
    expectedLog: [[0, 1000, 0, 0, 0]],
    expectedNoops: [],
    expectedCommitted: [[0, 1000, 0, 0, 0]],
    expectedPublished: [],
    expectedSockets: [1, 2],
  },
]

describe('Fault and outcome schedules', () => {
  it('has the five fault points for each of three profiles and three transaction outcomes for two', () => {
    expect(FAULT_ROWS).toHaveLength(21)
    expect(new Set(FAULT_ROWS.map(({ name }) => name)).size).toBe(21)
    expect(FAULT_ROWS.filter(({ fault }) => fault === 'commit')).toHaveLength(2)
  })
  runRows(FAULT_ROWS)
})

describe('Close-first late fencing answers', () => {
  it('has one row for each profile and each fencing code', () => {
    expect(LATE_FENCE_ROWS).toHaveLength(6)
  })
  runRows(LATE_FENCE_ROWS)
})

describe('Every event order of the four events', () => {
  const orders = interleavings(['answer', 'timeout', 'reconnect', 'expiry'])
  it('has a row for each order, shape and kind-profile, once', () => {
    expect(REPLAY_ROWS).toHaveLength(144)
    expect(new Set(REPLAY_ROWS.map(({ name }) => name)).size).toBe(144)
    const expected = new Set()
    for (const profile of ['default', 'tv1', 'tv2']) {
      for (const shape of ['inflight-and-queued', 'two-inflight']) {
        for (const order of orders) {
          const names = order.map((i) => ['answer', 'timeout', 'reconnect', 'expiry'][i])
          expected.add(`${profile} ${shape} ${names.join('>')}`)
        }
      }
    }
    expect(new Set(REPLAY_ROWS.map(({ name }) => name))).toEqual(expected)
  })
  runRows(REPLAY_ROWS)
})
