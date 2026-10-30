const { Kafka, logLevel } = require('../../../../index')
const Cluster = require('../../../cluster')
const {
  createScriptedCluster,
  groupScript,
  consumerProtocolScript,
} = require('../../../../testHelpers/scriptedCluster')
const { RecordBatch } = require('../../../protocol/recordBatch/v0')
const RecordEncoder = require('../../../protocol/recordBatch/record/v0')
const { TOPIC } = require('./fetchHarness')

const NODE = { id: 0, host: 'node-0', port: 9092 }
const WAIT_MS = 10000
const GROUP = 'close-group'

/** @returns {{promise:Promise<unknown>,resolve:Function,reject:Function}} a controlled boundary */
const gate = () => {
  let resolve, reject
  const promise = new Promise((ok, fail) => {
    resolve = ok
    reject = fail
  })
  return { promise, resolve, reject }
}

/** @param {number} ms @returns {Promise<void>} resolves after the real delay */
const pause = (ms) => new Promise((resolve) => setTimeout(resolve, ms))

/**
 * @param {() => boolean} predicate
 * @param {string} label names what is awaited, for the failure
 * @returns {Promise<void>} resolves when the predicate holds
 * @throws {Error} when it does not hold within the budget
 */
const until = async (predicate, label) => {
  const deadline = Date.now() + WAIT_MS
  while (!predicate()) {
    if (Date.now() > deadline) throw new Error(`Did not happen in time: ${label}`)
    await pause(5)
  }
}

/**
 * One write of the production encoder, so the decoder reads it as it reads a broker's.
 * @param {number} offset first and only offset of the batch
 * @param {boolean} [corrupt=false] flip one byte inside the CRC-covered region
 * @returns {Promise<Buffer>} a one-record batch whose value names its offset
 */
const batchAt = async (offset, corrupt = false) => {
  const batch = Buffer.from(
    (
      await RecordBatch({
        firstOffset: String(offset),
        firstTimestamp: 1700000000000,
        maxTimestamp: 1700000000000,
        partitionLeaderEpoch: 5,
        records: [
          RecordEncoder({
            key: Buffer.from(`key-${offset}`),
            value: Buffer.from(`value-${offset}`),
          }),
        ],
      })
    ).buffer
  )
  if (corrupt) batch[8 + 4 + 4 + 1 + 4 + 2 + 4 + 8 + 8 + 8] ^= 0x01
  return batch
}

/**
 * Holds the answer of one API after the request was received and recorded: the broker has applied
 * what the request carried, only its answer waits.
 * @param {object} cluster the scripted cluster
 * @param {string} api the API name
 * @returns {{ hits: number, received: object[], open: boolean, release: () => void }} the hold
 */
const holdAnswer = (cluster, api) => {
  const original = cluster.answerers.get(api)
  const opened = gate()
  const hold = {
    hits: 0,
    received: [],
    open: false,
    release: () => {
      hold.open = true
      opened.resolve()
    },
  }
  cluster.answer(api, async (fields, record) => {
    hold.hits += 1
    hold.received.push(fields)
    if (!hold.open) await opened.promise
    return original(fields, record)
  })
  return hold
}

/**
 * Takes over the cluster's group-coordinator lookup for the consumers of one test. Every lookup
 * counts; the coordinator a membership caches can be made to look disconnected, so the next
 * caller really looks it up; and scripted steps hold, delay or fail the next lookups.
 */
class LookupSwitch {
  constructor() {
    this.calls = []
    /** @type {import('../../../cluster')|null} the cluster of the first consumer that looked a coordinator up */
    this.cluster = null
    this.stale = false
    this.steps = []
    this.original = Cluster.prototype.findGroupCoordinator
    const self = this
    this.spy = jest
      .spyOn(Cluster.prototype, 'findGroupCoordinator')
      .mockImplementation(async function (args) {
        self.calls.push(args)
        self.cluster ??= this
        const step = self.steps.shift()
        const broker = step
          ? await step(
              () => self.original.call(this, args),
              () => self.original.call(this, { groupId: args.groupId })
            )
          : await self.original.call(this, args)
        return step ? broker : self.wrap(broker)
      })
  }

  /** @param {object} broker a real broker @returns {object} the same broker, its connection reading closed while stale */
  wrap(broker) {
    return new Proxy(broker, {
      get: (target, prop) => {
        if (prop === 'isConnected') return () => !this.stale && target.isConnected()
        const value = Reflect.get(target, prop)
        return typeof value === 'function' ? value.bind(target) : value
      },
    })
  }

  /** Makes every coordinator this switch handed out, and will hand out, read as disconnected. */
  makeStale() {
    this.stale = true
  }

  /**
   * Holds the next lookup before anything is asked of the cluster; once released it answers as a
   * lookup that knows nothing of the caller's fence.
   * @returns {{ release: () => void, entered: Promise<void> }}
   */
  hold() {
    const opened = gate()
    const entered = gate()
    this.steps.push(async (_with, without) => {
      entered.resolve()
      await opened.promise
      return without()
    })
    return { release: opened.resolve, entered: entered.promise }
  }

  /**
   * The next lookup spends `ms` and then fails with a retriable error.
   * @param {number} ms
   * @returns {void}
   */
  failAfter(ms) {
    this.steps.push(async () => {
      await pause(ms)
      throw Object.assign(new Error('coordinator moved'), {
        name: 'KafkaJSConnectionError',
        retriable: true,
      })
    })
  }

  /**
   * The next lookup spends `ms` and then answers.
   * @param {number} ms
   * @returns {void}
   */
  answerAfter(ms) {
    this.steps.push(async (answer) => {
      await pause(ms)
      return answer()
    })
  }

  /** @returns {void} restores the cluster's own lookup */
  restore() {
    this.spy.mockRestore()
  }
}

/**
 * A public consumer over a scripted one-partition topic whose log holds one record per batch at
 * offsets `0..last`, with everything the tests hold or count.
 */
class CloseRig {
  /**
   * @param {object} [options]
   * @param {'classic'|'consumer'} [options.protocol='classic']
   * @param {number} [options.last=2] last record offset
   * @param {boolean} [options.enforce=true] whether the transport enforces request timeouts
   * @param {object} [options.retry] client retry options
   * @param {number} [options.partitions=1]
   * @param {Record<number, string>} [options.committed] committed offsets by partition
   * @param {Record<string, Buffer>} [options.log] raw batches by `partition|offset`, over the default log
   * @returns {Promise<CloseRig>}
   */
  static async open({
    protocol = 'classic',
    last = 2,
    enforce = true,
    retry = {},
    partitions = 1,
    committed,
    log,
  } = {}) {
    const batches = {}
    for (let offset = 0; offset <= last; offset++) batches[`0|${offset}`] = await batchAt(offset)
    Object.assign(batches, log)
    const script = protocol === 'consumer' ? consumerProtocolScript : groupScript
    const cluster = script(
      createScriptedCluster({
        nodes: [NODE],
        profile: protocol === 'consumer' ? 'consumer' : 'default',
      }),
      { partitions, committed }
    ).answer('Fetch', async ({ maxWaitMs, topics }) => {
      const entries = topics[0].partitions.map(({ partition, fetchOffset }) => ({
        partition,
        errorCode: 0,
        records: batches[`${partition}|${fetchOffset}`] ?? false,
        abortedTransactions: [],
      }))
      if (entries.every(({ records }) => records === false)) await pause(maxWaitMs)
      return { partitions: entries }
    })
    const kafka = new Kafka(
      cluster.kafkaOptions({
        logLevel: logLevel.NOTHING,
        enforceRequestTimeout: enforce,
        retry: { retries: 1, initialRetryTime: 10, maxRetryTime: 20, ...retry },
      })
    )
    return new CloseRig({ cluster, kafka, protocol })
  }

  /** @param {{cluster: object, kafka: Kafka, protocol: string}} parts */
  constructor({ cluster, kafka, protocol }) {
    this.cluster = cluster
    this.kafka = kafka
    this.protocol = protocol
    this.consumers = []
    this.holds = []
    this.lookups = null
  }

  /** @returns {LookupSwitch} the lookup switch of this rig, installed on first use */
  lookup() {
    this.lookups ??= new LookupSwitch()
    return this.lookups
  }

  /**
   * @param {object} [options] consumer options over the rig's defaults
   * @returns {object} a consumer, disconnected by `dispose`
   */
  consumer(options = {}) {
    const consumer = this.kafka.consumer({
      groupId: GROUP,
      maxWaitTimeInMs: 20,
      ...(this.protocol === 'consumer' ? { groupProtocol: 'consumer' } : {}),
      ...options,
    })
    this.consumers.push(consumer)
    return consumer
  }

  /**
   * Starts a consumer and waits for its first deliveries.
   * @param {object} [options]
   * @param {object} [options.consumer] consumer options
   * @param {object} [options.run] run options
   * @param {number} [options.delivered=3] records to wait for
   * @returns {Promise<{ consumer: object, delivered: string[], events: string[] }>}
   */
  async started({ consumer: consumerOptions = {}, run = {}, delivered = 3 } = {}) {
    const consumer = this.consumer(consumerOptions)
    const seen = []
    const events = []
    for (const name of ['STOP', 'DISCONNECT'])
      consumer.on(consumer.events[name], () => events.push(name))
    await consumer.connect()
    await consumer.subscribe({ topic: TOPIC, fromBeginning: true })
    await consumer.run({
      autoCommit: true,
      autoCommitInterval: 60000,
      eachMessage: async ({ message }) => {
        seen.push(message.value.toString())
      },
      ...run,
    })
    await until(() => seen.length >= delivered, 'the deliveries')
    return { consumer, delivered: seen, events }
  }

  /** @param {string} api @returns {object} a hold of that API's answer, released by `dispose` */
  hold(api) {
    const held = holdAnswer(this.cluster, api)
    this.holds.push(held)
    return held
  }

  /** @param {string} api @returns {object[]} the frames of one API */
  frames(api) {
    return this.cluster.requests().filter((request) => request.api === api)
  }

  /** @returns {Promise<void>} releases every hold, disconnects every consumer, checks the cluster */
  async dispose() {
    for (const held of this.holds.splice(0)) held.release()
    for (const consumer of this.consumers.splice(0)) await consumer.disconnect().catch(() => {})
    this.lookups?.restore()
    this.cluster.assertClean()
  }
}

module.exports = {
  NODE,
  GROUP,
  TOPIC,
  gate,
  pause,
  until,
  batchAt,
  holdAnswer,
  LookupSwitch,
  CloseRig,
}
