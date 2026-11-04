const createFetchManager = require('../../fetchManager')
const { createLogger, LEVELS } = require('../../../loggers')
const apiKeys = require('../../../protocol/requests/apiKeys')
const { FetcherRecreations } = require('./fetcherRecreations')
const {
  TOPIC,
  TOPIC_ID,
  apiVersionsBody,
  createFakeBroker,
  createFetchGroup,
  decodeFetchRequest,
  fetchResponse,
} = require('./fetchHarness')

/** The leader epoch the seam's first Metadata answer carries; a later answer counts up from it. */
const EPOCH = 10
/** The Fetch wait of every node, ms: a poll nothing interrupts lasts this long. */
const MAX_WAIT_MS = 5000
/** How far one `advance` step moves the fake clock, ms; the resolution of every recorded time. */
const STEP_MS = 50
/** A few ms of fake time, ms: the allowance for a wake that needs no timer. */
const WAKE_MS = 100
/** The nodes a seam can address: each gets a broker, so an answer may add a node of the roster. */
const ROSTER = [1, 2, 3, 4]

/**
 * Lets continuations, immediates and zero-delay timers run without moving the fake clock.
 * @param {number} [rounds] how many times to yield
 * @returns {Promise<void>}
 */
const drain = async (rounds = 3) => {
  for (let round = 0; round < rounds; round++) {
    await new Promise(setImmediate)
    await jest.advanceTimersByTimeAsync(0)
    for (let turn = 0; turn < 12; turn++) await Promise.resolve()
  }
}

/**
 * One consumer group over one topic and a roster of fake nodes, driven by the real fetch manager
 * and its fetchers on a fake clock, with no broker and no socket to a real one. The group, the
 * metadata cache and the fetchers are the production ones. A Metadata answer is installed as
 * `BrokerPool#refreshMetadata` installs it: it becomes the routing the group reads, and then goes
 * through the cache, which publishes the routing change, applies its stale-epoch rule and writes
 * the epoch rule's result back onto the answer. Only the node set and the brokers are stand-ins:
 * the node set is exactly what the last answer advertised, and each node answers a Fetch after a
 * long poll that lasts the Fetch wait, or until the test releases it.
 *
 * Use it under `jest.useFakeTimers` that leaves `setImmediate` and `nextTick` real; `open` sets
 * that up, and `close` restores real timers.
 */
class RoutingSeam {
  /**
   * @param {object} options
   * @param {number[]} options.nodes the node ids the cluster advertises first
   * @param {Record<number, number>} options.leaders the leader of each partition, `-1` for none;
   *   the keys are the topic's partitions
   * @returns {Promise<RoutingSeam>} a seam whose first answer is installed and whose fetchers have
   *   not started
   */
  static async open({ nodes, leaders }) {
    const partitions = Object.keys(leaders).map(Number)
    const roster = [...nodes]
    const harness = await createFetchGroup({
      partitions,
      knownNodes: roster,
      partitionLeaders: leaders,
    })
    const seam = new RoutingSeam(harness, roster, partitions)
    await seam.connectBrokers()
    seam.install({ leaders, epoch: EPOCH })
    harness.group.maxWaitTime = MAX_WAIT_MS
    jest.useFakeTimers({ doNotFake: ['setImmediate', 'nextTick'] })
    return seam
  }

  /**
   * @param {object} harness what `createFetchGroup` returned
   * @param {number[]} nodes the live node set; the seam changes it in place
   * @param {number[]} partitions the topic's partitions
   */
  constructor(harness, nodes, partitions) {
    this.harness = harness
    this.group = harness.group
    this.nodes = nodes
    this.partitions = partitions
    /** @type {object|null} the answer the group routes by: the pool's `metadata` */
    this.current = null
    /** @type {Array<{ at: number, node: number, partitions: number[] }>} every Fetch written */
    this.fetches = []
    this.recreations = new FetcherRecreations(() => Date.now())
    /** @type {Map<number, object>} the broker of each node of the roster */
    this.brokers = new Map()
    /** @type {Set<number>} nodes whose polls stay open until `release` */
    this.held = new Set()
    /** @type {Map<number, Set<() => void>>} the open polls of each node, each with its finish */
    this.polls = new Map()
    this.manager = null
    this.running = null

    const { cluster } = harness
    cluster.getNodeIds = () => [...this.nodes]
    cluster.findBroker = ({ nodeId }) => this.brokers.get(nodeId)
    cluster.findTopicPartitionMetadata = () => this.current.topicMetadata[0].partitionMetadata
  }

  /**
   * Builds and connects the broker of every node of the roster, before the fake clock starts.
   * @returns {Promise<void>}
   */
  async connectBrokers() {
    for (const node of ROSTER) {
      const { broker, state } = createFakeBroker((header) =>
        header.apiKey === apiKeys.ApiVersions
          ? apiVersionsBody(18, true)
          : fetchResponse(
              decodeFetchRequest(state.writes).topics[0].partitions.map(({ partition }) => ({
                partition,
                errorCode: 0,
              }))
            )
      )
      await broker.connect()
      const send = broker.fetch.bind(broker)
      broker.fetch = async (request) => {
        const partitions = request.topics.flatMap((entry) => entry.partitions)
        this.fetches.push({
          at: Date.now(),
          node,
          partitions: partitions.map(({ partition }) => partition),
        })
        await this.poll(node, request.maxWaitTime)
        return send(request)
      }
      this.brokers.set(node, broker)
    }
  }

  /**
   * One long poll of a node: it ends at the Fetch wait, when the node is released or when the
   * node leaves the node set.
   * @param {number} node
   * @param {number} waitMs
   * @returns {Promise<void>}
   */
  poll(node, waitMs) {
    return new Promise((resolve) => {
      const open = this.polls.get(node) ?? new Set()
      this.polls.set(node, open)
      let timer = null
      const finish = () => {
        clearTimeout(timer)
        open.delete(finish)
        resolve()
      }
      if (!this.held.has(node)) timer = setTimeout(finish, waitMs)
      open.add(finish)
    })
  }

  /**
   * Keeps every poll of a node open, the ones to come as well, until `release`.
   * @param {number} node
   * @returns {void}
   */
  hold(node) {
    this.held.add(node)
  }

  /**
   * Ends the polls a node has open and stops holding it.
   * @param {number} node
   * @returns {void}
   */
  release(node) {
    this.held.delete(node)
    for (const finish of [...(this.polls.get(node) ?? [])]) finish()
  }

  /**
   * Installs a Metadata answer the way the pool does and applies the node set it advertises. A node
   * that leaves the set has its open polls ended, as a closed connection ends them.
   * @param {object} answer
   * @param {Record<number, number>} [answer.leaders] the leader the answer names per partition; a
   *   partition left out keeps the leader the routing holds
   * @param {number} answer.epoch the leader epoch every partition of the answer carries
   * @param {number[]} [answer.brokers] the nodes the answer advertises; the current set when left out
   * @returns {void}
   */
  install({ leaders = {}, epoch, brokers = [...this.nodes] }) {
    const held = this.current?.topicMetadata[0].partitionMetadata ?? []
    const response = {
      brokers: brokers.map((nodeId) => ({ nodeId, host: `kafka${nodeId}`, port: 9092 })),
      topicMetadata: [
        {
          topic: TOPIC,
          topicId: TOPIC_ID,
          partitionMetadata: this.partitions.map((partitionId) => ({
            partitionId,
            leader: leaders[partitionId] ?? held.find((p) => p.partitionId === partitionId)?.leader,
            leaderEpoch: epoch,
            offlineReplicas: [],
          })),
        },
      ],
    }
    this.current = response
    this.harness.metadataCache.update(response)
    const left = this.nodes.filter((node) => !brokers.includes(node))
    this.nodes.splice(0, this.nodes.length, ...brokers)
    for (const node of left) this.release(node)
  }

  /**
   * Starts the real fetch manager over the current node set.
   * @returns {void}
   */
  start() {
    const logger = createLogger({ level: LEVELS.DEBUG, logCreator: this.recreations.logCreator })
    this.manager = createFetchManager({
      logger,
      getNodeIds: () => this.group.getNodeIds(),
      fetch: (nodeId) => this.group.fetch(nodeId),
      handler: async () => {},
      concurrency: 1,
    })
    this.running = this.manager.start()
    this.running.catch(() => {})
  }

  /**
   * Moves the fake clock forward in small steps, letting the work each step releases run.
   * @param {number} ms
   * @returns {Promise<void>}
   */
  async advance(ms) {
    for (let elapsed = 0; elapsed < ms; elapsed += STEP_MS) {
      await jest.advanceTimersByTimeAsync(Math.min(STEP_MS, ms - elapsed))
      await drain(3)
    }
  }

  /**
   * @returns {number} the fake clock, ms
   */
  now() {
    return Date.now()
  }

  /**
   * @param {number} node
   * @param {number} partition
   * @param {number} since fake clock, ms
   * @returns {number} how long after `since` the node first wrote a Fetch that carried the
   *   partition; `Infinity` when it has not
   */
  waited(node, partition, since) {
    const fetch = this.fetches.find(
      (f) => f.node === node && f.at >= since && f.partitions.includes(partition)
    )
    return fetch ? fetch.at - since : Infinity
  }

  /**
   * Stops the fetchers, ends every poll, restores real timers and disconnects the brokers.
   * @returns {Promise<void>}
   * @throws {Error} what the fetch manager ended with
   */
  async close() {
    try {
      this.group.onStopRequested()
      for (const node of ROSTER) this.release(node)
      if (this.manager) await this.finish(Promise.all([this.manager.stop(), this.running]))
    } finally {
      jest.clearAllTimers()
      jest.useRealTimers()
      await Promise.all([...this.brokers.values()].map((broker) => broker.disconnect()))
      await this.harness.broker.disconnect()
    }
  }

  /**
   * Lets work and the fake clock run until a promise settles, so a stuck stop fails instead of
   * hanging.
   * @param {Promise<*>} promise
   * @returns {Promise<*>} what it resolved with
   * @throws {Error} what it rejected with, or that it did not settle in time
   */
  async finish(promise) {
    const state = { settled: false }
    const settle = () => {
      state.settled = true
    }
    promise.then(settle, settle)
    for (let n = 0; n < 200 && !state.settled; n++) {
      await drain(1)
      await jest.advanceTimersByTimeAsync(100)
    }
    if (!state.settled) throw new Error('The promise did not settle')
    return promise
  }
}

module.exports = { RoutingSeam, EPOCH, MAX_WAIT_MS, WAKE_MS }
