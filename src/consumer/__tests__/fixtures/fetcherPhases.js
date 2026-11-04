const { AsyncLocalStorage } = require('node:async_hooks')

/** The most rows one observation keeps; later ones are counted, never stored. */
const MAX_PHASES = 1000

/** @returns {object} the production classes whose prototypes the observer watches */
const productionSeams = () => ({
  Runner: require('../../runner'),
  ConsumerGroup: require('../../consumerGroup'),
  IdleWait: require('../../idleWait'),
  Broker: require('../../../broker'),
})

/**
 * Records, per node, where each fetcher is in its round: in the round's preparation, waiting for
 * metadata, registered in the idle wait and woken from it, awaiting its heartbeat, and the moment
 * its Fetch is written. It uses spies on the prototypes of the classes a round runs through and
 * an `AsyncLocalStorage` that carries the round's node id to the calls that do not take one, so it
 * touches no production code and exports nothing from it. A call outside any round has no node and
 * is not recorded.
 *
 * A woken idle wait cannot be told from an expired one here: the row only says it ended.
 *
 * Nothing is recorded before `open`, so a case keeps only the window it asks about.
 */
class FetcherPhases {
  /**
   * @param {() => number} [now] clock, replaceable for offline replay
   * @param {object} [seams] the classes to watch, replaceable for offline replay
   */
  constructor(now = () => performance.now(), seams = productionSeams()) {
    this.now = now
    this.seams = seams
    /** @type {Array<{ at: number, nodeId: number, phase: string, event: string }>} */
    this.rows = []
    this.dropped = 0
    this.recording = false
    this.spies = []
    /** @type {AsyncLocalStorage<number>} the node of the round the running code belongs to */
    this.nodes = new AsyncLocalStorage()
  }

  /**
   * Starts watching. Pair it with `detach`.
   * @returns {void}
   */
  attach() {
    const { Runner, ConsumerGroup, IdleWait, Broker } = this.seams
    this.watch(Runner.prototype, 'fetch', (call, runner, [nodeId]) => this.nodes.run(nodeId, call))
    this.watch(Runner.prototype, 'heartbeat', (call) => this.span('heartbeat', call))
    this.watch(ConsumerGroup.prototype, 'prepareFetch', (call, group, [nodeId]) =>
      this.span('preparation', call, nodeId)
    )
    for (const method of ['refreshLeaderlessMetadata', 'refreshCluster']) {
      this.watch(ConsumerGroup.prototype, method, (call) => this.span('metadata', call))
    }
    this.watch(IdleWait.prototype, 'wait', (call) => this.span('idle', call))
    this.watch(Broker.prototype, 'fetch', (call, broker) => {
      this.note('fetch', 'written', broker.nodeId)
      return call()
    })
  }

  /**
   * @param {object} target the prototype to spy on
   * @param {string} method
   * @param {(call: () => unknown, self: object, args: unknown[]) => unknown} around runs in place
   *   of the method and calls the original through `call`
   * @returns {void}
   */
  watch(target, method, around) {
    const original = target[method]
    this.spies.push(
      jest.spyOn(target, method).mockImplementation(function watched(...args) {
        return around(() => original.apply(this, args), this, args)
      })
    )
  }

  /**
   * Notes the entry and the end of one awaited step of a round and hands the step's own promise
   * back untouched.
   * @param {string} phase
   * @param {() => Promise<unknown>} call
   * @param {number} [nodeId] the round's node; the stored one when left out
   * @returns {Promise<unknown>}
   */
  span(phase, call, nodeId = this.nodes.getStore()) {
    if (!this.recording || nodeId == null) return call()
    this.note(phase, 'entered', nodeId)
    const result = call()
    const left = () => this.note(phase, 'left', nodeId)
    result.then(left, left)
    return result
  }

  /**
   * @param {string} phase
   * @param {string} event
   * @param {number} nodeId
   * @returns {void}
   */
  note(phase, event, nodeId) {
    if (!this.recording) return
    if (this.rows.length < MAX_PHASES) this.rows.push({ at: this.now(), nodeId, phase, event })
    else this.dropped += 1
  }

  /**
   * Starts recording from now on.
   * @returns {void}
   */
  open() {
    this.recording = true
  }

  /**
   * @param {number} at `performance.now()` of the earliest row wanted
   * @returns {{ rows: object[], dropped: number }} every row from `at` on, and how many later
   *   ones the cap discarded
   */
  since(at) {
    return { rows: this.rows.filter((row) => row.at >= at), dropped: this.dropped }
  }

  /**
   * Stops recording and restores every watched method. Idempotent.
   * @returns {void}
   */
  detach() {
    this.recording = false
    for (const spy of this.spies) spy.mockRestore()
    this.spies = []
  }
}

/**
 * @param {Array<{ at: number, decoded: { brokers: Array<{ nodeId: number }> } }>} answers every
 *   Metadata answer of the wire transcript
 * @param {number} from `performance.now()` of the earliest change wanted
 * @returns {{ changes: Array<{ at: number, nodeIds: number[] }>, dropped: number }} each answer
 *   from `from` on that advertised a node set other than the answer before it, so a broker-set
 *   change is told apart from the routing publications a changed leader or epoch also causes
 */
const brokerSetChanges = (answers, from) => {
  const changes = []
  let dropped = 0
  let last = null
  for (const { at, decoded } of [...answers].sort((a, b) => a.at - b.at)) {
    const nodeIds = decoded.brokers.map(({ nodeId }) => nodeId).sort((a, b) => a - b)
    const key = nodeIds.join(',')
    if (last !== null && key !== last && at >= from) {
      if (changes.length < MAX_PHASES) changes.push({ at, nodeIds })
      else dropped += 1
    }
    last = key
  }
  return { changes, dropped }
}

module.exports = { FetcherPhases, MAX_PHASES, brokerSetChanges }
