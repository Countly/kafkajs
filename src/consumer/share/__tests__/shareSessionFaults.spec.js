const assert = require('node:assert/strict')
const {
  ackAnswer,
  acquired,
  deferred,
  fetchAnswer,
  flush,
  part,
  topError,
  until,
} = require('../../../../testHelpers/shareHarness')
const javaRecord = require('../../../../testHelpers/wire/expected').shareFetchV2
const ConnectionPool = require('../../../network/connectionPool')
const RequestQueue = require('../../../network/requestQueue')
const { requests } = require('../../../protocol/requests')
const { ShareSession } = require('../shareSession')

const {
  FaultAudit,
  FaultCase,
  NAME,
  IDS,
  ALIAS,
  H,
  J,
  STALE_CALL,
  dataPart,
  codesOf,
  track,
  covers,
} = require('../../../../testHelpers/shareFaultAudit')

/**
 * @param {{fields: object}} frame A recorded ShareFetch or ShareAcknowledge frame.
 * @returns {object[]} The acknowledgement batches it carried, in wire order.
 */
const batchesOf = ({ fields }) =>
  (fields.topics || []).flatMap((topic) =>
    topic.partitions.flatMap((partition) => partition.acknowledgementBatches)
  )

const A0_H = `A:0@${H}`
const A1_H = `A:1@${H}`
const NODE_ENDPOINT = { nodeId: 2, host: 'node2', port: 9094, rack: null }
const RETRY_250 = {
  initialRetryTime: 250,
  maxRetryTime: 250,
  multiplier: 1,
  factor: 0,
  retries: 100,
}

/**
 * @param {object} fields A decoded ShareFetch or ShareAcknowledge request.
 * @returns {Array<{topicId: string, partition: number}>} Every partition it lists, in wire order.
 */
const listed = (fields) =>
  (fields.topics || []).flatMap((topic) =>
    topic.partitions.map(({ partitionIndex }) => ({
      topicId: topic.topicId,
      partition: partitionIndex,
    }))
  )

/**
 * A fault answer for one share frame: the literal broker response a row injects. Partition 1 of
 * topic A is the selected partition; every other listed partition answers quietly, so a fault never
 * hides behind an omitted sibling unless the row asks for it.
 * @param {78|79} api The frame's API key.
 * @param {object} fields The frame's decoded fields.
 * @param {{code?: number, partition?: number, ack?: number, omit?: boolean, duplicate?: boolean}} spec
 *   `code` is a top-level error; `partition` is the selected partition's error (the fetch error
 *   for API 78, the acknowledgement error for API 79); `ack` is a ShareFetch acknowledge error.
 * @returns {object} The answer object for the scripted cluster.
 */
const faultAnswer = (api, fields, { code, partition, ack, omit, duplicate, target = 1 }) => {
  if (code !== undefined) return api === 78 ? topError(code) : ackAnswer([], { errorCode: code })
  const rows = listed(fields).filter(({ topicId, partition: index }) => {
    return !(omit && topicId === IDS.A && index === target)
  })
  const selected = (row) => row.topicId === IDS.A && row.partition === target
  const build = (row) => {
    if (api === 79) return { ...row, code: selected(row) ? partition : 0 }
    return part({
      topicId: row.topicId,
      partition: row.partition,
      code: selected(row) && ack === undefined ? partition : 0,
      ack: selected(row) ? (ack ?? 0) : 0,
      leader:
        selected(row) && (partition === 6 || partition === 74)
          ? { leaderId: 2, leaderEpoch: 8 }
          : { leaderId: -1, leaderEpoch: -1 },
    })
  }
  const answered = rows.map(build)
  if (duplicate) answered.push(...rows.filter(selected).map(build))
  const extra = partition === 6 || partition === 74 ? { nodeEndpoints: [NODE_ENDPOINT] } : {}
  return api === 78 ? fetchAnswer(answered, extra) : ackAnswer(answered, extra)
}

/**
 * The shared driver of the session and acknowledgement fault rows: one affected frame of node 1
 * (or of node 0 for a renewal of its held handler) is held at the wire, answered with the row's
 * fault, and the effects are read from the public events and the recorded frames.
 */
class DataFaultCase extends FaultCase {
  /**
   * @param {object} row The literal oracle row.
   * @returns {DataFaultCase} A case with no client yet.
   */
  constructor(row) {
    super(row)
    this.lockMs = 15000
    this.spec = null
    this.target = 1
  }

  /**
   * Installs the session-aware broker fixture after the original client setup.
   * @param {object} options Original start options.
   * @returns {Promise<void>}
   */
  async start(options) {
    await super.start(options)
    this.cluster.answer('ShareFetch', (fields, record) => this.fetchAnswerFor(fields, record))
  }

  /**
   * Serves supplied acquisitions from the broker's independently decoded incremental model.
   * Omitted request partitions remain in that model until a fetch explicitly forgets them.
   * @param {object} fields A decoded ShareFetch request.
   * @param {object} record The independently recorded request on its original socket.
   * @returns {object} A's H/J or B's H control, with the case's lock duration.
   */
  fetchAnswerFor(fields, record) {
    const model = new Map()
    for (const frame of this.frames(78, 79)) {
      if (frame.node !== record.node || frame.generation !== record.generation) continue
      if ([0, -1].includes(frame.fields.shareSessionEpoch)) model.clear()
      for (const { topicId, partition } of listed(frame.fields))
        model.set(`${topicId}:${partition}`, { topicId, partition })
      for (const topic of frame.fields.forgottenTopicsData || [])
        for (const partition of topic.partitions) model.delete(`${topic.topicId}:${partition}`)
      if (frame.correlationId === record.correlationId) break
    }
    const topics = new Map()
    for (const { topicId, partition } of listed(fields))
      model.set(`${topicId}:${partition}`, { topicId, partition })
    for (const { topicId, partition } of model.values()) {
      if (!topics.has(topicId)) topics.set(topicId, { topicId, partitions: [] })
      topics.get(topicId).partitions.push({ partitionIndex: partition })
    }
    const answer = super.fetchAnswerFor({ ...fields, topics: [...topics.values()] })
    for (const topic of answer.responses) {
      if (topic.topicId !== IDS.B) continue
      for (const partition of topic.partitions) {
        if (!partition.acquiredRecords.length) continue
        partition.acquiredRecords = [acquired(H, H, 2)]
        partition.records = javaRecord.recordsHex
      }
    }
    return { ...answer, acquisitionLockTimeoutMs: this.lockMs }
  }

  /**
   * Starts the client and the run: node 0 holds A0/H, node 1 serves A1 and A2.
   * @param {object} [options] Case options.
   * @param {string[]} [options.hold] Records whose handlers wait.
   * @param {number} [options.expectCalls] Handler entries to wait for.
   * @param {object} [options.startOptions] Options of {@link FaultCase#start}.
   * @param {string[]} [options.supply] Partitions supplied with acquisitions of H and J.
   * @param {object} [options.supplyCounts] Literal acquisition counts overriding the default one.
   * @returns {Promise<void>}
   */
  async begin({
    hold = [A0_H],
    expectCalls = 5,
    startOptions = {},
    supply = ['A:0', 'A:1', 'A:2'],
    supplyCounts = {},
  } = {}) {
    await this.start({ retry: RETRY_250, ...startOptions })
    await this.armBeforeJoin?.()
    this.supplyOnce(...supply)
    for (const [key, count] of Object.entries(supplyCounts)) this.supply.set(key, count)
    for (const identity of hold) this.hold(identity)
    await this.join()
    if (expectCalls) await until(() => this.calls.length >= expectCalls, 'the original handlers')
    await flush()
  }

  /**
   * @param {string} identity An identity such as `A:0@9007199254740993`.
   * @returns {object|undefined} The first call that delivered it.
   */
  callOf(identity) {
    return this.calls.find((call) => call.identity === identity)
  }

  /**
   * Holds one frame at the wire.
   * @param {78|79} api The frame's API key.
   * @param {(call: object) => boolean} match Selects the frame.
   * @returns {object} The held gate.
   */
  holdFrame(api, match) {
    return this.holdWire({ api: api === 78 ? 'ShareFetch' : 'ShareAcknowledge', match })
  }

  /**
   * Ends a held frame: a typed transport failure, or the literal fault answer.
   * @param {object} gate The held gate.
   * @param {object} spec The fault spec: a transport kind or an answer spec.
   * @returns {Promise<void>}
   */
  async fire(gate, spec) {
    const call = await this.arrival(gate)
    await flush()
    if (spec.transport) {
      this.transportGate = gate
      if (spec.transport === 'timeout') await this.pump(3100)
      else await this.fireTransport(spec.transport)
    } else gate.release(faultAnswer(call.apiKey, call.fields, { target: this.target, ...spec }))
    await flush()
  }

  /**
   * @param {number} partition Partition of topic A.
   * @returns {object[]} The public acknowledge results of that partition, oldest first.
   */
  resultsOf(partition) {
    return this.seen.acks.filter((ack) => ack.partition === partition)
  }

  /**
   * @param {number} partition Partition of topic A.
   * @param {string} offset An exact offset.
   * @returns {object[]} The terminal results covering the offset.
   */
  terminalFor(partition, offset) {
    return this.resultsOf(partition).filter((ack) => ack.type !== 'renew' && covers([ack], offset))
  }

  /**
   * Asserts the latest terminal result of the selected record, with its reason and original cause;
   * the audit proves each acquisition settled once.
   * @param {number} partition Partition of topic A.
   * @param {string} offset An exact offset.
   * @param {'accepted'|'session-lost'|'rejected-by-broker'|'membership-lost'} result Expected.
   * @param {number|string|null|undefined} cause Expected protocol code, when numeric.
   * @returns {void}
   */
  assertResult(partition, offset, result, cause) {
    const terminal = this.terminalFor(partition, offset)
    assert(terminal.length >= 1, `a terminal result of ${partition}@${offset}`)
    const outcome = terminal.at(-1)
    if (result === 'accepted') return assert.equal(outcome.error, null)
    assert.equal(outcome.error.reason, result)
    if (typeof cause === 'number') assert(codesOf(outcome.error).includes(cause), 'cause code')
    return undefined
  }

  /**
   * @param {number} node A node id.
   * @returns {object[]} The ShareFetch and ShareAcknowledge frames written to the node.
   */
  frameLog(node) {
    return this.frames(78, 79).filter((frame) => frame.node === node)
  }

  /**
   * @param {number} node A node id.
   * @param {number} from How many frames of that node existed at the fault.
   * @returns {object|undefined} The first frame of the node after the fault.
   */
  nextFrame(node, from) {
    return this.frameLog(node)[from]
  }
}

/** Literal complete observation sets; tuple expansion only substitutes fixed fixture notation. */
const OBSERVATIONS = {
  accepted: {
    handlers: [
      ['A', 0, 'HJ'],
      ['A', 1, 'HJ'],
      ['A', 2, 'HJ'],
    ],
    mutations: [
      ['A', 0, 'HJ', [1]],
      ['A', 1, 'HJ', [1]],
      ['A', 2, 'HJ', [1]],
    ],
    settlements: [
      ['A', 0, 'HJ', 'accept', null, null],
      ['A', 1, 'HJ', 'accept', null, null],
      ['A', 2, 'HJ', 'accept', null, null],
    ],
  },
  recreated: {
    handlers: [
      ['A', 0, 'HJ'],
      ['A', 1, 'HJ'],
      ['A', 2, 'HJ'],
      ['A', 2, 'HJ'],
      ['B', 0, 'H'],
    ],
    mutations: [
      ['A', 0, 'HJ', [1]],
      ['A', 1, 'HJ', [1]],
      ['A', 2, 'HJ', [1]],
      ['B', 0, 'H', [1]],
    ],
    settlements: [
      ['A', 0, 'HJ', 'accept', null, null],
      ['A', 1, 'HJ', 'accept', null, null],
      ['A', 2, 'HJ', 'accept', null, null],
      ['A', 2, 'HJ', 'accept', 'session-lost', 103],
      ['B', 0, 'H', 'accept', null, null],
    ],
  },
  incrementalRecreated: {
    handlers: [
      ['A', 0, 'HJ'],
      ['A', 1, 'HJ'],
      ['A', 1, 'HJ'],
      ['A', 2, 'HJ'],
      ['A', 2, 'HJ'],
      ['B', 0, 'H'],
    ],
    mutations: [
      ['A', 0, 'HJ', [1]],
      ['A', 1, 'HJ', [1]],
      ['A', 1, 'HJ', [1]],
      ['A', 2, 'HJ', [1]],
      ['B', 0, 'H', [1]],
    ],
    settlements: [
      ['A', 0, 'HJ', 'accept', null, null],
      ['A', 1, 'HJ', 'accept', null, null],
      ['A', 1, 'HJ', 'accept', null, null],
      ['A', 2, 'HJ', 'accept', null, null],
      ['A', 2, 'HJ', 'accept', 'session-lost', 103],
      ['B', 0, 'H', 'accept', null, null],
    ],
  },
  empty: { handlers: [], mutations: [], settlements: [] },
  a0: {
    handlers: [['A', 0, 'HJ']],
    mutations: [['A', 0, 'HJ', [1]]],
    settlements: [['A', 0, 'HJ', 'accept', null, null]],
  },
  capacity: {
    handlers: [
      ['A', 0, 'HJ'],
      ['A', 1, 'HJ'],
    ],
    mutations: [
      ['A', 0, 'HJ', [1]],
      ['A', 1, 'HJ', [1]],
    ],
    settlements: [
      ['A', 0, 'HJ', 'accept', null, null],
      ['A', 1, 'HJ', 'accept', null, null],
    ],
  },
  final: {
    handlers: [['A', 0, 'H']],
    mutations: [['A', 0, 'J', [2]]],
    settlements: [
      ['A', 0, 'H', 'release', 'membership-lost', null],
      ['A', 0, 'J', 'release', null, null],
    ],
  },
  openTerminal: {
    handlers: [
      ['A', 0, 'H'],
      ['A', 0, 'HJ'],
      ['A', 1, 'HJ'],
      ['A', 2, 'HJ'],
    ],
    mutations: [
      ['A', 0, 'HJ', [1]],
      ['A', 1, 'HJ', [1]],
      ['A', 2, 'HJ', [1]],
      ['A', 0, 'J', [2]],
    ],
    settlements: [
      ['A', 0, 'HJ', 'accept', null, null],
      ['A', 1, 'HJ', 'accept', null, null],
      ['A', 2, 'HJ', 'accept', null, null],
      ['A', 0, 'H', 'release', 'membership-lost', null],
      ['A', 0, 'J', 'release', null, null],
    ],
  },
  incrementalAccepted: {
    handlers: [
      ['A', 0, 'HJ'],
      ['A', 1, 'HJ'],
      ['A', 1, 'HJ'],
      ['A', 2, 'HJ'],
    ],
    mutations: [
      ['A', 0, 'HJ', [1]],
      ['A', 1, 'HJ', [1]],
      ['A', 1, 'HJ', [1]],
      ['A', 2, 'HJ', [1]],
    ],
    settlements: [
      ['A', 0, 'HJ', 'accept', null, null],
      ['A', 1, 'HJ', 'accept', null, null],
      ['A', 2, 'HJ', 'accept', null, null],
      ['A', 1, 'HJ', 'accept', null, null],
    ],
  },
  incrementalLost: {
    handlers: [
      ['A', 0, 'HJ'],
      ['A', 1, 'HJ'],
      ['A', 1, 'HJ'],
      ['A', 2, 'HJ'],
    ],
    mutations: [
      ['A', 0, 'HJ', [1]],
      ['A', 1, 'HJ', [1]],
      ['A', 1, 'HJ', [1]],
      ['A', 2, 'HJ', [1]],
    ],
    settlements: [
      ['A', 0, 'HJ', 'accept', null, null],
      ['A', 1, 'HJ', 'accept', null, null],
      ['A', 2, 'HJ', 'accept', null, null],
      ['A', 1, 'HJ', 'accept', 'session-lost', null],
    ],
  },
  incrementalLost122: {
    handlers: [
      ['A', 0, 'HJ'],
      ['A', 1, 'HJ'],
      ['A', 1, 'HJ'],
      ['A', 2, 'HJ'],
    ],
    mutations: [
      ['A', 0, 'HJ', [1]],
      ['A', 1, 'HJ', [1]],
      ['A', 1, 'HJ', [1]],
      ['A', 2, 'HJ', [1]],
    ],
    settlements: [
      ['A', 0, 'HJ', 'accept', null, null],
      ['A', 1, 'HJ', 'accept', null, null],
      ['A', 2, 'HJ', 'accept', null, null],
      ['A', 1, 'HJ', 'accept', 'session-lost', 122],
    ],
  },
  incrementalLost123: {
    handlers: [
      ['A', 0, 'HJ'],
      ['A', 1, 'HJ'],
      ['A', 1, 'HJ'],
      ['A', 2, 'HJ'],
    ],
    mutations: [
      ['A', 0, 'HJ', [1]],
      ['A', 1, 'HJ', [1]],
      ['A', 1, 'HJ', [1]],
      ['A', 2, 'HJ', [1]],
    ],
    settlements: [
      ['A', 0, 'HJ', 'accept', null, null],
      ['A', 1, 'HJ', 'accept', null, null],
      ['A', 2, 'HJ', 'accept', null, null],
      ['A', 1, 'HJ', 'accept', 'session-lost', 123],
    ],
  },
  incrementalLost133: {
    handlers: [
      ['A', 0, 'HJ'],
      ['A', 1, 'HJ'],
      ['A', 1, 'HJ'],
      ['A', 2, 'HJ'],
    ],
    mutations: [
      ['A', 0, 'HJ', [1]],
      ['A', 1, 'HJ', [1]],
      ['A', 1, 'HJ', [1]],
      ['A', 2, 'HJ', [1]],
    ],
    settlements: [
      ['A', 0, 'HJ', 'accept', null, null],
      ['A', 1, 'HJ', 'accept', null, null],
      ['A', 2, 'HJ', 'accept', null, null],
      ['A', 1, 'HJ', 'accept', 'session-lost', 133],
    ],
  },
  incrementalUnknown: {
    handlers: [
      ['A', 0, 'HJ'],
      ['A', 1, 'HJ'],
      ['A', 1, 'HJ'],
      ['A', 2, 'HJ'],
    ],
    mutations: [
      ['A', 0, 'HJ', [1]],
      ['A', 1, 'HJ', [1]],
      ['A', 1, 'HJ', [1]],
      ['A', 2, 'HJ', [1]],
    ],
    settlements: [
      ['A', 0, 'HJ', 'accept', null, null],
      ['A', 1, 'HJ', 'accept', null, null],
      ['A', 2, 'HJ', 'accept', null, null],
      ['A', 1, 'HJ', 'accept', 'rejected-by-broker', -1],
    ],
  },
  incrementalRetry: {
    handlers: [
      ['A', 0, 'HJ'],
      ['A', 1, 'HJ'],
      ['A', 1, 'HJ'],
      ['A', 2, 'HJ'],
    ],
    mutations: [
      ['A', 0, 'HJ', [1]],
      ['A', 1, 'HJ', [1]],
      ['A', 1, 'HJ', [1]],
      ['A', 2, 'HJ', [1]],
    ],
    settlements: [
      ['A', 0, 'HJ', 'accept', null, null],
      ['A', 1, 'HJ', 'accept', null, null],
      ['A', 2, 'HJ', 'accept', null, null],
      ['A', 1, 'HJ', 'accept', 'rejected-by-broker', 7],
    ],
  },
  incrementalTerminal35: {
    handlers: [
      ['A', 0, 'H'],
      ['A', 1, 'HJ'],
      ['A', 1, 'HJ'],
      ['A', 2, 'HJ'],
      ['A', 0, 'HJ'],
    ],
    mutations: [
      ['A', 0, 'HJ', [1]],
      ['A', 1, 'HJ', [1]],
      ['A', 1, 'HJ', [1]],
      ['A', 2, 'HJ', [1]],
      ['A', 0, 'J', [2]],
    ],
    settlements: [
      ['A', 1, 'HJ', 'accept', null, null],
      ['A', 2, 'HJ', 'accept', null, null],
      ['A', 1, 'HJ', 'accept', 'rejected-by-broker', 35],
      ['A', 0, 'HJ', 'accept', null, null],
      ['A', 0, 'H', 'release', 'membership-lost', null],
      ['A', 0, 'J', 'release', null, null],
    ],
  },
  incrementalTerminal30: {
    handlers: [
      ['A', 0, 'H'],
      ['A', 1, 'HJ'],
      ['A', 1, 'HJ'],
      ['A', 2, 'HJ'],
      ['A', 0, 'HJ'],
    ],
    mutations: [
      ['A', 0, 'HJ', [1]],
      ['A', 1, 'HJ', [1]],
      ['A', 1, 'HJ', [1]],
      ['A', 2, 'HJ', [1]],
      ['A', 0, 'J', [2]],
    ],
    settlements: [
      ['A', 1, 'HJ', 'accept', null, null],
      ['A', 2, 'HJ', 'accept', null, null],
      ['A', 1, 'HJ', 'accept', 'rejected-by-broker', 30],
      ['A', 0, 'HJ', 'accept', null, null],
      ['A', 0, 'H', 'release', 'membership-lost', null],
      ['A', 0, 'J', 'release', null, null],
    ],
  },
  incrementalTerminal25: {
    handlers: [
      ['A', 0, 'H'],
      ['A', 1, 'HJ'],
      ['A', 1, 'HJ'],
      ['A', 2, 'HJ'],
      ['A', 0, 'HJ'],
    ],
    mutations: [
      ['A', 0, 'HJ', [1]],
      ['A', 1, 'HJ', [1]],
      ['A', 1, 'HJ', [1]],
      ['A', 2, 'HJ', [1]],
      ['A', 0, 'J', [2]],
    ],
    settlements: [
      ['A', 1, 'HJ', 'accept', null, null],
      ['A', 2, 'HJ', 'accept', null, null],
      ['A', 1, 'HJ', 'accept', 'rejected-by-broker', 25],
      ['A', 0, 'HJ', 'accept', null, null],
      ['A', 0, 'H', 'release', 'membership-lost', null],
      ['A', 0, 'J', 'release', null, null],
    ],
  },
  incrementalTerminal42: {
    handlers: [
      ['A', 0, 'H'],
      ['A', 1, 'HJ'],
      ['A', 1, 'HJ'],
      ['A', 2, 'HJ'],
      ['A', 0, 'HJ'],
    ],
    mutations: [
      ['A', 0, 'HJ', [1]],
      ['A', 1, 'HJ', [1]],
      ['A', 1, 'HJ', [1]],
      ['A', 2, 'HJ', [1]],
      ['A', 0, 'J', [2]],
    ],
    settlements: [
      ['A', 1, 'HJ', 'accept', null, null],
      ['A', 2, 'HJ', 'accept', null, null],
      ['A', 1, 'HJ', 'accept', 'rejected-by-broker', 42],
      ['A', 0, 'HJ', 'accept', null, null],
      ['A', 0, 'H', 'release', 'membership-lost', null],
      ['A', 0, 'J', 'release', null, null],
    ],
  },
  ackRetry133: {
    handlers: [
      ['A', 0, 'HJ'],
      ['A', 1, 'HJ'],
      ['A', 2, 'HJ'],
    ],
    mutations: [
      ['A', 0, 'HJ', [1]],
      ['A', 1, 'HJ', [1]],
      ['A', 2, 'HJ', [1]],
      ['A', 0, 'H', [4]],
      ['A', 0, 'H', [4]],
      ['A', 0, 'H', [4]],
    ],
    settlements: [
      ['A', 0, 'HJ', 'accept', null, null],
      ['A', 1, 'HJ', 'accept', null, null],
      ['A', 2, 'HJ', 'accept', null, null],
      ['A', 0, 'H', 'renew', null, null],
      ['A', 0, 'H', 'renew', 'rejected-by-broker', 133],
      ['A', 0, 'H', 'renew', null, null],
    ],
  },
  ackUnknown: {
    handlers: [
      ['A', 0, 'HJ'],
      ['A', 1, 'HJ'],
      ['A', 2, 'HJ'],
    ],
    mutations: [
      ['A', 0, 'HJ', [1]],
      ['A', 1, 'HJ', [1]],
      ['A', 2, 'HJ', [1]],
      ['A', 0, 'H', [4]],
      ['A', 0, 'H', [4]],
      ['A', 0, 'H', [4]],
    ],
    settlements: [
      ['A', 0, 'HJ', 'accept', null, null],
      ['A', 1, 'HJ', 'accept', null, null],
      ['A', 2, 'HJ', 'accept', null, null],
      ['A', 0, 'H', 'renew', null, null],
      ['A', 0, 'H', 'renew', 'rejected-by-broker', -1],
      ['A', 0, 'H', 'renew', null, null],
    ],
  },
  ackRetry7: {
    handlers: [
      ['A', 0, 'HJ'],
      ['A', 1, 'HJ'],
      ['A', 2, 'HJ'],
    ],
    mutations: [
      ['A', 0, 'HJ', [1]],
      ['A', 1, 'HJ', [1]],
      ['A', 2, 'HJ', [1]],
      ['A', 0, 'H', [4]],
      ['A', 0, 'H', [4]],
      ['A', 0, 'H', [4]],
    ],
    settlements: [
      ['A', 0, 'HJ', 'accept', null, null],
      ['A', 1, 'HJ', 'accept', null, null],
      ['A', 2, 'HJ', 'accept', null, null],
      ['A', 0, 'H', 'renew', null, null],
      ['A', 0, 'H', 'renew', 'rejected-by-broker', 7],
      ['A', 0, 'H', 'renew', null, null],
    ],
  },
  ackReset: {
    handlers: [
      ['A', 0, 'H'],
      ['A', 1, 'HJ'],
      ['A', 2, 'HJ'],
      ['A', 0, 'HJ'],
    ],
    mutations: [
      ['A', 1, 'HJ', [1]],
      ['A', 2, 'HJ', [1]],
      ['A', 0, 'H', [4]],
      ['A', 0, 'H', [4]],
      ['A', 0, 'HJ', [1]],
    ],
    settlements: [
      ['A', 1, 'HJ', 'accept', null, null],
      ['A', 2, 'HJ', 'accept', null, null],
      ['A', 0, 'H', 'renew', null, null],
      ['A', 0, 'H', 'renew', 'session-lost', null],
      ['A', 0, 'HJ', 'release', 'session-lost', null],
      ['A', 0, 'HJ', 'accept', null, null],
    ],
  },
  ackReset122: {
    handlers: [
      ['A', 0, 'H'],
      ['A', 1, 'HJ'],
      ['A', 2, 'HJ'],
      ['A', 0, 'HJ'],
    ],
    mutations: [
      ['A', 1, 'HJ', [1]],
      ['A', 2, 'HJ', [1]],
      ['A', 0, 'H', [4]],
      ['A', 0, 'H', [4]],
      ['A', 0, 'HJ', [1]],
    ],
    settlements: [
      ['A', 1, 'HJ', 'accept', null, null],
      ['A', 2, 'HJ', 'accept', null, null],
      ['A', 0, 'H', 'renew', null, null],
      ['A', 0, 'H', 'renew', 'session-lost', 122],
      ['A', 0, 'HJ', 'release', 'session-lost', 122],
      ['A', 0, 'HJ', 'accept', null, null],
    ],
  },
  ackReset123: {
    handlers: [
      ['A', 0, 'H'],
      ['A', 1, 'HJ'],
      ['A', 2, 'HJ'],
      ['A', 0, 'HJ'],
    ],
    mutations: [
      ['A', 1, 'HJ', [1]],
      ['A', 2, 'HJ', [1]],
      ['A', 0, 'H', [4]],
      ['A', 0, 'H', [4]],
      ['A', 0, 'HJ', [1]],
    ],
    settlements: [
      ['A', 1, 'HJ', 'accept', null, null],
      ['A', 2, 'HJ', 'accept', null, null],
      ['A', 0, 'H', 'renew', null, null],
      ['A', 0, 'H', 'renew', 'session-lost', 123],
      ['A', 0, 'HJ', 'release', 'session-lost', 123],
      ['A', 0, 'HJ', 'accept', null, null],
    ],
  },
  ackLost6: {
    handlers: [
      ['A', 0, 'HJ'],
      ['A', 1, 'HJ'],
      ['A', 2, 'HJ'],
    ],
    mutations: [
      ['A', 0, 'HJ', [1]],
      ['A', 1, 'HJ', [1]],
      ['A', 2, 'HJ', [1]],
      ['A', 0, 'H', [4]],
      ['A', 0, 'H', [4]],
      ['A', 0, 'H', [4]],
    ],
    settlements: [
      ['A', 0, 'HJ', 'accept', null, null],
      ['A', 1, 'HJ', 'accept', null, null],
      ['A', 2, 'HJ', 'accept', null, null],
      ['A', 0, 'H', 'renew', null, null],
      ['A', 0, 'H', 'renew', 'session-lost', 6],
      ['A', 0, 'H', 'renew', null, null],
    ],
  },
  ackLost35: {
    handlers: [
      ['A', 0, 'H'],
      ['A', 1, 'HJ'],
      ['A', 2, 'HJ'],
      ['A', 0, 'HJ'],
    ],
    mutations: [
      ['A', 1, 'HJ', [1]],
      ['A', 2, 'HJ', [1]],
      ['A', 0, 'H', [4]],
      ['A', 0, 'H', [4]],
      ['A', 0, 'HJ', [1]],
    ],
    settlements: [
      ['A', 1, 'HJ', 'accept', null, null],
      ['A', 2, 'HJ', 'accept', null, null],
      ['A', 0, 'H', 'renew', null, null],
      ['A', 0, 'H', 'renew', 'rejected-by-broker', 35],
      ['A', 0, 'HJ', 'release', 'membership-lost', null],
      ['A', 0, 'HJ', 'accept', null, null],
    ],
  },
  ackLost30: {
    handlers: [
      ['A', 0, 'H'],
      ['A', 1, 'HJ'],
      ['A', 2, 'HJ'],
      ['A', 0, 'HJ'],
    ],
    mutations: [
      ['A', 1, 'HJ', [1]],
      ['A', 2, 'HJ', [1]],
      ['A', 0, 'H', [4]],
      ['A', 0, 'H', [4]],
      ['A', 0, 'HJ', [1]],
    ],
    settlements: [
      ['A', 1, 'HJ', 'accept', null, null],
      ['A', 2, 'HJ', 'accept', null, null],
      ['A', 0, 'H', 'renew', null, null],
      ['A', 0, 'H', 'renew', 'rejected-by-broker', 30],
      ['A', 0, 'HJ', 'release', 'membership-lost', null],
      ['A', 0, 'HJ', 'accept', null, null],
    ],
  },
  ackLost25: {
    handlers: [
      ['A', 0, 'H'],
      ['A', 1, 'HJ'],
      ['A', 2, 'HJ'],
      ['A', 0, 'HJ'],
    ],
    mutations: [
      ['A', 1, 'HJ', [1]],
      ['A', 2, 'HJ', [1]],
      ['A', 0, 'H', [4]],
      ['A', 0, 'H', [4]],
      ['A', 0, 'HJ', [1]],
    ],
    settlements: [
      ['A', 1, 'HJ', 'accept', null, null],
      ['A', 2, 'HJ', 'accept', null, null],
      ['A', 0, 'H', 'renew', null, null],
      ['A', 0, 'H', 'renew', 'rejected-by-broker', 25],
      ['A', 0, 'HJ', 'release', 'membership-lost', null],
      ['A', 0, 'HJ', 'accept', null, null],
    ],
  },
  ackLost42: {
    handlers: [
      ['A', 0, 'H'],
      ['A', 1, 'HJ'],
      ['A', 2, 'HJ'],
      ['A', 0, 'HJ'],
    ],
    mutations: [
      ['A', 1, 'HJ', [1]],
      ['A', 2, 'HJ', [1]],
      ['A', 0, 'H', [4]],
      ['A', 0, 'H', [4]],
      ['A', 0, 'HJ', [1]],
    ],
    settlements: [
      ['A', 1, 'HJ', 'accept', null, null],
      ['A', 2, 'HJ', 'accept', null, null],
      ['A', 0, 'H', 'renew', null, null],
      ['A', 0, 'H', 'renew', 'rejected-by-broker', 42],
      ['A', 0, 'HJ', 'release', 'membership-lost', null],
      ['A', 0, 'HJ', 'accept', null, null],
    ],
  },
  partitionFatal: {
    handlers: [
      ['A', 0, 'H'],
      ['B', 0, 'H'],
    ],
    mutations: [
      ['B', 0, 'H', [1]],
      ['A', 0, 'J', [2]],
    ],
    settlements: [
      ['B', 0, 'H', 'accept', null, null],
      ['A', 0, 'H', 'release', 'membership-lost', null],
      ['A', 0, 'J', 'release', null, null],
    ],
  },
  partitionSiblingFatal: {
    handlers: [
      ['A', 0, 'H'],
      ['B', 0, 'H'],
    ],
    mutations: [
      ['A', 0, 'J', [2]],
      ['B', 0, 'H', [1]],
    ],
    settlements: [
      ['A', 0, 'H', 'release', 'membership-lost', null],
      ['A', 0, 'J', 'release', null, null],
      ['A', 2, 'HJ', 'release', 'membership-lost', null],
      ['B', 0, 'H', 'accept', null, null],
    ],
  },
  incrementalPartitionFatal: {
    handlers: [
      ['A', 0, 'H'],
      ['A', 1, 'HJ'],
      ['A', 1, 'HJ'],
      ['A', 2, 'HJ'],
      ['B', 0, 'H'],
    ],
    mutations: [
      ['A', 1, 'HJ', [1]],
      ['A', 1, 'HJ', [1]],
      ['A', 2, 'HJ', [1]],
      ['B', 0, 'H', [1]],
      ['A', 0, 'J', [2]],
    ],
    settlements: [
      ['A', 1, 'HJ', 'accept', null, null],
      ['A', 1, 'HJ', 'accept', null, null],
      ['A', 2, 'HJ', 'accept', null, null],
      ['B', 0, 'H', 'accept', null, null],
      ['A', 0, 'H', 'release', 'membership-lost', null],
      ['A', 0, 'J', 'release', null, null],
    ],
  },
  fifo: {
    handlers: [['A', 0, 'HJ']],
    mutations: [
      ['A', 0, 'H', [4]],
      ['A', 0, 'H', [1]],
      ['A', 0, 'J', [1]],
    ],
    settlements: [
      ['A', 0, 'H', 'renew', null, null],
      ['A', 0, 'H', 'accept', null, null],
      ['A', 0, 'J', 'accept', null, null],
    ],
  },
  queuedReset: {
    handlers: [
      ['A', 0, 'H'],
      ['A', 1, 'HJ'],
      ['A', 2, 'HJ'],
    ],
    mutations: [
      ['A', 0, 'H', [4]],
      ['A', 1, 'HJ', [1]],
      ['A', 2, 'HJ', [1]],
    ],
    settlements: [
      ['A', 0, 'H', 'renew', 'session-lost', null],
      ['A', 0, 'HJ', 'release', 'session-lost', null],
      ['A', 1, 'HJ', 'accept', null, null],
      ['A', 2, 'HJ', 'accept', null, null],
    ],
  },
  replacementCurrent: {
    handlers: [
      ['A', 0, 'H'],
      ['B', 0, 'H'],
    ],
    mutations: [
      ['A', 0, 'HJ', [1, 2]],
      ['B', 0, 'H', [1]],
    ],
    settlements: [
      ['A', 0, 'H', 'accept', null, null],
      ['A', 0, 'J', 'release', null, null],
      ['B', 0, 'H', 'accept', null, null],
    ],
  },
  replacementFenced: {
    handlers: [
      ['A', 0, 'H'],
      ['B', 0, 'H'],
    ],
    mutations: [
      ['B', 0, 'H', [1]],
      ['A', 0, 'J', [2]],
    ],
    settlements: [
      ['B', 0, 'H', 'accept', null, null],
      ['A', 0, 'H', 'release', 'membership-lost', null],
      ['A', 0, 'J', 'release', null, null],
    ],
  },
  reacquired: {
    handlers: [
      ['A', 0, 'HJ'],
      ['A', 1, 'H'],
      ['A', 1, 'HJ'],
    ],
    mutations: [
      ['A', 0, 'HJ', [1]],
      ['A', 1, 'H', [4]],
      ['A', 1, 'H', [4]],
      ['A', 1, 'HJ', [1]],
    ],
    settlements: [
      ['A', 0, 'HJ', 'accept', null, null],
      ['A', 1, 'H', 'renew', null, null],
      ['A', 1, 'H', 'renew', 'session-lost', 122],
      ['A', 1, 'HJ', 'release', 'session-lost', 122],
      ['A', 1, 'HJ', 'accept', null, null],
    ],
  },
  forget: {
    handlers: [
      ['A', 0, 'HJ'],
      ['A', 1, 'HJ'],
    ],
    mutations: [
      ['A', 0, 'HJ', [1]],
      ['A', 1, 'HJ', [1]],
      ['A', 2, 'HJ', [2]],
    ],
    settlements: [
      ['A', 0, 'HJ', 'accept', null, null],
      ['A', 1, 'HJ', 'accept', null, null],
      ['A', 2, 'HJ', 'release', null, null],
    ],
  },
  flushForget: {
    handlers: [
      ['A', 0, 'HJ'],
      ['A', 1, 'HJ'],
    ],
    mutations: [
      ['A', 0, 'H', [1]],
      ['A', 0, 'J', [1]],
      ['A', 1, 'H', [1]],
      ['A', 1, 'J', [1]],
      ['A', 2, 'HJ', [2]],
    ],
    settlements: [
      ['A', 0, 'H', 'accept', null, null],
      ['A', 0, 'J', 'accept', null, null],
      ['A', 1, 'H', 'accept', null, null],
      ['A', 1, 'J', 'accept', null, null],
      ['A', 2, 'HJ', 'release', null, null],
    ],
  },
  renewedForget: {
    handlers: [
      ['A', 0, 'HJ'],
      ['A', 1, 'HJ'],
      ['A', 2, 'H'],
    ],
    mutations: [
      ['A', 0, 'HJ', [1]],
      ['A', 1, 'HJ', [1]],
      ['A', 2, 'H', [4]],
      ['A', 2, 'HJ', [1, 2]],
    ],
    settlements: [
      ['A', 0, 'HJ', 'accept', null, null],
      ['A', 1, 'HJ', 'accept', null, null],
      ['A', 2, 'H', 'renew', null, null],
      ['A', 2, 'H', 'accept', null, null],
      ['A', 2, 'J', 'release', null, null],
    ],
  },
}

/**
 * Expands immutable UUID/offset notation without consulting a client, response or observation.
 * @param {object} literal Complete literal handler, mutation and settlement tuple arrays.
 * @returns {object} The shared audit's observation schema, retaining every duplicate.
 */
const literalObservations = (literal) => {
  const offsets = { H: [H, H], J: [J, J], HJ: [H, J] }
  return {
    handlers: literal.handlers.flatMap(([alias, partition, range]) =>
      (range === 'HJ' ? [H, J] : [offsets[range][0]]).map((offset) => ({
        topicId: IDS[alias],
        partition,
        offset,
        key: '6b',
        value: '76',
        deliveryCount: 2,
      }))
    ),
    mutations: literal.mutations.map(([alias, partition, range, acknowledgeTypes]) => ({
      topicId: IDS[alias],
      partition,
      firstOffset: offsets[range][0],
      lastOffset: offsets[range][1],
      acknowledgeTypes,
    })),
    settlements: literal.settlements.map(([alias, partition, range, type, reason, code]) => ({
      topicId: IDS[alias],
      partition,
      firstOffset: offsets[range][0],
      lastOffset: offsets[range][1],
      type,
      reason,
      code,
    })),
  }
}

const ROW_OBSERVATIONS = [
  OBSERVATIONS.accepted,
  OBSERVATIONS.accepted,
  OBSERVATIONS.accepted,
  OBSERVATIONS.accepted,
  OBSERVATIONS.accepted,
  OBSERVATIONS.accepted,
  OBSERVATIONS.accepted,
  OBSERVATIONS.openTerminal,
  OBSERVATIONS.openTerminal,
  OBSERVATIONS.openTerminal,
  OBSERVATIONS.accepted,
  OBSERVATIONS.openTerminal,
  OBSERVATIONS.incrementalLost122,
  OBSERVATIONS.incrementalLost123,
  OBSERVATIONS.incrementalLost133,
  OBSERVATIONS.incrementalLost,
  OBSERVATIONS.incrementalLost,
  OBSERVATIONS.incrementalUnknown,
  OBSERVATIONS.incrementalAccepted,
  OBSERVATIONS.incrementalTerminal35,
  OBSERVATIONS.incrementalTerminal30,
  OBSERVATIONS.incrementalTerminal25,
  OBSERVATIONS.incrementalRetry,
  OBSERVATIONS.incrementalTerminal42,
  OBSERVATIONS.ackReset122,
  OBSERVATIONS.ackReset123,
  OBSERVATIONS.ackRetry133,
  OBSERVATIONS.ackReset,
  OBSERVATIONS.ackReset,
  OBSERVATIONS.ackUnknown,
  OBSERVATIONS.ackLost6,
  OBSERVATIONS.ackLost35,
  OBSERVATIONS.ackLost30,
  OBSERVATIONS.ackLost25,
  OBSERVATIONS.ackRetry7,
  OBSERVATIONS.ackLost42,
  OBSERVATIONS.accepted,
  OBSERVATIONS.partitionSiblingFatal,
  OBSERVATIONS.accepted,
  OBSERVATIONS.accepted,
  OBSERVATIONS.accepted,
  OBSERVATIONS.recreated,
  OBSERVATIONS.accepted,
  OBSERVATIONS.accepted,
  OBSERVATIONS.accepted,
  OBSERVATIONS.partitionFatal,
  OBSERVATIONS.accepted,
  OBSERVATIONS.partitionFatal,
  OBSERVATIONS.accepted,
  OBSERVATIONS.partitionFatal,
  OBSERVATIONS.incrementalAccepted,
  OBSERVATIONS.incrementalAccepted,
  OBSERVATIONS.incrementalAccepted,
  OBSERVATIONS.incrementalRecreated,
  OBSERVATIONS.incrementalAccepted,
  OBSERVATIONS.incrementalAccepted,
  OBSERVATIONS.incrementalAccepted,
  OBSERVATIONS.incrementalPartitionFatal,
  OBSERVATIONS.incrementalAccepted,
  OBSERVATIONS.incrementalPartitionFatal,
  OBSERVATIONS.incrementalAccepted,
  OBSERVATIONS.incrementalPartitionFatal,
  OBSERVATIONS.accepted,
  OBSERVATIONS.accepted,
  OBSERVATIONS.a0,
  OBSERVATIONS.fifo,
  OBSERVATIONS.queuedReset,
  OBSERVATIONS.empty,
  OBSERVATIONS.empty,
  OBSERVATIONS.a0,
  OBSERVATIONS.a0,
  OBSERVATIONS.replacementCurrent,
  OBSERVATIONS.reacquired,
  OBSERVATIONS.a0,
  OBSERVATIONS.a0,
  OBSERVATIONS.forget,
  OBSERVATIONS.forget,
  OBSERVATIONS.flushForget,
  OBSERVATIONS.renewedForget,
  OBSERVATIONS.a0,
  OBSERVATIONS.capacity,
  OBSERVATIONS.empty,
  OBSERVATIONS.replacementCurrent,
  OBSERVATIONS.replacementCurrent,
  OBSERVATIONS.replacementFenced,
  OBSERVATIONS.replacementFenced,
  OBSERVATIONS.replacementCurrent,
  OBSERVATIONS.replacementCurrent,
  OBSERVATIONS.replacementFenced,
  OBSERVATIONS.replacementFenced,
  OBSERVATIONS.replacementCurrent,
  OBSERVATIONS.replacementCurrent,
  OBSERVATIONS.replacementFenced,
  OBSERVATIONS.replacementFenced,
  OBSERVATIONS.replacementCurrent,
  OBSERVATIONS.replacementCurrent,
  OBSERVATIONS.replacementFenced,
  OBSERVATIONS.replacementFenced,
].map(literalObservations)

/** Literal fault, phase and outcome rows; the runtime driver must not mutate this oracle. */
const rows = [
  {
    fault: 'top 122',
    step: 'open fetch',
    expected: {
      action: 'reset',
      state: 'STABLE',
      rounds: 8,
      nextEpoch: 0,
      held: 'session-lost',
      request: 'session-lost',
      backoff: 'first immediate then delayed',
      sameOpen: false,
      oldAckResends: 0,
    },
  },
  {
    fault: 'top 123',
    step: 'open fetch',
    expected: {
      action: 'reset',
      state: 'STABLE',
      rounds: 8,
      nextEpoch: 0,
      held: 'session-lost',
      request: 'session-lost',
      backoff: 'first immediate then delayed',
      sameOpen: false,
      oldAckResends: 0,
    },
  },
  {
    fault: 'top 133',
    step: 'open fetch',
    expected: {
      action: 'reset',
      state: 'STABLE',
      rounds: 8,
      nextEpoch: 0,
      held: 'session-lost',
      request: 'session-lost',
      backoff: 'always',
      sameOpen: false,
      oldAckResends: 0,
    },
  },
  {
    fault: 'transport disconnect',
    step: 'open fetch',
    expected: {
      action: 'reset',
      state: 'STABLE',
      rounds: 8,
      nextEpoch: 0,
      held: 'session-lost',
      request: 'session-lost',
      backoff: 'transport',
      sameOpen: false,
      oldAckResends: 0,
    },
  },
  {
    fault: 'transport timeout',
    step: 'open fetch',
    expected: {
      action: 'reset',
      state: 'STABLE',
      rounds: 8,
      nextEpoch: 0,
      held: 'session-lost',
      request: 'session-lost',
      backoff: 'transport',
      sameOpen: false,
      oldAckResends: 0,
    },
  },
  {
    fault: 'top -1',
    step: 'open fetch',
    expected: {
      action: 'retry',
      state: 'STABLE',
      rounds: 8,
      nextEpoch: 1,
      held: 'current',
      request: 'rejected-by-broker',
      backoff: 'retry',
      sameOpen: true,
      oldAckResends: 0,
    },
  },
  {
    fault: 'partition 6',
    step: 'open fetch',
    expected: {
      action: 'move',
      state: 'STABLE',
      rounds: 8,
      nextEpoch: 0,
      held: 'session-lost',
      request: 'session-lost',
      backoff: 'none',
      sameOpen: false,
      oldAckResends: 0,
    },
  },
  {
    fault: 'top 35',
    step: 'open fetch',
    expected: {
      action: 'fatal',
      state: 'FATAL',
      rounds: 8,
      nextEpoch: 1,
      held: 'membership-lost',
      request: 'rejected-by-broker',
      backoff: 'none',
      sameOpen: false,
      oldAckResends: 0,
    },
  },
  {
    fault: 'top 30',
    step: 'open fetch',
    expected: {
      action: 'fatal',
      state: 'FATAL',
      rounds: 8,
      nextEpoch: 1,
      held: 'membership-lost',
      request: 'rejected-by-broker',
      backoff: 'none',
      sameOpen: false,
      oldAckResends: 0,
    },
  },
  {
    fault: 'top 25',
    step: 'open fetch',
    expected: {
      action: 'fence',
      state: 'STABLE',
      rounds: 8,
      nextEpoch: 0,
      held: 'membership-lost',
      request: 'session-lost',
      backoff: 'none',
      sameOpen: false,
      oldAckResends: 0,
    },
  },
  {
    fault: 'top 7',
    step: 'open fetch',
    expected: {
      action: 'retry',
      state: 'STABLE',
      rounds: 8,
      nextEpoch: 1,
      held: 'current',
      request: 'rejected-by-broker',
      backoff: 'retry',
      sameOpen: true,
      oldAckResends: 0,
    },
  },
  {
    fault: 'top 42',
    step: 'open fetch',
    expected: {
      action: 'fatal',
      state: 'FATAL',
      rounds: 8,
      nextEpoch: 1,
      held: 'membership-lost',
      request: 'rejected-by-broker',
      backoff: 'none',
      sameOpen: false,
      oldAckResends: 0,
    },
  },
  {
    fault: 'top 122',
    step: 'incremental fetch',
    expected: {
      action: 'reset',
      state: 'STABLE',
      rounds: 8,
      nextEpoch: 0,
      held: 'session-lost',
      request: 'session-lost',
      backoff: 'first immediate then delayed',
      sameOpen: false,
      oldAckResends: 0,
    },
  },
  {
    fault: 'top 123',
    step: 'incremental fetch',
    expected: {
      action: 'reset',
      state: 'STABLE',
      rounds: 8,
      nextEpoch: 0,
      held: 'session-lost',
      request: 'session-lost',
      backoff: 'first immediate then delayed',
      sameOpen: false,
      oldAckResends: 0,
    },
  },
  {
    fault: 'top 133',
    step: 'incremental fetch',
    expected: {
      action: 'reset',
      state: 'STABLE',
      rounds: 8,
      nextEpoch: 0,
      held: 'session-lost',
      request: 'session-lost',
      backoff: 'always',
      sameOpen: false,
      oldAckResends: 0,
    },
  },
  {
    fault: 'transport disconnect',
    step: 'incremental fetch',
    expected: {
      action: 'reset',
      state: 'STABLE',
      rounds: 8,
      nextEpoch: 0,
      held: 'session-lost',
      request: 'session-lost',
      backoff: 'transport',
      sameOpen: false,
      oldAckResends: 0,
    },
  },
  {
    fault: 'transport timeout',
    step: 'incremental fetch',
    expected: {
      action: 'reset',
      state: 'STABLE',
      rounds: 8,
      nextEpoch: 0,
      held: 'session-lost',
      request: 'session-lost',
      backoff: 'transport',
      sameOpen: false,
      oldAckResends: 0,
    },
  },
  {
    fault: 'top -1',
    step: 'incremental fetch',
    expected: {
      action: 'retry',
      state: 'STABLE',
      rounds: 8,
      nextEpoch: 3,
      held: 'current',
      request: 'rejected-by-broker',
      backoff: 'retry',
      sameOpen: true,
      oldAckResends: 0,
    },
  },
  {
    fault: 'partition 6',
    step: 'incremental fetch',
    expected: {
      action: 'move',
      state: 'STABLE',
      rounds: 8,
      nextEpoch: 0,
      held: 'session-lost',
      request: 'accepted',
      backoff: 'none',
      sameOpen: false,
      oldAckResends: 0,
    },
  },
  {
    fault: 'top 35',
    step: 'incremental fetch',
    expected: {
      action: 'fatal',
      state: 'FATAL',
      rounds: 8,
      nextEpoch: 3,
      held: 'membership-lost',
      request: 'rejected-by-broker',
      backoff: 'none',
      sameOpen: false,
      oldAckResends: 0,
    },
  },
  {
    fault: 'top 30',
    step: 'incremental fetch',
    expected: {
      action: 'fatal',
      state: 'FATAL',
      rounds: 8,
      nextEpoch: 3,
      held: 'membership-lost',
      request: 'rejected-by-broker',
      backoff: 'none',
      sameOpen: false,
      oldAckResends: 0,
    },
  },
  {
    fault: 'top 25',
    step: 'incremental fetch',
    expected: {
      action: 'fence',
      state: 'STABLE',
      rounds: 8,
      nextEpoch: 0,
      held: 'membership-lost',
      request: 'rejected-by-broker',
      backoff: 'none',
      sameOpen: false,
      oldAckResends: 0,
    },
  },
  {
    fault: 'top 7',
    step: 'incremental fetch',
    expected: {
      action: 'retry',
      state: 'STABLE',
      rounds: 8,
      nextEpoch: 3,
      held: 'current',
      request: 'rejected-by-broker',
      backoff: 'retry',
      sameOpen: true,
      oldAckResends: 0,
    },
  },
  {
    fault: 'top 42',
    step: 'incremental fetch',
    expected: {
      action: 'fatal',
      state: 'FATAL',
      rounds: 8,
      nextEpoch: 3,
      held: 'membership-lost',
      request: 'rejected-by-broker',
      backoff: 'none',
      sameOpen: false,
      oldAckResends: 0,
    },
  },
  {
    fault: 'top 122',
    step: 'acknowledgement only',
    expected: {
      action: 'reset',
      state: 'STABLE',
      rounds: 8,
      nextEpoch: 0,
      held: 'session-lost',
      request: 'session-lost',
      backoff: 'first immediate then delayed',
      sameOpen: false,
      oldAckResends: 0,
    },
  },
  {
    fault: 'top 123',
    step: 'acknowledgement only',
    expected: {
      action: 'reset',
      state: 'STABLE',
      rounds: 8,
      nextEpoch: 0,
      held: 'session-lost',
      request: 'session-lost',
      backoff: 'first immediate then delayed',
      sameOpen: false,
      oldAckResends: 0,
    },
  },
  {
    fault: 'top 133',
    step: 'acknowledgement only',
    expected: {
      action: 'retry',
      state: 'STABLE',
      rounds: 8,
      nextEpoch: 3,
      held: 'current',
      request: 'rejected-by-broker',
      backoff: 'always',
      sameOpen: true,
      oldAckResends: 0,
    },
  },
  {
    fault: 'transport disconnect',
    step: 'acknowledgement only',
    expected: {
      action: 'reset',
      state: 'STABLE',
      rounds: 8,
      nextEpoch: 0,
      held: 'session-lost',
      request: 'session-lost',
      backoff: 'transport',
      sameOpen: false,
      oldAckResends: 0,
    },
  },
  {
    fault: 'transport timeout',
    step: 'acknowledgement only',
    expected: {
      action: 'reset',
      state: 'STABLE',
      rounds: 8,
      nextEpoch: 0,
      held: 'session-lost',
      request: 'session-lost',
      backoff: 'transport',
      sameOpen: false,
      oldAckResends: 0,
    },
  },
  {
    fault: 'top -1',
    step: 'acknowledgement only',
    expected: {
      action: 'retry',
      state: 'STABLE',
      rounds: 8,
      nextEpoch: 3,
      held: 'current',
      request: 'rejected-by-broker',
      backoff: 'retry',
      sameOpen: true,
      oldAckResends: 0,
    },
  },
  {
    fault: 'partition 6',
    step: 'acknowledgement only',
    expected: {
      action: 'session-lost',
      state: 'STABLE',
      rounds: 8,
      nextEpoch: 3,
      held: 'current',
      request: 'session-lost',
      backoff: 'none',
      sameOpen: true,
      oldAckResends: 0,
    },
  },
  {
    fault: 'top 35',
    step: 'acknowledgement only',
    expected: {
      action: 'fatal',
      state: 'FATAL',
      rounds: 8,
      nextEpoch: 3,
      held: 'membership-lost',
      request: 'rejected-by-broker',
      backoff: 'none',
      sameOpen: false,
      oldAckResends: 0,
    },
  },
  {
    fault: 'top 30',
    step: 'acknowledgement only',
    expected: {
      action: 'fatal',
      state: 'FATAL',
      rounds: 8,
      nextEpoch: 3,
      held: 'membership-lost',
      request: 'rejected-by-broker',
      backoff: 'none',
      sameOpen: false,
      oldAckResends: 0,
    },
  },
  {
    fault: 'top 25',
    step: 'acknowledgement only',
    expected: {
      action: 'fence',
      state: 'STABLE',
      rounds: 8,
      nextEpoch: 0,
      held: 'membership-lost',
      request: 'rejected-by-broker',
      backoff: 'none',
      sameOpen: false,
      oldAckResends: 0,
    },
  },
  {
    fault: 'top 7',
    step: 'acknowledgement only',
    expected: {
      action: 'retry',
      state: 'STABLE',
      rounds: 8,
      nextEpoch: 3,
      held: 'current',
      request: 'rejected-by-broker',
      backoff: 'retry',
      sameOpen: true,
      oldAckResends: 0,
    },
  },
  {
    fault: 'top 42',
    step: 'acknowledgement only',
    expected: {
      action: 'fatal',
      state: 'FATAL',
      rounds: 8,
      nextEpoch: 3,
      held: 'membership-lost',
      request: 'rejected-by-broker',
      backoff: 'none',
      sameOpen: false,
      oldAckResends: 0,
    },
  },
  {
    fault: 'top -1 deleted topic',
    step: 'open fetch',
    expected: {
      action: 'retry',
      state: 'STABLE',
      rounds: 8,
      nextEpoch: 1,
      held: 'current',
      request: 'none',
      backoff: 'retry',
      sameOpen: true,
      oldAckResends: 0,
    },
  },
  {
    fault: 'partition 29 with sibling acquisition',
    step: 'open fetch',
    expected: {
      action: 'fatal',
      state: 'FATAL',
      rounds: 8,
      affected: ['A:1@9007199254740993'],
      sibling: ['A:2@9007199254740993', 'A:2@9007199254740994'],
      held: 'membership-lost',
      cause: 29,
      settled: true,
      oldAckResends: 0,
    },
  },
  {
    fault: 'partition 74',
    step: 'open fetch',
    expected: {
      action: 'move',
      state: 'STABLE',
      rounds: 8,
      affected: ['A:1@9007199254740993'],
      sibling: ['A:0@9007199254740993'],
      nextNode: 2,
      held: 'session-lost',
      cause: 74,
      oldAckResends: 0,
    },
  },
  {
    fault: 'partition 3',
    step: 'open fetch',
    expected: {
      action: 'refresh',
      state: 'STABLE',
      rounds: 8,
      affected: ['A:1@9007199254740993'],
      sibling: ['A:0@9007199254740993'],
      nextNode: 1,
      held: 'current',
      cause: 3,
      oldAckResends: 0,
    },
  },
  {
    fault: 'partition 100',
    step: 'open fetch',
    expected: {
      action: 'refresh',
      state: 'STABLE',
      rounds: 8,
      affected: ['A:1@9007199254740993'],
      sibling: ['A:0@9007199254740993'],
      nextNode: 1,
      held: 'current',
      cause: 100,
      oldAckResends: 0,
    },
  },
  {
    fault: 'partition 103',
    step: 'open fetch',
    expected: {
      action: 'recreate',
      state: 'STABLE',
      rounds: 8,
      affected: ['A:1@9007199254740993'],
      sibling: ['A:0@9007199254740993'],
      nextNode: 1,
      held: 'session-lost',
      cause: 103,
      oldAckResends: 0,
    },
  },
  {
    fault: 'partition 9',
    step: 'open fetch',
    expected: {
      action: 'refresh',
      state: 'STABLE',
      rounds: 8,
      affected: ['A:1@9007199254740993'],
      sibling: ['A:0@9007199254740993'],
      nextNode: 1,
      held: 'current',
      cause: 9,
      oldAckResends: 0,
    },
  },
  {
    fault: 'partition 56',
    step: 'open fetch',
    expected: {
      action: 'refresh',
      state: 'STABLE',
      rounds: 8,
      affected: ['A:1@9007199254740993'],
      sibling: ['A:0@9007199254740993'],
      nextNode: 1,
      held: 'current',
      cause: 56,
      oldAckResends: 0,
    },
  },
  {
    fault: 'partition 78',
    step: 'open fetch',
    expected: {
      action: 'refresh',
      state: 'STABLE',
      rounds: 8,
      affected: ['A:1@9007199254740993'],
      sibling: ['A:0@9007199254740993'],
      nextNode: 1,
      held: 'current',
      cause: 78,
      oldAckResends: 0,
    },
  },
  {
    fault: 'partition 29',
    step: 'open fetch',
    expected: {
      action: 'fatal',
      state: 'FATAL',
      rounds: 8,
      affected: ['A:1@9007199254740993'],
      sibling: ['A:0@9007199254740993'],
      nextNode: 1,
      held: 'membership-lost',
      cause: 29,
      oldAckResends: 0,
    },
  },
  {
    fault: 'partition -1',
    step: 'open fetch',
    expected: {
      action: 'retry partition',
      state: 'STABLE',
      rounds: 8,
      affected: ['A:1@9007199254740993'],
      sibling: ['A:0@9007199254740993'],
      nextNode: 1,
      held: 'current',
      cause: -1,
      oldAckResends: 0,
    },
  },
  {
    fault: 'partition 2',
    step: 'open fetch',
    expected: {
      action: 'fatal protocol',
      state: 'FATAL',
      rounds: 8,
      affected: ['A:1@9007199254740993'],
      sibling: ['A:0@9007199254740993'],
      nextNode: 1,
      held: 'membership-lost',
      cause: 2,
      oldAckResends: 0,
    },
  },
  {
    fault: 'partition 7',
    step: 'open fetch',
    expected: {
      action: 'refresh',
      state: 'STABLE',
      rounds: 8,
      affected: ['A:1@9007199254740993'],
      sibling: ['A:0@9007199254740993'],
      nextNode: 1,
      held: 'current',
      cause: 7,
      oldAckResends: 0,
    },
  },
  {
    fault: 'partition 42',
    step: 'open fetch',
    expected: {
      action: 'fatal',
      state: 'FATAL',
      rounds: 8,
      affected: ['A:1@9007199254740993'],
      sibling: ['A:0@9007199254740993'],
      nextNode: 1,
      held: 'membership-lost',
      cause: 42,
      oldAckResends: 0,
    },
  },
  {
    fault: 'partition 74',
    step: 'incremental fetch',
    expected: {
      action: 'move',
      state: 'STABLE',
      rounds: 8,
      affected: ['A:1@9007199254740993'],
      sibling: ['A:0@9007199254740993'],
      nextNode: 2,
      held: 'session-lost',
      cause: 74,
      oldAckResends: 0,
    },
  },
  {
    fault: 'partition 3',
    step: 'incremental fetch',
    expected: {
      action: 'refresh',
      state: 'STABLE',
      rounds: 8,
      affected: ['A:1@9007199254740993'],
      sibling: ['A:0@9007199254740993'],
      nextNode: 1,
      held: 'current',
      cause: 3,
      oldAckResends: 0,
    },
  },
  {
    fault: 'partition 100',
    step: 'incremental fetch',
    expected: {
      action: 'refresh',
      state: 'STABLE',
      rounds: 8,
      affected: ['A:1@9007199254740993'],
      sibling: ['A:0@9007199254740993'],
      nextNode: 1,
      held: 'current',
      cause: 100,
      oldAckResends: 0,
    },
  },
  {
    fault: 'partition 103',
    step: 'incremental fetch',
    expected: {
      action: 'recreate',
      state: 'STABLE',
      rounds: 8,
      affected: ['A:1@9007199254740993'],
      sibling: ['A:0@9007199254740993'],
      nextNode: 1,
      held: 'session-lost',
      cause: 103,
      oldAckResends: 0,
    },
  },
  {
    fault: 'partition 9',
    step: 'incremental fetch',
    expected: {
      action: 'refresh',
      state: 'STABLE',
      rounds: 8,
      affected: ['A:1@9007199254740993'],
      sibling: ['A:0@9007199254740993'],
      nextNode: 1,
      held: 'current',
      cause: 9,
      oldAckResends: 0,
    },
  },
  {
    fault: 'partition 56',
    step: 'incremental fetch',
    expected: {
      action: 'refresh',
      state: 'STABLE',
      rounds: 8,
      affected: ['A:1@9007199254740993'],
      sibling: ['A:0@9007199254740993'],
      nextNode: 1,
      held: 'current',
      cause: 56,
      oldAckResends: 0,
    },
  },
  {
    fault: 'partition 78',
    step: 'incremental fetch',
    expected: {
      action: 'refresh',
      state: 'STABLE',
      rounds: 8,
      affected: ['A:1@9007199254740993'],
      sibling: ['A:0@9007199254740993'],
      nextNode: 1,
      held: 'current',
      cause: 78,
      oldAckResends: 0,
    },
  },
  {
    fault: 'partition 29',
    step: 'incremental fetch',
    expected: {
      action: 'fatal',
      state: 'FATAL',
      rounds: 8,
      affected: ['A:1@9007199254740993'],
      sibling: ['A:0@9007199254740993'],
      nextNode: 1,
      held: 'membership-lost',
      cause: 29,
      oldAckResends: 0,
    },
  },
  {
    fault: 'partition -1',
    step: 'incremental fetch',
    expected: {
      action: 'retry partition',
      state: 'STABLE',
      rounds: 8,
      affected: ['A:1@9007199254740993'],
      sibling: ['A:0@9007199254740993'],
      nextNode: 1,
      held: 'current',
      cause: -1,
      oldAckResends: 0,
    },
  },
  {
    fault: 'partition 2',
    step: 'incremental fetch',
    expected: {
      action: 'fatal protocol',
      state: 'FATAL',
      rounds: 8,
      affected: ['A:1@9007199254740993'],
      sibling: ['A:0@9007199254740993'],
      nextNode: 1,
      held: 'membership-lost',
      cause: 2,
      oldAckResends: 0,
    },
  },
  {
    fault: 'partition 7',
    step: 'incremental fetch',
    expected: {
      action: 'refresh',
      state: 'STABLE',
      rounds: 8,
      affected: ['A:1@9007199254740993'],
      sibling: ['A:0@9007199254740993'],
      nextNode: 1,
      held: 'current',
      cause: 7,
      oldAckResends: 0,
    },
  },
  {
    fault: 'partition 42',
    step: 'incremental fetch',
    expected: {
      action: 'fatal',
      state: 'FATAL',
      rounds: 8,
      affected: ['A:1@9007199254740993'],
      sibling: ['A:0@9007199254740993'],
      nextNode: 1,
      held: 'membership-lost',
      cause: 42,
      oldAckResends: 0,
    },
  },
  {
    fault: 'opening lists all wanted without acks',
    step: 'initial open',
    expected: {
      action: 'mechanics',
      state: 'STABLE',
      rounds: 8,
      epochs: [0, 1, 2],
      topics: ['A:0', 'A:1', 'A:2'],
      acks: [],
    },
  },
  {
    fault: 'epoch changes on answer only',
    step: 'held answer',
    expected: {
      action: 'mechanics',
      state: 'STABLE',
      rounds: 8,
      epochs: [0, 1, 2],
      inFlight: 1,
    },
  },
  {
    fault: 'epoch wrap',
    step: 'incremental fetch',
    expected: {
      action: 'mechanics',
      state: 'STABLE',
      rounds: 8,
      epochs: [2147483647, 1, 2],
      distinct: true,
    },
  },
  {
    fault: 'held answer blocks renewal and flush',
    step: 'queue',
    expected: {
      action: 'mechanics',
      state: 'STABLE',
      rounds: 8,
      order: ['fetch', 'renew', 'flush'],
      epochs: [1, 2, 3],
      inFlight: 1,
    },
  },
  {
    fault: 'queued reset rejects before held answer',
    step: 'queue',
    expected: {
      action: 'mechanics',
      state: 'STABLE',
      rounds: 8,
      queued: 'rejected',
      builderCalls: 0,
      oldAckResends: 0,
    },
  },
  {
    fault: 'fetch limit always delays',
    step: 'five second window',
    expected: {
      action: 'mechanics',
      state: 'STABLE',
      rounds: 24,
      maxFrames: 20,
      minGap: 250,
      window: [0, 5000],
      times: [
        0, 250, 500, 750, 1000, 1250, 1500, 1750, 2000, 2250, 2500, 2750, 3000, 3250, 3500, 3750,
        4000, 4250, 4500, 4750, 5000,
      ],
    },
  },
  {
    fault: 'consecutive resets delay then success clears streak',
    step: 'incremental fetch',
    expected: {
      action: 'mechanics',
      state: 'STABLE',
      rounds: 8,
      backoff: [false, true, false],
      epochs: [1, 0, 0, 1, 0],
    },
  },
  {
    fault: 'slot zero reconnect keeps data session',
    step: 'connection',
    expected: {
      action: 'mechanics',
      state: 'STABLE',
      rounds: 8,
      open: 'same',
      dataGeneration: 'same',
      epochs: [1, 2],
    },
  },
  {
    fault: 'slot one disconnect resets before counter changes',
    step: 'connection',
    expected: {
      action: 'mechanics',
      state: 'STABLE',
      rounds: 8,
      open: 'distinct',
      epochs: [2, 0],
      oldAckResends: 0,
    },
  },
  {
    fault: 'same generation different pool refuses old work',
    step: 'replacement',
    expected: {
      action: 'mechanics',
      state: 'STABLE',
      rounds: 8,
      oldWrites: 0,
      replacement: ['B:0@9007199254740993'],
    },
  },
  {
    fault: 'same offset clock epoch different acquisition',
    step: 'replacement',
    expected: {
      action: 'mechanics',
      state: 'STABLE',
      rounds: 8,
      oldEffects: 0,
      distinct: true,
      replacement: ['A:1@9007199254740993'],
    },
  },
  {
    fault: 'last unwanted established node closes once',
    step: 'empty assignment',
    expected: {
      action: 'mechanics',
      state: 'UNSUBSCRIBED',
      rounds: 8,
      epochs: [-1],
      api: 79,
      finals: 1,
      reopen: 0,
    },
  },
  {
    fault: 'last unwanted unopened node sends nothing',
    step: 'empty assignment',
    expected: {
      action: 'mechanics',
      state: 'UNSUBSCRIBED',
      rounds: 8,
      frames: 0,
      finals: 0,
    },
  },
  {
    fault: 'last ack and forget share frame',
    step: 'revocation',
    expected: {
      action: 'mechanics',
      state: 'STABLE',
      rounds: 8,
      topics: ['A:2'],
      forgotten: ['A:2'],
      laterTopics: [],
      acks: ['9007199254740993', '9007199254740994'],
    },
  },
  {
    fault: 'new ack only partition also forgotten',
    step: 'revocation',
    expected: {
      action: 'mechanics',
      state: 'STABLE',
      rounds: 8,
      topics: ['A:2'],
      forgotten: ['A:2'],
      laterTopics: [],
    },
  },
  {
    fault: 'first fetch after flush forgets',
    step: 'revocation',
    expected: {
      action: 'mechanics',
      state: 'STABLE',
      rounds: 8,
      order: [79, 78],
      forgotten: ['A:2'],
      laterTopics: [],
    },
  },
  {
    fault: 'renewed held partition remains until terminal',
    step: 'revocation',
    expected: {
      action: 'mechanics',
      state: 'STABLE',
      rounds: 8,
      renewForgotten: [],
      terminalForgotten: ['A:2'],
    },
  },
  {
    fault: 'empty round minimum fifty milliseconds',
    step: 'empty response',
    expected: {
      action: 'mechanics',
      state: 'STABLE',
      rounds: 8,
      times: [100, 150],
      maxFrames: 100,
      window: [0, 5000],
    },
  },
  {
    fault: 'independent node capacities and no prefetch',
    step: 'held handler',
    expected: {
      action: 'mechanics',
      state: 'STABLE',
      rounds: 8,
      nodeMax: 2,
      nodes: 2,
      totalMax: 4,
      blockedNodeWrites: 0,
    },
  },
  {
    fault: 'final errors never reopen',
    step: 'closing',
    expected: {
      action: 'mechanics',
      state: 'UNSUBSCRIBED',
      rounds: 8,
      codes: [122, 123, 133, 35, -1],
      finals: 1,
      resets: 0,
      reopen: 0,
    },
  },
  {
    fault: 'held data connect',
    step: 'session reset',
    expected: {
      action: 'guard',
      state: 'STABLE',
      rounds: 8,
      enforce: true,
      oldWrites: 0,
      oldEffects: 0,
      oldAckResends: 0,
      replacement: ['B:0@9007199254740993'],
    },
  },
  {
    fault: 'held data connect',
    step: 'session reset',
    expected: {
      action: 'guard',
      state: 'STABLE',
      rounds: 8,
      enforce: false,
      oldWrites: 0,
      oldEffects: 0,
      oldAckResends: 0,
      replacement: ['B:0@9007199254740993'],
    },
  },
  {
    fault: 'held data connect',
    step: 'replacement membership',
    expected: {
      action: 'guard',
      state: 'STABLE',
      rounds: 8,
      enforce: true,
      oldWrites: 0,
      oldEffects: 0,
      oldAckResends: 0,
      replacement: ['B:0@9007199254740993'],
    },
  },
  {
    fault: 'held data connect',
    step: 'replacement membership',
    expected: {
      action: 'guard',
      state: 'STABLE',
      rounds: 8,
      enforce: false,
      oldWrites: 0,
      oldEffects: 0,
      oldAckResends: 0,
      replacement: ['B:0@9007199254740993'],
    },
  },
  {
    fault: 'held data encode',
    step: 'session reset',
    expected: {
      action: 'guard',
      state: 'STABLE',
      rounds: 8,
      enforce: true,
      oldWrites: 0,
      oldEffects: 0,
      oldAckResends: 0,
      replacement: ['B:0@9007199254740993'],
    },
  },
  {
    fault: 'held data encode',
    step: 'session reset',
    expected: {
      action: 'guard',
      state: 'STABLE',
      rounds: 8,
      enforce: false,
      oldWrites: 0,
      oldEffects: 0,
      oldAckResends: 0,
      replacement: ['B:0@9007199254740993'],
    },
  },
  {
    fault: 'held data encode',
    step: 'replacement membership',
    expected: {
      action: 'guard',
      state: 'STABLE',
      rounds: 8,
      enforce: true,
      oldWrites: 0,
      oldEffects: 0,
      oldAckResends: 0,
      replacement: ['B:0@9007199254740993'],
    },
  },
  {
    fault: 'held data encode',
    step: 'replacement membership',
    expected: {
      action: 'guard',
      state: 'STABLE',
      rounds: 8,
      enforce: false,
      oldWrites: 0,
      oldEffects: 0,
      oldAckResends: 0,
      replacement: ['B:0@9007199254740993'],
    },
  },
  {
    fault: 'held data queue',
    step: 'session reset',
    expected: {
      action: 'guard',
      state: 'STABLE',
      rounds: 8,
      enforce: true,
      oldWrites: 0,
      oldEffects: 0,
      oldAckResends: 0,
      replacement: ['B:0@9007199254740993'],
    },
  },
  {
    fault: 'held data queue',
    step: 'session reset',
    expected: {
      action: 'guard',
      state: 'STABLE',
      rounds: 8,
      enforce: false,
      oldWrites: 0,
      oldEffects: 0,
      oldAckResends: 0,
      replacement: ['B:0@9007199254740993'],
    },
  },
  {
    fault: 'held data queue',
    step: 'replacement membership',
    expected: {
      action: 'guard',
      state: 'STABLE',
      rounds: 8,
      enforce: true,
      oldWrites: 0,
      oldEffects: 0,
      oldAckResends: 0,
      replacement: ['B:0@9007199254740993'],
    },
  },
  {
    fault: 'held data queue',
    step: 'replacement membership',
    expected: {
      action: 'guard',
      state: 'STABLE',
      rounds: 8,
      enforce: false,
      oldWrites: 0,
      oldEffects: 0,
      oldAckResends: 0,
      replacement: ['B:0@9007199254740993'],
    },
  },
  {
    fault: 'held data response',
    step: 'session reset',
    expected: {
      action: 'guard',
      state: 'STABLE',
      rounds: 8,
      enforce: true,
      oldWrites: 1,
      oldEffects: 0,
      oldAckResends: 0,
      replacement: ['B:0@9007199254740993'],
    },
  },
  {
    fault: 'held data response',
    step: 'session reset',
    expected: {
      action: 'guard',
      state: 'STABLE',
      rounds: 8,
      enforce: false,
      oldWrites: 1,
      oldEffects: 0,
      oldAckResends: 0,
      replacement: ['B:0@9007199254740993'],
    },
  },
  {
    fault: 'held data response',
    step: 'replacement membership',
    expected: {
      action: 'guard',
      state: 'STABLE',
      rounds: 8,
      enforce: true,
      oldWrites: 1,
      oldEffects: 0,
      oldAckResends: 0,
      replacement: ['B:0@9007199254740993'],
    },
  },
  {
    fault: 'held data response',
    step: 'replacement membership',
    expected: {
      action: 'guard',
      state: 'STABLE',
      rounds: 8,
      enforce: false,
      oldWrites: 1,
      oldEffects: 0,
      oldAckResends: 0,
      replacement: ['B:0@9007199254740993'],
    },
  },
].map((row, index) => ({
  ...row,
  expected: { ...row.expected, observations: ROW_OBSERVATIONS[index] },
}))

const SPECS = {
  'top 122': { code: 122 },
  'top 123': { code: 123 },
  'top 133': { code: 133 },
  'top -1': { code: -1 },
  'top -1 deleted topic': { code: -1 },
  'top 35': { code: 35 },
  'top 30': { code: 30 },
  'top 25': { code: 25 },
  'top 7': { code: 7 },
  'top 42': { code: 42 },
  'partition 6': { partition: 6 },
  'transport disconnect': { transport: 'disconnect' },
  'transport timeout': { transport: 'timeout' },
}
const NODE_OF = { 'open fetch': 1, 'incremental fetch': 1, 'acknowledgement only': 0 }
const ONE_NODE_SPEC = /^partition (-?\d+)/
const MAX_EPOCH = 2147483647

/**
 * Drives one literal session row through the public share consumer over the scripted cluster.
 * The row's step selects the affected frame: node 1's opening or incremental fetch, or a renewal
 * of the handler held on node 0. Mechanics, partition and boundary rows have their own drivers.
 */
class SessionFaultCase extends DataFaultCase {
  /**
   * @param {object} row The literal oracle row.
   * @returns {SessionFaultCase} A case with its family selected from the row.
   */
  constructor(row) {
    super(row)
    const { expected, fault } = row
    this.family = 'primary'
    if (expected.action === 'mechanics') this.family = 'mechanics'
    else if (expected.action === 'guard') this.family = 'guard'
    else if (expected.affected) this.family = 'partition'
    const partition = ONE_NODE_SPEC.exec(fault)
    this.spec = this.family === 'partition' ? { partition: Number(partition[1]) } : SPECS[fault]
    this.node = NODE_OF[row.step] ?? 1
    this.target = this.node === 0 ? 0 : 1
    this.transport = this.spec?.transport ?? null
  }

  /** @returns {Promise<void>} Reaches the affected frame. */
  async reach() {
    await this[`reach_${this.family}`]()
  }

  /** @returns {Promise<void>} Injects the row's fault. */
  async inject() {
    await this[`inject_${this.family}`]()
  }

  /** @returns {Promise<void>} Verifies the primary outcome. */
  async verify() {
    await this[`verify_${this.family}`]()
  }

  /** @returns {Promise<void>} Proves a current replacement works. */
  async replacement() {
    await this[`replacement_${this.family}`]()
  }

  /** @returns {Promise<void>} Starts a client sized for the row's fault. */
  async reach_primary() {
    const { step } = this.row
    const startOptions = { requestTimeout: this.transport === 'timeout' ? 3000 : 30000 }
    if (step === 'open fetch') {
      this.armBeforeJoin = async () => {
        this.gate = this.holdFrame(
          78,
          ({ node, fields }) => node === 1 && fields.shareSessionEpoch === 0
        )
      }
      await this.begin({ expectCalls: 1, startOptions })
    } else if (step === 'incremental fetch') {
      this.armBeforeJoin = async () => {
        this.gate = this.holdFrame(
          78,
          ({ node, fields }) => node === 1 && fields.shareSessionEpoch === 2
        )
      }
      await this.begin({ expectCalls: 1, startOptions, supplyCounts: { 'A:1': 2 } })
      await this.advanceUntil(
        () => this.calls.length === 7 && this.gate.call !== null,
        'seven handlers and the epoch-two frame'
      )
    } else {
      await this.begin({ expectCalls: 1, startOptions })
      const held = this.callOf(A0_H)
      await held.renew()
      this.gate = this.holdFrame(
        79,
        ({ node, fields }) => node === 0 && fields.shareSessionEpoch === 2
      )
      this.renewal = track(
        this.expectRejection(held.renew(), {
          name: 'KafkaJSShareAcknowledgementError',
          message: 'Acknowledgement was not confirmed',
          reason: this.row.expected.request,
        })
      )
    }
    await this.arrival(this.gate)
  }

  /** @returns {Promise<void>} Releases the held frame with the row's fault. */
  async inject_primary() {
    this.before = this.frameLog(this.node).length
    this.startedAt = Date.now()
    this.resetsBefore = this.seen.resets.length
    const written = this.observer.writes[this.wireIndex(this.gate.call)]
    this.originalOpen = written.authority.open
    this.oldCohorts = new Set(written.prepared.map(({ row }) => row))
    if (this.row.expected.action === 'fence') this.supplyOnce('A:0')
    await this.fire(this.gate, this.spec)
    this.faultAt = Date.now()
  }

  /**
   * @returns {Promise<object>} The first frame of the affected node after the fault.
   */
  async nextFrameOnNode() {
    await this.advanceUntil(
      () => this.frameLog(this.node).length > this.before,
      'the next frame of the affected node'
    )
    return this.nextFrame(this.node, this.before)
  }

  /** @returns {Promise<void>} Primary rows: effect per expected action. */
  async verify_primary() {
    const { expected, step } = this.row
    const { action } = expected
    const acks = step !== 'open fetch' && step !== 'acknowledgement only'
    if (action === 'reset') await this.verifyReset()
    else if (action === 'retry' || action === 'session-lost') await this.verifyAdvance()
    else if (action === 'fence') await this.verifyFence()
    else if (action === 'move') await this.verifyMove()
    else await this.verifyFatalSession()
    if (acks && expected.request !== 'none') {
      const result = expected.request
      this.assertResult(
        1,
        H,
        result,
        typeof this.spec.code === 'number' ? this.spec.code : undefined
      )
    }
    if (step === 'acknowledgement only') {
      const renewal = this.renewal
      await until(() => renewal.settled, 'the renewal outcome')
      assert(renewal.error, 'the faulted renewal rejects')
      assert.equal(renewal.error.reason, expected.request)
    }
    const held = this.callOf(A0_H)
    assert.equal(held.isRunning(), expected.held !== 'membership-lost')
    if (expected.held === 'membership-lost')
      assert.throws(() => held.acknowledge('accept'), { message: STALE_CALL })
    else if (expected.held === 'session-lost' && step === 'acknowledgement only')
      assert.throws(() => held.acknowledge('accept'), { message: STALE_CALL })
  }

  /** @returns {Promise<void>} A session reset: epoch zero, no old acknowledgements, row backoff. */
  async verifyReset() {
    const { expected } = this.row
    await until(() => this.seen.resets.length > this.resetsBefore, 'the session reset')
    assert.equal(this.seen.resets.length, this.resetsBefore + 1)
    assert.equal(this.seen.resets.at(-1).nodeId, this.node)
    const immediate = expected.backoff === 'first immediate then delayed'
    if (this.row.step === 'acknowledgement only') {
      const held = this.callOf(A0_H)
      assert.equal(held.isRunning(), true)
      assert.throws(() => held.acknowledge('accept'), { message: STALE_CALL })
      this.supplyOnce('A:0')
      this.reacquisitionSupplied = true
      this.release(A0_H)
    }
    if (immediate) {
      if (this.row.step === 'acknowledgement only')
        await this.advanceUntil(() => this.frameLog(this.node).length > this.before, 'the reopen')
      else await until(() => this.frameLog(this.node).length > this.before, 'the reopen')
    } else {
      await flush()
      assert.equal(this.frameLog(this.node).length, this.before, 'the reopen waits')
    }
    const next = await this.nextFrameOnNode()
    assert.equal(next.fields.shareSessionEpoch, expected.nextEpoch)
    assert.deepEqual(batchesOf(next), [], 'no old acknowledgements on the opening request')
    if (!immediate) assert(Date.now() - this.faultAt >= 250, 'the reopen is delayed')
    assert.equal(next.apiKey, 78)
    assert.notEqual(next.generation, undefined)
    const following = this.wireIndex(this.gate.call) + 1
    for (const write of this.observer.writes.slice(following))
      for (const { row } of write.prepared)
        assert.equal(this.oldCohorts.has(row), false, 'old acknowledgements are never resent')
  }

  /**
   * @returns {Promise<void>} An advanced epoch on the same open; current renewal work remains
   *   usable, while stale held work is reacquired under the row's policy.
   */
  async verifyAdvance() {
    const { expected, step } = this.row
    await flush()
    assert.equal(this.seen.resets.length, this.resetsBefore, 'no session reset')
    if (step === 'acknowledgement only' && expected.held === 'session-lost') {
      const held = this.callOf(A0_H)
      assert.throws(() => held.acknowledge('accept'), { message: STALE_CALL })
      this.supplyOnce('A:0')
      this.reacquisitionSupplied = true
      this.release(A0_H)
    } else if (step === 'acknowledgement only') {
      this.renewalAfter = track(this.callOf(A0_H).renew())
      await this.advanceUntil(
        () => this.frameLog(0).length > this.before,
        'the next frame of node 0'
      )
    }
    const next = await this.nextFrameOnNode()
    assert.equal(next.fields.shareSessionEpoch, expected.nextEpoch)
    const written = this.observer.writes[this.wireIndex(next)]
    assert.equal(written.authority.open === this.originalOpen, expected.sameOpen)
    assert.equal(
      this.frameLog(this.node)
        .slice(this.before)
        .some(({ fields }) => fields.shareSessionEpoch === 0),
      false
    )
    if (step === 'acknowledgement only' && expected.action === 'session-lost') {
      assert.equal(next.apiKey, 79)
      assert.equal(next.fields.isRenewAck, true)
      assert.deepEqual(batchesOf(next), [{ firstOffset: H, lastOffset: H, acknowledgeTypes: [4] }])
      await until(() => this.renewalAfter.settled, 'the subsequent successful renewal')
      assert.equal(this.renewalAfter.error, null)
      assert.equal(this.renewal.error.cause.code, 6)
      const held = this.callOf(A0_H)
      assert.equal(held.isRunning(), true)
      assert.doesNotThrow(() => held.acknowledge('accept'))
    }
  }

  /** @returns {Promise<void>} A leader move: the partition opens a new session on the hinted node. */
  async verifyMove() {
    await this.advanceUntil(
      () =>
        this.frames(78).some(
          ({ node, fields }) =>
            node === 2 &&
            fields.shareSessionEpoch === 0 &&
            listed(fields).some(({ partition }) => partition === 1)
        ),
      'the opening request on the hinted leader'
    )
    assert(this.seen.resets.length > this.resetsBefore, 'the old open was retired')
    assert.equal(this.seen.crashes.length, 0)
  }

  /** @returns {Promise<void>} A fence answer: the membership rejoins and old work is lost. */
  async verifyFence() {
    await this.advanceUntil(() => this.joins().length === 2, 'the rejoin')
    assert.equal(this.joins()[1].fields.memberId, this.joins()[0].fields.memberId)
    assert.equal(this.seen.crashes.length, 0)
  }

  /** @returns {Promise<void>} A fatal answer: one crash with restart false. */
  async verifyFatalSession() {
    await until(() => this.seen.crashes.length === 1, 'the crash event')
    const [crash] = this.seen.crashes
    assert.equal(crash.restart, false)
    if (this.spec.code === 35) assert.equal(crash.error.reason, 'refused-by-broker')
    else assert(codesOf(crash.error).includes(this.spec.code))
    assert.equal(this.restartErrors[0], crash.error)
  }

  /** @returns {Promise<void>} Primary rows: a current acquisition or a new run still works. */
  async replacement_primary() {
    const { action } = this.row.expected
    if (
      action === 'fence' &&
      this.calls.some((call) => call.identity === A0_H && call !== this.callOf(A0_H))
    ) {
      this.releaseAll()
      return
    }
    if (['fatal'].includes(action) || this.row.expected.held === 'membership-lost') {
      this.supplyOnce('A:0')
      const calls = this.calls.length
      this.releaseAll()
      if (action !== 'fence') await this.run()
      await this.advanceUntil(() => this.calls.length > calls, 'the replacement delivery')
      return
    }
    if (
      this.row.expected.held === 'session-lost' &&
      this.node === 0 &&
      !this.reacquisitionSupplied &&
      !this.calls.some((call) => call.identity === A0_H && call !== this.callOf(A0_H))
    )
      this.supplyOnce('A:0')
    this.release(A0_H)
    await this.advanceUntil(() => this.acceptedBoth(0), 'the current terminal acceptance')
    this.releaseAll()
  }

  /**
   * @param {number} partition Partition of topic A.
   * @returns {boolean} Whether both exact records were accepted without error.
   */
  acceptedBoth(partition) {
    const accepted = this.resultsOf(partition).filter((ack) => !ack.error && ack.type !== 'renew')
    return covers(accepted, H) && covers(accepted, J)
  }

  /** @returns {void} Lets every held handler return. */
  releaseAll() {
    for (const { gate } of this.heldGates) gate.resolve()
  }

  /** @returns {Promise<void>} Partition rows: node 1's frame is answered with a partition error. */
  async reach_partition() {
    const { step, expected } = this.row
    const reopen = expected.action === 'recreate' && step === 'open fetch'
    this.armBeforeJoin = async () => {
      this.gate = this.holdFrame(
        78,
        ({ node, fields }) =>
          node === 1 && fields.shareSessionEpoch === (step === 'open fetch' && !reopen ? 0 : 2)
      )
    }
    await this.begin({
      expectCalls: 1,
      supplyCounts: step === 'open fetch' ? {} : { 'A:1': 2 },
      startOptions: {
        retry: {
          ...RETRY_250,
          restartOnFailure: async (error) => {
            this.restartErrors.push(error)
            return false
          },
        },
      },
    })
    await this.advanceUntil(() => this.gate.call !== null, 'the partition fault frame')
    if (step === 'incremental fetch') assert.equal(this.calls.length, 7)
    if (reopen) {
      assert.equal(this.calls.length, 5)
      assert(this.acceptedBoth(1) && this.acceptedBoth(2))
      const old = await this.arrival(this.gate)
      this.gate.release()
      await flush()
      this.gate = this.holdFrame(
        78,
        ({ node, fields }) => node === 1 && fields.shareSessionEpoch === 0
      )
      this.socketFor(old).emit('end')
      await this.advanceUntil(() => this.gate.call !== null, 'the reopened partition fault frame')
      assert.deepEqual(batchesOf(this.gate.call), [])
      assert.deepEqual(listed(this.gate.call.fields), [
        { topicId: IDS.A, partition: 1 },
        { topicId: IDS.A, partition: 2 },
      ])
    }
    await this.arrival(this.gate)
  }

  /** @returns {Promise<void>} Releases the frame with the partition error of the row. */
  async inject_partition() {
    this.before = this.frameLog(1).length
    this.metadataBefore = this.cluster.requests().filter(({ api }) => api === 'Metadata').length
    this.resetsBefore = this.seen.resets.length
    const spec = { partition: this.spec.partition }
    if (this.row.expected.action === 'recreate') {
      await this.injectRecreation()
      return
    }
    if (this.row.expected.settled) {
      const call = await this.arrival(this.gate)
      const rows = listed(call.fields).map(({ topicId, partition }) =>
        partition === 2
          ? dataPart(2, { topicId })
          : part({ topicId, partition, code: partition === 1 ? 29 : 0 })
      )
      this.gate.release(fetchAnswer(rows))
      await flush()
      return
    }
    await this.fire(this.gate, spec)
  }

  /**
   * Holds the actual guarded refresh while A still owns newly decided, unsent work, then serves B
   * for the same topic name. All helper and cache transitions retain their original implementation.
   * @returns {Promise<void>}
   */
  async injectRecreation() {
    const written = this.observer.writes[this.wireIndex(this.gate.call)]
    this.recreatedLoop = [...this.observer.loops.keys()].find(
      (loop) => loop.session.token === written.authority.open
    )
    assert(this.recreatedLoop, 'the original fault session was captured')
    this.recreatedOpen = written.authority.open
    this.realCluster = this.recreatedLoop.cluster
    this.recreations = []
    this.removers.push(
      this.h.consumer.on(this.h.consumer.events.TOPIC_RECREATED, ({ payload }) =>
        this.recreations.push(payload)
      )
    )
    this.observer.watch(this.realCluster, 'refreshMetadata', (receiver, [scope], next) => {
      const refresh = { scope, settled: false }
      this.recreationRefresh = refresh
      return this.observe(
        Promise.resolve(next()).then((result) => {
          refresh.settled = true
          refresh.topicId = receiver.topicId(NAME)
          return result
        }),
        'recreation metadata'
      )
    })
    this.observer.watch(
      this.realCluster.metadataCache,
      'update',
      (cache, [metadata, scope], next) => {
        // structuredClone would return outer-realm arrays under Jest and break strict equality.
        this.recreationMetadata = {
          topicMetadata: metadata.topicMetadata.map(({ topic, topicId }) => ({ topic, topicId })),
        }
        this.metadataWasCurrent = scope.isCurrent()
        return next()
      }
    )
    this.metadataGate = this.holdWire({ api: 'Metadata' })
    this.gate.release(
      fetchAnswer([
        part({ topicId: IDS.A, partition: 1, code: 103 }),
        dataPart(2, { topicId: IDS.A }),
      ])
    )
    await this.arrival(this.metadataGate, 'the original guarded Metadata request')
    const refresh = this.recreationRefresh
    assert(refresh, 'the fault started the real Cluster refresh')
    assert.equal(refresh.scope.fence.session, this.recreatedLoop.session)
    assert.equal(refresh.scope.fence.guard(), null)
    assert.equal(refresh.scope.allowLeaderless, true)
    assert.equal(refresh.settled, false)
    assert.equal(this.recreatedLoop.session.token, this.recreatedOpen)
    assert.equal(this.realCluster.topicId(NAME), IDS.A)
    assert.deepEqual(this.recreations, [])
    const rows = this.recreatedLoop.cohorts.get(`${IDS.A}:2`)
    assert.equal(rows.length, 1, 'A owns the successful sibling acquisition before Metadata')
    this.retiringCohort = rows[0]
    assert.equal(this.retiringCohort.ledger.size, 2, 'both decided records still owe settlement')
    assert.equal(this.recreatedLoop.isLive(this.retiringCohort), true)
    this.h.table.set(NAME, {
      topicId: IDS.B,
      partitions: [
        { leader: 0, epoch: 1 },
        { leader: 1, epoch: 1 },
        { leader: 1, epoch: 1 },
      ],
    })
    this.metadataGate.release()
    await until(() => refresh.settled, 'the accepted guarded Metadata result')
    await flush()
  }

  /** @returns {Promise<void>} Partition rows: move, refresh, retry or fatal, with the sibling intact. */
  async verify_partition() {
    const { expected } = this.row
    const sibling = this.callOf(expected.sibling[0])
    if (expected.action.startsWith('fatal')) {
      await until(() => this.seen.crashes.length === 1, 'the crash event')
      const [crash] = this.seen.crashes
      assert.equal(crash.restart, false)
      assert.deepEqual(this.restartErrors, [crash.error])
      assert(codesOf(crash.error).includes(expected.cause))
      assert.equal(crash.error.retriable, false)
      if (expected.action === 'fatal protocol')
        assert.equal(crash.error.name, 'KafkaJSProtocolError')
      if (expected.settled) {
        await flush()
        for (const offset of [H, J]) {
          const terminal = this.terminalFor(2, offset)
          assert.equal(terminal.length, 1, `A2 record ${offset} has one settled outcome`)
        }
        assert.equal(
          this.calls.some((call) => call.alias === 'A' && call.partition === 2),
          false
        )
      }
      return
    }
    if (expected.action === 'recreate') {
      await this.verifyRecreation()
    } else if (expected.action === 'move') {
      await this.advanceUntil(
        () =>
          this.frames(78).some(
            ({ node, fields }) =>
              node === expected.nextNode &&
              fields.shareSessionEpoch === 0 &&
              listed(fields).some(({ partition }) => partition === 1)
          ),
        'the opening request on the new leader'
      )
      assert(this.seen.resets.length > this.resetsBefore, 'the old open was retired')
    } else {
      if (expected.action === 'refresh')
        await this.advanceUntil(
          () =>
            this.cluster.requests().filter(({ api }) => api === 'Metadata').length >
            this.metadataBefore,
          'the guarded metadata refresh'
        )
      await this.advanceUntil(
        () => this.frameLog(expected.nextNode).length > this.before,
        'the next frame of the node'
      )
    }
    assert(sibling.isRunning(), 'the sibling handler stays current')
    assert.equal(this.seen.crashes.length, 0)
  }

  /** @returns {Promise<void>} The guarded B result retires A's original unsent acquisition. */
  async verifyRecreation() {
    assert(
      this.cluster.requests().filter(({ api }) => api === 'Metadata').length > this.metadataBefore
    )
    assert.equal(this.metadataWasCurrent, true)
    assert.deepEqual(
      this.recreationMetadata.topicMetadata.map(({ topic, topicId }) => ({ topic, topicId })),
      [{ topic: NAME, topicId: IDS.B }]
    )
    assert.equal(this.recreationRefresh.topicId, IDS.B)
    assert.equal(this.realCluster.topicId(NAME), IDS.B)
    assert.equal(this.realCluster.topicName(IDS.A), undefined)
    assert.deepEqual(
      this.recreations.map(({ topic, previousTopicId, topicId }) => ({
        topic,
        previousTopicId,
        topicId,
      })),
      [{ topic: NAME, previousTopicId: IDS.A, topicId: IDS.B }]
    )
    assert.equal(this.recreatedLoop.isLive(this.retiringCohort), false)
    assert.equal(this.retiringCohort.ledger.size, 0)
    const outcomes = this.observer.published.filter(({ cohort }) => cohort === this.retiringCohort)
    assert.deepEqual(
      outcomes.map(({ event }) => ({
        firstOffset: event.firstOffset,
        lastOffset: event.lastOffset,
        type: event.type,
        reason: event.error?.reason,
        code: event.error?.cause?.code,
      })),
      [{ firstOffset: H, lastOffset: J, type: 'accept', reason: 'session-lost', code: 103 }]
    )
    await this.advanceUntil(
      () => this.frameLog(this.row.expected.nextNode).length > this.before,
      'the next frame after recreation'
    )
  }

  /** @returns {Promise<void>} Partition rows: the held sibling finishes under current authority. */
  async replacement_partition() {
    if (this.row.expected.action.startsWith('fatal')) {
      this.h.table.set(NAME, { topicId: IDS.B, partitions: [{ leader: 0, epoch: 2 }] })
      this.supplyOnce('B:0')
      this.releaseAll()
      await this.run()
      await this.advanceUntil(
        () => this.calls.some(({ identity }) => identity === `B:0@${H}`),
        'the current replacement after the fatal partition'
      )
      return
    }
    this.release(A0_H)
    await this.advanceUntil(() => this.acceptedBoth(0), 'the sibling acceptance')
    if (this.row.expected.action === 'recreate') await this.replaceRecreatedTopic()
    this.releaseAll()
  }

  /** @returns {Promise<void>} Installs B through a real heartbeat and delivers and accepts B0/H. */
  async replaceRecreatedTopic() {
    this.supplyOnce('B:0')
    const gate = await this.holdBeatSteady()
    gate.release({
      errorCode: 0,
      errorMessage: null,
      memberId: gate.call.fields.memberId,
      memberEpoch: gate.call.fields.memberEpoch,
      heartbeatIntervalMs: 50,
      assignment: { topicPartitions: [{ topicId: IDS.B, partitions: [0] }] },
    })
    await this.advanceUntil(
      () => this.calls.some(({ identity }) => identity === `B:0@${H}`),
      'the recreated B acquisition'
    )
    await this.advanceUntil(
      () =>
        this.observer.published.some(
          ({ cohort, event }) => cohort.topicId === IDS.B && event.type === 'accept' && !event.error
        ),
      'the recreated B acceptance'
    )
    assert.equal(this.calls.filter(({ identity }) => identity === `B:0@${H}`).length, 1)
    await this.advanceUntil(
      () => this.recreatedLoop.session.model.size === 0,
      'the old A node final clears its model'
    )
    const current = [...this.observer.loops.keys()].find((loop) =>
      loop.session.model.has(`${IDS.B}:0`)
    )
    assert(current, 'B installed in its actual session model')
    assert.deepEqual([...current.session.model.keys()], [`${IDS.B}:0`])
    for (const loop of this.observer.loops.keys()) {
      assert.equal(
        [...loop.session.model.keys()].some((key) => key.startsWith(IDS.A)),
        false
      )
      assert.equal(
        [...loop.cohorts.keys()].some((key) => key.startsWith(IDS.A)),
        false
      )
    }
    const forget = this.frames(78).find(
      ({ fields }) =>
        listed(fields).some(({ topicId }) => topicId === IDS.B) &&
        fields.forgottenTopicsData.some(({ topicId }) => topicId === IDS.A)
    )
    assert(forget, 'the B fetch forgets the original A model')
    assert.deepEqual(forget.fields.forgottenTopicsData, [{ topicId: IDS.A, partitions: [0] }])
    assert.deepEqual(listed(forget.fields), [{ topicId: IDS.B, partition: 0 }])
    const writes = this.observer.writes.slice(this.wireIndex(forget))
    assert.equal(
      writes.some(({ prepared }) => prepared.some(({ row }) => row === this.retiringCohort)),
      false,
      'retired A acknowledgements are never transmitted under B'
    )
  }

  /** @returns {Promise<void>} Mechanics rows: each has its own scenario. */
  async reach_mechanics() {
    this.mechanic = MECHANICS[this.row.fault]
    assert(this.mechanic, `no mechanics driver for ${this.row.fault}`)
    await this.mechanic.reach.call(this)
  }

  /** @returns {Promise<void>} Mechanics rows: the scenario's disturbance. */
  async inject_mechanics() {
    await this.mechanic.inject?.call(this)
  }

  /** @returns {Promise<void>} Mechanics rows: the scenario's oracle. */
  async verify_mechanics() {
    await this.mechanic.verify.call(this)
  }

  /** @returns {Promise<void>} Mechanics rows: nothing further. */
  async replacement_mechanics() {
    this.releaseAll()
    await flush()
  }

  /** @returns {Promise<void>} Boundary rows: reach node 1's established session. */
  async reach_guard() {
    this.enforce = this.row.expected.enforce
    await this.begin({
      expectCalls: 1,
      supply: ['A:0'],
      startOptions: { requestTimeout: 1000, enforce: this.enforce },
    })
  }

  /** @returns {Promise<void>} Boundary rows: hold node 1's next fetch at its stage, then retire it. */
  async inject_guard() {
    const boundary = this.row.fault.replace('held data ', '')
    await until(
      () =>
        this.frameLog(1).length === 1 &&
        [...this.observer.loops.keys()].some(
          (loop) => loop.session.binding?.broker.nodeId === 1 && loop.session.epoch === 1
        ),
      'the established guarded session'
    )
    this.guardedOpen = [...this.observer.loops.keys()].find(
      (loop) => loop.session.binding?.broker.nodeId === 1
    ).session.token
    this.steadyBefore = this.steadyFrames()
    this.pending = this.boundaryHold(boundary)
    await this.pump(50)
    await until(() => this.pending.reached(), `the ${boundary} boundary`)
    await flush()
    this.steadyAtHold = this.steadyFrames()
    if (this.row.step === 'session reset') {
      this.socketFor(this.frameLog(1)[0]).emit('end')
      await flush()
    } else {
      const gate = await this.holdBeatSteady()
      gate.release({ errorCode: 110 })
      await flush()
    }
  }

  /** @returns {Promise<object>} The held steady heartbeat gate. */
  async holdBeatSteady() {
    const gate = this.holdWire({
      api: 'ShareGroupHeartbeat',
      match: ({ fields }) => fields.memberEpoch > 0,
    })
    await this.advanceUntil(() => gate.call !== null, 'the steady heartbeat')
    await this.arrival(gate)
    await flush()
    return gate
  }

  /**
   * @param {'connect'|'encode'|'queue'|'response'} boundary The write stage to hold on node 1.
   * @returns {{reached: () => boolean, release: () => void}} The hold.
   */
  boundaryHold(boundary) {
    let reached = false
    if (boundary === 'response') {
      const wire = this.holdFrame(78, ({ node }) => node === 1)
      wire.arrived().then(() => {
        reached = true
      })
      return { reached: () => reached, release: () => wire.settle && wire.release() }
    }
    const gate = deferred()
    let taken = false
    const take = () => {
      if (taken) return false
      taken = true
      reached = true
      return true
    }
    const isNode1 = (pool) => pool?.host === 'node1' || pool?.brokerAddress?.startsWith('node1')
    if (boundary === 'connect') {
      const original = ConnectionPool.prototype.getConnectionByRequest
      jest.spyOn(ConnectionPool.prototype, 'getConnectionByRequest').mockImplementation(function (
        protocolRequest,
        ...rest
      ) {
        if (protocolRequest?.request?.apiKey !== 78 || !isNode1(this) || !take())
          return original.call(this, protocolRequest, ...rest)
        return gate.promise.then(() => original.call(this, protocolRequest, ...rest))
      })
    } else if (boundary === 'encode') {
      const original = requests.ShareFetch.protocol
      jest.spyOn(requests.ShareFetch, 'protocol').mockImplementation((selection) => {
        const factory = original(selection)
        return (options) => {
          const built = factory(options)
          if (!(options?.shareSessionEpoch >= 1) || taken) return built
          const { encode } = built.request
          return {
            ...built,
            request: {
              ...built.request,
              encode: async () => {
                take()
                await gate.promise
                return encode()
              },
            },
          }
        }
      })
    } else {
      let queue = null
      let released = false
      const push = RequestQueue.prototype.push
      const canSend = RequestQueue.prototype.canSendSocketRequestImmediately
      jest
        .spyOn(RequestQueue.prototype, 'canSendSocketRequestImmediately')
        .mockImplementation(function () {
          return this === queue && !released ? false : canSend.call(this)
        })
      jest.spyOn(RequestQueue.prototype, 'push').mockImplementation(function (request) {
        if (request.entry.apiKey === 78 && this.broker === 'node1:9092' && take()) queue = this
        return push.call(this, request)
      })
      this.observe(
        gate.promise.then(() => {
          released = true
          queue?.checkPendingRequests()
        }),
        'queue release'
      )
    }
    this.heldGates.push({ identity: `boundary:${boundary}`, gate })
    return { reached: () => reached, release: () => gate.resolve() }
  }

  /** @returns {number} Established-session ShareFetch frames written to node 1. */
  steadyFrames() {
    return this.observer.writes.filter(
      ({ authority, fields, socket }) =>
        authority.open === this.guardedOpen &&
        fields.shareSessionEpoch > 0 &&
        socket.endpoint.host === 'node1'
    ).length
  }

  /** @returns {Promise<void>} Boundary rows: zero or one old write, no late effect. */
  async verify_guard() {
    const { expected } = this.row
    assert.equal(
      this.steadyAtHold - this.steadyBefore,
      expected.oldWrites,
      'old frames written for the held boundary'
    )
    const results = this.seen.acks.length
    this.pending.release()
    await flush()
    assert.equal(
      this.steadyFrames(),
      this.steadyAtHold,
      'the released old request writes nothing more'
    )
    assert.equal(this.seen.crashes.length, 0)
    assert.equal(this.seen.acks.length, results, 'no late effect')
  }

  /** @returns {Promise<void>} Boundary rows: the replacement delivers B0/H. */
  async replacement_guard() {
    const identity = this.row.expected.replacement[0]
    this.h.table.set(NAME, { topicId: IDS.B, partitions: [{ leader: 0, epoch: 2 }] })
    this.supplyOnce('B:0')
    if (this.row.step === 'replacement membership')
      await this.advanceUntil(() => this.joins().length >= 2, 'the rejoin')
    const gate = await this.holdBeatSteady()
    gate.release({
      errorCode: 0,
      errorMessage: null,
      memberId: gate.call.fields.memberId,
      memberEpoch: 2,
      heartbeatIntervalMs: 50,
      assignment: { topicPartitions: [{ topicId: IDS.B, partitions: [0] }] },
    })
    await until(
      () =>
        this.observer.assignments.some(({ assignment }) =>
          assignment.some(({ topicId }) => topicId === IDS.B)
        ),
      'the B assignment'
    )
    this.release(A0_H)
    await this.advanceUntil(
      () => this.calls.some((call) => call.identity === identity),
      'the replacement delivery'
    )
    await flush()
    assert.equal(this.calls.filter((call) => call.identity === identity).length, 1)
    this.releaseAll()
  }

  /**
   * Runs an independent mechanics companion after this case's complete receipt and cleanup.
   * @param {object} options Literal fixture overrides; never provider state.
   * @returns {Promise<void>}
   * @throws {AggregateError} Preserves the companion's primary and unconditional cleanup failures.
   */
  async companion(options) {
    await this.assertInvariants()
    await this.dispose()
    const scenario = new SessionFaultCase(this.row)
    Object.assign(scenario, options, { isCompanion: true })
    const failures = []
    try {
      await scenario.reach()
      await scenario.inject()
      await scenario.verify()
      await scenario.replacement()
      await scenario.assertInvariants()
    } catch (error) {
      failures.push(error)
    }
    for (const finish of [
      () => scenario.dispose(),
      () => scenario.assertBaseline(),
      () => scenario.cluster?.assertClean(),
    ]) {
      try {
        await finish()
      } catch (error) {
        failures.push(error)
      }
    }
    if (failures.length) throw new AggregateError(failures, 'Mechanics companion failed')
  }

  /** @returns {Promise<void>} Drains released controls and replays the complete literal sets. */
  async assertInvariants() {
    if (this.h) {
      if (!this.disposed) {
        this.releaseAll()
        await this.advanceUntil(
          () => [...this.observer.ledgers.keys()].every((ledger) => ledger.size === 0),
          'all original acquisition outcomes'
        )
        await flush()
      }
      this.replay()
    } else {
      assert.deepEqual(this.row.expected.observations, literalObservations(OBSERVATIONS.empty))
    }
    if (this.auditFailures.length) throw this.auditFailures[0]
    assert.deepEqual(this.unhandled, [])
  }
}

/**
 * Mechanics scenarios by row fault. Each has `reach`, an optional `inject` and `verify`, run with
 * the case as `this`.
 */
const MECHANICS = {
  'opening lists all wanted without acks': {
    async reach() {
      await this.begin({ expectCalls: 1 })
      await this.advanceUntil(() => this.frameLog(1).length >= 3, 'three node 1 frames')
    },
    async verify() {
      const { expected } = this.row
      const first = this.frameLog(1)[0]
      assert.deepEqual(
        listed(first.fields).map(({ partition }) => `A:${partition}`),
        expected.topics.filter((key) => key !== 'A:0')
      )
      assert.equal(this.frameLog(0)[0].fields.shareSessionEpoch, 0)
      assert.deepEqual(batchesOf(first), expected.acks)
      assert.deepEqual(
        this.frameLog(1)
          .slice(0, 3)
          .map(({ fields }) => fields.shareSessionEpoch),
        expected.epochs
      )
    },
  },
  'epoch changes on answer only': {
    async reach() {
      this.armBeforeJoin = async () => {
        this.gate = this.holdFrame(78, ({ node }) => node === 1)
      }
      await this.begin({ expectCalls: 1 })
      await this.arrival(this.gate)
    },
    async verify() {
      await this.pump(200)
      assert.equal(this.frameLog(1).length, this.row.expected.inFlight, 'one request in flight')
      this.gate.release()
      await this.advanceUntil(() => this.frameLog(1).length >= 3, 'three frames')
      assert.deepEqual(
        this.frameLog(1)
          .slice(0, 3)
          .map(({ fields }) => fields.shareSessionEpoch),
        this.row.expected.epochs
      )
    },
  },
  'epoch wrap': {
    async reach() {
      this.sessions = []
      const apply = ShareSession.prototype.apply
      const owner = this
      jest.spyOn(ShareSession.prototype, 'apply').mockImplementation(function (...args) {
        if (owner.capture) {
          owner.sessions.push(this)
          owner.capture = false
        }
        return apply.apply(this, args)
      })
      this.armBeforeJoin = async () => {
        this.gate = this.holdFrame(78, ({ node }) => node === 1)
      }
      await this.begin({ expectCalls: 1, supply: ['A:0'] })
      await this.arrival(this.gate)
    },
    async inject() {
      this.capture = true
      this.gate.release()
      await flush()
      this.sessions[0].epoch = MAX_EPOCH
    },
    async verify() {
      await this.advanceUntil(() => this.frameLog(1).length >= 4, 'the wrapped frames')
      assert.deepEqual(
        this.frameLog(1)
          .slice(1, 4)
          .map(({ fields }) => fields.shareSessionEpoch),
        this.row.expected.epochs
      )
      assert.notEqual(
        this.frameLog(1)[1].fields.shareSessionEpoch,
        this.frameLog(1)[2].fields.shareSessionEpoch
      )
    },
  },
  'held answer blocks renewal and flush': {
    /** @returns {Promise<void>} Reaches the original production operation with its literal supply and gates. */
    async reach() {
      this.armBeforeJoin = async () => {
        this.gate = this.holdFrame(
          78,
          ({ node, fields }) => node === 0 && fields.shareSessionEpoch === 1
        )
      }
      await this.begin({ expectCalls: 0, supply: [], hold: [A0_H, `A:0@${J}`] })
      await this.advanceUntil(() => this.gate.call !== null, 'the held fetch')
    },
    /** @returns {Promise<void>} Checks the exact literal wire sequence, outcomes and replacement authority. */
    async verify() {
      const written = this.frameLog(0).length
      await this.pump(100)
      assert.equal(this.frameLog(0).length, written, 'no request passes the held fetch')
      this.gate.release(fetchAnswer([dataPart(0)]))
      await until(() => this.callOf(A0_H), 'the acquired handler')
      await this.pump(7501)
      const renew = this.holdFrame(79, ({ node }) => node === 0)
      this.first = track(this.callOf(A0_H).renew())
      await this.arrival(renew)
      this.release(A0_H)
      await until(
        () => this.calls.some(({ identity }) => identity === `A:0@${J}`),
        'the held J handler'
      )
      const loop = [...this.observer.loops.keys()].find(
        (candidate) => candidate.session.binding?.broker.nodeId === 0
      )
      assert(loop.session.queue.length >= 1, 'the terminal flush queues behind the renewal')
      assert.equal(this.first.settled, false, 'the renewal answer remains held')
      assert.equal(
        this.frames(79).filter(({ node }) => node === 0).length,
        this.row.expected.inFlight
      )
      renew.release()
      await until(() => this.terminalFor(0, H).length === 1, 'the terminal flush result')
      const frames = this.frameLog(0).slice(1)
      assert.deepEqual(
        frames.map(({ apiKey, fields }) =>
          apiKey === 78 ? 'fetch' : fields.isRenewAck ? 'renew' : 'flush'
        ),
        this.row.expected.order
      )
      assert.deepEqual(
        frames.map(({ fields }) => fields.shareSessionEpoch),
        this.row.expected.epochs
      )
      assert.equal(this.first.settled, true)
      assert.equal(this.first.error, null)
      assert.deepEqual(batchesOf(frames[1]), [
        { firstOffset: H, lastOffset: H, acknowledgeTypes: [4] },
      ])
      assert.deepEqual(batchesOf(frames[2]), [
        { firstOffset: H, lastOffset: H, acknowledgeTypes: [1] },
      ])
    },
  },
  'queued reset rejects before held answer': {
    /** @returns {Promise<void>} Reaches the original production operation with its literal supply and gates. */
    async reach() {
      await this.begin({ expectCalls: 1 })
      this.held = this.callOf(A0_H)
      this.gate = this.holdFrame(79, ({ node }) => node === 0)
      this.first = track(
        this.expectRejection(this.held.renew(), {
          name: 'KafkaJSShareAcknowledgementError',
          message: 'Acknowledgement was not confirmed',
          reason: 'session-lost',
        })
      )
      await this.arrival(this.gate)
      this.second = track(
        this.expectRejection(this.held.renew(), {
          name: 'KafkaJSConnectionClosedError',
          message: 'Closed connection',
        })
      )
      const loop = [...this.observer.loops.keys()].find(
        (candidate) => candidate.session.binding?.broker.nodeId === 0
      )
      assert.equal(loop.session.queue.length, 1)
      const reset = loop.session.reset
      const owner = this
      jest.spyOn(loop.session, 'reset').mockImplementation(function (reason, cause) {
        owner.resetCause = cause
        return reset.call(this, reason, cause)
      })
      this.queuedBuilder = jest.fn(loop.session.queue[0].work)
      loop.session.queue[0].work = this.queuedBuilder
      await flush()
    },
    /** @returns {Promise<void>} Applies the declared fault to its captured original provider. */
    async inject() {
      this.socketFor(this.gate.call).emit('end')
      await flush()
    },
    /** @returns {Promise<void>} Checks the exact literal wire sequence, outcomes and replacement authority. */
    async verify() {
      await until(() => this.second.settled, 'the queued renewal')
      assert.equal(this.second.error.name, 'KafkaJSConnectionClosedError')
      assert.equal(this.second.error.message, 'Closed connection')
      assert.equal(this.second.error, this.resetCause)
      assert.notEqual(this.gate.settle, null, 'the old answer remains held')
      assert.equal(this.first.settled, false, 'the written caller still owes its backoff')
      await this.pump(250)
      await until(() => this.first.settled, 'the written renewal loss')
      assert.equal(this.first.error.reason, 'session-lost')
      assert.equal(this.frames(79).filter(({ node }) => node === 0).length, 1)
      assert.equal(this.queuedBuilder.mock.calls.length, this.row.expected.builderCalls)
    },
  },
  'fetch limit always delays': {
    /** @returns {Promise<void>} Reaches the original production operation with its literal supply and gates. */
    async reach() {
      this.times = []
      await this.start({ retry: { ...RETRY_250, initialRetryTime: this.limitInitialRetry ?? 250 } })
      this.t0 = null
      this.cluster.answer('ShareFetch', (fields, record) => {
        if (record.node === 1 && fields.shareSessionEpoch === 0) {
          this.t0 ??= Date.now()
          const elapsed = Date.now() - this.t0
          this.times.push(elapsed)
          if (elapsed < 5000) return topError(133)
        }
        return this.fetchAnswerFor(fields, record)
      })
    },
    /** @returns {Promise<void>} Applies the declared fault to its captured original provider. */
    async inject() {
      await this.run()
      await until(() => this.times.length === 1, 'the first limited fetch')
      for (let step = 1; step <= 20; step++) {
        await until(
          () => this.seen.resets.filter(({ nodeId }) => nodeId === 1).length === step,
          'the limited fetch reset'
        )
        await flush()
        await this.pump(250)
        await until(() => this.times.length === step + 1, 'the next limited fetch')
      }
    },
    /** @returns {Promise<void>} Checks the exact literal wire sequence, outcomes and replacement authority. */
    async verify() {
      const { expected } = this.row
      assert.deepEqual(this.times.slice(0, expected.times.length), expected.times)
      assert.equal(this.times.filter((time) => time < 5000).length, expected.maxFrames)
      assert.equal(this.times.at(-1), 5000)
      await until(
        () =>
          [...this.observer.loops.keys()].some(
            (loop) =>
              loop.session.binding?.broker.nodeId === 1 &&
              loop.session.epoch === 1 &&
              loop.session.resetStreak === 0
          ),
        'the accepted five-second fetch'
      )
      if (!this.isCompanion) await this.companion({ limitInitialRetry: 0 })
    },
  },
  'consecutive resets delay then success clears streak': {
    async reach() {
      this.stamps = []
      let node1 = 0
      await this.start({ retry: RETRY_250 })
      this.cluster.answer('ShareFetch', (fields, record) => {
        if (record.node !== 1) return this.fetchAnswerFor(fields, record)
        node1++
        this.stamps.push([fields.shareSessionEpoch, Date.now()])
        return [2, 3, 5].includes(node1) ? topError(122) : this.fetchAnswerFor(fields, record)
      })
    },
    async inject() {
      await this.run()
      await this.advanceUntil(() => this.stamps.length >= 6, 'six node 1 frames', 24)
    },
    async verify() {
      const { expected } = this.row
      assert.deepEqual(
        this.stamps.slice(1, 6).map(([epoch]) => epoch),
        expected.epochs
      )
      assert.deepEqual(
        [2, 3, 5].map((index) => this.stamps[index][1] - this.stamps[index - 1][1] >= 250),
        expected.backoff
      )
      assert.equal(
        this.stamps[2][1] - this.stamps[1][1] < 250,
        true,
        'the first reset is immediate'
      )
      assert.equal(this.stamps[3][1] - this.stamps[2][1] >= 250, true, 'the second reset waits')
      assert.equal(this.stamps[5][1] - this.stamps[4][1] < 250, true, 'success clears the streak')
    },
  },
  'slot zero reconnect keeps data session': {
    /** @returns {Promise<void>} Reaches the original production operation with its literal supply and gates. */
    async reach() {
      this.supplyOnce('A:0')
      this.armBeforeJoin = async () => {
        this.gate = this.holdFrame(
          78,
          ({ node, fields }) => node === 0 && fields.shareSessionEpoch === 1
        )
      }
      await this.begin({ expectCalls: 1, hold: [], supply: ['A:0'] })
      await this.arrival(this.gate)
    },
    /** @returns {Promise<void>} Applies the declared fault to its captured original provider. */
    async inject() {
      const [join] = this.frames(76)
      this.socketFor(join).emit('end')
      await this.pump(50)
      this.gate.release()
      await flush()
    },
    /** @returns {Promise<void>} Checks the exact literal wire sequence, outcomes and replacement authority. */
    async verify() {
      await this.advanceUntil(() => this.frameLog(0).length >= 3, 'two more node 0 frames')
      assert.deepEqual(
        this.frameLog(0)
          .slice(1, 3)
          .map(({ fields }) => fields.shareSessionEpoch),
        this.row.expected.epochs
      )
      assert.equal(new Set(this.frameLog(0).map(({ generation }) => generation)).size, 1)
      assert.deepEqual(this.seen.resets, [])
    },
  },
  'slot one disconnect resets before counter changes': {
    /** @returns {Promise<void>} Reaches the original production operation with its literal supply and gates. */
    async reach() {
      this.supplyOnce('A:0')
      this.armBeforeJoin = async () => {
        this.gate = this.holdFrame(
          78,
          ({ node, fields }) => node === 1 && fields.shareSessionEpoch === 2
        )
      }
      await this.begin({ expectCalls: 1, supply: ['A:0'] })
      await this.advanceUntil(() => this.gate.call !== null, 'the epoch-two frame')
      await this.arrival(this.gate)
    },
    /** @returns {Promise<void>} Applies the declared fault to its captured original provider. */
    async inject() {
      this.socketFor(this.gate.call).emit('end')
      await flush()
    },
    /** @returns {Promise<void>} Checks the exact literal wire sequence, outcomes and replacement authority. */
    async verify() {
      await this.advanceUntil(() => this.frameLog(1).length >= 4, 'the reopening frame')
      assert.deepEqual(
        this.frameLog(1)
          .slice(2, 4)
          .map(({ fields }) => fields.shareSessionEpoch),
        this.row.expected.epochs
      )
      assert(this.frameLog(1)[3].generation > this.frameLog(1)[2].generation)
      assert.equal(this.seen.resets.length, 1)
    },
  },
  'same generation different pool refuses old work': {
    /** @returns {Promise<void>} Reaches the original production operation with its literal supply and gates. */
    async reach() {
      this.armBeforeJoin = async () => {
        this.gate = this.holdFrame(78, ({ node }) => node === 1)
      }
      await this.begin({ expectCalls: 1 })
      await this.arrival(this.gate)
    },
    /** @returns {Promise<void>} Applies the declared fault to its captured original provider. */
    async inject() {
      this.written = this.frameLog(1).length
      const loop = [...this.observer.loops.keys()].find(
        (candidate) => candidate.session.binding?.broker.nodeId === 1
      )
      this.oldBinding = loop.session.binding
      const { broker, pool, generation } = this.oldBinding
      const replacement = await loop.cluster.brokerPool.buildConnectionPool({
        host: pool.host,
        port: pool.port,
        rack: pool.rack,
      })
      try {
        await replacement.pool[1].connect()
        assert.equal(replacement.generationFor(78), generation)
        broker.connectionPool = replacement
        assert.notEqual(broker.connectionPool, pool)
        assert.equal(this.oldBinding.connection.isConnected(), true)
        assert.equal(ShareSession.bindingLost(this.oldBinding), true)
        assert.equal(
          loop.session.refusal(loop.session.token).message,
          'Share session connection was lost'
        )
        this.gate.release()
        await flush()
      } finally {
        await (broker.connectionPool === replacement ? pool : replacement).destroy({
          drainTimeout: 0,
        })
      }
    },
    /** @returns {Promise<void>} Checks the exact literal wire sequence, outcomes and replacement authority. */
    async verify() {
      assert.equal(
        this.frameLog(1)
          .slice(this.written)
          .filter((record) => this.socketFor(record) === this.socketFor(this.gate.call)).length,
        this.row.expected.oldWrites
      )
      this.h.table.set(NAME, { topicId: IDS.B, partitions: [{ leader: 0, epoch: 2 }] })
      this.supplyOnce('B:0')
      const gate = await this.holdBeatSteady()
      gate.release({
        errorCode: 0,
        errorMessage: null,
        memberId: gate.call.fields.memberId,
        memberEpoch: 2,
        heartbeatIntervalMs: 50,
        assignment: { topicPartitions: [{ topicId: IDS.B, partitions: [0] }] },
      })
      await until(
        () =>
          this.observer.assignments.some(({ assignment }) =>
            assignment.some(({ topicId }) => topicId === IDS.B)
          ),
        'the B assignment'
      )
      this.release(A0_H)
      await this.advanceUntil(
        () => this.calls.some((call) => call.identity === this.row.expected.replacement[0]),
        'the replacement delivery'
      )
    },
  },
  'same offset clock epoch different acquisition': {
    /** @returns {Promise<void>} Reaches the original production operation with its literal supply and gates. */
    async reach() {
      this.callTimes = new Map()
      this.onStart = (call) => this.callTimes.set(call, Date.now())
      await this.begin({ expectCalls: 2, supply: ['A:0', 'A:1'], hold: [A0_H, A1_H] })
      this.old = this.callOf(A1_H)
      this.fixedDate = this.callTimes.get(this.old)
      jest.spyOn(Date, 'now').mockReturnValue(this.fixedDate)
      this.renewal = track(this.old.renew())
      await flush()
    },
    /** @returns {Promise<void>} Applies the declared fault to its captured original provider. */
    async inject() {
      this.supplyOnce('A:1')
      const gate = this.holdFrame(79, ({ node }) => node === 1)
      this.renewalTwo = track(
        this.expectRejection(this.old.renew(), {
          name: 'KafkaJSShareAcknowledgementError',
          message: 'Acknowledgement was not confirmed',
          reason: 'session-lost',
        })
      )
      await this.arrival(gate)
      gate.release(ackAnswer([], { errorCode: 122 }))
      await until(
        () => this.seen.resets.some(({ nodeId }) => nodeId === 1),
        'the old acquisition reset'
      )
      this.release(A1_H)
      await flush()
    },
    /** @returns {Promise<void>} Checks the exact literal wire sequence, outcomes and replacement authority. */
    async verify() {
      await this.advanceUntil(
        () => this.calls.filter((call) => call.identity === A1_H).length === 2,
        'the reacquisition'
      )
      const [old, fresh] = this.calls.filter((call) => call.identity === A1_H)
      assert.notEqual(old.token, fresh.token)
      assert.equal(this.callTimes.get(fresh), this.fixedDate)
      assert.equal(fresh.offset, old.offset)
      assert.deepEqual(
        this.frameLog(1)
          .filter(({ fields }) => fields.shareSessionEpoch === 0)
          .map(({ fields }) => fields.shareSessionEpoch),
        [0, 0]
      )
      assert.throws(() => old.acknowledge('accept'), { message: STALE_CALL })
      assert.equal(this.row.expected.distinct, true)
      assert.equal(fresh.isRunning(), true)
    },
  },
  'last unwanted established node closes once': {
    async reach() {
      await this.begin({ expectCalls: 1, supply: ['A:0'] })
      await this.advanceUntil(() => this.frameLog(1).length >= 2, 'node 1 established')
    },
    async inject() {
      await this.removeNode1()
    },
    async verify() {
      await this.advanceUntil(() => this.finals().length >= 1, 'the final')
      const finals = this.finals().filter(({ node }) => node === 1)
      assert.equal(finals.length, this.row.expected.finals)
      assert.equal(finals[0].apiKey, this.row.expected.api)
      assert.deepEqual(
        finals.map(({ fields }) => fields.shareSessionEpoch),
        this.row.expected.epochs
      )
      await this.pump(200)
      assert.equal(
        this.frameLog(1).filter(({ fields }) => fields.shareSessionEpoch === 0).length,
        1
      )
    },
  },
  'last unwanted unopened node sends nothing': {
    async reach() {
      this.armBeforeJoin = async () => {
        this.gate = this.holdFrame(78, ({ node }) => node === 1)
      }
      await this.begin({ expectCalls: 1, supply: ['A:0'] })
      await this.arrival(this.gate)
    },
    async inject() {
      this.written = this.frameLog(1).length
      await this.removeNode1()
    },
    async verify() {
      await this.pump(200)
      assert.equal(this.frameLog(1).length - this.written, this.row.expected.frames)
      assert.equal(this.finals().filter(({ node }) => node === 1).length, this.row.expected.finals)
    },
  },
  'last ack and forget share frame': forgetting('same frame'),
  'new ack only partition also forgotten': forgetting('ack only'),
  'first fetch after flush forgets': forgetting('first fetch'),
  'renewed held partition remains until terminal': forgetting('renewed'),
  'empty round minimum fifty milliseconds': {
    /** @returns {Promise<void>} Holds the original empty fetch response. */
    async reach() {
      this.armBeforeJoin = async () => {
        this.gate = this.holdFrame(78, ({ node }) => node === 1)
      }
      await this.begin({
        expectCalls: 1,
        supply: ['A:0'],
        startOptions: { share: this.emptyOptions ?? {} },
      })
      await this.arrival(this.gate)
      this.sentAt = Date.now()
    },
    /** @returns {Promise<void>} Proves the send-to-send minimum with a delayed response. */
    async verify() {
      const before = this.frameLog(1).length
      await this.pump(5)
      this.gate.release(fetchAnswer([]))
      await flush()
      await this.pump(44)
      assert.equal(Date.now() - this.sentAt, 49)
      assert.equal(this.frameLog(1).length, before, 'no frame at send time plus 49')
      await this.pump(1)
      await until(() => this.frameLog(1).length === before + 1, 'the fifty-millisecond fetch')
      assert.equal(Date.now() - this.sentAt, 50)
      assert.equal(
        this.frameLog(1).at(-1).fields.maxWaitMs,
        this.emptyOptions?.maxWaitTimeInMs ?? 500
      )
      assert.equal(this.frameLog(1).at(-1).fields.minBytes, this.emptyOptions?.minBytes ?? 1)
      if (!this.isCompanion) {
        await this.companion({ emptyOptions: { maxWaitTimeInMs: 0 } })
        await this.companion({ emptyOptions: { minBytes: 0 } })
      }
    },
  },
  'independent node capacities and no prefetch': {
    async reach() {
      this.hold(A0_H)
      this.hold(A1_H)
      this.supplyOnce('A:0', 'A:1')
      await this.start({ retry: RETRY_250, share: { maxRecords: 2 } })
      await this.join()
      await until(() => this.calls.length >= 2, "both nodes' handlers")
      this.counts = [0, 1].map((node) => this.frameLog(node).length)
    },
    async verify() {
      await this.pump(300)
      const { expected } = this.row
      assert.deepEqual(
        [0, 1].map((node) => this.frameLog(node).length),
        this.counts,
        'no node prefetches while its handler runs'
      )
      assert.equal(this.row.expected.blockedNodeWrites, 0)
      const held = this.calls.filter((call) => call.alias === 'A')
      assert.equal(held.length <= expected.totalMax, true)
      assert.equal(new Set(held.map((call) => call.partition)).size, expected.nodes)
    },
  },
  'final errors never reopen': {
    /** @returns {Promise<void>} Reaches the original production operation with its literal supply and gates. */
    async reach() {},
    /** @returns {Promise<void>} Checks the exact literal wire sequence, outcomes and replacement authority. */
    async verify() {
      for (const code of this.row.expected.codes) {
        const scenario = new SessionFaultCase({
          fault: 'final',
          step: 'closing',
          expected: {
            ...this.row.expected,
            rounds: 8,
            observations: literalObservations(OBSERVATIONS.final),
          },
        })
        const failures = []
        try {
          await scenario.finalWith(code)
          await scenario.assertInvariants()
        } catch (error) {
          failures.push(error)
        }
        try {
          await scenario.dispose()
        } catch (error) {
          failures.push(error)
        }
        for (const check of [
          () => scenario.assertBaseline(),
          () => scenario.cluster?.assertClean(),
        ]) {
          try {
            await check()
          } catch (error) {
            failures.push(error)
          }
        }
        if (failures.length)
          throw new AggregateError(failures, `Final fault subcase ${code} failed`)
      }
    },
  },
}

/**
 * @param {'same frame'|'ack only'|'first fetch'|'renewed'} kind Which forgetting rule is read.
 * @returns {object} A mechanics scenario with a held node-1 invocation and A2's exact terminal set.
 */
function forgetting(kind) {
  return {
    /** @returns {Promise<void>} Holds node 1 before the selected partition can complete. */
    async reach() {
      const renewed = kind === 'renewed'
      this.supplyOnce('A:0', 'A:1', 'A:2')
      this.hold(A0_H)
      this.hold(renewed ? `A:2@${H}` : A1_H)
      await this.start({ retry: RETRY_250 })
      if (kind === 'ack only') {
        this.beatShape = (fields, answer) => ({
          ...answer,
          assignment: { topicPartitions: [{ topicId: IDS.A, partitions: [0, 1] }] },
        })
        this.gate = this.holdFrame(
          78,
          ({ node, fields }) => node === 1 && fields.shareSessionEpoch === 0
        )
      }
      await this.join()
      if (kind === 'ack only') {
        await this.arrival(this.gate)
        assert.deepEqual(listed(this.gate.call.fields), [{ topicId: IDS.A, partition: 1 }])
        this.supply.delete('A:1')
        this.supply.delete('A:2')
        this.gate.release(fetchAnswer([dataPart(1), dataPart(2)]))
      }
      await until(
        () => this.calls.some(({ identity }) => identity === (renewed ? `A:2@${H}` : A1_H)),
        'the selected held handler'
      )
      this.loop = [...this.observer.loops.keys()].find(
        (loop) => loop.session.binding?.broker.nodeId === 1
      )
      if (kind === 'ack only')
        assert.equal(this.loop.session.model.has(`${IDS.A}:2`), false, 'A2 was never listed')
      if (renewed) {
        this.renewed = track(this.calls.find(({ identity }) => identity === `A:2@${H}`).renew())
        await until(() => this.renewed.settled, 'the renewal')
        assert.equal(this.renewed.error, null)
      }
    },
    /** @returns {Promise<void>} Revokes A2 and makes terminal work eligible at the declared boundary. */
    async inject() {
      if (kind !== 'ack only') {
        const gate = await this.holdBeatSteady()
        gate.release({
          errorCode: 0,
          errorMessage: null,
          memberId: gate.call.fields.memberId,
          memberEpoch: 1,
          heartbeatIntervalMs: 50,
          assignment: { topicPartitions: [{ topicId: IDS.A, partitions: [0, 1] }] },
        })
        await flush()
      }
      if (kind === 'renewed') {
        const before = this.frameLog(1).length
        await this.pump(100)
        assert.equal(this.frameLog(1).length, before, 'held A2 prevents a fetching request')
        assert.equal(this.loop.session.model.has(`${IDS.A}:2`), true)
      }
      if (kind === 'first fetch') await this.pump(7501)
      this.beforeRelease = this.frames(78, 79).length
      this.release(kind === 'renewed' ? `A:2@${H}` : A1_H)
    },
    /** @returns {Promise<void>} Reads the exact terminal, forget and subsequent-fetch transcript. */
    async verify() {
      const { expected } = this.row
      const forgotten = (frame) =>
        (frame.fields.forgottenTopicsData || []).flatMap((topic) =>
          topic.partitions.map((partition) => `${ALIAS.get(topic.topicId)}:${partition}`)
        )
      await this.advanceUntil(
        () => this.frameLog(1).some((frame) => forgotten(frame).includes('A:2')),
        'the frame that forgets A2'
      )
      const frame = this.frameLog(1).find((candidate) => forgotten(candidate).includes('A:2'))
      const beforeForget = this.frames(78, 79).slice(0, this.frames(78, 79).indexOf(frame))
      if (kind === 'renewed') {
        assert.deepEqual(beforeForget.flatMap(forgotten), expected.renewForgotten)
        assert.deepEqual(forgotten(frame), expected.terminalForgotten)
      } else {
        assert.deepEqual(forgotten(frame), expected.forgotten)
        assert.deepEqual(
          listed(frame.fields)
            .filter(({ partition }) => partition === 2)
            .map(({ partition }) => `A:${partition}`),
          ['A:2']
        )
      }
      assert.equal(frame.apiKey, 78)
      const acks = frame.fields.topics.flatMap(({ topicId, partitions }) =>
        partitions
          .filter(({ partitionIndex }) => topicId === IDS.A && partitionIndex === 2)
          .flatMap(({ acknowledgementBatches }) => acknowledgementBatches)
      )
      assert.deepEqual(acks, [
        { firstOffset: H, lastOffset: J, acknowledgeTypes: kind === 'renewed' ? [1, 2] : [2] },
      ])
      if (kind === 'first fetch') {
        const frames = this.frames(78, 79)
          .slice(this.beforeRelease)
          .filter(({ node }) => node === 1)
        const index = frames.indexOf(frame)
        assert(index > 0, 'a flush precedes the forgetting fetch')
        assert.deepEqual(
          frames.slice(index - 1, index + 1).map(({ apiKey }) => apiKey),
          expected.order
        )
        assert.equal(
          frames.slice(0, index).some(({ apiKey }) => apiKey === 78),
          false
        )
      }
      const written = this.frameLog(1).length
      await this.advanceUntil(() => this.frameLog(1).length > written, 'the following fetch')
      const later = this.frameLog(1).slice(this.frameLog(1).indexOf(frame) + 1)
      assert.deepEqual(
        later.flatMap((candidate) =>
          listed(candidate.fields)
            .filter(({ partition }) => partition === 2)
            .map(() => 'A:2')
        ),
        []
      )
      const terminal = this.terminalFor(2, H)
      assert.equal(terminal.length, 1)
      assert.equal(terminal[0].error, null)
      if (kind !== 'renewed')
        assert.equal(
          this.calls.some(({ partition }) => partition === 2),
          false
        )
    },
  }
}

SessionFaultCase.prototype.removeNode1 = async function removeNode1() {
  const gate = await this.holdBeatSteady()
  gate.release({
    errorCode: 0,
    errorMessage: null,
    memberId: gate.call.fields.memberId,
    memberEpoch: 1,
    heartbeatIntervalMs: 50,
    assignment: { topicPartitions: [{ topicId: IDS.A, partitions: [0] }] },
  })
  await flush()
}

/**
 * Closes node 1's session with a final answered by `code` and checks it never reopens.
 * @param {number} code The final's top-level error.
 * @returns {Promise<void>}
 */
SessionFaultCase.prototype.finalWith = async function finalWith(code) {
  await this.begin({ expectCalls: 1, supply: ['A:0'] })
  await this.advanceUntil(() => this.frameLog(1).length >= 2, 'node 1 established')
  const gate = this.holdFrame(
    79,
    ({ node, fields }) => node === 1 && fields.shareSessionEpoch === -1
  )
  const closing = track(this.h.consumer.disconnect({ timeout: 400 }))
  await this.pump(300)
  await this.arrival(gate)
  gate.release(ackAnswer([], { errorCode: code }))
  await until(() => closing.settled, 'the disconnect')
  const finals = this.finals().filter(({ node }) => node === 1)
  assert.equal(finals.length, 1)
  assert.deepEqual(
    this.seen.resets.filter(({ reason }) => reason !== 'membership'),
    []
  )
  assert.equal(
    this.frameLog(1).filter(({ fields }) => fields.shareSessionEpoch === 0).length,
    1,
    'a final answer never reopens'
  )
}

describe('share session faults', () => {
  afterEach(() => {
    jest.useRealTimers()
    jest.restoreAllMocks()
  })

  it.each(rows)('$fault at $step (row $#)', async (row) => {
    const scenario = new SessionFaultCase(row)
    const failures = []
    try {
      await scenario.reach()
      await scenario.inject()
      await scenario.verify()
      await scenario.replacement()
      await scenario.assertInvariants()
    } catch (error) {
      failures.push(error)
    }
    for (const finish of [() => scenario.dispose(), () => scenario.assertBaseline()]) {
      try {
        await finish()
      } catch (error) {
        failures.push(error)
      }
    }
    try {
      scenario.cluster?.assertClean()
    } catch (error) {
      failures.push(error)
    }
    if (failures.length) throw new AggregateError(failures, 'Share fault case failed')
  })
})

describe('session fault observations', () => {
  test('retains every literal scenario once', () => {
    expect(rows).toHaveLength(98)
    expect(
      new Set(rows.map((row) => `${row.fault} / ${row.step} / ${row.expected.enforce ?? ''}`)).size
    ).toBe(98)
    expect(rows.every((row) => row.expected.rounds >= 8)).toBe(true)
    expect(ROW_OBSERVATIONS).toHaveLength(98)
    for (const { expected } of rows)
      expect(Object.keys(expected.observations).sort()).toEqual([
        'handlers',
        'mutations',
        'settlements',
      ])
  })

  test('rejects opening acknowledgements seen by the independent reader', () => {
    const audit = new FaultAudit()
    const fields = {
      shareSessionEpoch: 0,
      topics: [
        {
          topicId: 'A',
          partitions: [
            {
              partitionIndex: 0,
              acknowledgementBatches: [
                {
                  firstOffset: '9007199254740993',
                  lastOffset: '9007199254740993',
                  acknowledgeTypes: [1],
                },
              ],
            },
          ],
        },
      ],
    }
    expect(() => audit.frame({ apiKey: 78, fields }, { allowed: true })).toThrow('opening')
  })
  test('allows last acknowledgement and forget in the same frame', () => {
    const audit = new FaultAudit()
    const authority = {
      allowed: true,
      kind: 'ordinary',
      open: Symbol('open'),
      wanted: new Set(),
    }
    const fields = {
      shareSessionEpoch: 2,
      maxBytes: 1024,
      maxRecords: 2,
      topics: [
        {
          topicId: 'A',
          partitions: [{ partitionIndex: 2, acknowledgementBatches: [] }],
        },
      ],
      forgottenTopicsData: [{ topicId: 'A', partitions: [2] }],
    }
    audit.frame({ apiKey: 78, fields }, authority)
    expect(() => audit.frame({ apiKey: 78, fields }, authority)).toThrow('forgotten')
    authority.wanted.add('A:2')
    expect(() =>
      audit.frame({ apiKey: 78, fields: { ...fields, forgottenTopicsData: [] } }, authority)
    ).not.toThrow()
  })
  test('requires closing authority for a final acknowledgement', () => {
    const audit = new FaultAudit()
    const record = {
      apiKey: 79,
      fields: { shareSessionEpoch: -1, topics: [], isRenewAck: false },
    }
    expect(() => audit.frame(record, { allowed: true, kind: 'ordinary' })).toThrow()
    expect(() =>
      audit.frame(record, { allowed: true, kind: 'closing', episode: Symbol('episode') })
    ).not.toThrow()
    expect(() => audit.frame(record, { allowed: false, kind: 'closing' })).toThrow('unauthorized')
  })
  test('rejects a renewal sent as a fetching request', () => {
    const audit = new FaultAudit()
    expect(() =>
      audit.frame(
        {
          apiKey: 78,
          fields: { shareSessionEpoch: 1, topics: [], isRenewAck: true },
        },
        { allowed: true }
      )
    ).toThrow()
  })
  test('rejects a second final from the same captured episode and node', () => {
    const audit = new FaultAudit()
    const record = {
      node: 1,
      apiKey: 79,
      fields: { shareSessionEpoch: -1, topics: [], isRenewAck: false },
    }
    const authority = {
      allowed: true,
      kind: 'closing',
      episode: Symbol('episode'),
    }
    audit.frame(record, authority)
    expect(() => audit.frame(record, authority)).toThrow('duplicate final')
  })
})
