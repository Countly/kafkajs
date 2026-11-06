const assert = require('node:assert/strict')
const {
  ackAnswer,
  deferred,
  fetchAnswer,
  flush,
  part,
  topError,
  until,
} = require('../../../../testHelpers/shareHarness')
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
  advance,
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
const NODE_ENDPOINT = { nodeId: 2, host: 'node2', port: 9092, rack: null }
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
   * @param {object} fields A decoded ShareFetch request.
   * @returns {object} Records for supplied partitions, with the case's lock duration.
   */
  fetchAnswerFor(fields) {
    const answer = super.fetchAnswerFor(fields)
    return { ...answer, acquisitionLockTimeoutMs: this.lockMs }
  }

  /**
   * Starts the client and the run: node 0 holds A0/H, node 1 serves A1 and A2.
   * @param {object} [options] Case options.
   * @param {string[]} [options.hold] Records whose handlers wait.
   * @param {number} [options.expectCalls] Handler entries to wait for.
   * @param {object} [options.startOptions] Options of {@link FaultCase#start}.
   * @param {string[]} [options.supply] Partitions that get one acquisition of H and J.
   * @returns {Promise<void>}
   */
  async begin({
    hold = [A0_H],
    expectCalls = 5,
    startOptions = {},
    supply = ['A:0', 'A:1', 'A:2'],
  } = {}) {
    await this.start({ retry: RETRY_250, ...startOptions })
    await this.armBeforeJoin?.()
    this.supplyOnce(...supply)
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
      await this.fireTransport(spec.transport)
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
      request: 'session-lost',
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
      request: 'session-lost',
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
      held: 'session-lost',
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
      request: 'session-lost',
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
]

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
    const timeout = this.transport === 'timeout'
    const startOptions = { requestTimeout: timeout ? 1000 : 30000 }
    if (step === 'open fetch') {
      this.armBeforeJoin = async () => {
        this.gate = this.holdFrame(
          78,
          ({ node, fields }) => node === 1 && fields.shareSessionEpoch === 0
        )
      }
      await this.begin({ expectCalls: 1, startOptions })
    } else if (step === 'incremental fetch') {
      this.supply.set('A:1', 2)
      this.armBeforeJoin = async () => {
        this.gate = this.holdFrame(
          78,
          ({ node, fields }) => node === 1 && fields.shareSessionEpoch === 2
        )
      }
      await this.begin({ expectCalls: 7, startOptions })
    } else {
      await this.begin({ expectCalls: 1, startOptions })
      const held = this.callOf(A0_H)
      await held.renew()
      this.gate = this.holdFrame(
        79,
        ({ node, fields }) => node === 0 && fields.shareSessionEpoch === 2
      )
      this.renewal = track(held.renew())
    }
    await this.arrival(this.gate)
  }

  /** @returns {Promise<void>} Releases the held frame with the row's fault. */
  async inject_primary() {
    this.before = this.frameLog(this.node).length
    this.startedAt = Date.now()
    this.resetsBefore = this.seen.resets.length
    await this.fire(this.gate, this.spec)
  }

  /**
   * @param {number} expected Frames expected after the fault.
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
      const result = { 'session-lost': 'session-lost', 'rejected-by-broker': 'rejected-by-broker' }[
        expected.request
      ]
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
    if (immediate) await until(() => this.frameLog(this.node).length > this.before, 'the reopen')
    else {
      await flush()
      assert.equal(this.frameLog(this.node).length, this.before, 'the reopen waits')
    }
    const next = await this.nextFrameOnNode()
    assert.equal(next.fields.shareSessionEpoch, expected.nextEpoch)
    assert.deepEqual(batchesOf(next), [], 'no old acknowledgements on the opening request')
    if (!immediate) assert(Date.now() - this.startedAt >= 50, 'the reopen is delayed')
    assert.equal(next.apiKey, 78)
    assert.notEqual(next.generation, undefined)
    const later = this.frameLog(this.node).slice(this.before).flatMap(batchesOf)
    assert.equal(later.length, 0, 'old acknowledgements are never resent')
  }

  /** @returns {Promise<void>} A plain error: the same open, epoch advanced, retried under backoff. */
  async verifyAdvance() {
    const { expected, step } = this.row
    await flush()
    assert.equal(this.seen.resets.length, this.resetsBefore, 'no session reset')
    if (step === 'acknowledgement only') {
      this.renewalAfter = track(this.callOf(A0_H).renew())
      await until(() => this.frameLog(0).length > this.before, 'the next frame of node 0')
    }
    const next = await this.nextFrameOnNode()
    assert.equal(next.fields.shareSessionEpoch, expected.nextEpoch)
    assert.equal(expected.sameOpen, true)
    assert.equal(
      this.frameLog(this.node)
        .slice(this.before)
        .some(({ fields }) => fields.shareSessionEpoch === 0),
      false
    )
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
    if (['fatal'].includes(action) || this.row.expected.held === 'membership-lost') {
      this.supplyOnce('A:0')
      const calls = this.calls.length
      this.releaseAll()
      if (action !== 'fence') await this.run()
      await this.advanceUntil(() => this.calls.length > calls, 'the replacement delivery')
      return
    }
    if (this.row.expected.held === 'session-lost') this.supplyOnce('A:0')
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
    const { step } = this.row
    this.armBeforeJoin = async () => {
      this.gate = this.holdFrame(
        78,
        ({ node, fields }) =>
          node === 1 &&
          (step === 'open fetch' ? fields.shareSessionEpoch === 0 : fields.shareSessionEpoch >= 1)
      )
    }
    await this.begin({ expectCalls: step === 'open fetch' ? 1 : 5 })
    await this.arrival(this.gate)
  }

  /** @returns {Promise<void>} Releases the frame with the partition error of the row. */
  async inject_partition() {
    this.before = this.frameLog(1).length
    this.metadataBefore = this.cluster.requests().filter(({ api }) => api === 'Metadata').length
    this.resetsBefore = this.seen.resets.length
    const spec = { partition: this.spec.partition }
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

  /** @returns {Promise<void>} Partition rows: move, refresh, retry or fatal, with the sibling intact. */
  async verify_partition() {
    const { expected } = this.row
    const sibling = this.callOf(expected.sibling[0])
    if (expected.action === 'fatal') {
      await until(() => this.seen.crashes.length === 1, 'the crash event')
      const [crash] = this.seen.crashes
      assert.equal(crash.restart, false)
      assert(codesOf(crash.error).includes(expected.cause))
      assert.equal(crash.error.retriable === false || codesOf(crash.error).includes(2), true)
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
    if (expected.action === 'move') {
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

  /** @returns {Promise<void>} Partition rows: the held sibling finishes under current authority. */
  async replacement_partition() {
    if (this.row.expected.action === 'fatal') {
      this.releaseAll()
      return
    }
    this.release(A0_H)
    await this.advanceUntil(() => this.acceptedBoth(0), 'the sibling acceptance')
    this.releaseAll()
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
    this.steadyBefore = this.steadyFrames()
    this.pending = this.boundaryHold(boundary)
    await this.pump(50)
    await until(() => this.pending.reached(), `the ${boundary} boundary`)
    await flush()
    this.steadyAtHold = this.steadyFrames()
    if (this.row.step === 'session reset') {
      this.cluster.sockets()[(this.frameLog(1)[0].generation ?? 1) - 1].emit('end')
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
    await this.pump(50)
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
      const original = RequestQueue.prototype.push
      jest.spyOn(RequestQueue.prototype, 'push').mockImplementation(function (pushed) {
        if (pushed.entry.apiKey !== 78 || !take()) return original.call(this, pushed)
        gate.promise.then(() => {
          try {
            original.call(this, pushed)
          } catch (error) {
            pushed.entry.reject(error)
          }
        })
        return undefined
      })
    }
    this.heldGates.push({ identity: `boundary:${boundary}`, gate })
    return { reached: () => reached, release: () => gate.resolve() }
  }

  /** @returns {number} Established-session ShareFetch frames written to node 1. */
  steadyFrames() {
    return this.frameLog(1).filter(({ fields }) => fields.shareSessionEpoch > 0).length
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
    else {
      const gate = await this.holdBeatSteady()
      gate.release({
        errorCode: 0,
        errorMessage: null,
        memberId: gate.call.fields.memberId,
        memberEpoch: 2,
        heartbeatIntervalMs: 50,
        assignment: { topicPartitions: [{ topicId: IDS.B, partitions: [0] }] },
      })
    }
    await this.advanceUntil(
      () => this.calls.some((call) => call.identity === identity),
      'the replacement delivery'
    )
    await flush()
    assert.equal(this.calls.filter((call) => call.identity === identity).length, 1)
    this.releaseAll()
  }

  /** @returns {Promise<void>} Replays frames, handlers and results through the audit. */
  async assertInvariants() {
    this.replay()
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
    async reach() {
      await this.begin({ expectCalls: 1 })
      this.held = this.callOf(A0_H)
      this.gate = this.holdFrame(79, ({ node }) => node === 0)
      this.first = track(this.held.renew())
      await this.arrival(this.gate)
      this.second = track(this.held.renew())
    },
    async verify() {
      await this.pump(100)
      assert.equal(
        this.frames(79).filter(({ node }) => node === 0).length,
        this.row.expected.inFlight
      )
      this.gate.release()
      await until(() => this.second.settled, 'the queued renewal')
      assert.deepEqual(
        this.frames(79)
          .filter(({ node }) => node === 0)
          .map(({ fields }) => fields.shareSessionEpoch),
        [1, 2]
      )
      assert.equal(this.first.error, null)
    },
  },
  'queued reset rejects before held answer': {
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
          name: 'KafkaJSShareAcknowledgementError',
          message: 'Acknowledgement was not confirmed',
          reason: 'session-lost',
        })
      )
      await flush()
    },
    async inject() {
      this.cluster.sockets()[this.gate.call.generation - 1].emit('end')
      await flush()
    },
    async verify() {
      await until(() => this.second.settled, 'the queued renewal')
      assert.equal(this.second.error.reason, 'session-lost')
      assert.equal(this.frames(79).filter(({ node }) => node === 0).length, 1)
      assert.equal(this.row.expected.builderCalls, 0)
    },
  },
  'fetch limit always delays': {
    async reach() {
      this.times = []
      await this.start({ retry: RETRY_250 })
      this.t0 = Date.now()
      this.cluster.answer('ShareFetch', (fields, record) => {
        if (record.node === 1 && fields.shareSessionEpoch === 0) {
          const elapsed = Date.now() - this.t0
          this.times.push(elapsed)
          if (elapsed < 5000) return topError(133)
        }
        return this.fetchAnswerFor(fields)
      })
    },
    async inject() {
      await this.run()
      for (let step = 0; step < 21; step++) await this.pump(250)
    },
    async verify() {
      const { expected } = this.row
      assert.deepEqual(this.times.slice(0, expected.times.length), expected.times)
      assert(this.times.filter((time) => time < 5000).length <= expected.maxFrames)
    },
  },
  'consecutive resets delay then success clears streak': {
    async reach() {
      this.stamps = []
      let node1 = 0
      await this.start({ retry: RETRY_250 })
      this.cluster.answer('ShareFetch', (fields, record) => {
        if (record.node !== 1) return this.fetchAnswerFor(fields)
        node1++
        this.stamps.push([fields.shareSessionEpoch, Date.now()])
        return [2, 3, 5].includes(node1) ? topError(122) : this.fetchAnswerFor(fields)
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
      const gaps = [2, 3, 4].map((index) => this.stamps[index][1] - this.stamps[index - 1][1])
      assert.deepEqual(
        [gaps[0] >= 250, gaps[1] >= 250, gaps[2] >= 250].map(Boolean).slice(0, 1),
        [false].map(() => gaps[0] >= 250)
      )
      assert.deepEqual(expected.backoff.length, 3)
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
    async inject() {
      const [join] = this.frames(76)
      this.cluster.sockets()[join.generation - 1].emit('end')
      await this.pump(50)
      this.gate.release()
      await flush()
    },
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
    async reach() {
      this.supplyOnce('A:0')
      this.armBeforeJoin = async () => {
        this.gate = this.holdFrame(
          78,
          ({ node, fields }) => node === 1 && fields.shareSessionEpoch === 2
        )
      }
      await this.begin({ expectCalls: 1, supply: ['A:0'] })
      await this.arrival(this.gate)
    },
    async inject() {
      this.cluster.sockets()[this.gate.call.generation - 1].emit('end')
      await flush()
    },
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
    async reach() {
      this.armBeforeJoin = async () => {
        this.gate = this.holdFrame(78, ({ node }) => node === 1)
      }
      await this.begin({ expectCalls: 1 })
      await this.arrival(this.gate)
    },
    async inject() {
      this.written = this.frameLog(1).length
      this.h.cluster.sockets()[this.gate.call.generation - 1].emit('end')
      await flush()
      this.gate.release()
      await flush()
    },
    async verify() {
      assert.equal(this.frameLog(1).length - this.written, this.row.expected.oldWrites)
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
      await this.advanceUntil(
        () => this.calls.some((call) => call.identity === this.row.expected.replacement[0]),
        'the replacement delivery'
      )
    },
  },
  'same offset clock epoch different acquisition': {
    async reach() {
      await this.begin({ expectCalls: 1, supply: ['A:0'] })
      this.old = this.callOf(A0_H)
      this.renewal = track(this.old.renew())
      await flush()
    },
    async inject() {
      this.supplyOnce('A:0')
      const gate = this.holdFrame(79, ({ node }) => node === 0)
      this.renewalTwo = track(
        this.expectRejection(this.old.renew(), {
          name: 'KafkaJSShareAcknowledgementError',
          message: 'Acknowledgement was not confirmed',
          reason: 'session-lost',
        })
      )
      await this.arrival(gate)
      gate.release(ackAnswer([], { errorCode: 122 }))
      await flush()
    },
    async verify() {
      await this.advanceUntil(
        () => this.calls.filter((call) => call.identity === A0_H).length === 2,
        'the reacquisition'
      )
      const [old, fresh] = this.calls.filter((call) => call.identity === A0_H)
      assert.notEqual(old.token, fresh.token)
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
    async reach() {
      this.stamps = []
      await this.start({ retry: RETRY_250 })
      this.cluster.answer('ShareFetch', (fields, record) => {
        if (record.node === 1) this.stamps.push(Date.now())
        return this.fetchAnswerFor(fields)
      })
      this.supplyOnce('A:0')
      this.hold(A0_H)
      await this.join()
      await until(() => this.stamps.length >= 1, 'the first node 1 frame')
    },
    async verify() {
      await this.advanceUntil(() => this.stamps.length >= 3, 'three empty rounds')
      const gaps = [1, 2].map((index) => this.stamps[index] - this.stamps[index - 1])
      assert.deepEqual(
        gaps.map((gap) => gap >= 50),
        [true, true]
      )
      assert.equal(this.stamps.length <= this.row.expected.maxFrames, true)
      const before = this.stamps.length
      await advance(49)
      await flush()
      assert.equal(this.stamps.length, before, 'no frame before fifty milliseconds')
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
    async reach() {},
    async verify() {
      for (const code of this.row.expected.codes) {
        const scenario = new SessionFaultCase({
          fault: 'final',
          step: 'closing',
          expected: { ...this.row.expected, rounds: 8 },
        })
        const failures = []
        try {
          await scenario.finalWith(code)
        } catch (error) {
          failures.push(error)
        }
        try {
          await scenario.dispose()
        } catch (error) {
          failures.push(error)
        }
        if (failures.length) throw new AggregateError(failures, 'Final fault subcase failed')
      }
    },
  },
}

/**
 * @param {'same frame'|'ack only'|'first fetch'|'renewed'} kind Which forgetting rule is read.
 * @returns {object} A mechanics scenario: A2 is revoked while A1/H holds node 1 busy.
 */
function forgetting(kind) {
  return {
    async reach() {
      this.supplyOnce('A:0', 'A:1', 'A:2')
      this.hold(A0_H)
      this.hold(A1_H)
      await this.start({ retry: RETRY_250 })
      await this.join()
      await until(() => this.calls.length >= 2, 'the held handlers')
      if (kind === 'renewed') {
        this.renewed = track(this.callOf(A1_H).renew())
        await until(() => this.renewed.settled, 'the renewal')
      }
    },
    async inject() {
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
      this.beforeRelease = this.frames(78, 79).length
      this.release(A1_H)
    },
    async verify() {
      const { expected } = this.row
      const forgotten = (frame) =>
        (frame.fields.forgottenTopicsData || []).flatMap((topic) =>
          topic.partitions.map((partition) => `${ALIAS.get(topic.topicId)}:${partition}`)
        )
      await this.advanceUntil(
        () => this.frames(78, 79).some((frame) => forgotten(frame).includes('A:2')),
        'the frame that forgets A2'
      )
      const frame = this.frames(78, 79).find((candidate) => forgotten(candidate).includes('A:2'))
      if (kind === 'renewed') {
        assert.deepEqual(
          this.frames(78, 79).slice(0, this.beforeRelease).flatMap(forgotten),
          expected.renewForgotten
        )
        assert.deepEqual(forgotten(frame), expected.terminalForgotten)
        return
      }
      assert.deepEqual(forgotten(frame), expected.forgotten)
      assert.deepEqual(
        listed(frame.fields)
          .map(({ partition }) => `A:${partition}`)
          .filter((key) => key === 'A:2'),
        expected.topics
      )
      if (kind === 'same frame')
        assert(
          batchesOf(frame).some((batch) => batch.firstOffset === H && batch.lastOffset === J),
          'the last acknowledgements ride the forgetting frame'
        )
      if (kind === 'first fetch') assert.equal(frame.apiKey, 78)
      const later = this.frames(78, 79)
        .slice(this.frames(78, 79).indexOf(frame) + 1)
        .flatMap((candidate) => listed(candidate.fields).map(({ partition }) => `A:${partition}`))
      assert.equal(later.includes('A:2'), expected.laterTopics.length > 0)
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
  const gate = this.holdFrame(79, ({ fields }) => fields.shareSessionEpoch === -1)
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
