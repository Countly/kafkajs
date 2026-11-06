const assert = require('node:assert/strict')
const {
  ackAnswer,
  acquired,
  deferred,
  fetchAnswer,
  flush,
  part,
  recordsHex,
  topError,
  until,
} = require('../../../../testHelpers/shareHarness')
const ConnectionPool = require('../../../network/connectionPool')
const RequestQueue = require('../../../network/requestQueue')
const { requests } = require('../../../protocol/requests')
const { readRequestHeader } = require('../../../../testHelpers/fakeSocket')

const {
  FaultAudit,
  FaultCase,
  NAME,
  IDS,
  H,
  J,
  STALE_CALL,
  codesOf,
  track,
  covers,
  dataPart,
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
 * @param {{code?: number, partition?: number, ack?: number, omit?: boolean, duplicate?: boolean,
 *   target?: number, sibling?: boolean}} spec
 *   `code` is a top-level error; `partition` is the selected partition's error (the fetch error
 *   for API 78, the acknowledgement error for API 79); `ack` is a ShareFetch acknowledge error.
 * @returns {object} The answer object for the scripted cluster.
 */
const faultAnswer = (
  api,
  fields,
  { code, partition, ack, omit, duplicate, target = 1, sibling = false }
) => {
  if (code !== undefined) return api === 78 ? topError(code) : ackAnswer([], { errorCode: code })
  const rows = listed(fields).filter(({ topicId, partition: index }) => {
    return !(omit && topicId === IDS.A && index === target)
  })
  if (sibling && !rows.some((row) => row.topicId === IDS.A && row.partition === 2))
    rows.push({ topicId: IDS.A, partition: 2 })
  const selected = (row) => row.topicId === IDS.A && row.partition === target
  const build = (row) => {
    if (api === 79) return { ...row, code: selected(row) ? partition : 0 }
    if (sibling && row.topicId === IDS.A && row.partition === 2) return dataPart(2)
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
    return {
      ...answer,
      acquisitionLockTimeoutMs: listed(fields).some(({ partition }) => partition === 1)
        ? this.lockMs
        : 15000,
    }
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

const MILLION_LAST = '9007199255740992'

/**
 * Literal row oracles. Handler tuples are [UUID, partition, offset, key hex, value hex, count];
 * mutation tuples are [UUID, partition, first, last, types]; settlement tuples are
 * [UUID, partition, first, last, type, reason, cause code]. No tuple is inferred from a provider.
 */
const rows = [
  {
    fault: 'partition 121',
    step: 'flush; held handler',
    expected: {
      action: 'flush',
      state: 'STABLE',
      rounds: 8,
      result: 'rejected-by-broker',
      cause: 121,
      reset: false,
      terminal: true,
      selected: ['A:1@9007199254740993'],
      held: ['A:0@9007199254740993'],
      revoked: [],
      oldAckResends: 0,
      nextEpoch: 3,
    },
    observations: {
      handlers: [
        [IDS.A, 0, H, '6b', '76', 2],
        [IDS.A, 0, J, '6b', '76', 2],
        [IDS.A, 1, H, '6b', '76', 2],
        [IDS.A, 1, J, '6b', '76', 2],
        [IDS.A, 2, H, '6b', '76', 2],
        [IDS.A, 2, J, '6b', '76', 2],
      ],
      mutations: [
        [IDS.A, 0, H, J, [1]],
        [IDS.A, 1, H, H, [1]],
        [IDS.A, 1, J, J, [1]],
        [IDS.A, 2, H, J, [1]],
      ],
      settlements: [
        [IDS.A, 0, H, J, 'accept', null, null],
        [IDS.A, 1, H, H, 'accept', 'rejected-by-broker', 121],
        [IDS.A, 1, J, J, 'accept', null, null],
        [IDS.A, 2, H, J, 'accept', null, null],
      ],
    },
  },
  {
    fault: 'partition 121',
    step: 'flush; held handler and revoked partition',
    expected: {
      action: 'flush',
      state: 'STABLE',
      rounds: 8,
      result: 'rejected-by-broker',
      cause: 121,
      reset: false,
      terminal: true,
      selected: ['A:1@9007199254740993'],
      held: ['A:0@9007199254740993'],
      revoked: ['A:2@9007199254740993', 'A:2@9007199254740994'],
      oldAckResends: 0,
      nextEpoch: 3,
      revokedType: 'release',
      revokedResult: 'accepted',
      revokedWire: 'eligible terminal frame',
    },
    observations: {
      handlers: [
        [IDS.A, 0, H, '6b', '76', 2],
        [IDS.A, 0, J, '6b', '76', 2],
        [IDS.A, 1, H, '6b', '76', 2],
        [IDS.A, 1, J, '6b', '76', 2],
      ],
      mutations: [
        [IDS.A, 0, H, J, [1]],
        [IDS.A, 1, H, H, [1]],
        [IDS.A, 1, J, J, [1]],
        [IDS.A, 2, H, J, [2]],
      ],
      settlements: [
        [IDS.A, 0, H, J, 'accept', null, null],
        [IDS.A, 1, H, H, 'accept', 'rejected-by-broker', 121],
        [IDS.A, 1, J, J, 'accept', null, null],
        [IDS.A, 2, H, J, 'release', null, null],
      ],
    },
  },
  {
    fault: 'partition 6',
    step: 'flush; held handler',
    expected: {
      action: 'flush',
      state: 'STABLE',
      rounds: 8,
      result: 'session-lost',
      cause: 6,
      reset: false,
      terminal: true,
      selected: ['A:1@9007199254740993'],
      held: ['A:0@9007199254740993'],
      revoked: [],
      oldAckResends: 0,
      nextEpoch: 3,
    },
    observations: {
      handlers: [
        [IDS.A, 0, H, '6b', '76', 2],
        [IDS.A, 0, J, '6b', '76', 2],
        [IDS.A, 1, H, '6b', '76', 2],
        [IDS.A, 1, J, '6b', '76', 2],
        [IDS.A, 2, H, '6b', '76', 2],
        [IDS.A, 2, J, '6b', '76', 2],
      ],
      mutations: [
        [IDS.A, 0, H, J, [1]],
        [IDS.A, 1, H, H, [1]],
        [IDS.A, 1, J, J, [1]],
        [IDS.A, 2, H, J, [1]],
      ],
      settlements: [
        [IDS.A, 0, H, J, 'accept', null, null],
        [IDS.A, 1, H, H, 'accept', 'session-lost', 6],
        [IDS.A, 1, J, J, 'accept', null, null],
        [IDS.A, 2, H, J, 'accept', null, null],
      ],
    },
  },
  {
    fault: 'partition 6',
    step: 'flush; held handler and revoked partition',
    expected: {
      action: 'flush',
      state: 'STABLE',
      rounds: 8,
      result: 'session-lost',
      cause: 6,
      reset: false,
      terminal: true,
      selected: ['A:1@9007199254740993'],
      held: ['A:0@9007199254740993'],
      revoked: ['A:2@9007199254740993', 'A:2@9007199254740994'],
      oldAckResends: 0,
      nextEpoch: 3,
      revokedType: 'release',
      revokedResult: 'accepted',
      revokedWire: 'eligible terminal frame',
    },
    observations: {
      handlers: [
        [IDS.A, 0, H, '6b', '76', 2],
        [IDS.A, 0, J, '6b', '76', 2],
        [IDS.A, 1, H, '6b', '76', 2],
        [IDS.A, 1, J, '6b', '76', 2],
      ],
      mutations: [
        [IDS.A, 0, H, J, [1]],
        [IDS.A, 1, H, H, [1]],
        [IDS.A, 1, J, J, [1]],
        [IDS.A, 2, H, J, [2]],
      ],
      settlements: [
        [IDS.A, 0, H, J, 'accept', null, null],
        [IDS.A, 1, H, H, 'accept', 'session-lost', 6],
        [IDS.A, 1, J, J, 'accept', null, null],
        [IDS.A, 2, H, J, 'release', null, null],
      ],
    },
  },
  {
    fault: 'partition 100',
    step: 'flush; held handler',
    expected: {
      action: 'flush',
      state: 'STABLE',
      rounds: 8,
      result: 'rejected-by-broker',
      cause: 100,
      reset: false,
      terminal: true,
      selected: ['A:1@9007199254740993'],
      held: ['A:0@9007199254740993'],
      revoked: [],
      oldAckResends: 0,
      nextEpoch: 3,
    },
    observations: {
      handlers: [
        [IDS.A, 0, H, '6b', '76', 2],
        [IDS.A, 0, J, '6b', '76', 2],
        [IDS.A, 1, H, '6b', '76', 2],
        [IDS.A, 1, J, '6b', '76', 2],
        [IDS.A, 2, H, '6b', '76', 2],
        [IDS.A, 2, J, '6b', '76', 2],
      ],
      mutations: [
        [IDS.A, 0, H, J, [1]],
        [IDS.A, 1, H, H, [1]],
        [IDS.A, 1, J, J, [1]],
        [IDS.A, 2, H, J, [1]],
      ],
      settlements: [
        [IDS.A, 0, H, J, 'accept', null, null],
        [IDS.A, 1, H, H, 'accept', 'rejected-by-broker', 100],
        [IDS.A, 1, J, J, 'accept', null, null],
        [IDS.A, 2, H, J, 'accept', null, null],
      ],
    },
  },
  {
    fault: 'partition 100',
    step: 'flush; held handler and revoked partition',
    expected: {
      action: 'flush',
      state: 'STABLE',
      rounds: 8,
      result: 'rejected-by-broker',
      cause: 100,
      reset: false,
      terminal: true,
      selected: ['A:1@9007199254740993'],
      held: ['A:0@9007199254740993'],
      revoked: ['A:2@9007199254740993', 'A:2@9007199254740994'],
      oldAckResends: 0,
      nextEpoch: 3,
      revokedType: 'release',
      revokedResult: 'accepted',
      revokedWire: 'eligible terminal frame',
    },
    observations: {
      handlers: [
        [IDS.A, 0, H, '6b', '76', 2],
        [IDS.A, 0, J, '6b', '76', 2],
        [IDS.A, 1, H, '6b', '76', 2],
        [IDS.A, 1, J, '6b', '76', 2],
      ],
      mutations: [
        [IDS.A, 0, H, J, [1]],
        [IDS.A, 1, H, H, [1]],
        [IDS.A, 1, J, J, [1]],
        [IDS.A, 2, H, J, [2]],
      ],
      settlements: [
        [IDS.A, 0, H, J, 'accept', null, null],
        [IDS.A, 1, H, H, 'accept', 'rejected-by-broker', 100],
        [IDS.A, 1, J, J, 'accept', null, null],
        [IDS.A, 2, H, J, 'release', null, null],
      ],
    },
  },
  {
    fault: 'top 122',
    step: 'flush; held handler',
    expected: {
      action: 'flush',
      state: 'STABLE',
      rounds: 8,
      result: 'session-lost',
      cause: 122,
      reset: true,
      terminal: true,
      selected: ['A:1@9007199254740993'],
      held: ['A:0@9007199254740993'],
      revoked: [],
      oldAckResends: 0,
      nextEpoch: 0,
    },
    observations: {
      handlers: [
        [IDS.A, 0, H, '6b', '76', 2],
        [IDS.A, 0, J, '6b', '76', 2],
        [IDS.A, 1, H, '6b', '76', 2],
        [IDS.A, 1, J, '6b', '76', 2],
      ],
      mutations: [
        [IDS.A, 0, H, J, [1]],
        [IDS.A, 1, H, H, [1]],
      ],
      settlements: [
        [IDS.A, 0, H, J, 'accept', null, null],
        [IDS.A, 1, H, H, 'accept', 'session-lost', 122],
        [IDS.A, 1, J, J, 'release', 'session-lost', 122],
        [IDS.A, 2, H, J, 'release', 'session-lost', 122],
      ],
    },
  },
  {
    fault: 'top 122',
    step: 'flush; held handler and revoked partition',
    expected: {
      action: 'flush',
      state: 'STABLE',
      rounds: 8,
      result: 'session-lost',
      cause: 122,
      reset: true,
      terminal: true,
      selected: ['A:1@9007199254740993'],
      held: ['A:0@9007199254740993'],
      revoked: ['A:2@9007199254740993', 'A:2@9007199254740994'],
      oldAckResends: 0,
      nextEpoch: 0,
      revokedType: 'release',
      revokedResult: 'session-lost',
      revokedWire: 'none',
    },
    observations: {
      handlers: [
        [IDS.A, 0, H, '6b', '76', 2],
        [IDS.A, 0, J, '6b', '76', 2],
        [IDS.A, 1, H, '6b', '76', 2],
        [IDS.A, 1, J, '6b', '76', 2],
      ],
      mutations: [
        [IDS.A, 0, H, J, [1]],
        [IDS.A, 1, H, H, [1]],
      ],
      settlements: [
        [IDS.A, 0, H, J, 'accept', null, null],
        [IDS.A, 1, H, H, 'accept', 'session-lost', 122],
        [IDS.A, 1, J, J, 'release', 'session-lost', 122],
        [IDS.A, 2, H, J, 'release', 'session-lost', 122],
      ],
    },
  },
  {
    fault: 'top 123',
    step: 'flush; held handler',
    expected: {
      action: 'flush',
      state: 'STABLE',
      rounds: 8,
      result: 'session-lost',
      cause: 123,
      reset: true,
      terminal: true,
      selected: ['A:1@9007199254740993'],
      held: ['A:0@9007199254740993'],
      revoked: [],
      oldAckResends: 0,
      nextEpoch: 0,
    },
    observations: {
      handlers: [
        [IDS.A, 0, H, '6b', '76', 2],
        [IDS.A, 0, J, '6b', '76', 2],
        [IDS.A, 1, H, '6b', '76', 2],
        [IDS.A, 1, J, '6b', '76', 2],
      ],
      mutations: [
        [IDS.A, 0, H, J, [1]],
        [IDS.A, 1, H, H, [1]],
      ],
      settlements: [
        [IDS.A, 0, H, J, 'accept', null, null],
        [IDS.A, 1, H, H, 'accept', 'session-lost', 123],
        [IDS.A, 1, J, J, 'release', 'session-lost', 123],
        [IDS.A, 2, H, J, 'release', 'session-lost', 123],
      ],
    },
  },
  {
    fault: 'top 123',
    step: 'flush; held handler and revoked partition',
    expected: {
      action: 'flush',
      state: 'STABLE',
      rounds: 8,
      result: 'session-lost',
      cause: 123,
      reset: true,
      terminal: true,
      selected: ['A:1@9007199254740993'],
      held: ['A:0@9007199254740993'],
      revoked: ['A:2@9007199254740993', 'A:2@9007199254740994'],
      oldAckResends: 0,
      nextEpoch: 0,
      revokedType: 'release',
      revokedResult: 'session-lost',
      revokedWire: 'none',
    },
    observations: {
      handlers: [
        [IDS.A, 0, H, '6b', '76', 2],
        [IDS.A, 0, J, '6b', '76', 2],
        [IDS.A, 1, H, '6b', '76', 2],
        [IDS.A, 1, J, '6b', '76', 2],
      ],
      mutations: [
        [IDS.A, 0, H, J, [1]],
        [IDS.A, 1, H, H, [1]],
      ],
      settlements: [
        [IDS.A, 0, H, J, 'accept', null, null],
        [IDS.A, 1, H, H, 'accept', 'session-lost', 123],
        [IDS.A, 1, J, J, 'release', 'session-lost', 123],
        [IDS.A, 2, H, J, 'release', 'session-lost', 123],
      ],
    },
  },
  {
    fault: 'top 133',
    step: 'flush; held handler',
    expected: {
      action: 'flush',
      state: 'STABLE',
      rounds: 8,
      result: 'rejected-by-broker',
      cause: 133,
      reset: false,
      terminal: true,
      selected: ['A:1@9007199254740993'],
      held: ['A:0@9007199254740993'],
      revoked: [],
      oldAckResends: 0,
      nextEpoch: 3,
    },
    observations: {
      handlers: [
        [IDS.A, 0, H, '6b', '76', 2],
        [IDS.A, 0, J, '6b', '76', 2],
        [IDS.A, 1, H, '6b', '76', 2],
        [IDS.A, 1, J, '6b', '76', 2],
        [IDS.A, 2, H, '6b', '76', 2],
        [IDS.A, 2, J, '6b', '76', 2],
      ],
      mutations: [
        [IDS.A, 0, H, J, [1]],
        [IDS.A, 1, H, H, [1]],
        [IDS.A, 1, J, J, [1]],
        [IDS.A, 2, H, J, [1]],
      ],
      settlements: [
        [IDS.A, 0, H, J, 'accept', null, null],
        [IDS.A, 1, H, H, 'accept', 'rejected-by-broker', 133],
        [IDS.A, 1, J, J, 'accept', null, null],
        [IDS.A, 2, H, J, 'accept', null, null],
      ],
    },
  },
  {
    fault: 'top 133',
    step: 'flush; held handler and revoked partition',
    expected: {
      action: 'flush',
      state: 'STABLE',
      rounds: 8,
      result: 'rejected-by-broker',
      cause: 133,
      reset: false,
      terminal: true,
      selected: ['A:1@9007199254740993'],
      held: ['A:0@9007199254740993'],
      revoked: ['A:2@9007199254740993', 'A:2@9007199254740994'],
      oldAckResends: 0,
      nextEpoch: 3,
      revokedType: 'release',
      revokedResult: 'accepted',
      revokedWire: 'eligible terminal frame',
    },
    observations: {
      handlers: [
        [IDS.A, 0, H, '6b', '76', 2],
        [IDS.A, 0, J, '6b', '76', 2],
        [IDS.A, 1, H, '6b', '76', 2],
        [IDS.A, 1, J, '6b', '76', 2],
      ],
      mutations: [
        [IDS.A, 0, H, J, [1]],
        [IDS.A, 1, H, H, [1]],
        [IDS.A, 1, J, J, [1]],
        [IDS.A, 2, H, J, [2]],
      ],
      settlements: [
        [IDS.A, 0, H, J, 'accept', null, null],
        [IDS.A, 1, H, H, 'accept', 'rejected-by-broker', 133],
        [IDS.A, 1, J, J, 'accept', null, null],
        [IDS.A, 2, H, J, 'release', null, null],
      ],
    },
  },
  {
    fault: 'disconnect',
    step: 'flush; held handler',
    expected: {
      action: 'flush',
      state: 'STABLE',
      rounds: 8,
      result: 'session-lost',
      cause: null,
      reset: true,
      terminal: true,
      selected: ['A:1@9007199254740993'],
      held: ['A:0@9007199254740993'],
      revoked: [],
      oldAckResends: 0,
      nextEpoch: 0,
    },
    observations: {
      handlers: [
        [IDS.A, 0, H, '6b', '76', 2],
        [IDS.A, 0, J, '6b', '76', 2],
        [IDS.A, 1, H, '6b', '76', 2],
        [IDS.A, 1, J, '6b', '76', 2],
      ],
      mutations: [
        [IDS.A, 0, H, J, [1]],
        [IDS.A, 1, H, H, [1]],
      ],
      settlements: [
        [IDS.A, 0, H, J, 'accept', null, null],
        [IDS.A, 1, H, H, 'accept', 'session-lost', null],
        [IDS.A, 1, J, J, 'release', 'session-lost', null],
        [IDS.A, 2, H, J, 'release', 'session-lost', null],
      ],
    },
  },
  {
    fault: 'disconnect',
    step: 'flush; held handler and revoked partition',
    expected: {
      action: 'flush',
      state: 'STABLE',
      rounds: 8,
      result: 'session-lost',
      cause: null,
      reset: true,
      terminal: true,
      selected: ['A:1@9007199254740993'],
      held: ['A:0@9007199254740993'],
      revoked: ['A:2@9007199254740993', 'A:2@9007199254740994'],
      oldAckResends: 0,
      nextEpoch: 0,
      revokedType: 'release',
      revokedResult: 'session-lost',
      revokedWire: 'none',
    },
    observations: {
      handlers: [
        [IDS.A, 0, H, '6b', '76', 2],
        [IDS.A, 0, J, '6b', '76', 2],
        [IDS.A, 1, H, '6b', '76', 2],
        [IDS.A, 1, J, '6b', '76', 2],
      ],
      mutations: [
        [IDS.A, 0, H, J, [1]],
        [IDS.A, 1, H, H, [1]],
      ],
      settlements: [
        [IDS.A, 0, H, J, 'accept', null, null],
        [IDS.A, 1, H, H, 'accept', 'session-lost', null],
        [IDS.A, 1, J, J, 'release', 'session-lost', null],
        [IDS.A, 2, H, J, 'release', 'session-lost', null],
      ],
    },
  },
  {
    fault: 'timeout',
    step: 'flush; held handler',
    expected: {
      action: 'flush',
      state: 'STABLE',
      rounds: 8,
      result: 'session-lost',
      cause: null,
      reset: true,
      terminal: true,
      selected: ['A:1@9007199254740993'],
      held: ['A:0@9007199254740993'],
      revoked: [],
      oldAckResends: 0,
      nextEpoch: 0,
    },
    observations: {
      handlers: [
        [IDS.A, 0, H, '6b', '76', 2],
        [IDS.A, 0, J, '6b', '76', 2],
        [IDS.A, 1, H, '6b', '76', 2],
        [IDS.A, 1, J, '6b', '76', 2],
      ],
      mutations: [
        [IDS.A, 0, H, J, [1]],
        [IDS.A, 1, H, H, [1]],
      ],
      settlements: [
        [IDS.A, 0, H, J, 'accept', null, null],
        [IDS.A, 1, H, H, 'accept', 'session-lost', null],
        [IDS.A, 1, J, J, 'release', 'session-lost', null],
        [IDS.A, 2, H, J, 'release', 'session-lost', null],
      ],
    },
  },
  {
    fault: 'timeout',
    step: 'flush; held handler and revoked partition',
    expected: {
      action: 'flush',
      state: 'STABLE',
      rounds: 8,
      result: 'session-lost',
      cause: null,
      reset: true,
      terminal: true,
      selected: ['A:1@9007199254740993'],
      held: ['A:0@9007199254740993'],
      revoked: ['A:2@9007199254740993', 'A:2@9007199254740994'],
      oldAckResends: 0,
      nextEpoch: 0,
      revokedType: 'release',
      revokedResult: 'session-lost',
      revokedWire: 'none',
    },
    observations: {
      handlers: [
        [IDS.A, 0, H, '6b', '76', 2],
        [IDS.A, 0, J, '6b', '76', 2],
        [IDS.A, 1, H, '6b', '76', 2],
        [IDS.A, 1, J, '6b', '76', 2],
      ],
      mutations: [
        [IDS.A, 0, H, J, [1]],
        [IDS.A, 1, H, H, [1]],
      ],
      settlements: [
        [IDS.A, 0, H, J, 'accept', null, null],
        [IDS.A, 1, H, H, 'accept', 'session-lost', null],
        [IDS.A, 1, J, J, 'release', 'session-lost', null],
        [IDS.A, 2, H, J, 'release', 'session-lost', null],
      ],
    },
  },
  {
    fault: 'partition 121',
    step: 'piggyback; held handler',
    expected: {
      action: 'piggyback',
      state: 'STABLE',
      rounds: 8,
      result: 'rejected-by-broker',
      cause: 121,
      reset: false,
      terminal: true,
      selected: ['A:1@9007199254740993'],
      held: ['A:0@9007199254740993'],
      revoked: [],
      oldAckResends: 0,
      nextEpoch: 3,
    },
    observations: {
      handlers: [
        [IDS.A, 0, H, '6b', '76', 2],
        [IDS.A, 0, J, '6b', '76', 2],
        [IDS.A, 1, H, '6b', '76', 2],
        [IDS.A, 1, J, '6b', '76', 2],
        [IDS.A, 2, H, '6b', '76', 2],
        [IDS.A, 2, J, '6b', '76', 2],
      ],
      mutations: [
        [IDS.A, 0, H, J, [1]],
        [IDS.A, 1, H, J, [1]],
        [IDS.A, 2, H, J, [1]],
      ],
      settlements: [
        [IDS.A, 0, H, J, 'accept', null, null],
        [IDS.A, 1, H, J, 'accept', 'rejected-by-broker', 121],
        [IDS.A, 2, H, J, 'accept', null, null],
      ],
    },
  },
  {
    fault: 'partition 121',
    step: 'piggyback; held handler and revoked partition',
    expected: {
      action: 'piggyback',
      state: 'STABLE',
      rounds: 8,
      result: 'rejected-by-broker',
      cause: 121,
      reset: false,
      terminal: true,
      selected: ['A:1@9007199254740993'],
      held: ['A:0@9007199254740993'],
      revoked: ['A:2@9007199254740993', 'A:2@9007199254740994'],
      oldAckResends: 0,
      nextEpoch: 3,
      revokedType: 'release',
      revokedResult: 'accepted',
      revokedWire: 'eligible terminal frame',
    },
    observations: {
      handlers: [
        [IDS.A, 0, H, '6b', '76', 2],
        [IDS.A, 0, J, '6b', '76', 2],
        [IDS.A, 1, H, '6b', '76', 2],
        [IDS.A, 1, J, '6b', '76', 2],
      ],
      mutations: [
        [IDS.A, 0, H, J, [1]],
        [IDS.A, 1, H, J, [1]],
        [IDS.A, 2, H, J, [2]],
      ],
      settlements: [
        [IDS.A, 0, H, J, 'accept', null, null],
        [IDS.A, 1, H, J, 'accept', 'rejected-by-broker', 121],
        [IDS.A, 2, H, J, 'release', null, null],
      ],
    },
  },
  {
    fault: 'partition 6',
    step: 'piggyback; held handler',
    expected: {
      action: 'piggyback',
      state: 'STABLE',
      rounds: 8,
      result: 'session-lost',
      cause: 6,
      reset: false,
      terminal: true,
      selected: ['A:1@9007199254740993'],
      held: ['A:0@9007199254740993'],
      revoked: [],
      oldAckResends: 0,
      nextEpoch: 3,
    },
    observations: {
      handlers: [
        [IDS.A, 0, H, '6b', '76', 2],
        [IDS.A, 0, J, '6b', '76', 2],
        [IDS.A, 1, H, '6b', '76', 2],
        [IDS.A, 1, J, '6b', '76', 2],
        [IDS.A, 2, H, '6b', '76', 2],
        [IDS.A, 2, J, '6b', '76', 2],
      ],
      mutations: [
        [IDS.A, 0, H, J, [1]],
        [IDS.A, 1, H, J, [1]],
        [IDS.A, 2, H, J, [1]],
      ],
      settlements: [
        [IDS.A, 0, H, J, 'accept', null, null],
        [IDS.A, 1, H, J, 'accept', 'session-lost', 6],
        [IDS.A, 2, H, J, 'accept', null, null],
      ],
    },
  },
  {
    fault: 'partition 6',
    step: 'piggyback; held handler and revoked partition',
    expected: {
      action: 'piggyback',
      state: 'STABLE',
      rounds: 8,
      result: 'session-lost',
      cause: 6,
      reset: false,
      terminal: true,
      selected: ['A:1@9007199254740993'],
      held: ['A:0@9007199254740993'],
      revoked: ['A:2@9007199254740993', 'A:2@9007199254740994'],
      oldAckResends: 0,
      nextEpoch: 3,
      revokedType: 'release',
      revokedResult: 'accepted',
      revokedWire: 'eligible terminal frame',
    },
    observations: {
      handlers: [
        [IDS.A, 0, H, '6b', '76', 2],
        [IDS.A, 0, J, '6b', '76', 2],
        [IDS.A, 1, H, '6b', '76', 2],
        [IDS.A, 1, J, '6b', '76', 2],
      ],
      mutations: [
        [IDS.A, 0, H, J, [1]],
        [IDS.A, 1, H, J, [1]],
        [IDS.A, 2, H, J, [2]],
      ],
      settlements: [
        [IDS.A, 0, H, J, 'accept', null, null],
        [IDS.A, 1, H, J, 'accept', 'session-lost', 6],
        [IDS.A, 2, H, J, 'release', null, null],
      ],
    },
  },
  {
    fault: 'partition 100',
    step: 'piggyback; held handler',
    expected: {
      action: 'piggyback',
      state: 'STABLE',
      rounds: 8,
      result: 'rejected-by-broker',
      cause: 100,
      reset: false,
      terminal: true,
      selected: ['A:1@9007199254740993'],
      held: ['A:0@9007199254740993'],
      revoked: [],
      oldAckResends: 0,
      nextEpoch: 3,
    },
    observations: {
      handlers: [
        [IDS.A, 0, H, '6b', '76', 2],
        [IDS.A, 0, J, '6b', '76', 2],
        [IDS.A, 1, H, '6b', '76', 2],
        [IDS.A, 1, J, '6b', '76', 2],
        [IDS.A, 2, H, '6b', '76', 2],
        [IDS.A, 2, J, '6b', '76', 2],
      ],
      mutations: [
        [IDS.A, 0, H, J, [1]],
        [IDS.A, 1, H, J, [1]],
        [IDS.A, 2, H, J, [1]],
      ],
      settlements: [
        [IDS.A, 0, H, J, 'accept', null, null],
        [IDS.A, 1, H, J, 'accept', 'rejected-by-broker', 100],
        [IDS.A, 2, H, J, 'accept', null, null],
      ],
    },
  },
  {
    fault: 'partition 100',
    step: 'piggyback; held handler and revoked partition',
    expected: {
      action: 'piggyback',
      state: 'STABLE',
      rounds: 8,
      result: 'rejected-by-broker',
      cause: 100,
      reset: false,
      terminal: true,
      selected: ['A:1@9007199254740993'],
      held: ['A:0@9007199254740993'],
      revoked: ['A:2@9007199254740993', 'A:2@9007199254740994'],
      oldAckResends: 0,
      nextEpoch: 3,
      revokedType: 'release',
      revokedResult: 'accepted',
      revokedWire: 'eligible terminal frame',
    },
    observations: {
      handlers: [
        [IDS.A, 0, H, '6b', '76', 2],
        [IDS.A, 0, J, '6b', '76', 2],
        [IDS.A, 1, H, '6b', '76', 2],
        [IDS.A, 1, J, '6b', '76', 2],
      ],
      mutations: [
        [IDS.A, 0, H, J, [1]],
        [IDS.A, 1, H, J, [1]],
        [IDS.A, 2, H, J, [2]],
      ],
      settlements: [
        [IDS.A, 0, H, J, 'accept', null, null],
        [IDS.A, 1, H, J, 'accept', 'rejected-by-broker', 100],
        [IDS.A, 2, H, J, 'release', null, null],
      ],
    },
  },
  {
    fault: 'top 122',
    step: 'piggyback; held handler',
    expected: {
      action: 'piggyback',
      state: 'STABLE',
      rounds: 8,
      result: 'session-lost',
      cause: 122,
      reset: true,
      terminal: true,
      selected: ['A:1@9007199254740993'],
      held: ['A:0@9007199254740993'],
      revoked: [],
      oldAckResends: 0,
      nextEpoch: 0,
    },
    observations: {
      handlers: [
        [IDS.A, 0, H, '6b', '76', 2],
        [IDS.A, 0, J, '6b', '76', 2],
        [IDS.A, 1, H, '6b', '76', 2],
        [IDS.A, 1, J, '6b', '76', 2],
        [IDS.A, 2, H, '6b', '76', 2],
        [IDS.A, 2, J, '6b', '76', 2],
      ],
      mutations: [
        [IDS.A, 0, H, J, [1]],
        [IDS.A, 1, H, J, [1]],
        [IDS.A, 2, H, J, [1]],
      ],
      settlements: [
        [IDS.A, 0, H, J, 'accept', null, null],
        [IDS.A, 1, H, J, 'accept', 'session-lost', 122],
        [IDS.A, 2, H, J, 'accept', 'session-lost', 122],
      ],
    },
  },
  {
    fault: 'top 122',
    step: 'piggyback; held handler and revoked partition',
    expected: {
      action: 'piggyback',
      state: 'STABLE',
      rounds: 8,
      result: 'session-lost',
      cause: 122,
      reset: true,
      terminal: true,
      selected: ['A:1@9007199254740993'],
      held: ['A:0@9007199254740993'],
      revoked: ['A:2@9007199254740993', 'A:2@9007199254740994'],
      oldAckResends: 0,
      nextEpoch: 0,
      revokedType: 'release',
      revokedResult: 'session-lost',
      revokedWire: 'none',
    },
    observations: {
      handlers: [
        [IDS.A, 0, H, '6b', '76', 2],
        [IDS.A, 0, J, '6b', '76', 2],
        [IDS.A, 1, H, '6b', '76', 2],
        [IDS.A, 1, J, '6b', '76', 2],
      ],
      mutations: [
        [IDS.A, 0, H, J, [1]],
        [IDS.A, 1, H, J, [1]],
        [IDS.A, 2, H, J, [2]],
      ],
      settlements: [
        [IDS.A, 0, H, J, 'accept', null, null],
        [IDS.A, 1, H, J, 'accept', 'session-lost', 122],
        [IDS.A, 2, H, J, 'release', 'session-lost', 122],
      ],
    },
  },
  {
    fault: 'top 123',
    step: 'piggyback; held handler',
    expected: {
      action: 'piggyback',
      state: 'STABLE',
      rounds: 8,
      result: 'session-lost',
      cause: 123,
      reset: true,
      terminal: true,
      selected: ['A:1@9007199254740993'],
      held: ['A:0@9007199254740993'],
      revoked: [],
      oldAckResends: 0,
      nextEpoch: 0,
    },
    observations: {
      handlers: [
        [IDS.A, 0, H, '6b', '76', 2],
        [IDS.A, 0, J, '6b', '76', 2],
        [IDS.A, 1, H, '6b', '76', 2],
        [IDS.A, 1, J, '6b', '76', 2],
        [IDS.A, 2, H, '6b', '76', 2],
        [IDS.A, 2, J, '6b', '76', 2],
      ],
      mutations: [
        [IDS.A, 0, H, J, [1]],
        [IDS.A, 1, H, J, [1]],
        [IDS.A, 2, H, J, [1]],
      ],
      settlements: [
        [IDS.A, 0, H, J, 'accept', null, null],
        [IDS.A, 1, H, J, 'accept', 'session-lost', 123],
        [IDS.A, 2, H, J, 'accept', 'session-lost', 123],
      ],
    },
  },
  {
    fault: 'top 123',
    step: 'piggyback; held handler and revoked partition',
    expected: {
      action: 'piggyback',
      state: 'STABLE',
      rounds: 8,
      result: 'session-lost',
      cause: 123,
      reset: true,
      terminal: true,
      selected: ['A:1@9007199254740993'],
      held: ['A:0@9007199254740993'],
      revoked: ['A:2@9007199254740993', 'A:2@9007199254740994'],
      oldAckResends: 0,
      nextEpoch: 0,
      revokedType: 'release',
      revokedResult: 'session-lost',
      revokedWire: 'none',
    },
    observations: {
      handlers: [
        [IDS.A, 0, H, '6b', '76', 2],
        [IDS.A, 0, J, '6b', '76', 2],
        [IDS.A, 1, H, '6b', '76', 2],
        [IDS.A, 1, J, '6b', '76', 2],
      ],
      mutations: [
        [IDS.A, 0, H, J, [1]],
        [IDS.A, 1, H, J, [1]],
        [IDS.A, 2, H, J, [2]],
      ],
      settlements: [
        [IDS.A, 0, H, J, 'accept', null, null],
        [IDS.A, 1, H, J, 'accept', 'session-lost', 123],
        [IDS.A, 2, H, J, 'release', 'session-lost', 123],
      ],
    },
  },
  {
    fault: 'top 133',
    step: 'piggyback; held handler',
    expected: {
      action: 'piggyback',
      state: 'STABLE',
      rounds: 8,
      result: 'session-lost',
      cause: 133,
      reset: true,
      terminal: true,
      selected: ['A:1@9007199254740993'],
      held: ['A:0@9007199254740993'],
      revoked: [],
      oldAckResends: 0,
      nextEpoch: 0,
    },
    observations: {
      handlers: [
        [IDS.A, 0, H, '6b', '76', 2],
        [IDS.A, 0, J, '6b', '76', 2],
        [IDS.A, 1, H, '6b', '76', 2],
        [IDS.A, 1, J, '6b', '76', 2],
        [IDS.A, 2, H, '6b', '76', 2],
        [IDS.A, 2, J, '6b', '76', 2],
      ],
      mutations: [
        [IDS.A, 0, H, J, [1]],
        [IDS.A, 1, H, J, [1]],
        [IDS.A, 2, H, J, [1]],
      ],
      settlements: [
        [IDS.A, 0, H, J, 'accept', null, null],
        [IDS.A, 1, H, J, 'accept', 'session-lost', 133],
        [IDS.A, 2, H, J, 'accept', 'session-lost', 133],
      ],
    },
  },
  {
    fault: 'top 133',
    step: 'piggyback; held handler and revoked partition',
    expected: {
      action: 'piggyback',
      state: 'STABLE',
      rounds: 8,
      result: 'session-lost',
      cause: 133,
      reset: true,
      terminal: true,
      selected: ['A:1@9007199254740993'],
      held: ['A:0@9007199254740993'],
      revoked: ['A:2@9007199254740993', 'A:2@9007199254740994'],
      oldAckResends: 0,
      nextEpoch: 0,
      revokedType: 'release',
      revokedResult: 'session-lost',
      revokedWire: 'none',
    },
    observations: {
      handlers: [
        [IDS.A, 0, H, '6b', '76', 2],
        [IDS.A, 0, J, '6b', '76', 2],
        [IDS.A, 1, H, '6b', '76', 2],
        [IDS.A, 1, J, '6b', '76', 2],
      ],
      mutations: [
        [IDS.A, 0, H, J, [1]],
        [IDS.A, 1, H, J, [1]],
        [IDS.A, 2, H, J, [2]],
      ],
      settlements: [
        [IDS.A, 0, H, J, 'accept', null, null],
        [IDS.A, 1, H, J, 'accept', 'session-lost', 133],
        [IDS.A, 2, H, J, 'release', 'session-lost', 133],
      ],
    },
  },
  {
    fault: 'disconnect',
    step: 'piggyback; held handler',
    expected: {
      action: 'piggyback',
      state: 'STABLE',
      rounds: 8,
      result: 'session-lost',
      cause: null,
      reset: true,
      terminal: true,
      selected: ['A:1@9007199254740993'],
      held: ['A:0@9007199254740993'],
      revoked: [],
      oldAckResends: 0,
      nextEpoch: 0,
    },
    observations: {
      handlers: [
        [IDS.A, 0, H, '6b', '76', 2],
        [IDS.A, 0, J, '6b', '76', 2],
        [IDS.A, 1, H, '6b', '76', 2],
        [IDS.A, 1, J, '6b', '76', 2],
        [IDS.A, 2, H, '6b', '76', 2],
        [IDS.A, 2, J, '6b', '76', 2],
      ],
      mutations: [
        [IDS.A, 0, H, J, [1]],
        [IDS.A, 1, H, J, [1]],
        [IDS.A, 2, H, J, [1]],
      ],
      settlements: [
        [IDS.A, 0, H, J, 'accept', null, null],
        [IDS.A, 1, H, J, 'accept', 'session-lost', null],
        [IDS.A, 2, H, J, 'accept', 'session-lost', null],
      ],
    },
  },
  {
    fault: 'disconnect',
    step: 'piggyback; held handler and revoked partition',
    expected: {
      action: 'piggyback',
      state: 'STABLE',
      rounds: 8,
      result: 'session-lost',
      cause: null,
      reset: true,
      terminal: true,
      selected: ['A:1@9007199254740993'],
      held: ['A:0@9007199254740993'],
      revoked: ['A:2@9007199254740993', 'A:2@9007199254740994'],
      oldAckResends: 0,
      nextEpoch: 0,
      revokedType: 'release',
      revokedResult: 'session-lost',
      revokedWire: 'none',
    },
    observations: {
      handlers: [
        [IDS.A, 0, H, '6b', '76', 2],
        [IDS.A, 0, J, '6b', '76', 2],
        [IDS.A, 1, H, '6b', '76', 2],
        [IDS.A, 1, J, '6b', '76', 2],
      ],
      mutations: [
        [IDS.A, 0, H, J, [1]],
        [IDS.A, 1, H, J, [1]],
        [IDS.A, 2, H, J, [2]],
      ],
      settlements: [
        [IDS.A, 0, H, J, 'accept', null, null],
        [IDS.A, 1, H, J, 'accept', 'session-lost', null],
        [IDS.A, 2, H, J, 'release', 'session-lost', null],
      ],
    },
  },
  {
    fault: 'timeout',
    step: 'piggyback; held handler',
    expected: {
      action: 'piggyback',
      state: 'STABLE',
      rounds: 8,
      result: 'session-lost',
      cause: null,
      reset: true,
      terminal: true,
      selected: ['A:1@9007199254740993'],
      held: ['A:0@9007199254740993'],
      revoked: [],
      oldAckResends: 0,
      nextEpoch: 0,
    },
    observations: {
      handlers: [
        [IDS.A, 0, H, '6b', '76', 2],
        [IDS.A, 0, J, '6b', '76', 2],
        [IDS.A, 1, H, '6b', '76', 2],
        [IDS.A, 1, J, '6b', '76', 2],
        [IDS.A, 2, H, '6b', '76', 2],
        [IDS.A, 2, J, '6b', '76', 2],
      ],
      mutations: [
        [IDS.A, 0, H, J, [1]],
        [IDS.A, 1, H, J, [1]],
        [IDS.A, 2, H, J, [1]],
      ],
      settlements: [
        [IDS.A, 0, H, J, 'accept', null, null],
        [IDS.A, 1, H, J, 'accept', 'session-lost', null],
        [IDS.A, 2, H, J, 'accept', 'session-lost', null],
      ],
    },
  },
  {
    fault: 'timeout',
    step: 'piggyback; held handler and revoked partition',
    expected: {
      action: 'piggyback',
      state: 'STABLE',
      rounds: 8,
      result: 'session-lost',
      cause: null,
      reset: true,
      terminal: true,
      selected: ['A:1@9007199254740993'],
      held: ['A:0@9007199254740993'],
      revoked: ['A:2@9007199254740993', 'A:2@9007199254740994'],
      oldAckResends: 0,
      nextEpoch: 0,
      revokedType: 'release',
      revokedResult: 'session-lost',
      revokedWire: 'none',
    },
    observations: {
      handlers: [
        [IDS.A, 0, H, '6b', '76', 2],
        [IDS.A, 0, J, '6b', '76', 2],
        [IDS.A, 1, H, '6b', '76', 2],
        [IDS.A, 1, J, '6b', '76', 2],
      ],
      mutations: [
        [IDS.A, 0, H, J, [1]],
        [IDS.A, 1, H, J, [1]],
        [IDS.A, 2, H, J, [2]],
      ],
      settlements: [
        [IDS.A, 0, H, J, 'accept', null, null],
        [IDS.A, 1, H, J, 'accept', 'session-lost', null],
        [IDS.A, 2, H, J, 'release', 'session-lost', null],
      ],
    },
  },
  {
    fault: 'partition 121',
    step: 'close; held handler',
    expected: {
      action: 'close',
      state: 'UNSUBSCRIBED',
      rounds: 8,
      result: 'rejected-by-broker',
      cause: 121,
      reset: false,
      terminal: true,
      selected: ['A:1@9007199254740993'],
      held: ['A:0@9007199254740993'],
      revoked: [],
      oldAckResends: 0,
      nextEpoch: null,
    },
    observations: {
      handlers: [
        [IDS.A, 0, H, '6b', '76', 2],
        [IDS.A, 0, H, '6b', '76', 2],
        [IDS.A, 0, J, '6b', '76', 2],
        [IDS.A, 1, H, '6b', '76', 2],
      ],
      mutations: [
        [IDS.A, 0, J, J, [2]],
        [IDS.A, 0, H, J, [1]],
        [IDS.A, 1, H, J, [1, 2]],
        [IDS.A, 2, H, J, [2]],
      ],
      settlements: [
        [IDS.A, 0, H, H, 'release', 'membership-lost', null],
        [IDS.A, 0, J, J, 'release', null, null],
        [IDS.A, 0, H, J, 'accept', null, null],
        [IDS.A, 1, H, H, 'accept', 'rejected-by-broker', 121],
        [IDS.A, 1, J, J, 'release', 'rejected-by-broker', 121],
        [IDS.A, 2, H, J, 'release', null, null],
      ],
    },
  },
  {
    fault: 'partition 121',
    step: 'close; held handler and revoked partition',
    expected: {
      action: 'close',
      state: 'UNSUBSCRIBED',
      rounds: 8,
      result: 'rejected-by-broker',
      cause: 121,
      reset: false,
      terminal: true,
      selected: ['A:1@9007199254740993'],
      held: ['A:0@9007199254740993'],
      revoked: ['A:2@9007199254740993', 'A:2@9007199254740994'],
      oldAckResends: 0,
      nextEpoch: null,
      revokedType: 'release',
      revokedResult: 'accepted',
      revokedWire: 'eligible terminal frame',
    },
    observations: {
      handlers: [
        [IDS.A, 0, H, '6b', '76', 2],
        [IDS.A, 0, H, '6b', '76', 2],
        [IDS.A, 0, J, '6b', '76', 2],
        [IDS.A, 1, H, '6b', '76', 2],
      ],
      mutations: [
        [IDS.A, 0, J, J, [2]],
        [IDS.A, 0, H, J, [1]],
        [IDS.A, 1, H, J, [1, 2]],
        [IDS.A, 2, H, J, [2]],
      ],
      settlements: [
        [IDS.A, 0, H, H, 'release', 'membership-lost', null],
        [IDS.A, 0, J, J, 'release', null, null],
        [IDS.A, 0, H, J, 'accept', null, null],
        [IDS.A, 1, H, H, 'accept', 'rejected-by-broker', 121],
        [IDS.A, 1, J, J, 'release', 'rejected-by-broker', 121],
        [IDS.A, 2, H, J, 'release', null, null],
      ],
    },
  },
  {
    fault: 'partition 6',
    step: 'close; held handler',
    expected: {
      action: 'close',
      state: 'UNSUBSCRIBED',
      rounds: 8,
      result: 'session-lost',
      cause: 6,
      reset: false,
      terminal: true,
      selected: ['A:1@9007199254740993'],
      held: ['A:0@9007199254740993'],
      revoked: [],
      oldAckResends: 0,
      nextEpoch: null,
    },
    observations: {
      handlers: [
        [IDS.A, 0, H, '6b', '76', 2],
        [IDS.A, 0, H, '6b', '76', 2],
        [IDS.A, 0, J, '6b', '76', 2],
        [IDS.A, 1, H, '6b', '76', 2],
      ],
      mutations: [
        [IDS.A, 0, J, J, [2]],
        [IDS.A, 0, H, J, [1]],
        [IDS.A, 1, H, J, [1, 2]],
        [IDS.A, 2, H, J, [2]],
      ],
      settlements: [
        [IDS.A, 0, H, H, 'release', 'membership-lost', null],
        [IDS.A, 0, J, J, 'release', null, null],
        [IDS.A, 0, H, J, 'accept', null, null],
        [IDS.A, 1, H, H, 'accept', 'session-lost', 6],
        [IDS.A, 1, J, J, 'release', 'session-lost', 6],
        [IDS.A, 2, H, J, 'release', null, null],
      ],
    },
  },
  {
    fault: 'partition 6',
    step: 'close; held handler and revoked partition',
    expected: {
      action: 'close',
      state: 'UNSUBSCRIBED',
      rounds: 8,
      result: 'session-lost',
      cause: 6,
      reset: false,
      terminal: true,
      selected: ['A:1@9007199254740993'],
      held: ['A:0@9007199254740993'],
      revoked: ['A:2@9007199254740993', 'A:2@9007199254740994'],
      oldAckResends: 0,
      nextEpoch: null,
      revokedType: 'release',
      revokedResult: 'accepted',
      revokedWire: 'eligible terminal frame',
    },
    observations: {
      handlers: [
        [IDS.A, 0, H, '6b', '76', 2],
        [IDS.A, 0, H, '6b', '76', 2],
        [IDS.A, 0, J, '6b', '76', 2],
        [IDS.A, 1, H, '6b', '76', 2],
      ],
      mutations: [
        [IDS.A, 0, J, J, [2]],
        [IDS.A, 0, H, J, [1]],
        [IDS.A, 1, H, J, [1, 2]],
        [IDS.A, 2, H, J, [2]],
      ],
      settlements: [
        [IDS.A, 0, H, H, 'release', 'membership-lost', null],
        [IDS.A, 0, J, J, 'release', null, null],
        [IDS.A, 0, H, J, 'accept', null, null],
        [IDS.A, 1, H, H, 'accept', 'session-lost', 6],
        [IDS.A, 1, J, J, 'release', 'session-lost', 6],
        [IDS.A, 2, H, J, 'release', null, null],
      ],
    },
  },
  {
    fault: 'partition 100',
    step: 'close; held handler',
    expected: {
      action: 'close',
      state: 'UNSUBSCRIBED',
      rounds: 8,
      result: 'rejected-by-broker',
      cause: 100,
      reset: false,
      terminal: true,
      selected: ['A:1@9007199254740993'],
      held: ['A:0@9007199254740993'],
      revoked: [],
      oldAckResends: 0,
      nextEpoch: null,
    },
    observations: {
      handlers: [
        [IDS.A, 0, H, '6b', '76', 2],
        [IDS.A, 0, H, '6b', '76', 2],
        [IDS.A, 0, J, '6b', '76', 2],
        [IDS.A, 1, H, '6b', '76', 2],
      ],
      mutations: [
        [IDS.A, 0, J, J, [2]],
        [IDS.A, 0, H, J, [1]],
        [IDS.A, 1, H, J, [1, 2]],
        [IDS.A, 2, H, J, [2]],
      ],
      settlements: [
        [IDS.A, 0, H, H, 'release', 'membership-lost', null],
        [IDS.A, 0, J, J, 'release', null, null],
        [IDS.A, 0, H, J, 'accept', null, null],
        [IDS.A, 1, H, H, 'accept', 'rejected-by-broker', 100],
        [IDS.A, 1, J, J, 'release', 'rejected-by-broker', 100],
        [IDS.A, 2, H, J, 'release', null, null],
      ],
    },
  },
  {
    fault: 'partition 100',
    step: 'close; held handler and revoked partition',
    expected: {
      action: 'close',
      state: 'UNSUBSCRIBED',
      rounds: 8,
      result: 'rejected-by-broker',
      cause: 100,
      reset: false,
      terminal: true,
      selected: ['A:1@9007199254740993'],
      held: ['A:0@9007199254740993'],
      revoked: ['A:2@9007199254740993', 'A:2@9007199254740994'],
      oldAckResends: 0,
      nextEpoch: null,
      revokedType: 'release',
      revokedResult: 'accepted',
      revokedWire: 'eligible terminal frame',
    },
    observations: {
      handlers: [
        [IDS.A, 0, H, '6b', '76', 2],
        [IDS.A, 0, H, '6b', '76', 2],
        [IDS.A, 0, J, '6b', '76', 2],
        [IDS.A, 1, H, '6b', '76', 2],
      ],
      mutations: [
        [IDS.A, 0, J, J, [2]],
        [IDS.A, 0, H, J, [1]],
        [IDS.A, 1, H, J, [1, 2]],
        [IDS.A, 2, H, J, [2]],
      ],
      settlements: [
        [IDS.A, 0, H, H, 'release', 'membership-lost', null],
        [IDS.A, 0, J, J, 'release', null, null],
        [IDS.A, 0, H, J, 'accept', null, null],
        [IDS.A, 1, H, H, 'accept', 'rejected-by-broker', 100],
        [IDS.A, 1, J, J, 'release', 'rejected-by-broker', 100],
        [IDS.A, 2, H, J, 'release', null, null],
      ],
    },
  },
  {
    fault: 'top 122',
    step: 'close; held handler',
    expected: {
      action: 'close',
      state: 'UNSUBSCRIBED',
      rounds: 8,
      result: 'session-lost',
      cause: 122,
      reset: false,
      terminal: true,
      selected: ['A:1@9007199254740993'],
      held: ['A:0@9007199254740993'],
      revoked: [],
      oldAckResends: 0,
      nextEpoch: null,
    },
    observations: {
      handlers: [
        [IDS.A, 0, H, '6b', '76', 2],
        [IDS.A, 0, H, '6b', '76', 2],
        [IDS.A, 0, J, '6b', '76', 2],
        [IDS.A, 1, H, '6b', '76', 2],
      ],
      mutations: [
        [IDS.A, 0, J, J, [2]],
        [IDS.A, 0, H, J, [1]],
        [IDS.A, 1, H, J, [1, 2]],
        [IDS.A, 2, H, J, [2]],
      ],
      settlements: [
        [IDS.A, 0, H, H, 'release', 'membership-lost', null],
        [IDS.A, 0, J, J, 'release', null, null],
        [IDS.A, 0, H, J, 'accept', null, null],
        [IDS.A, 1, H, H, 'accept', 'session-lost', 122],
        [IDS.A, 1, J, J, 'release', 'session-lost', 122],
        [IDS.A, 2, H, J, 'release', 'session-lost', 122],
      ],
    },
  },
  {
    fault: 'top 122',
    step: 'close; held handler and revoked partition',
    expected: {
      action: 'close',
      state: 'UNSUBSCRIBED',
      rounds: 8,
      result: 'session-lost',
      cause: 122,
      reset: false,
      terminal: true,
      selected: ['A:1@9007199254740993'],
      held: ['A:0@9007199254740993'],
      revoked: ['A:2@9007199254740993', 'A:2@9007199254740994'],
      oldAckResends: 0,
      nextEpoch: null,
      revokedType: 'release',
      revokedResult: 'session-lost',
      revokedWire: 'eligible terminal frame',
    },
    observations: {
      handlers: [
        [IDS.A, 0, H, '6b', '76', 2],
        [IDS.A, 0, H, '6b', '76', 2],
        [IDS.A, 0, J, '6b', '76', 2],
        [IDS.A, 1, H, '6b', '76', 2],
      ],
      mutations: [
        [IDS.A, 0, J, J, [2]],
        [IDS.A, 0, H, J, [1]],
        [IDS.A, 1, H, J, [1, 2]],
        [IDS.A, 2, H, J, [2]],
      ],
      settlements: [
        [IDS.A, 0, H, H, 'release', 'membership-lost', null],
        [IDS.A, 0, J, J, 'release', null, null],
        [IDS.A, 0, H, J, 'accept', null, null],
        [IDS.A, 1, H, H, 'accept', 'session-lost', 122],
        [IDS.A, 1, J, J, 'release', 'session-lost', 122],
        [IDS.A, 2, H, J, 'release', 'session-lost', 122],
      ],
    },
  },
  {
    fault: 'top 123',
    step: 'close; held handler',
    expected: {
      action: 'close',
      state: 'UNSUBSCRIBED',
      rounds: 8,
      result: 'session-lost',
      cause: 123,
      reset: false,
      terminal: true,
      selected: ['A:1@9007199254740993'],
      held: ['A:0@9007199254740993'],
      revoked: [],
      oldAckResends: 0,
      nextEpoch: null,
    },
    observations: {
      handlers: [
        [IDS.A, 0, H, '6b', '76', 2],
        [IDS.A, 0, H, '6b', '76', 2],
        [IDS.A, 0, J, '6b', '76', 2],
        [IDS.A, 1, H, '6b', '76', 2],
      ],
      mutations: [
        [IDS.A, 0, J, J, [2]],
        [IDS.A, 0, H, J, [1]],
        [IDS.A, 1, H, J, [1, 2]],
        [IDS.A, 2, H, J, [2]],
      ],
      settlements: [
        [IDS.A, 0, H, H, 'release', 'membership-lost', null],
        [IDS.A, 0, J, J, 'release', null, null],
        [IDS.A, 0, H, J, 'accept', null, null],
        [IDS.A, 1, H, H, 'accept', 'session-lost', 123],
        [IDS.A, 1, J, J, 'release', 'session-lost', 123],
        [IDS.A, 2, H, J, 'release', 'session-lost', 123],
      ],
    },
  },
  {
    fault: 'top 123',
    step: 'close; held handler and revoked partition',
    expected: {
      action: 'close',
      state: 'UNSUBSCRIBED',
      rounds: 8,
      result: 'session-lost',
      cause: 123,
      reset: false,
      terminal: true,
      selected: ['A:1@9007199254740993'],
      held: ['A:0@9007199254740993'],
      revoked: ['A:2@9007199254740993', 'A:2@9007199254740994'],
      oldAckResends: 0,
      nextEpoch: null,
      revokedType: 'release',
      revokedResult: 'session-lost',
      revokedWire: 'eligible terminal frame',
    },
    observations: {
      handlers: [
        [IDS.A, 0, H, '6b', '76', 2],
        [IDS.A, 0, H, '6b', '76', 2],
        [IDS.A, 0, J, '6b', '76', 2],
        [IDS.A, 1, H, '6b', '76', 2],
      ],
      mutations: [
        [IDS.A, 0, J, J, [2]],
        [IDS.A, 0, H, J, [1]],
        [IDS.A, 1, H, J, [1, 2]],
        [IDS.A, 2, H, J, [2]],
      ],
      settlements: [
        [IDS.A, 0, H, H, 'release', 'membership-lost', null],
        [IDS.A, 0, J, J, 'release', null, null],
        [IDS.A, 0, H, J, 'accept', null, null],
        [IDS.A, 1, H, H, 'accept', 'session-lost', 123],
        [IDS.A, 1, J, J, 'release', 'session-lost', 123],
        [IDS.A, 2, H, J, 'release', 'session-lost', 123],
      ],
    },
  },
  {
    fault: 'top 133',
    step: 'close; held handler',
    expected: {
      action: 'close',
      state: 'UNSUBSCRIBED',
      rounds: 8,
      result: 'rejected-by-broker',
      cause: 133,
      reset: false,
      terminal: true,
      selected: ['A:1@9007199254740993'],
      held: ['A:0@9007199254740993'],
      revoked: [],
      oldAckResends: 0,
      nextEpoch: null,
    },
    observations: {
      handlers: [
        [IDS.A, 0, H, '6b', '76', 2],
        [IDS.A, 0, H, '6b', '76', 2],
        [IDS.A, 0, J, '6b', '76', 2],
        [IDS.A, 1, H, '6b', '76', 2],
      ],
      mutations: [
        [IDS.A, 0, J, J, [2]],
        [IDS.A, 0, H, J, [1]],
        [IDS.A, 1, H, J, [1, 2]],
        [IDS.A, 2, H, J, [2]],
      ],
      settlements: [
        [IDS.A, 0, H, H, 'release', 'membership-lost', null],
        [IDS.A, 0, J, J, 'release', null, null],
        [IDS.A, 0, H, J, 'accept', null, null],
        [IDS.A, 1, H, H, 'accept', 'rejected-by-broker', 133],
        [IDS.A, 1, J, J, 'release', 'rejected-by-broker', 133],
        [IDS.A, 2, H, J, 'release', 'rejected-by-broker', 133],
      ],
    },
  },
  {
    fault: 'top 133',
    step: 'close; held handler and revoked partition',
    expected: {
      action: 'close',
      state: 'UNSUBSCRIBED',
      rounds: 8,
      result: 'rejected-by-broker',
      cause: 133,
      reset: false,
      terminal: true,
      selected: ['A:1@9007199254740993'],
      held: ['A:0@9007199254740993'],
      revoked: ['A:2@9007199254740993', 'A:2@9007199254740994'],
      oldAckResends: 0,
      nextEpoch: null,
      revokedType: 'release',
      revokedResult: 'rejected-by-broker',
      revokedWire: 'eligible terminal frame',
    },
    observations: {
      handlers: [
        [IDS.A, 0, H, '6b', '76', 2],
        [IDS.A, 0, H, '6b', '76', 2],
        [IDS.A, 0, J, '6b', '76', 2],
        [IDS.A, 1, H, '6b', '76', 2],
      ],
      mutations: [
        [IDS.A, 0, J, J, [2]],
        [IDS.A, 0, H, J, [1]],
        [IDS.A, 1, H, J, [1, 2]],
        [IDS.A, 2, H, J, [2]],
      ],
      settlements: [
        [IDS.A, 0, H, H, 'release', 'membership-lost', null],
        [IDS.A, 0, J, J, 'release', null, null],
        [IDS.A, 0, H, J, 'accept', null, null],
        [IDS.A, 1, H, H, 'accept', 'rejected-by-broker', 133],
        [IDS.A, 1, J, J, 'release', 'rejected-by-broker', 133],
        [IDS.A, 2, H, J, 'release', 'rejected-by-broker', 133],
      ],
    },
  },
  {
    fault: 'disconnect',
    step: 'close; held handler',
    expected: {
      action: 'close',
      state: 'UNSUBSCRIBED',
      rounds: 8,
      result: 'session-lost',
      cause: null,
      reset: false,
      terminal: true,
      selected: ['A:1@9007199254740993'],
      held: ['A:0@9007199254740993'],
      revoked: [],
      oldAckResends: 0,
      nextEpoch: null,
    },
    observations: {
      handlers: [
        [IDS.A, 0, H, '6b', '76', 2],
        [IDS.A, 0, H, '6b', '76', 2],
        [IDS.A, 0, J, '6b', '76', 2],
        [IDS.A, 1, H, '6b', '76', 2],
      ],
      mutations: [
        [IDS.A, 0, J, J, [2]],
        [IDS.A, 0, H, J, [1]],
        [IDS.A, 1, H, J, [1, 2]],
        [IDS.A, 2, H, J, [2]],
      ],
      settlements: [
        [IDS.A, 0, H, H, 'release', 'membership-lost', null],
        [IDS.A, 0, J, J, 'release', null, null],
        [IDS.A, 0, H, J, 'accept', null, null],
        [IDS.A, 1, H, H, 'accept', 'session-lost', null],
        [IDS.A, 1, J, J, 'release', 'session-lost', null],
        [IDS.A, 2, H, J, 'release', 'session-lost', null],
      ],
    },
  },
  {
    fault: 'disconnect',
    step: 'close; held handler and revoked partition',
    expected: {
      action: 'close',
      state: 'UNSUBSCRIBED',
      rounds: 8,
      result: 'session-lost',
      cause: null,
      reset: false,
      terminal: true,
      selected: ['A:1@9007199254740993'],
      held: ['A:0@9007199254740993'],
      revoked: ['A:2@9007199254740993', 'A:2@9007199254740994'],
      oldAckResends: 0,
      nextEpoch: null,
      revokedType: 'release',
      revokedResult: 'session-lost',
      revokedWire: 'eligible terminal frame',
    },
    observations: {
      handlers: [
        [IDS.A, 0, H, '6b', '76', 2],
        [IDS.A, 0, H, '6b', '76', 2],
        [IDS.A, 0, J, '6b', '76', 2],
        [IDS.A, 1, H, '6b', '76', 2],
      ],
      mutations: [
        [IDS.A, 0, J, J, [2]],
        [IDS.A, 0, H, J, [1]],
        [IDS.A, 1, H, J, [1, 2]],
        [IDS.A, 2, H, J, [2]],
      ],
      settlements: [
        [IDS.A, 0, H, H, 'release', 'membership-lost', null],
        [IDS.A, 0, J, J, 'release', null, null],
        [IDS.A, 0, H, J, 'accept', null, null],
        [IDS.A, 1, H, H, 'accept', 'session-lost', null],
        [IDS.A, 1, J, J, 'release', 'session-lost', null],
        [IDS.A, 2, H, J, 'release', 'session-lost', null],
      ],
    },
  },
  {
    fault: 'timeout',
    step: 'close; held handler',
    expected: {
      action: 'close',
      state: 'UNSUBSCRIBED',
      rounds: 8,
      result: 'session-lost',
      cause: null,
      reset: false,
      terminal: true,
      selected: ['A:1@9007199254740993'],
      held: ['A:0@9007199254740993'],
      revoked: [],
      oldAckResends: 0,
      nextEpoch: null,
    },
    observations: {
      handlers: [
        [IDS.A, 0, H, '6b', '76', 2],
        [IDS.A, 0, H, '6b', '76', 2],
        [IDS.A, 0, J, '6b', '76', 2],
        [IDS.A, 1, H, '6b', '76', 2],
      ],
      mutations: [
        [IDS.A, 0, J, J, [2]],
        [IDS.A, 0, H, J, [1]],
        [IDS.A, 1, H, J, [1, 2]],
        [IDS.A, 2, H, J, [2]],
      ],
      settlements: [
        [IDS.A, 0, H, H, 'release', 'membership-lost', null],
        [IDS.A, 0, J, J, 'release', null, null],
        [IDS.A, 0, H, J, 'accept', null, null],
        [IDS.A, 1, H, H, 'accept', 'session-lost', null],
        [IDS.A, 1, J, J, 'release', 'session-lost', null],
        [IDS.A, 2, H, J, 'release', 'session-lost', null],
      ],
    },
  },
  {
    fault: 'timeout',
    step: 'close; held handler and revoked partition',
    expected: {
      action: 'close',
      state: 'UNSUBSCRIBED',
      rounds: 8,
      result: 'session-lost',
      cause: null,
      reset: false,
      terminal: true,
      selected: ['A:1@9007199254740993'],
      held: ['A:0@9007199254740993'],
      revoked: ['A:2@9007199254740993', 'A:2@9007199254740994'],
      oldAckResends: 0,
      nextEpoch: null,
      revokedType: 'release',
      revokedResult: 'session-lost',
      revokedWire: 'eligible terminal frame',
    },
    observations: {
      handlers: [
        [IDS.A, 0, H, '6b', '76', 2],
        [IDS.A, 0, H, '6b', '76', 2],
        [IDS.A, 0, J, '6b', '76', 2],
        [IDS.A, 1, H, '6b', '76', 2],
      ],
      mutations: [
        [IDS.A, 0, J, J, [2]],
        [IDS.A, 0, H, J, [1]],
        [IDS.A, 1, H, J, [1, 2]],
        [IDS.A, 2, H, J, [2]],
      ],
      settlements: [
        [IDS.A, 0, H, H, 'release', 'membership-lost', null],
        [IDS.A, 0, J, J, 'release', null, null],
        [IDS.A, 0, H, J, 'accept', null, null],
        [IDS.A, 1, H, H, 'accept', 'session-lost', null],
        [IDS.A, 1, J, J, 'release', 'session-lost', null],
        [IDS.A, 2, H, J, 'release', 'session-lost', null],
      ],
    },
  },
  {
    fault: 'partition 121',
    step: 'renew; held handler',
    expected: {
      action: 'renew',
      state: 'STABLE',
      rounds: 8,
      result: 'rejected-by-broker',
      cause: 121,
      reset: false,
      terminal: false,
      selected: ['A:1@9007199254740993'],
      held: ['A:0@9007199254740993'],
      revoked: [],
      oldAckResends: 0,
      nextEpoch: 3,
    },
    observations: {
      handlers: [
        [IDS.A, 0, H, '6b', '76', 2],
        [IDS.A, 0, J, '6b', '76', 2],
        [IDS.A, 1, H, '6b', '76', 2],
        [IDS.A, 1, J, '6b', '76', 2],
        [IDS.A, 2, H, '6b', '76', 2],
        [IDS.A, 2, J, '6b', '76', 2],
      ],
      mutations: [
        [IDS.A, 0, H, J, [1]],
        [IDS.A, 1, H, H, [4]],
        [IDS.A, 1, H, J, [1]],
        [IDS.A, 2, H, J, [1]],
      ],
      settlements: [
        [IDS.A, 0, H, J, 'accept', null, null],
        [IDS.A, 1, H, H, 'renew', 'rejected-by-broker', 121],
        [IDS.A, 1, H, J, 'accept', null, null],
        [IDS.A, 2, H, J, 'accept', null, null],
      ],
    },
  },
  {
    fault: 'partition 121',
    step: 'renew; held handler and revoked partition',
    expected: {
      action: 'renew',
      state: 'STABLE',
      rounds: 8,
      result: 'rejected-by-broker',
      cause: 121,
      reset: false,
      terminal: false,
      selected: ['A:1@9007199254740993'],
      held: ['A:0@9007199254740993'],
      revoked: ['A:2@9007199254740993', 'A:2@9007199254740994'],
      oldAckResends: 0,
      nextEpoch: 3,
      revokedType: 'release',
      revokedResult: 'accepted',
      revokedWire: 'eligible terminal frame',
    },
    observations: {
      handlers: [
        [IDS.A, 0, H, '6b', '76', 2],
        [IDS.A, 0, J, '6b', '76', 2],
        [IDS.A, 1, H, '6b', '76', 2],
        [IDS.A, 1, J, '6b', '76', 2],
      ],
      mutations: [
        [IDS.A, 0, H, J, [1]],
        [IDS.A, 1, H, H, [4]],
        [IDS.A, 1, H, J, [1]],
        [IDS.A, 2, H, J, [2]],
      ],
      settlements: [
        [IDS.A, 0, H, J, 'accept', null, null],
        [IDS.A, 1, H, H, 'renew', 'rejected-by-broker', 121],
        [IDS.A, 1, H, J, 'accept', null, null],
        [IDS.A, 2, H, J, 'release', null, null],
      ],
    },
  },
  {
    fault: 'partition 6',
    step: 'renew; held handler',
    expected: {
      action: 'renew',
      state: 'STABLE',
      rounds: 8,
      result: 'session-lost',
      cause: 6,
      reset: false,
      terminal: false,
      selected: ['A:1@9007199254740993'],
      held: ['A:0@9007199254740993'],
      revoked: [],
      oldAckResends: 0,
      nextEpoch: 3,
    },
    observations: {
      handlers: [
        [IDS.A, 0, H, '6b', '76', 2],
        [IDS.A, 0, J, '6b', '76', 2],
        [IDS.A, 1, H, '6b', '76', 2],
        [IDS.A, 1, J, '6b', '76', 2],
        [IDS.A, 2, H, '6b', '76', 2],
        [IDS.A, 2, J, '6b', '76', 2],
      ],
      mutations: [
        [IDS.A, 0, H, J, [1]],
        [IDS.A, 1, H, H, [4]],
        [IDS.A, 1, H, J, [1]],
        [IDS.A, 2, H, J, [1]],
      ],
      settlements: [
        [IDS.A, 0, H, J, 'accept', null, null],
        [IDS.A, 1, H, H, 'renew', 'session-lost', 6],
        [IDS.A, 1, H, J, 'accept', null, null],
        [IDS.A, 2, H, J, 'accept', null, null],
      ],
    },
  },
  {
    fault: 'partition 6',
    step: 'renew; held handler and revoked partition',
    expected: {
      action: 'renew',
      state: 'STABLE',
      rounds: 8,
      result: 'session-lost',
      cause: 6,
      reset: false,
      terminal: false,
      selected: ['A:1@9007199254740993'],
      held: ['A:0@9007199254740993'],
      revoked: ['A:2@9007199254740993', 'A:2@9007199254740994'],
      oldAckResends: 0,
      nextEpoch: 3,
      revokedType: 'release',
      revokedResult: 'accepted',
      revokedWire: 'eligible terminal frame',
    },
    observations: {
      handlers: [
        [IDS.A, 0, H, '6b', '76', 2],
        [IDS.A, 0, J, '6b', '76', 2],
        [IDS.A, 1, H, '6b', '76', 2],
        [IDS.A, 1, J, '6b', '76', 2],
      ],
      mutations: [
        [IDS.A, 0, H, J, [1]],
        [IDS.A, 1, H, H, [4]],
        [IDS.A, 1, H, J, [1]],
        [IDS.A, 2, H, J, [2]],
      ],
      settlements: [
        [IDS.A, 0, H, J, 'accept', null, null],
        [IDS.A, 1, H, H, 'renew', 'session-lost', 6],
        [IDS.A, 1, H, J, 'accept', null, null],
        [IDS.A, 2, H, J, 'release', null, null],
      ],
    },
  },
  {
    fault: 'partition 100',
    step: 'renew; held handler',
    expected: {
      action: 'renew',
      state: 'STABLE',
      rounds: 8,
      result: 'rejected-by-broker',
      cause: 100,
      reset: false,
      terminal: false,
      selected: ['A:1@9007199254740993'],
      held: ['A:0@9007199254740993'],
      revoked: [],
      oldAckResends: 0,
      nextEpoch: 3,
    },
    observations: {
      handlers: [
        [IDS.A, 0, H, '6b', '76', 2],
        [IDS.A, 0, J, '6b', '76', 2],
        [IDS.A, 1, H, '6b', '76', 2],
        [IDS.A, 1, J, '6b', '76', 2],
        [IDS.A, 2, H, '6b', '76', 2],
        [IDS.A, 2, J, '6b', '76', 2],
      ],
      mutations: [
        [IDS.A, 0, H, J, [1]],
        [IDS.A, 1, H, H, [4]],
        [IDS.A, 1, H, J, [1]],
        [IDS.A, 2, H, J, [1]],
      ],
      settlements: [
        [IDS.A, 0, H, J, 'accept', null, null],
        [IDS.A, 1, H, H, 'renew', 'rejected-by-broker', 100],
        [IDS.A, 1, H, J, 'accept', null, null],
        [IDS.A, 2, H, J, 'accept', null, null],
      ],
    },
  },
  {
    fault: 'partition 100',
    step: 'renew; held handler and revoked partition',
    expected: {
      action: 'renew',
      state: 'STABLE',
      rounds: 8,
      result: 'rejected-by-broker',
      cause: 100,
      reset: false,
      terminal: false,
      selected: ['A:1@9007199254740993'],
      held: ['A:0@9007199254740993'],
      revoked: ['A:2@9007199254740993', 'A:2@9007199254740994'],
      oldAckResends: 0,
      nextEpoch: 3,
      revokedType: 'release',
      revokedResult: 'accepted',
      revokedWire: 'eligible terminal frame',
    },
    observations: {
      handlers: [
        [IDS.A, 0, H, '6b', '76', 2],
        [IDS.A, 0, J, '6b', '76', 2],
        [IDS.A, 1, H, '6b', '76', 2],
        [IDS.A, 1, J, '6b', '76', 2],
      ],
      mutations: [
        [IDS.A, 0, H, J, [1]],
        [IDS.A, 1, H, H, [4]],
        [IDS.A, 1, H, J, [1]],
        [IDS.A, 2, H, J, [2]],
      ],
      settlements: [
        [IDS.A, 0, H, J, 'accept', null, null],
        [IDS.A, 1, H, H, 'renew', 'rejected-by-broker', 100],
        [IDS.A, 1, H, J, 'accept', null, null],
        [IDS.A, 2, H, J, 'release', null, null],
      ],
    },
  },
  {
    fault: 'top 122',
    step: 'renew; held handler',
    expected: {
      action: 'renew',
      state: 'STABLE',
      rounds: 8,
      result: 'session-lost',
      cause: 122,
      reset: true,
      terminal: false,
      selected: ['A:1@9007199254740993'],
      held: ['A:0@9007199254740993'],
      revoked: [],
      oldAckResends: 0,
      nextEpoch: 0,
    },
    observations: {
      handlers: [
        [IDS.A, 0, H, '6b', '76', 2],
        [IDS.A, 0, J, '6b', '76', 2],
        [IDS.A, 1, H, '6b', '76', 2],
      ],
      mutations: [
        [IDS.A, 0, H, J, [1]],
        [IDS.A, 1, H, H, [4]],
      ],
      settlements: [
        [IDS.A, 0, H, J, 'accept', null, null],
        [IDS.A, 1, H, H, 'renew', 'session-lost', 122],
        [IDS.A, 1, H, J, 'release', 'session-lost', 122],
        [IDS.A, 2, H, J, 'release', 'session-lost', 122],
      ],
    },
  },
  {
    fault: 'top 122',
    step: 'renew; held handler and revoked partition',
    expected: {
      action: 'renew',
      state: 'STABLE',
      rounds: 8,
      result: 'session-lost',
      cause: 122,
      reset: true,
      terminal: false,
      selected: ['A:1@9007199254740993'],
      held: ['A:0@9007199254740993'],
      revoked: ['A:2@9007199254740993', 'A:2@9007199254740994'],
      oldAckResends: 0,
      nextEpoch: 0,
      revokedType: 'release',
      revokedResult: 'session-lost',
      revokedWire: 'none',
    },
    observations: {
      handlers: [
        [IDS.A, 0, H, '6b', '76', 2],
        [IDS.A, 0, J, '6b', '76', 2],
        [IDS.A, 1, H, '6b', '76', 2],
      ],
      mutations: [
        [IDS.A, 0, H, J, [1]],
        [IDS.A, 1, H, H, [4]],
      ],
      settlements: [
        [IDS.A, 0, H, J, 'accept', null, null],
        [IDS.A, 1, H, H, 'renew', 'session-lost', 122],
        [IDS.A, 1, H, J, 'release', 'session-lost', 122],
        [IDS.A, 2, H, J, 'release', 'session-lost', 122],
      ],
    },
  },
  {
    fault: 'top 123',
    step: 'renew; held handler',
    expected: {
      action: 'renew',
      state: 'STABLE',
      rounds: 8,
      result: 'session-lost',
      cause: 123,
      reset: true,
      terminal: false,
      selected: ['A:1@9007199254740993'],
      held: ['A:0@9007199254740993'],
      revoked: [],
      oldAckResends: 0,
      nextEpoch: 0,
    },
    observations: {
      handlers: [
        [IDS.A, 0, H, '6b', '76', 2],
        [IDS.A, 0, J, '6b', '76', 2],
        [IDS.A, 1, H, '6b', '76', 2],
      ],
      mutations: [
        [IDS.A, 0, H, J, [1]],
        [IDS.A, 1, H, H, [4]],
      ],
      settlements: [
        [IDS.A, 0, H, J, 'accept', null, null],
        [IDS.A, 1, H, H, 'renew', 'session-lost', 123],
        [IDS.A, 1, H, J, 'release', 'session-lost', 123],
        [IDS.A, 2, H, J, 'release', 'session-lost', 123],
      ],
    },
  },
  {
    fault: 'top 123',
    step: 'renew; held handler and revoked partition',
    expected: {
      action: 'renew',
      state: 'STABLE',
      rounds: 8,
      result: 'session-lost',
      cause: 123,
      reset: true,
      terminal: false,
      selected: ['A:1@9007199254740993'],
      held: ['A:0@9007199254740993'],
      revoked: ['A:2@9007199254740993', 'A:2@9007199254740994'],
      oldAckResends: 0,
      nextEpoch: 0,
      revokedType: 'release',
      revokedResult: 'session-lost',
      revokedWire: 'none',
    },
    observations: {
      handlers: [
        [IDS.A, 0, H, '6b', '76', 2],
        [IDS.A, 0, J, '6b', '76', 2],
        [IDS.A, 1, H, '6b', '76', 2],
      ],
      mutations: [
        [IDS.A, 0, H, J, [1]],
        [IDS.A, 1, H, H, [4]],
      ],
      settlements: [
        [IDS.A, 0, H, J, 'accept', null, null],
        [IDS.A, 1, H, H, 'renew', 'session-lost', 123],
        [IDS.A, 1, H, J, 'release', 'session-lost', 123],
        [IDS.A, 2, H, J, 'release', 'session-lost', 123],
      ],
    },
  },
  {
    fault: 'top 133',
    step: 'renew; held handler',
    expected: {
      action: 'renew',
      state: 'STABLE',
      rounds: 8,
      result: 'rejected-by-broker',
      cause: 133,
      reset: false,
      terminal: false,
      selected: ['A:1@9007199254740993'],
      held: ['A:0@9007199254740993'],
      revoked: [],
      oldAckResends: 0,
      nextEpoch: 3,
    },
    observations: {
      handlers: [
        [IDS.A, 0, H, '6b', '76', 2],
        [IDS.A, 0, J, '6b', '76', 2],
        [IDS.A, 1, H, '6b', '76', 2],
        [IDS.A, 1, J, '6b', '76', 2],
        [IDS.A, 2, H, '6b', '76', 2],
        [IDS.A, 2, J, '6b', '76', 2],
      ],
      mutations: [
        [IDS.A, 0, H, J, [1]],
        [IDS.A, 1, H, H, [4]],
        [IDS.A, 1, H, J, [1]],
        [IDS.A, 2, H, J, [1]],
      ],
      settlements: [
        [IDS.A, 0, H, J, 'accept', null, null],
        [IDS.A, 1, H, H, 'renew', 'rejected-by-broker', 133],
        [IDS.A, 1, H, J, 'accept', null, null],
        [IDS.A, 2, H, J, 'accept', null, null],
      ],
    },
  },
  {
    fault: 'top 133',
    step: 'renew; held handler and revoked partition',
    expected: {
      action: 'renew',
      state: 'STABLE',
      rounds: 8,
      result: 'rejected-by-broker',
      cause: 133,
      reset: false,
      terminal: false,
      selected: ['A:1@9007199254740993'],
      held: ['A:0@9007199254740993'],
      revoked: ['A:2@9007199254740993', 'A:2@9007199254740994'],
      oldAckResends: 0,
      nextEpoch: 3,
      revokedType: 'release',
      revokedResult: 'accepted',
      revokedWire: 'eligible terminal frame',
    },
    observations: {
      handlers: [
        [IDS.A, 0, H, '6b', '76', 2],
        [IDS.A, 0, J, '6b', '76', 2],
        [IDS.A, 1, H, '6b', '76', 2],
        [IDS.A, 1, J, '6b', '76', 2],
      ],
      mutations: [
        [IDS.A, 0, H, J, [1]],
        [IDS.A, 1, H, H, [4]],
        [IDS.A, 1, H, J, [1]],
        [IDS.A, 2, H, J, [2]],
      ],
      settlements: [
        [IDS.A, 0, H, J, 'accept', null, null],
        [IDS.A, 1, H, H, 'renew', 'rejected-by-broker', 133],
        [IDS.A, 1, H, J, 'accept', null, null],
        [IDS.A, 2, H, J, 'release', null, null],
      ],
    },
  },
  {
    fault: 'disconnect',
    step: 'renew; held handler',
    expected: {
      action: 'renew',
      state: 'STABLE',
      rounds: 8,
      result: 'session-lost',
      cause: null,
      reset: true,
      terminal: false,
      selected: ['A:1@9007199254740993'],
      held: ['A:0@9007199254740993'],
      revoked: [],
      oldAckResends: 0,
      nextEpoch: 0,
    },
    observations: {
      handlers: [
        [IDS.A, 0, H, '6b', '76', 2],
        [IDS.A, 0, J, '6b', '76', 2],
        [IDS.A, 1, H, '6b', '76', 2],
      ],
      mutations: [
        [IDS.A, 0, H, J, [1]],
        [IDS.A, 1, H, H, [4]],
      ],
      settlements: [
        [IDS.A, 0, H, J, 'accept', null, null],
        [IDS.A, 1, H, H, 'renew', 'session-lost', null],
        [IDS.A, 1, H, J, 'release', 'session-lost', null],
        [IDS.A, 2, H, J, 'release', 'session-lost', null],
      ],
    },
  },
  {
    fault: 'disconnect',
    step: 'renew; held handler and revoked partition',
    expected: {
      action: 'renew',
      state: 'STABLE',
      rounds: 8,
      result: 'session-lost',
      cause: null,
      reset: true,
      terminal: false,
      selected: ['A:1@9007199254740993'],
      held: ['A:0@9007199254740993'],
      revoked: ['A:2@9007199254740993', 'A:2@9007199254740994'],
      oldAckResends: 0,
      nextEpoch: 0,
      revokedType: 'release',
      revokedResult: 'session-lost',
      revokedWire: 'none',
    },
    observations: {
      handlers: [
        [IDS.A, 0, H, '6b', '76', 2],
        [IDS.A, 0, J, '6b', '76', 2],
        [IDS.A, 1, H, '6b', '76', 2],
      ],
      mutations: [
        [IDS.A, 0, H, J, [1]],
        [IDS.A, 1, H, H, [4]],
      ],
      settlements: [
        [IDS.A, 0, H, J, 'accept', null, null],
        [IDS.A, 1, H, H, 'renew', 'session-lost', null],
        [IDS.A, 1, H, J, 'release', 'session-lost', null],
        [IDS.A, 2, H, J, 'release', 'session-lost', null],
      ],
    },
  },
  {
    fault: 'timeout',
    step: 'renew; held handler',
    expected: {
      action: 'renew',
      state: 'STABLE',
      rounds: 8,
      result: 'session-lost',
      cause: null,
      reset: true,
      terminal: false,
      selected: ['A:1@9007199254740993'],
      held: ['A:0@9007199254740993'],
      revoked: [],
      oldAckResends: 0,
      nextEpoch: 0,
    },
    observations: {
      handlers: [
        [IDS.A, 0, H, '6b', '76', 2],
        [IDS.A, 0, J, '6b', '76', 2],
        [IDS.A, 1, H, '6b', '76', 2],
      ],
      mutations: [
        [IDS.A, 0, H, J, [1]],
        [IDS.A, 1, H, H, [4]],
      ],
      settlements: [
        [IDS.A, 0, H, J, 'accept', null, null],
        [IDS.A, 1, H, H, 'renew', 'session-lost', null],
        [IDS.A, 1, H, J, 'release', 'session-lost', null],
        [IDS.A, 2, H, J, 'release', 'session-lost', null],
      ],
    },
  },
  {
    fault: 'timeout',
    step: 'renew; held handler and revoked partition',
    expected: {
      action: 'renew',
      state: 'STABLE',
      rounds: 8,
      result: 'session-lost',
      cause: null,
      reset: true,
      terminal: false,
      selected: ['A:1@9007199254740993'],
      held: ['A:0@9007199254740993'],
      revoked: ['A:2@9007199254740993', 'A:2@9007199254740994'],
      oldAckResends: 0,
      nextEpoch: 0,
      revokedType: 'release',
      revokedResult: 'session-lost',
      revokedWire: 'none',
    },
    observations: {
      handlers: [
        [IDS.A, 0, H, '6b', '76', 2],
        [IDS.A, 0, J, '6b', '76', 2],
        [IDS.A, 1, H, '6b', '76', 2],
      ],
      mutations: [
        [IDS.A, 0, H, J, [1]],
        [IDS.A, 1, H, H, [4]],
      ],
      settlements: [
        [IDS.A, 0, H, J, 'accept', null, null],
        [IDS.A, 1, H, H, 'renew', 'session-lost', null],
        [IDS.A, 1, H, J, 'release', 'session-lost', null],
        [IDS.A, 2, H, J, 'release', 'session-lost', null],
      ],
    },
  },
  {
    fault: 'partition 74',
    step: 'flush',
    expected: {
      action: 'flush',
      state: 'STABLE',
      rounds: 8,
      terminal: true,
      selected: ['A:1@9007199254740993'],
      oldAckResends: 0,
      result: 'session-lost',
      cause: 74,
    },
    observations: {
      handlers: [
        [IDS.A, 0, H, '6b', '76', 2],
        [IDS.A, 0, J, '6b', '76', 2],
        [IDS.A, 1, H, '6b', '76', 2],
        [IDS.A, 1, J, '6b', '76', 2],
        [IDS.A, 2, H, '6b', '76', 2],
        [IDS.A, 2, J, '6b', '76', 2],
      ],
      mutations: [
        [IDS.A, 0, H, J, [1]],
        [IDS.A, 1, H, H, [1]],
        [IDS.A, 1, J, J, [1]],
        [IDS.A, 2, H, J, [1]],
      ],
      settlements: [
        [IDS.A, 0, H, J, 'accept', null, null],
        [IDS.A, 1, H, H, 'accept', 'session-lost', 74],
        [IDS.A, 1, J, J, 'accept', null, null],
        [IDS.A, 2, H, J, 'accept', null, null],
      ],
    },
  },
  {
    fault: 'partition unknown',
    step: 'flush',
    expected: {
      action: 'flush',
      state: 'STABLE',
      rounds: 8,
      terminal: true,
      selected: ['A:1@9007199254740993'],
      oldAckResends: 0,
      result: 'rejected-by-broker',
      cause: -1,
    },
    observations: {
      handlers: [
        [IDS.A, 0, H, '6b', '76', 2],
        [IDS.A, 0, J, '6b', '76', 2],
        [IDS.A, 1, H, '6b', '76', 2],
        [IDS.A, 1, J, '6b', '76', 2],
        [IDS.A, 2, H, '6b', '76', 2],
        [IDS.A, 2, J, '6b', '76', 2],
      ],
      mutations: [
        [IDS.A, 0, H, J, [1]],
        [IDS.A, 1, H, H, [1]],
        [IDS.A, 1, J, J, [1]],
        [IDS.A, 2, H, J, [1]],
      ],
      settlements: [
        [IDS.A, 0, H, J, 'accept', null, null],
        [IDS.A, 1, H, H, 'accept', 'rejected-by-broker', -1],
        [IDS.A, 1, J, J, 'accept', null, null],
        [IDS.A, 2, H, J, 'accept', null, null],
      ],
    },
  },
  {
    fault: 'omitted partition',
    step: 'flush',
    expected: {
      action: 'flush',
      state: 'STABLE',
      rounds: 8,
      terminal: true,
      selected: ['A:1@9007199254740993'],
      oldAckResends: 0,
      result: 'rejected-by-broker',
      cause: 'invalid response',
    },
    observations: {
      handlers: [
        [IDS.A, 0, H, '6b', '76', 2],
        [IDS.A, 0, J, '6b', '76', 2],
        [IDS.A, 1, H, '6b', '76', 2],
        [IDS.A, 1, J, '6b', '76', 2],
        [IDS.A, 2, H, '6b', '76', 2],
        [IDS.A, 2, J, '6b', '76', 2],
      ],
      mutations: [
        [IDS.A, 0, H, J, [1]],
        [IDS.A, 1, H, H, [1]],
        [IDS.A, 1, J, J, [1]],
        [IDS.A, 2, H, J, [1]],
      ],
      settlements: [
        [IDS.A, 0, H, J, 'accept', null, null],
        [IDS.A, 1, H, H, 'accept', 'rejected-by-broker', null],
        [IDS.A, 1, J, J, 'accept', null, null],
        [IDS.A, 2, H, J, 'accept', null, null],
      ],
    },
  },
  {
    fault: 'duplicate partition',
    step: 'flush',
    expected: {
      action: 'flush',
      state: 'STABLE',
      rounds: 8,
      terminal: true,
      selected: ['A:1@9007199254740993'],
      oldAckResends: 0,
      result: 'rejected-by-broker',
      cause: 'invalid response',
    },
    observations: {
      handlers: [
        [IDS.A, 0, H, '6b', '76', 2],
        [IDS.A, 0, J, '6b', '76', 2],
        [IDS.A, 1, H, '6b', '76', 2],
        [IDS.A, 1, J, '6b', '76', 2],
        [IDS.A, 2, H, '6b', '76', 2],
        [IDS.A, 2, J, '6b', '76', 2],
      ],
      mutations: [
        [IDS.A, 0, H, J, [1]],
        [IDS.A, 1, H, H, [1]],
        [IDS.A, 1, J, J, [1]],
        [IDS.A, 2, H, J, [1]],
      ],
      settlements: [
        [IDS.A, 0, H, J, 'accept', null, null],
        [IDS.A, 1, H, H, 'accept', 'rejected-by-broker', null],
        [IDS.A, 1, J, J, 'accept', null, null],
        [IDS.A, 2, H, J, 'accept', null, null],
      ],
    },
  },
  {
    fault: 'top 35',
    step: 'flush',
    expected: {
      action: 'flush',
      state: 'FATAL',
      rounds: 8,
      terminal: true,
      selected: ['A:1@9007199254740993'],
      oldAckResends: 0,
      result: 'rejected-by-broker',
      cause: 35,
    },
    observations: {
      handlers: [
        [IDS.A, 0, H, '6b', '76', 2],
        [IDS.A, 0, H, '6b', '76', 2],
        [IDS.A, 0, J, '6b', '76', 2],
        [IDS.A, 1, H, '6b', '76', 2],
        [IDS.A, 1, J, '6b', '76', 2],
      ],
      mutations: [
        [IDS.A, 0, J, J, [2]],
        [IDS.A, 0, H, J, [1]],
        [IDS.A, 1, H, H, [1]],
        [IDS.A, 2, H, J, [2]],
      ],
      settlements: [
        [IDS.A, 0, H, H, 'release', 'membership-lost', null],
        [IDS.A, 0, J, J, 'release', null, null],
        [IDS.A, 0, H, J, 'accept', null, null],
        [IDS.A, 1, H, H, 'accept', 'rejected-by-broker', 35],
        [IDS.A, 1, J, J, 'release', 'membership-lost', null],
        [IDS.A, 2, H, J, 'release', null, null],
      ],
    },
  },
  {
    fault: 'top 30',
    step: 'flush',
    expected: {
      action: 'flush',
      state: 'FATAL',
      rounds: 8,
      terminal: true,
      selected: ['A:1@9007199254740993'],
      oldAckResends: 0,
      result: 'rejected-by-broker',
      cause: 30,
    },
    observations: {
      handlers: [
        [IDS.A, 0, H, '6b', '76', 2],
        [IDS.A, 0, H, '6b', '76', 2],
        [IDS.A, 0, J, '6b', '76', 2],
        [IDS.A, 1, H, '6b', '76', 2],
        [IDS.A, 1, J, '6b', '76', 2],
      ],
      mutations: [
        [IDS.A, 0, J, J, [2]],
        [IDS.A, 0, H, J, [1]],
        [IDS.A, 1, H, H, [1]],
        [IDS.A, 2, H, J, [2]],
      ],
      settlements: [
        [IDS.A, 0, H, H, 'release', 'membership-lost', null],
        [IDS.A, 0, J, J, 'release', null, null],
        [IDS.A, 0, H, J, 'accept', null, null],
        [IDS.A, 1, H, H, 'accept', 'rejected-by-broker', 30],
        [IDS.A, 1, J, J, 'release', 'membership-lost', null],
        [IDS.A, 2, H, J, 'release', null, null],
      ],
    },
  },
  {
    fault: 'partition 74',
    step: 'piggyback',
    expected: {
      action: 'piggyback',
      state: 'STABLE',
      rounds: 8,
      terminal: true,
      selected: ['A:1@9007199254740993'],
      oldAckResends: 0,
      result: 'session-lost',
      cause: 74,
    },
    observations: {
      handlers: [
        [IDS.A, 0, H, '6b', '76', 2],
        [IDS.A, 0, J, '6b', '76', 2],
        [IDS.A, 1, H, '6b', '76', 2],
        [IDS.A, 1, J, '6b', '76', 2],
        [IDS.A, 2, H, '6b', '76', 2],
        [IDS.A, 2, J, '6b', '76', 2],
      ],
      mutations: [
        [IDS.A, 0, H, J, [1]],
        [IDS.A, 1, H, J, [1]],
        [IDS.A, 2, H, J, [1]],
      ],
      settlements: [
        [IDS.A, 0, H, J, 'accept', null, null],
        [IDS.A, 1, H, J, 'accept', 'session-lost', 74],
        [IDS.A, 2, H, J, 'accept', null, null],
      ],
    },
  },
  {
    fault: 'partition unknown',
    step: 'piggyback',
    expected: {
      action: 'piggyback',
      state: 'STABLE',
      rounds: 8,
      terminal: true,
      selected: ['A:1@9007199254740993'],
      oldAckResends: 0,
      result: 'rejected-by-broker',
      cause: -1,
    },
    observations: {
      handlers: [
        [IDS.A, 0, H, '6b', '76', 2],
        [IDS.A, 0, J, '6b', '76', 2],
        [IDS.A, 1, H, '6b', '76', 2],
        [IDS.A, 1, J, '6b', '76', 2],
        [IDS.A, 2, H, '6b', '76', 2],
        [IDS.A, 2, J, '6b', '76', 2],
      ],
      mutations: [
        [IDS.A, 0, H, J, [1]],
        [IDS.A, 1, H, J, [1]],
        [IDS.A, 2, H, J, [1]],
      ],
      settlements: [
        [IDS.A, 0, H, J, 'accept', null, null],
        [IDS.A, 1, H, J, 'accept', 'rejected-by-broker', -1],
        [IDS.A, 2, H, J, 'accept', null, null],
      ],
    },
  },
  {
    fault: 'omitted partition',
    step: 'piggyback',
    expected: {
      action: 'piggyback',
      state: 'STABLE',
      rounds: 8,
      terminal: true,
      selected: ['A:1@9007199254740993'],
      oldAckResends: 0,
      result: 'rejected-by-broker',
      cause: 'invalid response',
    },
    observations: {
      handlers: [
        [IDS.A, 0, H, '6b', '76', 2],
        [IDS.A, 0, J, '6b', '76', 2],
        [IDS.A, 1, H, '6b', '76', 2],
        [IDS.A, 1, J, '6b', '76', 2],
        [IDS.A, 2, H, '6b', '76', 2],
        [IDS.A, 2, J, '6b', '76', 2],
      ],
      mutations: [
        [IDS.A, 0, H, J, [1]],
        [IDS.A, 1, H, J, [1]],
        [IDS.A, 2, H, J, [1]],
      ],
      settlements: [
        [IDS.A, 0, H, J, 'accept', null, null],
        [IDS.A, 1, H, J, 'accept', 'rejected-by-broker', null],
        [IDS.A, 2, H, J, 'accept', null, null],
      ],
    },
  },
  {
    fault: 'duplicate partition',
    step: 'piggyback',
    expected: {
      action: 'piggyback',
      state: 'STABLE',
      rounds: 8,
      terminal: true,
      selected: ['A:1@9007199254740993'],
      oldAckResends: 0,
      result: 'rejected-by-broker',
      cause: 'invalid response',
    },
    observations: {
      handlers: [
        [IDS.A, 0, H, '6b', '76', 2],
        [IDS.A, 0, J, '6b', '76', 2],
        [IDS.A, 1, H, '6b', '76', 2],
        [IDS.A, 1, J, '6b', '76', 2],
        [IDS.A, 2, H, '6b', '76', 2],
        [IDS.A, 2, J, '6b', '76', 2],
      ],
      mutations: [
        [IDS.A, 0, H, J, [1]],
        [IDS.A, 1, H, J, [1]],
        [IDS.A, 2, H, J, [1]],
      ],
      settlements: [
        [IDS.A, 0, H, J, 'accept', null, null],
        [IDS.A, 1, H, J, 'accept', 'rejected-by-broker', null],
        [IDS.A, 2, H, J, 'accept', null, null],
      ],
    },
  },
  {
    fault: 'top 35',
    step: 'piggyback',
    expected: {
      action: 'piggyback',
      state: 'FATAL',
      rounds: 8,
      terminal: true,
      selected: ['A:1@9007199254740993'],
      oldAckResends: 0,
      result: 'rejected-by-broker',
      cause: 35,
    },
    observations: {
      handlers: [
        [IDS.A, 0, H, '6b', '76', 2],
        [IDS.A, 0, H, '6b', '76', 2],
        [IDS.A, 0, J, '6b', '76', 2],
        [IDS.A, 1, H, '6b', '76', 2],
        [IDS.A, 1, J, '6b', '76', 2],
        [IDS.A, 2, H, '6b', '76', 2],
        [IDS.A, 2, J, '6b', '76', 2],
      ],
      mutations: [
        [IDS.A, 0, J, J, [2]],
        [IDS.A, 0, H, J, [1]],
        [IDS.A, 1, H, J, [1]],
        [IDS.A, 2, H, J, [1]],
      ],
      settlements: [
        [IDS.A, 0, H, H, 'release', 'membership-lost', null],
        [IDS.A, 0, J, J, 'release', null, null],
        [IDS.A, 0, H, J, 'accept', null, null],
        [IDS.A, 1, H, J, 'accept', 'rejected-by-broker', 35],
        [IDS.A, 2, H, J, 'accept', 'rejected-by-broker', 35],
      ],
    },
  },
  {
    fault: 'top 30',
    step: 'piggyback',
    expected: {
      action: 'piggyback',
      state: 'FATAL',
      rounds: 8,
      terminal: true,
      selected: ['A:1@9007199254740993'],
      oldAckResends: 0,
      result: 'rejected-by-broker',
      cause: 30,
    },
    observations: {
      handlers: [
        [IDS.A, 0, H, '6b', '76', 2],
        [IDS.A, 0, H, '6b', '76', 2],
        [IDS.A, 0, J, '6b', '76', 2],
        [IDS.A, 1, H, '6b', '76', 2],
        [IDS.A, 1, J, '6b', '76', 2],
        [IDS.A, 2, H, '6b', '76', 2],
        [IDS.A, 2, J, '6b', '76', 2],
      ],
      mutations: [
        [IDS.A, 0, J, J, [2]],
        [IDS.A, 0, H, J, [1]],
        [IDS.A, 1, H, J, [1]],
        [IDS.A, 2, H, J, [1]],
      ],
      settlements: [
        [IDS.A, 0, H, H, 'release', 'membership-lost', null],
        [IDS.A, 0, J, J, 'release', null, null],
        [IDS.A, 0, H, J, 'accept', null, null],
        [IDS.A, 1, H, J, 'accept', 'rejected-by-broker', 30],
        [IDS.A, 2, H, J, 'accept', 'rejected-by-broker', 30],
      ],
    },
  },
  {
    fault: 'partition 74',
    step: 'close',
    expected: {
      action: 'close',
      state: 'UNSUBSCRIBED',
      rounds: 8,
      terminal: true,
      selected: ['A:1@9007199254740993'],
      oldAckResends: 0,
      result: 'session-lost',
      cause: 74,
    },
    observations: {
      handlers: [
        [IDS.A, 0, H, '6b', '76', 2],
        [IDS.A, 0, H, '6b', '76', 2],
        [IDS.A, 0, J, '6b', '76', 2],
        [IDS.A, 1, H, '6b', '76', 2],
      ],
      mutations: [
        [IDS.A, 0, J, J, [2]],
        [IDS.A, 0, H, J, [1]],
        [IDS.A, 1, H, J, [1, 2]],
        [IDS.A, 2, H, J, [2]],
      ],
      settlements: [
        [IDS.A, 0, H, H, 'release', 'membership-lost', null],
        [IDS.A, 0, J, J, 'release', null, null],
        [IDS.A, 0, H, J, 'accept', null, null],
        [IDS.A, 1, H, H, 'accept', 'session-lost', 74],
        [IDS.A, 1, J, J, 'release', 'session-lost', 74],
        [IDS.A, 2, H, J, 'release', null, null],
      ],
    },
  },
  {
    fault: 'partition unknown',
    step: 'close',
    expected: {
      action: 'close',
      state: 'UNSUBSCRIBED',
      rounds: 8,
      terminal: true,
      selected: ['A:1@9007199254740993'],
      oldAckResends: 0,
      result: 'rejected-by-broker',
      cause: -1,
    },
    observations: {
      handlers: [
        [IDS.A, 0, H, '6b', '76', 2],
        [IDS.A, 0, H, '6b', '76', 2],
        [IDS.A, 0, J, '6b', '76', 2],
        [IDS.A, 1, H, '6b', '76', 2],
      ],
      mutations: [
        [IDS.A, 0, J, J, [2]],
        [IDS.A, 0, H, J, [1]],
        [IDS.A, 1, H, J, [1, 2]],
        [IDS.A, 2, H, J, [2]],
      ],
      settlements: [
        [IDS.A, 0, H, H, 'release', 'membership-lost', null],
        [IDS.A, 0, J, J, 'release', null, null],
        [IDS.A, 0, H, J, 'accept', null, null],
        [IDS.A, 1, H, H, 'accept', 'rejected-by-broker', -1],
        [IDS.A, 1, J, J, 'release', 'rejected-by-broker', -1],
        [IDS.A, 2, H, J, 'release', null, null],
      ],
    },
  },
  {
    fault: 'omitted partition',
    step: 'close',
    expected: {
      action: 'close',
      state: 'UNSUBSCRIBED',
      rounds: 8,
      terminal: true,
      selected: ['A:1@9007199254740993'],
      oldAckResends: 0,
      result: 'rejected-by-broker',
      cause: 'invalid response',
    },
    observations: {
      handlers: [
        [IDS.A, 0, H, '6b', '76', 2],
        [IDS.A, 0, H, '6b', '76', 2],
        [IDS.A, 0, J, '6b', '76', 2],
        [IDS.A, 1, H, '6b', '76', 2],
      ],
      mutations: [
        [IDS.A, 0, J, J, [2]],
        [IDS.A, 0, H, J, [1]],
        [IDS.A, 1, H, J, [1, 2]],
        [IDS.A, 2, H, J, [2]],
      ],
      settlements: [
        [IDS.A, 0, H, H, 'release', 'membership-lost', null],
        [IDS.A, 0, J, J, 'release', null, null],
        [IDS.A, 0, H, J, 'accept', null, null],
        [IDS.A, 1, H, H, 'accept', 'rejected-by-broker', null],
        [IDS.A, 1, J, J, 'release', 'rejected-by-broker', null],
        [IDS.A, 2, H, J, 'release', null, null],
      ],
    },
  },
  {
    fault: 'duplicate partition',
    step: 'close',
    expected: {
      action: 'close',
      state: 'UNSUBSCRIBED',
      rounds: 8,
      terminal: true,
      selected: ['A:1@9007199254740993'],
      oldAckResends: 0,
      result: 'rejected-by-broker',
      cause: 'invalid response',
    },
    observations: {
      handlers: [
        [IDS.A, 0, H, '6b', '76', 2],
        [IDS.A, 0, H, '6b', '76', 2],
        [IDS.A, 0, J, '6b', '76', 2],
        [IDS.A, 1, H, '6b', '76', 2],
      ],
      mutations: [
        [IDS.A, 0, J, J, [2]],
        [IDS.A, 0, H, J, [1]],
        [IDS.A, 1, H, J, [1, 2]],
        [IDS.A, 2, H, J, [2]],
      ],
      settlements: [
        [IDS.A, 0, H, H, 'release', 'membership-lost', null],
        [IDS.A, 0, J, J, 'release', null, null],
        [IDS.A, 0, H, J, 'accept', null, null],
        [IDS.A, 1, H, H, 'accept', 'rejected-by-broker', null],
        [IDS.A, 1, J, J, 'release', 'rejected-by-broker', null],
        [IDS.A, 2, H, J, 'release', null, null],
      ],
    },
  },
  {
    fault: 'top 35',
    step: 'close',
    expected: {
      action: 'close',
      state: 'UNSUBSCRIBED',
      rounds: 8,
      terminal: true,
      selected: ['A:1@9007199254740993'],
      oldAckResends: 0,
      result: 'rejected-by-broker',
      cause: 35,
    },
    observations: {
      handlers: [
        [IDS.A, 0, H, '6b', '76', 2],
        [IDS.A, 0, H, '6b', '76', 2],
        [IDS.A, 0, J, '6b', '76', 2],
        [IDS.A, 1, H, '6b', '76', 2],
      ],
      mutations: [
        [IDS.A, 0, J, J, [2]],
        [IDS.A, 0, H, J, [1]],
        [IDS.A, 1, H, J, [1, 2]],
        [IDS.A, 2, H, J, [2]],
      ],
      settlements: [
        [IDS.A, 0, H, H, 'release', 'membership-lost', null],
        [IDS.A, 0, J, J, 'release', null, null],
        [IDS.A, 0, H, J, 'accept', null, null],
        [IDS.A, 1, H, H, 'accept', 'rejected-by-broker', 35],
        [IDS.A, 1, J, J, 'release', 'rejected-by-broker', 35],
        [IDS.A, 2, H, J, 'release', 'rejected-by-broker', 35],
      ],
    },
  },
  {
    fault: 'top 30',
    step: 'close',
    expected: {
      action: 'close',
      state: 'UNSUBSCRIBED',
      rounds: 8,
      terminal: true,
      selected: ['A:1@9007199254740993'],
      oldAckResends: 0,
      result: 'rejected-by-broker',
      cause: 30,
    },
    observations: {
      handlers: [
        [IDS.A, 0, H, '6b', '76', 2],
        [IDS.A, 0, H, '6b', '76', 2],
        [IDS.A, 0, J, '6b', '76', 2],
        [IDS.A, 1, H, '6b', '76', 2],
      ],
      mutations: [
        [IDS.A, 0, J, J, [2]],
        [IDS.A, 0, H, J, [1]],
        [IDS.A, 1, H, J, [1, 2]],
        [IDS.A, 2, H, J, [2]],
      ],
      settlements: [
        [IDS.A, 0, H, H, 'release', 'membership-lost', null],
        [IDS.A, 0, J, J, 'release', null, null],
        [IDS.A, 0, H, J, 'accept', null, null],
        [IDS.A, 1, H, H, 'accept', 'rejected-by-broker', 30],
        [IDS.A, 1, J, J, 'release', 'rejected-by-broker', 30],
        [IDS.A, 2, H, J, 'release', 'rejected-by-broker', 30],
      ],
    },
  },
  {
    fault: 'partition 74',
    step: 'renew',
    expected: {
      action: 'renew',
      state: 'STABLE',
      rounds: 8,
      terminal: false,
      selected: ['A:1@9007199254740993'],
      oldAckResends: 0,
      result: 'session-lost',
      cause: 74,
    },
    observations: {
      handlers: [
        [IDS.A, 0, H, '6b', '76', 2],
        [IDS.A, 0, J, '6b', '76', 2],
        [IDS.A, 1, H, '6b', '76', 2],
        [IDS.A, 1, J, '6b', '76', 2],
        [IDS.A, 2, H, '6b', '76', 2],
        [IDS.A, 2, J, '6b', '76', 2],
      ],
      mutations: [
        [IDS.A, 0, H, J, [1]],
        [IDS.A, 1, H, H, [4]],
        [IDS.A, 1, H, J, [1]],
        [IDS.A, 2, H, J, [1]],
      ],
      settlements: [
        [IDS.A, 0, H, J, 'accept', null, null],
        [IDS.A, 1, H, H, 'renew', 'session-lost', 74],
        [IDS.A, 1, H, J, 'accept', null, null],
        [IDS.A, 2, H, J, 'accept', null, null],
      ],
    },
  },
  {
    fault: 'partition unknown',
    step: 'renew',
    expected: {
      action: 'renew',
      state: 'STABLE',
      rounds: 8,
      terminal: false,
      selected: ['A:1@9007199254740993'],
      oldAckResends: 0,
      result: 'rejected-by-broker',
      cause: -1,
    },
    observations: {
      handlers: [
        [IDS.A, 0, H, '6b', '76', 2],
        [IDS.A, 0, J, '6b', '76', 2],
        [IDS.A, 1, H, '6b', '76', 2],
        [IDS.A, 1, J, '6b', '76', 2],
        [IDS.A, 2, H, '6b', '76', 2],
        [IDS.A, 2, J, '6b', '76', 2],
      ],
      mutations: [
        [IDS.A, 0, H, J, [1]],
        [IDS.A, 1, H, H, [4]],
        [IDS.A, 1, H, J, [1]],
        [IDS.A, 2, H, J, [1]],
      ],
      settlements: [
        [IDS.A, 0, H, J, 'accept', null, null],
        [IDS.A, 1, H, H, 'renew', 'rejected-by-broker', -1],
        [IDS.A, 1, H, J, 'accept', null, null],
        [IDS.A, 2, H, J, 'accept', null, null],
      ],
    },
  },
  {
    fault: 'omitted partition',
    step: 'renew',
    expected: {
      action: 'renew',
      state: 'STABLE',
      rounds: 8,
      terminal: false,
      selected: ['A:1@9007199254740993'],
      oldAckResends: 0,
      result: 'rejected-by-broker',
      cause: 'invalid response',
    },
    observations: {
      handlers: [
        [IDS.A, 0, H, '6b', '76', 2],
        [IDS.A, 0, J, '6b', '76', 2],
        [IDS.A, 1, H, '6b', '76', 2],
        [IDS.A, 1, J, '6b', '76', 2],
        [IDS.A, 2, H, '6b', '76', 2],
        [IDS.A, 2, J, '6b', '76', 2],
      ],
      mutations: [
        [IDS.A, 0, H, J, [1]],
        [IDS.A, 1, H, H, [4]],
        [IDS.A, 1, H, J, [1]],
        [IDS.A, 2, H, J, [1]],
      ],
      settlements: [
        [IDS.A, 0, H, J, 'accept', null, null],
        [IDS.A, 1, H, H, 'renew', 'rejected-by-broker', null],
        [IDS.A, 1, H, J, 'accept', null, null],
        [IDS.A, 2, H, J, 'accept', null, null],
      ],
    },
  },
  {
    fault: 'duplicate partition',
    step: 'renew',
    expected: {
      action: 'renew',
      state: 'STABLE',
      rounds: 8,
      terminal: false,
      selected: ['A:1@9007199254740993'],
      oldAckResends: 0,
      result: 'rejected-by-broker',
      cause: 'invalid response',
    },
    observations: {
      handlers: [
        [IDS.A, 0, H, '6b', '76', 2],
        [IDS.A, 0, J, '6b', '76', 2],
        [IDS.A, 1, H, '6b', '76', 2],
        [IDS.A, 1, J, '6b', '76', 2],
        [IDS.A, 2, H, '6b', '76', 2],
        [IDS.A, 2, J, '6b', '76', 2],
      ],
      mutations: [
        [IDS.A, 0, H, J, [1]],
        [IDS.A, 1, H, H, [4]],
        [IDS.A, 1, H, J, [1]],
        [IDS.A, 2, H, J, [1]],
      ],
      settlements: [
        [IDS.A, 0, H, J, 'accept', null, null],
        [IDS.A, 1, H, H, 'renew', 'rejected-by-broker', null],
        [IDS.A, 1, H, J, 'accept', null, null],
        [IDS.A, 2, H, J, 'accept', null, null],
      ],
    },
  },
  {
    fault: 'top 35',
    step: 'renew',
    expected: {
      action: 'renew',
      state: 'FATAL',
      rounds: 8,
      terminal: false,
      selected: ['A:1@9007199254740993'],
      oldAckResends: 0,
      result: 'rejected-by-broker',
      cause: 35,
    },
    observations: {
      handlers: [
        [IDS.A, 0, H, '6b', '76', 2],
        [IDS.A, 0, H, '6b', '76', 2],
        [IDS.A, 0, J, '6b', '76', 2],
        [IDS.A, 1, H, '6b', '76', 2],
      ],
      mutations: [
        [IDS.A, 0, J, J, [2]],
        [IDS.A, 0, H, J, [1]],
        [IDS.A, 1, H, H, [4]],
        [IDS.A, 1, J, J, [2]],
        [IDS.A, 2, H, J, [2]],
      ],
      settlements: [
        [IDS.A, 0, H, H, 'release', 'membership-lost', null],
        [IDS.A, 0, J, J, 'release', null, null],
        [IDS.A, 0, H, J, 'accept', null, null],
        [IDS.A, 1, H, H, 'renew', 'rejected-by-broker', 35],
        [IDS.A, 1, H, H, 'release', 'membership-lost', null],
        [IDS.A, 1, J, J, 'release', null, null],
        [IDS.A, 2, H, J, 'release', null, null],
      ],
    },
  },
  {
    fault: 'top 30',
    step: 'renew',
    expected: {
      action: 'renew',
      state: 'FATAL',
      rounds: 8,
      terminal: false,
      selected: ['A:1@9007199254740993'],
      oldAckResends: 0,
      result: 'rejected-by-broker',
      cause: 30,
    },
    observations: {
      handlers: [
        [IDS.A, 0, H, '6b', '76', 2],
        [IDS.A, 0, H, '6b', '76', 2],
        [IDS.A, 0, J, '6b', '76', 2],
        [IDS.A, 1, H, '6b', '76', 2],
      ],
      mutations: [
        [IDS.A, 0, J, J, [2]],
        [IDS.A, 0, H, J, [1]],
        [IDS.A, 1, H, H, [4]],
        [IDS.A, 1, J, J, [2]],
        [IDS.A, 2, H, J, [2]],
      ],
      settlements: [
        [IDS.A, 0, H, H, 'release', 'membership-lost', null],
        [IDS.A, 0, J, J, 'release', null, null],
        [IDS.A, 0, H, J, 'accept', null, null],
        [IDS.A, 1, H, H, 'renew', 'rejected-by-broker', 30],
        [IDS.A, 1, H, H, 'release', 'membership-lost', null],
        [IDS.A, 1, J, J, 'release', null, null],
        [IDS.A, 2, H, J, 'release', null, null],
      ],
    },
  },
  {
    fault: 'successful ack failed fetch',
    step: 'piggyback',
    expected: {
      action: 'piggyback',
      state: 'STABLE',
      rounds: 8,
      terminal: true,
      selected: ['A:1@9007199254740993'],
      oldAckResends: 0,
      result: 'accepted',
      cause: null,
      fetchCode: 6,
    },
    observations: {
      handlers: [
        [IDS.A, 0, H, '6b', '76', 2],
        [IDS.A, 0, J, '6b', '76', 2],
        [IDS.A, 1, H, '6b', '76', 2],
        [IDS.A, 1, J, '6b', '76', 2],
        [IDS.A, 2, H, '6b', '76', 2],
        [IDS.A, 2, J, '6b', '76', 2],
      ],
      mutations: [
        [IDS.A, 0, H, J, [1]],
        [IDS.A, 1, H, J, [1]],
        [IDS.A, 2, H, J, [1]],
      ],
      settlements: [
        [IDS.A, 0, H, J, 'accept', null, null],
        [IDS.A, 1, H, J, 'accept', null, null],
        [IDS.A, 2, H, J, 'accept', null, null],
      ],
    },
  },
  {
    fault: 'failed ack successful fetch',
    step: 'piggyback',
    expected: {
      action: 'piggyback',
      state: 'STABLE',
      rounds: 8,
      terminal: true,
      selected: ['A:1@9007199254740993'],
      oldAckResends: 0,
      result: 'rejected-by-broker',
      cause: 121,
      fetchCode: 0,
    },
    observations: {
      handlers: [
        [IDS.A, 0, H, '6b', '76', 2],
        [IDS.A, 0, J, '6b', '76', 2],
        [IDS.A, 1, H, '6b', '76', 2],
        [IDS.A, 1, J, '6b', '76', 2],
        [IDS.A, 2, H, '6b', '76', 2],
        [IDS.A, 2, J, '6b', '76', 2],
      ],
      mutations: [
        [IDS.A, 0, H, J, [1]],
        [IDS.A, 1, H, J, [1]],
        [IDS.A, 2, H, J, [1]],
      ],
      settlements: [
        [IDS.A, 0, H, J, 'accept', null, null],
        [IDS.A, 1, H, J, 'accept', 'rejected-by-broker', 121],
        [IDS.A, 2, H, J, 'accept', null, null],
      ],
    },
  },
  {
    fault: 'held acknowledgement connect',
    step: 'ordinary fenced',
    expected: {
      action: 'guard',
      state: 'STABLE',
      rounds: 8,
      enforce: true,
      writes: 0,
      accepted: false,
      finals: 0,
      lateEffects: 0,
      replacement: ['B:0@9007199254740993'],
    },
    observations: {
      handlers: [
        [IDS.A, 0, H, '6b', '76', 2],
        [IDS.A, 1, H, '6b', '76', 2],
        [IDS.A, 1, J, '6b', '76', 2],
        [IDS.B, 0, H, '6b', '76', 2],
        [IDS.B, 0, J, '6b', '76', 2],
      ],
      mutations: [
        [IDS.A, 0, J, J, [2]],
        [IDS.B, 0, H, J, [1]],
      ],
      settlements: [
        [IDS.A, 0, H, H, 'release', 'membership-lost', null],
        [IDS.A, 0, J, J, 'release', null, null],
        [IDS.A, 1, H, H, 'accept', 'membership-lost', null],
        [IDS.A, 1, J, J, 'release', 'membership-lost', null],
        [IDS.A, 2, H, J, 'release', 'membership-lost', null],
        [IDS.B, 0, H, J, 'accept', null, null],
      ],
    },
  },
  {
    fault: 'held acknowledgement connect',
    step: 'ordinary fenced',
    expected: {
      action: 'guard',
      state: 'STABLE',
      rounds: 8,
      enforce: false,
      writes: 0,
      accepted: false,
      finals: 0,
      lateEffects: 0,
      replacement: ['B:0@9007199254740993'],
    },
    observations: {
      handlers: [
        [IDS.A, 0, H, '6b', '76', 2],
        [IDS.A, 1, H, '6b', '76', 2],
        [IDS.A, 1, J, '6b', '76', 2],
        [IDS.B, 0, H, '6b', '76', 2],
        [IDS.B, 0, J, '6b', '76', 2],
      ],
      mutations: [
        [IDS.A, 0, J, J, [2]],
        [IDS.B, 0, H, J, [1]],
      ],
      settlements: [
        [IDS.A, 0, H, H, 'release', 'membership-lost', null],
        [IDS.A, 0, J, J, 'release', null, null],
        [IDS.A, 1, H, H, 'accept', 'membership-lost', null],
        [IDS.A, 1, J, J, 'release', 'membership-lost', null],
        [IDS.A, 2, H, J, 'release', 'membership-lost', null],
        [IDS.B, 0, H, J, 'accept', null, null],
      ],
    },
  },
  {
    fault: 'held acknowledgement connect',
    step: 'captured current',
    expected: {
      action: 'guard',
      state: 'STABLE',
      rounds: 8,
      enforce: true,
      writes: 1,
      accepted: true,
      finals: 1,
      lateEffects: 0,
      replacement: ['B:0@9007199254740993'],
    },
    observations: {
      handlers: [
        [IDS.A, 0, H, '6b', '76', 2],
        [IDS.A, 1, H, '6b', '76', 2],
        [IDS.B, 0, H, '6b', '76', 2],
        [IDS.B, 0, J, '6b', '76', 2],
      ],
      mutations: [
        [IDS.A, 0, J, J, [2]],
        [IDS.A, 1, H, J, [1, 2]],
        [IDS.A, 2, H, J, [2]],
        [IDS.B, 0, H, J, [1]],
      ],
      settlements: [
        [IDS.A, 0, H, H, 'release', 'membership-lost', null],
        [IDS.A, 0, J, J, 'release', null, null],
        [IDS.A, 1, H, H, 'accept', null, null],
        [IDS.A, 1, J, J, 'release', null, null],
        [IDS.A, 2, H, J, 'release', null, null],
        [IDS.B, 0, H, J, 'accept', null, null],
      ],
    },
  },
  {
    fault: 'held acknowledgement connect',
    step: 'captured current',
    expected: {
      action: 'guard',
      state: 'STABLE',
      rounds: 8,
      enforce: false,
      writes: 1,
      accepted: true,
      finals: 1,
      lateEffects: 0,
      replacement: ['B:0@9007199254740993'],
    },
    observations: {
      handlers: [
        [IDS.A, 0, H, '6b', '76', 2],
        [IDS.A, 1, H, '6b', '76', 2],
        [IDS.B, 0, H, '6b', '76', 2],
        [IDS.B, 0, J, '6b', '76', 2],
      ],
      mutations: [
        [IDS.A, 0, J, J, [2]],
        [IDS.A, 1, H, J, [1, 2]],
        [IDS.A, 2, H, J, [2]],
        [IDS.B, 0, H, J, [1]],
      ],
      settlements: [
        [IDS.A, 0, H, H, 'release', 'membership-lost', null],
        [IDS.A, 0, J, J, 'release', null, null],
        [IDS.A, 1, H, H, 'accept', null, null],
        [IDS.A, 1, J, J, 'release', null, null],
        [IDS.A, 2, H, J, 'release', null, null],
        [IDS.B, 0, H, J, 'accept', null, null],
      ],
    },
  },
  {
    fault: 'held acknowledgement connect',
    step: 'captured revoked',
    expected: {
      action: 'guard',
      state: 'STABLE',
      rounds: 8,
      enforce: true,
      writes: 0,
      accepted: false,
      finals: 0,
      lateEffects: 0,
      replacement: ['B:0@9007199254740993'],
    },
    observations: {
      handlers: [
        [IDS.A, 0, H, '6b', '76', 2],
        [IDS.A, 1, H, '6b', '76', 2],
        [IDS.B, 0, H, '6b', '76', 2],
        [IDS.B, 0, J, '6b', '76', 2],
      ],
      mutations: [
        [IDS.A, 0, J, J, [2]],
        [IDS.B, 0, H, J, [1]],
      ],
      settlements: [
        [IDS.A, 0, H, H, 'release', 'membership-lost', null],
        [IDS.A, 0, J, J, 'release', null, null],
        [IDS.A, 1, H, H, 'accept', 'not-sent', null],
        [IDS.A, 1, J, J, 'release', 'not-sent', null],
        [IDS.A, 2, H, J, 'release', 'not-sent', null],
        [IDS.B, 0, H, J, 'accept', null, null],
      ],
    },
  },
  {
    fault: 'held acknowledgement connect',
    step: 'captured revoked',
    expected: {
      action: 'guard',
      state: 'STABLE',
      rounds: 8,
      enforce: false,
      writes: 0,
      accepted: false,
      finals: 0,
      lateEffects: 0,
      replacement: ['B:0@9007199254740993'],
    },
    observations: {
      handlers: [
        [IDS.A, 0, H, '6b', '76', 2],
        [IDS.A, 1, H, '6b', '76', 2],
        [IDS.B, 0, H, '6b', '76', 2],
        [IDS.B, 0, J, '6b', '76', 2],
      ],
      mutations: [
        [IDS.A, 0, J, J, [2]],
        [IDS.B, 0, H, J, [1]],
      ],
      settlements: [
        [IDS.A, 0, H, H, 'release', 'membership-lost', null],
        [IDS.A, 0, J, J, 'release', null, null],
        [IDS.A, 1, H, H, 'accept', 'not-sent', null],
        [IDS.A, 1, J, J, 'release', 'not-sent', null],
        [IDS.A, 2, H, J, 'release', 'not-sent', null],
        [IDS.B, 0, H, J, 'accept', null, null],
      ],
    },
  },
  {
    fault: 'held acknowledgement connect',
    step: 'captured expired',
    expected: {
      action: 'guard',
      state: 'STABLE',
      rounds: 8,
      enforce: true,
      writes: 0,
      accepted: false,
      finals: 0,
      lateEffects: 0,
      replacement: ['B:0@9007199254740993'],
    },
    observations: {
      handlers: [
        [IDS.A, 0, H, '6b', '76', 2],
        [IDS.A, 1, H, '6b', '76', 2],
        [IDS.B, 0, H, '6b', '76', 2],
        [IDS.B, 0, J, '6b', '76', 2],
      ],
      mutations: [
        [IDS.A, 0, J, J, [2]],
        [IDS.B, 0, H, J, [1]],
      ],
      settlements: [
        [IDS.A, 0, H, H, 'release', 'membership-lost', null],
        [IDS.A, 0, J, J, 'release', null, null],
        [IDS.A, 1, H, H, 'accept', 'not-sent', null],
        [IDS.A, 1, J, J, 'release', 'not-sent', null],
        [IDS.A, 2, H, J, 'release', 'not-sent', null],
        [IDS.B, 0, H, J, 'accept', null, null],
      ],
    },
  },
  {
    fault: 'held acknowledgement connect',
    step: 'captured expired',
    expected: {
      action: 'guard',
      state: 'STABLE',
      rounds: 8,
      enforce: false,
      writes: 0,
      accepted: false,
      finals: 0,
      lateEffects: 0,
      replacement: ['B:0@9007199254740993'],
    },
    observations: {
      handlers: [
        [IDS.A, 0, H, '6b', '76', 2],
        [IDS.A, 1, H, '6b', '76', 2],
        [IDS.B, 0, H, '6b', '76', 2],
        [IDS.B, 0, J, '6b', '76', 2],
      ],
      mutations: [
        [IDS.A, 0, H, J, [2]],
        [IDS.B, 0, H, J, [1]],
      ],
      settlements: [
        [IDS.A, 0, H, J, 'release', null, null],
        [IDS.A, 1, H, H, 'accept', 'not-sent', null],
        [IDS.A, 1, J, J, 'release', 'not-sent', null],
        [IDS.A, 2, H, J, 'release', 'not-sent', null],
        [IDS.B, 0, H, J, 'accept', null, null],
      ],
    },
  },
  {
    fault: 'held acknowledgement encode',
    step: 'ordinary fenced',
    expected: {
      action: 'guard',
      state: 'STABLE',
      rounds: 8,
      enforce: true,
      writes: 0,
      accepted: false,
      finals: 0,
      lateEffects: 0,
      replacement: ['B:0@9007199254740993'],
    },
    observations: {
      handlers: [
        [IDS.A, 0, H, '6b', '76', 2],
        [IDS.A, 1, H, '6b', '76', 2],
        [IDS.A, 1, J, '6b', '76', 2],
        [IDS.B, 0, H, '6b', '76', 2],
        [IDS.B, 0, J, '6b', '76', 2],
      ],
      mutations: [
        [IDS.A, 0, J, J, [2]],
        [IDS.B, 0, H, J, [1]],
      ],
      settlements: [
        [IDS.A, 0, H, H, 'release', 'membership-lost', null],
        [IDS.A, 0, J, J, 'release', null, null],
        [IDS.A, 1, H, H, 'accept', 'membership-lost', null],
        [IDS.A, 1, J, J, 'release', 'membership-lost', null],
        [IDS.A, 2, H, J, 'release', 'membership-lost', null],
        [IDS.B, 0, H, J, 'accept', null, null],
      ],
    },
  },
  {
    fault: 'held acknowledgement encode',
    step: 'ordinary fenced',
    expected: {
      action: 'guard',
      state: 'STABLE',
      rounds: 8,
      enforce: false,
      writes: 0,
      accepted: false,
      finals: 0,
      lateEffects: 0,
      replacement: ['B:0@9007199254740993'],
    },
    observations: {
      handlers: [
        [IDS.A, 0, H, '6b', '76', 2],
        [IDS.A, 1, H, '6b', '76', 2],
        [IDS.A, 1, J, '6b', '76', 2],
        [IDS.B, 0, H, '6b', '76', 2],
        [IDS.B, 0, J, '6b', '76', 2],
      ],
      mutations: [
        [IDS.A, 0, J, J, [2]],
        [IDS.B, 0, H, J, [1]],
      ],
      settlements: [
        [IDS.A, 0, H, H, 'release', 'membership-lost', null],
        [IDS.A, 0, J, J, 'release', null, null],
        [IDS.A, 1, H, H, 'accept', 'membership-lost', null],
        [IDS.A, 1, J, J, 'release', 'membership-lost', null],
        [IDS.A, 2, H, J, 'release', 'membership-lost', null],
        [IDS.B, 0, H, J, 'accept', null, null],
      ],
    },
  },
  {
    fault: 'held acknowledgement encode',
    step: 'captured current',
    expected: {
      action: 'guard',
      state: 'STABLE',
      rounds: 8,
      enforce: true,
      writes: 1,
      accepted: true,
      finals: 1,
      lateEffects: 0,
      replacement: ['B:0@9007199254740993'],
    },
    observations: {
      handlers: [
        [IDS.A, 0, H, '6b', '76', 2],
        [IDS.A, 1, H, '6b', '76', 2],
        [IDS.B, 0, H, '6b', '76', 2],
        [IDS.B, 0, J, '6b', '76', 2],
      ],
      mutations: [
        [IDS.A, 0, J, J, [2]],
        [IDS.A, 1, H, J, [1, 2]],
        [IDS.A, 2, H, J, [2]],
        [IDS.B, 0, H, J, [1]],
      ],
      settlements: [
        [IDS.A, 0, H, H, 'release', 'membership-lost', null],
        [IDS.A, 0, J, J, 'release', null, null],
        [IDS.A, 1, H, H, 'accept', null, null],
        [IDS.A, 1, J, J, 'release', null, null],
        [IDS.A, 2, H, J, 'release', null, null],
        [IDS.B, 0, H, J, 'accept', null, null],
      ],
    },
  },
  {
    fault: 'held acknowledgement encode',
    step: 'captured current',
    expected: {
      action: 'guard',
      state: 'STABLE',
      rounds: 8,
      enforce: false,
      writes: 1,
      accepted: true,
      finals: 1,
      lateEffects: 0,
      replacement: ['B:0@9007199254740993'],
    },
    observations: {
      handlers: [
        [IDS.A, 0, H, '6b', '76', 2],
        [IDS.A, 1, H, '6b', '76', 2],
        [IDS.B, 0, H, '6b', '76', 2],
        [IDS.B, 0, J, '6b', '76', 2],
      ],
      mutations: [
        [IDS.A, 0, J, J, [2]],
        [IDS.A, 1, H, J, [1, 2]],
        [IDS.A, 2, H, J, [2]],
        [IDS.B, 0, H, J, [1]],
      ],
      settlements: [
        [IDS.A, 0, H, H, 'release', 'membership-lost', null],
        [IDS.A, 0, J, J, 'release', null, null],
        [IDS.A, 1, H, H, 'accept', null, null],
        [IDS.A, 1, J, J, 'release', null, null],
        [IDS.A, 2, H, J, 'release', null, null],
        [IDS.B, 0, H, J, 'accept', null, null],
      ],
    },
  },
  {
    fault: 'held acknowledgement encode',
    step: 'captured revoked',
    expected: {
      action: 'guard',
      state: 'STABLE',
      rounds: 8,
      enforce: true,
      writes: 0,
      accepted: false,
      finals: 0,
      lateEffects: 0,
      replacement: ['B:0@9007199254740993'],
    },
    observations: {
      handlers: [
        [IDS.A, 0, H, '6b', '76', 2],
        [IDS.A, 1, H, '6b', '76', 2],
        [IDS.B, 0, H, '6b', '76', 2],
        [IDS.B, 0, J, '6b', '76', 2],
      ],
      mutations: [
        [IDS.A, 0, J, J, [2]],
        [IDS.B, 0, H, J, [1]],
      ],
      settlements: [
        [IDS.A, 0, H, H, 'release', 'membership-lost', null],
        [IDS.A, 0, J, J, 'release', null, null],
        [IDS.A, 1, H, H, 'accept', 'not-sent', null],
        [IDS.A, 1, J, J, 'release', 'not-sent', null],
        [IDS.A, 2, H, J, 'release', 'not-sent', null],
        [IDS.B, 0, H, J, 'accept', null, null],
      ],
    },
  },
  {
    fault: 'held acknowledgement encode',
    step: 'captured revoked',
    expected: {
      action: 'guard',
      state: 'STABLE',
      rounds: 8,
      enforce: false,
      writes: 0,
      accepted: false,
      finals: 0,
      lateEffects: 0,
      replacement: ['B:0@9007199254740993'],
    },
    observations: {
      handlers: [
        [IDS.A, 0, H, '6b', '76', 2],
        [IDS.A, 1, H, '6b', '76', 2],
        [IDS.B, 0, H, '6b', '76', 2],
        [IDS.B, 0, J, '6b', '76', 2],
      ],
      mutations: [
        [IDS.A, 0, J, J, [2]],
        [IDS.B, 0, H, J, [1]],
      ],
      settlements: [
        [IDS.A, 0, H, H, 'release', 'membership-lost', null],
        [IDS.A, 0, J, J, 'release', null, null],
        [IDS.A, 1, H, H, 'accept', 'not-sent', null],
        [IDS.A, 1, J, J, 'release', 'not-sent', null],
        [IDS.A, 2, H, J, 'release', 'not-sent', null],
        [IDS.B, 0, H, J, 'accept', null, null],
      ],
    },
  },
  {
    fault: 'held acknowledgement encode',
    step: 'captured expired',
    expected: {
      action: 'guard',
      state: 'STABLE',
      rounds: 8,
      enforce: true,
      writes: 0,
      accepted: false,
      finals: 0,
      lateEffects: 0,
      replacement: ['B:0@9007199254740993'],
    },
    observations: {
      handlers: [
        [IDS.A, 0, H, '6b', '76', 2],
        [IDS.A, 1, H, '6b', '76', 2],
        [IDS.B, 0, H, '6b', '76', 2],
        [IDS.B, 0, J, '6b', '76', 2],
      ],
      mutations: [
        [IDS.A, 0, J, J, [2]],
        [IDS.B, 0, H, J, [1]],
      ],
      settlements: [
        [IDS.A, 0, H, H, 'release', 'membership-lost', null],
        [IDS.A, 0, J, J, 'release', null, null],
        [IDS.A, 1, H, H, 'accept', 'not-sent', null],
        [IDS.A, 1, J, J, 'release', 'not-sent', null],
        [IDS.A, 2, H, J, 'release', 'not-sent', null],
        [IDS.B, 0, H, J, 'accept', null, null],
      ],
    },
  },
  {
    fault: 'held acknowledgement encode',
    step: 'captured expired',
    expected: {
      action: 'guard',
      state: 'STABLE',
      rounds: 8,
      enforce: false,
      writes: 0,
      accepted: false,
      finals: 0,
      lateEffects: 0,
      replacement: ['B:0@9007199254740993'],
    },
    observations: {
      handlers: [
        [IDS.A, 0, H, '6b', '76', 2],
        [IDS.A, 1, H, '6b', '76', 2],
        [IDS.B, 0, H, '6b', '76', 2],
        [IDS.B, 0, J, '6b', '76', 2],
      ],
      mutations: [
        [IDS.A, 0, H, J, [2]],
        [IDS.B, 0, H, J, [1]],
      ],
      settlements: [
        [IDS.A, 0, H, J, 'release', null, null],
        [IDS.A, 1, H, H, 'accept', 'not-sent', null],
        [IDS.A, 1, J, J, 'release', 'not-sent', null],
        [IDS.A, 2, H, J, 'release', 'not-sent', null],
        [IDS.B, 0, H, J, 'accept', null, null],
      ],
    },
  },
  {
    fault: 'held acknowledgement queue',
    step: 'ordinary fenced',
    expected: {
      action: 'guard',
      state: 'STABLE',
      rounds: 8,
      enforce: true,
      writes: 0,
      accepted: false,
      finals: 0,
      lateEffects: 0,
      replacement: ['B:0@9007199254740993'],
    },
    observations: {
      handlers: [
        [IDS.A, 0, H, '6b', '76', 2],
        [IDS.A, 1, H, '6b', '76', 2],
        [IDS.A, 1, J, '6b', '76', 2],
        [IDS.B, 0, H, '6b', '76', 2],
        [IDS.B, 0, J, '6b', '76', 2],
      ],
      mutations: [
        [IDS.A, 0, J, J, [2]],
        [IDS.B, 0, H, J, [1]],
      ],
      settlements: [
        [IDS.A, 0, H, H, 'release', 'membership-lost', null],
        [IDS.A, 0, J, J, 'release', null, null],
        [IDS.A, 1, H, H, 'accept', 'membership-lost', null],
        [IDS.A, 1, J, J, 'release', 'membership-lost', null],
        [IDS.A, 2, H, J, 'release', 'membership-lost', null],
        [IDS.B, 0, H, J, 'accept', null, null],
      ],
    },
  },
  {
    fault: 'held acknowledgement queue',
    step: 'ordinary fenced',
    expected: {
      action: 'guard',
      state: 'STABLE',
      rounds: 8,
      enforce: false,
      writes: 0,
      accepted: false,
      finals: 0,
      lateEffects: 0,
      replacement: ['B:0@9007199254740993'],
    },
    observations: {
      handlers: [
        [IDS.A, 0, H, '6b', '76', 2],
        [IDS.A, 1, H, '6b', '76', 2],
        [IDS.A, 1, J, '6b', '76', 2],
        [IDS.B, 0, H, '6b', '76', 2],
        [IDS.B, 0, J, '6b', '76', 2],
      ],
      mutations: [
        [IDS.A, 0, J, J, [2]],
        [IDS.B, 0, H, J, [1]],
      ],
      settlements: [
        [IDS.A, 0, H, H, 'release', 'membership-lost', null],
        [IDS.A, 0, J, J, 'release', null, null],
        [IDS.A, 1, H, H, 'accept', 'membership-lost', null],
        [IDS.A, 1, J, J, 'release', 'membership-lost', null],
        [IDS.A, 2, H, J, 'release', 'membership-lost', null],
        [IDS.B, 0, H, J, 'accept', null, null],
      ],
    },
  },
  {
    fault: 'held acknowledgement queue',
    step: 'captured current',
    expected: {
      action: 'guard',
      state: 'STABLE',
      rounds: 8,
      enforce: true,
      writes: 1,
      accepted: true,
      finals: 1,
      lateEffects: 0,
      replacement: ['B:0@9007199254740993'],
    },
    observations: {
      handlers: [
        [IDS.A, 0, H, '6b', '76', 2],
        [IDS.A, 1, H, '6b', '76', 2],
        [IDS.B, 0, H, '6b', '76', 2],
        [IDS.B, 0, J, '6b', '76', 2],
      ],
      mutations: [
        [IDS.A, 0, J, J, [2]],
        [IDS.A, 1, H, J, [1, 2]],
        [IDS.A, 2, H, J, [2]],
        [IDS.B, 0, H, J, [1]],
      ],
      settlements: [
        [IDS.A, 0, H, H, 'release', 'membership-lost', null],
        [IDS.A, 0, J, J, 'release', null, null],
        [IDS.A, 1, H, H, 'accept', null, null],
        [IDS.A, 1, J, J, 'release', null, null],
        [IDS.A, 2, H, J, 'release', null, null],
        [IDS.B, 0, H, J, 'accept', null, null],
      ],
    },
  },
  {
    fault: 'held acknowledgement queue',
    step: 'captured current',
    expected: {
      action: 'guard',
      state: 'STABLE',
      rounds: 8,
      enforce: false,
      writes: 1,
      accepted: true,
      finals: 1,
      lateEffects: 0,
      replacement: ['B:0@9007199254740993'],
    },
    observations: {
      handlers: [
        [IDS.A, 0, H, '6b', '76', 2],
        [IDS.A, 1, H, '6b', '76', 2],
        [IDS.B, 0, H, '6b', '76', 2],
        [IDS.B, 0, J, '6b', '76', 2],
      ],
      mutations: [
        [IDS.A, 0, J, J, [2]],
        [IDS.A, 1, H, J, [1, 2]],
        [IDS.A, 2, H, J, [2]],
        [IDS.B, 0, H, J, [1]],
      ],
      settlements: [
        [IDS.A, 0, H, H, 'release', 'membership-lost', null],
        [IDS.A, 0, J, J, 'release', null, null],
        [IDS.A, 1, H, H, 'accept', null, null],
        [IDS.A, 1, J, J, 'release', null, null],
        [IDS.A, 2, H, J, 'release', null, null],
        [IDS.B, 0, H, J, 'accept', null, null],
      ],
    },
  },
  {
    fault: 'held acknowledgement queue',
    step: 'captured revoked',
    expected: {
      action: 'guard',
      state: 'STABLE',
      rounds: 8,
      enforce: true,
      writes: 0,
      accepted: false,
      finals: 0,
      lateEffects: 0,
      replacement: ['B:0@9007199254740993'],
    },
    observations: {
      handlers: [
        [IDS.A, 0, H, '6b', '76', 2],
        [IDS.A, 1, H, '6b', '76', 2],
        [IDS.B, 0, H, '6b', '76', 2],
        [IDS.B, 0, J, '6b', '76', 2],
      ],
      mutations: [
        [IDS.A, 0, J, J, [2]],
        [IDS.B, 0, H, J, [1]],
      ],
      settlements: [
        [IDS.A, 0, H, H, 'release', 'membership-lost', null],
        [IDS.A, 0, J, J, 'release', null, null],
        [IDS.A, 1, H, H, 'accept', 'not-sent', null],
        [IDS.A, 1, J, J, 'release', 'not-sent', null],
        [IDS.A, 2, H, J, 'release', 'not-sent', null],
        [IDS.B, 0, H, J, 'accept', null, null],
      ],
    },
  },
  {
    fault: 'held acknowledgement queue',
    step: 'captured revoked',
    expected: {
      action: 'guard',
      state: 'STABLE',
      rounds: 8,
      enforce: false,
      writes: 0,
      accepted: false,
      finals: 0,
      lateEffects: 0,
      replacement: ['B:0@9007199254740993'],
    },
    observations: {
      handlers: [
        [IDS.A, 0, H, '6b', '76', 2],
        [IDS.A, 1, H, '6b', '76', 2],
        [IDS.B, 0, H, '6b', '76', 2],
        [IDS.B, 0, J, '6b', '76', 2],
      ],
      mutations: [
        [IDS.A, 0, J, J, [2]],
        [IDS.B, 0, H, J, [1]],
      ],
      settlements: [
        [IDS.A, 0, H, H, 'release', 'membership-lost', null],
        [IDS.A, 0, J, J, 'release', null, null],
        [IDS.A, 1, H, H, 'accept', 'not-sent', null],
        [IDS.A, 1, J, J, 'release', 'not-sent', null],
        [IDS.A, 2, H, J, 'release', 'not-sent', null],
        [IDS.B, 0, H, J, 'accept', null, null],
      ],
    },
  },
  {
    fault: 'held acknowledgement queue',
    step: 'captured expired',
    expected: {
      action: 'guard',
      state: 'STABLE',
      rounds: 8,
      enforce: true,
      writes: 0,
      accepted: false,
      finals: 0,
      lateEffects: 0,
      replacement: ['B:0@9007199254740993'],
    },
    observations: {
      handlers: [
        [IDS.A, 0, H, '6b', '76', 2],
        [IDS.A, 1, H, '6b', '76', 2],
        [IDS.B, 0, H, '6b', '76', 2],
        [IDS.B, 0, J, '6b', '76', 2],
      ],
      mutations: [
        [IDS.A, 0, J, J, [2]],
        [IDS.B, 0, H, J, [1]],
      ],
      settlements: [
        [IDS.A, 0, H, H, 'release', 'membership-lost', null],
        [IDS.A, 0, J, J, 'release', null, null],
        [IDS.A, 1, H, H, 'accept', 'not-sent', null],
        [IDS.A, 1, J, J, 'release', 'not-sent', null],
        [IDS.A, 2, H, J, 'release', 'not-sent', null],
        [IDS.B, 0, H, J, 'accept', null, null],
      ],
    },
  },
  {
    fault: 'held acknowledgement queue',
    step: 'captured expired',
    expected: {
      action: 'guard',
      state: 'STABLE',
      rounds: 8,
      enforce: false,
      writes: 0,
      accepted: false,
      finals: 0,
      lateEffects: 0,
      replacement: ['B:0@9007199254740993'],
    },
    observations: {
      handlers: [
        [IDS.A, 0, H, '6b', '76', 2],
        [IDS.A, 1, H, '6b', '76', 2],
        [IDS.B, 0, H, '6b', '76', 2],
        [IDS.B, 0, J, '6b', '76', 2],
      ],
      mutations: [
        [IDS.A, 0, H, J, [2]],
        [IDS.B, 0, H, J, [1]],
      ],
      settlements: [
        [IDS.A, 0, H, J, 'release', null, null],
        [IDS.A, 1, H, H, 'accept', 'not-sent', null],
        [IDS.A, 1, J, J, 'release', 'not-sent', null],
        [IDS.A, 2, H, J, 'release', 'not-sent', null],
        [IDS.B, 0, H, J, 'accept', null, null],
      ],
    },
  },
  {
    fault: 'held acknowledgement response',
    step: 'ordinary fenced',
    expected: {
      action: 'guard',
      state: 'STABLE',
      rounds: 8,
      enforce: true,
      writes: 1,
      accepted: false,
      finals: 0,
      lateEffects: 0,
      replacement: ['B:0@9007199254740993'],
    },
    observations: {
      handlers: [
        [IDS.A, 0, H, '6b', '76', 2],
        [IDS.A, 1, H, '6b', '76', 2],
        [IDS.A, 1, J, '6b', '76', 2],
        [IDS.B, 0, H, '6b', '76', 2],
        [IDS.B, 0, J, '6b', '76', 2],
      ],
      mutations: [
        [IDS.A, 0, J, J, [2]],
        [IDS.A, 1, H, H, [1]],
        [IDS.B, 0, H, J, [1]],
      ],
      settlements: [
        [IDS.A, 0, H, H, 'release', 'membership-lost', null],
        [IDS.A, 0, J, J, 'release', null, null],
        [IDS.A, 1, H, H, 'accept', 'membership-lost', null],
        [IDS.A, 1, J, J, 'release', 'membership-lost', null],
        [IDS.A, 2, H, J, 'release', 'membership-lost', null],
        [IDS.B, 0, H, J, 'accept', null, null],
      ],
    },
  },
  {
    fault: 'held acknowledgement response',
    step: 'ordinary fenced',
    expected: {
      action: 'guard',
      state: 'STABLE',
      rounds: 8,
      enforce: false,
      writes: 1,
      accepted: false,
      finals: 0,
      lateEffects: 0,
      replacement: ['B:0@9007199254740993'],
    },
    observations: {
      handlers: [
        [IDS.A, 0, H, '6b', '76', 2],
        [IDS.A, 1, H, '6b', '76', 2],
        [IDS.A, 1, J, '6b', '76', 2],
        [IDS.B, 0, H, '6b', '76', 2],
        [IDS.B, 0, J, '6b', '76', 2],
      ],
      mutations: [
        [IDS.A, 0, J, J, [2]],
        [IDS.A, 1, H, H, [1]],
        [IDS.B, 0, H, J, [1]],
      ],
      settlements: [
        [IDS.A, 0, H, H, 'release', 'membership-lost', null],
        [IDS.A, 0, J, J, 'release', null, null],
        [IDS.A, 1, H, H, 'accept', 'membership-lost', null],
        [IDS.A, 1, J, J, 'release', 'membership-lost', null],
        [IDS.A, 2, H, J, 'release', 'membership-lost', null],
        [IDS.B, 0, H, J, 'accept', null, null],
      ],
    },
  },
  {
    fault: 'held acknowledgement response',
    step: 'captured current',
    expected: {
      action: 'guard',
      state: 'STABLE',
      rounds: 8,
      enforce: true,
      writes: 1,
      accepted: true,
      finals: 1,
      lateEffects: 0,
      replacement: ['B:0@9007199254740993'],
    },
    observations: {
      handlers: [
        [IDS.A, 0, H, '6b', '76', 2],
        [IDS.A, 1, H, '6b', '76', 2],
        [IDS.B, 0, H, '6b', '76', 2],
        [IDS.B, 0, J, '6b', '76', 2],
      ],
      mutations: [
        [IDS.A, 0, J, J, [2]],
        [IDS.A, 1, H, J, [1, 2]],
        [IDS.A, 2, H, J, [2]],
        [IDS.B, 0, H, J, [1]],
      ],
      settlements: [
        [IDS.A, 0, H, H, 'release', 'membership-lost', null],
        [IDS.A, 0, J, J, 'release', null, null],
        [IDS.A, 1, H, H, 'accept', null, null],
        [IDS.A, 1, J, J, 'release', null, null],
        [IDS.A, 2, H, J, 'release', null, null],
        [IDS.B, 0, H, J, 'accept', null, null],
      ],
    },
  },
  {
    fault: 'held acknowledgement response',
    step: 'captured current',
    expected: {
      action: 'guard',
      state: 'STABLE',
      rounds: 8,
      enforce: false,
      writes: 1,
      accepted: true,
      finals: 1,
      lateEffects: 0,
      replacement: ['B:0@9007199254740993'],
    },
    observations: {
      handlers: [
        [IDS.A, 0, H, '6b', '76', 2],
        [IDS.A, 1, H, '6b', '76', 2],
        [IDS.B, 0, H, '6b', '76', 2],
        [IDS.B, 0, J, '6b', '76', 2],
      ],
      mutations: [
        [IDS.A, 0, J, J, [2]],
        [IDS.A, 1, H, J, [1, 2]],
        [IDS.A, 2, H, J, [2]],
        [IDS.B, 0, H, J, [1]],
      ],
      settlements: [
        [IDS.A, 0, H, H, 'release', 'membership-lost', null],
        [IDS.A, 0, J, J, 'release', null, null],
        [IDS.A, 1, H, H, 'accept', null, null],
        [IDS.A, 1, J, J, 'release', null, null],
        [IDS.A, 2, H, J, 'release', null, null],
        [IDS.B, 0, H, J, 'accept', null, null],
      ],
    },
  },
  {
    fault: 'held acknowledgement response',
    step: 'captured revoked',
    expected: {
      action: 'guard',
      state: 'STABLE',
      rounds: 8,
      enforce: true,
      writes: 1,
      accepted: false,
      finals: 0,
      lateEffects: 0,
      replacement: ['B:0@9007199254740993'],
    },
    observations: {
      handlers: [
        [IDS.A, 0, H, '6b', '76', 2],
        [IDS.A, 1, H, '6b', '76', 2],
        [IDS.B, 0, H, '6b', '76', 2],
        [IDS.B, 0, J, '6b', '76', 2],
      ],
      mutations: [
        [IDS.A, 0, J, J, [2]],
        [IDS.A, 1, H, J, [1, 2]],
        [IDS.A, 2, H, J, [2]],
        [IDS.B, 0, H, J, [1]],
      ],
      settlements: [
        [IDS.A, 0, H, H, 'release', 'membership-lost', null],
        [IDS.A, 0, J, J, 'release', null, null],
        [IDS.A, 1, H, H, 'accept', 'session-lost', null],
        [IDS.A, 1, J, J, 'release', 'session-lost', null],
        [IDS.A, 2, H, J, 'release', 'session-lost', null],
        [IDS.B, 0, H, J, 'accept', null, null],
      ],
    },
  },
  {
    fault: 'held acknowledgement response',
    step: 'captured revoked',
    expected: {
      action: 'guard',
      state: 'STABLE',
      rounds: 8,
      enforce: false,
      writes: 1,
      accepted: false,
      finals: 0,
      lateEffects: 0,
      replacement: ['B:0@9007199254740993'],
    },
    observations: {
      handlers: [
        [IDS.A, 0, H, '6b', '76', 2],
        [IDS.A, 1, H, '6b', '76', 2],
        [IDS.B, 0, H, '6b', '76', 2],
        [IDS.B, 0, J, '6b', '76', 2],
      ],
      mutations: [
        [IDS.A, 0, J, J, [2]],
        [IDS.A, 1, H, J, [1, 2]],
        [IDS.A, 2, H, J, [2]],
        [IDS.B, 0, H, J, [1]],
      ],
      settlements: [
        [IDS.A, 0, H, H, 'release', 'membership-lost', null],
        [IDS.A, 0, J, J, 'release', null, null],
        [IDS.A, 1, H, H, 'accept', 'session-lost', null],
        [IDS.A, 1, J, J, 'release', 'session-lost', null],
        [IDS.A, 2, H, J, 'release', 'session-lost', null],
        [IDS.B, 0, H, J, 'accept', null, null],
      ],
    },
  },
  {
    fault: 'held acknowledgement response',
    step: 'captured expired',
    expected: {
      action: 'guard',
      state: 'STABLE',
      rounds: 8,
      enforce: true,
      writes: 1,
      accepted: false,
      finals: 0,
      lateEffects: 0,
      replacement: ['B:0@9007199254740993'],
    },
    observations: {
      handlers: [
        [IDS.A, 0, H, '6b', '76', 2],
        [IDS.A, 1, H, '6b', '76', 2],
        [IDS.B, 0, H, '6b', '76', 2],
        [IDS.B, 0, J, '6b', '76', 2],
      ],
      mutations: [
        [IDS.A, 0, J, J, [2]],
        [IDS.A, 1, H, J, [1, 2]],
        [IDS.A, 2, H, J, [2]],
        [IDS.B, 0, H, J, [1]],
      ],
      settlements: [
        [IDS.A, 0, H, H, 'release', 'membership-lost', null],
        [IDS.A, 0, J, J, 'release', null, null],
        [IDS.A, 1, H, H, 'accept', 'session-lost', null],
        [IDS.A, 1, J, J, 'release', 'session-lost', null],
        [IDS.A, 2, H, J, 'release', 'session-lost', null],
        [IDS.B, 0, H, J, 'accept', null, null],
      ],
    },
  },
  {
    fault: 'held acknowledgement response',
    step: 'captured expired',
    expected: {
      action: 'guard',
      state: 'STABLE',
      rounds: 8,
      enforce: false,
      writes: 1,
      accepted: false,
      finals: 0,
      lateEffects: 0,
      replacement: ['B:0@9007199254740993'],
    },
    observations: {
      handlers: [
        [IDS.A, 0, H, '6b', '76', 2],
        [IDS.A, 1, H, '6b', '76', 2],
        [IDS.B, 0, H, '6b', '76', 2],
        [IDS.B, 0, J, '6b', '76', 2],
      ],
      mutations: [
        [IDS.A, 0, H, J, [2]],
        [IDS.A, 1, H, J, [1, 2]],
        [IDS.A, 2, H, J, [2]],
        [IDS.B, 0, H, J, [1]],
      ],
      settlements: [
        [IDS.A, 0, H, J, 'release', null, null],
        [IDS.A, 1, H, H, 'accept', 'session-lost', null],
        [IDS.A, 1, J, J, 'release', 'session-lost', null],
        [IDS.A, 2, H, J, 'release', 'session-lost', null],
        [IDS.B, 0, H, J, 'accept', null, null],
      ],
    },
  },
  {
    fault: 'compact million offset error',
    step: 'compact outcomes',
    expected: {
      action: 'compact',
      state: 'STABLE',
      rounds: 8,
      first: '9007199254740993',
      last: '9007199255740992',
      ranges: 1,
      type: 'release',
      result: 'rejected-by-broker',
      cause: 121,
    },
    observations: {
      handlers: [
        [IDS.A, 0, H, '6b', '76', 2],
        [IDS.A, 0, J, '6b', '76', 2],
      ],
      mutations: [
        [IDS.A, 0, H, J, [1]],
        [IDS.A, 1, H, MILLION_LAST, [2]],
      ],
      settlements: [
        [IDS.A, 0, H, J, 'accept', null, null],
        [IDS.A, 1, H, MILLION_LAST, 'release', 'rejected-by-broker', 121],
      ],
    },
  },
  {
    fault: 'compact million offset success',
    step: 'compact outcomes',
    expected: {
      action: 'compact',
      state: 'STABLE',
      rounds: 8,
      first: '9007199254740993',
      last: '9007199255740992',
      ranges: 1,
      type: 'gap',
      result: 'accepted',
    },
    observations: {
      handlers: [
        [IDS.A, 0, H, '6b', '76', 2],
        [IDS.A, 0, J, '6b', '76', 2],
      ],
      mutations: [
        [IDS.A, 0, H, J, [1]],
        [IDS.A, 1, H, MILLION_LAST, [0]],
      ],
      settlements: [
        [IDS.A, 0, H, J, 'accept', null, null],
        [IDS.A, 1, H, MILLION_LAST, 'gap', null, null],
      ],
    },
  },
  {
    fault: 'two flushes settle disjoint terminal subsets',
    step: 'compact outcomes',
    expected: {
      action: 'compact',
      state: 'STABLE',
      rounds: 8,
      first: ['9007199254740993', '9007199254740994'],
      results: ['accept', 'reject'],
      terminalCount: 2,
    },
    observations: {
      handlers: [
        [IDS.A, 0, H, '6b', '76', 2],
        [IDS.A, 0, J, '6b', '76', 2],
        [IDS.A, 1, H, '6b', '76', 2],
        [IDS.A, 1, J, '6b', '76', 2],
        [IDS.A, 2, H, '6b', '76', 2],
        [IDS.A, 2, J, '6b', '76', 2],
      ],
      mutations: [
        [IDS.A, 0, H, J, [1]],
        [IDS.A, 2, H, J, [1]],
        [IDS.A, 1, H, H, [1]],
        [IDS.A, 1, J, J, [3]],
      ],
      settlements: [
        [IDS.A, 0, H, J, 'accept', null, null],
        [IDS.A, 2, H, J, 'accept', null, null],
        [IDS.A, 1, H, H, 'accept', null, null],
        [IDS.A, 1, J, J, 'reject', null, null],
      ],
    },
  },
  {
    fault: 'renew twice then terminal once',
    step: 'compact outcomes',
    expected: {
      action: 'compact',
      state: 'STABLE',
      rounds: 8,
      nonterminal: 2,
      terminal: 1,
      deliveryCount: 2,
    },
    observations: {
      handlers: [
        [IDS.A, 0, H, '6b', '76', 2],
        [IDS.A, 0, J, '6b', '76', 2],
        [IDS.A, 1, H, '6b', '76', 2],
        [IDS.A, 1, J, '6b', '76', 2],
        [IDS.A, 2, H, '6b', '76', 2],
        [IDS.A, 2, J, '6b', '76', 2],
      ],
      mutations: [
        [IDS.A, 0, H, J, [1]],
        [IDS.A, 2, H, J, [1]],
        [IDS.A, 1, H, H, [4]],
        [IDS.A, 1, H, H, [4]],
        [IDS.A, 1, H, J, [1]],
      ],
      settlements: [
        [IDS.A, 0, H, J, 'accept', null, null],
        [IDS.A, 2, H, J, 'accept', null, null],
        [IDS.A, 1, H, H, 'renew', null, null],
        [IDS.A, 1, H, H, 'renew', null, null],
        [IDS.A, 1, H, J, 'accept', null, null],
      ],
    },
  },
  {
    fault: 'unawaited renew blocks terminal prepare',
    step: 'compact outcomes',
    expected: {
      action: 'compact',
      state: 'STABLE',
      rounds: 8,
      order: ['renew answer', 'terminal prepare', 'fetch'],
      unhandled: 0,
    },
    observations: {
      handlers: [
        [IDS.A, 0, H, '6b', '76', 2],
        [IDS.A, 0, J, '6b', '76', 2],
        [IDS.A, 1, H, '6b', '76', 2],
        [IDS.A, 1, J, '6b', '76', 2],
        [IDS.A, 2, H, '6b', '76', 2],
        [IDS.A, 2, J, '6b', '76', 2],
      ],
      mutations: [
        [IDS.A, 0, H, J, [1]],
        [IDS.A, 2, H, J, [1]],
        [IDS.A, 1, H, H, [4]],
        [IDS.A, 1, H, J, [1]],
      ],
      settlements: [
        [IDS.A, 0, H, J, 'accept', null, null],
        [IDS.A, 2, H, J, 'accept', null, null],
        [IDS.A, 1, H, H, 'renew', null, null],
        [IDS.A, 1, H, J, 'accept', null, null],
      ],
    },
  },
  {
    fault: 'renew selected deadline only',
    step: 'compact outcomes',
    expected: {
      action: 'compact',
      state: 'STABLE',
      rounds: 8,
      selected: ['9007199254740993'],
      unchanged: ['9007199254740994'],
      deliveryCount: 2,
    },
    observations: {
      handlers: [
        [IDS.A, 0, H, '6b', '76', 2],
        [IDS.A, 0, J, '6b', '76', 2],
        [IDS.A, 1, H, '6b', '76', 2],
        [IDS.A, 1, J, '6b', '76', 2],
        [IDS.A, 2, H, '6b', '76', 2],
        [IDS.A, 2, J, '6b', '76', 2],
      ],
      mutations: [
        [IDS.A, 0, H, J, [1]],
        [IDS.A, 2, H, J, [1]],
        [IDS.A, 1, H, H, [4]],
        [IDS.A, 1, H, H, [4]],
        [IDS.A, 1, H, J, [1]],
      ],
      settlements: [
        [IDS.A, 0, H, J, 'accept', null, null],
        [IDS.A, 2, H, J, 'accept', null, null],
        [IDS.A, 1, H, H, 'renew', null, null],
        [IDS.A, 1, H, H, 'renew', 'rejected-by-broker', 121],
        [IDS.A, 1, H, J, 'accept', null, null],
      ],
    },
  },
  {
    fault: 'same offsets new acquisition rejects old callback',
    step: 'compact outcomes',
    expected: {
      action: 'compact',
      state: 'STABLE',
      rounds: 8,
      oldEffects: 0,
      distinct: true,
      replacement: ['A:1@9007199254740993'],
    },
    observations: {
      handlers: [
        [IDS.A, 0, H, '6b', '76', 2],
        [IDS.A, 0, J, '6b', '76', 2],
        [IDS.A, 1, H, '6b', '76', 2],
        [IDS.A, 1, H, '6b', '76', 2],
        [IDS.A, 1, J, '6b', '76', 2],
      ],
      mutations: [
        [IDS.A, 0, H, J, [1]],
        [IDS.A, 1, H, H, [4]],
        [IDS.A, 1, H, J, [1]],
      ],
      settlements: [
        [IDS.A, 0, H, J, 'accept', null, null],
        [IDS.A, 1, H, H, 'renew', 'session-lost', 122],
        [IDS.A, 1, H, J, 'release', 'session-lost', 122],
        [IDS.A, 1, H, J, 'accept', null, null],
        [IDS.A, 2, H, J, 'release', 'session-lost', 122],
      ],
    },
  },
  {
    fault: 'observer reentry cannot settle twice',
    step: 'compact outcomes',
    expected: {
      action: 'compact',
      state: 'STABLE',
      rounds: 8,
      terminal: 1,
      reentry: 'stop',
      listenerFailures: ['sync', 'async'],
    },
    observations: {
      handlers: [
        [IDS.A, 0, H, '6b', '76', 2],
        [IDS.A, 1, H, '6b', '76', 2],
        [IDS.A, 1, J, '6b', '76', 2],
      ],
      mutations: [
        [IDS.A, 0, H, J, [1, 2]],
        [IDS.A, 1, H, H, [1]],
        [IDS.A, 1, J, J, [4]],
        [IDS.A, 1, J, J, [1]],
      ],
      settlements: [
        [IDS.A, 0, H, H, 'accept', null, null],
        [IDS.A, 0, J, J, 'release', null, null],
        [IDS.A, 1, H, H, 'accept', null, null],
        [IDS.A, 1, J, J, 'renew', null, null],
        [IDS.A, 1, J, J, 'accept', null, null],
      ],
    },
  },
]

const A1_J = `A:1@${J}`
const FAULT_SPECS = {
  'partition 121': { partition: 121 },
  'partition 6': { partition: 6 },
  'partition 100': { partition: 100 },
  'partition 74': { partition: 74 },
  'partition unknown': { partition: -1 },
  'omitted partition': { omit: true, partition: 0 },
  'duplicate partition': { duplicate: true, partition: 0 },
  'top 122': { code: 122 },
  'top 123': { code: 123 },
  'top 133': { code: 133 },
  'top 35': { code: 35 },
  'top 30': { code: 30 },
  disconnect: { transport: 'disconnect' },
  timeout: { transport: 'timeout' },
}
const KINDS = ['flush', 'piggyback', 'close', 'renew']

/**
 * Drives one literal acknowledgement row through the public share consumer over the scripted
 * cluster. The row's step names the request kind: a boundary flush, the acknowledgements riding the
 * next fetch, the captured final of a close, or a handler-driven renewal. Node 0 always holds A0/H;
 * the selected record is A1/H on node 1.
 */
class AcknowledgementFaultCase extends DataFaultCase {
  /**
   * @param {object} row The literal oracle row.
   * @returns {AcknowledgementFaultCase} A case with its kind, fault and family selected.
   */
  constructor(row) {
    super(row)
    this.kind = KINDS.find((kind) => row.step.startsWith(kind))
    this.revoke = row.step.includes('revoked partition')
    const base = FAULT_SPECS[row.fault]
    this.spec = base
    this.family = { guard: 'guard', compact: 'compact' }[row.expected.action] ?? 'kind'
    if (row.fault === 'successful ack failed fetch') this.spec = { partition: 6 }
    else if (row.fault === 'failed ack successful fetch')
      this.spec = { ack: 121, partition: 0, sibling: true }
    else if (
      this.kind === 'piggyback' &&
      base?.partition !== undefined &&
      !base.omit &&
      !base.duplicate
    )
      this.spec = { ack: base.partition, partition: 0 }
    this.transport = this.spec?.transport ?? null
    this.lockMs = this.kind === 'flush' ? 400 : 15000
  }

  /** @returns {Promise<void>} Reaches the family's frame. */
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

  /** @returns {void} Lets every held handler return. */
  releaseAll() {
    for (const { gate } of this.heldGates) gate.resolve()
  }

  /**
   * @param {number} partition Partition of topic A.
   * @returns {boolean} Whether both exact records were accepted without error.
   */
  acceptedBoth(partition) {
    const accepted = this.resultsOf(partition).filter((ack) => !ack.error && ack.type !== 'renew')
    return covers(accepted, H) && covers(accepted, J)
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

  /** @returns {Promise<void>} Answers a heartbeat with A2 revoked from the assignment. */
  async revokeA2() {
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

  /** @returns {Promise<void>} Reaches the frame of the row's request kind and holds it. */
  async reach_kind() {
    const timeout = this.transport === 'timeout'
    const startOptions = { requestTimeout: timeout ? 1000 : 30000 }
    if (this.kind === 'flush') await this.reachFlush(startOptions)
    else if (this.kind === 'piggyback') await this.reachPiggyback(startOptions)
    else if (this.kind === 'close') await this.reachClose(startOptions)
    else await this.reachRenew(startOptions)
    await this.arrival(this.gate)
  }

  /**
   * Flush: the A1/H handler returns past half of its lock while A1/J still runs, so the lazy
   * acknowledgement request leaves for H alone.
   * @param {object} startOptions Client options.
   * @returns {Promise<void>}
   */
  async reachFlush(startOptions) {
    await this.begin({ hold: [A0_H, A1_H, A1_J], expectCalls: 2, startOptions })
    this.gate = this.holdFrame(
      79,
      ({ node, fields }) => node === 1 && fields.shareSessionEpoch > 0 && !fields.isRenewAck
    )
    if (this.revoke) await this.revokeA2()
    await this.pump(250)
    this.release(A1_H)
  }

  /**
   * Piggyback: the node finishes its handlers and the next fetch carries the terminal decisions.
   * @param {object} startOptions Client options.
   * @returns {Promise<void>}
   */
  async reachPiggyback(startOptions) {
    await this.begin({
      hold: [A0_H, A1_H],
      expectCalls: 2,
      startOptions,
      supply: this.spec.sibling ? ['A:0', 'A:1'] : ['A:0', 'A:1', 'A:2'],
    })
    this.gate = this.holdFrame(
      78,
      ({ node, fields }) =>
        node === 1 && fields.shareSessionEpoch >= 1 && batchesOf({ fields }).length > 0
    )
    if (this.revoke) await this.revokeA2()
    this.release(A1_H)
  }

  /**
   * Close: the A1/H handler decides H and keeps running, so the captured final carries H and the
   * unstarted records of the node.
   * @param {object} startOptions Client options.
   * @returns {Promise<void>}
   */
  async reachClose(startOptions) {
    await this.start({ retry: RETRY_250, ...startOptions })
    this.onStart = (call) => (call.identity === A1_H ? call.acknowledge('accept') : undefined)
    this.supplyOnce('A:0', 'A:1', 'A:2')
    this.hold(A0_H)
    this.hold(A1_H)
    await this.join()
    await until(() => this.calls.length >= 2, 'the held handlers')
    if (this.revoke) await this.revokeA2()
    this.gate = this.holdFrame(
      79,
      ({ node, fields }) => node === 1 && fields.shareSessionEpoch === -1
    )
    this.closing = track(this.h.consumer.disconnect({ timeout: 400 }))
    await this.pump(300)
  }

  /**
   * Renew: the held A1/H handler asks for a renewal; its frame is the one that fails.
   * @param {object} startOptions Client options.
   * @returns {Promise<void>}
   */
  async reachRenew(startOptions) {
    await this.begin({ hold: [A0_H, A1_H], expectCalls: 2, startOptions })
    if (this.revoke) await this.revokeA2()
    this.gate = this.holdFrame(79, ({ node, fields }) => node === 1 && fields.isRenewAck === true)
    this.renewal = track(this.callOf(A1_H).renew())
  }

  /** @returns {Promise<void>} Releases the held frame with the row's fault. */
  async inject_kind() {
    this.startedAt = Date.now()
    this.resetsBefore = this.seen.resets.length
    this.beforeNode1 = this.frameLog(1).length
    await this.fire(this.gate, this.spec)
  }

  /** @returns {Promise<void>} Kind rows: result, cause, reset, held handler and no resends. */
  async verify_kind() {
    const { expected } = this.row
    const fatal = expected.state === 'FATAL'
    if (fatal) {
      await until(() => this.seen.crashes.length === 1, 'the crash event')
      assert.equal(this.seen.crashes[0].restart, false)
      assert(
        expected.cause === 35
          ? this.seen.crashes[0].error.reason === 'refused-by-broker'
          : codesOf(this.seen.crashes[0].error).includes(expected.cause)
      )
    }
    if (this.kind === 'close') await this.advanceUntil(() => this.closing.settled, 'the disconnect')
    if (this.kind === 'renew') {
      await this.advanceUntil(() => this.renewal.settled, 'the renewal outcome')
      assert(this.renewal.error, 'the faulted renewal rejects')
      assert.equal(this.renewal.error.reason, expected.result)
      const retired = expected.reset || fatal
      const terminal = this.terminalFor(1, H)
      assert.equal(terminal.length, Number(retired), 'only retirement settles terminal work')
      if (retired) {
        assert.equal(terminal[0].type, 'release')
        assert.equal(terminal[0].error.reason, expected.reset ? 'session-lost' : 'membership-lost')
      }
      assert.equal(this.renewal.error, this.resultsOf(1).find((ack) => ack.type === 'renew').error)
      if (typeof expected.cause === 'number')
        assert(codesOf(this.renewal.error).includes(expected.cause), 'original renewal cause')
    } else if (this.row.fault !== 'omitted partition' && this.row.fault !== 'duplicate partition') {
      await until(() => this.terminalFor(1, H).length >= 1, 'the terminal result of A1/H')
      this.assertResult(1, H, expected.result, expected.cause)
    } else {
      await until(() => this.terminalFor(1, H).length >= 1, 'the terminal result of A1/H')
      this.assertResult(1, H, expected.result, undefined)
    }
    if (expected.reset) {
      assert.equal(this.seen.resets.length, this.resetsBefore + 1, 'one session reset')
    } else if (!fatal && this.kind !== 'close' && this.row.expected.fetchCode !== 6) {
      assert.equal(this.seen.resets.length, this.resetsBefore, 'no session reset')
    }
    if (this.kind === 'close') {
      assert.equal(this.finals().filter(({ node }) => node === 1).length, 1, 'one final')
      assert.equal(
        this.frameLog(1).filter(({ fields }) => fields.shareSessionEpoch === 0).length,
        1
      )
    }
    const held = this.callOf(A0_H)
    if (fatal || this.kind === 'close') assert.equal(held.isRunning(), false)
    else assert.equal(held.isRunning(), true)
    if (expected.fetchCode === 6)
      assert.equal(
        this.seen.resets.length,
        this.resetsBefore + 1,
        'the fetch retires its partition'
      )
    if (this.spec.sibling) {
      await this.advanceUntil(() => this.acceptedBoth(2), 'the new sibling acquisition settles')
      assert.deepEqual(
        this.calls.filter(({ partition }) => partition === 2).map(({ offset }) => offset),
        [H, J]
      )
    }
    this.assertNoResends()
    if (this.revoke) await this.verifyRevoked()
  }

  /** @returns {void} No acknowledgement of the lost or consumed ticket is written again. */
  assertNoResends() {
    const written = this.frames(78, 79)
      .flatMap((frame) =>
        (frame.fields.topics || []).flatMap((topic) =>
          topic.partitions
            .filter(({ partitionIndex }) => topic.topicId === IDS.A && partitionIndex === 1)
            .flatMap(({ acknowledgementBatches }) => acknowledgementBatches)
        )
      )
      .filter((batch) => batch.firstOffset === H)
    assert(written.length <= 1, 'the acknowledgement of A1/H is written at most once')
  }

  /** @returns {Promise<void>} The revoked A2 records: never started, result per the row. */
  async verifyRevoked() {
    const { expected } = this.row
    assert.equal(
      this.calls.some((call) => call.alias === 'A' && call.partition === 2),
      false,
      'revoked records start no handler'
    )
    this.releaseAll()
    await this.advanceUntil(
      () => [H, J].every((offset) => covers(this.resultsOf(2), offset)),
      'both revoked records settle'
    )
    const outcomes = this.resultsOf(2).filter(({ type }) => type !== 'renew')
    for (const offset of [H, J]) {
      const selected = outcomes.filter((outcome) => covers([outcome], offset))
      assert.equal(selected.length, 1, `exactly one revoked result for ${offset}`)
      assert.equal(selected[0].type, expected.revokedType)
      if (expected.revokedResult === 'accepted') assert.equal(selected[0].error, null)
      else {
        assert(selected[0].error, 'a revoked failure must include its error')
        assert.equal(selected[0].error.reason, expected.revokedResult)
        if (typeof expected.cause === 'number')
          assert(codesOf(selected[0].error).includes(expected.cause), 'original revoked cause')
      }
    }
    const mutations = this.frames(78, 79).flatMap(({ fields }) =>
      (fields.topics || []).flatMap(({ topicId, partitions }) =>
        partitions
          .filter(({ partitionIndex }) => topicId === IDS.A && partitionIndex === 2)
          .flatMap(({ acknowledgementBatches }) => acknowledgementBatches)
      )
    )
    assert.deepEqual(
      mutations,
      expected.revokedWire === 'none'
        ? []
        : [{ firstOffset: H, lastOffset: J, acknowledgeTypes: [2] }],
      'the exact permitted revoked transmission set'
    )
  }

  /** @returns {Promise<void>} Kind rows: current work or a new run still works. */
  async replacement_kind() {
    const { expected } = this.row
    const fatal = expected.state === 'FATAL'
    if (fatal || this.kind === 'close') {
      this.supplyOnce('A:0')
      const calls = this.calls.length
      this.releaseAll()
      await this.run()
      await until(() => this.calls.length > calls, 'the control delivery')
      return
    }
    this.release(A0_H)
    this.releaseAll()
    await this.advanceUntil(() => this.acceptedBoth(0), 'the current terminal acceptance')
  }

  /** @returns {Promise<void>} Guard rows: an ack frame or captured final held at a write stage. */
  async reach_guard() {
    this.enforce = this.row.expected.enforce
    this.boundary = this.row.fault.replace('held acknowledgement ', '')
    this.captured = this.row.step.startsWith('captured')
    const startOptions = { requestTimeout: 1000, enforce: this.enforce }
    if (this.captured) {
      await this.start({ retry: RETRY_250, ...startOptions })
      this.onStart = (call) => {
        if (call.identity === A1_H) call.acknowledge('accept')
        if (call.identity === A0_H && this.row.step === 'captured expired' && !this.enforce)
          call.acknowledge('release')
      }
      this.supplyOnce('A:0', 'A:1', 'A:2')
      this.hold(A0_H)
      this.hold(A1_H)
      await this.join()
      await until(() => this.calls.length >= 2, 'the held handlers')
      this.framesBefore = this.countFrames()
      this.pending = this.boundaryHold(this.boundary, 79, -1)
      this.capOnly = this.row.step === 'captured expired' && !this.enforce
      this.closing = track(
        this.capOnly ? this.h.consumer.stop() : this.h.consumer.disconnect({ timeout: 400 })
      )
      await this.pump(this.capOnly ? 250 : 300)
      if (this.capOnly) {
        this.release(A0_H)
        this.release(A1_H)
      }
    } else {
      this.lockMs = 400
      await this.begin({ hold: [A0_H, A1_H, A1_J], expectCalls: 2, startOptions })
      this.framesBefore = this.countFrames()
      this.pending = this.boundaryHold(this.boundary, 79, 1)
      await this.pump(250)
      this.release(A1_H)
    }
    await until(() => this.pending.reached(), `the ${this.boundary} boundary`)
    this.steadyAtHold = this.countFrames()
  }

  /** @returns {number} ShareAcknowledge frames written to node 1. */
  countFrames() {
    return this.frames(79).filter(({ node }) => node === 1).length
  }

  /**
   * @param {'connect'|'encode'|'queue'|'response'} boundary The write stage to hold on node 1.
   * @param {number} apiKey The held API.
   * @param {number} epoch -1 for a final, 1 for an ordinary acknowledgement.
   * @returns {{reached: () => boolean, release: () => void}} The hold.
   */
  boundaryHold(boundary, apiKey, epoch) {
    let reached = false
    if (boundary === 'response') {
      const wire = this.holdFrame(
        79,
        ({ node, fields }) =>
          node === 1 &&
          (epoch === -1 ? fields.shareSessionEpoch === -1 : fields.shareSessionEpoch > 0)
      )
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
    const isNode1 = (pool) => pool?.host === 'node1'
    if (boundary === 'connect') {
      const original = ConnectionPool.prototype.getConnectionByRequest
      jest.spyOn(ConnectionPool.prototype, 'getConnectionByRequest').mockImplementation(function (
        protocolRequest,
        ...rest
      ) {
        if (protocolRequest?.request?.apiKey !== apiKey || !isNode1(this) || taken)
          return original.call(this, protocolRequest, ...rest)
        taken = true
        const pending = original.call(this, protocolRequest, ...rest)
        return pending.then(async (connection) => {
          reached = true
          await gate.promise
          return connection
        })
      })
    } else if (boundary === 'encode') {
      const original = requests.ShareAcknowledge.protocol
      jest.spyOn(requests.ShareAcknowledge, 'protocol').mockImplementation((selection) => {
        const factory = original(selection)
        return (options) => {
          const built = factory(options)
          const matches =
            epoch === -1 ? options?.shareSessionEpoch === -1 : options?.shareSessionEpoch > 0
          const selected = options.topics?.some((topic) =>
            topic.partitions.some(({ partitionIndex }) => partitionIndex === 1)
          )
          if (!matches || !selected || taken) return built
          const { encode } = built.request
          return {
            ...built,
            request: {
              ...built.request,
              encode: async () => {
                const encoded = await encode.call(built.request)
                take()
                await gate.promise
                return encoded
              },
            },
          }
        }
      })
    } else {
      const owner = this
      const original = RequestQueue.prototype.sendSocketRequest
      jest
        .spyOn(RequestQueue.prototype, 'sendSocketRequest')
        .mockImplementation(function (request) {
          if (request.entry.apiKey !== apiKey || this.broker !== 'node1:9092' || !take())
            return original.call(this, request)
          owner.observe(
            gate.promise.then(() => original.call(this, request)),
            'queue transmission'
          )
          return undefined
        })
    }
    this.heldGates.push({ identity: `boundary:${boundary}`, gate })
    return { reached: () => reached, release: () => gate.resolve() }
  }

  /** @returns {Promise<void>} Guard rows: retire the held request's authority as the row says. */
  async inject_guard() {
    const { step } = this.row
    if (!this.captured) {
      this.rejoin = this.holdWire({
        api: 'ShareGroupHeartbeat',
        match: ({ fields }) => fields.memberEpoch === 0,
      })
      this.fenced = true
      const gate = await this.holdBeatSteady()
      gate.release({ errorCode: 110 })
      await flush()
      return
    }
    if (step === 'captured revoked') {
      this.observe(this.h.consumer.disconnect({ timeout: 0 }), 'disconnect')
      await flush()
    } else if (step === 'captured expired') {
      const episode = [...this.observer.runs].at(-1).closing
      const deadline = Math.min(episode.deadlineAt, episode.finalDeadline)
      assert.equal(deadline - episode.now(), this.capOnly ? 1000 : 100, 'the remaining D/F budget')
      const now = episode.now()
      jest.setSystemTime(Date.now() + deadline - now)
      const originalNow = performance.now.bind(performance)
      jest.spyOn(performance, 'now').mockImplementation(() => originalNow() + deadline - now)
      assert.equal(episode.now(), deadline, 'deadline equality without timer delivery')
      assert.equal(episode.revoked, false, 'the deadline wake has not run')
    }
  }

  /** @returns {Promise<void>} Guard rows: writes, finals, acceptance and no late effect. */
  async verify_guard() {
    const { expected } = this.row
    const historical = this.boundary === 'response' ? 1 : 0
    assert.equal(this.steadyAtHold - this.framesBefore, historical, 'historical frames at the hold')
    assert.equal(this.rawAckWrites(), this.countFrames(), 'original socket writes at the hold')
    const results = this.seen.acks.length
    this.pending.release()
    if (this.captured) await until(() => this.closing.settled, 'the captured close settles')
    else await flush()
    assert.equal(this.countFrames() - this.framesBefore, expected.writes, 'permitted total writes')
    assert.equal(this.rawAckWrites(), this.countFrames(), 'original socket writes after release')
    const finals = this.finals().filter(({ node }) => node === 1).length
    const historicalFinals = this.captured ? historical : 0
    assert.equal(
      finals - historicalFinals,
      this.row.step === 'captured current' ? 1 - historicalFinals : expected.finals,
      'additional final frames'
    )
    const accepted = this.terminalFor(1, H).some((ack) => !ack.error)
    assert.equal(accepted, expected.accepted, 'the held request is accepted only when current')
    if (!this.captured) assert.equal(this.seen.acks.length, results, 'no late effect')
    assert.equal(this.seen.crashes.length, 0)
  }

  /** @returns {number} API 79 writes on original node-1 sockets, including destroyed sockets. */
  rawAckWrites() {
    return this.cluster
      .sockets()
      .filter(({ endpoint }) => endpoint.host === 'node1')
      .flatMap(({ writes }) => writes)
      .filter((frame) => readRequestHeader(frame).apiKey === 79).length
  }

  /** @returns {Promise<void>} Guard rows: the replacement membership delivers B0/H. */
  async replacement_guard() {
    const identity = this.row.expected.replacement[0]
    this.h.table.set(NAME, { topicId: IDS.B, partitions: [{ leader: 0, epoch: 2 }] })
    this.supplyOnce('B:0')
    if (this.captured) {
      this.releaseAll()
      await this.run()
    } else {
      await this.advanceUntil(() => this.rejoin.call !== null, 'the replacement join')
      const gate = this.rejoin
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
    await this.advanceUntil(
      () =>
        [H, J].every((offset) =>
          this.observer.published.some(
            ({ cohort, event }) =>
              cohort.topicId === IDS.B && !event.error && covers([event], offset)
          )
        ),
      'both replacement records are accepted'
    )
    if (!this.captured) {
      const finals = this.finals().filter(({ node }) => node === 1).length
      const before = this.finals().length
      const pending = track(this.h.consumer.stop())
      this.releaseAll()
      await this.advanceUntil(() => pending.settled, 'the current replacement final')
      assert.deepEqual(
        this.finals()
          .slice(before)
          .map(({ node }) => node),
        [0]
      )
      assert.equal(this.finals().filter(({ node }) => node === 1).length, finals)
    }
  }

  /** @returns {Promise<void>} Compact rows: each has its own scenario. */
  async reach_compact() {
    this.compact = COMPACT[this.row.fault]
    assert(this.compact, `no compact driver for ${this.row.fault}`)
    await this.compact.reach.call(this)
  }

  /** @returns {Promise<void>} Compact rows: the scenario's disturbance. */
  async inject_compact() {
    await this.compact.inject?.call(this)
  }

  /** @returns {Promise<void>} Compact rows: the scenario's oracle. */
  async verify_compact() {
    await this.compact.verify.call(this)
  }

  /** @returns {Promise<void>} Compact rows: nothing further. */
  async replacement_compact() {
    this.releaseAll()
    await flush()
  }

  /**
   * Reads a held offset's exact deadline through its original ledger's eligibility query.
   * @param {object} ledger Original acquisition ledger.
   * @param {string} offset Exact held offset.
   * @returns {number} The first ineligible monotonic millisecond, checked on both sides.
   * @throws {AssertionError} If the deadline is outside the declared fixture bounds.
   */
  heldDeadline(ledger, offset) {
    let before = 0
    let after = 100000
    assert.equal(ledger.canStart(offset, before), true)
    assert.equal(ledger.canStart(offset, after), false)
    while (after - before > 1) {
      const middle = Math.floor((before + after) / 2)
      if (ledger.canStart(offset, middle)) before = middle
      else after = middle
    }
    assert.equal(ledger.canStart(offset, after - 1), true)
    assert.equal(ledger.canStart(offset, after), false)
    return after
  }

  /**
   * Compares the row's complete literal multisets after cleanup; duplicate entries remain visible.
   * Cross-node scheduling does not impose a global handler or settlement order.
   * @param {{handlers: Array[], mutations: Array[], settlements: Array[]}} expected Literal tuples.
   * @returns {void}
   * @throws {AssertionError} For any missing, extra or changed handler, mutation or settlement.
   */
  assertObservations(expected) {
    const handlers = this.calls.map(
      ({ topicId, partition, offset, keyBytes, valueBytes, deliveryCount }) => [
        topicId,
        partition,
        offset,
        keyBytes,
        valueBytes,
        deliveryCount,
      ]
    )
    const mutations = this.frames(78, 79).flatMap(({ fields }) =>
      (fields.topics || []).flatMap(({ topicId, partitions }) =>
        partitions.flatMap(({ partitionIndex, acknowledgementBatches }) =>
          acknowledgementBatches.map(({ firstOffset, lastOffset, acknowledgeTypes }) => [
            topicId,
            partitionIndex,
            firstOffset,
            lastOffset,
            acknowledgeTypes,
          ])
        )
      )
    )
    const settlements = this.observer.published.map(({ cohort, event }) => [
      cohort.topicId,
      cohort.partition,
      event.firstOffset,
      event.lastOffset,
      event.type,
      event.error?.reason ?? null,
      event.error?.cause?.code ?? null,
    ])
    const canonical = (entries) => entries.map((entry) => JSON.stringify(entry)).sort()
    for (const [key, actual] of Object.entries({ handlers, mutations, settlements }))
      assert.deepEqual(canonical(actual), canonical(expected[key]), `complete literal ${key}`)
  }

  /** @returns {Promise<void>} Replays frames, handlers and results through the audit. */
  async assertInvariants() {
    this.releaseAll()
    await this.advanceUntil(
      () => [...this.observer.ledgers.keys()].every((ledger) => ledger.size === 0),
      'the complete acquisition outcomes'
    )
    this.replay()
    if (this.auditFailures.length) throw this.auditFailures[0]
    assert.deepEqual(this.unhandled, [])
  }
}

/**
 * @param {boolean} corrupt Whether the acquired batch is corrupt, so the walk halts and releases.
 * @returns {object} A ShareFetch partition answer acquiring a million offsets with no records.
 */
const millionPart = (corrupt) => {
  const records = corrupt ? Buffer.from(recordsHex(H, []), 'hex') : Buffer.alloc(0)
  if (corrupt) records[records.length - 1] ^= 0xff
  return part({
    topicId: IDS.A,
    partition: 1,
    acquire: [acquired(H, MILLION_LAST, 1)],
    records: records.toString('hex'),
  })
}

/** Compact-result scenarios by row fault, each run with the case as `this`. */
const COMPACT = {
  'compact million offset error': {
    /** @returns {Promise<void>} Reaches the scenario through the original public runtime. */
    async reach() {
      await this.start({ retry: RETRY_250 })
      this.cluster.answer('ShareFetch', (fields, record) => {
        if (record.node === 1 && !this.served) {
          this.served = true
          return fetchAnswer([millionPart(true)])
        }
        if (record.node === 1 && batchesOf({ fields }).length)
          return faultAnswer(78, fields, { ack: 121 })
        return this.fetchAnswerFor(fields)
      })
      this.cluster.answer('ShareAcknowledge', (fields) =>
        ackAnswer(
          listed(fields).map(({ topicId, partition }) => ({ topicId, partition, code: 121 }))
        )
      )
      this.supplyOnce('A:0')
      this.hold(A0_H)
      await this.join()
    },
    /** @returns {Promise<void>} Asserts the literal compact outcome and its original identity. */
    async verify() {
      const { expected } = this.row
      await this.advanceUntil(
        () => this.resultsOf(1).some((ack) => ack.error?.ranges),
        'the compact failure'
      )
      const [outcome] = this.resultsOf(1).filter((ack) => ack.error?.ranges)
      assert.equal(outcome.firstOffset, expected.first)
      assert.equal(outcome.lastOffset, expected.last)
      assert.equal(outcome.type, expected.type)
      assert.equal(outcome.error.ranges.length, expected.ranges)
      assert.equal(outcome.error.reason, expected.result)
      assert(codesOf(outcome.error).includes(expected.cause))
    },
  },
  'compact million offset success': {
    /** @returns {Promise<void>} Reaches the scenario through the original public runtime. */
    async reach() {
      await this.start({ retry: RETRY_250 })
      this.cluster.answer('ShareFetch', (fields, record) =>
        record.node === 1 && !this.served
          ? ((this.served = true), fetchAnswer([millionPart(false)]))
          : this.fetchAnswerFor(fields)
      )
      this.supplyOnce('A:0')
      this.hold(A0_H)
      await this.join()
    },
    /** @returns {Promise<void>} Asserts the literal compact outcome and its original identity. */
    async verify() {
      const { expected } = this.row
      await this.advanceUntil(
        () => this.resultsOf(1).some((ack) => ack.type === 'gap'),
        'the compact gap'
      )
      const [outcome] = this.resultsOf(1).filter((ack) => ack.type === 'gap')
      assert.equal(outcome.firstOffset, expected.first)
      assert.equal(outcome.lastOffset, expected.last)
      assert.equal(outcome.error, null)
      assert.equal(this.resultsOf(1).filter((ack) => ack.type === 'gap').length, 1)
    },
  },
  'two flushes settle disjoint terminal subsets': {
    /** @returns {Promise<void>} Reaches the scenario through the original public runtime. */
    async reach() {
      this.lockMs = 400
      this.onStart = (call) => call.acknowledge(call.identity === A1_J ? 'reject' : 'accept')
      await this.begin({ hold: [A0_H, A1_H, A1_J], expectCalls: 2 })
      await this.pump(250)
      this.release(A1_H)
    },
    /** @returns {Promise<void>} Asserts the literal compact outcome and its original identity. */
    async verify() {
      await until(() => this.terminalFor(1, H).length >= 1, 'the first flush result')
      await until(() => this.calls.some((call) => call.identity === A1_J), 'the J handler')
      this.release(A1_J)
      await this.advanceUntil(() => this.terminalFor(1, J).length >= 1, 'the second flush result')
      assert.deepEqual(
        [this.terminalFor(1, H)[0].type, this.terminalFor(1, J)[0].type],
        this.row.expected.results
      )
      assert.equal(
        this.terminalFor(1, H).length + this.terminalFor(1, J).length,
        this.row.expected.terminalCount
      )
    },
  },
  'renew twice then terminal once': {
    /** @returns {Promise<void>} Reaches the scenario through the original public runtime. */
    async reach() {
      await this.begin({ hold: [A0_H, A1_H], expectCalls: 2 })
      this.call = this.callOf(A1_H)
      await this.call.renew()
      await this.call.renew()
    },
    /** @returns {Promise<void>} Asserts the literal compact outcome and its original identity. */
    async verify() {
      const { expected } = this.row
      assert.equal(
        this.resultsOf(1).filter((ack) => ack.type === 'renew').length,
        expected.nonterminal
      )
      this.release(A1_H)
      await this.advanceUntil(() => this.terminalFor(1, H).length >= 1, 'the terminal result')
      assert.equal(this.terminalFor(1, H).length, expected.terminal)
      assert.equal(this.call.deliveryCount, expected.deliveryCount)
    },
  },
  'unawaited renew blocks terminal prepare': {
    /** @returns {Promise<void>} Reaches the scenario through the original public runtime. */
    async reach() {
      await this.begin({ hold: [A0_H, A1_H], expectCalls: 2 })
      this.call = this.callOf(A1_H)
      this.gate = this.holdFrame(79, ({ node, fields }) => node === 1 && fields.isRenewAck === true)
      this.terminalPrepares = []
      this.observer.watch(this.call.cohort.ledger, 'prepare', (ledger, [options], next) => {
        if (!options.renewOffsets) this.terminalPrepares.push(options)
        return next()
      })
      this.terminalBefore = this.frames(78, 79).filter(({ node }) => node === 1).length
      this.renewal = track(this.call.renew())
      await this.arrival(this.gate)
    },
    /** @returns {Promise<void>} Applies the fixed disturbance to the reached original operation. */
    async inject() {
      assert.equal(this.renewal.settled, false)
      assert.deepEqual(this.terminalPrepares, [])
      this.release(A1_H)
      await this.pump(100)
      assert.equal(this.renewal.settled, false)
      assert.deepEqual(this.terminalPrepares, [], 'no preparation during the held renewal')
      assert.equal(
        this.frames(78, 79).filter(({ node }) => node === 1).length,
        this.terminalBefore + 1,
        'only the renewal is written'
      )
    },
    /** @returns {Promise<void>} Asserts the literal compact outcome and its original identity. */
    async verify() {
      assert.equal(this.renewal.settled, false)
      assert.deepEqual(this.terminalPrepares, [], 'terminal preparation waits for the renewal')
      this.gate.release()
      await until(() => this.renewal.settled, 'the renewal answer')
      await this.advanceUntil(() => this.terminalFor(1, H).length >= 1, 'the terminal result')
      assert.equal(this.renewal.error, null)
      assert.deepEqual(this.unhandled, [])
    },
  },
  'renew selected deadline only': {
    /** @returns {Promise<void>} Reaches the scenario through the original public runtime. */
    async reach() {
      await this.begin({ hold: [A0_H, A1_H], expectCalls: 2 })
      this.call = this.callOf(A1_H)
      const opening = this.observer.writes.find(
        ({ apiKey, fields }) =>
          apiKey === 78 && listed(fields).some(({ partition }) => partition === 1)
      )
      const cohort = this.call.cohort
      const loop = [...this.observer.loops.keys()].find((loop) =>
        [...loop.cohorts.values()].some((rows) => rows.includes(cohort))
      )
      assert(opening, 'the original acquisition write')
      this.deadline = this.heldDeadline(cohort.ledger, H)
      assert.equal(this.heldDeadline(cohort.ledger, J), this.deadline)
      this.beforeDecisions = cohort.ledger.size
      await this.pump(100)
      this.renewSentAt = loop.now()
      this.gate = this.holdFrame(79, ({ node, fields }) => node === 1 && fields.isRenewAck === true)
      this.renewal = track(this.call.renew())
      await this.arrival(this.gate)
    },
    /** @returns {Promise<void>} Asserts the literal compact outcome and its original identity. */
    async verify() {
      const { expected } = this.row
      const ledger = this.call.cohort.ledger
      const batches = batchesOf(this.gate.call)
      assert.deepEqual(batches, [
        {
          firstOffset: expected.selected[0],
          lastOffset: expected.selected[0],
          acknowledgeTypes: [4],
        },
      ])
      assert.equal(this.heldDeadline(ledger, H), this.deadline)
      assert.equal(this.heldDeadline(ledger, J), this.deadline)
      this.gate.release(
        ackAnswer([{ topicId: IDS.A, partition: 1 }], { acquisitionLockTimeoutMs: 20000 })
      )
      await until(() => this.renewal.settled, 'the renewal answer')
      assert.equal(this.renewal.error, null)
      const renewed = this.heldDeadline(ledger, H)
      assert.equal(renewed, this.renewSentAt + 20000)
      assert(renewed > this.deadline)
      assert.equal(this.heldDeadline(ledger, J), this.deadline, 'J deadline is unchanged')
      assert.equal(ledger.size, this.beforeDecisions)
      const failed = this.holdFrame(
        79,
        ({ node, fields }) => node === 1 && fields.isRenewAck === true
      )
      const pending = this.call.renew()
      this.expectRejection(pending, {
        name: 'KafkaJSShareAcknowledgementError',
        message: 'Acknowledgement was not confirmed',
        reason: 'rejected-by-broker',
      })
      const rejection = track(pending)
      await this.arrival(failed)
      failed.release(ackAnswer([{ topicId: IDS.A, partition: 1, code: 121 }]))
      await until(() => rejection.settled, 'the failed renewal')
      assert.equal(rejection.error.reason, 'rejected-by-broker')
      assert(codesOf(rejection.error).includes(121))
      assert.equal(this.heldDeadline(ledger, H), renewed, 'failed renewal keeps H deadline')
      assert.equal(this.heldDeadline(ledger, J), this.deadline, 'failed renewal keeps J deadline')
      assert.equal(ledger.size, this.beforeDecisions)
      assert.equal(this.call.deliveryCount, expected.deliveryCount)
    },
  },
  'same offsets new acquisition rejects old callback': {
    /** @returns {Promise<void>} Reaches the scenario through the original public runtime. */
    async reach() {
      this.supplyOnce('A:1')
      await this.begin({ hold: [A0_H, A1_H], expectCalls: 2 })
      this.old = this.callOf(A1_H)
      this.gate = this.holdFrame(79, ({ node, fields }) => node === 1 && fields.isRenewAck === true)
      const pending = this.old.renew()
      this.expectRejection(pending, {
        name: 'KafkaJSShareAcknowledgementError',
        message: 'Acknowledgement was not confirmed',
        reason: 'session-lost',
      })
      this.renewal = track(pending)
      await this.arrival(this.gate)
    },
    /** @returns {Promise<void>} Applies the fixed disturbance to the reached original operation. */
    async inject() {
      this.supplyOnce('A:1')
      this.gate.release(ackAnswer([], { errorCode: 122 }))
      await until(() => this.renewal.settled, 'the lost renewal')
      assert.equal(this.renewal.error.reason, 'session-lost')
      this.release(A1_H)
      this.hold(A1_H)
      await flush()
    },
    /** @returns {Promise<void>} Asserts the literal compact outcome and its original identity. */
    async verify() {
      await this.advanceUntil(
        () => this.calls.filter((call) => call.identity === A1_H).length === 2,
        'the reacquisition'
      )
      const [old, fresh] = this.calls.filter((call) => call.identity === A1_H)
      assert.notEqual(old.token, fresh.token)
      assert.throws(() => old.acknowledge('accept'), { message: STALE_CALL })
      assert.throws(() => old.renew(), { message: STALE_CALL })
      assert.equal(this.row.expected.distinct, true)
      assert.equal(fresh.isRunning(), true)
    },
  },
  'observer reentry cannot settle twice': {
    /** @returns {Promise<void>} Reaches the scenario through the original public runtime. */
    async reach() {
      this.lockMs = 400
      this.stops = []
      this.onStart = (call) => (call.identity === A1_J ? call.renew() : undefined)
      await this.start({ retry: RETRY_250 })
      const { events, on } = this.h.consumer
      this.removers.push(
        on(events.ACKNOWLEDGE, () => {
          throw new Error('sync listener failure')
        }),
        on(events.ACKNOWLEDGE, async () => {
          throw new Error('async listener failure')
        }),
        on(events.ACKNOWLEDGE, () => {
          this.stops.push(this.h.consumer.stop())
        })
      )
      this.supplyOnce('A:0', 'A:1')
      this.hold(A0_H)
      this.hold(A1_H)
      this.hold(A1_J)
      await this.join()
      await until(() => this.calls.length >= 2, 'the held handlers')
      await this.pump(250)
      this.release(A1_H)
    },
    /** @returns {Promise<void>} Asserts the literal compact outcome and its original identity. */
    async verify() {
      await this.advanceUntil(() => this.terminalFor(1, H).length >= 1, 'the terminal result')
      await flush()
      assert.equal(this.terminalFor(1, H).length, this.row.expected.terminal)
      assert.equal(this.stops.length >= 1, true)
      this.tracked.push(...this.stops)
    },
  },
}

describe('share acknowledgement faults', () => {
  afterEach(() => {
    jest.useRealTimers()
    jest.restoreAllMocks()
  })

  it.each(rows)('$fault at $step (row $#)', async (row) => {
    const scenario = new AcknowledgementFaultCase(row)
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
      scenario.assertObservations(row.observations)
    } catch (error) {
      failures.push(error)
    }
    try {
      scenario.cluster?.assertClean()
    } catch (error) {
      failures.push(error)
    }
    if (failures.length) throw new AggregateError(failures, 'Share fault case failed')
  })
})

describe('acknowledgement fault observations', () => {
  test('retains every literal scenario once', () => {
    expect(rows).toHaveLength(130)
    expect(
      new Set(rows.map((row) => `${row.fault} / ${row.step} / ${row.expected.enforce ?? ''}`)).size
    ).toBe(130)
    expect(rows.every((row) => row.expected.rounds >= 8)).toBe(true)
  })

  test('keeps a million-offset result compact and rejects terminal overlap', () => {
    const audit = new FaultAudit()
    const acquisition = Symbol('acquisition')
    const event = {
      topicId: 'A',
      partition: 1,
      firstOffset: '9007199254740993',
      lastOffset: '9007199255740992',
      type: 'release',
    }
    audit.settlement(acquisition, event)
    expect(audit.settled.get(acquisition).get('A:1')).toHaveLength(1)
    expect(() => audit.settlement(acquisition, { ...event, type: 'accept' })).toThrow('overlap')
  })
  test('allows repeated nonterminal renew results before one terminal result', () => {
    const audit = new FaultAudit()
    const acquisition = Symbol('acquisition')
    const event = {
      topicId: 'A',
      partition: 1,
      firstOffset: '9007199254740993',
      lastOffset: '9007199254740993',
      type: 'renew',
    }
    audit.settlement(acquisition, event)
    audit.settlement(acquisition, event)
    audit.settlement(acquisition, { ...event, type: 'accept' })
    expect(audit.settled.get(acquisition).get('A:1')).toHaveLength(1)
  })
  test('does not collapse equal offsets from different acquisitions', () => {
    const audit = new FaultAudit()
    const event = {
      topicId: 'A',
      partition: 1,
      firstOffset: '9007199254740993',
      lastOffset: '9007199254740993',
      type: 'accept',
    }
    audit.settlement(Symbol('first'), event)
    audit.settlement(Symbol('second'), event)
    expect(audit.settled.size).toBe(2)
  })
  test('rejects fetch fields on an acknowledgement request', () => {
    const audit = new FaultAudit()
    expect(() =>
      audit.frame(
        {
          apiKey: 79,
          fields: { shareSessionEpoch: 2, topics: [], maxBytes: 0 },
        },
        { allowed: true }
      )
    ).toThrow()
  })
  test('retains signed int64 boundaries as exact strings', () => {
    const audit = new FaultAudit()
    expect(audit.range('9223372036854775807', '9223372036854775807')).toEqual([
      9223372036854775807n,
      9223372036854775807n,
    ])
    expect(() => audit.range('-1', '0')).toThrow()
    expect(() => audit.range('0', '9223372036854775808')).toThrow()
  })
})
