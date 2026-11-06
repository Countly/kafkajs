const Broker = require('../../../broker')
const ConnectionPool = require('../../../network/connectionPool')
const { newLogger } = require('testHelpers')
const { createScriptedCluster } = require('../../../../testHelpers/scriptedCluster')
const { Schedule } = require('../../../../testHelpers/schedule')
const {
  batch,
  record,
} = require('../../../protocol/requests/fetch/__tests__/fixtures/recordBatchBytes')
const { ConsumerOperation } = require('../../groupProtocol/interface')
const {
  KafkaJSProtocolError,
  KafkaJSShareAcknowledgementError,
  KafkaJSShareGroupsNotSupportedError,
} = require('../../../errors')
const { ShareSession } = require('../shareSession')
const { ShareFetchLoop } = require('../shareFetchLoop')
const { ShareAckLedger } = require('../shareAckLedger')
const { ShareRecordWalk } = require('../shareRecordWalk')

const A = '00112233-4455-6677-8899-aabbccddeeff'
const B = '10112233-4455-6677-8899-aabbccddeeff'

/** @param {object} [options] Planner overrides. @returns {object} A planner with no transport and a fixed clock. */
const planner = (options = {}) => {
  const operation = new ConsumerOperation()
  jest.spyOn(operation, 'sleep').mockResolvedValue()
  const session = new ShareSession({ operation })
  const loop = new ShareFetchLoop({
    session,
    groupId: 'group',
    memberId: 'member',
    minBytes: 1,
    maxBytes: 1048576,
    maxRecords: 500,
    acquireMode: 'record_limit',
    now: () => 10000,
    ...options,
  })
  return { loop, session, operation }
}

/** @param {object} session Session. @param {number} partition Partition. @param {boolean} [renew] Renew ticket. @returns {object} Real compact ledger ticket. */
const prepared = (session, partition, renew = false) => {
  const walk = ShareRecordWalk.walk({
    acquiredRecords: [
      { firstOffset: '9007199254740993', lastOffset: '9007199254740993', deliveryCount: 1 },
    ],
    messages: [{ offset: '9007199254740993' }],
  })
  const ledger = new ShareAckLedger({
    topic: '__proto__',
    topicId: A,
    partition,
    token: session.token,
    sentAt: 100,
    lockDuration: 1000,
    walk,
  })
  if (!renew) ledger.complete(['9007199254740993'], 'accept')
  const { ticket } = ledger.prepare({
    token: session.token,
    ...(renew ? { renewOffsets: ['9007199254740993'] } : {}),
  })
  return { ledger, ticket }
}

describe('share node frame planning', () => {
  afterEach(() => jest.restoreAllMocks())

  test('a held handler prevents prefetch while an unawaited renewal stays ahead of the next fetch', async () => {
    const { loop, session } = planner()
    const schedule = new Schedule()
    const handlerGate = schedule.hold({ api: 'handler' })
    const renewGate = schedule.hold({ api: 'renew' })
    const calls = []
    const fetch = async () => {
      calls.push('fetch')
      return { reply: [], sentAt: 100, answeredAt: 105, empty: false }
    }
    const first = loop.round(fetch, () => schedule.run({ api: 'handler' }, () => null))
    await handlerGate.arrived()
    for (let i = 0; i < 6; i++) await Promise.resolve()
    const second = loop.round(fetch, async () => {})
    expect(second).toBe(first)
    expect(calls).toEqual(['fetch'])
    const renew = session.enqueue(async () => {
      calls.push('renew')
      await schedule.run({ api: 'renew' }, () => null)
    })
    await renewGate.arrived()
    handlerGate.release()
    await first
    const next = loop.round(fetch, async () => {})
    expect(calls).toEqual(['fetch', 'renew'])
    renewGate.release()
    await renew
    await next
    expect(calls).toEqual(['fetch', 'renew', 'fetch'])
  })

  test('the empty-round delay is owned by the original session operation', async () => {
    const { loop, operation } = planner()
    await loop.round(
      async () => ({ reply: [], sentAt: 100, answeredAt: 105, empty: true }),
      async () => {}
    )
    expect(operation.sleep).toHaveBeenCalledTimes(1)
    expect(operation.sleep).toHaveBeenCalledWith(45)
  })

  test('opening fields use 500 milliseconds and positive per-node record limits', () => {
    const { loop } = planner()
    expect(loop.buildFetch([{ topicId: A, partition: 0 }], [], new Set())).toEqual({
      groupId: 'group',
      memberId: 'member',
      shareSessionEpoch: 0,
      maxWaitMs: 500,
      minBytes: 1,
      maxBytes: 1048576,
      maxRecords: 500,
      batchSize: 500,
      shareAcquireMode: 1,
      isRenewAck: false,
      topics: [{ topicId: A, partitions: [{ partitionIndex: 0, acknowledgementBatches: [] }] }],
      forgottenTopicsData: [],
    })
  })

  test('piggyback forgets a revoked acknowledgement-only partition in its very first frame', () => {
    const { loop, session } = planner()
    session.epoch = 3
    session.model.set(`${A}:0`, { topicId: A, partition: 0 })
    const ack = prepared(session, 1)
    const body = loop.buildFetch([{ topicId: B, partition: 2 }], [ack], new Set())
    expect(body.topics).toEqual([
      {
        topicId: A,
        partitions: [
          {
            partitionIndex: 1,
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
      { topicId: B, partitions: [{ partitionIndex: 2, acknowledgementBatches: [] }] },
    ])
    expect(body.forgottenTopicsData).toEqual([{ topicId: A, partitions: [0, 1] }])
    session.apply(
      { ...body, epoch: body.shareSessionEpoch, token: session.token, apiKey: 78 },
      { errorCode: 0 }
    )
    expect([...session.model.keys()]).toEqual([`${B}:2`])
    expect(loop.buildFetch([{ topicId: B, partition: 2 }], [], new Set()).topics).toEqual([])
  })

  test.each(['flush', 'renew'])(
    'first fetch after %s forgets old listed partitions without acknowledgements',
    () => {
      const { loop, session } = planner()
      session.epoch = 2
      session.apply(
        {
          token: session.token,
          epoch: 2,
          apiKey: 79,
          topics: [{ topicId: A, partitions: [{ partitionIndex: 1 }] }],
        },
        { errorCode: 0 }
      )
      const body = loop.buildFetch([{ topicId: B, partition: 2 }], [], new Set())
      expect(body.forgottenTopicsData).toEqual([{ topicId: A, partitions: [1] }])
      expect(body.topics).toEqual([
        { topicId: B, partitions: [{ partitionIndex: 2, acknowledgementBatches: [] }] },
      ])
    }
  )

  test('held unwanted partitions are retained until the last decision is eligible', () => {
    const { loop, session } = planner()
    session.epoch = 2
    session.model.set(`${A}:0`, { topicId: A, partition: 0 })
    expect(loop.buildFetch([], [], new Set([`${A}:0`])).forgottenTopicsData).toEqual([])
    expect(loop.buildFetch([], [], new Set()).forgottenTopicsData).toEqual([
      { topicId: A, partitions: [0] },
    ])
  })

  test('opening never carries acknowledgements and finals skip an unopened node', () => {
    const { loop, session } = planner()
    expect(() => loop.buildFetch([], [prepared(session, 0)], new Set())).toThrow('opening')
    expect(loop.buildAcknowledge([], true)).toBeNull()
    expect(() => loop.buildAcknowledge([])).toThrow('established')
  })

  test('a duplicate acquisition of one partition is refused before any frame exists', () => {
    const { loop, session } = planner()
    session.epoch = 2
    expect(() =>
      loop.buildFetch([], [prepared(session, 0), prepared(session, 0)], new Set())
    ).toThrow('One acquisition per partition per request is required')
  })

  test('flush renewal and final use acknowledgement frames with no fetch or forgotten fields', () => {
    const { loop, session } = planner()
    session.epoch = 1
    const renew = loop.buildAcknowledge([prepared(session, 0, true)])
    expect(renew.isRenewAck).toBe(true)
    expect(renew.topics[0].partitions[0].acknowledgementBatches[0].acknowledgeTypes).toEqual([4])
    const terminal = prepared(session, 1)
    const final = loop.buildAcknowledge([terminal], true)
    expect(final.shareSessionEpoch).toBe(-1)
    expect(final.isRenewAck).toBe(false)
    for (const body of [renew, final]) {
      expect(body).not.toHaveProperty('maxRecords')
      expect(body).not.toHaveProperty('maxWaitMs')
      expect(body).not.toHaveProperty('forgottenTopicsData')
    }
    expect(() => loop.buildAcknowledge([prepared(session, 3, true)], true)).toThrow('renew')
    expect(() => loop.buildFetch([], [prepared(session, 2, true)], new Set())).toThrow(
      'ShareAcknowledge'
    )
  })

  test.each([0, 1, 500])(
    'empty answers are paced with maxWaitTimeInMs %s and minBytes zero',
    (maxWaitTimeInMs) => {
      const { loop } = planner({ maxWaitTimeInMs, minBytes: 0 })
      expect(loop.buildFetch([], [], new Set()).maxWaitMs).toBe(maxWaitTimeInMs)
      expect(loop.buildFetch([], [], new Set()).minBytes).toBe(0)
      expect(loop.emptyDelay(100, 105, true)).toBe(45)
      expect(loop.emptyDelay(100, 150, true)).toBe(0)
      expect(loop.emptyDelay(100, 175, true)).toBe(0)
      expect(loop.emptyDelay(100, 105, false)).toBe(0)
    }
  )

  test.each(['record_limit', 'batch_optimized'])(
    'mode %s keeps limits per node without borrowing another node capacity',
    (acquireMode) => {
      const { loop } = planner({ acquireMode, maxRecords: 7 })
      const body = loop.buildFetch([], [], new Set())
      expect(body.maxRecords).toBe(7)
      expect(body.batchSize).toBe(7)
      expect(body.shareAcquireMode).toBe(acquireMode === 'record_limit' ? 1 : 0)
    }
  )

  test.each([{ maxRecords: 0 }, { maxBytes: 0 }])(
    'zero fetching limits are refused: %j',
    (options) => {
      const { loop } = planner(options)
      expect(() => loop.buildFetch([], [], new Set())).toThrow('positive')
    }
  )
})

/** Offsets of the records one answer acquires, with the wire pair's literal base. */
const acquired = (first, last, deliveryCount = 1) => ({
  firstOffset: String(first),
  lastOffset: String(last),
  deliveryCount,
})

/** @returns {string} Hex of one valid record batch whose records carry the given values. */
const recordsHex = (base, values) =>
  batch({
    baseOffset: BigInt(base),
    records: values.map((value, i) => record({ offsetDelta: i, key: `k${i}`, value })),
  }).toString('hex')

/** @returns {object} A complete ShareFetch partition answer. */
const part = ({
  topicId = A,
  partition,
  code = 0,
  ack = 0,
  leader = { leaderId: -1, leaderEpoch: -1 },
  acquire = [],
  records = '',
}) => ({
  topicId,
  partitionIndex: partition,
  errorCode: code,
  errorMessage: null,
  acknowledgeErrorCode: ack,
  acknowledgeErrorMessage: null,
  currentLeader: leader,
  acquiredRecords: acquire,
  records,
})

/** @returns {object[]} Rows grouped by topic id the way the wire answer lists them. */
const group = (rows) => {
  const topics = new Map()
  for (const { topicId, ...partition } of rows) {
    if (!topics.has(topicId)) topics.set(topicId, { topicId, partitions: [] })
    topics.get(topicId).partitions.push(partition)
  }
  return [...topics.values()]
}

/** @returns {object} A ShareFetch answer; `rows` are complete partition answers. */
const fetchAnswer = (rows, extra = {}) => ({
  errorCode: 0,
  errorMessage: null,
  throttleTime: 0,
  acquisitionLockTimeoutMs: 15000,
  nodeEndpoints: [],
  responses: group(rows),
  ...extra,
})

/** @returns {object} A ShareAcknowledge answer; `rows` are `{topicId, partition, code}`. */
const ackAnswer = (rows, extra = {}) => ({
  errorCode: 0,
  errorMessage: null,
  throttleTime: 0,
  acquisitionLockTimeoutMs: 15000,
  nodeEndpoints: [],
  responses: group(
    rows.map(({ topicId = A, partition, code = 0 }) => ({
      topicId,
      partitionIndex: partition,
      errorCode: code,
      errorMessage: null,
      currentLeader: { leaderId: -1, leaderEpoch: -1 },
    }))
  ),
  ...extra,
})

/** @returns {object} A top-level failure answer with no partitions. */
const topError = (code) => fetchAnswer([], { errorCode: code })

/** @returns {object} The answer a quiet broker gives: every listed partition confirmed, no records. */
const echo = (api, fields) => {
  const listed = fields.topics.flatMap((topic) =>
    topic.partitions.map((partition) => ({
      topicId: topic.topicId,
      partition: partition.partitionIndex,
    }))
  )
  return api === 'ShareFetch'
    ? fetchAnswer(listed.map(({ topicId, partition }) => part({ topicId, partition })))
    : ackAnswer(listed)
}

/** @param {object} cluster Scripted cluster. @param {...number} keys API keys. @returns {object[]} Share frames the client wrote, oldest first. */
const frames = (cluster, ...keys) =>
  cluster
    .requests()
    .filter(({ apiKey }) => (keys.length ? keys.includes(apiKey) : apiKey >= 78 && apiKey <= 79))

/** @returns {Promise<void>} Lets queued socket and promise work run without any timer. */
const flush = async () => {
  for (let turn = 0; turn < 20; turn++) await new Promise(setImmediate)
}

/**
 * A scripted cluster with one Broker, pool and loop per node. Answers come from `script`, falling
 * back to the quiet broker. The clock moves only when a pacing sleep or an answer moves it.
 * @param {object} [options] `nodes` count, `loop` option overrides, `pool` option overrides.
 * @returns {Promise<object>} The harness.
 */
const harness = async ({
  nodes = 1,
  loop = {},
  pool: poolOptions = {},
  names = new Map([
    [A, 'topic-a'],
    [B, 'topic-b'],
  ]),
} = {}) => {
  const schedule = new Schedule()
  const topology = [0, 1, 2].slice(0, nodes).map((id) => ({ id, host: `node${id}`, port: 9092 }))
  const cluster = createScriptedCluster({ nodes: topology, profile: 'share', schedule })
  const clock = { time: 1000 }
  const gates = []
  const pools = []
  const metadata = {
    topicName: jest.fn((id) => names.get(id)),
    topicId: jest.fn((name) => [...names].find(([, candidate]) => candidate === name)?.[0]),
    hasNode: jest.fn(() => true),
    cachedLeader: jest.fn(() => 0),
    applyLeaderHint: jest.fn(async () => true),
    refreshMetadata: jest.fn(async () => {}),
  }
  const newPool = (node) => {
    const created = new ConnectionPool({
      host: node.host,
      port: node.port,
      clientId: 'test-client',
      socketFactory: cluster.socketFactory,
      logger: newLogger(),
      connectionTimeout: 1000,
      requestTimeout: 1000,
      enforceRequestTimeout: false,
      connectionsMaxIdle: 0,
      ...poolOptions,
    })
    pools.push(created)
    return created
  }
  /** One run on a node: its own operation, session and loop and observers, over the node's broker. */
  const runOn = ({ node, pool, broker }) => {
    const run = {
      node,
      pool,
      broker,
      want: [],
      handled: [],
      results: [],
      moves: [],
      fatals: [],
      resets: [],
      sleeps: [],
    }
    run.operation = new ConsumerOperation()
    jest.spyOn(run.operation, 'sleep').mockImplementation(async (delay) => {
      run.sleeps.push(delay)
      clock.time += delay
    })
    run.session = new ShareSession({ operation: run.operation })
    run.dispatch = async (rows) => {
      for (const row of rows) {
        const offsets = []
        row.walk.deliver.records.forEach((message, i) => {
          run.handled.push({
            partition: row.partition,
            topicId: row.topicId,
            offset: message.offset,
            value: message.value.toString(),
            deliveryCount: row.walk.deliver.deliveryCounts[i],
          })
          offsets.push(message.offset)
        })
        if (offsets.length) row.ledger.complete(offsets, 'accept')
      }
    }
    runs.push(run)
    run.loop = new ShareFetchLoop({
      session: run.session,
      groupId: 'group',
      memberId: 'member',
      minBytes: 1,
      maxBytes: 1048576,
      maxRecords: 500,
      acquireMode: 'record_limit',
      broker,
      cluster: metadata,
      logger: newLogger(),
      now: () => clock.time,
      wanted: () => run.want,
      onResults: (results, row) =>
        run.results.push(
          ...results.map((outcome) => ({
            ...outcome,
            partition: row.partition,
            topicId: row.topicId,
          }))
        ),
      onMove: (move) => run.moves.push(move),
      onFatal: (error) => run.fatals.push(error),
      onReset: (open, reason, cause) => run.resets.push({ reason, cause }),
      dispatch: (rows) => run.dispatch(rows),
      ...loop,
    })
    return run
  }
  const members = []
  const runs = []
  for (const node of topology) {
    const pool = newPool(node)
    const broker = new Broker({ connectionPool: pool, logger: newLogger() })
    await broker.connect()
    members.push(runOn({ node, pool, broker }))
  }
  return {
    cluster,
    schedule,
    clock,
    metadata,
    members,
    pools,
    newPool,
    runOn,
    gates,
    hold(selector) {
      const gate = schedule.hold(selector)
      gates.push(gate)
      return gate
    },
    script(api, steps = []) {
      const queue = [...steps]
      cluster.answer(api, (fields, call) => {
        const step = queue.shift()
        const answer = typeof step === 'function' ? step(fields, call) : step
        return answer ?? echo(api, fields)
      })
    },
    scriptNodes(api, byNode) {
      cluster.answer(api, (fields, call) => {
        const step = byNode[call.node]?.shift()
        const answer = typeof step === 'function' ? step(fields, call) : step
        return answer ?? echo(api, fields)
      })
    },
    async close() {
      for (const gate of gates) if (gate.settle) gate.release()
      for (const run of runs) run.loop.dispose()
      await Promise.all(pools.map((created) => created.destroy({ drainTimeout: 0 })))
      cluster.assertClean()
    },
  }
}

/** The answer of an opening fetch that acquires `values` of partition `partition` from `base`. */
const acquire = (partition, base, values, extra = {}) => {
  const first = BigInt(base)
  return part({
    partition,
    acquire: [acquired(first, first + BigInt(values.length - 1))],
    records: recordsHex(first, values),
    ...extra,
  })
}

/** An independent model of the broker's session set: an opening request replaces it, later ones add Topics and drop Forgotten. */
const sessionSet = () => {
  const held = new Set()
  return {
    apply({ shareSessionEpoch, topics, forgottenTopicsData }) {
      if (shareSessionEpoch === 0) held.clear()
      for (const topic of topics)
        for (const partition of topic.partitions)
          held.add(`${topic.topicId}:${partition.partitionIndex}`)
      for (const topic of forgottenTopicsData ?? [])
        for (const partition of topic.partitions) held.delete(`${topic.topicId}:${partition}`)
    },
    has: (topicId, partition) => held.has(`${topicId}:${partition}`),
    keys: () => [...held].sort(),
  }
}

/**
 * Registers an acquisition the node holds without a broker answer: one record, optionally decided.
 * @returns {object} The retained acquisition.
 */
const inject = (h, m, topicId, partition, offset, value, { decide = true } = {}) => {
  const row = m.loop.adopt(
    { open: m.session.token, sentAt: h.clock.time },
    topicId,
    {
      partitionIndex: partition,
      acquiredRecords: [acquired(offset, offset)],
      messages: [{ offset: String(offset), value: Buffer.from(value) }],
    },
    15000
  )
  if (decide) {
    row.ledger.complete([String(offset)], 'accept')
    row.pendingHandlers = false
  }
  return row
}

const fetchFrames = (h, node) =>
  frames(h.cluster, 78).filter(({ node: id }) => node === undefined || id === node)
const okResults = (m) => m.results.filter((outcome) => !outcome.error)
const failures = (m) => m.results.filter((outcome) => outcome.error)
const settled = (promise) =>
  promise.then(
    (value) => ({ value }),
    (error) => ({ error })
  )
const deferred = () => {
  const gate = {}
  gate.promise = new Promise((resolve) => {
    gate.resolve = resolve
  })
  return gate
}
const A0 = { topicId: A, partition: 0 }
const A1 = { topicId: A, partition: 1 }
const batchOf = (first, last, type) => ({
  firstOffset: String(first),
  lastOffset: String(last),
  acknowledgeTypes: [type],
})

describe('share node socket integration', () => {
  let h
  afterEach(async () => {
    if (h) await h.close()
    h = null
    jest.restoreAllMocks()
  })

  test('waits for every node handler before fetching that node again', async () => {
    h = await harness({ nodes: 3 })
    const [n0, n1, n2] = h.members
    n0.want = [A0, A1]
    n1.want = [{ topicId: A, partition: 2 }]
    n2.want = [{ topicId: A, partition: 3 }]
    const values = (partition) => [`p${partition}-0`, `p${partition}-1`]
    h.scriptNodes('ShareFetch', {
      0: [fetchAnswer([acquire(0, 100, values(0)), acquire(1, 200, values(1))])],
      1: [fetchAnswer([acquire(2, 300, values(2))])],
      2: [fetchAnswer([acquire(3, 400, values(3))])],
    })
    const arrived = deferred()
    const held = deferred()
    n0.dispatch = async (rows) => {
      await Promise.all(
        rows.map(async (row) => {
          const { records } = row.walk.deliver
          records.forEach((message) =>
            n0.handled.push({ partition: row.partition, value: message.value.toString() })
          )
          if (row.partition === 0) {
            arrived.resolve()
            await held.promise
          }
          row.ledger.complete(
            records.map((message) => message.offset),
            'accept'
          )
        })
      )
    }
    const first0 = n0.loop.poll()
    const first1 = n1.loop.poll()
    const first2 = n2.loop.poll()
    await arrived.promise
    await Promise.all([first1, first2])
    await Promise.all([n1.loop.poll(), n2.loop.poll()])
    expect(n0.loop.poll()).toBe(first0)
    await flush()
    expect(fetchFrames(h, 0)).toHaveLength(1)
    expect(frames(h.cluster, 79)).toHaveLength(0)
    const acks = (frame) =>
      frame.fields.topics[0].partitions.map(({ partitionIndex, acknowledgementBatches }) => [
        partitionIndex,
        acknowledgementBatches,
      ])
    expect(acks(fetchFrames(h, 1)[1])).toEqual([[2, [batchOf(300, 301, 1)]]])
    expect(acks(fetchFrames(h, 2)[1])).toEqual([[3, [batchOf(400, 401, 1)]]])
    held.resolve()
    await first0
    await n0.loop.poll()
    expect(acks(fetchFrames(h, 0)[1])).toEqual([
      [0, [batchOf(100, 101, 1)]],
      [1, [batchOf(200, 201, 1)]],
    ])
    expect(frames(h.cluster, 79)).toHaveLength(0)
    const handled = [...n0.handled, ...n1.handled, ...n2.handled].map(({ value }) => value)
    expect(handled.sort()).toEqual(['p0-0', 'p0-1', 'p1-0', 'p1-1', 'p2-0', 'p2-1', 'p3-0', 'p3-1'])
  })

  test('forgets a revoked partition in the frame carrying its final acknowledgement', async () => {
    h = await harness()
    const [m] = h.members
    m.want = [A0, A1]
    const model = sessionSet()
    h.script('ShareFetch', [
      (fields) => {
        model.apply(fields)
        return fetchAnswer([acquire(0, 9007199254740993n, ['x', 'y']), acquire(1, 500, ['z'])])
      },
      (fields) => {
        model.apply(fields)
        return fetchAnswer([part({ partition: 0 }), part({ partition: 1 })])
      },
      (fields) => {
        model.apply(fields)
        return fetchAnswer(model.has(A, 0) ? [acquire(0, 900, ['poison'])] : [])
      },
    ])
    await m.loop.poll()
    m.want = [A1]
    await m.loop.poll()
    await m.loop.poll()
    const [, piggyback, later] = fetchFrames(h)
    expect(piggyback.fields.topics).toEqual([
      {
        topicId: A,
        partitions: [
          {
            partitionIndex: 0,
            acknowledgementBatches: [batchOf('9007199254740993', '9007199254740994', 1)],
          },
          { partitionIndex: 1, acknowledgementBatches: [batchOf(500, 500, 1)] },
        ],
      },
    ])
    expect(piggyback.fields.forgottenTopicsData).toEqual([{ topicId: A, partitions: [0] }])
    expect(later.fields.topics).toEqual([])
    expect(later.fields.forgottenTopicsData).toEqual([])
    expect(okResults(m).filter(({ partition }) => partition === 0)).toEqual([
      expect.objectContaining({
        firstOffset: '9007199254740993',
        lastOffset: '9007199254740994',
        type: 'accept',
      }),
    ])
    expect(m.handled.map(({ value }) => value)).toEqual(['x', 'y', 'z'])
    expect(model.keys()).toEqual([`${A}:1`])
  })

  test.each(['topic-a', '__proto__'])(
    'forgets a newly listed acknowledgement-only partition immediately (%s)',
    async (name) => {
      h = await harness({
        names: new Map([
          [A, name],
          [B, 'topic-b'],
        ]),
      })
      const [m] = h.members
      m.want = [A1]
      await m.loop.poll()
      inject(h, m, A, 0, 40, 'a')
      inject(h, m, B, 0, 41, 'b')
      await m.loop.poll()
      const [, piggyback] = fetchFrames(h)
      expect(piggyback.fields.topics).toEqual([
        {
          topicId: A,
          partitions: [{ partitionIndex: 0, acknowledgementBatches: [batchOf(40, 40, 1)] }],
        },
        {
          topicId: B,
          partitions: [{ partitionIndex: 0, acknowledgementBatches: [batchOf(41, 41, 1)] }],
        },
      ])
      expect(piggyback.fields.forgottenTopicsData).toEqual([
        { topicId: A, partitions: [0] },
        { topicId: B, partitions: [0] },
      ])
      expect(okResults(m).map(({ topicId, partition }) => [topicId, partition])).toEqual([
        [A, 0],
        [B, 0],
      ])
      expect([...m.session.model.keys()]).toEqual([`${A}:1`])
    }
  )

  test('forgets on the first fetch after a flush', async () => {
    h = await harness()
    const [m] = h.members
    m.want = [A0, A1]
    const model = sessionSet()
    h.script('ShareFetch', [
      (fields) => {
        model.apply(fields)
        return fetchAnswer([acquire(0, 10, ['a']), part({ partition: 1 })])
      },
      (fields) => {
        model.apply(fields)
        return fetchAnswer(
          model.has(A, 0)
            ? [acquire(0, 20, ['poison']), part({ partition: 1 })]
            : [part({ partition: 1 })]
        )
      },
    ])
    await m.loop.poll()
    m.want = [A1]
    await m.loop.acknowledge()
    expect(fetchFrames(h)).toHaveLength(1)
    const [flushed] = frames(h.cluster, 79)
    expect(flushed.fields).toEqual({
      groupId: 'group',
      memberId: 'member',
      shareSessionEpoch: 1,
      isRenewAck: false,
      topics: [
        {
          topicId: A,
          partitions: [{ partitionIndex: 0, acknowledgementBatches: [batchOf(10, 10, 1)] }],
        },
      ],
    })
    await m.loop.poll()
    const forget = fetchFrames(h)[1].fields
    expect(forget.shareSessionEpoch).toBe(2)
    expect(forget.topics).toEqual([])
    expect(forget.forgottenTopicsData).toEqual([{ topicId: A, partitions: [0] }])
    expect(m.handled.map(({ value }) => value)).toEqual(['a'])
  })

  describe('renewal then terminal decision', () => {
    /** Starts a round whose handler holds record 30 of partition 0 until `proceed` is called. */
    const holdRound = async (m) => {
      const started = deferred()
      const proceed = deferred()
      const captured = {}
      m.dispatch = async (rows) => {
        captured.rows = rows
        started.resolve()
        await proceed.promise
        rows[0].ledger.complete(['30'], 'release')
      }
      const round = m.loop.poll()
      await started.promise
      return { captured, proceed: proceed.resolve, round }
    }

    test('forgets on the first eligible fetch after a renewal and terminal decision', async () => {
      h = await harness()
      const [m] = h.members
      m.want = [A0, A1]
      const model = sessionSet()
      h.script('ShareFetch', [
        (fields) => {
          model.apply(fields)
          return fetchAnswer([acquire(0, 30, ['r']), part({ partition: 1 })])
        },
        (fields) => {
          model.apply(fields)
          return fetchAnswer(
            model.has(A, 0)
              ? [acquire(0, 70, ['poison']), part({ partition: 1 })]
              : [part({ partition: 0 }), part({ partition: 1 })]
          )
        },
      ])
      const gate = h.hold({ api: 'ShareAcknowledge' })
      const { captured, proceed, round } = await holdRound(m)
      const renewal = settled(
        m.loop.acknowledge({ renew: [{ cohort: captured.rows[0], offsets: ['30'] }] })
      )
      await gate.arrived()
      m.want = [A1]
      expect(frames(h.cluster, 79)[0].fields).toMatchObject({
        shareSessionEpoch: 1,
        isRenewAck: true,
        topics: [
          {
            topicId: A,
            partitions: [{ partitionIndex: 0, acknowledgementBatches: [batchOf(30, 30, 4)] }],
          },
        ],
      })
      expect(fetchFrames(h)).toHaveLength(1)
      gate.release(ackAnswer([{ partition: 0 }]))
      expect((await renewal).value).toMatchObject({ lockDuration: 15000 })
      expect(fetchFrames(h)).toHaveLength(1)
      proceed()
      await round
      await m.loop.poll()
      const next = fetchFrames(h)[1].fields
      expect(next.shareSessionEpoch).toBe(2)
      expect(next.topics).toEqual([
        {
          topicId: A,
          partitions: [{ partitionIndex: 0, acknowledgementBatches: [batchOf(30, 30, 2)] }],
        },
      ])
      expect(next.forgottenTopicsData).toEqual([{ topicId: A, partitions: [0] }])
      expect(model.keys()).toEqual([`${A}:1`])
      expect(okResults(m).map(({ type }) => type)).toEqual(['renew', 'release'])
    })

    test('flushing the terminal decision first lets the next fetch forget without acknowledgements', async () => {
      h = await harness()
      const [m] = h.members
      m.want = [A0, A1]
      h.script('ShareFetch', [fetchAnswer([acquire(0, 30, ['r']), part({ partition: 1 })])])
      const gate = h.hold({ api: 'ShareAcknowledge' })
      const { captured, proceed, round } = await holdRound(m)
      const renewal = settled(
        m.loop.acknowledge({ renew: [{ cohort: captured.rows[0], offsets: ['30'] }] })
      )
      await gate.arrived()
      m.want = [A1]
      gate.release(ackAnswer([{ partition: 0 }]))
      await renewal
      proceed()
      await round
      await m.loop.acknowledge()
      const terminal = frames(h.cluster, 79)[1].fields
      expect(terminal).toMatchObject({ shareSessionEpoch: 2, isRenewAck: false })
      expect(terminal.topics[0].partitions[0].acknowledgementBatches).toEqual([batchOf(30, 30, 2)])
      await m.loop.poll()
      const forget = fetchFrames(h)[1].fields
      expect(forget.topics).toEqual([])
      expect(forget.forgottenTopicsData).toEqual([{ topicId: A, partitions: [0] }])
    })
  })

  test('never forgets another held acquisition of the same partition', async () => {
    h = await harness()
    const [m] = h.members
    m.want = [A0, A1]
    h.script('ShareFetch', [fetchAnswer([acquire(0, 10, ['a']), part({ partition: 1 })])])
    await m.loop.poll()
    const held = inject(h, m, A, 0, 20, 'b', { decide: false })
    m.want = [A1]
    await m.loop.poll()
    await m.loop.poll()
    const [, firstFrame, secondFrame] = fetchFrames(h)
    expect(firstFrame.fields.topics).toEqual([
      {
        topicId: A,
        partitions: [{ partitionIndex: 0, acknowledgementBatches: [batchOf(10, 10, 1)] }],
      },
    ])
    expect(firstFrame.fields.forgottenTopicsData).toEqual([])
    expect(secondFrame.fields.topics).toEqual([])
    expect(secondFrame.fields.forgottenTopicsData).toEqual([])
    held.ledger.complete(['20'], 'accept')
    held.pendingHandlers = false
    await m.loop.poll()
    const last = fetchFrames(h)[3].fields
    expect(last.topics).toEqual([
      {
        topicId: A,
        partitions: [{ partitionIndex: 0, acknowledgementBatches: [batchOf(20, 20, 1)] }],
      },
    ])
    expect(last.forgottenTopicsData).toEqual([{ topicId: A, partitions: [0] }])
    const written = frames(h.cluster).length
    const first = inject(h, m, A, 0, 31, 'c')
    const second = inject(h, m, A, 0, 32, 'd')
    const tickets = [first, second].map((row) => ({
      ledger: row.ledger,
      ticket: row.ledger.prepare({ token: row.token }).ticket,
    }))
    expect(() => m.loop.buildFetch([A1], tickets, new Set())).toThrow(
      'One acquisition per partition per request is required'
    )
    expect(frames(h.cluster)).toHaveLength(written)
  })

  test.each([
    ['default wait and record limit', {}, { maxWaitMs: 500, minBytes: 1, shareAcquireMode: 1 }],
    [
      'zero wait and batch optimized',
      { maxWaitTimeInMs: 0, minBytes: 0, acquireMode: 'batch_optimized' },
      { maxWaitMs: 0, minBytes: 0, shareAcquireMode: 0 },
    ],
  ])(
    'uses acknowledgement requests for flush renewal and final (%s)',
    async (label, options, expected) => {
      h = await harness({ loop: options })
      const [m] = h.members
      m.want = [A0]
      h.script('ShareFetch', [
        fetchAnswer([acquire(0, 5, ['a', 'b', 'c'])]),
        fetchAnswer([acquire(0, 8, ['d'])]),
      ])
      await m.loop.poll()
      await m.loop.acknowledge()
      const started = deferred()
      const proceed = deferred()
      const captured = {}
      m.dispatch = async (rows) => {
        captured.rows = rows
        started.resolve()
        await proceed.promise
        rows[0].ledger.complete(['8'], 'accept')
      }
      const round = m.loop.poll()
      await started.promise
      await m.loop.acknowledge({ renew: [{ cohort: captured.rows[0], offsets: ['8'] }] })
      proceed.resolve()
      await round
      await m.loop.finishNode()
      const fetches = fetchFrames(h).map(({ fields }) => fields)
      expect(fetches.map(({ shareSessionEpoch }) => shareSessionEpoch)).toEqual([0, 2])
      for (const fields of fetches) {
        expect(fields).toMatchObject({
          groupId: 'group',
          memberId: 'member',
          maxBytes: 1048576,
          maxRecords: 500,
          batchSize: 500,
          isRenewAck: false,
          ...expected,
        })
      }
      expect(fetches[0].topics).toEqual([
        { topicId: A, partitions: [{ partitionIndex: 0, acknowledgementBatches: [] }] },
      ])
      const acknowledgements = frames(h.cluster, 79).map(({ fields }) => fields)
      expect(acknowledgements.map(({ shareSessionEpoch }) => shareSessionEpoch)).toEqual([1, 3, -1])
      expect(acknowledgements.map(({ isRenewAck }) => isRenewAck)).toEqual([false, true, false])
      for (const fields of acknowledgements) {
        expect(Object.keys(fields).sort()).toEqual([
          'groupId',
          'isRenewAck',
          'memberId',
          'shareSessionEpoch',
          'topics',
        ])
      }
      expect(acknowledgements[0].topics[0].partitions[0].acknowledgementBatches).toEqual([
        batchOf(5, 7, 1),
      ])
      expect(acknowledgements[1].topics[0].partitions[0].acknowledgementBatches).toEqual([
        batchOf(8, 8, 4),
      ])
      expect(acknowledgements[2].topics[0].partitions[0].acknowledgementBatches).toEqual([
        batchOf(8, 8, 1),
      ])
    }
  )

  describe('empty-round pacing', () => {
    test.each([
      ['default wait', { maxWaitTimeInMs: 500, minBytes: 1 }],
      ['zero wait and zero minimum bytes', { maxWaitTimeInMs: 0, minBytes: 0 }],
    ])('paces five-millisecond empty rounds at fifty milliseconds (%s)', async (label, options) => {
      h = await harness({ loop: options })
      const [m] = h.members
      m.want = [A0]
      h.clock.time = 100
      h.script('ShareFetch', [
        () => {
          h.clock.time = 105
          return fetchAnswer([part({ partition: 0 })])
        },
      ])
      const first = await m.loop.poll()
      expect(first).toMatchObject({ sentAt: 100, answeredAt: 105, empty: true })
      expect(m.sleeps).toEqual([45])
      expect(h.clock.time).toBe(150)
      const second = await m.loop.poll()
      expect(second.sentAt).toBe(150)
      expect(fetchFrames(h)[0].fields).toMatchObject({
        maxWaitMs: options.maxWaitTimeInMs,
        minBytes: options.minBytes,
      })
    })

    test('a gap or an error answer is not an empty round', async () => {
      h = await harness()
      const [m] = h.members
      m.want = [A0]
      h.script('ShareFetch', [
        fetchAnswer([part({ partition: 0, acquire: [acquired(10, 10)] })]),
        fetchAnswer([part({ partition: 0, code: 9 })]),
      ])
      const gap = await m.loop.poll()
      expect(gap.empty).toBe(false)
      expect(m.sleeps).toEqual([])
      const error = await m.loop.poll()
      expect(error).toMatchObject({ empty: false, backoff: true })
      expect(m.sleeps).toHaveLength(1)
      expect(m.sleeps[0]).toBeGreaterThanOrEqual(50)
    })

    test('empty writes are at least fifty milliseconds apart so a five second window holds at most one hundred', async () => {
      h = await harness()
      const [m] = h.members
      m.want = [A0]
      const writes = []
      for (let i = 0; i < 101; i++) writes.push((await m.loop.poll()).sentAt)
      const gaps = writes.slice(1).map((at, i) => at - writes[i])
      expect(gaps.every((gap) => gap >= 50)).toBe(true)
      expect(writes.filter((at) => at < writes[0] + 5000).length).toBeLessThanOrEqual(100)
    })

    test('session-limit resets are at least 250 milliseconds apart so a five second window holds at most twenty', async () => {
      h = await harness({
        loop: { retry: { initialRetryTime: 0, factor: 0, multiplier: 1, maxRetryTime: 0 } },
      })
      const [m] = h.members
      m.want = [A0]
      h.script(
        'ShareFetch',
        Array.from({ length: 21 }, () => topError(133))
      )
      const writes = []
      for (let i = 0; i < 21; i++) writes.push((await m.loop.poll()).sentAt)
      const gaps = writes.slice(1).map((at, i) => at - writes[i])
      expect(gaps.every((gap) => gap >= 250)).toBe(true)
      expect(writes.filter((at) => at < writes[0] + 5000).length).toBeLessThanOrEqual(20)
    })
  })

  test('limits record acquisitions independently per node', async () => {
    h = await harness({ nodes: 3, loop: { maxRecords: 7 } })
    const gates = h.members.map(() => deferred())
    const everyone = deferred()
    let held = 0
    h.members.forEach((m, i) => {
      m.want = [{ topicId: A, partition: i }]
      m.dispatch = async (rows) => {
        held += rows.reduce((sum, row) => sum + row.walk.deliver.records.length, 0)
        if (held === 21) everyone.resolve()
        await gates[i].promise
        for (const row of rows)
          row.ledger.complete(
            row.walk.deliver.records.map((message) => message.offset),
            'accept'
          )
      }
    })
    h.scriptNodes(
      'ShareFetch',
      Object.fromEntries(
        h.members.map((m, i) => [
          i,
          [
            fetchAnswer([
              acquire(
                i,
                100 * (i + 1),
                Array.from({ length: 7 }, (_, j) => `n${i}-${j}`)
              ),
            ]),
          ],
        ])
      )
    )
    const rounds = h.members.map((m) => m.loop.poll())
    await everyone.promise
    expect(held).toBe(21)
    for (const frame of fetchFrames(h)) {
      expect(frame.fields).toMatchObject({ maxRecords: 7, batchSize: 7, shareAcquireMode: 1 })
    }
    gates[0].resolve()
    await rounds[0]
    await h.members[0].loop.poll()
    expect(fetchFrames(h, 0)).toHaveLength(2)
    expect(fetchFrames(h, 0)[1].fields).toMatchObject({ maxRecords: 7, batchSize: 7 })
    expect(fetchFrames(h, 1)).toHaveLength(1)
    expect(fetchFrames(h, 2)).toHaveLength(1)
    gates[1].resolve()
    gates[2].resolve()
    await Promise.all(rounds)
  })

  test('does not truncate an aligned batch that exceeds the batch-optimized target', async () => {
    h = await harness({ loop: { acquireMode: 'batch_optimized', maxRecords: 7 } })
    const [m] = h.members
    m.want = [A0]
    const values = Array.from({ length: 8 }, (_, i) => `v${i}`)
    h.script('ShareFetch', [fetchAnswer([acquire(0, 10, values)])])
    await m.loop.poll()
    expect(fetchFrames(h)[0].fields).toMatchObject({
      maxRecords: 7,
      batchSize: 7,
      shareAcquireMode: 0,
    })
    expect(m.handled.map(({ value }) => value)).toEqual(values)
    await m.loop.poll()
    expect(fetchFrames(h)[1].fields.topics[0].partitions[0].acknowledgementBatches).toEqual([
      batchOf(10, 17, 1),
    ])
  })

  describe('leader movement', () => {
    const endpoints = [{ nodeId: 2, host: 'node2', port: 9092, rack: null }]
    const moved = (leader, extra = {}) =>
      fetchAnswer([part({ partition: 0, code: 6, leader }), part({ partition: 1 })], extra)

    test('uses leader hints and retires the old node acquisition', async () => {
      h = await harness({ nodes: 3 })
      const [n0, , n2] = h.members
      n0.want = [A0, A1]
      h.metadata.cachedLeader.mockReturnValue(2)
      h.scriptNodes('ShareFetch', {
        0: [
          fetchAnswer([acquire(0, 10, ['a']), acquire(1, 20, ['b'])]),
          moved({ leaderId: 2, leaderEpoch: 8 }, { nodeEndpoints: endpoints }),
        ],
      })
      await n0.loop.poll()
      inject(h, n0, A, 0, 99, 'held', { decide: false })
      await n0.loop.poll()
      expect(h.metadata.applyLeaderHint).toHaveBeenCalledTimes(1)
      expect(h.metadata.applyLeaderHint).toHaveBeenCalledWith(
        { topic: 'topic-a', partition: 0, leaderId: 2, leaderEpoch: 8, nodeEndpoints: endpoints },
        { fence: n0.operation }
      )
      expect(h.metadata.refreshMetadata).not.toHaveBeenCalled()
      expect(n0.moves).toEqual([
        { topic: 'topic-a', topicId: A, partition: 0, leaderId: 2, reason: 'leader-moved' },
      ])
      expect(failures(n0)).toHaveLength(1)
      expect(failures(n0)[0].error.reason).toBe('session-lost')
      expect(failures(n0)[0]).toMatchObject({ firstOffset: '99', lastOffset: '99' })
      expect(okResults(n0).filter(({ partition }) => partition === 0)).toHaveLength(1)
      n0.want = [A1]
      n2.want = [A0]
      await n2.loop.poll()
      await n0.loop.poll()
      expect(fetchFrames(h, 2)[0].fields).toMatchObject({
        shareSessionEpoch: 0,
        topics: [{ topicId: A, partitions: [{ partitionIndex: 0, acknowledgementBatches: [] }] }],
      })
      expect(fetchFrames(h, 0)[2].fields.forgottenTopicsData).toEqual([
        { topicId: A, partitions: [0] },
      ])
    })

    test.each([
      ['no hint', { leaderId: -1, leaderEpoch: -1 }, 0],
      ['a stale hint', { leaderId: 2, leaderEpoch: 8 }, 1],
    ])('refreshes metadata under the node operation after %s', async (label, leader, hints) => {
      h = await harness({ nodes: 3 })
      const [n0] = h.members
      n0.want = [A0]
      h.metadata.applyLeaderHint.mockResolvedValue(false)
      h.metadata.cachedLeader.mockReturnValue(2)
      h.scriptNodes('ShareFetch', {
        0: [fetchAnswer([part({ partition: 0 })]), moved(leader, { nodeEndpoints: endpoints })],
      })
      await n0.loop.poll()
      await n0.loop.poll()
      expect(h.metadata.applyLeaderHint).toHaveBeenCalledTimes(hints)
      expect(h.metadata.refreshMetadata).toHaveBeenCalledTimes(1)
      expect(h.metadata.refreshMetadata).toHaveBeenCalledWith({
        fence: n0.operation,
        allowLeaderless: true,
      })
      expect(n0.moves).toHaveLength(1)
      expect(n0.moves[0]).toMatchObject({ partition: 0, leaderId: 2 })
    })
  })

  describe('topic identity and retriable partition errors', () => {
    test.each([3, 100, 103, 9, 56, 78, 7])(
      'partition code %s refreshes metadata and keeps the sibling delivery',
      async (code) => {
        h = await harness()
        const [m] = h.members
        m.want = [A0, A1]
        h.script('ShareFetch', [
          fetchAnswer([part({ partition: 0, code }), acquire(1, 10, ['sib'])]),
        ])
        const result = await m.loop.poll()
        expect(result.backoff).toBe(true)
        expect(h.metadata.refreshMetadata).toHaveBeenCalledTimes(1)
        expect(m.handled.map(({ value }) => value)).toEqual(['sib'])
        expect(m.moves).toEqual([])
        expect(m.fatals).toEqual([])
        expect(m.sleeps[0]).toBeGreaterThanOrEqual(50)
      }
    )

    test('a recreated topic retires the old identity before the new one is installed', async () => {
      h = await harness()
      const [m] = h.members
      m.want = [A0]
      h.script('ShareFetch', [
        fetchAnswer([acquire(0, 10, ['a'])]),
        fetchAnswer([part({ partition: 0, code: 100 })]),
      ])
      await m.loop.poll()
      inject(h, m, A, 0, 20, 'old', { decide: false })
      h.metadata.refreshMetadata.mockImplementation(async () => {
        h.metadata.topicId.mockReturnValue(B)
      })
      await m.loop.poll()
      expect(h.metadata.refreshMetadata).toHaveBeenCalledTimes(1)
      expect(failures(m)).toHaveLength(1)
      expect(failures(m)[0]).toMatchObject({ firstOffset: '20', lastOffset: '20' })
      expect(failures(m)[0].error.reason).toBe('session-lost')
      m.want = [{ topicId: B, partition: 0 }]
      await m.loop.poll()
      const next = fetchFrames(h)[2].fields
      expect(next.topics).toEqual([
        { topicId: B, partitions: [{ partitionIndex: 0, acknowledgementBatches: [] }] },
      ])
      expect(next.forgottenTopicsData).toEqual([{ topicId: A, partitions: [0] }])
    })

    test('a metadata refresh finished after retirement routes nothing', async () => {
      h = await harness()
      const [m] = h.members
      m.want = [A0]
      h.script('ShareFetch', [fetchAnswer([part({ partition: 0, code: 9 })])])
      const entered = deferred()
      const release = deferred()
      h.metadata.refreshMetadata.mockImplementationOnce(async () => {
        entered.resolve()
        await release.promise
      })
      const round = settled(m.loop.poll())
      await entered.promise
      m.operation.retire(new Error('run closed'))
      release.resolve()
      expect((await round).error.message).toBe('run closed')
      await flush()
      expect(fetchFrames(h)).toHaveLength(1)
      expect(m.moves).toEqual([])
    })
  })

  describe('fatal and unknown failures', () => {
    test.each([29, 2, 42])(
      'partition code %s is fatal after its sibling acquisition was registered and released',
      async (code) => {
        h = await harness()
        const [m] = h.members
        m.want = [A0, A1]
        h.script('ShareFetch', [
          fetchAnswer([part({ partition: 0, code }), acquire(1, 10, ['sib'])]),
        ])
        const failure = (await settled(m.loop.poll())).error
        expect(failure).toBeInstanceOf(KafkaJSProtocolError)
        expect(failure).toMatchObject({ code, retriable: false })
        expect(m.fatals).toEqual([failure])
        await flush()
        expect(fetchFrames(h)).toHaveLength(1)
        expect(m.handled).toEqual([])
        await m.loop.finishNode()
        const final = frames(h.cluster, 79)
        expect(final).toHaveLength(1)
        expect(final[0].fields).toMatchObject({ shareSessionEpoch: -1 })
        expect(final[0].fields.topics).toEqual([
          {
            topicId: A,
            partitions: [{ partitionIndex: 1, acknowledgementBatches: [batchOf(10, 10, 2)] }],
          },
        ])
        expect(okResults(m)).toEqual([expect.objectContaining({ type: 'release', partition: 1 })])
        expect(failures(m)).toEqual([])
      }
    )

    test('a top-level deleted-topic -1 advances the epoch under backoff and never resets', async () => {
      h = await harness()
      const [m] = h.members
      m.want = [A0]
      h.script('ShareFetch', [topError(-1)])
      const result = await m.loop.poll()
      expect(result).toMatchObject({ backoff: true, reset: false })
      expect(m.resets).toEqual([])
      expect(m.session.epoch).toBe(1)
      expect(m.sleeps[0]).toBeGreaterThanOrEqual(50)
      await m.loop.poll()
      expect(fetchFrames(h)[1].fields.shareSessionEpoch).toBe(1)
    })

    test('an unknown partition error -1 is logged and skipped while siblings deliver', async () => {
      h = await harness()
      const [m] = h.members
      m.want = [A0, A1]
      h.script('ShareFetch', [
        fetchAnswer([part({ partition: 0, code: -1 }), acquire(1, 10, ['sib'])]),
      ])
      const result = await m.loop.poll()
      expect(result.backoff).toBe(true)
      expect(m.handled.map(({ value }) => value)).toEqual(['sib'])
      expect(m.fatals).toEqual([])
    })

    test('runtime 35 names ShareFetch and ends the run without a retry frame', async () => {
      h = await harness()
      const [m] = h.members
      m.want = [A0]
      h.script('ShareFetch', [topError(35)])
      const failure = (await settled(m.loop.poll())).error
      expect(failure).toBeInstanceOf(KafkaJSShareGroupsNotSupportedError)
      expect(failure).toMatchObject({
        apiKey: 78,
        apiName: 'ShareFetch',
        reason: 'refused-by-broker',
      })
      expect(m.fatals).toEqual([failure])
      await flush()
      expect(fetchFrames(h)).toHaveLength(1)
    })

    test('runtime 30 is a fatal authorization failure', async () => {
      h = await harness()
      const [m] = h.members
      m.want = [A0]
      h.script('ShareFetch', [topError(30)])
      const failure = (await settled(m.loop.poll())).error
      expect(failure).toMatchObject({ code: 30, retriable: false })
      expect(m.fatals).toEqual([failure])
    })

    test('runtime 25 goes to the membership with the issued token', async () => {
      const token = Symbol('membership')
      const membership = { onMembershipError: jest.fn() }
      h = await harness({ loop: { membership, authority: { membershipToken: token } } })
      const [m] = h.members
      m.want = [A0]
      h.script('ShareFetch', [topError(25)])
      await m.loop.poll()
      expect(membership.onMembershipError).toHaveBeenCalledTimes(1)
      const [error, issued] = membership.onMembershipError.mock.calls[0]
      expect(error).toMatchObject({ code: 25 })
      expect(issued).toBe(token)
    })
  })

  describe('acknowledgement fields', () => {
    test.each([
      ['ack 121', 121, 'rejected-by-broker'],
      ['ack 6', 6, 'session-lost'],
      ['ack 74', 74, 'session-lost'],
      ['ack 100', 100, 'rejected-by-broker'],
      ['ack 133', 133, 'rejected-by-broker'],
      ['an unlisted ack code', 7, 'rejected-by-broker'],
    ])('fetch success with %s settles %s without a session reset', async (label, ack, reason) => {
      h = await harness()
      const [m] = h.members
      m.want = [A0]
      h.script('ShareFetch', [
        fetchAnswer([acquire(0, 10, ['a'])]),
        fetchAnswer([part({ partition: 0, ack })]),
      ])
      await m.loop.poll()
      await m.loop.poll()
      expect(failures(m)).toHaveLength(1)
      expect(failures(m)[0].error.reason).toBe(reason)
      expect(failures(m)[0].error.cause).toMatchObject({ code: ack })
      expect(okResults(m)).toEqual([])
      expect(m.resets).toEqual([])
      expect(m.session.epoch).toBe(2)
    })

    test('a rejected acknowledgement does not stop the same answer from delivering a new acquisition', async () => {
      h = await harness()
      const [m] = h.members
      m.want = [A0]
      h.script('ShareFetch', [
        fetchAnswer([acquire(0, 10, ['a'])]),
        fetchAnswer([acquire(0, 20, ['new'], { ack: 121 })]),
      ])
      await m.loop.poll()
      await m.loop.poll()
      expect(failures(m)[0].error.cause).toMatchObject({ code: 121 })
      expect(m.handled.map(({ value }) => value)).toEqual(['a', 'new'])
    })

    test('fetch error 6 with a confirmed acknowledgement confirms once and moves the leader', async () => {
      h = await harness()
      const [m] = h.members
      m.want = [A0, A1]
      h.metadata.cachedLeader.mockReturnValue(2)
      h.script('ShareFetch', [
        fetchAnswer([acquire(0, 10, ['a']), part({ partition: 1 })]),
        fetchAnswer([
          part({ partition: 0, code: 6, ack: 0, leader: { leaderId: 2, leaderEpoch: 8 } }),
          part({ partition: 1 }),
        ]),
      ])
      await m.loop.poll()
      inject(h, m, A, 0, 99, 'held', { decide: false })
      await m.loop.poll()
      expect(okResults(m).filter(({ partition }) => partition === 0)).toHaveLength(1)
      expect(failures(m)).toHaveLength(1)
      expect(failures(m)[0].error.reason).toBe('session-lost')
      expect(m.moves).toHaveLength(1)
    })

    test('a forgotten partition stays in the settlement set of its original request', async () => {
      h = await harness()
      const [m] = h.members
      m.want = [A0, A1]
      h.script('ShareFetch', [
        fetchAnswer([acquire(0, 10, ['a']), part({ partition: 1 })]),
        fetchAnswer([part({ partition: 0, ack: 121 }), part({ partition: 1 })]),
      ])
      await m.loop.poll()
      m.want = [A1]
      await m.loop.poll()
      expect(fetchFrames(h)[1].fields.forgottenTopicsData).toEqual([
        { topicId: A, partitions: [0] },
      ])
      expect(failures(m)).toHaveLength(1)
      expect(failures(m)[0].error.cause).toMatchObject({ code: 121 })
    })

    test('an omitted acknowledgement partition fails its ticket without stranding sent work', async () => {
      h = await harness()
      const [m] = h.members
      m.want = [A0, A1]
      h.script('ShareFetch', [
        fetchAnswer([acquire(0, 10, ['a']), acquire(1, 20, ['b'])]),
        fetchAnswer([part({ partition: 1 })]),
      ])
      await m.loop.poll()
      await m.loop.poll()
      expect(failures(m)).toHaveLength(1)
      expect(failures(m)[0]).toMatchObject({ partition: 0, firstOffset: '10', lastOffset: '10' })
      expect(failures(m)[0].error.reason).toBe('rejected-by-broker')
      expect(failures(m)[0].error.cause.message).toMatch(/omitted a partition/)
      expect(okResults(m)).toEqual([expect.objectContaining({ partition: 1, firstOffset: '20' })])
      await m.loop.poll()
      expect(fetchFrames(h)[2].fields.topics).toEqual([])
      expect(m.results).toHaveLength(2)
    })

    test('a repeated response partition fails that ticket and settles its siblings normally', async () => {
      h = await harness()
      const [m] = h.members
      m.want = [A0, A1]
      h.script('ShareFetch', [
        fetchAnswer([acquire(0, 10, ['a']), acquire(1, 20, ['b'])]),
        fetchAnswer([part({ partition: 0 }), part({ partition: 1 }), part({ partition: 1 })]),
      ])
      await m.loop.poll()
      await m.loop.poll()
      expect(failures(m)).toHaveLength(1)
      expect(failures(m)[0]).toMatchObject({ partition: 1 })
      expect(failures(m)[0].error.cause.message).toMatch(/repeated a partition/)
      expect(okResults(m)).toEqual([expect.objectContaining({ partition: 0 })])
    })
  })

  describe('unawaited renewal', () => {
    /** One acquisition whose handler starts a renewal, decides the record and returns without awaiting it. */
    const renewing = async (answer) => {
      h = await harness()
      const [m] = h.members
      m.want = [A0]
      h.script('ShareFetch', [fetchAnswer([acquire(0, 10, ['a'])])])
      const gate = h.hold({ api: 'ShareAcknowledge' })
      const prepare = jest.spyOn(ShareAckLedger.prototype, 'prepare')
      const captured = {}
      m.dispatch = async (rows) => {
        captured.row = rows[0]
        captured.renewal = settled(
          m.loop.acknowledge({ renew: [{ cohort: rows[0], offsets: ['10'] }] })
        )
        await new Promise(setImmediate)
        rows[0].ledger.complete(['10'], 'accept')
      }
      await m.loop.poll()
      await gate.arrived()
      const next = m.loop.poll()
      await flush()
      expect(prepare).toHaveBeenCalledTimes(1)
      expect(fetchFrames(h)).toHaveLength(1)
      gate.release(answer)
      return { m, captured, next, prepare }
    }

    test('settles an unawaited renewal before preparing terminal work', async () => {
      const { m, captured, next, prepare } = await renewing(
        ackAnswer([{ partition: 0 }], { acquisitionLockTimeoutMs: 20000 })
      )
      expect((await captured.renewal).value).toMatchObject({ lockDuration: 20000 })
      expect(m.results.filter((outcome) => outcome.type === 'accept')).toEqual([])
      expect(captured.row.ledger.needsFlush(9000)).toBe(false)
      expect(captured.row.walk.deliver.deliveryCounts[0]).toBe(1)
      await next
      expect(prepare).toHaveBeenCalledTimes(2)
      expect(fetchFrames(h)[1].fields).toMatchObject({ shareSessionEpoch: 2 })
      expect(fetchFrames(h)[1].fields.topics[0].partitions[0].acknowledgementBatches).toEqual([
        batchOf(10, 10, 1),
      ])
    })

    test('a renewal rejected with 121 fails with the compact error and keeps the old deadline', async () => {
      const { m, captured, next } = await renewing(ackAnswer([{ partition: 0, code: 121 }]))
      const failure = (await captured.renewal).error
      expect(failure).toBeInstanceOf(KafkaJSShareAcknowledgementError)
      expect(failure.reason).toBe('rejected-by-broker')
      expect(failure.cause).toMatchObject({ code: 121 })
      expect(captured.row.ledger.needsFlush(9000)).toBe(true)
      await next
      expect(
        m.results.filter((outcome) => outcome.type === 'accept' && !outcome.error)
      ).toHaveLength(1)
    })
  })

  describe('requests with nothing to send', () => {
    test('a node with nothing wanted and nothing modelled writes no fetch', async () => {
      h = await harness()
      const [m] = h.members
      m.want = []
      const result = await m.loop.poll()
      expect(result.empty).toBe(true)
      expect(frames(h.cluster)).toHaveLength(0)
      expect(m.session.epoch).toBe(0)
    })

    test('a flush with no decided acquisition writes no acknowledgement', async () => {
      h = await harness()
      const [m] = h.members
      m.want = [A0]
      await m.loop.poll()
      const result = await m.loop.acknowledge()
      expect(result.empty).toBe(true)
      expect(frames(h.cluster, 79)).toHaveLength(0)
      expect(m.session.epoch).toBe(1)
    })

    test('an acknowledgement on a session that never opened is refused locally', async () => {
      h = await harness()
      const [m] = h.members
      await expect(m.loop.acknowledge()).rejects.toThrow('established')
      expect(frames(h.cluster)).toHaveLength(0)
    })
  })

  describe('closing a node', () => {
    test('sends no final for a node that never opened', async () => {
      h = await harness()
      const [m] = h.members
      m.want = []
      await m.loop.finishNode()
      expect(frames(h.cluster)).toHaveLength(0)
      expect(m.session.closed).toBe(true)
      await expect(m.loop.poll()).rejects.toThrow('closing')
    })

    test('closes an established last-unwanted node once with its terminal tickets', async () => {
      h = await harness()
      const [m] = h.members
      m.want = [A0]
      h.script('ShareFetch', [fetchAnswer([acquire(0, 10, ['a', 'b'])])])
      await m.loop.poll()
      m.want = []
      await m.loop.finishNode()
      await m.loop.finishNode()
      expect(fetchFrames(h)).toHaveLength(1)
      const finals = frames(h.cluster, 79)
      expect(finals).toHaveLength(1)
      expect(finals[0].fields).toMatchObject({ shareSessionEpoch: -1, isRenewAck: false })
      expect(finals[0].fields.topics).toEqual([
        {
          topicId: A,
          partitions: [{ partitionIndex: 0, acknowledgementBatches: [batchOf(10, 11, 1)] }],
        },
      ])
      expect(okResults(m)).toEqual([
        expect.objectContaining({ firstOffset: '10', lastOffset: '11', type: 'accept' }),
      ])
      await expect(m.loop.poll()).rejects.toThrow('closing')
      expect(frames(h.cluster)).toHaveLength(2)
    })

    test.each([
      [122, 'session-lost'],
      [123, 'session-lost'],
      [133, 'rejected-by-broker'],
      [35, 'rejected-by-broker'],
      [-1, 'rejected-by-broker'],
    ])(
      'a final answered with %s keeps the node terminal and fails only its tickets',
      async (code, reason) => {
        h = await harness()
        const [m] = h.members
        m.want = [A0]
        h.script('ShareFetch', [fetchAnswer([acquire(0, 10, ['a'])])])
        h.script('ShareAcknowledge', [topError(code)])
        await m.loop.poll()
        m.want = []
        await m.loop.finishNode()
        expect(m.resets).toEqual([])
        expect(m.sleeps).toEqual([])
        expect(m.session.closed).toBe(true)
        expect(frames(h.cluster, 79)).toHaveLength(1)
        expect(fetchFrames(h)).toHaveLength(1)
        expect(failures(m)).toHaveLength(1)
        expect(failures(m)[0].error.reason).toBe(reason)
        expect(okResults(m)).toEqual([])
      }
    )

    test('an idle close waits for a pending renewal before its final', async () => {
      h = await harness()
      const [m] = h.members
      m.want = [A0]
      h.script('ShareFetch', [fetchAnswer([acquire(0, 10, ['a'])])])
      const gate = h.hold({ api: 'ShareAcknowledge' })
      const renewal = deferred()
      m.dispatch = async (rows) => {
        renewal.resolve(m.loop.acknowledge({ renew: [{ cohort: rows[0], offsets: ['10'] }] }))
        await new Promise(setImmediate)
        rows[0].ledger.complete(['10'], 'accept')
      }
      await m.loop.poll()
      await gate.arrived()
      m.want = []
      const closing = m.loop.finishNode()
      await flush()
      expect(frames(h.cluster, 79)).toHaveLength(1)
      gate.release(ackAnswer([{ partition: 0 }]))
      await closing
      await renewal.promise
      const written = frames(h.cluster, 79).map(({ fields }) => fields)
      expect(written.map(({ isRenewAck }) => isRenewAck)).toEqual([true, false])
      expect(written[1].shareSessionEpoch).toBe(-1)
      expect(written[1].topics[0].partitions[0].acknowledgementBatches).toEqual([
        batchOf(10, 10, 1),
      ])
    })
  })

  test('a final that gets no answer fails its tickets session-lost without reopening the node', async () => {
    h = await harness({ pool: { requestTimeout: 100, enforceRequestTimeout: true } })
    const [m] = h.members
    m.want = [A0]
    h.script('ShareFetch', [fetchAnswer([acquire(0, 10, ['a'])])])
    await m.loop.poll()
    h.hold({ api: 'ShareAcknowledge' })
    m.want = []
    await m.loop.finishNode()
    expect(frames(h.cluster, 79)).toHaveLength(1)
    expect(m.resets).toEqual([])
    expect(m.session.closed).toBe(true)
    expect(failures(m).map(({ error }) => error.reason)).toEqual(['session-lost'])
    expect(okResults(m)).toEqual([])
  })

  describe('captured closing', () => {
    /** An established node holding one decided record, 10 and 11, of partition 0. */
    const established = async () => {
      h = await harness()
      const [m] = h.members
      m.want = [A0]
      h.script('ShareFetch', [fetchAnswer([acquire(0, 10, ['a', 'b'])])])
      await m.loop.poll()
      return m
    }

    test('uses only the captured closing predicate for a preserved terminal subset', async () => {
      const m = await established()
      const captured = m.loop.captureFinal()
      expect(captured.prepared).toHaveLength(1)
      m.operation.retire(new Error('run closed'))
      expect(m.session.refusal(m.session.token)).toBeInstanceOf(Error)
      await m.loop.finishNode({ closing: { guard: () => null, captured } })
      const finals = frames(h.cluster, 79)
      expect(finals).toHaveLength(1)
      expect(finals[0].fields).toMatchObject({ shareSessionEpoch: -1 })
      expect(finals[0].fields.topics[0].partitions[0].acknowledgementBatches).toEqual([
        batchOf(10, 11, 1),
      ])
      expect(okResults(m)).toEqual([
        expect.objectContaining({ firstOffset: '10', lastOffset: '11' }),
      ])
      expect(failures(m)).toEqual([])
      expect(m.session.closed).toBe(true)
    })

    test('a revoked closing predicate sends nothing and settles the tickets as not sent', async () => {
      const m = await established()
      const captured = m.loop.captureFinal()
      await m.loop.finishNode({ closing: { guard: () => new Error('revoked'), captured } })
      expect(frames(h.cluster, 79)).toHaveLength(0)
      expect(failures(m).map(({ error }) => error.reason)).toEqual(['not-sent'])
      expect(okResults(m)).toEqual([])
    })

    test('a final answered after its predicate expired publishes no acceptance', async () => {
      const m = await established()
      const captured = m.loop.captureFinal()
      const gate = h.hold({ api: 'ShareAcknowledge' })
      let expired = false
      const closing = m.loop.finishNode({
        closing: { guard: () => (expired ? new Error('closing expired') : null), captured },
      })
      await gate.arrived()
      expired = true
      gate.release(ackAnswer([{ partition: 0 }]))
      await closing
      expect(okResults(m)).toEqual([])
      expect(failures(m).map(({ error }) => error.reason)).toEqual(['session-lost'])
    })

    test('a forced close skips a node whose request is still running', async () => {
      const m = await established()
      const gate = h.hold({ api: 'ShareFetch' })
      const round = m.loop.poll()
      await gate.arrived()
      expect(m.loop.captureFinal()).toBeNull()
      gate.release()
      await round
    })
  })

  describe('resources', () => {
    test('removes an in-flight request and its waits when the node is disposed', async () => {
      h = await harness()
      const [m] = h.members
      m.want = [A0]
      await m.loop.poll()
      const gate = h.hold({ api: 'ShareFetch' })
      const round = settled(m.loop.poll())
      await gate.arrived()
      const sockets = h.cluster.sockets().length
      m.loop.dispose(new Error('node closed'))
      expect((await round).error.message).toBe('node closed')
      await flush()
      expect(m.operation.waiters.size).toBe(0)
      expect(m.operation.disposers.size).toBe(0)
      expect(m.session.queue).toHaveLength(0)
      expect(m.loop.exchanges.size).toBe(0)
      expect(m.loop.cohorts.size).toBe(0)
      expect(m.pool.pool[1].requestQueue.pending).toHaveLength(0)
      expect(m.pool.pool[1].isConnected()).toBe(false)
      gate.release()
      await flush()
      expect(h.cluster.sockets()).toHaveLength(sockets)
    })

    test('removes a pending backoff wait when the node is disposed', async () => {
      h = await harness()
      const [m] = h.members
      m.want = [A0]
      m.operation.sleep.mockRestore()
      h.script('ShareFetch', [topError(-1)])
      const round = settled(m.loop.poll())
      for (let turn = 0; turn < 200 && m.operation.disposers.size === 0; turn++)
        await new Promise(setImmediate)
      expect(m.operation.disposers.size).toBe(1)
      m.loop.dispose(new Error('node closed'))
      expect((await round).error.message).toBe('node closed')
      expect(m.operation.disposers.size).toBe(0)
      expect(m.operation.waiters.size).toBe(0)
      expect(m.operation.timer).toBeNull()
    })

    test('forgetting one partition keeps only the other partition state', async () => {
      h = await harness()
      const [m] = h.members
      m.want = [A0, A1]
      h.script('ShareFetch', [fetchAnswer([acquire(0, 10, ['a']), acquire(1, 20, ['b'])])])
      await m.loop.poll()
      m.want = [A1]
      await m.loop.poll()
      expect([...m.session.model.keys()]).toEqual([`${A}:1`])
      expect([...m.loop.cohorts.keys()]).toEqual([])
      expect(m.operation.waiters.size).toBe(0)
      expect(m.operation.disposers.size).toBe(0)
      expect(m.session.queue).toHaveLength(0)
      expect(m.loop.exchanges.size).toBe(0)
    })
  })

  describe('compact halted and incomplete records', () => {
    test('releases compact halted remainder without guessing a corrupt interval', async () => {
      h = await harness()
      const [m] = h.members
      m.want = [A0]
      const good = batch({
        baseOffset: 100n,
        records: [
          record({ offsetDelta: 0, key: 'k0', value: 'p' }),
          record({ offsetDelta: 1, key: 'k1', value: 'q' }),
        ],
      })
      const bad = batch({
        baseOffset: 102n,
        records: [record({ offsetDelta: 0, key: 'k2', value: 'x' })],
        crc: 1,
      })
      h.script('ShareFetch', [
        fetchAnswer([
          part({
            partition: 0,
            acquire: [acquired(100, 1000099)],
            records: Buffer.concat([good, bad]).toString('hex'),
          }),
        ]),
      ])
      await m.loop.poll()
      expect(m.handled.map(({ value }) => value)).toEqual(['p', 'q'])
      await m.loop.poll()
      expect(fetchFrames(h)[1].fields.topics[0].partitions[0].acknowledgementBatches).toEqual([
        batchOf(100, 101, 1),
        batchOf(102, 1000099, 2),
      ])
      expect(
        okResults(m).map(({ firstOffset, lastOffset, type }) => [firstOffset, lastOffset, type])
      ).toEqual([
        ['100', '101', 'accept'],
        ['102', '1000099', 'release'],
      ])
    })

    test('turns an incomplete trailing batch into one gap acknowledgement', async () => {
      h = await harness()
      const [m] = h.members
      m.want = [A0]
      const incomplete = Buffer.alloc(13)
      incomplete.writeBigInt64BE(0n, 0)
      incomplete.writeInt32BE(100, 8)
      h.script('ShareFetch', [
        fetchAnswer([
          part({ partition: 0, acquire: [acquired(0, 0)], records: incomplete.toString('hex') }),
        ]),
      ])
      await m.loop.poll()
      expect(m.handled).toEqual([])
      await m.loop.poll()
      expect(fetchFrames(h)[1].fields.topics[0].partitions[0].acknowledgementBatches).toEqual([
        batchOf(0, 0, 0),
      ])
    })
  })
})
