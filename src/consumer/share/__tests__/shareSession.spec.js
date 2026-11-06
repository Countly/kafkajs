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
const { KafkaJSShareAcknowledgementError } = require('../../../errors')
const { ShareSession } = require('../shareSession')
const { ShareFetchLoop } = require('../shareFetchLoop')

const A = '00112233-4455-6677-8899-aabbccddeeff'

/** @returns {{session: ShareSession, onReset: jest.Mock, operation: ConsumerOperation}} A session on a real operation. */
const unit = () => {
  const onReset = jest.fn()
  const operation = new ConsumerOperation()
  return { session: new ShareSession({ operation, onReset }), onReset, operation }
}

/** @param {ShareSession} session Session. @param {number} [apiKey] API. @param {object} [extra] Fields. @returns {object} Original request descriptor. */
const request = (session, apiKey = 78, extra = {}) => ({
  token: session.token,
  epoch: session.epoch,
  apiKey,
  topics: [],
  ...extra,
})

describe('serialized share session object', () => {
  test('opens at zero advances only on answers wraps and closes', () => {
    const { session } = unit()
    const open = request(session)
    expect(session.epoch).toBe(0)
    session.apply(open, { errorCode: 0 })
    expect(session.epoch).toBe(1)
    session.apply(request(session), { errorCode: 0 })
    expect(session.epoch).toBe(2)
    session.epoch = 2147483647
    session.apply(request(session), { errorCode: 0 })
    expect(session.epoch).toBe(1)
    session.apply(request(session, 79, { epoch: -1 }), { errorCode: 0 })
    expect(session.closed).toBe(true)
    for (const errorCode of [122, 123, 133, 35, -1]) {
      const final = unit()
      final.session.epoch = 2
      const policy = final.session.apply(request(final.session, 79, { epoch: -1 }), { errorCode })
      expect(policy).toMatchObject({ action: 'closed', backoff: false })
      expect(final.session.closed).toBe(true)
      expect(final.session.refusal(final.session.token)).toBeInstanceOf(Error)
      expect(final.onReset).not.toHaveBeenCalled()
    }
  })

  test('a held answer prevents a second request and builders are lazy', async () => {
    const { session } = unit()
    const schedule = new Schedule()
    const gate = schedule.hold({ api: 'first' })
    const calls = []
    const first = session.enqueue(async () => {
      calls.push('first')
      await schedule.run({ api: 'first' }, () => null)
    })
    await gate.arrived()
    const second = session.enqueue(async () => {
      calls.push('second')
      return 2
    })
    expect(calls).toEqual(['first'])
    gate.release()
    await expect(first).resolves.toBeUndefined()
    await expect(second).resolves.toBe(2)
    expect(calls).toEqual(['first', 'second'])
  })

  test.each([122, 123, 133])(
    'fetch code %s resets and old queued work cannot resend',
    async (code) => {
      const { session, onReset } = unit()
      const schedule = new Schedule()
      const gate = schedule.hold({ api: 'active' })
      session.epoch = 4
      const old = session.token
      const active = session.enqueue(() => schedule.run({ api: 'active' }, () => null))
      await gate.arrived()
      const builder = jest.fn()
      const queued = session.enqueue(builder)
      let queuedError
      queued.catch((error) => {
        queuedError = error
      })
      try {
        const reply = session.apply(request(session), { errorCode: code })
        expect(reply.backoff).toBe(code === 133)
        expect(session.token).not.toBe(old)
        expect(session.epoch).toBe(0)
        expect(onReset.mock.calls[0][0]).toBe(old)
        expect(session.queue).toHaveLength(0)
        await Promise.resolve()
        expect(queuedError).toBeInstanceOf(Error)
        expect(builder).not.toHaveBeenCalled()
        expect(session.apply(request(session), { errorCode: code }).backoff).toBe(true)
        expect(() =>
          session.apply({ token: old, apiKey: 78, epoch: 4, topics: [] }, { errorCode: 0 })
        ).toThrow()
      } finally {
        gate.release()
        await Promise.allSettled([active, queued])
      }
    }
  )

  test('a reset rejects queued work before its callback can queue fresh work', async () => {
    const operation = new ConsumerOperation()
    let fresh
    const session = new ShareSession({
      operation,
      onReset: () => {
        fresh = session.enqueue(async () => 'fresh')
      },
    })
    const schedule = new Schedule()
    const gate = schedule.hold({ api: 'active' })
    const active = session.enqueue(() => schedule.run({ api: 'active' }, () => null))
    await gate.arrived()
    const stale = session.enqueue(async () => 'stale')
    session.reset('connection', new Error('lost'))
    await expect(stale).rejects.toThrow('lost')
    gate.release()
    await active
    await expect(fresh).resolves.toBe('fresh')
  })

  test('acknowledge 133 advances without resetting and unknown advances with retry', () => {
    const { session, onReset } = unit()
    session.epoch = 2
    const token = session.token
    expect(session.apply(request(session, 79), { errorCode: 133 }).action).toBe('retry')
    expect(session.epoch).toBe(3)
    expect(session.token).toBe(token)
    expect(session.apply(request(session), { errorCode: -1 }).action).toBe('retry')
    expect(session.epoch).toBe(4)
    expect(onReset).not.toHaveBeenCalled()
  })

  test('top-level unknown error -1 advances the epoch under backoff without a reset', () => {
    const { session, onReset } = unit()
    session.epoch = 5
    expect(session.apply(request(session), { errorCode: -1 })).toMatchObject({
      action: 'retry',
      backoff: true,
    })
    expect(session.epoch).toBe(6)
    expect(onReset).not.toHaveBeenCalled()
  })

  test('the first consecutive reset is immediate and a success clears the streak', () => {
    const { session } = unit()
    session.epoch = 3
    expect(session.apply(request(session), { errorCode: 122 }).backoff).toBe(false)
    expect(session.apply(request(session), { errorCode: 122 }).backoff).toBe(true)
    session.apply(request(session), { errorCode: 0 })
    session.epoch = 3
    expect(session.apply(request(session), { errorCode: 123 }).backoff).toBe(false)
  })

  test('models every listed partition then removes same-frame forgotten entries', () => {
    const { session } = unit()
    session.model.set(`${A}:0`, { topicId: A, partition: 0 })
    session.apply(
      request(session, 78, {
        topics: [{ topicId: A, partitions: [{ partitionIndex: 1 }, { partitionIndex: 2 }] }],
        forgottenTopicsData: [{ topicId: A, partitions: [0, 1] }],
      }),
      { errorCode: 0 }
    )
    expect([...session.model.keys()]).toEqual([`${A}:2`])
  })

  test('connection loss refuses before generation changes and cannot adopt a fresh socket', () => {
    const { session } = unit()
    let connected = true
    const connection = { isConnected: () => connected }
    const pool = { generationFor: jest.fn(() => 8) }
    const broker = { connectionPool: pool }
    const token = session.token
    session.bind(token, { broker, pool, connection, generation: 8 })
    expect(session.refusal(token)).toBeNull()
    expect(session.connectionLost(token)).toBe(false)
    connected = false
    expect(session.refusal(token)).toBeInstanceOf(Error)
    expect(session.connectionLost(token)).toBe(true)
    connected = true
    broker.connectionPool = { generationFor: () => 8 }
    expect(session.refusal(token)).toBeInstanceOf(Error)
    broker.connectionPool = pool
    expect(() =>
      session.bind(token, { broker, pool, generation: 8, connection: { isConnected: () => true } })
    ).toThrow('replaced')
  })

  test('connection loss is not reported for a stale open or a retired operation', () => {
    const { session, operation } = unit()
    let connected = true
    const pool = { generationFor: () => 1 }
    const token = session.token
    session.bind(token, {
      broker: { connectionPool: pool },
      pool,
      connection: { isConnected: () => connected },
      generation: 1,
    })
    connected = false
    expect(session.connectionLost(token)).toBe(true)
    expect(session.connectionLost(Symbol('other'))).toBe(false)
    operation.retire(new Error('closed'))
    expect(session.connectionLost(token)).toBe(false)
  })

  test('transport reset invalidates old identity synchronously even with equal epoch values', () => {
    const { session, onReset } = unit()
    const token = session.token
    session.reset('connection', new Error('timeout'))
    expect(session.epoch).toBe(0)
    expect(session.refusal(token)).toBeInstanceOf(Error)
    expect(onReset).toHaveBeenCalledTimes(1)
    expect(onReset.mock.calls[0][1]).toBe('connection')
  })

  test('retirement rejects queued work and starts no lazy builder after a held answer', async () => {
    const { session } = unit()
    const schedule = new Schedule()
    const gate = schedule.hold({ api: 'first' })
    const first = session.enqueue(() => schedule.run({ api: 'first' }, () => null))
    await gate.arrived()
    const builder = jest.fn()
    const next = session.enqueue(builder)
    const error = new Error('closed')
    session.retire(error)
    await expect(next).rejects.toBe(error)
    gate.release()
    await expect(first).rejects.toBe(error)
    await Promise.resolve()
    expect(builder).not.toHaveBeenCalled()
    expect(session.queue).toHaveLength(0)
  })

  test('busy covers a running request and a queued one', async () => {
    const { session } = unit()
    const schedule = new Schedule()
    const gate = schedule.hold({ api: 'first' })
    expect(session.busy).toBe(false)
    const first = session.enqueue(() => schedule.run({ api: 'first' }, () => null))
    await gate.arrived()
    const second = session.enqueue(async () => null)
    expect(session.busy).toBe(true)
    gate.release()
    await Promise.all([first, second])
    expect(session.busy).toBe(false)
  })

  test('a captured final ends the session after the ordinary operation retired', () => {
    const { session, operation } = unit()
    session.epoch = 3
    operation.retire(new Error('run closed'))
    expect(() => session.apply(request(session, 79, { epoch: -1 }), { errorCode: 0 })).toThrow(
      'run closed'
    )
    expect(
      session.applyFinal(request(session, 79, { epoch: -1 }), { errorCode: 122 })
    ).toMatchObject({ action: 'closed', backoff: false })
    expect(session.closed).toBe(true)
  })
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
  names = new Map([[A, 'topic-a']]),
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

describe('share session socket integration', () => {
  let h
  afterEach(async () => {
    if (h) await h.close()
    h = null
    jest.restoreAllMocks()
  })

  test('serializes data and acknowledgement requests at the socket', async () => {
    h = await harness()
    const [m] = h.members
    m.want = [{ topicId: A, partition: 0 }]
    h.script('ShareFetch')
    const gate = h.hold({ api: 'ShareFetch' })
    const send = () =>
      m.session.enqueue((open) => m.loop.exchange({ apiKey: 78, kind: 'fetch' }, open))
    const first = send()
    await gate.arrived()
    const second = send()
    const third = send()
    await flush()
    expect(frames(h.cluster)).toHaveLength(1)
    expect(frames(h.cluster)[0].fields).toMatchObject({
      shareSessionEpoch: 0,
      topics: [{ topicId: A, partitions: [{ partitionIndex: 0, acknowledgementBatches: [] }] }],
    })
    gate.release()
    await Promise.all([first, second, third])
    expect(frames(h.cluster).map(({ fields }) => fields.shareSessionEpoch)).toEqual([0, 1, 2])
    m.session.epoch = 2147483647
    await send()
    await send()
    expect(frames(h.cluster).map(({ fields }) => fields.shareSessionEpoch)).toEqual([
      0, 1, 2, 2147483647, 1,
    ])
    await m.loop.finishNode()
    const last = frames(h.cluster).at(-1)
    expect(last).toMatchObject({ apiKey: 79, fields: { shareSessionEpoch: -1, isRenewAck: false } })
  })

  describe('session errors', () => {
    const retry = { initialRetryTime: 0, factor: 0, multiplier: 1, maxRetryTime: 0 }

    test.each([122, 123, 133])(
      'opening fetch code %s resets without tickets and reopens at epoch zero',
      async (code) => {
        h = await harness({ loop: { retry } })
        const [m] = h.members
        m.want = [{ topicId: A, partition: 0 }]
        h.script('ShareFetch', [topError(code)])
        const result = await m.loop.poll()
        expect(result).toMatchObject({ reset: true, backoff: code === 133 })
        expect(m.resets).toHaveLength(1)
        expect(m.session.epoch).toBe(0)
        if (code === 133) expect(m.sleeps).toEqual([250])
        else expect(m.sleeps).toEqual([])
        await m.loop.poll()
        const opening = frames(h.cluster, 78)
        expect(opening.map(({ fields }) => fields.shareSessionEpoch)).toEqual([0, 0])
        expect(opening[1].fields.topics[0].partitions).toEqual([
          { partitionIndex: 0, acknowledgementBatches: [] },
        ])
      }
    )

    test.each([122, 123, 133])(
      'piggyback fetch code %s resets and settles the old acknowledgement session-lost once',
      async (code) => {
        h = await harness({ loop: { retry } })
        const [m] = h.members
        m.want = [{ topicId: A, partition: 0 }]
        h.script('ShareFetch', [fetchAnswer([acquire(0, 10, ['a', 'b'])]), topError(code)])
        await m.loop.poll()
        expect(m.session.epoch).toBe(1)
        const result = await m.loop.poll()
        expect(result.reset).toBe(true)
        const failures = m.results.filter((outcome) => outcome.error)
        expect(failures).toHaveLength(1)
        expect(failures[0]).toMatchObject({ firstOffset: '10', lastOffset: '11', type: 'accept' })
        expect(failures[0].error).toBeInstanceOf(KafkaJSShareAcknowledgementError)
        expect(failures[0].error.reason).toBe('session-lost')
        expect(m.results.filter((outcome) => !outcome.error)).toEqual([])
        await m.loop.poll()
        const last = frames(h.cluster, 78).at(-1).fields
        expect(last.shareSessionEpoch).toBe(0)
        expect(last.topics).toEqual([
          { topicId: A, partitions: [{ partitionIndex: 0, acknowledgementBatches: [] }] },
        ])
        expect(m.results.filter((outcome) => outcome.error)).toHaveLength(1)
      }
    )

    test.each([122, 123])(
      'acknowledge code %s resets and never replays the old decision',
      async (code) => {
        h = await harness()
        const [m] = h.members
        m.want = [{ topicId: A, partition: 0 }]
        h.script('ShareFetch', [fetchAnswer([acquire(0, 10, ['a', 'b'])])])
        h.script('ShareAcknowledge', [topError(code)])
        await m.loop.poll()
        const result = await m.loop.acknowledge()
        expect(result.reset).toBe(true)
        expect(m.resets).toHaveLength(1)
        const failures = m.results.filter((outcome) => outcome.error)
        expect(failures.map((outcome) => outcome.error.reason)).toEqual(['session-lost'])
        await m.loop.poll()
        const last = frames(h.cluster, 78).at(-1).fields
        expect(last.shareSessionEpoch).toBe(0)
        expect(last.topics[0].partitions[0].acknowledgementBatches).toEqual([])
      }
    )

    test('acknowledge code 133 advances and rejects its tickets without a reset', async () => {
      h = await harness()
      const [m] = h.members
      m.want = [{ topicId: A, partition: 0 }]
      h.script('ShareFetch', [fetchAnswer([acquire(0, 10, ['a', 'b'])])])
      h.script('ShareAcknowledge', [topError(133)])
      await m.loop.poll()
      const open = m.session.token
      const result = await m.loop.acknowledge()
      expect(result).toMatchObject({ reset: false, backoff: true })
      expect(m.resets).toEqual([])
      expect(m.session.token).toBe(open)
      expect(m.session.epoch).toBe(2)
      const failures = m.results.filter((outcome) => outcome.error)
      expect(failures.map((outcome) => outcome.error.reason)).toEqual(['rejected-by-broker'])
    })

    test('the first reset is immediate, later ones are delayed, and 133 waits even with a zero retry time', async () => {
      h = await harness({ loop: { retry } })
      const [m] = h.members
      m.want = [{ topicId: A, partition: 0 }]
      h.script('ShareFetch', [topError(122), topError(122), topError(133)])
      expect(await m.loop.poll()).toMatchObject({ reset: true, backoff: false })
      expect(m.sleeps).toEqual([])
      expect(await m.loop.poll()).toMatchObject({ reset: true, backoff: true })
      expect(await m.loop.poll()).toMatchObject({ reset: true, backoff: true })
      expect(m.sleeps).toEqual([50, 250])
    })

    test('a handler held across a reset gets no traffic afterwards', async () => {
      h = await harness()
      const [m] = h.members
      m.want = [{ topicId: A, partition: 0 }]
      h.script('ShareFetch', [fetchAnswer([acquire(0, 10, ['a', 'b'])])])
      h.script('ShareAcknowledge', [topError(122)])
      let rows
      let started
      const running = new Promise((resolve) => {
        started = resolve
      })
      let proceed
      const held = new Promise((resolve) => {
        proceed = resolve
      })
      let late
      m.dispatch = async (cohorts) => {
        rows = cohorts
        started()
        await held
        try {
          rows[0].ledger.complete(['10', '11'], 'accept')
        } catch (error) {
          late = error
        }
      }
      const round = m.loop.poll()
      const outcome = round.then(
        (value) => ({ value }),
        (error) => ({ error })
      )
      await running
      await expect(
        m.loop.acknowledge({ renew: [{ cohort: rows[0], offsets: ['10', '11'] }] })
      ).rejects.toBeInstanceOf(KafkaJSShareAcknowledgementError)
      expect(m.resets).toHaveLength(1)
      const before = frames(h.cluster).length
      proceed()
      expect((await outcome).error).toBeInstanceOf(Error)
      expect(late.message).toMatch(/outside its active handler acquisition/)
      expect(frames(h.cluster)).toHaveLength(before)
      expect(m.results.filter((result) => !result.error)).toEqual([])
    })
  })

  describe('written but unanswered mutations', () => {
    test('drops a written unanswered mutation instead of resending it', async () => {
      h = await harness({ pool: { requestTimeout: 100, enforceRequestTimeout: true } })
      const [m] = h.members
      m.want = [{ topicId: A, partition: 0 }]
      h.script('ShareFetch', [fetchAnswer([acquire(0, 10, ['a', 'b'])])])
      await m.loop.poll()
      const gate = h.hold({
        api: 'ShareFetch',
        match: (call) => call.fields.shareSessionEpoch === 1,
      })
      const result = await m.loop.poll()
      const written = frames(h.cluster, 78)
      expect(written.map(({ fields }) => fields.shareSessionEpoch)).toEqual([0, 1])
      expect(written[1].fields.topics[0].partitions[0].acknowledgementBatches).toEqual([
        { firstOffset: '10', lastOffset: '11', acknowledgeTypes: [1] },
      ])
      expect(result).toMatchObject({ reset: true, backoff: true })
      expect(m.resets.map(({ reason }) => reason)).toEqual(['connection'])
      const failures = m.results.filter((outcome) => outcome.error)
      expect(failures.map((outcome) => outcome.error.reason)).toEqual(['session-lost'])
      expect(m.results.filter((outcome) => !outcome.error)).toEqual([])
      await m.loop.poll()
      const reopened = frames(h.cluster, 78).at(-1).fields
      expect(reopened.shareSessionEpoch).toBe(0)
      expect(reopened.topics[0].partitions[0].acknowledgementBatches).toEqual([])
      const settled = m.results.length
      gate.release(fetchAnswer([part({ partition: 0 })]))
      await flush()
      expect(m.results).toHaveLength(settled)
      expect(m.session.epoch).toBe(1)
    })

    test('a disconnect before the generation counter moves refuses at once and resets', async () => {
      h = await harness()
      const [m] = h.members
      m.want = [{ topicId: A, partition: 0 }]
      h.script('ShareFetch', [fetchAnswer([acquire(0, 10, ['a', 'b'])])])
      await m.loop.poll()
      const generation = m.pool.generationFor(78)
      await m.pool.pool[1].disconnect({ drainTimeout: 0 })
      expect(m.pool.generationFor(78)).toBe(generation)
      expect(m.session.refusal(m.session.token)).toBeInstanceOf(Error)
      const written = frames(h.cluster).length
      const result = await m.loop.poll()
      expect(result.reset).toBe(true)
      expect(frames(h.cluster)).toHaveLength(written)
      expect(m.results.filter((outcome) => outcome.error).map((o) => o.error.reason)).toEqual([
        'session-lost',
      ])
      await m.loop.poll()
      const reopened = frames(h.cluster, 78).at(-1).fields
      expect(reopened.shareSessionEpoch).toBe(0)
      expect(reopened.topics[0].partitions[0].acknowledgementBatches).toEqual([])
    })
  })

  test('keeps the session across a heartbeat-socket reconnect only', async () => {
    h = await harness()
    const [m] = h.members
    m.want = [{ topicId: A, partition: 0 }]
    await m.loop.poll()
    const data = m.pool.generationFor(78)
    const heartbeat = m.pool.pool[0].generation
    await m.pool.pool[0].disconnect({ drainTimeout: 0 })
    await m.pool.getConnection(0)
    expect(m.pool.pool[0].generation).toBeGreaterThan(heartbeat)
    expect(m.pool.generationFor(78)).toBe(data)
    await m.loop.poll()
    expect(m.resets).toEqual([])
    expect(frames(h.cluster, 78).map(({ fields }) => fields.shareSessionEpoch)).toEqual([0, 1])
    await m.pool.pool[1].disconnect({ drainTimeout: 0 })
    expect(m.pool.generationFor(78)).toBe(data)
    expect(m.session.refusal(m.session.token)).toBeInstanceOf(Error)
    expect((await m.loop.poll()).reset).toBe(true)
    await m.loop.poll()
    expect(frames(h.cluster, 78).map(({ fields }) => fields.shareSessionEpoch)).toEqual([0, 1, 0])
    const replacement = h.newPool(m.node)
    m.broker.replaceConnectionPool(replacement)
    expect(m.session.refusal(m.session.token)).toBeInstanceOf(Error)
    expect((await m.loop.poll()).reset).toBe(true)
    await m.loop.poll()
    expect(frames(h.cluster, 78).map(({ fields }) => fields.shareSessionEpoch)).toEqual([
      0, 1, 0, 0,
    ])
    expect(m.session.binding.pool).toBe(replacement)
  })

  describe('delayed old callbacks', () => {
    /** Starts a replacement run on the same broker with equal member and wanted data. */
    const replace = (m) => {
      const next = h.runOn({ node: m.node, pool: m.pool, broker: m.broker })
      next.want = [...m.want]
      return next
    }

    test('an old connection acquisition cannot touch a replacement run', async () => {
      h = await harness()
      const [old] = h.members
      old.want = [{ topicId: A, partition: 0 }]
      const original = old.pool.getConnectionByRequest.bind(old.pool)
      let entered
      const arrived = new Promise((resolve) => {
        entered = resolve
      })
      let release
      const held = new Promise((resolve) => {
        release = resolve
      })
      jest.spyOn(old.pool, 'getConnectionByRequest').mockImplementationOnce(async (...args) => {
        entered()
        await held
        return original(...args)
      })
      const stale = old.loop.poll()
      const staleOutcome = stale.then(
        () => 'resolved',
        (error) => error.message
      )
      await arrived
      old.operation.retire(new Error('run closed'))
      const next = replace(old)
      await next.loop.poll()
      expect(frames(h.cluster, 78)).toHaveLength(1)
      release()
      expect(await staleOutcome).toBe('run closed')
      await flush()
      expect(frames(h.cluster, 78)).toHaveLength(1)
      expect([...old.session.model.keys()]).toEqual([])
      expect([...next.session.model.keys()]).toEqual([`${A}:0`])
      expect(old.results).toEqual([])
      expect(next.session.epoch).toBe(1)
    })

    test('an old transmission is refused at the socket after its run retired', async () => {
      h = await harness()
      const [old] = h.members
      old.want = [{ topicId: A, partition: 0 }]
      const original = old.pool.send.bind(old.pool)
      let entered
      const arrived = new Promise((resolve) => {
        entered = resolve
      })
      let release
      const held = new Promise((resolve) => {
        release = resolve
      })
      jest.spyOn(old.pool, 'send').mockImplementationOnce(async (...args) => {
        entered()
        await held
        return original(...args)
      })
      const stale = old.loop.poll()
      const staleOutcome = stale.then(
        () => 'resolved',
        (error) => error.message
      )
      await arrived
      old.operation.retire(new Error('run closed'))
      const next = replace(old)
      await next.loop.poll()
      release()
      expect(await staleOutcome).toBe('run closed')
      await flush()
      expect(frames(h.cluster, 78)).toHaveLength(1)
      expect(old.session.epoch).toBe(0)
      expect(next.session.epoch).toBe(1)
      expect(old.results).toEqual([])
    })

    test('an old answer applies nothing to a replacement run', async () => {
      h = await harness()
      const [old] = h.members
      old.want = [{ topicId: A, partition: 0 }]
      const gate = h.hold({ api: 'ShareFetch' })
      const stale = old.loop.poll()
      const staleOutcome = stale.then(
        () => 'resolved',
        (error) => error.message
      )
      await gate.arrived()
      old.operation.retire(new Error('run closed'))
      const next = replace(old)
      await next.loop.poll()
      expect(frames(h.cluster, 78)).toHaveLength(2)
      gate.release(fetchAnswer([acquire(0, 10, ['stale'])]))
      expect(await staleOutcome).toBe('run closed')
      await flush()
      expect(old.handled).toEqual([])
      expect(next.handled).toEqual([])
      expect([...old.session.model.keys()]).toEqual([])
      expect(next.session.epoch).toBe(1)
      expect(frames(h.cluster, 78)).toHaveLength(2)
    })

    test('an old leader-hint lookup routes nothing after its run retired', async () => {
      h = await harness()
      const [old] = h.members
      old.want = [{ topicId: A, partition: 0 }]
      h.script('ShareFetch', [
        fetchAnswer([part({ partition: 0, code: 6, leader: { leaderId: 2, leaderEpoch: 8 } })]),
      ])
      let entered
      const arrived = new Promise((resolve) => {
        entered = resolve
      })
      let release
      const held = new Promise((resolve) => {
        release = resolve
      })
      h.metadata.applyLeaderHint.mockImplementationOnce(async () => {
        entered()
        await held
        return true
      })
      const stale = old.loop.poll()
      const staleOutcome = stale.then(
        () => 'resolved',
        (error) => error.message
      )
      await arrived
      old.operation.retire(new Error('run closed'))
      const next = replace(old)
      await next.loop.poll()
      release()
      expect(await staleOutcome).toBe('run closed')
      await flush()
      expect(old.moves).toEqual([])
      expect(next.moves).toEqual([])
      expect(h.metadata.refreshMetadata).not.toHaveBeenCalled()
      expect(frames(h.cluster, 78)).toHaveLength(2)
    })
  })
})
