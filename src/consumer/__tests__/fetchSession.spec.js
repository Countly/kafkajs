const { FetchSession, FetchSessions } = require('../fetchSession')
const ids = new Map([['__proto__', 'old-id']])
const request = (offset = '5', extra = {}) => [
  {
    topic: '__proto__',
    partitions: [
      { partition: 0, fetchOffset: offset, maxBytes: 32768, currentLeaderEpoch: 2, ...extra },
    ],
  },
]
const build = (s, requests = request(), topicIds = ids, usesTopicIds = true) =>
  s.build(requests, { topicIds, usesTopicIds })
const response = (sessionId = 42, partitions = [0], extra = {}) => ({
  errorCode: 0,
  sessionId,
  responses: partitions.length
    ? [{ topicName: '__proto__', partitions: partitions.map((partition) => ({ partition })) }]
    : [],
  ...extra,
})
const active = () => {
  const s = new FetchSession()
  const first = build(s)
  expect(s.handleResponse(first.token, response())).toBe(true)
  return s
}

test('build uses epoch zero and an accepted full response selects epoch one', () => {
  const s = new FetchSession()
  const first = build(s)
  expect(first.wire).toMatchObject({ sessionId: 0, sessionEpoch: 0, forgottenTopics: [] })
  expect(s.epoch).toBe(0)
  expect(s.handleResponse(first.token, response())).toBe(true)
  expect(s.epoch).toBe(1)
  expect(s.handleResponse(first.token, response())).toBe(false)
  expect(s.epoch).toBe(1)
})
test('unchanged active partitions poll and accepted increments advance exactly once', () => {
  const s = active()
  for (const epoch of [1, 2, 3]) {
    const next = build(s)
    expect(next.wire).toMatchObject({
      sessionId: 42,
      sessionEpoch: epoch,
      topics: [],
      forgottenTopics: [],
    })
    expect(next.wire.topicIds).toEqual(ids)
    expect(s.epoch).toBe(epoch)
    expect(s.handleResponse(next.token, response(99, []))).toBe(true)
    expect(s.sessionId).toBe(42)
    expect(s.epoch).toBe(epoch + 1)
  }
})
test('accepted maximum epoch wraps to one without changing request sequence', () => {
  const s = active()
  s.epoch = 2147483647
  const next = build(s)
  const seq = s.seq
  expect(s.handleResponse(next.token, response(42, []))).toBe(true)
  expect(s.epoch).toBe(1)
  expect(s.seq).toBe(seq)
})
test.each([
  ['offset', '6', {}],
  ['bytes', '5', { maxBytes: 2048 }],
  ['leader epoch', '5', { currentLeaderEpoch: 3 }],
])('a changed %s is sent in the next diff', (_name, offset, extra) => {
  const s = active()
  const next = build(s, request(offset, extra))
  expect(next.wire.topics).toHaveLength(1)
  expect(next.wire.topics[0].partitions[0]).toMatchObject({ fetchOffset: offset, ...extra })
})
test('replaced topic sends its recorded old ID and the new full decoding map', () => {
  const s = active()
  const topicIds = new Map([['__proto__', 'new-id']])
  const next = build(s, request(), topicIds)
  expect(next.wire.forgottenTopics).toEqual([
    { topic: '__proto__', topicId: 'old-id', partitions: [0] },
  ])
  expect(next.wire.topicIds.get('__proto__')).toBe('new-id')
  expect(next.wire.topics[0].topicId).toBe('new-id')
})
test('forgetting the last partition sends one recorded ID then becomes idle', () => {
  const s = active()
  const next = build(s, [], new Map())
  expect(next.wire.forgottenTopics).toEqual([
    { topic: '__proto__', topicId: 'old-id', partitions: [0] },
  ])
  expect(s.handleResponse(next.token, response(42, []))).toBe(true)
  expect(s.partitions.size).toBe(0)
  expect(build(s, [], new Map()).wire).toMatchObject({ topics: [], forgottenTopics: [] })
})
test('name and ID mode changes use a full close-existing-attempt-new request', () => {
  const s = active()
  const old = build(s)
  const named = build(s, request(), undefined, false)
  expect(named.wire).toMatchObject({ sessionId: 42, sessionEpoch: 0 })
  expect(named.wire.topics).toHaveLength(1)
  expect(s.handleResponse(old.token, response())).toBe(false)
  expect(s.handleResponse(named.token, response(43))).toBe(true)
  const keyed = build(s)
  expect(keyed.wire).toMatchObject({ sessionId: 43, sessionEpoch: 0 })
})
test.each([70, 71, 106, 7])(
  'top-level error %i resets protocol state and fences duplicate responses',
  (errorCode) => {
    const s = active()
    const next = build(s)
    expect(s.handleResponse(next.token, response(42, [], { errorCode }))).toBe(false)
    expect(build(s).wire).toMatchObject({ sessionId: errorCode === 70 ? 0 : 42, sessionEpoch: 0 })
    expect(s.handleResponse(next.token, response())).toBe(false)
  }
)
test.each(['full', 'incremental'])(
  'ID zero accepts valid %s data and retries full next time',
  (mode) => {
    const s = mode === 'full' ? new FetchSession() : active()
    const next = build(s)
    expect(s.handleResponse(next.token, response(0))).toBe(true)
    expect(build(s).wire).toMatchObject({ sessionId: 0, sessionEpoch: 0 })
  }
)
test.each([
  ['missing', response(42, [])],
  ['extra', response(42, [0, 1])],
  ['throttled', response(42, [], { throttleTime: 100 })],
  ['duplicate', response(42, [0, 0])],
  [
    'unknown ID',
    {
      errorCode: 0,
      sessionId: 42,
      responses: [{ topicName: null, topicId: 'unknown', partitions: [] }],
    },
  ],
])('invalid full response %s delivers nothing and returns to initial', (_name, answer) => {
  const s = new FetchSession()
  const next = build(s)
  expect(s.handleResponse(next.token, answer)).toBe(false)
  expect(build(s).wire).toMatchObject({ sessionId: 0, sessionEpoch: 0 })
})
test('incremental omission is valid but an extra partition forces a full rebuild', () => {
  const s = active()
  const empty = build(s)
  expect(s.handleResponse(empty.token, response(42, []))).toBe(true)
  const extra = build(s)
  expect(s.handleResponse(extra.token, response(42, [1]))).toBe(false)
  expect(build(s).wire).toMatchObject({ sessionId: 42, sessionEpoch: 0 })
})
test('late errors and responses cannot change the latest build or a closed session', () => {
  const s = active()
  const old = build(s)
  const current = build(s, request('6'))
  s.handleError(old.token)
  expect(s.handleResponse(old.token, response(0))).toBe(false)
  expect(s.current(current.token)).toBe(true)
  expect(s.close()).toEqual({ sessionId: 42 })
  s.handleError(current.token)
  expect(s.handleResponse(current.token, response())).toBe(false)
  expect(s.close()).toBeNull()
  expect(s.partitions.size).toBe(0)
})
test('send errors keep the known ID and use a full retry at the unchanged offset', () => {
  const s = active()
  const next = build(s)
  s.handleError(next.token)
  const retry = build(s)
  expect(retry.wire).toMatchObject({ sessionId: 42, sessionEpoch: 0 })
  expect(retry.wire.topics[0].partitions[0].fetchOffset).toBe('5')
})
test('dropped data leaves offset unchanged and an empty diff still accepts redelivery', () => {
  const s = active()
  const data = build(s)
  expect(s.handleResponse(data.token, response())).toBe(true)
  const retry = build(s)
  expect(retry.wire.topics).toEqual([])
  expect(s.partitions.values().next().value.fetchOffset).toBe('5')
  expect(s.handleResponse(retry.token, response())).toBe(true)
})
test('node retirement removes the map entry and fences its held response', () => {
  const registry = new FetchSessions({
    sendClose: jest.fn(),
    requestTimeout: 5000,
    logger: { debug: jest.fn() },
  })
  const s = registry.forNode(1)
  const next = build(s)
  registry.retain([2])
  expect(registry.sessions.size).toBe(0)
  expect(s.handleResponse(next.token, response())).toBe(false)
  expect(registry.forNode(1)).not.toBe(s)
})
test('close sends once per positive ID in parallel with one shared promise', async () => {
  const releases = []
  const sendClose = jest.fn(() => new Promise((resolve) => releases.push(resolve)))
  const registry = new FetchSessions({
    sendClose,
    requestTimeout: 5000,
    logger: { debug: jest.fn() },
  })
  for (const node of [1, 2]) {
    const s = registry.forNode(node)
    const next = build(s)
    s.handleResponse(next.token, response(node))
  }
  registry.forNode(3)
  const close = registry.closeAll({ deadlineAt: performance.now() + 10000 })
  expect(registry.closeAll()).toBe(close)
  await Promise.resolve()
  await Promise.resolve()
  expect(sendClose).toHaveBeenCalledTimes(2)
  for (const [, fields, operation] of sendClose.mock.calls) {
    expect(fields).toMatchObject({ sessionEpoch: -1, topics: [], forgottenTopics: [] })
    expect(operation.remaining()).toBeLessThanOrEqual(10000)
  }
  releases.forEach((resolve) => resolve())
  await close
  expect(registry.sessions.size).toBe(0)
})
test('expired close never calls the sender and failures stay debug-only', async () => {
  const sendClose = jest.fn()
  const debug = jest.fn()
  const registry = new FetchSessions({ sendClose, requestTimeout: 5000, logger: { debug } })
  const s = registry.forNode(1)
  const next = build(s)
  s.handleResponse(next.token, response())
  await registry.closeAll({ deadlineAt: performance.now() })
  expect(sendClose).not.toHaveBeenCalled()
  expect(debug).toHaveBeenCalledTimes(1)
})
test('close without a deadline bounds a never-settling sender by the request timeout', async () => {
  const sendClose = jest.fn(() => new Promise(() => {}))
  const debug = jest.fn()
  const registry = new FetchSessions({ sendClose, requestTimeout: 10, logger: { debug } })
  const s = registry.forNode(1)
  const next = build(s)
  s.handleResponse(next.token, response())
  await registry.closeAll()
  expect(sendClose).toHaveBeenCalledTimes(1)
  expect(debug).toHaveBeenCalledTimes(1)
})

test.each([null, 0, 1, 'true', [], {}])(
  'invalid session option %p rejects at both factory entries',
  (fetchSessions) => {
    const Kafka = require('../../index')
    const direct = require('../index')
    const kafka = new Kafka({ brokers: ['localhost:9092'] })
    expect(() => kafka.consumer({ groupId: 'options', fetchSessions })).toThrow(
      'fetchSessions must be a boolean'
    )
    expect(() => direct({ groupId: 'options', fetchSessions })).toThrow(
      'fetchSessions must be a boolean'
    )
  }
)

test('a throttled empty full response is refused even for an empty desired set', () => {
  const s = new FetchSession()
  const next = build(s, [], new Map())
  expect(s.handleResponse(next.token, response(42, [], { throttleTime: 100 }))).toBe(false)
  expect(s.sessionId).toBe(0)
  expect(s.epoch).toBe(0)
})
test('a close without an explicit deadline caps its ordinary request budget at two seconds', async () => {
  let remaining
  const sendClose = jest.fn(async (_node, _fields, operation) => {
    remaining = operation.remaining()
  })
  const registry = new FetchSessions({
    sendClose,
    requestTimeout: 5000,
    logger: { debug: jest.fn() },
  })
  const s = registry.forNode(1)
  const next = build(s)
  s.handleResponse(next.token, response())
  await registry.closeAll()
  expect(sendClose).toHaveBeenCalledTimes(1)
  expect(remaining).toBeLessThanOrEqual(2000)
  expect(remaining).toBeGreaterThan(0)
})
test('build refuses a closed session, a missing topic ID and a duplicate partition', () => {
  const s = new FetchSession()
  expect(() => build(s, request(), new Map())).toThrow('Fetch topic ID is missing')
  const [{ topic, partitions }] = request()
  expect(() => build(s, [{ topic, partitions: [partitions[0], partitions[0]] }])).toThrow(
    'Duplicate Fetch partition'
  )
  s.close()
  expect(() => build(s)).toThrow('Fetch session is closed')
})
test('a retired registry refuses new node sessions', () => {
  const registry = new FetchSessions({
    sendClose: jest.fn(),
    requestTimeout: 5000,
    logger: { debug: jest.fn() },
  })
  registry.retireAll()
  expect(() => registry.forNode(1)).toThrow('Fetch sessions are closing')
})
