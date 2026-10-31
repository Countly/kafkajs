jest.mock('../../utils/shuffle', () => jest.fn((cells) => [...cells].reverse()))

const shuffle = require('../../utils/shuffle')
const {
  TOPIC_ID,
  apiVersionsBody,
  createFakeBroker,
  decodeFetchRequest,
  fetchResponse,
} = require('../../consumer/__tests__/fixtures/fetchHarness')
const apiKeys = require('../../protocol/requests/apiKeys')

const OLD_ID = '21222324-2526-2728-292a-2b2c2d2e2f30'
const partition = (id) => ({ partition: id, fetchOffset: String(id * 10), maxBytes: 4096 })
const diff = [
  { topic: 'test-topic', partitions: [partition(0), partition(1)] },
  { topic: 'other-topic', partitions: [partition(2)] },
]
const topicIds = new Map([
  ['test-topic', TOPIC_ID],
  ['other-topic', '31323334-3536-3738-393a-3b3c3d3e3f40'],
])

/**
 * @param {number} fetchMaxVersion the highest Fetch version the fake broker advertises
 * @returns {Promise<{broker: object, state: {writes: Buffer[]}}>} a connected broker answering an
 *   empty Fetch
 */
const connected = async (fetchMaxVersion = 18) => {
  const created = createFakeBroker((header) =>
    header.apiKey === apiKeys.ApiVersions
      ? apiVersionsBody(fetchMaxVersion, false)
      : fetchResponse([], { apiVersion: fetchMaxVersion, includeTopic: false })
  )
  await created.broker.connect()
  return created
}

const base = { maxWaitTime: 100, minBytes: 1, maxBytes: 10485760, topics: diff, topicIds }

beforeEach(() => shuffle.mockClear())

describe('Broker#fetch with a Fetch session', () => {
  it('writes a positive epoch diff in its own order with the forgotten recorded ID', async () => {
    const { broker, state } = await connected()

    await broker.fetch({
      ...base,
      sessionId: 42,
      sessionEpoch: 3,
      rackId: 'rack-1',
      forgottenTopics: [{ topic: 'gone', topicId: OLD_ID, partitions: [7, 5] }],
    })

    expect(shuffle).not.toHaveBeenCalled()
    const wire = decodeFetchRequest(state.writes)
    expect(wire).toMatchObject({ sessionId: 42, sessionEpoch: 3, rackId: 'rack-1' })
    expect(wire.topics.map(({ topicId }) => topicId)).toEqual([
      TOPIC_ID,
      '31323334-3536-3738-393a-3b3c3d3e3f40',
    ])
    expect(wire.topics.map(({ partitions }) => partitions.map((p) => p.partition))).toEqual([
      [0, 1],
      [2],
    ])
    expect(wire.forgottenTopics).toEqual([{ topic: null, topicId: OLD_ID, partitions: [7, 5] }])
  })

  it.each([0, -1, undefined])('shuffles a request of epoch %p exactly once', async (epoch) => {
    const { broker, state } = await connected()

    await broker.fetch({ ...base, sessionEpoch: epoch })

    expect(shuffle).toHaveBeenCalledTimes(1)
    expect(shuffle.mock.calls[0][0].map(({ partition: { partition: id } }) => id)).toEqual([
      0, 1, 2,
    ])
    const wire = decodeFetchRequest(state.writes)
    expect(wire.sessionId).toBe(0)
    expect(wire.sessionEpoch).toBe(epoch ?? -1)
    expect(wire.topics.map(({ partitions }) => partitions.map((p) => p.partition))).toEqual([
      [2],
      [1, 0],
    ])
  })

  it('writes an empty close request with an empty ID mapping at the negotiated ID version', async () => {
    const { broker, state } = await connected()

    await broker.fetch({
      maxWaitTime: 0,
      minBytes: 0,
      maxBytes: 10485760,
      topics: [],
      topicIds: new Map(),
      sessionId: 42,
      sessionEpoch: -1,
      forgottenTopics: [],
    })

    expect(decodeFetchRequest(state.writes)).toMatchObject({
      apiVersion: 18,
      sessionId: 42,
      sessionEpoch: -1,
      topics: [],
      forgottenTopics: [],
    })
  })

  it('hands the guard to the final sender', async () => {
    const { broker } = await connected()
    const send = jest.spyOn(broker.connectionPool, 'send')
    const guard = () => null

    await broker.fetch({ ...base, sessionId: 42, sessionEpoch: 1, guard })

    const [, , options] = send.mock.calls.find(
      ([protocolRequest]) => protocolRequest.request.apiKey === apiKeys.Fetch
    )
    expect(options).toEqual({ guard })
  })

  it('keeps below-v7 requests free of session fields', async () => {
    const plain = await connected(6)
    const tracked = await connected(6)

    await plain.broker.fetch({ ...base, topicIds: undefined })
    await tracked.broker.fetch({
      ...base,
      topicIds: undefined,
      sessionId: 42,
      sessionEpoch: 0,
      forgottenTopics: [{ topic: 'gone', partitions: [1] }],
    })

    const fetchFrame = ({ state }) => state.writes[state.writes.length - 1]
    expect(fetchFrame(tracked)).toEqual(fetchFrame(plain))
  })
})
