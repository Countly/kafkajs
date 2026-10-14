const createAdmin = require('../index')
const { KafkaJSConnectionError } = require('../../errors')
const createTopicsResponse = require('../../protocol/requests/createTopics/v7/response')
const deleteTopicsResponse = require('../../protocol/requests/deleteTopics/v6/response')
const createPartitionsResponse = require('../../protocol/requests/createPartitions/v3/response')
const metadataResponse = require('../../protocol/requests/metadata/v13/response')
const { TOPIC } = require('../../protocol/configResourceTypes')
const { newLogger } = require('testHelpers')

const THROTTLING_QUOTA_EXCEEDED = 89
const UNKNOWN_TOPIC_OR_PARTITION = 3
const NOT_CONTROLLER = 41
// Explicit, so that an attempt count means the same under every run mode: three attempts in all.
const RETRY = { retries: 2, initialRetryTime: 1, maxRetryTime: 1 }

/**
 * What each API's admin call sends and what it rejects with, so one table of cases serves the three.
 * `parse` is the newest decoder's own, so the error is the one a real answer raises.
 */
const APIS = [
  {
    name: 'createTopics',
    method: 'createTopics',
    parse: createTopicsResponse.parse,
    call: (admin, names) =>
      admin.createTopics({ topics: names.map((topic) => ({ topic })), waitForLeaders: false }),
    sent: ({ topics }) => topics.map(({ topic }) => topic),
    refusal: (error) => error.errors[0],
  },
  {
    name: 'deleteTopics',
    method: 'deleteTopics',
    parse: deleteTopicsResponse.parse,
    call: (admin, names) => admin.deleteTopics({ topics: names }),
    sent: ({ topics }) => topics,
    refusal: (error) => error,
  },
  {
    name: 'createPartitions',
    method: 'createPartitions',
    parse: createPartitionsResponse.parse,
    call: (admin, names) =>
      admin.createPartitions({ topicPartitions: names.map((topic) => ({ topic, count: 4 })) }),
    sent: ({ topicPartitions }) => topicPartitions.map(({ topic }) => topic),
    refusal: (error) => error,
  },
]

/**
 * An admin whose controller answers each request with the next of `answers`, the last one for ever.
 * An answer is `[topic, errorCode]` rows run through the API's own `parse`.
 */
const adminAnswering = ({ method, parse }, answers) => {
  const queue = [...answers]
  const broker = {
    [method]: jest.fn(async () => {
      const rows = queue.length > 1 ? queue.shift() : queue[0]
      return parse({ topicErrors: rows.map(([topic, errorCode]) => ({ topic, errorCode })) })
    }),
  }
  const cluster = {
    refreshMetadata: jest.fn(async () => {}),
    findControllerBroker: jest.fn(async () => broker),
    targetTopics: new Set(),
  }
  const admin = createAdmin({ logger: newLogger(), retry: RETRY, cluster })

  return { admin, broker }
}

describe.each(APIS)('Admin > $name > a quota refuses topics', (api) => {
  test('sends again only the topics that were refused, and resolves when they go through', async () => {
    const { admin, broker } = adminAnswering(api, [
      [
        ['a', THROTTLING_QUOTA_EXCEEDED],
        ['b', 0],
      ],
      [['a', 0]],
    ])

    await api.call(admin, ['a', 'b'])

    expect(broker[api.method].mock.calls.map(([request]) => api.sent(request))).toEqual([
      ['a', 'b'],
      ['a'],
    ])
  })

  test('keeps sending only the topics that are still refused', async () => {
    const { admin, broker } = adminAnswering(api, [
      [
        ['a', THROTTLING_QUOTA_EXCEEDED],
        ['b', THROTTLING_QUOTA_EXCEEDED],
        ['c', 0],
      ],
      [
        ['a', 0],
        ['b', THROTTLING_QUOTA_EXCEEDED],
      ],
      [['b', 0]],
    ])

    await api.call(admin, ['a', 'b', 'c'])

    expect(broker[api.method].mock.calls.map(([request]) => api.sent(request))).toEqual([
      ['a', 'b', 'c'],
      ['a', 'b'],
      ['b'],
    ])
  })

  test('rejects once the retry budget is spent, with the quota error as the cause', async () => {
    const { admin, broker } = adminAnswering(api, [[['a', THROTTLING_QUOTA_EXCEEDED]]])

    const error = await api.call(admin, ['a']).catch((e) => e)

    expect(error.name).toBe('KafkaJSNumberOfRetriesExceeded')
    expect(api.refusal(error.cause)).toMatchObject({ type: 'THROTTLING_QUOTA_EXCEEDED' })
    expect(broker[api.method]).toHaveBeenCalledTimes(RETRY.retries + 1)
  })

  test('does not take a connection failure for a refusal', async () => {
    const failure = new KafkaJSConnectionError('Connection error: ECONNRESET')
    const { admin, broker } = adminAnswering(api, [[['a', 0]]])
    broker[api.method].mockRejectedValueOnce(failure)

    await expect(api.call(admin, ['a'])).rejects.toBe(failure)

    expect(broker[api.method]).toHaveBeenCalledTimes(1)
  })

  test('does not send again when another topic failed in the same answer', async () => {
    const { admin, broker } = adminAnswering(api, [
      [
        ['a', THROTTLING_QUOTA_EXCEEDED],
        ['b', UNKNOWN_TOPIC_OR_PARTITION],
      ],
    ])

    await expect(api.call(admin, ['a', 'b'])).rejects.toBeDefined()

    expect(broker[api.method]).toHaveBeenCalledTimes(1)
  })
})

describe('Admin > createTopics > a quota refuses a topic another client created meanwhile', () => {
  test('resolves false when the repeat finds only topics that exist', async () => {
    const api = APIS[0]
    const { admin } = adminAnswering(api, [[['a', THROTTLING_QUOTA_EXCEEDED]], [['a', 36]]])

    await expect(api.call(admin, ['a'])).resolves.toBe(false)
  })
})

/**
 * An admin whose cluster treats a deleted topic as a real one does: `refreshMetadata` asks for the
 * target topics and rejects with what the named Metadata v13 `parse` raises for one that no longer
 * exists. The broker answers each DeleteTopics request with the next of `answers` (`[topic,
 * errorCode]` rows, for the topics that request named, run through the DeleteTopics v6 `parse`) and
 * deletes the topics it answers with 0.
 */
const adminOverDeletingBroker = (answers, targets) => {
  const queue = [...answers]
  const deleted = new Set()
  const broker = {
    deleteTopics: jest.fn(async ({ topics }) => {
      const rows = (queue.length > 1 ? queue.shift() : queue[0]).filter(([topic]) =>
        topics.includes(topic)
      )
      rows.filter(([, errorCode]) => errorCode === 0).forEach(([topic]) => deleted.add(topic))
      return deleteTopicsResponse.parse({
        topicErrors: rows.map(([topic, errorCode]) => ({ topic, errorCode })),
      })
    }),
    describeConfigs: jest.fn(async () => ({ resources: [] })),
  }
  const cluster = {
    targetTopics: new Set(targets),
    refreshMetadata: jest.fn(async () => {
      await metadataResponse.parse({
        errorCode: 0,
        topicMetadata: [...cluster.targetTopics].map((topic) => ({
          topicErrorCode: deleted.has(topic) ? UNKNOWN_TOPIC_OR_PARTITION : 0,
          topic,
          partitionMetadata: [],
        })),
      })
    }),
    findControllerBroker: jest.fn(async () => broker),
  }
  const admin = createAdmin({ logger: newLogger(), retry: RETRY, cluster })

  return { admin, broker, cluster }
}

const sentTopics = (broker) => broker.deleteTopics.mock.calls.map(([{ topics }]) => topics)

describe('Admin > deleteTopics > the broker deletes part of a batch', () => {
  test('a quota refusal leaves the deleted topic out of the refresh, so the refused one is sent again', async () => {
    const { admin, broker, cluster } = adminOverDeletingBroker(
      [
        [
          ['gone', 0],
          ['kept', THROTTLING_QUOTA_EXCEEDED],
        ],
        [['kept', 0]],
      ],
      ['gone']
    )

    await admin.deleteTopics({ topics: ['gone', 'kept'] })

    expect(sentTopics(broker)).toEqual([['gone', 'kept'], ['kept']])
    expect(cluster.targetTopics.size).toBe(0)
  })

  test('a quota refusal that outlives the retry budget keeps the refused topic as a target and drops the deleted one', async () => {
    const { admin, broker, cluster } = adminOverDeletingBroker(
      [
        [
          ['gone', 0],
          ['kept', THROTTLING_QUOTA_EXCEEDED],
        ],
        [['kept', THROTTLING_QUOTA_EXCEEDED]],
      ],
      ['gone', 'kept']
    )

    const error = await admin.deleteTopics({ topics: ['gone', 'kept'] }).catch((e) => e)

    expect(error.cause).toMatchObject({ type: 'THROTTLING_QUOTA_EXCEEDED' })
    expect(sentTopics(broker)).toEqual([['gone', 'kept'], ['kept'], ['kept']])
    expect([...cluster.targetTopics]).toEqual(['kept'])
    await expect(
      admin.describeConfigs({ resources: [{ type: TOPIC, name: 'kept' }] })
    ).resolves.toEqual({ resources: [] })
  })

  test.each([
    ['UNKNOWN_TOPIC_OR_PARTITION', UNKNOWN_TOPIC_OR_PARTITION],
    ['NOT_CONTROLLER', NOT_CONTROLLER],
  ])(
    'a %s for another topic still drops the deleted one from the target topics',
    async (type, errorCode) => {
      const { admin, broker, cluster } = adminOverDeletingBroker(
        [
          [
            ['gone', 0],
            ['other', errorCode],
          ],
        ],
        ['gone', 'other']
      )

      const error = await admin.deleteTopics({ topics: ['gone', 'other'] }).catch((e) => e)

      expect(error.cause).toMatchObject({ type })
      expect(broker.deleteTopics).toHaveBeenCalledTimes(RETRY.retries + 1)
      expect([...cluster.targetTopics]).toEqual(['other'])
    }
  )
})
