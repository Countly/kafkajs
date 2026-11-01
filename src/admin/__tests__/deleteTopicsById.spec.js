const createAdmin = require('..')
const { KafkaJSInvalidConfigurationError } = require('../../errors')
const { createErrorFromCode } = require('../../protocol/error')
const logger = { namespace: () => logger, debug() {}, info() {}, warn() {}, error() {} }
const first = '00112233-4455-6677-8899-aabbccddeeff'
const second = '10112233-4455-6677-8899-aabbccddeeff'
const fixture = () => {
  const broker = { deleteTopics: jest.fn(async () => {}) }
  const cluster = {
    refreshMetadata: jest.fn(async () => {}),
    findControllerBroker: jest.fn(async () => broker),
    targetTopics: new Set(),
  }
  return {
    broker,
    cluster,
    admin: createAdmin({ cluster, logger, retry: { retries: 1, initialRetryTime: 1, factor: 0 } }),
  }
}
describe('delete topics by id', () => {
  it('normalizes ids once, forwards timeout and resolves undefined', async () => {
    const { admin, broker } = fixture()
    expect(
      await admin.deleteTopics({ topicIds: [first.toUpperCase()], timeout: 1234 })
    ).toBeUndefined()
    expect(broker.deleteTopics).toHaveBeenCalledWith({ topicIds: [first], timeout: 1234 })
  })
  it.each([
    {},
    { topics: ['t'], topicIds: [first] },
    { topicIds: [] },
    { topicIds: [first, first.toUpperCase()] },
    { topicIds: ['00000000-0000-0000-0000-000000000000'] },
    { topicIds: ['bad'] },
    { topicIds: [null] },
  ])('rejects invalid id selection before discovery %p', async (options) => {
    const { admin, cluster } = fixture()
    await expect(admin.deleteTopics(options)).rejects.toBeInstanceOf(
      KafkaJSInvalidConfigurationError
    )
    expect(cluster.refreshMetadata).not.toHaveBeenCalled()
  })
  it('retries only the quota-refused id', async () => {
    const { admin, broker } = fixture()
    broker.deleteTopics.mockRejectedValueOnce(
      Object.assign(createErrorFromCode(89), {
        topicErrors: [{ topic: null, topicId: second, errorCode: 89 }],
      })
    )
    await admin.deleteTopics({ topicIds: [first, second] })
    expect(broker.deleteTopics.mock.calls.map((x) => x[0].topicIds)).toEqual([
      [first, second],
      [second],
    ])
  })
  it('retains failed id identity through the legacy thrown contract', async () => {
    const { admin, broker } = fixture()
    const error = Object.assign(createErrorFromCode(100), {
      topicErrors: [{ topic: null, topicId: first, errorCode: 100 }],
    })
    broker.deleteTopics.mockRejectedValue(error)
    await expect(admin.deleteTopics({ topicIds: [first] })).rejects.toMatchObject({
      topicErrors: [{ topic: first, topicId: first, errorCode: 100 }],
    })
    expect(broker.deleteTopics).toHaveBeenCalledTimes(1)
  })
  it('keeps name deletion arguments and void result', async () => {
    const { admin, broker, cluster } = fixture()
    cluster.targetTopics.add('t')
    expect(await admin.deleteTopics({ topics: ['t'] })).toBeUndefined()
    expect(broker.deleteTopics).toHaveBeenCalledWith({ topics: ['t'], timeout: undefined })
    expect(cluster.targetTopics.has('t')).toBe(false)
  })
})

describe('delete id capabilities', () => {
  it.each([undefined, { minVersion: 1, maxVersion: 5 }])(
    'refuses missing or older key before sending %p',
    async (range) => {
      const Broker = require('../../broker')
      const broker = Object.create(Broker.prototype)
      broker.versions = range ? { 20: range } : {}
      broker.lookupRequest = jest.fn()
      await expect(broker.deleteTopics({ topicIds: [first] })).rejects.toMatchObject({
        name: 'KafkaJSServerDoesNotSupportApiKey',
        apiName: 'DeleteTopics',
        message: expect.stringMatching(/client implements v1-v6, broker supports/),
      })
      expect(broker.lookupRequest).not.toHaveBeenCalled()
    }
  )
  it('forgets only confirmed names still bound to their original ids', async () => {
    const { admin, broker, cluster } = fixture()
    cluster.targetTopics = new Set(['old', 'new'])
    cluster.metadataCache = {
      topicName: (id) => (id === first ? 'old' : 'new'),
      topicId: (name) => (name === 'old' ? first : second),
    }
    broker.deleteTopics.mockRejectedValueOnce(
      Object.assign(createErrorFromCode(89), {
        topicErrors: [{ topic: null, topicId: second, errorCode: 89 }],
      })
    )
    broker.deleteTopics.mockImplementationOnce(async () => {
      expect(cluster.targetTopics.has('old')).toBe(false)
      expect(cluster.targetTopics.has('new')).toBe(true)
    })
    await admin.deleteTopics({ topicIds: [first, second] })
    expect([...cluster.targetTopics]).toEqual([])
  })
})

describe('delete id advertised downgrade', () => {
  it('refuses an unrepresentable option on the wire-negotiated range', async () => {
    const { Kafka } = require('../../..')
    const { createScriptedCluster } = require('../../../testHelpers/scriptedCluster')
    const scripted = createScriptedCluster({
      nodes: [{ id: 0, host: 'node0', port: 9092 }],
      profile: 'storage',
      advertise: { DeleteTopics: [5, 5] },
    })
    const admin = new Kafka(scripted.kafkaOptions({ retry: { retries: 0 } })).admin()
    try {
      await admin.connect()
      await expect(admin.deleteTopics({ topicIds: [first] })).rejects.toMatchObject({
        name: 'KafkaJSServerDoesNotSupportApiKey',
      })
      expect(scripted.requests().filter((r) => r.api === 'DeleteTopics')).toHaveLength(0)
    } finally {
      await admin.disconnect()
      scripted.assertClean()
    }
  })
})

/** Uses the production Broker builder, decoder and parser with independent response bytes.
 * @param {Array<Array<[string, number]>>} replies per-attempt identities and codes
 * @returns {object} public admin, controller and metadata seams
 */
const decodedFixture = (replies) => {
  const Broker = require('../../broker')
  const requests = require('../../protocol/requests').requests
  const controller = Object.create(Broker.prototype)
  controller.versions = { 20: { minVersion: 6, maxVersion: 6 } }
  controller.lookupRequest = jest.fn(() => requests.DeleteTopics.protocol({ version: 6 }))
  const send = Object.getOwnPropertySymbols(Broker.prototype).find(
    (key) => key.description === 'private:Broker:sendRequest'
  )
  controller[send] = jest.fn(async ({ response }) => {
    const rows = replies.shift()
    const bytes = Buffer.concat([
      Buffer.alloc(4),
      Buffer.from([rows.length + 1]),
      ...rows.map(([id, code]) => {
        const errorCode = Buffer.alloc(2)
        errorCode.writeInt16BE(code)
        return Buffer.concat([
          Buffer.from([0]),
          Buffer.from(id.replaceAll('-', ''), 'hex'),
          errorCode,
          Buffer.from([0, 0]),
        ])
      }),
      Buffer.from([0]),
    ])
    return response.parse(await response.decode(bytes))
  })
  const result = fixture()
  result.broker.deleteTopics.mockImplementation((options) => controller.deleteTopics(options))
  result.cluster.targetTopics.add('t')
  result.cluster.metadataCache = { topicName: () => 't', topicId: () => first }
  return result
}

describe('delete id response identity', () => {
  it.each([
    ['missing', []],
    ['foreign', [[second, 0]]],
    [
      'duplicate',
      [
        [first, 0],
        [first, 0],
      ],
    ],
    ['foreign error', [[second, 89]]],
  ])('rejects %s rows before success or error cleanup', async (label, rows) => {
    const { admin, broker, cluster } = decodedFixture([rows])
    await expect(admin.deleteTopics({ topicIds: [first] })).rejects.toBeInstanceOf(
      require('../../errors').KafkaJSInvariantViolation
    )
    expect(broker.deleteTopics).toHaveBeenCalledTimes(1)
    expect(cluster.targetTopics.has('t')).toBe(true)
  })
  it.each([
    ['missing', []],
    ['foreign', [[first, 0]]],
    [
      'duplicate',
      [
        [second, 0],
        [second, 0],
      ],
    ],
  ])('validates the narrowed retry set for %s rows', async (label, rows) => {
    const { admin, broker } = decodedFixture([
      [
        [first, 0],
        [second, 89],
      ],
      rows,
    ])
    await expect(admin.deleteTopics({ topicIds: [first, second] })).rejects.toBeInstanceOf(
      require('../../errors').KafkaJSInvariantViolation
    )
    expect(broker.deleteTopics.mock.calls.map(([options]) => options.topicIds)).toEqual([
      [first, second],
      [second],
    ])
  })
  it('accepts exact narrowed coverage and keeps the void contract', async () => {
    const { admin, broker } = decodedFixture([
      [
        [first, 0],
        [second, 89],
      ],
      [[second, 0]],
    ])
    expect(await admin.deleteTopics({ topicIds: [first, second] })).toBeUndefined()
    expect(broker.deleteTopics.mock.calls.map(([options]) => options.topicIds)).toEqual([
      [first, second],
      [second],
    ])
  })
})

describe('delete id refresh completion', () => {
  it.each([false, true])(
    'never resends confirmed ids after transient refresh failure, quota subset %s',
    async (mixed) => {
      const { admin, broker, cluster } = fixture()
      let refreshed = 0
      const followUp = mixed ? 3 : 2
      cluster.refreshMetadata.mockImplementation(async () => {
        if (++refreshed === followUp) throw createErrorFromCode(41)
      })
      if (mixed)
        broker.deleteTopics.mockRejectedValueOnce(
          Object.assign(createErrorFromCode(89), {
            topicErrors: [{ topic: null, topicId: second, errorCode: 89 }],
          })
        )
      const outcome = await admin
        .deleteTopics({ topicIds: mixed ? [first, second] : [first] })
        .then(
          () => null,
          (error) => error
        )
      expect(outcome).toBeNull()
      expect(broker.deleteTopics.mock.calls.map(([options]) => options.topicIds)).toEqual(
        mixed ? [[first, second], [second]] : [[first]]
      )
      expect(refreshed).toBe(followUp + 1)
    }
  )
  it.each([false, true])(
    'propagates exhausted follow-up failure without mutation replay, quota subset %s',
    async (mixed) => {
      const { admin, broker, cluster } = fixture()
      let refreshed = 0
      const followUp = mixed ? 3 : 2
      const failure = createErrorFromCode(41)
      cluster.refreshMetadata.mockImplementation(async () => {
        if (++refreshed >= followUp) throw failure
      })
      if (mixed)
        broker.deleteTopics.mockRejectedValueOnce(
          Object.assign(createErrorFromCode(89), {
            topicErrors: [{ topic: null, topicId: second, errorCode: 89 }],
          })
        )
      await expect(
        admin.deleteTopics({ topicIds: mixed ? [first, second] : [first] })
      ).rejects.toMatchObject({ cause: failure })
      expect(broker.deleteTopics.mock.calls.map(([options]) => options.topicIds)).toEqual(
        mixed ? [[first, second], [second]] : [[first]]
      )
      expect(refreshed).toBe(followUp + 1)
    }
  )
  it('preserves a recreated target when the deleted id is confirmed', async () => {
    const { admin, broker, cluster } = fixture()
    cluster.targetTopics.add('t')
    let current = first
    cluster.metadataCache = { topicName: () => 't', topicId: () => current }
    broker.deleteTopics.mockImplementation(async () => {
      current = second
    })
    expect(await admin.deleteTopics({ topicIds: [first] })).toBeUndefined()
    expect(cluster.targetTopics.has('t')).toBe(true)
  })
})
