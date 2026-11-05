const createAdmin = require('../index')
const apiKeys = require('../../protocol/requests/apiKeys')
const { KafkaJSDeleteGroupsError, KafkaJSProtocolError, KafkaJSTimeout } = require('../../errors')
const { createErrorFromCode } = require('../../protocol/error')
const { createGroupApiBroker } = require('../../broker/__tests__/fixtures/groupApiHarness')
const { newLogger } = require('testHelpers')

const response = (version, caseId) =>
  require(
    `../../protocol/requests/deleteGroups/fixtures/java/deleteGroupsResponse-v${version}-${caseId}.json`
  )
const request = (version, caseId) =>
  require(
    `../../protocol/requests/deleteGroups/fixtures/java/deleteGroupsRequest-v${version}-${caseId}.json`
  )

/** @param {string} text @returns {string} a COMPACT_STRING as hex: UVARINT length + 1, then the bytes */
const compact = (text) =>
  (Buffer.byteLength(text) + 1).toString(16).padStart(2, '0') + Buffer.from(text).toString('hex')

describe('Admin > deleteGroups > the per-group message of DeleteGroups v3', () => {
  let harness

  /**
   * An admin over one real broker on a fake socket that offers DeleteGroups up to `maxVersion` and
   * answers every request with `hex`.
   */
  const adminOver = (maxVersion, hex, retry = { retries: 0 }) => {
    harness = createGroupApiBroker({
      versions: { [apiKeys.DeleteGroups]: { minVersion: 0, maxVersion } },
      respond: () => Buffer.from(hex, 'hex'),
    })

    return createAdmin({
      logger: newLogger(),
      retry,
      cluster: {
        refreshMetadata: async () => {},
        findGroupCoordinator: async () => harness.broker,
        targetTopics: new Set(),
      },
    })
  }

  afterEach(async () => {
    if (harness) {
      await harness.pool.destroy()
      harness = null
    }
  })

  test('mixed deletion errors retain their own broker messages', async () => {
    const groupIds = ['test-group', 'other-group', 'third-group', 'fourth-group']
    const admin = adminOver(3, response(3, 'mixedOutcomes').hex, {
      retries: 3,
      initialRetryTime: 1,
      factor: 0,
    })

    const failure = await admin.deleteGroups(groupIds).catch((error) => error)

    expect(failure).toBeInstanceOf(KafkaJSDeleteGroupsError)
    expect(failure.groups).toEqual([
      {
        groupId: 'other-group',
        errorCode: 134,
        error: expect.objectContaining({ type: 'GROUP_DELETION_FAILED', code: 134 }),
        errorMessage: null,
      },
      {
        groupId: 'third-group',
        errorCode: 134,
        error: expect.objectContaining({ type: 'GROUP_DELETION_FAILED', code: 134 }),
        errorMessage: '',
      },
      {
        groupId: 'fourth-group',
        errorCode: 134,
        error: expect.objectContaining({ type: 'GROUP_DELETION_FAILED', code: 134 }),
        errorMessage: 'cleanup failed',
      },
    ])
    expect(failure.groups.map(({ groupId }) => groupId)).not.toContain('test-group')
    expect(harness.requests).toHaveLength(1)
    expect(harness.requests[0].apiVersion).toEqual(3)
    expect(harness.requests[0].body.toString('hex')).toEqual(
      `05${groupIds.map(compact).join('')}00`
    )
  })

  test('a deletion that succeeds resolves each group with its own message, null when the broker sent none', async () => {
    const admin = adminOver(3, response(3, 'ok').hex)

    await expect(admin.deleteGroups(request(3, 'basic').fields.groupIds)).resolves.toEqual([
      { groupId: 'test-group', errorCode: 0, errorMessage: null },
      { groupId: 'other-group', errorCode: 0, errorMessage: null },
    ])
    expect(harness.requests).toHaveLength(1)
    expect(harness.requests[0].apiVersion).toEqual(3)
    expect(harness.requests[0].body.toString('hex')).toEqual(request(3, 'basic').hex)
  })

  test('a broker that offers up to v2 is asked at v2 and its groups carry no errorMessage', async () => {
    const admin = adminOver(2, response(2, 'error').hex)

    const failure = await admin.deleteGroups(request(2, 'basic').fields.groupIds).catch((e) => e)

    expect(failure).toBeInstanceOf(KafkaJSDeleteGroupsError)
    expect(failure.groups).toEqual([
      { groupId: 'test-group', errorCode: 68, error: expect.objectContaining({ code: 68 }) },
      { groupId: 'other-group', errorCode: 69, error: expect.objectContaining({ code: 69 }) },
    ])
    for (const group of failure.groups) expect(group).not.toHaveProperty('errorMessage')
    expect(harness.requests.map(({ apiVersion }) => apiVersion)).toEqual([2])
    expect(harness.requests[0].body.toString('hex')).toEqual(request(2, 'basic').hex)
  })

  test('successful deletion is not resent after a coordinator retry', async () => {
    const sent = []
    const brokerFor = (nodeId) => ({
      nodeId,
      deleteGroups: jest.fn(async (groupIds) => {
        sent.push(...groupIds)
        return {
          results: groupIds.map((groupId) => ({ groupId, errorCode: 0, errorMessage: null })),
        }
      }),
    })
    const [first, second] = [brokerFor(1), brokerFor(2)]
    const findGroupCoordinator = jest
      .fn()
      .mockImplementationOnce(async () => first)
      .mockImplementationOnce(async () => {
        throw new KafkaJSProtocolError(createErrorFromCode(16))
      })
      .mockImplementationOnce(async () => first)
      .mockImplementationOnce(async () => second)
    const admin = createAdmin({
      logger: newLogger(),
      retry: { retries: 3, initialRetryTime: 1, factor: 0 },
      cluster: { refreshMetadata: jest.fn(), findGroupCoordinator, targetTopics: new Set() },
    })

    await expect(admin.deleteGroups(['one', 'two'])).resolves.toEqual([
      { groupId: 'one', errorCode: 0, errorMessage: null },
      { groupId: 'two', errorCode: 0, errorMessage: null },
    ])

    expect(findGroupCoordinator).toHaveBeenCalledTimes(4)
    expect(first.deleteGroups.mock.calls).toEqual([[['one']]])
    expect(second.deleteGroups.mock.calls).toEqual([[['two']]])
    expect(sent).toEqual(['one', 'two'])
  })
})

describe('Admin > deleteGroups > a deadline that has passed', () => {
  const held = () => {
    let resolve
    const promise = new Promise((yes) => {
      resolve = yes
    })
    return { promise, resolve }
  }
  const logger = { namespace: () => logger, debug() {}, info() {}, warn() {}, error() {} }
  const flush = async () => {
    for (let n = 0; n < 30; n++) await Promise.resolve()
  }

  beforeEach(() => jest.useFakeTimers())
  afterEach(() => {
    expect(jest.getTimerCount()).toBe(0)
    jest.useRealTimers()
  })

  test.each([
    ['routing', 'findGroupCoordinator'],
    ['response', 'deleteGroups'],
  ])(
    'expired deletion cannot publish a late result or resend, held at its %s',
    async (_stage, heldCall) => {
      const gate = held()
      const answer = { results: [{ groupId: 'g', errorCode: 0, errorMessage: null }] }
      const broker = {
        nodeId: 1,
        deleteGroups: jest.fn(() => (heldCall === 'deleteGroups' ? gate.promise : answer)),
      }
      const cluster = {
        refreshMetadata: jest.fn(async () => {}),
        findGroupCoordinator: jest.fn(() =>
          heldCall === 'findGroupCoordinator' ? gate.promise : broker
        ),
        targetTopics: new Set(),
      }
      const admin = createAdmin({
        logger,
        cluster,
        apiTimeout: 10,
        retry: { retries: 3, initialRetryTime: 1, factor: 0 },
      })
      const outcome = {}
      admin.deleteGroups(['g']).then(
        (value) => Object.assign(outcome, { settled: true, value }),
        (error) => Object.assign(outcome, { settled: true, error })
      )

      await jest.advanceTimersByTimeAsync(9)
      expect(outcome.settled).toBeUndefined()
      await jest.advanceTimersByTimeAsync(1)
      expect(outcome.error).toBeInstanceOf(KafkaJSTimeout)
      expect(outcome.error.message).toContain('deleteGroups')
      expect(outcome.error.message).toContain('10 ms')

      const before = {
        refresh: cluster.refreshMetadata.mock.calls.length,
        route: cluster.findGroupCoordinator.mock.calls.length,
        delete: broker.deleteGroups.mock.calls.length,
      }
      gate.resolve(heldCall === 'findGroupCoordinator' ? broker : answer)
      await jest.advanceTimersByTimeAsync(100)
      await flush()

      expect(outcome.value).toBeUndefined()
      expect(outcome.error).toBeInstanceOf(KafkaJSTimeout)
      expect({
        refresh: cluster.refreshMetadata.mock.calls.length,
        route: cluster.findGroupCoordinator.mock.calls.length,
        delete: broker.deleteGroups.mock.calls.length,
      }).toEqual(before)
      expect(before.delete).toEqual(heldCall === 'findGroupCoordinator' ? 0 : 1)
    }
  )
})
