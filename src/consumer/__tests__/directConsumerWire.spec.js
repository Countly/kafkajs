const { createGroupApiBroker } = require('../../broker/__tests__/fixtures/groupApiHarness')
const { KafkaJSInvariantViolation } = require('../../errors')
const apiKeys = require('../../protocol/requests/apiKeys')
const vector = require('../../protocol/requests/offsetFetch/fixtures/java/offsetFetchResponse-v9-basic.json')
const {
  isStaleMembershipVerdict,
  rebalanceAction,
  rebalanceReason,
  stampGeneration,
  REBALANCE_PATHS,
} = require('../rebalanceErrorContract')
const { createErrorFromCode } = require('../../protocol/error')

// The Java response bytes are shared by these two response schema versions.
describe('Consumer offset response identity', () => {
  let harness
  afterEach(async () => {
    if (harness) await harness.pool.destroy()
    harness = null
  })

  test.each([8, 9])('retains matching group and metadata at version %i', async (version) => {
    harness = createGroupApiBroker({
      versions: { [apiKeys.OffsetFetch]: { minVersion: version, maxVersion: version } },
      respond: () => Buffer.from(vector.hex, 'hex'),
    })
    const response = await harness.broker.offsetFetch({
      groupId: 'test-group',
      topics: [{ topic: 'test-topic', partitions: [{ partition: 0 }] }],
      requireStable: true,
    })
    expect(response.groupId).toBe('test-group')
    expect(response.responses).toEqual([
      {
        topic: 'test-topic',
        partitions: [
          {
            partition: 0,
            offset: '10',
            committedLeaderEpoch: 3,
            metadata: null,
            errorCode: 0,
          },
        ],
      },
    ])
    expect(harness.requests.map(({ apiVersion }) => apiVersion)).toEqual([version])
  })

  test.each([8, 9])(
    'refuses another group at version %i before returning offsets',
    async (version) => {
      harness = createGroupApiBroker({
        versions: { [apiKeys.OffsetFetch]: { minVersion: version, maxVersion: version } },
        respond: () => Buffer.from(vector.hex, 'hex'),
      })
      const error = await harness.broker
        .offsetFetch({
          groupId: 'wanted-group',
          topics: [{ topic: 'test-topic', partitions: [{ partition: 0 }] }],
        })
        .then(
          () => null,
          (failure) => failure
        )
      expect(error).toBeInstanceOf(KafkaJSInvariantViolation)
      expect(error.message).toContain('wanted-group')
      expect(error.message).toContain('test-group')
      expect(harness.requests).toHaveLength(1)
    }
  )

  test.each([8, 9])('preserves matching group errors at version %i', async (version) => {
    const bytes = Buffer.from(vector.hex, 'hex')
    bytes.writeInt16BE(30, bytes.length - 4)
    harness = createGroupApiBroker({
      versions: { [apiKeys.OffsetFetch]: { minVersion: version, maxVersion: version } },
      respond: () => bytes,
    })
    await expect(
      harness.broker.offsetFetch({ groupId: 'test-group', topics: [] })
    ).rejects.toMatchObject({ code: 30 })
    await expect(
      harness.broker.offsetFetch({ groupId: 'wanted-group', topics: [] })
    ).rejects.toBeInstanceOf(KafkaJSInvariantViolation)
  })

  test.each([8, 9])('retains the one-group cardinality check at version %i', async (version) => {
    const response = require(`../../protocol/requests/offsetFetch/v${version}/response`)
    await expect(response.decode(Buffer.from('000000000100', 'hex'))).rejects.toBeInstanceOf(
      KafkaJSInvariantViolation
    )
  })

  test.each(Object.values(REBALANCE_PATHS))(
    'does not classify manual offset refusal as membership loss on %s',
    (path) => {
      for (const code of [14, 15, 16, 22, 25, 27, 30, 42, 69, 82, 110, 111, 113]) {
        const error = createErrorFromCode(code)
        expect(rebalanceAction(error, path, { kind: 'manual' })).toEqual({
          action: null,
          fatal: false,
          clearsMemberId: false,
        })
        expect(rebalanceReason(error, { kind: 'manual' })).toBeNull()
      }
    }
  )

  test('never judges a manual run stale by a sent generation', () => {
    const verdict = { kind: 'lost', error: stampGeneration(createErrorFromCode(22), 7) }
    expect(isStaleMembershipVerdict(verdict, 'manual', -1)).toBe(false)
    expect(isStaleMembershipVerdict(verdict, 'classic', -1)).toBe(true)
  })
})
