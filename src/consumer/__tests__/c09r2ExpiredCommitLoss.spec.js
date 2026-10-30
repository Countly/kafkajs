const OffsetManager = require('../offsetManager')
const GroupOwnership = require('../groupOwnership')
const InstrumentationEventEmitter = require('../../instrumentation/emitter')
const { ConsumerOperation, markCallDeadline } = require('../groupProtocol/interface')
const { KafkaJSProtocolError, KafkaJSDeliveryTimeout } = require('../../errors')
const { createErrorFromCode } = require('../../protocol/error')
const { sentOwnership, isStaleMembershipVerdict } = require('../rebalanceErrorContract')
const { newLogger } = require('testHelpers')

/** @param {number} code @returns {Error} a decoded broker error */
const brokerError = (code) => new KafkaJSProtocolError(createErrorFromCode(code))

const PAYLOAD = {
  groupId: 'g',
  groupGenerationId: 7,
  memberId: 'm',
  topics: [{ topic: 't', partitions: [{ partition: 0, offset: '1' }] }],
}

/**
 * A real OffsetManager and GroupOwnership over one held OffsetCommit.
 * @param {{kind?: string, generation?: number}} [options]
 * @returns {object} the manager, the ownership, the held request and the commit-error log
 */
const setup = ({ kind = 'classic', generation = 7 } = {}) => {
  const state = { generation }
  const ownership = new GroupOwnership({
    isStale: (cause) =>
      isStaleMembershipVerdict({ kind: 'lost', error: cause }, kind, state.generation),
  })
  const held = {}
  const coordinator = {
    isConnected: () => true,
    offsetCommit: jest.fn(
      () =>
        new Promise((resolve, reject) => {
          held.resolve = resolve
          held.reject = reject
        })
    ),
  }
  const lost = []
  const manager = new OffsetManager({
    cluster: { committedOffsets: () => ({ t: { 0: '0' } }), topicId: () => undefined },
    coordinator,
    memberAssignment: { t: [0] },
    autoCommit: true,
    topicConfigurations: { t: {} },
    instrumentationEmitter: new InstrumentationEventEmitter(),
    groupId: 'g',
    generationId: generation,
    memberId: 'm',
    ownership,
    membershipProtocol: kind,
    onCommitError: (error) => lost.push(error),
    logger: newLogger(),
  })
  return { manager, ownership, held, coordinator, lost, state }
}

/** Sends one commit, holds it, expires the caller's guard, then answers with `answer`. */
const expireThenAnswer = async (h, answer) => {
  let expired = null
  const sending = h.manager
    .sendOffsetCommit(PAYLOAD, { guard: () => expired })
    .catch((error) => error)
  await new Promise((resolve) => setImmediate(resolve))
  expect(h.coordinator.offsetCommit).toHaveBeenCalledTimes(1)
  expired = markCallDeadline(new KafkaJSDeliveryTimeout('caller expired'))
  answer(h.held)
  return { expired, result: await sending }
}

describe('an expired operation still learns the original broker ownership loss', () => {
  it('learns a current-token code 25 and returns the caller timeout', async () => {
    const h = setup()
    const cause = brokerError(25)
    const { expired, result } = await expireThenAnswer(h, ({ reject }) => reject(cause))
    expect(result).toBe(expired)
    expect(h.ownership.refusal(h.ownership.token()).cause).toBe(cause)
    expect(sentOwnership(cause)).toBe(h.ownership.token())
    expect(h.lost).toEqual([cause])
  })

  it('learns the same code without any expiry (control)', async () => {
    const h = setup()
    const cause = brokerError(25)
    const sending = h.manager.sendOffsetCommit(PAYLOAD, { guard: () => null }).catch((e) => e)
    await new Promise((resolve) => setImmediate(resolve))
    h.held.reject(cause)
    expect((await sending).cause).toBe(cause)
    expect(h.ownership.refusal(h.ownership.token()).cause).toBe(cause)
    expect(h.lost).toEqual([cause])
  })

  it.each([69, 30, 42, 111, 25, 110])(
    'learns consumer-protocol code %i inside the same token after epoch 7 became 8',
    async (code) => {
      const h = setup({ kind: 'consumer' })
      const cause = brokerError(code)
      const { expired, result } = await expireThenAnswer(h, ({ reject }) => {
        h.state.generation = 8
        reject(cause)
      })
      expect(result).toBe(expired)
      expect(h.ownership.refusal(h.ownership.token()).cause).toBe(cause)
      expect(h.lost).toEqual([cause])
    }
  )

  it('ignores a classic answer about a generation the member no longer holds', async () => {
    const h = setup()
    const { expired, result } = await expireThenAnswer(h, ({ reject }) => {
      h.state.generation = 8
      reject(brokerError(22))
    })
    expect(result).toBe(expired)
    expect(h.ownership.refusal(h.ownership.token())).toBeNull()
  })

  it('does not poison a replacement token', async () => {
    const h = setup()
    const oldToken = h.ownership.token()
    const { expired, result } = await expireThenAnswer(h, ({ reject }) => {
      h.ownership.renew()
      reject(brokerError(25))
    })
    expect(result).toBe(expired)
    expect(h.ownership.token()).not.toBe(oldToken)
    expect(h.ownership.refusal(h.ownership.token())).toBeNull()
    expect(h.lost).toEqual([])
  })

  it('a force-retired operation publishes no loss, commit error or rejoin', async () => {
    const h = setup()
    const operation = new ConsumerOperation()
    const sending = h.manager
      .sendOffsetCommit(PAYLOAD, { guard: operation.guard, fence: operation })
      .catch((error) => error)
    await new Promise((resolve) => setImmediate(resolve))
    operation.retire(new KafkaJSDeliveryTimeout('forced close'))
    h.held.reject(brokerError(25))
    expect(await sending).toBe(operation.error)
    expect(h.ownership.refusal(h.ownership.token())).toBeNull()
    expect(h.lost).toEqual([])
    expect(h.coordinator.offsetCommit).toHaveBeenCalledTimes(1)
    operation.finish()
  })

  it('an unmarked timeout from a guard publishes nothing, like any other retirement', async () => {
    const h = setup()
    let refusal = null
    const sending = h.manager
      .sendOffsetCommit(PAYLOAD, { guard: () => refusal })
      .catch((error) => error)
    await new Promise((resolve) => setImmediate(resolve))
    refusal = new KafkaJSDeliveryTimeout('close deadline')
    h.held.reject(brokerError(25))
    expect(await sending).toBe(refusal)
    expect(h.ownership.refusal(h.ownership.token())).toBeNull()
    expect(h.lost).toEqual([])
  })

  it('a public call whose own deadline expires learns the loss while its run is current', async () => {
    const h = setup()
    const operation = new ConsumerOperation({
      deadlineAt: performance.now() + 20,
      onTimeout: (cause) =>
        markCallDeadline(new KafkaJSDeliveryTimeout('call deadline', { cause })),
    })
    const sending = h.manager
      .sendOffsetCommit(PAYLOAD, { guard: operation.guard, fence: operation })
      .catch((error) => error)
    await new Promise((resolve) => setImmediate(resolve))
    await new Promise((resolve) => setTimeout(resolve, 40))
    const cause = brokerError(25)
    h.held.reject(cause)
    expect(await sending).toBe(operation.error)
    expect(h.ownership.refusal(h.ownership.token()).cause).toBe(cause)
    expect(h.lost).toEqual([cause])
    operation.finish()
  })

  it('sends nothing for an operation that expired before the first await', async () => {
    const h = setup()
    const expired = new KafkaJSDeliveryTimeout('already expired')
    await expect(h.manager.sendOffsetCommit(PAYLOAD, { guard: () => expired })).rejects.toBe(
      expired
    )
    expect(h.coordinator.offsetCommit).not.toHaveBeenCalled()
    expect(h.lost).toEqual([])
  })
})
