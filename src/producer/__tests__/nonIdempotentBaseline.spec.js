const {
  NON_IDEMPOTENT_CASES,
  METADATA_ORDER_CASE,
  captureNonIdempotentRequests,
  captureMetadataOrder,
} = require('../../../testHelpers/compatBaseline/capture')
const capturedRequests = require('../../../testHelpers/compatBaseline/nonIdempotentRequests.json')
const capturedMetadataOrder = require('../../../testHelpers/compatBaseline/nonIdempotentMetadataOrder.json')

/** @param {{ topicName: string, partition: number }} metadata @returns {string} */
const metadataKey = ({ topicName, partition }) => `${topicName}/${partition}`

describe('Producer > idempotent: false, against the captured baseline of the send path', () => {
  test.each(NON_IDEMPOTENT_CASES)(
    '$case: sends the request bytes it sent at capture',
    async (testCase) => {
      const captured = capturedRequests.find((entry) => entry.case === testCase.case)

      expect(captured).toBeDefined()
      expect(await captureNonIdempotentRequests(testCase)).toEqual(captured)
    }
  )

  test('the baseline covers acks -1, 1 and 0, both codecs, one and two topics, and two brokers', () => {
    const inputs = capturedRequests.map(({ input }) => input)

    expect(new Set(inputs.map(({ acks }) => acks))).toEqual(new Set([-1, 1, 0]))
    expect(new Set(inputs.map(({ compression }) => compression))).toEqual(new Set([0, 1]))
    expect(inputs.some(({ topicMessages }) => topicMessages.length === 2)).toBe(true)
    expect(
      capturedRequests.some(
        ({ requests }) => new Set(requests.map(({ nodeId }) => nodeId)).size === 2
      )
    ).toBe(true)
    expect(capturedRequests.map((entry) => entry.case).sort()).toEqual(
      NON_IDEMPOTENT_CASES.map((testCase) => testCase.case).sort()
    )
  })

  test('resolves RecordMetadata in the order it resolved at capture', async () => {
    expect(await captureMetadataOrder(METADATA_ORDER_CASE)).toEqual(capturedMetadataOrder)
  })

  test('that order is grouped by the broker that led each partition, not sorted', () => {
    const order = capturedMetadataOrder.metadata.map(metadataKey)

    expect(order).toEqual(['baseline-topic-a/1', 'baseline-topic-b/0', 'baseline-topic-a/0'])
    expect(order).not.toEqual([...order].sort())
  })
})
