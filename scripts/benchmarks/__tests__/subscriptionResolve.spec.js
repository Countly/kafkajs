const Subscription = require('../../../src/consumer/subscription')
const { createWorkload, createSubscription, verifyResult, run } = require('../subscriptionResolve')

describe('subscription evaluation work controls', () => {
  test('evaluates five patterns over 10000 topics and excludes the 1000 internal topics', () => {
    const workload = createWorkload()
    expect(workload.topics).toHaveLength(5)
    expect(workload.metadata.topicMetadata).toHaveLength(10000)
    const result = createSubscription(Subscription, workload).evaluate(workload.metadata)
    expect(result.names).toHaveLength(9000)
    expect(() => verifyResult(result, workload)).not.toThrow()
    expect(run({ control: true })).toEqual({ matches: 9000, evaluations: 1 })
  })

  test('rejects missing work, internal-topic substitution and an incomplete configuration map', () => {
    const workload = createWorkload()
    const result = createSubscription(Subscription, workload).evaluate(workload.metadata)
    expect(() => verifyResult({ ...result, names: result.names.slice(1) }, workload)).toThrow(
      'complete'
    )
    const names = [...result.names]
    names[0] = 'prefix-kind-0-item-0'
    expect(() => verifyResult({ ...result, names }, workload)).toThrow('complete')
    expect(() => verifyResult({ ...result, topicConfigurations: new Map() }, workload)).toThrow(
      'complete'
    )
  })
})
