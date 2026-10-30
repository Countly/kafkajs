const path = require('path')
const { performance } = require('perf_hooks')

/** Creates five pattern buckets over 10000 names, with exactly 1000 internal names.
 * @returns {{topics: RegExp[], metadata: object, expected: number}}
 */
const createWorkload = () => ({
  topics: [/kind-0-/g, /kind-1-/y, /kind-2-/gy, /KIND-3-/i, /kind-4-/],
  metadata: {
    topicMetadata: Array.from({ length: 10000 }, (_, index) => ({
      topic: `prefix-kind-${index % 5}-item-${index}`,
      isInternal: index % 10 === 0,
      topicErrorCode: 0,
    })),
  },
  expected: 9000,
})

/** Verifies exact names as well as the count, independently of the pattern evaluator.
 * @param {object} result snapshot returned by Subscription.evaluate
 * @param {object} workload generated metadata and expected count
 * @returns {void}
 * @throws {Error} when work was dropped, duplicated or includes an internal topic
 */
const verifyResult = (result, workload) => {
  const expected = workload.metadata.topicMetadata
    .filter((_, index) => index % 10 !== 0)
    .map(({ topic }) => topic)
    .sort()
  if (
    result.names.length !== workload.expected ||
    result.names.some((name, index) => name !== expected[index]) ||
    result.topicConfigurations.size !== workload.expected
  ) {
    throw new Error('Subscription evaluation did not return the complete external-topic set')
  }
}

/** Constructs one real evaluator; input creation and verification stay outside timing.
 * @param {Function} Subscription internal implementation under measurement
 * @param {object} workload immutable workload input
 * @returns {object} configured subscription
 */
const createSubscription = (Subscription, workload) => {
  const subscription = new Subscription()
  subscription.add({ topics: workload.topics, policy: { kind: 'earliest' } })
  return subscription
}

/** Runs a single untimed control or a sample of complete evaluations.
 * @param {{tree?: string, control?: boolean, iterations?: number}} [options]
 * @returns {{evaluationMs?: number, matches: number, evaluations: number}}
 * @throws {Error} on invalid iterations, missing implementation or a failed work control
 */
const run = ({
  tree = path.resolve(__dirname, '../..'),
  control = false,
  iterations = 100,
} = {}) => {
  if (!Number.isSafeInteger(iterations) || iterations < 1)
    throw new Error('iterations must be positive')
  const Subscription = require(path.join(tree, 'src/consumer/subscription'))
  const workload = createWorkload()
  const subscription = createSubscription(Subscription, workload)
  verifyResult(subscription.evaluate(workload.metadata), workload)
  if (control) return { matches: workload.expected, evaluations: 1 }
  let result
  const start = performance.now()
  for (let index = 0; index < iterations; index++) result = subscription.evaluate(workload.metadata)
  const evaluationMs = (performance.now() - start) / iterations
  verifyResult(result, workload)
  return { evaluationMs, matches: result.names.length, evaluations: iterations }
}

/** Parses the small CLI and prints one sample; session.js owns rotation, medians and A/A.
 * @param {string[]} args command arguments
 * @returns {void}
 * @throws {Error} on an unknown option or invalid argument
 */
const main = (args) => {
  const options = {}
  for (let index = 0; index < args.length; index++) {
    if (args[index] === '--control') options.control = true
    else if (args[index] === '--tree') options.tree = path.resolve(args[++index])
    else if (args[index] === '--iterations') options.iterations = Number(args[++index])
    else throw new Error(`Unknown option: ${args[index]}`)
  }
  process.stdout.write(`${JSON.stringify(run(options))}\n`)
}

module.exports = { createWorkload, createSubscription, verifyResult, run, main }
if (require.main === module) main(process.argv.slice(2))
