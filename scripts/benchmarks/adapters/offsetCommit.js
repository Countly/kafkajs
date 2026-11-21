/**
 * Report adapter of the offset-retirement row: the auto-commit consume workload (`eachMessage`,
 * threshold 100) on this tree and on the tree before committed-offset retirement
 * (`BENCH_TREES["before-retirement"]`), interleaved with an A/A copy of each. It is the old and
 * new timing of the commit path, with the throughput rows' controls and sample rule.
 */
const { rowAdapter } = require('./consumerSession')
const { controls, inconclusive, corpus } = require('./consumeThroughput')

module.exports = rowAdapter({ controls, inconclusive, corpus })
