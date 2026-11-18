/**
 * Report adapter of the rebalance-storm row: 200 live cooperative rounds on this tree, the heap
 * sampled five times after two collections each. Not an inherited contract and not a session:
 * one run, no A/A copy. The driver exits 1 when its last-third slope passes 32 KB per round; that
 * exit is shown as information, never a status.
 */
const { rowAdapter, armsWhere } = require('./consumerSession')

/**
 * @param {object} session
 * @returns {import('./consumerSession').Control[]}
 */
const controls = (session) => {
  const rounds = armsWhere(session, (r) => r.rounds === 200)
  const samples = armsWhere(session, (r) => Array.isArray(r.samples) && r.samples.length === 5)
  const exits = Object.values(session.arms).flatMap(({ results }) =>
    results.map((r) => `exit ${r.exitCode}, within slope gate ${r.withinSlopeGate}`)
  )
  return [
    { arm: '*', name: '200 cooperative rounds', passed: rounds.length === 0 },
    {
      arm: '*',
      name: 'five retained-heap samples',
      passed: samples.length === 0,
      detail: `${exits.join('; ')} (information)`,
    },
  ]
}

/** @param {object} r one run @returns {string} its corpus */
const corpus = (r) => `${r.rounds} rounds over ${r.partitions} partitions, ${r.groupProtocol}`

module.exports = rowAdapter({ controls, corpus, timeoutMs: 2 * 60 * 60 * 1000 })
