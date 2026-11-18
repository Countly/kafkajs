/**
 * Report adapter of the internal `fetchRound.js` row: one fetch round on stubs at 100, 1 000 and
 * 5 000 partitions over three nodes, run from this checkout against each tree. Every measured
 * round must have made one metadata lookup per node (the topics times the nodes) and built one
 * batch per partition. A round whose no-GC window saw a collection fails in the driver and is an
 * `error` row with that reason. This row is never sizing evidence.
 */
const { rowAdapter, armsWhere } = require('./consumerSession')

/**
 * @param {object} session
 * @param {import('./consumerSession').Row} row
 * @returns {import('./consumerSession').Control[]}
 */
const controls = (session, row) => {
  const partitionsOf = (label) =>
    Number(row.configs.find(({ name }) => name === session.arms[label].config).args[1])
  const lookups = armsWhere(session, (r) => r.lookups === 3)
  const batches = armsWhere(session, (r, label) => r.batches === partitionsOf(label))
  const windows = armsWhere(
    session,
    (r) => Number.isInteger(r.windowAttempts) && r.windowAttempts >= 1
  )
  return [
    {
      arm: '*',
      name: 'no-GC window',
      passed: windows.length === 0,
      detail: windows.length
        ? `no clean window reported on ${windows.join(', ')}`
        : 'every measured round had a window without a collection',
    },
    {
      arm: '*',
      name: 'metadata lookups = topics x nodes',
      passed: lookups.length === 0,
      detail: lookups.length ? `wrong on ${lookups.join(', ')}` : '1 topic x 3 nodes',
    },
    {
      arm: '*',
      name: 'batches built = partitions',
      passed: batches.length === 0,
      detail: batches.length ? `wrong on ${batches.join(', ')}` : 'one batch per partition',
    },
  ]
}

/** @param {object} r one run @returns {string} its corpus */
const corpus = (r) => `${r.batches} partitions over ${r.batchesByNode?.length} nodes`

module.exports = rowAdapter({ controls, corpus })
