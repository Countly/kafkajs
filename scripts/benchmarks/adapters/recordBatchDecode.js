/**
 * Report adapter of the no-change record-batch decode row: `recordBatchDecode.js` arms
 * (uncompressed, gzip, snappy) as the identical driver copy in each tree. Every run must have
 * its codec available, and each arm must decode the same number of records on every tree.
 */
const { rowAdapter, armsWhere } = require('./consumerSession')

/**
 * @param {object} session
 * @returns {import('./consumerSession').Control[]}
 */
const controls = (session) => {
  const unavailable = armsWhere(session, (r) => r.available === 1)
  const byConfig = new Map()
  for (const { config, results } of Object.values(session.arms)) {
    for (const { records } of results) {
      byConfig.set(config, (byConfig.get(config) ?? new Set()).add(records))
    }
  }
  const differ = [...byConfig].filter(([, counts]) => counts.size !== 1).map(([c]) => c)
  return [
    {
      arm: '*',
      name: 'codec available',
      passed: unavailable.length === 0,
      detail: unavailable.length ? `unavailable on ${unavailable.join(', ')}` : 'every arm',
    },
    {
      arm: '*',
      name: 'decoded records equal across trees',
      passed: byConfig.size > 0 && differ.length === 0,
      detail: differ.length ? `differ on ${differ.join(', ')}` : 'same count per arm',
    },
  ]
}

/** @param {object} r one run @returns {string} its corpus */
const corpus = (r) => `${r.records} records, ${r.iterations} iterations`

module.exports = rowAdapter({ controls, corpus })
