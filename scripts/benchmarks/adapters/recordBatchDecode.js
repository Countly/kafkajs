/**
 * Report adapter of the no-change record-batch decode row: `recordBatchDecode.js` arms
 * (uncompressed, gzip, snappy) as the identical driver copy in each tree. Every run must have
 * its codec available and decode records, each arm must decode the same number of records on
 * every tree, and the driver's untimed `--checksum` run per tree must give each arm the same
 * decoded-content checksum (partition, offset, key and value of every record) on every tree; a
 * missing checksum or record count on any tree fails the control.
 */
const { rowAdapter, armsWhere } = require('./consumerSession')

const SHA256 = /^[0-9a-f]{64}$/

/**
 * @param {object} [c] one arm's checksum control
 * @returns {boolean} whether it decoded a positive whole number of records into a SHA-256
 */
const decoded = (c) => Number.isInteger(c?.records) && c.records > 0 && SHA256.test(c.sha256)

/**
 * @param {object} session
 * @param {import('./consumerSession').Row} row
 * @returns {import('./consumerSession').Control[]}
 */
const controls = (session, row) => {
  const unavailable = armsWhere(session, (r) => r.available === 1)
  const byConfig = new Map()
  for (const { config, results } of Object.values(session.arms)) {
    for (const { records } of results) {
      byConfig.set(config, (byConfig.get(config) ?? new Set()).add(records))
    }
  }
  const differ = [...byConfig]
    .filter(([, counts]) => counts.size !== 1 || [...counts].some((n) => !(n > 0)))
    .map(([c]) => c)
  const trees = Object.keys(session.trees).filter((label) => !session.trees[label].error)
  const checksums = row.configs.map(({ name: config }) => {
    const sums = trees.map((label) => [label, session.controls[label]?.checksums?.[config]])
    const proven = sums.every(([, c]) => decoded(c))
    return {
      arm: config,
      name: 'decoded checksum equal across trees',
      passed: proven && new Set(sums.map(([, c]) => c.sha256)).size === 1,
      detail: sums
        .map(([label, c]) => {
          if (decoded(c)) return `${label} ${c.sha256}`
          return `${label} ${session.controls[label]?.error ?? `no checksum: ${JSON.stringify(c ?? null)}`}`
        })
        .join(', '),
    }
  })
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
      detail: differ.length ? `differ or none on ${differ.join(', ')}` : 'same count per arm',
    },
    ...checksums,
  ]
}

/** @param {object} r one run @returns {string} its corpus */
const corpus = (r) => `${r.records} records, ${r.iterations} iterations`

module.exports = rowAdapter({ controls, corpus })
