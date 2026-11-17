/**
 * Report adapter of the maxBytes sizing rows: `consumeThroughput.js --partitions 1000
 * --max-bytes <10, 25 or 50 MiB> --rss` under `--expose-gc`, never the fetch-round stubs. Each
 * run reports the sampled peak RSS, the process RSS high water and the heap retained after two
 * collections while the consumer still holds its assignment. The driver fails a run in which a
 * partition received no data, so such a run is an `error` row with that reason, never a number;
 * the adapter checks the coverage the driver reports as well. Sizing is recorded, not judged.
 */
const { rowAdapter, armsWhere } = require('./consumerSession')

/**
 * @param {object} session
 * @returns {import('./consumerSession').Control[]}
 */
const controls = (session) => {
  const bad = armsWhere(session, (r) => r.sizing?.partitionsWithData === r.partitions)
  return [
    {
      arm: '*',
      name: 'every partition received data',
      passed: bad.length === 0,
      detail: bad.length ? `short on ${bad.join(', ')}` : 'partitionsWithData = partitions',
    },
  ]
}

/** @param {object} r one run @returns {string} its corpus */
const corpus = (r) => `${r.messages} x ${r.size} B over ${r.partitions} partitions, ${r.handler}`

module.exports = rowAdapter({ controls, corpus })
