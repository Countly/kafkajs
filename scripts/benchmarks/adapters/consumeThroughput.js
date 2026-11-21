/**
 * Report adapter of the `consumeThroughput.js` rows (eachBatch, eachMessage, autocommit, listener,
 * 1000 partitions): the shared session rules plus the driver's own counts. Every measured run
 * must deliver the whole corpus, and its handler entries must equal the batches (`eachBatch`) or
 * the messages (`eachMessage`). The delivery-gap p99 needs its sample count: at least 1 000 batch
 * gaps for the 32 KiB `eachBatch` arms and 200 000 message gaps for `eachMessage`, else the row
 * is `inconclusive`, as is a run without a finite gap count. The listener arm's hooks are counted
 * by the driver, which fails a run unless each fired once, so a listener run that finished passed
 * them. The 1000-partition row's untimed `consumeCoverage.js` run per tree proves the corpus
 * populates, and the consumer receives data from, every partition; a tree without that proof makes
 * the row `error`.
 */
const { rowAdapter, armsWhere, HEAD } = require('./consumerSession')

/**
 * @param {import('./consumerSession').Row} row
 * @returns {number} the gap samples the row's delivery-gap p99 needs, 0 when it is not judged
 */
const minimumGaps = ({ args }) =>
  args.includes('eachMessage') ? 200000 : args.includes('32768') ? 1000 : 0

/**
 * @param {object} session
 * @param {import('./consumerSession').Row} row
 * @returns {import('./consumerSession').Control[]}
 */
const controls = (session, row) => {
  const failing = (name, test, detail) => {
    const bad = armsWhere(session, test)
    return {
      arm: '*',
      name,
      passed: bad.length === 0,
      detail: bad.length ? `${detail}: ${bad.join(', ')}` : detail,
    }
  }
  const list = [
    failing(
      'delivered messages',
      (r) => r.deliveredMessages === r.messages,
      'delivered messages equal the corpus'
    ),
    failing(
      'handler entries',
      (r) =>
        r.handlerEntries ===
        (r.handler === 'eachMessage' ? r.deliveredMessages : r.deliveredBatches),
      'handler entries equal the delivered batches or messages'
    ),
  ]
  if (row.controls.includes('1000 partitions')) {
    list.push(
      failing('1000 partitions', (r) => r.partitions === 1000, 'every run used 1000 partitions')
    )
  }
  if (row.controls.includes('data on every partition')) {
    for (const [label, tree] of Object.entries(session.trees)) {
      if (tree.error) continue
      const c = session.controls[label] ?? { error: 'no coverage run' }
      list.push({
        arm: label,
        name: 'data on every partition',
        fatal: true,
        passed: c.populatedPartitions === 1000 && c.partitionsWithData === 1000,
        detail:
          c.error ??
          `${c.populatedPartitions} populated and ${c.partitionsWithData} consumed of 1000 partitions`,
      })
    }
  }
  if (row.controls.includes('listener hooks once each')) {
    const listened = Object.values(session.arms).filter(
      (a) => a.config === 'listener-1' && a.tree === HEAD
    )
    list.push({
      arm: '*',
      name: 'listener hooks once each',
      passed: listened.length > 0 && listened.every((a) => a.measured === session.measured),
      detail: 'the driver fails a listener run unless the assigned and revoked hooks fired once',
    })
  }
  return list
}

/**
 * @param {object} session
 * @param {import('./consumerSession').Row} row
 * @returns {string|undefined} which arms have too few delivery-gap samples
 */
const inconclusive = (session, row) => {
  const minimum = minimumGaps(row)
  if (minimum === 0) return undefined
  const short = armsWhere(session, (r) => Number.isFinite(r.gapSamples) && r.gapSamples >= minimum)
  return short.length
    ? `fewer than ${minimum} counted delivery-gap samples on ${short.join(', ')}`
    : undefined
}

/** @param {object} r one run @returns {string} its corpus */
const corpus = (r) => `${r.messages} x ${r.size} B over ${r.partitions} partitions, ${r.handler}`

module.exports = {
  ...rowAdapter({ controls, inconclusive, corpus }),
  controls,
  inconclusive,
  corpus,
  minimumGaps,
}
