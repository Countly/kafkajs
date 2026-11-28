/**
 * Report adapter of the internal `subscriptionResolve.js` row: five patterns evaluated over
 * 10 000 topics, `isInternal` excluded, run from this checkout against each tree. Every measured
 * run must match the generated expectation of 9 000 topics.
 */
const { rowAdapter, armsWhere } = require('./consumerSession')

const EXPECTED = 9000

/**
 * @param {object} session
 * @returns {import('./consumerSession').Control[]}
 */
const controls = (session) => {
  const bad = armsWhere(session, (r) => r.matches === EXPECTED)
  return [
    {
      arm: '*',
      name: 'match count = generated expectation',
      passed: bad.length === 0,
      detail: bad.length ? `wrong on ${bad.join(', ')}` : `${EXPECTED} matches`,
    },
  ]
}

/** @param {object} r one run @returns {string} its corpus */
const corpus = (r) => `${r.evaluations} evaluations of 10000 topics`

module.exports = rowAdapter({ controls, corpus })
