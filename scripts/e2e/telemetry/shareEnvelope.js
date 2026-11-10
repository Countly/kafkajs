/** Work, complete envelope and local cleanup reserve of the configured share oracle. */
const SHARE_ENVELOPE = Object.freeze({ work: 28000, full: 30000, cleanup: 2000 })

/**
 * Runs the share fault/delivery oracle with a separate cleanup reserve, preserving both failures.
 * @param {object} options owner signal, remaining budget, bounded runner, work, cleanup and taint
 * @returns {Promise<*>} the complete oracle's result
 * @throws {Error|AggregateError} admission, work or operational cleanup failures
 */
const runShareEnvelope = async ({ signal, remainingMs, bounded, work, cleanup, taint }) => {
  if (remainingMs < SHARE_ENVELOPE.full) {
    const error = new Error('The remaining budget cannot hold the share subarm')
    taint(error)
    throw error
  }
  return bounded(
    signal,
    SHARE_ENVELOPE.full,
    async (scope) => {
      let result
      const errors = []
      try {
        result = await bounded(scope, SHARE_ENVELOPE.work, work, {
          label: 'The share fault and oracle work',
        })
      } catch (error) {
        errors.push(error)
      }
      try {
        await bounded(scope, SHARE_ENVELOPE.cleanup, cleanup, {
          label: 'The share local cleanup reserve',
        })
      } catch (error) {
        taint(error)
        errors.push(error)
      }
      if (errors.length === 1) throw errors[0]
      if (errors.length) throw new AggregateError(errors, 'The share oracle and cleanup failed')
      return result
    },
    { label: 'The complete share oracle envelope' }
  )
}
module.exports = { SHARE_ENVELOPE, runShareEnvelope }
