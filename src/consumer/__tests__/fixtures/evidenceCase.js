/**
 * Runs a live case's body under its cleanup stack and writes the case's evidence row between the
 * two, however the body ended, so the row is written before any cleanup runs. A failed write never
 * replaces the body's own failure; with none, it is thrown, so a lost row is not read as a pass.
 * @param {import('../../../producer/__tests__/fixtures/liveProbe').CleanupStack} cleanup
 * @param {(failure: Error|null) => Promise<Error|null>} write writes the row from what the case
 *   retained; it returns its own failure and never throws
 * @param {() => Promise<void>} body
 * @param {number} cleanupBudgetMs the most the cleanups may take together
 * @returns {Promise<void>}
 * @throws {Error} the body's failure, the failed write, or the failed cleanups
 */
const runWithEvidence = (cleanup, write, body, cleanupBudgetMs) =>
  cleanup.run(
    async () => {
      let failure = null
      let reporting = null
      try {
        await body()
      } catch (error) {
        failure = error
        throw error
      } finally {
        reporting = await write(failure)
      }
      if (reporting) throw reporting
    },
    { budgetMs: cleanupBudgetMs }
  )

/**
 * @param {{topic: string, partition: number, offset: string|number, value: string}} record
 * @returns {string} the complete identity of a handled or expected record, offset included
 */
const positionOf = ({ topic, partition, offset, value }) =>
  `${topic}|${partition}|${offset}|${value}`

module.exports = { positionOf, runWithEvidence }
