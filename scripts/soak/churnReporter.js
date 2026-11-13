'use strict'

const MIB = 1024 * 1024
const DIRECTORY = '.tmp/telemetry-churn-reporter'

/**
 * Eight-reopen CI exports reached the retained 2 MiB sink and lost up to 379 rows. The largest
 * measured bound is 2,096,328 saved bytes plus 379 records at 4,096 bytes and a newline each.
 * Those runs abort at the last 350 s fence: allow four clients another ten periodic rows and
 * two lifecycle edge rows each. Reserve 25% above the combined bound and round up to MiB.
 * This budgets the shared broker file, not
 * per-client observer storage; queue, record, segment and acceptance limits remain unchanged.
 */
const SINK_BUDGET = Object.freeze({
  observedBytes: 2096328,
  droppedRows: 379,
  tailRows: 4 * (10 + 2),
  requiredBytes: 2096328 + (379 + 4 * (10 + 2)) * 4097,
  margin: 1.25,
})
const SINK_BYTES = Math.ceil((SINK_BUDGET.requiredBytes * SINK_BUDGET.margin) / MIB) * MIB

module.exports = { DIRECTORY, SINK_BYTES, SINK_BUDGET }

if (require.main === module) {
  const { BoundedReporter } = require('./longWindowReporter')
  new BoundedReporter().prepare()
}
