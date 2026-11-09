const { createHash } = require('node:crypto')

/**
 * Seals the original first-use completion fields, excluding outer transport/tree stamps.
 * @param {object} row Original first-use completion.
 * @returns {string} SHA-256 of the fixed completion projection.
 */
const primingReceiptHash = (row) =>
  createHash('sha256')
    .update(
      JSON.stringify({
        scenario: row.scenario,
        status: row.status,
        tainted: row.tainted,
        schedule: row.schedule,
        profileMs: row.profileMs,
        elapsedProfileMs: row.elapsedProfileMs,
        timing: row.timing,
        cleanup: row.cleanup,
        final: row.final,
        protocolSha256: row.protocolSha256,
      })
    )
    .digest('hex')

/**
 * Checks the sealed first-use completion link, order and original clock before accepting cleanup.
 * Full workload/protocol/resource reconstruction remains the independent evidence validator's job.
 * @param {object[]} rows Original ordered JSONL from one acceptance process.
 * @returns {string[]} Missing, failed, duplicate, substituted or late completion linkage.
 */
const verifySharePrimingLink = (rows) => {
  const select = (event) => rows.filter((r) => r?.event === event)
  const starts = select('priming-start')
  const primes = select('priming-complete')
  const boundaries = select('priming-boundary')
  const scores = select('case-complete')
  if ([starts, primes, boundaries, scores].some((set) => set.length !== 1))
    return ['exactly one priming completion link is required']
  const [start] = starts
  const [prime] = primes
  const [boundary] = boundaries
  const [score] = scores
  const link = score.priming
  const times = [
    start.startedAt,
    prime.timing?.observedAt,
    link?.completedAt,
    link?.boundaryAt,
    score.timing?.stages?.find((s) => s.name === 'setup')?.startedAt,
  ]
  if (
    !(
      rows.indexOf(start) < rows.indexOf(prime) &&
      rows.indexOf(prime) < rows.indexOf(boundary) &&
      rows.indexOf(boundary) < rows.indexOf(score)
    ) ||
    rows.some((r) => r?.event === 'case-failed') ||
    !/^[a-f0-9]{64}$/.test(prime.protocolSha256 ?? '') ||
    prime.scope !== 'priming' ||
    score.scope !== 'scored' ||
    prime.scenario === score.scenario ||
    prime.status !== 'passed' ||
    prime.tainted !== false ||
    link?.status !== 'passed' ||
    link?.scenario !== prime.scenario ||
    link?.passes !== 1 ||
    link?.cycles !== 30 ||
    link?.resets !== 10 ||
    link?.verifiers !== 31 ||
    link?.receiptSha256 !== primingReceiptHash(prime) ||
    JSON.stringify(link) !== JSON.stringify(boundary.priming) ||
    start.totalMs !== 1020000 ||
    start.passes !== 1 ||
    start.cycles !== 30 ||
    start.resets !== 10 ||
    prime.schedule?.slotMs !== 32000 ||
    prime.profileMs !== 960000 ||
    prime.elapsedProfileMs < 960000 ||
    prime.elapsedProfileMs > 970000 ||
    prime.cleanup?.groupsAbsent !== true ||
    prime.cleanup?.topicsAbsent !== true ||
    prime.cleanup?.clock?.origin !== 'hrtime' ||
    !Number.isSafeInteger(prime.cleanup?.completedAt) ||
    prime.cleanup.completedAt > score.cleanup?.completedAt ||
    score.timing?.startedAt !== start.startedAt ||
    prime.timing?.startedAt !== start.startedAt ||
    prime.timing?.workDue !== start.startedAt + 1010000 ||
    times.some((t, i) => !Number.isFinite(t) || (i && t < times[i - 1])) ||
    link?.boundaryAt > start.startedAt + 1020000
  )
    return ['invalid sealed priming completion link or original chronology']
  return []
}

module.exports = { primingReceiptHash, verifySharePrimingLink }
