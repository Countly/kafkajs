const { isDeepStrictEqual } = require('node:util')
const { slope } = require('../soak/bounds')

const PHASES = ['share-steady', 'share-reset', 'share-members']
const QUIET_SLOPE_BYTES_PER_MINUTE = 98304

/**
 * Independently validates the additive quiet-heap predicate from authenticated original rows.
 * The caller must first authenticate every row's tree, source, run, selector and broker stamps.
 * This does not replace the guard's periodic, resource, schedule, terminal or cleanup predicates.
 * @param {object[]} rows one scenario's original quiet resource rows and resource verdict, in order
 * @returns {{ points: object[], bytesPerMinute: number|null, problems: string[] }} recomputed facts
 */
const verifyShareQuietHeap = (rows) => {
  const problems = []
  const ends = rows.filter(({ kind }) => kind === 'phase-end')
  const points = ends.map(({ phase, snapshot }) => ({
    phase,
    heapAtMs: snapshot?.heapAtMs ?? null,
    heapUsed: snapshot?.heapUsed ?? null,
  }))
  const observations = rows.filter(({ kind }) => kind !== 'resource-verdict')
  const expected = [
    'baseline',
    'phase-end',
    'phase-end',
    'phase-end',
    'teardown',
    'resource-verdict',
  ]
  const valid =
    rows.length === expected.length &&
    rows.every(
      ({ kind, event }, index) => kind === expected[index] && event === 'soak-resources'
    ) &&
    points.length === PHASES.length &&
    points.every(({ phase }, index) => phase === PHASES[index]) &&
    observations.every(
      ({ snapshot }, index) =>
        Number.isSafeInteger(snapshot?.heapUsed) &&
        snapshot.heapUsed >= 0 &&
        Number.isFinite(snapshot?.heapAtMs) &&
        snapshot.heapAtMs >= 0 &&
        (index === 0 || snapshot.heapAtMs > observations[index - 1].snapshot.heapAtMs)
    )
  let bytesPerMinute = null
  if (valid) {
    try {
      bytesPerMinute = slope(points.map(({ heapAtMs, heapUsed }) => [heapAtMs / 60000, heapUsed]))
    } catch {}
  }
  if (
    bytesPerMinute === null ||
    !Number.isFinite(bytesPerMinute) ||
    bytesPerMinute > QUIET_SLOPE_BYTES_PER_MINUTE
  )
    problems.push('phase-end quiet heap slope screen')
  if (!Number.isFinite(bytesPerMinute)) bytesPerMinute = null
  const verdict = rows.find(({ kind }) => kind === 'resource-verdict')
  if (verdict?.ok !== true || !Array.isArray(verdict.violations) || verdict.violations.length)
    problems.push('share resource verdict is absent or failed')
  if (!isDeepStrictEqual(verdict?.measured?.quietSlope, { points, bytesPerMinute }))
    problems.push('quiet slope verdict differs from original observations')
  return { points, bytesPerMinute, problems }
}

module.exports = { verifyShareQuietHeap }
