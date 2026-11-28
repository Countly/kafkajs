/**
 * Report adapter of the G6 row: the `stag-500+500x30k` shape (500 members owning a previous round,
 * 500 joining, 30 000 partitions) of the classic `sticky` assigner, one timed worker process per
 * run from this checkout against each tree, one discarded warm-up round and seven measured
 * rounds, every run under `--cpu-prof`. The profiles are archived beside the observations and
 * their self time per function is shown for the optimise-or-accept review. An untimed worker per
 * tree prints the assignment buffers; they must be identical on every tree. Equality with the
 * Java vectors is the assigner's golden-vector unit spec.
 */
const { rowAdapter, HEAD } = require('./consumerSession')

/**
 * @param {object} session
 * @returns {import('./consumerSession').Control[]}
 */
const controls = (session) => {
  const reference = session.controls[HEAD]
  const assignments = Object.entries(session.trees)
    .filter(([, tree]) => !tree.error)
    .map(([label]) => {
      const output = session.controls[label]
      const same = Boolean(reference?.digest) && output?.digest === reference.digest
      return {
        arm: label,
        name: 'assignment identical across trees',
        passed: same,
        detail: output?.error ?? `sha256 ${output?.digest}`,
      }
    })
  const expected = session.warmup + session.measured
  const profiles = Object.entries(session.arms)
    .filter(([, arm]) => arm.errors.length === 0)
    .map(([label]) => ({
      arm: label,
      name: 'CPU profile per run',
      passed: session.profiles?.[label] === expected,
      detail: `${session.profiles?.[label] ?? 0} of ${expected}`,
    }))
  return [...assignments, ...profiles]
}

module.exports = rowAdapter({ controls })
