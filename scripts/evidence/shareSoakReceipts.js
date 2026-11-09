/**
 * Consumes required CI evidence contracts for a predeclared release inventory. Outcomes are
 * platform observations supplied by the authenticated downloader, never fields in a child seal.
 * Every frozen repeat is named in advance; passing retries cannot replace failed attempts.
 * @param {object[]} inventory Required immutable source/run/attempt/profile/repeat tuples.
 * @param {object[]} receipts Authenticated producing, upload, replay and Exit-gate results.
 * @returns {string[]} Missing, failed, mixed or substituted CI evidence obligations.
 */
const receiptProblems = (inventory, receipts) => {
  const problems = []
  if (!Array.isArray(inventory) || inventory.length === 0)
    return ['No predeclared CI evidence inventory']
  const keys = new Set()
  for (const expected of inventory) {
    const key = JSON.stringify(expected)
    if (keys.has(key)) problems.push('Duplicate frozen inventory entry')
    keys.add(key)
    if (
      expected.type !== 'share-soak-ci-v1' ||
      !['short', 'full'].includes(expected.profile) ||
      !expected.repeat ||
      !/^[a-f0-9]{40}$/.test(expected.commit) ||
      !expected.runId ||
      !expected.attempt
    ) {
      problems.push('Invalid required CI evidence contract')
      continue
    }
    const matches = receipts.filter((r) =>
      Object.entries(expected).every(([name, value]) => r[name] === value)
    )
    if (matches.length !== 1) {
      problems.push('Missing unique frozen CI receipt')
      continue
    }
    const receipt = matches[0]
    if (
      ['producer', 'upload', 'replay', 'verdict', 'exitGate'].some(
        (name) => receipt[name] !== 'success'
      ) ||
      receipt.complete !== true ||
      !/^[a-f0-9]{64}$/.test(receipt.contractSha256) ||
      !/^[a-f0-9]{64}$/.test(receipt.semanticSha256) ||
      receipt.semanticSha256 !== receipt.replaySemanticSha256 ||
      !/^sha256:[a-f0-9]{64}$/.test(receipt.artifactDigest) ||
      receipt.artifactDigest !== receipt.downloadDigest
    )
      problems.push(`Incomplete CI receipt: ${expected.profile}/${expected.repeat}`)
  }
  const groups = new Map()
  for (const entry of inventory) {
    const key = JSON.stringify([entry.commit, entry.repeat])
    if (!groups.has(key)) groups.set(key, new Set())
    groups.get(key).add(entry.profile)
  }
  if ([...groups.values()].some((profiles) => profiles.size !== 2))
    problems.push('Both profiles are required for every frozen repeat')
  return problems
}
module.exports = { receiptProblems }
