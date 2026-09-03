'use strict'

/**
 * Gap classification (spec B3). Exactly one primary class per API —
 *   current      fork max == Kafka max
 *   behind:N     fork max is N versions below Kafka max
 *   stub         the api key is known but maps to noImplementedRequestDefinitions
 *   missing      planned, no codec yet
 *   not-planned  deliberately unimplemented
 * — plus two orthogonal signals reported alongside it:
 *   onFloor      fork max == Kafka min: one KIP-896 floor raise from breaking
 *   belowFloor   implemented versions the pinned broker already rejects
 *
 * Two sentinels exist for inputs that must never reach the committed output;
 * `compat:check` fails on both:
 *   ahead:N      fork max > Kafka max at the pinned tag
 *   unknown      a codec directory whose exported `versions` array is empty
 */

/** [0, 1, 2, 5] -> "0-2, 5"; [] -> "none". */
const formatVersions = versions => {
  if (versions === null || versions === undefined || versions.length === 0) return 'none'
  const sorted = versions.slice().sort((a, b) => a - b)
  const parts = []
  let start = sorted[0]
  let previous = sorted[0]
  for (let i = 1; i <= sorted.length; i++) {
    const version = sorted[i]
    if (version !== previous + 1) {
      parts.push(start === previous ? String(start) : `${start}-${previous}`)
      start = version
    }
    previous = version
  }
  return parts.join(', ')
}

/**
 * @param {{ validVersions: {min: number, max: number}|null, forkVersions: number[]|null,
 *           hasCodec: boolean, stub: boolean, planned: string }} input
 * @returns {{ gap: string, onFloor: boolean, belowFloor: number[] }}
 */
const classify = input => {
  const none = { onFloor: false, belowFloor: [] }
  if (input.stub) return Object.assign({ gap: 'stub' }, none)
  if (!input.hasCodec) {
    return Object.assign({ gap: input.planned === 'no' ? 'not-planned' : 'missing' }, none)
  }
  const valid = input.validVersions
  const fork = input.forkVersions
  if (valid === null || fork === null || fork.length === 0) {
    return Object.assign({ gap: 'unknown' }, none)
  }
  const forkMax = Math.max.apply(null, fork)
  const belowFloor = fork.filter(version => version < valid.min).sort((a, b) => a - b)
  const onFloor = forkMax === valid.min && valid.min > 0
  let gap
  if (forkMax === valid.max) gap = 'current'
  else if (forkMax < valid.max) gap = `behind:${valid.max - forkMax}`
  else gap = `ahead:${forkMax - valid.max}`
  return { gap, onFloor, belowFloor }
}

module.exports = { classify, formatVersions }
