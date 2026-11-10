'use strict'

const STAGES = Object.freeze([
  'after-second-gc',
  'before-turn-1',
  'after-turn-1',
  'before-turn-2',
  'after-turn-2',
])
const FIELDS = Object.freeze([
  'at',
  'rss',
  'heapUsed',
  'external',
  'arrayBuffers',
  'retained',
  'submittedId',
  'submittedJobId',
  'submittedCodec',
  'settledId',
  'settledJobId',
  'settledCodec',
  'pending',
])

/**
 * Independently checks complete ordered pairs and joins scalar native IDs to the edge marks.
 * @param {object[]} events disk facts @param {object[]} readings original completed readings
 * @returns {string[]} malformed, missing, foreign or over-capacity observations
 */
const validateSettlements = (events, readings) => {
  const problems = []
  const rows = events.filter((event) => event.type === 'settlement')
  const seen = new Set()
  const state = {
    submittedId: 0,
    submittedJobId: 0,
    submittedCodec: 0,
    settledId: 0,
    settledJobId: 0,
    settledCodec: 0,
  }
  const edges = events
    .filter((event) => event.type === 'mark' && event.native)
    .map((event) => ({
      ...event.native,
      at: event.mark?.at,
    }))
  const jobs = events.find((event) => event.type === 'terminal')?.native?.rows
  const submitted = new Set()
  const settled = new Set()
  let edgeAt = -1
  for (const native of edges) {
    const job = jobs?.find((row) => row.id === native.id)
    const ids = native.event === 'submitted' ? submitted : settled
    if (
      !['submitted', 'settled'].includes(native.event) ||
      !['gzip', 'zstd'].includes(native.codec) ||
      !Number.isSafeInteger(native.id) ||
      native.id <= 0 ||
      !Number.isSafeInteger(native.jobId) ||
      native.jobId <= 0 ||
      !Number.isFinite(native.at) ||
      native.at < edgeAt ||
      ids.has(native.id) ||
      (native.event === 'settled' && !submitted.has(native.id)) ||
      (jobs &&
        (!job ||
          job.jobId !== native.jobId ||
          job.codec !== native.codec ||
          native.at < job[native.event]?.at))
    )
      problems.push('native edge identity/order')
    ids.add(native.id)
    edgeAt = native.at
  }
  if (jobs?.some((job) => !submitted.has(job.id) || !settled.has(job.id)))
    problems.push('missing native edge identity')
  let edge = 0
  let lastAt = -1
  for (const [index, row] of rows.entries()) {
    const points = row.observations
    if (
      row.id !== index + 1 ||
      seen.has(row.id) ||
      typeof row.phase !== 'string' ||
      !row.phase ||
      !Array.isArray(row.checkpoints) ||
      row.checkpoints.length < 2 ||
      row.checkpoints.length > 6 ||
      !Array.isArray(points) ||
      points.length !== row.checkpoints.length * STAGES.length ||
      points.length > 30
    ) {
      problems.push('settlement ordering/capacity')
      continue
    }
    seen.add(row.id)
    for (const [i, point] of points.entries()) {
      const round = Math.floor(i / STAGES.length)
      const checkpoint = row.checkpoints[round]
      while (edge < edges.length && edges[edge].at <= point.at) {
        const native = edges[edge++]
        const prefix = native.event === 'submitted' ? 'submitted' : 'settled'
        state[`${prefix}Id`] = native.id
        state[`${prefix}JobId`] = native.jobId
        state[`${prefix}Codec`] = native.codec === 'gzip' ? 1 : 4
      }
      if (
        point.round !== round ||
        point.stage !== STAGES[i % STAGES.length] ||
        FIELDS.some((key) => !Number.isFinite(point[key]) || point[key] < 0) ||
        point.at < lastAt ||
        point.at < checkpoint.admittedAt ||
        point.at > checkpoint.at ||
        point.retained !== point.heapUsed + point.external ||
        Object.keys(state).some((key) => point[key] !== state[key]) ||
        point.pending !== (state.submittedId === state.settledId ? 0 : 1)
      )
        problems.push('settlement chronology/arithmetic/native identity')
      lastAt = point.at
    }
    if (row.readingAt !== row.checkpoints.at(-1)?.at) problems.push('settlement final reading')
  }
  for (const reading of readings) {
    const row = rows.find((row) => row.id === reading?.gc?.diagnosticId)
    if (
      !row ||
      row.readingAt !== reading.at ||
      JSON.stringify(row.checkpoints) !== JSON.stringify(reading.gc.checkpoints)
    )
      problems.push('missing or mismatched settlement journal')
  }
  if (rows.some((row) => !readings.some((reading) => reading?.gc?.diagnosticId === row.id)))
    problems.push('foreign settlement reading')
  return [...new Set(problems)]
}

/**
 * Separates measured allocation across callback turns from the unchanged post-turn readings.
 * Signed differences are temporal evidence, not an ownership census or acceptance correction.
 * @param {object[]} events disk facts @returns {object} every pair and native edge identity
 */
const settlementReport = (events) => {
  const matchedRetained =
    events.find((event) => event.type === 'checkpoint' && event.name === 'M')?.pre.retained ?? null
  return {
    diagnostic: true,
    acceptanceEligible: false,
    matchedRetained,
    pairs: events
      .filter((event) => event.type === 'settlement')
      .flatMap((row) =>
        row.checkpoints.map((reading, round) => {
          const points = row.observations.slice(round * STAGES.length, (round + 1) * STAGES.length)
          const difference = (before, after) =>
            Object.fromEntries(
              ['heapUsed', 'external', 'arrayBuffers', 'retained'].map((key) => [
                key,
                after[key] - before[key],
              ])
            )
          return {
            id: row.id,
            phase: row.phase,
            round,
            afterSecondGc: points[0],
            reading,
            turns: [difference(points[1], points[2]), difference(points[3], points[4])],
            postGcToReading: difference(points[0], reading),
            postGcMinusM: matchedRetained === null ? null : points[0].retained - matchedRetained,
            readingMinusM: matchedRetained === null ? null : reading.retained - matchedRetained,
            nativeEdges: events
              .filter(
                (event) =>
                  event.type === 'mark' &&
                  event.native &&
                  event.mark.at >= points[0].at &&
                  event.mark.at <= reading.at
              )
              .map((event) => ({ at: event.mark.at, ...event.native })),
          }
        })
      ),
    interpretation:
      'Positive post-GC deltas locate net allocation after collection; native edges identify overlap, not causation. Surviving ownership requires stable-ID self-byte and backing-store paths from M/closed-0/closed-5/all-released. Both can coexist; unknown ownership remains open. No subtraction or acceptance verdict.',
  }
}

module.exports = { STAGES, FIELDS, validateSettlements, settlementReport }
