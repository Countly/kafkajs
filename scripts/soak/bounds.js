/**
 * What a soak run is held to, and the arithmetic that holds it. Pure: it reads the records a run
 * wrote and says which bound broke, so every bound can be shown to catch what it is for.
 *
 * The bounds are those of a short-run screen. The first run records what it measures and a later
 * run may only tighten them; a positive slope inside its bound can still be linear growth, so a
 * pass here is never proof of bounded long-run memory.
 */

/** How often a sample is taken. */
const SAMPLE_MS = 30000

/** How long a run is left to settle before the heap series counts. */
const WARMUP_MS = 180000

/** The most the heap may have grown, the median of the last three samples over the first three. */
const ENDPOINT_BYTES = 2 * 1024 * 1024

/** The steepest least-squares slope of the post-warm-up heap, per minute. */
const SLOPE_BYTES_PER_MINUTE = 256 * 1024

/** How many samples the endpoint check takes from each end of the series. */
const ENDS = 3

const MINUTE_MS = 60000

/** The resource types whose count at every quiet point has to be the baseline's. */
const QUIET_RESOURCES = ['TCPSocketWrap', 'Timeout']

/**
 * What the resource probe reports, as a soak run stores it.
 * @typedef {object} SoakSnapshot
 * @property {Record<string, number>} listeners listeners on each tracked emitter
 * @property {{ created: number, destroyed: number, open: number }} sockets
 * @property {number} queueDepth requests pending plus in flight
 * @property {number} ledgerSize
 * @property {number} obligationSize
 * @property {Record<string, number>} activeResources event-loop resources by type
 * @property {number} [heapUsed] bytes after two collections
 */

/**
 * @param {number[]} values
 * @returns {number} the middle value; the upper middle of an even count
 */
const median = (values) => [...values].sort((a, b) => a - b)[Math.floor(values.length / 2)]

/**
 * @param {Array<[number, number]>} points `[x, y]`
 * @returns {number} the slope of the least-squares line through them, in y per x
 * @throws {Error} unless the points have at least two different x
 */
const slope = (points) => {
  const n = points.length
  const meanX = points.reduce((total, [x]) => total + x, 0) / n
  const meanY = points.reduce((total, [, y]) => total + y, 0) / n
  const spread = points.reduce((total, [x]) => total + (x - meanX) ** 2, 0)
  if (!(spread > 0)) {
    throw new Error('a slope needs at least two different x')
  }

  return points.reduce((total, [x, y]) => total + (x - meanX) * (y - meanY), 0) / spread
}

/**
 * @param {Record<string, number>|undefined} counts
 * @param {string} type
 * @returns {number} how many of the type, 0 when none
 */
const countOf = (counts, type) => (counts && counts[type]) || 0

/**
 * Checks one quiet point, when the clients are disconnected, against the baseline: the same bounds
 * at the end of every phase and after the last disconnect.
 * @param {string} when how a violation names the point, `after <phase>` or `after disconnect`
 * @param {SoakSnapshot} snapshot
 * @param {SoakSnapshot} baseline
 * @returns {string[]} one line for every bound that broke
 */
const quietViolations = (when, snapshot, baseline) => {
  const violations = []

  for (const [name, count] of Object.entries(baseline.listeners)) {
    if (snapshot.listeners[name] !== count) {
      violations.push(
        `${when}: ${snapshot.listeners[name]} listeners on ${name}, ${count} at the start`
      )
    }
  }
  for (const gauge of ['queueDepth', 'ledgerSize', 'obligationSize']) {
    if (snapshot[gauge] !== 0) {
      violations.push(`${when}: ${gauge} is ${snapshot[gauge]}, not 0`)
    }
  }
  if (snapshot.sockets.open !== 0) {
    violations.push(`${when}: ${snapshot.sockets.open} sockets still open`)
  }
  for (const type of QUIET_RESOURCES) {
    const now = countOf(snapshot.activeResources, type)
    const before = countOf(baseline.activeResources, type)
    if (now !== before) {
      violations.push(`${when}: ${now} ${type}, ${before} at the start`)
    }
  }

  return violations
}

/**
 * Checks what a run left behind against the bounds.
 * @param {object} run
 * @param {SoakSnapshot} run.baseline taken before any client was built
 * @param {Array<{ phase: string, snapshot: SoakSnapshot }>} run.phaseEnds taken when a phase's
 *   clients are disconnected and the run is quiet
 * @param {Array<{ atMs: number, snapshot: SoakSnapshot }>} run.samples the periodic readings
 * @param {SoakSnapshot} run.teardown taken after the last phase
 * @param {number} [run.warmupMs=WARMUP_MS]
 * @returns {{ ok: boolean, violations: string[], measured: { heapSamples: number,
 *   endpointBytes: number|null, slopeBytesPerMinute: number|null } }} `violations` names every
 *   bound that broke
 */
const evaluate = ({ baseline, phaseEnds, samples, teardown, warmupMs = WARMUP_MS }) => {
  const violations = [
    ...phaseEnds.flatMap(({ phase, snapshot }) =>
      quietViolations(`after ${phase}`, snapshot, baseline)
    ),
    ...quietViolations('after disconnect', teardown, baseline),
  ]

  const heap = samples
    .filter(({ atMs, snapshot }) => atMs >= warmupMs && snapshot.heapUsed !== undefined)
    .map(({ atMs, snapshot }) => [atMs / MINUTE_MS, snapshot.heapUsed])
  const measured = { heapSamples: heap.length, endpointBytes: null, slopeBytesPerMinute: null }

  if (heap.length < 2 * ENDS) {
    violations.push(`${heap.length} heap samples after warm-up, ${2 * ENDS} are needed`)
  } else {
    const heaps = heap.map(([, bytes]) => bytes)
    measured.endpointBytes = median(heaps.slice(-ENDS)) - median(heaps.slice(0, ENDS))
    measured.slopeBytesPerMinute = slope(heap)
    if (measured.endpointBytes > ENDPOINT_BYTES) {
      violations.push(`the heap grew ${measured.endpointBytes} bytes, more than ${ENDPOINT_BYTES}`)
    }
    if (measured.slopeBytesPerMinute > SLOPE_BYTES_PER_MINUTE) {
      violations.push(
        `the heap slope is ${Math.round(measured.slopeBytesPerMinute)} bytes a minute, more than ${SLOPE_BYTES_PER_MINUTE}`
      )
    }
  }

  return { ok: violations.length === 0, violations, measured }
}

module.exports = {
  SAMPLE_MS,
  WARMUP_MS,
  ENDPOINT_BYTES,
  SLOPE_BYTES_PER_MINUTE,
  median,
  slope,
  evaluate,
}
