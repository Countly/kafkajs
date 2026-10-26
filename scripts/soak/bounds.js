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

/** How long into a phase its heap samples start to count, for `evaluatePhases`. */
const PHASE_WARMUP_MS = 60000

/**
 * The fewest heap samples a phase keeps after its warm-up. A 300 s phase sampled every 30 s keeps
 * seven or eight of its nine or ten.
 */
const MIN_PHASE_SAMPLES = 6

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
 * Checks every quiet point, each phase end and the one after the last disconnect, against the
 * baseline.
 * @param {object} run
 * @param {SoakSnapshot} run.baseline
 * @param {Array<{ phase: string, snapshot: SoakSnapshot }>} run.phaseEnds
 * @param {SoakSnapshot} run.teardown
 * @returns {string[]} one line for every bound that broke
 */
const quietPointViolations = ({ baseline, phaseEnds, teardown }) => [
  ...phaseEnds.flatMap(({ phase, snapshot }) =>
    quietViolations(`after ${phase}`, snapshot, baseline)
  ),
  ...quietViolations('after disconnect', teardown, baseline),
]

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
  const violations = quietPointViolations({ baseline, phaseEnds, teardown })

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

/**
 * Checks one phase's heap on its own: after the phase's warm-up, the median of its last three
 * samples over the median of its first three, and the least-squares slope through them.
 * @param {string} phase
 * @param {Array<{ phase: string, phaseMs: number, snapshot: SoakSnapshot }>} samples every sample
 *   of the run
 * @param {number} warmupMs how long into the phase its samples start to count
 * @param {number} minSamples the fewest samples the phase must keep, at least 2
 * @returns {{ reading: { phase: string, heapSamples: number, endpointBytes: number|null,
 *   slopeBytesPerMinute: number|null }, violations: string[] }}
 */
const phaseHeap = (phase, samples, warmupMs, minSamples) => {
  const heap = samples
    .filter(
      (sample) =>
        sample.phase === phase &&
        sample.phaseMs >= warmupMs &&
        sample.snapshot.heapUsed !== undefined
    )
    .map(({ phaseMs, snapshot }) => [phaseMs / MINUTE_MS, snapshot.heapUsed])
  const reading = {
    phase,
    heapSamples: heap.length,
    endpointBytes: null,
    slopeBytesPerMinute: null,
  }
  if (heap.length < minSamples) {
    return {
      reading,
      violations: [`${phase}: ${heap.length} heap samples after warm-up, ${minSamples} are needed`],
    }
  }

  const violations = []
  const heaps = heap.map(([, bytes]) => bytes)
  reading.endpointBytes = median(heaps.slice(-ENDS)) - median(heaps.slice(0, ENDS))
  reading.slopeBytesPerMinute = slope(heap)
  if (reading.endpointBytes > ENDPOINT_BYTES) {
    violations.push(
      `${phase}: the heap grew ${reading.endpointBytes} bytes, more than ${ENDPOINT_BYTES}`
    )
  }
  if (reading.slopeBytesPerMinute > SLOPE_BYTES_PER_MINUTE) {
    violations.push(
      `${phase}: the heap slope is ${Math.round(reading.slopeBytesPerMinute)} bytes a minute, more than ${SLOPE_BYTES_PER_MINUTE}`
    )
  }

  return { reading, violations }
}

/**
 * Checks what a producer-churn run left behind against the bounds. The quiet-point bounds are those
 * of `evaluate`; the heap is judged in two parts, because a phase may hold more than its neighbours
 * by design (the buffer-pressure phase holds a full buffer and its pending timers), so the series
 * of the whole run says nothing a phase's own series does not:
 * - inside each phase, from its warm-up on: growth of at most `ENDPOINT_BYTES` and a slope of at
 *   most `SLOPE_BYTES_PER_MINUTE`, from at least `minSamples` samples;
 * - across the quiet points: what is retained at each later phase end and after the last
 *   disconnect exceeds the first phase end's reading by at most `ENDPOINT_BYTES`.
 * @param {object} run
 * @param {SoakSnapshot} run.baseline taken before any client was built
 * @param {Array<{ phase: string, snapshot: SoakSnapshot }>} run.phaseEnds in the order of the phases
 * @param {Array<{ phase: string, phaseMs: number, snapshot: SoakSnapshot }>} run.samples the
 *   periodic readings, each with the phase it was taken in and how long into it
 * @param {SoakSnapshot} run.teardown taken after the last phase
 * @param {number} [run.warmupMs=PHASE_WARMUP_MS] how long into each phase its samples start to count
 * @param {number} [run.minSamples=MIN_PHASE_SAMPLES] the fewest samples a phase must keep, at
 *   least 2
 * @returns {{ ok: boolean, violations: string[], measured: { phases: Array<{ phase: string,
 *   heapSamples: number, endpointBytes: number|null, slopeBytesPerMinute: number|null }>,
 *   quietPoints: Array<{ when: string, aboveFirstBytes: number }> } }} `violations` names every
 *   bound that broke
 */
const evaluatePhases = ({
  baseline,
  phaseEnds,
  samples,
  teardown,
  warmupMs = PHASE_WARMUP_MS,
  minSamples = MIN_PHASE_SAMPLES,
}) => {
  const violations = quietPointViolations({ baseline, phaseEnds, teardown })
  const measured = { phases: [], quietPoints: [] }

  for (const { phase } of phaseEnds) {
    const checked = phaseHeap(phase, samples, warmupMs, minSamples)
    measured.phases.push(checked.reading)
    violations.push(...checked.violations)
  }

  const quiet = [
    ...phaseEnds.map(({ phase, snapshot }) => [`after ${phase}`, snapshot.heapUsed]),
    ['after disconnect', teardown.heapUsed],
  ]
  const [firstWhen, first] = quiet[0]
  if (first === undefined) violations.push(`${firstWhen}: no heap reading`)
  for (const [when, bytes] of first === undefined ? [] : quiet.slice(1)) {
    if (bytes === undefined) {
      violations.push(`${when}: no heap reading`)
      continue
    }
    const aboveFirstBytes = bytes - first
    measured.quietPoints.push({ when, aboveFirstBytes })
    if (aboveFirstBytes > ENDPOINT_BYTES) {
      violations.push(
        `${when}: ${aboveFirstBytes} bytes retained above ${firstWhen}, more than ${ENDPOINT_BYTES}`
      )
    }
  }

  return { ok: violations.length === 0, violations, measured }
}

module.exports = {
  SAMPLE_MS,
  WARMUP_MS,
  PHASE_WARMUP_MS,
  MIN_PHASE_SAMPLES,
  ENDPOINT_BYTES,
  SLOPE_BYTES_PER_MINUTE,
  median,
  slope,
  evaluate,
  evaluatePhases,
}
