/**
 * What an operation that was told to stop gets, at most, to wind down before its bound gives up on
 * it. Every phase of a case can spend this once, so the case's join reserve must hold one per phase.
 */
const STOP_GRACE_MS = 500

/**
 * What Jest keeps beyond a case's own budget, so that the case's expiry and its stop grace always
 * fire before Jest could abandon the case.
 */
const CASE_JOIN_MS = 4 * STOP_GRACE_MS

/** A signal that never aborts, for work that no owner cancels. */
const NEVER = new AbortController().signal

/**
 * Whether the live run of one spec file can still vouch for a pass. An operation that was told to
 * stop and ignored it may finish later, and nothing here can stop it: the harness fails the
 * awaiting code at once and issues nothing further itself, but physical containment of the run is
 * the guard's. What this class adds is integrity: once anything may have escaped, the run is
 * tainted and every later case refuses to start, so no pass can come from a run in which earlier
 * work may still be running. Jest gives every test file its own module registry, so the shared
 * `liveRun` is one run per spec file.
 */
class LiveRun {
  /** Starts clean. */
  constructor() {
    this.tainted = false
    /** @type {unknown} the first thing that tainted the run */
    this.cause = undefined
    /** @type {boolean} whether the case that began last passed the gate */
    this.admitted = false
  }

  /**
   * Marks the run as one in which work may still be running.
   * @param {unknown} cause what escaped; only the first is kept
   * @returns {void}
   */
  taint(cause) {
    if (this.tainted) return
    this.tainted = true
    this.cause = cause
  }

  /**
   * The gate every live case passes before it issues anything.
   * @returns {void}
   * @throws {Error} naming the first cause, when the run is tainted
   */
  assertClean() {
    if (!this.tainted) return
    const reason = this.cause?.message ?? String(this.cause)
    throw new Error(
      `An earlier case may have left work running, so no later case in this run can pass: ${reason}`,
      { cause: this.cause }
    )
  }

  /**
   * Begins a case: the gate, then the mark that tells the hooks around the case that it was let
   * in. A refused case is marked not admitted, so no hook issues anything for it.
   * @returns {void}
   * @throws {Error} what `assertClean` throws
   */
  admit() {
    this.admitted = false
    this.assertClean()
    this.admitted = true
  }

  /**
   * For a hook that cleans up after the case that just ended.
   * @returns {boolean} whether that case passed the gate; the mark is cleared for the next case
   */
  takeAdmission() {
    const { admitted } = this
    this.admitted = false
    return admitted
  }

  /**
   * Clears the taint and the mark. For the harness's own tests; a live spec never calls it.
   * @returns {void}
   */
  reset() {
    this.tainted = false
    this.cause = undefined
    this.admitted = false
  }
}

/** The run of this spec file. */
const liveRun = new LiveRun()

/**
 * The one way a live scenario awaits anything: `op` runs under a scope that aborts when `signal`
 * does or when `ms` passes. At either, the scope is aborted, the operation gets `graceMs` to stop,
 * and the call rejects, so the awaiting code never resumes on a late answer and issues nothing
 * further itself. An operation that starts another one checks its scope first. One that ignores
 * its abort may still finish later: whether or not it stopped inside the grace, an abort taints the
 * run (`liveRun`), because the harness cannot tell that nothing was left running. The scope also
 * aborts when `op` settles, so whatever it left pending sees an aborted scope.
 * @template T
 * @param {AbortSignal} signal the owner's; never start under an aborted one
 * @param {number} ms at most how long to wait; `Infinity` when the owner's deadline is the bound
 * @param {(scope: AbortSignal) => T|Promise<T>} op passes `scope` on to whatever it starts
 * @param {object} [options]
 * @param {string} [options.label] names the operation in its time-out message
 * @param {number} [options.graceMs] how long an aborted operation may take to settle
 * @returns {Promise<T>} what `op` returned in time
 * @throws {Error} `op`'s own failure; `<label> did not settle within <ms> ms`; or the reason the
 *   owner aborted with
 */
const bounded = async (
  signal,
  ms,
  op,
  { label = 'The operation', graceMs = STOP_GRACE_MS } = {}
) => {
  signal.throwIfAborted()
  const own = new AbortController()
  const scope = AbortSignal.any([signal, own.signal])
  let onAbort
  const aborted = new Promise((_, reject) => {
    onAbort = () => reject(scope.reason)
    scope.addEventListener('abort', onAbort, { once: true })
  })
  const timer = Number.isFinite(ms)
    ? setTimeout(
        () => own.abort(new Error(`${label} did not settle within ${Math.round(ms)} ms`)),
        ms
      )
    : undefined
  const work = (async () => op(scope))()
  try {
    return await Promise.race([work, aborted])
  } catch (error) {
    if (scope.aborted) {
      liveRun.taint(scope.reason)
      await stops(work, graceMs)
    }
    throw error
  } finally {
    clearTimeout(timer)
    scope.removeEventListener('abort', onAbort)
    own.abort()
  }
}

/**
 * Gives an aborted operation a moment to stop.
 * @param {Promise<unknown>} work
 * @param {number} ms
 * @returns {Promise<void>} once `work` settled, in whatever way, or `ms` passed
 */
const stops = async (work, ms) => {
  let timer
  const gone = new Promise((resolve) => {
    timer = setTimeout(resolve, ms)
  })
  await Promise.race([
    work.then(
      () => {},
      () => {}
    ),
    gone,
  ])
  clearTimeout(timer)
}

/**
 * Waits `ms` under `signal`; the timer goes, and the wait ends, when the signal aborts.
 * @param {AbortSignal} signal
 * @param {number} ms
 * @returns {Promise<void>}
 * @throws {Error} the reason the signal aborted with
 */
const delay = (signal, ms) =>
  bounded(
    signal,
    Infinity,
    (scope) =>
      new Promise((resolve) => {
        const timer = setTimeout(resolve, ms)
        scope.addEventListener(
          'abort',
          () => {
            clearTimeout(timer)
            resolve()
          },
          { once: true }
        )
      })
  )

/**
 * Polls `check` until it answers true. Every check and every pause goes through `bounded`, so none
 * starts once `ms` has passed or `signal` has aborted, and an answer that comes late is dropped.
 * @param {AbortSignal} signal
 * @param {number} ms the budget of the whole poll
 * @param {(scope: AbortSignal) => boolean|Promise<boolean>} check a failure it throws is the
 *   caller's; one that is only "not yet" answers false
 * @param {object} [options]
 * @param {number} [options.delayMs=250] the pause between checks
 * @param {string} [options.label] names the poll in its time-out message
 * @returns {Promise<void>}
 * @throws {Error} `<label> did not settle within <ms> ms`, or what `check` or the owner threw
 */
const pollUntil = (signal, ms, check, { delayMs = 250, label = 'The wait' } = {}) =>
  bounded(
    signal,
    ms,
    async (scope) => {
      while (!(await bounded(scope, Infinity, check))) await delay(scope, delayMs)
    },
    { label }
  )

/**
 * `pollUntil` for a check that throws while its condition does not hold yet. What the check
 * throws counts as "not yet"; the last such failure is kept, so a time-out says why the condition
 * never held and not only that it did not. A failure caused by the poll's own abort is not one.
 * @param {AbortSignal} signal
 * @param {number} ms the budget of the whole poll
 * @param {(scope: AbortSignal) => boolean|Promise<boolean>} check answers true when the condition
 *   holds; false or a throw when it does not yet
 * @param {object} [options] as `pollUntil`
 * @param {number} [options.delayMs=250] the pause between checks
 * @param {string} [options.label] names the poll in its time-out message
 * @returns {Promise<void>}
 * @throws {Error} `<label> did not settle within <ms> ms; last error: <name>: <message>`, with
 *   the last tolerated failure as its `cause`; the plain `pollUntil` error when no check threw
 */
const pollTolerating = async (signal, ms, check, options) => {
  let last
  try {
    await pollUntil(
      signal,
      ms,
      async (scope) => {
        try {
          return await check(scope)
        } catch (error) {
          if (scope.aborted) throw error
          last = error
          return false
        }
      },
      options
    )
  } catch (error) {
    if (last === undefined) throw error
    const detail = `${last?.name ?? 'Error'}: ${String(last?.message ?? last).slice(0, 150)}`
    throw new Error(`${error.message}; last error: ${detail}`, { cause: last })
  }
}

/**
 * The Jest timeout of a live case: its budget plus the join reserve, so that `bounded`'s expiry,
 * which taints the run, always fires before Jest can abandon the case.
 * @param {number} budgetMs the case's budget, as given to `liveCase`
 * @returns {number} ms
 */
const caseTimeout = (budgetMs) => budgetMs + CASE_JOIN_MS

/**
 * Declares one live case: the function a Jest `test` runs, declared as
 * `test(title, liveCase(budgetMs, body), caseTimeout(budgetMs))`. The run's gate comes first, so a
 * tainted run refuses the case before it issues anything, and the case is marked admitted for the
 * hooks that must not run for a refused one (`liveRun.takeAdmission`). `body` then runs under
 * `bounded` with the case's scope. Expiry aborts the scope and taints the run. The scope also
 * aborts whenever the case ends, without tainting solely because it ended, so an outstanding
 * discovery sees an aborted scope. Pass it to `ProducerProbe#capture` and every bounded step.
 * @template {unknown[]} A
 * @template T
 * @param {number} budgetMs the whole case; its Jest timeout is `caseTimeout(budgetMs)`
 * @param {(scope: AbortSignal, ...row: A) => T|Promise<T>} body gets the row of a `test.each`
 *   after the scope
 * @returns {(...row: A) => Promise<T>}
 * @throws {Error} as the returned function: the gate's refusal, `body`'s own failure, or
 *   `The case did not settle within <budgetMs> ms`
 */
const liveCase =
  (budgetMs, body) =>
  async (...row) => {
    liveRun.admit()
    return bounded(NEVER, budgetMs, (scope) => body(scope, ...row), { label: 'The case' })
  }

module.exports = {
  CASE_JOIN_MS,
  LiveRun,
  NEVER,
  STOP_GRACE_MS,
  bounded,
  caseTimeout,
  delay,
  liveCase,
  liveRun,
  pollTolerating,
  pollUntil,
}
