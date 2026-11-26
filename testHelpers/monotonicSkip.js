/**
 * Shows the faked `performance.now()` as `ms` without running any timer: what a timer delivered
 * late, or a paused process, observes. It keeps advancing with the fake timers afterwards, and a
 * later call moves it again. Only under Jest's fake timers: they install a fresh `performance`
 * object, so `jest.useRealTimers()` or another `jest.useFakeTimers()` drops the skip.
 * @param {number} ms the monotonic time to show, in the fake clock's milliseconds (may be fractional)
 * @returns {void}
 */
const skipMonotonicTo = (ms) => {
  const clock = globalThis.performance
  if (clock.now.skipped === undefined) {
    const fake = clock.now
    const now = () => fake() + now.skipped
    now.skipped = 0
    clock.now = now
  }
  clock.now.skipped += ms - clock.now()
}

module.exports = { skipMonotonicTo }
