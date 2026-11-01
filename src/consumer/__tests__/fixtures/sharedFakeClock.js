const { performance: importedClock } = require('node:perf_hooks')

/**
 * Installs Jest's modern fake timers and points the imported `node:perf_hooks` clock at the faked
 * global one. Production reads one monotonic clock through both names; Jest replaces only the
 * global, so without this a hook's deadline (global) is compared with real worker age (imported).
 * Callers restore with `jest.useRealTimers()` and `jest.restoreAllMocks()`.
 * @returns {void}
 */
const useSharedFakeClock = () => {
  jest.useFakeTimers({ doNotFake: ['setImmediate'] })
  jest.spyOn(importedClock, 'now').mockImplementation(() => performance.now())
}

module.exports = { useSharedFakeClock }
