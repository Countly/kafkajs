// TEMPORARY DIAGNOSTIC: shows what a hanging scripted admin test waits on. Remove after one CI run.

/**
 * Records every timer and interval created after fake timers are installed, with where it was
 * created, until it fires or is cleared.
 * @returns {{ live: () => object[], restore: () => void }}
 */
const traceTimers = () => {
  const real = {
    setTimeout: global.setTimeout,
    clearTimeout: global.clearTimeout,
    setInterval: global.setInterval,
    clearInterval: global.clearInterval,
  }
  const live = new Map()
  const where = () =>
    new Error().stack
      .split('\n')
      .slice(3, 10)
      .map((line) => line.trim())
      .join(' < ')
  global.setTimeout = (fn, ms, ...args) => {
    const id = real.setTimeout(
      (...values) => {
        live.delete(id)
        fn(...values)
      },
      ms,
      ...args
    )
    live.set(id, { kind: 'timeout', ms, where: where() })
    return id
  }
  global.setInterval = (fn, ms, ...args) => {
    const id = real.setInterval(fn, ms, ...args)
    live.set(id, { kind: 'interval', ms, where: where() })
    return id
  }
  global.clearTimeout = (id) => {
    live.delete(id)
    return real.clearTimeout(id)
  }
  global.clearInterval = (id) => {
    live.delete(id)
    return real.clearInterval(id)
  }
  return {
    live: () => [...live.values()],
    restore: () => Object.assign(global, real),
  }
}

/**
 * @param {object|null} scripted the test's scripted cluster
 * @returns {object} its frames and recorded harness failures
 */
const clusterState = (scripted) =>
  scripted
    ? {
        frames: scripted.requests().map(({ api, node, generation, order, answeredOrder }) => ({
          api,
          node,
          generation,
          order,
          answeredOrder,
        })),
        errors: scripted.errors.map((error) => String(error?.message ?? error)),
      }
    : null

/**
 * Prints one line per snapshot: now, and again after fake time moved on, so a wait that only a
 * timer can end shows up as progress in the second line.
 * @param {{ scripted: object|null, logs: object[], timers: object|null }} state
 * @returns {Promise<void>}
 */
const report = async (state) => {
  const name = expect.getState().currentTestName
  const snapshot = (stage) =>
    console.log(
      'DIAG ' +
        JSON.stringify({
          name,
          stage,
          timerCount: jest.getTimerCount(),
          timers: state.timers?.live(),
          cluster: clusterState(state.scripted),
          logs: state.logs.slice(-40),
        })
    )
  snapshot('at end')
  await jest.advanceTimersByTimeAsync(200000)
  snapshot('after 200 s of fake time')
}

/**
 * @param {object[]} logs where entries go
 * @returns {Function} a logCreator keeping the last 200 client log entries
 */
const logInto =
  (logs) =>
  () =>
  ({ namespace, level, log }) => {
    logs.push({ namespace, level, message: log.message, error: log.error })
    if (logs.length > 200) logs.shift()
  }

module.exports = { traceTimers, report, logInto }
