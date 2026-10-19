const { parentPort, workerData } = require('worker_threads')

/**
 * The thread `runInWorker` starts: runs the handler on each case in order and reports every
 * result at once. The index of the case being run is written to the shared `progress` before the
 * case starts, so a thread that has to be terminated says which one it was on.
 */
const { handler, cases, progress } = workerData
const run = require(handler)

;(async () => {
  const results = []
  for (let index = 0; index < cases.length; index++) {
    Atomics.store(progress, 0, index)
    results.push(await run(cases[index]))
  }
  parentPort.postMessage(results)
})()
