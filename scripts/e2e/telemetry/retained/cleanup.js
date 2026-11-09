'use strict'

/**
 * Runs independent cleanup even after capture, serialization or decoding fails.
 * @param {() => Promise<void>} work primary workload
 * @param {Array<{name:string,run:() => Promise<void>}>} actions joined cleanup actions
 * @param {(stage:string,error:Error) => void} report scalar failure sink
 * @returns {Promise<boolean>} true only after all required work and cleanup succeeds
 */
const joinedCleanup = async (work, actions, report) => {
  let complete = true
  try {
    await work()
  } catch (error) {
    complete = false
    report('workload', error)
  }
  for (const action of actions) {
    try {
      await action.run()
    } catch (error) {
      complete = false
      report(`cleanup:${action.name}`, error)
    }
  }
  return complete
}
module.exports = { joinedCleanup }
