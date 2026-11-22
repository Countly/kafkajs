/**
 * @template T
 * @param { (...args: any) => Promise<T> } [asyncFunction]
 * Promise returning function that will only ever be invoked sequentially.
 * @param { (...args: any) => unknown } [scopeOf]
 * The scope of a call, from its arguments: a call whose scope differs from the running one's
 * starts its own invocation and becomes the one later calls share; the older one runs on. Only the
 * latest invocation is tracked, so no scope is remembered once it settles.
 * @returns { ((...args: any) => Promise<T>) & { isRunning: () => boolean } }
 * Function that may invoke asyncFunction if there is not a currently executing invocation.
 * Returns promise from the currently executing invocation. Its `isRunning()` tells whether an
 * invocation is tracked: one stays tracked until its settlement has been observed, and a call in
 * its scope joins it meanwhile.
 */
module.exports = (asyncFunction, scopeOf = () => undefined) => {
  let running = null

  const shared = (...args) => {
    const scope = scopeOf(...args)
    if (running === null || running.scope !== scope) {
      const invocation = { scope, promise: null }
      invocation.promise = asyncFunction(...args).finally(() => {
        if (running === invocation) running = null
      })
      running = invocation
    }
    return running.promise
  }
  shared.isRunning = () => running !== null
  return shared
}
