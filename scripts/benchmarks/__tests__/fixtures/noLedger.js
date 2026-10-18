/**
 * Preloaded into the consume benchmark to make it run as on a tree that has no offset ledger: the
 * request for the ledger module fails as a missing module does.
 */
const Module = require('module')

const resolve = Module._resolveFilename

Module._resolveFilename = function resolveFilename(request, ...rest) {
  if (request === '../../src/consumer/offsetManager/offsetOperations') {
    throw Object.assign(new Error(`Cannot find module '${request}'`), { code: 'MODULE_NOT_FOUND' })
  }
  return resolve.call(this, request, ...rest)
}
