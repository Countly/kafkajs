import { existsSync } from 'node:fs'
import { fileURLToPath } from 'node:url'

// Registers import-in-the-middle's synchronous loader hooks (`module.registerHooks()`) and leaves
// on `globalThis` what happened, for the test beside it: `registered`, or the reason it could not.
// Only an import-in-the-middle without `register-hooks.mjs` (absent, or before 3.1.0) or a Node.js
// without synchronous hooks is a reason; any other failure, a registration included, fails the
// process.
const REGISTER_HOOKS = 'import-in-the-middle/register-hooks.mjs'

const hasRegisterHooks = () => {
  try {
    return existsSync(fileURLToPath(import.meta.resolve(REGISTER_HOOKS)))
  } catch (error) {
    if (error.code === 'ERR_MODULE_NOT_FOUND') {
      return false
    }
    throw error
  }
}

let state = 'import-in-the-middle has no synchronous hooks'

if (hasRegisterHooks()) {
  const { register, supportsSyncHooks } = await import(REGISTER_HOOKS)

  if (supportsSyncHooks()) {
    register()
    state = 'registered'
  } else {
    state = 'this Node.js has no synchronous module hooks'
  }
}

globalThis[Symbol.for('kafkajs-otel-test.syncHooks')] = state
