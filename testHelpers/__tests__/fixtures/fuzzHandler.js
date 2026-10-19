const fs = require('fs')

/**
 * A worker handler for the specs of the fuzz helper: answers with what it was given, fails, never
 * returns, waits or leaves a file behind on request.
 * @param {{ n?: number, buffer?: Uint8Array, hang?: boolean, fail?: boolean, sleepMs?: number,
 *   mark?: string }} fuzzCase `mark` is a path the handler writes when it runs the case
 * @returns {Promise<{ n?: number, bytes: number, isBuffer: boolean }>}
 */
module.exports = async (fuzzCase) => {
  if (fuzzCase.mark) {
    fs.writeFileSync(fuzzCase.mark, 'ran')
  }

  if (fuzzCase.sleepMs) {
    await new Promise((resolve) => setTimeout(resolve, fuzzCase.sleepMs))
  }

  if (fuzzCase.hang) {
    for (;;) {
      // never returns, and never yields: only terminating the worker ends it
    }
  }

  if (fuzzCase.fail) {
    throw new Error('handler failed')
  }

  return {
    n: fuzzCase.n,
    bytes: fuzzCase.buffer ? fuzzCase.buffer.length : 0,
    isBuffer: Buffer.isBuffer(fuzzCase.buffer),
  }
}
