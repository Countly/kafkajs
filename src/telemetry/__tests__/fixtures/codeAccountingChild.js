'use strict'

/**
 * The code-accounting sanity of the data quantity, run in its own `node --expose-gc` process by
 * `resources.spec.js` on every Node line. It takes settled readings through the same protocol the
 * memory arms use and prints, per step, how the retained bytes, the engine's bytecode and
 * compiled-code bytes, and the data bytes (retained minus code) moved:
 *
 * - compiling and running distinct functions moves the code counters, and they never account for
 *   more bytes than the heap grew by, so the subtraction cannot hide a leak;
 * - a retained array a little over 1 MiB, on the heap and as a typed array, moves the data bytes
 *   by at least 1 MiB while the code counters stay near still, so a real leak is not subtracted.
 *
 * It never calls `process.exit`.
 */
const path = require('node:path')

const ROOT = path.join(__dirname, '..', '..', '..', '..')
const { ProcessMemory, dataBytes } = require(path.join(ROOT, 'testHelpers', 'processMemory.js'))

const FUNCTIONS = 600
const STATEMENTS = 40
const MIB = 1048576
const HEAP_ELEMENTS = MIB / 8 + 8192
const TYPED_BYTES = MIB + 65536

/**
 * @param {object} reading a settled reading @returns {number} bytecode plus compiled-code bytes at
 *   its final checkpoint
 */
const codeBytes = (reading) => {
  const { bytecode, code } = reading.gc.checkpoints.at(-1).engine
  return bytecode + code
}

/**
 * @param {object} before the settled reading before a step @param {object} after the one after it
 * @returns {{retained: number, code: number, data: number}} how far each quantity moved
 */
const moved = (before, after) => ({
  retained: after.retained - before.retained,
  code: codeBytes(after) - codeBytes(before),
  data: dataBytes(after) - dataBytes(before),
})

/**
 * @param {number} index function number @returns {Function} a function with its own source, so the
 *   engine compiles it rather than finding it in a cache; every call has run once
 */
const compiled = (index) => {
  const lines = Array.from(
    { length: STATEMENTS },
    (_, line) => `a[${line % 7}] = a[${(line + 1) % 7}] + ${index * STATEMENTS + line};`
  )
  // Compiling distinct source at run time is this fixture's whole purpose; it is a test fixture.
  // eslint-disable-next-line no-new-func
  const fn = new Function('a', `${lines.join('\n')}\nreturn a[0]`)
  fn([1, 2, 3, 4, 5, 6, 7])
  return fn
}

/** @returns {Promise<{compile: object, heapArray: object, typedArray: object}>} the three steps */
const run = async () => {
  const memory = new ProcessMemory()
  const keep = []
  const base = await memory.settled()
  for (let index = 0; index < FUNCTIONS; index++) keep.push(compiled(index))
  const afterCompile = await memory.settled()
  keep.push(Array.from({ length: HEAP_ELEMENTS }, () => 1))
  const afterHeap = await memory.settled()
  keep.push(new Uint8Array(TYPED_BYTES))
  const afterTyped = await memory.settled()
  memory.stop()
  return {
    functions: FUNCTIONS,
    compile: moved(base, afterCompile),
    heapArray: moved(afterCompile, afterHeap),
    typedArray: moved(afterHeap, afterTyped),
    kept: keep.length,
  }
}

run().then(
  (result) => process.stdout.write(`${JSON.stringify(result)}\n`),
  (error) => {
    process.stderr.write(`${error.stack}\n`)
    process.exitCode = 1
  }
)
