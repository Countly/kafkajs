const fs = require('fs')
const path = require('path')
const { Worker } = require('worker_threads')
const { mulberry32 } = require('./schedule')

/**
 * The values written over a byte: zero, one (a varint length of -1, which reads as null), the
 * largest positive, the smallest negative, all ones.
 */
const BYTE_VALUES = [0x00, 0x01, 0x7f, 0x80, 0xff]

/** The most bytes a body can have to be mutated at every position; above, a seeded sample is. */
const EXHAUSTIVE_BYTES = 512

/** How many positions a body above `EXHAUSTIVE_BYTES` has mutated. */
const SAMPLE_POSITIONS = 2000

/** The cases one worker runs before it is replaced. */
const CASES_PER_WORKER = 500

/** How long a worker may run one batch of cases before it is terminated. */
const WATCHDOG_MS = 10000

const FIELDS_OF_A_CASE = ['seed', 'hex', 'api', 'version', 'direction', 'expect']

/**
 * What a failure prints, so it can be replayed: the seed of the stream the case came from, its
 * index in that stream and the bytes it decoded.
 * @param {{ seed: number, index: number, buffer: Buffer }} failure
 * @returns {string} `seed=<n> case=<k> hex=<bytes>`
 */
const caseLabel = ({ seed, index, buffer }) =>
  `seed=${seed} case=${index} hex=${Buffer.from(buffer).toString('hex')}`

/**
 * Every proper prefix of `buffer`, the empty one first: a body cut at every byte boundary.
 * @param {Buffer} buffer
 * @yields {{ name: string, buffer: Buffer }}
 */
function* truncations(buffer) {
  for (let length = 0; length < buffer.length; length++) {
    yield { name: `cut to ${length}`, buffer: buffer.subarray(0, length) }
  }
}

/**
 * The positions to mutate: every one of a small body, a seeded sample of a large one.
 * @param {number} length the body's length less what a mutation writes past its position
 * @param {() => number} random
 * @returns {number[]} ascending
 */
const positionsOf = (length, random) => {
  const all = Array.from({ length: Math.max(length, 0) }, (_, position) => position)
  if (all.length <= EXHAUSTIVE_BYTES) {
    return all
  }

  for (let i = all.length - 1; i > 0; i--) {
    const j = Math.floor(random() * (i + 1))
    ;[all[i], all[j]] = [all[j], all[i]]
  }
  return all.slice(0, SAMPLE_POSITIONS).sort((a, b) => a - b)
}

/**
 * Each byte set to each of `BYTE_VALUES` (exhaustive up to `EXHAUSTIVE_BYTES`, a seeded sample of
 * `SAMPLE_POSITIONS` above) that differs from what it was.
 * @param {Buffer} buffer
 * @param {number} seed
 * @yields {{ name: string, buffer: Buffer }}
 */
function* byteMutations(buffer, seed) {
  for (const position of positionsOf(buffer.length, mulberry32(seed))) {
    for (const value of BYTE_VALUES) {
      if (buffer[position] !== value) {
        const copy = Buffer.from(buffer)
        copy[position] = value
        yield { name: `byte ${position} = 0x${value.toString(16)}`, buffer: copy }
      }
    }
  }
}

/**
 * Every 4 bytes read as an int32 set to -1, 0, 2^31 - 1, -2^31, and to the bytes that follow the
 * field, less one and plus one, each value once. The positions are those of `byteMutations`.
 * @param {Buffer} buffer
 * @param {number} seed
 * @yields {{ name: string, buffer: Buffer }}
 */
function* int32Mutations(buffer, seed) {
  for (const position of positionsOf(buffer.length - 3, mulberry32(seed))) {
    const following = buffer.length - position - 4
    for (const value of new Set([-1, 0, 0x7fffffff, -0x80000000, following - 1, following + 1])) {
      if (buffer.readInt32BE(position) !== value) {
        const copy = Buffer.from(buffer)
        copy.writeInt32BE(value, position)
        yield { name: `int32 ${position} = ${value}`, buffer: copy }
      }
    }
  }
}

/**
 * A seeded slice of the body repeated where it ends: the same entry twice, a length with its
 * payload twice.
 * @param {Buffer} buffer
 * @param {number} seed
 * @param {number} [count=200]
 * @yields {{ name: string, buffer: Buffer }}
 */
function* repeatedSlices(buffer, seed, count = 200) {
  const random = mulberry32(seed)
  for (let i = 0; i < count && buffer.length > 1; i++) {
    const start = Math.floor(random() * (buffer.length - 1))
    const end = Math.min(buffer.length, start + 1 + Math.floor(random() * 32))
    yield {
      name: `slice ${start}-${end} repeated`,
      buffer: Buffer.concat([buffer.subarray(0, end), buffer.subarray(start)]),
    }
  }
}

/**
 * The mutations a body gets: bytes, int32s and repeated slices.
 * @param {Buffer} buffer
 * @param {number} seed
 * @yields {{ name: string, buffer: Buffer }}
 */
function* mutations(buffer, seed) {
  yield* byteMutations(buffer, seed)
  yield* int32Mutations(buffer, seed + 1)
  yield* repeatedSlices(buffer, seed + 2)
}

/**
 * The smallest input that still fails, by cutting the end off it and then zeroing bytes while it
 * does. A failure found by a mutation is shrunk before it is promoted to a corpus case, so a case
 * names the bytes that matter.
 * @param {Buffer} buffer a failing input
 * @param {(candidate: Buffer) => boolean|Promise<boolean>} stillFails
 * @returns {Promise<Buffer>} an input of at most the same length for which `stillFails` is true
 * @throws {Error} when `buffer` does not fail
 */
const shrink = async (buffer, stillFails) => {
  if (!(await stillFails(buffer))) {
    throw new Error('shrink needs an input that fails')
  }

  let current = Buffer.from(buffer)
  while (current.length > 0 && (await stillFails(current.subarray(0, current.length - 1)))) {
    current = current.subarray(0, current.length - 1)
  }

  for (let position = 0; position < current.length; position++) {
    if (current[position] !== 0) {
      const zeroed = Buffer.from(current)
      zeroed[position] = 0
      if (await stillFails(zeroed)) {
        current = zeroed
      }
    }
  }

  return current
}

/**
 * The cases kept under a directory, each a permanent regression: every JSON file in it, each
 * `{ seed, hex, api, version, direction, expect }`, in name order.
 * @param {string} directory
 * @returns {Array<{ name: string, seed: number, hex: string, api: string, version: number,
 *   direction: string, expect: object }>}
 * @throws {Error} naming the file, when a case lacks a field
 */
const loadCorpus = (directory) =>
  fs
    .readdirSync(directory)
    .filter((file) => file.endsWith('.json'))
    .sort()
    .map((file) => {
      const found = JSON.parse(fs.readFileSync(path.join(directory, file), 'utf8'))
      const missing = FIELDS_OF_A_CASE.filter((field) => !(field in found))
      if (missing.length > 0) {
        throw new Error(`Corpus case ${file} has no ${missing.join(', ')}`)
      }

      return { name: path.basename(file, '.json'), ...found }
    })

/**
 * @template T
 * @param {T[]} cases
 * @param {number} [size=CASES_PER_WORKER]
 * @returns {T[][]} `cases` in runs of at most `size`
 */
const chunks = (cases, size = CASES_PER_WORKER) => {
  const runs = []
  for (let start = 0; start < cases.length; start += size) {
    runs.push(cases.slice(start, start + size))
  }
  return runs
}

/**
 * Runs a batch of cases in a worker thread and waits for its report, so a decoder that never
 * returns costs one terminated worker instead of the suite. The worker's module is run as
 * `handler(case)` for every case in order, and the index of the case it is on is kept in memory
 * shared with this thread, so when the watchdog fires the failure names the case that did not
 * return. Cases are cloned to the worker, which turns a `Buffer` into a plain `Uint8Array`.
 * @param {object} options
 * @param {string} options.handler absolute path of a module exporting `async (case) => result`
 * @param {object[]} options.cases the batch
 * @param {number} options.seed the seed the cases came from, for the failure label
 * @param {number} [options.watchdogMs=WATCHDOG_MS]
 * @param {number} [options.firstIndex=0] the index in its stream of the first case, for the label
 * @returns {Promise<object[]>} the handler's result for each case, in order
 * @throws {Error} `watchdog: seed=<n> case=<k> did not return in <ms> ms` when the worker had to
 *   be terminated, or the error the worker failed with
 */
const runInWorker = ({ handler, cases, seed, watchdogMs = WATCHDOG_MS, firstIndex = 0 }) =>
  new Promise((resolve, reject) => {
    const progress = new Int32Array(new SharedArrayBuffer(4))
    const worker = new Worker(path.join(__dirname, 'fuzzWorker.js'), {
      workerData: { handler, cases, progress },
    })
    let watchdog = null
    const timer = setTimeout(() => {
      watchdog = new Error(
        `watchdog: seed=${seed} case=${firstIndex + Atomics.load(progress, 0)} did not return in ${watchdogMs} ms`
      )
      worker.terminate()
    }, watchdogMs)
    const settle = (finish) => (value) => {
      clearTimeout(timer)
      finish(value)
    }

    worker.once('message', settle(resolve))
    worker.once('error', settle(reject))
    worker.once('exit', (code) =>
      settle(reject)(watchdog || new Error(`worker exited with code ${code} before it reported`))
    )
  })

/**
 * Runs cases in workers, `CASES_PER_WORKER` to a worker and at most `concurrency` workers at once.
 * @param {object} options as {@link runInWorker}, less `firstIndex`
 * @param {object[]} options.cases
 * @param {number} [options.concurrency=4]
 * @returns {Promise<object[]>} the handler's result for each case, in order
 * @throws {Error} as {@link runInWorker}, for the first batch that fails; no batch is started after
 *   one has failed
 */
const runCases = async ({ cases, concurrency = 4, ...options }) => {
  const batches = chunks(cases)
  const results = new Array(batches.length)
  let next = 0
  let failed = false

  const lane = async () => {
    while (!failed && next < batches.length) {
      const at = next++
      try {
        results[at] = await runInWorker({
          ...options,
          cases: batches[at],
          firstIndex: at * CASES_PER_WORKER,
        })
      } catch (error) {
        failed = true
        throw error
      }
    }
  }

  await Promise.all(Array.from({ length: Math.min(concurrency, batches.length) }, lane))
  return results.flat()
}

module.exports = {
  CASES_PER_WORKER,
  mulberry32,
  caseLabel,
  truncations,
  byteMutations,
  int32Mutations,
  repeatedSlices,
  mutations,
  shrink,
  loadCorpus,
  chunks,
  runInWorker,
  runCases,
}
