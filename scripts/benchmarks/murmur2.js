/**
 * murmur2 microbenchmark: ns per hash for 16- and 256-byte keys, this tree's implementation against
 * a baseline tree's, in alternating rounds of the same process (the order swaps on odd rounds), and
 * whether the two agree on every key hashed: random keys of both sizes and of every length that
 * reaches a different tail of the algorithm. `DefaultPartitioner` hashes every keyed record with it.
 * A bad argument exits 2.
 * Usage: node scripts/benchmarks/murmur2.js --baseline-tree <dir> [--iterations 200000]
 * Prints one JSON line: { iterations, rounds, sizes: { <bytes>: { nsPerOp, baselineNsPerOp,
 *   ratio } }, identical }, where ratio is nsPerOp / baselineNsPerOp (below 1 is faster).
 */
const path = require('path')
const crypto = require('crypto')
const murmur2 = require('../../src/producer/partitioners/default/murmur2')

/**
 * Reads the flag `--<name>`.
 * @param {string} name
 * @param {string|null|number} fallback returned when the flag is absent
 * @returns {string|null|number}
 */
const arg = (name, fallback) => {
  const i = process.argv.indexOf(`--${name}`)
  return i === -1 ? fallback : process.argv[i + 1]
}

const BASELINE_TREE = arg('baseline-tree', null)
const ITERATIONS = Number(arg('iterations', 200000))
const SIZES = [16, 256]
// Lengths that are not a multiple of 4 reach the algorithm's tail; 0 and 4 have none.
const AGREEMENT_LENGTHS = [0, 1, 2, 3, 4, 5, 6, 7, 15, 17]
const KEYS_PER_LENGTH = 64
const KEYS_PER_SIZE = 1024
// Seven rounds per arm, alternating; the first round of each arm is the warm-up and is dropped.
const ROUNDS = 7

if (!BASELINE_TREE) {
  console.error('--baseline-tree <dir> is required: the tree whose murmur2 is the baseline')
  process.exit(2)
}
if (!Number.isFinite(ITERATIONS) || ITERATIONS < 1) {
  console.error('--iterations needs a positive number')
  process.exit(2)
}

const baseline = require(
  path.resolve(BASELINE_TREE, 'src/producer/partitioners/default/murmur2.js')
)

/**
 * @param {number[]} values
 * @returns {number} the middle value of the sorted list (the upper one for an even count)
 */
const median = (values) => [...values].sort((a, b) => a - b)[Math.floor(values.length / 2)]

/**
 * Hashes `keys` round-robin `iterations` times.
 * @param {(key: Buffer) => number} hash
 * @param {Buffer[]} keys
 * @returns {number} ns per hash
 */
const time = (hash, keys) => {
  let sink = 0
  const started = process.hrtime.bigint()
  for (let i = 0; i < ITERATIONS; i++) {
    sink ^= hash(keys[i % keys.length])
  }
  const ns = Number(process.hrtime.bigint() - started) / ITERATIONS
  // Read so the loop cannot be optimised away.
  return sink === 0.5 ? -1 : ns
}

/**
 * @param {number} length
 * @param {number} count
 * @returns {Buffer[]} `count` random keys of `length` bytes
 */
const randomKeys = (length, count) =>
  Array.from({ length: count }, () => crypto.randomBytes(length))

/**
 * @param {Buffer[]} keys
 * @returns {boolean} whether this tree's murmur2 and the baseline's hash every key alike
 */
const agree = (keys) => keys.every((key) => murmur2(key) === baseline(key))

const sizes = {}
let identical = AGREEMENT_LENGTHS.every((length) => agree(randomKeys(length, KEYS_PER_LENGTH)))
for (const size of SIZES) {
  const keys = randomKeys(size, KEYS_PER_SIZE)
  identical = identical && agree(keys)
  const ours = []
  const theirs = []
  for (let round = 0; round < ROUNDS; round++) {
    if (round % 2 === 0) {
      ours.push(time(murmur2, keys))
      theirs.push(time(baseline, keys))
    } else {
      theirs.push(time(baseline, keys))
      ours.push(time(murmur2, keys))
    }
  }
  const nsPerOp = median(ours.slice(1))
  const baselineNsPerOp = median(theirs.slice(1))
  sizes[size] = { nsPerOp, baselineNsPerOp, ratio: nsPerOp / baselineNsPerOp }
}

console.log(JSON.stringify({ iterations: ITERATIONS, rounds: ROUNDS, sizes, identical }))
