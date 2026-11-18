/**
 * Snappy throughput, event-loop fairness and memory gates. Each arm has a fresh process so
 * resourceUsage().maxRSS is a per-arm high-water mark. Input setup and warm-up precede the
 * measured interval; output bytes are checked before any result is accepted.
 *
 * Usage: node --expose-gc scripts/benchmarks/snappyCodec.js            one run of the eight arms
 *        node --expose-gc scripts/benchmarks/snappyCodec.js --session --raw <file>
 * `--session` is the report session (`snappySession`): a warm-up and eight measured runs of the
 * eight arms and of an A/A copy, interleaved. It runs only in the benchmark job that owns the row
 * (`BENCH_SNAPPY_ROWS=1`); elsewhere it prints an `omitted` record, and after a failed setup step
 * of that job (`BENCH_SETUP`) an `error` record. Exit code 0 is an accepted or omitted session, 3 a
 * void one, 1 an error.
 */
const { spawnSync } = require('child_process')
const { performance, monitorEventLoopDelay } = require('perf_hooks')
const { setTimeout: delay } = require('timers/promises')
const { createHash } = require('crypto')
const path = require('path')
const fs = require('fs')
const assert = require('assert')
const codec = require('../../src/protocol/message/compression/snappy')
const { runSession, summarize, setupFailure } = require('./session')

const MiB = 1048576
const ENCODER_DIR = path.join(__dirname, '../../src/protocol/message/compression/fixtures/encoder')
/**
 * The report session: `runs` measured runs of the codec and of its A/A copy after one warm-up run
 * each (eight, so both take both positions equally often), a child limit, a wall-clock budget, the
 * metrics kept and the two the A/A rule judges (3 %, `session.js`).
 */
const SESSION = Object.freeze({
  runs: 8,
  childMs: 600000,
  budgetMs: 100 * 60000,
  metrics: [
    'decodeMiBs',
    'encodeMiBs',
    'maxDelayMs',
    'maxRssRatio',
    'checkedBytes',
    'readbacks',
    'failures',
  ],
  controlOn: ['decodeMiBs', 'encodeMiBs'],
})
const SESSION_EXIT = { accepted: 0, omitted: 0, void: 3, error: 1 }
const OUTSIDE = 'runs in its own job of the benchmark workflow (BENCH_SNAPPY_ROWS=1)'
const ARMS = [
  'mixed',
  'json',
  'random',
  'expansion',
  'encode64',
  'mixedLarge',
  'tinyBlocks',
  'tinyTokens',
]

/**
 * Produces deterministic bytes without depending on the codec.
 * @param {number} length bytes
 * @param {boolean} random whether every byte is random
 * @returns {Buffer} corpus
 */
const corpus = (length, random = false) => {
  const input = Buffer.allocUnsafe(length)
  const text = Buffer.from('{"key":"value","count":123,"active":true}\n')
  let state = 0x12345678
  for (let i = 0; i < length; i += 1) {
    state ^= state << 13
    state ^= state >>> 17
    state ^= state << 5
    input[i] = random || i % 256 < 64 ? state : text[i % text.length]
  }
  return input
}

/**
 * Makes a valid maximal-expansion raw stream: one literal followed by copies of up to 64 bytes.
 * @param {number} length output bytes
 * @returns {Buffer} encoded stream
 */
const expansion = (length) => {
  const header = []
  let n = length
  do {
    header.push((n & 127) | (n >= 128 ? 128 : 0))
    n >>>= 7
  } while (n)
  const output = Buffer.alloc(header.length + 2 + 3 * Math.ceil((length - 1) / 64))
  Buffer.from([...header, 0, 97]).copy(output)
  let at = header.length + 2
  for (let remaining = length - 1; remaining > 0;) {
    const take = Math.min(64, remaining)
    output[at++] = ((take - 1) << 2) | 2
    output.writeUInt16LE(1, at)
    at += 2
    remaining -= take
  }
  return output
}

/**
 * Measures an operation after warm-up with a live histogram and a per-process RSS watermark.
 * @param {() => Promise<Buffer>} operation codec operation
 * @param {number} bytes uncompressed bytes used by the MiB/s denominator
 * @returns {Promise<object>} unrounded metrics and output
 */
const measure = async (operation, bytes) => {
  await operation()
  if (global.gc) global.gc()
  await delay(10)
  const baselineRss = process.memoryUsage().rss
  const histogram = monitorEventLoopDelay({ resolution: 1 })
  histogram.enable()
  await delay(5)
  histogram.reset()
  const start = performance.now()
  const output = await operation()
  const ms = performance.now() - start
  await delay(2)
  const peak = Math.max(process.memoryUsage().rss, process.resourceUsage().maxRSS * 1024)
  histogram.disable()
  return {
    MiBs: bytes / MiB / (ms / 1000),
    delayMs: histogram.max / 1e6,
    rssGrowth: Math.max(0, peak - baselineRss),
    output,
  }
}

/**
 * Runs one arm with setup outside timing and validates its output against source bytes.
 * @param {string} name corpus arm
 * @returns {Promise<object>} arm metrics including the byte-based memory ceiling
 * @throws {Error} for a wrong decoded byte or an unknown arm
 */
const runArm = async (name) => {
  let input, encoded
  if (name === 'expansion') {
    input = Buffer.alloc(64 * MiB, 97)
    encoded = expansion(input.length)
  } else if (name === 'tinyBlocks') {
    input = Buffer.alloc(1000000, 97)
    encoded = Buffer.alloc(16 + 7 * input.length)
    Buffer.from('82534e41505059000000000100000001', 'hex').copy(encoded)
    for (let at = 16; at < encoded.length; at += 7)
      Buffer.from('00000003010061', 'hex').copy(encoded, at)
  } else if (name === 'tinyTokens') {
    input = Buffer.alloc(MiB, 97)
    encoded = Buffer.alloc(3 + 2 * input.length)
    Buffer.from('808040', 'hex').copy(encoded)
    for (let at = 3; at < encoded.length; at += 2) encoded[at + 1] = 97
  } else {
    const lengths = {
      mixed: 3 * MiB,
      json: 232000,
      random: MiB,
      encode64: 64 * MiB,
      mixedLarge: 32 * MiB,
    }
    if (!lengths[name]) throw new Error(`Unknown arm ${name}`)
    input = corpus(lengths[name], name === 'random' || name === 'encode64')
    if (name === 'json') {
      const record = JSON.stringify({ key: 'value', count: 123, active: true })
      const count = Math.floor((lengths.json - 2) / (record.length + 1))
      input = Buffer.from(
        ('[' + Array(count).fill(record).join(',') + ']').padEnd(lengths.json, ' ')
      )
    }
    encoded = await codec.compress({ buffer: input })
  }
  let encoding
  if (['mixed', 'json', 'random', 'encode64'].includes(name)) {
    encoding = await measure(() => codec.compress({ buffer: input }), input.length)
    assert.deepStrictEqual(await codec.decompress(encoding.output), input)
  }
  const decoding = await measure(() => codec.decompress(encoded), input.length)
  assert.deepStrictEqual(decoding.output, input)
  const memoryBound = 3 * (input.length + encoded.length) + 16 * MiB
  return {
    decodeMiBs: decoding.MiBs,
    encodeMiBs: encoding?.MiBs ?? 0,
    delayMs: Math.max(decoding.delayMs, encoding?.delayMs ?? 0),
    rssGrowth: Math.max(decoding.rssGrowth, encoding?.rssGrowth ?? 0),
    memoryBound,
    inputBytes: encoded.length,
    outputBytes: input.length,
    checkedBytes: decoding.output.length,
  }
}

/**
 * Evaluates absolute gates without rounding. The caller separately establishes a valid A/A.
 * @param {Record<string, object>} results all measured arms
 * @returns {string[]} unmet gates
 */
const failuresOf = (results) =>
  Object.entries(results).flatMap(([name, result]) => [
    ...(result.checkedBytes !== result.outputBytes ? [`${name}: byte control`] : []),
    ...(result.delayMs >= 20 ? [`${name}: event loop delay`] : []),
    ...(result.rssGrowth > result.memoryBound ? [`${name}: RSS`] : []),
    ...(name === 'mixed' && result.decodeMiBs < 80 ? ['mixed: decode floor'] : []),
    ...(name === 'mixed' && result.encodeMiBs < 30 ? ['mixed: encode floor'] : []),
  ])

/**
 * @returns {number} the committed encoder artifacts that the Java read-back checks
 */
const countReadbacks = () =>
  fs.readdirSync(ENCODER_DIR).filter((name) => name.endsWith('.json')).length

/**
 * Reduces the eight arms of one run to its line.
 * @param {Record<string, object>} results every arm's result
 * @param {number} readbacks the encoder artifacts counted
 * @returns {{decodeMiBs:number,encodeMiBs:number,maxDelayMs:number,maxRssRatio:number,
 *   checkedBytes:number,readbacks:number,failures:number}} the mixed arm's throughput, the worst
 *   delay and RSS ratio, the bytes checked and the number of missed declared gates
 */
const summaryOf = (results, readbacks) => ({
  decodeMiBs: results.mixed.decodeMiBs,
  encodeMiBs: results.mixed.encodeMiBs,
  maxDelayMs: Math.max(...Object.values(results).map((r) => r.delayMs)),
  maxRssRatio: Math.max(...Object.values(results).map((r) => r.rssGrowth / r.memoryBound)),
  checkedBytes: Object.values(results).reduce((n, r) => n + r.checkedBytes, 0),
  readbacks,
  failures: failuresOf(results).length,
})

/**
 * The report session. Each run spawns the eight arms in order, each child limited to
 * `SESSION.childMs` and to what the budget has left; every child's arguments, exit and output are
 * appended to `raw` before it is read, every observation (with its eight arm results) to
 * `<raw>.observations.jsonl`, which `session.js summarize` reads. A missed declared gate is counted,
 * never fatal; a failed or unreadable child ends the session in an error.
 * @param {object} options
 * @param {string} options.raw the attempts file; the observations file sits beside it
 * @param {Function} [options.spawn] `child_process.spawnSync`, replaceable in tests
 * @param {Record<string,string|undefined>} [options.env] the environment that names the job
 * @param {() => number} [options.now] monotonic millisecond clock of the budget
 * @returns {object} the session record: `accepted` or `void` with both arms' stats (median, min,
 *   max, n), the A/A comparison and the observations that missed a gate; `omitted` outside the
 *   row's job; `error` with its reason, without starting a child after a setup step of the job
 *   that did not succeed (`BENCH_SETUP`)
 */
const snappySession = ({
  raw,
  spawn = spawnSync,
  env = process.env,
  now = () => performance.now(),
}) => {
  const record = { schema: 1, session: 'snappy' }
  if (env.BENCH_SNAPPY_ROWS !== '1') return { ...record, status: 'omitted', reason: OUTSIDE }
  const setup = setupFailure(env.BENCH_SETUP)
  if (setup) return { ...record, status: 'error', reason: setup }
  Object.assign(record, {
    node: process.version,
    driver: {
      file: 'scripts/benchmarks/snappyCodec.js',
      sha256: createHash('sha256').update(fs.readFileSync(__filename)).digest('hex'),
    },
  })
  const deadline = now() + SESSION.budgetMs
  const observationsFile = `${raw}.observations.jsonl`
  fs.writeFileSync(raw, '')
  fs.writeFileSync(observationsFile, '')
  let attempt = 0
  const observe = (label) => {
    const results = {}
    for (const name of ARMS) {
      const timeout = Math.min(SESSION.childMs, Math.floor(deadline - now()))
      if (!(timeout > 0)) {
        throw new Error(
          `the session budget of ${SESSION.budgetMs} ms leaves no time for ${label} ${name}`
        )
      }
      const args = ['--expose-gc', __filename, '--arm', name]
      const child = spawn(process.execPath, args, { encoding: 'utf8', timeout })
      const evidence = {
        attempt: ++attempt,
        label,
        arm: name,
        args,
        timeout,
        status: child.status,
        signal: child.signal ?? null,
        error: child.error?.message ?? null,
        stdout: child.stdout ?? '',
        stderr: child.stderr ?? '',
      }
      fs.appendFileSync(raw, `${JSON.stringify(evidence)}\n`)
      let result = null
      try {
        result = JSON.parse(String(evidence.stdout).trim().split('\n').pop())
      } catch {
        // Judged below with the exit.
      }
      if (child.status !== 0 || result === null || typeof result !== 'object') {
        const why = String(evidence.stderr).trim().slice(-300) || 'no result line'
        const error = child.error ? ` (${child.error.message})` : ''
        throw new Error(`${label} ${name}: exit ${child.status ?? child.signal}${error}: ${why}`)
      }
      results[name] = result
    }
    return { ...summaryOf(results, countReadbacks()), arms: results }
  }
  try {
    const observations = runSession({
      arms: [{ label: 'snappy', run: observe }],
      runs: SESSION.runs,
      reference: 'snappy',
      onObservation: (observation) =>
        fs.appendFileSync(observationsFile, `${JSON.stringify(observation)}\n`),
    })
    const summary = summarize(observations, {
      metrics: SESSION.metrics,
      reference: 'snappy',
      controlOn: SESSION.controlOn,
    })
    return {
      ...record,
      status: summary.void ? 'void' : 'accepted',
      runs: SESSION.runs,
      observations: observations.length,
      arms: summary.arms,
      control: summary.control,
      void: summary.void,
      missed: observations.filter(({ warmup, result }) => !warmup && result.failures > 0).length,
    }
  } catch (error) {
    return { ...record, status: 'error', reason: error.message }
  }
}

/**
 * Prints one numeric JSON line, one child-arm result, or one `--session` record. No network is used.
 * @param {string[]} args `--arm <name>` for a child, `--session --raw <file>` for the report session
 * @returns {Promise<number>} 0 for passing gates or an accepted or omitted session, 1 for a gate or
 *   child failure or a session error, 3 for a void session
 * @throws {Error} for malformed child output
 */
const main = async (args) => {
  if (args[0] === '--arm') {
    process.stdout.write(JSON.stringify(await runArm(args[1])) + '\n')
    return 0
  }
  if (args[0] === '--session') {
    assert(args.length === 3 && args[1] === '--raw', 'Expected --session --raw <file>')
    const record = snappySession({ raw: path.resolve(args[2]) })
    process.stdout.write(JSON.stringify(record) + '\n')
    return SESSION_EXIT[record.status]
  }
  const results = {}
  for (const name of ARMS) {
    const child = spawnSync(process.execPath, ['--expose-gc', __filename, '--arm', name], {
      encoding: 'utf8',
      timeout: 600000,
    })
    if (child.status !== 0) throw new Error(child.stderr || `Arm ${name} failed`)
    results[name] = JSON.parse(child.stdout)
  }
  const readbacks = countReadbacks()
  if (readbacks !== 7)
    throw new Error(
      'Expected seven Java read-back artifacts; run the Java verification before this gate'
    )
  const failures = failuresOf(results)
  process.stdout.write(JSON.stringify(summaryOf(results, readbacks)) + '\n')
  if (failures.length) process.stderr.write(failures.join('\n') + '\n')
  return failures.length ? 1 : 0
}

module.exports = {
  ARMS,
  SESSION,
  corpus,
  expansion,
  measure,
  runArm,
  failuresOf,
  summaryOf,
  snappySession,
  main,
}
if (require.main === module)
  main(process.argv.slice(2)).then(
    (code) => {
      process.exitCode = code
    },
    (error) => {
      process.stderr.write(error.stack + '\n')
      process.exitCode = 1
    }
  )
