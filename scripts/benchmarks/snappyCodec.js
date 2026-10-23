/**
 * Snappy throughput, event-loop fairness and memory gates. Each arm has a fresh process so
 * resourceUsage().maxRSS is a per-arm high-water mark. Input setup and warm-up precede the
 * measured interval; output bytes are checked before any result is accepted.
 */
const { spawnSync } = require('child_process')
const { performance, monitorEventLoopDelay } = require('perf_hooks')
const { setTimeout: delay } = require('timers/promises')
const path = require('path')
const fs = require('fs')
const assert = require('assert')
const codec = require('../../src/protocol/message/compression/snappy')

const MiB = 1048576
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
 * Prints one numeric JSON line for session.js, or one child-arm result. No network is used.
 * @param {string[]} args --arm and name for a child
 * @returns {Promise<number>} 0 for passing gates, 1 for a gate or child failure
 * @throws {Error} for malformed child output
 */
const main = async (args) => {
  if (args[0] === '--arm') {
    process.stdout.write(JSON.stringify(await runArm(args[1])) + '\n')
    return 0
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
  const readbacks = fs
    .readdirSync(path.join(__dirname, '../../src/protocol/message/compression/fixtures/encoder'))
    .filter((name) => name.endsWith('.json')).length
  if (readbacks !== 7)
    throw new Error(
      'Expected seven Java read-back artifacts; run the Java verification before this gate'
    )
  const failures = failuresOf(results)
  process.stdout.write(
    JSON.stringify({
      decodeMiBs: results.mixed.decodeMiBs,
      encodeMiBs: results.mixed.encodeMiBs,
      maxDelayMs: Math.max(...Object.values(results).map((r) => r.delayMs)),
      maxRssRatio: Math.max(...Object.values(results).map((r) => r.rssGrowth / r.memoryBound)),
      checkedBytes: Object.values(results).reduce((n, r) => n + r.checkedBytes, 0),
      readbacks,
      failures: failures.length,
    }) + '\n'
  )
  if (failures.length) process.stderr.write(failures.join('\n') + '\n')
  return failures.length ? 1 : 0
}

module.exports = { corpus, expansion, measure, runArm, failuresOf, main }
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
