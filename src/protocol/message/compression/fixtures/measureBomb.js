/**
 * Decompresses a bomb in this fresh process and prints the outcome as JSON, so the resident memory
 * it reads holds nothing but the decompression. Usage: node measureBomb.js <codec>, with the
 * compressed bytes on stdin.
 */
const { Types, lookupCodec, decompressBounded } = require('../index')

const LIMIT = 64 * 1048576
const CODECS = {
  gzip: [Types.GZIP, 1],
  zstd: [Types.ZSTD, 4],
  lz4: [Types.LZ4, 3],
  snappy: [Types.Snappy, 2],
}

/** @returns {Promise<Buffer>} everything on stdin */
const readStdin = async () => {
  const parts = []
  for await (const part of process.stdin) parts.push(part)
  return Buffer.concat(parts)
}

/**
 * Decompresses under the bound and reports the refusal and the peak resident memory it added,
 * sampled every 2 ms (decompression runs on the thread pool).
 * @param {string} name codec key of CODECS
 * @param {Buffer} input
 * @returns {Promise<{ error: object, growth: number, nativeCalls: number, milliseconds: number }>}
 */
const run = async (name, input) => {
  const [type, version] = CODECS[name]
  const codec = lookupCodec(type)
  const native = name === 'lz4' ? require('lz4-napi') : {}
  const original = native.decompressFrame
  let nativeCalls = 0
  if (original) {
    native.decompressFrame = (...args) => {
      nativeCalls++
      return original(...args)
    }
  }

  const before = process.memoryUsage.rss()
  let peak = before
  const sampler = setInterval(() => {
    peak = Math.max(peak, process.memoryUsage.rss())
  }, 2)
  const started = Date.now()
  const error = await decompressBounded(codec, version, input, LIMIT).catch((e) => e)
  clearInterval(sampler)

  return {
    error: { reason: error.reason, codec: error.codec, limit: error.limit },
    growth: Math.max(peak, process.memoryUsage.rss()) - before,
    nativeCalls,
    milliseconds: Date.now() - started,
  }
}

readStdin()
  .then((input) => run(process.argv[2], input))
  .then(
    (result) => process.stdout.write(JSON.stringify(result)),
    (e) => {
      process.stderr.write(String((e && e.stack) || e))
      process.exitCode = 1
    }
  )
