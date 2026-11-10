const assert = require('assert/strict')
const crypto = require('crypto')
const fs = require('fs')
const path = require('path')
const { spawnSync } = require('child_process')
const { performance } = require('perf_hooks')

/**
 * Creates a runner-local AppCDS archive from the verified, pinned offline decoder.
 * Preparation runs once before any timed observer work. Its first archive-backed decode must
 * finish under the observer's unchanged 3 s supervisor and reproduce the entire decoded body.
 * No failed preparation or decode is retried, and a failed archive is never published.
 * @param {object} options preparation inputs
 * @param {string} options.line retained Kafka line
 * @param {string} options.archive new absolute archive path, used by subsequent decoder children
 * @param {object} [options.env=process.env] verified cache and private temporary environment
 * @param {string} [options.script] offline runner; a finite subprocess seam for unit controls
 * @returns {object} archive digest, verified input identity and preparation/readiness timings
 * @throws {Error} for invalid paths, changed inputs, failed children or mismatching output
 */
function prepareDecoder({
  line,
  archive,
  env = process.env,
  script = path.join(__dirname, 'run.sh'),
}) {
  assert(['4.3.1', '4.2.2'].includes(line), 'Unsupported decoder line')
  assert(path.isAbsolute(archive), 'Expected an absolute decoder archive path')
  assert(!fs.existsSync(archive), 'Decoder archive already exists')
  const directory = fs.mkdtempSync(path.join(path.dirname(archive), 'decoder-prepare-'))
  const prepared = path.join(directory, 'classes.jsa')
  const raw = path.join(directory, 'nonzero.bin')
  try {
    const vector = JSON.parse(
      fs.readFileSync(path.join(__dirname, 'fixtures/golden/vectors.json'), 'utf8')
    ).vectors.nonzero
    const bytes = Buffer.from(vector.hex, 'hex')
    const digest = crypto.createHash('sha256').update(bytes).digest('hex')
    assert.equal(bytes.length, vector.bytes)
    assert.equal(digest, vector.sha256)
    fs.writeFileSync(raw, bytes, { flag: 'wx', mode: 0o600 })
    /**
     * Runs one supervised stage, retaining diagnostics and requiring the two-line decode contract.
     * @param {string} mode offline runner mode
     * @param {string} budget timeout duration; preparation is outside all observer/case budgets
     * @returns {object} verified decoder output, elapsed time and stderr
     * @throws {Error} for timeout, nonzero exit, invalid identity or incomplete decoding
     */
    const run = (mode, budget) => {
      const start = performance.now()
      const result = spawnSync('timeout', ['-k', '1s', budget, 'bash', script, mode, line, raw], {
        env: { ...env, TMPDIR: directory, TELEMETRY_DECODER_ARCHIVE: prepared },
        encoding: 'utf8',
        maxBuffer: 1024 * 1024,
      })
      if (result.error) throw result.error
      assert.equal(
        result.status,
        0,
        `Decoder ${mode} failed (${result.status ?? result.signal}): ${result.stderr}`
      )
      const lines = result.stdout.trim().split('\n')
      assert.equal(lines.length, 2, 'Decoder must print exactly two JSON lines')
      const [identity, body] = lines.map((line) => JSON.parse(line))
      assert(identity.verified === true && identity.line === line, 'Unverified decoder identity')
      assert(body.parse === 'ok' && body.truncated === false, 'Incomplete decoder preparation')
      assert.equal(body.dataBytes, bytes.length)
      assert.equal(body.dataSha256, digest)
      return { identity, body, elapsedMs: performance.now() - start, stderr: result.stderr }
    }
    const training = run('prepare', '30s')
    assert(fs.existsSync(prepared), 'Decoder did not create an archive')
    const info = fs.lstatSync(prepared)
    assert(info.isFile() && info.size > 0, 'Invalid decoder archive')
    const readiness = run('decode', '3s')
    assert.deepEqual(
      [readiness.identity, readiness.body],
      [training.identity, training.body],
      'Prepared decoder output does not match training'
    )
    const archiveSha256 = crypto
      .createHash('sha256')
      .update(fs.readFileSync(prepared))
      .digest('hex')
    // link refuses an existing destination; a partial or stale archive cannot replace a good one.
    fs.linkSync(prepared, archive)
    return {
      verified: true,
      line,
      archive,
      archiveSha256,
      identity: training.identity,
      rawSha256: digest,
      preparationMs: training.elapsedMs,
      readinessMs: readiness.elapsedMs,
      preparationStderr: training.stderr,
      readinessStderr: readiness.stderr,
    }
  } finally {
    fs.rmSync(directory, { recursive: true, force: true })
  }
}

if (require.main === module) {
  const result = prepareDecoder({
    line: process.env.KAFKA_VERSION,
    archive: process.env.TELEMETRY_DECODER_ARCHIVE,
  })
  fs.writeFileSync(result.archive + '.json', JSON.stringify(result) + '\n', {
    flag: 'wx',
    mode: 0o600,
  })
  console.log(JSON.stringify(result))
}

module.exports = { prepareDecoder }
