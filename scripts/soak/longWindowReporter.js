'use strict'

const assert = require('node:assert/strict')
const crypto = require('node:crypto')
const fs = require('node:fs')
const path = require('node:path')
const { execFileSync } = require('node:child_process')
const { selected, SINK_BYTES, SINK_BUDGET } = require('./longWindow')

const ROOT = path.resolve(__dirname, '../..')
const DIRECTORY = '.tmp/telemetry-long-reporter'
const DEFAULT_CAP = 'MAX_FILE_BYTES = 2L * 1024 * 1024;'
const sha = (bytes) => crypto.createHash('sha256').update(bytes).digest('hex')

/**
 * Prepares the long diagnostic's reporter outside the timed run. Retained source, jars, decoder
 * inputs and Compose bytes stay immutable. Only the reporter's file cap changes; queue overflow,
 * file overflow and every evidence check remain fatal. The derived jar and its source/input hashes
 * are archived, and CI ownership verifies the actual mounted jar against that receipt.
 */
class LongWindowReporter {
  /**
   * @param {object} [options] CI environment and bounded compiler seam
   * @param {string} [options.directory] fresh absolute output directory
   * @param {object} [options.env] environment selecting the long diagnostic and pinned Java cache
   * @param {function(string,string[],object):*} [options.run] synchronous child runner
   */
  constructor({
    env = process.env,
    run = execFileSync,
    directory = path.join(ROOT, DIRECTORY),
  } = {}) {
    assert(env.CI === 'true' && selected(env), 'Long reporter preparation is diagnostic CI-only')
    assert(['4.2.2', '4.3.1'].includes(env.KAFKA_VERSION), 'Unsupported long reporter line')
    this.env = env
    this.run = run
    assert(path.isAbsolute(directory), 'Long reporter directory must be absolute')
    this.directory = directory
    this.jar = path.join(this.directory, 'reporter.jar')
    this.compose = path.join(this.directory, 'compose.json')
  }

  /**
   * @param {string} original retained reporter source
   * @returns {string} the same source with exactly its file cap replaced by the derived long cap
   * @throws {Error} if the retained cap is missing or appears more than once
   */
  static source(original) {
    assert.equal(original.split(DEFAULT_CAP).length, 2, 'Unexpected reporter file cap')
    return original.replace(DEFAULT_CAP, `MAX_FILE_BYTES = ${SINK_BYTES}L;`)
  }

  /**
   * @returns {object} verified retained source and dependency identities
   * @throws {Error} for changed source, retained reporter or compiler inputs
   */
  inputs() {
    const fixture = path.join(ROOT, 'scripts/e2e/telemetry')
    const manifest = JSON.parse(fs.readFileSync(path.join(fixture, 'fixtures/manifest.json')))
    const read = (file, digest) => {
      const stat = fs.lstatSync(file)
      assert(stat.isFile() && !stat.isSymbolicLink(), 'Reporter input must be a regular file')
      const bytes = fs.readFileSync(file)
      assert.equal(sha(bytes), digest, 'Long reporter input identity mismatch')
      return bytes
    }
    const original = read(
      path.join(fixture, 'CountingTelemetryReporter.java'),
      manifest.scripts['CountingTelemetryReporter.java']
    ).toString('utf8')
    const retained = manifest.lines[this.env.KAFKA_VERSION].reporterJar
    const retainedJar = path.join(ROOT, retained.path)
    read(retainedJar, retained.sha256)
    const dependencies = ['kafka-clients-' + this.env.KAFKA_VERSION, 'protobuf-java'].map((key) => {
      const input = manifest.inputs[key]
      const file = path.join(this.env.TELEMETRY_JAVA_CACHE, input.cachePath)
      read(file, input.sha256)
      return { file, sha256: input.sha256 }
    })
    return {
      original,
      retainedJar,
      dependencies,
      sourceSha256: sha(original),
      retainedSha256: retained.sha256,
    }
  }

  /**
   * @returns {object} complete build receipt, published only after the derived jar exists
   * @throws {Error} on a stale directory, unverified input or failed bounded compiler/package command
   */
  prepare() {
    const inputs = this.inputs()
    fs.mkdirSync(this.directory, { recursive: false })
    const classes = path.join(this.directory, 'classes')
    fs.mkdirSync(classes)
    const source = path.join(this.directory, 'CountingTelemetryReporter.java')
    const prepared = LongWindowReporter.source(inputs.original)
    fs.writeFileSync(source, prepared, { flag: 'wx' })
    const run = (command, args) =>
      this.run(command, args, {
        cwd: classes,
        env: this.env,
        timeout: 60000,
        stdio: 'pipe',
        maxBuffer: 1024 * 1024,
      })
    run('jar', ['--extract', '--file', inputs.retainedJar])
    run('javac', [
      '--release',
      '17',
      '-Xlint:all',
      '-cp',
      [inputs.retainedJar, ...inputs.dependencies.map((input) => input.file)].join(path.delimiter),
      '-d',
      classes,
      source,
    ])
    run('jar', ['--create', '--date=2026-01-01T00:00:00Z', '--file', this.jar, '-C', classes, '.'])
    const receipt = {
      diagnostic: true,
      acceptanceEligible: false,
      line: this.env.KAFKA_VERSION,
      sinkBytes: SINK_BYTES,
      sinkBudget: SINK_BUDGET,
      sourceSha256: inputs.sourceSha256,
      preparedSourceSha256: sha(prepared),
      retainedSha256: inputs.retainedSha256,
      dependencies: inputs.dependencies,
      sha256: sha(fs.readFileSync(this.jar)),
    }
    const services = Object.fromEntries(
      ['kafka1', 'kafka2', 'kafka3'].map((name) => [
        name,
        {
          volumes: [
            {
              type: 'bind',
              source: this.jar,
              target: '/opt/kafka/libs/counting-telemetry-reporter.jar',
              read_only: true,
            },
          ],
        },
      ])
    )
    fs.writeFileSync(this.compose, JSON.stringify({ services }), { flag: 'wx' })
    fs.writeFileSync(path.join(this.directory, 'receipt.json'), JSON.stringify(receipt), {
      flag: 'wx',
    })
    return receipt
  }

  /**
   * @returns {object} verified derived jar identity and Compose override path for CI ownership
   * @throws {Error} if source, limits, inputs, generated source, jar or override changed after preparation
   */
  read() {
    const inputs = this.inputs()
    const receipt = JSON.parse(fs.readFileSync(path.join(this.directory, 'receipt.json')))
    assert.equal(receipt.diagnostic, true)
    assert.equal(receipt.acceptanceEligible, false)
    assert.equal(receipt.line, this.env.KAFKA_VERSION)
    assert.equal(receipt.sinkBytes, SINK_BYTES)
    assert.deepEqual(receipt.sinkBudget, SINK_BUDGET)
    assert.equal(receipt.sourceSha256, inputs.sourceSha256)
    assert.equal(receipt.retainedSha256, inputs.retainedSha256)
    assert.deepEqual(receipt.dependencies, inputs.dependencies)
    const source = fs.readFileSync(
      path.join(this.directory, 'CountingTelemetryReporter.java'),
      'utf8'
    )
    assert.equal(source, LongWindowReporter.source(inputs.original))
    assert.equal(sha(source), receipt.preparedSourceSha256)
    assert.equal(sha(fs.readFileSync(this.jar)), receipt.sha256)
    const { services } = JSON.parse(fs.readFileSync(this.compose))
    assert.deepEqual(Object.keys(services).sort(), ['kafka1', 'kafka2', 'kafka3'])
    for (const service of Object.values(services))
      assert.deepEqual(service, {
        volumes: [
          {
            type: 'bind',
            source: this.jar,
            target: '/opt/kafka/libs/counting-telemetry-reporter.jar',
            read_only: true,
          },
        ],
      })
    return { ...receipt, compose: this.compose }
  }
}

if (require.main === module) new LongWindowReporter().prepare()

module.exports = { LongWindowReporter, DIRECTORY }
