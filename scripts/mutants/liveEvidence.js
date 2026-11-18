/**
 * Guard evidence for live mutants: a record, written from the started Compose cluster, that proves which
 * tree, Node, broker build, wire capabilities, compose images and configuration a live verdict came from.
 * The runner runs a live record only with complete evidence for its own tree; the report checks the
 * evidence again, independently, before it counts a live verdict.
 *
 * Usage (inside the live job, after the cluster started): node scripts/mutants/liveEvidence.js --out <file>
 */
const fs = require('fs')
const { spawnSync } = require('child_process')

/** The PLAINTEXT host ports of the three brokers in `docker-compose.yml`. */
const BROKER_PORTS = [9092, 9095, 9098]

/** Writes and checks live guard evidence. */
class LiveEvidence {
  /** Every field a guard-evidence record must carry, non-empty. */
  static FIELDS = Object.freeze([
    'at',
    'lane',
    'node',
    'brokerVersion',
    'wireVersions',
    'config',
    'composeImages',
    'runId',
    'sha',
  ])

  /**
   * @param {unknown} value
   * @returns {boolean} whether the value is absent, an empty string, or an object or array that is
   *   empty or holds an empty value
   */
  static empty(value) {
    if (value === null || value === undefined || value === '') return true
    if (typeof value !== 'object') return false
    const values = Object.values(value)
    return values.length === 0 || values.some(LiveEvidence.empty)
  }

  /**
   * @param {object|null} evidence a guard-evidence record
   * @returns {string[]} the fields that are missing or empty, in {@link LiveEvidence.FIELDS} order
   */
  static missing(evidence) {
    return LiveEvidence.FIELDS.filter((f) => LiveEvidence.empty(evidence?.[f]))
  }

  /**
   * Why guard evidence cannot back a live verdict, or null when it can.
   * @param {object|null} evidence the record
   * @param {object} expected
   * @param {string} expected.sha the tree the verdict is for
   * @param {number} [expected.since] the run's start, in milliseconds; `at` must not be earlier
   * @param {number} [expected.until] the latest acceptable `at`, in milliseconds
   * @returns {string|null} the refusal, starting `guard evidence`
   */
  static refusal(evidence, { sha, since, until }) {
    if (!evidence || typeof evidence !== 'object') return 'guard evidence is missing'
    const missing = LiveEvidence.missing(evidence)
    if (missing.length > 0) return `guard evidence lacks ${missing.join(', ')}`
    if (evidence.lane !== 'docker') return `guard evidence lane is ${evidence.lane}, not docker`
    if (evidence.sha !== sha) return `guard evidence names another tree (${evidence.sha})`
    const at = Date.parse(evidence.at)
    if (
      Number.isNaN(at) ||
      (since !== undefined && at < since) ||
      (until !== undefined && at > until)
    )
      return `guard evidence at ${evidence.at} is outside the run`
    return null
  }

  /**
   * @param {string} file a guard-evidence file
   * @returns {object|null} its record, or null when it is absent or unreadable
   */
  static read(file) {
    try {
      return JSON.parse(fs.readFileSync(file, 'utf8'))
    } catch {
      return null
    }
  }

  /**
   * The compose images of the running cluster, from `docker compose images --format json` (an array, or
   * one object per line).
   * @returns {Record<string, string>} container → `repository:tag@image id`
   * @throws {Error} when the command fails or an image has no id
   */
  static composeImages() {
    const r = spawnSync('docker', ['compose', 'images', '--format', 'json'], { encoding: 'utf8' })
    if (r.status !== 0) throw new Error(`docker compose images failed: ${r.stderr}`)
    const text = r.stdout.trim()
    const entries = text.startsWith('[')
      ? JSON.parse(text)
      : text
          .split('\n')
          .filter(Boolean)
          .map((line) => JSON.parse(line))
    const images = {}
    for (const e of entries) {
      if (!e.ID) throw new Error(`compose image of ${e.ContainerName} has no id`)
      images[e.ContainerName] = `${e.Repository}:${e.Tag}@${e.ID}`
    }
    return images
  }

  /**
   * Collects guard evidence from the started cluster. All three brokers must offer the same API versions.
   * @param {object} [sources] injectable inputs; the defaults read the real cluster and tree
   * @returns {Promise<object>} the record, complete
   * @throws {Error} when the brokers disagree, a source fails or a field stays empty
   */
  static async collect(sources = {}) {
    const probe = require('../e2e/apiVersionsProbe')
    const io = {
      readApiVersions: probe.readApiVersions,
      readBrokerVersion: () => require('../../testHelpers/brokerVersion').readBrokerVersion(),
      composeImages: LiveEvidence.composeImages,
      treeSha: () => spawnSync('git', ['rev-parse', 'HEAD'], { encoding: 'utf8' }).stdout.trim(),
      now: Date.now,
      env: process.env,
      ...sources,
    }
    const offered = []
    for (const port of BROKER_PORTS)
      offered.push(probe.allVersionsOf((await io.readApiVersions({ port })).apiVersions))
    const flat = offered.map((v) => JSON.stringify(Object.entries(v).sort()))
    if (flat.some((v) => v !== flat[0])) throw new Error('the brokers offer different API versions')
    const { env } = io
    const evidence = {
      at: new Date(io.now()).toISOString(),
      lane: env.KAFKAJS_LANE,
      node: process.version,
      brokerVersion: io.readBrokerVersion(),
      wireVersions: offered[0],
      config: {
        compose: env.COMPOSE_FILE || 'docker-compose.yml',
        env: { NODE_ENV: env.NODE_ENV, BROKER_BOUNCE_E2E: env.BROKER_BOUNCE_E2E },
      },
      composeImages: io.composeImages(),
      runId: env.GITHUB_RUN_ID,
      sha: io.treeSha(),
    }
    const missing = LiveEvidence.missing(evidence)
    if (missing.length > 0) throw new Error(`guard evidence lacks ${missing.join(', ')}`)
    return evidence
  }

  /**
   * The CLI: collects the evidence and writes it to `--out`.
   * @param {string[]} args the command-line arguments
   * @param {object} [sources] as for {@link LiveEvidence.collect}
   * @returns {Promise<void>}
   * @throws {Error} without `--out`, or when collection fails
   */
  static async main(args, sources) {
    const at = args.indexOf('--out')
    const out = at === -1 ? undefined : args[at + 1]
    if (!out) throw new Error('usage: liveEvidence.js --out <file>')
    fs.writeFileSync(out, JSON.stringify(await LiveEvidence.collect(sources), null, 1))
  }
}

module.exports = { LiveEvidence }

if (require.main === module) {
  LiveEvidence.main(process.argv.slice(2)).catch((error) => {
    console.error(`liveEvidence: ${error.message}`)
    process.exitCode = 1
  })
}
