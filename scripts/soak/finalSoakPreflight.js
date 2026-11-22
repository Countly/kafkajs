'use strict'
/**
 * Final soak preflight: run by the runbook after the cluster is up and before the driver starts.
 * Exits non-zero on any miss and writes `preflight.json` (the host census and broker files the
 * header binds) into the artifact directory either way.
 *
 * Usage:
 *   KAFKA_BROKERS=... TELEMETRY_EVIDENCE_DIR=... node scripts/soak/finalSoakPreflight.js \
 *     --expect-sha <40-hex> --mode smoke|rehearsal|acceptance --out <artifact dir> [--diagnostic]
 *   TELEMETRY_JAVA_CACHE=... node scripts/soak/finalSoakPreflight.js --prepare-reporter --out <artifact dir>
 *
 * Checks: Node 22, 24 or 26 on Linux with a boot id; dependencies installed by `npm ci`; a clean
 * checkout at the expected SHA; all three brokers in metadata; `share.version` finalized and read
 * back; the run's client-metrics subscription created and read back, then one decoded telemetry push
 * with metrics from a probe producer and a probe consumer; per-node
 * broker provenance (image digests from `docker image inspect`, or operator evidence for a supplied
 * cluster); the prepared reporter's file cap, mounted jar digest and the cap each broker's load proof
 * reports; a quiet host (no agent or test
 * process other than this run's own broker containers, judged only once every broker container root
 * is visible to the census), MemAvailable, free disk and load within the matrix floors. With `--diagnostic` (smoke
 * or rehearsal, for a hosted CI runner) the four host-capacity checks are still taken and written but do not
 * fail the preflight, and the record says so; such a run is never acceptance evidence.
 */
const crypto = require('node:crypto')
const fs = require('node:fs')
const os = require('node:os')
const path = require('node:path')
const { execFileSync } = require('node:child_process')

const ROOT = path.resolve(__dirname, '../..')
const NODE_MAJORS = [22, 24, 26]
const NODES = [1, 2, 3]
const PROBES = ['producer', 'consumer']
const DIGEST = /^sha256:[0-9a-f]{64}$/
const SHA = /^[0-9a-f]{64}$/
/** The checks a diagnostic smoke records without requiring them. */
const HOST_CHECKS = ['quiet-host', 'memory', 'disk', 'load']
/** The run's client-metrics subscription: every metric, pushed within the matrix's interval. */
const SUBSCRIPTION = 'final-soak'
/** Process names whose presence means the host is not paused for the run. */
const BUSY = /^(node|jest|npm|npx|codex|claude|java|javac|mvn|gradle|cargo|go|python3?|pytest)$/

/** @returns {number} monotonic milliseconds since boot, the run clock's base */
const monotonicMs = () => Number(process.hrtime.bigint()) / 1e6
/** @param {string|Buffer} data @returns {string} */
const sha256 = (data) => crypto.createHash('sha256').update(data).digest('hex')

/**
 * Judges preflight facts against the matrix floors. Gathering is separate (`read`), so every check
 * is exercised without a cluster.
 */
class Preflight {
  /**
   * @param {object} options
   * @param {string} options.sha the expected candidate commit
   * @param {string} options.mode smoke, rehearsal or acceptance
   * @param {object} options.matrix the reviewed matrix
   * @param {() => Promise<object>} [options.read] gathers the facts; the live gatherer by default
   * @param {boolean} [options.diagnostic] smoke or rehearsal: host-capacity misses are recorded,
   *   not failed
   * @throws {Error} for a diagnostic acceptance preflight
   */
  constructor({ sha, mode, matrix, read, diagnostic = false }) {
    if (diagnostic && mode === 'acceptance')
      throw new Error('--diagnostic is accepted only in smoke mode or rehearsal mode')
    this.sha = sha
    this.mode = mode
    this.matrix = matrix
    this.read = read
    this.diagnostic = diagnostic
  }

  /**
   * @returns {Promise<{ checks: { name: string, ok: boolean, required: boolean, reason?: string }[],
   *   failed: string[], exitCode: number, facts: object }>} every check (with the reason a check
   *   gave for failing), the names that failed and the process exit code (only required checks
   *   fail it)
   */
  async run() {
    const facts = await this.read()
    const checks = Object.entries(this.checks(facts)).map(([name, check]) => {
      let result = false
      try {
        result = check()
      } catch {
        result = false
      }
      const required = !(this.diagnostic && HOST_CHECKS.includes(name))
      const reason = typeof result === 'string' ? { reason: result } : {}
      return { name, ok: result === true, required, ...reason }
    })
    const failed = checks.filter((c) => !c.ok).map((c) => c.name)
    const blocking = checks.filter((c) => !c.ok && c.required)
    return { checks, failed, exitCode: blocking.length === 0 ? 0 : 1, facts }
  }

  /**
   * @param {object} f the gathered facts
   * @returns {Record<string, () => boolean|string>} named checks; a check passes only on `true`, and
   *   a string is its reason for failing
   */
  checks(f) {
    const host = this.matrix.host
    const share = f.share.finalized['share.version']
    return {
      'node-version': () => NODE_MAJORS.includes(Number(/^v(\d+)\./.exec(f.node)?.[1])),
      clock: () => f.clock.platform === 'linux' && f.clock.bootId.length > 0,
      dependencies: () => f.deps.installed === true && f.deps.lockMatches === true,
      'clean-checkout': () => f.git.porcelain === '',
      'expected-sha': () => /^[0-9a-f]{40}$/.test(this.sha) && f.git.head === this.sha,
      'cluster-ready': () => [...f.cluster.brokers].sort().join() === NODES.join(),
      'share-readback': () => share !== undefined && share.maxVersionLevel >= 1,
      'telemetry-subscription': () =>
        f.telemetry.subscription?.metrics === '*' &&
        Number(f.telemetry.subscription?.intervalMs) > 0 &&
        Number(f.telemetry.subscription?.intervalMs) <=
          this.matrix.traffic.telemetryPushMaxMs.value,
      'telemetry-proof': () =>
        PROBES.every((probe) =>
          f.telemetry.pushes.some((p) => p.probe === probe && p.decoded === true && p.metrics > 0)
        ),
      'broker-provenance': () =>
        f.provenance.nodes.map((n) => n.node).join() === NODES.join() &&
        f.provenance.nodes.every((n) =>
          f.provenance.kind === 'image' ? DIGEST.test(n.digest) : SHA.test(n.evidenceSha256)
        ),
      reporter: () =>
        f.reporter.sinkBytes === host.brokerReporterFileBytes.value &&
        SHA.test(f.reporter.jarSha256) &&
        f.reporter.mounted === f.reporter.jarSha256 &&
        f.reporter.loadedCaps.length === NODES.length &&
        f.reporter.loadedCaps.every((cap) => cap === f.reporter.sinkBytes),
      'quiet-host': () => {
        if (f.host.view?.verified !== true)
          return `the process census cannot see the host: ${f.host.view?.reason ?? 'no view'}`
        const busy = f.host.census.filter((p) => p.class === 'agent' || p.class === 'test')
        return (
          busy.length === 0 || `busy: ${busy.map((p) => `${p.name} (pid ${p.pid})`).join(', ')}`
        )
      },
      memory: () => f.host.memAvailable >= host.minMemAvailableBytes.value,
      disk: () => f.host.freeDisk >= host.minFreeDiskBytes.value,
      load: () => f.host.load1m <= host.maxLoadAverage1m.value,
    }
  }
}

/** @param {string} command @param {string[]} args @returns {string} trimmed stdout */
const run = (command, args, options = {}) =>
  execFileSync(command, args, { cwd: ROOT, encoding: 'utf8', ...options }).trim()

/**
 * Classes one `ps -eo pid=,ppid=,pcpu=,rss=,comm=` table for the paused-work census. A process whose
 * ancestry reaches a visible root of this run's broker containers is `own-broker` and names its node
 * and container; any other agent or test process, a foreign `java` included, keeps its class.
 * @param {string} table the `ps` output
 * @param {{ self?: number, roots?: { node: number, container: string, pid: number,
 *   visible: boolean }[] }} [options] the preflight's own pid (it and its children are left out) and
 *   the broker container roots from `brokerRoots`
 * @returns {{ pid: number, name: string, cpu: number, rss: number, class: string, node?: number,
 *   container?: string }[]}
 */
function classifyCensus(table, { self = process.pid, roots = [] } = {}) {
  const rows = table
    .split('\n')
    .map((line) => line.trim().split(/\s+/))
    .map(([pid, ppid, cpu, rss, name]) => ({
      pid: Number(pid),
      ppid: Number(ppid),
      cpu,
      rss,
      name,
    }))
  const parent = new Map(rows.map((r) => [r.pid, r.ppid]))
  const rootOf = new Map(roots.filter((r) => r.visible).map((r) => [r.pid, r]))
  const ownRoot = (pid) => {
    for (let p = pid, hops = 0; p !== undefined && hops <= rows.length; p = parent.get(p), hops++)
      if (rootOf.has(p)) return rootOf.get(p)
    return undefined
  }
  return rows
    .filter((r) => r.pid !== self && r.ppid !== self)
    .map(({ pid, cpu, rss, name }) => {
      const root = ownRoot(pid)
      const entry = { pid, name, cpu: Number(cpu), rss: Number(rss) * 1024 }
      if (root) return { ...entry, class: 'own-broker', node: root.node, container: root.container }
      const cls = /^(codex|claude)$/.test(name) ? 'agent' : BUSY.test(name) ? 'test' : 'other'
      return { ...entry, class: cls }
    })
}

/**
 * The paused-work census: pid, executable name, CPU and RSS, and a class. No environment, credential
 * or command-line argument is read.
 * @param {{ roots?: object[] }} [options] this run's broker container roots, from `brokerRoots`
 * @returns {object[]} the classed processes, as `classifyCensus` returns them
 */
const census = ({ roots = [] } = {}) =>
  classifyCensus(run('ps', ['-eo', 'pid=,ppid=,pcpu=,rss=,comm=']), { roots })

/**
 * The host pids of this run's broker containers (the containers `captureBroker` recorded), so the
 * census can tell the run's own brokers from foreign processes by ancestry rather than by name. A
 * root is `visible` only when that pid's cgroup names its container: in another pid namespace
 * (Docker in a sidecar) the pid is some other process, so it excludes nothing. A supplied cluster
 * has no containers here.
 * @param {object} provenance the captured broker provenance
 * @param {{ inspect?: (container: string) => string, cgroup?: (pid: number) => string }} [docker]
 *   reads a container's host pid and a pid's cgroup file
 * @returns {{ node: number, container: string, pid: number, visible: boolean }[]}
 * @throws {Error} when Docker does not answer or a broker container has no running process
 */
function brokerRoots(
  provenance,
  {
    inspect = (container) => run('docker', ['inspect', '--format', '{{.State.Pid}}', container]),
    cgroup = (pid) => fs.readFileSync(`/proc/${pid}/cgroup`, 'utf8'),
  } = {}
) {
  if (provenance.kind !== 'image') return []
  return provenance.nodes.map(({ node, container }) => {
    let pid
    try {
      pid = Number(inspect(container))
    } catch (e) {
      throw new Error(`docker did not report the pid of kafka${node} (${container}): ${e.message}`)
    }
    if (!Number.isInteger(pid) || pid <= 0)
      throw new Error(`kafka${node} (${container}) has no running process`)
    let visible = false
    try {
      visible = container.length > 0 && cgroup(pid).includes(container)
    } catch {
      visible = false
    }
    return { node, container, pid, visible }
  })
}

/**
 * Whether the census sees the host's processes rather than only its own pid namespace. With this
 * run's Compose cluster that holds only when every broker container root is visible (`brokerRoots`):
 * a census in another pid namespace sees neither the run's brokers nor foreign work, so an empty one
 * proves nothing. A supplied cluster has no container to confirm the view with, so it is never
 * verified.
 * @param {{ kind: string }} provenance the captured broker provenance
 * @param {{ node: number, pid: number, visible: boolean }[]} roots from `brokerRoots`
 * @returns {{ verified: boolean, reason: string|null }} the reason names what could not be confirmed
 */
function censusView(provenance, roots) {
  if (provenance.kind !== 'image')
    return {
      verified: false,
      reason: 'a supplied cluster has no broker container to confirm the view with',
    }
  if (roots.length !== NODES.length)
    return {
      verified: false,
      reason: `expected ${NODES.length} broker container roots, found ${roots.length}`,
    }
  const hidden = roots.filter((r) => r.visible !== true)
  if (hidden.length > 0)
    return {
      verified: false,
      reason: `${hidden.map((r) => `kafka${r.node} (pid ${r.pid})`).join(', ')} not visible in this pid namespace`,
    }
  return { verified: true, reason: null }
}

/** @returns {number} Linux MemAvailable in bytes */
const memAvailable = () =>
  Number(/MemAvailable:\s+(\d+) kB/.exec(fs.readFileSync('/proc/meminfo', 'utf8'))[1]) * 1024

/**
 * Builds a private cluster of `kafka` the way `Kafka#admin` builds its own (no auto topic creation)
 * with telemetry and instrumentation off: the public API exposes no per-node ApiVersions ranges.
 * @param {object} kafka a `Kafka`
 * @returns {object} an unconnected cluster
 * @throws {Error} when the client has no private cluster factory
 */
function privateCluster(kafka) {
  const factory = Object.getOwnPropertySymbols(kafka).find(
    (s) => s.description === 'private:Kafka:createCluster'
  )
  if (typeof kafka[factory] !== 'function')
    throw new Error('this client has no private cluster factory to read ApiVersions ranges with')
  return kafka[factory]({
    telemetryMetrics: null,
    allowAutoTopicCreation: false,
    instrumentationEmitter: null,
  })
}

/**
 * Captures per-node broker provenance and capabilities into the artifact. The private cluster is
 * disconnected whether or not every node answers.
 * @param {object} kafka a connected-capable `Kafka`
 * @param {object} admin a connected admin
 * @param {string} out artifact directory
 * @returns {Promise<{ provenance: object, broker: object }>}
 * @throws {Error} when a node is missing from metadata or does not answer
 */
async function captureBroker(kafka, admin, out) {
  fs.mkdirSync(path.join(out, 'broker'), { recursive: true })
  const cluster = privateCluster(kafka)
  const nodes = []
  try {
    await cluster.connect()
    await cluster.refreshMetadata()
    for (const node of NODES) {
      const broker = await cluster.findBroker({ nodeId: String(node) })
      const features = await admin.describeFeatures({ brokerId: node })
      nodes.push({
        node,
        apiRanges: Object.entries(broker.versions).map(([apiKey, v]) => ({
          apiKey: Number(apiKey),
          minVersion: v.minVersion,
          maxVersion: v.maxVersion,
        })),
        supportedFeatures: Object.entries(features.supportedFeatures ?? {}).map(([name, v]) => ({
          name,
          minVersion: v.minVersion,
          maxVersion: v.maxVersion,
        })),
        finalizedFeatures: Object.entries(features.finalizedFeatures ?? {}).map(([name, v]) => ({
          name,
          ...v,
        })),
      })
    }
  } finally {
    await cluster.disconnect()
  }
  fs.writeFileSync(path.join(out, 'broker/capabilities.json'), JSON.stringify({ nodes }) + '\n')
  const supplied = process.env.FINAL_SOAK_SUPPLIED_EVIDENCE
  let provenance
  if (supplied) {
    const evidence = JSON.parse(fs.readFileSync(supplied, 'utf8'))
    provenance = {
      kind: 'supplied',
      nodes: evidence.nodes.map((n) => {
        const bytes = fs.readFileSync(n.file)
        const file = `broker/evidence-${n.node}${path.extname(n.file)}`
        fs.writeFileSync(path.join(out, file), bytes)
        return {
          node: n.node,
          version: n.version,
          evidenceFile: file,
          evidenceSha256: sha256(bytes),
        }
      }),
    }
  } else {
    provenance = {
      kind: 'image',
      nodes: NODES.map((node) => {
        const container = run('docker', ['compose', 'ps', '-q', `kafka${node}`])
        const image = run('docker', ['inspect', '--format', '{{.Image}}', container])
        const inspect = run('docker', ['image', 'inspect', image])
        const inspectFile = `broker/inspect-${node}.json`
        fs.writeFileSync(path.join(out, inspectFile), inspect)
        const [details] = JSON.parse(inspect)
        const digest = (details.RepoDigests?.[0] ?? '').split('@')[1] ?? ''
        return {
          node,
          image: details.RepoTags?.[0] ?? image,
          digest,
          container,
          version: process.env.KAFKA_VERSION ?? 'unknown',
          inspectFile,
          inspectSha256: sha256(inspect),
        }
      }),
    }
  }
  fs.writeFileSync(
    path.join(out, 'broker/provenance.json'),
    JSON.stringify({ nodes: provenance.nodes }) + '\n'
  )
  return {
    provenance,
    broker: {
      provenanceKind: provenance.kind,
      provenanceFile: 'broker/provenance.json',
      capabilitiesFile: 'broker/capabilities.json',
      composeFile: 'broker/compose.json',
    },
  }
}

/**
 * Pushes from one telemetry-enabled probe producer and one probe consumer and waits for the broker
 * reporter's decoded line of each.
 * @param {object} kafkaOptions the `Kafka` options every preflight client starts from
 * @param {string} sinkDir the host reporter directory
 * @param {number} waitMs how long to wait for both pushes
 * @returns {Promise<object[]>} the decoded pushes found, by probe
 */
async function telemetryProof(kafkaOptions, sinkDir, waitMs) {
  const { Kafka } = require('../../index')
  const tag = crypto.randomBytes(4).toString('hex')
  const producer = new Kafka({
    ...kafkaOptions,
    clientId: `preflight-producer-${tag}`,
  }).producer({ enableMetricsPush: true })
  const consumer = new Kafka({
    ...kafkaOptions,
    clientId: `preflight-consumer-${tag}`,
  }).consumer({ groupId: `preflight-${tag}`, enableMetricsPush: true })
  await producer.connect()
  await consumer.connect()
  const deadline = Date.now() + waitMs
  const found = []
  try {
    while (Date.now() < deadline && found.length < PROBES.length) {
      await new Promise((resolve) => setTimeout(resolve, 2000))
      found.length = 0
      for (const file of fs.readdirSync(sinkDir).filter((f) => /^exports-\d+\.jsonl$/.test(f))) {
        for (const line of fs
          .readFileSync(path.join(sinkDir, file), 'utf8')
          .split('\n')
          .filter(Boolean)) {
          const push = JSON.parse(line)
          const probe = PROBES.find((p) => push.clientId === `preflight-${p}-${tag}`)
          if (probe && !found.some((f) => f.probe === probe))
            found.push({
              probe,
              metrics: Number(push.metricsTotal ?? 0),
              decoded: push.parse === 'ok' && push.truncated !== true,
            })
        }
      }
    }
  } finally {
    await consumer.disconnect()
    await producer.disconnect()
  }
  return found
}

/**
 * Creates (or keeps) the run's client-metrics subscription for every client and reads it back:
 * all metrics, pushed at half the matrix's push ceiling.
 * @param {object} admin a connected admin
 * @param {object} matrix the reviewed matrix
 * @returns {Promise<{ metrics: string|null, intervalMs: string|null }>} the values read back
 */
async function telemetrySubscription(admin, matrix) {
  const entries = {
    metrics: '*',
    'interval.ms': String(matrix.traffic.telemetryPushMaxMs.value / 2),
  }
  await admin.incrementalAlterConfigs({
    resources: [
      {
        type: 16,
        name: SUBSCRIPTION,
        configEntries: Object.entries(entries).map(([name, value]) => ({
          name,
          operation: 0,
          value,
        })),
      },
    ],
  })
  const { resources } = await admin.describeConfigs({
    resources: [{ type: 16, name: SUBSCRIPTION, configNames: Object.keys(entries) }],
  })
  const value = (name) =>
    resources[0]?.configEntries.find((e) => e.configName === name)?.configValue ?? null
  return { metrics: value('metrics'), intervalMs: value('interval.ms') }
}

/** @returns {object} the `Kafka` options of the brokers in `KAFKA_BROKERS` @throws {Error} when unset */
const brokerOptions = () => {
  if (!process.env.KAFKA_BROKERS) throw new Error('KAFKA_BROKERS must list the brokers')
  const { logLevel } = require('../../index')
  return { brokers: process.env.KAFKA_BROKERS.split(','), logLevel: logLevel.NOTHING }
}

/**
 * Gathers every preflight fact from the live host, checkout and cluster. Each cluster step is
 * named in `progress.stage` before it starts and its result kept in `progress.completed` once it
 * ends, so a failure record shows how far gathering got and what it read.
 * @param {object} options
 * @param {string} options.out the artifact directory
 * @param {object} options.matrix the reviewed matrix
 * @param {object} [options.kafkaOptions] the `Kafka` options every preflight client starts from;
 *   the brokers in `KAFKA_BROKERS` by default
 * @param {{ stage: string|null, completed: object }} [options.progress] filled as steps end
 * @returns {Promise<object>} the facts `Preflight` judges, plus the header fields it writes
 * @throws {Error} the first failing step's error
 */
async function gatherFacts({
  out,
  matrix,
  kafkaOptions = brokerOptions(),
  progress = { stage: null, completed: {} },
}) {
  const { Kafka } = require('../../index')
  const step = async (name, work) => {
    progress.stage = name
    const value = await work()
    progress.completed[name] = value
    return value
  }
  const kafka = new Kafka({ ...kafkaOptions, clientId: 'preflight-admin' })
  const admin = kafka.admin()
  await step('admin-connect', () => admin.connect())
  try {
    const { brokers } = await step('describe-cluster', () => admin.describeCluster())
    const features = await step('describe-features', () => admin.describeFeatures())
    const { provenance, broker } = await step('capture-broker', () =>
      captureBroker(kafka, admin, out)
    )
    const subscription = await step('telemetry-subscription', () =>
      telemetrySubscription(admin, matrix)
    )
    const pushes = await step('telemetry-proof', () =>
      telemetryProof(
        kafkaOptions,
        process.env.TELEMETRY_EVIDENCE_DIR,
        2 * matrix.traffic.telemetryPushMaxMs.value
      )
    )
    progress.stage = 'reporter'
    const receipt = JSON.parse(fs.readFileSync(path.join(out, 'reporter/receipt.json'), 'utf8'))
    const mounted = NODES.map(
      (node) =>
        run('docker', [
          'compose',
          'exec',
          '-T',
          `kafka${node}`,
          'sha256sum',
          '/opt/kafka/libs/counting-telemetry-reporter.jar',
        ]).split(/\s+/)[0]
    )
    const loadedCaps = NODES.map((node) => {
      const proof = path.join(process.env.TELEMETRY_EVIDENCE_DIR, `reporter-${node}.json`)
      return JSON.parse(fs.readFileSync(proof, 'utf8')).maxFileBytes
    })
    progress.stage = 'host'
    const roots = brokerRoots(provenance)
    const statfs = fs.statfsSync(out)
    return {
      node: process.version,
      clock: {
        platform: process.platform,
        bootId: fs.readFileSync('/proc/sys/kernel/random/boot_id', 'utf8').trim(),
      },
      git: {
        head: run('git', ['rev-parse', 'HEAD']),
        porcelain: run('git', ['status', '--porcelain']),
      },
      deps: {
        installed: fs.existsSync(path.join(ROOT, 'node_modules/.package-lock.json')),
        lockMatches: tryRun('npm', ['ls', '--depth=0']),
      },
      cluster: { brokers: brokers.map((b) => b.nodeId) },
      share: { finalized: features.finalizedFeatures ?? {} },
      telemetry: { subscription, pushes },
      provenance,
      broker,
      reporter: {
        sinkBytes: receipt.sinkBytes,
        jarSha256: receipt.sha256,
        mounted: mounted.every((m) => m === mounted[0]) ? mounted[0] : 'mixed',
        loadedCaps,
      },
      host: {
        memAvailable: memAvailable(),
        freeDisk: statfs.bavail * statfs.bsize,
        load1m: os.loadavg()[0],
        census: census({ roots }),
        roots,
        view: censusView(provenance, roots),
      },
    }
  } finally {
    await admin.disconnect()
  }
}

/**
 * @param {string} command @param {string[]} args
 * @returns {boolean} whether the command exits zero; its output is discarded
 */
const tryRun = (command, args) => {
  try {
    execFileSync(command, args, { cwd: ROOT, stdio: 'ignore' })
    return true
  } catch {
    return false
  }
}

/**
 * The final run's reporter source: the retained source through the repository's own per-line
 * adaptation (`prepare-reporter-source.sh`, the step every reporter build takes; for Kafka 4.4.0 it
 * moves the `AppInfoParser` import), then with only its file cap changed.
 * @param {{ version: string, sinkBytes: number, dir: string }} options the Kafka line, the file cap
 *   and a fresh scratch directory for the adapted source
 * @returns {string} the prepared source
 * @throws {Error} for an unsupported Kafka line or a retained source without exactly one file cap
 */
function reporterSource({ version, sinkBytes, dir }) {
  const { BoundedReporter } = require('./longWindowReporter')
  const script = path.join(ROOT, 'scripts/e2e/telemetry/prepare-reporter-source.sh')
  const adapted = execFileSync('bash', [script, version, dir], { encoding: 'utf8' }).trim()
  return BoundedReporter.source(fs.readFileSync(adapted, 'utf8'), sinkBytes)
}

/**
 * Prepares the final run's reporter jar from the retained source, adapted to the Kafka line and with
 * only its file cap changed to the matrix's per-node cap, and the Compose override that mounts it on
 * all three brokers.
 * @param {{ out: string, matrix: object }} options
 * @returns {object} the receipt written to `reporter/receipt.json`
 */
function prepareReporter({ out, matrix }) {
  const fixture = path.join(ROOT, 'scripts/e2e/telemetry')
  const manifest = JSON.parse(fs.readFileSync(path.join(fixture, 'fixtures/manifest.json'), 'utf8'))
  const line = manifest.lines[process.env.KAFKA_VERSION]
  if (!line) throw new Error(`no pinned reporter inputs for Kafka ${process.env.KAFKA_VERSION}`)
  const original = fs.readFileSync(path.join(fixture, 'CountingTelemetryReporter.java'), 'utf8')
  if (sha256(original) !== manifest.scripts['CountingTelemetryReporter.java'])
    throw new Error('retained reporter source changed')
  const retainedJar = path.join(ROOT, line.reporterJar.path)
  if (sha256(fs.readFileSync(retainedJar)) !== line.reporterJar.sha256)
    throw new Error('retained reporter jar changed')
  const dependencies = ['kafka-clients-' + process.env.KAFKA_VERSION, 'protobuf-java'].map(
    (key) => {
      const input = manifest.inputs[key]
      const file = path.join(process.env.TELEMETRY_JAVA_CACHE, input.cachePath)
      if (sha256(fs.readFileSync(file)) !== input.sha256)
        throw new Error(`reporter input ${key} changed`)
      return file
    }
  )
  const dir = path.join(out, 'reporter')
  const classes = path.join(dir, 'classes')
  fs.mkdirSync(classes, { recursive: true })
  const sinkBytes = matrix.host.brokerReporterFileBytes.value
  const prepared = reporterSource({
    version: process.env.KAFKA_VERSION,
    sinkBytes,
    dir: path.join(dir, 'adapted'),
  })
  const source = path.join(dir, 'CountingTelemetryReporter.java')
  fs.writeFileSync(source, prepared, { flag: 'wx' })
  const jar = path.join(dir, 'reporter.jar')
  const exec = (command, args) =>
    execFileSync(command, args, { cwd: classes, stdio: 'pipe', timeout: 60000 })
  exec('jar', ['--extract', '--file', retainedJar])
  exec('javac', [
    '--release',
    '17',
    '-cp',
    [retainedJar, ...dependencies].join(path.delimiter),
    '-d',
    classes,
    source,
  ])
  exec('jar', ['--create', '--date=2026-01-01T00:00:00Z', '--file', jar, '-C', classes, '.'])
  const services = Object.fromEntries(
    NODES.map((n) => [
      `kafka${n}`,
      {
        environment: { KAFKA_AUTO_CREATE_TOPICS_ENABLE: 'false' },
        volumes: [
          {
            type: 'bind',
            source: jar,
            target: '/opt/kafka/libs/counting-telemetry-reporter.jar',
            read_only: true,
          },
        ],
      },
    ])
  )
  fs.mkdirSync(path.join(out, 'broker'), { recursive: true })
  fs.writeFileSync(path.join(out, 'broker/compose.json'), JSON.stringify({ services }) + '\n')
  const receipt = {
    line: process.env.KAFKA_VERSION,
    sinkBytes,
    sourceSha256: sha256(original),
    preparedSourceSha256: sha256(prepared),
    retainedSha256: line.reporterJar.sha256,
    sha256: sha256(fs.readFileSync(jar)),
  }
  fs.writeFileSync(path.join(dir, 'receipt.json'), JSON.stringify(receipt) + '\n')
  return receipt
}

/**
 * The command-line entry.
 * @param {string[]} argv arguments after the script name
 * @param {{ kafkaOptions?: object }} [options] the `Kafka` options of the cluster; the brokers in
 *   `KAFKA_BROKERS` by default
 * @returns {Promise<number>} the exit code
 * @throws {Error} when the facts cannot be gathered, after writing the failure record
 */
async function main(argv, { kafkaOptions } = {}) {
  const flag = (name) => argv[argv.indexOf(name) + 1]
  const out = path.resolve(flag('--out'))
  const matrix = JSON.parse(fs.readFileSync(path.join(__dirname, 'finalSoakMatrix.json'), 'utf8'))
  if (argv.includes('--prepare-reporter')) {
    prepareReporter({ out, matrix })
    return 0
  }
  const clockFile = path.join(out, 'clock.json')
  if (!fs.existsSync(clockFile)) {
    const bootId = fs.readFileSync('/proc/sys/kernel/random/boot_id', 'utf8').trim()
    fs.writeFileSync(clockFile, JSON.stringify({ originMs: monotonicMs(), bootId }) + '\n')
  }
  const { originMs } = JSON.parse(fs.readFileSync(clockFile, 'utf8'))
  const sha = flag('--expect-sha')
  const mode = flag('--mode')
  const diagnostic = argv.includes('--diagnostic')
  let facts = null
  const progress = { stage: null, completed: {} }
  const preflight = new Preflight({
    sha,
    mode,
    matrix,
    diagnostic,
    read: async () => (facts = await gatherFacts({ out, matrix, kafkaOptions, progress })),
  })
  let result
  try {
    result = await preflight.run()
  } catch (e) {
    const record = {
      sha,
      mode,
      diagnostic,
      checks: [],
      failed: ['facts'],
      stage: progress.stage,
      completed: progress.completed,
      error: String(e.stack),
    }
    fs.writeFileSync(path.join(out, 'preflight.json'), JSON.stringify(record, null, 2) + '\n')
    throw e
  }
  const measuredAt = monotonicMs() - originMs
  fs.writeFileSync(
    path.join(out, 'preflight.json'),
    JSON.stringify(
      {
        sha,
        mode,
        diagnostic,
        checks: result.checks,
        failed: result.failed,
        census: facts.host.census,
        ownBrokers: facts.host.roots,
        censusView: facts.host.view,
        telemetry: facts.telemetry,
        broker: facts.broker,
        host: {
          cpus: os.cpus().length,
          totalMemoryBytes: os.totalmem(),
          availableMemoryBytes: facts.host.memAvailable,
          freeDiskBytes: facts.host.freeDisk,
          loadAverage1m: facts.host.load1m,
          docker: tryRun('docker', ['version'])
            ? run('docker', ['version', '--format', '{{.Server.Version}}'])
            : 'absent',
          measuredAt,
          pausedWork: !diagnostic && !result.failed.includes('quiet-host'),
        },
      },
      null,
      2
    ) + '\n'
  )
  for (const c of result.checks)
    console.log(
      `${c.ok ? 'ok  ' : c.required ? 'FAIL' : 'miss'} ${c.name}${c.reason ? `: ${c.reason}` : ''}`
    )
  return result.exitCode
}

if (require.main === module) {
  main(process.argv.slice(2)).then(
    (code) => (process.exitCode = code),
    (e) => {
      console.error(e.stack)
      process.exitCode = 1
    }
  )
}

module.exports = {
  Preflight,
  captureBroker,
  gatherFacts,
  main,
  tryRun,
  prepareReporter,
  reporterSource,
  census,
  classifyCensus,
  brokerRoots,
  censusView,
  telemetrySubscription,
}
