const assert = require('assert/strict')
const crypto = require('crypto')
const fs = require('fs')
const path = require('path')
const zlib = require('zlib')
const { performance } = require('perf_hooks')
const {
  TelemetryExportReader,
  verifyMetricWindows,
} = require('../../../testHelpers/telemetryFixtures')
const {
  SEGMENT_FORMAT,
  checkSegmentFiles,
  SCHEDULES,
  segmentCatalogue,
  segmentIdentities,
} = require('../../../testHelpers/telemetrySegments')

const {
  assertCatalogue,
  assertCadence,
  assertJavaEvidence,
  assertJavaCaptures,
  assertJavaSubscription,
  JAVA_REQUESTED,
  unexpectedPinnedJavaStderr,
  assertJsCounter,
  assertCustomShape,
  ordinaryOwnership,
} = require('./semantics')
const {
  confirmedDecisions,
  finalAcknowledgement,
  acquisitionStart,
  heldAcquisitions,
} = require('./shareEvidence')
const { cacheRetirement, connectionIdentity } = require('./cacheTrace')

const sha = (bytes) => crypto.createHash('sha256').update(bytes).digest('hex')
const KINDS = ['producer', 'consumer', 'admin', 'share']
const CAP = 16 * 1024 * 1024
const CONFIGURED_COMPOSE = 'docker-compose.telemetry.yml'
/** Broker lines with a committed reporter jar, the only lines the configured compose can run. */
const REPORTER_LINES = Object.freeze(['4.4.0', '4.3.1', '4.2.2'])
/** Broker lines the outcomes scenario accepts; lines without a reporter jar run in base mode only. */
const OUTCOMES_LINES = Object.freeze([...REPORTER_LINES, '4.1.2', '4.0.2'])
const LIVE = Object.freeze([
  'telemetry-prefix',
  'telemetry-custom',
  'telemetry-payload',
  'telemetry-java',
  'telemetry-outcomes',
  'telemetry-shutdown',
])
const UNSUPPORTED = 'telemetry-unsupported'
const CHURN = 'telemetry-churn'
const CHURN_KINDS = ['producer', 'consumer', 'admin', 'shareConsumer']
const JAVA_ROLES = ['producer', 'consumer', 'admin']
const FAULT_MODES = ['drop', 'blackhole', 'late']
const SHUTDOWN = 'telemetry-shutdown'
const SHUTDOWN_ROUNDS = 20
const FINAL_WINDOW_MS = 1100
const FINAL_CAP_MS = 1000
const SCHEDULING_MS = 100
/**
 * The unit titles that hold what the close-and-reopen case cannot observe through the public API:
 * private waiter, queue and snapshot release and internal listener identity.
 */
const SHUTDOWN_SEAMS = Object.freeze([
  [
    'src/telemetry/__tests__/resources.spec.js',
    'bounds timers queues listeners and sockets for %s',
  ],
  ['src/telemetry/__tests__/resources.spec.js', 'bounds independent id waiters for %s'],
  [
    'src/telemetry/__tests__/resources.spec.js',
    'holds one native job across one hundred retirements for %s with late %s',
  ],
  [
    'src/telemetry/__tests__/resources.spec.js',
    'holds the native result through public cycles for %s with late %s',
  ],
  [
    'src/telemetry/__tests__/resources.spec.js',
    'fails each deliberate fault with its own diagnostic for %s with late %s',
  ],
])
const SHUTDOWN_WORK_MS = 98000
const SHUTDOWN_CLEANUP_MS = 20000
const SHUTDOWN_TOTAL_MS = 118000
const BROKER_LISTENERS = Object.freeze({ 19092: '9092', 19095: '9095', 19098: '9098' })
const TRACE_LINE =
  /GET_TELEMETRY_SUBSCRIPTIONS|TRACE Removing client connection id|TRACE Client connection id \[[^\]\s]+\] is not found in the client instance cache/

/** @param {string} scenario selected case @returns {string} the only admitted wire-zero exception */
const zeroDisposition = (scenario) => {
  if (scenario === 'telemetry-prefix') return 'unmatched-requested-prefix'
  if (scenario === 'telemetry-custom') return 'all-custom-points-unregistered'
  return 'unexpected-wire-zero'
}

/** Owns one case's cleanup tasks in reverse allocation order. */
class Cleanup {
  /** @param {object} evidence mutable attempt retained even when the case fails */
  constructor(evidence) {
    this.evidence = evidence
    this.tasks = []
  }

  /** @param {function():Promise<void>|void} task release to attempt at case exit @returns {void} */
  defer(task) {
    this.tasks.push(task)
  }

  /** @returns {Promise<*[]>} ordered failures after every release was attempted */
  async run() {
    const errors = []
    while (this.tasks.length) {
      try {
        await this.tasks.pop()()
      } catch (error) {
        errors.push(error)
      }
    }
    return errors
  }
}

/**
 * Finalizes passing and failed attempts after every cleanup, preserving the original failure first.
 * Failed records have their own catalogue and an on-disk result even if broker stamping fails.
 * @param {string} name scenario name
 * @param {function(Cleanup,function(object):void,object):Promise<void>} body case and partial evidence
 * @param {object} [options] directory and stamped writer seam for offline failure controls
 * @returns {Promise<void>} when body, cleanup and persistence all succeed
 * @throws {*} original failure or an AggregateError with ordered cleanup/persistence causes
 */
const runCase = async (
  name,
  body,
  {
    directory = process.env.TELEMETRY_EVIDENCE_DIR,
    write = require('../../../testHelpers/e2eEvidence').writeEvidence,
    now = () => performance.now(),
  } = {}
) => {
  const startedAt = now()
  const evidence = {
    name,
    directory,
    observers: {},
    stage: 'setup',
    record: {
      run: {
        stem: `${name}-${crypto.randomBytes(8).toString('hex')}`,
        line: process.env.KAFKA_VERSION,
        project: process.env.COMPOSE_PROJECT_NAME,
      },
    },
  }
  const cleanup = new Cleanup(evidence)
  const errors = []
  try {
    await body(cleanup, (value) => Object.assign(evidence, value), evidence)
  } catch (error) {
    errors.push(error)
  }
  const bodyMs = now() - startedAt
  if (!errors.length) evidence.stage = 'cleanup'
  errors.push(...(await cleanup.run()))
  const cleanupMs = now() - startedAt - bodyMs
  const describe = (error, depth = 0) => ({
    name: error?.name || typeof error,
    message: String(error?.message ?? error).slice(0, 4096),
    ...(depth < 4 && Array.isArray(error?.errors)
      ? { causes: error.errors.slice(0, 16).map((cause) => describe(cause, depth + 1)) }
      : {}),
  })
  const record = {
    ...evidence.record,
    status: errors.length ? 'failed' : 'passed',
    timing: { bodyMs, cleanupMs },
  }
  const sealed = (evidence.segments ?? []).flatMap((owner) => owner.sealed())
  if (evidence.segments) record.format = SEGMENT_FORMAT
  if ([...LIVE, UNSUPPORTED].includes(name)) {
    record.oracles = {
      ...record.oracles,
      ...(record.results?.subscriptionDeletions
        ? { subscriptionDeletions: record.results.subscriptionDeletions }
        : {}),
      ...(record.results?.rawOracle ? { rawControl: record.results.rawOracle } : {}),
      ...Object.fromEntries(
        Object.entries(record.results?.arms ?? {}).map(([key, value]) => [key, value.inputs])
      ),
      ...(record.corpus ? { corpus: record.corpus.inputs } : {}),
      ...(record.results?.share?.inputs ? { share: record.results.share.inputs } : {}),
    }
    record.pushes = [
      ...Object.entries(evidence.observers).flatMap(([owner, observer]) =>
        observer
          .readTranscript()
          .filter((r) => r.apiKey === 72 && r.response?.errorCode === 0 && r.metricsBytes > 0)
          .map(({ requestKey }) => ({ owner, requestKey }))
      ),
      ...sealed.flatMap((pack) =>
        pack.accepted.map((requestKey) => ({ owner: pack.id, requestKey }))
      ),
    ]
    record.zeroPushes = [
      ...Object.entries(evidence.observers).flatMap(([owner, observer]) =>
        observer
          .readTranscript()
          .filter((r) => r.apiKey === 72 && r.metricsBytes === 0)
          .map(({ requestKey }) => ({ owner, requestKey, disposition: zeroDisposition(name) }))
      ),
      ...sealed.flatMap((pack) =>
        pack.empty.map((requestKey) => ({
          owner: pack.id,
          requestKey,
          disposition: zeroDisposition(name),
        }))
      ),
    ]
  }
  if (errors.length)
    record.failure = { stage: evidence.stage, causes: errors.map((error) => describe(error)) }
  try {
    record.sidecars = persistEvidence({ ...evidence, record })
    if (errors.length)
      fs.writeFileSync(
        path.join(directory, record.sidecars.root, 'failed-attempt.json'),
        JSON.stringify(record, null, 2)
      )
    write(errors.length ? `${name}-failed` : name, record)
  } catch (error) {
    errors.push(error)
  }
  if (errors.length === 1) throw errors[0]
  if (errors.length) throw new AggregateError(errors, 'Telemetry case and cleanup failed')
}

/**
 * Archives immutable per-case sidecars after natural client/observer cleanup, before another case
 * can append to the broker sinks. Stamped evidence references only these relative paths.
 * @param {object} evidence scenario name, record, run directory and observed clients
 * @returns {object} relative archive root and exact file sizes/digests
 * @throws {Error} for a missing observer receipt, symlink, non-file or bound violation
 */
const persistEvidence = ({ name, record, directory, observers, segments = [] }) => {
  const root = `case-${record.run.stem}`
  const target = path.join(directory, root)
  fs.mkdirSync(target)
  const files = []
  const archiveErrors = []
  const failed = record.status === 'failed'
  let total = 0
  const retain = (source, relative) => {
    const stat = fs.lstatSync(source)
    assert(stat.isFile() && !stat.isSymbolicLink(), 'Sidecar must be a regular file')
    total += stat.size
    assert(total <= CAP, 'Sidecar archive exceeds bound')
    const bytes = fs.readFileSync(source)
    const output = path.join(target, relative)
    fs.mkdirSync(path.dirname(output), { recursive: true })
    fs.writeFileSync(output, bytes, { flag: 'wx', mode: 0o600 })
    files.push({ path: `${root}/${relative}`, bytes: bytes.length, sha256: sha(bytes) })
  }
  const available = (work) => {
    try {
      work()
    } catch (error) {
      if (!failed) throw error
      archiveErrors.push(error.message)
    }
  }
  for (const file of fs.readdirSync(directory))
    if (/^(exports-\d+\.jsonl|reporter-\d+\.json|OVERFLOW.*|identity\.json)$/.test(file))
      available(() => retain(path.join(directory, file), file))
  const oracleFile = path.join(target, 'oracles.json')
  fs.writeFileSync(oracleFile, JSON.stringify(record.oracles ?? {}))
  const oracleBytes = fs.readFileSync(oracleFile)
  total += oracleBytes.length
  assert(total <= CAP, 'Sidecar archive exceeds bound')
  files.push({ path: `${root}/oracles.json`, bytes: oracleBytes.length, sha256: sha(oracleBytes) })
  const owners = {}
  for (const [kind, observer] of Object.entries(observers)) {
    available(() =>
      assert(
        fs.existsSync(path.join(observer.directory, 'transcript.json')),
        'Missing observer cleanup receipt'
      )
    )
    const subdir = path.basename(observer.directory)
    owners[kind] = `${root}/${subdir}/transcript.json`
    available(() => {
      for (const file of fs.readdirSync(observer.directory))
        if (
          /^(transcript\.json|raw-[a-f0-9]+\.bin|push-r\d+\.bin|decoded-r\d+\.json|decoder-\d+\.json)$/.test(
            file
          )
        )
          available(() => retain(path.join(observer.directory, file), `${subdir}/${file}`))
    })
  }
  const manifest = {
    scenario: name,
    archiveErrors,
    root,
    owners,
    files,
    cleanupReceipt: { path: 'ownership-receipt.json', project: record.run.project },
  }
  if (segments.length) {
    const identities = segmentIdentities(name)
    const entries = []
    for (const pack of segments.flatMap((owner) => owner.sealed())) {
      available(() => {
        const { kind, role } = identities.get(pack.id)
        for (const file of [pack.transcript, pack.bundle]) {
          retain(path.join(directory, 'segments', file.name), `segments/${file.name}`)
          assert.equal(files.at(-1).sha256, file.sha256, 'A sealed segment changed after sealing')
        }
        entries.push({
          id: pack.id,
          kind,
          role,
          transcript: `${root}/segments/${pack.transcript.name}`,
          bundle: `${root}/segments/${pack.bundle.name}`,
        })
      })
    }
    entries.sort((a, b) => (a.id < b.id ? -1 : 1))
    available(() => {
      const bytes = Buffer.from(
        JSON.stringify({ format: SEGMENT_FORMAT, scenario: name, segments: entries })
      )
      total += bytes.length
      assert(total <= CAP, 'Sidecar archive exceeds bound')
      fs.writeFileSync(path.join(target, 'index.json'), bytes, { flag: 'wx', mode: 0o600 })
      files.push({ path: `${root}/index.json`, bytes: bytes.length, sha256: sha(bytes) })
    })
    manifest.format = SEGMENT_FORMAT
    manifest.segments = entries
  }
  return manifest
}

/**
 * Pins scenario dimensions from the fixture contract before any evidence-driven iteration.
 * @param {object} record stamped scenario and independently selected broker mode
 * @returns {string[]} required physical observer owners
 * @throws {Error} for omitted, duplicate, foreign or inapplicable dimensions
 */
const requiredLiveCatalogue = (record) => {
  const scenario = record.sidecars.scenario
  const results = record.results
  const exact = (value, keys, label) =>
    assert.deepEqual(
      Object.keys(value ?? {}).sort(),
      [...keys].sort(),
      `Required ${label} catalogue differs`
    )
  if (['telemetry-prefix', 'telemetry-custom'].includes(scenario)) return ['producer']
  if (scenario === 'telemetry-payload') return ['producer-candidate']
  if (scenario === 'telemetry-java') {
    assert.equal(results.javaEvidenceVersion, 2, 'Missing Java evidence version 2')
    const roles = JAVA_ROLES
    exact(results.java, roles, 'Java role')
    exact(results.js, KINDS, 'JS role')
    exact(results.uuids.java, roles, 'Java UUID')
    exact(results.uuids.js, KINDS, 'JS UUID')
    for (const role of roles) assert.equal(results.java[role].uuid, results.uuids.java[role])
    for (const kind of KINDS) assert.equal(results.js[kind].uuid, results.uuids.js[kind])
    return KINDS.map((kind) => `js-${kind}`)
  }
  if (scenario === 'telemetry-outcomes') {
    assert(OUTCOMES_LINES.includes(record.run.line), 'Unsupported outcomes broker line')
    assert(['configured', 'base'].includes(record.mode), 'Unknown outcomes lane mode')
    const configured = record.mode === 'configured'
    if (configured)
      assert(
        REPORTER_LINES.includes(record.run.line),
        'Configured lane has no reporter jar for this line'
      )
    else
      assert.equal(record.run.composeSha256, undefined, 'Base lane claims the configured fixture')
    const modes = configured ? FAULT_MODES : []
    const arms = ['control', 'optin', ...modes]
    exact(results.arms, arms, 'ordinary arm')
    exact(results.faults, modes, 'fault mode')
    for (const mode of modes) {
      const faults = results.faults[mode]
      assert(Array.isArray(faults))
      assert.deepEqual(
        faults.map((f) => f.owner).sort(),
        [`${mode}-consumer`, `${mode}-producer`],
        'Required fault owner catalogue differs'
      )
      assert(faults.every((f) => f.mode === mode))
    }
    if (!configured) {
      exact(results.inapplicable, ['drop', 'blackhole', 'late'], 'inapplicable fault')
      assert(Object.values(results.inapplicable).every((v) => v === 'absent telemetry transport'))
      assertShareRefusal(results.share, record.run.line)
    } else {
      assert.equal(results.share?.recovery?.owner, 'share', 'Missing configured share recovery')
      assert.equal(results.share.recovery.mode, 'drop')
    }
    return [...arms.flatMap((arm) => [`${arm}-producer`, `${arm}-consumer`]), 'share']
  }
  assert.equal(scenario, SHUTDOWN)
  return []
}

/**
 * Requires one owner's accepted subscription and nonempty export for the claimed UUID.
 * @param {object} transcript actual owner stream
 * @param {string} uuid expected UUID
 * @returns {void}
 * @throws {Error} for a missing or foreign stream
 */
const assertOwnerUuid = (transcript, uuid) => {
  assert(
    transcript.records.some(
      (r) => r.apiKey === 71 && r.response?.errorCode === 0 && r.response.uuid === uuid
    ),
    'Missing owner subscription'
  )
  assert(
    transcript.records.some(
      (r) => r.apiKey === 72 && r.uuid === uuid && r.metricsBytes > 0 && r.response?.errorCode === 0
    ),
    'Missing owner export'
  )
}

/**
 * Authenticates the five-minute reporter's archived build receipt using the same cap-only source
 * derivation as the long-window builder. Compiler paths belong to the CI runner; their pinned
 * content hashes, rather than host paths, identify the ordered inputs during offline replay.
 * @param {object} receipt derived reporter receipt captured by CI ownership
 * @param {string} line scenario broker line
 * @param {object} pinned retained fixture manifest
 * @returns {string} authenticated built-jar digest to compare with every broker-mounted jar
 * @throws {Error} if the profile, source derivation, pinned inputs or built digest differ
 */
const churnReporterHash = (receipt, line, pinned) => {
  const { BoundedReporter } = require('../../soak/longWindowReporter')
  const { SINK_BYTES, SINK_BUDGET } = require('../../soak/churnReporter')
  assert(['4.2.2', '4.3.1'].includes(line), 'Unsupported churn reporter broker line')
  assert.equal(receipt.diagnostic, false, 'Churn reporter diagnostic profile differs')
  assert.equal(receipt.acceptanceEligible, true, 'Churn reporter acceptance eligibility differs')
  assert.equal(receipt.line, line, 'Churn reporter broker line differs')
  assert.equal(receipt.sinkBytes, SINK_BYTES, 'Churn reporter sink cap differs')
  assert.deepEqual(receipt.sinkBudget, SINK_BUDGET, 'Churn reporter sink budget differs')
  const source = fs.readFileSync(path.join(__dirname, 'CountingTelemetryReporter.java'), 'utf8')
  const sourceHash = pinned.scripts['CountingTelemetryReporter.java']
  assert.equal(sha(source), sourceHash, 'Churn reporter source identity differs')
  assert.equal(receipt.sourceSha256, sourceHash, 'Churn reporter source identity differs')
  assert.equal(
    receipt.preparedSourceSha256,
    sha(BoundedReporter.source(source, SINK_BYTES)),
    'Churn reporter prepared source differs'
  )
  assert.equal(
    receipt.retainedSha256,
    pinned.lines[line].reporterJar.sha256,
    'Churn reporter retained jar differs'
  )
  assert.deepEqual(
    receipt.dependencies?.map((input) => input.sha256),
    [`kafka-clients-${line}`, 'protobuf-java'].map((key) => pinned.inputs[key].sha256),
    'Churn reporter dependencies differ'
  )
  assert(/^[0-9a-f]{64}$/.test(receipt.sha256), 'Churn reporter built jar hash is invalid')
  return receipt.sha256
}

/**
 * Verifies a single stamped scenario's entire sidecar catalogue, decode/sink/request joins,
 * independently observed arithmetic and natural cleanup. All referenced paths stay under the run.
 * @param {object} record stamped evidence with a sidecar manifest
 * @param {string} directory absolute run directory
 * @returns {void}
 * @throws {Error} for incomplete, tampered or inconsistent evidence
 */
const verifyScenario = (record, directory) => {
  assert.equal(record.status, 'passed')
  const manifest = record.sidecars
  assert(manifest && /^case-[a-z0-9-]+$/.test(manifest.root), 'Missing sidecar manifest')
  assert(manifest.files.length > 0 && manifest.files.length <= 512, 'Invalid sidecar count')
  const paths = new Set()
  let total = 0
  for (const item of manifest.files) {
    assert(
      item.path.startsWith(`${manifest.root}/`) &&
        !item.path.split('/').includes('..') &&
        !item.path.includes('\\') &&
        !paths.has(item.path),
      'Invalid sidecar path'
    )
    paths.add(item.path)
    const file = path.join(directory, item.path)
    assert.equal(fs.realpathSync(file), file, 'Sidecar traverses a symlink')
    const stat = fs.lstatSync(file)
    total += stat.size
    assert(stat.isFile() && total <= CAP, 'Invalid sidecar size')
    const bytes = fs.readFileSync(file)
    assert.equal(bytes.length, item.bytes, 'Sidecar size mismatch')
    assert.equal(sha(bytes), item.sha256, 'Sidecar digest mismatch')
  }
  const read = (relative) => {
    assert(paths.has(relative), 'Unlisted sidecar')
    return JSON.parse(fs.readFileSync(path.join(directory, relative), 'utf8'))
  }
  if (record.run.project) {
    assert.deepEqual(manifest.cleanupReceipt, {
      path: 'ownership-receipt.json',
      project: record.run.project,
    })
    const identityPath = `${manifest.root}/identity.json`
    const identity = read(identityPath)
    assert.equal(identity.project, record.run.project)
    assert.equal(identity.line, record.run.line)
    assert.equal(identity.preflightPassed, true)
    assert.deepEqual(identity.preflight, { containers: [], networks: [], volumes: [] })
    assert.equal(identity.images.length, 3)
    assert.equal(identity.brokers.length, 3)
    assert.equal(new Set(identity.images.map((item) => item.container)).size, 3)
    assert(identity.readyAt && identity.startedAt && identity.configSha256)
    if (
      manifest.scenario === 'telemetry-reporter' ||
      ([...LIVE, CHURN].includes(manifest.scenario) && identity.compose === CONFIGURED_COMPOSE)
    ) {
      const pinned = JSON.parse(fs.readFileSync(path.join(__dirname, 'fixtures/manifest.json')))
      for (const image of identity.images)
        assert(
          pinned.lines[record.run.line].repoDigests.some((digest) =>
            image.repoDigests.includes(digest)
          ),
          'Image digest mismatch'
        )
      const reporterHash =
        manifest.scenario === CHURN && identity.churnReporter
          ? churnReporterHash(identity.churnReporter, record.run.line, pinned)
          : pinned.lines[record.run.line].reporterJar.sha256
      for (const broker of identity.brokers) {
        assert.equal(broker.reporterSha256, reporterHash, 'Reporter jar identity mismatch')
        assert(broker.configSha256)
      }
      assert.equal(
        record.fixture.manifestSha256,
        sha(fs.readFileSync(path.join(__dirname, 'fixtures/manifest.json')))
      )
      assert.equal(record.run.composeSha256, pinned.composeSha256['docker-compose.telemetry.yml'])
    }
  }
  if ([SHUTDOWN, CHURN].includes(manifest.scenario))
    assert.equal(manifest.format, SEGMENT_FORMAT, 'Missing segmented evidence format')
  const noPlugin = ['telemetry-reporter-no-plugin', UNSUPPORTED, 'telemetry-floor'].includes(
    manifest.scenario
  )
  const ownerKeys = LIVE.includes(manifest.scenario)
    ? requiredLiveCatalogue(record)
    : manifest.scenario === CHURN
      ? []
      : manifest.scenario === 'telemetry-floor'
        ? ['producer', 'consumer', 'admin']
        : noPlugin
          ? [...KINDS, ...KINDS.map((k) => k + '-off')]
          : KINDS
  assert.deepEqual(
    Object.keys(manifest.owners).sort(),
    [...ownerKeys].sort(),
    'Required owner catalogue differs'
  )
  assert.equal(
    new Set(Object.values(manifest.owners)).size,
    ownerKeys.length,
    'Reused owner stream'
  )
  for (const [owner, relative] of Object.entries(manifest.owners)) {
    const transcript = read(relative)
    if (['telemetry-java', 'telemetry-outcomes'].includes(manifest.scenario)) {
      const requests = transcript.records.filter((r) => r.type === 'request')
      assert(
        requests.length > 0 && requests.every((r) => r.clientId === `${record.run.stem}-${owner}`),
        'Owner stream client identity differs'
      )
    }
    assert.deepEqual(transcript.failures, [])
    if (![SHUTDOWN, UNSUPPORTED, 'telemetry-outcomes'].includes(manifest.scenario))
      assert.deepEqual(transcript.cleanup, { children: 0, timers: 0, stillOpen: [], attached: 0 })
  }
  if (manifest.scenario === CHURN) {
    verifyChurn(record, directory, manifest, read)
    return
  }
  if (manifest.scenario === 'telemetry-reporter') {
    const { results } = record
    assert.equal(new Set(KINDS.map((kind) => results[kind].uuid)).size, 4)
    assert.equal(results.resource.deleted, true)
    const reader = new TelemetryExportReader(path.join(directory, manifest.root))
    const consumed = new Set()
    let adminSamples
    for (const kind of KINDS) {
      const result = results[kind]
      assert(/^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$/.test(result.uuid))
      assert.notEqual(result.uuid, '00000000-0000-0000-0000-000000000000')
      const transcript = read(manifest.owners[kind])
      const gets = transcript.records.filter(
        (r) =>
          r.apiKey === 71 &&
          r.response?.errorCode === 0 &&
          (r.uuid === result.uuid || r.response.uuid === result.uuid)
      )
      assert(gets.length > 0)
      for (const get of gets) {
        assert.equal(get.apiVersion, 0)
        assert.equal(get.response.pushIntervalMs, 1000)
        assert.equal(get.response.telemetryMaxBytes, 4096)
        assert.deepEqual(get.response.requestedMetrics, ['org.apache.kafka.'])
      }
      const subdir = path.dirname(manifest.owners[kind])
      const stream = transcript.records.filter(
        (r) => r.apiKey === 72 && !r.terminating && r.writeAt <= results.window.deadline
      )
      assert.deepEqual(
        result.requestKeys,
        stream.map((r) => r.requestKey),
        'Incomplete Push stream'
      )
      assert.equal(
        new Set(result.requestKeys).size,
        result.requestKeys.length,
        'Duplicate Push identity'
      )
      for (const push of stream) {
        assert(push.writeAt >= results.window.start, 'Push before capture')
        assert(
          push.response &&
            push.response.errorCode === 0 &&
            push.response.responseAt <= results.window.deadline,
          'Failed or unmatched Push window'
        )
      }
      const samples = result.requestKeys.map((key) => {
        const decoded = read(`${subdir}/decoded-${key}.json`)
        const rawFile = `${subdir}/raw-${decoded.rawSha256}.bin`
        assert(paths.has(rawFile), 'Missing raw sidecar')
        assert.equal(sha(fs.readFileSync(path.join(directory, rawFile))), decoded.rawSha256)
        assert.equal(fs.statSync(path.join(directory, rawFile)).size, decoded.rawBytes)
        assert.equal(decoded.decoded.dataSha256, decoded.rawSha256)
        assert.equal(decoded.decoded.dataBytes, decoded.rawBytes)
        assert(decoded.rawBytes > 0 && decoded.rawBytes <= 4096 && decoded.transmittedBytes <= 4096)
        const request = transcript.records.find((r) => r.requestKey === key)
        assert(request && request.apiKey === 72 && request.apiVersion === 0 && !request.terminating)
        assert.equal(request.response.errorCode, 0)
        assert(gets.some((get) => get.response.subscriptionId === request.subscriptionId))
        assert.equal(request.uuid, result.uuid)
        assert.equal(request.clientId, result.clientId)
        assert.equal(decoded.requestKey, key)
        assert.equal(decoded.uuid, result.uuid)
        assert.equal(decoded.node, request.node)
        assert.equal(decoded.exportIdentity.node, request.node, 'Export source node mismatch')
        assert.equal(decoded.compressionType, request.compressionType)
        assert.equal(decoded.transmittedBytes, request.metricsBytes)
        const transmittedPath = `${subdir}/push-${key}.bin`
        assert(paths.has(transmittedPath), 'Missing transmitted sidecar')
        const transmitted = fs.readFileSync(path.join(directory, transmittedPath))
        assert.equal(transmitted.length, request.metricsBytes)
        assert.equal(sha(transmitted), request.metricsSha256)
        assert.equal(sha(transmitted), decoded.transmittedSha256)
        const options = { maxOutputLength: 4096 }
        let raw
        if (decoded.compressionType === 0) raw = transmitted
        else if (decoded.compressionType === 1) raw = zlib.gunzipSync(transmitted, options)
        // eslint-disable-next-line n/no-unsupported-features/node-builtins -- required live Node lines provide the built-in codec
        else if (decoded.compressionType === 4) raw = zlib.zstdDecompressSync(transmitted, options)
        else throw new Error('Unsupported captured compression')
        assert.equal(sha(raw), decoded.rawSha256)
        assert(
          request.writeAt >= results.window.start &&
            request.response.responseAt <= results.window.deadline
        )
        const identity = decoded.exportIdentity
        const token = `${identity.node}:${identity.seq}`
        assert(!consumed.has(token), 'Reused sink record')
        consumed.add(token)
        const rows = reader
          .readExports({ uuid: result.uuid })
          .filter((row) => row.node === identity.node && row.seq === identity.seq)
        assert.equal(rows.length, 1)
        assert.equal(rows[0].clientId, result.clientId)
        assert.equal(rows[0].dataSha256, decoded.rawSha256)
        assert.equal(rows[0].dataBytes, decoded.rawBytes)
        assert.equal(rows[0].pushIntervalMs, 1000)
        assert.equal(rows[0].terminating, false)
        assert.equal(rows[0].parse, 'ok')
        return { ...decoded, writeAt: request.writeAt }
      })
      assert(samples.length >= 2)
      if (kind === 'admin') adminSamples = samples
      const { requests, connects, delta } = result.arithmetic
      const topology = results.control.discovery.filter((r) => r.apiKey === 3)
      assert.equal(topology.length, 1)
      assert.equal(results.control.knownAfter, topology[0].response.responseAt)
      for (const event of requests) {
        assert.equal(
          event.knownNode,
          kind === 'admin' && event.writeAt > results.control.knownAfter
        )
        if (kind !== 'admin') assert([17, 18, 36].includes(event.apiKey))
      }
      const suffix = kind === 'share' ? 'consumer' : kind
      for (const sample of samples) {
        const expectedResources =
          kind === 'admin'
            ? {}
            : {
                client_rack: 'fixture-rack',
                ...(['consumer', 'share'].includes(kind)
                  ? { group_id: `${record.run.stem}-${kind === 'share' ? 'share-group' : 'group'}` }
                  : {}),
              }
        assert.equal(sample.decoded.resources.length, 1)
        assert.deepEqual(
          Object.fromEntries(
            sample.decoded.resources[0].attrs.map(([key, type, value]) => {
              assert.equal(type, 'string')
              return [key, value]
            })
          ),
          expectedResources
        )
        const normalSum = sample.decoded.metrics.find(
          (m) => m.name === `org.apache.kafka.${suffix}.connection.creation.total`
        )
        assert.equal(normalSum.kind, 'SUM')
        assert.equal(normalSum.monotonic, true)
        assert.equal(
          normalSum.temporality,
          delta ? 'AGGREGATION_TEMPORALITY_DELTA' : 'AGGREGATION_TEMPORALITY_CUMULATIVE'
        )
      }
      for (const event of requests) {
        const wire = transcript.records.find((r) => r.requestKey === event.requestKey)
        assert(
          wire &&
            wire.response &&
            wire.correlation === event.correlationId &&
            wire.apiKey === event.apiKey &&
            wire.apiVersion === event.apiVersion &&
            wire.clientId === event.clientId &&
            wire.socketId === event.socketId &&
            wire.node === event.node
        )
        assert.equal(event.responseAt, wire.response.responseAt)
        assert.equal(event.writeAt, wire.writeAt)
      }
      const normalIds = new Set(
        transcript.sockets.filter((s) => s.kind === 'normal').map((s) => s.id)
      )
      assert.deepEqual(
        requests.map((r) => r.requestKey).sort(),
        transcript.records
          .filter((r) => r.type === 'request' && r.response && normalIds.has(r.socketId))
          .map((r) => r.requestKey)
          .sort()
      )
      assert.deepEqual(
        connects,
        transcript.records.filter((r) => r.type === 'connect' && normalIds.has(r.socketId))
      )
      assert.deepEqual(
        verifyMetricWindows({ kind, samples, requests, connects, delta }),
        result.arithmetic
      )
    }
    verifyControl(results, read(manifest.owners.admin), adminSamples)
    for (const node of ['1', '2', '3']) {
      const proof = read(`${manifest.root}/reporter-${node}.json`)
      assert.equal(proof.node, node)
      assert.equal(proof.kafkaClients, record.run.line)
    }
  } else if (manifest.scenario === SHUTDOWN) {
    verifyShutdown(record, directory, manifest, read)
  } else if (LIVE.includes(manifest.scenario)) {
    verifyLive(record, directory, manifest, read)
  } else {
    assert(
      [
        'telemetry-reporter-no-resource',
        'telemetry-reporter-no-plugin',
        UNSUPPORTED,
        'telemetry-floor',
      ].includes(manifest.scenario)
    )
    for (const [kind, relative] of Object.entries(manifest.owners)) {
      const transcript = read(relative)
      const { records, sockets } = transcript
      assert(!records.some((r) => r.apiKey === 72), 'Unexpected Push in negative control')
      if (noPlugin) {
        assert(!records.some((r) => r.apiKey === 71))
        if (manifest.scenario === 'telemetry-floor')
          ordinaryOwnership(transcript, { owner: kind, clientId: `${record.run.stem}-${kind}` })
        else if (manifest.scenario !== UNSUPPORTED)
          assert(!sockets.some((s) => s.kind === 'telemetry'))
        assert(
          records.some((r) => r.type === 'connect'),
          'No ordinary baseline'
        )
        if (manifest.scenario === UNSUPPORTED) {
          assertCorpus(record.corpus)
          verifyCorpusInputs(record.corpus, read(`${manifest.root}/oracles.json`).corpus)
          const oracles = read(`${manifest.root}/oracles.json`)
          verifyCorpusInputs(record.results.disabledCorpus, oracles.disabledCorpus)
          if (!['4.0.2', '4.1.2'].includes(record.run.line))
            for (const owner of ['share', 'share-off']) {
              const share = oracles.share[owner]
              verifyShareIdentity(
                share,
                `${record.run.stem}-share-work`,
                share.keys.map((key) => ({ key, valueHex: Buffer.from(key).toString('hex') }))
              )
              const confirmed = confirmedDecisions(share.frames, share.group)
              assert.equal(confirmed.length, 10, 'Incomplete capable share broker settlement')
              assert.deepEqual(
                confirmed.map((r) => r.offset).sort(),
                Array.from({ length: 10 }, (_, i) => String(i))
              )
              assert(
                confirmed.every(
                  (r) => r.type === 1 && r.partition === 0 && r.topicId === share.topicId
                ),
                'Capable share broker settlement identity differs'
              )
              verifyPublicSettlements(share.settlements, share.topic, () => 'accept')
            }
        }
        if (manifest.scenario === 'telemetry-floor') {
          assertCorpus(record.corpus)
          verifyCorpusInputs(record.corpus, read(`${manifest.root}/oracles.json`).corpus)
        }
        const outcome = (kind.endsWith('-off') ? record.disabledOutcomes : record.outcomes)[
          kind.replace(/-off$/, '')
        ]
        if (!kind.startsWith('share') || !['4.0.2', '4.1.2'].includes(record.run.line))
          assert.equal(outcome.status, 'connected', 'Ordinary baseline failed')
        else assertShareRefusal(outcome, record.run.line)
      } else {
        const gets = records.filter((r) => r.apiKey === 71)
        assert.equal(gets.length, 1)
        assert.equal(gets[0].response.uuid, record.uuids[kind])
        assert.deepEqual(gets[0].response.requestedMetrics, [])
        assert.equal(gets[0].response.pushIntervalMs, 300000)
      }
    }
  }
}

/** @param {object} transcript wire ledger @throws {Error} for an API 71/72 request */
const assertNoTelemetryRequests = ({ records }) => {
  assert(
    !records.some((r) => r.type === 'request' && [71, 72].includes(r.apiKey)),
    'Unexpected API 71/72 request'
  )
}

/** @param {object} outcome public connect result @param {string} line broker line */
const assertShareRefusal = (outcome, line) => {
  if (!['4.0.2', '4.1.2'].includes(line)) {
    assert.deepEqual(outcome, { status: 'connected' }, 'Share capability outcome differs')
    return
  }
  const reason = line === '4.0.2' ? 'api-not-advertised' : 'api-version-range'
  assertRefusal(outcome, { reason, apiKey: line === '4.0.2' ? 76 : 78 })
}

/**
 * Compares a recorded share refusal exactly; a recorder that keeps the error message must hold
 * the message the client builds from the reason.
 * @param {object} outcome recorded connect outcome
 * @param {{ reason: string, apiKey: number }} expected
 * @throws {Error} when any field differs
 */
const assertRefusal = (outcome, { reason, apiKey }) => {
  const { message, ...fields } = outcome
  assert.deepEqual(
    fields,
    {
      status: 'rejected',
      name: 'KafkaJSShareGroupsNotSupportedError',
      reason,
      apiKey,
      shareVersion: 0,
    },
    'Share capability refusal differs'
  )
  if (message !== undefined) {
    assert.equal(message, `Share groups refused: ${reason}`, 'Share capability refusal differs')
  }
}

/**
 * Qualifies Kafka's timestamp-preserving 117/76 refresh: validation advances the failed Push's
 * timestamp, but the immediate Get bypasses quota without updating its own timestamp. Its first
 * jittered Push may consequently receive 89. Every other early request remains a failure.
 * @param {object} transcript complete owner or segment ledger
 * @param {object} request request answered with error 89
 * @returns {boolean} whether the trace proves this exception and interval-delayed recovery
 */
const isRefreshThrottle = (transcript, request) => {
  const telemetry = transcript.records.filter(
    (r) => r.type === 'request' && [71, 72].includes(r.apiKey)
  )
  const index = telemetry.indexOf(request)
  const [prior, get] = telemetry.slice(Math.max(0, index - 2), index)
  const next = telemetry[index + 1]
  const interval = get?.response?.pushIntervalMs
  return !!(
    request.apiKey === 72 &&
    !request.terminating &&
    prior?.apiKey === 72 &&
    !prior.terminating &&
    [117, 76].includes(prior.response?.errorCode) &&
    get?.apiKey === 71 &&
    get.response?.errorCode === 0 &&
    Number.isInteger(interval) &&
    interval > 0 &&
    [prior, get].every((r) => r.socketId === request.socketId && r.node === request.node) &&
    prior.uuid === request.uuid &&
    get.response.uuid === request.uuid &&
    get.uuid === request.uuid &&
    request.subscriptionId === get.response.subscriptionId &&
    get.writeAt >= prior.response.responseAt &&
    get.writeAt < prior.response.responseAt + interval &&
    request.writeAt >= get.response.responseAt + Math.round(interval * 0.5) &&
    request.writeAt < prior.response.responseAt + interval &&
    next?.apiKey === 71 &&
    next.socketId === request.socketId &&
    next.node === request.node &&
    next.uuid === request.uuid &&
    next.writeAt >= request.response.responseAt + interval &&
    next.response?.errorCode === 0
  )
}

/**
 * Qualifies the sole best-effort final rejected after deletion of its active subscription.
 * Kafka recomputes the subscription before validating even a terminating Push. Close must keep
 * the last accepted ID, send no Get/retry, and use its captured existing socket and boundary.
 * @param {object} transcript complete owner ledger
 * @param {object} request terminating Push answered with 117
 * @param {object} receipt independent admin readback, deletion response and absent listing
 * @returns {boolean} whether the receipt and complete stream prove this exact cleanup race
 */
const isDeletedSubscriptionFinal = (transcript, request, receipt) => {
  try {
    const telemetry = transcript.records.filter(
      (r) => r.type === 'request' && [71, 72].includes(r.apiKey)
    )
    const index = telemetry.indexOf(request)
    const prior = telemetry[index - 1]
    const get = telemetry.slice(0, index).findLast((r) => r.apiKey === 71)
    const close = transcript.closures.find(
      (c) => request.writeAt >= c.at && request.response.responseAt <= c.end
    )
    assert(request.apiKey === 72 && request.terminating && index === telemetry.length - 1)
    assert(close && close.pending.length === 0)
    assert.equal(telemetry.filter((r) => r.terminating && r.writeAt >= close.at).length, 1)
    assert(
      get?.response?.errorCode === 0 &&
        prior?.apiKey === 72 &&
        !prior.terminating &&
        prior.response?.errorCode === 0
    )
    for (const r of [get, prior]) {
      for (const key of ['node', 'socketId', 'clientId']) assert.equal(r[key], request[key])
    }
    assert(typeof request.clientId === 'string' && request.clientId.length > 0)
    assert(
      Number.isInteger(request.subscriptionId) &&
        request.subscriptionId >= -2147483648 &&
        request.subscriptionId <= 2147483647
    )
    assert(get.response.responseAt <= prior.writeAt && prior.writeAt <= prior.response.responseAt)
    assert.equal(get.response.uuid, request.uuid)
    assert.equal(prior.uuid, request.uuid)
    assert.equal(request.subscriptionId, get.response.subscriptionId)
    assert.equal(prior.subscriptionId, request.subscriptionId)
    const { name, entries, readback, createdAt, deletion } = receipt
    const success = (r) => r.resourceType === 16 && r.resourceName === name && r.errorCode === 0
    assert(success(readback))
    assert.deepEqual(
      Object.fromEntries(readback.configEntries.map((e) => [e.configName, e.configValue])),
      entries
    )
    assert.deepEqual(get.response.requestedMetrics, entries.metrics.split(','))
    assert(get.response.pushIntervalMs > 0)
    assert.equal(get.response.pushIntervalMs, Number(entries['interval.ms']))
    assert(
      entries.match.startsWith('client_id=') &&
        new RegExp(entries.match.slice(10)).test(request.clientId)
    )
    assert(Number.isFinite(createdAt) && createdAt <= get.writeAt)
    assert(Number.isFinite(deletion.startedAt) && prior.response.responseAt <= deletion.startedAt)
    assert(
      deletion.startedAt <= deletion.responseAt &&
        deletion.responseAt <= deletion.listedAt &&
        deletion.listedAt <= close.at
    )
    assert.equal(deletion.response.resources.length, 1)
    assert(success(deletion.response.resources[0]))
    assert(!deletion.listing.resources.some((r) => r.type === 16 && r.name === name))
    const socket = transcript.sockets.find((s) => s.id === request.socketId)
    assert(socket.closedAt >= close.end)
    return true
  } catch {
    return false
  }
}

/**
 * Requires every Push and Get of every owner to be accounted for: answered with success, answered
 * with an error the case declared for that owner, the source-qualified refresh throttle or
 * subscription-deletion final,
 * or an exact request-level cancellation joined
 * to its public-close boundary or one deliberately faulted exchange.
 * @param {Object<string,object>} transcripts observer transcript by owner label
 * @param {{errors?:Object<string,number[]>}} allowed per-owner deliberate response errors; unanswered
 *   requests require transcript cancellations joined to closures or declared request-level faults
 * @param {{subscriptionDeletions?:Object<string,object>}} context archived admin deletion receipts
 * @returns {void}
 * @throws {Error} naming the first undeclared error or unanswered request
 */
const assertStreamAccounted = (transcripts, allowed = {}, context = {}) => {
  for (const [owner, transcript] of Object.entries(transcripts)) {
    let acceptedUuid
    for (const get of transcript.records.filter((r) => r.apiKey === 71)) {
      if (acceptedUuid) assert.equal(get.uuid, acceptedUuid, 'Get rotated accepted UUID')
      if (get.response?.errorCode === 0) {
        assert(/^[a-f0-9]{8}(-[a-f0-9]{4}){3}-[a-f0-9]{12}$/.test(get.response.uuid))
        assert.notEqual(get.response.uuid, '00000000-0000-0000-0000-000000000000')
        acceptedUuid ??= get.response.uuid
        assert.equal(get.response.uuid, acceptedUuid)
      }
    }
    const cancellations = transcript.cancellations ?? []
    const unanswered = transcript.records.filter(
      (r) => r.type === 'request' && [71, 72].includes(r.apiKey) && !r.response
    )
    assert.deepEqual(
      cancellations.map((c) => c.requestKey).sort(),
      unanswered.map((r) => r.requestKey).sort(),
      `Unanswered telemetry request of ${owner}`
    )
    assert.equal(new Set(cancellations.map((c) => c.requestKey)).size, cancellations.length)
    for (const r of transcript.records) {
      if (r.type !== 'request' || ![71, 72].includes(r.apiKey)) continue
      assert.equal(r.apiVersion, 0)
      if (!r.response) {
        const c = cancellations.find((c) => c.requestKey === r.requestKey)
        for (const key of ['apiKey', 'socketId', 'writeAt'])
          assert.equal(c[key], r[key], 'Cancellation identity mismatch')
        const socket = transcript.sockets.find((s) => s.id === r.socketId)
        assert.equal(c.end, socket.closedAt)
        assert(Number.isFinite(c.at) && c.end >= c.at && c.end >= r.writeAt)
        if (c.disposition === 'faulted-transport') {
          assert(transcript.faults.some((f) => f.socketId === r.socketId && f.at === c.at))
          assert(r.writeAt >= c.at)
          assert.equal(
            transcript.records.filter(
              (p) =>
                p.type === 'request' &&
                [71, 72].includes(p.apiKey) &&
                p.socketId === r.socketId &&
                p.writeAt >= c.at
            ).length,
            1,
            'Fault covers unrelated requests'
          )
        } else {
          const close = transcript.closures.find((close) => close.at === c.at)
          assert(close, 'Missing close boundary')
          assert(
            transcript.records.filter(
              (p) =>
                close.pending.includes(p.requestKey) &&
                p.socketId === r.socketId &&
                [71, 72].includes(p.apiKey)
            ).length <= 1,
            'Close allowance covers unrelated in-flight requests'
          )
          assert(
            transcript.records.filter(
              (p) =>
                p.apiKey === 72 && p.terminating && p.writeAt >= close.at && p.writeAt <= close.end
            ).length <= 1,
            'Multiple final requests for one close'
          )
          if (c.disposition === 'close-in-flight')
            assert(close.pending.includes(r.requestKey) && r.writeAt <= c.at)
          else {
            assert.equal(c.disposition, 'close-final')
            assert(r.apiKey === 72 && r.terminating && r.writeAt >= c.at && r.writeAt <= close.end)
          }
        }
      } else if (r.response.errorCode === 117 && r.terminating)
        assert(
          isDeletedSubscriptionFinal(transcript, r, context.subscriptionDeletions?.[owner]),
          `Unqualified final subscription error for ${r.requestKey} of ${owner}`
        )
      else if (r.response.errorCode === 89)
        assert(
          isRefreshThrottle(transcript, r),
          `Unqualified broker throttle for ${r.requestKey} of ${owner}`
        )
      else if (r.response.errorCode !== 0)
        assert(
          (allowed.errors?.[owner] ?? []).includes(r.response.errorCode),
          `Undeclared error ${r.response.errorCode} for ${r.requestKey} of ${owner}`
        )
    }
  }
}

/**
 * Checks the three independent oracles of one ordinary workload against each other.
 * @param {object} corpus count, three set digests, handler entries, raw commits, processed next and raw end
 * @returns {void}
 * @throws {Error} when the sets differ, a record is missing or duplicated, or commits are not at the end
 */
const assertCorpus = (corpus) => {
  assert.equal(corpus.count, 1000)
  assert.equal(corpus.logSha256, corpus.expectedSha256, 'Durable log differs from the corpus')
  assert.equal(corpus.handlerSha256, corpus.expectedSha256, 'Handler set differs from the corpus')
  assert.equal(corpus.handlerEntries, corpus.count, 'Handler entries differ from the corpus')
  assert.deepEqual(corpus.rawCommitted, corpus.processedNext, 'Raw commits differ from processed')
  assert.deepEqual(corpus.rawCommitted, corpus.rawEnd, 'Raw commits are not at the log end')
}

/**
 * @param {object} transcript one owner's or segment's wire ledger
 * @param {object} request an accepted Push of that ledger
 * @returns {object|undefined} the last successful Get on the Push's own socket before it was written
 */
const lastSubscription = (transcript, request) =>
  transcript.records
    .filter(
      (r) =>
        r.apiKey === 71 &&
        r.socketId === request.socketId &&
        r.response?.errorCode === 0 &&
        r.response.responseAt <= request.writeAt
    )
    .at(-1)

/**
 * The shared proof of one accepted nonempty Push: its successful subscription, transmitted bytes,
 * bounded decompression, stored raw body, full independent decode and the decoder's own receipt.
 * The bytes are supplied lazily so each caller keeps its own order of source checks.
 * @param {object} input the request and everything the archive holds about it
 * @param {object} input.request captured Push request
 * @param {object} input.subscription its successful Get
 * @param {object} input.decoded the decoded sidecar of the request
 * @param {string} input.line broker line the decoder receipts must identify
 * @param {function():Buffer} input.transmitted the archived transmitted bytes
 * @param {function():Buffer} input.stored the archived raw body
 * @param {function():object[]} input.receipts the decoder receipts for the raw body
 * @returns {void}
 * @throws {Error} for any contradiction between the request, bytes, decode and receipts
 */
const assertPushProof = ({
  request,
  subscription,
  decoded,
  line,
  transmitted,
  stored,
  receipts,
}) => {
  assert(subscription, 'Push without a successful subscription')
  assert.equal(subscription.response.uuid, request.uuid)
  assert.equal(subscription.response.subscriptionId, request.subscriptionId)
  assert.equal(decoded.requestKey, request.requestKey)
  assert.equal(decoded.uuid, request.uuid)
  assert.equal(decoded.node, request.node)
  assert.equal(decoded.compressionType, request.compressionType)
  assert.equal(decoded.transmittedSha256, request.metricsSha256)
  assert.equal(decoded.transmittedBytes, request.metricsBytes)
  const sent = transmitted()
  assert.equal(sha(sent), request.metricsSha256)
  assert.equal(sent.length, request.metricsBytes)
  const cap = Math.min(4096, subscription.response.telemetryMaxBytes)
  const options = { maxOutputLength: cap }
  let raw
  if (request.compressionType === 0) raw = sent
  else if (request.compressionType === 1) raw = zlib.gunzipSync(sent, options)
  // eslint-disable-next-line n/no-unsupported-features/node-builtins -- live Node lines supply the built-in codec
  else if (request.compressionType === 4) raw = zlib.zstdDecompressSync(sent, options)
  else throw new Error('Unsupported captured compression')
  assert(raw.length > 0 && raw.length <= cap && sent.length <= cap)
  assert.equal(sha(raw), decoded.rawSha256, 'Transmitted/raw relationship differs')
  assert.equal(raw.length, decoded.rawBytes)
  assert(raw.equals(stored()))
  assert.equal(decoded.decoded.dataSha256, sha(raw))
  assert.equal(decoded.decoded.dataBytes, raw.length)
  assert.equal(decoded.decoded.parse, 'ok')
  assert.equal(decoded.decoded.truncated, false)
  const found = receipts()
  assert(found.length > 0, 'Missing independent decoder receipt')
  assert(
    found.some((receipt) => {
      if (receipt.code !== 0 || receipt.signal !== null || receipt.line !== line) return false
      const [identity, body] = receipt.stdout.trim().split('\n').map(JSON.parse)
      if (identity.verified !== true || identity.line !== line) return false
      try {
        assert.deepEqual(body, decoded.decoded)
        return true
      } catch {
        return false
      }
    }),
    'Decoded body differs from independent receipt'
  )
  assert(
    decoded.decoded.metrics.every((m) =>
      subscription.response.requestedMetrics.some((prefix) => m.name.startsWith(prefix))
    ),
    'Metric outside subscription'
  )
}

/**
 * Replays the version-2 Java contract from complete sink files and raw process receipts. JS wire
 * and independent decoder joins remain mandatory in the enclosing scenario verifier.
 * @param {object} record candidate and result stamps
 * @param {string} directory full all-node sink archive
 * @param {object} transcripts four JS subscription confirmation streams
 * @returns {void}
 * @throws {Error} for old format, unqualified inputs or any invalid role close proof
 */
const verifyJavaInterop = (record, directory, transcripts) => {
  const { results } = record
  requiredLiveCatalogue(record)
  const reader = new TelemetryExportReader(directory)
  assert.equal(results.javaEvidenceVersion, 2, 'Missing Java evidence version 2')
  assert.deepEqual(
    Object.keys(results.children).sort(),
    [...JAVA_ROLES].sort(),
    'Missing Java child catalogue'
  )
  const provenance = results.provenance
  assert.equal(provenance.sourceSha256, record.fixture.clientSourceSha256)
  assert.equal(provenance.runnerSha256, record.fixture.runnerSha256)
  const pins = fs
    .readFileSync(path.join(__dirname, 'interop.sha256'), 'utf8')
    .trim()
    .split('\n')
    .map((line) => {
      const [sha256, name] = line.trim().split(/\s+/)
      return { name, sha256 }
    })
  assert.deepEqual(
    provenance.cache.map(({ name, sha256 }) => ({ name, sha256 })),
    pins,
    'Wrong Java pinned input catalogue'
  )
  for (const role of ['producer', 'consumer', 'admin'])
    assert(provenance.jdk[role]?.jdk && provenance.jdk[role]?.vendor)
  assert.equal(results.readback.metrics, JAVA_REQUESTED, 'Wrong narrow Java subscription')
  assert.deepEqual(
    [...results.readback.ids].sort(),
    [
      ...JAVA_ROLES.map((role) => `${record.run.stem}-java-${role}`),
      ...KINDS.map((kind) => `${record.run.stem}-js-${kind}`),
    ].sort(),
    'Wrong Java subscription client catalogue'
  )
  for (const kind of KINDS) {
    const gets = transcripts[`js-${kind}`].records.filter(
      (r) => r.apiKey === 71 && r.response?.errorCode === 0
    )
    assert(gets.length > 0, 'Missing Java subscription Get confirmation')
    for (const get of gets) assertJavaSubscription(get.response)
  }
  assert(
    results.java.admin.checkpoint && results.java.admin.cessation,
    'Missing admin checkpoint/cessation'
  )
  const subscription = results.subscription
  const expectedEntries = {
    metrics: JAVA_REQUESTED,
    'interval.ms': '1000',
    match: `client_id=^(${results.readback.ids.join('|')})$`,
  }
  assert.equal(
    results.subscriptionOperations.length,
    2,
    'Missing public Java subscription operations'
  )
  for (const [index, operation] of results.subscriptionOperations.entries()) {
    assert.deepEqual(
      operation.request,
      {
        timeout: 3000,
        resources: [
          {
            type: 16,
            name: record.run.stem,
            configEntries: Object.entries(expectedEntries).map(([name, value]) => ({
              name,
              operation: index,
              ...(index === 0 ? { value } : {}),
            })),
          },
        ],
      },
      'Wrong public Java subscription operation'
    )
    assert.equal(operation.response.resources.length, 1)
    assert.equal(operation.response.resources[0].resourceType, 16)
    assert.equal(operation.response.resources[0].resourceName, record.run.stem)
    assert.equal(operation.response.resources[0].errorCode, 0)
    assert(
      Number.isFinite(operation.startedAt) &&
        operation.startedAt >= 0 &&
        operation.completedAt >= operation.startedAt
    )
  }
  assert(subscription.createdAt >= results.subscriptionOperations[0].completedAt)
  assert(
    results.subscriptionOperations[1].startedAt >= results.java.admin.cessation.quiet.at(-1).at,
    'Java subscription removed before cessation'
  )
  assert.equal(subscription.readback.configEntries.length, 3)
  assert.equal(subscription.name, record.run.stem)
  assert.deepEqual(subscription.entries, expectedEntries, 'Wrong actual Java subscription')
  assert.equal(subscription.readback.resourceType, 16)
  assert.equal(subscription.readback.resourceName, record.run.stem)
  assert.equal(subscription.readback.errorCode, 0)
  assert.deepEqual(
    Object.fromEntries(
      subscription.readback.configEntries.map((e) => [e.configName, e.configValue])
    ),
    expectedEntries,
    'Wrong actual Java readback'
  )
  assert(subscription.createdAt <= results.java.admin.checkpoint.proof.at)
  assert(
    subscription.deletion.startedAt >= results.java.admin.cessation.quiet.at(-1).at,
    'Java subscription removed before cessation'
  )
  assert.equal(subscription.deletion.response.resources[0].errorCode, 0)
  assert(
    !subscription.deletion.listing.resources.some(
      (r) => r.type === 16 && r.name === record.run.stem
    )
  )
  assert.deepEqual(results.controls, { producerRecords: 2, seedCommit: '1' })
  assert(
    [...Object.values(results.uuids.java), ...Object.values(results.uuids.js)].every(
      (uuid) =>
        /^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$/.test(uuid) &&
        uuid !== '00000000-0000-0000-0000-000000000000'
    ),
    'Invalid Java interop UUID'
  )
  for (const [role, java] of Object.entries(results.java)) {
    assert.equal(java.assignedId, `${record.run.stem}-java-${role}`)
    const rows = reader
      .readExports({ uuid: java.uuid })
      .sort(
        (a, b) => a.tsMs - b.tsMs || Number(a.terminating) - Number(b.terminating) || a.seq - b.seq
      )
    const child = results.children[role]
    assert.deepEqual(unexpectedPinnedJavaStderr(child.stderr), [], 'Unexpected Java stderr')
    if (role === 'admin') {
      assertJavaCaptures(
        [java.checkpoint?.proof, ...java.cessation.drain, ...java.cessation.quiet],
        directory
      )
    }
    assertJavaEvidence(role, rows, java, child, {
      provenance,
      fixture: record.fixture,
      readback: results.readback,
      group: `${record.run.stem}-java-consumer-group`,
    })
  }
  assert.equal(
    new Set([...Object.values(results.uuids.java), ...Object.values(results.uuids.js)]).size,
    7
  )
}

/**
 * Re-derives, from the archived transcripts and sink files, what the L2–L8 scenarios proved: the
 * complete stream is accounted for, every claimed Push joins its decode, transmitted bytes and a
 * once-consumed sink record, and each scenario's own wire or sink conclusion still holds.
 * @param {object} record stamped evidence
 * @param {string} directory absolute run directory
 * @param {object} manifest sidecar manifest
 * @param {function(string):object} read verified JSON sidecar reader
 * @returns {void}
 * @throws {Error} for any unaccounted request, unjoined Push or contradicted conclusion
 */
const verifyLive = (record, directory, manifest, read) => {
  const transcripts = Object.fromEntries(
    Object.entries(manifest.owners).map(([owner, relative]) => [owner, read(relative)])
  )
  const oracles = read(`${manifest.root}/oracles.json`)
  let subscriptionDeletions
  if (['telemetry-custom', 'telemetry-payload'].includes(manifest.scenario)) {
    subscriptionDeletions = oracles.subscriptionDeletions
    if (subscriptionDeletions) {
      const owner = manifest.scenario === 'telemetry-custom' ? 'producer' : 'producer-candidate'
      assert.deepEqual(Object.keys(subscriptionDeletions), [owner])
      assert.equal(subscriptionDeletions[owner].name, record.run.stem)
    }
  }
  assertStreamAccounted(transcripts, record.allowed, { subscriptionDeletions })
  const reader = new TelemetryExportReader(path.join(directory, manifest.root))
  const consumed = new Set()
  const actual = Object.entries(transcripts).flatMap(([owner, transcript]) =>
    transcript.records
      .filter((r) => r.apiKey === 72 && r.response?.errorCode === 0 && r.metricsBytes > 0)
      .map(({ requestKey }) => ({ owner, requestKey }))
  )
  assertCatalogue(record.pushes, actual)
  const zeros = Object.entries(transcripts).flatMap(([owner, transcript]) =>
    transcript.records
      .filter((r) => r.apiKey === 72 && r.metricsBytes === 0)
      .map(({ requestKey }) => ({
        owner,
        requestKey,
        disposition: zeroDisposition(manifest.scenario),
      }))
  )
  assert.deepEqual(record.zeroPushes, zeros, 'Incomplete zero-byte exception catalogue')
  const samples = {}
  for (const { owner, requestKey } of actual) {
    const transcript = transcripts[owner]
    const request = transcript.records.find((r) => r.requestKey === requestKey)
    const subdir = path.dirname(manifest.owners[owner])
    const decoded = read(`${subdir}/decoded-${requestKey}.json`)
    const subscription = lastSubscription(transcript, request)
    assertPushProof({
      request,
      subscription,
      decoded,
      line: record.run.line,
      transmitted: () => {
        // read() also enforces catalogue membership for the binary files through their metadata.
        assert(manifest.files.some((f) => f.path === `${subdir}/push-${requestKey}.bin`))
        return fs.readFileSync(path.join(directory, subdir, `push-${requestKey}.bin`))
      },
      stored: () => {
        const rawPath = `${subdir}/raw-${decoded.rawSha256}.bin`
        assert(manifest.files.some((f) => f.path === rawPath))
        return fs.readFileSync(path.join(directory, rawPath))
      },
      receipts: () =>
        manifest.files
          .filter((f) => f.path.startsWith(`${subdir}/decoder-`) && f.path.endsWith('.json'))
          .map((f) => read(f.path))
          .filter((r) => r.rawSha256 === decoded.rawSha256),
    })
    assert.equal(decoded.exportIdentity.node, request.node)
    const token = `${decoded.exportIdentity.node}:${decoded.exportIdentity.seq}`
    assert(!consumed.has(token), 'Reused sink record')
    consumed.add(token)
    const rows = reader
      .readExports({ uuid: request.uuid })
      .filter((r) => r.node === decoded.exportIdentity.node && r.seq === decoded.exportIdentity.seq)
    assert.equal(rows.length, 1)
    const [row] = rows
    assert.equal(row.clientId, request.clientId)
    assert.equal(row.dataSha256, decoded.rawSha256)
    assert.equal(row.dataBytes, decoded.rawBytes)
    assert.equal(row.terminating, request.terminating)
    assert.equal(row.pushIntervalMs, subscription.response.pushIntervalMs)
    assert.equal(row.parse, 'ok')
    samples[`${owner}:${requestKey}`] = { ...decoded, request, row }
  }
  for (const { owner, requestKey } of zeros) {
    const request = transcripts[owner].records.find((r) => r.requestKey === requestKey)
    const subscription = transcripts[owner].records
      .filter(
        (r) =>
          r.apiKey === 71 &&
          r.socketId === request.socketId &&
          r.response?.errorCode === 0 &&
          r.response.responseAt <= request.writeAt
      )
      .at(-1)
    assert(subscription && subscription.response.subscriptionId === request.subscriptionId)
    if (manifest.scenario === 'telemetry-prefix')
      assert.deepEqual(subscription.response.requestedMetrics, ['never.matches.telemetry.'])
    else if (manifest.scenario === 'telemetry-custom')
      assert(request.index > record.results.unregisterFence)
    else assert.fail('Unexpected zero-byte Push')
    assert.equal(request.metricsSha256, sha(Buffer.alloc(0)))
    assert.equal(request.compressionType, 0)
    const subdir = path.dirname(manifest.owners[owner])
    assert(manifest.files.some((f) => f.path === `${subdir}/push-${requestKey}.bin`))
    assert.equal(fs.statSync(path.join(directory, subdir, `push-${requestKey}.bin`)).size, 0)
  }
  // Every independently decoded successful nonempty Push has exactly one catalogue entry.
  const decodedFiles = manifest.files
    .filter((f) => /\/decoded-r\d+\.json$/.test(f.path))
    .map((f) => f.path)
    .sort()
  assert.deepEqual(
    decodedFiles,
    actual
      .map(
        ({ owner, requestKey }) =>
          `${path.dirname(manifest.owners[owner])}/decoded-${requestKey}.json`
      )
      .sort(),
    'Extra decoded Push sidecar'
  )
  const { results } = record
  if (['telemetry-prefix', 'telemetry-custom', 'telemetry-payload'].includes(manifest.scenario))
    assertOwnerUuid(
      transcripts[manifest.scenario === 'telemetry-payload' ? 'producer-candidate' : 'producer'],
      results.uuid
    )
  if (manifest.scenario === 'telemetry-prefix') {
    assert.equal(results.quiet.owner, 'producer')
    const { owner, from, to } = results.quiet
    const wire = transcripts[owner].records
    const noMatch = wire.find((r) => r.requestKey === results.noMatchGetKey)
    assert.deepEqual(noMatch.response.requestedMetrics, ['never.matches.telemetry.'])
    assert.equal(noMatch.response.pushIntervalMs, 1000)
    const zero = wire.find((r) => r.requestKey === results.noMatch.push.requestKey)
    assert(
      zero.apiKey === 72 &&
        zero.metricsBytes === 0 &&
        zero.compressionType === 0 &&
        !zero.terminating &&
        zero.response?.errorCode === 0
    )
    assert.equal(zero.subscriptionId, noMatch.response.subscriptionId)
    const refused = wire.find((r) => r.requestKey === results.update.refusedKey)
    const replacement = wire.find((r) => r.requestKey === results.update.getKey)
    assert.equal(refused.response.errorCode, 117)
    assert.equal(replacement.uuid, results.uuid)
    assert.equal(replacement.socketId, refused.socketId)
    assert(replacement.writeAt >= refused.response.responseAt + refused.response.throttleMs)
    assert.deepEqual(replacement.response.requestedMetrics, ['org.apache.kafka.producer.'])
    assert.notEqual(replacement.response.subscriptionId, noMatch.response.subscriptionId)
    const selected = samples[`${owner}:${results.update.pushKey}`]
    assert(selected && selected.request.response.responseAt - results.update.readbackAt <= 10000)
    assert.deepEqual(
      selected.decoded.metrics.map((m) => m.name),
      results.update.names
    )

    const empty = transcripts[owner].records.find(
      (r) => r.apiKey === 71 && r.response?.responseAt === results.empty.q0
    )
    assert(
      empty &&
        empty.response.requestedMetrics.length === 0 &&
        empty.response.pushIntervalMs === 1000
    )
    const polls = transcripts[owner].records.filter((r) => r.apiKey === 71 && r.index > empty.index)
    assert(polls.length > 0)
    assertCadence(polls, empty.response.responseAt, 1000)
    assert.equal(polls[0].response.errorCode, 0)
    assert.deepEqual(polls[0].response.requestedMetrics, [])
    assert.equal(polls[0].response.pushIntervalMs, 1000)
    assert(
      Object.values(samples).some((s) =>
        s.decoded.metrics.some(
          (m) => m.name === 'org.apache.kafka.producer.connection.creation.total'
        )
      )
    )

    assert.deepEqual(
      transcripts[owner].records.filter(
        (r) => r.apiKey === 72 && r.writeAt >= from && r.writeAt < to
      ),
      [],
      'A Push was written in the empty-selection interval'
    )
  } else if (manifest.scenario === 'telemetry-custom') {
    verifyCustom(results, samples, transcripts.producer)
  } else if (manifest.scenario === 'telemetry-payload') {
    assert.equal(results.rawControl.errorCode, 118)
    assert.equal(results.rawControl.transmittedBytes, 4097)
    assert.notEqual(results.rawControl.uuid, results.uuid)
    const raw = oracles.rawControl
    assert.equal(
      raw.probeSha256,
      '139afdbec5034f7c1498b90306889fd918ae13097efee1ea9f435b03dcf9b1b1'
    )
    assert.equal(raw.advertisement.code, 0)
    assert.equal(raw.control.code, 4)
    assert.equal(raw.control.signal, null)
    assert.equal(raw.vector.vectors.oversize.hex, '00'.repeat(4097))
    for (const child of [raw.advertisement, raw.control])
      assert.deepEqual(child.stdout.trim().split('\n').map(JSON.parse), child.lines)
    assert.equal(raw.advertisement.lines.length, 1)
    assert.equal(raw.advertisement.lines[0].key71[0], 0)
    assert.equal(raw.advertisement.lines[0].key72[0], 0)
    assert.equal(raw.control.lines.length, 2)
    const [getControl, pushControl] = raw.control.lines
    assert.equal(getControl.errorCode, 0)
    assert.equal(getControl.clientInstanceId, results.rawControl.uuid)
    assert.equal(pushControl.clientInstanceId, getControl.clientInstanceId)
    assert.equal(pushControl.errorCode, 118)
    assert.equal(pushControl.sentBytes, 4097)
    assert.equal(pushControl.rawSha256, sha(Buffer.alloc(4097)))
    assert.equal(getControl.fullyConsumed, true)
    assert.equal(pushControl.fullyConsumed, true)
    assert.deepEqual(reader.readExports({ uuid: results.rawControl.uuid }), [])
    for (const reported of results.candidate.samples) {
      const actual = samples[`producer-candidate:${reported.requestKey}`]
      assert(actual, 'Invented candidate sample')
      for (const key of ['rawBytes', 'transmittedBytes', 'compressionType'])
        assert.equal(reported[key], actual[key], 'Contradicted payload result')
      assert.deepEqual(reported.partialPoints, [])
    }

    const subscription = transcripts['producer-candidate'].records.find(
      (r) => r.apiKey === 71 && r.response?.errorCode === 0
    )
    assert.deepEqual(results.candidate.offered, subscription.response.acceptedCompressionTypes)
    // eslint-disable-next-line n/no-unsupported-features/node-builtins -- live Node lines supply the built-in codec
    const zstd = typeof zlib.zstdCompressSync === 'function'
    const expectedCodec =
      subscription.response.acceptedCompressionTypes.find((t) => t === 1 || (t === 4 && zstd)) ?? 0
    assert.equal(results.candidate.expectedCodec, expectedCodec)
    const stream = transcripts['producer-candidate'].records.filter(
      (r) => r.apiKey === 72 && !r.terminating
    )
    assert(stream.filter((r) => r.response?.errorCode === 0 && r.metricsBytes > 0).length >= 3)
    assertCadence(stream.slice(1), stream[0].response.responseAt, 1000)
    for (const { decoded, compressionType, rawBytes } of Object.values(samples)) {
      const ids = []
      for (const metric of decoded.metrics)
        for (const point of metric.points) {
          const attrs = Object.fromEntries(
            point.attrs.map(([k, type, value]) => {
              assert.equal(type, 'string')
              return [k, value]
            })
          )
          const id = Number(attrs.series)
          assert(Number.isInteger(id) && id >= 0 && id < 128)
          assert.equal(metric.name, `com.example.payload.${id}`)
          assert.equal(metric.kind, 'GAUGE')
          assert.equal(metric.unit, '1')
          assert.deepEqual(attrs, { series: String(id), pad: 'x'.repeat(128) })
          assert.equal(point.valueKind, 'AS_DOUBLE')
          assert.equal(Number(point.asDouble), id)
          ids.push(id)
        }
      assert(ids.length > 0 && ids.length < 128)
      assert.equal(new Set(ids).size, ids.length)
      const reported = results.candidate.samples.find(
        (r) => samples[`producer-candidate:${r.requestKey}`]?.decoded === decoded
      )
      if (reported) assert.deepEqual(reported.pointIds, ids, 'Contradicted point identity result')
      if (rawBytes > 1000) assert.equal(compressionType, results.candidate.expectedCodec)
    }
  } else if (manifest.scenario === 'telemetry-java') {
    for (const kind of KINDS) {
      assertOwnerUuid(transcripts[`js-${kind}`], results.uuids.js[kind])
      assert(
        transcripts[`js-${kind}`].records.filter(
          (r) =>
            r.apiKey === 72 && !r.terminating && r.metricsBytes > 0 && r.response?.errorCode === 0
        ).length >= 2,
        'Too few JS interop exports'
      )
      for (const sample of Object.entries(samples)
        .filter(([key]) => key.startsWith(`js-${kind}:`))
        .map(([, value]) => value)) {
        assert.equal(sample.request.uuid, results.uuids.js[kind])
        assertJsCounter(kind, sample.decoded)
      }
    }
    verifyJavaInterop(record, path.join(directory, manifest.root), transcripts)
  } else if (manifest.scenario === 'telemetry-outcomes') {
    assert.deepEqual(
      Object.keys(oracles).sort(),
      [
        'control',
        'optin',
        ...(record.mode === 'configured' ? [...FAULT_MODES, 'share'] : []),
      ].sort(),
      'Required ordinary oracle catalogue differs'
    )
    for (const [name, arm] of Object.entries(results.arms)) {
      assertCorpus(arm)
      verifyCorpusInputs(arm, oracles[name])
      assert.deepEqual(
        oracles[name].expected.map((r) => r.key).sort(),
        Array.from(
          { length: 1000 },
          (_, i) => `${record.run.stem}-${name}-${String(i).padStart(4, '0')}`
        ).sort(),
        'Ordinary arm corpus identity differs'
      )
      for (const kind of ['producer', 'consumer']) {
        const wire = transcripts[`${name}-${kind}`]
        const ordinary = wire.records.filter(
          (r) => r.type === 'request' && ![17, 18, 36, 71, 72].includes(r.apiKey)
        )
        assert(
          ordinary.length > 0 &&
            ordinary.every((r) => r.clientId === `${record.run.stem}-${name}-${kind}`),
          'Missing ordinary arm work'
        )
        const requiredApis = kind === 'producer' ? [0] : [1, 8]
        assert(
          requiredApis.every((api) => ordinary.some((r) => r.apiKey === api)),
          'Missing ordinary arm data or commit requests'
        )
        if (record.mode === 'base' || name === 'control') assertNoTelemetryRequests(wire)
        else {
          const get = wire.records.find((r) => r.apiKey === 71 && r.response?.errorCode === 0)
          assert(get, 'Missing opted-in arm subscription')
          assertOwnerUuid(wire, get.response.uuid)
        }
      }
    }
    if (record.mode === 'configured')
      verifyShare(
        results.share,
        oracles.share,
        transcripts,
        `${record.run.stem}-share-topic`,
        samples
      )
    else {
      assertNoTelemetryRequests(transcripts.share)
      assertShareRefusal(results.share, record.run.line)
    }
    for (const recovery of Object.values(results.faults).flat())
      verifyRecovery(recovery, transcripts[recovery.owner], samples)
    const [control, ...others] = Object.values(results.arms)
    for (const arm of others) {
      assert.deepEqual(arm.rawCommitted, control.rawCommitted)
      assert.deepEqual(arm.rawEnd, control.rawEnd)
    }
  }
}

/**
 * Keeps the broker log lines the cache proof reads, with the size and digest of the whole window,
 * so a shared trace chunk stays small while remaining bound to the complete log it came from.
 * @param {object} window one contiguous log window
 * @param {number} window.index ordinal of the window within the run
 * @param {string} window.since inclusive start
 * @param {string} window.until exclusive end
 * @param {Object<string,{text:string,truncated:boolean}>} window.logs full log of each broker node
 * @returns {object} the retained lines per node and the digest of each complete log
 */
const traceChunk = ({ index, since, until, logs }) => ({
  index,
  since,
  until,
  logs: Object.fromEntries(
    Object.entries(logs).map(([node, log]) => [
      node,
      {
        text: log.text
          .split('\n')
          .filter((line) => TRACE_LINE.test(line))
          .join('\n'),
        truncated: log.truncated,
      },
    ])
  ),
  complete: Object.fromEntries(
    Object.entries(logs).map(([node, log]) => [
      node,
      {
        bytes: Buffer.byteLength(log.text),
        sha256: sha(log.text),
        lines: log.text.split('\n').length,
      },
    ])
  ),
})

/**
 * @param {object} event a cache retirement from `cacheRetirement`
 * @returns {object} the part that stays identical however far the log window extends
 */
const stableCacheEvent = ({ source, node, connectionIdentity: identity, connectionId, line }) => ({
  source,
  node,
  connectionIdentity: identity,
  connectionId,
  line,
})

/**
 * @param {object[]} chunks consecutive trace chunks
 * @param {number} from first chunk index
 * @param {number} to last chunk index, inclusive
 * @returns {Object<string,{text:string,truncated:boolean}>} their per-node concatenation
 */
const joinChunks = (chunks, from, to) =>
  Object.fromEntries(
    ['1', '2', '3'].map((node) => {
      const part = chunks.slice(from, to + 1)
      return [
        node,
        {
          text: part.map((chunk) => chunk.logs[node].text).join('\n'),
          truncated: part.some((chunk) => chunk.logs[node].truncated),
        },
      ]
    })
  )

/**
 * The log of one old connection's episode: the window that holds its Get, whole, followed by every
 * later window reduced to its cache lines. Later windows hold later lifecycles of the same client,
 * whose identical client id and correlation would make the connection's own Get ambiguous.
 * @param {object[]} chunks consecutive trace chunks
 * @param {number} ordinal window that holds the connection's Get
 * @param {number} last last window to include, inclusive
 * @returns {Object<string,{text:string,truncated:boolean}>} the per-node episode log
 */
const episodeLogs = (chunks, ordinal, last) =>
  Object.fromEntries(
    ['1', '2', '3'].map((node) => {
      const later = chunks.slice(ordinal + 1, last + 1).map((chunk) => ({
        text: chunk.logs[node].text
          .split('\n')
          .filter((line) => !line.includes('GET_TELEMETRY_SUBSCRIPTIONS'))
          .join('\n'),
        truncated: chunk.logs[node].truncated,
      }))
      const all = [chunks[ordinal].logs[node], ...later]
      return [
        node,
        { text: all.map((part) => part.text).join('\n'), truncated: all.some((p) => p.truncated) },
      ]
    })
  )

/**
 * @param {object} transcript one segment's wire ledger
 * @returns {object[]} its accepted, nonempty Push requests
 */
const acceptedPushes = (transcript) =>
  transcript.records.filter(
    (r) =>
      r.type === 'request' && r.apiKey === 72 && r.response?.errorCode === 0 && r.metricsBytes > 0
  )

/**
 * @param {object} transcript one segment's wire ledger
 * @returns {object} its first successful Get, the connect of that Get's socket, node and port
 * @throws {Error} when the segment never obtained a subscription
 */
const lifecycleOf = (transcript) => {
  const get = transcript.records.find((r) => r.apiKey === 71 && r.response?.errorCode === 0)
  assert(get, 'Segment without a successful subscription')
  const connect = transcript.records.find(
    (r) => r.type === 'connect' && r.socketId === get.socketId
  )
  assert(connect, 'Subscription socket without a connect record')
  return { get, connect, node: get.node, port: connect.port }
}

/**
 * Proves one lifecycle boundary of one client kind: the three ordering notes, the final of the
 * closing segment, the replacement stream and its accepted Push, and the cache retirement of the
 * old connection read from the shared trace chunks to the end of the run.
 * @param {object} input everything the boundary is derived from
 * @returns {void}
 * @throws {Error} naming the first contradicted proof
 */
const verifyBoundary = ({
  kind,
  round,
  uuid,
  claimed,
  journal,
  from,
  to,
  chunks,
  source,
  lifecycles,
}) => {
  const fromId = from.owner
  const toId = to.owner
  assert.equal(claimed?.round, round, `Missing ${kind} round ${round}`)
  assert.deepEqual(
    [claimed.from, claimed.to],
    [fromId, toId],
    'Round segments differ from the schedule'
  )
  const note = (type) => {
    const found = journal.filter((n) => n.type === type && n.from === fromId && n.to === toId)
    assert.equal(found.length, 1, `Missing or duplicate ${type} note for ${toId}`)
    return found[0]
  }
  const [select, resolved, called] = ['select', 'disconnect-resolved', 'connect-called'].map(note)
  assert(select.seq < resolved.seq, 'The next segment was not selected before the close resolved')
  assert.equal(called.seq, resolved.seq + 1, 'Connect was not the next public action')
  const [closure] = from.closures
  assert.equal(from.closures.length, 1, 'A lifecycle must have exactly one public close')
  assert(
    claimed.observedAt <= select.at &&
      select.at <= closure.at &&
      closure.end <= resolved.at &&
      resolved.at <= called.at,
    'Boundary times are out of order'
  )
  assert.deepEqual(
    from.records.filter(
      (r) =>
        (r.type === 'connect' && r.at >= closure.at) ||
        (r.type === 'request' && [17, 18, 36, 71].includes(r.apiKey) && r.writeAt >= closure.at)
    ),
    [],
    'Get, connect or authentication during the final'
  )
  const finals = from.records.filter((r) => r.apiKey === 72 && r.terminating)
  assert.equal(finals.length, 1, 'Expected exactly one final at close')
  assert(
    finals[0].response?.errorCode === 0 &&
      finals[0].writeAt >= closure.at &&
      finals[0].writeAt <= closure.end &&
      finals[0].response.responseAt <= closure.at + FINAL_WINDOW_MS,
    'The final was not accepted inside the close window'
  )
  assert.equal(claimed.cachedUuid, uuid, 'The cached timeout-zero UUID differs')
  const gets = to.records.filter((r) => r.apiKey === 71)
  assert(
    gets.every((r) => r.uuid === uuid),
    'A replacement Get did not carry the cached UUID'
  )
  const { get } = lifecycles[round]
  assert.equal(get.response.uuid, uuid)
  assert(get.writeAt >= called.at, 'The replacement Get precedes the reconnect')
  assert(lifecycles[round].connect.at >= called.at, 'The replacement socket precedes the reconnect')
  const push = acceptedPushes(to).find(
    (r) =>
      !r.terminating &&
      r.uuid === uuid &&
      r.socketId === get.socketId &&
      r.index > get.index &&
      r.writeAt > get.response.responseAt
  )
  assert(push, 'No accepted current normal Push after the replacement Get')
  assert.equal(claimed.push, push.requestKey, 'The advertised Push is not the first current one')
  assert.equal(claimed.get, get.requestKey)
  const rejected = to.records.filter(
    (r) => r.apiKey === 72 && !r.terminating && r.response?.errorCode
  )
  assert(
    rejected.length <= 1 && rejected.every((r) => r.response.errorCode === 42),
    'Corrective Push budget exceeded'
  )
  const last = chunks.length - 1
  const old = lifecycles[round - 1]
  const localPort = BROKER_LISTENERS[old.port]
  const identity = connectionIdentity({
    logs: joinChunks(chunks, round - 1, round - 1),
    get: old.get,
    connect: old.connect,
    node: old.node,
    localPort,
  })
  const retired = cacheRetirement({
    logs: episodeLogs(chunks, round - 1, last),
    node: old.node,
    localPort,
    get: old.get,
    connect: old.connect,
    uuid,
    since: chunks[round - 1].since,
    completedAt: chunks[last].until,
    source,
  })
  assert.equal(retired.source, 'broker-disconnect-trace')
  assert.deepEqual(
    retired.connectionIdentity,
    identity,
    'The retired connection is not the old one'
  )
  assert.deepEqual(stableCacheEvent(retired), claimed.cacheEvent, 'Contradicted cache retirement')
}

/**
 * Re-derives the complete close-and-reopen proof from the sealed segments: the fixed catalogue,
 * per-segment wire, payload, decode and sink joins, each lifecycle boundary, the four timeout-zero
 * arms, the blackhole arm's cap of `min(1000, T) + 100` ms for its finite remaining budget T,
 * budgets and the strict-seam links. Resource acceptance is replayed only in memory-gates.
 * Advertised summaries are never trusted.
 * @param {object} record stamped evidence
 * @param {string} directory absolute run directory
 * @param {object} manifest sidecar manifest of the segmented archive
 * @param {function(string):object} read verified JSON sidecar reader
 * @returns {void}
 * @throws {Error} for a missing, extra, swapped or altered segment, proof or boundary
 */
const verifyShutdown = (record, directory, manifest, read) => {
  const scenario = manifest.scenario
  const schedule = SCHEDULES[scenario]
  const stem = record.run.stem
  const { results } = record
  const listed = new Set(manifest.files.map((f) => f.path))
  const bytesOf = (relative) => {
    assert(listed.has(relative), 'Unlisted sidecar')
    return fs.readFileSync(path.join(directory, relative))
  }
  assert.deepEqual(
    read(`${manifest.root}/index.json`),
    { format: SEGMENT_FORMAT, scenario, segments: manifest.segments },
    'Segment index differs from the manifest'
  )
  const identities = segmentIdentities(scenario)
  assert.deepEqual(
    manifest.segments.map((s) => s.id).sort(),
    segmentCatalogue(scenario),
    'Required segment catalogue differs'
  )
  for (const kind of schedule.kinds)
    assert.equal(
      manifest.segments.filter((s) => s.kind === kind && s.role === 'ordinary').length,
      schedule.perKind,
      `Segment count of ${kind} differs`
    )
  const transcripts = {}
  const bundles = {}
  const used = new Set()
  const suffix = { ordinary: 'ep', timeout0: 't0', blackhole: 'bh' }
  for (const entry of manifest.segments) {
    const meta = identities.get(entry.id)
    assert.deepEqual(
      { kind: entry.kind, role: entry.role },
      { kind: meta.kind, role: meta.role },
      'Segment identity differs'
    )
    const files = {
      transcript: `${manifest.root}/segments/${entry.id}.transcript.json`,
      bundle: `${manifest.root}/segments/${entry.id}.bundle.jsonl`,
    }
    assert.deepEqual(
      { transcript: entry.transcript, bundle: entry.bundle },
      files,
      'Segment path does not belong to its identity'
    )
    for (const file of Object.values(files)) {
      assert(!used.has(file), 'Reused segment path')
      used.add(file)
    }
    const checked = checkSegmentFiles({
      id: entry.id,
      transcript: bytesOf(files.transcript),
      bundle: bytesOf(files.bundle).toString('utf8'),
    })
    const transcript = checked.transcript
    const requests = transcript.records.filter((r) => r.type === 'request')
    assert(
      requests.length > 0 &&
        requests.every((r) => r.clientId === `${stem}-${meta.kind}-${suffix[meta.role]}`),
      'Segment client identity differs'
    )
    assert.deepEqual(transcript.failures, [])
    transcripts[entry.id] = transcript
    bundles[entry.id] = checked
  }
  const errors = Object.fromEntries(
    [...identities].filter(([, meta]) => meta.role === 'ordinary').map(([id]) => [id, [42]])
  )
  assert.deepEqual(
    record.allowed?.errors,
    errors,
    'Declared error allowance differs from the schedule'
  )
  assertStreamAccounted(transcripts, { errors })
  const sink = new Map(
    new TelemetryExportReader(path.join(directory, manifest.root))
      .readExports()
      .map((row) => [`${row.node}:${row.seq}`, row])
  )
  const consumed = new Set()
  const actual = []
  for (const id of Object.keys(transcripts).sort()) {
    const transcript = transcripts[id]
    assert(
      !transcript.records.some((r) => r.apiKey === 72 && r.metricsBytes === 0),
      'Unexpected zero-byte Push'
    )
    for (const request of acceptedPushes(transcript)) {
      const proof = bundles[id].proofs.get(request.requestKey)
      const { decoded } = proof
      assertPushProof({
        request,
        subscription: lastSubscription(transcript, request),
        decoded,
        line: record.run.line,
        transmitted: () => bundles[id].pushes.get(request.requestKey).blob,
        stored: () => proof.raw,
        receipts: () => proof.receipts,
      })
      assert.equal(decoded.exportIdentity.node, request.node)
      const token = `${decoded.exportIdentity.node}:${decoded.exportIdentity.seq}`
      assert(!consumed.has(token), 'Reused sink record')
      consumed.add(token)
      const row = sink.get(token)
      assert(row, 'Missing sink record')
      assert.equal(row.clientInstanceId, request.uuid)
      assert.equal(row.clientId, request.clientId)
      assert.equal(row.dataSha256, decoded.rawSha256)
      assert.equal(row.dataBytes, decoded.rawBytes)
      assert.equal(row.terminating, request.terminating)
      assert.equal(row.parse, 'ok')
      actual.push({ owner: id, requestKey: request.requestKey })
    }
  }
  assertCatalogue(record.pushes, actual)
  assert.deepEqual(record.zeroPushes, [], 'Unexpected zero-byte exception catalogue')
  const oracles = read(`${manifest.root}/oracles.json`)
  assert.deepEqual(Object.keys(oracles), ['traces'], 'Required trace oracle catalogue differs')
  const { chunks, source, loggerReadbacks } = oracles.traces
  assert.equal(chunks.length, schedule.perKind, 'Trace chunk catalogue differs')
  chunks.forEach((chunk, index) => {
    assert.equal(chunk.index, index)
    if (index)
      assert.equal(chunk.since, chunks[index - 1].until, 'Trace windows are not contiguous')
  })
  assert.equal(source.receipt.code, 0)
  assert.equal(source.receipt.stdout.split(/\s+/)[0], source.sha256)
  assert.deepEqual(loggerReadbacks, results.loggerReceipts)
  assert.deepEqual(
    results.restoredLevels,
    results.originalLevels,
    'Logger overrides were not restored'
  )
  const levels = (text) =>
    Object.fromEntries(
      ['org.apache.kafka.server.ClientMetricsManager', 'kafka.request.logger'].map((logger) => [
        logger,
        new RegExp(`^\\s*${logger.replaceAll('.', '\\.')}=(\\w+)`, 'm').exec(text)?.[1] ?? null,
      ])
    )
  for (const id of ['1', '2', '3']) {
    const reads = results.loggerReceipts.filter((r) => r.id === id && r.action[0] === '--describe')
    assert(reads.length >= 3 && reads.every((r) => r.result.code === 0 && !r.result.truncated))
    assert.deepEqual(levels(reads[0].result.stdout), results.originalLevels[id])
    assert.deepEqual(levels(reads.at(-1).result.stdout), results.restoredLevels[id])
  }
  for (const kind of schedule.kinds) {
    const ids = Array.from({ length: schedule.perKind }, (_, index) => schedule.name(kind, index))
    const lifecycles = ids.map((id) => lifecycleOf(transcripts[id]))
    const uuid = lifecycles[0].get.response.uuid
    assert.notEqual(uuid, '00000000-0000-0000-0000-000000000000')
    assert.equal(results.initial?.[kind]?.uuid, uuid, 'The initial UUID differs')
    assert(acceptedPushes(transcripts[ids[0]]).filter((r) => !r.terminating).length >= 2)
    const rounds = results.rounds?.[kind]
    assert(Array.isArray(rounds) && rounds.length === SHUTDOWN_ROUNDS, `Missing ${kind} rounds`)
    const journal = results.journal?.[kind]
    assert.equal(journal?.length, 3 * SHUTDOWN_ROUNDS, `${kind} ordering journal differs`)
    const ownerOf = (id) => ({ ...transcripts[id], owner: id })
    for (let round = 1; round <= SHUTDOWN_ROUNDS; round++)
      verifyBoundary({
        kind,
        round,
        uuid,
        claimed: rounds[round - 1],
        journal,
        from: ownerOf(ids[round - 1]),
        to: ownerOf(ids[round]),
        chunks,
        source,
        lifecycles,
      })
    const [closure] = transcripts[ids.at(-1)].closures
    const final = transcripts[ids.at(-1)].records.filter((r) => r.apiKey === 72 && r.terminating)
    assert(
      final.length === 1 &&
        final[0].response?.errorCode === 0 &&
        final[0].writeAt >= closure.at &&
        final[0].writeAt <= closure.end,
      'The terminal segment final is not accepted at close'
    )
    for (const id of ids) {
      const keys = transcripts[id].records
        .filter((r) => r.type === 'request')
        .map((r) => r.requestKey)
      assert.equal(new Set(keys).size, keys.length, `Duplicate request identity in ${id}`)
    }
  }
  for (const kind of ['producer', 'consumer', 'admin', 'share']) {
    const transcript = transcripts[`${kind}-t0`]
    const zero = results.timeout0?.[kind]
    assertOwnerUuid(transcript, zero?.uuid)
    assert.equal(zero.finalZeroTimeoutWrites, 0)
    assert.equal(transcript.closures.length, 1)
    const [closure] = transcript.closures
    assert.deepEqual(
      transcript.records.filter(
        (r) =>
          (r.type === 'connect' && r.at >= closure.at) ||
          (r.type === 'request' && [71, 72].includes(r.apiKey) && r.writeAt >= closure.at)
      ),
      [],
      'Telemetry work followed a zero-timeout close'
    )
  }
  const blackhole = transcripts['admin-bh']
  const hole = results.blackhole
  assertOwnerUuid(blackhole, hole?.uuid)
  const [close] = blackhole.closures
  assert.equal(blackhole.closures.length, 1)
  const faulted = blackhole.sockets.find((s) => s.id === hole.faultedSocket)
  assert(
    faulted?.kind === 'telemetry' && Number.isFinite(faulted.closedAt),
    'Blackholed transport survived'
  )
  assert(
    Number.isFinite(hole.remainingT) && hole.remainingT > 0 && hole.remainingT < FINAL_CAP_MS,
    'The blackholed close needs a finite remaining budget below the final cap'
  )
  const cap = Math.min(FINAL_CAP_MS, hole.remainingT) + SCHEDULING_MS
  assert(faulted.closedAt - close.at <= cap, 'The blackholed transport outlived its cap')
  assert(hole.telemetryCleanupMs <= cap, 'The blackholed telemetry cleanup outlived its cap')
  assert(blackhole.records.filter((r) => r.apiKey === 72 && r.terminating).length <= 1)
  assert.deepEqual(
    blackhole.records.filter(
      (r) =>
        (r.type === 'connect' && r.at >= close.at) ||
        (r.type === 'request' && [17, 18, 36, 71].includes(r.apiKey) && r.writeAt >= close.at)
    ),
    [],
    'Get, connect or authentication during the blackholed final'
  )
  const normal = new Set(blackhole.sockets.filter((s) => s.kind === 'normal').map((s) => s.id))
  assert(
    blackhole.records.some(
      (r) =>
        r.type === 'request' &&
        normal.has(r.socketId) &&
        r.response &&
        r.writeAt <= close.end &&
        r.response.responseAt >= close.at
    ),
    'No core work succeeded beside the blackholed close'
  )
  assert.equal(hole.core?.sameResult, true, 'The core result identity was not preserved')
  assert(results.workMs <= SHUTDOWN_WORK_MS, 'The work exceeded its deadline')
  assert(record.timing.cleanupMs <= SHUTDOWN_CLEANUP_MS, 'Cleanup exceeded its budget')
  assert(
    results.workMs + record.timing.cleanupMs <= SHUTDOWN_TOTAL_MS,
    'The case exceeded its total budget'
  )
  verifySeams(results.seams)
}

/**
 * Requires the close-and-reopen case's links to its fixed strict-seam catalogue, each title still
 * present in its unit file of this tree.
 * @param {object[]} seams the archived links
 * @returns {void} @throws {Error} for a missing, extra, reordered or deleted link
 */
const verifySeams = (seams) => {
  assert.deepEqual(
    seams,
    SHUTDOWN_SEAMS.map(([file, title]) => ({ file, title, lane: 'strict seam, unit lane' })),
    'Strict-seam links differ from the fixed catalogue'
  )
  for (const [file, title] of SHUTDOWN_SEAMS)
    assert(
      fs.readFileSync(path.resolve(__dirname, '../../..', file), 'utf8').includes(title),
      `The strict seam "${title}" no longer exists`
    )
}

/**
 * The quiet-window resource proof of a case, from its two archived readings: the fixed identity,
 * role and registration source catalogues; no owned socket, segment or pending public call; no
 * resource the case or the quiet measurement created; no unknown-owner resource the baseline did
 * not already hold, so a same-count substitution fails by identity; the same process listener
 * registrations with multiplicity; and the same event-loop counts.
 * @param {{baseline:object, final:object}} readings the snapshots before the case and after quiet
 * @returns {void}
 * @throws {Error} naming the first missing source or surviving identity
 */
const verifyQuietResources = ({ baseline, final }) => {
  const identities = ['openSockets', 'pending', 'undischarged']
  for (const reading of [baseline, final]) {
    assert.deepEqual(
      Object.keys(reading?.identities ?? {}).sort(),
      identities,
      'Required resource source catalogue differs'
    )
    assert.deepEqual(
      Object.keys(reading.resources ?? {}).sort(),
      ['case', 'quiet', 'unknown'],
      'Required resource role catalogue differs'
    )
    assert.deepEqual(
      Object.keys(reading.registrations ?? {}),
      ['process'],
      'Required registration source differs'
    )
  }
  assert.deepEqual(
    final.identities,
    { openSockets: [], pending: [], undischarged: [] },
    'Owned resources survive the quiet window'
  )
  for (const role of ['case', 'quiet'])
    assert.deepEqual(final.resources[role], {}, `A ${role} resource survives the quiet window`)
  for (const [type, ids] of Object.entries(final.resources.unknown))
    for (const id of ids)
      assert(
        (baseline.resources.unknown[type] ?? []).includes(id),
        'An unknown-owner resource created during the case survives'
      )
  assert.deepEqual(
    final.registrations,
    baseline.registrations,
    'Listener registrations differ from the baseline'
  )
  assert.deepEqual(final.activeResources, baseline.activeResources, 'Quiet resources differ')
}

/**
 * Recomputes the ordinary corpus from the archived raw log, handler multiset and broker commits.
 * @param {object} result advertised summary
 * @param {object} inputs independent ledgers
 * @returns {void}
 * @throws {Error} for an omission, duplicate, changed value, offset or contradicted summary
 */
const verifyCorpusInputs = (result, inputs) => {
  assert(inputs, 'Missing independent corpus ledgers')
  assert.equal(inputs.expected.length, 1000)
  const digest = (keys) => sha(Buffer.from([...keys].sort().join('\n')))
  const expected = new Map(inputs.expected.map((r) => [r.key, r]))
  assert.equal(expected.size, 1000)
  assert.equal(inputs.log.length, 1000)
  assert.equal(inputs.handled.length, 1000)
  const projection = (rows) => rows.map((r) => `${r.partition}:${r.offset}:${r.key}`).sort()
  assert.deepEqual(
    projection(inputs.handled),
    projection(inputs.log),
    'Handler ledger differs from durable log'
  )
  assert.equal(new Set(projection(inputs.log)).size, 1000)
  const ends = {}
  for (const row of inputs.log) {
    const original = expected.get(row.key)
    assert(original, 'Unknown durable record')
    assert.equal(row.valueHex, original.valueHex)
    assert.equal(row.partition, original.partition)
    assert.equal(row.offset, String(ends[row.partition] ?? 0))
    ends[row.partition] = (ends[row.partition] ?? 0) + 1
  }
  const rawEnd = Object.fromEntries(Object.entries(ends).map(([p, n]) => [p, String(n)]))
  const rows = inputs.commits[0]?.partitions ?? inputs.commits
  const committed = Object.fromEntries(rows.map((r) => [r.partition, String(r.offset)]))
  assert.deepEqual(committed, rawEnd)
  assert.deepEqual(inputs.ends, rawEnd)
  assert.deepEqual(result.rawEnd, rawEnd)
  assert.deepEqual(result.rawCommitted, committed)
  assert.equal(result.expectedSha256, digest([...expected.keys()]))
  assert.equal(result.logSha256, digest(inputs.log.map((r) => r.key)))
  assert.equal(result.handlerSha256, digest(inputs.handled.map((r) => r.key)))
  assert.equal(result.handlerEntries, inputs.handled.length)
}

/**
 * Re-derives literal custom controls, update ordering, write-fenced removal and fresh Sum origin.
 * @param {object} results scenario markers
 * @param {object} samples complete joined Pushes
 * @param {object} transcript producer wire ledger
 * @returns {void}
 * @throws {Error} for changed values, an old post-fence point or a reused origin
 */
const verifyCustom = (results, samples, transcript) => {
  const sample = (key) => {
    const item = samples[`producer:${key}`]
    assert(item, 'Missing custom Push')
    return item
  }
  const points = (key) => {
    const index = sample(key).request.index
    const names =
      index <= results.removal.removeFence
        ? ['anchor', 'negative', 'total', 'zero']
        : index <= results.readded.readdFence
          ? ['anchor', 'negative']
          : index <= results.unregisterFence
            ? ['anchor', 'negative', 'total']
            : []
    assertCustomShape(
      sample(key).decoded,
      names.map((name) => `com.example.${name}`)
    )
    return Object.fromEntries(
      sample(key).decoded.metrics.map((m) => {
        assert.equal(m.points.length, 1)
        assert.equal(m.unit, '1')
        assert.deepEqual(m.points[0].attrs, [['series', 'string', 'live']])
        if (m.name.endsWith('.total')) {
          assert.equal(m.kind, 'SUM')
          assert.equal(m.monotonic, true)
          assert.equal(m.temporality, 'AGGREGATION_TEMPORALITY_DELTA')
          assert.equal(m.points[0].valueKind, 'AS_INT')
        } else {
          assert.equal(m.kind, 'GAUGE')
          assert.equal(m.points[0].valueKind, 'AS_DOUBLE')
        }
        assert(BigInt(m.points[0].startNs) <= BigInt(m.points[0].timeNs))
        return [m.name.slice('com.example.'.length), m.points[0]]
      })
    )
  }
  const first = points(results.firstRequestKey)
  assert.deepEqual(Object.keys(first).sort(), ['anchor', 'negative', 'total', 'zero'])
  assert.equal(first.anchor.asDouble, '1.0')
  assert.equal(first.zero.asDouble, '0.0')
  assert.equal(first.zero.asDoubleBits, '0')
  assert.equal(first.negative.asDouble, '-2.5')
  assert.equal(first.negative.asDoubleBits, 'c004000000000000')
  assert.equal(first.total.asInt, '9007199254740993')
  const completeUpdate = transcript.records
    .filter(
      (r) =>
        r.apiKey === 72 &&
        r.response?.errorCode === 0 &&
        r.metricsBytes > 0 &&
        r.index > results.update.updateFence &&
        r.index <= results.removal.removeFence
    )
    .map((r) => r.requestKey)
  assert.deepEqual(results.update.requestKeys, completeUpdate, 'Incomplete custom update stream')
  const states = completeUpdate.map((key) => {
    assert(sample(key).request.index > results.update.updateFence)
    const p = points(key)
    const values = [p.zero.asDouble, p.negative.asDouble, p.total.asInt].join(',')
    const state = { '0.0,-2.5,0': 'old', '6.25,-3.5,9': 'updated', '6.25,-3.5,0': 'following' }[
      values
    ]
    assert(state, 'Contradicted custom update')
    return state
  })
  assert.deepEqual(states, results.update.states)
  assert(/^(old,)?updated,following(,following)*$/.test(states.join(',')))
  for (const item of Object.values(samples)) {
    const p = points(item.request.requestKey)
    assert(!Object.keys(p).some((key) => !['anchor', 'zero', 'negative', 'total'].includes(key)))
    if (item.request.index <= results.removal.removeFence) {
      assert.deepEqual(Object.keys(p).sort(), ['anchor', 'negative', 'total', 'zero'])
      assert.equal(p.anchor.asDouble, '1.0')
      if (item.request.index <= results.update.updateFence) {
        assert.equal(p.zero.asDouble, '0.0')
        assert.equal(p.negative.asDouble, '-2.5')
        assert.equal(
          p.total.asInt,
          item.request.requestKey === results.firstRequestKey ? '9007199254740993' : '0'
        )
        assert(BigInt(p.total.startNs) >= BigInt(first.total.startNs))
      }
    } else if (item.request.index <= results.readded.readdFence) {
      assert.deepEqual(Object.keys(p).sort(), ['anchor', 'negative'])
      assert.equal(p.anchor.asDouble, '1.0')
      assert.equal(p.negative.asDouble, '-3.5')
    } else if (item.request.index <= results.unregisterFence) {
      assert.deepEqual(Object.keys(p).sort(), ['anchor', 'negative', 'total'])
      assert.equal(p.anchor.asDouble, '1.0')
      assert.equal(p.negative.asDouble, '-3.5')
      assert(BigInt(p.total.startNs) >= BigInt(results.readded.startNs))
      assert.equal(
        p.total.asInt,
        item.request.requestKey === results.readded.requestKey ? '1' : '0'
      )
    }
  }
  const removal = sample(results.removal.requestKey)
  assert(removal.request.index > results.removal.removeFence)
  assert(removal.exportIdentity.seq > results.removal.cursor[removal.exportIdentity.node])
  assert.deepEqual(
    removal.decoded.metrics.map((m) => m.name),
    results.removal.names
  )
  for (const item of Object.values(samples))
    if (
      item.request.index > results.removal.removeFence &&
      item.request.index <= results.readded.readdFence
    ) {
      const names = item.decoded.metrics.map((m) => m.name)
      assert(names.includes('com.example.anchor'))
      assert(!names.includes('com.example.zero') && !names.includes('com.example.total'))
    }
  const again = points(results.readded.requestKey).total
  assert(sample(results.readded.requestKey).request.index > results.readded.readdFence)
  assert.equal(again.asInt, '1')
  assert(BigInt(again.startNs) > BigInt(first.total.startNs))
  assert.equal(again.startNs, results.readded.startNs)
  assert.equal(again.asInt, results.readded.asInt)
  for (const r of transcript.records.filter(
    (r) => r.apiKey === 72 && r.index > results.unregisterFence
  ))
    assert.equal(r.metricsBytes, 0, 'Custom points survived final unregister')
}

/**
 * Checks actual post-injection exchange, transport retirement and same-UUID recovery.
 * @param {object} recovery captured fault and recovery identities
 * @param {object} transcript member wire ledger
 * @param {object} samples complete verified accepted/decode/sink stream, keyed by owner and request
 * @returns {void}
 * @throws {Error} for an unexercised fault or replacement without a later joined normal Push
 */
const verifyRecovery = (recovery, transcript, samples) => {
  assert(recovery && transcript, 'Missing mandatory recovery')
  assertOwnerUuid(transcript, recovery.uuid)
  const request = (key) => transcript.records.find((r) => r.requestKey === key)
  const injected = request(recovery.injectedPush)
  assert(recovery.attempted.apiKey === 72 && recovery.attempted.at >= recovery.injectedAt)
  if (recovery.mode !== 'blackhole') {
    assert(injected && injected.apiKey === 72 && injected.index > recovery.checkpoint)
    assert.equal(injected.socketId, recovery.faultedSocket)
    assert.equal(recovery.attempted.correlation, injected.correlation)
  } else assert.equal(recovery.injectedPush, null)
  if (recovery.mode === 'drop') assert.equal(recovery.droppedCorrelation, injected.correlation)
  const socket = transcript.sockets.find((s) => s.id === recovery.faultedSocket)
  assert(
    Number.isFinite(socket.closedAt) &&
      socket.closedAt >= (injected?.writeAt ?? recovery.attempted.at)
  )
  const get = request(recovery.replacementGet)
  assert(
    get &&
      get.apiKey === 71 &&
      get.uuid === recovery.uuid &&
      get.socketId !== recovery.faultedSocket &&
      get.response?.errorCode === 0 &&
      get.response.uuid === recovery.uuid,
    'Invalid recovery replacement Get'
  )
  assert(get.writeAt >= socket.closedAt)
  const push = request(recovery.recoveredPush)
  assert(
    push &&
      push.apiKey === 72 &&
      push.socketId === get.socketId &&
      push.uuid === recovery.uuid &&
      push.response?.errorCode === 0 &&
      !push.terminating &&
      push.metricsBytes > 0 &&
      push.index > get.index &&
      push.writeAt > get.response.responseAt,
    'Invalid recovered Push'
  )
  assert.equal(
    samples[`${recovery.owner}:${push.requestKey}`]?.request,
    push,
    'Missing joined recovered Push'
  )
}

/**
 * Joins the created topic metadata, raw acquisitions, durable records and exact handler identities.
 * @param {object} inputs independent share ledgers
 * @param {string} topic independently configured topic name
 * @param {object[]} expected pre-produced key/value corpus
 * @returns {void}
 * @throws {Error} for a foreign topic, incomplete acquisition or changed handler identity
 */
const verifyShareIdentity = (inputs, topic, expected) => {
  assert.equal(inputs.topic, topic)
  assert.deepEqual(
    inputs.metadata.topics.map((t) => [t.name, t.topicId]),
    [[topic, inputs.topicId]]
  )
  assert(/^[a-f0-9]{8}(-[a-f0-9]{4}){3}-[a-f0-9]{12}$/.test(inputs.topicId))
  assert.notEqual(inputs.topicId, '00000000-0000-0000-0000-000000000000')
  assert.equal(expected.length, 10)
  const identities = expected.map(({ key }, index) => ({
    topic,
    partition: 0,
    offset: String(index),
    key,
  }))
  const projection = (rows) =>
    rows.map(({ topic, partition, offset, key }) => ({ topic, partition, offset, key }))
  assert.deepEqual(projection(inputs.log), identities, 'Share durable identities differ')
  assert.deepEqual(
    inputs.log.map((r) => r.valueHex),
    expected.map((r) => r.valueHex)
  )
  assert.deepEqual(projection(inputs.handled), identities, 'Share handler identities differ')
  const held = heldAcquisitions(inputs.frames, inputs.group)
  assert.deepEqual(inputs.held, held, 'Contradicted held acquisition ranges')
  const offsets = []
  for (const range of held) {
    assert.equal(range.topicId, inputs.topicId, 'Share acquisition topic differs')
    assert.equal(range.partition, 0)
    const first = BigInt(range.firstOffset)
    const last = BigInt(range.lastOffset)
    assert(first >= 0n && last < 10n && last >= first)
    for (let n = first; n <= last; n++) offsets.push(String(n))
  }
  assert.deepEqual(
    offsets.sort(),
    identities.map((r) => r.offset).sort(),
    'Incomplete or duplicate held acquisitions'
  )
}

/**
 * Requires one public settlement per exact topic/partition/offset, without discarding duplicates.
 * @param {object[]} settlements public events
 * @param {string} topic owned topic name
 * @param {function(number):string} type independently required decision
 * @returns {void}
 * @throws {Error} for extra, duplicate, foreign or incorrect public outcomes
 */
const verifyPublicSettlements = (settlements, topic, type) => {
  const offsets = Array.from({ length: 10 }, () => [])
  for (const event of settlements) {
    assert.equal(event.topic, topic, 'Public share settlement topic differs')
    assert.equal(event.partition, 0)
    const first = BigInt(event.firstOffset)
    const last = BigInt(event.lastOffset)
    assert(
      first >= 0n && last < 10n && first <= last,
      'Public share settlement outside owned offsets'
    )
    for (let n = first; n <= last; n++) offsets[Number(n)].push(event)
  }
  for (const [index, events] of offsets.entries()) {
    assert.equal(events.length, 1, 'Public share settlement must occur exactly once')
    assert.equal(events[0].type, type(index))
    if (type(index) === 'accept') assert.equal(events[0].error, null)
    else
      assert.deepEqual(events[0].error, {
        name: 'KafkaJSShareAcknowledgementError',
        reason: 'membership-lost',
        retriable: false,
        topic,
        partition: 0,
      })
  }
}

/**
 * Checks the accepted-prefix/rest-release broker ledger and redelivery before lock expiry.
 * @param {object} result share summary
 * @param {object} inputs archived independent ledgers
 * @param {object} transcripts owner transcripts
 * @param {string} topic independently configured created topic name
 * @param {object} samples complete verified accepted/decode/sink streams
 * @returns {void}
 * @throws {Error} for missing confirmations, duplicates, altered records or late release
 */
const verifyShare = (result, inputs, transcripts, topic, samples) => {
  assert(inputs, 'Missing share oracle')
  assert.equal(result.lockMs, 15000)
  assert.equal(inputs.acquiredAt, acquisitionStart(inputs.frames, inputs.group))
  assert(inputs.handlerAt >= inputs.acquiredAt)
  assert.equal(result.recovery?.owner, 'share', 'Missing configured share recovery')
  assert.equal(result.recovery.mode, 'drop')
  verifyRecovery(result.recovery, transcripts.share, samples)
  verifyShareIdentity(inputs, topic, inputs.expected)
  assert(inputs.held.every((r) => r.lockMs === result.lockMs))
  assert.deepEqual(
    confirmedDecisions(inputs.frames, inputs.group),
    inputs.confirmed,
    'Contradicted acknowledgement ledger'
  )
  assert.equal(inputs.confirmed.length, 4, 'Extra or duplicate broker share settlement')
  for (let index = 0; index < 4; index++) {
    const decisions = inputs.confirmed.filter((r) => r.offset === String(index))
    assert.equal(decisions.length, 1, 'Share settlement must occur exactly once')
    const [decision] = decisions
    assert.equal(decision.topicId, inputs.topicId, 'Broker share settlement topic differs')
    assert.equal(decision.partition, 0)
    assert.equal(decision.type, 1)
    assert.equal(decision.errorCode, 0)
    assert(
      decision.at >= inputs.acquiredAt && decision.at < inputs.acquiredAt + result.lockMs,
      'Settlement after lock expiry'
    )
  }
  const final = finalAcknowledgement(inputs.frames, inputs.group, inputs.topicId)
  assert.deepEqual(
    confirmedDecisions([final], inputs.group),
    inputs.confirmed,
    'Accepted prefix must be confirmed by the final acknowledgement'
  )
  assert(
    final.at >= inputs.handlerAt && final.responseAt < inputs.acquiredAt + result.lockMs,
    'Settlement after lock expiry'
  )
  const suffix = inputs.expected
    .slice(4)
    .map((r) => r.key)
    .sort()
  const identities = inputs.log.slice(4).map(({ topic, partition, offset, key, valueHex }) => ({
    topic,
    partition,
    offset,
    key,
    valueHex,
  }))
  const redeliveryIdentities = (rows) =>
    rows
      .map(({ topic, partition, offset, key, valueHex }) =>
        JSON.stringify([topic, partition, offset, key, valueHex])
      )
      .sort()
  assert.deepEqual(redeliveryIdentities(inputs.redelivered), redeliveryIdentities(identities))
  assert(
    inputs.redelivered.every(
      (r) =>
        r.deliveryCount >= 2 && r.at >= final.responseAt && r.at < inputs.acquiredAt + result.lockMs
    ),
    'Redelivery outside final-release window'
  )
  verifyPublicSettlements(inputs.settlements, topic, (index) => (index < 4 ? 'accept' : 'release'))
  assert.deepEqual(result.redelivered, suffix)
}

/**
 * Binds the mandatory admin RTT control to the complete ordinary ledger and next three Pushes.
 * Every controlled response must be allocated exactly once in the first two, followed by idle.
 * @param {object} results complete configured results
 * @param {object} transcript independent admin transcript
 * @param {object[]} samples verified complete admin Push stream
 * @returns {void}
 * @throws {Error} on invented identities, omitted windows, contamination or ambiguous partitions
 */
const verifyControl = (results, transcript, samples) => {
  const control = results.control
  assert(
    Number.isFinite(control.controlledAt) && control.controlledAt > control.knownAfter,
    'Invalid controlled request time'
  )
  const ordinaryIds = new Set(
    transcript.sockets.filter((s) => s.kind === 'normal').map((s) => s.id)
  )
  assert(
    Number.isSafeInteger(control.checkpoint) && control.checkpoint >= 0,
    'Invalid control checkpoint'
  )
  const discovery = transcript.records.filter(
    (r) =>
      r.type === 'request' &&
      ordinaryIds.has(r.socketId) &&
      control.discovery.some((d) => d.requestKey === r.requestKey)
  )
  assert.deepEqual(control.discovery, discovery, 'Invented discovery identity')
  assert(
    discovery.some((r) => r.apiKey === 3 && r.response?.responseAt === control.knownAfter),
    'Missing topology response'
  )
  assert(
    discovery.some((r) => r.apiKey === 32 && r.writeAt > control.knownAfter),
    'Missing topology follow-up'
  )
  const wire = transcript.records.filter(
    (r) => r.type === 'request' && ordinaryIds.has(r.socketId) && r.index > control.checkpoint
  )
  assert.deepEqual(control.wire, wire, 'Incomplete control wire ledger')
  assert(
    wire.length > 0 && wire.every((r) => r.response && r.writeAt >= control.controlledAt),
    'Missing or unmatched controlled request'
  )
  const keys = new Set(wire.map((r) => r.requestKey))
  const requests = results.admin.arithmetic.requests.filter((r) => keys.has(r.requestKey))
  assert.deepEqual(control.requests, requests, 'Invented control request')
  assert.equal(requests.length, wire.length)
  assert(
    requests.every((r) => r.knownNode),
    'Control node is unknown'
  )
  assert.equal(control.requestEvents.length, requests.length)
  for (const request of requests) {
    const events = control.requestEvents.filter(
      (e) =>
        e.correlationId === request.correlationId &&
        e.apiKey === request.apiKey &&
        e.apiVersion === request.apiVersion &&
        e.clientId === request.clientId
    )
    assert.equal(events.length, 1, 'Control event identity mismatch')
    for (const field of ['duration', 'sentAt', 'broker'])
      assert.equal(events[0][field], request[field])
  }
  const selected = samples.filter((sample) => sample.writeAt > control.controlledAt).slice(0, 3)
  assert.equal(selected.length, 3, 'Missing controlled or quiescent sample')
  assert.deepEqual(
    control.samples,
    selected.map((sample) => sample.requestKey),
    'Control sample order mismatch'
  )
  const indexes = selected.map((sample) => samples.indexOf(sample))
  const prior = samples[indexes[0] - 1]
  assert(
    prior && prior.writeAt > control.knownAfter && prior.writeAt <= control.controlledAt,
    'Missing pre-control quiescent sample'
  )
  const nodePoints = (sample) =>
    sample.decoded.metrics.filter((m) => m.kind === 'GAUGE').flatMap((m) => m.points)
  assert.equal(nodePoints(prior).length, 0, 'Pre-control sample is not quiescent')
  assert.equal(nodePoints(selected[2]).length, 0, 'Missing quiescent control result')
  const allocated = results.admin.arithmetic.allocations.flatMap(({ windows }) =>
    indexes.slice(0, 2).flatMap((index) => windows[index])
  )
  assert.deepEqual(
    allocated.sort(),
    [...keys].sort(),
    'Control next-two-sample allocation mismatch'
  )
  const emitted = new Set(
    selected
      .slice(0, 2)
      .flatMap(nodePoints)
      .map((p) => p.attrs[0][2])
  )
  assert.deepEqual([...emitted].sort(), [...new Set(requests.map((r) => r.node))].sort())
  // Multiple time-feasible partitions with identical arithmetic cannot establish this control.
  for (const node of emitted) {
    const events = requests.filter((r) => r.node === node)
    let matches = 0
    for (let split = 0; split <= events.length; split++) {
      const partitions = [events.slice(0, split), events.slice(split)]
      const compatible = partitions.every((part, i) => {
        const metrics = selected[i].decoded.metrics
        const points = (suffix) =>
          metrics
            .find((m) => m.name.endsWith(suffix))
            ?.points.filter((p) => p.attrs[0][2] === node) || []
        const avg = points('.avg')
        const max = points('.max')
        if (!part.length) return avg.length === 0 && max.length === 0
        if (part.some((r) => r.responseAt > selected[i].writeAt)) return false
        const next = partitions[i + 1] || []
        if (next.some((r) => r.responseAt <= prior.writeAt)) return false
        const mean = part.reduce((sum, r) => sum + r.duration, 0) / part.length
        return (
          avg.length === 1 &&
          max.length === 1 &&
          Math.abs(Number(avg[0].asDouble) - mean) <= 1e-9 + 1e-12 * Math.abs(mean) &&
          Number(max[0].asDouble) === Math.max(...part.map((r) => r.duration))
        )
      })
      if (compatible) matches++
    }
    assert(
      matches === 1 || (events.length === 1 && events[0].duration === 0 && matches > 0),
      'Ambiguous controlled response partition'
    )
  }
}

const CHURN_NAMES = Object.freeze({
  lifecycle: 'reopen',
  retire: 'retire',
  confirm: 'subscription',
})

/**
 * Re-derives the structure of the churn archive from the sealed segment files alone: the fixed
 * segment catalogue, every segment's owner, client, sockets and cleanup, the declared proof
 * selection, and the byte, response, node, subscription and sink join of every accepted Push, with
 * the selected ones decoded in full and tied to the parent's own decoder receipts.
 * @param {object} record stamped evidence @param {string} directory absolute run directory
 * @param {object} manifest the sidecar manifest @param {object} archive the parent's replay inputs
 * @returns {{transcripts:Object<string,object>, bundles:Object<string,object>,
 *   hashes:Set<string>}} the parsed transcript and checked bundle of every segment, by segment ID,
 *   and the raw hash of every decoded proof
 * @throws {Error} for a missing, extra, swapped, altered or unjoined segment, proof or Push
 */
const verifyChurnSegments = (record, directory, manifest, archive) => {
  const { TelemetryProofs } = require('../../soak/telemetryChurn')
  const schedule = SCHEDULES[CHURN]
  const stem = archive.stem
  const listed = new Set(manifest.files.map((f) => f.path))
  const bytesOf = (relative) => {
    assert(listed.has(relative), 'Unlisted sidecar')
    return fs.readFileSync(path.join(directory, relative))
  }
  assert.deepEqual(
    JSON.parse(bytesOf(`${manifest.root}/index.json`)),
    { format: SEGMENT_FORMAT, scenario: CHURN, segments: manifest.segments },
    'Segment index differs from the manifest'
  )
  const identities = segmentIdentities(CHURN)
  assert.deepEqual(
    manifest.segments.map((s) => s.id).sort(),
    segmentCatalogue(CHURN),
    'Required segment catalogue differs'
  )
  for (const kind of schedule.kinds)
    assert.equal(
      manifest.segments.filter((s) => s.kind === kind && s.role === 'ordinary').length,
      schedule.perKind,
      `Segment count of ${kind} differs`
    )
  const sink = new Map(
    new TelemetryExportReader(path.join(directory, manifest.root), {
      fileBytes: require('../../soak/churnReporter').SINK_BYTES,
    })
      .readExports()
      .map((row) => [`${row.node}:${row.seq}`, row])
  )
  const consumed = new Set()
  const transcripts = {}
  const bundles = {}
  const hashes = new Set()
  const used = new Set()
  const calls = new Map()
  for (const call of archive.decoder.calls) calls.set(call.rawSha256, call.kind)
  for (const entry of manifest.segments) {
    const meta = identities.get(entry.id)
    assert.deepEqual(
      { kind: entry.kind, role: entry.role },
      { kind: meta.kind, role: meta.role },
      'Segment identity differs'
    )
    const files = {
      transcript: `${manifest.root}/segments/${entry.id}.transcript.json`,
      bundle: `${manifest.root}/segments/${entry.id}.bundle.jsonl`,
    }
    assert.deepEqual(
      { transcript: entry.transcript, bundle: entry.bundle },
      files,
      'Segment path does not belong to its identity'
    )
    for (const file of Object.values(files)) {
      assert(!used.has(file), 'Reused segment path')
      used.add(file)
    }
    const transcriptBytes = bytesOf(files.transcript)
    const keys = TelemetryProofs.selection(JSON.parse(transcriptBytes).records)
    const checked = checkSegmentFiles({
      id: entry.id,
      transcript: transcriptBytes,
      bundle: bytesOf(files.bundle).toString('utf8'),
      proofs: keys,
    })
    const transcript = checked.transcript
    const requests = transcript.records.filter((r) => r.type === 'request')
    assert(
      requests.length > 0 && requests.every((r) => r.clientId === `${stem}-${meta.kind}`),
      'Segment client identity differs'
    )
    assert.deepEqual(transcript.failures, [])
    assert.deepEqual(transcript.cleanup, { children: 0, timers: 0, stillOpen: [], attached: 0 })
    assert(transcript.sockets.length > 0, 'A segment without sockets')
    assert(
      transcript.sockets.every((s) => Number.isFinite(s.closedAt)),
      'A segment socket never closed'
    )
    assert(transcript.closures.length === 1, 'A lifecycle must have exactly one public close')
    transcripts[entry.id] = transcript
    bundles[entry.id] = checked
    for (const request of transcript.records.filter(
      (r) =>
        r.type === 'request' && r.apiKey === 72 && r.response?.errorCode === 0 && r.metricsBytes > 0
    )) {
      const blob = checked.pushes.get(request.requestKey).blob
      const cap = 4096
      const options = { maxOutputLength: cap }
      let raw
      if (request.compressionType === 0) raw = blob
      else if (request.compressionType === 1) raw = zlib.gunzipSync(blob, options)
      // eslint-disable-next-line n/no-unsupported-features/node-builtins -- required live Node lines provide the built-in codec
      else if (request.compressionType === 4) raw = zlib.zstdDecompressSync(blob, options)
      else throw new Error('Unsupported captured compression')
      assert(raw.length > 0 && raw.length <= cap, 'A Push body is outside its bound')
      const rawSha = sha(raw)
      const row = [...sink]
        .filter(
          ([token, r]) =>
            !consumed.has(token) &&
            r.node === request.node &&
            r.clientInstanceId === request.uuid &&
            r.clientId === request.clientId &&
            r.dataSha256 === rawSha &&
            r.dataBytes === raw.length &&
            r.terminating === request.terminating &&
            r.parse === 'ok'
        )
        .at(0)
      assert(row, `No unconsumed sink record joins ${entry.id}/${request.requestKey}`)
      consumed.add(row[0])
      const subscription = lastSubscription(transcript, request)
      assert(subscription, 'Push without a successful subscription')
      assert.equal(subscription.response.uuid, request.uuid)
      assert.equal(subscription.response.subscriptionId, request.subscriptionId)
      if (!keys.includes(request.requestKey)) continue
      const proof = checked.proofs.get(request.requestKey)
      const { decoded } = proof
      hashes.add(decoded.rawSha256)
      assert.equal(calls.get(decoded.rawSha256), meta.kind, 'The parent never decoded this proof')
      assertPushProof({
        request,
        subscription,
        decoded,
        line: record.run.line,
        transmitted: () => blob,
        stored: () => proof.raw,
        receipts: () =>
          archive.decoder.receipts
            .filter(
              (r) =>
                r.rawSha256 === decoded.rawSha256 &&
                r.line === record.run.line &&
                r.bodySha256 === sha(JSON.stringify(decoded.decoded))
            )
            .map((r) => ({
              code: r.code,
              signal: r.signal,
              line: r.line,
              stdout: `${JSON.stringify(r.identity)}\n${JSON.stringify(decoded.decoded)}`,
            })),
      })
      assert.deepEqual(
        decoded.exportIdentity,
        { node: row[1].node, seq: row[1].seq },
        'The decoded Push joined another sink record'
      )
    }
  }
  const faulted = {}
  for (const kind of CHURN_KINDS)
    for (const claim of archive.workers?.[kind]?.claims?.retirements ?? [])
      (faulted[claim.segment] ??= []).push(claim.faulted)
  verifyChurnStream(transcripts, faulted)
  return { transcripts, bundles, hashes }
}

/**
 * The complete-stream accounting of the churn segments: F08's request, error and cancellation
 * predicate over every segment with allowances fixed by the catalogue and the schedule, never by
 * the transcript itself (error 42 only, and faults exactly on the sockets the segment's scheduled
 * retirements faulted); no Get error at all; and the final of each segment's one close. Under a
 * nonempty selection in force when the close began, the close owes exactly one final, written
 * inside it, answered with error 0 and named by the full-proof selection; under an empty one it
 * owes none. No telemetry request follows a final.
 * @param {Object<string,object>} transcripts every segment transcript by segment ID
 * @param {Object<string,number[]>} [faulted] the socket each scheduled retirement faulted, by
 *   segment ID
 * @returns {void}
 * @throws {Error} naming the first unaccounted, undeclared, missing, cancelled, retried or late
 *   request
 */
const verifyChurnStream = (transcripts, faulted = {}) => {
  const { TelemetryProofs } = require('../../soak/telemetryChurn')
  const errors = Object.fromEntries(segmentCatalogue(CHURN).map((id) => [id, [42]]))
  const order = (ids) => [...ids].sort((a, b) => a - b)
  for (const [id, transcript] of Object.entries(transcripts)) {
    assert(errors[id], `Segment ${id} is outside the catalogue`)
    assert.deepEqual(
      order(transcript.faults.map((f) => f.socketId)),
      order(faulted[id] ?? []),
      `Declared faults of ${id} differ from its scheduled retirements`
    )
  }
  assertStreamAccounted(transcripts, { errors })
  for (const [id, transcript] of Object.entries(transcripts)) {
    const telemetry = transcript.records.filter(
      (r) => r.type === 'request' && [71, 72].includes(r.apiKey)
    )
    for (const get of telemetry.filter((r) => r.apiKey === 71 && r.response))
      assert.equal(get.response.errorCode, 0, 'Unexpected Get error')
    const finals = telemetry.filter((r) => r.apiKey === 72 && r.terminating)
    if (TelemetryProofs.owesFinal(transcript)) {
      assert.equal(finals.length, 1, `The close of ${id} owes exactly one final`)
      const [closure] = transcript.closures
      const [final] = finals
      assert(
        final.writeAt >= closure.at && final.writeAt <= closure.end,
        `The final of ${id} was not written inside its close`
      )
      assert.equal(final.response?.errorCode, 0, `The final of ${id} was not answered with error 0`)
      assert(
        TelemetryProofs.selection(transcript.records).includes(final.requestKey),
        `The final of ${id} is outside the full-proof selection`
      )
    } else assert.equal(finals.length, 0, `${id} sent a final under an empty selection`)
    for (const final of finals)
      assert(!telemetry.some((r) => r.index > final.index), 'Telemetry work followed the final')
  }
}

/**
 * @param {object} transcript one segment's transcript @param {string} key a request key
 * @returns {object} that record @throws {Error} when the segment holds no such record
 */
const churnRecord = (transcript, key) => {
  const found = transcript.records.find((r) => r.requestKey === key)
  assert(found, `Missing record ${key}`)
  return found
}

/**
 * The strict proof of one Push on the socket of a replacement Get.
 * @param {object} transcript the segment @param {object} get the replacement Get
 * @param {string} pushKey the advertised proving Push @returns {object} the proving Push
 * @throws {Error} unless the advertised Push is the first accepted nonempty nonterminating one,
 *   on the same socket, later in order and strictly after the Get's response
 */
const churnProof = (transcript, get, pushKey) => {
  const { TelemetryProofs } = require('../../soak/telemetryChurn')
  const found = TelemetryProofs.provingPush(transcript.records, get)
  assert(found, `No proving Push after the Get ${get.requestKey}`)
  assert.equal(found.requestKey, pushKey, 'The advertised proving Push is not the first one')
  assert(found.index > get.index && found.writeAt > get.response.responseAt)
  assert.equal(found.socketId, get.socketId, 'The proving Push is on another socket')
  return found
}

/**
 * Re-derives one public reopen from its two segments and the ordering journal: the next segment was
 * selected before the close resolved, connect was the next public action, there was no Get, connect
 * or authentication during the final, the cached UUID and a successful replacement Get that carried
 * it, and admission paused, drained and confirmed before the close.
 * @param {object} input `claim`, `from` and `to` transcripts, the worker `journal`, its `uuid`
 *   and optional `releasedAt` of the disconnected subscription batch
 * @returns {void} @throws {Error} naming the first contradicted proof
 */
const verifyChurnReopen = ({ claim, from, to, journal, uuid, releasedAt }) => {
  const { TelemetryProofs } = require('../../soak/telemetryChurn')
  const note = (type) => {
    const found = journal.filter(
      (n) => n.type === type && n.from === claim.from && n.to === claim.to
    )
    assert.equal(found.length, 1, `Missing or duplicate ${type} note for ${claim.to}`)
    return found[0]
  }
  const [select, resolved, called] = ['select', 'disconnect-resolved', 'connect-called'].map(note)
  assert(select.seq < resolved.seq, 'The next segment was not selected before the close resolved')
  assert.equal(called.seq, resolved.seq + 1, 'Connect was not the next public action')
  assert.equal(from.closures.length, 1, 'A lifecycle must have exactly one public close')
  const [closure] = from.closures
  assert(
    claim.observedAt <= select.at &&
      select.at <= closure.at &&
      closure.end <= resolved.at &&
      resolved.at <= called.at,
    'Boundary times are out of order'
  )
  assert.deepEqual(
    from.records.filter(
      (r) =>
        (r.type === 'connect' && r.at >= closure.at) ||
        (r.type === 'request' && [17, 18, 36, 71].includes(r.apiKey) && r.writeAt >= closure.at)
    ),
    [],
    'Get, connect or authentication during the final'
  )
  if (releasedAt !== undefined)
    assert(
      closure.end <= claim.closedAt && claim.closedAt <= releasedAt && releasedAt <= called.at,
      'The subscription release is outside the disconnected lifecycle gap'
    )
  assert.equal(claim.cachedUuid, uuid, 'The cached timeout-zero UUID differs')
  const get = churnRecord(to, claim.get)
  assert(
    TelemetryProofs.gets(to.records, uuid).some((g) => g.requestKey === get.requestKey),
    'The replacement Get is not a successful Get carrying the cached UUID'
  )
  assert(get.writeAt >= called.at, 'The replacement Get precedes the reconnect')
  const connect = to.records.find((r) => r.type === 'connect' && r.socketId === get.socketId)
  assert(connect && connect.at >= called.at, 'The replacement socket precedes the reconnect')
  assert(
    claim.pause.pausedAt <= claim.pause.idleAt &&
      claim.pause.idleAt <= claim.pause.confirmedAt &&
      claim.pause.confirmedAt <= claim.observedAt,
    'Admission was not paused, drained and confirmed before the close'
  )
}

/**
 * Re-derives one telemetry retirement: the declared fault on a telemetry socket, exactly one cut-off
 * exchange joined to it, ordinary core work on a normal socket from the declaration on, and a
 * successful recovery Get on a new socket.
 * @param {object} input `claim`, the segment `transcript` and the worker `uuid`
 * @returns {void} @throws {Error} naming the first contradicted proof
 */
const verifyChurnRetirement = ({ claim, transcript, uuid }) => {
  const { TelemetryProofs } = require('../../soak/telemetryChurn')
  assert(
    transcript.faults.some((f) => f.socketId === claim.faulted),
    'Undeclared faulted socket'
  )
  const socket = transcript.sockets.find((s) => s.id === claim.faulted)
  assert(
    socket?.kind === 'telemetry' && Number.isFinite(socket.closedAt),
    'Faulted a non-telemetry socket'
  )
  const cut = transcript.cancellations.filter((c) => c.socketId === claim.faulted)
  assert.equal(cut.length, 1, 'Exactly one exchange must be cut off')
  assert.equal(cut[0].disposition, 'faulted-transport')
  const core = churnRecord(transcript, claim.core)
  assert(![71, 72].includes(core.apiKey) && core.response, 'No ordinary core work beside the fault')
  assert(core.writeAt >= claim.declaredAt, 'The core work precedes the fault declaration')
  assert.equal(
    transcript.sockets.find((s) => s.id === core.socketId)?.kind,
    'normal',
    'The core work ran on a telemetry socket'
  )
  const get = churnRecord(transcript, claim.get)
  assert(
    TelemetryProofs.gets(transcript.records, uuid).some((g) => g.requestKey === get.requestKey) &&
      get.socketId !== claim.faulted &&
      get.writeAt >= claim.declaredAt,
    'The recovery Get is not a successful Get on a new socket'
  )
}

/**
 * Re-derives one subscription transition: the parent's set and readback, the successful Get that
 * proves the final selection of the disconnected batch after the acknowledged fence, and the
 * current Push proof a nonempty reopened lifecycle owes. Intermediate selections are proved by
 * ordered set/readbacks; a batch may return to the previous selection and subscription ID.
 * @param {object} input `claim`, `tick`, the parent's `rotation`, the segment `transcript`, the
 *   worker `uuid`, the `selection` the table requires and the `previous` subscription ID
 * @returns {void} @throws {Error} naming the first contradicted proof
 */
const verifyChurnTransition = ({
  claim,
  tick,
  rotation,
  transcript,
  uuid,
  selection,
  previous,
}) => {
  const { TelemetryProofs } = require('../../soak/telemetryChurn')
  assert(rotation, 'No parent set and readback for the transition')
  assert.equal(rotation.selection, selection)
  assert.equal(rotation.readback, rotation.selection, 'The readback differs from the set')
  assert(rotation.setAt <= rotation.readbackAt)
  assert.equal(claim.selection, rotation.selection)
  const get = churnRecord(transcript, claim.get)
  const list = claim.selection === '' ? [] : [claim.selection]
  assert(
    TelemetryProofs.gets(transcript.records, uuid).some((g) => g.requestKey === get.requestKey) &&
      get.writeAt >= claim.receivedAt &&
      Number.isSafeInteger(claim.after) &&
      claim.after >= 0 &&
      get.index > claim.after &&
      get.response.pushIntervalMs === 1000 &&
      JSON.stringify(get.response.requestedMetrics) === JSON.stringify(list),
    'No successful Get proves the selection'
  )
  assert.equal(get.response.subscriptionId, claim.subscriptionId)
  if (!tick.transitions)
    assert.notEqual(claim.subscriptionId, previous, 'The subscription did not change')
  const owed = tick.transitions
    ? selection === ''
      ? []
      : ['reopen']
    : tick.actions.filter((a) => a !== 'subscription').sort()
  if (!owed.length) return assert.equal(claim.proof, null, 'Unexpected current Push proof')
  assert(claim.proof, 'A lifecycle change without its current Push proof')
  assert.deepEqual([...claim.proof.owedBy].sort(), owed)
  assert(list.length > 0, 'A proof cannot be owed under an empty selection')
  const first = TelemetryProofs.gets(transcript.records, uuid).find(
    (g) => g.socketId === claim.proof.socketId
  )
  assert.equal(
    first?.requestKey,
    claim.proof.get,
    'The proof Get is not the socket replacement Get'
  )
  churnProof(transcript, first, claim.proof.push)
}

/**
 * The cross-segment identity and correction rules of one worker's lifecycles: one nonzero UUID on
 * every telemetry request (only setup Gets before the first assignment carry the zero ID), no Push
 * accepted under an empty selection, and the one corrective Push: only a lifecycle that follows a
 * sent final has it, only as error 42 and only before its first accepted Push. Successful Gets,
 * subscription changes and telemetry-only replacements never refill it.
 * @param {{names:string[], transcripts:Object<string,object>, uuid:string}} input the kind's segment
 *   IDs in lifecycle order, the transcripts and the assigned UUID
 * @returns {void} @throws {Error} naming the first rotation, empty-selection Push or excess correction
 */
const verifyChurnLifecycles = ({ names, transcripts, uuid }) => {
  const { TelemetryProofs } = require('../../soak/telemetryChurn')
  const zero = '00000000-0000-0000-0000-000000000000'
  assert.notEqual(uuid, zero)
  names.forEach((id, position) => {
    assert(transcripts[id], `Unknown segment ${id}`)
    const records = transcripts[id].records
    let assigned = position > 0
    for (const request of records.filter(
      (r) => r.type === 'request' && [71, 72].includes(r.apiKey)
    )) {
      assert(
        request.uuid === uuid || (!assigned && request.apiKey === 71 && request.uuid === zero),
        'A request rotated the accepted UUID'
      )
      if (request.apiKey === 71 && request.response?.errorCode === 0) assigned = true
    }
    for (const push of TelemetryProofs.accepted(records))
      assert(
        TelemetryProofs.governing(records, push)?.response.requestedMetrics.length > 0,
        'A Push was accepted under an empty selection'
      )
    const pushes = records.filter((r) => r.type === 'request' && r.apiKey === 72 && !r.terminating)
    const rejected = pushes.filter((r) => r.response?.errorCode)
    const previous = position > 0 ? transcripts[names[position - 1]].records : []
    const budget = previous.some((r) => r.type === 'request' && r.apiKey === 72 && r.terminating)
      ? 1
      : 0
    const firstAccepted = pushes.findIndex((r) => r.response?.errorCode === 0)
    assert(
      rejected.length <= budget &&
        rejected.every(
          (r) =>
            r.response.errorCode === 42 && (firstAccepted < 0 || pushes.indexOf(r) < firstAccepted)
        ),
      'Corrective Push budget exceeded'
    )
  })
}

/**
 * Re-derives the coordinated schedule from the parent's raw stage receipts and every worker's own
 * stage journal, never from counts: the exact `(tick, stage, kind)` catalogue once and in order,
 * each tick begun at or after its due time and within the late bound, every stage sent after the
 * previous stage's receipts from all four workers, the set and readback after every worker's fence
 * acknowledgement and before its release, every receipt within its deadline, one offset mapping each worker's
 * monotonic clock onto the parent's, each worker's measurement interval on that mapping, and every
 * claim time inside its own stage. The measured action counts are derived from these joined rows.
 * @param {object} input `archive` (stages, rotations, tickStarts, missed and worker facts) and the
 *   verdict's `timeline`
 * @returns {Object<string,{subscription:number, retire:number, reopen:number}>} measured counts by kind
 * @throws {Error} naming the first missing, duplicate, reordered, early, late or foreign receipt
 */
const verifyChurnStages = ({ archive, timeline }) => {
  const churn = require('../../soak/telemetryChurn')
  const table = new churn.TelemetrySchedule({ warmupMs: 60000, measureMs: 300000 })
  const late = 5000
  const deadline = { lifecycle: 12000, retire: 8000, confirm: 10000 }
  const missed = new Set(archive.missed.map((m) => m.tick))
  for (const m of archive.missed)
    assert(!table.measured(table.ticks[m.tick]), 'A measured tick was missed')
  const expected = []
  for (const tick of table.ticks)
    if (!missed.has(tick.number))
      for (const stage of table.stages(tick))
        for (const kind of churn.KINDS) expected.push([tick.number, stage, kind])
  assert.deepEqual(
    archive.stages.map((r) => [r.tick, r.stage, r.kind]),
    expected,
    'Stage catalogue differs from the fixed table'
  )
  assert.deepEqual(
    archive.tickStarts.map((t) => t.tick),
    table.ticks.filter((t) => !missed.has(t.number)).map((t) => t.number),
    'Tick starts differ from the fixed table'
  )
  assert.deepEqual(
    archive.rotations.map((r) => [r.tick, r.index]),
    table.ticks
      .filter((t) => !missed.has(t.number) && t.actions.includes('subscription'))
      .flatMap((t) => (t.transitions ?? [t.index]).map((index) => [t.number, index])),
    'Transition set and readback differ from the table'
  )
  let barrier = timeline.epochAt
  for (const start of archive.tickStarts) {
    const tick = table.ticks[start.tick]
    const due = timeline.epochAt + tick.at
    assert(start.beganAt >= due && start.beganAt >= barrier, 'A tick began early')
    assert(start.beganAt - due <= late, 'A tick began beyond its late bound')
    barrier = start.beganAt
    for (const stage of table.stages(tick)) {
      const rows = archive.stages.filter((r) => r.tick === tick.number && r.stage === stage)
      const [{ sentAt }] = rows
      if (stage === 'confirm') {
        const rotations = archive.rotations.filter((r) => r.tick === tick.number)
        let after = barrier
        for (const rotation of rotations) {
          assert.equal(
            rotation.selection,
            table.selection(rotation.index),
            'A set names another selection'
          )
          assert.equal(rotation.readback, rotation.selection, 'The readback differs from the set')
          for (const row of rows)
            assert(
              row.fence &&
                row.fence.parentAt >= sentAt &&
                row.fence.parentAt <= rotation.requestedAt,
              'The rotation preceded a worker confirmation fence'
            )
          assert(
            rotation.requestedAt >= after &&
              rotation.setAt >= rotation.requestedAt &&
              rotation.readbackAt >= rotation.setAt &&
              rows.every((r) => r.releasedAt >= rotation.readbackAt && r.releasedAt <= r.parentAt),
            'The set and readback are outside their stage barrier'
          )
          after = rotation.readbackAt
        }
      }
      for (const row of rows) {
        assert.equal(row.sentAt, sentAt, 'One stage was sent at different times')
        assert(sentAt >= barrier, 'A stage was sent before the previous stage was joined')
        assert(row.parentAt >= sentAt, 'A stage receipt precedes its command')
        assert(row.parentAt - sentAt <= deadline[stage], 'A stage missed its deadline')
      }
      barrier = Math.max(...rows.map((r) => r.parentAt))
    }
  }
  assert(timeline.boundaryAt >= barrier, 'Stage work continued past the measurement boundary')
  const measured = {}
  for (const kind of churn.KINDS) {
    const facts = archive.workers[kind]
    const rows = archive.stages.filter((r) => r.kind === kind)
    assert.deepEqual(
      facts.stages.map((s) => [s.tick, s.stage, s.startedAt, s.doneAt]),
      rows.map((r) => [r.tick, r.stage, r.workerStartedAt, r.workerDoneAt]),
      `${kind} stage journal differs from the parent's receipts`
    )
    let lo = -Infinity
    let hi = Infinity
    for (const row of rows) {
      assert(row.workerStartedAt <= row.workerDoneAt, 'A worker stage ends before it starts')
      lo = Math.max(lo, row.sentAt - row.workerStartedAt)
      hi = Math.min(hi, row.parentAt - row.workerDoneAt)
      if (row.stage === 'confirm') {
        const claim = facts.claims.transitions.find((c) => c.tick === row.tick)
        assert(claim, 'A confirmation receipt has no transition claim')
        lo = Math.max(lo, row.releasedAt - claim.releasedAt)
        hi = Math.min(hi, row.fence.parentAt - claim.receivedAt)
      }
    }
    const { timing } = facts
    lo = Math.max(lo, timeline.epochAt - timing.goAt, timeline.boundaryAt - timing.boundaryAt)
    hi = Math.min(hi, timeline.epochAt - timing.phaseAt)
    assert(lo <= hi, `${kind} clock cannot be mapped onto the parent epoch`)
    assert.equal(
      timing.measurementMs,
      timing.boundaryAt - timing.phaseAt - table.warmupMs,
      `${kind} measurement interval differs from its stamps`
    )
    assert(timing.measurementMs >= 300000, `${kind} measured less than 300000 ms`)
    const within = (tick, stage, ...times) => {
      const row = rows.find((r) => r.tick === tick && r.stage === stage)
      assert(row, `${kind} claim at tick ${tick} has no ${stage} receipt`)
      assert(
        times.every((t) => t >= row.workerStartedAt && t <= row.workerDoneAt),
        `${kind} claim at tick ${tick} lies outside its ${stage} stage`
      )
    }
    for (const c of facts.claims.reopens) within(c.tick, 'confirm', c.observedAt, c.closedAt)
    for (const c of facts.claims.retirements) within(c.tick, 'retire', c.declaredAt, c.faultedAt)
    for (const c of facts.claims.transitions) {
      within(c.tick, 'confirm', c.receivedAt, c.releasedAt)
      const row = rows.find((r) => r.tick === c.tick && r.stage === 'confirm')
      assert.deepEqual(
        { receivedAt: c.receivedAt, after: c.after, segment: c.segment },
        {
          receivedAt: row.fence.receivedAt,
          after: row.fence.after,
          segment: row.fence.segment,
        },
        'The transition differs from its acknowledged fence'
      )
      assert.equal(
        c.receivedAt,
        row.workerStartedAt,
        'The confirmation stage did not start at its fence'
      )
      const reopen = facts.claims.reopens.find((r) => r.tick === c.tick)
      assert(reopen, 'The subscription transition has no disconnected lifecycle boundary')
      assert(
        reopen.closedAt + lo <= row.fence.parentAt && reopen.closedAt <= c.releasedAt,
        'A subscription transition was released inside an active lifecycle'
      )
      assert(
        c.receivedAt + lo <= row.fence.parentAt && c.releasedAt + hi >= row.releasedAt,
        'The worker confirmation fence or release cannot map onto the parent epoch'
      )
    }
    const counts = { subscription: 0, retire: 0, reopen: 0 }
    for (const row of rows)
      if (table.measured(table.ticks[row.tick])) {
        if (row.stage === 'confirm') {
          counts.subscription += table.ticks[row.tick].transitions.length
          counts.reopen += 1
        } else counts[CHURN_NAMES[row.stage]] += 1
      }
    assert.deepEqual(
      counts,
      table.required(),
      `${kind} measured stage counts differ from the table`
    )
    for (const [action, minimum] of Object.entries(churn.MINIMUMS))
      assert(counts[action] >= minimum, `${kind} measured ${action} actions below ${minimum}`)
    measured[kind] = counts
  }
  return measured
}

/**
 * Re-derives every lifecycle claim of one worker from its segments' wire records: the setup and
 * initial lifecycles, each public reopen, each telemetry retirement, each subscription transition,
 * every proving Push, the measured stage counts, the empty-selection and corrective-budget rules.
 * @param {object} context `kind`, `uuid`, `facts`, `transcripts`, `schedule` and `archive`
 * @returns {void}
 * @throws {Error} naming the first contradicted claim
 */
const verifyChurnWorker = ({ kind, uuid, facts, transcripts, schedule, archive }) => {
  const { TelemetryProofs, TelemetrySchedule } = require('../../soak/telemetryChurn')
  const table = new TelemetrySchedule({ warmupMs: 60000, measureMs: 300000 })
  const names = Array.from({ length: schedule.perKind }, (_, i) => schedule.name(kind, i))
  const segment = (id) => {
    assert(transcripts[id], `Unknown segment ${id}`)
    return transcripts[id]
  }
  verifyChurnLifecycles({ names, transcripts, uuid })
  for (const id of names)
    for (const get of segment(id).records.filter(
      (r) => r.type === 'request' && r.apiKey === 71 && r.response?.errorCode === 0
    ))
      assert.equal(get.response.uuid, uuid, 'A Get returned another instance ID')
  const { claims, journal } = facts
  assert.equal(claims.setup.segment, names[0])
  const setup = churnRecord(segment(names[0]), claims.setup.get)
  assert(setup.apiKey === 71 && setup.response?.errorCode === 0 && setup.response.uuid === uuid)
  assert.equal(claims.initial.segment, names[1])
  assert.equal(claims.initial.cachedUuid, uuid)
  const initial = churnRecord(segment(names[1]), claims.initial.get)
  assert(
    TelemetryProofs.gets(segment(names[1]).records, uuid).some(
      (r) => r.requestKey === initial.requestKey
    )
  )
  churnProof(segment(names[1]), initial, claims.initial.push)
  assert.equal(
    journal.filter((n) => n.type === 'select').length,
    schedule.perKind - 1,
    `${kind} select journal differs`
  )
  const reopenTicks = table.ticks.filter((t) => t.actions.includes('reopen'))
  assert.equal(claims.reopens.length, reopenTicks.length, `Missing ${kind} reopens`)
  claims.reopens.forEach((claim, position) => {
    assert.equal(claim.tick, reopenTicks[position].number)
    assert.deepEqual(
      [claim.from, claim.to],
      [names[position + 1], names[position + 2]],
      'Reopen segments differ from the schedule'
    )
    const transition = claims.transitions.find((c) => c.tick === claim.tick)
    assert(transition, 'A reopen lacks its subscription release')
    verifyChurnReopen({
      claim,
      from: segment(claim.from),
      to: segment(claim.to),
      journal,
      uuid,
      releasedAt: transition.releasedAt,
    })
  })
  const retireTicks = table.ticks.filter((t) => t.actions.includes('retire'))
  assert.equal(claims.retirements.length, retireTicks.length, `Missing ${kind} retirements`)
  claims.retirements.forEach((claim, position) => {
    assert.equal(claim.tick, retireTicks[position].number)
    verifyChurnRetirement({ claim, transcript: segment(claim.segment), uuid })
  })
  const transitionTicks = table.ticks.filter((t) => t.actions.includes('subscription'))
  assert.equal(claims.transitions.length, transitionTicks.length, `Missing ${kind} transitions`)
  let previous = null
  claims.transitions.forEach((claim, position) => {
    const tick = transitionTicks[position]
    assert.equal(claim.tick, tick.number)
    verifyChurnTransition({
      claim,
      tick,
      rotation: archive.rotations.find((r) => r.tick === tick.number && r.index === tick.index),
      transcript: segment(claim.segment),
      uuid,
      selection: table.selection(tick.index),
      previous,
    })
    const get = churnRecord(segment(claim.segment), claim.get)
    assert(get.writeAt >= claim.releasedAt, 'The next handshake preceded the subscription release')
    previous = claim.subscriptionId
  })
  for (const claim of claims.retirements) {
    const proof = claims.proofs.find((p) => p.tick === claim.tick && p.owedBy.includes('retire'))
    assert(proof, 'A retirement lacks its current Push proof')
    const transcript = segment(claim.segment)
    churnProof(transcript, churnRecord(transcript, claim.get), proof.push)
  }
  const unique = new Set(claims.proofs.map((p) => `${p.segment}/${p.push}`))
  assert.equal(unique.size, claims.proofs.length, 'A proving Push was claimed twice')
}

/**
 * Places a Push against one journalled write fence.
 * @param {{cursors:Object<string,number>}} fence segment cursors when the call returned
 * @param {string[]} names the kind's segment IDs in lifecycle order
 * @param {string} id the Push's segment @param {number} index its observer-local record index
 * @returns {boolean} whether the Push was written after the call
 * @throws {Error} for a fence without any cursor
 */
const afterFence = (fence, names, id, index) => {
  const cursors = Object.entries(fence?.cursors ?? {})
  assert(
    cursors.length > 0 && cursors.every(([seg, at]) => names.includes(seg) && Number.isInteger(at)),
    'Missing custom registration fences'
  )
  if (fence.cursors[id] !== undefined) return index > fence.cursors[id]
  return names.indexOf(id) > Math.max(...cursors.map(([seg]) => names.indexOf(seg)))
}

/**
 * Recomputes the custom-identity fence of every decoded custom Push of one worker, in lifecycle and
 * request order. A replacement is an unregistration and a registration of another attribute
 * identity: from the unregister fence on the removed identity never appears again, with no grace,
 * and from the register fence the slot carries exactly the new identity. A same-identity update
 * keeps the identity; only the first accepted Push after its fence may still carry the one
 * previously frozen old value, and every other Push after it carries the new value.
 * @param {string} kind the worker kind @param {object} facts its archived facts
 * @param {Object<string,object>} bundles checked bundle by segment ID @param {Object<string,object>} transcripts
 * @param {string[]} [names] the kind's segment IDs in lifecycle order, by default its transcripts
 *   sorted by lifecycle index
 * @returns {number} how many custom proofs were checked
 * @throws {Error} when a decoded payload carries a retired identity, a wrong cardinality or more
 *   than the one frozen old value
 */
const verifyChurnFence = (kind, facts, bundles, transcripts, names) => {
  const { TelemetryProofs } = require('../../soak/telemetryChurn')
  const slot = `com.example.soak.${kind}.000`
  const order =
    names ??
    Object.keys(transcripts)
      .filter((id) => id.startsWith(`${kind}-`))
      .sort((a, b) => Number(/\d+$/.exec(a)[0]) - Number(/\d+$/.exec(b)[0]))
  const journal = facts.replacements
  let variant = 'a'
  for (const entry of journal) {
    assert(['replace', 'update'].includes(entry.type), 'Unknown custom journal entry')
    if (entry.type === 'replace') {
      assert.equal(entry.removedVariant, variant, 'Custom replacement identity differs')
      assert.notEqual(entry.addedVariant, variant, 'A replacement must change the identity')
      variant = entry.addedVariant
    } else assert.equal(entry.variant, variant, 'A custom update changed the identity')
  }
  const firstAfter = new Map()
  for (const id of order)
    for (const push of TelemetryProofs.accepted(transcripts[id]?.records ?? []))
      for (const entry of journal)
        if (
          entry.type === 'update' &&
          !firstAfter.has(entry) &&
          afterFence(entry.fence, order, id, push.index)
        )
          firstAfter.set(entry, `${id}/${push.requestKey}`)
  let checked = 0
  for (const id of order) {
    const records = transcripts[id]?.records ?? []
    const proofs = [...(bundles[id]?.proofs ?? new Map())]
      .map(([key, proof]) => ({ push: records.find((r) => r.requestKey === key), proof }))
      .sort((a, b) => a.push.index - b.push.index)
    for (const { push, proof } of proofs) {
      const governing = TelemetryProofs.governing(records, push)
      if (!governing?.response.requestedMetrics.some((p) => p.startsWith('com.example.'))) continue
      let current = 'a'
      let update = null
      for (const entry of journal) {
        if (entry.type === 'update') {
          if (afterFence(entry.fence, order, id, push.index)) update = entry
          continue
        }
        if (!afterFence(entry.unregisterFence, order, id, push.index)) break
        current = afterFence(entry.registerFence, order, id, push.index) ? entry.addedVariant : null
        update = null
      }
      const metrics = proof.decoded.decoded.metrics.filter((m) => m.name === slot)
      assert.equal(metrics.length, current === null ? 0 : 1, 'Custom slot cardinality differs')
      for (const metric of metrics) {
        assert.equal(metric.points.length, 1, 'Custom point cardinality differs')
        const [point] = metric.points
        const attrs = Object.fromEntries(point.attrs.map(([name, , value]) => [name, value]))
        assert.deepEqual(
          Object.keys(attrs).sort(),
          ['slot', 'variant'],
          'Custom point identity differs'
        )
        assert.equal(attrs.slot, '0', 'Custom point identity differs')
        assert.equal(
          attrs.variant,
          current,
          'A retired custom identity was exported after its fence'
        )
        if (update && point.asInt !== update.value)
          assert(
            point.asInt === update.previousValue &&
              firstAfter.get(update) === `${id}/${push.requestKey}`,
            'A frozen old value exceeded the one update allowance'
          )
      }
      checked += 1
    }
  }
  return checked
}

/**
 * Binds one worker's resource input to the worker itself: the settled baseline and teardown carry
 * the digest of exactly the canonical descriptor population of that kind with that UUID, and the
 * disabled control ran the frozen catalogue of that same kind and topic, which the enabled worker
 * replayed operation for operation. Numbers that fit cannot stand in for another kind,
 * configuration or population.
 * @param {{kind:string, uuid:string, stem:string, input:object}} context the row being verified
 * @returns {void} @throws {Error} naming the first substituted population or experiment
 */
const verifyChurnReference = ({ kind, uuid, stem, input }) => {
  const churn = require('../../soak/telemetryChurn')
  assert.deepEqual(
    churn.JournalConstancy.violations(input),
    [],
    'Journal storage changed during the measurement'
  )
  const registry = new churn.Registry(
    { registerMetricForSubscription() {}, unregisterMetricFromSubscription() {} },
    kind
  )
  registry.fill()
  const digest = registry.digest(uuid)
  assert.equal(
    input.baseline?.registryDigest,
    digest,
    'The retained registry and UUID population differs from the worker'
  )
  assert.equal(input.teardown?.registryDigest, digest, 'The teardown population differs')
  const { control, foreground } = input.reference ?? {}
  assert.deepEqual(
    control?.manifest,
    churn.controlManifest(kind, {
      topic: `${stem}-${churn.labelOf(kind)}`,
      ctl: `${stem}-ctl`,
    }),
    'The control ran another frozen catalogue'
  )
  assert.equal(control.kind, kind, 'The control is of another kind')
  assert.deepEqual(
    foreground?.map((o) => [o.n, o.status]),
    control.manifest.operations.map((o) => [o.n, 'fulfilled']),
    'The enabled worker did not replay the control catalogue'
  )
}

/**
 * Requires one hello per kind from four distinct processes of one Node build, flags, platform and
 * codec capability, each carrying the driver source the parent recorded, and that recorded source
 * to be this module tree's own `scripts/soak/telemetryChurn.js` and `scripts/soak/soak.js`, by path
 * and digest. Workers that agree only with each other cannot stand in for the run's driver.
 * @param {object[]} handshakes the parent's archived hello facts
 * @param {object} source the parent's archived `sourceStamp` of the driver
 * @returns {void} @throws {Error} for a missing, duplicate or foreign handshake or source
 */
const verifyChurnHandshakes = (handshakes, source) => {
  const { KINDS, sourceStamp } = require('../../soak/telemetryChurn')
  assert.deepEqual(
    source,
    sourceStamp(path.resolve(__dirname, '../../soak/soak.js')),
    'The recorded driver source differs from the verified tree'
  )
  assert.deepEqual(
    (handshakes ?? []).map((h) => h.kind),
    [...KINDS],
    'Worker handshakes differ from the four kinds'
  )
  assert.equal(new Set(handshakes.map((h) => h.pid)).size, 4, 'Two workers share one process')
  const [first] = handshakes
  for (const hello of handshakes) {
    assert(Number.isFinite(hello.origin) && Number.isFinite(hello.parentAt), 'Handshake time')
    for (const key of ['node', 'v8', 'platform', 'arch', 'execArgv', 'codecs'])
      assert.deepEqual(hello[key], first[key], `Worker ${hello.kind} ${key} differs`)
    assert.deepEqual(hello.source, source, `Worker ${hello.kind} source differs from the driver`)
  }
  assert(first.execArgv.includes('--expose-gc'), 'Workers ran without real collection')
}

/**
 * Requires the separately reported memory of one decoder child to be labelled: the kernel status
 * source, byte units and capability, with nonnegative integer peaks when the source was read and
 * an explicit `unavailable` otherwise. It is reported, never gated or subtracted.
 * @param {object} memory the receipt's `memory` fact
 * @returns {void} @throws {Error} for a missing, unlabelled or malformed fact
 */
const verifyDecoderMemory = (memory) => {
  const counter = (value) => Number.isSafeInteger(value) && value >= 0
  assert(
    memory?.source === 'proc-pid-status' &&
      memory.units === 'bytes' &&
      ['available', 'unavailable'].includes(memory.capability),
    'A decoder child memory fact is missing or unlabelled'
  )
  if (memory.capability === 'available')
    assert(
      counter(memory.peakHwmBytes) && counter(memory.peakRssBytes) && memory.samples > 0,
      'A decoder child memory fact is malformed'
    )
}

/**
 * Re-derives the whole churn verdict from the sealed archive and the stamped rows, never from an
 * advertised boolean: every segment and Push, every lifecycle claim of every worker, the four
 * resource inputs judged again with a native journal that covers every compressed Push, the four
 * natural exits and the final budgets from their raw stamps, the ordinary and share oracles from
 * their archived inputs (the share exchanges kept once and joined by hash), the worker sources
 * joined to this tree, and the parent's decoder receipts with each child's reported memory.
 * @param {object} record the stamped churn record whose `results.rows` holds the soak's rows
 * @param {string} directory absolute run directory @param {object} manifest sidecar manifest
 * @param {function(string):object} read verified JSON sidecar reader
 * @returns {void}
 * @throws {Error} for a missing, duplicate, substituted, failed, seeded or aggregate-only fact
 */
const verifyChurn = (record, directory, manifest, read) => {
  const { TelemetryResourceVerdict } = require('../../soak/soak')
  const churn = require('../../soak/telemetryChurn')
  const schedule = SCHEDULES[CHURN]
  const rows = record.results?.rows
  assert(Array.isArray(rows), 'Missing churn rows')
  assert(
    rows.every((row) => row.seed === undefined),
    'A seeded run is never acceptance evidence'
  )
  const of = (kind) => rows.filter((row) => row.kind === kind)
  const oracles = read(`${manifest.root}/oracles.json`)
  assert.deepEqual(Object.keys(oracles), ['churn'], 'Required oracle catalogue differs')
  const archive = oracles.churn
  assert.equal(typeof archive.stem, 'string')
  const resources = of('resource-verdict')
  const exits = of('natural-exit')
  assert.deepEqual(resources.map((row) => row.clientKind).sort(), [...CHURN_KINDS].sort())
  assert.deepEqual(exits.map((row) => row.clientKind).sort(), [...CHURN_KINDS].sort())
  assert.deepEqual(Object.keys(archive.workers).sort(), [...CHURN_KINDS].sort())
  const { transcripts, bundles, hashes } = verifyChurnSegments(record, directory, manifest, archive)
  const uuids = new Set()
  for (const row of resources) {
    assert.equal(row.input.clientCount, 1, 'A churn input must be one client')
    const evaluated = TelemetryResourceVerdict.evaluate(row.input)
    assert.equal(
      evaluated.ok,
      true,
      `${row.clientKind} memory: ${evaluated.violations} ${evaluated.inconclusive}`
    )
    assert.deepEqual(
      evaluated,
      row.result,
      'The advertised resource verdict differs from its inputs'
    )
    assert.equal(row.clientId, `${archive.stem}-${row.clientKind}`, 'Client identity differs')
    assert(row.uuid, 'Missing client identity')
    uuids.add(row.uuid)
    const facts = archive.workers[row.clientKind]
    verifyChurnReference({
      kind: row.clientKind,
      uuid: row.uuid,
      stem: archive.stem,
      input: row.input,
    })
    assert.equal(facts.kind, row.clientKind)
    assert.equal(facts.uuid, row.uuid, 'A worker identity differs from its resource row')
    assert(facts.requirements.ok, `${row.clientKind} requirements`)
    const names = Array.from({ length: schedule.perKind }, (_, i) =>
      schedule.name(row.clientKind, i)
    )
    const journal = row.input.reference?.native?.journal?.rows ?? []
    for (const [codec, type] of [
      ['gzip', 1],
      ['zstd', 4],
    ]) {
      const written = names
        .flatMap((id) => transcripts[id].records)
        .filter((r) => r.type === 'request' && r.apiKey === 72 && r.compressionType === type)
      assert(
        journal.filter((r) => r.codec === codec && r.outcome === 'resolved').length >=
          written.length,
        `The ${row.clientKind} native journal lacks the ${codec} compression of its Pushes`
      )
    }
    verifyChurnWorker({
      kind: row.clientKind,
      uuid: row.uuid,
      facts,
      transcripts,
      schedule,
      archive,
    })
    assert(
      verifyChurnFence(row.clientKind, facts, bundles, transcripts, names) > 0,
      'No custom Push was decoded for the fence'
    )
    const last = transcripts[schedule.name(row.clientKind, schedule.perKind - 1)]
    const [closure] = last.closures
    assert(
      facts.timing.receiptAt >= closure.end && facts.timing.receiptAt - closure.end <= 500,
      'The explicit-disconnect receipt did not follow the close immediately'
    )
  }
  assert.equal(uuids.size, 4, 'The four workers must hold four distinct instance IDs')
  verifyChurnHandshakes(archive.handshakes, archive.source)
  for (const row of exits) {
    assert.equal(row.event, 'close')
    assert.equal(row.code, 0)
    assert.equal(row.signal, null)
    assert.equal(row.stderrBytes, 0)
    assert.equal(row.unhandled, 0)
    assert.equal(row.receipts, 1, 'Exactly one explicit-disconnect receipt is required')
    assert.equal(row.killed, false)
    assert.equal(row.watchdogExpired, false)
    assert(Number.isFinite(row.receiptAt) && Number.isFinite(row.closedAt))
    assert.equal(row.afterDisconnectMs, row.closedAt - row.receiptAt)
    assert(row.afterDisconnectMs >= 0 && row.afterDisconnectMs <= 10000)
  }
  const [oracle, ...extraOracle] = of('oracle')
  const [verdict, ...extraVerdict] = of('verdict')
  assert(oracle && verdict && !extraOracle.length && !extraVerdict.length, 'Missing churn verdict')
  assert.equal(rows.at(-1), verdict, 'The verdict must follow every other row')
  const { stem, inputs } = archive
  assert(inputs, 'Missing oracle inputs')
  const share = inputs.shareConsumer
  const shareClosures = Object.keys(transcripts)
    .filter((id) => id.startsWith('shareConsumer-'))
    .sort()
    .flatMap((id) => transcripts[id].closures.map(({ at, end }) => ({ at, end })))
  assert.deepEqual(share.closures, shareClosures, 'Share close boundaries differ from the segments')
  assert.deepEqual(inputs.consumer.handled, archive.workers.consumer.ordinary.handled)
  assert.deepEqual(share.handled, archive.workers.shareConsumer.ordinary.handled)
  assert.deepEqual(inputs.producer.offered, archive.workers.producer.ordinary.offered)
  const captured = archive.workers.shareConsumer.share
  assert.equal(share.group, captured.group, 'Share group differs from the capture')
  assert(!Object.hasOwn(share, 'exchanges'), 'Share exchanges are archived twice')
  assert.equal(
    sha(JSON.stringify(captured.exchanges)),
    captured.exchangesSha256,
    'Share exchange facts differ from their capture hash'
  )
  assert.equal(
    share.exchangesSha256,
    captured.exchangesSha256,
    'The share oracle inputs name other exchanges'
  )
  assert.deepEqual(
    captured.exchanges.map((e) => e.n),
    captured.exchanges.map((_, i) => i + 1),
    'Share exchanges are missing or reordered'
  )
  assert.deepEqual(share.settled, captured.settled)
  const checks = {
    producer: churn.OrdinaryOracle.check({ kind: 'producer', stem, ...inputs.producer }),
    consumer: churn.OrdinaryOracle.check({ kind: 'consumer', stem, ...inputs.consumer }),
    shareConsumer: churn.ShareOracle.check({ stem, ...share, exchanges: captured.exchanges }),
  }
  for (const [kind, check] of Object.entries(checks))
    assert.deepEqual(check.violations, [], `${kind} oracle: ${check.violations}`)
  assert.equal(oracle.ok, true)
  assert.deepEqual(
    oracle.independentSetHashes,
    Object.fromEntries(Object.entries(checks).map(([kind, check]) => [kind, check.hashes])),
    'The advertised oracle hashes differ from the archived inputs'
  )
  for (const kind of ['consumer', 'shareConsumer'])
    assert.equal(
      new Set(inputs.parentOffered[kind]).size,
      10000,
      `The parent offered an incomplete ${kind} corpus`
    )
  const { receipts, calls } = archive.decoder
  assert.equal(receipts.length, calls.length, 'Decoder receipts differ from the decode requests')
  assert(
    calls.length > 0 && calls.every((c) => hashes.has(c.rawSha256)),
    'A decode was not a declared proof'
  )
  for (const receipt of receipts) {
    assert.equal(receipt.code, 0)
    assert.equal(receipt.signal, null)
    assert.equal(receipt.line, record.run.line)
    assert.equal(receipt.identity?.verified, true)
    assert.equal(receipt.identity.line, record.run.line)
    verifyDecoderMemory(receipt.memory)
  }
  const { timeline } = verdict
  assert(timeline.boundaryAt - timeline.epochAt >= 360000, 'The measurement boundary came early')
  const derived = verifyChurnStages({ archive, timeline })
  for (const counts of Object.values(derived))
    assert.deepEqual(verdict.measured, counts, 'The advertised measured counts differ')
  assert.equal(verdict.setupMs, timeline.epochAt - timeline.t0)
  assert.equal(verdict.cleanupMs, timeline.cleanupEndAt - timeline.boundaryAt)
  assert.equal(verdict.totalMs, timeline.cleanupEndAt - timeline.t0)
  assert(verdict.setupMs <= 48000, 'The setup exceeded its budget')
  assert(verdict.cleanupMs <= 120000, 'The cleanup exceeded its budget')
  assert(verdict.totalMs <= 538000, 'The case exceeded its internal budget')
  assert.equal(verdict.ok, true)
  assert.deepEqual(verdict.violations, [])
}

/**
 * Requires exactly the scenario catalogue selected by the CI lane, then verifies every sidecar.
 * @param {string} group reporter or base CI group
 * @param {string} directory sidecar directory
 * @param {string[]} files stamped JSONL paths for this isolated run
 * @returns {void}
 * @throws {Error} on a missing, duplicate, unexpected or unverifiable scenario
 */
const verifyEvidence = (group, directory, files) => {
  const expected =
    group === 'telemetry-reporter'
      ? ['telemetry-reporter', 'telemetry-reporter-no-resource']
      : group === 'telemetry-base'
        ? ['telemetry-reporter-no-plugin']
        : group === 'telemetry-soak'
          ? [CHURN]
          : [...LIVE, UNSUPPORTED].includes(group)
            ? [group]
            : []
  assert(expected.length, 'Unknown telemetry lane')
  const records = files.flatMap((file) =>
    fs.readFileSync(file, 'utf8').trim().split('\n').map(JSON.parse)
  )
  assert.deepEqual(
    records.map((r) => r.sidecars?.scenario).sort(),
    expected.sort(),
    'Scenario catalogue mismatch'
  )
  for (const record of records) {
    assert.equal(record.run.project, process.env.COMPOSE_PROJECT_NAME, 'Ownership project mismatch')
    assert(record.run.project, 'Missing ownership project')
    verifyScenario(record, directory)
  }
}

/**
 * Checks the final removal receipt and its digest links to the stamped scenario files.
 * @param {string} directory sidecar directory
 * @param {string} stamps directory holding the stamped JSONL
 * @param {string} project expected isolated project
 * @returns {void}
 * @throws {Error} if removal, identities or archived digests cannot be verified
 */
const verifyCleanup = (directory, stamps, project) => {
  const manifest = JSON.parse(fs.readFileSync(path.join(directory, 'cleanup-manifest.json')))
  assert.equal(manifest.project, project)
  assert.equal(manifest.receipt.path, 'ownership-receipt.json')
  const bytes = fs.readFileSync(path.join(directory, manifest.receipt.path))
  assert.equal(bytes.length, manifest.receipt.bytes)
  assert.equal(sha(bytes), manifest.receipt.sha256)
  const receipt = JSON.parse(bytes)
  assert.equal(receipt.project, project)
  assert.equal(receipt.preflightPassed, true)
  assert.equal(receipt.removalVerified, true)
  assert.equal(receipt.journalComplete, true)
  for (const key of ['journalError', 'cleanupError', 'removalError']) assert(!receipt[key])
  assert(receipt.startedAt && receipt.stoppedAt)
  assert.deepEqual(receipt.removalProof, { containers: [], networks: [], volumes: [] })
  assert.deepEqual(receipt.ownedRemovalProof, { containers: [], networks: [], volumes: [] })
  const indexes = []
  const failedIndexes = new Set()
  for (const item of manifest.scenarios) {
    assert(/^[a-z0-9-]+\.jsonl$/.test(item.path))
    const content = fs.readFileSync(path.join(stamps, item.path))
    assert.equal(content.length, item.bytes)
    assert.equal(sha(content), item.sha256)
    for (const line of content.toString('utf8').split('\n').filter(Boolean)) {
      const record = JSON.parse(line)
      const sidecars = record.sidecars
      if (sidecars?.format === SEGMENT_FORMAT) {
        const index = `${sidecars.root}/index.json`
        indexes.push(index)
        if (record.status === 'failed') failedIndexes.add(index)
      }
    }
  }
  const listed = manifest.segmented ?? []
  assert.deepEqual(
    listed.map((item) => item.path).sort(),
    indexes.sort(),
    'Segment index catalogue differs'
  )
  for (const item of listed) {
    const bytes = fs.readFileSync(path.join(directory, item.path))
    assert(
      bytes.length === item.bytes && sha(bytes) === item.sha256,
      'Segment index digest differs'
    )
    assert.equal(item.format, SEGMENT_FORMAT, 'Segment index format differs')
    const index = JSON.parse(bytes)
    assert.equal(index.format, SEGMENT_FORMAT, 'Segment index format differs')
    assert.equal(index.scenario, item.scenario, 'Segment index scenario differs')
    assert.equal(
      item.segments,
      index.segments.length,
      'Segment index count differs from the schedule'
    )
    const planned = segmentCatalogue(item.scenario).length
    if (failedIndexes.has(item.path))
      assert(item.segments <= planned, 'Failed segment index exceeds the schedule')
    else assert.equal(item.segments, planned, 'Segment index count differs from the schedule')
  }
}

if (require.main === module)
  verifyEvidence(process.argv[2], process.env.TELEMETRY_EVIDENCE_DIR, process.argv.slice(3))

module.exports = {
  runCase,
  persistEvidence,
  verifyScenario,
  verifyJavaInterop,
  verifyChurn,
  verifyChurnFence,
  verifyChurnHandshakes,
  verifyChurnLifecycles,
  verifyChurnReference,
  verifyChurnReopen,
  verifyChurnRetirement,
  verifyChurnStages,
  verifyChurnStream,
  verifyChurnTransition,
  verifyChurnWorker,
  verifyDecoderMemory,
  verifyShutdown,
  verifySeams,
  SHUTDOWN_SEAMS,
  traceChunk,
  joinChunks,
  episodeLogs,
  stableCacheEvent,
  verifyEvidence,
  verifyCleanup,
  verifyQuietResources,
  assertStreamAccounted,
  assertCorpus,
  verifyCorpusInputs,
  verifyCustom,
  verifyRecovery,
  verifyShare,
}
