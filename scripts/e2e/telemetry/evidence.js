const assert = require('assert/strict')
const crypto = require('crypto')
const fs = require('fs')
const path = require('path')
const zlib = require('zlib')
const {
  TelemetryExportReader,
  verifyMetricWindows,
} = require('../../../testHelpers/telemetryFixtures')

const {
  assertCatalogue,
  assertCadence,
  assertJavaRows,
  assertJsCounter,
  assertCustomShape,
  ordinaryOwnership,
} = require('./semantics')
const { confirmedDecisions, acquisitionStart, heldAcquisitions } = require('./shareEvidence')
const { cacheRetirement } = require('./cacheTrace')

const sha = (bytes) => crypto.createHash('sha256').update(bytes).digest('hex')
const KINDS = ['producer', 'consumer', 'admin', 'share']
const CAP = 16 * 1024 * 1024
const CONFIGURED_COMPOSE = 'docker-compose.telemetry.yml'
const LIVE = Object.freeze([
  'telemetry-prefix',
  'telemetry-custom',
  'telemetry-payload',
  'telemetry-java',
  'telemetry-outcomes',
  'telemetry-shutdown',
])
const UNSUPPORTED = 'telemetry-unsupported'
const JAVA_ROLES = ['producer', 'consumer', 'admin']
const FAULT_MODES = ['drop', 'blackhole', 'late']
const ZERO_TIMEOUT_KINDS = ['producer', 'consumer', 'share']

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
  } = {}
) => {
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
  if (!errors.length) evidence.stage = 'cleanup'
  errors.push(...(await cleanup.run()))
  const describe = (error, depth = 0) => ({
    name: error?.name || typeof error,
    message: String(error?.message ?? error).slice(0, 4096),
    ...(depth < 4 && Array.isArray(error?.errors)
      ? { causes: error.errors.slice(0, 16).map((cause) => describe(cause, depth + 1)) }
      : {}),
  })
  const record = { ...evidence.record, status: errors.length ? 'failed' : 'passed' }
  if ([...LIVE, UNSUPPORTED].includes(name)) {
    record.oracles = {
      ...record.oracles,
      ...(record.results?.rawOracle ? { rawControl: record.results.rawOracle } : {}),
      ...Object.fromEntries(
        Object.entries(record.results?.arms ?? {}).map(([key, value]) => [key, value.inputs])
      ),
      ...(record.corpus ? { corpus: record.corpus.inputs } : {}),
      ...(record.results?.share?.inputs ? { share: record.results.share.inputs } : {}),
    }
    record.pushes = Object.entries(evidence.observers).flatMap(([owner, observer]) =>
      observer
        .readTranscript()
        .filter((r) => r.apiKey === 72 && r.response?.errorCode === 0 && r.metricsBytes > 0)
        .map(({ requestKey }) => ({ owner, requestKey }))
    )
    record.zeroPushes = Object.entries(evidence.observers).flatMap(([owner, observer]) =>
      observer
        .readTranscript()
        .filter((r) => r.apiKey === 72 && r.metricsBytes === 0)
        .map(({ requestKey }) => ({ owner, requestKey, disposition: zeroDisposition(name) }))
    )
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
const persistEvidence = ({ name, record, directory, observers }) => {
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
  return {
    scenario: name,
    archiveErrors,
    root,
    owners,
    files,
    cleanupReceipt: { path: 'ownership-receipt.json', project: record.run.project },
  }
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
    assert(['4.3.1', '4.2.2', '4.1.2'].includes(record.run.line))
    const configured = record.run.line !== '4.1.2'
    assert.equal(record.mode, configured ? 'configured' : 'base')
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
      assert.deepEqual(results.share, {
        status: 'rejected',
        name: 'KafkaJSShareGroupsNotSupportedError',
        reason: 'api-version-range',
        apiKey: 78,
        shareVersion: 0,
      })
    } else {
      assert.equal(results.share?.recovery?.owner, 'share', 'Missing configured share recovery')
      assert.equal(results.share.recovery.mode, 'drop')
    }
    return [...arms.flatMap((arm) => [`${arm}-producer`, `${arm}-consumer`]), 'share']
  }
  assert.equal(scenario, 'telemetry-shutdown')
  exact(results.episodes, KINDS, 'shutdown episode')
  const zero = ZERO_TIMEOUT_KINDS
  exact(results.timeout0, zero, 'zero-timeout')
  assert(results.blackhole && results.blackhole.uuid, 'Missing admin blackhole')
  return [...KINDS.map((kind) => `${kind}-ep`), ...zero.map((kind) => `${kind}-t0`), 'admin-bh']
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
      (LIVE.includes(manifest.scenario) && identity.compose === CONFIGURED_COMPOSE)
    ) {
      const pinned = JSON.parse(fs.readFileSync(path.join(__dirname, 'fixtures/manifest.json')))
      for (const image of identity.images)
        assert(
          pinned.lines[record.run.line].repoDigests.some((digest) =>
            image.repoDigests.includes(digest)
          ),
          'Image digest mismatch'
        )
      for (const broker of identity.brokers) {
        assert.equal(broker.reporterSha256, pinned.lines[record.run.line].reporterJar.sha256)
        assert(broker.configSha256)
      }
      assert.equal(
        record.fixture.manifestSha256,
        sha(fs.readFileSync(path.join(__dirname, 'fixtures/manifest.json')))
      )
      assert.equal(record.run.composeSha256, pinned.composeSha256['docker-compose.telemetry.yml'])
    }
  }
  const noPlugin = ['telemetry-reporter-no-plugin', UNSUPPORTED, 'telemetry-floor'].includes(
    manifest.scenario
  )
  const ownerKeys = LIVE.includes(manifest.scenario)
    ? requiredLiveCatalogue(record)
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
    if (
      ['telemetry-java', 'telemetry-outcomes', 'telemetry-shutdown'].includes(manifest.scenario)
    ) {
      const requests = transcript.records.filter((r) => r.type === 'request')
      assert(
        requests.length > 0 && requests.every((r) => r.clientId === `${record.run.stem}-${owner}`),
        'Owner stream client identity differs'
      )
    }
    assert.deepEqual(transcript.failures, [])
    assert.deepEqual(transcript.cleanup, { children: 0, timers: 0, stillOpen: [], attached: 0 })
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
      const { records, sockets } = read(relative)
      assert(!records.some((r) => r.apiKey === 72), 'Unexpected Push in negative control')
      if (noPlugin) {
        assert(!records.some((r) => r.apiKey === 71))
        if ([UNSUPPORTED, 'telemetry-floor'].includes(manifest.scenario))
          ordinaryOwnership({ records, sockets })
        else assert(!sockets.some((s) => s.kind === 'telemetry'))
        assert(
          records.some((r) => r.type === 'connect'),
          'No ordinary baseline'
        )
        if (manifest.scenario === UNSUPPORTED) {
          assertCorpus(record.corpus)
          verifyCorpusInputs(record.corpus, read(`${manifest.root}/oracles.json`).corpus)
          const oracles = read(`${manifest.root}/oracles.json`)
          for (const kind of KINDS) {
            const ownership = (owner) => ordinaryOwnership(read(manifest.owners[owner]))
            const on = ownership(kind)
            const off = ownership(`${kind}-off`)
            assert.deepEqual(record.results.ownership[kind], { on, off })
            assert.deepEqual(
              on.map((r) => r.split(':').at(-1)).sort(),
              off.map((r) => r.split(':').at(-1)).sort(),
              'Enabled/disabled socket ownership differs'
            )
          }

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
              assert.equal(confirmed.length, 10)
              assert.deepEqual(
                confirmed.map((r) => r.offset).sort(),
                Array.from({ length: 10 }, (_, i) => String(i))
              )
              assert(
                confirmed.every(
                  (r) => r.type === 1 && r.partition === 0 && r.topicId === share.topicId
                )
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
        else assert.equal(outcome.name, 'KafkaJSShareGroupsNotSupportedError')
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

/**
 * Requires every Push and Get of every owner to be accounted for: answered with success, answered
 * with an error the case declared for that owner, or an exact request-level cancellation joined
 * to its public-close boundary or one deliberately faulted exchange.
 * @param {Object<string,object>} transcripts observer transcript by owner label
 * @param {{errors?:Object<string,number[]>}} allowed per-owner deliberate response errors; unanswered
 *   requests require transcript cancellations joined to closures or declared request-level faults
 * @returns {void}
 * @throws {Error} naming the first undeclared error or unanswered request
 */
const assertStreamAccounted = (transcripts, allowed = {}) => {
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
      } else if (r.response.errorCode !== 0)
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
  assertStreamAccounted(transcripts, record.allowed)
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
    const subscription = transcript.records
      .filter(
        (r) =>
          r.apiKey === 71 &&
          r.socketId === request.socketId &&
          r.response?.errorCode === 0 &&
          r.response.responseAt <= request.writeAt
      )
      .at(-1)
    assert(subscription, 'Push without a successful subscription')
    assert.equal(subscription.response.uuid, request.uuid)
    assert.equal(subscription.response.subscriptionId, request.subscriptionId)
    assert.equal(decoded.requestKey, requestKey)
    assert.equal(decoded.uuid, request.uuid)
    assert.equal(decoded.node, request.node)
    assert.equal(decoded.compressionType, request.compressionType)
    assert.equal(decoded.transmittedSha256, request.metricsSha256)
    assert.equal(decoded.transmittedBytes, request.metricsBytes)
    const transmitted = fs.readFileSync(path.join(directory, subdir, `push-${requestKey}.bin`))
    // read() also enforces catalogue membership for the binary files through their metadata.
    assert(manifest.files.some((f) => f.path === `${subdir}/push-${requestKey}.bin`))
    assert.equal(sha(transmitted), request.metricsSha256)
    assert.equal(transmitted.length, request.metricsBytes)
    const cap = Math.min(4096, subscription.response.telemetryMaxBytes)
    const options = { maxOutputLength: cap }
    let raw
    if (request.compressionType === 0) raw = transmitted
    else if (request.compressionType === 1) raw = zlib.gunzipSync(transmitted, options)
    // eslint-disable-next-line n/no-unsupported-features/node-builtins -- live Node lines supply the built-in codec
    else if (request.compressionType === 4) raw = zlib.zstdDecompressSync(transmitted, options)
    else throw new Error('Unsupported captured compression')
    assert(raw.length > 0 && raw.length <= cap && transmitted.length <= cap)
    assert.equal(sha(raw), decoded.rawSha256, 'Transmitted/raw relationship differs')
    assert.equal(raw.length, decoded.rawBytes)
    const rawPath = `${subdir}/raw-${decoded.rawSha256}.bin`
    assert(manifest.files.some((f) => f.path === rawPath))
    assert(raw.equals(fs.readFileSync(path.join(directory, rawPath))))
    assert.equal(decoded.decoded.dataSha256, sha(raw))
    assert.equal(decoded.decoded.dataBytes, raw.length)
    assert.equal(decoded.decoded.parse, 'ok')
    assert.equal(decoded.decoded.truncated, false)
    const receipts = manifest.files
      .filter((f) => f.path.startsWith(`${subdir}/decoder-`) && f.path.endsWith('.json'))
      .map((f) => read(f.path))
      .filter((r) => r.rawSha256 === decoded.rawSha256)
    assert(receipts.length > 0, 'Missing independent decoder receipt')
    assert(
      receipts.some((receipt) => {
        if (receipt.code !== 0 || receipt.signal !== null || receipt.line !== record.run.line)
          return false
        const [identity, body] = receipt.stdout.trim().split('\n').map(JSON.parse)
        if (identity.verified !== true || identity.line !== record.run.line) return false
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
    assert.equal(row.dataSha256, sha(raw))
    assert.equal(row.dataBytes, raw.length)
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
  const oracles = read(`${manifest.root}/oracles.json`)
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
    for (const [role, java] of Object.entries(results.java)) {
      assert.equal(java.assignedId, `${record.run.stem}-java-${role}`)
      const rows = reader.readExports({ uuid: java.uuid })
      assert(rows.filter((r) => !r.terminating).length >= 2, `Too few ${role} exports`)
      assert.equal(rows.filter((r) => r.terminating).length, 1, `Missing ${role} final`)
      assertJavaRows(
        role,
        rows.sort((a, b) => a.tsMs - b.tsMs || a.seq - b.seq),
        {
          clientId: `${record.run.stem}-java-${role}`,
          closing: java.closing,
          closed: java.closed,
          group: `${record.run.stem}-java-consumer-group`,
          membership: java.stdout
            .trim()
            .split('\n')
            .map(JSON.parse)
            .filter((r) => r.membership === true),
        }
      )
      assert.equal(java.exitCode, 0)
      assert.equal(java.exitSignal, null)
      assert(java.closeToExitMs <= 10000)
      assert.deepEqual(java.liveDescendants, [])
    }
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
      pins
    )
    for (const role of ['producer', 'consumer', 'admin'])
      assert(provenance.jdk[role]?.jdk && provenance.jdk[role]?.vendor)
    assert.equal(
      new Set([...Object.values(results.uuids.java), ...Object.values(results.uuids.js)]).size,
      7
    )
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
        if (record.mode === 'base' || name === 'control') ordinaryOwnership(wire)
        else {
          const get = wire.records.find((r) => r.apiKey === 71 && r.response?.errorCode === 0)
          assert(get, 'Missing opted-in arm subscription')
          assertOwnerUuid(wire, get.response.uuid)
        }
      }
    }
    if (record.mode === 'configured')
      verifyShare(results.share, oracles.share, transcripts, `${record.run.stem}-share-topic`)
    else ordinaryOwnership(transcripts.share)
    for (const recovery of Object.values(results.faults).flat())
      verifyRecovery(recovery, transcripts[recovery.owner])
    const [control, ...others] = Object.values(results.arms)
    for (const arm of others) {
      assert.deepEqual(arm.rawCommitted, control.rawCommitted)
      assert.deepEqual(arm.rawEnd, control.rawEnd)
    }
  } else if (manifest.scenario === 'telemetry-shutdown') {
    assert.deepEqual(
      Object.keys(oracles).sort(),
      [...KINDS].sort(),
      'Required shutdown oracle catalogue differs'
    )
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
      const reads = results.loggerReceipts.filter(
        (r) => r.id === id && r.action[0] === '--describe'
      )
      assert(reads.length >= 3 && reads.every((r) => r.result.code === 0 && !r.result.truncated))
      assert.deepEqual(levels(reads[0].result.stdout), results.originalLevels[id])
      assert.deepEqual(levels(reads.at(-1).result.stdout), results.restoredLevels[id])
    }

    for (const [kind, episode] of Object.entries(results.episodes)) {
      assertOwnerUuid(transcripts[`${kind}-ep`], episode.before.uuid)
      assert.equal(episode.reopen.calledBeforeCacheAwait, true, `${kind} reopen was delayed`)
      assert.equal(episode.reopen.cachedUuid, episode.before.uuid)
      assert.equal(episode.reopen.get.requestUuid, episode.before.uuid)
      assert.equal(episode.reopen.finalResends, 0)
      assert(episode.reopen.correctiveExtraPushes <= 1)
      assert.deepEqual(episode.close.getOrConnectOrAuthFrames, [])
      const trace = oracles[kind]
      assert.equal(trace.source.receipt.code, 0)
      assert.equal(trace.source.receipt.stdout.split(/\s+/)[0], trace.source.sha256)
      assert.deepEqual(trace.loggerReadbacks, results.loggerReceipts)
      assert.deepEqual(cacheRetirement(trace), episode.cacheEvent)
      const get = transcripts[`${kind}-ep`].records.find(
        (r) => r.requestKey === trace.get.requestKey
      )
      assert.deepEqual(get, trace.get)
      assert.equal(get.response.uuid, episode.before.uuid)
      assert.equal(trace.uuid, episode.before.uuid)
      assert.equal(episode.cacheEvent.source, 'broker-disconnect-trace')
      assert.deepEqual(
        episode.cacheEvent.connectionIdentity,
        episode.before.serverConnectionIdentity
      )
    }
    for (const [kind, zero] of Object.entries(results.timeout0)) {
      const transcript = transcripts[`${kind}-t0`]
      assertOwnerUuid(transcript, zero.uuid)
      assert.equal(zero.finalZeroTimeoutWrites, 0)
      assert(
        !transcript.records.some((r) => r.apiKey === 72 && r.terminating),
        'Zero-timeout final was written'
      )
    }
    assertOwnerUuid(transcripts['admin-bh'], results.blackhole.uuid)
    assert(
      transcripts['admin-bh'].sockets.some(
        (s) => s.id === results.blackhole.faultedSocket && Number.isFinite(s.closedAt)
      )
    )
  }
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
 * @returns {void}
 * @throws {Error} for an unexercised fault or invented replacement
 */
const verifyRecovery = (recovery, transcript) => {
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
      get.uuid === recovery.uuid &&
      get.socketId !== recovery.faultedSocket &&
      get.response?.errorCode === 0
  )
  assert(get.writeAt >= socket.closedAt)
  const push = request(recovery.recoveredPush)
  assert(
    push &&
      push.socketId === get.socketId &&
      push.uuid === recovery.uuid &&
      push.response?.errorCode === 0 &&
      !push.terminating
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
    assert.equal(range.topicId, inputs.topicId)
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
    assert.equal(event.topic, topic)
    assert.equal(event.partition, 0)
    const first = BigInt(event.firstOffset)
    const last = BigInt(event.lastOffset)
    assert(first >= 0n && last < 10n && first <= last)
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
 * @returns {void}
 * @throws {Error} for missing confirmations, duplicates, altered records or late release
 */
const verifyShare = (result, inputs, transcripts, topic) => {
  assert(inputs, 'Missing share oracle')
  assert.equal(result.lockMs, 15000)
  assert.equal(inputs.acquiredAt, acquisitionStart(inputs.frames, inputs.group))
  assert(inputs.handlerAt >= inputs.acquiredAt)
  assert.equal(result.recovery?.owner, 'share', 'Missing configured share recovery')
  assert.equal(result.recovery.mode, 'drop')
  verifyRecovery(result.recovery, transcripts.share)
  verifyShareIdentity(inputs, topic, inputs.expected)
  assert(inputs.held.every((r) => r.lockMs === result.lockMs))
  assert.deepEqual(
    confirmedDecisions(inputs.frames, inputs.group),
    inputs.confirmed,
    'Contradicted acknowledgement ledger'
  )
  assert.equal(inputs.confirmed.length, 10, 'Extra or duplicate broker share settlement')
  for (let index = 0; index < 10; index++) {
    const decisions = inputs.confirmed.filter((r) => r.offset === String(index))
    assert.equal(decisions.length, 1, 'Share settlement must occur exactly once')
    const [decision] = decisions
    assert.equal(decision.topicId, inputs.topicId)
    assert.equal(decision.partition, 0)
    assert.equal(decision.type, index < 4 ? 1 : 2)
    assert.equal(decision.errorCode, 0)
    assert(
      decision.at >= inputs.acquiredAt && decision.at < inputs.acquiredAt + result.lockMs,
      'Settlement after lock expiry'
    )
  }
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
      (r) => r.deliveryCount >= 2 && r.at < inputs.acquiredAt + result.lockMs
    )
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
  for (const item of manifest.scenarios) {
    assert(/^[a-z0-9-]+\.jsonl$/.test(item.path))
    const content = fs.readFileSync(path.join(stamps, item.path))
    assert.equal(content.length, item.bytes)
    assert.equal(sha(content), item.sha256)
  }
}

if (require.main === module)
  verifyEvidence(process.argv[2], process.env.TELEMETRY_EVIDENCE_DIR, process.argv.slice(3))

module.exports = {
  runCase,
  persistEvidence,
  verifyScenario,
  verifyEvidence,
  verifyCleanup,
  assertStreamAccounted,
  assertCorpus,
  verifyCorpusInputs,
  verifyCustom,
  verifyRecovery,
  verifyShare,
}
