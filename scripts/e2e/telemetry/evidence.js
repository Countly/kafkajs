const assert = require('assert/strict')
const crypto = require('crypto')
const fs = require('fs')
const path = require('path')
const zlib = require('zlib')
const {
  TelemetryExportReader,
  verifyMetricWindows,
} = require('../../../testHelpers/telemetryFixtures')

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
  const noPlugin = ['telemetry-reporter-no-plugin', UNSUPPORTED].includes(manifest.scenario)
  const ownerKeys = noPlugin ? [...KINDS, ...KINDS.map((k) => k + '-off')] : KINDS
  if (LIVE.includes(manifest.scenario)) assert(Object.keys(manifest.owners).length > 0)
  else assert.deepEqual(Object.keys(manifest.owners).sort(), ownerKeys.sort())
  for (const relative of Object.values(manifest.owners)) {
    const transcript = read(relative)
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
      ['telemetry-reporter-no-resource', 'telemetry-reporter-no-plugin', UNSUPPORTED].includes(
        manifest.scenario
      )
    )
    for (const [kind, relative] of Object.entries(manifest.owners)) {
      const { records, sockets } = read(relative)
      assert(!records.some((r) => r.apiKey === 72), 'Unexpected Push in negative control')
      if (noPlugin) {
        assert(!records.some((r) => r.apiKey === 71))
        assert(!sockets.some((s) => s.kind === 'telemetry'))
        assert(
          records.some((r) => r.type === 'connect'),
          'No ordinary baseline'
        )
        if (manifest.scenario === UNSUPPORTED) assertCorpus(record.corpus)
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
 * with an error the case declared for that owner, or unanswered only as a cut-off terminating Push
 * or a request on a socket the case deliberately faulted.
 * @param {Object<string,object>} transcripts observer transcript by owner label
 * @param {{errors?:Object<string,number[]>,faultedSockets?:Object<string,number[]>}} allowed the
 *   case's declared deliberate outcomes
 * @returns {void}
 * @throws {Error} naming the first undeclared error or unanswered request
 */
const assertStreamAccounted = (transcripts, allowed = {}) => {
  for (const [owner, transcript] of Object.entries(transcripts))
    for (const r of transcript.records) {
      if (r.type !== 'request' || ![71, 72].includes(r.apiKey)) continue
      if (!r.response) {
        const faulted = allowed.faultedSockets?.[owner] ?? []
        assert(
          (r.apiKey === 72 && r.terminating) || faulted.includes(r.socketId),
          `Unanswered ${r.apiKey} request ${r.requestKey} of ${owner}`
        )
      } else if (r.response.errorCode !== 0)
        assert(
          (allowed.errors?.[owner] ?? []).includes(r.response.errorCode),
          `Undeclared error ${r.response.errorCode} for ${r.requestKey} of ${owner}`
        )
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
  for (const { owner, requestKey } of record.pushes) {
    const request = transcripts[owner].records.find((r) => r.requestKey === requestKey)
    assert(request && request.apiKey === 72 && request.response?.errorCode === 0)
    const subdir = path.dirname(manifest.owners[owner])
    const decoded = read(`${subdir}/decoded-${requestKey}.json`)
    assert.equal(decoded.requestKey, requestKey)
    assert.equal(decoded.uuid, request.uuid)
    assert.equal(decoded.transmittedSha256, request.metricsSha256)
    assert.equal(decoded.transmittedBytes, request.metricsBytes)
    assert.equal(decoded.exportIdentity.node, request.node)
    const token = `${owner}:${decoded.exportIdentity.node}:${decoded.exportIdentity.seq}`
    assert(!consumed.has(token), 'Reused sink record')
    consumed.add(token)
    const rows = reader
      .readExports({ uuid: request.uuid })
      .filter((r) => r.node === decoded.exportIdentity.node && r.seq === decoded.exportIdentity.seq)
    assert.equal(rows.length, 1)
    assert.equal(rows[0].dataSha256, decoded.rawSha256)
  }
  const { results } = record
  if (manifest.scenario === 'telemetry-prefix') {
    const { owner, from, to } = results.quiet
    assert.deepEqual(
      transcripts[owner].records.filter(
        (r) => r.apiKey === 72 && r.writeAt >= from && r.writeAt < to
      ),
      [],
      'A Push was written in the empty-selection interval'
    )
  } else if (manifest.scenario === 'telemetry-payload') {
    assert.equal(results.rawControl.errorCode, 118)
    assert.equal(results.rawControl.transmittedBytes, 4097)
    assert.notEqual(results.rawControl.uuid, results.uuid)
    for (const sample of results.candidate.samples) {
      assert(sample.rawBytes <= 4096 && sample.transmittedBytes <= 4096)
      assert.deepEqual(sample.partialPoints, [])
    }
  } else if (manifest.scenario === 'telemetry-java') {
    for (const [role, java] of Object.entries(results.java)) {
      const rows = reader.readExports({ uuid: java.uuid })
      assert(rows.filter((r) => !r.terminating).length >= 2, `Too few ${role} exports`)
      assert.equal(rows.filter((r) => r.terminating).length, 1, `Missing ${role} final`)
      assert(rows.every((r) => r.parse === 'ok' && r.truncated === false))
      assert.equal(java.exitCode, 0)
      assert.equal(java.exitSignal, null)
      assert(java.closeToExitMs <= 10000)
      assert.deepEqual(java.liveDescendants, [])
    }
    assert.equal(
      new Set([...Object.values(results.uuids.java), ...Object.values(results.uuids.js)]).size,
      7
    )
  } else if (manifest.scenario === 'telemetry-outcomes') {
    for (const arm of Object.values(results.arms)) assertCorpus(arm)
    const [control, ...others] = Object.values(results.arms)
    for (const arm of others) {
      assert.deepEqual(arm.rawCommitted, control.rawCommitted)
      assert.deepEqual(arm.rawEnd, control.rawEnd)
    }
  } else if (manifest.scenario === 'telemetry-shutdown') {
    for (const [kind, episode] of Object.entries(results.episodes)) {
      assert.equal(episode.reopen.calledBeforeCacheAwait, true, `${kind} reopen was delayed`)
      assert.equal(episode.reopen.cachedUuid, episode.before.uuid)
      assert.equal(episode.reopen.get.requestUuid, episode.before.uuid)
      assert.equal(episode.reopen.finalResends, 0)
      assert(episode.reopen.correctiveExtraPushes <= 1)
      assert.deepEqual(episode.close.getOrConnectOrAuthFrames, [])
      assert.equal(episode.cacheEvent.source, 'broker-disconnect-trace')
      assert.deepEqual(
        episode.cacheEvent.connectionIdentity,
        episode.before.serverConnectionIdentity
      )
    }
    for (const zero of Object.values(results.timeout0)) assert.equal(zero.finalZeroTimeoutWrites, 0)
  }
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
}
