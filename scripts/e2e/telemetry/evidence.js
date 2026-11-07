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
  for (const file of fs.readdirSync(directory))
    if (/^(exports-\d+\.jsonl|reporter-\d+\.json|OVERFLOW.*|identity\.json)$/.test(file))
      retain(path.join(directory, file), file)
  const owners = {}
  for (const [kind, observer] of Object.entries(observers)) {
    assert(
      fs.existsSync(path.join(observer.directory, 'transcript.json')),
      'Missing observer cleanup receipt'
    )
    const subdir = path.basename(observer.directory)
    owners[kind] = `${root}/${subdir}/transcript.json`
    for (const file of fs.readdirSync(observer.directory))
      if (
        /^(transcript\.json|raw-[a-f0-9]+\.bin|push-r\d+\.bin|decoded-r\d+\.json|decoder-\d+\.json)$/.test(
          file
        )
      )
        retain(path.join(observer.directory, file), `${subdir}/${file}`)
  }
  return {
    scenario: name,
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
    if (manifest.scenario === 'telemetry-reporter') {
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
  const ownerKeys =
    manifest.scenario === 'telemetry-reporter-no-plugin'
      ? [...KINDS, ...KINDS.map((k) => k + '-off')]
      : KINDS
  assert.deepEqual(Object.keys(manifest.owners).sort(), ownerKeys.sort())
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
    assert(results.control.requests.length > 0)
    for (const node of ['1', '2', '3']) {
      const proof = read(`${manifest.root}/reporter-${node}.json`)
      assert.equal(proof.node, node)
      assert.equal(proof.kafkaClients, record.run.line)
    }
  } else {
    assert(
      ['telemetry-reporter-no-resource', 'telemetry-reporter-no-plugin'].includes(manifest.scenario)
    )
    for (const [kind, relative] of Object.entries(manifest.owners)) {
      const { records, sockets } = read(relative)
      assert(!records.some((r) => r.apiKey === 72), 'Unexpected Push in negative control')
      if (manifest.scenario.endsWith('no-plugin')) {
        assert(!records.some((r) => r.apiKey === 71))
        assert(!sockets.some((s) => s.kind === 'telemetry'))
        assert(
          records.some((r) => r.type === 'connect'),
          'No ordinary baseline'
        )
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

module.exports = { persistEvidence, verifyScenario, verifyEvidence, verifyCleanup }
