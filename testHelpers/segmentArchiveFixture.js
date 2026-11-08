const crypto = require('crypto')
const fs = require('fs')
const os = require('os')
const path = require('path')
const { SCHEDULES, packSegment } = require('./telemetrySegments')
const { cacheRetirement } = require('../scripts/e2e/telemetry/cacheTrace')
const {
  SHUTDOWN_SEAMS,
  episodeLogs,
  persistEvidence,
  stableCacheEvent,
  traceChunk,
} = require('../scripts/e2e/telemetry/evidence')

const SCENARIO = 'telemetry-shutdown'
const LINE = '4.3.1'
const ZERO = '00000000-0000-0000-0000-000000000000'
const NODE_OF_KIND = { producer: '1', consumer: '2', admin: '3', share: '1' }
const PORTS = { 1: 19092, 2: 19095, 3: 19098 }
const LISTENERS = { 1: '9092', 2: '9095', 3: '9098' }
const LOGGER = 'org.apache.kafka.server.ClientMetricsManager'
const REQUEST_LOGGER = 'kafka.request.logger'
const sha = (bytes) => crypto.createHash('sha256').update(bytes).digest('hex')
const uuidFor = (n) => `10000000-0000-4000-8000-${String(n).padStart(12, '0')}`

/**
 * Builds the transcript, payloads, decoded sidecars and sink rows of one segment.
 * @param {object} model the scenario model that owns the sink rows
 * @param {object} spec the segment's identity and timeline
 * @returns {object} `{ id, transcript, blobs, decoded, receipts }`
 */
const makeSegment = (model, spec) => {
  const { id, uuid, firstUuid, start, pushes, closeAt, final, hole } = spec
  const node = spec.node
  const clientId = `${model.stem}-${id.replace(/-\d+$/, '')}`
  const records = []
  const blobs = new Map()
  const decoded = new Map()
  const receipts = []
  const add = (record) => {
    record.index = records.length + 1
    if (record.type === 'request') {
      Object.assign(record, {
        requestKey: `r${record.index}`,
        owner: id,
        clientId,
        node,
        size: 40,
        sha256: sha(`${id}:${record.index}`),
      })
    }
    records.push(record)
    return record
  }
  add({
    type: 'connect',
    socketId: 1,
    node,
    port: PORTS[node],
    localAddress: '127.0.0.1',
    localPort: 40000 + records.length,
    remoteAddress: '127.0.0.1',
    remotePort: PORTS[node],
    at: start + 10,
  })
  add({
    type: 'request',
    socketId: 1,
    apiKey: 71,
    apiVersion: 0,
    correlation: 1,
    uuid: firstUuid,
    writeAt: start + 20,
    response: {
      responseAt: start + 30,
      size: 60,
      sha256: sha(`${id}:get`),
      throttleMs: 0,
      errorCode: 0,
      uuid,
      subscriptionId: 7,
      acceptedCompressionTypes: [0],
      pushIntervalMs: 1000,
      telemetryMaxBytes: 4096,
      deltaTemporality: false,
      requestedMetrics: ['org.apache.kafka.'],
    },
  })
  const push = (label, writeAt, terminating, answered) => {
    const blob = Buffer.from(`${id}:${label}`)
    const record = add({
      type: 'request',
      socketId: 1,
      apiKey: 72,
      apiVersion: 0,
      correlation: records.length + 1,
      uuid,
      subscriptionId: 7,
      terminating,
      compressionType: 0,
      writeAt,
      metricsBytes: blob.length,
      metricsSha256: sha(blob),
    })
    blobs.set(record.requestKey, blob)
    if (!answered) return record
    record.response = {
      responseAt: writeAt + 10,
      size: 20,
      sha256: sha(`${id}:${label}:response`),
      throttleMs: 0,
      errorCode: 0,
    }
    const rows = model.sink[node]
    rows.push({
      v: 1,
      node,
      seq: rows.length + 1,
      clientInstanceId: uuid,
      clientId,
      parse: 'ok',
      dataBytes: blob.length,
      dataSha256: sha(blob),
      terminating,
      pushIntervalMs: 1000,
    })
    const body = {
      dataBytes: blob.length,
      dataSha256: sha(blob),
      parse: 'ok',
      truncated: false,
      resources: [],
      metrics: [{ name: 'org.apache.kafka.producer.connection.creation.total', points: [] }],
    }
    decoded.set(record.requestKey, {
      requestKey: record.requestKey,
      uuid,
      node,
      compressionType: 0,
      rawBytes: blob.length,
      transmittedBytes: blob.length,
      transmittedSha256: sha(blob),
      rawSha256: sha(blob),
      decoded: body,
      exportIdentity: { node, seq: rows.length },
    })
    receipts.push({
      code: 0,
      signal: null,
      line: LINE,
      stdout: `${JSON.stringify({ verified: true, line: LINE })}\n${JSON.stringify(body)}\n`,
      stderr: '',
      rawSha256: sha(blob),
    })
    return record
  }
  for (let index = 0; index < pushes; index++)
    push(`p${index}`, start + 1100 + index * 1000, false, true)
  const sockets = [{ id: 1, kind: 'telemetry', node, foregroundBootstrap: false }]
  const transcript = {
    owner: id,
    closures: [{ at: closeAt, pending: [], end: closeAt + 20 }],
    faults: [],
    cancellations: [],
    records,
    sockets,
    cleanup: { children: 0, timers: 0, stillOpen: [], attached: 0 },
    failures: [],
  }
  if (final) push('final', closeAt + 5, true, true)
  sockets[0].closedAt = closeAt + 25
  if (hole) {
    const faultAt = closeAt - 10
    transcript.faults.push({ socketId: 1, at: faultAt })
    const attempted = push('lost-final', closeAt + 2, true, false)
    transcript.cancellations.push({
      requestKey: attempted.requestKey,
      apiKey: 72,
      socketId: 1,
      writeAt: attempted.writeAt,
      at: faultAt,
      end: closeAt + 900,
      disposition: 'faulted-transport',
    })
    sockets[0].closedAt = closeAt + 900
    sockets.push({ id: 2, kind: 'normal', node, foregroundBootstrap: true, closedAt: closeAt + 30 })
    add({
      type: 'request',
      socketId: 2,
      apiKey: 3,
      apiVersion: 6,
      correlation: 90,
      writeAt: closeAt - 2,
      response: { responseAt: closeAt + 15, size: 10, sha256: sha(`${id}:metadata`) },
    })
  }
  return { id, transcript, blobs, decoded, receipts }
}

/**
 * Builds a complete, valid, mutable model of the segmented shutdown scenario: 89 segments, shared
 * trace windows, round and journal records, the arms, budgets and quiet resources.
 * @param {object} [options] `stem` of the run
 * @returns {object} the model: segments, sink rows, results, chunks and source; set `padding` to bloat the oracle file
 */
const buildShutdownModel = ({ stem = 'telemetry-shutdown-0a1b2c3d' } = {}) => {
  const schedule = SCHEDULES[SCENARIO]
  const model = {
    stem,
    segments: new Map(),
    sink: { 1: [], 2: [], 3: [] },
    lines: Array.from({ length: schedule.perKind }, () => ({ 1: [], 2: [], 3: [] })),
    results: { initial: {}, rounds: {}, journal: {}, timeout0: {} },
    source: {
      artifact: `kafka-server-${LINE}.jar`,
      sha256: 'a'.repeat(64),
      receipt: { code: 0, stdout: `${'a'.repeat(64)}  /opt/kafka/libs/kafka-server-${LINE}.jar` },
      className: LOGGER,
    },
  }
  const base = Date.UTC(2026, 10, 7, 10, 0, 0)
  const lifecycles = {}
  schedule.kinds.forEach((kind, n) => {
    const node = NODE_OF_KIND[kind]
    const uuid = uuidFor(n + 1)
    const notes = []
    lifecycles[kind] = []
    let start = 1000 + n * 7
    for (let s = 0; s < schedule.perKind; s++) {
      const id = schedule.name(kind, s)
      const closeAt = start + (s === 0 ? 4000 : 3000)
      const segment = makeSegment(model, {
        id,
        uuid,
        firstUuid: s === 0 ? ZERO : uuid,
        node,
        start,
        pushes: s === 0 ? 2 : 1,
        closeAt,
        final: true,
      })
      model.segments.set(id, segment)
      const connId = `172.18.0.${node}:${LISTENERS[node]}-172.18.0.1:${50000 + n * 100 + s}-${n * 100 + s + 1}`
      const clientId = `${stem}-${kind}-ep`
      model.lines[s][node].push(
        `TRACE Completed request:RequestHeader(apiKey=GET_TELEMETRY_SUBSCRIPTIONS, clientId=${clientId}, correlationId=1) -- GetTelemetrySubscriptionsRequestData(clientInstanceId=${s === 0 ? ZERO : uuid}) with response:GetTelemetrySubscriptionsResponseData(clientInstanceId=${uuid}) from connection ${connId};totalTime:1`
      )
      if (s > 0)
        model.lines[s][lifecycles[kind][s - 1].node].push(
          `TRACE Removing client connection id [${lifecycles[kind][s - 1].connId}] from the client instance cache`
        )
      lifecycles[kind].push({
        node,
        connId,
        get: segment.transcript.records[1],
        connect: segment.transcript.records[0],
        closeAt,
      })
      if (s < schedule.perKind - 1) {
        const end = closeAt + 20
        const seq = notes.length
        const pair = { from: id, to: schedule.name(kind, s + 1) }
        notes.push(
          { seq: seq + 1, type: 'select', at: closeAt - 1, ...pair },
          { seq: seq + 2, type: 'disconnect-resolved', at: end + 1, ...pair },
          { seq: seq + 3, type: 'connect-called', at: end + 2, ...pair }
        )
        start = end + 5
      }
    }
    model.results.journal[kind] = notes
    model.results.initial[kind] = { segment: schedule.name(kind, 0), uuid, get: 'r2' }
  })
  model.chunks = model.lines.map((nodes, index) =>
    traceChunk({
      index,
      since: new Date(base + index * 1000).toISOString(),
      until: new Date(base + (index + 1) * 1000).toISOString(),
      logs: Object.fromEntries(
        Object.entries(nodes).map(([node, lines]) => [
          node,
          { text: ['INFO unrelated broker line', ...lines].join('\n'), truncated: false },
        ])
      ),
    })
  )
  model.rebuildRounds = () => {
    for (const kind of schedule.kinds) {
      const uuid = model.results.initial[kind].uuid
      model.results.rounds[kind] = Array.from({ length: 20 }, (_, index) => {
        const round = index + 1
        const old = lifecycles[kind][round - 1]
        const retired = cacheRetirement({
          logs: episodeLogs(model.chunks, round - 1, model.chunks.length - 1),
          node: old.node,
          localPort: LISTENERS[old.node],
          get: old.get,
          connect: old.connect,
          uuid,
          since: model.chunks[round - 1].since,
          completedAt: model.chunks.at(-1).until,
          source: model.source,
        })
        return {
          round,
          from: schedule.name(kind, round - 1),
          to: schedule.name(kind, round),
          cachedUuid: uuid,
          observedAt: old.closeAt - 5,
          get: 'r2',
          push: 'r3',
          cacheEvent: stableCacheEvent(retired),
        }
      })
    }
  }
  model.rebuildRounds()
  ;['producer', 'consumer', 'share', 'admin'].forEach((kind, n) => {
    const id = `${kind}-t0`
    const uuid = uuidFor(10 + n)
    model.segments.set(
      id,
      makeSegment(model, {
        id,
        uuid,
        firstUuid: ZERO,
        node: NODE_OF_KIND[kind],
        start: 500,
        pushes: 2,
        closeAt: 3500,
        final: false,
      })
    )
    model.results.timeout0[kind] = { uuid, finalZeroTimeoutWrites: 0 }
  })
  const holeUuid = uuidFor(20)
  model.segments.set(
    'admin-bh',
    makeSegment(model, {
      id: 'admin-bh',
      uuid: holeUuid,
      firstUuid: ZERO,
      node: '3',
      start: 600,
      pushes: 2,
      closeAt: 3600,
      final: false,
      hole: true,
    })
  )
  model.results.blackhole = {
    uuid: holeUuid,
    faultedSocket: 1,
    coreCloseMs: 40,
    remainingT: 800,
    telemetryCleanupMs: 900,
    core: { sameResult: true },
  }
  model.results.seams = SHUTDOWN_SEAMS.map(([file, title]) => ({
    file,
    title,
    lane: 'strict seam, unit lane',
  }))
  const describe = (id, levels) => ({
    service: `kafka${id}`,
    id,
    action: ['--describe'],
    result: {
      code: 0,
      truncated: false,
      stdout: Object.entries(levels)
        .filter(([, level]) => level)
        .map(([logger, level]) => `${logger}=${level}`)
        .join('\n'),
    },
  })
  const original = { [LOGGER]: null, [REQUEST_LOGGER]: 'WARN' }
  const traced = { [LOGGER]: 'TRACE', [REQUEST_LOGGER]: 'TRACE' }
  model.results.originalLevels = { 1: original, 2: original, 3: original }
  model.results.restoredLevels = { 1: original, 2: original, 3: original }
  model.results.loggerReceipts = ['1', '2', '3'].flatMap((id) => [
    describe(id, original),
    describe(id, traced),
    describe(id, original),
  ])
  model.results.workMs = 60000
  const quiet = {
    activeResources: { Timeout: 2 },
    identities: { openSockets: [], pending: [], undischarged: [] },
    resources: { case: {}, quiet: {}, unknown: { Timeout: [3, 4] } },
    registrations: { process: { id: 1, events: { exit: [2] } } },
  }
  model.results.resources = { baseline: quiet, final: JSON.parse(JSON.stringify(quiet)) }
  model.timing = { bodyMs: 60000, cleanupMs: 8000 }
  return model
}

/**
 * Packs every segment, writes the three sinks and persists the archive, as a passing run would.
 * @param {object} model the (possibly mutated) scenario model
 * @param {object} [options] `directory` of the run, a fresh temporary one by default
 * @returns {{record:object,directory:string,packs:object[]}} stamped record with its manifest
 */
const writeShutdownArchive = (model, { directory } = {}) => {
  const root = directory ?? fs.mkdtempSync(path.join(os.tmpdir(), 'segment-archive-'))
  for (const node of ['1', '2', '3'])
    fs.writeFileSync(
      path.join(root, `exports-${node}.jsonl`),
      model.sink[node].map((row) => JSON.stringify(row) + '\n').join('')
    )
  const scratch = fs.mkdtempSync(path.join(root, 'scratch-'))
  const packs = [...model.segments.values()].map((segment) => {
    const source = fs.mkdtempSync(path.join(scratch, 'observer-'))
    fs.writeFileSync(path.join(source, 'transcript.json'), JSON.stringify(segment.transcript))
    for (const [key, blob] of segment.blobs)
      fs.writeFileSync(path.join(source, `push-${key}.bin`), blob)
    let receipt = 0
    for (const [key, sidecar] of segment.decoded) {
      fs.writeFileSync(path.join(source, `decoded-${key}.json`), JSON.stringify(sidecar))
      fs.writeFileSync(path.join(source, `raw-${sidecar.rawSha256}.bin`), segment.blobs.get(key))
    }
    for (const entry of segment.receipts)
      fs.writeFileSync(path.join(source, `decoder-${++receipt}.json`), JSON.stringify(entry))
    return packSegment({ id: segment.id, source, target: path.join(root, 'segments') })
  })
  fs.rmSync(scratch, { recursive: true, force: true })
  const errors = Object.fromEntries(
    SCHEDULES[SCENARIO].kinds.flatMap((kind) =>
      Array.from({ length: SCHEDULES[SCENARIO].perKind }, (_, index) => [
        SCHEDULES[SCENARIO].name(kind, index),
        [42],
      ])
    )
  )
  const record = {
    run: { stem: model.stem, line: LINE },
    status: 'passed',
    timing: model.timing,
    results: model.results,
    allowed: { errors, faultedSockets: { 'admin-bh': [1] } },
    oracles: {
      traces: {
        source: model.source,
        loggerReadbacks: model.results.loggerReceipts,
        chunks: model.chunks,
      },
      ...(model.padding ? { padding: 'x'.repeat(model.padding) } : {}),
    },
    pushes: packs.flatMap((pack) =>
      pack.accepted.map((requestKey) => ({ owner: pack.id, requestKey }))
    ),
    zeroPushes: [],
  }
  record.sidecars = persistEvidence({
    name: SCENARIO,
    record,
    directory: root,
    observers: {},
    segments: [{ sealed: () => packs }],
  })
  return { record, directory: root, packs }
}

module.exports = { LINE, SCENARIO, buildShutdownModel, writeShutdownArchive }
