const crypto = require('node:crypto')
const fs = require('node:fs')
const path = require('node:path')
const {
  ROWS,
  CLIENT_KINDS,
  RECORD_KINDS,
  StreamValidator,
  validateCapabilities,
  validateConfig,
  validateHeader,
  validateMatrix,
  validateProvenance,
  validateRecord,
  mergePieces,
} = require('../finalSoakSchema')

const ROOT = path.join(__dirname, '../../..')
const matrix = JSON.parse(fs.readFileSync(path.join(__dirname, '../finalSoakMatrix.json'), 'utf8'))
const copy = (value) => JSON.parse(JSON.stringify(value))
const sha256 = (text) => crypto.createHash('sha256').update(text).digest('hex')

const RUN = '0123456789ab'
const A = 'AAAAAAAAAAAAAAAAAAAAAA'
const B = 'BBBBBBBBBBBBBBBBBBBBBB'
const SHA = 'e'.repeat(64)
const TREE = 'b'.repeat(40)
const KEY = { topic: 'main', topicId: A, partition: 0 }
const slotId = (prefix, n) => `${prefix}${String(n).padStart(2, '0')}`
const kindOf = (kind) => matrix.clientKinds[kind]

/** @returns {object} a valid config: 12 subjects, the isolated seed, observers and one churn extra */
const buildConfig = () => {
  const processes = CLIENT_KINDS.map((_, i) => ({
    slot: slotId('p', i + 1),
    role: 'subject',
    arm: 'unseeded',
    controlId: null,
  }))
  processes.push(
    { slot: 'p13', role: 'seed', arm: 'interning-seeded', controlId: 'interning-seeded' },
    { slot: 'p14', role: 'reader', arm: 'observer', controlId: null },
    { slot: 'p15', role: 'collector', arm: 'observer', controlId: null },
    { slot: 'p16', role: 'decoder', arm: 'observer', controlId: null }
  )
  const clients = CLIENT_KINDS.map((kind, i) => ({
    slot: slotId('c', i + 1),
    process: slotId('p', i + 1),
    clientKind: kind,
    purpose: 'subject',
    arm: 'unseeded',
    controlId: null,
    group: kindOf(kind).grouped ? `g-${RUN}-${slotId('c', i + 1)}` : null,
    groupInstanceId: kindOf(kind).static ? `static-${i + 1}` : null,
    standing: true,
    telemetryEnabled: kindOf(kind).telemetry,
  }))
  clients.push(
    {
      slot: 'c13',
      process: 'p13',
      clientKind: 'classic-eager-dynamic',
      purpose: 'control',
      arm: 'interning-seeded',
      controlId: 'interning-seeded',
      group: `g-${RUN}-c13`,
      groupInstanceId: null,
      standing: true,
      telemetryEnabled: false,
    },
    {
      slot: 'c14',
      process: 'p14',
      clientKind: 'admin',
      purpose: 'observer',
      arm: 'observer',
      controlId: null,
      group: null,
      groupInstanceId: null,
      standing: true,
      telemetryEnabled: false,
    },
    {
      slot: 'c15',
      process: 'p02',
      clientKind: 'classic-eager-static',
      purpose: 'churn',
      arm: 'unseeded',
      controlId: null,
      group: `g-${RUN}-c02`,
      groupInstanceId: 'static-extra',
      standing: false,
      telemetryEnabled: false,
    }
  )
  return { schema: 3, mode: 'smoke', runId: RUN, processes, clients }
}

const BASE_CONFIG = buildConfig()
const slotOf = (slot) => BASE_CONFIG.clients.find((c) => c.slot === slot)
const groupOf = (slot) => slotOf(slot).group

const EAGER = 'c01.0'
const SHARE = 'c07.0'
const PRODUCER = 'c08.0'
const TXN = 'c09.0'
const ADMIN = 'c10.0'
const TELEMETRY = 'c11.0'
const SEED = 'c13.0'
const READER = 'c14.0'

const authority = (client) => ({
  client,
  run: 'r1',
  member: 'm1',
  epoch: 1,
  token: 1,
  assignmentEpoch: 1,
  acquiredAt: 1,
  recreatedAt: 0,
})

/** @returns {object} a valid process start record */
const processStart = (p, i) => ({
  kind: 'process',
  event: 'start',
  source: p.slot,
  process: p.slot,
  pid: 100 + i,
  parent: 'outer',
  arm: p.arm,
  controlId: p.controlId,
  execArgv: ['--expose-gc', '--no-flush-bytecode'],
  nodeOptions: [],
  node: 'v26.7.0',
  v8: '14.6.202',
  bootId: 'boot-1',
})

/** @returns {object} a client construct record matching its config slot */
const construct = (slot, incarnation = 0) => {
  const c = slotOf(slot)
  return {
    kind: 'client',
    event: 'construct',
    source: c.process,
    client: `${slot}.${incarnation}`,
    slot,
    clientKind: c.clientKind,
    process: c.process,
    arm: c.arm,
    controlId: c.controlId,
    purpose: c.purpose,
    group: c.group,
    groupInstanceId: c.groupInstanceId,
    standing: c.standing,
    telemetryEnabled: c.telemetryEnabled,
    configSha256: SHA,
  }
}

const call = (event, source, client, phase, id) => ({
  kind: 'client',
  event,
  source,
  client,
  phase,
  call: id,
  result: phase === 'end' ? 'ok' : null,
  errorType: null,
})

/** One valid entry per inventory section, owned by `client`. */
const ENTRIES = {
  queues: (client) => [
    { queue: `q-${client}`, client, connection: 'n1', scope: 'run-1', pending: 0, inflight: 1 },
  ],
  requests: () => [
    {
      request: 'rq1',
      queue: 'q-c01.0',
      apiKey: 1,
      phase: 'inflight',
      scope: 'run-1',
      beganAt: 5,
      deadlineAt: 35,
    },
  ],
  sockets: (client) => [
    { socket: 's1', client, scope: 'run-1', node: 'b1', connection: 'n1', destroyed: false },
  ],
  listeners: (client) => [
    {
      emitter: 'e1',
      client,
      scope: 'run-1',
      event: 'request',
      registrations: [{ listener: 'l1', scope: 'run-1' }],
    },
    { emitter: 'e2', client: null, scope: 'harness-1', event: null, registrations: [] },
  ],
  resources: (client) => [{ resource: 'h1', type: 'Timeout', scope: 'run-1', client }],
  scopes: () => [
    { scope: 'run-1', owner: 'r1', type: 'run', live: true, retiredAt: null },
    { scope: 'req-9', owner: 'rq9', type: 'request', live: false, retiredAt: 7 },
  ],
  subscription: (client) => [
    {
      client,
      run: 'r1',
      revision: 1,
      accepted: [{ topic: 'main', topicId: A }],
      desired: [{ topic: 'main', topicId: A }],
      observed: [{ topic: 'regex-1', topicId: null }],
    },
  ],
  'pause-state': (client) => [
    {
      client,
      run: 'r1',
      stateId: 'w1',
      topic: 'main',
      originTopicId: A,
      subscriptionLife: `p01.20/${A}`,
      pauseAll: false,
      paused: [],
      resumed: [0],
      lastOperation: 'o2',
    },
  ],
  assignment: (client) => [{ client, key: KEY, authority: authority(client) }],
  'membership-state': (client) => [
    { client, run: 'r1', token: 1, current: true, holders: 0, refused: false },
  ],
  'operation-cell': () => [{ ledger: 'L1', topic: 'main', partition: 0, open: 1, tickets: ['t1'] }],
  'reset-debt': (client) => [{ client, topic: 'main', topicId: A, discharged: [0] }],
  'other-table': (client) => [
    { client, table: 'preferred-replicas', keys: [{ topic: 'main', topicId: A, partition: 0 }] },
  ],
  'share-state': (client) => [
    {
      client,
      run: `${client}.r1`,
      sessions: [{ node: 1, session: 3 }],
      records: [{ acquisition: 'a1', key: KEY, offset: '0', state: 'held' }],
      ranges: [{ acquisition: 'a1', key: KEY, from: '0', to: '0' }],
      cohorts: ['k1'],
    },
  ],
}
const PROCESS_SECTIONS = matrix.recordSections.process
const ORDINARY_SECTIONS = matrix.recordSections.ordinary

/** @returns {object[]} one single-chunk inventory record per section and owner */
const cut = (inventory, process, owners) =>
  owners.flatMap(([client, sections]) =>
    sections.map((section) => ({
      kind: 'inventory',
      source: process,
      inventory,
      process,
      client,
      section,
      chunk: 0,
      chunks: 1,
      entries: ENTRIES[section](client ?? 'c01.0'),
    }))
  )

const GC = {
  id: 'gc-1',
  firstBeginAt: 11,
  firstEndAt: 12,
  secondBeginAt: 113,
  secondEndAt: 114,
  forcedCollections: 2,
  cumulativeForced: 2,
}

const sample = (process, role, inventory, clients, fields = {}) => ({
  kind: 'sample',
  source: process,
  process,
  role,
  slot: role === 'periodic' ? 0 : null,
  boundary: role === 'quiet' || role === 'closed' ? 'b0' : null,
  reading: role === 'quiet' ? 0 : null,
  admittedAt: 10,
  completedAt: 115,
  rss: 50000000,
  heapUsed: 9000000,
  external: 100000,
  activeResources: [
    { type: 'TCPSocketWrap', count: 3 },
    { type: 'Timeout', count: 2 },
  ],
  gc: { ...GC },
  inventory,
  clients,
  ...fields,
})

const identity = (stage, source, fields) => ({ kind: 'identity', stage, source, ...fields })

/** @returns {object[]} a valid stream holding every record kind and variant */
const baseRecords = () => {
  const records = BASE_CONFIG.processes.map(processStart)
  for (const c of BASE_CONFIG.clients) {
    if (c.purpose !== 'churn') records.push(construct(c.slot))
  }
  const G1 = groupOf('c01')
  const G7 = groupOf('c07')
  records.push(
    construct('c15'),
    call('connect', 'p01', EAGER, 'begin', 'k1'),
    call('connect', 'p01', EAGER, 'end', 'k1'),
    call('run', 'p01', EAGER, 'begin', 'k1r'),
    call('run', 'p01', EAGER, 'end', 'k1r'),
    {
      kind: 'shape',
      phase: 'start',
      source: 'p15',
      execution: 'x1',
      shape: 'regex-churn',
      participants: [EAGER],
      operationCount: 0,
    },
    {
      kind: 'topic',
      source: 'p10',
      client: ADMIN,
      call: 'k2',
      op: 'create',
      topic: 'regex-1',
      topicId: B,
      partitions: [0, 1],
      errorCode: 0,
    },
    {
      kind: 'operation',
      phase: 'begin',
      source: 'p01',
      execution: 'x1',
      operation: 'o1',
      op: 'subscribe',
      client: EAGER,
      keys: [],
      call: 'k3',
      result: null,
      errorType: null,
      causes: [],
    },
    {
      kind: 'operation',
      phase: 'end',
      source: 'p01',
      execution: 'x1',
      operation: 'o1',
      op: 'subscribe',
      client: EAGER,
      keys: [],
      call: 'k3',
      result: 'ok',
      errorType: null,
      causes: ['p10.0'],
    },
    {
      kind: 'subscription',
      source: 'p01',
      client: EAGER,
      revision: 1,
      run: 'r1',
      names: [{ topic: 'regex-1', topicId: B }],
      mode: 'client-pattern',
      pattern: '^regex-',
      cause: 'o1',
    },
    {
      kind: 'shape',
      phase: 'end',
      source: 'p15',
      execution: 'x1',
      shape: 'regex-churn',
      participants: [EAGER],
      operationCount: 1,
    },
    {
      kind: 'route',
      event: 'start',
      source: 'p15',
      group: G1,
      topic: 'main',
      topicId: A,
      partition: 0,
      from: '0',
      to: null,
      topicRecord: 'p10.0',
      scan: null,
    },
    identity('offered', 'p08', {
      client: PRODUCER,
      id: 'p08.1',
      key: KEY,
      payloadSha256: SHA,
      bytes: 256,
      transaction: null,
    }),
    identity('ack', 'p08', {
      client: PRODUCER,
      id: 'p08.1',
      outcome: 'acknowledged',
      key: KEY,
      offset: '0',
      errorType: null,
    }),
    identity('transaction', 'p09', {
      client: TXN,
      transaction: 'tx1',
      decision: 'abort',
      outcome: 'aborted',
      call: 'k4',
      errorType: null,
    }),
    {
      kind: 'scan',
      source: 'p14',
      scan: 's1',
      key: KEY,
      from: '0',
      to: '1',
      lso: '1',
      highWatermark: '1',
      rawFile: 'raw/scan-1.bin',
      rawSha256: SHA,
      records: 1,
    },
    identity('log', 'p14', {
      reader: READER,
      scan: 's1',
      id: 'p08.1',
      key: KEY,
      offset: '0',
      payloadSha256: SHA,
      transaction: null,
    }),
    {
      kind: 'boundary',
      source: 'p01',
      client: EAGER,
      group: G1,
      run: 'r1',
      member: 'm1',
      epoch: 1,
      protocol: 'eager',
      event: 'acquire',
      key: KEY,
      authority: authority(EAGER),
      position: '0',
      cause: 'o1',
    },
    {
      kind: 'round',
      source: 'p01',
      client: EAGER,
      group: G1,
      run: 'r1',
      round: 'rd1',
      protocol: 'eager',
      epoch: 1,
      assigned: [KEY],
      revoked: [],
      causes: [],
    },
    identity('handler-enter', 'p01', {
      client: EAGER,
      group: G1,
      handler: 'h1',
      id: 'p08.1',
      key: KEY,
      offset: '0',
      payloadSha256: SHA,
      authority: authority(EAGER),
      acquisition: null,
      deliveryCount: null,
    }),
    identity('handler-end', 'p01', { handler: 'h1', outcome: 'processed', errorType: null }),
    ...['begin', 'write', 'end', 'publish'].map((phase) =>
      identity('commit', 'p01', {
        client: EAGER,
        group: G1,
        commit: 'cm1',
        key: KEY,
        next: '1',
        authority: authority(EAGER),
        phase,
        outcome: phase === 'end' ? 'ok' : null,
        errorType: null,
      })
    ),
    identity('broker-commit', 'p10', {
      client: ADMIN,
      group: G1,
      commit: 'cm1',
      key: KEY,
      next: '1',
      errorCode: 0,
    }),
    {
      kind: 'replay',
      source: 'p01',
      replay: 'rp1',
      group: G1,
      key: KEY,
      from: '0',
      to: '1',
      reason: 'rebalance',
      oldAuthority: authority(EAGER),
      newAuthority: { ...authority(EAGER), token: 2 },
      cause: 'p01.30',
    },
    {
      kind: 'share-acquire',
      source: 'p07',
      acquisition: 'a1',
      client: SHARE,
      group: G7,
      key: KEY,
      from: '0',
      to: '0',
      deliveryCount: 1,
      receivedAt: 5,
      lockMs: 30000,
      wireFile: 'wire/share-1.bin',
      wireSha256: SHA,
    },
    identity('handler-enter', 'p07', {
      client: SHARE,
      group: G7,
      handler: 'h2',
      id: 'p08.1',
      key: KEY,
      offset: '0',
      payloadSha256: SHA,
      authority: null,
      acquisition: 'a1',
      deliveryCount: 1,
    }),
    ...['request', 'response'].map((phase) => ({
      kind: 'share-settle',
      source: 'p07',
      acquisition: 'a1',
      client: SHARE,
      group: G7,
      key: KEY,
      offset: '0',
      type: 'accept',
      phase,
      errorCode: phase === 'response' ? 0 : null,
      wireFile: 'wire/share-1.bin',
      wireSha256: SHA,
    })),
    identity('share-outcome', 'p07', {
      client: SHARE,
      group: G7,
      key: KEY,
      from: '0',
      to: '0',
      type: 'accept',
      reason: 'session-lost',
      errorType: 'KafkaJSShareAcknowledgementError',
    }),
    {
      kind: 'telemetry',
      source: 'p16',
      client: TELEMETRY,
      instanceId: 'inst-1',
      subscriptionId: 'sub-1',
      broker: 1,
      metrics: 12,
      terminating: false,
      rawFile: 'telemetry/raw-1.bin',
      rawSha256: SHA,
      decodedFile: 'telemetry/decoded-1.json',
      decodedSha256: SHA,
    },
    ...['start', 'clear'].map((event) => ({
      kind: 'seed',
      event,
      source: 'p13',
      process: 'p13',
      client: SEED,
      controlId: 'interning-seeded',
      mapEntries: event === 'start' ? 0 : 5,
      cause: 'q1',
    })),
    {
      kind: 'operation',
      phase: 'begin',
      source: 'p15',
      operation: 'o9',
      op: 'broker-stop',
      client: null,
      keys: [],
      call: 'docker-1',
      result: null,
      errorType: null,
      causes: [],
    },
    {
      kind: 'broker-state',
      source: 'p15',
      node: 1,
      pid: 4242,
      container: 'kafka-1',
      event: 'ready',
      members: [1, 2, 3],
      partitions: 1,
      cause: 'o9',
      downMs: 2000,
      processAt: 12,
      processTime: '2026-11-18T16:00:00.012345678Z',
    },
    { kind: 'partition-state', source: 'p15', key: KEY, leader: 1, isr: [1, 2, 3] },
    {
      kind: 'wave',
      source: 'p01',
      event: 'transfer',
      client: EAGER,
      member: 'c15.0',
      paused: 1,
      held: 1,
      observed: true,
    },
    { kind: 'wave-key', source: 'p01', key: KEY, held: true },
    ...['begin', 'end'].map((phase) => ({
      kind: 'ticket',
      source: 'p01',
      ticket: 't1',
      ledger: 'L1',
      client: EAGER,
      ownerRun: 'r1',
      phase,
      keys: [KEY],
      token: 1,
    })),
    {
      kind: 'ticket',
      source: 'p10',
      ticket: 't2',
      ledger: 'L2',
      client: ADMIN,
      ownerRun: null,
      phase: 'begin',
      keys: [],
      token: null,
    },
    { kind: 'diagnostic', source: 'p15', message: 'note', detail: { free: [1, 'two'] } },
    {
      kind: 'route',
      event: 'seal',
      source: 'p15',
      group: G1,
      topic: 'main',
      topicId: A,
      partition: 0,
      from: '0',
      to: '1',
      topicRecord: 'p10.0',
      scan: 's1',
    },
    ...cut('i1', 'p01', [
      [null, PROCESS_SECTIONS],
      [EAGER, ORDINARY_SECTIONS],
    ]),
    sample('p01', 'periodic', 'i1', [EAGER]),
    ...cut('i7', 'p07', [
      [null, PROCESS_SECTIONS],
      [SHARE, ['share-state']],
    ]),
    sample('p07', 'quiet', 'i7', [SHARE]),
    ...cut('i8', 'p08', [[null, PROCESS_SECTIONS]]),
    sample('p08', 'baseline', 'i8', []),
    call('stop', 'p01', EAGER, 'begin', 'k5'),
    call('stop', 'p01', EAGER, 'end', 'k5'),
    call('disconnect', 'p01', EAGER, 'begin', 'k6'),
    call('disconnect', 'p01', EAGER, 'end', 'k6'),
    {
      kind: 'process',
      event: 'exit',
      source: 'p15',
      process: 'p01',
      code: 0,
      signal: null,
      spawnRecord: 'p01.0',
    }
  )
  return records
}

/**
 * Adds envelopes: per-source gap-free seq, `recordId`, `t` one per line and `at` = `t` unless the
 * record sets its own, except that all chunks of one inventory cut share the first chunk's `at`. A
 * `wave-key` or `partition-state` without a `cause` cites its source's latest `wave` or
 * `broker-state`.
 * @returns {string[]} the stream lines
 */
const encode = (records) => {
  const seqs = new Map()
  const cutAt = new Map()
  const summaries = new Map()
  return records.map((record, i) => {
    const seq = seqs.has(record.source) ? seqs.get(record.source) + 1 : 0
    seqs.set(record.source, seq)
    const t = i + 1
    let at = record.at ?? t
    if (record.kind === 'inventory') {
      if (!cutAt.has(record.inventory)) cutAt.set(record.inventory, t)
      at = cutAt.get(record.inventory)
    }
    const recordId = `${record.source}.${seq}`
    if (['wave', 'broker-state'].includes(record.kind)) summaries.set(record.source, recordId)
    const cause = ['wave-key', 'partition-state'].includes(record.kind)
      ? { cause: record.cause ?? summaries.get(record.source) }
      : {}
    return JSON.stringify({
      execution: null,
      ...record,
      ...cause,
      recordId,
      seq,
      at,
      t,
    })
  })
}

/** @returns {object} a valid header for the lines, in the mode */
const buildHeader = (lines, mode = 'smoke') => {
  const text = lines.map((line) => `${line}\n`).join('')
  const stream = {
    file: 'stream.ndjson',
    bytes: Buffer.byteLength(text),
    records: lines.length,
    sha256: sha256(text),
  }
  const files = [
    'config.json',
    ...matrix.acceptance.archive.map((f) => `archive/${f}`),
    'broker/provenance.json',
    'broker/capabilities.json',
    'broker/compose.yml',
    'companions/natural-exit.json',
  ].map((file) => ({ file, bytes: 1, sha256: SHA }))
  files.push({ file: stream.file, bytes: stream.bytes, sha256: stream.sha256 })
  const requestedMs = matrix.acceptance.modes[mode].requestedMs.value
  return {
    schema: 3,
    mode,
    runId: RUN,
    treeSha: TREE,
    node: 'v26.7.0',
    v8: '14.6.202',
    platform: 'linux',
    files,
    clock: { kind: 'linux-monotonic', originMs: 0, bootId: 'boot-1' },
    window: {
      runStartAt: 1000,
      runEndAt: 1000 + requestedMs,
      observedEndAt: 2000 + requestedMs,
      requestedMs,
    },
    host: {
      cpus: 12,
      totalMemoryBytes: 33472352256,
      availableMemoryBytes: 20000000000,
      freeDiskBytes: 300000000000,
      loadAverage1m: 0.5,
      docker: '27.3.1',
      measuredAt: 500,
      pausedWork: true,
    },
    broker: {
      provenanceKind: 'image',
      provenanceFile: 'broker/provenance.json',
      capabilitiesFile: 'broker/capabilities.json',
      composeFile: 'broker/compose.yml',
    },
    exit: { supervisorProcess: 'p15', code: 0, signal: null, observedAt: 3000 + requestedMs },
    stream,
    companions: [
      {
        name: 'natural-exit',
        treeSha: TREE,
        file: 'companions/natural-exit.json',
        sha256: SHA,
        runUrl: 'https://github.com/example/kafkajs/actions/runs/1',
      },
    ],
  }
}

/**
 * Validates an artifact built from the base fixture after `edit`, with the stream digest, counts
 * and manifest recomputed so a mutation fails its own check, not an incidental hash.
 * @returns {string[]}
 */
const run = ({
  edit = () => {},
  editConfig = () => {},
  editHeader = () => {},
  mode = 'smoke',
} = {}) => {
  const records = baseRecords()
  edit(records)
  const config = buildConfig()
  config.mode = mode
  editConfig(config)
  const lines = encode(records)
  const header = buildHeader(lines, mode)
  editHeader(header)
  return validateAll({ matrix, config, header, lines })
}

/**
 * Validates matrix, config and header, then streams the lines: the order a driver or verifier
 * follows, since the stream validator needs all three valid.
 * @returns {string[]}
 */
function validateAll({ matrix, config, header, lines }) {
  const matrixProblems = validateMatrix(matrix)
  if (matrixProblems.length > 0) return matrixProblems
  const problems = [...validateConfig(config, matrix), ...validateHeader(header, matrix)]
  if (problems.length > 0) return problems
  const stream = new StreamValidator({ matrix, config, header })
  lines.forEach((line) => stream.push(line))
  return stream.finish()
}

/** @param {string} ref a dotted matrix path @returns {any} the value it names */
const resolve = (ref) => ref.split('.').reduce((v, k) => v[k], matrix)
/** @param {{ of: Array<number|string> }} minimum @returns {number} its derived value */
const minimumOf = (minimum) =>
  minimum.of.reduce((n, f) => {
    const v = typeof f === 'number' ? f : resolve(f)
    return n * (typeof v === 'number' ? v : v.value)
  }, 1)

const at = (records, predicate) => records.findIndex(predicate)

/** A broker-commit the observer process writes about the churn client `c15.0` of process p02. */
const observed = () => ({
  kind: 'identity',
  stage: 'broker-commit',
  source: 'p10',
  client: 'c15.0',
  group: groupOf('c15'),
  commit: null,
  key: KEY,
  next: null,
  errorCode: 0,
})

/** A call of `c15.0` from its own process p02. */
const ownCall = { ...call('connect', 'p02', 'c15.0', 'begin', 'k15') }

/** Moves the construction of `c15.0` to just after the first record matching `predicate`. */
const moveConstructAfter = (records, predicate) => {
  const [construction] = records.splice(
    at(records, (x) => x.event === 'construct' && x.slot === 'c15'),
    1
  )
  records.splice(at(records, predicate) + 1, 0, construction)
}

/** @returns {string[]} the problems of the records' stream under a validator with `options` */
const validateWith = (records, options) => {
  const lines = encode(records)
  const header = buildHeader(lines)
  const stream = new StreamValidator({ matrix, config: buildConfig(), header, ...options })
  lines.forEach((line) => stream.push(line))
  return stream.finish()
}
const isSample = (process) => (r) => r.kind === 'sample' && r.process === process

/**
 * Runs named cases and returns, for each, whether its problems include the expected text, so a
 * failure names the case.
 * @param {Record<string, [() => string[], string]>} cases name → [problems, expected text]
 */
const expectCases = (cases) => {
  const results = Object.fromEntries(
    Object.entries(cases).map(([name, [problems, expected]]) => {
      const found = problems()
      expect(Array.isArray(found)).toBe(true)
      return [name, found.some((p) => typeof p === 'string' && p.includes(expected)) || found]
    })
  )
  expect(results).toEqual(Object.fromEntries(Object.keys(cases).map((name) => [name, true])))
}

/** @returns {Array<{ source: string, value: unknown }>} every sourced setting or bound */
const sourced = (value) => {
  if (Array.isArray(value)) return value.flatMap(sourced)
  if (value === null || typeof value !== 'object') return []
  const own = typeof value.source === 'string' ? [{ source: value.source, value: value.value }] : []
  return [...own, ...Object.values(value).flatMap(sourced)]
}

describe('scripts/soak/finalSoakSchema', () => {
  it('every registry row has workload, observations, bound source and predicate', () => {
    expect(validateMatrix(matrix)).toEqual([])
    expect(matrix.rows.map((row) => row.id)).toEqual(ROWS)
    for (const row of matrix.rows) {
      expect(row.scope.kinds.length).toBeGreaterThan(0)
      expect(row.scope.shapes.length).toBeGreaterThan(0)
      expect(row.evidence.length).toBeGreaterThan(0)
      expect(row.predicate.length).toBeGreaterThan(40)
      expect(row.bounds.map((ref) => resolve(ref).source.length > 0)).not.toContain(false)
      for (const name of row.uses) expect(matrix.predicates[name].rules.length).toBeGreaterThan(0)
    }
    expect(
      Object.fromEntries(matrix.rows.map((row) => [row.id, row.minima.map(minimumOf)]))
    ).toEqual({
      'rs-p10': [24, 96, 48, 96, 288, 12, 24, 46096, 936, 10000],
      'rs-t11': [72, 864, 1728],
      'rs-g07': [46096, 936, 672],
      'rs-l14': [72, 24, 96, 3],
      'rs-l15': [80000, 80000],
      'rs-l16': [1000000, 72],
      'rs-l22': [384, 288, 1728, 288],
      'rs-l24': [96, 192, 144],
    })
    const rulesOf = (name) => matrix.predicates[name].rules.join(' ')
    const anchors = {
      Complete: ['every idle window', 'every broker recovery', 'startup proof', 'accepts it'],
      Delivery: ['intermediate', 'readback', 'processedPrefix', 'terminal scan'],
      AuthorityOK: [
        'half-open',
        'revoke-begin',
        'publication',
        'stale',
        'transmitted before',
        'handler-end with outcome retry',
        '[last confirmed broker next, old processed next)',
        'strictly greater delivery count',
      ],
      ResourcesOK: ['socket grace', 'standing peers', 'preconstruction baseline'],
      HeapOK: ['upper middle', 'least-squares'],
      TablesOK: ['unassignment', 'relabelled', 'pause intent'],
    }
    for (const [name, phrases] of Object.entries(anchors)) {
      for (const phrase of phrases)
        expect([name, rulesOf(name).includes(phrase)]).toEqual([name, true])
    }
    const recovery = matrix.predicates.Complete.recovery
    expect(Object.keys(recovery)).toEqual(CLIENT_KINDS)
    const tables = matrix.predicates.TablesOK.tables
    expect(Object.keys(tables).sort()).toEqual(
      [
        'subscription',
        'pause-state',
        'assignment',
        'membership-state',
        'operation-cell',
        'reset-debt',
        'topic-configurations',
        'preferred-replicas',
        'grown-topic-names',
        'queued-seeks',
        'cluster-targets',
        'acquired',
        'halted',
        'share-state',
      ].sort()
    )
    expect(tables['membership-state'].owner).toContain('holders')
    expect(tables['operation-cell'].owner).toContain('multiplicity')
    expect(tables['reset-debt'].owner).toContain('partial')
    const seed = matrix.controls['interning-seeded'].timing
    expect(seed.smoke).toEqual({
      starts: 'after-baseline-before-traffic',
      clears: 'after-only-quiet-triplet-before-disconnect',
      sensitivity: 'report-only',
    })
    expect(seed.acceptance.starts).toBe('after-first-quiet-triplet')
    const sources = sourced(matrix)
    expect(sources.length).toBeGreaterThan(40)
    for (const { source, value } of sources) {
      const symbol = /^symbol: ([\w./-]+)#([\w.]+)$/.exec(source)
      const code = /^code: ([\w./-]+)#(\w+)$/.exec(source)
      if (symbol) {
        const exported = symbol[2]
          .split('.')
          .reduce((v, k) => v?.[k], require(path.join(ROOT, symbol[1])))
        expect([source, typeof exported === 'function' ? 0 : exported]).toEqual([source, value])
      } else if (code) {
        const text = fs.readFileSync(path.join(ROOT, code[1]), 'utf8')
        expect([source, text.includes(code[2])]).toEqual([source, true])
      }
    }
  })

  it('a rehearsal is a third mode of whole day hours, at least three, with the seed timed as on the day and its sensitivity only reported, and a rehearsal stream is structurally valid', () => {
    expect(matrix.acceptance.modes.rehearsal.requestedMs.value).toBe(3 * matrix.schedule.hourMs)
    const seed = matrix.controls['interning-seeded'].timing
    expect(seed.rehearsal).toEqual({ ...seed.acceptance, sensitivity: 'report-only' })
    expect(run({ mode: 'rehearsal' })).toEqual([])
    const lengthOf = (ms) => {
      const copy = JSON.parse(JSON.stringify(matrix))
      copy.acceptance.modes.rehearsal.requestedMs.value = ms
      return validateMatrix(copy)
    }
    const problem =
      'matrix.acceptance.modes.rehearsal.requestedMs is not a whole number of hours, at least three'
    expect(lengthOf(4 * matrix.schedule.hourMs)).toEqual([])
    expect(lengthOf(2 * matrix.schedule.hourMs)).toEqual([problem])
    expect(lengthOf(3.5 * matrix.schedule.hourMs)).toEqual([problem])
  })

  it('a complete fixture with every record kind and variant is structurally valid', () => {
    expect(run()).toEqual([])
    expect(run({ mode: 'acceptance' })).toEqual([])
    const kinds = new Set(baseRecords().map((r) => r.kind))
    expect([...kinds].sort()).toEqual([...RECORD_KINDS].sort())
    const variants = new Set(
      baseRecords().map(
        (r) => `${r.kind}/${r.event ?? r.stage ?? (r.kind === 'shape' ? r.phase : '')}`
      )
    )
    for (const v of [
      'process/exit',
      'client/construct',
      'client/stop',
      'route/seal',
      'seed/clear',
    ]) {
      expect(variants.has(v)).toBe(true)
    }
    for (const stage of [
      'offered',
      'ack',
      'transaction',
      'log',
      'handler-enter',
      'handler-end',
      'commit',
      'broker-commit',
      'share-outcome',
    ]) {
      expect(variants.has(`identity/${stage}`)).toBe(true)
    }
  })

  it('the schema rejects an artifact without broker provenance', () => {
    expectCases({
      'broker block deleted': [
        () => run({ editHeader: (h) => delete h.broker }),
        'header.broker is missing',
      ],
      'provenance file not in the manifest': [
        () =>
          run({
            editHeader: (h) =>
              (h.files = h.files.filter((f) => f.file !== 'broker/provenance.json')),
          }),
        'does not bind the broker provenance',
      ],
      'unknown provenance kind': [
        () => run({ editHeader: (h) => (h.broker.provenanceKind = 'apiversions') }),
        'header.broker is not a broker provenance',
      ],
      'wire capabilities offered as provenance': [
        () => validateProvenance({ nodes: [{ node: 1, apiRanges: [] }] }, 'image'),
        'provenance.nodes is not',
      ],
      'empty provenance': [
        () => validateProvenance({ nodes: [] }, 'image'),
        'provenance.nodes is empty',
      ],
    })
  })

  it('the schema rejects a stream whose digest does not match', () => {
    const lines = encode(baseRecords())
    const header = buildHeader(lines)
    const config = buildConfig()
    const tampered = [...lines]
    tampered[3] = tampered[3].replace('"pid":103', '"pid":104')
    expect(tampered[3]).not.toEqual(lines[3])
    expectCases({
      'one line changed after the header': [
        () => validateAll({ matrix, config, header, lines: tampered }),
        'the stream digest does not match the header',
      ],
      'header digest replaced in both places': [
        () =>
          run({
            editHeader: (h) => {
              h.stream.sha256 = SHA
              h.files.find((f) => f.file === 'stream.ndjson').sha256 = SHA
            },
          }),
        'the stream digest does not match the header',
      ],
      'header digest differs from its manifest entry': [
        () => run({ editHeader: (h) => (h.stream.sha256 = SHA) }),
        'header.stream differs from its manifest entry',
      ],
      'a line dropped': [
        () => validateAll({ matrix, config, header, lines: lines.slice(0, -1) }),
        'the stream record count differs from the header',
      ],
    })
  })

  it('the schema distinguishes smoke from acceptance mode', () => {
    expect(run({ mode: 'smoke' })).toEqual([])
    expect(run({ mode: 'acceptance' })).toEqual([])
    const smokeMs = matrix.acceptance.modes.smoke.requestedMs.value
    expectCases({
      'smoke window relabelled acceptance': [
        () =>
          run({
            mode: 'acceptance',
            editHeader: (h) => {
              h.window.requestedMs = smokeMs
              h.window.runEndAt = h.window.runStartAt + smokeMs
              h.window.observedEndAt = h.window.runEndAt + 1000
              h.exit.observedAt = h.window.runEndAt + 2000
            },
          }),
        'header.window.requestedMs is not the acceptance length',
      ],
      'run window shorter than requested': [
        () => run({ mode: 'acceptance', editHeader: (h) => (h.window.runEndAt -= 600000) }),
        'header.window run length differs from requestedMs',
      ],
      'zero observed window': [
        () => run({ editHeader: (h) => (h.window.observedEndAt = h.window.runStartAt) }),
        'header exits are not observed after the run end',
      ],
      'exit beyond the teardown allowance': [
        () =>
          run({
            mode: 'acceptance',
            editHeader: (h) => (h.exit.observedAt = h.window.runEndAt + 900001),
          }),
        'within the teardown allowance',
      ],
      'unknown mode': [
        () => run({ editHeader: (h) => (h.mode = 'calibration') }),
        'header.mode is not',
      ],
      'acceptance sample without a gc witness': [
        () => run({ mode: 'acceptance', edit: (r) => (r[at(r, isSample('p01'))].gc = null) }),
        'an acceptance sample has no gc witness',
      ],
    })
    expect(run({ mode: 'smoke', edit: (r) => (r[at(r, isSample('p01'))].gc = null) })).toEqual([])
  })

  it('the matrix is rejected when a mandatory section, version, row or control is missing or replaced', () => {
    const edited = (edit) => () => {
      const m = copy(matrix)
      edit(m)
      return validateMatrix(m)
    }
    const cases = {
      'schema 2': [edited((m) => (m.schema = 2)), 'matrix.schema is not 3'],
      'a row dropped': [edited((m) => m.rows.pop()), 'matrix.rows does not hold the 8 rows'],
      'rows reordered': [edited((m) => m.rows.reverse()), 'rows[0].id is not'],
      'controls emptied': [
        edited((m) => (m.controls = {})),
        'matrix.controls is not exactly interning-seeded',
      ],
      'a second control': [
        edited((m) => (m.controls.other = m.controls['interning-seeded'])),
        'matrix.controls is not exactly interning-seeded',
      ],
      'seed not isolated': [
        edited((m) => (m.controls['interning-seeded'].isolated = false)),
        'isolated is not',
      ],
      'seed of a producer kind': [
        edited((m) => (m.controls['interning-seeded'].clientKind = 'producer-idempotent')),
        'clientKind is not an ordinary consumer kind',
      ],
      'bound without source': [
        edited((m) => delete m.bounds['heap-endpoint'].source),
        'matrix.bounds.heap-endpoint is not a bound',
      ],
      'numeric bound left as report-only null without the flag': [
        edited((m) => (m.bounds['heap-endpoint'].value = null)),
        'matrix.bounds.heap-endpoint is not a bound',
      ],
      'a row bound naming nothing': [
        edited((m) => m.rows[0].bounds.push('bounds.missing')),
        'rows[0].bounds is not',
      ],
      'a row bound naming an unsourced value': [
        edited((m) => m.rows[0].bounds.push('schedule.hourMs')),
        'rows[0].bounds is not',
      ],
      'a minimum derived from nothing': [
        edited((m) => (m.rows[0].minima[0].of = ['schedule.totals.none'])),
        'rows[0].minima is not',
      ],
      'a minimum with a stored value instead of a derivation': [
        edited((m) => (m.rows[0].minima[0] = { name: 'idle windows', value: 24 })),
        'rows[0].minima is not',
      ],
      'a predicate definition deleted': [
        edited((m) => delete m.predicates.AuthorityOK),
        'matrix.predicates.AuthorityOK is missing',
      ],
      'a predicate without rules': [
        edited((m) => (m.predicates.Delivery.rules = [])),
        'matrix.predicates.Delivery.rules is not',
      ],
      'an unknown predicate': [
        edited((m) => (m.predicates.Fast = { rules: ['always'] })),
        'matrix.predicates has unknown field Fast',
      ],
      'a row using an undefined predicate': [
        edited((m) => m.rows[0].uses.push('Fast')),
        'rows[0].uses is not',
      ],
      'a row naming a predicate it does not use': [
        edited((m) => (m.rows[2].uses = ['Complete'])),
        'rows[2].uses differs from the predicates its text names',
      ],
      'a row using a predicate its text never names': [
        edited((m) => m.rows[4].uses.push('HeapOK')),
        'rows[4].uses differs from the predicates its text names',
      ],
      'the smoke seed borrows the day start': [
        edited((m) => {
          m.controls['interning-seeded'].timing.smoke.starts = 'after-first-quiet-triplet'
        }),
        'matrix.controls.interning-seeded.timing.smoke.starts is not',
      ],
      'the smoke seed claims sensitivity': [
        edited((m) => (m.controls['interning-seeded'].timing.smoke.sensitivity = 'required')),
        'matrix.controls.interning-seeded.timing.smoke.sensitivity is not',
      ],
      'the day seed takes the smoke start': [
        edited((m) => {
          m.controls['interning-seeded'].timing.acceptance.starts = 'after-baseline-before-traffic'
        }),
        'matrix.controls.interning-seeded.timing.acceptance.starts is not',
      ],
      'a single untimed seed start': [
        edited((m) => (m.controls['interning-seeded'].starts = 'after the first triplet')),
        'matrix.controls.interning-seeded has unknown field starts',
      ],
      'no executions': [
        edited((m) => (m.schedule.executions = [])),
        'matrix.schedule.executions is not one execution of each auxiliary shape',
      ],
      'an object-valued execution length': [
        edited((m) => (m.schedule.executions[0].maxMs = { toString: null })),
        'matrix.schedule.executions[0].maxMs is not',
      ],
      'empty totals': [
        edited((m) => (m.schedule.totals = {})),
        'matrix.schedule.totals.idleWindows is missing',
      ],
      'empty smoke profile': [
        edited((m) => (m.schedule.smoke = {})),
        'matrix.schedule.smoke.activeMs is missing',
      ],
      'archive without the driver': [
        edited((m) => m.acceptance.archive.pop()),
        'matrix.acceptance.archive is not',
      ],
      'source without a kind': [
        edited((m) => (m.sampling.periodMs.source = 'bounds.js')),
        'matrix.sampling.periodMs is not a setting',
      ],
      'a kind removed': [
        edited((m) => delete m.clientKinds.share),
        'matrix.clientKinds is not the twelve kinds',
      ],
      'a composite telemetry kind': [
        edited((m) => (m.clientKinds['telemetry-producer'].role = 'telemetry')),
        'matrix.clientKinds.telemetry-producer.role is not',
      ],
      'an unknown record kind as evidence': [
        edited((m) => m.rows[1].evidence.push('summary')),
        'rows[1].evidence is not',
      ],
      'an unknown shape in scope': [
        edited((m) => m.rows[1].scope.shapes.push('calibration')),
        'rows[1].scope is not',
      ],
      'periodic slots off the grid': [
        edited((m) => (m.sampling.periodicSlots.value = 2880)),
        'periodicSlots does not cover the day',
      ],
      'one forced collection': [
        edited((m) => (m.sampling.gcCollections.value = 1)),
        'gcCollections is not 2',
      ],
      'a decision id in text': [
        edited((m) => (m.rows[0].pass = `see ${['D', '146'].join('')}`)),
        'names a decision id',
      ],
      'an uppercase registry label': [
        edited((m) => (m.rows[0].fail = `see ${['RS', 'P10'].join('-')}`)),
        'uppercase registry or task label',
      ],
      'a local-only path': [
        edited((m) => (m.rows[0].fail = `see ${['', 'var', 'tmp', 'notes'].join('/')}`)),
        'names a local-only path',
      ],
      'an unknown row field': [
        edited((m) => (m.rows[2].summary = 'PASS')),
        'rows[2] has unknown field summary',
      ],
    }
    for (const name of [
      'schema',
      'acceptance',
      'host',
      'traffic',
      'clientKinds',
      'topology',
      'schedule',
      'sampling',
      'controls',
      'recordSections',
      'observations',
      'bounds',
      'predicates',
      'rows',
    ]) {
      cases[`${name} deleted`] = [edited((m) => delete m[name]), `matrix.${name} is missing`]
      if (name !== 'schema') {
        cases[`${name} replaced by a string`] = [
          edited((m) => (m[name] = 'x')),
          `matrix.${name} is not`,
        ]
      }
    }
    for (const keys of [
      ['acceptance'],
      ['acceptance', 'modes'],
      ['acceptance', 'modes', 'acceptance'],
      ['acceptance', 'modes', 'smoke'],
      ['host'],
      ['traffic'],
      ['traffic', 'floors'],
      ['schedule'],
      ['schedule', 'executions', 0],
      ['schedule', 'executions', 1],
      ['schedule', 'executions', 2],
      ['schedule', 'executions', 3],
      ['schedule', 'totals'],
      ['schedule', 'smoke'],
      ['schedule', 'smoke', 'floors'],
      ['sampling'],
      ['controls', 'interning-seeded'],
      ['controls', 'interning-seeded', 'timing'],
      ['controls', 'interning-seeded', 'timing', 'acceptance'],
      ['controls', 'interning-seeded', 'timing', 'smoke'],
      ['predicates'],
      ['predicates', 'Complete'],
      ['predicates', 'Complete', 'recovery'],
      ['predicates', 'TablesOK'],
      ['predicates', 'TablesOK', 'tables'],
    ]) {
      const where = `matrix${keys.map((k) => (typeof k === 'number' ? `[${k}]` : `.${k}`)).join('')}`
      const parent = (m) => keys.reduce((v, k) => v[k], m)
      for (const field of Object.keys(parent(matrix))) {
        cases[`${where}.${field} deleted`] = [
          edited((m) => delete parent(m)[field]),
          `${where}.${field} is missing`,
        ]
        cases[`${where}.${field} replaced by null`] = [
          edited((m) => (parent(m)[field] = null)),
          `${where}.${field} is not`,
        ]
      }
    }
    for (const field of Object.keys(matrix.rows[0])) {
      cases[`row field ${field} deleted`] = [
        edited((m) => delete m.rows[0][field]),
        `rows[0].${field} is missing`,
      ]
    }
    expectCases(cases)
  })

  it('identity and settlement records are rejected without each required stage field', () => {
    const DISCRIMINATORS = {
      process: 'event',
      client: 'event',
      route: 'event',
      seed: 'event',
      identity: 'stage',
      shape: 'phase',
      operation: 'phase',
    }
    const cases = {}
    for (const line of encode(baseRecords())) {
      const record = JSON.parse(line)
      expect(validateRecord(record)).toEqual([])
      const variant = record.event ?? record.stage ?? record.phase ?? ''
      for (const field of Object.keys(record)) {
        if (record.kind === 'diagnostic' && field === 'detail') continue
        const name = `${record.kind}/${variant} without ${field}`
        const removed = { ...record }
        delete removed[field]
        const discriminator = field === 'kind' || field === DISCRIMINATORS[record.kind]
        cases[name] = [
          () => validateRecord(removed),
          discriminator ? 'is not a known' : `.${field} is missing`,
        ]
      }
      if (record.kind !== 'diagnostic') {
        cases[`${record.kind}/${variant} with an unknown field`] = [
          () => validateRecord({ ...record, summary: 1 }),
          'has unknown field summary',
        ]
      }
    }
    expect(Object.keys(cases).length).toBeGreaterThan(400)
    expectCases(cases)
  })

  it('an acknowledged send whose location the broker did not return has no offset', () => {
    const lines = encode(baseRecords()).map((line) => JSON.parse(line))
    const ack = lines.find((r) => r.kind === 'identity' && r.stage === 'ack')
    expect(validateRecord({ ...ack, offset: null })).toEqual([])
    expect(validateRecord({ ...ack, offset: '-1' })).toEqual([
      expect.stringContaining('.offset is not an Offset or null'),
    ])
  })

  it('cross-field rules reject contradictory records', () => {
    const lines = encode(baseRecords()).map((line) => JSON.parse(line))
    const find = (kind, extra = () => true) => ({
      ...lines.find((r) => r.kind === kind && extra(r)),
    })
    const enter = find('identity', (r) => r.stage === 'handler-enter' && r.authority !== null)
    expectCases({
      'an unknown send with an offset': [
        () => validateRecord({ ...find('identity', (r) => r.stage === 'ack'), outcome: 'unknown' }),
        'only an acknowledged send has an offset',
      ],
      'commit decision seen aborted': [
        () =>
          validateRecord({
            ...find('identity', (r) => r.stage === 'transaction'),
            decision: 'commit',
          }),
        'the transaction outcome contradicts its decision',
      ],
      'handler with both authority and acquisition': [
        () => validateRecord({ ...enter, acquisition: 'a1', deliveryCount: 1 }),
        'a handler carries an authority or a share acquisition',
      ],
      'commit begin with an outcome': [
        () =>
          validateRecord({
            ...find('identity', (r) => r.stage === 'commit' && r.phase === 'begin'),
            outcome: 'ok',
          }),
        'only a commit end has an outcome',
      ],
      'scan to above lso': [
        () => validateRecord({ ...find('scan'), to: '2' }),
        'from <= to <= lso <= highWatermark',
      ],
      'empty replay': [
        () => validateRecord({ ...find('replay'), to: '0' }),
        'a replay range is empty',
      ],
      'share request with an error code': [
        () =>
          validateRecord({ ...find('share-settle', (r) => r.phase === 'request'), errorCode: 0 }),
        'only a response has an errorCode',
      ],
      'route seal without scan': [
        () => validateRecord({ ...find('route', (r) => r.event === 'seal'), scan: null }),
        'a route seal needs its end offset and scan',
      ],
      'route ending before it starts': [
        () => validateRecord({ ...find('route', (r) => r.event === 'seal'), from: '5' }),
        'a route ends before it starts',
      ],
      'process without --no-flush-bytecode': [
        () =>
          validateRecord({
            ...find('process', (r) => r.event === 'start'),
            execArgv: ['--expose-gc'],
          }),
        'node flag --no-flush-bytecode is absent',
      ],
      'seeded arm without control id': [
        () => validateRecord({ ...find('process', (r) => r.process === 'p13'), controlId: null }),
        'arm and controlId disagree',
      ],
      'shape start with operations': [
        () => validateRecord({ ...find('shape', (r) => r.phase === 'start'), operationCount: 1 }),
        'operationCount is 0 at start',
      ],
      'client call end without result': [
        () =>
          validateRecord({
            ...find('client', (r) => r.event === 'connect' && r.phase === 'end'),
            result: null,
          }),
        'result is set only at the end of a call',
      ],
      'broker operation naming a client': [
        () =>
          validateRecord({ ...find('operation', (r) => r.op === 'broker-stop'), client: EAGER }),
        'only broker operations have no client',
      ],
      'literal subscription with a pattern': [
        () => validateRecord({ ...find('subscription'), mode: 'literal' }),
        'pattern is set only for a pattern mode',
      ],
      'owned ticket without token': [
        () => validateRecord({ ...find('ticket', (r) => r.ownerRun !== null), token: null }),
        'ownerRun and token go together',
      ],
    })
  })

  it('malformed values return problems instead of throwing', () => {
    const record = (line) => () => validateRecord(JSON.parse(line))
    const log = JSON.parse(encode(baseRecords()).find((l) => l.includes('"stage":"log"')))
    const header = buildHeader(encode(baseRecords()))
    const objectValued = { toString: null }
    const cases = {
      'record null': [() => validateRecord(null), 'record is not an object'],
      'record array': [() => validateRecord([]), 'record is not an object'],
      'object-valued kind': [
        record('{"kind":{"toString":null}}'),
        'record.kind is not a known record kind',
      ],
      'object-valued stage': [
        record('{"kind":"identity","stage":{"toString":null}}'),
        'record.stage is not a known identity variant',
      ],
      'inherited kind toString': [
        record('{"kind":"toString"}'),
        'record.kind is not a known record kind',
      ],
      'inherited kind __proto__': [
        record('{"kind":"__proto__"}'),
        'record.kind is not a known record kind',
      ],
      'inherited stage hasOwnProperty': [
        record('{"kind":"identity","stage":"hasOwnProperty"}'),
        'is not a known identity variant',
      ],
      'object-valued payload hash': [
        () => validateRecord({ ...log, payloadSha256: objectValued }),
        'payloadSha256 is not a sha256',
      ],
      'object-valued offset': [
        () => validateRecord({ ...log, offset: objectValued }),
        'offset is not an Offset',
      ],
      'numeric offset': [() => validateRecord({ ...log, offset: 5 }), 'offset is not an Offset'],
      'negative offset': [
        () => validateRecord({ ...log, offset: '-1' }),
        'offset is not an Offset',
      ],
      'leading-zero offset': [
        () => validateRecord({ ...log, offset: '01' }),
        'offset is not an Offset',
      ],
      'offset above int64': [
        () => validateRecord({ ...log, offset: '9223372036854775808' }),
        'offset is not an Offset',
      ],
      'twenty-digit offset': [
        () => validateRecord({ ...log, offset: '10000000000000000000' }),
        'offset is not an Offset',
      ],
      'unsafe integer seq': [() => validateRecord({ ...log, seq: 2 ** 53 }), 'seq is not a Count'],
      'object-valued role': [
        () =>
          validateRecord({
            ...JSON.parse(encode(baseRecords()).find((l) => l.includes('"kind":"sample"'))),
            role: objectValued,
          }),
        'role is not one of',
      ],
      'object-valued arm': [
        () => validateRecord({ ...JSON.parse(encode(baseRecords())[0]), arm: objectValued }),
        'arm is not one of',
      ],
      'prototype key as a field': [
        () => validateRecord(JSON.parse(JSON.stringify(log).replace('{', '{"__proto__":{"x":1},'))),
        'has unknown field __proto__',
      ],
      'matrix null': [() => validateMatrix(null), 'matrix is not an object'],
      'object-valued rows': [
        () => validateMatrix({ ...copy(matrix), rows: objectValued }),
        'matrix.rows is not a list',
      ],
      'object-valued row id': [
        () => {
          const m = copy(matrix)
          m.rows[3].id = objectValued
          return validateMatrix(m)
        },
        'rows[3].id is not',
      ],
      'inherited client kind key': [
        () =>
          validateMatrix(
            JSON.parse(
              JSON.stringify(matrix).replace('"clientKinds":{', '"clientKinds":{"__proto__":{},')
            )
          ),
        'matrix.clientKinds is not the twelve kinds',
      ],
      'null seeded client kind': [
        () => {
          const m = copy(matrix)
          m.clientKinds['classic-eager-dynamic'] = null
          return validateMatrix(m)
        },
        'matrix.clientKinds.classic-eager-dynamic is not an object',
      ],
      'object-valued execution shape': [
        () => {
          const m = copy(matrix)
          m.schedule.executions[1].shape = objectValued
          return validateMatrix(m)
        },
        'matrix.schedule.executions[1].shape is not',
      ],
      'object-valued bound reference': [
        () => {
          const m = copy(matrix)
          m.rows[0].bounds = [objectValued]
          return validateMatrix(m)
        },
        'rows[0].bounds is not',
      ],
      'object-valued minimum factor': [
        () => {
          const m = copy(matrix)
          m.rows[0].minima[0].of = [objectValued]
          return validateMatrix(m)
        },
        'rows[0].minima is not',
      ],
      'object-valued predicate use': [
        () => {
          const m = copy(matrix)
          m.rows[0].uses = [objectValued]
          return validateMatrix(m)
        },
        'rows[0].uses is not',
      ],
      'null predicate definition': [
        () => {
          const m = copy(matrix)
          m.predicates.Complete = null
          return validateMatrix(m)
        },
        'matrix.predicates.Complete is not an object',
      ],
      'header null': [() => validateHeader(null, matrix), 'header is not an object'],
      'object-valued header stream digest': [
        () =>
          validateHeader({ ...header, stream: { ...header.stream, sha256: objectValued } }, matrix),
        'header.stream is not a stream digest',
      ],
      'object-valued manifest digest': [
        () =>
          validateHeader(
            { ...header, files: [{ ...header.files[0], sha256: objectValued }] },
            matrix
          ),
        'header.files is not',
      ],
      'object-valued header mode': [
        () => validateHeader({ ...header, mode: objectValued }, matrix),
        'header.mode is not',
      ],
      'config null': [() => validateConfig(null, matrix), 'config is not an object'],
      'object-valued config processes': [
        () => validateConfig({ ...buildConfig(), processes: objectValued }, matrix),
        'config.processes is not a list',
      ],
      'object-valued config runId': [
        () => validateConfig({ ...buildConfig(), runId: objectValued }, matrix),
        'config.runId is not 12 hex digits',
      ],
      'capabilities null': [() => validateCapabilities(null), 'capabilities is not an object'],
      'object-valued provenance kind': [
        () => validateProvenance({ nodes: [] }, objectValued),
        'provenance kind is not',
      ],
    }
    const streamOf = (lines) => () => {
      const stream = new StreamValidator({ matrix, config: buildConfig(), header: buildHeader([]) })
      lines.forEach((line) => stream.push(line))
      return stream.finish()
    }
    Object.assign(cases, {
      'stream line not JSON': [streamOf(['{']), 'line 1: is not JSON'],
      'stream line not a string': [streamOf([{}]), 'line 1: is not a string'],
      'stream line an array': [streamOf(['[]']), 'line 1: record is not an object'],
      'stream object-valued kind': [
        streamOf(['{"kind":{"toString":null}}']),
        'record.kind is not a known record kind',
      ],
    })
    for (const [problems] of Object.values(cases)) {
      expect(problems).not.toThrow()
    }
    expectCases(cases)
  })

  it('the config binds every client to one process, kind and arm with one isolated seed', () => {
    const config = buildConfig()
    expect(validateConfig(config, matrix)).toEqual([])
    const telemetryProducer = config.clients.find((c) => c.clientKind === 'telemetry-producer')
    expect([telemetryProducer.group, telemetryProducer.telemetryEnabled]).toEqual([null, true])
    const edited = (edit) => () => {
      const c = buildConfig()
      edit(c)
      return validateConfig(c, matrix)
    }
    const client = (c, slot) => c.clients.find((x) => x.slot === slot)
    expectCases({
      'seed process missing': [
        edited((c) => {
          c.processes = c.processes.filter((p) => p.slot !== 'p13')
          c.clients = c.clients.filter((x) => x.slot !== 'c13')
        }),
        'config.processes has 0 seed processes',
      ],
      'seed of the wrong kind': [
        edited((c) => (client(c, 'c13').clientKind = 'consumer-dynamic')),
        'c13 is a control client in a seed process',
      ],
      'two seed clients': [
        edited((c) => c.clients.push({ ...client(c, 'c13'), slot: 'c16', group: `g-${RUN}-c16` })),
        'does not host exactly one control client',
      ],
      'seed colocated with a subject': [
        edited((c) =>
          Object.assign(client(c, 'c13'), { process: 'p01', arm: 'unseeded', controlId: null })
        ),
        'c13 is a control client in a subject process',
      ],
      'subject moved into the seed process': [
        edited((c) =>
          Object.assign(client(c, 'c05'), {
            process: 'p13',
            arm: 'interning-seeded',
            controlId: 'interning-seeded',
          })
        ),
        'c05 is a subject client in a seed process',
      ],
      'seed relabelled unseeded': [
        edited((c) =>
          Object.assign(
            c.processes.find((p) => p.slot === 'p13'),
            { arm: 'unseeded', controlId: null }
          )
        ),
        'config process p13 has the wrong arm for a seed',
      ],
      'observer substituted for a subject': [
        edited((c) => (client(c, 'c10').purpose = 'observer')),
        'c10 is a observer client in a subject process',
      ],
      'a kind with no subject': [
        edited((c) => (c.clients = c.clients.filter((x) => x.slot !== 'c03'))),
        'config has no classic-cooperative-dynamic subject',
      ],
      'synthetic telemetry pair in one process': [
        edited((c) => (client(c, 'c12').process = 'p11')),
        'does not host exactly one subject kind',
      ],
      'telemetry producer given a consumer group': [
        edited((c) => (client(c, 'c11').group = `g-${RUN}-c11`)),
        'c11 has a group its kind does not use',
      ],
      'telemetry disabled on a telemetry kind': [
        edited((c) => (client(c, 'c12').telemetryEnabled = false)),
        'c12 telemetry does not match its kind',
      ],
      'static kind without static id': [
        edited((c) => (client(c, 'c02').groupInstanceId = null)),
        'c02 static identity does not match its kind',
      ],
      'static id shared by two slots': [
        edited((c) => (client(c, 'c15').groupInstanceId = 'static-2')),
        'c15 repeats a static identity',
      ],
      'extra in another kind group': [
        edited((c) => (client(c, 'c15').group = `g-${RUN}-c01`)),
        "c15 group is not its kind's run-scoped group",
      ],
      'extra of a non-churn kind': [
        edited((c) =>
          c.clients.push({ ...client(c, 'c08'), slot: 'c16', purpose: 'churn', standing: false })
        ),
        'c16 is a churn client in a subject process',
      ],
      'stream relabels the seed as unseeded': [
        () =>
          run({
            edit: (r) =>
              Object.assign(
                r.find((x) => x.client === SEED && x.event === 'construct'),
                { arm: 'unseeded', controlId: null }
              ),
          }),
        'client arm differs from its config slot',
      ],
      'seed record from a subject process': [
        () =>
          run({
            edit: (r) =>
              Object.assign(
                r.find((x) => x.kind === 'seed'),
                { source: 'p01', process: 'p01', client: EAGER }
              ),
          }),
        'seed record is not from the isolated seed process and its control client',
      ],
      'telemetry from a client without telemetry': [
        () => run({ edit: (r) => (r.find((x) => x.kind === 'telemetry').client = PRODUCER) }),
        'telemetry from a client without telemetry',
      ],
      'ordinary handler with a share acquisition': [
        () =>
          run({
            edit: (r) =>
              Object.assign(
                r.find((x) => x.stage === 'handler-enter' && x.client === SHARE),
                {
                  client: EAGER,
                  group: groupOf('c01'),
                }
              ),
          }),
        'handler authority does not match the client kind',
      ],
    })
  })

  it('the stream rejects source gaps, reorder, events after exit and times outside the window', () => {
    const lines = encode(baseRecords())
    const config = buildConfig()
    const replace = (i, edit) => {
      const copyLines = [...lines]
      const record = JSON.parse(copyLines[i])
      edit(record)
      copyLines[i] = JSON.stringify(record)
      return copyLines
    }
    const validate = (edited) => () =>
      validateAll({ matrix, config, header: buildHeader(edited), lines: edited })
    const last = lines.length - 1
    const connectBegin = lines.findIndex((l) => l.includes('"call":"k1"'))
    expectCases({
      'a source gap': [
        validate(lines.filter((_, i) => i !== connectBegin)),
        'seq is not the next of its source',
      ],
      'two records of one source swapped': [
        validate([
          ...lines.slice(0, connectBegin),
          lines[connectBegin + 1],
          lines[connectBegin],
          ...lines.slice(connectBegin + 2),
        ]),
        'seq is not the next of its source',
      ],
      'recordId not source.seq': [
        validate(replace(20, (r) => (r.recordId = 'p05.9'))),
        'recordId is not <source>.<seq>',
      ],
      'at after t': [validate(replace(20, (r) => (r.at = r.t + 1))), 'at is after t'],
      't decreasing': [validate(replace(20, (r) => (r.t = 1))), 't decreases'],
      'at decreasing within a source': [
        validate(replace(last - 1, (r) => (r.at = 1))),
        'at decreases within its source',
      ],
      't after the observed end': [
        validate(replace(last, (r) => Object.assign(r, { at: 1e9, t: 1e9 }))),
        't is after the observed end',
      ],
      'a record after its process exited': [
        () => run({ edit: (r) => r.push(call('connect', 'p01', EAGER, 'begin', 'k9')) }),
        'its source process has exited',
      ],
      'a process observing its own exit': [
        () => run({ edit: (r) => (r[r.length - 1].source = 'p01') }),
        'a process cannot observe its own exit',
      ],
      'a process on another boot': [
        () => run({ edit: (r) => (r[2].bootId = 'boot-2') }),
        'process bootId or node differs from the header',
      ],
      'a process on another node version': [
        () => run({ edit: (r) => (r[2].node = 'v24.0.0') }),
        'process bootId or node differs from the header',
      ],
      'an unknown source': [
        () => run({ edit: (r) => (r[20].source = 'p99') }),
        'source is not a config process',
      ],
      'a reused client id': [
        () => run({ edit: (r) => r.splice(40, 0, construct('c01')) }),
        'client id is reused',
      ],
      'a client id off its slot': [
        () =>
          run({
            edit: (r) =>
              (r.find((x) => x.event === 'construct' && x.slot === 'c15').client = 'c16.0'),
          }),
        'client id is not <slot>.<incarnation>',
      ],
      'a client constructed before its process starts': [
        () => run({ edit: (r) => r.unshift(construct('c01', 1)) }),
        'client is constructed outside a live process',
      ],
      'a never-constructed client': [
        () => run({ edit: (r) => (r.find((x) => x.kind === 'topic').client = 'c10.7') }),
        'names a client that was never constructed',
      ],
      "a record of the client's own process before its construction": [
        () =>
          run({
            edit: (r) =>
              r.splice(
                at(r, (x) => x.client === 'c15.0'),
                0,
                ownCall
              ),
          }),
        'names a client before its construction',
      ],
      'a held record whose client turns out to lack what it needs': [
        () =>
          run({
            edit: (r) => {
              r.find((x) => x.kind === 'telemetry').client = 'c15.0'
              moveConstructAfter(r, (x) => x.kind === 'telemetry')
            },
          }),
        'telemetry from a client without telemetry',
      ],
      'more held records than the bound': [
        () => {
          const records = baseRecords()
          moveConstructAfter(records, (x) => x.kind === 'wave')
          records.splice(
            at(records, (x) => x.kind === 'wave'),
            0,
            observed(),
            observed(),
            observed()
          )
          return validateWith(records, { maxHeld: 2 })
        },
        'more than 2 records wait for a client not yet constructed',
      ],
      'a key record citing no wave of its source': [
        () => run({ edit: (r) => (r.find((x) => x.kind === 'wave-key').cause = 'p01.0') }),
        'cites no open wave of its source',
      ],
      'a wave missing a key record': [
        () =>
          run({
            edit: (r) =>
              r.splice(
                at(r, (x) => x.kind === 'wave-key'),
                1
              ),
          }),
        'misses 1 of its wave-key records',
      ],
      'a wave whose two key records name the same key, leaving a paused key unnamed': [
        () =>
          run({
            edit: (r) => {
              const wave = r.find((x) => x.kind === 'wave')
              wave.paused = 2
              wave.held = 2
              const i = at(r, (x) => x.kind === 'wave-key')
              r.splice(i, 0, { ...r[i] })
            },
          }),
        'repeats a key of its wave',
      ],
      'a wave whose held count differs from its key records': [
        () => run({ edit: (r) => (r.find((x) => x.kind === 'wave-key').held = false) }),
        'wave held count differs from its wave-key records',
      ],
      'a key record of another execution than its wave': [
        () => run({ edit: (r) => (r.find((x) => x.kind === 'wave-key').execution = 'x1') }),
        'is of another execution than its wave',
      ],
      'a wave whose member was never constructed': [
        () => run({ edit: (r) => (r.find((x) => x.kind === 'wave').member = 'c15.9') }),
        'names a client that was never constructed',
      ],
      'a broker state with a partition record too many': [
        () =>
          run({
            edit: (r) => {
              const i = at(r, (x) => x.kind === 'partition-state')
              r.splice(i, 0, { ...r[i] })
            },
          }),
        'cites no open broker-state of its source',
      ],
      'a blank line': [
        () => {
          const edited = [...lines.slice(0, 5), '', ...lines.slice(5)]
          return validateAll({ matrix, config, header: buildHeader(edited), lines: edited })
        },
        'is blank',
      ],
      'an ordinary line over its byte cap': [
        () =>
          run({ edit: (r) => (r.find((x) => x.kind === 'diagnostic').message = 'x'.repeat(1100)) }),
        'exceeds 1024 bytes',
      ],
      'an execution id reused': [
        () =>
          run({
            edit: (r) => {
              const i = at(r, (x) => x.kind === 'shape' && x.phase === 'end')
              r.splice(i + 1, 0, { ...r[at(r, (x) => x.kind === 'shape')] }, { ...r[i] })
            },
          }),
        'execution id is reused',
      ],
      'an end count that differs from the operations': [
        () =>
          run({
            edit: (r) =>
              (r.find((x) => x.kind === 'shape' && x.phase === 'end').operationCount = 3),
          }),
        'operationCount differs from the ended operations',
      ],
      'an execution that never ends': [
        () =>
          run({
            edit: (r) =>
              r.splice(
                at(r, (x) => x.kind === 'shape' && x.phase === 'end'),
                1
              ),
          }),
        'execution x1 never ends',
      ],
      'an operation outside any open execution': [
        () => run({ edit: (r) => (r.find((x) => x.op === 'broker-stop').execution = 'x7') }),
        'operation names no open execution',
      ],
    })
  })

  it("a day's membership waves validate with their live state released as each closes", () => {
    const records = baseRecords()
    const exit = records.pop()
    for (let w = 0; w < 288; w++) {
      const execution = `h${String(Math.floor(w / 12)).padStart(2, '0')}m${w % 12}`
      const shape = (phase) => ({
        kind: 'shape',
        phase,
        source: 'p15',
        execution,
        shape: 'pause-resume',
        participants: [EAGER],
        operationCount: 0,
      })
      const wave = (event) => ({
        kind: 'wave',
        source: 'p01',
        execution,
        event,
        client: EAGER,
        member: event === 'transfer' ? 'c15.0' : EAGER,
        paused: 2,
        held: 1,
        observed: true,
      })
      const key = (partition, held) => ({
        kind: 'wave-key',
        source: 'p01',
        execution,
        key: { ...KEY, partition },
        held,
      })
      records.push(
        shape('start'),
        wave('transfer'),
        key(0, true),
        key(1, false),
        wave('return'),
        key(0, false),
        key(1, true),
        shape('end')
      )
    }
    records.push(exit)
    const lines = encode(records)
    const stream = new StreamValidator({
      matrix,
      config: buildConfig(),
      header: buildHeader(lines),
    })
    lines.forEach((line) => stream.push(line))
    expect(stream.finish()).toEqual([])
    expect([stream.summaries.size, stream.openItems, stream.executions.size]).toEqual([0, 0, 0])
  })

  it('a stream with many executions that never end fails once at its cap and keeps its problems bounded', () => {
    const records = baseRecords()
    const exit = records.pop()
    for (let n = 0; n < 3000; n++)
      records.push({
        kind: 'shape',
        phase: 'start',
        source: 'p15',
        execution: `orphan${n}`,
        shape: 'pause-resume',
        participants: [EAGER],
        operationCount: 0,
      })
    records.push(exit)
    const problems = validateWith(records, { maxOpen: 100 })
    expect(problems.filter((p) => p.includes('more than 100 open executions'))).toHaveLength(1)
    expect(problems.filter((p) => p.includes('never ends'))).toHaveLength(100)
    expect(problems.length).toBeLessThanOrEqual(1001)
    const unbounded = validateWith(records, {})
    expect(unbounded.length).toBeLessThanOrEqual(1001)
    expect(unbounded.at(-1)).toMatch(/^\d+ more problems omitted$/)
  })

  it('a record from another process naming a client is judged when the construction arrives, in any stream order', () => {
    expect(
      run({
        edit: (r) => {
          r.splice(
            at(r, (x) => x.kind === 'wave'),
            0,
            observed()
          )
          moveConstructAfter(r, (x) => x.kind === 'wave')
        },
      })
    ).toEqual([])
  })

  it('a record its process wrote before it exited is valid wherever the exit record falls in the stream', () => {
    expect(
      run({
        edit: (r) => {
          const exitT = r.length
          r.push({ ...call('connect', 'p01', EAGER, 'begin', 'k9'), at: exitT - 0.5 })
        },
      })
    ).toEqual([])
    expect(
      run({
        edit: (r) => {
          r.at(-1).at = r.length - 1.5
        },
      })
    ).toEqual([expect.stringContaining('the process has a record after its exit')])
  })

  it('inventory cuts are complete per client section and chunk before a sample uses them', () => {
    const chunkOf = (r, section, client = null) =>
      r.find(
        (x) =>
          x.kind === 'inventory' &&
          x.inventory === 'i1' &&
          x.section === section &&
          x.client === client
      )
    expectCases({
      'a process section omitted': [
        () => run({ edit: (r) => r.splice(r.indexOf(chunkOf(r, 'listeners')), 1) }),
        'inventory cut omits listeners',
      ],
      'a client pause-state section omitted': [
        () => run({ edit: (r) => r.splice(r.indexOf(chunkOf(r, 'pause-state', EAGER)), 1) }),
        'inventory cut omits pause-state',
      ],
      'a chunk missing': [
        () => run({ edit: (r) => (chunkOf(r, 'queues').chunks = 2) }),
        'inventory cut misses a queues chunk',
      ],
      'a chunk repeated': [
        () =>
          run({
            edit: (r) => r.splice(r.indexOf(chunkOf(r, 'queues')), 0, { ...chunkOf(r, 'queues') }),
          }),
        'chunk repeats',
      ],
      'chunks disagreeing on their count': [
        () =>
          run({
            edit: (r) => {
              const first = chunkOf(r, 'queues')
              first.chunks = 2
              r.splice(r.indexOf(first) + 1, 0, { ...first, chunk: 1, chunks: 3, entries: [] })
            },
          }),
        'chunks of one section disagree on their count',
      ],
      'a repeated entry key': [
        () =>
          run({
            edit: (r) =>
              chunkOf(r, 'sockets').entries.push({ ...chunkOf(r, 'sockets').entries[0] }),
          }),
        'sockets entry key repeats',
      ],
      'a share section for an ordinary client': [
        () =>
          run({
            edit: (r) =>
              Object.assign(chunkOf(r, 'reset-debt', EAGER), {
                section: 'share-state',
                entries: [],
              }),
          }),
        'section does not apply to this owner',
      ],
      'a process section owned by a client': [
        () => run({ edit: (r) => (chunkOf(r, 'queues').client = EAGER) }),
        'section does not apply to this owner',
      ],
      'a sample citing no cut': [
        () => run({ edit: (r) => (r.find(isSample('p01')).inventory = 'i404') }),
        'sample cites no inventory cut of its process',
      ],
      'a sample citing another process cut': [
        () => run({ edit: (r) => (r.find(isSample('p08')).inventory = 'i7') }),
        'sample cites no inventory cut of its process',
      ],
      'a sample listing another process client': [
        () => run({ edit: (r) => r.find(isSample('p01')).clients.push(SHARE) }),
        'sample lists a client of another process',
      ],
      'an incomplete cut never sampled': [
        () =>
          run({
            edit: (r) => {
              chunkOf(r, 'queues').chunks = 2
              r.find(isSample('p01')).inventory = 'i8b'
            },
          }),
        'inventory i1 misses a chunk',
      ],
      'operation cell open differs from its tickets': [
        () => run({ edit: (r) => (chunkOf(r, 'operation-cell', EAGER).entries[0].open = 2) }),
        'open differs from its tickets',
      ],
      'pause state restamped to another topic id': [
        () => run({ edit: (r) => (chunkOf(r, 'pause-state', EAGER).entries[0].originTopicId = B) }),
        'subscriptionLife does not end in its origin topic id',
      ],
      'listener with registrations but no event': [
        () => run({ edit: (r) => (chunkOf(r, 'listeners').entries[0].event = null) }),
        'only an empty emitter has a null event',
      ],
      'retired scope still live': [
        () => run({ edit: (r) => (chunkOf(r, 'scopes').entries[1].live = true) }),
        'a live scope has no retiredAt',
      ],
      'an inventory line over its byte cap': [
        () =>
          run({
            edit: (r) => {
              const scopes = chunkOf(r, 'scopes')
              for (let i = 0; i < 1000; i++) {
                scopes.entries.push({
                  scope: `h-${i}`,
                  owner: 'harness',
                  type: 'harness',
                  live: true,
                  retiredAt: null,
                })
              }
            },
          }),
        'exceeds 65536 bytes',
      ],
    })
    expect(
      run({
        edit: (r) => {
          const pause = chunkOf(r, 'pause-state', EAGER).entries[0]
          Object.assign(pause, { pauseAll: true, paused: [], resumed: [] })
        },
      })
    ).toEqual([])
  })

  it('a share-state entry larger than one inventory line is journalled in pieces that validate and reassemble to the held set of one entry', () => {
    const { chunk } = require('../finalSoakCollector')
    const held = Array.from({ length: 3000 }, (_, i) => ({
      acquisition: `${SHARE}.a${Math.floor(i / 500)}`,
      key: KEY,
      offset: String(i),
      state: i % 2 ? 'held' : 'settling',
    }))
    const entry = { ...ENTRIES['share-state'](SHARE)[0], records: held }
    const budget = matrix.host.maxInventoryLineBytes.value - 1024
    expect(Buffer.byteLength(JSON.stringify(entry))).toBeGreaterThan(budget)
    const parts = chunk([entry], budget)
    expect(parts.length).toBeGreaterThan(1)
    expect(parts.map((p) => p.piece)).toEqual(
      parts.map((_, index) => ({ index, pieces: parts.length }))
    )
    expect(mergePieces(parts.map((p) => p.entries[0]))).toEqual({ entry, problems: [] })

    /** Swaps the share-state record of cut i7 for the pieces `shape` returns. */
    const split =
      (shape = (records) => records) =>
      (r) => {
        const i = at(r, (x) => x.inventory === 'i7' && x.section === 'share-state')
        const records = parts.map(({ entries, piece }, n) => ({
          ...r[i],
          chunk: n,
          chunks: parts.length,
          entries,
          piece,
        }))
        r.splice(i, 1, ...shape(records))
      }
    expect(run({ edit: split() })).toEqual([])
    expectCases({
      'a piece missing': [
        () =>
          run({
            edit: split((p) =>
              p.filter((_, n) => n !== 1).map((x, n) => ({ ...x, chunk: n, chunks: p.length - 1 }))
            ),
          }),
        'misses a piece',
      ],
      'pieces disagreeing on a field outside their lists': [
        () =>
          run({
            edit: split((p) =>
              p.map((x, n) => (n === 1 ? { ...x, entries: [{ ...x.entries[0], run: 'r9' }] } : x))
            ),
          }),
        'pieces of one entry disagree on run',
      ],
      'pieces disagreeing on their count': [
        () =>
          run({
            edit: split((p) =>
              p.map((x, n) => (n === 1 ? { ...x, piece: { ...x.piece, pieces: 99 } } : x))
            ),
          }),
        'pieces of one entry disagree on their count',
      ],
      'a whole entry repeating a split key': [
        () =>
          run({
            edit: split((p) =>
              [
                ...p,
                {
                  ...p[0],
                  chunk: p.length,
                  chunks: p.length + 1,
                  entries: ENTRIES['share-state'](SHARE),
                  piece: null,
                },
              ].map((x) => ({ ...x, chunks: p.length + 1 }))
            ),
          }),
        'share-state entry key repeats',
      ],
      'a piece holding two entries': [
        () =>
          run({
            edit: split((p) =>
              p.map((x, n) =>
                n === 0 ? { ...x, entries: [...x.entries, ...ENTRIES['share-state']('c07.9')] } : x
              )
            ),
          }),
        'a piece is one entry',
      ],
    })
  })

  it('entry rules judge a split entry once reassembled, not each piece', () => {
    const { chunk } = require('../finalSoakCollector')
    const tickets = Array.from({ length: 6000 }, (_, i) => `t${i}`)
    const cell = { ledger: 'L1', topic: 'main', partition: 0, open: tickets.length, tickets }
    const budget = matrix.host.maxInventoryLineBytes.value - 1024
    const edit = (open) => (r) => {
      const i = at(r, (x) => x.inventory === 'i1' && x.section === 'operation-cell')
      const parts = chunk([{ ...cell, open }], budget)
      expect(parts.length).toBeGreaterThan(1)
      r.splice(
        i,
        1,
        ...parts.map(({ entries, piece }, n) => ({
          ...r[i],
          chunk: n,
          chunks: parts.length,
          entries,
          piece,
        }))
      )
    }
    expect(run({ edit: edit(tickets.length) })).toEqual([])
    expect(run({ edit: edit(tickets.length - 1) })).toEqual([
      expect.stringContaining('open differs from its tickets'),
    ])
  })

  it('samples carry their role fields and two new forced collections per reading', () => {
    const second = (fields) => (r) => {
      const i = at(r, isSample('p01'))
      r.splice(
        i + 1,
        0,
        ...cut('i1b', 'p01', [
          [null, PROCESS_SECTIONS],
          [EAGER, ORDINARY_SECTIONS],
        ]),
        {
          ...sample('p01', 'periodic', 'i1b', [EAGER], { slot: 1 }),
          gc: {
            ...GC,
            id: 'gc-2',
            firstBeginAt: 200,
            firstEndAt: 201,
            secondBeginAt: 302,
            secondEndAt: 303,
            cumulativeForced: 4,
          },
          admittedAt: 199,
          completedAt: 304,
          ...fields,
        }
      )
    }
    expect(run({ edit: second({}) })).toEqual([])
    const gcOf = (fields) => ({ gc: { ...GC, ...fields } })
    expectCases({
      'periodic without slot': [
        () => run({ edit: (r) => (r.find(isSample('p01')).slot = null) }),
        'slot is required for a periodic sample',
      ],
      'baseline with a slot': [
        () => run({ edit: (r) => (r.find(isSample('p08')).slot = 0) }),
        'slot is null for a baseline sample',
      ],
      'quiet without boundary': [
        () => run({ edit: (r) => (r.find(isSample('p07')).boundary = null) }),
        'boundary is required for a quiet sample',
      ],
      'quiet reading 3': [
        () => run({ edit: (r) => (r.find(isSample('p07')).reading = 3) }),
        'reading is 0, 1 or 2',
      ],
      'one forced collection': [
        () =>
          run({
            edit: (r) => Object.assign(r.find(isSample('p01')), gcOf({ forcedCollections: 1 })),
          }),
        'gc is not a Gc',
      ],
      'collections under 100 ms apart': [
        () =>
          run({ edit: (r) => Object.assign(r.find(isSample('p01')), gcOf({ secondBeginAt: 50 })) }),
        'gc collections are not two ordered calls 100 ms apart inside the sample',
      ],
      'heap read before the second collection ends': [
        () => run({ edit: (r) => (r.find(isSample('p01')).completedAt = 113) }),
        'gc collections are not two ordered calls 100 ms apart inside the sample',
      ],
      'first reading counting no collection': [
        () =>
          run({
            edit: (r) => Object.assign(r.find(isSample('p01')), gcOf({ cumulativeForced: 0 })),
          }),
        'cumulativeForced does not count this reading',
      ],
      'reused cumulative count': [
        () =>
          run({
            edit: second({
              gc: {
                ...GC,
                id: 'gc-2',
                firstBeginAt: 200,
                firstEndAt: 201,
                secondBeginAt: 302,
                secondEndAt: 303,
              },
            }),
          }),
        'cumulativeForced does not rise by two new collections',
      ],
      'reused gc interval': [
        () =>
          run({
            edit: second({ gc: { ...GC, cumulativeForced: 4 }, admittedAt: 10, completedAt: 115 }),
          }),
        'gc interval overlaps the previous reading',
      ],
      'TCPSocketWrap omitted': [
        () =>
          run({
            edit: (r) =>
              (r.find(isSample('p01')).activeResources = [{ type: 'Timeout', count: 2 }]),
          }),
        'activeResources omits TCPSocketWrap',
      ],
      'a resource type repeated': [
        () =>
          run({
            edit: (r) =>
              r.find(isSample('p01')).activeResources.push({ type: 'Timeout', count: 0 }),
          }),
        'activeResources repeats a type',
      ],
      'completed before admitted': [
        () => run({ edit: (r) => (r.find(isSample('p01')).admittedAt = 116) }),
        'a sample completes before it is admitted',
      ],
    })
  })

  it('manifest paths stay inside the artifact and bind every archived input', () => {
    expectCases({
      'absolute path': [
        () => run({ editHeader: (h) => (h.files[0].file = '/etc/passwd') }),
        'header.files is not',
      ],
      'parent path': [
        () => run({ editHeader: (h) => (h.files[0].file = 'raw/../../x') }),
        'header.files is not',
      ],
      'repeated path': [
        () => run({ editHeader: (h) => h.files.push({ ...h.files[0] }) }),
        'header.files repeats a path',
      ],
      'archived schema missing': [
        () =>
          run({
            editHeader: (h) =>
              (h.files = h.files.filter(
                (f) => f.file !== 'archive/scripts/soak/finalSoakSchema.js'
              )),
          }),
        'does not bind archived scripts/soak/finalSoakSchema.js',
      ],
      'config missing': [
        () =>
          run({ editHeader: (h) => (h.files = h.files.filter((f) => f.file !== 'config.json')) }),
        'does not bind config.json',
      ],
      'companion digest not in the manifest': [
        () => run({ editHeader: (h) => (h.companions[0].sha256 = 'f'.repeat(64)) }),
        'companion natural-exit is not bound by the manifest',
      ],
      'companion repeated': [
        () => run({ editHeader: (h) => h.companions.push({ ...h.companions[0] }) }),
        'header.companions repeats natural-exit',
      ],
      'unknown companion': [
        () => run({ editHeader: (h) => (h.companions[0].name = 'green-ci') }),
        'header.companions is not',
      ],
      'config and header disagree on the run': [
        () => run({ editHeader: (h) => (h.runId = 'ffffffffffff') }),
        'config and header disagree on run id or mode',
      ],
    })
  })

  it('broker capability and provenance files have their fixed types', () => {
    const capabilities = {
      nodes: [1, 2, 3].map((node) => ({
        node,
        apiRanges: [
          { apiKey: 0, minVersion: 3, maxVersion: 13 },
          { apiKey: 18, minVersion: 0, maxVersion: 4 },
        ],
        supportedFeatures: [{ name: 'share.version', minVersion: 0, maxVersion: 1 }],
        finalizedFeatures: [{ name: 'metadata.version', minVersionLevel: 27, maxVersionLevel: 27 }],
      })),
    }
    const image = {
      nodes: [1, 2, 3].map((node) => ({
        node,
        image: 'apache/kafka:4.4.0',
        digest: `sha256:${SHA}`,
        container: `kafka-${node}`,
        version: '4.4.0',
        inspectFile: `broker/inspect-${node}.json`,
        inspectSha256: SHA,
      })),
    }
    const supplied = {
      nodes: [
        { node: 1, version: '4.4.0', evidenceFile: 'broker/build-1.txt', evidenceSha256: SHA },
      ],
    }
    expect(validateCapabilities(capabilities)).toEqual([])
    expect(validateProvenance(image, 'image')).toEqual([])
    expect(validateProvenance(supplied, 'supplied')).toEqual([])
    const edited = (value, edit) => {
      const v = copy(value)
      edit(v)
      return v
    }
    expectCases({
      'inverted api range': [
        () =>
          validateCapabilities(
            edited(capabilities, (c) => (c.nodes[0].apiRanges[0].minVersion = 20))
          ),
        'has empty, repeated or inverted ranges',
      ],
      'repeated api key': [
        () =>
          validateCapabilities(
            edited(capabilities, (c) =>
              c.nodes[0].apiRanges.push({ apiKey: 0, minVersion: 0, maxVersion: 1 })
            )
          ),
        'has empty, repeated or inverted ranges',
      ],
      'no api ranges': [
        () => validateCapabilities(edited(capabilities, (c) => (c.nodes[1].apiRanges = []))),
        'has empty, repeated or inverted ranges',
      ],
      'no nodes': [
        () => validateCapabilities({ nodes: [] }),
        'capabilities.nodes is empty or repeats a node',
      ],
      'repeated node': [
        () => validateCapabilities(edited(capabilities, (c) => (c.nodes[1].node = 1))),
        'capabilities.nodes is empty or repeats a node',
      ],
      'image digest without its algorithm': [
        () =>
          validateProvenance(
            edited(image, (p) => (p.nodes[0].digest = SHA)),
            'image'
          ),
        'provenance.nodes is not',
      ],
      'image provenance without inspect bytes': [
        () =>
          validateProvenance(
            edited(image, (p) => delete p.nodes[0].inspectSha256),
            'image'
          ),
        'provenance.nodes is not',
      ],
      'supplied provenance without build evidence': [
        () =>
          validateProvenance(
            edited(supplied, (p) => delete p.nodes[0].evidenceSha256),
            'supplied'
          ),
        'provenance.nodes is not',
      ],
    })
  })
})

describe('inventory cuts across a consumer crash restart', () => {
  const { AsyncLocalStorage } = require('node:async_hooks')
  const ConsumerGroup = require('../../../src/consumer/consumerGroup')
  const GroupOwnership = require('../../../src/consumer/groupOwnership')
  const OffsetOperations = require('../../../src/consumer/offsetManager/offsetOperations')
  const ResetObligations = require('../../../src/consumer/offsetManager/resetObligations')
  const SeekOffsets = require('../../../src/consumer/seekOffsets')
  const SubscriptionState = require('../../../src/consumer/subscriptionState')
  const dictionary = require('../../../src/utils/dictionary')
  const { PassiveObserver } = require('../finalSoakObserver')

  /**
   * One run of a consumer as the consumer factory starts it: a fresh group whose run identity is
   * current while the factory's run slot holds its token; every run shares the consumer's ledger.
   * @returns {ConsumerGroup}
   */
  const startRun = (consumer, context) => {
    const token = {}
    consumer.slot = token
    const group = Object.create(ConsumerGroup.prototype)
    Object.assign(group, {
      groupId: groupOf('c01'),
      runIdentity: { isCurrent: () => consumer.slot === token },
      topicsSubscribed: [],
      topicConfigurations: { main: {} },
      preferredReadReplicasPerTopicPartition: dictionary(),
      seekOffset: new SeekOffsets(),
      acquired: new Map([['main|0', { acquiredAt: 1, changedAt: 1, recreatedAt: 0 }]]),
      acquireClock: 1,
      haltedPartitions: new Map(),
      ownership: new GroupOwnership(),
      subscriptionState: new SubscriptionState(),
      operations: consumer.ledger,
      obligations: new ResetObligations(),
      assignmentEpoch: 1,
      groupMembership: {
        kind: 'classic',
        grownTopicNames: new Set(),
        memberId: 'm1',
        generationId: 5,
      },
      cluster: { topicId: (t) => (t === 'main' ? A : null), targetTopics: new Set(['main']) },
    })
    context.run(EAGER, () => {
      group.retainSubscription(['main'])
      group.subscriptionState.pause([{ topic: 'main', partitions: [0] }])
    })
    return group
  }

  /** @returns {(r: object[]) => void} puts the observer's sections into cut `inventory` of p01 */
  const fill = (inventory, sections) => (r) => {
    for (const x of r)
      if (x.kind === 'inventory' && x.inventory === inventory && x.client === EAGER)
        x.entries = sections[x.section]
  }

  it('a cut taken after a crash restart, while the dropped run is not yet collected, holds only the restarted run and passes the stream validator; a ticket on the shared ledger names the restarted run', () => {
    const context = new AsyncLocalStorage()
    const tickets = []
    const observer = new PassiveObserver({
      owners: { ConsumerGroup, SubscriptionState, OffsetOperations },
      watched: {
        ConsumerGroup: ['retainSubscription'],
        SubscriptionState: ['pause'],
        OffsetOperations: ['begin'],
      },
      context,
      onEvent: (e) => e.type === 'ticket' && tickets.push(e.record),
    })
    observer.lives = () => `p01.20/${A}`
    observer.install()
    try {
      const consumer = { slot: null, ledger: new OffsetOperations() }
      const crashed = startRun(consumer, context)
      const before = observer.ordinarySections(EAGER).sections
      consumer.slot = null
      const restarted = startRun(consumer, context)
      const after = observer.ordinarySections(EAGER).sections
      context.run(EAGER, () => consumer.ledger.begin([{ topic: 'main', partition: 0 }]))
      const secondCut = (r) => {
        r.splice(
          at(r, isSample('p01')) + 1,
          0,
          ...cut('i1b', 'p01', [
            [null, PROCESS_SECTIONS],
            [EAGER, ORDINARY_SECTIONS],
          ]),
          {
            ...sample('p01', 'periodic', 'i1b', [EAGER], { slot: 1 }),
            gc: {
              ...GC,
              id: 'gc-2',
              firstBeginAt: 200,
              firstEndAt: 201,
              secondBeginAt: 302,
              secondEndAt: 303,
              cumulativeForced: 4,
            },
            admittedAt: 199,
            completedAt: 304,
          }
        )
        fill('i1', before)(r)
        fill('i1b', after)(r)
      }
      expect(run({ edit: secondCut })).toEqual([])
      const runs = (sections) => [
        ...new Set(
          ['subscription', 'pause-state', 'membership-state'].flatMap((s) =>
            sections[s].map((e) => e.run)
          )
        ),
      ]
      expect(runs(before)).toEqual([observer.runOf(crashed)])
      expect(runs(after)).toEqual([observer.runOf(restarted)])
      expect(after.assignment.map((e) => e.authority.run)).toEqual([observer.runOf(restarted)])
      expect(tickets.map((t) => t.ownerRun)).toEqual([observer.runOf(restarted)])
      expect(after['other-table'].map((t) => t.table)).toEqual([
        'acquired',
        'halted',
        'topic-configurations',
        'preferred-replicas',
        'queued-seeks',
        'grown-topic-names',
        'cluster-targets',
      ])
    } finally {
      observer.restore()
    }
  })
})
