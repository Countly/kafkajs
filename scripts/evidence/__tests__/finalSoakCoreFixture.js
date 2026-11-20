'use strict'

/**
 * Builds a small healthy final-soak world for the core checker's spec: a config, a journal of
 * records, a header and supervisor receipt, and the Apache dump files that a real run would
 * produce. Every Apache file is rendered from one log layout, so a test edits the layout (the
 * broker's truth) or one rendered file (a broken dump) and leaves the rest consistent.
 */
const crypto = require('node:crypto')
const fs = require('node:fs')
const path = require('node:path')
const schema = require('../../soak/finalSoakSchema')

const matrix = JSON.parse(
  fs.readFileSync(path.join(__dirname, '../../soak/finalSoakMatrix.json'), 'utf8')
)
const RUN = '0123456789ab'
const TOPIC_ID = 'AAAAAAAAAAAAAAAAAAAAAA'
const MAIN = `fs-${RUN}-main`
const TXN = `fs-${RUN}-txn`
const START = 1000
const slotId = (prefix, n) => `${prefix}${String(n).padStart(2, '0')}`
const sha256 = (data) => crypto.createHash('sha256').update(data).digest('hex')
const copy = (value) => JSON.parse(JSON.stringify(value))

/** @returns {object} a config `validateConfig` accepts, with the driver's topic names */
function buildConfig() {
  const kindOf = (kind) => matrix.clientKinds[kind]
  const processes = schema.CLIENT_KINDS.map((_, i) => ({
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
  const client = (slot, process, clientKind, purpose, extra = {}) => ({
    slot,
    process,
    clientKind,
    purpose,
    arm:
      purpose === 'control' ? 'interning-seeded' : purpose === 'observer' ? 'observer' : 'unseeded',
    controlId: purpose === 'control' ? 'interning-seeded' : null,
    group: kindOf(clientKind).grouped && purpose !== 'observer' ? `g-${RUN}-${slot}` : null,
    groupInstanceId: kindOf(clientKind).static ? `static-${slot}` : null,
    standing: purpose !== 'churn',
    telemetryEnabled:
      purpose !== 'observer' && purpose !== 'control' && kindOf(clientKind).telemetry,
    ...extra,
  })
  const clients = schema.CLIENT_KINDS.map((kind, i) =>
    client(slotId('c', i + 1), slotId('p', i + 1), kind, 'subject')
  )
  clients.push(
    client('c13', 'p13', 'classic-eager-dynamic', 'control'),
    client('c14', 'p14', 'admin', 'observer')
  )
  return {
    schema: 3,
    mode: 'acceptance',
    runId: RUN,
    processes,
    clients,
    topics: { main: MAIN, transactions: TXN },
  }
}

/** @param {object} config @returns {Map<string, object>} client slot to its subject/control config */
const slots = (config) => new Map(config.clients.map((c) => [c.slot, c]))

/** The value a producer offers: `run|id|transaction|` padded with dots to the matrix width. */
const valueOf = (id, transaction) =>
  `${RUN}|${id}|${transaction ?? ''}|`.padEnd(matrix.traffic.valueBytes.value, '.')

/**
 * Whole log layout: twelve main records, then one committed transaction of four members and one
 * aborted transaction of two (`closed` appends the driver's nontransactional closing record). An
 * entry may be a control `marker`, `invisible` to a read-committed reader, `absent` (its send ended
 * unknown and it never reached the log) or a `duplicate` copy.
 */
function buildLayout() {
  const main = Array.from({ length: 12 }, (_, i) => ({ id: `c08.${i}` }))
  const members = (from, n, tx, extra) =>
    Array.from({ length: n }, (_, i) => ({ id: `c09.${from + i}`, transaction: tx, ...extra }))
  const transactions = [
    ...members(0, 4, 'c09.0.x1'),
    { marker: 'commit' },
    ...members(4, 2, 'c09.0.x2', { invisible: true }),
    { marker: 'abort' },
  ]
  return { main, transactions }
}

/** @returns {object[]} the entries with their log offset, null for one that never reached the log */
const placed = (entries) => {
  let next = 0
  return entries.map((e) => ({ ...e, offset: e.absent ? null : next++ }))
}
const logEnd = (entries) => placed(entries).filter((e) => e.offset !== null).length

const authority = (client, extra = {}) => ({
  client,
  run: `${client}.r1`,
  member: 'm1',
  epoch: 1,
  token: 1,
  assignmentEpoch: 1,
  acquiredAt: 1,
  recreatedAt: 0,
  ...extra,
})
const key = (topic) => ({ topic, topicId: TOPIC_ID, partition: 0 })

/** @returns {object[]} identity and boundary records of the healthy traffic, in time order */
function trafficRecords(config, layout) {
  const records = []
  let t = 2000
  const add = (r) => records.push({ t: t++, ...r })
  const offered = (topic, entries, producer) => {
    placed(entries).forEach((e) => {
      if (e.marker || e.duplicate) return
      const base = { client: producer, id: e.id, key: key(topic) }
      add({
        kind: 'identity',
        stage: 'offered',
        source: 'p08',
        ...base,
        payloadSha256: sha256(e.id),
        bytes: matrix.traffic.valueBytes.value,
        transaction: e.transaction ?? null,
      })
      add({
        kind: 'identity',
        stage: 'ack',
        source: 'p08',
        ...base,
        ...(e.absent
          ? { outcome: 'unknown', offset: null, errorType: 'KafkaJSNumberOfRetriesExceeded' }
          : { outcome: 'acknowledged', offset: String(e.offset), errorType: null }),
      })
    })
  }
  offered(MAIN, layout.main, 'c08.0')
  offered(TXN, layout.transactions, 'c09.0')
  for (const [tx, decision] of [
    ['c09.0.x1', 'commit'],
    ['c09.0.x2', 'abort'],
  ])
    add({
      kind: 'identity',
      stage: 'transaction',
      source: 'p09',
      client: 'c09.0',
      transaction: tx,
      decision,
      outcome: decision === 'commit' ? 'committed' : 'aborted',
      call: `${tx}.${decision}`,
      errorType: null,
    })
  const bySlot = slots(config)
  const recipients = (role) =>
    matrix.topology.topics
      .find((t) => t.role === role)
      .recipients.map((kind) =>
        [...bySlot.values()].find((c) => c.clientKind === kind && c.purpose === 'subject')
      )
      .concat(role === 'main' ? [bySlot.get('c13')] : [])
  for (const [topic, role, entries] of [
    [MAIN, 'main', placed(layout.main)],
    [TXN, 'transactions', placed(layout.transactions).filter((e) => !e.marker && !e.invisible)],
  ]) {
    for (const c of recipients(role)) {
      const client = `${c.slot}.0`
      const share = matrix.clientKinds[c.clientKind].role === 'share'
      if (!share)
        add({
          kind: 'boundary',
          source: c.process,
          client,
          group: c.group,
          run: `${client}.r1`,
          member: 'm1',
          epoch: 1,
          protocol: 'eager',
          event: 'acquire',
          key: key(topic),
          authority: authority(client),
          position: null,
          cause: `${client}.a1`,
        })
      for (const e of entries) {
        if (e.absent || e.duplicate) continue
        add({
          kind: 'identity',
          stage: 'handler-enter',
          source: c.process,
          client,
          group: c.group,
          handler: `${client}.h${t}`,
          id: e.id,
          key: key(topic),
          offset: String(e.offset),
          payloadSha256: sha256(e.id),
          authority: share ? null : authority(client),
          acquisition: share ? `${client}.q${t}` : null,
          deliveryCount: share ? 1 : null,
        })
      }
    }
  }
  return records
}

/** @returns {{ at: number[], slots: number }} the sampling plan of the mode */
function planOf(mode) {
  const requestedMs = matrix.acceptance.modes[mode].requestedMs.value
  const smoke = mode === 'smoke'
  return {
    requestedMs,
    slots: requestedMs / matrix.sampling.periodMs.value + 1,
    hours: smoke ? 1 : requestedMs / matrix.schedule.hourMs,
    quietAt: smoke ? matrix.schedule.smoke.quietAtMs : matrix.schedule.quietAtMs,
  }
}

const RESOURCES = (tcp, timeout) => [
  { type: 'TCPSocketWrap', count: tcp },
  { type: 'Timeout', count: timeout },
]

/** @returns {object[]} start, exit and every sample record of all sixteen processes */
function processRecords(config, mode, withInventory) {
  const plan = planOf(mode)
  const end = START + plan.requestedMs
  const records = []
  const sample = (process, role, at, fields = {}) => {
    const inventory = `${process}.i${at}.${role}`
    if (withInventory)
      for (const section of matrix.recordSections.process)
        records.push({
          t: at,
          at,
          kind: 'inventory',
          source: process,
          inventory,
          process,
          client: null,
          section,
          chunk: 0,
          chunks: 1,
          entries: [],
        })
    records.push({
      t: at,
      kind: 'sample',
      source: process,
      process,
      role,
      slot: null,
      boundary: null,
      reading: null,
      admittedAt: at - 5,
      completedAt: at,
      rss: 5e7,
      heapUsed: 9e6,
      external: 1e5,
      activeResources: role === 'final' ? RESOURCES(0, 0) : RESOURCES(3, 2),
      gc: null,
      inventory,
      clients: [],
      ...fields,
    })
  }
  config.processes.forEach((p, i) => {
    records.push({
      t: 100 + i,
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
    sample(p.slot, 'baseline', 400 + i)
    for (let k = 0; k < plan.slots; k++)
      sample(p.slot, 'periodic', START + k * 30000 + i, {
        slot: k,
        admittedAt: START + k * 30000 + i,
      })
    if (p.role === 'subject' || p.role === 'seed')
      for (let h = 0; h < plan.hours; h++)
        for (let reading = 0; reading < 3; reading++) {
          const at = START + h * matrix.schedule.hourMs + plan.quietAt[reading] + i
          sample(p.slot, 'quiet', at, {
            boundary: `h${String(h).padStart(2, '0')}`,
            reading,
            admittedAt: at,
            completedAt: at + 5,
            heapUsed: 9e6 + (reading - 1) * 1000,
          })
        }
    sample(p.slot, 'final', end + 100 + i)
  })
  for (const [phase, t] of [
    ['start', START - 10],
    ['end', end + 50],
  ])
    records.push({
      t,
      at: t,
      kind: 'shape',
      source: 'p15',
      execution: 'traffic',
      phase,
      shape: 'traffic',
      participants: Object.keys(matrix.clientKinds),
      operationCount: 0,
    })
  config.processes.forEach((p, i) =>
    records.push({
      t: end + (p.slot === 'p15' ? 2000 : 1000 + i),
      kind: 'process',
      event: 'exit',
      source: p.slot === 'p15' ? 'outer' : 'p15',
      process: p.slot,
      code: 0,
      signal: null,
      spawnRecord: `${p.slot}.0`,
    })
  )
  return records
}

/** @returns {object[]} the client construct records of every config client */
const constructRecords = (config) =>
  config.clients.map((c, i) => ({
    t: 200 + i,
    kind: 'client',
    event: 'construct',
    source: c.process,
    client: `${c.slot}.0`,
    slot: c.slot,
    clientKind: c.clientKind,
    process: c.process,
    arm: c.arm,
    controlId: c.controlId,
    purpose: c.purpose,
    group: c.group,
    groupInstanceId: c.groupInstanceId,
    standing: c.standing,
    telemetryEnabled: c.telemetryEnabled,
    configSha256: 'e'.repeat(64),
  }))

/** @param {object} layout @returns {string[]} the console-consumer lines of one topic's visible records */
const dumpLines = (entries) =>
  placed(entries).flatMap((e) =>
    e.marker || e.absent || e.invisible
      ? []
      : [`Partition:0\tOffset:${e.offset}\t${e.id}\t${valueOf(e.id, e.transaction)}`]
  )

/** @returns {string} what `kafka-consumer-groups.sh --describe` prints for a group at the given offsets */
const describeText = (group, rows) =>
  [
    '',
    'GROUP                       TOPIC                       PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG             CONSUMER-ID     HOST            CLIENT-ID',
    ...rows.map(
      ([topic, current, end]) =>
        `${group} ${topic} 0          ${current}               ${end}               ${current === '-' ? '-' : end - current}               -               -               -`
    ),
    '',
  ].join('\n')

/** Renders one dump or describe step's three files into `files`. */
function putStep(files, name, stdout, stderrExtra = '') {
  const lines = stdout.split('\n').length - (stdout.endsWith('\n') ? 1 : 0)
  const count = stdout === '' ? 0 : lines
  files[name] = stdout
  files[`${name}.stderr`] = stderrExtra || `Processed a total of ${count} messages\n`
  files[`${name}.end`] = `exit=0 lines=${count}\n`
}

/** @returns {Record<string, string>} every Apache file of the world, from its layout */
function renderApache(world) {
  const files = {}
  const { layout, config } = world
  for (const [role, entries] of Object.entries(layout)) {
    const lines = dumpLines(entries)
    putStep(files, `${role}.dump`, lines.length ? `${lines.join('\n')}\n` : '')
  }
  const bySlot = slots(config)
  const end = { main: logEnd(layout.main), transactions: logEnd(layout.transactions) }
  for (const c of bySlot.values()) {
    if (!c.group || matrix.clientKinds[c.clientKind].role === 'share' || c.purpose === 'churn')
      continue
    const rows = [[MAIN, end.main, end.main]]
    if (c.purpose === 'subject') rows.push([TXN, end.transactions, end.transactions])
    putStep(
      files,
      `groups/${c.group}.describe`,
      describeText(c.group, rows),
      'Consumer group has no active members.\n'
    )
  }
  return files
}

/** @returns {object} a healthy world in the mode (journal records, header, supervisor, Apache files) */
function healthy({ mode = 'acceptance', withInventory = false } = {}) {
  const config = buildConfig()
  config.mode = mode
  const layout = buildLayout()
  const plan = planOf(mode)
  const end = START + plan.requestedMs
  const world = {
    matrix,
    config,
    mode,
    layout,
    withInventory,
    trafficTail: [],
    header: {
      schema: 3,
      mode,
      runId: RUN,
      treeSha: 'b'.repeat(40),
      node: 'v26.7.0',
      v8: '14.6.202',
      platform: 'linux',
      files: [],
      clock: { kind: 'linux-monotonic', originMs: 0, bootId: 'boot-1' },
      window: {
        runStartAt: START,
        runEndAt: end,
        observedEndAt: end + 10000,
        requestedMs: plan.requestedMs,
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
      exit: { supervisorProcess: 'p15', code: 0, signal: null, observedAt: end + 10001 },
      stream: { file: 'stream.ndjson', bytes: 0, records: 0, sha256: '0'.repeat(64) },
      companions: [],
    },
    supervisor: {
      runId: RUN,
      mode,
      treeSha: 'b'.repeat(40),
      failures: [],
      problems: [],
      forced: [],
      aborted: null,
      incomplete: null,
      diagnostic: false,
      steps: { ok: 10, late: 0, error: 0, aborted: 0 },
    },
  }
  world.records = () => [
    ...constructRecords(config),
    ...processRecords(config, mode, withInventory),
    ...trafficRecords(config, world.layout),
    ...world.trafficTail,
  ]
  world.apache = renderApache(world)
  return world
}

/**
 * A healthy world whose transactions log ends, as the driver's final drain makes it end, in one
 * nontransactional closing record after every transaction's markers.
 * @returns {object}
 */
function closed(options) {
  const world = healthy(options)
  world.layout.transactions.push({ id: 'c08.12' })
  world.apache = renderApache(world)
  return world
}

/** Replaces one rendered dump's lines and keeps its `.end` and stderr in step. */
function redump(world, role, lines) {
  putStep(world.apache, `${role}.dump`, lines.length ? `${lines.join('\n')}\n` : '')
}

const encode = (records) => {
  const seqs = new Map()
  const cutAt = new Map()
  return records
    .map((r, i) => ({ r, i }))
    .sort((a, b) => a.r.t - b.r.t || a.i - b.i)
    .map(({ r }) => {
      const seq = seqs.has(r.source) ? seqs.get(r.source) + 1 : 0
      seqs.set(r.source, seq)
      if (r.kind === 'inventory' && !cutAt.has(r.inventory)) cutAt.set(r.inventory, r.t)
      const at = r.kind === 'inventory' ? cutAt.get(r.inventory) : (r.at ?? r.t)
      return JSON.stringify({ execution: null, ...r, recordId: `${r.source}.${seq}`, seq, at })
    })
}

/**
 * Writes the world as a complete artifact directory: config, stream, archive and broker files,
 * supervisor receipt, Apache files and a header whose manifest hashes every written file.
 * @returns {string} the directory
 */
function writeArtifact(world, dir) {
  const put = (file, text) => {
    fs.mkdirSync(path.dirname(path.join(dir, file)), { recursive: true })
    fs.writeFileSync(path.join(dir, file), text)
  }
  const lines = encode(world.records())
  const stream = `${lines.join('\n')}\n`
  put('config.json', JSON.stringify(world.config))
  put('stream.ndjson', stream)
  put('supervisor.json', JSON.stringify(world.supervisor))
  const { code, observedAt } = world.header.exit
  put('outer-exit.json', JSON.stringify({ status: code, observedAt, ...world.outerExit }))
  for (const file of matrix.acceptance.archive)
    put(
      `archive/${file}`,
      file.endsWith('finalSoakMatrix.json')
        ? fs.readFileSync(path.join(__dirname, '../../soak/finalSoakMatrix.json'))
        : `archived ${file}\n`
    )
  for (const f of ['provenance.json', 'capabilities.json', 'compose.yml']) put(`broker/${f}`, f)
  for (const [name, text] of Object.entries(world.apache)) put(`apache/${name}`, text)
  const header = copy(world.header)
  const walk = (rel) =>
    fs.readdirSync(path.join(dir, rel), { withFileTypes: true }).flatMap((e) => {
      const file = rel ? `${rel}/${e.name}` : e.name
      return e.isDirectory() ? walk(file) : [file]
    })
  header.files = walk('').map((file) => {
    const bytes = fs.readFileSync(path.join(dir, file))
    return { file, bytes: bytes.length, sha256: sha256(bytes) }
  })
  header.stream = {
    file: 'stream.ndjson',
    bytes: Buffer.byteLength(stream),
    records: lines.length,
    sha256: sha256(stream),
  }
  put('header.json', JSON.stringify(header))
  return dir
}

module.exports = {
  renderApache,
  MAIN,
  TXN,
  RUN,
  START,
  matrix,
  copy,
  sha256,
  healthy,
  closed,
  redump,
  valueOf,
  describeText,
  putStep,
  writeArtifact,
  key,
  authority,
  planOf,
}
