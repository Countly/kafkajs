const crypto = require('node:crypto')
const fs = require('node:fs')
const os = require('node:os')
const path = require('node:path')
const { performance } = require('node:perf_hooks')
const { EventEmitter } = require('node:events')
const { runEntry, renderMarkdown } = require('../report')
const {
  ShareSession,
  SESSION_BUDGET,
  LIMITS,
  Frames,
  AcquisitionLog,
  ShareThroughput,
  measureWindow,
  ownerFile,
  evaluateSession,
} = require('../shareThroughput')
const { setupFailure } = require('../session')
const { ARMS: SNAPPY_ARMS, snappySession } = require('../snappyCodec')
const shareAdapter = require('../adapters/shareThroughput')
const walkAdapter = require('../adapters/shareRecordWalk')
const snappyAdapter = require('../adapters/snappyReadback')

const ENCODER_DIR = path.join(
  __dirname,
  '../../../src/protocol/message/compression/fixtures/encoder'
)
const ENCODER_FILES = fs
  .readdirSync(ENCODER_DIR)
  .filter((file) => file.endsWith('.json'))
  .sort()
/** Bytes the eight Snappy arms decode and compare: 3 MiB, 232000, 1, 64, 64, 32 MiB, 1000000, 1 MiB. */
const SNAPPY_CHECKED_BYTES = 174247040
const MiB = 1048576
const SNAPPY_SIZES = {
  mixed: 3 * MiB,
  json: 232000,
  random: MiB,
  expansion: 64 * MiB,
  encode64: 64 * MiB,
  mixedLarge: 32 * MiB,
  tinyBlocks: 1000000,
  tinyTokens: MiB,
}

const scratch = fs.mkdtempSync(path.join(os.tmpdir(), 'bench-share-'))
afterAll(() => fs.rmSync(scratch, { recursive: true, force: true }))
let dirs = 0
const freshDir = () => {
  const dir = path.join(scratch, String(++dirs))
  fs.mkdirSync(dir)
  return dir
}
const sha = (text) => crypto.createHash('sha256').update(text).digest('hex')
const valueAfter = (args, flag) => args[args.indexOf(flag) + 1]

const SHARE_METRICS = [
  { key: 'recordsPerSecond', unit: 'records/s' },
  { key: 'p99', unit: 'ms' },
  { key: 'fetchPer1000', unit: 'frames' },
  { key: 'heapDelta', unit: 'bytes' },
]
const WALK_METRICS = [
  { key: 'nsPerOffset', unit: 'ns' },
  { key: 'retainedHeap', unit: 'bytes' },
  { key: 'retainedBuffers', unit: 'bytes' },
  { key: 'retainedTotal', unit: 'bytes' },
  { key: 'bytesPerDelivery', unit: 'bytes' },
  { key: 'ledgerMs', unit: 'ms' },
]
const shareRow = (name, session) => ({
  name,
  adapter: 'shareThroughput',
  command: [
    '--expose-gc',
    'scripts/benchmarks/shareThroughput.js',
    '--session',
    session,
    '--raw',
    '{raw}',
  ],
  broker: true,
  metrics: SHARE_METRICS,
  arms: ['fork'],
})
const walkRow = (name, offsets) => ({
  name,
  adapter: 'shareRecordWalk',
  command: [
    '--expose-gc',
    'scripts/benchmarks/shareRecordWalk.js',
    '--offsets',
    offsets,
    '--control',
    '0',
  ],
  broker: false,
  metrics: WALK_METRICS,
  arms: ['fork'],
})
/** The five manifest rows of the share and Snappy measurements, exactly as the manifest holds them. */
const ENTRIES = [
  shareRow('shareThroughput', 'batch'),
  shareRow('shareThroughput-eachMessage', 'eachMessage'),
  walkRow('shareRecordWalk-100k', '100000'),
  walkRow('shareRecordWalk-1m', '1000000'),
  {
    name: 'snappyReadback',
    adapter: 'snappyReadback',
    command: ['--expose-gc', 'scripts/benchmarks/snappyCodec.js', '--session', '--raw', '{raw}'],
    readback: '.tmp/bench/snappy-java-readback.log',
    broker: false,
    metrics: [
      { key: 'decodeMiBs', unit: 'MiB/s' },
      { key: 'encodeMiBs', unit: 'MiB/s' },
      { key: 'maxDelayMs', unit: 'ms' },
      { key: 'maxRssRatio', unit: 'ratio' },
    ],
    arms: ['fork'],
  },
]
const shareEntry = (session) => ENTRIES[session === 'batch' ? 0 : 1]
const walkEntry = ENTRIES[2]
const snappyEntry = (readback) => ({ ...ENTRIES[4], readback })

/** Runs one report entry over scripted spawn results, with its own raw directory. */
const report = (entry, results) => {
  const rawDir = freshDir()
  const calls = []
  const spawn = (command, args) => {
    calls.push(args)
    return results[Math.min(calls.length, results.length) - 1]
  }
  const result = runEntry(entry, { runs: 3, forkDir: 'f', baselineDir: 'b', spawn, rawDir })
  return { ...result, calls, raw: path.join(rawDir, entry.name) }
}
const control = (detail, arm, name) =>
  detail.controls.find((row) => row.arm === arm && row.name === name)

/**
 * One driver observation as `shareThroughput.js` prints it: the measured window for a share arm,
 * the ordinary reference's handler gaps, the delivered counts and the archived trace.
 */
const driverOutput = (args, env, changes = {}) => {
  const arm = valueAfter(args, '--arm')
  const handler = valueAfter(args, '--handler')
  const acquireMode = valueAfter(args, '--acquire-mode')
  const frame = valueAfter(args, '--max-records') === '1'
  const share = arm === 'share'
  const perMessage = handler === 'eachMessage'
  const records = frame ? LIMITS.controlMeasured : LIMITS.measured
  const messages = frame ? LIMITS.controlMessages : LIMITS.messages
  const rps = { batch_optimized: 152000, record_limit: 148000, ordinary: 251000 }
  return {
    status: 'valid',
    ...(share
      ? {
          start: 1200.5,
          end: 2420.75,
          records,
          callbacks: perMessage ? records : records / (frame ? 1 : LIMITS.maxRecords),
          independentMessageSum: records,
          fetch: frame ? 1000 : 360,
          ack: 0,
          fetchPer1000: frame ? 1000 : 2,
          p99: 31,
          latencySamples: records,
          recordsPerSecond: perMessage ? 131000 : rps[acquireMode],
        }
      : { recordsPerSecond: rps.ordinary, p99: 4.5, fetchPer1000: 0, gapSamples: 390 }),
    heapDelta: 81234,
    before: { heapUsed: 40000000, queueDepth: 0 },
    idle: { shareRecords: 0, shareRanges: 0 },
    after: { heapUsed: 40081234, queueDepth: 0, shareSessions: 0, shareRecords: 0 },
    delivered: {
      batches: perMessage || frame ? messages : share ? messages / LIMITS.maxRecords : 391,
      messages,
    },
    controls: true,
    groupId: `bench-${crypto.randomUUID()}`,
    arm: share ? acquireMode : 'ordinary',
    handler,
    trace: env.P9_SHARE_TRACE,
    traceHash: sha(env.P9_SHARE_TRACE),
    config: { arm, handler, acquireMode },
    checks: { status: 'controls-passed' },
    schema: 1,
    ...changes,
  }
}

/**
 * The children of a share session: corpus preparation and cleanup as the fixture prints them (a
 * preparation writes its ownership record first, then the manifest), and one driver observation
 * per run. `change(args, call)` edits an observation, or with `exit` and `stderr` makes it fail; `fixture(phase, file, call)` may replace a
 * fixture's result after its files are written; `tick()` runs once per child.
 */
const sessionSpawn = (change = () => ({}), { fixture = () => undefined, tick = () => {} } = {}) => {
  const calls = []
  const spawn = (command, args, options) => {
    calls.push({ args, env: options.env, timeout: options.timeout })
    tick()
    const phase = ['--prepare', '--cleanup'].find((flag) => args.includes(flag))
    if (phase) {
      const file = options.env.P9_SHARE_CORPUS
      let topic
      if (phase === '--prepare') {
        topic = `share-bench-${crypto.randomUUID()}`
        fs.writeFileSync(ownerFile(file), JSON.stringify({ schema: 1, topic }))
        const expected = ['a', 'b', 'c'].map((id) => JSON.stringify(['t', 0, id]))
        fs.writeFileSync(
          file,
          JSON.stringify({ schema: 1, topic, expected, messages: 200000, size: 100, partitions: 3 })
        )
      } else {
        topic = JSON.parse(fs.readFileSync(ownerFile(file), 'utf8')).topic
      }
      const scripted = fixture(phase.slice(2), file, calls.length)
      if (scripted) return scripted
      const line = { status: 'fixture-complete', phase: phase.slice(2), topic, corpus: file }
      return { status: 0, stdout: `${JSON.stringify(line)}\n`, stderr: '' }
    }
    const changes = change(args, calls.length)
    if (changes.exit !== undefined)
      return { status: changes.exit, stdout: '', stderr: changes.stderr }
    const output = driverOutput(args, options.env, changes)
    return { status: output.status === 'valid' ? 0 : 2, stdout: `${JSON.stringify(output)}\n` }
  }
  return { spawn, calls }
}
/** What each child of a session was: `prepare`, `cleanup` or `observe`. */
const phases = (calls) =>
  calls.map(({ args }) =>
    args.includes('--prepare') ? 'prepare' : args.includes('--cleanup') ? 'cleanup' : 'observe'
  )

const guardFile = () => {
  const file = path.join(freshDir(), 'cluster.json')
  fs.writeFileSync(file, JSON.stringify({ compose: 'docker-compose.share.yml', owner: 'bench' }))
  return file
}
const adminAt =
  (level, steps = []) =>
  () => ({
    connect: async () => steps.push('connect'),
    disconnect: async () => steps.push('disconnect'),
    describeFeatures: async () => {
      steps.push('describeFeatures')
      return {
        finalizedFeatures: level === null ? {} : { 'share.version': { maxVersionLevel: level } },
      }
    },
  })
/** The environment of the workflow job that owns the share rows. */
const shareJob = (env = {}) => ({
  BENCH_SHARE_ROWS: '1',
  KAFKAJS_CLUSTER_MANIFEST: guardFile(),
  ...env,
})

/** Runs a whole session through `ShareSession` and returns the record it prints, as JSON. */
const sessionRecord = async (name, change, { fixture, tick, now } = {}) => {
  const { spawn, calls } = sessionSpawn(change, { fixture, tick })
  const record = await new ShareSession(name, {
    raw: path.join(freshDir(), 'fork-run-1.jsonl'),
    spawn,
    env: shareJob(),
    admin: adminAt(2),
    ...(now && { now }),
  }).run()
  return { record, calls }
}
const sessionResult = (record) => ({
  status: { accepted: 0, omitted: 0, void: 3, inconclusive: 3, error: 1 }[record.status],
  stdout: `share session\n${JSON.stringify(record)}\n`,
  stderr: '',
})

/** Seven measured observations of a record-walk arm, as `observe` returns them. */
const walkObservations = (nsPerOffset, retainedHeap) =>
  Array.from({ length: 7 }, (_, i) => ({
    nsPerOffset: nsPerOffset + i * 0.01,
    retainedHeap: retainedHeap + i,
    retainedBuffers: 0,
    retainedTotal: retainedHeap + i,
    deliveries: 50000,
    bytesPerDelivery: (retainedHeap + i) / 50000,
    ledgerMs: 3.25,
  }))
const walkSession = (walkNs) => {
  const observations = {
    walk: walkObservations(walkNs, 800000),
    control: walkObservations(64, 9000000),
  }
  const median = (rows, key) =>
    rows.map((row) => row[key]).sort((a, b) => a - b)[Math.floor(rows.length / 2)]
  const medians = Object.fromEntries(
    Object.entries(observations).map(([name, rows]) => [
      name,
      Object.fromEntries(Object.keys(rows[0]).map((key) => [key, median(rows, key)])),
    ])
  )
  return { observations, medians }
}
/** A `shareRecordWalk.js` session line; a void line is printed before the driver throws. */
const walkLine = (secondWalkNs, status) => ({
  internalAPI: true,
  host: { node: 'v24.11.0', cpus: 4, loadBefore: [0.1, 0.1, 0.1], loadAfter: [0.2, 0.1, 0.1] },
  offsets: 100000,
  correctness: { validArms: 2, rejectedControls: 13 },
  first: walkSession(20),
  second: walkSession(secondWalkNs),
  timed: true,
  ...(status === 'void'
    ? { status: 'void' }
    : { failed: [], status: 'candidate; compare both sizes and first accepted baseline ceilings' }),
})

/** One `snappyCodec.js --arm` child line; `decodeMiBs` is the mixed arm's. */
const snappyArmLine = (name, decodeMiBs = 412.5, changes = {}) => ({
  decodeMiBs: name === 'mixed' ? decodeMiBs : 500,
  encodeMiBs: ['mixed', 'json', 'random', 'encode64'].includes(name) ? 188.25 : 0,
  delayMs: 7.5,
  rssGrowth: SNAPPY_SIZES[name],
  memoryBound: 4 * SNAPPY_SIZES[name],
  inputBytes: 1000,
  outputBytes: SNAPPY_SIZES[name],
  checkedBytes: SNAPPY_SIZES[name],
  ...changes,
})
/**
 * Runs a whole Snappy session over scripted `--arm` children and returns the record it prints.
 * `line(name, observation, label)` may replace a child's line.
 */
const snappyRecord = (line = () => undefined) => {
  let calls = 0
  const spawn = (command, args) => {
    const observation = Math.floor(calls++ / SNAPPY_ARMS.length)
    const label = ((observation % 2) + Math.floor(observation / 2)) % 2 ? 'snappy~AA' : 'snappy'
    const name = valueAfter(args, '--arm')
    const output = line(name, observation, label) ?? snappyArmLine(name)
    return { status: 0, stdout: `${JSON.stringify(output)}\n`, stderr: '' }
  }
  const raw = path.join(freshDir(), 'fork-run-1.jsonl')
  return snappySession({ raw, spawn, env: { BENCH_SNAPPY_ROWS: '1' } })
}
const snappyResult = (record) => ({
  status: { accepted: 0, omitted: 0, void: 3, error: 1 }[record.status],
  stdout: `${JSON.stringify(record)}\n`,
  stderr: '',
})
const receipt = (files = ENCODER_FILES) => {
  const file = path.join(freshDir(), 'snappy-java-readback.log')
  fs.writeFileSync(file, files.map((name) => `read-back ok ${name}\n`).join(''))
  return file
}
const json = (value, status = 0, stderr = '') => ({
  status,
  stdout: `${JSON.stringify(value)}\n`,
  stderr,
})

describe('Benchmark report share and Snappy adapters', () => {
  test('share throughput refuses to run until share is enabled and read back', async () => {
    const raw = () => path.join(freshDir(), 'fork-run-1.jsonl')
    const unguarded = sessionSpawn()
    const created = []
    await expect(
      new ShareSession('eachMessage', {
        raw: raw(),
        spawn: unguarded.spawn,
        env: { BENCH_SHARE_ROWS: '1' },
        admin: () => created.push('admin'),
      }).run()
    ).rejects.toThrow('A share session requires the reviewed guard (KAFKAJS_CLUSTER_MANIFEST)')
    expect([unguarded.calls, created]).toEqual([[], []])

    for (const level of [null, 0]) {
      const steps = []
      const disabled = sessionSpawn()
      await expect(
        new ShareSession('eachMessage', {
          raw: raw(),
          spawn: disabled.spawn,
          env: shareJob(),
          admin: adminAt(level, steps),
        }).run()
      ).rejects.toThrow(
        'share.version is finalized at level 0; enable share and read it back first'
      )
      expect(disabled.calls).toEqual([])
      expect(steps).toEqual(['connect', 'describeFeatures', 'disconnect'])
    }

    const { record, calls } = await sessionRecord('eachMessage')
    expect(record.share).toEqual({ feature: 'share.version', level: 2 })
    expect(record.status).toBe('accepted')
    expect(calls[0].args).toContain('--prepare')
    expect(calls.at(-1).args).toContain('--cleanup')
    const observations = calls.slice(1, -1)
    expect(observations).toHaveLength(2 * 2 * LIMITS.observations)
    const traces = observations.map(({ env }) => env.P9_SHARE_TRACE)
    expect(new Set(traces).size).toBe(traces.length)
    expect(observations.map(({ args }) => valueAfter(args, '--handler')).slice(0, 4)).toEqual([
      'eachMessage',
      'eachBatch',
      'eachBatch',
      'eachMessage',
    ])

    const unread = { ...record, share: { feature: 'share.version', level: 0 } }
    const { rows, details } = report(shareEntry('eachMessage'), [sessionResult(unread)])
    expect(rows).toEqual([expect.objectContaining({ status: 'invalid' })])
    expect(control(details[0], 'session', 'share enabled and read back')).toMatchObject({
      passed: false,
      detail: 'share.version level 0',
    })
  })

  test('a void share record walk session is inconclusive', () => {
    const stderr = 'A/A exceeds three percent or has no measurable denominator\n'
    const { rows, details } = report(walkEntry, [json(walkLine(22, 'void'), 1, stderr)])
    expect(rows.map((row) => row.status)).toEqual(Array(24).fill('inconclusive'))
    expect(rows.map((row) => row.arm)).toEqual(
      ['walk', 'control', 'walk~AA', 'control~AA'].flatMap((arm) => Array(6).fill(arm))
    )
    expect(rows[0]).toMatchObject({ metric: 'nsPerOffset', runs: 7, median: 20.03, min: 20 })
    expect(rows[12]).toMatchObject({ arm: 'walk~AA', metric: 'nsPerOffset', median: 22.03 })
    expect(details[0]).toMatchObject({
      status: 'inconclusive',
      reason: 'the A/A control is void: walk nsPerOffset differs by 9.99 % between sessions',
    })
    expect(control(details[0], 'walk', 'A/A spread')).toMatchObject({ passed: false })

    expect(report(walkEntry, [json(walkLine(20.2), 0)]).rows[0]).toMatchObject({
      status: 'valid',
      median: 20.03,
    })
    for (const [result, reason] of [
      [json(walkLine(20.2, 'void'), 0), 'exit 0 disagrees with the void session'],
      [json(walkLine(22), 0), 'void false disagrees with an A/A spread of 9.99 %'],
      [{ status: 1, stdout: '', stderr }, 'exit 1: A/A exceeds three percent'],
    ]) {
      expect(report(walkEntry, [result]).rows).toEqual([
        expect.objectContaining({ status: 'error', reason: expect.stringContaining(reason) }),
      ])
    }
  })

  test('Snappy readback needs all seven Java readbacks or reports error', () => {
    expect(ENCODER_FILES).toHaveLength(7)
    const record = snappyRecord()
    const run = snappyResult(record)
    const missing = path.join(freshDir(), 'absent.log')
    for (const [readback, reason] of [
      [receipt(ENCODER_FILES.slice(1)), `6 Java read-backs, expected the 7 encoder artifacts`],
      [receipt([...ENCODER_FILES.slice(1), ENCODER_FILES[1]]), `missing ${ENCODER_FILES[0]}`],
      [missing, `no Java read-back receipt at ${missing}`],
    ]) {
      const { rows, calls } = report(snappyEntry(readback), [run])
      expect(rows).toEqual([
        expect.objectContaining({ status: 'error', reason: expect.stringContaining(reason) }),
      ])
      expect(calls).toHaveLength(1)
    }
    const failed = snappyResult({
      schema: 1,
      session: 'snappy',
      status: 'error',
      reason: 'snappy mixed: exit 1: RangeError: boom',
    })
    expect(report(snappyEntry(receipt()), [failed]).rows).toEqual([
      expect.objectContaining({
        status: 'error',
        reason: expect.stringContaining('session error: snappy mixed: exit 1: RangeError: boom'),
      }),
    ])
    const short = { median: 7, min: 6, max: 7, n: 8 }
    const counted = report(snappyEntry(receipt()), [
      snappyResult({
        ...record,
        arms: { ...record.arms, snappy: { ...record.arms.snappy, readbacks: short } },
      }),
    ])
    expect(counted.rows).toEqual([expect.objectContaining({ status: 'invalid' })])
    expect(control(counted.details[0], 'fork', 'seven encoder artifacts')).toMatchObject({
      passed: false,
    })

    const { rows, details } = report(snappyEntry(receipt()), [run])
    expect(rows.map(({ arm, metric, status, runs }) => [arm, metric, status, runs])).toEqual(
      ['fork', 'fork~AA'].flatMap((arm) =>
        ['decodeMiBs', 'encodeMiBs', 'maxDelayMs', 'maxRssRatio'].map((metric) => [
          arm,
          metric,
          'valid',
          8,
        ])
      )
    )
    expect(rows[0]).toMatchObject({ median: 412.5, min: 412.5, max: 412.5 })
    expect(control(details[0], 'fork', 'seven Java read-backs')).toEqual({
      arm: 'fork',
      name: 'seven Java read-backs',
      passed: true,
      detail: `read-back ok ${ENCODER_FILES.join(', ')}`,
    })
  })

  test('every share and Snappy row keeps its raw controls and corpus identity', async () => {
    const batch = await sessionRecord('batch')
    expect(batch.record.status).toBe('accepted')
    const share = report(shareEntry('batch'), [sessionResult(batch.record)])
    expect([...new Set(share.rows.map((row) => row.arm))]).toEqual([
      'batch_optimized',
      'batch_optimized~AA',
      'ordinary',
      'ordinary~AA',
      'record_limit',
      'record_limit~AA',
    ])
    expect(share.rows.every((row) => row.status === 'valid' && row.runs === 7)).toBe(true)
    const corpus = batch.record.corpus
    expect(control(share.details[0], 'session', 'corpus identity')).toEqual({
      arm: 'session',
      name: 'corpus identity',
      passed: true,
      detail: `${corpus.topic}: 200000 records of 100 bytes on 3 partitions, identities sha256 ${corpus.identities}`,
    })
    expect(control(share.details[0], 'session', 'frame positive control')).toMatchObject({
      passed: true,
      detail: 'fetchPer1000 1000, at least 900',
    })
    expect(control(share.details[0], 'ordinary', 'delivered = corpus')).toMatchObject({
      passed: true,
    })
    expect(control(share.details[0], 'record_limit', 'trace archived')).toMatchObject({
      passed: true,
      detail: '16 distinct traces',
    })
    const stored = JSON.parse(fs.readFileSync(path.join(share.raw, 'fork.json'), 'utf8'))
    expect(JSON.parse(stored.runs[0].stdout.split('\n')[1])).toEqual(batch.record)
    expect(JSON.parse(fs.readFileSync(path.join(share.raw, 'entry.json'), 'utf8'))).toEqual(
      shareEntry('batch')
    )

    const walk = report(walkEntry, [json(walkLine(20.2), 0)])
    expect(control(walk.details[0], 'session', 'corpus identity')).toEqual({
      arm: 'session',
      name: 'corpus identity',
      passed: true,
      detail: '100000 acquired offsets, 50000 deliveries in every observation',
    })
    expect(control(walk.details[0], 'session', 'corrupt outputs rejected')).toMatchObject({
      passed: true,
      detail: '13 of 13',
    })
    expect(fs.readFileSync(path.join(walk.raw, 'fork.json'), 'utf8')).toContain('"rejectedControls')

    const snappy = report(snappyEntry(receipt()), [snappyResult(snappyRecord())])
    expect(control(snappy.details[0], 'fork', 'decoded bytes = corpus')).toEqual({
      arm: 'fork',
      name: 'decoded bytes = corpus',
      passed: true,
      detail: `${SNAPPY_CHECKED_BYTES} bytes in every observation of both arms`,
    })
    expect(fs.readdirSync(snappy.raw).sort()).toEqual(['entry.json', 'fork.json'])

    const broken = await sessionRecord('batch', (args, call) =>
      call === 9 ? { delivered: { batches: 400, messages: 199999 } } : {}
    )
    const invalid = report(shareEntry('batch'), [sessionResult(broken.record)])
    expect(invalid.rows).toEqual([expect.objectContaining({ status: 'invalid' })])
    expect(invalid.details[0].reason).toMatch(/^failed controls: .* delivered = corpus$/)
  })

  test('the eachMessage row shows one handler call per record beside its eachBatch control', async () => {
    const { record } = await sessionRecord('eachMessage')
    const { rows, details } = report(shareEntry('eachMessage'), [sessionResult(record)])
    expect([...new Set(rows.map((row) => row.arm))]).toEqual([
      'eachMessage',
      'eachMessage~AA',
      'record_limit',
      'record_limit~AA',
    ])
    expect(rows.find((row) => row.arm === 'eachMessage')).toMatchObject({
      metric: 'recordsPerSecond',
      status: 'valid',
      median: 131000,
    })
    expect(control(details[0], 'eachMessage', 'handler path')).toMatchObject({
      passed: true,
      detail: 'eachMessage: one call per record, 200000 calls',
    })
    expect(control(details[0], 'record_limit', 'handler path')).toMatchObject({
      passed: true,
      detail: 'eachBatch: 400 calls for 200000 records',
    })
    expect(details[0].notes).toEqual(
      expect.arrayContaining([
        `driver sha256 ${record.driver.sha256}`,
        expect.stringMatching(/^node v\d+/),
        'eachMessage 7633.59 ns per record; observed A/A spread 0.00 % and retained scatter 0.00 % (repeatability, not a detection limit); the per-message context-entry cost is owed to an invocation-path measurement',
      ])
    )

    const batched = await sessionRecord('eachMessage', (args) =>
      valueAfter(args, '--handler') === 'eachMessage'
        ? { delivered: { batches: 400, messages: 200000 } }
        : {}
    )
    const wrong = report(shareEntry('eachMessage'), [sessionResult(batched.record)])
    expect(wrong.details[0].reason).toBe('failed controls: eachMessage handler path')

    const noisy = await sessionRecord('eachMessage', (args, call) =>
      valueAfter(args, '--handler') === 'eachBatch' && call > 17 ? { recordsPerSecond: 160000 } : {}
    )
    expect(noisy.record.status).toBe('void')
    const voided = report(shareEntry('eachMessage'), [sessionResult(noisy.record)])
    expect(voided.rows.every((row) => row.status === 'inconclusive')).toBe(true)
    expect(voided.details[0].reason).toBe(
      'the A/A control is void: record_limit recordsPerSecond differs by 8.11 % between repetitions'
    )
  })

  test('a share corpus is removed whenever its preparation recorded a topic, and every failure is kept', async () => {
    const exit1 = { status: 1, stdout: '', stderr: 'Corpus cleanup failed: producer disconnect' }
    const late = await sessionRecord('eachMessage', undefined, {
      fixture: (phase) => (phase === 'prepare' ? exit1 : undefined),
    })
    expect(phases(late.calls)).toEqual(['prepare', 'cleanup'])
    expect(late.record).toMatchObject({ status: 'error', corpus: null })
    expect(late.record.reason).toBe(
      'corpus prepare failed: exit 1: Corpus cleanup failed: producer disconnect'
    )

    const killed = await sessionRecord('eachMessage', undefined, {
      fixture: (phase, file) => {
        if (phase !== 'prepare') return undefined
        fs.rmSync(file)
        return { status: null, signal: 'SIGKILL', stdout: '', stderr: '' }
      },
    })
    expect(phases(killed.calls)).toEqual(['prepare', 'cleanup'])
    expect(killed.record.reason).toBe('prepare killed by SIGKILL')

    const deadline = { exit: 1, stderr: 'Error: Benchmark deadline exceeded' }
    const observed = await sessionRecord('eachMessage', (args, call) =>
      call === 3 ? deadline : {}
    )
    expect(phases(observed.calls)).toEqual(['prepare', 'observe', 'observe', 'cleanup'])
    expect(observed.record.reason).toBe(
      'record_limit a 0: exit 1: Error: Benchmark deadline exceeded'
    )

    const frame = await sessionRecord('batch', undefined, {
      fixture: (phase, file) => (phase === 'prepare' && file.includes('frame') ? exit1 : undefined),
    })
    expect(phases(frame.calls).slice(-4)).toEqual(['observe', 'prepare', 'cleanup', 'cleanup'])
    expect(frame.calls.slice(-2).map(({ env }) => path.basename(env.P9_SHARE_CORPUS))).toEqual([
      'fork-run-1.jsonl.frame-corpus.json',
      'fork-run-1.jsonl.corpus.json',
    ])
    expect(frame.record.reason).toBe(
      'corpus prepare failed: exit 1: Corpus cleanup failed: producer disconnect'
    )

    const stuck = { status: 1, stdout: '', stderr: 'topic still listed' }
    const unclean = await sessionRecord('eachMessage', undefined, {
      fixture: (phase) => (phase === 'cleanup' ? stuck : undefined),
    })
    expect(unclean.record.status).toBe('error')
    expect(unclean.record.evaluation.status).toBe('accepted')
    expect(unclean.record.reason).toBe('cleanup: corpus cleanup failed: exit 1: topic still listed')

    const both = await sessionRecord('eachMessage', (args, call) => (call === 2 ? deadline : {}), {
      fixture: (phase) => (phase === 'cleanup' ? stuck : undefined),
    })
    expect(phases(both.calls)).toEqual(['prepare', 'observe', 'cleanup'])
    expect(both.record.reason).toBe(
      'eachMessage a 0: exit 1: Error: Benchmark deadline exceeded; ' +
        'cleanup: corpus cleanup failed: exit 1: topic still listed'
    )
  })

  test('a share session without a measurement window is inconclusive, without numbers when no observation formed one', async () => {
    const noWindow = {
      status: 'inconclusive',
      reason: 'Cannot form declared full-acquisition window',
      start: undefined,
      end: undefined,
      records: undefined,
      callbacks: undefined,
      recordsPerSecond: undefined,
      p99: undefined,
      fetchPer1000: undefined,
      latencySamples: undefined,
    }
    const none = await sessionRecord('eachMessage', () => noWindow)
    expect(none.record.status).toBe('inconclusive')
    const empty = report(shareEntry('eachMessage'), [sessionResult(none.record)])
    expect(empty.rows).toEqual([
      {
        benchmark: 'shareThroughput-eachMessage',
        arm: '-',
        metric: '-',
        unit: '-',
        runs: 1,
        status: 'inconclusive',
        reason: expect.stringMatching(
          /^no measurement window: eachMessage a 0: Cannot form declared full-acquisition window; /
        ),
      },
    ])
    expect(empty.details[0]).toMatchObject({ status: 'inconclusive', reason: empty.rows[0].reason })
    expect(empty.details[0].controls.filter(({ passed }) => !passed)).toEqual([])
    const markdown = renderMarkdown(empty.rows, { runs: 1 }, empty.details)
    expect(markdown).toContain(
      '| shareThroughput-eachMessage | - | - | inconclusive | - | - | - | 1 | - |'
    )
    expect(markdown).toContain('### shareThroughput-eachMessage: inconclusive')

    const half = await sessionRecord('eachMessage', (args) =>
      valueAfter(args, '--handler') === 'eachMessage' ? noWindow : {}
    )
    const mixed = report(shareEntry('eachMessage'), [sessionResult(half.record)])
    expect([...new Set(mixed.rows.map((row) => row.arm))]).toEqual([
      'record_limit',
      'record_limit~AA',
    ])
    expect(mixed.rows.every((row) => row.status === 'inconclusive')).toBe(true)
    expect(mixed.rows[0]).toMatchObject({ metric: 'recordsPerSecond', median: 148000, runs: 7 })
    expect(mixed.details[0].reason).toMatch(/^no measurement window: eachMessage a 0: /)
    expect(renderMarkdown(mixed.rows, { runs: 1 }, mixed.details)).toContain(
      '| shareThroughput-eachMessage | record_limit | recordsPerSecond | inconclusive | 148000 | 148000 to 148000 | records/s | 7 | - |'
    )
  })

  test('a share session stops within its budget and keeps the time its corpus cleanup needs', async () => {
    const child = LIMITS.timeout + 60000
    expect(SESSION_BUDGET).toEqual({ totalMs: 290 * 60000, childMs: child, cleanupMs: 2 * child })
    expect(shareAdapter.timeoutMs).toBe(SESSION_BUDGET.totalMs + 10 * 60000)
    expect(snappyAdapter.timeoutMs).toBe(110 * 60000)
    expect(walkAdapter.timeoutMs).toBe(20 * 60000)

    let now = 0
    const step = 7 * 60000
    const { record, calls } = await sessionRecord('batch', undefined, {
      now: () => now,
      tick: () => {
        now += step
      },
    })
    const due = SESSION_BUDGET.totalMs - SESSION_BUDGET.cleanupMs
    const starts = calls.map((_, i) => i * step)
    expect(record.status).toBe('error')
    expect(record.reason).toBe(
      'the session budget leaves no time for observe batch_optimized aprime 4'
    )
    expect(calls).toHaveLength(40)
    expect(phases(calls)).toEqual([
      'prepare',
      ...Array(calls.length - 2).fill('observe'),
      'cleanup',
    ])
    const timeouts = calls.map(({ timeout }) => timeout)
    expect(timeouts.slice(0, -2).every((timeout) => timeout === child)).toBe(true)
    expect(timeouts.at(-2)).toBe(due - starts.at(-2))
    expect(timeouts.at(-2)).toBeLessThan(child)
    expect(timeouts.at(-1)).toBe(child)
    for (let i = 0; i < calls.length - 1; i++)
      expect(starts[i] + timeouts[i]).toBeLessThanOrEqual(due)
    expect(starts.at(-1) + timeouts.at(-1)).toBeLessThanOrEqual(SESSION_BUDGET.totalMs)
  })

  test('share and Snappy rows run only in the workflow job that owns them', async () => {
    const { spawn, calls } = sessionSpawn()
    const admins = []
    const record = await new ShareSession('batch', {
      raw: path.join(freshDir(), 'fork-run-1.jsonl'),
      spawn,
      env: { KAFKAJS_CLUSTER_MANIFEST: guardFile() },
      admin: () => admins.push('admin'),
    }).run()
    const reason = 'runs in its own job of the benchmark workflow (BENCH_SHARE_ROWS=1)'
    expect(record).toEqual({ schema: 1, session: 'batch', status: 'omitted', reason })
    expect([calls, admins]).toEqual([[], []])
    expect(report(shareEntry('batch'), [sessionResult(record)]).rows).toEqual([
      {
        benchmark: 'shareThroughput',
        arm: '-',
        metric: '-',
        unit: '-',
        runs: 1,
        status: 'omitted',
        reason,
      },
    ])

    const snappy = snappySession({ raw: path.join(freshDir(), 'raw.jsonl'), env: {} })
    const elsewhere = path.join(freshDir(), 'no-receipt.log')
    expect(report(snappyEntry(elsewhere), [snappyResult(snappy)]).rows).toEqual([
      {
        benchmark: 'snappyReadback',
        arm: '-',
        metric: '-',
        unit: '-',
        runs: 1,
        status: 'omitted',
        reason: 'runs in its own job of the benchmark workflow (BENCH_SNAPPY_ROWS=1)',
      },
    ])
  })

  /**
   * One share observation through the driver's own handler path, over a scripted nonprefetching
   * share loop: three nodes, four full 500-record acquisitions each, every fetch answered 2 ms after
   * its write, the next fetch written when the acquisition's last handler call returns, and its
   * answer settling the acquisition. Each record costs `recordMs` and each handler call `addedMs`
   * more: the seeded per-call cost. Returns the driver's measured window over 3000 records.
   */
  const observeLoop = async (handler, recordMs, addedMs) => {
    let now = 0
    const clock = jest.spyOn(performance, 'now').mockImplementation(() => now)
    try {
      const frames = new Frames(() => new EventEmitter())
      const driver = new ShareThroughput(
        { arm: 'share', handler, acquireMode: 'record_limit', maxRecords: 500, messages: 6000 },
        { kafka: null, frames, resources: {}, trace: '' }
      )
      driver.log = new AcquisitionLog(frames, new Map([0, 1, 2].map((node) => [node, `n${node}`])))
      driver.consumer = { acquisitionLockTimeoutMs: () => LIMITS.lock }
      const fetch = (node) => {
        const row = { endpoint: `n${node}`, apiKey: 78, writeAt: now, responseAt: null }
        frames.rows.push(row)
        now += 2
        row.responseAt = now
      }
      for (const node of [0, 1, 2]) fetch(node)
      for (let round = 0; round < 4; round++)
        for (const node of [0, 1, 2]) {
          const first = round * 500
          const messages = Array.from({ length: 500 }, (_, i) => ({
            offset: String(first + i),
            key: Buffer.from('k'),
            value: Buffer.from('v'),
          }))
          if (handler === 'eachMessage') {
            for (const message of messages) {
              now += recordMs + addedMs
              await driver.messageHandler({ topic: 't', partition: node, message })
            }
          } else {
            now += 500 * recordMs + addedMs
            const batch = {
              topic: 't',
              partition: node,
              messages,
              acquisitionLockTimeoutMs: LIMITS.lock,
            }
            await driver.handler({ batch })
          }
          fetch(node)
          const range = { firstOffset: String(first), lastOffset: String(first + 499) }
          driver.log.acknowledge(
            { topic: 't', partition: node, ...range, type: 'accept', error: null },
            now
          )
        }
      const window = measureWindow({ log: driver.log, frames, maxRecords: 500, target: 3000 })
      const inside = driver.log.calls.filter(
        ({ enteredAt }) => enteredAt > window.start && enteredAt <= window.end
      ).length
      return { window, inside }
    } finally {
      clock.mockRestore()
    }
  }

  test('the eachMessage measured window grows by the seeded cost of every handler call inside it', async () => {
    const recordMs = 0.01
    const plain = await observeLoop('eachMessage', recordMs, 0)
    const seeded = await observeLoop('eachMessage', recordMs, 0.0001)
    expect(plain.window.status).toBe('valid')
    expect(plain.inside).toBeGreaterThan(0)
    expect(seeded.inside).toBe(plain.inside)
    expect(
      seeded.window.end - seeded.window.start - (plain.window.end - plain.window.start)
    ).toBeCloseTo(plain.inside * 0.0001, 6)
    expect(seeded.window.recordsPerSecond).toBeLessThan(plain.window.recordsPerSecond)
  })

  /**
   * A copy of an accepted session record whose `label` arm has `base` records/s in its discarded
   * observation and `base × multipliers[i]` in the seven retained ones, in both repetitions, with
   * the evaluation recomputed so the record still agrees with itself.
   */
  const withThroughput = (record, label, base, multipliers = [1, 1, 1, 1, 1, 1, 1]) => {
    const copy = JSON.parse(JSON.stringify(record))
    for (const rep of ['a', 'aprime'])
      copy.arms[label][rep].forEach((row, i) => {
        row.recordsPerSecond = base * (i === 0 ? 1 : multipliers[i - 1])
      })
    copy.evaluation = evaluateSession({ arms: copy.arms }, null, Object.keys(copy.arms).sort())
    copy.status = copy.evaluation.status
    return copy
  }

  test('the eachMessage comparison shows its observed difference and scatter and leaves the per-message cost owed', async () => {
    const { record: pre } = await sessionRecord('eachMessage')
    const { record: post } = await sessionRecord('eachMessage', (args) =>
      valueAfter(args, '--handler') === 'eachMessage' ? { recordsPerSecond: 130000 } : {}
    )
    const owed =
      'the per-message context-entry cost is owed to an invocation-path measurement; these runs show the observed difference, A/A spread and scatter, not a detection limit'
    const real = shareAdapter.compareEachMessage(pre, post)
    expect(real).toEqual({
      status: 'inconclusive',
      reason: owed,
      perRecordNs: { pre: 1e9 / 131000, post: 1e9 / 130000 },
      deltaNs: 1e9 / 130000 - 1e9 / 131000,
      controlDrift: 0,
      adjustedDeltaNs: 1e9 / 130000 - 1e9 / 131000,
      aaSpread: 0,
      scatter: 0,
    })

    const wide = [0.8, 0.9, 0.99, 1, 1.01, 1.1, 1.2]
    const scattered = shareAdapter.compareEachMessage(
      withThroughput(pre, 'eachMessage', 100000, wide),
      withThroughput(pre, 'eachMessage', 99999, wide)
    )
    expect(scattered.adjustedDeltaNs).toBeCloseTo(0.100001, 6)
    expect(scattered.aaSpread).toBe(0)
    expect(scattered.scatter).toBeCloseTo(0.4, 9)
    expect(scattered.status).toBe('inconclusive')
    expect(scattered.reason).toBe(owed)
    for (const claim of ['resolved', 'resolutionNs', 'noise', 'comparable'])
      expect(scattered).not.toHaveProperty(claim)
  })

  test('the eachMessage comparison refuses missing provenance, unsuccessful sessions and incomplete or nonpositive observations', async () => {
    const { record: pre } = await sessionRecord('eachMessage')
    const { record: post } = await sessionRecord('eachMessage')
    const compare = shareAdapter.compareEachMessage
    expect(compare(pre, post).status).toBe('inconclusive')

    expect(() => compare({ ...pre, driver: { file: pre.driver.file } }, post)).toThrow(
      'PRE: no driver sha256'
    )
    expect(() => compare(pre, { ...post, node: undefined })).toThrow('POST: no Node version')
    expect(() =>
      compare(pre, { ...post, driver: { ...post.driver, sha256: 'b'.repeat(64) } })
    ).toThrow(`the runs used different drivers: ${pre.driver.sha256} and ${'b'.repeat(64)}`)
    expect(() => compare(pre, { ...post, node: 'v99.0.0' })).toThrow(
      `the runs used different Node versions: ${pre.node} and v99.0.0`
    )
    expect(() => compare({ ...pre, session: 'batch' }, post)).toThrow(
      'PRE: not an eachMessage session record'
    )

    const { record: unclean } = await sessionRecord('eachMessage', undefined, {
      fixture: (phase) =>
        phase === 'cleanup' ? { status: 1, stdout: '', stderr: 'topic still listed' } : undefined,
    })
    expect(unclean.evaluation.status).toBe('accepted')
    expect(() => compare(pre, unclean)).toThrow(
      'POST: the session is error: cleanup: corpus cleanup failed: exit 1: topic still listed'
    )
    const { record: noisy } = await sessionRecord('eachMessage', (args, call) =>
      valueAfter(args, '--handler') === 'eachBatch' && call > 17 ? { recordsPerSecond: 160000 } : {}
    )
    expect(() => compare(noisy, post)).toThrow('PRE: the session is void')
    expect(() =>
      compare(pre, { ...post, status: 'inconclusive', reason: 'no measurement window: x' })
    ).toThrow('POST: the session is inconclusive: no measurement window: x')

    const short = {
      ...post,
      arms: {
        ...post.arms,
        eachMessage: { ...post.arms.eachMessage, aprime: post.arms.eachMessage.aprime.slice(1) },
      },
    }
    expect(() => compare(pre, short)).toThrow(
      'POST: eachMessage aprime has 7 observations, expected 8'
    )
    expect(() => compare(withThroughput(pre, 'record_limit', 0), post)).toThrow(
      'PRE: record_limit a 0 has no positive throughput'
    )

    const receipt = (record, result) => {
      const file = path.join(freshDir(), 'fork.json')
      const stdout = `share session\n${JSON.stringify(record)}\n`
      fs.writeFileSync(
        file,
        JSON.stringify({ arm: 'fork', runs: [{ run: 1, args: [], stderr: '', stdout, ...result }] })
      )
      return file
    }
    const done = { status: 0, signal: null, error: null }
    expect(shareAdapter.compareFiles(receipt(pre, done), receipt(post, done))).toEqual(
      compare(pre, post)
    )
    expect(() =>
      shareAdapter.compareFiles(
        receipt(pre, { status: null, signal: 'SIGTERM' }),
        receipt(post, done)
      )
    ).toThrow('PRE: killed by SIGTERM')
    expect(() =>
      shareAdapter.compareFiles(receipt(pre, done), receipt(unclean, { status: 1, signal: null }))
    ).toThrow('POST: session error: cleanup: corpus cleanup failed: exit 1: topic still listed')
  })

  test('a selected share or Snappy job whose workflow setup failed reports an error row and starts no work', async () => {
    expect(setupFailure(undefined)).toBeUndefined()
    expect(setupFailure('start=success share=success')).toBeUndefined()
    expect(setupFailure('start=success share=failure guard=skipped junk')).toBe(
      'workflow setup did not succeed: share=failure, guard=skipped, junk'
    )

    const { spawn, calls } = sessionSpawn()
    const admins = []
    const reason = 'workflow setup did not succeed: share=failure, guard=skipped'
    const record = await new ShareSession('eachMessage', {
      raw: path.join(freshDir(), 'fork-run-1.jsonl'),
      spawn,
      env: { BENCH_SHARE_ROWS: '1', BENCH_SETUP: 'start=success share=failure guard=skipped' },
      admin: () => admins.push('admin'),
    }).run()
    expect(record).toEqual({ schema: 1, session: 'eachMessage', status: 'error', reason })
    expect([calls, admins]).toEqual([[], []])
    const shown = report(shareEntry('eachMessage'), [sessionResult(record)])
    expect(shown.rows).toEqual([
      expect.objectContaining({ status: 'error', reason: `run 1 of 1: session error: ${reason}` }),
    ])
    const stored = JSON.parse(fs.readFileSync(path.join(shown.raw, 'fork.json'), 'utf8'))
    expect(JSON.parse(stored.runs[0].stdout.split('\n')[1])).toEqual(record)

    let children = 0
    const snappy = snappySession({
      raw: path.join(freshDir(), 'fork-run-1.jsonl'),
      spawn: () => children++,
      env: { BENCH_SNAPPY_ROWS: '1', BENCH_SETUP: 'install=success java=failure' },
    })
    const javaReason = 'workflow setup did not succeed: java=failure'
    expect(snappy).toEqual({ schema: 1, session: 'snappy', status: 'error', reason: javaReason })
    expect(children).toBe(0)
    const missing = path.join(freshDir(), 'no-receipt.log')
    const snappyShown = report(snappyEntry(missing), [snappyResult(snappy)])
    expect(snappyShown.rows).toEqual([
      expect.objectContaining({
        status: 'error',
        reason: `run 1 of 1: session error: ${javaReason}`,
      }),
    ])
    expect(fs.readdirSync(snappyShown.raw).sort()).toEqual(['entry.json', 'fork.json'])
  })

  test('the manifest runs exactly the five share and Snappy rows and no longer excludes their drivers', () => {
    const manifest = require('../manifest.json')
    const names = ENTRIES.map(({ name }) => name)
    expect(manifest.entries.filter(({ name }) => names.includes(name))).toEqual(ENTRIES)
    expect(manifest.excluded.map(({ file }) => file)).not.toEqual(
      expect.arrayContaining(['shareRecordWalk.js'])
    )
    for (const file of ['shareRecordWalk.js', 'shareThroughput.js', 'snappyCodec.js'])
      expect(manifest.excluded.map((row) => row.file)).not.toContain(file)
  })
})
