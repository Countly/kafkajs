const crypto = require('node:crypto')
const fs = require('node:fs')
const os = require('node:os')
const path = require('node:path')
const { runEntry } = require('../report')
const { ShareSession, LIMITS } = require('../shareThroughput')

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

const shareEntry = (session) => ({
  name: session === 'batch' ? 'shareThroughput' : 'shareThroughput-eachMessage',
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
  metrics: [
    { key: 'recordsPerSecond', unit: 'records/s' },
    { key: 'p99', unit: 'ms' },
    { key: 'fetchPer1000', unit: 'frames' },
    { key: 'heapDelta', unit: 'bytes' },
  ],
  arms: ['fork'],
})
const walkEntry = {
  name: 'shareRecordWalk-100k',
  adapter: 'shareRecordWalk',
  command: [
    '--expose-gc',
    'scripts/benchmarks/shareRecordWalk.js',
    '--offsets',
    '100000',
    '--control',
    '0',
  ],
  broker: false,
  metrics: [
    { key: 'nsPerOffset', unit: 'ns' },
    { key: 'retainedHeap', unit: 'bytes' },
    { key: 'retainedTotal', unit: 'bytes' },
    { key: 'bytesPerDelivery', unit: 'bytes' },
  ],
  arms: ['fork'],
}
const snappyEntry = (readback) => ({
  name: 'snappyReadback',
  adapter: 'snappyReadback',
  command: ['--expose-gc', 'scripts/benchmarks/snappyCodec.js'],
  readback,
  broker: false,
  metrics: [
    { key: 'decodeMiBs', unit: 'MiB/s' },
    { key: 'encodeMiBs', unit: 'MiB/s' },
    { key: 'maxDelayMs', unit: 'ms' },
    { key: 'maxRssRatio', unit: 'ratio' },
  ],
  arms: ['fork'],
})

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
 * The children of a share session: corpus preparation and cleanup as the fixture prints them, and
 * one driver observation per run. `change(args, call)` edits an observation.
 */
const sessionSpawn = (change = () => ({})) => {
  const calls = []
  const spawn = (command, args, options) => {
    calls.push({ args, env: options.env })
    const phase = ['--prepare', '--cleanup'].find((flag) => args.includes(flag))
    if (phase) {
      const file = options.env.P9_SHARE_CORPUS
      const topic = `share-bench-${crypto.randomUUID()}`
      if (phase === '--prepare') {
        const expected = ['a', 'b', 'c'].map((id) => JSON.stringify(['t', 0, id]))
        fs.writeFileSync(
          file,
          JSON.stringify({ schema: 1, topic, expected, messages: 200000, size: 100, partitions: 3 })
        )
      }
      const line = { status: 'fixture-complete', phase: phase.slice(2), topic, corpus: file }
      return { status: 0, stdout: `${JSON.stringify(line)}\n`, stderr: '' }
    }
    const output = driverOutput(args, options.env, change(args, calls.length))
    return { status: output.status === 'valid' ? 0 : 2, stdout: `${JSON.stringify(output)}\n` }
  }
  return { spawn, calls }
}

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

/** Runs a whole session through `ShareSession` and returns the record it prints, as JSON. */
const sessionRecord = async (name, change) => {
  const { spawn, calls } = sessionSpawn(change)
  const record = await new ShareSession(name, {
    raw: path.join(freshDir(), 'fork-run-1.jsonl'),
    spawn,
    env: { KAFKAJS_CLUSTER_MANIFEST: guardFile() },
    admin: adminAt(2),
  }).run()
  return { record, calls }
}
const sessionResult = (record) => ({
  status: { accepted: 0, void: 3, inconclusive: 3, error: 1 }[record.status],
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

/** One `snappyCodec.js` line over the seven committed encoder artifacts. */
const snappyLine = (changes = {}) => ({
  decodeMiBs: 412.5,
  encodeMiBs: 188.25,
  maxDelayMs: 7.5,
  maxRssRatio: 0.41,
  checkedBytes: SNAPPY_CHECKED_BYTES,
  readbacks: 7,
  failures: 0,
  ...changes,
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
        env: {},
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
          env: { KAFKAJS_CLUSTER_MANIFEST: guardFile() },
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
    expect(rows.map((row) => row.status)).toEqual(Array(16).fill('inconclusive'))
    expect(rows.map((row) => row.arm)).toEqual(
      ['walk', 'control', 'walk~AA', 'control~AA'].flatMap((arm) => Array(4).fill(arm))
    )
    expect(rows[0]).toMatchObject({ metric: 'nsPerOffset', runs: 7, median: 20.03, min: 20 })
    expect(rows[8]).toMatchObject({ arm: 'walk~AA', metric: 'nsPerOffset', median: 22.03 })
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
    const run = json(snappyLine())
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
    const failed = {
      status: 1,
      stdout: '',
      stderr: 'Error: Expected seven Java read-back artifacts; run the Java verification first',
    }
    expect(report(snappyEntry(receipt()), [failed]).rows).toEqual([
      expect.objectContaining({
        status: 'error',
        reason: expect.stringContaining('Expected seven Java read-back artifacts'),
      }),
    ])
    const counted = report(snappyEntry(receipt()), [json(snappyLine({ readbacks: 6 }))])
    expect(counted.rows).toEqual([expect.objectContaining({ status: 'invalid' })])
    expect(control(counted.details[0], 'fork', 'seven encoder artifacts')).toMatchObject({
      passed: false,
    })

    const { rows, details } = report(snappyEntry(receipt()), [run])
    expect(rows.map(({ metric, status, runs }) => [metric, status, runs])).toEqual([
      ['decodeMiBs', 'valid', 3],
      ['encodeMiBs', 'valid', 3],
      ['maxDelayMs', 'valid', 3],
      ['maxRssRatio', 'valid', 3],
    ])
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

    const snappy = report(snappyEntry(receipt()), [json(snappyLine())])
    expect(control(snappy.details[0], 'fork', 'decoded bytes = corpus')).toEqual({
      arm: 'fork',
      name: 'decoded bytes = corpus',
      passed: true,
      detail: `${SNAPPY_CHECKED_BYTES} bytes in every run`,
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
})
