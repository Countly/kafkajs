'use strict'

const path = require('node:path')
const fs = require('node:fs')
const { EventEmitter } = require('node:events')
const { PassThrough } = require('node:stream')
const { MEMORY_FLAGS } = require('../../../../testHelpers/processMemory')

const load = () => require(path.join(__dirname, '../warmupWitness'))
const points = ['m0', 'warmupEnd', 'matched', 'firstActive', 'closed']
const reading = (at, data = 1000000) => {
  const row = {
    at,
    rss: 10000000,
    heapUsed: data + 200,
    external: 100,
    arrayBuffers: 50,
    retained: data + 300,
    engine: { code: 200, bytecode: 100 },
  }
  return {
    ...row,
    gc: {
      admittedAt: at - 200,
      completedAt: at,
      observed: 4,
      checkpoints: [
        { ...row, at: at - 100, admittedAt: at - 200, observed: 2 },
        { ...row, admittedAt: at - 100, observed: 2 },
      ],
    },
  }
}

/** @returns {object} deterministic weak references without invoking collection */
const census = (capacity = 100) => {
  const dead = new WeakSet()
  const meter = new (load().WarmupWitnesses)({
    capacity,
    weak: (target) => ({ deref: () => (dead.has(target) ? undefined : target) }),
  })
  return { meter, dead }
}

/** @returns {object} complete synthetic original-child output with distinct pair identities */
const pair = () => {
  const { meter, dead } = census()
  const target = {}
  meter.track('foreground', target)
  const readings = points.map((_, i) => reading(1000 + i * 1000, i === 4 ? 600000 : 1000000))
  points.forEach((point, i) => {
    if (i === 4) dead.add(target)
    meter.check(point, readings[i])
  })
  const output = (variant) => ({
    diagnostic: true,
    acceptanceEligible: false,
    identity: {
      variant,
      arm: 'public',
      kind: 'producer',
      mode: 'unseeded',
      seed: null,
      childId: variant,
      tree: 'a'.repeat(40),
      runId: 'run',
      node: process.version,
      v8: process.versions.v8,
      platform: process.platform,
      arch: process.arch,
    },
    code: 0,
    signal: null,
    timedOut: false,
    failure: null,
    stderrBytes: 0,
    readings: {
      manifest: {
        arm: 'public',
        kind: 'producer',
        childId: variant,
        tree: 'a'.repeat(40),
        runId: 'run',
        node: process.version,
        v8: process.versions.v8,
        platform: process.platform,
        arch: process.arch,
        pid: 123,
        execArgv: MEMORY_FLAGS,
        seed: null,
        gc: { available: true, native: true },
        instrumented: { inspector: false, coverage: false, jest: false },
      },
      warmup: { m0: readings[0], end: readings[1] },
      matched: readings[2],
      actives: [{ phase: 'gzip', ...readings[3] }],
      closed: readings[4],
      ...(variant === 'witness' ? { warmupWitnesses: meter.summary() } : {}),
    },
  })
  return { scalar: output('scalar'), witness: output('witness') }
}

describe('warm-up lifecycle witnesses', () => {
  test('enumerates own catalogue foreground map set and backing objects without invoking getters', () => {
    const { meter } = census()
    const shared = {}
    meter.exclude(shared)
    const value = {}
    const buffer = Buffer.alloc(10)
    const root = { value, map: new Map([[value, new Set([value])]]), buffer, shared }
    Object.defineProperty(root, 'unsafe', {
      get: () => {
        throw new Error('getter invoked')
      },
    })
    meter.track('foreground', root)
    meter.track('alias', root)
    points.forEach((point, i) => meter.check(point, reading(1000 + i * 1000)))
    const rows = meter.summary().objects
    expect(rows).toHaveLength(6)
    expect(new Set(rows.map((row) => row.id)).size).toBe(6)
    expect(rows.every((row) => row.alive.every((alive) => alive === true))).toBe(true)
    expect(rows.some((row) => row.label.includes('shared'))).toBe(false)
    expect(rows.some((row) => row.label.endsWith('.buffer'))).toBe(true)
  })

  test('preserves pre-M deaths late catalogue births and post-M death intervals', () => {
    const { meter, dead } = census()
    const early = {}
    const late = {}
    meter.track('foreground', early)
    meter.check('m0', reading(1000))
    meter.track('catalogue', late)
    dead.add(early)
    meter.check('warmupEnd', reading(2000))
    meter.check('matched', reading(3000))
    meter.check('firstActive', reading(4000))
    dead.add(late)
    meter.check('closed', reading(5000))
    expect(meter.summary().objects.map((row) => row.alive)).toEqual([
      [true, false, false, false, false],
      [null, true, true, true, false],
    ])
  })

  test('fails on overflow duplicate phases skipped phases and late registration', () => {
    const { meter } = census(1)
    meter.track('foreground', {})
    expect(() => meter.track('extra', {})).toThrow(/capacity/)
    meter.check('m0', reading(1000))
    expect(() => meter.check('m0', reading(2000))).toThrow(/phase/)
    expect(() => meter.check('matched', reading(3000))).toThrow(/phase/)
    meter.check('warmupEnd', reading(2000))
    expect(() => meter.track('late', {})).toThrow(/registration/)
  })

  test('joins exact code and data scalars with post-M deaths without claiming causation', () => {
    const result = load().analysePair(pair())
    expect(result.acceptanceEligible).toBe(false)
    expect(result.scalar.points.closed).toMatchObject({ data: 600000, code: 200, bytecode: 100 })
    expect(result.witness.points.closed.checkpoints).toHaveLength(2)
    expect(result.answer).toMatchObject({
      aliveAtM: [1],
      survivedPastM: [1],
      diedAfterM: [{ id: 1, from: 'firstActive', to: 'closed', dataDelta: -400000 }],
      closedDataDelta: -400000,
      closedNegativeCoincidesWithPostMDeath: true,
      causationEstablished: false,
    })
  })

  test('distinguishes a negative delta without late witness deaths and a surviving witness', () => {
    const input = pair()
    input.witness.readings.warmupWitnesses.objects[0].alive = [true, false, false, false, false]
    expect(load().analysePair(input).answer).toMatchObject({
      survivedPastM: [],
      diedAfterM: [],
      closedNegativeCoincidesWithPostMDeath: false,
    })
    input.witness.readings.warmupWitnesses.objects[0].alive = [true, true, true, true, true]
    expect(load().analysePair(input).answer).toMatchObject({
      survivedPastM: [1],
      stillAliveAtClosed: [1],
      diedAfterM: [],
    })
  })

  test('rejects missing duplicate reordered nonfinite mismatched and resurrected evidence', () => {
    const mutants = [
      (p) => {
        p.witness.readings.warmupWitnesses.points.pop()
      },
      (p) => {
        p.witness.readings.warmupWitnesses.objects.push(
          p.witness.readings.warmupWitnesses.objects[0]
        )
      },
      (p) => {
        p.witness.readings.warmupWitnesses.points.reverse()
      },
      (p) => {
        p.scalar.readings.closed.gc.checkpoints[1].engine.code = NaN
      },
      (p) => {
        p.scalar.readings.closed.heapUsed++
      },
      (p) => {
        p.witness.readings.manifest.tree = 'b'.repeat(40)
      },
      (p) => {
        p.witness.identity.kind = 'admin'
      },
      (p) => {
        p.witness.readings.warmupWitnesses.objects[0].alive = [true, false, true, true, false]
      },
      (p) => {
        p.witness.readings.warmupWitnesses.points[4].at++
      },
      (p) => {
        p.scalar.timedOut = true
      },
      (p) => {
        p.scalar.readings.manifest.execArgv = ['--expose-gc']
      },
    ]
    for (const mutate of mutants) {
      const input = pair()
      mutate(input)
      expect(() => load().analysePair(input)).toThrow()
    }
  })

  test('launches original children with exact flags and keeps collector contrasts unseeded', async () => {
    const calls = []
    const launch = (file, args, options) => {
      calls.push({ file, args, options })
      const child = new EventEmitter()
      child.pid = 123
      child.stdout = new PassThrough()
      child.stderr = new PassThrough()
      child.kill = jest.fn()
      setImmediate(() => {
        child.stdout.end(JSON.stringify({ readings: { manifest: {} } }) + '\n')
        child.emit('close', 0, null)
      })
      return child
    }
    for (const arm of ['public', 'collector']) {
      await load().supervise(
        { arm, kind: 'producer', variant: 'witness', mode: 'close-warmup' },
        launch
      )
    }
    expect(calls.map((call) => call.args.slice(0, -2))).toEqual([MEMORY_FLAGS, MEMORY_FLAGS])
    expect(calls.map((call) => path.basename(call.args.at(-2)))).toEqual([
      'publicMemoryChild.js',
      'resourceChild.js',
    ])
    expect(calls[0].options.env.RESOURCE_SEED).toBe('close-warmup')
    expect(calls[1].options.env.RESOURCE_SEED).toBeUndefined()
    expect(calls.every((call) => call.options.env.RESOURCE_WITNESSES === '1')).toBe(true)
    expect(calls.every((call) => call.options.env.NODE_OPTIONS === undefined)).toBe(true)
  })

  test('hooks the five original settled boundaries and never adds a collection or settle', () => {
    for (const file of ['publicMemoryChild.js', 'resourceChild.js']) {
      const source = fs.readFileSync(
        path.join(__dirname, '../../../../src/telemetry/__tests__/fixtures', file),
        'utf8'
      )
      for (const phase of points) expect(source).toContain(`witnesses?.check('${phase}'`)
      expect(source).toContain("process.env.RESOURCE_WITNESSES === '1'")
      expect(source).toContain('warmupWitnesses: witnesses.summary()')
      expect(source).not.toMatch(/globalThis\.gc\(|witnesses\?\.settled\(/)
    }
  })
})
