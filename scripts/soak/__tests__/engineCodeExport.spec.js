const v8 = require('node:v8')
const { EngineCodeJournal, JournalConstancy } = require('../telemetryChurn')
const { slope } = require('../bounds')

/** @returns {object} independent archived raw and engine journals, including non-sample readings */
const canned = () =>
  JSON.parse(`{
    "samples": [
      {"completedAtMs":60000.125,"snapshot":{"memory":{"phase":"settled","at":70000.25,"retainedBytes":10000000}}},
      {"completedAtMs":90000.25,"snapshot":{"memory":{"phase":"settled","at":100000.375,"retainedBytes":10050001}}},
      {"completedAtMs":150000.5,"snapshot":{"memory":{"phase":"settled","at":160000.625,"retainedBytes":10250003}}},
      {"completedAtMs":240000.75,"snapshot":{"memory":{"phase":"settled","at":250000.875,"retainedBytes":10400002}}}
    ],
    "engine": {
      "version":1,
      "readings":[
        {"readingIndex":0,"phase":"c0","at":100.125,"code":1000000,"bytecode":500000},
        {"readingIndex":1,"phase":"settled","at":70000.25,"code":1500000,"bytecode":500000},
        {"readingIndex":2,"phase":"settled","at":100000.375,"code":1520000,"bytecode":500002},
        {"readingIndex":3,"phase":"settled","at":160000.625,"code":1560000,"bytecode":500001},
        {"readingIndex":4,"phase":"settled","at":250000.875,"code":1620000,"bytecode":500004},
        {"readingIndex":5,"phase":"final","at":300000.125,"code":1630000,"bytecode":500005}
      ],
      "samples":[
        {"sampleIndex":0,"readingIndex":1,"completedAtMs":60000.125},
        {"sampleIndex":1,"readingIndex":2,"completedAtMs":90000.25},
        {"sampleIndex":2,"readingIndex":3,"completedAtMs":150000.5},
        {"sampleIndex":3,"readingIndex":4,"completedAtMs":240000.75}
      ]
    }
  }`)

/** @param {object} engine canned journal @returns {EngineCodeJournal} recorded fixed storage */
const record = (engine) => {
  const journal = new EngineCodeJournal({ capacity: engine.readings.length })
  const spy = jest.spyOn(v8, 'getHeapCodeStatistics')
  try {
    for (const reading of engine.readings) {
      spy.mockReturnValueOnce({
        code_and_metadata_size: reading.code,
        bytecode_and_metadata_size: reading.bytecode,
      })
      journal.record(reading)
    }
  } finally {
    spy.mockRestore()
  }
  return journal
}

describe('telemetry engine scalar export', () => {
  test('joins canned raw and engine journals by exact sample identity and both clocks', () => {
    const { samples, engine } = canned()
    const original = JSON.stringify({ samples, engine })
    const joined = EngineCodeJournal.join(samples, engine)
    expect(joined).toHaveLength(4)
    expect(joined[1]).toEqual({
      sampleIndex: 1,
      readingIndex: 2,
      phase: 'settled',
      at: 100000.375,
      completedAtMs: 90000.25,
      raw: 10050001,
      code: 1520000,
      bytecode: 500002,
    })
    expect(joined.map((row) => row.readingIndex)).toEqual([1, 2, 3, 4])
    expect(JSON.stringify({ samples, engine })).toBe(original)
  })

  test('rejects missing, duplicate, reordered, wrong-time and wrong-identity journal rows', () => {
    const corruptions = [
      (raw, code) => raw.splice(1, 1),
      (raw, code) => code.samples.splice(1, 1),
      (raw, code) => code.readings.splice(2, 1),
      (raw, code) => raw.splice(1, 0, raw[0]),
      (raw, code) => code.samples.splice(1, 0, code.samples[0]),
      (raw, code) => code.readings.splice(1, 0, code.readings[0]),
      (raw, code) => raw.reverse(),
      (raw, code) => code.samples.reverse(),
      (raw, code) => code.readings.reverse(),
      (raw, code) => (code.readings[2].at += 0.125),
      (raw, code) => (raw[1].snapshot.memory.at += 0.125),
      (raw, code) => (code.samples[1].completedAtMs += 0.125),
      (raw, code) => (code.samples[1].sampleIndex = 0),
      (raw, code) => (code.samples[1].readingIndex = 1),
      (raw, code) => (code.readings[2].readingIndex = 1),
      (raw, code) => (code.readings[2].phase = 'c0'),
      (raw, code) => (code.readings[2].at = code.readings[1].at),
      (raw, code) => (code.readings[0].at = code.readings[1].at),
      (raw, code) => (code.version = 2),
    ]
    for (const corrupt of corruptions) {
      const { samples, engine } = canned()
      corrupt(samples, engine)
      expect(() => EngineCodeJournal.join(samples, engine)).toThrow(/engine.*(join|journal)/i)
    }
  })

  test('rejects non-finite scalars and their JSON null encodings in either journal', () => {
    const corruptions = [
      (raw, code, value) => (raw[1].completedAtMs = value),
      (raw, code, value) => (raw[1].snapshot.memory.at = value),
      (raw, code, value) => (raw[1].snapshot.memory.retainedBytes = value),
      (raw, code, value) => (code.samples[1].completedAtMs = value),
      (raw, code, value) => (code.samples[1].sampleIndex = value),
      (raw, code, value) => (code.samples[1].readingIndex = value),
      (raw, code, value) => (code.readings[2].at = value),
      (raw, code, value) => (code.readings[2].code = value),
      (raw, code, value) => (code.readings[2].bytecode = value),
      (raw, code, value) => (code.readings[0].code = value),
    ]
    for (const corrupt of corruptions)
      for (const value of [NaN, Infinity, -Infinity, null, '1000']) {
        const { samples, engine } = canned()
        corrupt(samples, engine, value)
        expect(() => EngineCodeJournal.join(samples, engine)).toThrow(/engine.*(join|journal)/i)
      }
  })

  test('exports every reading and replays all original OLS slopes and endpoints exactly', () => {
    const { samples, engine } = canned()
    const journal = record(engine)
    const report = journal.report(samples)
    const archive = JSON.parse(JSON.stringify(report))
    expect(archive.engine).toEqual(engine)
    expect(archive).toMatchObject({
      scope: 'shadow',
      acceptance: false,
      complete: true,
      readings: 4,
    })
    const joined = EngineCodeJournal.join(samples, archive.engine)
    for (const [name, bytes] of [
      ['raw', (row) => row.raw],
      ['code', (row) => row.code + row.bytecode],
      ['data', (row) => row.raw - (row.code + row.bytecode)],
    ]) {
      const points = joined.map((row) => [row.completedAtMs / 60000, bytes(row)])
      expect(archive[name]).toEqual({
        slopeBytesPerMinute: slope(points),
        endpointBytes: points[3][1] - points[0][1],
      })
    }
    expect([
      archive.raw.endpointBytes,
      archive.code.endpointBytes,
      archive.data.endpointBytes,
    ]).toEqual([400002, 120004, 279998])
  })

  test('keeps prefilled storage constant through closed and export and makes caught overflow fatal', () => {
    const { samples, engine } = canned()
    const journal = new EngineCodeJournal({ capacity: engine.readings.length })
    expect(journal.rows.storage.equals(Buffer.alloc(journal.journalState().byteLength, 0xa5))).toBe(
      true
    )
    const baseline = { journals: journal.journalState() }
    const spy = jest.spyOn(v8, 'getHeapCodeStatistics')
    const snapshots = []
    try {
      for (const reading of engine.readings) {
        spy.mockReturnValueOnce({
          code_and_metadata_size: reading.code,
          bytecode_and_metadata_size: reading.bytecode,
        })
        journal.record(reading)
        snapshots.push({ snapshot: { journals: journal.journalState() } })
      }
      const teardown = snapshots.at(-1).snapshot
      const closed = journal.journalState()
      expect(JournalConstancy.violations({ baseline, samples: snapshots, teardown })).toEqual([])
      const beforeExport = Buffer.from(journal.rows.storage)
      expect(journal.report(samples).engine).toEqual(engine)
      expect(journal.journalState()).toEqual(closed)
      expect(journal.rows.storage.equals(beforeExport)).toBe(true)
      expect(() => journal.record({ phase: 'final', at: 400000 })).toThrow(/engine code.*overflow/)
      const overflow = JournalConstancy.apply(
        { ok: true, outcome: 'passed', violations: [] },
        { baseline, samples: snapshots, teardown: { journals: journal.journalState() } }
      )
      expect(overflow).toMatchObject({ ok: false, outcome: 'failed' })
      expect(overflow.violations).toContain('journal overflow')
      expect(journal.report(samples)).toMatchObject({ complete: false })
      expect(journal.journalState().byteLength).toBe(baseline.journals.byteLength)
    } finally {
      spy.mockRestore()
    }
  })

  test('marks a failed join incomplete without publishing a partial OLS summary', () => {
    const { samples, engine } = canned()
    const journal = record(engine)
    samples[1].snapshot.memory.at += 1
    const report = journal.report(samples)
    expect(report).toMatchObject({ scope: 'shadow', acceptance: false, complete: false })
    expect(report.engine.readings).toEqual(engine.readings)
    expect(report).not.toHaveProperty('raw')
    expect(report).not.toHaveProperty('code')
    expect(report).not.toHaveProperty('data')
  })
})
