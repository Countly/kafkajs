const fs = require('node:fs')
const os = require('node:os')
const path = require('node:path')
const { LongWindowDiagnostic } = require('../longWindowDiagnostic')
const { LONG_WINDOW, ShadowReport, identityOf } = require('../longWindow')

const KINDS = ['producer', 'consumer', 'admin', 'shareConsumer']

const reportOf = (kind, { tripped = false, complete = true } = {}) =>
  complete
    ? { kind, complete: true, firstUse: { tripped }, mid: { data: { slopeBytesPerMinute: 0 } } }
    : { kind, complete: false, error: 'no worker result' }

describe('long-window driver', () => {
  let directory
  beforeEach(() => {
    directory = fs.mkdtempSync(path.join(os.tmpdir(), 'long-window-driver-'))
  })
  afterEach(() => {
    fs.rmSync(directory, { recursive: true, force: true })
  })

  const driver = (arm) =>
    new LongWindowDiagnostic({
      directory,
      arm,
      identity: identityOf({ arm, replicate: '1', tree: 'a'.repeat(40), env: {} }),
      modules: { ShadowReport, LONG_WINDOW },
    })

  test('refuses to run outside diagnostic CI, for an unknown arm or with a stale switch', async () => {
    const env = { CI: 'true', LONG_WINDOW_ARM: 'unseeded', LONG_WINDOW_REPLICATE: '1' }
    const run = (override, argv = ['--diagnostic-only', directory]) =>
      LongWindowDiagnostic.main(argv, { ...env, ...override })
    await expect(run({ CI: undefined })).rejects.toThrow('Use --diagnostic-only')
    await expect(run({ LONG_WINDOW_ARM: 'other' })).rejects.toThrow('Use --diagnostic-only')
    await expect(run({ TELEMETRY_SEED: 'slope2x' })).rejects.toThrow('Use --diagnostic-only')
    await expect(run({ [LONG_WINDOW.env]: '1' })).rejects.toThrow('Use --diagnostic-only')
    await expect(run({ SHARE_PEAK_DIAGNOSTIC: '1' })).rejects.toThrow('Use --diagnostic-only')
    await expect(run({}, [directory])).rejects.toThrow('Use --diagnostic-only')
    fs.writeFileSync(path.join(directory, 'stale'), '')
    await expect(run({})).rejects.toThrow('must be fresh')
  })

  test('writes every file with the shadow flags, the frozen candidate and the commit first', () => {
    driver('seeded').write('long-window.json', { workers: [] })
    const written = JSON.parse(fs.readFileSync(path.join(directory, 'long-window.json'), 'utf8'))
    expect(Object.keys(written).slice(0, 3)).toEqual(['diagnostic', 'scope', 'acceptanceEligible'])
    expect(written).toMatchObject({
      diagnostic: true,
      acceptanceEligible: false,
      arm: 'seeded',
      tree: 'a'.repeat(40),
      workers: [],
    })
    expect(written.candidate.warmupMs).toBe(420000)
  })

  test('reports a worker without a result as an incomplete shadow report', () => {
    expect(driver('unseeded').reportOf({ kind: 'admin' })).toEqual({
      scope: 'shadow',
      acceptance: false,
      diagnostic: true,
      kind: 'admin',
      arm: 'unseeded',
      complete: false,
      error: 'no worker result',
    })
  })

  test('archives a worker beside its report without the id ledgers', () => {
    const archived = driver('unseeded').archiveOf({
      kind: 'producer',
      uuid: 'u',
      exit: { code: 1 },
      result: { ok: false },
      input: { samples: [] },
      shadow: { heapSpaces: [] },
      seed: undefined,
      facts: {
        stages: [{ tick: 1 }],
        timing: { phaseAt: 1 },
        actions: {},
        requirements: { ok: true },
        ordinary: { offered: [1, 2, 3] },
        share: { exchanges: [1] },
      },
    })
    expect(archived.facts).toEqual({
      stages: [{ tick: 1 }],
      timing: { phaseAt: 1 },
      actions: {},
      requirements: { ok: true },
    })
    expect(archived).toMatchObject({ kind: 'producer', seed: null, input: { samples: [] } })
  })

  test('requires all four reports and, in the warm-up-seed arm, a tripped first-use bound', () => {
    const reports = (options) => KINDS.map((kind) => reportOf(kind, options))
    expect(() => driver('unseeded').verify(reports({}))).not.toThrow()
    expect(() => driver('unseeded').verify(reports({}).slice(1))).toThrow('incomplete long-window')
    expect(() =>
      driver('seeded').verify([...reports({}).slice(1), reportOf('producer', { complete: false })])
    ).toThrow('producer: no worker result')
    expect(() => driver('warmup-seed').verify(reports({ tripped: true }))).not.toThrow()
    expect(() => driver('warmup-seed').verify(reports({ tripped: false }))).toThrow(
      'did not trip the first-use bound for producer, consumer, admin, shareConsumer'
    )
  })
})
