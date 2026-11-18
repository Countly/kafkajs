const fs = require('fs')
const os = require('os')
const path = require('path')

const { LiveEvidence } = require('../liveEvidence')
const { MutationReport } = require('../report')

const SHA = 'b'.repeat(40)
const AT = '2026-11-17T10:30:00.000Z'
const APIS = [
  { apiKey: 0, minVersion: 3, maxVersion: 12 },
  { apiKey: 3, minVersion: 0, maxVersion: 13 },
]

/** @returns {object} the injectable sources of a started three-broker cluster */
const sources = () => ({
  readApiVersions: jest.fn(async () => ({ apiVersions: APIS })),
  readBrokerVersion: () => '4.4.0',
  composeImages: () => ({ 'kafkajs-kafka1-1': 'apache/kafka:4.4.0@sha256:1' }),
  treeSha: () => SHA,
  now: () => Date.parse(AT),
  env: {
    COMPOSE_FILE: 'docker-compose.yml',
    NODE_ENV: 'production',
    BROKER_BOUNCE_E2E: '1',
    KAFKAJS_LANE: 'docker',
    GITHUB_RUN_ID: '7',
  },
})

/** @returns {object} complete guard evidence */
const evidence = () => ({
  at: AT,
  lane: 'docker',
  node: 'v26.0.0',
  brokerVersion: '4.4.0',
  wireVersions: { Produce: [3, 12], Metadata: [0, 13] },
  config: {
    compose: 'docker-compose.yml',
    env: { NODE_ENV: 'production', BROKER_BOUNCE_E2E: '1' },
  },
  composeImages: { 'kafkajs-kafka1-1': 'apache/kafka:4.4.0@sha256:1' },
  runId: '7',
  sha: SHA,
})

const LIVE = {
  id: 'D/D12',
  area: 'D',
  source: 'historical',
  commandClass: 'live',
  disposition: 'live',
  target: 'src/consumer/__tests__/fixtures/rebalanceAssertions.js',
  line: 1,
  operator: 'patch',
  original: "const ASSERTED = ['classic', 'consumer']",
  mutated: "const ASSERTED = ['classic']",
  spec: 'src/consumer/__tests__/brokerBounceLive.spec.js',
  title:
    'Broker bounce oracle both group protocols stop a short-arm violation and fail any rebalance violation',
}

/**
 * @param {object|null} guard the live job's guard evidence
 * @param {object[]} rows the live ledger
 * @returns {object} the report's live section
 */
const liveBuild = (guard, rows) =>
  new MutationReport({
    selection: {
      sha: SHA,
      runId: '7',
      shards: 1,
      startedAt: '2026-11-17T10:00:00.000Z',
      records: [LIVE],
    },
    ledgers: [{ file: 'mutants-live.jsonl', rows }],
    evidence: guard,
    runId: '7',
    now: () => Date.parse('2026-11-17T11:00:00.000Z'),
  }).build()

/**
 * @param {object|null} guard the live job's guard evidence
 * @param {object[]} rows the live ledger
 * @returns {object} the report's live section
 */
const liveReport = (guard, rows) => liveBuild(guard, rows).live

describe('scripts/mutants/liveEvidence.js', () => {
  test('writes every guard-evidence field from a started cluster', async () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'live-evidence-'))
    const out = path.join(dir, 'live-guard.json')
    const io = sources()
    try {
      await LiveEvidence.main(['--out', out], io)
      const written = JSON.parse(fs.readFileSync(out, 'utf8'))
      expect(Object.keys(written).sort()).toEqual([...LiveEvidence.FIELDS].sort())
      expect(written).toEqual({
        at: AT,
        lane: 'docker',
        node: process.version,
        brokerVersion: '4.4.0',
        wireVersions: { Produce: [3, 12], Metadata: [0, 13] },
        config: {
          compose: 'docker-compose.yml',
          env: { NODE_ENV: 'production', BROKER_BOUNCE_E2E: '1' },
        },
        composeImages: { 'kafkajs-kafka1-1': 'apache/kafka:4.4.0@sha256:1' },
        runId: '7',
        sha: SHA,
      })
      expect(io.readApiVersions.mock.calls.map(([b]) => b.port)).toEqual([9092, 9095, 9098])
      expect(LiveEvidence.refusal(written, { sha: SHA })).toBeNull()
    } finally {
      fs.rmSync(dir, { recursive: true, force: true })
    }
  })

  test('refuses to write evidence when the brokers disagree or a field is empty', async () => {
    const split = sources()
    split.readApiVersions = async ({ port }) => ({
      apiVersions: port === 9098 ? APIS.slice(0, 1) : APIS,
    })
    await expect(LiveEvidence.collect(split)).rejects.toThrow(
      'the brokers offer different API versions'
    )
    const unset = sources()
    unset.env = { ...unset.env, BROKER_BOUNCE_E2E: '' }
    await expect(LiveEvidence.collect(unset)).rejects.toThrow('guard evidence lacks config')
  })

  test('a live record without complete guard evidence is invalid and never not-run-as-pass', () => {
    const partial = evidence()
    delete partial.at
    partial.wireVersions = {}
    partial.config.env.BROKER_BOUNCE_E2E = ''
    expect(LiveEvidence.refusal(partial, { sha: SHA })).toBe(
      'guard evidence lacks at, wireVersions, config'
    )
    expect(LiveEvidence.refusal(null, { sha: SHA })).toBe('guard evidence is missing')

    const killed = { id: LIVE.id, area: 'D', verdict: 'killed', reason: '' }
    const refused = liveReport(partial, [killed])
    expect(refused.areas[0]).toMatchObject({ killed: 0, invalid: 1, notRun: 0 })
    expect(refused.records[0]).toMatchObject({
      verdict: 'invalid',
      reason: 'guard evidence lacks at, wireVersions, config',
    })
    expect(refused.open).toEqual(['D/D12'])

    const missing = liveReport(null, [])
    expect(missing.areas[0]).toMatchObject({ killed: 0, notRun: 1 })
    expect(missing.open).toEqual(['D/D12'])

    const discharged = liveReport(evidence(), [killed])
    expect(discharged.areas[0]).toMatchObject({ killed: 1, invalid: 0 })
    expect(discharged.open).toEqual([])
  })

  test('a surviving live record with valid guard evidence is a high-risk survivor and stays OPEN', () => {
    const report = liveBuild(evidence(), [
      { id: LIVE.id, area: 'D', verdict: 'survived', reason: 'the witness still passes' },
    ])
    expect(report.survivors).toEqual([
      expect.objectContaining({ id: 'D/D12', operatorClass: 'live', highRisk: true }),
    ])
    expect(report.live.records).toEqual([
      { id: 'D/D12', verdict: 'survived', reason: 'the witness still passes' },
    ])
    expect(report.live.open).toEqual(['D/D12'])
    const md = MutationReport.markdown(report)
    expect(md).toContain(
      '1. **P2 · live · high risk** `src/consumer/__tests__/fixtures/rebalanceAssertions.js:1`'
    )
    expect(md).toContain('- OPEN: D/D12')
  })

  test('a live record whose guard evidence names another SHA is invalid', () => {
    const foreign = { ...evidence(), sha: 'c'.repeat(40) }
    expect(LiveEvidence.refusal(foreign, { sha: SHA })).toBe(
      `guard evidence names another tree (${'c'.repeat(40)})`
    )
    const report = liveReport(foreign, [{ id: LIVE.id, area: 'D', verdict: 'killed', reason: '' }])
    expect(report.records[0]).toMatchObject({ verdict: 'invalid' })
    expect(report.open).toEqual(['D/D12'])
    const late = { ...evidence(), at: '2026-11-17T09:00:00.000Z' }
    expect(
      LiveEvidence.refusal(late, { sha: SHA, since: Date.parse('2026-11-17T10:00:00.000Z') })
    ).toBe('guard evidence at 2026-11-17T09:00:00.000Z is outside the run')
  })
})
