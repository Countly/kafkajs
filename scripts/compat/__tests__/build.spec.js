'use strict'

const { buildModel, toJsonArtifact, serializeJson, asciiCompare } = require('../build')
const trackedApis = require('../tracked-apis.json')
const config = require('../config.json')

describe('scripts/compat/build', () => {
  const model = buildModel()

  it('builds one row per tracked API, sorted by api key', () => {
    expect(model.apis).toHaveLength(trackedApis.tracked.length)
    const keys = model.apis.map(api => api.apiKey)
    expect(keys).toEqual(keys.slice().sort((a, b) => a - b))
    expect(model.kafkaTag).toBe(config.kafkaTag)
  })

  it('gives every row the full B2 field set', () => {
    for (const api of model.apis) {
      expect(Object.keys(api).sort()).toEqual([
        'apiKey',
        'apiKeysName',
        'belowFloor',
        'deprecatedVersions',
        'flexibleFrom',
        'forkName',
        'forkVersions',
        'gap',
        'kafkaName',
        'latestVersionUnstable',
        'notes',
        'onFloor',
        'planned',
        'validVersions',
      ])
    }
  })

  it('never produces a sentinel gap for the real data', () => {
    const sentinels = model.apis.filter(
      api => api.gap === 'unknown' || api.gap.indexOf('ahead:') === 0
    )
    expect(sentinels).toEqual([])
  })

  it('resolves each waiver against the API it names', () => {
    for (const waiver of model.waivers) {
      expect(typeof waiver.kafkaMin).toBe('number')
      expect(waiver.until).toBe('phase-2 exit')
    }
  })

  it('is deterministic: two builds serialize identically', () => {
    expect(serializeJson(toJsonArtifact(buildModel()))).toBe(
      serializeJson(toJsonArtifact(buildModel()))
    )
  })

  it('serializes with a trailing newline and no timestamp', () => {
    const serialized = serializeJson(toJsonArtifact(model))
    expect(serialized.endsWith('}\n')).toBe(true)
    expect(serialized).not.toMatch(/\d{4}-\d{2}-\d{2}T/)
    expect(JSON.parse(serialized).generatedFrom).toEqual({
      kafkaTag: config.kafkaTag,
      tool: 'scripts/compat',
      schemaVersion: config.schemaVersion,
    })
  })

  it('keeps the transient fork findings out of the committed artifact', () => {
    expect(model.findings).toBeDefined()
    expect(toJsonArtifact(model).findings).toBeUndefined()
  })

  it('sorts waivers with the same plain ASCII comparator it exports', () => {
    const keys = model.waivers.map(waiver => waiver.api)
    expect(keys).toEqual(keys.slice().sort(asciiCompare))
  })

  it('asciiCompare orders mixed-case and underscore keys byte-wise, not locale-aware', () => {
    const input = ['b_b', 'B_A', 'a', 'Z', 'A_B']
    expect(input.slice().sort(asciiCompare)).toEqual(['A_B', 'B_A', 'Z', 'a', 'b_b'])
  })
})
