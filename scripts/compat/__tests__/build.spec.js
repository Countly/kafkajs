'use strict'

const fs = require('fs')
const path = require('path')
const { buildModel, toJsonArtifact, serializeJson, asciiCompare } = require('../build')
const trackedApis = require('../tracked-apis.json')
const config = require('../config.json')

describe('scripts/compat/build', () => {
  const model = buildModel()

  it('builds one row per tracked API, sorted by api key', () => {
    expect(model.apis).toHaveLength(trackedApis.tracked.length)
    const keys = model.apis.map((api) => api.apiKey)
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
      (api) => api.gap === 'unknown' || api.gap.indexOf('ahead:') === 0
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
    const keys = model.waivers.map((waiver) => waiver.api)
    expect(keys).toEqual(keys.slice().sort(asciiCompare))
  })

  it('asciiCompare orders mixed-case and underscore keys byte-wise, not locale-aware', () => {
    const input = ['b_b', 'B_A', 'a', 'Z', 'A_B']
    expect(input.slice().sort(asciiCompare)).toEqual(['A_B', 'B_A', 'Z', 'a', 'b_b'])
  })

  describe('the pinned baseline', () => {
    const REPO = path.join(__dirname, '..', '..', '..')
    const read = (...parts) => fs.readFileSync(path.join(REPO, ...parts), 'utf8')

    it('is Apache Kafka 4.4.0', () => {
      expect(config.kafkaTag).toBe('4.4.0')
    })

    it('is the tag of the message snapshot, the error snapshot and every generated file', () => {
      const tag = config.kafkaTag

      expect(
        fs.existsSync(path.join(__dirname, '..', 'messages', tag, 'DeleteGroupsResponse.json'))
      ).toBe(true)
      expect(JSON.parse(read('scripts', 'compat', 'errors', `${tag}.json`)).kafkaTag).toBe(tag)
      expect(JSON.parse(read('docs', 'kafka-compatibility.json')).generatedFrom.kafkaTag).toBe(tag)
      expect(read('docs', 'KafkaCompatibility.md')).toContain(
        `Apache Kafka **${tag}** message definitions`
      )
      expect(read('src', 'protocol', 'flexibleVersions.js')).toContain(
        `Generated from the Apache Kafka ${tag} message definitions`
      )
      const provenance = JSON.parse(read('scripts', 'protocol', 'provenance.json'))
      expect(new Set(Object.values(provenance).map((entry) => entry.kafkaTag))).toEqual(
        new Set([tag])
      )
    })

    it('keeps the 4.3.1 snapshots beside it, with the same files as the pinned one', () => {
      const names = (tag) => fs.readdirSync(path.join(__dirname, '..', 'messages', tag)).sort()

      const addedAfter431 = [
        'UnregisterControllerRequest.json',
        'UnregisterControllerResponse.json',
      ]
      expect(names('4.3.1')).toEqual(
        names(config.kafkaTag).filter((n) => !addedAfter431.includes(n))
      )
      expect(fs.existsSync(path.join(__dirname, '..', 'errors', '4.3.1.json'))).toBe(true)
    })

    it('reports DeleteGroups at the 4.4.0 maximum, 3', () => {
      const api = model.apis.find((candidate) => candidate.kafkaName === 'DeleteGroups')

      expect(api.validVersions).toEqual([0, 3])
      expect(api.forkVersions).toEqual([0, 1, 2, 3])
      expect(api.gap).toBe('current')
    })
  })
})
