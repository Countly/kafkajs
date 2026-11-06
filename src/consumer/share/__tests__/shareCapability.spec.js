const shareCapability = require('../shareCapability')
const { requests } = require('../../../protocol/requests')
const apiKeys = require('../../../protocol/requests/apiKeys')
const ranges = require('../../../../scripts/protocol/brokerRanges.json')
const features = require('../../../../scripts/protocol/brokerFeatures.json')
const { readBrokerCapture } = require('../../../../testHelpers/brokerCapture')
const {
  KafkaJSShareGroupsNotSupportedError,
  KafkaJSServerDoesNotSupportApiKey,
} = require('../../../errors')

const NAMES = [
  'ShareGroupHeartbeat',
  'ShareGroupDescribe',
  'ShareFetch',
  'ShareAcknowledge',
  'DescribeShareGroupOffsets',
  'AlterShareGroupOffsets',
  'DeleteShareGroupOffsets',
]
const implemented = Object.fromEntries(NAMES.map((name) => [apiKeys[name], requests[name]]))
const all = Object.fromEntries(
  NAMES.map((name) => [
    apiKeys[name],
    {
      minVersion: Number(requests[name].versions[0]),
      maxVersion: Number(requests[name].versions[0]),
    },
  ])
)

/** The consumer's required keys, each with its name and the one version this client implements. */
const CONSUMER = [
  [76, 'ShareGroupHeartbeat', 1],
  [78, 'ShareFetch', 2],
  [79, 'ShareAcknowledge', 2],
]

/**
 * @param {object} [overrides] Capability input changes.
 * @returns {object} Independent valid defaults: every share API at its implemented version and
 *   `share.version` finalized at level 1.
 */
const input = (overrides = {}) => ({
  versions: all,
  finalizedFeaturesEpoch: '7',
  finalizedFeatures: { 'share.version': { minVersionLevel: 1, maxVersionLevel: 1 } },
  implemented,
  required: [76, 78, 79],
  ...overrides,
})

/**
 * What a consumer must be told about a broker, derived from its capture alone: the numeric ranges
 * the broker advertised and the finalized level it reported, never from the committed tables or
 * from the code under test.
 * @param {Record<string, number[]>} apiRanges Captured `[min, max]` by numeric API key.
 * @param {number} level Finalized `share.version` level, 0 when the feature is not finalized.
 * @returns {object} `{ ok: true }` or the refusal the stages in order must produce.
 */
const judge = (apiRanges, level) => {
  for (const [key, name] of CONSUMER) {
    if (!Object.hasOwn(apiRanges, key)) {
      return { ok: false, reason: 'api-not-advertised', apiKey: key, apiName: name }
    }
  }
  for (const [key, name, version] of CONSUMER) {
    const [min, max] = apiRanges[key]
    if (version < min || version > max) {
      return {
        ok: false,
        reason: 'api-version-range',
        apiKey: key,
        apiName: name,
        brokerRange: [min, max],
        clientRange: [version, version],
      }
    }
  }
  return level >= 1
    ? { ok: true }
    : { ok: false, reason: 'feature-disabled', apiKey: 76, apiName: 'ShareGroupHeartbeat' }
}

describe('share capability', () => {
  describe.each(Object.keys(ranges))('the recorded %s broker', (line) => {
    test('is judged as its capture says, from the committed ranges and features', () => {
      const { apiRanges, features: evidence } = readBrokerCapture(line)
      const shared = evidence.finalizedFeatures.find((feature) => feature.name === 'share.version')
      const level = shared ? shared.maxVersionLevel : 0
      const versions = Object.fromEntries(
        NAMES.filter((name) => Object.hasOwn(ranges[line], name)).map((name) => [
          apiKeys[name],
          { minVersion: ranges[line][name][0], maxVersion: ranges[line][name][1] },
        ])
      )
      const finalizedFeatures = Object.fromEntries(
        Object.entries(features[line]).map(([name, value]) => [name, { maxVersionLevel: value }])
      )

      const result = shareCapability(
        input({
          versions,
          finalizedFeatures,
          finalizedFeaturesEpoch: evidence.finalizedFeaturesEpoch,
        })
      )

      const expected = judge(apiRanges, level)
      if (expected.ok) expect(result).toEqual({ ok: true })
      else expect(result).toMatchObject({ ...expected, shareVersion: level })
    })
  })

  test.each([1, 2])('accepts a finalized share.version level of %s', (maxVersionLevel) => {
    expect(
      shareCapability(input({ finalizedFeatures: { 'share.version': { maxVersionLevel } } }))
    ).toEqual({ ok: true })
  })

  test('refuses a preview ShareFetch range even when the feature is finalized', () => {
    expect(
      shareCapability(input({ versions: { ...all, 78: { minVersion: 1, maxVersion: 1 } } }))
    ).toMatchObject({
      reason: 'api-version-range',
      apiKey: 78,
      apiName: 'ShareFetch',
      shareVersion: 1,
      brokerRange: [1, 1],
      clientRange: [2, 2],
    })
  })

  test('checks every advertisement before any overlap or feature refusal', () => {
    const versions = { ...all, 76: { minVersion: 0, maxVersion: 0 } }
    delete versions[79]
    expect(shareCapability(input({ versions, finalizedFeatures: {} }))).toMatchObject({
      reason: 'api-not-advertised',
      apiKey: 79,
    })
  })

  test.each([{}, { 'share.version': { maxVersionLevel: 0 } }])(
    'treats the finalized features %j as disabled',
    (finalizedFeatures) => {
      expect(shareCapability(input({ finalizedFeatures }))).toMatchObject({
        reason: 'feature-disabled',
        shareVersion: 0,
        apiKey: 76,
        apiName: 'ShareGroupHeartbeat',
      })
    }
  )

  test.each(['-1', -1, -1n])(
    'defers an absent feature epoch %p to runtime',
    (finalizedFeaturesEpoch) => {
      expect(shareCapability(input({ finalizedFeatures: {}, finalizedFeaturesEpoch }))).toEqual({
        ok: true,
      })
    }
  )

  test.each(['9223372036854775807', 9223372036854775807n])(
    'accepts the largest int64 feature epoch %p',
    (finalizedFeaturesEpoch) => {
      expect(
        shareCapability(input({ finalizedFeaturesEpoch, finalizedFeatures: {} }))
      ).toMatchObject({ reason: 'feature-disabled', shareVersion: 0 })
    }
  )

  test('does not read share support out of the API version map', () => {
    expect(
      shareCapability(
        input({
          versions: { ...all, 'share.version': { maxVersionLevel: 1 } },
          finalizedFeatures: {},
        })
      )
    ).toMatchObject({ reason: 'feature-disabled' })
  })

  test.each([
    [77, 'ShareGroupDescribe'],
    [90, 'DescribeShareGroupOffsets'],
    [91, 'AlterShareGroupOffsets'],
    [92, 'DeleteShareGroupOffsets'],
  ])('checks only admin key %s', (key, apiName) => {
    expect(shareCapability(input({ versions: { [key]: all[key] }, required: [key] }))).toEqual({
      ok: true,
    })
    expect(shareCapability(input({ versions: {}, required: [key] }))).toMatchObject({
      reason: 'api-not-advertised',
      apiKey: key,
      apiName,
    })
    expect(shareCapability(input({ required: [key], finalizedFeatures: {} }))).toMatchObject({
      reason: 'feature-disabled',
      apiKey: key,
      apiName,
    })
  })

  test('uses the inherited selector and reports both non-overlapping ranges', () => {
    expect(
      shareCapability(input({ versions: { ...all, 78: { minVersion: 3, maxVersion: 4 } } }))
    ).toMatchObject({
      reason: 'api-version-range',
      apiKey: 78,
      brokerRange: [3, 4],
      clientRange: [2, 2],
    })
  })

  test('keeps caller snapshots unchanged', () => {
    const value = input()
    const before = JSON.stringify(value)
    expect(shareCapability(value)).toEqual({ ok: true })
    expect(JSON.stringify(value)).toBe(before)
  })

  test('materializes a named non-retriable subclass from the pure refusal', () => {
    const result = shareCapability(input({ versions: {} }))
    const error = new KafkaJSShareGroupsNotSupportedError('ShareGroupHeartbeat is absent', result)
    expect(error).toBeInstanceOf(KafkaJSServerDoesNotSupportApiKey)
    expect(error).toMatchObject({
      apiKey: 76,
      apiName: 'ShareGroupHeartbeat',
      reason: 'api-not-advertised',
      retriable: false,
    })
  })
})
