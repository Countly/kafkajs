const assert = require('assert')
const fs = require('fs')
const path = require('path')

/**
 * Whether a JSON value is an object rather than an array or null.
 * @param {unknown} value
 * @returns {boolean}
 */
const isObject = (value) => value !== null && typeof value === 'object' && !Array.isArray(value)

/**
 * Whether two endpoints form an ordered, non-negative integer range.
 * @param {number} min
 * @param {number} max
 * @returns {boolean}
 */
const isRange = (min, max) =>
  Number.isInteger(min) && min >= 0 && Number.isInteger(max) && max >= min

/**
 * Reads independent broker evidence by version. Numeric API keys and raw feature fields stay in
 * their capture representation; no production name or feature conversion supplies expectations.
 * @param {string} version the recorded broker's major.minor.patch version
 * @returns {{ provenance: object, apiRanges: Record<string, number[]>, features: object }}
 * @throws {Error} when required capture data or provenance is missing or malformed, identifying
 *   the broker recording as incomplete
 */
const readBrokerCapture = (version) => {
  try {
    assert(/^\d+\.\d+\.\d+$/.test(version), 'invalid broker version')
    const file = path.join(__dirname, '../scripts/protocol/captures', version, 'apiVersions.json')
    const capture = JSON.parse(fs.readFileSync(file, 'utf8'))
    const { provenance, apiRanges, features } = capture
    assert(isObject(provenance), 'missing provenance')
    assert.strictEqual(provenance.brokerVersion, version, 'provenance broker version differs')
    assert(typeof provenance.source === 'string' && provenance.source.length > 0, 'missing source')
    assert(Array.isArray(provenance.captures) && provenance.captures.length > 0, 'missing sources')
    for (const source of provenance.captures) {
      assert(typeof source.file === 'string' && source.file.length > 0, 'missing capture source')
      assert(/^[a-f0-9]{64}$/.test(source.sha256), 'invalid source digest')
      assert(
        /^\d{4}-\d{2}-\d{2}(?:T\d{2}:\d{2}:\d{2}Z)?$/.test(source.capturedOn),
        'invalid capture date'
      )
      assert(
        typeof source.howProduced === 'string' && source.howProduced.length > 0,
        'missing capture method'
      )
    }

    assert(isObject(apiRanges) && Object.keys(apiRanges).length > 0, 'missing numeric API ranges')
    for (const [key, range] of Object.entries(apiRanges)) {
      assert(/^(0|[1-9]\d*)$/.test(key), 'invalid numeric API key')
      assert(
        Array.isArray(range) && range.length === 2 && isRange(...range),
        `invalid API ${key} range`
      )
    }

    assert(isObject(features), 'missing feature response')
    assert.strictEqual(features.errorCode, 0, 'unsuccessful feature response')
    assert.strictEqual(
      features.apiKeyCount,
      Object.keys(apiRanges).length,
      'incomplete numeric API set'
    )
    assert(
      typeof features.finalizedFeaturesEpoch === 'string' &&
        /^\d+$/.test(features.finalizedFeaturesEpoch) &&
        BigInt(features.finalizedFeaturesEpoch) <= 9223372036854775807n,
      'invalid finalized-features epoch'
    )
    for (const [field, min, max] of [
      ['supportedFeatures', 'min', 'max'],
      ['finalizedFeatures', 'minVersionLevel', 'maxVersionLevel'],
    ]) {
      const entries = features[field]
      assert(Array.isArray(entries), `missing ${field}`)
      const names = new Set()
      for (const entry of entries) {
        assert(
          isObject(entry) && typeof entry.name === 'string' && entry.name.length > 0,
          `invalid ${field} entry`
        )
        assert(!names.has(entry.name), `duplicate ${field} entry`)
        names.add(entry.name)
        assert(isRange(entry[min], entry[max]), `invalid ${field} range`)
      }
    }

    return capture
  } catch (error) {
    throw new Error(`Incomplete broker recording for ${version}: ${error.message}`)
  }
}

module.exports = { readBrokerCapture }
