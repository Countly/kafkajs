const assert = require('node:assert/strict')
const { KafkaJSServerDoesNotSupportApiKey } = require('../src/errors')

/** Checks floor observations without guessing capability from a release string. */
class FloorCapabilities {
  /**
   * Compares a connected broker's real negotiation with the retained probe and codecs.
   * @param {object} broker Connected Broker with its current generation's versions.
   * @param {string[]} names Required API names; an absent capability fails the fixture.
   * @param {string} floor Recorded probe entry selected for this invocation.
   * @returns {object} Safe range facts and selected versions, one per required API.
   * @throws {AssertionError|Error} A missing probe, stale negotiation or unusable codec.
   */
  static fromBroker(broker, names, floor) {
    const recorded = require('../scripts/protocol/brokerRanges.json')[floor]
    const { rangeOf } = require('../scripts/e2e/apiVersionsProbe')
    const keys = require('../src/protocol/requests/apiKeys')
    const { requests } = require('../src/protocol/requests')
    assert(recorded, 'Recorded floor probe missing')
    const tables = { recorded: {}, observed: {}, implemented: {} }
    for (const name of names) {
      const range = broker.versions[keys[name]]
      tables.recorded[name] = rangeOf(recorded, keys[name])
      tables.observed[name] = range && [range.minVersion, range.maxVersion]
      tables.implemented[name] = requests[name].versions.map(Number)
    }
    const oracle = new FloorCapabilities(tables)
    return Object.fromEntries(
      names.map((name) => [
        name,
        {
          recorded: tables.recorded[name],
          offered: tables.observed[name],
          implemented: tables.implemented[name],
          selected: oracle.version(name),
        },
      ])
    )
  }

  /**
   * Binds independently recorded, observed and implemented ranges.
   * @param {object} ranges Named maps with recorded, observed and implemented entries.
   * @returns {FloorCapabilities} A test-only range oracle.
   * @throws {AssertionError} When a required range is missing or malformed.
   */
  constructor({ recorded, observed, implemented }) {
    this.recorded = recorded
    this.observed = observed
    this.implemented = implemented
  }

  /**
   * Requires a genuine negotiated overlap and returns its highest version.
   * @param {string} api API name in all three independently built tables.
   * @returns {number} The highest implemented version inside the broker range.
   * @throws {AssertionError} On absent, malformed, stale or nonoverlapping ranges.
   */
  version(api) {
    const offered = this.observed[api]
    const recorded = this.recorded[api]
    const versions = this.implemented[api]
    for (const range of [offered, recorded]) {
      assert(Array.isArray(range) && range.length === 2, `${api}: missing range`)
      assert(
        range.every((v) => Number.isInteger(v) && v >= 0),
        `${api}: invalid range`
      )
      assert(range[0] <= range[1], `${api}: reversed range`)
    }
    assert.deepEqual(offered, recorded, `${api}: observed range differs from the recorded probe`)
    assert(Array.isArray(versions) && versions.length > 0, `${api}: no client versions`)
    assert(
      versions.every((v) => Number.isInteger(v) && v >= 0),
      `${api}: invalid client version`
    )
    const common = versions.filter((v) => v >= offered[0] && v <= offered[1])
    assert(common.length > 0, `${api}: no overlap`)
    return Math.max(...common)
  }

  /**
   * Checks the actual refusal and both ranges; it never constructs a production error.
   * @param {Error} error The error returned by the public operation or its preserved cause.
   * @param {string} api API whose unavailable option was requested.
   * @param {number} required Minimum version needed by that option.
   * @param {string} option Exact option named by the refusal.
   * @returns {void} After all diagnostic and capability assertions pass.
   * @throws {AssertionError} On a wrong class, missing diagnostic or available capability.
   */
  unsupported(error, api, required, option) {
    assert(this.version(api) < required, `${api}: option is supported`)
    assert(error instanceof KafkaJSServerDoesNotSupportApiKey, `${api}: wrong refusal class`)
    const client = this.implemented[api]
    const offered = this.observed[api]
    assert(error.message.includes(api), 'API name missing')
    assert(error.message.includes(option), 'Option name missing')
    assert(
      new RegExp(`(?:needs|requires) (?:${api} )?v${required}(?:[+; ]|$)`).test(error.message),
      'Required version missing'
    )
    assert(
      error.message.includes(`v${Math.min(...client)}-v${Math.max(...client)}`),
      'Client range missing'
    )
    assert(error.message.includes(`v${offered[0]}-v${offered[1]}`), 'Broker range missing')
  }

  /**
   * Refuses an incomplete variant population, including a silently omitted variant.
   * @param {string[]} required Exact expected distinct variants.
   * @param {string[]} observed Actual successful variants, including duplicates.
   * @returns {void} After exact identity equality.
   * @throws {AssertionError} On a missing, extra or duplicated variant.
   */
  static variants(required, observed) {
    assert.equal(new Set(required).size, required.length, 'Duplicate required variant')
    assert.equal(new Set(observed).size, observed.length, 'Duplicate observed variant')
    assert.deepEqual([...observed].sort(), [...required].sort(), 'Variant population differs')
  }
}

module.exports = FloorCapabilities
