const apiKeys = require('../../protocol/requests/apiKeys')
const { selectVersion } = require('../../protocol/requests')
const { KafkaJSServerDoesNotSupportApiKey } = require('../../errors')

const names = new Map(Object.entries(apiKeys).map(([name, key]) => [key, name]))

/**
 * Decides whether a broker can run the required share APIs: first every required key must be
 * advertised, then each must overlap an implemented version, then `share.version` must be
 * finalized at level 1 or above (a higher level passes). A refusal names the first key that failed
 * its stage, and a feature refusal names `required[0]`. Only the finalized map counts: an API
 * range, a supported feature or a scripted profile is never read as finalized support.
 *
 * The function is pure: it opens no connection, changes none of its inputs and builds an error only
 * on a refusal. A feature epoch of -1 means the broker reported no finalized features, so the
 * level is `null` and the decision is left to the runtime answer; a reported epoch with no
 * `share.version` entry means level 0. Runtime code 35 is classified by the caller that sent the
 * request.
 * @param {object} input A capability snapshot, not a live broker reference.
 * @param {Object<number, {minVersion?: number, maxVersion: number}>} input.versions Advertised ranges by API key.
 * @param {Object<string, {maxVersionLevel: number}>} input.finalizedFeatures Finalized features by name.
 * @param {string|number|bigint} input.finalizedFeaturesEpoch Exact feature epoch from ApiVersions.
 * @param {Object<number, {versions: string[]}>} input.implemented Request definitions by API key.
 * @param {number[]} input.required Nonempty ordered keys: `[76, 78, 79]` for a consumer, one key for an admin call.
 * @returns {object} `{ ok: true }`, or `{ ok: false, reason, apiKey, apiName, shareVersion }` where
 *   `reason` is `api-not-advertised`, `api-version-range` or `feature-disabled`; a range refusal adds
 *   `brokerRange` and `clientRange`, each `[min, max]`.
 * @throws {Error} For invalid internal input or an unexpected selector failure; a broker refusal is a result, not a throw.
 */
module.exports = function shareCapability({
  versions,
  finalizedFeatures,
  finalizedFeaturesEpoch,
  implemented,
  required,
}) {
  const shareVersion =
    BigInt(finalizedFeaturesEpoch) >= 0n
      ? (finalizedFeatures['share.version']?.maxVersionLevel ?? 0)
      : null
  const refusal = (reason, apiKey, detail) => ({
    ok: false,
    reason,
    apiKey,
    apiName: names.get(apiKey),
    shareVersion,
    ...detail,
  })

  for (const apiKey of required) {
    if (!versions[apiKey] || versions[apiKey].maxVersion == null) {
      return refusal('api-not-advertised', apiKey)
    }
  }
  for (const apiKey of required) {
    try {
      selectVersion(versions, apiKey, implemented[apiKey])
    } catch (error) {
      if (!(error instanceof KafkaJSServerDoesNotSupportApiKey)) throw error
      const clientVersions = implemented[apiKey].versions.map(Number)
      return refusal('api-version-range', apiKey, {
        brokerRange: [versions[apiKey].minVersion ?? 0, versions[apiKey].maxVersion],
        clientRange: [Math.min(...clientVersions), Math.max(...clientVersions)],
      })
    }
  }
  if (shareVersion !== null && shareVersion < 1) return refusal('feature-disabled', required[0])
  return { ok: true }
}
