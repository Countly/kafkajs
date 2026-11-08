const TelemetryMetrics = require('../../metrics')

/**
 * Builds the maximum legal collector using production validation and numeric observations: 128
 * custom series at the largest descriptor the public fields allow (name 200, unit 16, eight keys of
 * 63 bytes and eight values of 256 bytes, charge 2768 each, 354304 of the 524288 byte allowance)
 * and 64 observed node pairs, so 257 points are selected.
 * @param {string} kind public client kind
 * @returns {{metrics:object, subscription:object, slots:object[]}} owned finite input population
 */
const maximum = (kind) => {
  const metrics = new TelemetryMetrics({ kind })
  const subscription = { revision: 1n, requestedMetrics: ['*'], deltaTemporality: true }
  metrics.select(subscription)
  metrics.observeConnection({})
  for (let i = 0; i < 128; i++) {
    metrics.register({
      name: `com.example.resource.${i}`.padEnd(200, 'x'),
      unit: 'u'.repeat(16),
      type: 'sum',
      value: 9007199254740993n,
      attributes: Object.fromEntries(
        Array.from({ length: 8 }, (_, a) => [`a${a}`.padEnd(63, 'k'), 'x'.repeat(256)])
      ),
    })
  }
  const slots = Array.from({ length: 64 }, (_, nodeId) => {
    const slot = metrics.bindNode({ nodeId, endpoint: `b${nodeId}:9092`, endpointRevision: {} })
    metrics.observeRequest(slot, nodeId)
    return slot
  })
  return { metrics, subscription, slots }
}

module.exports = { maximum }
