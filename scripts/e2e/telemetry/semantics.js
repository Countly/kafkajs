const assert = require('assert/strict')

/**
 * Requires the exact complete accepted nonempty stream, with no duplicates or invented entries.
 * @param {object[]} claimed archived catalogue
 * @param {object[]} actual independently derived catalogue
 * @returns {void}
 * @throws {Error} for a different catalogue
 */
const assertCatalogue = (claimed, actual) => {
  const keys = (rows) => rows.map(({ owner, requestKey }) => `${owner}:${requestKey}`).sort()
  assert.deepEqual(keys(claimed), keys(actual), 'Incomplete Push catalogue')
  assert.equal(new Set(keys(claimed)).size, claimed.length, 'Incomplete Push catalogue')
}

/**
 * Rejects every early request, including requests followed by a later on-time success.
 * @param {object[]} requests complete ordered request sequence
 * @param {number} completion preceding response or local completion in the same clock domain
 * @param {number} interval minimum response-relative delay
 * @returns {void}
 * @throws {Error} for any premature write
 */
const assertCadence = (requests, completion, interval) => {
  for (const request of requests) {
    assert(request.writeAt >= completion + interval, 'Early telemetry request')
    completion = request.response?.responseAt ?? request.writeAt
  }
}

/**
 * Validates every complete Java export, including the UUID-specific final in its close episode.
 * Broker sinks and Java markers use epoch milliseconds; receive times are never compared to them.
 * @param {string} role producer, consumer or admin
 * @param {object[]} rows all UUID-selected exports in time order
 * @param {object} episode clientId and literal closing/closed markers
 * @returns {void}
 * @throws {Error} for missing, extra, duplicate or altered semantics or missing close evidence
 */
const assertJavaRows = (role, rows, { clientId, closing, closed }) => {
  assert.equal(closing?.closing, true, 'Missing closing:true')
  assert.equal(closed?.closed, true, 'Missing closed:true')
  assert(Number.isSafeInteger(closing.epochMs) && closed.epochMs >= closing.epochMs)
  assert(rows.filter((row) => !row.terminating).length >= 2)
  assert.equal(rows.filter((row) => row.terminating).length, 1)
  assert.equal(rows.at(-1).terminating, true)
  assert(
    rows.at(-1).tsMs >= closing.epochMs && rows.at(-1).tsMs <= closed.epochMs,
    'Final outside Java close episode'
  )
  const counter = `org.apache.kafka.${role}.connection.creation.total`
  const names = [
    counter,
    ...['gauge.double', 'gauge.long', 'sum.total'].map((s) => `org.apache.kafka.tf1.custom.${s}`),
  ]
  for (const [index, row] of rows.entries()) {
    assert.equal(row.parse, 'ok')
    assert.equal(row.truncated, false)
    assert.equal(row.clientId, clientId)
    assert.equal(row.pushIntervalMs, 1000)
    assert.equal(row.resourceMetricsCount, 1)
    assert.equal(row.metricsTotal, 4)
    assert.equal(row.metricsKept, 4)
    assert.equal(row.pointsTotal, 4)
    assert.equal(row.resources.length, 1)
    assert.equal(row.resources[0].droppedAttributes, 0)
    assert.deepEqual(
      row.resources[0].attrs,
      rows[0].resources[0].attrs,
      'Java resource catalogue changed'
    )
    const attrs = row.resources[0].attrs
    assert.equal(new Set(attrs.map(([key]) => key)).size, attrs.length)
    assert(
      attrs.every(
        ([key, type]) =>
          [
            'client_rack',
            'group_id',
            'group_instance_id',
            'group_member_id',
            'transactional_id',
          ].includes(key) && type === 'string'
      )
    )
    assert.deepEqual(
      row.metrics.map((metric) => metric.name).sort(),
      [...names].sort(),
      'Unexpected Java metric catalogue'
    )
    for (const metric of row.metrics) {
      assert.equal(metric.pointsInMessage, 1)
      assert.equal(metric.res, 0)
      assert.equal(metric.points.length, 1)
      const [point] = metric.points
      assert.deepEqual(point.attrs, [])
      assert.equal(point.flags, 0)
      assert(BigInt(point.timeNs) > 0n)
      if (metric.name === counter || metric.name.endsWith('sum.total')) {
        assert.equal(metric.kind, 'SUM')
        assert.equal(metric.monotonic, true)
        assert.equal(metric.temporality, 'AGGREGATION_TEMPORALITY_DELTA')
        assert.equal(point.valueKind, 'AS_DOUBLE')
        assert(BigInt(point.startNs) <= BigInt(point.timeNs))
        const value = Number(point.asDouble)
        assert(Number.isFinite(value) && value >= 0)
        if (metric.name.endsWith('sum.total')) assert.equal(value, index === 0 ? 7 : 0)
      } else {
        assert.equal(metric.kind, 'GAUGE')
        if (metric.name.endsWith('gauge.double')) {
          assert.equal(point.valueKind, 'AS_DOUBLE')
          assert.equal(point.asDouble, '42.5')
          assert.equal(point.asDoubleBits, '4045400000000000')
        } else {
          assert.equal(point.valueKind, 'AS_INT')
          assert.equal(point.asInt, '4242')
        }
      }
    }
  }
}

/**
 * Proves ownership of every physical connection, including setup-only sockets. The initial
 * foreground bootstrap is explicit; every other socket must carry actual ordinary work.
 * @param {object} transcript records and physical sockets
 * @returns {object[]} node/role ownership baseline (counts deliberately excluded)
 * @throws {Error} for telemetry, an undecided extra connection or an unowned setup-only socket
 */
const ordinaryOwnership = ({ records, sockets }) => {
  const roles = []
  for (const socket of sockets) {
    assert.equal(socket.kind, 'normal', 'Unowned or additional quiet socket')
    const work = records.filter(
      (r) => r.type === 'request' && r.socketId === socket.id && ![17, 18, 36].includes(r.apiKey)
    )
    assert(socket.foregroundBootstrap || work.length > 0, 'Unowned setup-only socket')
    roles.push(`${socket.node}:${socket.foregroundBootstrap ? 'bootstrap' : 'ordinary'}`)
  }
  return [...new Set(roles)].sort()
}

module.exports = { assertCatalogue, assertCadence, assertJavaRows, ordinaryOwnership }
