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
 * Supplies resource expectations from the fixture's explicit Properties and public group metadata.
 * Kafka 4.3.1 ClientTelemetryProvider uses original configuration, so rack, static member and
 * transaction fields are absent here. AbstractCoordinator adds group_member_id after JoinGroup;
 * a poll that changes membership brackets that transition independently of telemetry rows.
 * @param {string} role Java role
 * @param {string} group configured consumer group
 * @param {object[]} membership public groupMetadata observations bracketing each consumer poll
 * @param {string} timeNs metric collection epoch, never broker receive time
 * @returns {Array<Array>} exact permitted catalogues during that source-qualified transition
 * @throws {Error} for missing or inconsistent consumer observations
 */
const javaResources = (role, group, membership, timeNs) => {
  assert(['producer', 'consumer', 'admin'].includes(role))
  if (role !== 'consumer') return [[]]
  assert(typeof group === 'string' && group.length > 0)
  assert(membership.length > 0, 'Missing public Java membership observations')
  assert(
    membership.some((r) => r.memberId.length > 0),
    'Java consumer never joined'
  )
  let member = ''
  const time = Number(BigInt(timeNs) / 1000000n)
  const base = [['group_id', 'string', group]]
  const catalogue = (id) => (id ? [...base, ['group_member_id', 'string', id]] : [...base])
  for (const observation of membership) {
    assert.equal(observation.membership, true)
    assert(
      Number.isSafeInteger(observation.fromEpochMs) &&
        observation.epochMs >= observation.fromEpochMs
    )
    assert.equal(observation.beforeMemberId, member)
    assert.equal(typeof observation.memberId, 'string')
    if (time < observation.fromEpochMs) return [catalogue(member)]
    if (time <= observation.epochMs) return [catalogue(member), catalogue(observation.memberId)]
    member = observation.memberId
  }
  assert(member, 'Java consumer never joined')
  return [catalogue(member)]
}

/**
 * Requires the common connection counter on every JS interop export, including finals.
 * @param {string} kind public JS kind
 * @param {object} decoded complete independently decoded metric tree
 * @returns {void}
 * @throws {Error} for a missing, duplicate or altered counter or series
 */
const assertJsCounter = (kind, decoded) => {
  const counter = `org.apache.kafka.${kind === 'share' ? 'consumer' : kind}.connection.creation.total`
  const metrics = decoded.metrics.filter((m) => m.name === counter)
  assert.equal(metrics.length, 1, 'Missing or duplicate JS common counter')
  const [metric] = metrics
  assert.equal(metric.kind, 'SUM')
  assert.equal(metric.unit, '1')
  assert.equal(metric.monotonic, true, 'JS common counter must be monotonic')
  assert.equal(metric.temporality, 'AGGREGATION_TEMPORALITY_DELTA')
  assert.equal(metric.pointsInMessage, metric.points.length)
  assert.equal(metric.points.length, 1)
  const identities = metric.points.map((p) => JSON.stringify([...p.attrs].sort()))
  assert.equal(new Set(identities).size, identities.length, 'Duplicate JS counter series')
  for (const point of metric.points) {
    assert.deepEqual(point.attrs, [])
    assert.equal(point.valueKind, 'AS_INT')
    assert(BigInt(point.asInt) >= 0n)
    assert(BigInt(point.startNs) <= BigInt(point.timeNs) && BigInt(point.timeNs) > 0n)
    assert.equal(point.flags, 0)
  }
}

/**
 * Checks custom cardinalities and identities before any name-to-point projection.
 * @param {object} decoded complete snapshot
 * @param {string[]} names independently required metric names for this lifecycle window
 * @returns {void}
 * @throws {Error} for duplicate metrics/series, missing resources or incomplete point counts
 */
const assertCustomShape = (decoded, names) => {
  assert.equal(decoded.resourceMetricsCount, 1, 'Custom resource count differs')
  assert.equal(decoded.resources.length, 1, 'Custom resource catalogue count differs')
  assert.equal(decoded.resources[0].droppedAttributes, 0)
  assert.equal(decoded.metricsTotal, names.length, 'Custom metric count differs')
  assert.equal(decoded.metricsKept, names.length, 'Custom retained metric count differs')
  assert.equal(decoded.pointsTotal, names.length, 'Custom total point count differs')
  assert.deepEqual(
    decoded.metrics.map((m) => m.name).sort(),
    [...names].sort(),
    'Custom metric catalogue differs'
  )
  for (const metric of decoded.metrics) {
    assert.equal(metric.res, 0)
    assert.equal(metric.pointsInMessage, 1)
    assert.equal(metric.points.length, 1)
    assert.deepEqual(metric.points[0].attrs, [['series', 'string', 'live']])
  }
}

/**
 * Validates every complete Java export, including the UUID-specific final in its close episode.
 * Broker sinks and Java markers use epoch milliseconds; receive times are never compared to them.
 * @param {string} role producer, consumer or admin
 * @param {object[]} rows all UUID-selected exports in time order
 * @param {object} episode clientId, closing/closed markers, configured group and public membership observations
 * @returns {void}
 * @throws {Error} for missing, extra, duplicate or altered semantics or missing close evidence
 */
const assertJavaRows = (role, rows, { clientId, closing, closed, group, membership = [] }) => {
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
  const counter = `org.apache.kafka.${role === 'admin' ? 'admin.client' : role}.connection.creation.total`
  const names = [
    counter,
    ...['gauge.double', 'gauge.long', 'sum.total'].map((s) => `org.apache.kafka.tf1.custom.${s}`),
  ]
  for (const [index, row] of rows.entries()) {
    assert.equal(row.parse, 'ok')
    assert.equal(row.truncated, false)
    assert.equal(row.clientId, clientId)
    assert.equal(row.pushIntervalMs, 1000)
    assert.equal(row.resourceMetricsCount, 4)
    assert.equal(row.metricsTotal, 4)
    assert.equal(row.metricsKept, 4)
    assert.equal(row.pointsTotal, 4)
    assert.equal(row.resources.length, 4)
    assert.deepEqual(row.metrics.map((metric) => metric.res).sort(), [0, 1, 2, 3])
    for (const metric of row.metrics) {
      const resource = row.resources[metric.res]
      assert.equal(resource.droppedAttributes, 0)
      const attrs = resource.attrs
      assert.equal(
        new Set(attrs.map(([key]) => key)).size,
        attrs.length,
        'Duplicate Java resource key'
      )
      const expected = javaResources(role, group, membership, metric.points[0].timeNs)
      assert(
        expected.some((catalogue) => {
          try {
            assert.deepEqual([...attrs].sort(), catalogue.sort())
            return true
          } catch {
            return false
          }
        }),
        'Unexpected Java resource catalogue'
      )
    }
    assert.deepEqual(
      row.metrics.map((metric) => metric.name).sort(),
      [...names].sort(),
      'Unexpected Java metric catalogue'
    )
    for (const metric of row.metrics) {
      assert.equal(metric.unit, '', 'Unexpected Java descriptor unit')
      assert.equal(metric.pointsInMessage, 1)
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

module.exports = {
  assertCatalogue,
  assertCadence,
  assertJavaRows,
  assertJsCounter,
  assertCustomShape,
  ordinaryOwnership,
}
