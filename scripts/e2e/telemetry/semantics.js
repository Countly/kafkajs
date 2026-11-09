const assert = require('assert/strict')
const crypto = require('crypto')
const fs = require('fs')
const path = require('path')
const { canonicalUuid } = require('../../../testHelpers/telemetryFixtures')

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
 * Validates every complete Java export independently of role close policy.
 * Broker sinks and Java markers use epoch milliseconds; receive times are never compared to them.
 * @param {string} role producer, consumer or admin
 * @param {object[]} rows all UUID-selected exports in time order
 * @param {object} episode clientId, closing/closed markers, configured group and public membership observations
 * @returns {void}
 * @throws {Error} for missing, extra, duplicate or altered semantics or missing close evidence
 */
const assertJavaSamples = (role, rows, { clientId, group, membership = [] }) => {
  assert(['producer', 'consumer', 'admin'].includes(role), 'Unknown Java role')
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
 * Returns every collection timestamp, independent of broker export time.
 * @param {object} row a fully validated reporter row
 * @returns {bigint[]} epoch nanoseconds
 */
const javaCollectionTimes = (row) =>
  row.metrics.flatMap((m) => m.points.map((p) => BigInt(p.timeNs)))

/**
 * Computes collection bounds independently from broker export times and reports every final.
 * @param {object[]} rows complete validated reporter stream
 * @returns {object} replayable timing and optional-final summary
 */
const javaSampleSummary = (rows) => {
  const times = rows.flatMap(javaCollectionTimes)
  return {
    firstCollectionNs: String(times.reduce((a, b) => (a < b ? a : b))),
    lastCollectionNs: String(times.reduce((a, b) => (a > b ? a : b))),
    firstExportEpochMs: Math.min(...rows.map((r) => r.tsMs)),
    lastExportEpochMs: Math.max(...rows.map((r) => r.tsMs)),
    finals: rows
      .filter((r) => r.terminating)
      .map((r) => ({
        node: r.node,
        seq: r.seq,
        dataSha256: r.dataSha256,
        exporterEpochMs: r.tsMs,
        collectionNs: javaCollectionTimes(r).map(String),
      })),
  }
}

/**
 * Validates the entire normal stream before selecting a strictly fresh admin checkpoint.
 * @param {object[]} rows complete UUID-selected reporter stream
 * @param {object} ready raw closeReady fields
 * @param {object} episode assigned client identity
 * @returns {object|undefined} first fresh normal row, once two normals exist
 * @throws {Error} for a foreign UUID or malformed sample
 */
const selectJavaCheckpoint = (rows, ready, episode) => {
  assertJavaSamples('admin', rows, episode)
  assert(
    rows.every((r) => canonicalUuid(r.clientInstanceId) === canonicalUuid(ready.clientInstanceId)),
    'Foreign checkpoint UUID'
  )
  if (rows.filter((r) => !r.terminating).length < 2) return undefined
  return rows.find(
    (r) =>
      !r.terminating && javaCollectionTimes(r).every((t) => t > BigInt(ready.epochMs) * 1000000n)
  )
}

/**
 * Applies the fixed role close policy after checking all sample semantics.
 * @param {string} role producer, consumer or admin
 * @param {object[]} rows full ordered UUID stream
 * @param {object} episode raw lifecycle fields and, for admin, the verified checkpoint
 * @returns {void}
 * @throws {Error} for missing normals, finals or invalid close collection times
 */
const assertJavaRows = (role, rows, episode) => {
  const { closing, closed } = episode
  assert.equal(closing?.closing, true, 'Missing closing:true')
  assert.equal(closed?.closed, true, 'Missing closed:true')
  assert(
    Number.isSafeInteger(closing.epochMs) &&
      Number.isSafeInteger(closed.epochMs) &&
      closing.epochMs > 0 &&
      closed.epochMs >= closing.epochMs
  )
  assert(rows.filter((r) => !r.terminating).length >= 2, 'Missing Java normal stream')
  assertJavaSamples(role, rows, episode)
  const finals = rows.filter((r) => r.terminating)
  if (role !== 'admin') {
    assert.equal(finals.length, 1, 'Missing Java final')
    assert.equal(rows.at(-1), finals[0], 'Java final is not last')
    assert(
      finals[0].tsMs >= closing.epochMs && finals[0].tsMs <= closed.epochMs,
      'Final outside Java close episode'
    )
  } else {
    assert(episode.checkpoint && episode.cessation, 'Missing admin checkpoint/cessation')
    assert(finals.length <= 1, 'Two admin finals')
    for (const row of rows) {
      const times = javaCollectionTimes(row)
      assert(
        times.every((t) => t <= BigInt(closed.epochMs) * 1000000n),
        'Sample collected after Java close'
      )
      if (row.terminating) {
        assert(
          times.every((t) => t >= BigInt(closing.epochMs) * 1000000n),
          'Final outside Java close episode'
        )
        assert(
          rows.every(
            (other) =>
              other === row ||
              javaCollectionTimes(other).every((t) => t < times.reduce((a, b) => (a < b ? a : b)))
          ),
          'Further collection after admin final'
        )
      }
    }
  }
}

/**
 * Retains every compiler/runtime diagnostic except the pinned no-binding notices.
 * @param {string} stderr complete bounded Java stderr
 * @returns {string[]} unexpected lines
 */
const unexpectedPinnedJavaStderr = (stderr) =>
  stderr
    .split('\n')
    .filter(
      (line) =>
        line &&
        ![
          'SLF4J: Failed to load class "org.slf4j.impl.StaticLoggerBinder".',
          'SLF4J: Defaulting to no-operation (NOP) logger implementation',
          'SLF4J: See http://www.slf4j.org/codes.html#StaticLoggerBinder for further details.',
        ].includes(line)
    )

const JAVA_JAR_SHA256 = '52501b7b47510c66f898871adaf6d2968ab7246561d44ced43643a8a587f0b36'
const JAVA_REQUESTED = [
  'org.apache.kafka.producer.connection.creation.total',
  'org.apache.kafka.consumer.connection.creation.total',
  'org.apache.kafka.admin.connection.creation.total',
  'org.apache.kafka.admin.client.connection.creation.total',
  'org.apache.kafka.tf1.custom.',
].join(',')

/**
 * Requires the complete narrow selection shared by all seven interop clients. Kafka 4.3.1
 * ClientMetricsManager unions matching prefixes in a HashSet, then returns an ArrayList of it;
 * its response is neither client-kind filtered nor ordered like the configuration.
 * @param {object} response decoded successful GetTelemetrySubscriptions response
 * @returns {void}
 * @throws {Error} for altered fields or missing, extra, duplicate or changed prefixes
 */
const assertJavaSubscription = (response) => {
  assert.equal(response.errorCode, 0)
  assert.equal(response.pushIntervalMs, 1000)
  assert.equal(response.deltaTemporality, true)
  assert.deepEqual(
    [...response.requestedMetrics].sort(),
    JAVA_REQUESTED.split(',').sort(),
    'Wrong narrow Java requested metrics'
  )
}

/**
 * Admits the admin exception only for the pinned candidate and actual cache identity.
 * @param {string} role fixed Java role
 * @param {object} provenance actual input hashes and JDK observations
 * @param {object} fixture candidate source/runner stamps
 * @returns {string} fixed role policy, never an artifact-supplied switch
 * @throws {Error} for unknown roles or unqualified inputs
 */
const javaClosePolicy = (role, provenance, fixture) => {
  assert(['producer', 'consumer', 'admin'].includes(role), 'Unknown Java role')
  assert.equal(provenance.clientVersion, '4.3.1', 'Unqualified Java client version')
  const jar = provenance.cache.filter((r) => r.name === 'kafka-clients-4.3.1.jar')
  assert.equal(jar.length, 1, 'Missing pinned Java jar')
  assert.equal(jar[0].sha256, JAVA_JAR_SHA256, 'Unqualified Java jar identity')
  for (const [actual, stamped, file] of [
    [provenance.sourceSha256, fixture.clientSourceSha256, 'JavaTelemetryClient.java'],
    [provenance.runnerSha256, fixture.runnerSha256, 'run.sh'],
  ]) {
    assert.equal(actual, stamped, 'Java candidate stamp mismatch')
    assert.equal(
      actual,
      crypto
        .createHash('sha256')
        .update(fs.readFileSync(path.join(__dirname, file)))
        .digest('hex'),
      'Wrong Java candidate source'
    )
  }
  assert(provenance.jdk[role]?.jdk && provenance.jdk[role]?.vendor, 'Missing actual JDK identity')
  return role === 'admin' ? 'preclose-checkpoint-v1' : 'required-final-v1'
}

/**
 * Parses one raw stdout lifecycle marker and its independent parent receive receipt.
 * @param {object} child bounded child stdout and line receipts
 * @param {string} key marker name
 * @returns {object} raw line receipt with parsed fields
 * @throws {Error} for missing, duplicate, reordered or unreceipted lines
 */
const javaMarker = (child, key) => {
  assert.equal(child.stdout.endsWith('\n'), true, 'Partial Java stdout')
  const lines = child.stdout.trimEnd().split('\n')
  assert.deepEqual(
    child.markers.map((m) => m.line),
    lines,
    'Unreceipted Java stdout'
  )
  assert(
    child.markers.every(
      (m, i, all) => Number.isFinite(m.at) && m.at >= 0 && (!i || m.at >= all[i - 1].at)
    ),
    'Java marker receipt order'
  )
  const found = child.markers
    .map((m, index) => ({ ...m, index, fields: JSON.parse(m.line) }))
    .filter(
      (m) =>
        Object.hasOwn(m.fields, key) &&
        (key !== 'clientInstanceId' || m.fields.closed === false) &&
        (key !== 'closed' || m.fields.closed !== false || !m.fields.clientInstanceId)
    )
  assert.equal(found.length, 1, `Missing or duplicate Java ${key}`)
  return found[0]
}

/**
 * Recomputes close/exit facts and admin checkpoint/cessation from raw archived inputs.
 * Capture receipts are verified separately against all archived node file prefixes.
 * @param {string} role fixed Java role
 * @param {object[]} rows full ordered UUID stream
 * @param {object} java advertised role result
 * @param {object} child real child-close receipt, raw pipes and markers
 * @param {object} context candidate provenance, fixture, readback and assigned client ID
 * @returns {void}
 * @throws {Error} for any missing, altered or contradictory evidence
 */
const assertJavaEvidence = (role, rows, java, child, context) => {
  assert.equal(
    java.closePolicy,
    javaClosePolicy(role, context.provenance, context.fixture),
    'Wrong Java close policy'
  )
  assert.equal(context.readback.metrics, JAVA_REQUESTED, 'Wrong narrow Java subscription')
  assert.equal(context.readback.interval, 1000)
  assert(context.readback.ids.includes(java.assignedId), 'Missing Java subscription identity')
  const closing = javaMarker(child, 'closing')
  const closed = javaMarker(child, 'closed')
  const identity = javaMarker(child, 'clientInstanceId')
  const jdk = javaMarker(child, 'jdk')
  assert.deepEqual(jdk.fields, context.provenance.jdk[role])
  assert.equal(identity.fields.role, role)
  assert.equal(
    canonicalUuid(identity.fields.clientInstanceId),
    java.uuid,
    'Wrong assigned Java UUID'
  )
  assert(identity.index < closing.index && closing.index < closed.index, 'Java marker order')
  assert.equal(closing.fields.role, role)
  assert.equal(closed.fields.role, role)
  assert.equal(closing.fields.requestedCloseMs, 3000)
  assert(/^\d+$/.test(closed.fields.closeElapsedNs), 'Missing monotonic public close duration')
  assert(
    BigInt(closed.fields.closeElapsedNs) >= 0n &&
      BigInt(closed.fields.closeElapsedNs) <= 10000000000n,
    'Invalid public close duration'
  )
  assert(
    Math.abs(
      Number(BigInt(closed.fields.closeElapsedNs)) / 1e6 -
        (closed.fields.epochMs - closing.fields.epochMs)
    ) <= 100,
    'Contradictory public close duration'
  )
  assert.deepEqual(java.closing, closing.fields)
  assert.deepEqual(java.closed, closed.fields)
  assert.equal(child.code, 0, 'Java child did not exit naturally')
  assert.equal(child.signal, null)
  assert.equal(child.error, undefined)
  assert.equal(child.overflow, false, 'Java output overflow')
  assert.equal(child.pipesDrained, true, 'Java pipes not drained')
  assert.equal(child.supervisorFailure, false, 'Java supervisor timeout/kill')
  assert.deepEqual(child.liveDescendants, [], 'Surviving Java descendant')
  const interval = child.exitAt - closing.at
  assert(
    Number.isFinite(interval) && interval >= 0 && interval <= 10000,
    'Invalid Java close-to-exit interval'
  )
  assert(closed.at <= child.exitAt, 'Java close marker after child exit')
  assert.equal(java.closeToExitMs, interval, 'Contradicted Java exit interval')
  assert.equal(java.exitCode, child.code)
  assert.equal(java.exitSignal, child.signal)
  assert.deepEqual(java.liveDescendants, child.liveDescendants)
  assert.deepEqual(java.markers, child.markers)
  assert.equal(java.stdout, child.stdout)
  assert.equal(java.stderr, child.stderr)
  assert(
    rows.every((r) => canonicalUuid(r.clientInstanceId) === java.uuid),
    'Foreign Java row UUID'
  )
  assert.equal(
    java.normalExports,
    rows.filter((r) => !r.terminating).length,
    'Forged Java normal count'
  )
  assert.equal(
    java.finalExports,
    rows.filter((r) => r.terminating).length,
    'Forged Java final count'
  )
  assert.deepEqual(
    java.exports,
    rows.map(({ node, seq, terminating, dataSha256 }) => ({ node, seq, terminating, dataSha256 })),
    'Incomplete Java row catalogue'
  )
  const episode = {
    clientId: java.assignedId,
    closing: closing.fields,
    closed: closed.fields,
    group: context.group,
    membership: child.markers.map((m) => JSON.parse(m.line)).filter((m) => m.membership === true),
    checkpoint: java.checkpoint,
    cessation: java.cessation,
  }
  assertJavaRows(role, rows, episode)
  assert.deepEqual(java.timing, javaSampleSummary(rows), 'Contradicted Java sample timing')
  if (role !== 'admin') {
    assert(
      !child.markers.some((m) => JSON.parse(m.line).closeReady || JSON.parse(m.line).closeReleased),
      'Foreign admin policy'
    )
    return
  }
  const ready = javaMarker(child, 'closeReady')
  const release = javaMarker(child, 'closeReleased')
  assert.equal(ready.fields.closeReady, true)
  assert.equal(release.fields.closeReleased, true)
  assert(
    identity.index < ready.index && ready.index < release.index && release.index < closing.index,
    'Admin checkpoint marker order'
  )
  for (const marker of [ready, release]) {
    assert.equal(marker.fields.role, 'admin')
    assert.equal(marker.fields.clientId, java.assignedId)
    assert.equal(canonicalUuid(marker.fields.clientInstanceId), java.uuid)
    assert(
      /^[A-Za-z0-9_-]{22}$/.test(marker.fields.checkpointId),
      'Invalid admin checkpoint identity'
    )
    assert(Number.isSafeInteger(marker.fields.epochMs))
  }
  assert.equal(release.fields.checkpointId, ready.fields.checkpointId, 'Foreign admin checkpoint')
  const operation = ready.fields.operation
  assert.equal(operation.api, 'listTopics')
  assert.equal(operation.success, true)
  assert(
    Number.isSafeInteger(operation.startedEpochMs) &&
      Number.isSafeInteger(operation.completedEpochMs) &&
      operation.startedEpochMs > 0 &&
      operation.startedEpochMs <= operation.completedEpochMs &&
      operation.completedEpochMs <= ready.fields.epochMs,
    'Incomplete admin foreground operation'
  )
  assert(
    ready.fields.epochMs <= release.fields.epochMs &&
      release.fields.epochMs <= closing.fields.epochMs,
    'Admin epoch order'
  )
  const { proof, command } = java.checkpoint
  assert(
    Number.isFinite(proof.at) && proof.at >= ready.at && proof.at - ready.at <= 8000,
    'Admin checkpoint deadline'
  )
  const preceding = rows.filter((r) => r.seq <= proof.cursors[r.node])
  const selected = selectJavaCheckpoint(preceding, ready.fields, episode)
  assert(selected, 'Missing fresh admin checkpoint')
  for (const key of ['node', 'seq', 'dataSha256'])
    assert.equal(release.fields[key], selected[key], 'Wrong checkpoint proving row')
  const raw = `CLOSE ${ready.fields.checkpointId} ${ready.fields.clientInstanceId} ${selected.node} ${selected.seq} ${selected.dataSha256}\n`
  assert.equal(command.raw, raw, 'Wrong admin close command')
  assert(Buffer.byteLength(raw) <= 512)
  assert(
    command.sentAt >= proof.at &&
      command.completedAt >= command.sentAt &&
      command.completedAt <= child.exitAt &&
      release.at >= command.sentAt,
    'Admin acknowledgement order'
  )
  assert(release.at - ready.at <= 10000, 'Admin acknowledgement expired')
  const { drain, quiet } = java.cessation
  assert(drain.length >= 2 && quiet.length >= 2, 'Untimed admin cessation')
  assert(drain[0].at >= child.exitAt, 'Admin drain before pipe close')
  assert(drain.at(-1).at - drain[0].at >= 2000, 'Short admin drain')
  assert.deepEqual(quiet[0], drain.at(-1), 'Gap in admin drain/quiet observation')
  assert(quiet.at(-1).at - quiet[0].at >= 2000, 'Short admin quiet window')
  for (const window of [drain, quiet]) {
    assert(
      window.every(
        (r, i) =>
          Number.isFinite(r.at) &&
          (!i || (r.at >= window[i - 1].at && r.at - window[i - 1].at <= 500))
      ),
      'Incomplete admin timed capture'
    )
    for (const node of ['1', '2', '3']) {
      assert(
        window.every(
          (r, i) =>
            Number.isSafeInteger(r.cursors[node]) &&
            r.cursors[node] >= 0 &&
            (!i || r.cursors[node] >= window[i - 1].cursors[node])
        ),
        'Invalid admin node cursors'
      )
    }
  }
  const end = quiet.at(-1)
  assert(
    rows.every((r) => r.seq <= end.cursors[r.node]),
    'Admin row after quiet capture'
  )
  assert(!rows.some((r) => r.seq > quiet[0].cursors[r.node]), 'Admin activity during quiet window')
}

/**
 * Owns the admin acknowledgement task alongside child supervision. Dependencies expose bounded
 * pipe writes, all-node capture and polling; no child exit can be mistaken for checkpoint success.
 */
class JavaAdminHandshake {
  /** @param {object} deps child, identity, polling, captures, rows, bounded send and clock */
  constructor(deps) {
    this.deps = deps
    this.scope = new AbortController()
  }

  /** @returns {void} cancels polling during case cleanup */
  abort() {
    this.scope.abort(new Error('Admin checkpoint cancelled'))
  }

  /**
   * Joins checkpoint work even on early child failure or case cancellation.
   * @returns {Promise<object>} raw command receipt and the complete proving capture
   * @throws {Error} for invalid samples, early exit, deadline or pipe failure
   */
  async run() {
    const { child, signal } = this.deps
    const abort = () => this.abort()
    signal.addEventListener('abort', abort, { once: true })
    if (signal.aborted) abort()
    const exit = child.done.then(() => {
      this.abort()
      throw new Error('Admin exited before checkpoint release')
    })
    const task = this.release()
    try {
      return await Promise.race([task, exit])
    } finally {
      this.abort()
      await Promise.allSettled([task])
      signal.removeEventListener('abort', abort)
    }
  }

  /**
   * Sends exactly one identity/row-bound command after every preceding row passes semantics.
   * @returns {Promise<object>} checkpoint inputs retained for live and replay verification
   * @throws {Error} for missing ready/stream, malformed rows, deadline or failed write
   */
  async release() {
    const { child, clientId, find, capture, rowsOf, send, now } = this.deps
    const ready = await find(this.scope.signal, 'admin closeReady', 30000, () =>
      child.marker('closeReady')
    )
    assert.equal(ready.fields.role, 'admin', 'Foreign admin checkpoint role')
    assert.equal(ready.fields.clientId, clientId, 'Foreign admin checkpoint client')
    assert(
      Number.isSafeInteger(ready.fields.epochMs) && ready.fields.epochMs > 0,
      'Missing checkpoint epoch'
    )
    assert(/^[A-Za-z0-9_-]{22}$/.test(ready.fields.checkpointId), 'Missing checkpoint identity')
    assert.equal(ready.fields.operation?.api, 'listTopics')
    assert.equal(ready.fields.operation.success, true, 'Incomplete admin work')
    const operation = ready.fields.operation
    assert(
      Number.isSafeInteger(operation.startedEpochMs) &&
        Number.isSafeInteger(operation.completedEpochMs) &&
        operation.startedEpochMs > 0 &&
        operation.startedEpochMs <= operation.completedEpochMs &&
        operation.completedEpochMs <= ready.fields.epochMs,
      'Incomplete admin work'
    )
    const uuid = canonicalUuid(ready.fields.clientInstanceId)
    const identity = child.marker('clientInstanceId')
    assert.equal(
      canonicalUuid(identity.fields.clientInstanceId),
      uuid,
      'Foreign admin checkpoint UUID'
    )
    const proof = await find(this.scope.signal, 'fresh complete admin checkpoint', 8000, () => {
      const receipt = capture()
      const selected = selectJavaCheckpoint(
        rowsOf(uuid).filter((r) => r.seq <= receipt.cursors[r.node]),
        ready.fields,
        { clientId }
      )
      return selected && { receipt, selected }
    })
    const { node, seq, dataSha256 } = proof.selected
    const raw = `CLOSE ${ready.fields.checkpointId} ${ready.fields.clientInstanceId} ${node} ${seq} ${dataSha256}\n`
    assert(
      /^CLOSE [A-Za-z0-9_-]{22} [A-Za-z0-9_-]{22} [123] [1-9][0-9]{0,9} [0-9a-f]{64}\n$/.test(
        raw
      ) && Buffer.byteLength(raw) <= 512,
      'Invalid admin close command'
    )
    this.scope.signal.throwIfAborted()
    const command = { raw, sentAt: now() }
    await send(raw)
    command.completedAt = now()
    return { proof: proof.receipt, command }
  }
}

/**
 * Joins every timed capture to full archived node prefixes before using any UUID filter.
 * @param {object[]} receipts all-node timed capture receipts
 * @param {string} directory archived sink directory
 * @returns {void}
 * @throws {Error} for missing nodes, partial lines, changed prefix bytes or reused sequences
 */
const assertJavaCaptures = (receipts, directory) => {
  for (const receipt of receipts) {
    assert.deepEqual(
      Object.keys(receipt.nodes).sort(),
      ['1', '2', '3'],
      'Missing Java node capture'
    )
    assert.deepEqual(
      Object.keys(receipt.cursors).sort(),
      ['1', '2', '3'],
      'Missing Java node cursor'
    )
    for (const node of ['1', '2', '3']) {
      const { bytes, sha256 } = receipt.nodes[node]
      const full = fs.readFileSync(path.join(directory, `exports-${node}.jsonl`))
      assert(
        Number.isSafeInteger(bytes) && bytes >= 0 && bytes <= full.length,
        'Invalid capture length'
      )
      const prefix = full.subarray(0, bytes)
      assert(!bytes || prefix.at(-1) === 10, 'Partial Java node capture')
      assert.equal(
        crypto.createHash('sha256').update(prefix).digest('hex'),
        sha256,
        'Changed Java capture prefix'
      )
      const rows = bytes
        ? prefix
            .toString()
            .trimEnd()
            .split('\n')
            .map(JSON.parse)
            .sort((a, b) => a.seq - b.seq)
        : []
      assert(
        rows.every((r, i) => String(r.node) === node && r.seq === i + 1),
        'Reused Java node sequence'
      )
      assert.equal(receipt.cursors[node], rows.length, 'Forged Java capture cursor')
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
  assertJavaSamples,
  selectJavaCheckpoint,
  javaClosePolicy,
  javaMarker,
  assertJavaEvidence,
  assertJavaCaptures,
  assertJavaSubscription,
  JAVA_REQUESTED,
  unexpectedPinnedJavaStderr,
  javaSampleSummary,
  JavaAdminHandshake,
  assertJsCounter,
  assertCustomShape,
  ordinaryOwnership,
}
