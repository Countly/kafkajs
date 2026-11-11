const assert = require('assert/strict')
const fetch = require('../../testHelpers/wire/shareFetchV2')
const heartbeat = require('../../testHelpers/wire/shareGroupHeartbeatV1')
const acknowledge = require('../../testHelpers/wire/shareAcknowledgeV2')

const SCENARIOS = Object.freeze({
  'share-kafka44': ['lifecycle', 'code-69', 'deleted-topic', 'forget', 'two-ranges'],
  'share-dlq-kafka44': [
    'dlq-reject-metadata',
    'dlq-reject-copy',
    'dlq-release-metadata',
    'dlq-release-copy',
  ],
})

/** @param {object[]} wire Raw body hex and same-socket exchange metadata. @returns {object[]} Independently decoded requests and answers. @throws {Error} Malformed bytes. */
const decodeWire = (wire) =>
  wire
    .filter(({ apiKey }) => [76, 78, 79].includes(apiKey))
    .map((entry) => {
      const codec = entry.apiKey === 76 ? heartbeat : entry.apiKey === 78 ? fetch : acknowledge
      return {
        ...entry,
        request: codec.readRequest(Buffer.from(entry.requestHex, 'hex')),
        answer:
          entry.responseHex === null
            ? null
            : codec.readResponse(Buffer.from(entry.responseHex, 'hex')),
      }
    })

/** @param {object[]} wire Decoded exchanges. @param {string} topicId Source topic. @param {number} partition Partition index. @returns {Array<{offset: string, type: number}>} Confirmed terminal decisions, preserving multiplicity and offset order. */
const decisions = (wire, topicId, partition) =>
  wire.flatMap(({ apiKey, request, answer }) => {
    if (![78, 79].includes(apiKey) || !answer || answer.errorCode !== 0) return []
    return request.topics
      .filter((topic) => topic.topicId === topicId)
      .flatMap(({ partitions }) =>
        partitions.flatMap((part) => {
          if (part.partitionIndex !== partition) return []
          const result = answer.responses
            .find((topic) => topic.topicId === topicId)
            ?.partitions.find((row) => row.partitionIndex === partition)
          if (!result || (apiKey === 78 ? result.acknowledgeErrorCode : result.errorCode) !== 0)
            return []
          return part.acknowledgementBatches.flatMap(
            ({ firstOffset, lastOffset, acknowledgeTypes }) => {
              const types = []
              for (let i = BigInt(firstOffset); i <= BigInt(lastOffset); i++)
                types.push({
                  offset: String(i),
                  type:
                    acknowledgeTypes.length === 1
                      ? acknowledgeTypes[0]
                      : acknowledgeTypes[Number(i - BigInt(firstOffset))],
                })
              return types
            }
          )
        })
      )
  })

/**
 * Checks original observations against scenario-specific expectations without a live broker or candidate share runtime.
 * @param {string} scenario Registered scenario name.
 * @param {object} proof Handler, source, destination and acknowledgement observations.
 * @param {object[]} raw Original same-socket request/response bodies.
 * @returns {void}
 * @throws {Error} Missing observations or a violated oracle.
 */
const assertScenario = (scenario, proof, raw) => {
  const wire = decodeWire(raw)
  assert(wire.length > 0, 'no share wire observations')
  const member = wire.filter(({ label }) => label === 'member')
  if (scenario.startsWith('dlq-')) {
    const release = scenario.includes('-release-')
    const copy = scenario.endsWith('-copy')
    assert.deepEqual(proof.deliveryCounts, release ? [1, 2] : [1])
    assert(proof.identities.every((identity) => identity === proof.source.identity))
    assert.equal(proof.records.length, 1, 'exactly one DLQ record required')
    const [record] = proof.records
    const expected = {
      '__dlq.errors.topic': proof.source.topic,
      '__dlq.errors.partition': '0',
      '__dlq.errors.offset': '0',
      '__dlq.errors.group': proof.group,
      '__dlq.errors.delivery.count': release ? '2' : '1',
      '__dlq.errors.message': release
        ? 'Offset delivery count exceeded the threshold.'
        : 'Offset rejected by client.',
    }
    assert.deepEqual(
      record.headers,
      Object.fromEntries(
        Object.entries(expected).map(([name, value]) => [name, Buffer.from(value).toString('hex')])
      )
    )
    assert.deepEqual(proof.source.headers, {
      'source-only': Buffer.from('must-not-copy').toString('hex'),
    })
    assert.equal(record.key, copy ? proof.source.key : null)
    assert.equal(record.value, copy ? proof.source.value : null)
    assert.equal(proof.quietMs, 3000)
    assert.deepEqual(
      decisions(member, proof.topicId, 0),
      (release ? [2, 2] : [3]).map((type) => ({ offset: '0', type }))
    )
    assert.deepEqual(proof.crashes, [])
  } else if (scenario === 'lifecycle') {
    assert.deepEqual(proof.handled, [{ identity: proof.identity, deliveryCount: 1 }])
    assert.deepEqual(decisions(member, proof.topicId, 0), [{ offset: '0', type: 1 }])
    const beats = member.filter(({ apiKey }) => apiKey === 76)
    assert.deepEqual(
      [
        beats.at(-1).request.memberEpoch,
        beats.at(-1).answer.memberEpoch,
        beats.at(-1).answer.errorCode,
      ],
      [-1, -1, 0]
    )
    assert.deepEqual(proof.crashes, [])
  } else if (scenario === 'code-69') {
    const beats = member.filter(({ apiKey }) => apiKey === 76)
    assert.equal(beats.filter(({ request }) => request.memberEpoch === 0).length, 1)
    assert.equal(beats.filter(({ request }) => request.memberEpoch > 0).length, 0)
    assert.equal(beats[0].answer.errorCode, 69)
    assert.equal(beats[0].answer.errorMessage, `Group ${proof.group} is not a share group.`)
    assert.equal(proof.failure.code, 69)
    assert.equal(proof.failure.retriable, false)
    assert.equal(proof.classicMembers, 1)
    assert(!member.some(({ apiKey }) => apiKey === 78))
  } else if (scenario === 'deleted-topic') {
    const failed = member.find(
      ({ apiKey, answeredAt, answer }) =>
        apiKey === 78 &&
        answeredAt >= proof.deletedAt &&
        answer?.responses.some(
          ({ topicId, partitions }) =>
            topicId === proof.doomedId &&
            partitions.some(({ errorCode }) => [3, 100].includes(errorCode))
        )
    )
    assert(failed, 'no deleted-topic partition error')
    assert(
      raw.some(
        ({ apiKey, label, requestedAt }) =>
          apiKey === 3 && label === 'member' && requestedAt >= failed.answeredAt
      ),
      'no metadata refresh after partition error'
    )
    assert(proof.handled.some(({ topic, value }) => topic === proof.kept && value === proof.value))
    assert(
      proof.acknowledgements.some(
        ({ topic, firstOffset, type, error }) =>
          topic === proof.kept && firstOffset === '1' && type === 'accept' && error === null
      )
    )
    assert.deepEqual(proof.crashes, [])
  } else if (scenario === 'forget') {
    assert(proof.lost.length > 0 && proof.kept.length > 0)
    const requests = wire.filter(({ apiKey, label }) => apiKey === 78 && label === 'A')
    for (const partition of proof.lost) {
      const ack = requests.findIndex(({ request }) =>
        request.topics.some(
          ({ topicId, partitions }) =>
            topicId === proof.topicId &&
            partitions.some(
              ({ partitionIndex, acknowledgementBatches }) =>
                partitionIndex === partition && acknowledgementBatches.length
            )
        )
      )
      assert(ack >= 0)
      const listed = (request) =>
        request.topics.some(
          ({ topicId, partitions }) =>
            topicId === proof.topicId &&
            partitions.some(({ partitionIndex }) => partitionIndex === partition)
        )
      const forgotten = requests.flatMap(({ request }, index) =>
        request.forgottenTopicsData.some(
          ({ topicId, partitions }) => topicId === proof.topicId && partitions.includes(partition)
        )
          ? [index]
          : []
      )
      assert.deepEqual(forgotten, [ack])
      assert(
        !requests.slice(ack + 1).some(({ request }) => listed(request)),
        'lost partition reappeared'
      )
      assert(
        proof.acknowledgements.some(
          (entry) => entry.partition === partition && entry.error === null
        )
      )
      assert(
        requests.some(
          ({ requestedAt, answer }) => requestedAt >= proof.settledAt && answer?.errorCode === 0
        ),
        'no later successful fetch'
      )
    }
    for (const partition of proof.kept)
      assert(
        !requests.some(({ request }) =>
          request.forgottenTopicsData.some(({ partitions }) => partitions.includes(partition))
        )
      )
    assert.deepEqual(proof.crashes, [])
  } else if (scenario === 'two-ranges') {
    assert(
      member.some(
        ({ apiKey, answer }) =>
          apiKey === 78 &&
          answer?.responses.some(
            ({ topicId, partitions }) =>
              topicId === proof.topicId &&
              partitions.some(
                ({ acquiredRecords }) =>
                  JSON.stringify(acquiredRecords) ===
                  JSON.stringify([
                    { firstOffset: '0', lastOffset: '9', deliveryCount: 2 },
                    { firstOffset: '20', lastOffset: '29', deliveryCount: 2 },
                  ])
              )
          )
      )
    )
    assert.equal(proof.deliveries.length, 50)
    for (let offset = 0; offset < 30; offset++) {
      const twice = offset < 10 || offset >= 20
      assert.deepEqual(
        proof.deliveries
          .filter((entry) => entry.offset === offset)
          .map(({ deliveryCount, identity }) => ({ deliveryCount, identity })),
        (twice ? [1, 2] : [1]).map((deliveryCount) => ({
          deliveryCount,
          identity: proof.identities[offset],
        }))
      )
      assert.deepEqual(
        decisions(member, proof.topicId, 0)
          .filter((entry) => entry.offset === String(offset))
          .map(({ type }) => type),
        twice ? [2, 1] : [1]
      )
    }
    assert.deepEqual(proof.crashes, [])
  } else throw new Error(`Unknown share scenario ${scenario}`)
}

/**
 * Requires all registered cases, independently checked observations and successful cleanup of each owned fixture.
 * @param {string} name Evidence file stem.
 * @param {object[]} rows Stamped JSONL records.
 * @returns {string[]} Every validation failure.
 */
const verifyShareKafka44 = (name, rows) => {
  const problems = []
  for (const scenario of SCENARIOS[name] || []) {
    try {
      const cases = rows.filter((row) => row?.case === scenario)
      const observed = cases.filter(({ event }) => event === 'observations')
      const completed = cases.filter(({ event }) => event === 'case-complete')
      assert.equal(observed.length, 1, `${scenario}: needs one observation row`)
      assert.equal(completed.length, 1, `${scenario}: needs one completion row`)
      assert.equal(cases.length, 2, `${scenario}: needs only observations and completion`)
      const [row] = observed
      assert.equal(row.scenario, completed[0].scenario)
      assert(row.scenario && row.features.finalizedFeatures['share.version'].maxVersionLevel === 2)
      assert(Object.keys(row.wireVersions).length > 0)
      assertScenario(scenario, row.proof, row.wire)
      assert.equal(completed[0].status, 'passed')
      assert.deepEqual(completed[0].cleanup, {
        groupsAbsent: true,
        topicsAbsent: true,
        adminDisconnected: true,
      })
      assert(completed[0].groups.length > 0 && completed[0].topics.length > 0)
    } catch (error) {
      problems.push(`${scenario}: ${error.message}`)
    }
  }
  return problems
}

module.exports = { SCENARIOS, assertScenario, verifyShareKafka44 }
