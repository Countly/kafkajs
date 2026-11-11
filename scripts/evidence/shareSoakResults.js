const { isDeepStrictEqual: same } = require('node:util')
const Decoder = require('../../src/protocol/decoder')
const { readRequestHeader } = require('../../testHelpers/fakeSocket')

/** @param {object[]} rows Comparable primitive rows. @returns {string[]} Stable unique rows across broker fan-out. */
const canonical = (rows) => [...new Set(rows.map((row) => JSON.stringify(row)))].sort()

/**
 * Projects only fields used by the original corpus, configuration and terminal predicates.
 * The same projection applies to public results and independently decoded wire replies.
 * @param {string} name Original operation name.
 * @param {*} result Original public result.
 * @returns {object[]|undefined} Comparable primitive rows, or an unrelated operation.
 */
const resultRows = (name, result) => {
  if (name === 'send')
    return result.map(({ partition, baseOffset, errorCode }) => ({
      partition,
      baseOffset,
      errorCode,
    }))
  if (name === 'describeConfigs')
    return result.resources.map((r) => ({
      type: r.resourceType,
      name: r.resourceName,
      errorCode: r.errorCode,
      entries: r.configEntries.map((e) => ({
        name: e.configName,
        value: e.configValue,
        source: e.configSource,
        synonyms: e.configSynonyms ?? [],
      })),
    }))
  if (name === 'fetchTopicMetadata')
    return result.topics.map((t) => ({
      name: t.name,
      id: t.topicId,
      partitions: t.partitions.map((p) => ({
        partition: p.partitionId,
        leader: p.leader,
        replicas: p.replicas,
      })),
    }))
  if (name === 'listTopics') return result.map((name) => ({ name }))
  if (name === 'describeCluster')
    return result.brokers.map(({ nodeId, host, port }) => ({ nodeId, host, port }))
  if (name === 'listGroups') return result.groups.map(({ groupId }) => ({ groupId }))
  if (name === 'describeShareGroups')
    return result.groups.map((g) => ({
      groupId: g.groupId,
      state: g.state,
      errorCode: g.errorCode,
      members: g.members.map((m) => ({
        memberId: m.memberId,
        assignment: m.assignment.map(({ topic, partitions }) => ({ topic, partitions })),
      })),
    }))
  if (name.startsWith('listShareGroupOffsets:'))
    return result.map((t) => ({
      topic: t.topic,
      topicId: t.topicId,
      partitions: t.partitions.map(({ partition, startOffset, lag, leaderEpoch, errorCode }) => ({
        partition,
        startOffset,
        lag,
        leaderEpoch,
        errorCode,
      })),
    }))
  if (name === 'fetchTopicOffsets')
    return result.map(({ partition, high }) => ({ partition, high }))
}

/**
 * Reads latest-offset request selection, keeping earliest replies out of the terminal high end.
 * @param {object} request Original framed request.
 * @returns {Set<string>} Latest topic/partition selections.
 * @throws {Error} On malformed or unsupported framing.
 */
const latestOffsets = (request) => {
  const bytes = Buffer.from(request.bytes, 'base64')
  const header = readRequestHeader(bytes)
  const decoder = new Decoder(bytes.subarray(header.byteLength))
  decoder.readInt32()
  if (header.apiVersion >= 2) decoder.readInt8()
  const flexible = header.apiVersion >= 6
  const array = (read) => (flexible ? decoder.readUVarIntArray(read) : decoder.readArray(read))
  const tags = () => {
    if (flexible) decoder.readTaggedFields()
  }
  const latest = new Set()
  array(() => {
    const topic = flexible ? decoder.readUVarIntString() : decoder.readString()
    array(() => {
      const partition = decoder.readInt32()
      if (header.apiVersion >= 4) decoder.readInt32()
      if (decoder.readInt64().toString() === '-1') latest.add(`${topic}:${partition}`)
      tags()
    })
    tags()
  })
  return latest
}

/**
 * Converts successful decoded fields into their owning public operation's result shape.
 * @param {object} request Original request and operation identity.
 * @param {object} decoded Low-level versioned response.
 * @returns {object[]|undefined} Comparable rows for that reply.
 */
const answerRows = (request, decoded) => {
  const name = request.operation
  if (request.apiKey === 0 && name === 'send')
    return resultRows(
      name,
      decoded.topics.flatMap((t) => t.partitions)
    )
  if (request.apiKey === 32 && name === 'describeConfigs') return resultRows(name, decoded)
  if (request.apiKey === 3 && ['fetchTopicMetadata', 'listTopics'].includes(name)) {
    const topics = decoded.topicMetadata.filter((t) => name !== 'listTopics' || !t.isInternal)
    return resultRows(
      name,
      name === 'listTopics'
        ? topics.map((t) => t.topic)
        : {
            topics: topics.map((t) => ({
              name: t.topic,
              topicId: t.topicId,
              partitions: t.partitionMetadata,
            })),
          }
    )
  }
  if (request.apiKey === 3 && name === 'describeCluster') return resultRows(name, decoded)
  if (request.apiKey === 60 && name === 'describeCluster')
    return resultRows(name, { brokers: decoded.brokers.map((b) => ({ ...b, nodeId: b.brokerId })) })
  if (request.apiKey === 16 && name === 'listGroups') return resultRows(name, decoded)
  if (request.apiKey === 77 && name === 'describeShareGroups')
    return resultRows(name, {
      groups: decoded.groups.map((g) => ({
        ...g,
        state: g.groupState,
        members: g.members.map((m) => ({
          memberId: m.memberId,
          assignment: m.assignment.topicPartitions.map((t) => ({
            topic: t.topicName,
            partitions: t.partitions,
          })),
        })),
      })),
    })
  if (request.apiKey === 90 && name?.startsWith('listShareGroupOffsets:'))
    return resultRows(
      name,
      decoded.groups.flatMap((g) =>
        g.topics.map((t) => ({
          topic: t.topicName,
          topicId: t.topicId,
          partitions: t.partitions.map((p) => ({
            ...p,
            partition: p.partitionIndex,
            lag: p.lag === '-1' ? null : p.lag,
          })),
        }))
      )
    )
  if (request.apiKey === 2 && name === 'fetchTopicOffsets') {
    const latest = latestOffsets(request)
    return decoded.responses.flatMap((t) =>
      t.partitions
        .filter((p) => latest.has(`${t.topic}:${p.partition}`))
        .map((p) => ({ partition: p.partition, high: p.offset }))
    )
  }
}

/**
 * Reconciles operation results with their original decoded replies and derived observations.
 * Broker fan-out is joined by operation id; another call's equal-looking reply cannot replace it.
 * @param {object[]} rows One scope's original observations.
 * @param {object[]} answers Decoded successful replies associated with original requests.
 * @returns {string[]} Missing or divergent wire/result/observation links.
 */
const resultProblems = (rows, answers) => {
  const problems = []
  const operations = rows.filter(
    (r) => r.event === 'soak-wire' && r.kind === 'operation' && r.status === 'passed'
  )
  const operationIds = new Set(operations.map((o) => o.id))
  const byOperation = new Map()
  for (const answer of answers) {
    const id = answer.request.operationId
    if (!byOperation.has(id)) byOperation.set(id, [])
    byOperation.get(id).push(answer)
  }
  const linked = new Set()
  for (const operation of operations) {
    const replies = byOperation.get(operation.id) ?? []
    const compared = replies.flatMap(({ request, decoded }) => {
      const values = answerRows(request, decoded)
      return values === undefined ? [] : [{ values }]
    })
    if (!compared.length) {
      if (Object.hasOwn(operation, 'result'))
        problems.push(
          `Original operation ${operation.id}/${operation.name}: missing decoded values`
        )
      continue
    }
    try {
      for (const { request, decoded } of replies) {
        if (
          request.apiKey === 0 &&
          operation.name === 'send' &&
          decoded.topics.some((t) => t.topicName !== operation.target?.topic)
        )
          throw new Error('decoded topic differs')
        if (
          request.apiKey === 90 &&
          operation.name.startsWith('listShareGroupOffsets:') &&
          decoded.groups.some((g) => g.groupId !== operation.target?.groupId)
        )
          throw new Error('decoded group differs')
      }
      const expected = resultRows(operation.name, operation.result)
      const actual = compared.flatMap((r) => r.values)
      if (!same(canonical(expected), canonical(actual))) throw new Error('decoded values differ')
      linked.add(JSON.stringify([operation.name, canonical(expected)]))
    } catch (error) {
      problems.push(`Original operation ${operation.id}/${operation.name}: ${error.message}`)
    }
  }
  for (const { request, decoded } of answers) {
    if (answerRows(request, decoded) !== undefined && !operationIds.has(request.operationId))
      problems.push(`Missing original operation result ${request.operationId}/${request.operation}`)
  }
  const requireResult = (name, result) => {
    const expected = canonical(resultRows(name, result))
    if (!linked.has(JSON.stringify([name, expected])))
      problems.push(`Original ${name} observation lacks matching decoded operation`)
  }
  for (const row of rows) {
    if (row.event === 'soak-producer') requireResult('send', row.sent)
    if (row.event === 'soak-configuration') {
      for (const read of row.config.reads)
        requireResult('describeConfigs', { resources: [read.resource] })
      requireResult('describeConfigs', row.config.topicConfig)
      requireResult('fetchTopicMetadata', row.config.topicMetadata)
    }
    if (row.event === 'soak-original-cycle' || row.event === 'soak-original-final') {
      for (const position of row.observation.positions) {
        requireResult('listShareGroupOffsets:named', position.named)
        requireResult('listShareGroupOffsets:all', position.all)
        requireResult('fetchTopicOffsets', position.ends)
      }
    }
    if (row.event === 'soak-membership')
      for (const read of row.reads) requireResult('describeShareGroups', read)
    if (row.event === 'soak-cleanup' && row.step === 'config-reset')
      requireResult('describeConfigs', row.read)
  }
  return problems
}
module.exports = { resultProblems }
