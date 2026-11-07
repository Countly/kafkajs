const { isDeepStrictEqual } = require('util')
const Broker = require('../src/broker')
const fixtures = require('./shareFixtures')
const { bounded, pollUntil } = require('../src/producer/__tests__/fixtures/bounded')
const { describeError } = require('../src/producer/__tests__/fixtures/liveProbe')
const shareFetch = require('./wire/shareFetchV2')
const shareAcknowledge = require('./wire/shareAcknowledgeV2')

/**
 * Keeps public acknowledgement outcomes, including the broker code nested in a failure's cause.
 * @param {object[]} results Public ACKNOWLEDGE payloads.
 * @returns {object[]} Compact ranges and their actual outcomes.
 */
const acknowledgementResults = (results) =>
  results.map(({ topic, partition, firstOffset, lastOffset, type, error }) => ({
    topic,
    partition,
    firstOffset,
    lastOffset,
    type,
    error: describeError(error),
  }))

/**
 * Reads acknowledgement-bearing requests already captured by a spec's socket tap.
 * @param {object[]} frames Captured frames with Kafka request headers.
 * @returns {object[]} Captured session epochs, destinations and acknowledgement batches.
 */
const acknowledgementRequests = (frames) =>
  frames.flatMap(({ apiKey, apiVersion, correlationId, label, socket, bytes, headerBytes }) => {
    if (apiKey !== 78 && apiKey !== 79) return []
    if (apiVersion !== 2) return [{ apiKey, apiVersion, error: 'Unsupported diagnostic reader' }]
    try {
      const reader = apiKey === 78 ? shareFetch : shareAcknowledge
      const request = reader.readRequest(bytes.subarray(headerBytes))
      const topics = request.topics
        .map(({ topicId, partitions }) => ({
          topicId,
          partitions: partitions.filter(
            ({ acknowledgementBatches }) => acknowledgementBatches.length
          ),
        }))
        .filter(({ partitions }) => partitions.length)
      if (!topics.length) return []
      const { groupId, memberId, shareSessionEpoch, isRenewAck } = request
      return [
        {
          apiKey,
          apiVersion,
          correlationId,
          label,
          socket,
          groupId,
          memberId,
          shareSessionEpoch,
          isRenewAck,
          topics,
        },
      ]
    } catch (error) {
      return [{ apiKey, apiVersion, correlationId, label, socket, error: describeError(error) }]
    }
  })

/**
 * Records the last observation before cleanup, retaining the original predicate and poll budget.
 * @param {AbortSignal} signal Case scope.
 * @param {number} ms Whole poll budget.
 * @param {Function} check Original poll predicate.
 * @param {object} options Poll options plus snapshot and record callbacks.
 * @param {Function} options.snapshot Last completed reads, without more I/O.
 * @param {Function} options.record Writes one evidence row.
 * @returns {Promise<void>} Once the original predicate holds.
 * @throws {Error} Original failure with the last observation appended to its message.
 */
const pollSharePosition = async (signal, ms, check, { snapshot, record, ...options }) => {
  try {
    await pollUntil(signal, ms, check, options)
  } catch (error) {
    const last = snapshot()
    record({ status: 'failed', last, error: describeError(error) })
    throw new Error(`${error.message}; last share observation: ${JSON.stringify(last)}`, {
      cause: error,
    })
  }
  record({ status: 'settled', last: snapshot() })
}

const DECIMAL = /^(0|[1-9][0-9]*)$/
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/

/**
 * Checks agreeing persisted rows against independently observed topic identity and log ends.
 * @param {{groupId:string, topic:string, partitions:object[]}} target Fixed floors and ends.
 * @param {{named:object[], all:object[], topicId:string, ends:object[], error?:object}} observation Actual reads.
 * @returns {{settled:boolean, rows:object[]}} Normalized rows, retaining every broker field used.
 */
const terminalSharePosition = (target, observation) => {
  const { named, all, topicId, ends, error } = observation
  const rows = []
  if (error || !UUID.test(topicId) || topicId === '00000000-0000-0000-0000-000000000000') {
    return { settled: false, rows }
  }
  const topics = [named, all].map((listing) =>
    Array.isArray(listing)
      ? listing.filter((row) => row.topic === target.topic || row.topicId === topicId)
      : []
  )
  if (
    topics.some(
      (matches) =>
        matches.length !== 1 || matches[0].topic !== target.topic || matches[0].topicId !== topicId
    )
  ) {
    return { settled: false, rows }
  }
  let settled = target.partitions.length > 0
  for (const { partition, floor, end } of target.partitions) {
    const forms = topics.map(([topic]) =>
      (topic.partitions ?? []).filter((row) => row.partition === partition)
    )
    const independent = (ends ?? []).filter((row) => row.partition === partition)
    const normalize = ({ startOffset, lag, errorCode, leaderEpoch }) => ({
      startOffset,
      lag,
      errorCode,
      leaderEpoch,
    })
    const first = forms[0][0]
    const agree = forms.every(
      (matches) =>
        matches.length === 1 && first && isDeepStrictEqual(normalize(matches[0]), normalize(first))
    )
    const row = {
      groupId: target.groupId,
      topic: target.topic,
      topicId,
      partition,
      ...(first && normalize(first)),
      floor,
      logEnd: independent[0]?.high,
      expectedEnd: end,
      agree,
    }
    rows.push(row)
    const canonical = [floor, end, row.startOffset].every(
      (value) => typeof value === 'string' && DECIMAL.test(value)
    )
    if (
      !agree ||
      independent.length !== 1 ||
      row.logEnd !== end ||
      row.errorCode !== 0 ||
      row.lag !== '0' ||
      !canonical ||
      BigInt(row.startOffset) < BigInt(floor) ||
      BigInt(row.startOffset) > BigInt(end)
    ) {
      settled = false
    }
  }
  return { settled, rows }
}

/**
 * Polls both offset-listing forms and independent log ends within the caller's existing bound.
 * @param {import('../types').Admin} admin Connected admin.
 * @param {{groupId:string, topic:string, partitions:{partition:number, floor:string, end:string}[]}} target Fixed bounds.
 * @param {{signal:AbortSignal, timeoutMs:number, label?:string, record?:Function}} options Existing poll scope and evidence sink.
 * @returns {Promise<{named:object[], all:object[], rows:object[]}>} Last agreeing observation.
 * @throws {Error} On failure with both listings, independent ends and lag retained.
 */
const awaitTerminalSharePosition = async (admin, target, { signal, timeoutMs, label, record }) => {
  const last = { named: null, all: null, ends: null, topicId: null, rows: [], error: null }
  await pollSharePosition(
    signal,
    timeoutMs,
    async (scope) => {
      last.error = null
      try {
        if (!last.topicId) {
          const metadata = await bounded(scope, Infinity, () =>
            admin.fetchTopicMetadata({ topics: [target.topic] })
          )
          const topics = metadata.topics.filter(({ name }) => name === target.topic)
          if (topics.length !== 1) throw new Error('Missing or duplicate topic metadata')
          last.topicId = topics[0].topicId
        }
        last.named = await bounded(scope, Infinity, () =>
          admin.listShareGroupOffsets({
            groupId: target.groupId,
            topics: [
              {
                topic: target.topic,
                partitions: target.partitions.map(({ partition }) => partition),
              },
            ],
            apiTimeout: Math.min(timeoutMs, 8000),
          })
        )
        last.all = await bounded(scope, Infinity, () =>
          admin.listShareGroupOffsets({
            groupId: target.groupId,
            apiTimeout: Math.min(timeoutMs, 8000),
          })
        )
        last.ends = await bounded(scope, Infinity, () =>
          admin.fetchTopicOffsets(target.topic, { isolationLevel: 'read_uncommitted' })
        )
      } catch (error) {
        last.error = describeError(error)
        if (scope.aborted || ![14, 15, 16].includes(error.code ?? error.cause?.code)) throw error
      }
      const result = terminalSharePosition(target, last)
      last.rows = result.rows
      return result.settled
    },
    {
      label: label || 'The terminal share position',
      snapshot: () => ({ ...target, ...last }),
      record: record || (() => {}),
    }
  )
  return { named: last.named, all: last.all, rows: last.rows }
}

/**
 * Actively verifies an assigned same-group member is quiet, then rechecks the unchanged log ends.
 * @param {{kafka:object, admin:object, groupId:string, topic:string, partitions:object[], signal:AbortSignal, windowMs?:number, timeoutMs:number, record?:Function, onClient?:Function}} options Existing group and enclosing bound.
 * @returns {Promise<{delivered:string[], position:object}>} Actual quiet and position evidence.
 * @throws {Error} On any verifier delivery, failed cleanup, or nonterminal observation.
 */
const assertQuietTerminalGroup = async ({
  kafka,
  admin,
  groupId,
  topic,
  partitions,
  signal,
  windowMs = 5000,
  timeoutMs,
  record,
  onClient,
}) =>
  bounded(signal, timeoutMs, async (scope) => {
    const observed = await fixtures.verifierMember(groupId, topic, {
      kafka,
      signal: scope,
      windowMs,
      record,
      onClient,
    })
    const delivered = observed.map(({ identity }) => identity)
    record?.({ status: delivered.length ? 'failed' : 'quiet', groupId, topic, windowMs, delivered })
    if (delivered.length)
      throw new Error(`Same-group verifier delivered: ${JSON.stringify(delivered)}`)
    const position = await awaitTerminalSharePosition(
      admin,
      { groupId, topic, partitions },
      {
        signal: scope,
        timeoutMs,
        record,
      }
    )
    return { delivered, position }
  })

/** Observes original broker calls and their answers without changing requests or outcomes. */
class ShareAcknowledgementObserver {
  /** Starts with no installed wrappers or exchanges. */
  constructor() {
    this.exchanges = []
    this.acquisitions = []
    this.restores = []
  }

  /** @returns {void} Installs call-through wrappers, pairing each answer with its original call. */
  install() {
    const observer = this
    for (const [method, apiKey] of [
      ['shareFetch', 78],
      ['shareAcknowledge', 79],
    ]) {
      const original = Broker.prototype[method]
      Broker.prototype[method] = async function observed(request, ...args) {
        const hasBatches = request.topics?.some(({ partitions }) =>
          partitions.some(({ acknowledgementBatches }) => acknowledgementBatches?.length)
        )
        const exchange = { apiKey, request }
        if (hasBatches) observer.exchanges.push(exchange)
        try {
          const response = await original.call(this, request, ...args)
          if (hasBatches) exchange.response = response
          if (apiKey === 78 && response.errorCode === 0) {
            for (const { topicId, partitions } of response.responses ?? []) {
              for (const { partitionIndex, errorCode, acquiredRecords } of partitions) {
                if (errorCode === 0 && acquiredRecords?.length)
                  observer.acquisitions.push({
                    groupId: request.groupId,
                    topicId,
                    partition: partitionIndex,
                    acquiredRecords,
                  })
              }
            }
          }
          return response
        } catch (error) {
          if (hasBatches) exchange.error = describeError(error)
          throw error
        }
      }
      this.restores.push(() => {
        Broker.prototype[method] = original
      })
    }
  }

  /** @returns {void} Restores the original broker methods after all client work is joined. */
  restore() {
    for (const restore of this.restores.splice(0).reverse()) restore()
  }

  /**
   * Retains acknowledgement batches with the matching original partition result, including failures.
   * @param {string} groupId Workload group.
   * @returns {object[]} Request and response provenance in invocation order; renewals stay diagnostic.
   */
  evidence(groupId) {
    return this.exchanges.flatMap(({ apiKey, request, response, error }, exchange) => {
      if (request.groupId !== groupId) return []
      return request.topics.flatMap(({ topicId, partitions }) =>
        partitions.flatMap(({ partitionIndex, acknowledgementBatches }) => {
          if (!acknowledgementBatches?.length) return []
          const matches = (response?.responses ?? [])
            .filter((topic) => topic.topicId === topicId)
            .flatMap(({ partitions }) =>
              partitions.filter((row) => row.partitionIndex === partitionIndex)
            )
          const result = matches.length === 1 ? matches[0] : null
          return [
            {
              exchange,
              apiKey,
              groupId,
              memberId: request.memberId,
              shareSessionEpoch: request.shareSessionEpoch,
              isRenewAck: request.isRenewAck,
              topicId,
              partition: partitionIndex,
              acknowledgementBatches,
              responseErrorCode: response?.errorCode,
              result,
              error,
              successful:
                !error &&
                !request.isRenewAck &&
                response?.errorCode === 0 &&
                result?.errorCode === 0 &&
                (apiKey !== 78 || result.acknowledgeErrorCode === 0),
            },
          ]
        })
      )
    })
  }

  /**
   * Reports actual acquired offsets from successful fetch responses, including control positions.
   * @param {string} groupId Workload group.
   * @param {string} topicId Independent UUID.
   * @param {number} partition Owned partition.
   * @returns {Set<string>} Offsets the broker actually acquired for this group.
   */
  acquiredOffsets(groupId, topicId, partition) {
    const offsets = new Set()
    for (const row of this.acquisitions) {
      if (row.groupId !== groupId || row.topicId !== topicId || row.partition !== partition)
        continue
      for (const { firstOffset, lastOffset } of row.acquiredRecords) {
        for (let offset = BigInt(firstOffset); offset <= BigInt(lastOffset); offset++)
          offsets.add(String(offset))
      }
    }
    return offsets
  }

  /**
   * Expands only successfully answered original terminal batches, preserving type and multiplicity.
   * @param {string} groupId Workload group.
   * @param {string} topicId Independently observed topic UUID.
   * @param {number} partition Owned partition.
   * @returns {Map<string, number[]>} Actual successful wire decisions per decimal offset.
   */
  types(groupId, topicId, partition) {
    const types = new Map()
    for (const row of this.evidence(groupId)) {
      if (!row.successful || row.topicId !== topicId || row.partition !== partition) continue
      for (const { firstOffset, lastOffset, acknowledgeTypes } of row.acknowledgementBatches) {
        for (let offset = BigInt(firstOffset); offset <= BigInt(lastOffset); offset++) {
          const type =
            acknowledgeTypes.length === 1
              ? acknowledgeTypes[0]
              : acknowledgeTypes[Number(offset - BigInt(firstOffset))]
          const key = String(offset)
          types.set(key, [...(types.get(key) ?? []), type])
        }
      }
    }
    return types
  }
}

module.exports = {
  acknowledgementResults,
  acknowledgementRequests,
  pollSharePosition,
  terminalSharePosition,
  awaitTerminalSharePosition,
  assertQuietTerminalGroup,
  ShareAcknowledgementObserver,
}
