const { KafkaJSNonRetriableError, KafkaJSNoOffsetError } = require('../errors')
const invalid = (value) =>
  new KafkaJSNonRetriableError(
    `Invalid value \`${String(value)}\` for configuration auto.offset.reset. The value must be either 'earliest', 'latest', 'none' or of the format 'by_duration:<PnDTnHnMn.nS.>'.`
  )

/**
 * Parses unsigned day/time components using exact checked nanoseconds.
 * @param {string} text duration without signs, weeks, months or years
 * @param {*} original value for the validation diagnostic
 * @returns {number} safe non-negative milliseconds, sub-ms truncated
 * @throws {KafkaJSNonRetriableError} invalid grammar, precision or range
 */
function durationMilliseconds(text, original) {
  const m = /^P(?:(\d+)D)?(?:T(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)(?:\.(\d{1,9}))?S)?)?$/i.exec(text)
  if (!m || !m.slice(1).some((v) => v !== undefined) || /T$/i.test(text)) throw invalid(original)
  const units = [86400000000000n, 3600000000000n, 60000000000n, 1000000000n]
  const max = BigInt(Number.MAX_SAFE_INTEGER) * 1000000n
  let ns = 0n
  for (let i = 0; i < 4; i++) {
    const digits = (m[i + 1] || '0').replace(/^0+/, '') || '0'
    if (digits.length > 16) throw invalid(original)
    ns += BigInt(digits) * units[i]
    if (ns > max) throw invalid(original)
  }
  ns += BigInt((m[5] || '').padEnd(9, '0'))
  if (ns > max) throw invalid(original)
  return Number(ns / 1000000n)
}

/**
 * Normalizes one topic policy before any subscription mutation.
 * @param {{offsetReset?: string, fromBeginning?: boolean}} [options]
 * @returns {{kind:'earliest'|'latest'|'none'|'by_duration',durationMs?:number}}
 * @throws {KafkaJSNonRetriableError} invalid value or explicit boolean conflict
 */
function normalizeResetPolicy({ offsetReset, fromBeginning } = {}) {
  if (fromBeginning !== undefined && typeof fromBeginning !== 'boolean')
    throw invalid(fromBeginning)
  if (offsetReset === undefined) return { kind: fromBeginning ? 'earliest' : 'latest' }
  let policy
  if (['earliest', 'latest', 'none'].includes(offsetReset)) policy = { kind: offsetReset }
  else if (typeof offsetReset === 'string' && offsetReset.startsWith('by_duration:')) {
    policy = {
      kind: 'by_duration',
      durationMs: durationMilliseconds(offsetReset.slice(12), offsetReset),
    }
  } else throw invalid(offsetReset)
  if (fromBeginning !== undefined && policy.kind !== (fromBeginning ? 'earliest' : 'latest'))
    throw invalid(offsetReset)
  return policy
}

/**
 * Computes the lookback when resolution begins, away from negative sentinels.
 * @param {{durationMs:number}} policy normalized duration policy
 * @param {number} [now] current resolution wall time
 * @returns {number} non-negative ListOffsets timestamp
 * @throws Never for normalized inputs and a valid wall clock.
 */
function resetTimestamp(policy, now = Date.now()) {
  return Math.max(0, now - policy.durationMs)
}
/**
 * Resolves reset boundaries without owning a ledger, cache or halt state.
 * The manager supplies its original identity predicate and captured contexts.
 * @class
 */
class ResetResolver {
  /**
   * @param {object} cluster existing cluster with fetchTopicsOffset
   * @returns {ResetResolver} resolution-only adapter
   * @throws Never.
   */
  constructor(cluster) {
    this.cluster = cluster
  }

  /**
   * Resolves invalid offsets, excluding none refusals from all ListOffsets calls.
   * Explicit pending sentinel seeks override the subscription policy. A timestamp
   * miss gets one latest fallback; stale work cannot issue that follow-up.
   * @param {object[]} entries current OffsetFetch entries after owed-reset marking
   * @param {object} configurations topic settings with resetPolicy
   * @param {Function} current original per-cell ledger and identity predicate
   * @param {Function} context captured pre-await context lookup
   * @param {Function} [pending] current explicit sentinel timestamp lookup
   * @param {{guard: () => Error|null, fence: import('../producer/generation')}} [preparation]
   *   the caller's round identity, handed to both lookups; absent, the lookups are unfenced
   * @returns {Promise<{offsets:object[],refusals:object[]}>} guarded positions and refusals
   * @throws {Error} lookup error or latest still returning an invalid offset
   */
  async resolve(entries, configurations, current, context, pending = () => undefined, preparation) {
    const offsets = []
    const requests = []
    const durations = new Set()
    const refusals = []
    const key = (topic, partition) => JSON.stringify([topic, partition])
    for (const { topic, partitions } of entries) {
      const policy =
        configurations[topic].resetPolicy || normalizeResetPolicy(configurations[topic])
      const kept = []
      const asked = []
      for (const entry of partitions) {
        const { partition, offset } = entry
        if (!current(topic, partition)) continue
        if (offset != null && offset !== '-1' && offset !== '-2') {
          kept.push(entry)
          continue
        }
        const explicit = pending(topic, partition)
        if (policy.kind === 'none' && explicit === undefined) {
          const error = new KafkaJSNoOffsetError(
            'No committed offset; seek the partition to resume',
            { topic, partition, reason: 'no-offset' }
          )
          refusals.push({
            topic,
            partition,
            offset: null,
            reason: 'no-offset',
            error,
            context: context(topic, partition),
          })
          continue
        }
        let timestamp = explicit
        if (timestamp === undefined && policy.kind === 'by_duration') {
          timestamp = resetTimestamp(policy)
          durations.add(key(topic, partition))
        }
        asked.push(timestamp === undefined ? { partition } : { partition, timestamp })
        kept.push(entry)
      }
      offsets.push({ topic, partitions: kept })
      if (asked.length)
        requests.push({
          topic,
          partitions: asked,
          fromBeginning: policy.kind === 'earliest',
        })
    }
    if (!requests.length) return { offsets, refusals }
    const lookup = preparation ? [{ fence: preparation.fence, guard: preparation.guard }] : []
    const answers = await this.cluster.fetchTopicsOffset(requests, ...lookup)
    const resolved = new Map(
      answers.flatMap(({ topic, partitions }) =>
        partitions.map((p) => [key(topic, p.partition), p.offset])
      )
    )
    const missing = answers
      .map(({ topic, partitions }) => ({
        topic,
        fromBeginning: false,
        partitions: partitions
          .filter(
            (p) =>
              p.offset === '-1' &&
              durations.has(key(topic, p.partition)) &&
              current(topic, p.partition)
          )
          .map(({ partition }) => ({ partition })),
      }))
      .filter((t) => t.partitions.length)
    if (missing.length) {
      const latest = await this.cluster.fetchTopicsOffset(missing, ...lookup)
      for (const { topic, partitions } of latest)
        for (const p of partitions) {
          if (!/^[0-9]+$/.test(p.offset))
            throw new KafkaJSNonRetriableError('Latest offset lookup returned no real offset')
          resolved.set(key(topic, p.partition), p.offset)
        }
    }
    return {
      refusals: refusals.filter((r) => current(r.topic, r.partition)),
      offsets: offsets.map(({ topic, partitions }) => ({
        topic,
        partitions: partitions
          .filter((p) => current(topic, p.partition))
          .map((p) => ({
            ...p,
            offset: resolved.get(key(topic, p.partition)) ?? p.offset,
          })),
      })),
    }
  }
}
module.exports = { normalizeResetPolicy, resetTimestamp, ResetResolver }
