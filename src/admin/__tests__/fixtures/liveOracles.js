const Decoder = require('../../../protocol/decoder')
const decodeBatch = require('../../../protocol/recordBatch/v0/decoder')

/** The produce quota the live scenario sets on its user and client id, in bytes per second. */
const QUOTA_BYTES_PER_SECOND = 65536
/** The broker's `quota.window.num` and `quota.window.size.seconds`; the scenario overrides neither. */
const QUOTA_WINDOW_SAMPLES = 11
const QUOTA_WINDOW_SECONDS = 1
/**
 * Bytes of the one request the quota admits late: a batch holds at most the producer's default
 * `batchSize` of 16384 bytes of keys and values, and its record framing is a few hundred bytes. A
 * request carries one batch per partition, and the scenario writes one partition.
 */
const REQUEST_BYTES = 20480

/** Fixed quota/control/recovery acceptance, independent of the producer's send implementation. */
class QuotaOracle {
  /**
   * The most bytes a broker accepts over a span while it enforces the produce quota. Kafka rates a
   * quota over `QUOTA_WINDOW_SAMPLES` one-second samples plus the overlapping one it keeps, so it
   * judges a window of up to `QUOTA_WINDOW_SAMPLES + 1` seconds. A request is appended before the
   * quota is recorded, and the throttle that follows delays only the response, by the time the
   * window average needs to fall back to the quota. A span therefore holds the quota for its length,
   * plus up to one window of bytes taken before the throttle caught up, plus the last request.
   * @see https://github.com/apache/kafka/blob/4.3.1/clients/src/main/java/org/apache/kafka/common/metrics/stats/SampledStat.java
   * @see https://github.com/apache/kafka/blob/4.3.1/clients/src/main/java/org/apache/kafka/common/metrics/stats/Rate.java
   * @see https://github.com/apache/kafka/blob/4.3.1/server-common/src/main/java/org/apache/kafka/server/quota/QuotaUtils.java
   * @see https://github.com/apache/kafka/blob/4.3.1/core/src/main/scala/kafka/server/KafkaApis.scala
   * @param {number} spanMs length of the span in milliseconds
   * @returns {number} the bound in bytes
   */
  static acceptedBound(spanMs) {
    const seconds = spanMs / 1000 + (QUOTA_WINDOW_SAMPLES + 1) * QUOTA_WINDOW_SECONDS
    return Math.ceil(QUOTA_BYTES_PER_SECOND * seconds) + REQUEST_BYTES
  }

  /**
   * Checks the three measured windows and the independent broker byte counter.
   * Windows exclude warm-up. Every acknowledged identity must occur once in the physical log.
   * @param {object} sample control/quota/recovery windows, broker bytes and the milliseconds between
   *   the two counter readings, throttle and replay facts
   * @returns {void}
   * @throws {Error} missing measurements, hidden failures or any predeclared bound violated
   */
  static verify(sample) {
    const { control, quota, recovery } = sample
    for (const [window, duration] of [
      [control, 30000],
      [quota, 30000],
      [recovery, 20000],
    ]) {
      if (
        window?.durationMs !== duration ||
        !Number.isSafeInteger(window.payloadBytes) ||
        window.payloadBytes < 0 ||
        window.offeredBytes !== (1048576 * duration) / 1000
      )
        throw new Error('Incomplete offered-load window')
    }
    const controlRate = control.payloadBytes / 30
    const quotaRate = quota.payloadBytes / 30
    const recoveryRate = recovery.payloadBytes / 20
    if (!(Number.isFinite(sample.brokerElapsedMs) && sample.brokerElapsedMs > 0))
      throw new Error('Incomplete broker interval')
    if (
      !Number.isSafeInteger(sample.brokerBytes) ||
      sample.brokerBytes < 0 ||
      sample.brokerBytes > QuotaOracle.acceptedBound(sample.brokerElapsedMs)
    )
      throw new Error('Broker byte bound exceeded')
    if (
      controlRate < 262144 ||
      quota.payloadBytes > QuotaOracle.acceptedBound(quota.durationMs) ||
      quotaRate > controlRate * 0.5
    )
      throw new Error('Quota did not meet the control bounds')
    if (!(sample.throttleMs > 0 || sample.quotaPending > sample.controlPending))
      throw new Error('No client throttle evidence')
    if (recoveryRate < 262144 || recoveryRate < controlRate * 0.8)
      throw new Error('Quota removal did not recover')
    if (
      sample.failedCount !== 0 ||
      !Array.isArray(sample.ackedIds) ||
      sample.ackedIds.length === 0 ||
      !Array.isArray(sample.physicalIds) ||
      new Set(sample.ackedIds).size !== sample.ackedIds.length ||
      JSON.stringify([...sample.ackedIds].sort()) !== JSON.stringify([...sample.physicalIds].sort())
    )
      throw new Error('Acknowledged replay differs')
  }
}

/** Compares named ListOffsets results against independent segment/upload observations. */
class StorageOracle {
  /**
   * Uses independently decoded segment boundaries, never another ListOffsets answer as oracle.
   * @param {object} sample logStart/high, localFirst/remoteLast/pendingFirst, named replies and data
   * @returns {void}
   * @throws {Error} absent upload/eviction, inconsistent boundary or incomplete retained corpus
   */
  static verify(sample) {
    const start = BigInt(sample.logStart)
    const high = BigInt(sample.high)
    const local = BigInt(sample.earliestLocal)
    const remote = BigInt(sample.latestTiered)
    if (
      !Number.isSafeInteger(sample.producedBytes) ||
      sample.producedBytes < 16 * 1048576 ||
      !Number.isSafeInteger(sample.remoteSegments) ||
      sample.remoteSegments < 1 ||
      !Number.isSafeInteger(sample.remoteBytes) ||
      sample.remoteBytes < 1048576
    )
      throw new Error('No nontrivial remote corpus')
    if (
      !(local > start && remote >= start && remote < high) ||
      local !== BigInt(sample.localFirst) ||
      remote !== BigInt(sample.remoteLast)
    )
      throw new Error('Segment boundaries differ')
    if (
      sample.hasPendingUploadSpec &&
      BigInt(sample.earliestPendingUpload) !== BigInt(sample.pendingFirst)
    )
      throw new Error('Pending upload boundary differs')
    if (
      !Array.isArray(sample.expectedIds) ||
      sample.expectedIds.length === 0 ||
      !Array.isArray(sample.retainedIds) ||
      JSON.stringify(sample.expectedIds) !== JSON.stringify(sample.retainedIds)
    )
      throw new Error('Retained corpus differs')
    if (
      sample.plain.earliestLocal !== sample.plain.logStart ||
      sample.plain.latestTiered !== '-1' ||
      (sample.hasPendingUploadSpec && sample.plain.earliestPendingUpload !== '-1')
    )
      throw new Error('Plain topic sentinel differs')
  }
}

/**
 * Decodes every record of a sealed log segment, checking each batch's CRC.
 * @param {Buffer} data complete log file: record batches back to back
 * @returns {Promise<object[]>} the records of all batches in file order, `offset` a decimal string
 * @throws {Error} a segment cut inside a batch, or a batch that fails to decode
 */
const decodeSegment = async (data) => {
  const records = []
  let offset = 0
  while (offset < data.length) {
    if (offset + 12 > data.length) throw new Error('Incomplete segment snapshot')
    const size = data.readInt32BE(offset + 8) + 12
    if (size < 61 || offset + size > data.length) throw new Error('Incomplete segment batch')
    const batch = await decodeBatch(new Decoder(data.subarray(offset, offset + size)))
    records.push(...batch.records)
    offset += size
  }
  return records
}

/**
 * Applies the fixed user/client produce quota; a validation-only request would invalidate the live arm.
 * @param {import('../../../../types').Admin} admin connected administrative client
 * @returns {Promise<Array<object>>} public per-entity results, inspected by the caller
 * @throws {Error} original admin failure
 */
const setProduceQuota = (admin) =>
  admin.alterClientQuotas({
    entries: [
      {
        entity: [
          { entityType: 'user', entityName: 'live-quota' },
          { entityType: 'client-id', entityName: 'live-quota' },
        ],
        ops: [{ key: 'producer_byte_rate', value: 65536 }],
      },
    ],
    validateOnly: false,
  })

module.exports = { QuotaOracle, StorageOracle, decodeSegment, setProduceQuota }
