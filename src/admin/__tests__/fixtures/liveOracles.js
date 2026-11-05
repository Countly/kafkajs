/** Fixed quota/control/recovery acceptance, independent of the producer's send implementation. */
class QuotaOracle {
  /**
   * Checks the three measured windows and the independent broker byte counter.
   * Windows exclude warm-up. Every acknowledged identity must occur once in the physical log.
   * @param {object} sample control/quota/recovery windows, broker bytes, throttle and replay facts
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
    if (
      !Number.isSafeInteger(sample.brokerBytes) ||
      sample.brokerBytes < 0 ||
      sample.brokerBytes > 65536 * 30 + 65536
    )
      throw new Error('Broker byte bound exceeded')
    if (controlRate < 262144 || quotaRate > 67721 || quotaRate > controlRate * 0.5)
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
          { entityType: 'user', entityName: 'p9d-quota' },
          { entityType: 'client-id', entityName: 'p9d-quota' },
        ],
        ops: [{ key: 'producer_byte_rate', value: 65536 }],
      },
    ],
    validateOnly: false,
  })

module.exports = { QuotaOracle, StorageOracle, setProduceQuota }
