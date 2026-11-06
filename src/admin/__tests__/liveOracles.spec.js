const { QuotaOracle, StorageOracle, decodeSegment } = require('./fixtures/liveOracles')
const { RecordBatch } = require('../../protocol/recordBatch/v0')
const Record = require('../../protocol/recordBatch/record/v0')

/** The bound over 30 s: 65536 B/s for 30 s plus twelve one-second windows plus one 20480-byte request. */
const BOUND_30S = 65536 * 42 + 20480

/** @returns {object} independent quota boundary example, not a measured live result */
const quota = () => ({
  control: { durationMs: 30000, offeredBytes: 31457280, payloadBytes: 7864320 },
  quota: { durationMs: 30000, offeredBytes: 31457280, payloadBytes: BOUND_30S },
  recovery: { durationMs: 20000, offeredBytes: 20971520, payloadBytes: 5242880 },
  brokerBytes: BOUND_30S,
  brokerElapsedMs: 30000,
  throttleMs: 1,
  quotaPending: 1,
  controlPending: 0,
  failedCount: 0,
  ackedIds: ['one', 'two'],
  physicalIds: ['two', 'one'],
})

test('quota acceptance admits exact fixed bounds', () => {
  expect(() => QuotaOracle.verify(quota())).not.toThrow()
})

test('the accepted bound is the quota for the span, twelve one-second windows and one request', () => {
  expect(QuotaOracle.acceptedBound(30000)).toBe(BOUND_30S)
  expect(QuotaOracle.acceptedBound(40000) - QuotaOracle.acceptedBound(30000)).toBe(65536 * 10)
})

test('quota acceptance admits what a live broker took before its throttle and refuses an unthrottled producer', () => {
  const sample = quota()
  sample.brokerBytes = 2222926
  sample.brokerElapsedMs = 30892
  sample.quota.payloadBytes = 2100000
  expect(() => QuotaOracle.verify(sample)).not.toThrow()
  sample.brokerBytes = sample.control.offeredBytes
  expect(() => QuotaOracle.verify(sample)).toThrow('Broker byte bound exceeded')
})

test('quota acceptance allows more broker bytes the longer the counter readings are apart', () => {
  const sample = quota()
  sample.brokerBytes = QuotaOracle.acceptedBound(40000)
  sample.brokerElapsedMs = 40000
  expect(() => QuotaOracle.verify(sample)).not.toThrow()
  sample.brokerElapsedMs = 30000
  expect(() => QuotaOracle.verify(sample)).toThrow('Broker byte bound exceeded')
})

test.each([undefined, 0, NaN])('quota acceptance refuses %p as the broker interval', (value) => {
  const sample = quota()
  sample.brokerElapsedMs = value
  expect(() => QuotaOracle.verify(sample)).toThrow('Incomplete broker interval')
})

test.each([
  'broker',
  'control',
  'difference',
  'throttle',
  'recovery',
  'replay',
  'failure',
  'offered',
  'relative recovery',
  'payload',
])('quota acceptance refuses a broken %s observation', (field) => {
  const sample = quota()
  if (field === 'broker') sample.brokerBytes++
  if (field === 'control') sample.control.payloadBytes--
  if (field === 'difference') sample.quota.payloadBytes = sample.control.payloadBytes
  if (field === 'throttle') {
    sample.throttleMs = 0
    sample.quotaPending = 0
  }
  if (field === 'recovery') sample.recovery.payloadBytes--
  if (field === 'replay') sample.physicalIds.push('one')
  if (field === 'failure') sample.failedCount = 1
  if (field === 'offered') sample.quota.offeredBytes--
  if (field === 'relative recovery') {
    sample.control.payloadBytes = 19660800
    sample.recovery.payloadBytes = 10485759
  }
  if (field === 'payload') sample.quota.payloadBytes++
  expect(() => QuotaOracle.verify(sample)).toThrow()
})

/** @returns {object} independent segment boundaries for checking oracle arithmetic only */
const storage = () => ({
  producedBytes: 16777216,
  remoteSegments: 2,
  remoteBytes: 2097152,
  logStart: '0',
  high: '20',
  localFirst: '10',
  remoteLast: '15',
  pendingFirst: '16',
  earliestLocal: '10',
  latestTiered: '15',
  earliestPendingUpload: '16',
  hasPendingUploadSpec: true,
  expectedIds: ['a', 'b'],
  retainedIds: ['a', 'b'],
  plain: { logStart: '0', earliestLocal: '0', latestTiered: '-1', earliestPendingUpload: '-1' },
})

test('tiered acceptance compares independent segment and upload boundaries', () => {
  expect(() => StorageOracle.verify(storage())).not.toThrow()
})

test.each(['corpus', 'remote', 'eviction', 'local', 'uploaded', 'pending', 'replay', 'plain'])(
  'tiered acceptance refuses a broken %s observation',
  (field) => {
    const sample = storage()
    if (field === 'corpus') sample.producedBytes--
    if (field === 'remote') sample.remoteSegments = 0
    if (field === 'eviction') sample.earliestLocal = '0'
    if (field === 'local') sample.localFirst = '11'
    if (field === 'uploaded') sample.remoteLast = '14'
    if (field === 'pending') sample.pendingFirst = '17'
    if (field === 'replay') sample.retainedIds.reverse()
    if (field === 'plain') sample.plain.latestTiered = '0'
    expect(() => StorageOracle.verify(sample)).toThrow()
  }
)

describe('segment decoding', () => {
  /**
   * @param {number} firstOffset offset of the batch's first record
   * @param {string[]} values one record per value
   * @returns {Promise<Buffer>} the batch as it lies in a log file
   */
  const batchOf = async (firstOffset, values) =>
    (
      await RecordBatch({
        firstOffset: String(firstOffset),
        firstTimestamp: 1700000000000,
        maxTimestamp: 1700000000000,
        lastOffsetDelta: values.length - 1,
        records: values.map((value, offsetDelta) =>
          Record({ offsetDelta, value: Buffer.from(value) })
        ),
      })
    ).buffer

  test('returns the records of every batch in file order, with offsets as strings', async () => {
    const segment = Buffer.concat([await batchOf(0, ['a', 'b']), await batchOf(2, ['c'])])
    const records = await decodeSegment(segment)
    expect(records.map(({ offset, value }) => [offset, value.toString()])).toEqual([
      ['0', 'a'],
      ['1', 'b'],
      ['2', 'c'],
    ])
  })

  test('refuses a segment cut inside a batch', async () => {
    const segment = Buffer.concat([await batchOf(0, ['a']), await batchOf(1, ['b'])])
    await expect(decodeSegment(segment.subarray(0, segment.length - 1))).rejects.toThrow(
      'Incomplete segment batch'
    )
  })

  test('refuses a segment cut inside a batch header', async () => {
    const segment = Buffer.concat([await batchOf(0, ['a']), Buffer.alloc(11)])
    await expect(decodeSegment(segment)).rejects.toThrow('Incomplete segment snapshot')
  })

  test('refuses a batch whose bytes were altered, by its CRC', async () => {
    const segment = await batchOf(0, ['a'])
    segment[segment.length - 2] ^= 0xff
    await expect(decodeSegment(segment)).rejects.toMatchObject({
      name: 'KafkaJSCorruptRecordError',
      reason: 'checksum',
    })
  })
})
