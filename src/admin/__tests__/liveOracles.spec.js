const { QuotaOracle, StorageOracle } = require('./fixtures/liveOracles')

/** @returns {object} independent quota boundary example, not a measured live result */
const quota = () => ({
  control: { durationMs: 30000, offeredBytes: 31457280, payloadBytes: 7864320 },
  quota: { durationMs: 30000, offeredBytes: 31457280, payloadBytes: 2031616 },
  recovery: { durationMs: 20000, offeredBytes: 20971520, payloadBytes: 5242880 },
  brokerBytes: 2031616,
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
  if (field === 'payload') sample.quota.payloadBytes = 2031631
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
