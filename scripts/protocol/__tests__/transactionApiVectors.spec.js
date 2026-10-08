const fs = require('fs')
const path = require('path')

const REQUESTS = path.join(__dirname, '..', '..', '..', 'src', 'protocol', 'requests')

/**
 * @param {string} message lower-camel Apache message name
 * @param {number[]} versions
 * @param {string[]} caseIds
 * @returns {string[]} vector file stems
 */
const stems = (message, versions, caseIds) =>
  versions.flatMap((version) => caseIds.map((caseId) => `${message}-v${version}-${caseId}`))

/** Every vector the generator writes for the transaction APIs, by fixtures directory. */
const MANIFEST = {
  addPartitionsToTxn: [
    ...stems('addPartitionsToTxnRequest', [2, 3], ['basic']),
    ...stems('addPartitionsToTxnResponse', [2, 3], ['ok', 'error']),
  ],
  addOffsetsToTxn: [
    ...stems('addOffsetsToTxnRequest', [2, 3, 4], ['basic']),
    ...stems('addOffsetsToTxnResponse', [2, 3, 4], ['ok', 'error']),
  ],
  endTxn: [
    ...stems('endTxnRequest', [2, 3, 4, 5], ['commit', 'abort']),
    ...stems('endTxnResponse', [2, 3, 4], ['ok', 'error']),
    ...stems('endTxnResponse', [5], ['producerIdAndEpoch', 'defaults', 'error']),
  ],
  txnOffsetCommit: [
    ...stems('txnOffsetCommitRequest', [2], ['legacy', 'leaderEpoch']),
    ...stems('txnOffsetCommitRequest', [3, 4, 5], ['legacy', 'groupMetadata']),
    ...stems('txnOffsetCommitResponse', [2, 3, 4, 5], ['ok', 'error']),
  ],
  initProducerId: stems(
    'initProducerIdRequest',
    [3, 4, 5],
    ['firstInit', 'reinit', 'nullTransactionalId']
  ),
  offsetFetch: stems('offsetFetchRequest', [7, 8, 9], ['requireStable', 'notRequireStable']),
}

const ENTRIES = Object.entries(MANIFEST).flatMap(([api, names]) =>
  names.map((name) => ({ api, name }))
)

/**
 * @param {string} api fixtures directory name
 * @param {string} name vector file stem
 * @returns {object} one vector document
 */
const load = (api, name) =>
  JSON.parse(fs.readFileSync(path.join(REQUESTS, api, 'fixtures', 'java', `${name}.json`), 'utf8'))

/**
 * @param {string} api fixtures directory name
 * @param {string} name vector file stem
 * @returns {Buffer} one vector's bytes
 */
const bytes = (api, name) => Buffer.from(load(api, name).hex, 'hex')

/**
 * @param {Buffer} a
 * @param {Buffer} b same length as `a`
 * @returns {number[]} the offsets at which the two differ
 */
const differences = (a, b) => {
  expect(a.length).toEqual(b.length)
  const offsets = []
  for (let i = 0; i < a.length; i++) {
    if (a[i] !== b[i]) {
      offsets.push(i)
    }
  }
  return offsets
}

/**
 * @param {number} start first offset
 * @param {number} length how many offsets
 * @returns {number[]} `length` consecutive offsets from `start`
 */
const span = (start, length) => Array.from({ length }, (_, i) => start + i)

/** The older OffsetFetch vectors share the directory; only the v7-v9 RequireStable pairs are this manifest's. */
const OFFSET_FETCH_STABLE = /^offsetFetchRequest-v[789]-(not)?[rR]equireStable\.json$/

/**
 * Wire pieces, so an expected body reads as the protocol and not as a copied hex string.
 *
 * @param {number} value non-negative
 * @param {number} size bytes, big-endian
 * @returns {string} the INT16 / INT32 / INT64 as hex
 */
const int = (value, size) => value.toString(16).padStart(size * 2, '0')

/**
 * @param {string} text
 * @returns {string} a STRING: INT16 length, then the UTF-8 bytes (classic versions)
 */
const str = (text) => int(text.length, 2) + Buffer.from(text).toString('hex')

/**
 * @param {string} text shorter than 127 bytes
 * @returns {string} a COMPACT_STRING: UVARINT length + 1, then the UTF-8 bytes (flexible versions)
 */
const compact = (text) => int(text.length + 1, 1) + Buffer.from(text).toString('hex')

/**
 * @param {number} elements fewer than 127
 * @returns {string} a COMPACT_ARRAY's one-byte length prefix: the element count + 1
 */
const count = (elements) => int(elements + 1, 1)

const TAGS = '00' // an empty tag buffer ends every flexible struct
const TRANSACTIONAL_ID = 'test-transactional-id'
const TOPIC = 'test-topic'
const THROTTLE = int(25, 4) // ThrottleTimeMs
const PRODUCER = int(1000, 8) + int(3, 2) // ProducerId INT64, ProducerEpoch INT16
const NO_ERROR = int(0, 2)
const CONCURRENT_TRANSACTIONS = int(51, 2) // 0x0033
const PRODUCER_FENCED = int(90, 2) // 0x005a
const ILLEGAL_GENERATION = int(22, 2) // 0x0016
const DEFAULT_PRODUCER_PAIR = 'ff'.repeat(10) // ProducerId -1, ProducerEpoch -1

/** Full expected bodies of the short vectors, as `[api, file stem, hex]`. */
const PINNED = [
  [
    'addPartitionsToTxn',
    'addPartitionsToTxnRequest-v2-basic',
    str(TRANSACTIONAL_ID) + PRODUCER + int(1, 4) + str(TOPIC) + int(2, 4) + int(0, 4) + int(2, 4),
  ],
  [
    'addPartitionsToTxn',
    'addPartitionsToTxnRequest-v3-basic',
    compact(TRANSACTIONAL_ID) +
      PRODUCER +
      count(1) +
      compact(TOPIC) +
      count(2) +
      int(0, 4) +
      int(2, 4) +
      TAGS +
      TAGS,
  ],
  ...[
    ['ok', NO_ERROR],
    ['error', CONCURRENT_TRANSACTIONS],
  ].flatMap(([caseId, code]) => [
    [
      'addPartitionsToTxn',
      `addPartitionsToTxnResponse-v2-${caseId}`,
      THROTTLE + int(1, 4) + str(TOPIC) + int(2, 4) + int(0, 4) + NO_ERROR + int(2, 4) + code,
    ],
    [
      'addPartitionsToTxn',
      `addPartitionsToTxnResponse-v3-${caseId}`,
      THROTTLE +
        count(1) +
        compact(TOPIC) +
        count(2) +
        (int(0, 4) + NO_ERROR + TAGS) +
        (int(2, 4) + code + TAGS) +
        TAGS +
        TAGS,
    ],
  ]),
  ['addOffsetsToTxn', 'addOffsetsToTxnResponse-v2-error', THROTTLE + CONCURRENT_TRANSACTIONS],
  [
    'addOffsetsToTxn',
    'addOffsetsToTxnResponse-v3-error',
    THROTTLE + CONCURRENT_TRANSACTIONS + TAGS,
  ],
  [
    'addOffsetsToTxn',
    'addOffsetsToTxnResponse-v4-error',
    THROTTLE + CONCURRENT_TRANSACTIONS + TAGS,
  ],
  ['endTxn', 'endTxnResponse-v2-error', THROTTLE + PRODUCER_FENCED],
  ['endTxn', 'endTxnResponse-v3-error', THROTTLE + PRODUCER_FENCED + TAGS],
  ['endTxn', 'endTxnResponse-v4-error', THROTTLE + PRODUCER_FENCED + TAGS],
  ['endTxn', 'endTxnResponse-v5-error', THROTTLE + PRODUCER_FENCED + DEFAULT_PRODUCER_PAIR + TAGS],
  [
    'txnOffsetCommit',
    'txnOffsetCommitResponse-v2-error',
    THROTTLE + int(1, 4) + str(TOPIC) + int(1, 4) + int(0, 4) + ILLEGAL_GENERATION,
  ],
  ...[3, 4, 5].map((version) => [
    'txnOffsetCommit',
    `txnOffsetCommitResponse-v${version}-error`,
    THROTTLE +
      count(1) +
      compact(TOPIC) +
      count(1) +
      (int(0, 4) + ILLEGAL_GENERATION + TAGS) +
      TAGS +
      TAGS,
  ]),
]

describe('Java message vectors > transaction APIs', () => {
  test('the manifest names sixty-three vectors', () => {
    expect(ENTRIES).toHaveLength(63)
  })

  test.each(ENTRIES)('$name is on disk and is what its name says', ({ api, name }) => {
    const vector = load(api, name)
    const [, message, version, caseId] = name.match(/^(\w+)-v(\d+)-(\w+)$/)

    expect(vector.message.charAt(0).toLowerCase() + vector.message.slice(1)).toEqual(message)
    expect(vector.version).toEqual(Number(version))
    expect(vector.case).toEqual(caseId)
    expect(vector.hex).toMatch(/^(?:[0-9a-f]{2})+$/)
    expect(vector.fields).toEqual(expect.any(Object))
    if ('producerId' in vector.fields) {
      expect(typeof vector.fields.producerId).toEqual('string')
    }
  })

  test.each(Object.keys(MANIFEST))(
    '%s: the fixtures directory holds exactly the manifest',
    (api) => {
      const onDisk = fs
        .readdirSync(path.join(REQUESTS, api, 'fixtures', 'java'))
        .filter((file) => api !== 'offsetFetch' || OFFSET_FETCH_STABLE.test(file))

      expect(onDisk.sort()).toEqual(MANIFEST[api].map((name) => `${name}.json`).sort())
    }
  )

  test.each(PINNED)(
    '%s/%s: the bytes are the protocol, error code included',
    (api, name, expected) => {
      expect(load(api, name).hex).toEqual(expected)
    }
  )

  test.each([7, 8, 9])(
    'OffsetFetch v%i: RequireStable is one byte, right before the tag buffer',
    (version) => {
      const stable = bytes('offsetFetch', `offsetFetchRequest-v${version}-requireStable`)
      const notStable = bytes('offsetFetch', `offsetFetchRequest-v${version}-notRequireStable`)

      expect(differences(stable, notStable)).toEqual([stable.length - 2])
      expect([stable[stable.length - 2], notStable[notStable.length - 2]]).toEqual([1, 0])
    }
  )

  test.each([3, 4, 5])(
    'InitProducerId v%i: a re-initialization differs only in the producer id and epoch',
    (version) => {
      const first = bytes('initProducerId', `initProducerIdRequest-v${version}-firstInit`)
      const reinit = bytes('initProducerId', `initProducerIdRequest-v${version}-reinit`)
      // ... ProducerId INT64, ProducerEpoch INT16, then the one-byte tag buffer
      const pidAndEpoch = span(first.length - 11, 10)

      expect(differences(first, reinit)).toEqual(pidAndEpoch)
      expect(first.subarray(first.length - 11, first.length - 1).toString('hex')).toEqual(
        'ff'.repeat(10)
      )
      expect(reinit.subarray(reinit.length - 11, reinit.length - 1).toString('hex')).toEqual(
        '00000000000003e8' + '0003'
      )
    }
  )

  test.each([3, 4, 5])(
    'InitProducerId v%i: a null TransactionalId is the single byte 0x00 ahead of the same timeout and -1/-1 pair',
    (version) => {
      const first = bytes('initProducerId', `initProducerIdRequest-v${version}-firstInit`)
      const anonymous = bytes(
        'initProducerId',
        `initProducerIdRequest-v${version}-nullTransactionalId`
      )
      // ... TransactionTimeoutMs INT32 (30000), ProducerId INT64, ProducerEpoch INT16, the tag buffer
      const afterTransactionalId = '00007530' + 'ff'.repeat(10) + '00'

      expect(anonymous.toString('hex')).toEqual('00' + afterTransactionalId)
      expect(first.subarray(first.length - 15).toString('hex')).toEqual(afterTransactionalId)
    }
  )

  test('EndTxn v5: the bumped pair and the -1/-1 defaults occupy the same ten bytes', () => {
    const adopted = bytes('endTxn', 'endTxnResponse-v5-producerIdAndEpoch')
    const defaults = bytes('endTxn', 'endTxnResponse-v5-defaults')

    // ThrottleTimeMs 4, ErrorCode 2, ProducerId 8, ProducerEpoch 2, tag buffer 1
    expect(adopted).toHaveLength(17)
    expect(adopted.subarray(0, 4).toString('hex')).toEqual('00000019')
    expect(differences(adopted, defaults)).toEqual(span(6, 10))
    expect(adopted.subarray(6, 16).toString('hex')).toEqual('00000000000003e8' + '0004')
    expect(defaults.subarray(6, 16).toString('hex')).toEqual('ff'.repeat(10))
  })

  test.each([2, 3, 4, 5])(
    'EndTxn v%i: commit and abort differ in the Committed byte alone',
    (version) => {
      const commit = bytes('endTxn', `endTxnRequest-v${version}-commit`)
      const abort = bytes('endTxn', `endTxnRequest-v${version}-abort`)
      const committedOffset = version >= 3 ? commit.length - 2 : commit.length - 1

      expect(differences(commit, abort)).toEqual([committedOffset])
      expect([commit[committedOffset], abort[committedOffset]]).toEqual([1, 0])
    }
  )

  test('TxnOffsetCommit v2: CommittedLeaderEpoch is four bytes, -1 when the epoch is unknown', () => {
    const legacy = bytes('txnOffsetCommit', 'txnOffsetCommitRequest-v2-legacy')
    const withEpoch = bytes('txnOffsetCommit', 'txnOffsetCommitRequest-v2-leaderEpoch')
    // ... CommittedLeaderEpoch INT32, then CommittedMetadata as a null STRING (INT16 -1)
    const epoch = span(legacy.length - 6, 4)

    expect(differences(legacy, withEpoch)).toEqual(epoch)
    expect(legacy.subarray(legacy.length - 6, legacy.length - 2).toString('hex')).toEqual(
      'ffffffff'
    )
    expect(withEpoch.subarray(withEpoch.length - 6, withEpoch.length - 2).toString('hex')).toEqual(
      '00000003'
    )
  })

  test.each([
    ['endTxn', 'endTxnRequest', [3, 4, 5], ['commit', 'abort']],
    ['endTxn', 'endTxnResponse', [3, 4], ['ok', 'error']],
    ['txnOffsetCommit', 'txnOffsetCommitRequest', [3, 4, 5], ['legacy', 'groupMetadata']],
    ['txnOffsetCommit', 'txnOffsetCommitResponse', [3, 4, 5], ['ok', 'error']],
    ['addOffsetsToTxn', 'addOffsetsToTxnRequest', [3, 4], ['basic']],
    ['addOffsetsToTxn', 'addOffsetsToTxnResponse', [3, 4], ['ok', 'error']],
    [
      'initProducerId',
      'initProducerIdRequest',
      [3, 4, 5],
      ['firstInit', 'reinit', 'nullTransactionalId'],
    ],
  ])(
    '%s/%s: the same bytes at versions %j - those versions change semantics, not the wire',
    (api, message, versions, caseIds) => {
      for (const caseId of caseIds) {
        const [first, ...rest] = versions.map(
          (version) => load(api, `${message}-v${version}-${caseId}`).hex
        )
        for (const other of rest) {
          expect(other).toEqual(first)
        }
      }
    }
  )

  test.each([
    [
      'addPartitionsToTxn',
      'addPartitionsToTxnRequest-v2-basic',
      'addPartitionsToTxnRequest-v3-basic',
    ],
    ['addOffsetsToTxn', 'addOffsetsToTxnRequest-v2-basic', 'addOffsetsToTxnRequest-v3-basic'],
    ['endTxn', 'endTxnRequest-v2-commit', 'endTxnRequest-v3-commit'],
    ['endTxn', 'endTxnResponse-v2-ok', 'endTxnResponse-v3-ok'],
  ])('%s: the first flexible version is a different body (%s vs %s)', (api, classic, flexible) => {
    expect(load(api, flexible).hex).not.toEqual(load(api, classic).hex)
  })
})
