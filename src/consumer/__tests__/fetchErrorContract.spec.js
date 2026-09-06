const {
  partitionFetchAction,
  topLevelFetchAction,
  validationPartitionAction,
  fetchedPartitionDropReason,
} = require('../fetchErrorContract')

describe('Consumer > fetch error contract', () => {
  describe('partitionFetchAction', () => {
    test.each([
      [1, 'offset-out-of-range'], // OFFSET_OUT_OF_RANGE
      [3, 'stale-metadata'], //     UNKNOWN_TOPIC_OR_PARTITION
      [5, 'stale-metadata'], //     LEADER_NOT_AVAILABLE
      [6, 'stale-metadata'], //     NOT_LEADER_FOR_PARTITION
      [9, 'stale-metadata'], //     REPLICA_NOT_AVAILABLE
      [56, 'stale-metadata'], //    KAFKA_STORAGE_ERROR
      [74, 'stale-metadata'], //    FENCED_LEADER_EPOCH
      [75, 'skip-and-forget-epoch'], // UNKNOWN_LEADER_EPOCH
      [78, 'skip'], //              OFFSET_NOT_AVAILABLE
      [100, 'stale-metadata'], //   UNKNOWN_TOPIC_ID
      [103, 'stale-metadata'], //   INCONSISTENT_TOPIC_ID
      [106, 'stale-metadata'], //   FETCH_SESSION_TOPIC_ID_ERROR
      [109, 'throw'], //            OFFSET_MOVED_TO_TIERED_STORAGE
    ])('code %i maps to %s', (code, action) => {
      expect(partitionFetchAction(code)).toEqual(action)
    })

    test.each([
      [2], //  CORRUPT_MESSAGE
      [29], // TOPIC_AUTHORIZATION_FAILED
      [76], // UNSUPPORTED_COMPRESSION_TYPE
      [133], // the highest code this client knows
      [9999], // and anything a future broker invents
    ])('an unmapped code %i keeps the previous behaviour and is thrown', (code) => {
      expect(partitionFetchAction(code)).toEqual('throw')
    })
  })

  describe('topLevelFetchAction', () => {
    test.each([
      [70, 'stale-metadata'], //  FETCH_SESSION_ID_NOT_FOUND
      [71, 'stale-metadata'], //  INVALID_FETCH_SESSION_EPOCH
      [106, 'stale-metadata'], // FETCH_SESSION_TOPIC_ID_ERROR
    ])('the fetch-session code %i is handled rather than thrown', (code, action) => {
      expect(topLevelFetchAction(code)).toEqual(action)
    })

    test('any other top-level code is thrown', () => {
      expect(topLevelFetchAction(6)).toEqual('throw')
      expect(topLevelFetchAction(41)).toEqual('throw')
    })
  })

  describe('validationPartitionAction', () => {
    test('no error is ok', () => {
      expect(validationPartitionAction(0)).toEqual('ok')
    })

    test.each([[3], [5], [6], [9], [56], [74], [75], [78]])(
      'code %i is retried against the leader',
      (code) => {
        expect(validationPartitionAction(code)).toEqual('retry')
      }
    )

    test.each([[29], [58], [9999]])('code %i is fatal for this assignment', (code) => {
      expect(validationPartitionAction(code)).toEqual('fatal')
    })
  })

  describe('fetchedPartitionDropReason', () => {
    test('returns null when nothing drops the partition', () => {
      expect(fetchedPartitionDropReason(false, false, false)).toBeNull()
    })

    test.each([
      [[true, false, false], 'seeked'],
      [[false, true, false], 'paused'],
      [[false, false, true], 'awaiting-validation'],
    ])('%o drops for %s', ([seeked, paused, awaitingValidation], reason) => {
      expect(fetchedPartitionDropReason(seeked, paused, awaitingValidation)).toEqual(reason)
    })

    test('the precedence is seeked, then paused, then awaiting validation', () => {
      expect(fetchedPartitionDropReason(true, true, true)).toEqual('seeked')
      expect(fetchedPartitionDropReason(false, true, true)).toEqual('paused')
    })

    test('takes its three flags positionally, so a fetched partition costs no argument object', () => {
      expect(fetchedPartitionDropReason.length).toEqual(3)
    })
  })

  test('the lookups are module-level constants, not per-call constructions', () => {
    // Keying by number exists so a failing partition costs a Map lookup rather than an Error
    // construction or a scan of the 134-entry error table. That cost is measured by the
    // consume benchmark, not here. What this case pins is the *shape* the claim depends on: the
    // module exports four pure functions and **no factory**, so nothing can construct per-call
    // state through this surface. The export-key-set and arity assertions are the two that
    // discriminate; the identity ones only pin the module's shape against a refactor.
    const again = require('../fetchErrorContract')

    expect(again.partitionFetchAction).toBe(partitionFetchAction)
    expect(again.topLevelFetchAction).toBe(topLevelFetchAction)
    expect(again.validationPartitionAction).toBe(validationPartitionAction)
    expect(again.fetchedPartitionDropReason).toBe(fetchedPartitionDropReason)

    expect(Object.keys(again).sort()).toEqual([
      'fetchedPartitionDropReason',
      'partitionFetchAction',
      'topLevelFetchAction',
      'validationPartitionAction',
    ])

    expect(partitionFetchAction.length).toEqual(1) // takes a code, not a factory argument
  })
})
