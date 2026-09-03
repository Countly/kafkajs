const { compareErrorTables, readSnapshot, SUCCESS_CODE } = require('../errors')
const notes = require('../notes.json')
const config = require('../config.json')
const { errorCodes } = require('../../../src/protocol/error')

const snapshot = errors => ({ kafkaTag: 'test', errors })

describe('scripts/compat/errors', () => {
  describe('compareErrorTables', () => {
    it('ignores Kafka NONE(0), which is the success sentinel', () => {
      const result = compareErrorTables({
        snapshot: snapshot([
          { code: SUCCESS_CODE, type: 'NONE' },
          { code: 1, type: 'A' },
        ]),
        forkErrorCodes: [{ code: 1, type: 'A' }],
        aliases: {},
      })
      expect(result.counts.kafka).toBe(1)
      expect(result.missingInFork).toEqual([])
    })

    it('reports codes defined upstream but missing here', () => {
      const result = compareErrorTables({
        snapshot: snapshot([
          { code: 1, type: 'A' },
          { code: 2, type: 'B' },
        ]),
        forkErrorCodes: [{ code: 1, type: 'A' }],
        aliases: {},
      })
      expect(result.missingInFork).toEqual([{ code: 2, type: 'B' }])
    })

    it('flags a code this client defines that Kafka does not', () => {
      const result = compareErrorTables({
        snapshot: snapshot([{ code: 1, type: 'A' }]),
        forkErrorCodes: [
          { code: 1, type: 'A' },
          { code: 900, type: 'INVENTED' },
        ],
        aliases: {},
      })
      expect(result.unknownToKafka).toEqual([{ code: 900, type: 'INVENTED' }])
    })

    it('flags an unaliased type-name mismatch', () => {
      const result = compareErrorTables({
        snapshot: snapshot([{ code: 5, type: 'KAFKA_NAME' }]),
        forkErrorCodes: [{ code: 5, type: 'FORK_NAME' }],
        aliases: {},
      })
      expect(result.mismatches).toEqual([
        { code: 5, forkType: 'FORK_NAME', kafkaType: 'KAFKA_NAME' },
      ])
      expect(result.aliased).toEqual([])
    })

    it('accepts a mismatch that is declared as an alias', () => {
      const result = compareErrorTables({
        snapshot: snapshot([{ code: 5, type: 'KAFKA_NAME' }]),
        forkErrorCodes: [{ code: 5, type: 'FORK_NAME' }],
        aliases: { FORK_NAME: { kafkaType: 'KAFKA_NAME', reason: 'public API' } },
      })
      expect(result.mismatches).toEqual([])
      expect(result.aliased).toEqual([
        { code: 5, forkType: 'FORK_NAME', kafkaType: 'KAFKA_NAME', reason: 'public API' },
      ])
    })

    it('rejects an alias that points at the wrong Kafka name', () => {
      const result = compareErrorTables({
        snapshot: snapshot([{ code: 5, type: 'KAFKA_NAME' }]),
        forkErrorCodes: [{ code: 5, type: 'FORK_NAME' }],
        aliases: { FORK_NAME: { kafkaType: 'SOMETHING_ELSE', reason: 'stale' } },
      })
      expect(result.mismatches).toHaveLength(1)
      expect(result.staleAliases).toEqual([{ forkType: 'FORK_NAME', kafkaType: 'SOMETHING_ELSE' }])
    })

    it('reports an alias that no longer applies as stale', () => {
      const result = compareErrorTables({
        snapshot: snapshot([{ code: 5, type: 'SAME' }]),
        forkErrorCodes: [{ code: 5, type: 'SAME' }],
        aliases: { OLD_NAME: { kafkaType: 'SAME', reason: 'renamed long ago' } },
      })
      expect(result.staleAliases).toEqual([{ forkType: 'OLD_NAME', kafkaType: 'SAME' }])
    })

    it('sorts staleAliases with a plain ASCII comparator, not locale-dependent collation', () => {
      // Byte-stable sort constraint: localeCompare orders this set as
      // ['alpha', 'NOT_LEADER', 'NOTLEADER', 'Zeta'] under default collation; ASCII order
      // (underscore sorts after every uppercase letter, and all uppercase before lowercase)
      // gives a different, code-point-only order. A future `localeCompare` reintroduction
      // flips this and fails the test.
      const result = compareErrorTables({
        snapshot: snapshot([]),
        forkErrorCodes: [],
        aliases: {
          NOT_LEADER: { kafkaType: 'X', reason: 'r' },
          NOTLEADER: { kafkaType: 'X', reason: 'r' },
          Zeta: { kafkaType: 'X', reason: 'r' },
          alpha: { kafkaType: 'X', reason: 'r' },
        },
      })
      expect(result.staleAliases.map(alias => alias.forkType)).toEqual([
        'NOTLEADER',
        'NOT_LEADER',
        'Zeta',
        'alpha',
      ])
    })
  })

  describe('against the committed snapshot and src/protocol/error.js', () => {
    const result = compareErrorTables({
      snapshot: readSnapshot(config.kafkaTag),
      forkErrorCodes: errorCodes,
      aliases: notes.errorAliases,
    })

    it('has no failure-class finding', () => {
      expect(result.unknownToKafka).toEqual([])
      expect(result.mismatches).toEqual([])
      expect(result.staleAliases).toEqual([])
    })

    it('records the five historical KafkaJS names as aliases', () => {
      expect(result.aliased.map(a => a.forkType).sort()).toEqual([
        'GROUP_COORDINATOR_NOT_AVAILABLE',
        'GROUP_LOAD_IN_PROGRESS',
        'NOT_COORDINATOR_FOR_GROUP',
        'NOT_LEADER_FOR_PARTITION',
        'UNKNOWN',
      ])
    })

    it('reports the codes Phase 4 still has to add', () => {
      expect(result.counts).toEqual({ kafka: 134, fork: 89 })
      expect(result.missingInFork).toHaveLength(45)
      expect(result.missingInFork[0]).toEqual({ code: 89, type: 'THROTTLING_QUOTA_EXCEEDED' })
      expect(result.missingInFork[44]).toEqual({ code: 133, type: 'SHARE_SESSION_LIMIT_REACHED' })
    })
  })
})
