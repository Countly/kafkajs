const { compareErrorTables, readSnapshot, SUCCESS_CODE } = require('../errors')
const notes = require('../notes.json')
const config = require('../config.json')
const { errorCodes } = require('../../../src/protocol/error')

const snapshot = (errors) => ({ kafkaTag: 'test', errors })

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

    it('reports a retriable mismatch on a code whose type matches', () => {
      const result = compareErrorTables({
        snapshot: snapshot([{ code: 5, type: 'SAME', retriable: true }]),
        forkErrorCodes: [{ code: 5, type: 'SAME', retriable: false }],
        aliases: {},
      })
      expect(result.retriableMismatches).toEqual([
        { code: 5, type: 'SAME', forkRetriable: false, kafkaRetriable: true },
      ])
    })

    it('reports a retriable mismatch on a code whose type is an accepted alias', () => {
      const result = compareErrorTables({
        snapshot: snapshot([{ code: 5, type: 'KAFKA_NAME', retriable: true }]),
        forkErrorCodes: [{ code: 5, type: 'FORK_NAME', retriable: false }],
        aliases: { FORK_NAME: { kafkaType: 'KAFKA_NAME', reason: 'public API' } },
      })
      expect(result.aliased).toEqual([
        { code: 5, forkType: 'FORK_NAME', kafkaType: 'KAFKA_NAME', reason: 'public API' },
      ])
      expect(result.retriableMismatches).toEqual([
        { code: 5, type: 'FORK_NAME', forkRetriable: false, kafkaRetriable: true },
      ])
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
      expect(result.staleAliases.map((alias) => alias.forkType)).toEqual([
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
      expect(result.retriableMismatches).toEqual([])
    })

    it('records the five historical KafkaJS names as aliases', () => {
      expect(result.aliased.map((a) => a.forkType).sort()).toEqual([
        'GROUP_COORDINATOR_NOT_AVAILABLE',
        'GROUP_LOAD_IN_PROGRESS',
        'NOT_COORDINATOR_FOR_GROUP',
        'NOT_LEADER_FOR_PARTITION',
        'UNKNOWN',
      ])
    })

    it('covers every error code Kafka defines, codes 90-136 included', () => {
      expect(result.counts).toEqual({ kafka: 137, fork: 137 })
      expect(result.missingInFork).toEqual([])
    })
  })

  describe('the committed 4.4.0 snapshot, written from the release Errors.java', () => {
    const NEW_IN_4_4_0 = [
      {
        code: 134,
        type: 'GROUP_DELETION_FAILED',
        exception: 'GroupDeletionFailedException',
        message:
          'DeleteGroups could not complete; see the error message on the per-group result for details.',
      },
      {
        code: 135,
        type: 'STREAMS_TOPOLOGY_DESCRIPTION_UPDATE_FAILED',
        exception: 'StreamsTopologyDescriptionUpdateFailedException',
        message:
          'The broker could not process the topology description update; see the error message for details.',
      },
      {
        code: 136,
        type: 'CONTROLLER_ID_NOT_REGISTERED',
        exception: 'ControllerIdNotRegisteredException',
        message: 'The given controller ID was not registered.',
      },
    ]
    const released = readSnapshot('4.4.0')
    const retained = readSnapshot('4.3.1')

    it('is labelled with the tag it was taken at, the one this tool pins', () => {
      expect(released.kafkaTag).toBe('4.4.0')
      expect(released.kafkaTag).toBe(config.kafkaTag)
      expect(retained.kafkaTag).toBe('4.3.1')
    })

    it('holds every code from -1 to 136 once, each with its own type', () => {
      const codes = released.errors.map((error) => error.code)

      expect(codes).toEqual(Array.from({ length: 138 }, (_, at) => at - 1))
      expect(new Set(released.errors.map((error) => error.type)).size).toBe(138)
    })

    it('adds codes 134, 135 and 136 to the retained codes, exactly as the release words them, none retriable', () => {
      const added = released.errors.filter((error) => error.code >= 134)

      expect(added).toEqual(NEW_IN_4_4_0.map((error) => ({ ...error, retriable: false })))
    })

    it('derives retriable from the exception chain, so a retriable family stays retriable', () => {
      const flag = (code) => released.errors.find((error) => error.code === code).retriable

      expect([flag(3), flag(5), flag(14), flag(15), flag(100)]).toEqual([
        true,
        true,
        true,
        true,
        true,
      ])
      expect([flag(1), flag(29), flag(90), flag(129), flag(134)]).toEqual([
        false,
        false,
        false,
        false,
        false,
      ])
    })

    it('keeps every retained code, with its type and retriable flag', () => {
      const byCode = new Map(released.errors.map((error) => [error.code, error]))

      for (const { code, type, retriable } of retained.errors) {
        expect(byCode.get(code)).toMatchObject({ type, retriable })
      }
    })

    it('is the table the runtime table is compared with, so a removed runtime code is reported', () => {
      const without = errorCodes.filter(({ code }) => code !== 135)
      const result = compareErrorTables({
        snapshot: released,
        forkErrorCodes: without,
        aliases: notes.errorAliases,
      })

      expect(result.missingInFork).toEqual([
        { code: 135, type: 'STREAMS_TOPOLOGY_DESCRIPTION_UPDATE_FAILED' },
      ])
    })
  })
})
