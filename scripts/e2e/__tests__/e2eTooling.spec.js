const fs = require('fs')
const { evidencePath, writeEvidence } = require('testHelpers')
const { decode } = require('../../../src/protocol/requests/apiVersions/v4/response')
const { transactionVersionOf } = require('../transactionVersionProbe')
const { errorCodeOf } = require('../idempotentAuthzProbe')

describe('E2E tooling', () => {
  describe('evidence files', () => {
    const name = `e2e-tooling-spec-${process.pid}`

    afterEach(() => fs.rmSync(evidencePath(name), { force: true }))

    test('appends one JSON line per record under .tmp/e2e', () => {
      writeEvidence(name, { arm: 'tv2', overlap: 5 })
      writeEvidence(name, { arm: 'tv1', overlap: 2 })

      const lines = fs
        .readFileSync(evidencePath(name), 'utf8')
        .trim()
        .split('\n')
        .map((line) => JSON.parse(line))

      expect(lines).toEqual([
        { at: expect.any(String), arm: 'tv2', overlap: 5 },
        { at: expect.any(String), arm: 'tv1', overlap: 2 },
      ])
      expect(evidencePath(name)).toMatch(/\/\.tmp\/e2e\/e2e-tooling-spec-\d+\.jsonl$/)
    })

    test.each(['../escape', 'Upper', '', 'a/b'])(
      'rejects %p as a name: it names a file, never a path',
      (bad) => {
        expect(() => evidencePath(bad)).toThrow('Evidence name must be lower-case kebab-case')
      }
    )
  })

  describe('transaction-version probe', () => {
    test('reads the finalized level and epoch off an ApiVersions response', () => {
      expect(
        transactionVersionOf({
          finalizedFeaturesEpoch: '12',
          finalizedFeatures: [
            { name: 'metadata.version', maxVersionLevel: 27, minVersionLevel: 27 },
            { name: 'transaction.version', maxVersionLevel: 1, minVersionLevel: 1 },
          ],
        })
      ).toEqual({ finalizedFeaturesEpoch: 12, maxVersionLevel: 1 })
    })

    test.each([
      [
        '4.3.1',
        require('../../../src/protocol/requests/apiVersions/fixtures/v4_response_kafka431.json'),
        148,
      ],
      [
        '4.1.2',
        require('../../../src/protocol/requests/apiVersions/fixtures/v4_response_kafka412.json'),
        142,
      ],
    ])(
      'reads level 2 and the epoch off a response captured from a Kafka %s broker',
      async (_version, fixture, epoch) => {
        const response = await decode(Buffer.from(fixture))

        expect(transactionVersionOf(response)).toEqual({
          finalizedFeaturesEpoch: epoch,
          maxVersionLevel: 2,
        })
      }
    )

    test('reports no level for an absent feature, and the -1 of a broker that sent no features', () => {
      expect(transactionVersionOf({ finalizedFeaturesEpoch: '-1', finalizedFeatures: [] })).toEqual(
        {
          finalizedFeaturesEpoch: -1,
          maxVersionLevel: null,
        }
      )
    })
  })

  describe('idempotent-authz probe', () => {
    test('reads the first numeric code down the cause chain', () => {
      const protocolError = Object.assign(new Error('denied'), { code: 31 })
      const wrapped = new Error('gave up', {
        cause: new Error('wrapper', { cause: protocolError }),
      })

      expect(errorCodeOf(wrapped)).toEqual(31)
    })

    test('answers null for an error carrying no code, and stops on a cycle', () => {
      const looping = new Error('loop')
      looping.cause = looping

      expect(errorCodeOf(new Error('plain'))).toBeNull()
      expect(errorCodeOf(looping)).toBeNull()
    })
  })
})
