const fs = require('fs')
const path = require('path')
const { evidencePath, writeEvidence } = require('testHelpers')
const { decode } = require('../../../src/protocol/requests/apiVersions/v4/response')
const { readSpec } = require('../../compat/parse')
const { transactionVersionOf } = require('../transactionVersionProbe')
const { errorCodeOf } = require('../idempotentAuthzProbe')
const { PROBED_APIS, versionsOf } = require('../apiVersionsProbe')

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

  describe('api-versions probe', () => {
    const SNAPSHOT = path.join(__dirname, '..', '..', 'compat', 'messages', '4.3.1')
    const ON_4_3 = {
      Metadata: [0, 13],
      OffsetCommit: [2, 10],
      OffsetFetch: [1, 10],
      JoinGroup: [0, 9],
      SyncGroup: [0, 5],
      Heartbeat: [0, 4],
      LeaveGroup: [0, 5],
      CreateTopics: [2, 7],
      DeleteTopics: [1, 6],
      CreatePartitions: [0, 3],
      DeleteRecords: [0, 2],
      DeleteGroups: [0, 2],
      DescribeAcls: [1, 3],
      CreateAcls: [1, 3],
      DeleteAcls: [1, 3],
      DescribeConfigs: [1, 4],
      AlterConfigs: [0, 2],
      IncrementalAlterConfigs: [0, 1],
      DescribeTopicPartitions: [0, 0],
    }
    // A 4.1 broker hides the unstable OffsetCommit and OffsetFetch v10 from ApiVersions.
    const ON_4_1 = { ...ON_4_3, OffsetCommit: [2, 9], OffsetFetch: [1, 9] }

    test.each([
      [
        '4.3.1',
        require('../../../src/protocol/requests/apiVersions/fixtures/v4_response_kafka431.json'),
        ON_4_3,
      ],
      [
        '4.1.2',
        require('../../../src/protocol/requests/apiVersions/fixtures/v4_response_kafka412.json'),
        ON_4_1,
      ],
    ])(
      'reads the probed ranges off a response captured from a Kafka %s broker',
      async (_v, fixture, expected) => {
        const { apiVersions } = await decode(Buffer.from(fixture))

        expect(versionsOf(apiVersions)).toEqual(expected)
      }
    )

    test('lists the nineteen probed APIs in the order the line prints them', () => {
      expect(Object.keys(versionsOf([]))).toEqual([
        'Metadata',
        'OffsetCommit',
        'OffsetFetch',
        'JoinGroup',
        'SyncGroup',
        'Heartbeat',
        'LeaveGroup',
        'CreateTopics',
        'DeleteTopics',
        'CreatePartitions',
        'DeleteRecords',
        'DeleteGroups',
        'DescribeAcls',
        'CreateAcls',
        'DeleteAcls',
        'DescribeConfigs',
        'AlterConfigs',
        'IncrementalAlterConfigs',
        'DescribeTopicPartitions',
      ])
    })

    test('reports null for an API the broker does not list', () => {
      const versions = versionsOf([{ apiKey: 3, minVersion: 0, maxVersion: 13 }])

      expect(versions.Metadata).toEqual([0, 13])
      expect(versions.DescribeTopicPartitions).toBeNull()
      expect(Object.values(versions).filter((range) => range === null)).toHaveLength(18)
    })

    test.each(Object.entries(PROBED_APIS))(
      '%s is key %i in the committed Kafka 4.3.1 message definition, whose newest version is what a 4.3.1 broker offers',
      (name, apiKey) => {
        const spec = readSpec(path.join(SNAPSHOT, `${name}Request.json`))

        expect(spec.apiKey).toEqual(apiKey)
        expect(spec.validVersions.max).toEqual(ON_4_3[name][1])
      }
    )
  })
})
