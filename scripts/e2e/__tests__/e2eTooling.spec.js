const { spawnSync } = require('child_process')
const fs = require('fs')
const os = require('os')
const path = require('path')
const { evidencePath, writeEvidence, useEvidenceRun } = require('testHelpers')
const { STAMP_FIELDS } = require('../../../testHelpers/e2eEvidence')
const { decode } = require('../../../src/protocol/requests/apiVersions/v4/response')
const { readSpec } = require('../../compat/parse')
const leaderElection = require('../../../testHelpers/leaderElection')
const {
  BROKER_VERSION_ARGV,
  parseBrokerVersion,
  readBrokerVersion,
} = require('../../../testHelpers/brokerVersion')
const { transactionVersionOf } = require('../transactionVersionProbe')
const { errorCodeOf } = require('../idempotentAuthzProbe')
const {
  PROBED_APIS,
  versionsOf,
  allVersionsOf,
  apiNames,
  rangeOf,
  SHARE_VERSION,
  featuresOf,
  recordRanges,
  assertSameVersion,
  main,
} = require('../apiVersionsProbe')

describe('E2E tooling', () => {
  describe('evidence files', () => {
    const name = `e2e-tooling-spec-${process.pid}`
    // Supplied here, so the unit lane needs no Docker, no Git and no environment.
    const run = {
      brokerVersion: '4.3.1',
      node: 'v26.7.0',
      tree: '0123456789abcdef0123456789abcdef01234567',
      lane: 'docker',
    }
    const stamps = { ...run, wireVersions: {}, config: {} }

    beforeEach(() => useEvidenceRun(run))

    afterEach(() => {
      useEvidenceRun()
      fs.rmSync(evidencePath(name), { force: true })
    })

    test('appends one JSON line per record under .tmp/e2e', () => {
      writeEvidence(name, { arm: 'tv2', overlap: 5 })
      writeEvidence(name, { arm: 'tv1', overlap: 2 })

      const lines = fs
        .readFileSync(evidencePath(name), 'utf8')
        .trim()
        .split('\n')
        .map((line) => JSON.parse(line))

      expect(lines).toEqual([
        { at: expect.any(String), arm: 'tv2', overlap: 5, ...stamps },
        { at: expect.any(String), arm: 'tv1', overlap: 2, ...stamps },
      ])
      expect(evidencePath(name)).toMatch(/\/\.tmp\/e2e\/e2e-tooling-spec-\d+\.jsonl$/)
    })

    test('stamps every line with each mandatory field, the run’s identity winning over the record’s', () => {
      writeEvidence(name, { arm: 'tv2', tree: 'forged', lane: 'fake' })

      const [line] = fs
        .readFileSync(evidencePath(name), 'utf8')
        .trim()
        .split('\n')
        .map((row) => JSON.parse(row))

      expect(Object.keys(line)).toEqual(expect.arrayContaining(STAMP_FIELDS))
      expect(line).toMatchObject({ arm: 'tv2', ...stamps })
    })

    test.each([
      ['brokerVersion', ''],
      ['node', undefined],
      ['tree', ''],
      ['lane', 'chaos'],
    ])('refuses a run whose %s is %p, so no line is stamped with a guess', (field, value) => {
      expect(() => useEvidenceRun({ ...run, [field]: value })).toThrow(`no valid ${field}`)
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

  describe('api-versions probe, the full table', () => {
    const CAPTURES = {
      '4.3.1': require('../../../src/protocol/requests/apiVersions/fixtures/v4_response_kafka431.json'),
      '4.1.2': require('../../../src/protocol/requests/apiVersions/fixtures/v4_response_kafka412.json'),
    }
    const RANGES = path.join(__dirname, '..', '..', 'protocol', 'brokerRanges.json')
    const decoded = async (version) => (await decode(Buffer.from(CAPTURES[version]))).apiVersions

    test('lists every API a broker offers, by the name the client gives it, in key order', async () => {
      const all = allVersionsOf(await decoded('4.3.1'))

      expect(Object.keys(all)).toHaveLength(75)
      expect(Object.keys(all).slice(0, 5)).toEqual([
        'Produce',
        'Fetch',
        'ListOffsets',
        'Metadata',
        'OffsetCommit',
      ])
      expect(all).toMatchObject({
        Metadata: [0, 13],
        GroupCoordinator: [0, 6],
        ConsumerGroupHeartbeat: [0, 1],
        DescribeTopicPartitions: [0, 0],
      })
    })

    test('orders the APIs by key whatever order the broker sent them in', () => {
      const all = allVersionsOf([
        { apiKey: 3, minVersion: 0, maxVersion: 13 },
        { apiKey: 0, minVersion: 0, maxVersion: 13 },
        { apiKey: 18, minVersion: 0, maxVersion: 4 },
      ])

      expect(Object.keys(all)).toEqual(['Produce', 'Metadata', 'ApiVersions'])
    })

    test('names a key the name table lacks ApiKey<n>', async () => {
      const all = allVersionsOf(await decoded('4.3.1'), new Map())

      expect(Object.keys(all)).toHaveLength(75)
      expect(all.ApiKey3).toEqual([0, 13])
      expect(all.Metadata).toBeUndefined()
      expect(allVersionsOf([{ apiKey: 9999, minVersion: 1, maxVersion: 2 }])).toEqual({
        ApiKey9999: [1, 2],
      })
    })

    test('the name table prefers the client name to the definition name and has none for an unknown key', () => {
      const names = apiNames()

      expect(names.get(10)).toBe('GroupCoordinator')
      expect(names.get(75)).toBe('DescribeTopicPartitions')
      expect(names.get(9999)).toBeUndefined()
    })

    describe('rangeOf', () => {
      test('reads a range by the name the client gives the key', () => {
        expect(rangeOf({ Metadata: [0, 13] }, 3)).toEqual([0, 13])
      })

      test.each([
        ['has no name for the key', new Map()],
        ['names the key after the entry was recorded', new Map([[47, 'OffsetDelete']])],
      ])('reads a range recorded as ApiKey<n> when the table %s', (_why, names) => {
        expect(rangeOf({ ApiKey47: [0, 0] }, 47, names)).toEqual([0, 0])
      })

      test('prefers the name when the entry was recorded with it', () => {
        const names = new Map([[47, 'OffsetDelete']])

        expect(rangeOf({ OffsetDelete: [0, 1], ApiKey47: [0, 0] }, 47, names)).toEqual([0, 1])
      })

      test('gives undefined for a key the entry holds under neither name', () => {
        expect(rangeOf({ Metadata: [0, 13] }, 9999)).toBeUndefined()
      })
    })

    test('a 4.1 broker offers fewer APIs and hides the unstable OffsetCommit v10', async () => {
      const all = allVersionsOf(await decoded('4.1.2'))

      expect(Object.keys(all)).toHaveLength(73)
      expect(all.OffsetCommit).toEqual([2, 9])
    })

    describe('assertSameVersion', () => {
      test.each([
        ['4.3.1', '4.3.1'],
        ['4.0.2', undefined],
        ['4.1.2', ''],
      ])('accepts a broker reporting %s when %p was asked for', (reported, requested) => {
        expect(() => assertSameVersion(reported, requested)).not.toThrow()
      })

      test('refuses a broker that is not the version the lane asked for, naming both', () => {
        expect(() => assertSameVersion('4.1.2', '4.3.1')).toThrow(
          'The broker reports 4.1.2 but KAFKA_VERSION=4.3.1: stopping'
        )
      })
    })

    describe('recordRanges', () => {
      let directory

      beforeEach(() => {
        directory = fs.mkdtempSync(path.join(os.tmpdir(), 'broker-ranges-spec-'))
      })

      afterEach(() => fs.rmSync(directory, { recursive: true, force: true }))

      const read = (file) => JSON.parse(fs.readFileSync(file, 'utf8'))

      test('creates the table, adds a version without touching the others and orders them', () => {
        const file = path.join(directory, 'ranges.json')

        recordRanges(file, '4.3.1', { Metadata: [0, 13] })
        recordRanges(file, '4.0.2', { Metadata: [0, 12] })
        recordRanges(file, '4.10.0', { Metadata: [0, 14] })

        expect(Object.keys(read(file))).toEqual(['4.0.2', '4.3.1', '4.10.0'])
        expect(read(file)['4.3.1']).toEqual({ Metadata: [0, 13] })
        expect(fs.readFileSync(file, 'utf8').endsWith('}\n')).toBe(true)
      })

      test('replaces the entry of a version it already holds', () => {
        const file = path.join(directory, 'ranges.json')
        recordRanges(file, '4.3.1', { Metadata: [0, 13], Fetch: [4, 11] })
        recordRanges(file, '4.1.2', { Metadata: [0, 12] })

        recordRanges(file, '4.3.1', { Metadata: [1, 13] })

        expect(read(file)).toEqual({
          '4.1.2': { Metadata: [0, 12] },
          '4.3.1': { Metadata: [1, 13] },
        })
      })

      test.each(['4.3', 'latest', '4.3.1-rc1', ''])('refuses %p as a version', (version) => {
        expect(() => recordRanges(path.join(directory, 'ranges.json'), version, {})).toThrow(
          'A broker version is major.minor.patch'
        )
      })
    })

    describe('main', () => {
      const asked = process.env.KAFKA_VERSION
      let directory
      let file
      let io

      beforeEach(() => {
        delete process.env.KAFKA_VERSION
        directory = fs.mkdtempSync(path.join(os.tmpdir(), 'broker-ranges-spec-'))
        file = path.join(directory, 'ranges.json')
        io = {
          readApiVersions: jest.fn(async () => ({ apiVersions: await decoded('4.1.2') })),
          readBrokerVersion: jest.fn(() => '4.0.2'),
          writeEvidence: jest.fn(),
        }
        jest.spyOn(console, 'log').mockImplementation(() => {})
      })

      afterEach(() => {
        jest.restoreAllMocks()
        fs.rmSync(directory, { recursive: true, force: true })
        if (asked === undefined) {
          delete process.env.KAFKA_VERSION
        } else {
          process.env.KAFKA_VERSION = asked
        }
      })

      const printed = () => JSON.parse(console.log.mock.calls[0][0])

      const lineOf = (apiVersions) => ({
        event: 'api-versions',
        kafkaVersion: '4.0.2',
        versions: allVersionsOf(apiVersions),
      })

      test('stops on a broker that is not the version asked for, before it writes anything', async () => {
        process.env.KAFKA_VERSION = '4.3.1'
        io.readBrokerVersion.mockReturnValue('4.1.2')
        fs.writeFileSync(file, '{"4.0.2":{"Metadata":[0,12]}}\n')

        await expect(main([`--record=${file}`], io)).rejects.toThrow(
          'The broker reports 4.1.2 but KAFKA_VERSION=4.3.1: stopping'
        )

        expect(fs.readFileSync(file, 'utf8')).toBe('{"4.0.2":{"Metadata":[0,12]}}\n')
        expect(io.writeEvidence).not.toHaveBeenCalled()
        expect(console.log).not.toHaveBeenCalled()
      })

      test('--all lists every API under the version the broker reports and records nothing', async () => {
        const { apiVersions } = await io.readApiVersions()

        await main(['--all'], io)

        expect(printed()).toEqual(lineOf(apiVersions))
        expect(io.writeEvidence).toHaveBeenCalledWith('api-versions-probe', lineOf(apiVersions))
        expect(fs.readdirSync(directory)).toEqual([])
      })

      test('--record writes that full table under the version the broker reports', async () => {
        const { apiVersions } = await io.readApiVersions()

        await main([`--record=${file}`], io)

        expect(printed()).toEqual(lineOf(apiVersions))
        expect(io.writeEvidence).toHaveBeenCalledWith('api-versions-probe', lineOf(apiVersions))
        expect(JSON.parse(fs.readFileSync(file, 'utf8'))).toEqual({
          '4.0.2': allVersionsOf(apiVersions),
        })
      })

      test.each([
        ['4.1.2', '4.1.2'],
        [undefined, '4.4.0'],
      ])(
        'lists the probed APIs under KAFKA_VERSION=%p, as %p, without asking the broker',
        async (requested, kafkaVersion) => {
          if (requested) {
            process.env.KAFKA_VERSION = requested
          }
          const { apiVersions } = await io.readApiVersions()

          await main([], io)

          expect(printed()).toEqual({
            event: 'api-versions',
            kafkaVersion,
            versions: versionsOf(apiVersions),
          })
          expect(io.readBrokerVersion).not.toHaveBeenCalled()
          expect(io.writeEvidence).toHaveBeenCalledTimes(1)
          expect(fs.readdirSync(directory)).toEqual([])
        }
      )

      test.each([['--record='], ['--record']])(
        'refuses %p, which names no file, before it reads or writes anything',
        async (flag) => {
          await expect(main([flag, file], io)).rejects.toThrow('Usage: node')

          expect(io.readApiVersions).not.toHaveBeenCalled()
          expect(io.writeEvidence).not.toHaveBeenCalled()
          expect(fs.readdirSync(directory)).toEqual([])
        }
      )

      test.each([['--record='], ['--record']])(
        'exits 1 with the usage on the command line for %p',
        (flag) => {
          const probe = path.join(__dirname, '..', 'apiVersionsProbe.js')

          const run = spawnSync(process.execPath, [probe, flag, file], { encoding: 'utf8' })

          expect(run.status).toBe(1)
          expect(run.stderr).toContain('Usage: node scripts/e2e/apiVersionsProbe.js')
          expect(run.stdout).toBe('')
          expect(fs.readdirSync(directory)).toEqual([])
        }
      )

      describe('--record-features', () => {
        const features = () => path.join(directory, 'features.json')
        const answerOf = async (version) => decode(Buffer.from(CAPTURES[version]))

        test('records the finalized share.version under the version the broker reports, after the API line', async () => {
          const answer = await answerOf('4.3.1')
          io.readApiVersions.mockResolvedValue(answer)
          io.readBrokerVersion.mockReturnValue('4.3.1')
          fs.writeFileSync(features(), '{\n  "4.0.2": {}\n}\n')
          const featureLine = {
            event: 'finalized-features',
            kafkaVersion: '4.3.1',
            features: { 'share.version': 1 },
          }

          await main([`--record-features=${features()}`], io)

          expect(console.log.mock.calls.map(([text]) => JSON.parse(text).event)).toEqual([
            'api-versions',
            'finalized-features',
          ])
          expect(io.readBrokerVersion).toHaveBeenCalledTimes(1)
          expect(JSON.parse(console.log.mock.calls[1][0])).toEqual(featureLine)
          expect(io.writeEvidence.mock.calls).toEqual([
            [
              'api-versions-probe',
              {
                event: 'api-versions',
                kafkaVersion: '4.3.1',
                versions: allVersionsOf(answer.apiVersions),
              },
            ],
            ['api-versions-probe', featureLine],
          ])
          expect(fs.readFileSync(features(), 'utf8')).toBe(
            '{\n  "4.0.2": {},\n  "4.3.1": {\n    "share.version": 1\n  }\n}\n'
          )
          expect(fs.readdirSync(directory)).toEqual(['features.json'])
        })

        test('writes both tables from one answer when --record comes too', async () => {
          const answer = await answerOf('4.1.2')
          io.readApiVersions.mockResolvedValue(answer)
          io.readBrokerVersion.mockReturnValue('4.1.2')

          await main([`--record=${file}`, `--record-features=${features()}`], io)

          expect(io.readApiVersions).toHaveBeenCalledTimes(1)
          expect(JSON.parse(fs.readFileSync(file, 'utf8'))).toEqual({
            '4.1.2': allVersionsOf(answer.apiVersions),
          })
          expect(JSON.parse(fs.readFileSync(features(), 'utf8'))).toEqual({
            '4.1.2': { 'share.version': 0 },
          })
        })

        test.each([['--record-features='], ['--record-features']])(
          'refuses %p, which names no file, before it reads or writes anything',
          async (flag) => {
            await expect(main([flag], io)).rejects.toThrow('Usage: node')

            expect(io.readApiVersions).not.toHaveBeenCalled()
            expect(io.writeEvidence).not.toHaveBeenCalled()
            expect(fs.readdirSync(directory)).toEqual([])
          }
        )

        test('stops before it writes anything on an answer that carries no finalized features', async () => {
          await expect(
            main([`--record=${file}`, `--record-features=${features()}`], io)
          ).rejects.toThrow('The broker sent no finalized features (epoch -1): nothing to record')

          expect(io.writeEvidence).not.toHaveBeenCalled()
          expect(console.log).not.toHaveBeenCalled()
          expect(fs.readdirSync(directory)).toEqual([])
        })
      })
    })

    describe('scripts/protocol/brokerRanges.json', () => {
      const table = () => JSON.parse(fs.readFileSync(RANGES, 'utf8'))

      test('every entry is a Kafka version holding [min, max] pairs of non-negative integers', () => {
        for (const [version, versions] of Object.entries(table())) {
          expect(version).toMatch(/^\d+\.\d+\.\d+$/)
          expect(Object.keys(versions).length).toBeGreaterThan(40)
          for (const range of Object.values(versions)) {
            expect(range).toHaveLength(2)
            expect(range.every((n) => Number.isInteger(n) && n >= 0)).toBe(true)
            expect(range[0]).toBeLessThanOrEqual(range[1])
          }
        }
      })

      test.each(['4.3.1', '4.1.2'])(
        'the %s entry is what the capture of that broker offers',
        async (version) => {
          const entry = table()[version]
          const captured = await decoded(version)

          expect(Object.keys(entry)).toHaveLength(captured.length)
          for (const { apiKey, minVersion, maxVersion } of captured) {
            expect(rangeOf(entry, apiKey)).toEqual([minVersion, maxVersion])
          }
        }
      )

      test('holds the floor image, the three lines the suite runs on and the 4.3.2 and 4.4.0 captures', () => {
        const versions = Object.keys(table())

        expect(versions).toHaveLength(6)
        expect(versions).toEqual(
          expect.arrayContaining(['4.1.2', '4.2.2', '4.3.1', '4.3.2', '4.4.0'])
        )
        expect(versions.filter((version) => /^4\.0\.\d+$/.test(version))).toHaveLength(1)
      })
    })

    describe('finalized features', () => {
      const supported = [{ name: 'share.version', minVersion: 0, maxVersion: 1 }]

      test.each([
        ['4.3.1', { 'share.version': 1 }],
        ['4.1.2', { 'share.version': 0 }],
      ])(
        'reads the share.version of a Kafka %s broker off its capture',
        async (version, expected) => {
          expect(featuresOf(await decode(Buffer.from(CAPTURES[version])))).toEqual(expected)
        }
      )

      test.each([
        [
          'the finalized maximum level',
          {
            supportedFeatures: supported,
            finalizedFeatures: [{ name: 'share.version', minVersionLevel: 0, maxVersionLevel: 1 }],
            finalizedFeaturesEpoch: '45',
          },
          { 'share.version': 1 },
        ],
        [
          'the finalized level 2 of a synthetic answer that supports 0 to 2',
          {
            supportedFeatures: [{ name: 'share.version', minVersion: 0, maxVersion: 2 }],
            finalizedFeatures: [{ name: 'share.version', minVersionLevel: 0, maxVersionLevel: 2 }],
            finalizedFeaturesEpoch: '120',
          },
          { 'share.version': 2 },
        ],
        [
          '0 for a feature supported but not finalized',
          { supportedFeatures: supported, finalizedFeatures: [], finalizedFeaturesEpoch: '359' },
          { 'share.version': 0 },
        ],
        [
          '0 for a feature supported but not finalized, at epoch 0',
          { supportedFeatures: supported, finalizedFeatures: [], finalizedFeaturesEpoch: '0' },
          { 'share.version': 0 },
        ],
        [
          'no key for a broker without the feature',
          {
            supportedFeatures: [{ name: 'group.version', minVersion: 0, maxVersion: 1 }],
            finalizedFeatures: [{ name: 'group.version', minVersionLevel: 1, maxVersionLevel: 1 }],
            finalizedFeaturesEpoch: '25',
          },
          {},
        ],
        [
          'a finalized level even when the broker lists no support for it',
          {
            supportedFeatures: [],
            finalizedFeatures: [{ name: 'share.version', minVersionLevel: 2, maxVersionLevel: 2 }],
            finalizedFeaturesEpoch: '7',
          },
          { 'share.version': 2 },
        ],
      ])('records %s', (_what, answer, expected) => {
        expect(featuresOf(answer)).toEqual(expected)
      })

      test.each([['-1'], [undefined]])(
        'refuses an answer whose finalized-features epoch is %p',
        (finalizedFeaturesEpoch) => {
          expect(() =>
            featuresOf({
              supportedFeatures: supported,
              finalizedFeatures: [],
              finalizedFeaturesEpoch,
            })
          ).toThrow('The broker sent no finalized features (epoch -1): nothing to record')
        }
      )
    })

    describe('scripts/protocol/brokerFeatures.json', () => {
      const FEATURES = path.join(__dirname, '..', '..', 'protocol', 'brokerFeatures.json')
      const features = () => JSON.parse(fs.readFileSync(FEATURES, 'utf8'))
      const ranges = () => JSON.parse(fs.readFileSync(RANGES, 'utf8'))
      const SHARE_API_KEYS = [76, 77, 78, 79, 90, 91, 92]

      const { readBrokerCapture } = require('../../../testHelpers/brokerCapture')

      test.each(Object.entries(features()))(
        '%s records the share.version its capture showed',
        (line, entry) => {
          expect(line).toMatch(/^\d+\.\d+\.\d+$/)
          expect(entry).toEqual(expect.any(Object))
          expect(Array.isArray(entry)).toBe(false)
          expect(Object.keys(entry).filter((name) => name !== SHARE_VERSION)).toEqual([])
          if (Object.hasOwn(entry, SHARE_VERSION)) {
            expect(Number.isInteger(entry[SHARE_VERSION])).toBe(true)
            expect(entry[SHARE_VERSION]).toBeGreaterThanOrEqual(0)
          }

          const { features: evidence } = readBrokerCapture(line)
          const expected = {}
          for (const feature of evidence.finalizedFeatures) {
            if (feature.name === SHARE_VERSION) expected[SHARE_VERSION] = feature.maxVersionLevel
          }
          if (
            !Object.hasOwn(expected, SHARE_VERSION) &&
            evidence.supportedFeatures.some((feature) => feature.name === SHARE_VERSION)
          ) {
            expected[SHARE_VERSION] = 0
          }
          expect(entry).toEqual(expected)
        }
      )

      test('covers the lines of brokerRanges.json, in the layout the recorder writes', () => {
        expect(Object.keys(features())).toEqual(Object.keys(ranges()))
        expect(fs.readFileSync(FEATURES, 'utf8')).toBe(`${JSON.stringify(features(), null, 2)}\n`)
      })

      test.each(Object.keys(features()))(
        'the %s line records share.version exactly when it offers the seven share APIs',
        (line) => {
          const offered = SHARE_API_KEYS.filter((apiKey) => rangeOf(ranges()[line] ?? {}, apiKey))

          expect([0, SHARE_API_KEYS.length]).toContain(offered.length)
          expect(SHARE_VERSION in features()[line]).toBe(offered.length === SHARE_API_KEYS.length)
        }
      )

      test.each(['4.3.1', '4.1.2'])(
        'the %s entry is what the probe reads off the capture of that broker',
        async (version) => {
          const answer = await decode(Buffer.from(CAPTURES[version]))

          expect(features()[version]).toEqual(featuresOf(answer))
        }
      )
    })
  })

  describe('share compose variants', () => {
    const ROOT = path.join(__dirname, '..', '..', '..')
    const VARIANTS = ['docker-compose.share.yml', 'docker-compose.share-limit.yml']
    const read = (file) => fs.readFileSync(path.join(ROOT, file), 'utf8')
    const DEAD_LETTER_QUEUE = /KAFKA_ERRORS_DEADLETTERQUEUE_|errors\.deadletterqueue\./i
    const ANCHOR = "    KAFKA_CONNECTIONS_MAX_REAUTH_MS: '15000'\n"

    /**
     * The lines of a compose or property file that set something: every line but a full-line comment.
     * @param {string} text
     * @returns {string[]}
     */
    const settingsOf = (text) => text.split('\n').filter((line) => /^\s*[^\s#]/.test(line))

    /**
     * The settings that configure a dead-letter queue, whether written as a broker environment key
     * (`KAFKA_ERRORS_DEADLETTERQUEUE_*`) or as a property or JVM option (`errors.deadletterqueue.*`).
     * @param {string} text
     * @returns {string[]}
     */
    const deadLetterSettings = (text) =>
      settingsOf(text).filter((line) => DEAD_LETTER_QUEUE.test(line))

    /**
     * Every file a variant takes broker settings from: itself, the base file it extends and the
     * broker config files it mounts.
     * @param {string} name the variant
     * @returns {string[]} repository-relative paths
     */
    const sourcesOf = (name) => {
      const mounted = [...read(name).matchAll(/^\s+- \.\/(testHelpers\/kafka\/[^:\s]+):/gm)]
      return [name, 'docker-compose.yml', ...mounted.map(([, file]) => file)]
    }

    test.each(VARIANTS)('%s and the configuration it inherits set no dead-letter queue', (name) => {
      const sources = sourcesOf(name)

      expect(sources).toEqual([
        name,
        'docker-compose.yml',
        'testHelpers/kafka/dns.security',
        'testHelpers/kafka/server-jaas.conf',
      ])
      expect(
        sources.flatMap((file) =>
          deadLetterSettings(read(file)).map((line) => `${file}: ${line.trim()}`)
        )
      ).toEqual([])
    })

    test.each([
      ['an environment key', "    KAFKA_ERRORS_DEADLETTERQUEUE_TOPIC_NAME: 'dlq'"],
      ['a property', 'errors.deadletterqueue.topic.name=dlq'],
      ['a JVM option', "    KAFKA_OPTS: '-Derrors.deadletterqueue.copy.record.enable=true'"],
    ])('flags %s as a dead-letter-queue setting', (_form, line) => {
      expect(deadLetterSettings(`services:\n${line}\n`)).toEqual([line])
    })

    test('does not flag a comment that names a dead-letter-queue setting', () => {
      expect(deadLetterSettings('# errors.deadletterqueue.topic.name is never set\n')).toEqual([])
    })

    test.each(VARIANTS)(
      'flags a dead-letter-queue key added to the environment every broker of %s shares',
      (name) => {
        const added = "    KAFKA_ERRORS_DEADLETTERQUEUE_TOPIC_NAME: 'dlq'"
        const text = read(name)

        expect(text.split(ANCHOR)).toHaveLength(2)
        expect(deadLetterSettings(text.replace(ANCHOR, `${ANCHOR}${added}\n`))).toEqual([added])
      }
    )
  })

  describe('the broker version command', () => {
    afterEach(() => jest.restoreAllMocks())

    test.each([
      ['4.0.2\n', '4.0.2'],
      ['4.3.1 (Commit:abcdef0123456789)\n', '4.3.1'],
      ['\n  4.1.2  \nCommit Id: 9.9.9\n', '4.1.2'],
      [Buffer.from('4.2.2\n'), '4.2.2'],
    ])('reads the version from %p', (output, version) => {
      expect(parseBrokerVersion(output)).toBe(version)
    })

    test.each([
      '',
      '\n',
      'usage: kafka-broker-api-versions\n',
      '4.3\n',
      'WARNING\n4.3.1\n',
      '4.3.1-SNAPSHOT\n',
      '4.0.2.1\n',
      'log4j:WARN No appenders (1.2.17)\n4.0.2\n',
    ])('refuses %p: the first non-empty line does not start with major.minor.patch', (output) => {
      expect(() => parseBrokerVersion(output)).toThrow('Cannot read a broker version')
    })

    test('runs its argv in kafka1, on a copy, and returns what the tool prints', () => {
      const exec = jest
        .spyOn(leaderElection, 'execInKafka1')
        .mockReturnValue(Buffer.from('4.0.2\n'))

      expect(readBrokerVersion()).toBe('4.0.2')

      expect(exec).toHaveBeenCalledTimes(1)
      expect(exec.mock.calls[0][0]).toEqual([
        '/opt/kafka/bin/kafka-broker-api-versions.sh',
        '--version',
      ])
      expect(exec.mock.calls[0][0]).not.toBe(BROKER_VERSION_ARGV)
      expect(Object.isFrozen(BROKER_VERSION_ARGV)).toBe(true)
    })

    test('lets the failure of the container call through', () => {
      jest.spyOn(leaderElection, 'execInKafka1').mockImplementation(() => {
        throw new Error('no such service: kafka1')
      })

      expect(() => readBrokerVersion()).toThrow('no such service: kafka1')
    })
  })

  describe('Java member launcher', () => {
    let directory
    let cache
    let bin
    const launcher = path.join(__dirname, '..', 'javaConsumer', 'run.sh')

    beforeEach(() => {
      directory = fs.mkdtempSync(path.join(os.tmpdir(), 'java-launcher-'))
      cache = path.join(directory, 'cache')
      bin = path.join(directory, 'bin')
      fs.mkdirSync(cache)
      fs.mkdirSync(bin)
      for (const name of ['kafka-clients-4.3.1.jar', 'slf4j-api-1.7.36.jar']) {
        fs.writeFileSync(path.join(cache, name), '')
      }
      const tool = `#!/usr/bin/env node
const fs = require('fs')
const path = require('path')
const command = path.basename(process.argv[1])
const args = process.argv.slice(2)
fs.appendFileSync(process.env.JAVA_TOOL_LOG, JSON.stringify({ command, args }) + '\\n')
if (command === 'curl') process.exit(91)
if (command === 'java' && process.env.JAVA_TOOL_FAILURE) {
  process.stderr.write('NoClassDefFoundError: missing class\\n')
  process.exit(23)
}
if (command === 'java') process.stdout.write(JSON.stringify({ args }) + '\\n')
`
      for (const name of ['java', 'javac', 'curl']) {
        fs.writeFileSync(path.join(bin, name), tool, { mode: 0o755 })
      }
    })

    afterEach(() => fs.rmSync(directory, { recursive: true, force: true }))

    /**
     * Executes the real shell launcher with cached jars and recording tool doubles. The launcher's
     * offline switch and extra classpath, and the doubles' failure switch, are reset so an
     * inherited value cannot change the result.
     * @param {string[]} args arguments passed unchanged to the selected Java member
     * @param {object} overrides environment additions; a key set to undefined is removed from the
     *   child environment
     * @returns {{status: number|null, stdout: string, stderr: string, calls: object[]}}
     */
    const launch = (args, overrides = {}) => {
      const log = path.join(directory, 'tools.jsonl')
      const run = spawnSync('timeout', ['600', 'bash', launcher, ...args], {
        env: {
          ...process.env,
          PATH: `${bin}${path.delimiter}${process.env.PATH}`,
          JAVA_E2E_WORK_DIR: cache,
          JAVA_E2E_OFFLINE: '0',
          JAVA_CODEC_CLASSPATH: '',
          JAVA_TOOL_FAILURE: '',
          JAVA_TOOL_LOG: log,
          KAFKA_CLIENTS_VERSION: '4.3.1',
          SLF4J_VERSION: '1.7.36',
          ...overrides,
        },
        encoding: 'utf8',
      })
      const calls = fs.existsSync(log)
        ? fs.readFileSync(log, 'utf8').trim().split('\n').map(JSON.parse)
        : []
      return { status: run.status, stdout: run.stdout, stderr: run.stderr, calls }
    }

    test.each([
      ['group probe', ['--probe'], 'JavaGroupMember'],
      [
        'consumer protocol',
        ['--group-protocol', 'consumer', 'host', 'group', 'topic', 'uniform', '100'],
        'JavaGroupMember',
      ],
      [
        'transactional producer',
        ['--role', 'transactional-producer', '--probe'],
        'JavaTransactionalMember',
      ],
      [
        'committed reader',
        ['--role', 'read-committed-consumer', '--probe'],
        'JavaTransactionalMember',
      ],
      ['unknown legacy role', ['--role', 'unknown', '--probe'], 'JavaTransactionalMember'],
      [
        'near-match share role',
        ['--role', 'share-member-extra', '--probe'],
        'JavaTransactionalMember',
      ],
      ['share probe', ['--role', 'share-member', '--probe'], 'JavaShareMember'],
      [
        'share poll journal',
        ['--role', 'share-member', '--report-polls', 'host', 'group', 'topic', '100'],
        'JavaShareMember',
      ],
      [
        'share halt',
        ['--role', 'share-member', '--halt-after', '2', 'host', 'group', 'topic', '100'],
        'JavaShareMember',
      ],
    ])('routes %s with its arguments intact', (_name, args, member) => {
      const run = launch(args)
      expect(run.status).toBe(0)
      expect(JSON.parse(run.stdout).args).toEqual([
        '-cp',
        [
          path.join(cache, 'kafka-clients-4.3.1.jar'),
          path.join(cache, 'slf4j-api-1.7.36.jar'),
          cache,
        ].join(path.delimiter),
        member,
        ...args,
      ])
      expect(run.calls.map(({ command }) => command)).toEqual(['javac', 'java'])
    })

    test('compiles all three members against the inherited two-jar classpath', () => {
      const run = launch(['--role', 'share-member', '--probe'])
      const source = path.dirname(launcher)
      expect(run.status).toBe(0)
      expect(run.calls[0].args).toEqual([
        '-cp',
        [
          path.join(cache, 'kafka-clients-4.3.1.jar'),
          path.join(cache, 'slf4j-api-1.7.36.jar'),
        ].join(path.delimiter),
        '-d',
        cache,
        ...['JavaGroupMember.java', 'JavaTransactionalMember.java', 'JavaShareMember.java'].map(
          (name) => path.join(source, name)
        ),
      ])
    })

    test('keeps both jar-version overrides and leaves the caller-owned cache untouched', () => {
      const planted = {
        'kafka-clients-custom.jar': 'cached clients bytes',
        'slf4j-api-custom.jar': 'cached slf4j bytes',
        'caller-sentinel.txt': 'owned by the caller',
      }
      for (const [name, content] of Object.entries(planted)) {
        fs.writeFileSync(path.join(cache, name), content)
      }
      const run = launch(['--probe'], { KAFKA_CLIENTS_VERSION: 'custom', SLF4J_VERSION: 'custom' })
      expect(run.status).toBe(0)
      expect(JSON.parse(run.stdout).args[1]).toBe(
        [
          path.join(cache, 'kafka-clients-custom.jar'),
          path.join(cache, 'slf4j-api-custom.jar'),
          cache,
        ].join(path.delimiter)
      )
      for (const [name, content] of Object.entries(planted)) {
        expect(fs.readFileSync(path.join(cache, name), 'utf8')).toBe(content)
      }
      expect(run.calls.map(({ command }) => command)).toEqual(['javac', 'java'])
    })

    test('falls back to the default jar versions when neither override is set', () => {
      const run = launch(['--probe'], {
        KAFKA_CLIENTS_VERSION: undefined,
        SLF4J_VERSION: undefined,
      })
      const classpath = [
        path.join(cache, 'kafka-clients-4.3.1.jar'),
        path.join(cache, 'slf4j-api-1.7.36.jar'),
      ].join(path.delimiter)
      expect(run.status).toBe(0)
      expect(run.calls.map(({ command }) => command)).toEqual(['javac', 'java'])
      expect(run.calls[0].args[1]).toBe(classpath)
      expect(run.calls[1].args[1]).toBe([classpath, cache].join(path.delimiter))
    })

    test('places the extra codec classpath after the jars in both the javac and java classpaths', () => {
      const codec = [path.join(directory, 'codec-one.jar'), path.join(directory, 'codec-two.jar')]
      const run = launch(['--probe'], { JAVA_CODEC_CLASSPATH: codec.join(path.delimiter) })
      const compiled = [
        path.join(cache, 'kafka-clients-4.3.1.jar'),
        path.join(cache, 'slf4j-api-1.7.36.jar'),
        ...codec,
      ].join(path.delimiter)
      expect(run.status).toBe(0)
      expect(run.calls.map(({ command }) => command)).toEqual(['javac', 'java'])
      expect(run.calls[0].args[1]).toBe(compiled)
      expect(run.calls[1].args[1]).toBe([compiled, cache].join(path.delimiter))
    })

    test('propagates a selected member classpath failure', () => {
      const run = launch(['--role', 'share-member', '--probe'], { JAVA_TOOL_FAILURE: '1' })
      expect(run.status).toBe(23)
      expect(run.stdout).toBe('')
      expect(run.stderr).toContain('NoClassDefFoundError: missing class')
      expect(run.calls.at(-1).args[2]).toBe('JavaShareMember')
    })
  })
})

describe('broker telemetry fixture', () => {
  const { createHash } = require('crypto')
  const root = path.resolve(__dirname, '../../..')
  const read = (name) => fs.readFileSync(path.join(root, name))
  const digest = (name) => createHash('sha256').update(read(name)).digest('hex')
  const fixture = 'scripts/e2e/telemetry/'

  test('retains the reviewed reporter and corrected host fixture identities', () => {
    const file = fixture + 'fixtures/manifest.json'
    expect(digest(file)).toBe('7ca0a20e383b745bc427b80211ee0a8e8ae8af0b1fd9a36e73ece96df77b38f4')
    const manifest = JSON.parse(read(file))
    expect(digest('docker-compose.telemetry.yml')).toBe(
      manifest.composeSha256['docker-compose.telemetry.yml']
    )
    for (const name of ['CountingTelemetryReporter.java', 'build.sh'])
      expect(digest(fixture + name)).toBe(manifest.scripts[name])
    expect(digest(fixture + 'fetch.sh')).toBe(manifest.scripts['fetch.sh'])
    expect(digest(fixture + 'fixtures/inputs.sha256')).toBe(
      '221be9594f870c453fc4a56126fcadaadcb4d3741750a0d727e6a08e332553b3'
    )
    for (const line of ['4.4.0', '4.3.1', '4.2.2']) {
      for (const kind of ['reporterJar', 'javaClientJar']) {
        const jar = manifest.lines[line][kind]
        expect(fs.statSync(path.join(root, jar.path)).isFile()).toBe(true)
        expect(digest(jar.path)).toBe(jar.sha256)
      }
    }
    expect(manifest.sink).toMatchObject({
      queueCapacity: 128,
      maxFileBytesPerNode: 2097152,
      maxRecordBytes: 4096,
    })
    for (const jar of [
      'counting-telemetry-reporter-4.4.0.jar',
      'counting-telemetry-reporter-4.3.1.jar',
      'java-telemetry-client-4.4.0.jar',
      'java-telemetry-client-4.3.1.jar',
    ])
      expect(read('.gitignore').toString()).toContain(`!scripts/e2e/telemetry/fixtures/${jar}`)
    expect(read('scripts/assertCleanTarball.sh').toString()).toContain('scripts/e2e/telemetry/')
    expect(read('scripts/assertCleanTarball.sh').toString()).toContain('.*\\.jar$')
  })

  test('keeps fixture startup separate from offline verification and decoding', () => {
    const runner = read(fixture + 'run.sh').toString()
    expect(runner).toContain('FullTelemetryDecoder "$RAW"')
    expect(runner).toContain('FullTelemetryDecoder.java')
    expect(runner).toContain('fixtures/full-telemetry-decoder.jar')
    expect(runner).toContain('TELEMETRY_JAVA_CACHE')
    expect(runner).toContain('sha256')
    expect(runner).not.toMatch(/docker compose|docker pull|curl |wget |npm run test:kafka/)
    const compose = read('docker-compose.telemetry.yml').toString()
    for (const port of [19092, 19095, 19098])
      expect(compose).toContain('PLAINTEXT_HOST://localhost:' + port)
    expect(compose).not.toContain('container_name:')
  })

  test('selects one guarded file and title for each telemetry script', () => {
    const scripts = JSON.parse(read('package.json')).scripts
    const selected = Object.entries(scripts).filter(([name]) =>
      name.startsWith('test:e2e:telemetry-')
    )
    expect(selected).toHaveLength(9)
    for (const [, command] of selected) {
      expect(command).toContain(
        'timeout -k 10s 600s node "' + '$' + '{P9_CLUSTER_GUARD:?set sole live guard}"'
      )
      expect(command).toContain(' tv2 TELEMETRY_E2E src/telemetry/__tests__/')
      expect(command).not.toMatch(/test:kafka|forceExit|--testPathPatterns/)
      expect(command.match(/src\/telemetry\/__tests__\/[^ ]+\.spec\.js/g)).toHaveLength(1)
    }
  })

  test('wires retained fixtures and all live combinations through isolated CI ownership', () => {
    const scripts = JSON.parse(read('package.json')).scripts
    const exact = (script, title) => {
      expect(scripts[script]).toContain('TELEMETRY_E2E=1 jest --detectOpenHandles --runInBand')
      expect(scripts[script]).not.toContain('forceExit')
      expect(scripts[script]).toContain(
        "--testPathPatterns 'src/telemetry/__tests__/reporterLive\\.spec\\.js'"
      )
      expect(scripts[script]).toContain(`--testNamePattern '${title}'`)
    }
    exact('test:group:telemetry-reporter', '^(reporter export|reporter no-resource control)$')
    exact('test:group:telemetry-base', '^reporter no-plugin control$')
    const live = [
      ['prefix', 'subscriptionLive', 'subscription updates'],
      ['custom', 'subscriptionLive', 'custom numeric snapshots'],
      ['payload', 'subscriptionLive', 'bounded compressed payload'],
      ['unsupported', 'unsupportedLive', 'unsupported telemetry'],
      ['java', 'javaInteropLive', 'java telemetry interoperability'],
      ['outcomes', 'outcomesLive', 'foreground outcomes'],
      ['shutdown', 'shutdownLive', 'close and reopen'],
      ['soak', 'shutdownLive', 'telemetry churn'],
    ]
    for (const [name, file, title] of live) {
      const script = scripts['test:group:telemetry-' + name]
      expect(script).toContain('TELEMETRY_E2E=1 jest --detectOpenHandles --runInBand')
      expect(script).not.toContain('forceExit')
      expect(script).toContain(`--testPathPatterns 'src/telemetry/__tests__/${file}\\.spec\\.js'`)
      expect(script).toContain(`--testNamePattern '^${title}$'`)
    }
    // eslint-disable-next-line n/no-extraneous-require -- YAML parser is pinned by the ESLint/Jest toolchain
    const yaml = require('js-yaml')
    const parse = (file) => yaml.load(read(file).toString())
    const ci = parse('.github/workflows/ci.yml')
    const gate = parse('.github/workflows/gate.yml')
    const workflow = parse('.github/workflows/telemetry.yml')
    expect(ci.jobs.telemetry.uses).toBe('./.github/workflows/telemetry.yml')
    expect(ci.jobs.telemetry.with.full).toBe(false)
    expect(gate.jobs.telemetry.uses).toBe('./.github/workflows/telemetry.yml')
    expect(gate.jobs.telemetry.with.full).toBe(true)
    expect(gate.jobs.telemetry.needs).toBe('plan')
    expect(gate.jobs.verdict.needs).toContain('telemetry')
    const summary = gate.jobs.verdict.steps.find((step) => step.name === 'Summarize results')
    expect(summary.env.TELEMETRY_RESULT).toBe('$' + '{{ needs.telemetry.result }}')
    expect(summary.run).toContain(
      "rows.push({ id: 'telemetry-full', result: process.env.TELEMETRY_RESULT })"
    )
    expect(gate.jobs.plan.if).toBe(
      "github.event_name != 'pull_request' || !github.event.pull_request.draft"
    )
    expect(gate.on.pull_request.types).toEqual([
      'opened',
      'synchronize',
      'reopened',
      'ready_for_review',
    ])
    expect(gate.on.push.branches).toEqual(['master'])
    expect(gate.on).toHaveProperty('workflow_dispatch')
    const plan = workflow.jobs.plan.steps.find((step) => step.id === 'plan')
    const source = plan.run.match(/node <<'NODE'\n([\s\S]*?)\nNODE/)[1]
    const vm = require('vm')
    const verdictSource = summary.run.match(/node <<'NODE'\n([\s\S]*?)\nNODE/)[1]
    for (const [status, expected] of [
      ['success', undefined],
      ['failure', 1],
      ['skipped', 1],
    ]) {
      const process = {
        env: {
          MATRIX: '{"include":[]}',
          PLAN_RESULT: 'success',
          CHECK_RESULT: 'success',
          TELEMETRY_RESULT: status,
        },
      }
      vm.runInNewContext(verdictSource, { process, require: () => ({ appendFileSync: () => {} }) })
      expect(process.exitCode).toBe(expected)
    }
    const catalogue = (full) => {
      let output
      vm.runInNewContext(source, {
        process: { env: { FULL: String(full), GITHUB_OUTPUT: 'output' } },
        require: () => ({
          appendFileSync: (file, value) => {
            output = value
          },
        }),
      })
      return JSON.parse(output.trim().slice('matrix='.length)).include
    }
    const configuredGroups = [
      'telemetry-reporter',
      'telemetry-prefix',
      'telemetry-custom',
      'telemetry-payload',
      'telemetry-java',
      'telemetry-outcomes',
      'telemetry-shutdown',
    ]
    const configured = (node, kafkas) =>
      configuredGroups.flatMap((group) =>
        kafkas.map((kafka) => ({ node, kafka, group, compose: 'docker-compose.telemetry.yml' }))
      )
    const base = (node, group, kafkas) =>
      kafkas.map((kafka) => ({ node, kafka, group, compose: 'docker-compose.yml' }))
    const noPlugin = ['4.4.0', '4.2.2', '4.1.2', '4.0.2']
    const rowsOf = (node, full) => {
      const rows = []
      for (const group of configuredGroups)
        rows.push(
          ...configured(node, full ? ['4.4.0', '4.2.2'] : ['4.4.0']).filter(
            (r) => r.group === group
          )
        )
      rows.push(...base(node, 'telemetry-base', full ? noPlugin : ['4.4.0']))
      rows.push(...base(node, 'telemetry-unsupported', full ? noPlugin : ['4.4.0']))
      if (full) rows.push(...base(node, 'telemetry-outcomes', ['4.4.0', '4.1.2']))
      return rows
    }
    expect(catalogue(false)).toEqual(rowsOf(24, false))
    expect(catalogue(false)).toHaveLength(9)
    const required = [22, 24, 26].flatMap((node) => rowsOf(node, true))
    expect(catalogue(true)).toEqual(required)
    expect(new Set(catalogue(true).map(JSON.stringify)).size).toBe(72)
    expect(new Set(catalogue(true).map((r) => `${r.group}-${r.kafka}-${r.node}`)).size).toBe(72)
    const resources = workflow.jobs.resources
    expect(resources.needs).toBeUndefined()
    const alone = resources.steps.find((step) => step.run?.includes('resources\\.spec'))
    expect(alone.run).toContain('--runInBand')
    expect(alone.run).toContain('--expose-gc')
    expect(alone.run).not.toContain('forceExit')
    expect(alone.run).toContain("--testNamePattern '^(?!.*resource memory for)'")
    expect(alone.env.TELEMETRY_RESOURCES_QUIET).toBe('1')
    expect(alone['timeout-minutes']).toBeGreaterThan(0)
    const names = resources.steps.map((step) => step.name ?? step.uses)
    expect(names).toEqual([
      'actions/checkout@v4',
      'actions/setup-node@v4',
      undefined,
      'Run the bounded-resource schedules',
    ])
    expect(resources.env).toBeUndefined()
    expect(resources.strategy.matrix.node).toBe(
      '$' + "{{ fromJSON(inputs.full && '[22, 24, 26]' || '[24]') }}"
    )
    expect(workflow.jobs['retained-diagnostic']).toBeUndefined()
    expect(workflow.jobs.telemetry.steps.some((step) => step.name?.includes('churn soak'))).toBe(
      false
    )
    expect(
      workflow.jobs.telemetry.steps.find((step) => step.name === 'Run telemetry lane').if
    ).toBeUndefined()
    const job = workflow.jobs.telemetry
    for (const identity of ['github.run_id', 'github.run_attempt', 'strategy.job-index'])
      expect(job.env.COMPOSE_PROJECT_NAME).toContain(identity)
    const steps = job.steps.filter((step) => step.run)
    for (const step of steps) expect(step['timeout-minutes']).toBeGreaterThan(0)
    const position = (text) => steps.findIndex((step) => step.run.includes(text))
    const ordered = [
      'run.sh verify',
      'ci.js preflight',
      'docker compose -f "$COMPOSE_FILE" pull',
      'docker compose -f "$COMPOSE_FILE" up',
      'ci.js capture',
      'test:group:',
      'verifyEvidence(',
      'ci.js stop',
    ]
    const positions = ordered.map(position)
    expect(positions.every((index) => index >= 0)).toBe(true)
    expect([...positions].sort((a, b) => a - b)).toEqual(positions)
    const stop = steps[position('ci.js stop')]
    expect(stop.if).toBe('always()')
    expect(stop.run).toContain('timeout -k 5s 150s')
    expect(job.steps.at(-1).if).toBe('always()')
    expect(steps[position('run.sh verify')]['timeout-minutes']).toBeGreaterThanOrEqual(5)
    const serialized = JSON.stringify(job)
    expect(serialized).not.toContain('build.sh')
    expect(serialized).not.toContain('testWithKafka')
    const owner = read(fixture + 'ci.js').toString()
    expect(owner).toContain("'down', '-v', '--remove-orphans'")
    for (const type of ['containers', 'networks', 'volumes']) expect(owner).toContain(type)
    expect(serialized).not.toMatch(/forceExit|docker (pull|system prune)/)
  })

  test('keeps memory acceptance in guarded jobs with the full Node and broker coverage', () => {
    // eslint-disable-next-line n/no-extraneous-require -- YAML parser is pinned by the ESLint/Jest toolchain
    const yaml = require('js-yaml')
    const workflow = yaml.load(read('.github/workflows/memory-gates.yml').toString())
    expect(workflow.on).toHaveProperty('workflow_dispatch')
    expect(workflow.on.pull_request.types).toEqual([
      'opened',
      'synchronize',
      'reopened',
      'ready_for_review',
    ])
    expect(Object.keys(workflow.jobs)).toEqual([
      'share-soak-short',
      'resources',
      'retained-diagnostic',
      'telemetry-soak',
      'telemetry-verdicts',
      'telemetry-verdict-controls',
    ])
    for (const job of Object.values(workflow.jobs)) {
      expect(job.if).toBe(
        "github.event_name == 'workflow_dispatch' || startsWith(github.head_ref, 'p9m/')"
      )
      expect(job.needs).toBeUndefined()
      if (job.uses) {
        expect(job.uses).toBe('./.github/workflows/telemetry.yml')
        expect(job.with).toEqual({ full: true, 'memory-verdicts': true })
        expect(job['continue-on-error']).toBeUndefined()
        continue
      }
      expect(job.steps[0].uses).toBe('actions/checkout@v4')
      expect(job.steps[1].uses).toBe('actions/setup-node@v4')
      expect(job.steps.at(-1)).toMatchObject({
        if: 'always()',
        uses: 'actions/upload-artifact@v4',
        with: { 'include-hidden-files': true },
      })
    }
    const shared = yaml.load(read('.github/workflows/telemetry.yml').toString())
    const plan = shared.jobs.plan.steps[0]
    const source = plan.run.match(/node <<'NODE'\n([\s\S]*?)\nNODE/)[1]
    let output
    require('vm').runInNewContext(source, {
      process: { env: { FULL: 'true', MEMORY_VERDICTS: 'true', GITHUB_OUTPUT: 'output' } },
      require: () => ({
        appendFileSync: (_file, value) => {
          output = value
        },
      }),
    })
    const rows = JSON.parse(output.trim().slice('matrix='.length)).include
    const expected = [22, 24, 26].flatMap((node) => [
      ...['telemetry-outcomes', 'telemetry-shutdown'].flatMap((group) =>
        ['4.4.0', '4.2.2'].map((kafka) => ({
          node,
          kafka,
          group,
          compose: 'docker-compose.telemetry.yml',
        }))
      ),
      ...['4.4.0', '4.2.2', '4.1.2', '4.0.2'].map((kafka) => ({
        node,
        kafka,
        group: 'telemetry-unsupported',
        compose: 'docker-compose.yml',
      })),
      ...['4.4.0', '4.1.2'].map((kafka) => ({
        node,
        kafka,
        group: 'telemetry-outcomes',
        compose: 'docker-compose.yml',
      })),
    ])
    expect(rows).toEqual(expected)
    const verdict = shared.jobs.telemetry.steps.find((step) =>
      step.run?.includes('verifyMemoryEvidence')
    )
    expect(verdict.if).toBe('$' + '{{ always() && inputs.memory-verdicts }}')
    expect(verdict['continue-on-error']).toBeUndefined()
    expect(shared.on.workflow_call.inputs['memory-verdicts'].default).toBe(false)
    const mutationControls = workflow.jobs['telemetry-verdict-controls']
    const controlsRun = mutationControls.steps.find((step) =>
      step.run?.includes('memoryVerdicts.spec.js')
    )
    expect(controlsRun.run).toContain('testHelpers/__tests__/telemetryFixtures.spec.js')
    expect(controlsRun['continue-on-error']).toBeUndefined()
    const resources = workflow.jobs.resources
    expect(resources.strategy).toEqual({ 'fail-fast': false, matrix: { node: [22, 24, 26] } })
    expect(resources.env.TELEMETRY_RESOURCES_ARTIFACT_DIR).toContain('telemetry-resources')
    expect(resources.steps.map((step) => step.name ?? step.uses)).toEqual([
      'actions/checkout@v4',
      'actions/setup-node@v4',
      undefined,
      'Stamp the run and prepare the artifact directory',
      'Measure single-client memory alone on the idle runner',
      'Validate the recorded measurements for this tree and Node',
      'actions/upload-artifact@v4',
    ])
    const memory = resources.steps.find((step) => step.name?.startsWith('Measure'))
    expect(memory.env.TELEMETRY_RESOURCES_QUIET).toBe('1')
    expect(memory.run).toContain('timeout -k 10s 540s node --expose-gc')
    expect(memory.run).toContain("--testNamePattern 'resource memory for'")
    expect(memory.run).toContain('--runInBand --ci --detectOpenHandles')
    const validate = resources.steps.find((step) => step.run?.includes('resourceArtifact.js'))
    expect(validate.if).toBe('always()')
    expect(validate.run).toContain('--tree "$KAFKAJS_TREE" --node "$(node -p process.version)"')
    expect(resources.steps.at(-1).with).toMatchObject({
      name: 'resources-node$' + '{{ matrix.node }}',
      path: '.tmp/telemetry-resources/',
      'if-no-files-found': 'error',
    })
    const diagnostic = workflow.jobs['retained-diagnostic']
    expect(diagnostic.steps[1].with['node-version']).toBe('24')
    expect(diagnostic.env.TELEMETRY_RESOURCES_QUIET).toBe('1')
    const controls = diagnostic.steps.find((step) => step.name?.includes('control groups'))
    expect(controls.env.TELEMETRY_RESOURCES_QUIET).toBe('0')
    expect(controls.run).toContain('timeout -k 5s 110s npx jest --maxWorkers=2 --ci')
    expect(controls.run).toContain('timeout -k 5s 50s npx jest --maxWorkers=2 --ci')
    const diagnose = diagnostic.steps.find((step) => step.run?.includes('retainedDiagnostic.js'))
    expect(diagnose.run).toContain('timeout -k 10s 1750s')
    expect(diagnose.run).toContain('--diagnostic-only "$RETAINED_DIAGNOSTIC_DIR"')
    const analyse = diagnostic.steps.find((step) => step.name?.startsWith('Analyse'))
    expect(analyse.if).toBe('always()')
    expect(analyse.run).toContain('timeout -k 5s 290s node --max-old-space-size=6144')
    expect(diagnostic.steps.at(-1).with).toMatchObject({
      name: 'retained-diagnostic-node24',
      path: '.tmp/retained-diagnostic/',
      'if-no-files-found': 'error',
    })
    const soak = workflow.jobs['telemetry-soak']
    expect(soak.strategy).toEqual({
      'fail-fast': false,
      matrix: { node: [22, 24, 26], kafka: ['4.3.1', '4.2.2'] },
    })
    expect(soak.env.COMPOSE_FILE).toBe('docker-compose.telemetry.yml')
    for (const identity of ['github.run_id', 'github.run_attempt', 'strategy.job-index'])
      expect(soak.env.COMPOSE_PROJECT_NAME).toContain(identity)
    const steps = soak.steps.filter((step) => step.run)
    for (const step of steps) expect(step['timeout-minutes']).toBeGreaterThan(0)
    const ordered = [
      'run.sh verify',
      'fetch.sh',
      'prepareDecoder.js',
      'ci.js preflight',
      'docker compose -f "$COMPOSE_FILE" pull',
      'docker compose -f "$COMPOSE_FILE" up',
      'ci.js capture',
      'timeout -k 10s 600s npm run test:group:telemetry-soak -- --ci',
      "verifyEvidence('telemetry-soak'",
      'ci.js stop',
    ]
    const positions = ordered.map((text) => steps.findIndex((step) => step.run.includes(text)))
    expect(positions.every((index) => index >= 0)).toBe(true)
    expect([...positions].sort((a, b) => a - b)).toEqual(positions)
    expect(steps[positions[8]].if).toBe('always()')
    expect(steps[positions[8]].run).toContain("'scripts/evidence/verify.js'")
    expect(steps[positions[9]].if).toBe('always()')
    expect(steps[positions[9]].run).toContain('verifyCleanup(')
    expect(soak.steps.at(-1).with).toMatchObject({
      name: 'e2e-telemetry-soak-$' + '{{ matrix.kafka }}-node$' + '{{ matrix.node }}',
      'if-no-files-found': 'error',
    })
    expect(JSON.stringify(soak)).not.toContain('forceExit')
  })
})

describe('Java telemetry role runner', () => {
  const runner = path.resolve(__dirname, '../telemetry/run.sh')
  const directories = []

  afterEach(() => {
    for (const directory of directories.splice(0))
      fs.rmSync(directory, { recursive: true, force: true })
  })

  /**
   * Exercises shell argument composition with finite offline tool stubs, never a client or broker.
   * @param {string} mode runner mode
   * @param {string} role literal role argument
   * @returns {{ result: object, argv: string[] | null }} exit and Java argv
   * @throws {Error} for local fixture creation failures
   */
  const invoke = (mode, role = 'producer') => {
    const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'telemetry-runner-'))
    directories.push(directory)
    const argvFile = path.join(directory, 'java.json')
    fs.writeFileSync(path.join(directory, 'node'), '#!/bin/sh\ncat >/dev/null\n', {
      mode: 0o700,
    })
    fs.writeFileSync(
      path.join(directory, 'java'),
      '#!/bin/sh\nexec "$ROLE_TEST_NODE" -e \'require("fs").writeFileSync(process.env.ROLE_TEST_ARGV, JSON.stringify(process.argv.slice(1)))\' -- "$@"\n',
      { mode: 0o700 }
    )
    const result = spawnSync(
      'timeout',
      [
        '-k',
        '1s',
        '5s',
        'bash',
        runner,
        mode,
        '4.3.1',
        role,
        'localhost:19092',
        'telemetry-java-client',
        'telemetry-java-topic',
        'telemetry-java-group',
        '5000',
        'active-close',
      ],
      {
        encoding: 'utf8',
        env: {
          ...process.env,
          PATH: directory + path.delimiter + process.env.PATH,
          TMPDIR: directory,
          TELEMETRY_INTEROP_CACHE: directory,
          TELEMETRY_JAVA_CACHE: '',
          ROLE_TEST_NODE: process.execPath,
          ROLE_TEST_ARGV: argvFile,
        },
      }
    )
    return {
      result,
      argv: fs.existsSync(argvFile) ? JSON.parse(fs.readFileSync(argvFile, 'utf8')) : null,
    }
  }

  test.each(['producer', 'consumer', 'admin'])(
    'passes literal %s client arguments once',
    (role) => {
      const { result, argv } = invoke('client', role)
      expect(result.status).toBe(0)
      expect(result.signal).toBeNull()
      expect(argv.slice(-7)).toEqual([
        role,
        'localhost:19092',
        'telemetry-java-client',
        'telemetry-java-topic',
        'telemetry-java-group',
        '5000',
        'active-close',
      ])
      expect(argv).not.toContain('--check')
      const cp = argv[argv.indexOf('--class-path') + 1].split(path.delimiter)
      expect(cp.map((file) => path.basename(file))).toEqual([
        'kafka-clients-4.3.1.jar',
        'slf4j-api-1.7.36.jar',
        'lz4-java-1.10.2.jar',
        'snappy-java-1.1.10.7.jar',
        'zstd-jni-1.5.6-10.jar',
      ])
      expect(argv[argv.length - 8]).toBe(
        path.join(path.dirname(runner), 'JavaTelemetryClient.java')
      )
    }
  )

  test('places the no-network readiness flag before role arguments', () => {
    const { result, argv } = invoke('client-check', 'consumer')
    expect(result.status).toBe(0)
    expect(argv.slice(-8, -6)).toEqual(['--check', 'consumer'])
    expect(argv.filter((value) => value === '--check')).toHaveLength(1)
  })

  test('rejects an unknown mode before invoking Java', () => {
    const { result, argv } = invoke('client-unknown')
    expect(result.status).toBe(2)
    expect(argv).toBeNull()
  })

  test('keeps the Java sources, pin lists and retained probe at their reviewed identities', () => {
    const { createHash } = require('crypto')
    const root = path.resolve(__dirname, '../../..')
    const fixture = path.join(root, 'scripts/e2e/telemetry')
    const digest = (file) => createHash('sha256').update(fs.readFileSync(file)).digest('hex')
    expect(digest(path.join(fixture, 'JavaTelemetryClient.java'))).toBe(
      '1145746dfeadc7d315e42b1c9d3b3468d819929324cc7701cbddbb423f66b580'
    )
    expect(digest(path.join(fixture, 'telemetryWireProbe.js'))).toBe(
      '139afdbec5034f7c1498b90306889fd918ae13097efee1ea9f435b03dcf9b1b1'
    )
    const source = fs.readFileSync(path.join(fixture, 'JavaTelemetryClient.java'), 'utf8')
    for (const preserved of [
      'gauge.double',
      'gauge.long',
      'sum.total',
      '42.5',
      '4242L',
      '7.0',
      'active-close',
    ])
      expect(source).toContain(preserved)
    const pins = Object.fromEntries(
      fs
        .readFileSync(path.join(fixture, 'interop.sha256'), 'utf8')
        .trim()
        .split('\n')
        .map((line) => line.split(/\s+/).reverse())
    )
    const script = fs.readFileSync(path.join(fixture, 'run.sh'), 'utf8')
    expect(Object.keys(pins)).toHaveLength(5)
    for (const [name, sha] of Object.entries(pins)) expect(script).toContain(`'${name}': '${sha}'`)
    const fetcher = fs.readFileSync(path.join(fixture, 'fetch-interop.sh'), 'utf8')
    for (const name of Object.keys(pins)) expect(fetcher).toContain(name)
    expect(fetcher).not.toMatch(/docker|--forceExit/)
  })
})

describe('broker cache retirement trace', () => {
  const { cacheRetirement } = require('../telemetry/cacheTrace')
  const uuid = '00112233-4455-6677-8899-aabbccddeeff'
  const registered = '172.18.0.3:9092-172.18.0.1:54321-3'
  const removing = (id) =>
    `TRACE Removing client connection id [${id}] from the client instance cache`
  const input = () => ({
    node: '1',
    localPort: '9092',
    uuid,
    since: '2026-11-07T10:00:00Z',
    completedAt: '2026-11-07T10:00:05Z',
    source: { sha256: 'a'.repeat(64) },
    get: {
      socketId: 1,
      node: '1',
      requestKey: 'r1',
      clientId: 'candidate',
      correlation: 7,
      uuid: '00000000-0000-0000-0000-000000000000',
      response: { errorCode: 0, uuid },
    },
    connect: {
      socketId: 1,
      node: '1',
      localAddress: '127.0.0.1',
      localPort: 54321,
      remoteAddress: '127.0.0.1',
      remotePort: 19092,
    },
    logs: {
      1: {
        text: `TRACE Completed request:RequestHeader(apiKey=GET_TELEMETRY_SUBSCRIPTIONS, clientId=candidate, correlationId=7) -- GetTelemetrySubscriptionsRequestData(clientInstanceId=00000000-0000-0000-0000-000000000000) with response:GetTelemetrySubscriptionsResponseData(clientInstanceId=${uuid}) from connection ${registered};totalTime:1\n${removing(registered)}`,
      },
      2: { text: '' },
      3: { text: '' },
    },
  })
  test('joins the captured Get, UUID and client endpoints through the exact server connection', () => {
    expect(cacheRetirement(input())).toMatchObject({
      connectionId: registered,
      connectionIdentity: { uuid, connectionId: registered, client: { localPort: 54321 } },
    })
  })
  test.each([
    [
      'a duplicate exact retirement',
      (i) => {
        i.logs[1].text += `\n${removing(registered)}`
      },
    ],
    [
      'another connection on the same listener',
      (i) => {
        i.logs[1].text = i.logs[1].text.replace(
          removing(registered),
          removing('172.18.0.3:9092-172.18.0.1:54322-4')
        )
      },
    ],
    [
      'another UUID',
      (i) => {
        i.uuid = '11223344-4455-6677-8899-aabbccddeeff'
      },
    ],
    [
      'another node',
      (i) => {
        i.logs[2] = i.logs[1]
        i.logs[1] = { text: '' }
      },
    ],
    [
      'a delayed not-found line',
      (i) => {
        i.logs[1].text += `\nTRACE Client connection id [${registered}] is not found in the client instance cache`
      },
    ],
    [
      'a truncated log',
      (i) => {
        i.logs[3].truncated = true
      },
    ],
    [
      'an unavailable log',
      (i) => {
        delete i.logs[2]
      },
    ],
    [
      'an incomplete episode',
      (i) => {
        i.completedAt = i.since
      },
    ],
    [
      'a wrong correlation',
      (i) => {
        i.get.correlation = 8
      },
    ],
  ])('rejects %s', (_name, change) => {
    const value = input()
    change(value)
    expect(() => cacheRetirement(value)).toThrow()
  })
})

describe('live telemetry scenario evidence accounting', () => {
  const evidence = require('../telemetry/evidence')
  const request = (extra) => ({
    type: 'request',
    requestKey: 'r1',
    apiKey: 72,
    apiVersion: 0,
    socketId: 1,
    terminating: false,
    response: { errorCode: 0 },
    ...extra,
  })
  const owners = (record) => ({ producer: { records: [record] } })

  test('allows only the exact request captured at a public-close boundary', () => {
    const r = request({ writeAt: 10, response: undefined })
    const transcript = {
      records: [r],
      sockets: [{ id: 1, closedAt: 13 }],
      closures: [{ at: 11, end: 12, pending: ['r1'] }],
      cancellations: [
        {
          requestKey: 'r1',
          apiKey: 72,
          socketId: 1,
          writeAt: 10,
          at: 11,
          end: 13,
          disposition: 'close-in-flight',
        },
      ],
    }
    expect(() => evidence.assertStreamAccounted({ producer: transcript })).not.toThrow()
    expect(() =>
      evidence.assertStreamAccounted({
        producer: {
          ...transcript,
          records: [
            ...transcript.records,
            request({ requestKey: 'r2', apiKey: 71, writeAt: 9, response: undefined }),
          ],
        },
      })
    ).toThrow('Unanswered')
    for (const field of [
      'requestKey',
      'apiKey',
      'socketId',
      'writeAt',
      'at',
      'end',
      'disposition',
    ]) {
      const bad = JSON.parse(JSON.stringify(transcript))
      bad.cancellations[0][field] = 'foreign'
      expect(() => evidence.assertStreamAccounted({ producer: bad })).toThrow()
    }
  })
  test('does not allow an unanswered final or all requests of a faulted socket implicitly', () => {
    expect(() =>
      evidence.assertStreamAccounted(owners(request({ terminating: true, response: undefined })))
    ).toThrow('Unanswered')
    expect(() =>
      evidence.assertStreamAccounted(owners(request({ response: undefined })), {
        faultedSockets: { producer: [1] },
      })
    ).toThrow('Unanswered')
  })

  test.each([
    [
      'an undeclared error code',
      request({ response: { errorCode: 118 } }),
      { errors: { producer: [117] } },
      'Undeclared error 118',
    ],
    [
      'an error declared for another owner',
      request({ response: { errorCode: 117 } }),
      { errors: { admin: [117] } },
      'Undeclared error 117',
    ],
    ['an unanswered normal Push', request({ response: undefined }), {}, 'Unanswered'],
    [
      'an unanswered Get off a faulted socket',
      request({ apiKey: 71, socketId: 3, response: undefined }),
      { faultedSockets: { producer: [2] } },
      'Unanswered',
    ],
  ])('rejects %s', (_name, record, allowed, message) => {
    expect(() => evidence.assertStreamAccounted(owners(record), allowed)).toThrow(message)
  })

  const corpus = () => ({
    count: 1000,
    expectedSha256: 'a',
    logSha256: 'a',
    handlerSha256: 'a',
    handlerEntries: 1000,
    rawCommitted: { 0: '334', 1: '333', 2: '333' },
    processedNext: { 0: '334', 1: '333', 2: '333' },
    rawEnd: { 0: '334', 1: '333', 2: '333' },
  })

  test('requires the durable log, handler set and raw commits to agree', () => {
    expect(() => evidence.assertCorpus(corpus())).not.toThrow()
    expect(() => evidence.assertCorpus({ ...corpus(), logSha256: 'b' })).toThrow(
      'Durable log differs'
    )
    expect(() => evidence.assertCorpus({ ...corpus(), handlerSha256: 'b' })).toThrow(
      'Handler set differs'
    )
    expect(() => evidence.assertCorpus({ ...corpus(), handlerEntries: 1001 })).toThrow(
      'Handler entries differ'
    )
    expect(() =>
      evidence.assertCorpus({ ...corpus(), rawCommitted: { 0: '300', 1: '333', 2: '333' } })
    ).toThrow('Raw commits differ from processed')
    expect(() =>
      evidence.assertCorpus({
        ...corpus(),
        rawCommitted: { 0: '334', 1: '333', 2: '333' },
        processedNext: { 0: '334', 1: '333', 2: '333' },
        rawEnd: { 0: '335', 1: '333', 2: '333' },
      })
    ).toThrow('Raw commits are not at the log end')
  })

  test.each(['telemetry-prefix', 'telemetry-java', 'telemetry-unsupported'])(
    'requires exactly the %s scenario in its lane',
    (group) => {
      expect(() => evidence.verifyEvidence(group, os.tmpdir(), [])).toThrow(
        'Scenario catalogue mismatch'
      )
      expect(() => evidence.verifyEvidence('telemetry-unknown', os.tmpdir(), [])).toThrow(
        'Unknown telemetry lane'
      )
    }
  )
})

describe('complete telemetry evidence controls', () => {
  const { assertCatalogue, assertCadence, assertJavaRows } = require('../telemetry/semantics')
  const pushes = [
    { owner: 'a', requestKey: 'r1' },
    { owner: 'a', requestKey: 'r2' },
  ]
  test('rejects an omitted, invented or duplicated accepted Push', () => {
    expect(() => assertCatalogue(pushes, pushes)).not.toThrow()
    for (const bad of [
      pushes.slice(0, 1),
      [...pushes, { owner: 'a', requestKey: 'r3' }],
      [pushes[0], pushes[0]],
    ])
      expect(() => assertCatalogue(bad, pushes)).toThrow('Incomplete Push catalogue')
  })
  test('rejects the first premature poll even when a later poll is on time', () => {
    expect(() => assertCadence([{ writeAt: 1100 }, { writeAt: 2100 }], 100, 1000)).not.toThrow()
    expect(() => assertCadence([{ writeAt: 200 }, { writeAt: 1100 }], 100, 1000)).toThrow(
      'Early telemetry request'
    )
  })
  test('rejects a Push before the full response-relative interval', () => {
    expect(() =>
      assertCadence([{ writeAt: 1000, response: { responseAt: 1050 } }, { writeAt: 2000 }], 0, 1000)
    ).toThrow('Early telemetry request')
  })
  const { javaRow: row } = require('../../../testHelpers/javaTelemetryArchive')
  const episode = {
    clientId: 'java-producer',
    closing: { closing: true, epochMs: 30 },
    closed: { closed: true, epochMs: 40 },
  }
  test('checks every Java row and the actual closed marker', () => {
    const rows = [row(), row(false, true), row(true, true)]
    expect(() => assertJavaRows('producer', rows, episode)).not.toThrow()
    for (const mutate of [
      (r) => {
        r[1].metrics[1].points[0].asDouble = '41.0'
      },
      (r) => {
        r[1].resources[2].attrs.push(['unexpected', 'string', 'value'])
      },
      (r) => {
        r[1].resources[2].droppedAttributes = 1
      },
      (r) => {
        r[1].metrics[2].res = 0
      },
      (r) => {
        r[1].metrics.shift()
      },
      (r) => {
        r[1].metrics.push(r[1].metrics[0])
      },
      (r) => {
        r[1].metrics.push({ ...r[1].metrics[0], name: 'org.apache.kafka.tf1.custom.extra' })
      },
    ]) {
      const bad = JSON.parse(JSON.stringify(rows))
      mutate(bad)
      expect(() => assertJavaRows('producer', bad, episode)).toThrow()
    }
    expect(() =>
      assertJavaRows('producer', rows, { ...episode, closed: { closed: false, epochMs: 40 } })
    ).toThrow()
  })
  describe('pinned Java admin checkpoint and replay', () => {
    const { createHash } = require('crypto')
    const { verifyJavaInterop } = require('../telemetry/evidence')
    const {
      JavaAdminHandshake,
      selectJavaCheckpoint,
      javaSampleSummary,
    } = require('../telemetry/semantics')
    const { canonicalUuid } = require('../../../testHelpers/telemetryFixtures')
    const digest = (bytes) => createHash('sha256').update(bytes).digest('hex')
    const clone = (value) => JSON.parse(JSON.stringify(value))

    const {
      createJavaInteropArchive: archive,
    } = require('../../../testHelpers/javaTelemetryArchive')

    test.each([false, true])(
      'verifies the complete version-2 scenario sidecar archive (admin final=%s)',
      (final) => {
        const a = archive(final)
        try {
          const { verifyScenario } = require('../telemetry/evidence')
          const { record, directory } = a
          const archiveRoot = 'case-case'
          const target = path.join(directory, archiveRoot)
          fs.mkdirSync(target)
          const files = []
          const retain = (relative, value) => {
            const bytes = Buffer.isBuffer(value) ? value : Buffer.from(JSON.stringify(value))
            const file = path.join(target, relative)
            fs.mkdirSync(path.dirname(file), { recursive: true })
            fs.writeFileSync(file, bytes)
            files.push({
              path: `${archiveRoot}/${relative}`,
              bytes: bytes.length,
              sha256: digest(bytes),
            })
          }
          const owners = {}
          record.pushes = []
          record.zeroPushes = []
          record.allowed = {}
          record.status = 'passed'
          record.run.line = '4.3.1'
          retain('oracles.json', {})
          const nodeOne = [...a.rows.producer]
          // Fixed OTLP sums, checked offline with the retained independent Java decoder.
          const rawHex = {
            producer:
              '0a5f125d125b0a336f72672e6170616368652e6b61666b612e70726f64756365722e636f6e6e656374696f6e2e6372656174696f6e2e746f74616c1a01313a210a1b11010000000000000019020000000000000031010000000000000010011801',
            consumer:
              '0a5f125d125b0a336f72672e6170616368652e6b61666b612e636f6e73756d65722e636f6e6e656374696f6e2e6372656174696f6e2e746f74616c1a01313a210a1b11010000000000000019020000000000000031010000000000000010011801',
            admin:
              '0a5c125a12580a306f72672e6170616368652e6b61666b612e61646d696e2e636f6e6e656374696f6e2e6372656174696f6e2e746f74616c1a01313a210a1b11010000000000000019020000000000000031010000000000000010011801',
          }
          for (const kind of ['producer', 'consumer', 'admin', 'share']) {
            const owner = `js-${kind}`
            const suffix = kind === 'share' ? 'consumer' : kind
            const raw = Buffer.from(rawHex[suffix], 'hex')
            const hash = digest(raw)
            const body = {
              dataBytes: raw.length,
              dataSha256: hash,
              parse: 'ok',
              unknownFieldBytes: 0,
              resourceMetricsCount: 1,
              metricsTotal: 1,
              pointsTotal: 1,
              metricsKept: 1,
              truncated: false,
              resources: [{ attrs: [], droppedAttributes: 0, hasResource: false, schemaUrl: '' }],
              metrics: [
                {
                  res: 0,
                  scope: 0,
                  scopeName: '',
                  scopeVersion: '',
                  scopeDroppedAttributes: 0,
                  name: `org.apache.kafka.${suffix}.connection.creation.total`,
                  unit: '1',
                  kind: 'SUM',
                  temporality: 'AGGREGATION_TEMPORALITY_DELTA',
                  monotonic: true,
                  points: [
                    {
                      attrs: [],
                      startNs: '1',
                      timeNs: '2',
                      flags: 0,
                      valueKind: 'AS_INT',
                      asInt: '1',
                    },
                  ],
                  pointsInMessage: 1,
                },
              ],
            }
            const uuid = record.results.uuids.js[kind]
            const get = {
              type: 'request',
              apiKey: 71,
              apiVersion: 0,
              requestKey: 'g1',
              clientId: `case-${owner}`,
              socketId: 1,
              uuid: '00000000-0000-0000-0000-000000000000',
              writeAt: 10,
              response: {
                ...a.transcripts[owner].records[0].response,
                uuid,
                subscriptionId: 1,
                telemetryMaxBytes: 4096,
                responseAt: 20,
              },
            }
            const requests = [get]
            retain(`${owner}/raw-${hash}.bin`, raw)
            retain(`${owner}/decoder-${hash}.json`, {
              rawSha256: hash,
              code: 0,
              signal: null,
              line: '4.3.1',
              stdout:
                JSON.stringify({ verified: true, line: '4.3.1' }) +
                '\n' +
                JSON.stringify(body) +
                '\n',
            })
            for (let index = 0; index < 3; index++) {
              const requestKey = `r${index + 2}`
              const terminating = index === 2
              const seq = nodeOne.length + 1
              const request = {
                type: 'request',
                apiKey: 72,
                apiVersion: 0,
                requestKey,
                socketId: 1,
                node: '1',
                clientId: `case-${owner}`,
                uuid,
                subscriptionId: 1,
                terminating,
                compressionType: 0,
                metricsBytes: raw.length,
                metricsSha256: hash,
                writeAt: 100 + index * 1000,
                response: { errorCode: 0, responseAt: 101 + index * 1000 },
              }
              requests.push(request)
              nodeOne.push({
                ...body,
                v: 1,
                node: '1',
                seq,
                clientId: `case-${owner}`,
                clientInstanceId: uuid,
                terminating,
                pushIntervalMs: 1000,
                tsMs: 5000 + seq,
              })
              retain(`${owner}/push-${requestKey}.bin`, raw)
              retain(`${owner}/decoded-${requestKey}.json`, {
                requestKey,
                uuid,
                node: '1',
                compressionType: 0,
                transmittedSha256: hash,
                transmittedBytes: raw.length,
                rawSha256: hash,
                rawBytes: raw.length,
                decoded: body,
                exportIdentity: { node: '1', seq },
              })
              record.pushes.push({ owner, requestKey })
            }
            retain(`${owner}/transcript.json`, {
              records: requests,
              failures: [],
              cleanup: { children: 0, timers: 0, stillOpen: [], attached: 0 },
            })
            owners[owner] = `${archiveRoot}/${owner}/transcript.json`
          }
          for (const [node, rows] of [
            ['1', nodeOne],
            ['2', a.rows.consumer],
            ['3', a.rows.admin],
          ])
            retain(
              `exports-${node}.jsonl`,
              Buffer.from(rows.map((r) => JSON.stringify(r) + '\n').join(''))
            )
          record.sidecars = { scenario: 'telemetry-java', root: archiveRoot, files, owners }
          expect(() => verifyScenario(record, directory)).not.toThrow()
          record.results.java.admin.normalExports++
          expect(() => verifyScenario(record, directory)).toThrow('Forged Java normal count')
        } finally {
          a.close()
        }
      }
    )

    test.each([false, true])(
      'replays a complete version-2 Java archive with optional admin final=%s',
      (final) => {
        const a = archive(final)
        try {
          expect(() => verifyJavaInterop(a.record, a.directory, a.transcripts)).not.toThrow()
        } finally {
          a.close()
        }
      }
    )

    test('replays the exact Java subscription in any broker prefix order for every JS kind', () => {
      const a = archive()
      try {
        for (const [index, transcript] of Object.values(a.transcripts).entries()) {
          const prefixes = transcript.records[0].response.requestedMetrics
          transcript.records[0].response.requestedMetrics = [
            ...prefixes.slice(index + 1),
            ...prefixes.slice(0, index + 1),
          ].reverse()
        }
        const before = clone(a.transcripts)
        expect(() => verifyJavaInterop(a.record, a.directory, a.transcripts)).not.toThrow()
        expect(a.transcripts).toEqual(before)
      } finally {
        a.close()
      }
    })

    test.each(['producer', 'consumer', 'admin', 'share'])(
      'rejects an altered Java subscription on any successful Get for JS %s',
      (kind) => {
        const a = archive()
        try {
          const transcript = a.transcripts[`js-${kind}`]
          const first = transcript.records[0]
          const prefixes = first.response.requestedMetrics
          const altered = clone(first)
          transcript.records.push(altered)
          for (const requestedMetrics of [
            prefixes.slice(1),
            prefixes.filter((p) => p !== 'org.apache.kafka.admin.connection.creation.total'),
            prefixes.filter((p) => p !== 'org.apache.kafka.admin.client.connection.creation.total'),
            [...prefixes, 'org.apache.kafka.'],
            [...prefixes, prefixes[0]],
            prefixes.map((p, i) => (i === 0 ? prefixes[1] : p)),
            prefixes.map((p) => p.replace('admin.client.', 'admin-client.')),
          ]) {
            altered.response.requestedMetrics = requestedMetrics
            expect(() => verifyJavaInterop(a.record, a.directory, a.transcripts)).toThrow(
              'Wrong narrow Java requested metrics'
            )
          }
          altered.response.requestedMetrics = [...prefixes].reverse()
          expect(() => verifyJavaInterop(a.record, a.directory, a.transcripts)).not.toThrow()
        } finally {
          a.close()
        }
      }
    )

    test.each([
      [
        'old format',
        (a) => {
          a.record.results.javaEvidenceVersion = 1
        },
        'version 2',
      ],
      [
        'another client version',
        (a) => {
          a.record.results.provenance.clientVersion = '4.4.0'
        },
        'Unqualified Java client version',
      ],
      [
        'wrong jar',
        (a) => {
          a.record.results.provenance.cache[0].sha256 = '0'.repeat(64)
        },
        'Wrong Java pinned input catalogue',
      ],
      [
        'wrong source',
        (a) => {
          a.record.results.provenance.sourceSha256 = '0'.repeat(64)
          a.record.fixture.clientSourceSha256 = '0'.repeat(64)
        },
        'Wrong Java candidate source',
      ],
      [
        'bare final switch',
        (a) => {
          delete a.record.results.java.admin.checkpoint
          a.record.results.java.admin.finalRequired = false
        },
        'Missing admin checkpoint/cessation',
      ],
      [
        'forged normal count',
        (a) => {
          a.record.results.java.admin.normalExports = 3
        },
        'Forged Java normal count',
      ],
      [
        'forged final count',
        (a) => {
          a.record.results.java.admin.finalExports = 1
        },
        'Forged Java final count',
      ],
      [
        'wrong command row hash',
        (a) => {
          a.record.results.java.admin.checkpoint.command.raw =
            a.record.results.java.admin.checkpoint.command.raw.replace(
              a.rows.admin[1].dataSha256,
              '0'.repeat(64)
            )
        },
        'Wrong admin close command',
      ],
      [
        'negative exit interval',
        (a) => {
          a.record.results.children.admin.exitAt = 299
        },
        'Invalid Java close-to-exit interval',
      ],
      [
        'short drain',
        (a) => {
          a.record.results.java.admin.cessation.drain.pop()
        },
        'Short admin drain',
      ],
      [
        'short quiet',
        (a) => {
          a.record.results.java.admin.cessation.quiet.pop()
        },
        'Short admin quiet',
      ],
      [
        'untimed quiet',
        (a) => {
          delete a.record.results.java.admin.cessation.quiet[3].at
        },
        'Incomplete admin timed capture',
      ],
      [
        'missing node',
        (a) => {
          fs.unlinkSync(path.join(a.directory, 'exports-2.jsonl'))
        },
        '',
      ],
      [
        'missing capture node',
        (a) => {
          delete a.record.results.java.admin.checkpoint.proof.nodes[2]
        },
        'Missing Java node capture',
      ],
      [
        'stderr',
        (a) => {
          a.record.results.children.admin.stderr = 'unexpected failure'
        },
        'Unexpected Java stderr',
      ],
      [
        'overflow',
        (a) => {
          a.record.results.children.admin.overflow = true
        },
        'Java output overflow',
      ],
      [
        'killed child',
        (a) => {
          a.record.results.children.admin.signal = 'SIGKILL'
        },
        '',
      ],
      [
        'failed close',
        (a) => {
          a.record.results.children.admin.code = 1
        },
        'Java child did not exit naturally',
      ],
      [
        'undrained pipes',
        (a) => {
          a.record.results.children.admin.pipesDrained = false
        },
        'Java pipes not drained',
      ],
      [
        'surviving descendant',
        (a) => {
          a.record.results.children.admin.liveDescendants = [1]
        },
        'Surviving Java descendant',
      ],
      [
        'missing producer final',
        (a) => {
          a.rows.producer.pop()
          a.record.results.java.producer.finalExports = 0
          a.record.results.java.producer.exports.pop()
          fs.writeFileSync(
            path.join(a.directory, 'exports-1.jsonl'),
            a.rows.producer.map((r) => JSON.stringify(r) + '\n').join('')
          )
        },
        'Missing Java final',
      ],
      [
        'admin policy for producer',
        (a) => {
          a.record.results.java.producer.closePolicy = 'preclose-checkpoint-v1'
        },
        'Wrong Java close policy',
      ],
      [
        'changed Get selection',
        (a) => {
          a.transcripts['js-admin'].records[0].response.requestedMetrics.pop()
        },
        '',
      ],
      [
        'early subscription removal',
        (a) => {
          a.record.results.subscription.deletion.startedAt = 1000
        },
        'Java subscription removed before cessation',
      ],
    ])('rejects %s at the intended replay gate', (_name, mutate, diagnostic) => {
      const a = archive()
      try {
        mutate(a)
        expect(() => verifyJavaInterop(a.record, a.directory, a.transcripts)).toThrow(diagnostic)
      } finally {
        a.close()
      }
    })

    test.each([
      [
        'three metrics',
        (r) => {
          r[1].metrics.pop()
        },
      ],
      [
        'JS admin name',
        (r) => {
          r[1].metrics[0].name = 'org.apache.kafka.admin.connection.creation.total'
        },
      ],
      [
        'wrong Java name',
        (r) => {
          r[1].metrics[0].name = 'org.apache.kafka.admin.client.connection.creation.rate'
        },
      ],
      [
        'duplicate counter',
        (r) => {
          r[1].metrics.push(r[1].metrics[0])
        },
      ],
      [
        'duplicate resource',
        (r) => {
          r[1].metrics[2].res = 0
        },
      ],
      [
        'duplicate point',
        (r) => {
          r[1].metrics[1].points.push(r[1].metrics[1].points[0])
        },
      ],
      [
        'double bits',
        (r) => {
          r[1].metrics[1].points[0].asDoubleBits = '0'.repeat(16)
        },
      ],
      [
        'integer value',
        (r) => {
          r[1].metrics[2].points[0].asInt = '4243'
        },
      ],
      [
        'sum history',
        (r) => {
          r[1].metrics[3].points[0].asDouble = '7'
        },
      ],
      [
        'foreign UUID',
        (r) => {
          r[1].clientInstanceId = '10112233-4455-6677-8899-aabbccddee03'
        },
      ],
    ])('never releases on %s', async (_name, mutate) => {
      const a = archive()
      try {
        const rows = clone(a.rows.admin)
        mutate(rows)
        const ready = JSON.parse(a.record.results.children.admin.markers[2].line)
        expect(() => selectJavaCheckpoint(rows, ready, { clientId: 'case-java-admin' })).toThrow()
      } finally {
        a.close()
      }
    })

    /** @param {object} a archive inputs @returns {void} updates byte receipts without changing cursors */
    const reseal = (a) => {
      for (const rows of Object.values(a.rows))
        fs.writeFileSync(
          path.join(a.directory, `exports-${rows[0].node}.jsonl`),
          rows.map((r) => JSON.stringify(r) + '\n').join('')
        )
      const admin = a.record.results.java.admin
      for (const receipt of [
        admin.checkpoint.proof,
        ...admin.cessation.drain,
        ...admin.cessation.quiet,
      ]) {
        for (const node of ['1', '2', '3']) {
          const bytes = Buffer.from(
            Object.values(a.rows)
              .find((r) => r[0].node === node)
              .slice(0, receipt.cursors[node])
              .map((r) => JSON.stringify(r) + '\n')
              .join('')
          )
          receipt.nodes[node] = { bytes: bytes.length, sha256: digest(bytes) }
        }
      }
      for (const role of ['producer', 'consumer', 'admin']) {
        const java = a.record.results.java[role]
        const child = a.record.results.children[role]
        java.timing = javaSampleSummary(a.rows[role])
        java.normalExports = a.rows[role].filter((r) => !r.terminating).length
        java.finalExports = a.rows[role].filter((r) => r.terminating).length
        java.exports = a.rows[role].map(({ node, seq, terminating, dataSha256 }) => ({
          node,
          seq,
          terminating,
          dataSha256,
        }))
        child.stdout = child.markers.map((m) => m.line + '\n').join('')
        java.markers = child.markers
        java.stdout = child.stdout
      }
    }

    test.each([
      [
        'stale checkpoint',
        (a) => {
          const m = a.record.results.children.admin.markers[2]
          const f = JSON.parse(m.line)
          f.epochMs = 250
          m.line = JSON.stringify(f)
        },
        'Missing fresh admin checkpoint',
      ],
      [
        'future checkpoint',
        (a) => {
          const m = a.record.results.children.admin.markers[2]
          const f = JSON.parse(m.line)
          f.epochMs = 280
          m.line = JSON.stringify(f)
        },
        'Admin epoch order',
      ],
      [
        'duplicate ready',
        (a) => {
          const m = a.record.results.children.admin.markers
          m.splice(3, 0, m[2])
        },
        'Missing or duplicate Java closeReady',
      ],
      [
        'missing release',
        (a) => {
          a.record.results.children.admin.markers.splice(3, 1)
        },
        'Missing or duplicate Java closeReleased',
      ],
      [
        'reordered markers',
        (a) => {
          const m = a.record.results.children.admin.markers
          ;[m[3].line, m[4].line] = [m[4].line, m[3].line]
          a.record.results.java.admin.closeToExitMs = 50
        },
        'Admin checkpoint marker order',
      ],
      [
        'late collection',
        (a) => {
          a.rows.admin[1].metrics[0].points[0].timeNs = '311000000'
        },
        'Sample collected after Java close',
      ],
      [
        'two admin finals',
        (a) => {
          a.rows.admin.push({ ...clone(a.rows.admin[2]), seq: 4, tsMs: 330 })
          for (const r of [
            ...a.record.results.java.admin.cessation.drain,
            ...a.record.results.java.admin.cessation.quiet,
          ])
            r.cursors[3] = 4
        },
        'Two admin finals',
      ],
      [
        'final before close',
        (a) => {
          for (const m of a.rows.admin[2].metrics) m.points[0].timeNs = '299000000'
        },
        'Final outside Java close episode',
      ],
      [
        'collection after final',
        (a) => {
          for (const m of a.rows.admin[1].metrics) m.points[0].timeNs = '306000000'
        },
        'Further collection after admin final',
      ],
      [
        'reused sequence',
        (a) => {
          a.rows.admin[1].seq = 1
        },
        'sequence or node mismatch',
      ],
      [
        'activity during quiet',
        (a) => {
          const late = clone(a.rows.admin[1])
          late.seq = 4
          late.tsMs = 2400
          a.rows.admin.push(late)
          for (const r of a.record.results.java.admin.cessation.quiet.slice(1)) r.cursors[3] = 4
        },
        'Admin activity during quiet window',
      ],
    ])('rejects resealed %s at its diagnostic', (_name, mutate, diagnostic) => {
      const a = archive(true)
      try {
        mutate(a)
        reseal(a)
        expect(() => verifyJavaInterop(a.record, a.directory, a.transcripts)).toThrow(diagnostic)
      } finally {
        a.close()
      }
    })

    test('keeps the KafkaJS admin spelling on the real collector', () => {
      const TelemetryMetrics = require('../../../src/telemetry/metrics')
      const metrics = new TelemetryMetrics({ kind: 'admin' })
      metrics.observeConnection({})
      const subscription = {
        revision: 1n,
        requestedMetrics: ['org.apache.kafka.admin.connection.creation.total'],
        deltaTemporality: true,
      }
      metrics.select(subscription)
      const points = metrics.snapshot(subscription).points
      expect(points).toHaveLength(1)
      expect(points[0]).toMatchObject({
        name: 'org.apache.kafka.admin.connection.creation.total',
        type: 'sum',
        unit: '1',
        value: 1n,
      })
    })

    test.each(['old sample', 'broker time only', 'one normal'])(
      'does not acknowledge %s',
      (mode) => {
        const a = archive()
        try {
          const rows = clone(a.rows.admin)
          if (mode === 'one normal') rows.pop()
          else for (const m of rows[1].metrics) m.points[0].timeNs = '200000000'
          if (mode === 'broker time only') rows[1].tsMs = 999999
          const ready = JSON.parse(a.record.results.children.admin.markers[2].line)
          expect(selectJavaCheckpoint(rows, ready, { clientId: 'case-java-admin' })).toBeUndefined()
        } finally {
          a.close()
        }
      }
    )

    test.each(['valid', 'early exit', 'source timeout', 'malformed row', 'wrong ready UUID'])(
      'joins the parent handshake for %s',
      async (mode) => {
        const a = archive()
        const writes = []
        let pending = 0
        try {
          const childRecord = a.record.results.children.admin
          const marker = (key) =>
            childRecord.markers
              .map((r) => ({ ...r, fields: JSON.parse(r.line) }))
              .find((m) => (key === 'clientInstanceId' ? m.fields.closed === false : m.fields[key]))
          if (mode === 'wrong ready UUID')
            marker('closeReady').fields.clientInstanceId = 'x'.repeat(22)
          const ready = marker('closeReady')
          if (mode === 'wrong ready UUID') ready.fields.clientInstanceId = 'x'.repeat(22)
          const handshake = new JavaAdminHandshake({
            child: {
              marker: (key) => (key === 'closeReady' ? ready : marker(key)),
              done: mode === 'early exit' ? Promise.resolve({ code: 0 }) : new Promise(() => {}),
            },
            signal: new AbortController().signal,
            clientId: 'case-java-admin',
            now: () => 261,
            find: async (signal, _label, _ms, lookup) => {
              pending++
              try {
                await Promise.resolve()
                if (signal.aborted) throw signal.reason
                if (mode === 'source timeout') throw new Error('source timeout')
                const found = lookup()
                if (!found) throw new Error('checkpoint timeout')
                return found
              } finally {
                pending--
              }
            },
            capture: () => a.capture(260, true),
            rowsOf: (id) => {
              expect(id).toBe(canonicalUuid(ready.fields.clientInstanceId))
              const rows = clone(a.rows.admin)
              if (mode === 'malformed row') rows[1].metrics.pop()
              return rows
            },
            send: async (raw) => {
              writes.push(raw)
            },
          })
          if (mode === 'valid') {
            await expect(handshake.run()).resolves.toMatchObject({
              command: { raw: a.record.results.java.admin.checkpoint.command.raw },
            })
            expect(writes).toHaveLength(1)
          } else {
            await expect(handshake.run()).rejects.toThrow()
            expect(writes).toHaveLength(0)
          }
          expect(pending).toBe(0)
        } finally {
          a.close()
        }
      }
    )
  })
})

describe('physical telemetry absence controls', () => {
  const { ordinaryOwnership } = require('../telemetry/semantics')
  test('rejects an additional socket that sent only setup frames', () => {
    const baseline = {
      records: [{ type: 'request', apiKey: 18, socketId: 1 }],
      sockets: [{ id: 1, node: '1', kind: 'normal', foregroundBootstrap: true }],
    }
    expect(ordinaryOwnership(baseline)).toEqual(['1:bootstrap'])
    for (const kind of ['undecided', 'normal', 'telemetry'])
      expect(() =>
        ordinaryOwnership({
          ...baseline,
          sockets: [...baseline.sockets, { id: 2, node: '1', kind }],
        })
      ).toThrow()
  })
})

describe('raw share acknowledgement evidence', () => {
  const { confirmedDecisions } = require('../telemetry/shareEvidence')
  const Encoder = require('../../../src/protocol/encoder')
  const request = require('../../../src/protocol/requests/shareAcknowledge/fixtures/java/shareAcknowledgeRequest-v2-uniform10.json')
  const response = require('../../../src/protocol/requests/shareAcknowledge/fixtures/java/shareAcknowledgeResponse-v2-mixed.json')
  const frame = (bytes) => new Encoder().writeInt32(bytes.length).writeBuffer(bytes).buffer
  const raw = () => ({
    requestHex: frame(
      new Encoder()
        .writeInt16(79)
        .writeInt16(2)
        .writeInt32(7)
        .writeString('tap')
        .writeTaggedFields([])
        .writeBuffer(Buffer.from(request.hex, 'hex')).buffer
    ).toString('hex'),
    responseHex: frame(
      new Encoder()
        .writeInt32(7)
        .writeTaggedFields([])
        .writeBuffer(Buffer.from(response.hex, 'hex')).buffer
    ).toString('hex'),
    responseAt: 12,
  })
  test('joins original request ranges and successful broker partition responses without losing int64 precision', () => {
    const decisions = confirmedDecisions([raw()], 'share-group')
    expect(decisions.map((d) => [d.offset, d.type])).toEqual(
      Array.from({ length: 10 }, (_, i) => [String(9007199254740993n + BigInt(i)), 1])
    )
  })
  test('rejects a foreign correlation and refuses unsuccessful broker confirmations', () => {
    const foreign = raw()
    const bytes = Buffer.from(foreign.responseHex, 'hex')
    bytes.writeInt32BE(8, 4)
    foreign.responseHex = bytes.toString('hex')
    expect(() => confirmedDecisions([foreign], 'share-group')).toThrow()
    const refused = raw()
    const error = Buffer.from(refused.responseHex, 'hex')
    error.writeInt16BE(121, 13)
    refused.responseHex = error.toString('hex')
    expect(confirmedDecisions([refused], 'share-group')).toEqual([])
  })
})

describe('custom metric archive conclusions', () => {
  const { verifyCustom } = require('../telemetry/evidence')
  const fixture = () => {
    const sample = (index, values, startNs = '1') => ({
      request: {
        requestKey: `r${index}`,
        apiKey: 72,
        index,
        response: { errorCode: 0 },
        metricsBytes: 10,
      },
      exportIdentity: { node: '1', seq: index / 10 },
      decoded: {
        metrics: Object.entries(values).map(([name, value]) => ({
          name: `com.example.${name}`,
          unit: '1',
          kind: name === 'total' ? 'SUM' : 'GAUGE',
          ...(name === 'total'
            ? { monotonic: true, temporality: 'AGGREGATION_TEMPORALITY_DELTA' }
            : {}),
          points: [
            {
              attrs: [['series', 'string', 'live']],
              startNs,
              timeNs: '200',
              valueKind: name === 'total' ? 'AS_INT' : 'AS_DOUBLE',
              ...(name === 'total' ? { asInt: value } : { asDouble: value }),
              ...(name === 'zero'
                ? { asDoubleBits: '0' }
                : name === 'negative' && value === '-2.5'
                  ? { asDoubleBits: 'c004000000000000' }
                  : {}),
            },
          ],
        })),
      },
    })
    const samples = [
      sample(10, { anchor: '1.0', zero: '0.0', negative: '-2.5', total: '9007199254740993' }),
      sample(20, { anchor: '1.0', zero: '6.25', negative: '-3.5', total: '9' }),
      sample(30, { anchor: '1.0', zero: '6.25', negative: '-3.5', total: '0' }),
      sample(40, { anchor: '1.0', negative: '-3.5' }),
      sample(50, { anchor: '1.0', negative: '-3.5', total: '1' }, '100'),
    ]
    for (const sample of samples) {
      Object.assign(sample.decoded, {
        resourceMetricsCount: 1,
        resources: [{ attrs: [], droppedAttributes: 0 }],
        metricsTotal: sample.decoded.metrics.length,
        metricsKept: sample.decoded.metrics.length,
        pointsTotal: sample.decoded.metrics.length,
      })
      for (const metric of sample.decoded.metrics)
        Object.assign(metric, { res: 0, pointsInMessage: 1 })
    }
    const results = {
      firstRequestKey: 'r10',
      update: { updateFence: 15, requestKeys: ['r20', 'r30'], states: ['updated', 'following'] },
      removal: {
        requestKey: 'r40',
        removeFence: 35,
        cursor: { 1: 3 },
        names: ['com.example.anchor', 'com.example.negative'],
      },
      readded: { requestKey: 'r50', readdFence: 45, startNs: '100', asInt: '1' },
      unregisterFence: 55,
    }
    return {
      results,
      samples: Object.fromEntries(samples.map((s) => [`producer:${s.request.requestKey}`, s])),
      transcript: { records: samples.map((s) => s.request) },
    }
  }
  test('replays update, removal and new-origin fences from the complete sample stream', () => {
    const { results, samples, transcript } = fixture()
    expect(() => verifyCustom(results, samples, transcript)).not.toThrow()
  })
  test.each([
    'changed-update',
    'missing-update',
    'stale-removal',
    'reused-origin',
    'contradicted-result',
  ])('rejects %s', (change) => {
    const { results, samples, transcript } = fixture()
    if (change === 'changed-update')
      samples['producer:r20'].decoded.metrics[1].points[0].asDouble = '8.0'
    if (change === 'missing-update') results.update.requestKeys.pop()
    if (change === 'stale-removal')
      samples['producer:r40'].decoded.metrics.push(samples['producer:r30'].decoded.metrics.at(-1))
    if (change === 'reused-origin')
      samples['producer:r50'].decoded.metrics.at(-1).points[0].startNs = '1'
    if (change === 'contradicted-result') results.removal.names.push('com.example.zero')
    expect(() => verifyCustom(results, samples, transcript)).toThrow()
  })
})

describe('telemetry churn evidence', () => {
  const { verifyEvidence } = require('../telemetry/evidence')

  test('knows the soak lane and refuses an unknown one with a different diagnostic', () => {
    expect(() => verifyEvidence('telemetry-soak', '/', [])).toThrow('Scenario catalogue mismatch')
    expect(() => verifyEvidence('telemetry-nonexistent', '/', [])).toThrow('Unknown telemetry lane')
  })
})

describe('segmented evidence in the cleanup manifest', () => {
  const crypto = require('crypto')
  const { CiOwnership } = require('../telemetry/ci')
  const { verifyCleanup } = require('../telemetry/evidence')
  const { SEGMENT_FORMAT, segmentCatalogue } = require('../../../testHelpers/telemetrySegments')
  let root
  let owner
  const sha = (bytes) => crypto.createHash('sha256').update(bytes).digest('hex')
  const archive = (
    segments = segmentCatalogue('telemetry-shutdown').length,
    format = SEGMENT_FORMAT
  ) => {
    const dir = path.join(owner.directory, 'case-telemetry-shutdown-abc')
    fs.mkdirSync(dir, { recursive: true })
    fs.writeFileSync(
      path.join(dir, 'index.json'),
      JSON.stringify({
        format,
        scenario: 'telemetry-shutdown',
        segments: Array.from({ length: segments }, (_, index) => ({ id: `s${index}` })),
      })
    )
    const record = { sidecars: { format, root: 'case-telemetry-shutdown-abc' } }
    fs.writeFileSync(
      path.join(path.dirname(owner.receiptFile), 'telemetry-shutdown.jsonl'),
      JSON.stringify(record) + '\n'
    )
  }
  const finalize = () => {
    const receipt = {
      project: owner.project,
      compose: owner.file,
      preflightPassed: true,
      removalVerified: true,
      journalComplete: true,
      startedAt: 'start',
      removalProof: { containers: [], networks: [], volumes: [] },
      ownedRemovalProof: { containers: [], networks: [], volumes: [] },
    }
    owner.save(receipt)
    owner.finalize(receipt)
    return JSON.parse(fs.readFileSync(path.join(owner.directory, 'cleanup-manifest.json')))
  }
  const verify = () =>
    verifyCleanup(owner.directory, path.dirname(owner.receiptFile), owner.project)

  beforeEach(() => {
    root = fs.mkdtempSync(path.join(os.tmpdir(), 'segment-manifest-'))
    owner = new CiOwnership({
      env: {
        CI: 'true',
        COMPOSE_PROJECT_NAME: 'telemetry-1-1-1',
        COMPOSE_FILE: 'docker-compose.telemetry.yml',
        TELEMETRY_EVIDENCE_DIR: path.join(root, 'sink'),
        KAFKA_VERSION: '4.3.1',
      },
      docker: () => '',
    })
    owner.receiptFile = path.join(root, 'stamps', 'ownership.json')
    fs.mkdirSync(owner.directory, { recursive: true })
    fs.mkdirSync(path.dirname(owner.receiptFile), { recursive: true })
  })
  afterEach(() => fs.rmSync(root, { recursive: true, force: true }))

  test('names every segment index with its digest, format and count', () => {
    archive()
    const manifest = finalize()
    const bytes = fs.readFileSync(
      path.join(owner.directory, 'case-telemetry-shutdown-abc/index.json')
    )
    expect(manifest.segmented).toEqual([
      {
        path: 'case-telemetry-shutdown-abc/index.json',
        bytes: bytes.length,
        sha256: sha(bytes),
        format: SEGMENT_FORMAT,
        scenario: 'telemetry-shutdown',
        segments: segmentCatalogue('telemetry-shutdown').length,
      },
    ])
    expect(() => verify()).not.toThrow()
  })

  test('lists no index for a run without segmented archives', () => {
    expect(finalize().segmented).toEqual([])
    expect(() => verify()).not.toThrow()
  })

  test.each([
    [
      'a stamped segmented record whose index the manifest omits',
      'Segment index catalogue differs',
      (manifest) => {
        manifest.segmented = []
      },
    ],
    [
      'an index that changed after it was listed',
      'Segment index digest differs',
      (manifest) => {
        manifest.segmented[0].sha256 = 'f'.repeat(64)
      },
    ],
    [
      'an index of another format',
      'Segment index format differs',
      (manifest) => {
        manifest.segmented[0].format = 'legacy'
      },
    ],
    [
      'an index with fewer segments than the schedule',
      'Segment index count differs from the schedule',
      (manifest) => {
        manifest.segmented[0].segments = 87
      },
    ],
  ])('rejects %s', (_name, diagnostic, change) => {
    archive()
    const manifest = finalize()
    change(manifest)
    fs.writeFileSync(path.join(owner.directory, 'cleanup-manifest.json'), JSON.stringify(manifest))
    expect(() => verify()).toThrow(diagnostic)
  })
})

describe('quiet-window resource identities', () => {
  const { verifyQuietResources } = require('../telemetry/evidence')
  /** Baseline and final readings of a case that left exactly what it found. */
  const readings = () => {
    const reading = () => ({
      activeResources: { Timeout: 2, TCPWRAP: 0 },
      identities: { openSockets: [], pending: [], undischarged: [] },
      resources: { case: {}, quiet: {}, unknown: { Timeout: [4, 9] } },
      registrations: { process: { id: 1, events: { exit: [2], warning: [3] } } },
    })
    return { baseline: reading(), final: reading() }
  }
  const verify = (change = () => {}) => {
    const input = readings()
    change(input)
    return () => verifyQuietResources(input)
  }

  test('accepts a case that left exactly the baseline identities', () => {
    expect(verify()).not.toThrow()
    expect(verify(({ final }) => (final.resources.unknown.Timeout = [9]))).not.toThrow()
  })

  test.each([
    [
      'a same-count substituted timer',
      ({ final }) => (final.resources.unknown.Timeout = [4, 12]),
      'An unknown-owner resource created during the case survives',
    ],
    [
      'a surviving unknown-owner handle',
      ({ final }) => (final.resources.unknown.TCPWRAP = [30]),
      'An unknown-owner resource created during the case survives',
    ],
    [
      'a surviving case timer',
      ({ final }) => (final.resources.case = { Timeout: [17] }),
      'A case resource survives the quiet window',
    ],
    [
      'a surviving quiet-window timer',
      ({ final }) => (final.resources.quiet = { Timeout: [18] }),
      'A quiet resource survives the quiet window',
    ],
    [
      'a duplicate listener registration',
      ({ final }) => (final.registrations.process.events.exit = [2, 2]),
      'Listener registrations differ from the baseline',
    ],
    [
      'a substituted listener at the same count',
      ({ final }) => (final.registrations.process.events.warning = [5]),
      'Listener registrations differ from the baseline',
    ],
    [
      'a pending public call',
      ({ final }) => (final.identities.pending = [3]),
      'Owned resources survive the quiet window',
    ],
    [
      'a missing identity source',
      ({ baseline }) => delete baseline.identities.pending,
      'Required resource source catalogue differs',
    ],
    [
      'a missing role source',
      ({ final }) => delete final.resources.quiet,
      'Required resource role catalogue differs',
    ],
    [
      'a missing registration source',
      ({ final }) => delete final.registrations,
      'Required registration source differs',
    ],
    [
      'a changed event-loop count',
      ({ final }) => (final.activeResources.Timeout = 3),
      'Quiet resources differ',
    ],
  ])('refuses %s', (_name, change, message) => {
    expect(verify(change)).toThrow(message)
  })
})

describe('subscription deletion final telemetry outcome', () => {
  const { assertStreamAccounted } = require('../telemetry/evidence')
  const uuid = '11223344-5566-7788-9900-aabbccddeeff'
  const fixture = () => {
    const request = (apiKey, requestKey, writeAt, responseAt, fields) => ({
      type: 'request',
      apiKey,
      apiVersion: 0,
      requestKey,
      writeAt,
      socketId: 2,
      node: '1',
      uuid,
      clientId: 'candidate',
      ...fields,
      response: { errorCode: 0, responseAt, ...fields.response },
    })
    const entries = {
      metrics: 'com.example.',
      'interval.ms': '1000',
      match: 'client_id=^candidate$',
    }
    const resource = { resourceType: 16, resourceName: 'subscription', errorCode: 0 }
    return {
      transcript: {
        records: [
          request(71, 'r1', 10, 11, {
            response: {
              uuid,
              subscriptionId: -7,
              pushIntervalMs: 1000,
              requestedMetrics: ['com.example.'],
            },
          }),
          request(72, 'r2', 1011, 1012, { subscriptionId: -7, terminating: false }),
          request(72, 'r3', 1201, 1202, {
            subscriptionId: -7,
            terminating: true,
            response: { errorCode: 117 },
          }),
        ],
        sockets: [{ id: 2, closedAt: 1204 }],
        closures: [{ at: 1200, end: 1203, pending: [] }],
      },
      receipt: {
        name: 'subscription',
        entries,
        createdAt: 1,
        readback: {
          ...resource,
          configEntries: Object.entries(entries).map(([configName, configValue]) => ({
            configName,
            configValue,
          })),
        },
        deletion: {
          startedAt: 1100,
          responseAt: 1101,
          response: { resources: [resource] },
          listedAt: 1102,
          listing: { resources: [] },
        },
      },
    }
  }
  const verify = ({ transcript, receipt }) =>
    assertStreamAccounted(
      { producer: transcript },
      { errors: { producer: [117] } },
      { subscriptionDeletions: { producer: receipt } }
    )
  test('qualifies only the current subscription final after verified resource deletion', () => {
    const { transcript, receipt } = fixture()
    expect(() =>
      assertStreamAccounted(
        { producer: transcript },
        {},
        {
          subscriptionDeletions: { producer: receipt },
        }
      )
    ).not.toThrow()
  })
  test.each([
    [
      'missing deletion receipt',
      (f) => {
        delete f.receipt
      },
    ],
    [
      'failed deletion',
      (f) => {
        f.receipt.deletion.response.resources[0].errorCode = 7
      },
    ],
    [
      'resource still listed',
      (f) => {
        f.receipt.deletion.listing.resources.push({ type: 16, name: 'subscription' })
      },
    ],
    [
      'deletion after close',
      (f) => {
        f.receipt.deletion.listedAt = 1201
      },
    ],
    [
      'foreign readback',
      (f) => {
        f.receipt.readback.resourceName = 'foreign'
      },
    ],
    [
      'foreign subscription',
      (f) => {
        f.transcript.records[2].subscriptionId = -8
      },
    ],
    [
      'foreign UUID',
      (f) => {
        f.transcript.records[2].uuid = '22334455-6677-8899-0011-aabbccddeeff'
      },
    ],
    [
      'foreign socket',
      (f) => {
        f.transcript.records[2].socketId = 3
      },
    ],
    [
      'outside close boundary',
      (f) => {
        f.transcript.records[2].writeAt = 1199
      },
    ],
    [
      'no successful preceding Push',
      (f) => {
        f.transcript.records.splice(1, 1)
      },
    ],
    [
      'refresh after final',
      (f) => {
        f.transcript.records.push({ ...f.transcript.records[0], requestKey: 'r4', writeAt: 1203 })
      },
    ],
    [
      'duplicate final',
      (f) => {
        f.transcript.records.push({ ...f.transcript.records[2], requestKey: 'r4' })
      },
    ],
  ])('rejects %s despite an owner-wide 117 allowance', (_name, mutate) => {
    const value = fixture()
    mutate(value)
    expect(() => verify(value)).toThrow('Unqualified final subscription error')
  })
})

describe('Java node export capture', () => {
  const crypto = require('crypto')
  const { assertJavaCaptures, readNodeExports } = require('../telemetry/semantics')
  const sha256 = (bytes) => crypto.createHash('sha256').update(bytes).digest('hex')
  const line = (node, seq) => JSON.stringify({ node, seq }) + '\n'
  /** @returns {string} an evidence directory where node 3 has not exported yet */
  const directory = () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'java-capture-'))
    fs.writeFileSync(path.join(dir, 'exports-1.jsonl'), line(1, 1))
    fs.writeFileSync(path.join(dir, 'exports-2.jsonl'), line(2, 1))
    return dir
  }
  /** @returns {object} a capture receipt over the given per-node byte prefixes */
  const receipt = (prefixes) => ({
    nodes: Object.fromEntries(
      Object.entries(prefixes).map(([node, bytes]) => [
        node,
        { bytes: bytes.length, sha256: sha256(bytes) },
      ])
    ),
    cursors: Object.fromEntries(
      Object.entries(prefixes).map(([node, bytes]) => [node, bytes.length ? 1 : 0])
    ),
  })

  test('reads a node that has not exported yet as empty', () => {
    const dir = directory()
    expect(readNodeExports(dir, '3')).toEqual(Buffer.alloc(0))
    expect(readNodeExports(dir, '1').toString()).toBe(line(1, 1))
  })

  test('accepts an absent node file captured with zero bytes', () => {
    const dir = directory()
    const prefixes = {
      1: Buffer.from(line(1, 1)),
      2: Buffer.from(line(2, 1)),
      3: Buffer.alloc(0),
    }
    expect(() => assertJavaCaptures([receipt(prefixes)], dir)).not.toThrow()
  })

  test('rejects an absent node file whose capture recorded bytes', () => {
    const dir = directory()
    const prefixes = {
      1: Buffer.from(line(1, 1)),
      2: Buffer.from(line(2, 1)),
      3: Buffer.from(line(3, 1)),
    }
    expect(() => assertJavaCaptures([receipt(prefixes)], dir)).toThrow('Invalid capture length')
  })
})
