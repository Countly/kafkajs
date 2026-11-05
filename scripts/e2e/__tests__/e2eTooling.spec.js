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
        [undefined, '4.3.1'],
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

      test('holds the floor image and the three lines the suite runs on', () => {
        const versions = Object.keys(table())

        expect(versions).toHaveLength(4)
        expect(versions).toEqual(expect.arrayContaining(['4.1.2', '4.2.2', '4.3.1']))
        expect(versions.filter((version) => /^4\.0\.\d+$/.test(version))).toHaveLength(1)
      })
    })
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
     * offline switch and extra classpath are reset so an inherited value cannot change the result.
     * @param {string[]} args arguments passed unchanged to the selected Java member
     * @param {object} overrides environment additions for a failure or version control
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
      ['share probe', ['--role', 'share-member', '--probe'], 'JavaShareMember'],
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

    test('keeps both jar-version overrides and the caller-owned cache', () => {
      fs.writeFileSync(path.join(cache, 'kafka-clients-custom.jar'), '')
      fs.writeFileSync(path.join(cache, 'slf4j-api-custom.jar'), '')
      const run = launch(['--probe'], { KAFKA_CLIENTS_VERSION: 'custom', SLF4J_VERSION: 'custom' })
      expect(run.status).toBe(0)
      expect(JSON.parse(run.stdout).args[1]).toBe(
        [
          path.join(cache, 'kafka-clients-custom.jar'),
          path.join(cache, 'slf4j-api-custom.jar'),
          cache,
        ].join(path.delimiter)
      )
      expect(fs.existsSync(cache)).toBe(true)
      expect(run.calls.map(({ command }) => command)).toEqual(['javac', 'java'])
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
