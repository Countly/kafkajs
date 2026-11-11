const fs = require('fs')
const os = require('os')
const path = require('path')
const { importCapture, mergeClientTables, main } = require('../importCaptureArtifacts')
const { projectBrokerTools } = require('../captureBrokerTools')
const { RAW, projectorInput, writeArtifact, wireObject } = require('./fixtures/brokerToolFixtures')

describe('Independent broker tool capture import', () => {
  /** @type {string[]} */
  const directories = []
  const scratch = () => {
    const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'capture-import-'))
    directories.push(directory)
    return directory
  }
  const artifact = (options) => {
    const directory = scratch()
    writeArtifact(directory, options)
    return directory
  }

  afterAll(() => {
    for (const directory of directories) fs.rmSync(directory, { recursive: true, force: true })
  })

  test('writes the projected capture and the raw outputs byte for byte', () => {
    const artifactDir = artifact()
    const outDir = scratch()

    const { capture, target } = importCapture({ artifactDir, version: '4.4.0', outDir })

    expect(target).toBe(path.join(outDir, '4.4.0'))
    expect(capture).toEqual(projectBrokerTools(projectorInput()))
    const written = fs.readFileSync(path.join(target, 'apiVersions.json'), 'utf8')
    expect(JSON.parse(written)).toEqual(capture)
    expect(written).toContain('"0": [3, 13]')
    for (const file of Object.values(RAW)) {
      expect(
        fs
          .readFileSync(path.join(target, file))
          .equals(fs.readFileSync(path.join(artifactDir, file)))
      ).toBe(true)
    }
    expect(fs.readdirSync(target).sort()).toEqual(
      [...Object.values(RAW), 'apiVersions.json'].sort()
    )
  })

  test('refuses a tampered artifact, a version the broker does not report and an existing capture', () => {
    const tampered = artifact()
    fs.appendFileSync(path.join(tampered, RAW.features), ' ')
    expect(() =>
      importCapture({ artifactDir: tampered, version: '4.4.0', outDir: scratch() })
    ).toThrow(/artifact digest mismatch for kafka-features-describe\.txt/)

    const outDir = scratch()
    expect(() => importCapture({ artifactDir: artifact(), version: '4.3.2', outDir })).toThrow(
      /reports 4\.4\.0, not 4\.3\.2/
    )
    expect(fs.readdirSync(outDir)).toEqual([])

    const mixed = artifact({
      identity:
        'INFO Kafka version: 4.4.0 (x)\nINFO Kafka version: 4.3.2 (x)\nINFO Kafka commitId: 0123456789abcdef (x)\n',
    })
    expect(() => importCapture({ artifactDir: mixed, version: '4.4.0', outDir })).toThrow(
      /exactly one version/
    )

    importCapture({ artifactDir: artifact(), version: '4.4.0', outDir })
    expect(() => importCapture({ artifactDir: artifact(), version: '4.4.0', outDir })).toThrow(
      /already exists/
    )
  })

  describe('client tables', () => {
    const capture = () => projectBrokerTools(projectorInput())
    const OTHER = { Produce: [0, 12] }

    /** The rows the probe records for the fixture capture: by name, `ApiKey<n>` for key 95. */
    const probed = () => ({
      Produce: [3, 13],
      Fetch: [4, 18],
      SaslHandshake: [0, 1],
      ApiVersions: [0, 5],
      ShareGroupHeartbeat: [1, 1],
      ApiKey95: [0, 2],
    })

    const files = (ranges, features) => {
      const tablesDir = scratch()
      fs.writeFileSync(
        path.join(tablesDir, 'brokerRanges.json'),
        JSON.stringify({ '4.4.0': ranges })
      )
      fs.writeFileSync(
        path.join(tablesDir, 'brokerFeatures.json'),
        JSON.stringify({ '4.4.0': features })
      )
      const repo = scratch()
      const rangesFile = path.join(repo, 'brokerRanges.json')
      const featuresFile = path.join(repo, 'brokerFeatures.json')
      fs.writeFileSync(rangesFile, JSON.stringify({ '4.3.1': OTHER }))
      fs.writeFileSync(featuresFile, JSON.stringify({ '4.3.1': { 'share.version': 1 } }))
      return { tablesDir, rangesFile, featuresFile }
    }

    test('merges client tables only when they equal the independent capture', () => {
      const merge = files(probed(), { 'share.version': 2 })
      mergeClientTables({ ...merge, version: '4.4.0', capture: capture() })

      expect(JSON.parse(fs.readFileSync(merge.rangesFile, 'utf8'))).toEqual({
        '4.3.1': OTHER,
        '4.4.0': probed(),
      })
      expect(JSON.parse(fs.readFileSync(merge.featuresFile, 'utf8'))).toEqual({
        '4.3.1': { 'share.version': 1 },
        '4.4.0': { 'share.version': 2 },
      })

      for (const [ranges, features, reason] of [
        [{ ...probed(), Produce: [3, 12] }, { 'share.version': 2 }, /API 0.*differs/],
        [{ ...probed(), Metadata: [0, 13] }, { 'share.version': 2 }, /lists 7 APIs/],
        [probed(), { 'share.version': 1 }, /share\.version/],
      ]) {
        const refused = files(ranges, features)
        const before = fs.readFileSync(refused.rangesFile, 'utf8')
        expect(() =>
          mergeClientTables({ ...refused, version: '4.4.0', capture: capture() })
        ).toThrow(reason)
        expect(fs.readFileSync(refused.rangesFile, 'utf8')).toBe(before)
      }
    })

    test('a dry run rejects an unsuccessful capture before merging client tables', () => {
      const protocol = path.join(__dirname, '..')
      const snapshot = () => [
        fs.readFileSync(path.join(protocol, 'brokerRanges.json'), 'utf8'),
        fs.readFileSync(path.join(protocol, 'brokerFeatures.json'), 'utf8'),
        fs.readdirSync(path.join(protocol, 'captures')).sort(),
      ]
      const before = snapshot()
      const { tablesDir } = files(probed(), { 'share.version': 2 })
      const artifactDir = artifact({ wire: { ...wireObject(), errorCode: 35 } })

      expect(() =>
        main(['--artifact', artifactDir, '--version', '4.4.0', '--tables', tablesDir, '--dry-run'])
      ).toThrow(/Incomplete broker recording for 4\.4\.0: unsuccessful feature response/)
      expect(snapshot()).toEqual(before)
    })

    test('a dry run checks the artifact and the client tables and writes nothing to the repository', () => {
      const protocol = path.join(__dirname, '..')
      const snapshot = () => [
        fs.readFileSync(path.join(protocol, 'brokerRanges.json'), 'utf8'),
        fs.readFileSync(path.join(protocol, 'brokerFeatures.json'), 'utf8'),
        fs.readdirSync(path.join(protocol, 'captures')).sort(),
      ]
      const before = snapshot()
      const { tablesDir } = files(probed(), { 'share.version': 2 })
      const log = jest.spyOn(console, 'log').mockImplementation(() => {})
      try {
        main(['--artifact', artifact(), '--version', '4.4.0', '--tables', tablesDir, '--dry-run'])
        expect(log).toHaveBeenCalledWith('dry run of 4.4.0: 6 API ranges, key 94 not offered')

        const { tablesDir: wrong } = files(
          { ...probed(), Produce: [3, 12] },
          { 'share.version': 2 }
        )
        expect(() =>
          main(['--artifact', artifact(), '--version', '4.4.0', '--tables', wrong, '--dry-run'])
        ).toThrow(/API 0.*differs/)
      } finally {
        log.mockRestore()
      }

      expect(snapshot()).toEqual(before)
    })
  })
})
