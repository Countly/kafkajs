const fs = require('fs')
const os = require('os')
const path = require('path')
const { introspect, versionDirs, requestDirectories, readFork } = require('../forkVersions')
const trackedApis = require('../tracked-apis.json')

/** Builds a throwaway src/protocol/requests-shaped tree: { produce: [3, 4], ... }. */
const buildTree = layout => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'compat-fork-'))
  for (const name of Object.keys(layout)) {
    const dir = path.join(root, name)
    fs.mkdirSync(dir)
    fs.writeFileSync(path.join(dir, 'index.js'), 'module.exports = { versions: [] }\n')
    for (const version of layout[name]) fs.mkdirSync(path.join(dir, `v${version}`))
  }
  return root
}

const entry = (kafkaName, apiKey, forkName, apiKeysName) => ({
  kafkaName,
  apiKey,
  forkName,
  apiKeysName,
  planned: forkName === null ? 'no' : 'done',
})

describe('scripts/compat/forkVersions', () => {
  describe('filesystem readers', () => {
    const root = buildTree({ produce: [0, 1, 10, 2], fetch: [], notAnApi: [] })

    beforeAll(() => {
      fs.rmSync(path.join(root, 'notAnApi', 'index.js'))
    })

    it('sorts v<N> directories numerically, not lexically', () => {
      expect(versionDirs(root, 'produce')).toEqual([0, 1, 2, 10])
    })

    it('only counts directories that export an index.js', () => {
      expect(requestDirectories(root)).toEqual(['fetch', 'produce'])
    })
  })

  describe('introspect', () => {
    const run = (layout, requests, apiKeys, entries) =>
      introspect({
        trackedApis: { tracked: entries.tracked, untracked: entries.untracked },
        requestsDir: buildTree(layout),
        requests,
        apiKeys,
      })

    it('reports nothing when directories, versions and apiKeys agree', () => {
      const result = run(
        { produce: [3, 4] },
        { Produce: { versions: ['3', '4'] } },
        { Produce: 0 },
        { tracked: [entry('Produce', 0, 'produce', 'Produce')], untracked: [] }
      )
      expect(result.findings).toEqual([])
      expect(result.apis.Produce).toEqual({
        hasCodec: true,
        stub: false,
        versionDirs: [3, 4],
        declaredVersions: [3, 4],
        forkVersions: [3, 4],
      })
    })

    it('errors when a v<N> directory is not in the versions array', () => {
      const result = run(
        { produce: [3, 4, 5] },
        { Produce: { versions: ['3', '4'] } },
        { Produce: 0 },
        { tracked: [entry('Produce', 0, 'produce', 'Produce')], untracked: [] }
      )
      expect(result.findings).toEqual([
        {
          level: 'error',
          code: 'dir-mismatch',
          message: 'Produce: v<N> directories [3,4,5] do not match index.js versions [3,4]',
        },
      ])
    })

    it('errors when the versions array lists a version with no directory', () => {
      const result = run(
        { produce: [3] },
        { Produce: { versions: ['3', '4'] } },
        { Produce: 0 },
        { tracked: [entry('Produce', 0, 'produce', 'Produce')], untracked: [] }
      )
      expect(result.findings.map(f => f.code)).toEqual(['dir-mismatch'])
    })

    it('errors on a request directory missing from tracked-apis.json', () => {
      const result = run(
        { produce: [3], ghost: [0] },
        { Produce: { versions: ['3'] } },
        { Produce: 0 },
        { tracked: [entry('Produce', 0, 'produce', 'Produce')], untracked: [] }
      )
      expect(result.findings).toEqual([
        {
          level: 'error',
          code: 'unknown-directory',
          message: 'src/protocol/requests/ghost/ is not listed in tracked-apis.json',
        },
      ])
    })

    it('errors on an apiKeys.js key missing from tracked-apis.json', () => {
      const result = run(
        { produce: [3] },
        { Produce: { versions: ['3'] }, Ghost: { versions: [] } },
        { Produce: 0, Ghost: 99 },
        { tracked: [entry('Produce', 0, 'produce', 'Produce')], untracked: [] }
      )
      expect(result.findings).toEqual([
        {
          level: 'error',
          code: 'unknown-api-key',
          message: 'apiKeys.js "Ghost" is not listed in tracked-apis.json',
        },
      ])
    })

    it('errors when an api key is listed twice', () => {
      const result = run(
        { produce: [3] },
        { Produce: { versions: ['3'] } },
        { Produce: 0 },
        {
          tracked: [entry('Produce', 0, 'produce', 'Produce')],
          untracked: [entry('ProduceAgain', 0, null, null)],
        }
      )
      expect(result.findings.map(f => f.code)).toContain('duplicate-entry')
    })

    it('classifies an empty versions array with no directory as a stub', () => {
      const result = run(
        {},
        { OffsetForLeaderEpoch: { versions: [] } },
        { OffsetForLeaderEpoch: 23 },
        {
          tracked: [entry('OffsetForLeaderEpoch', 23, null, 'OffsetForLeaderEpoch')],
          untracked: [],
        }
      )
      expect(result.findings).toEqual([])
      expect(result.apis.OffsetForLeaderEpoch).toMatchObject({ hasCodec: true, stub: true })
    })

    it('reports an apiKeys.js name with no entry in requests/index.js', () => {
      const result = run(
        {},
        {},
        { ElectPreferredLeaders: 43 },
        { tracked: [], untracked: [entry('ElectLeaders', 43, null, 'ElectPreferredLeaders')] }
      )
      expect(result.findings).toEqual([
        {
          level: 'report',
          code: 'missing-requests-entry',
          message:
            'apiKeys.js "ElectPreferredLeaders" (key 43) has no entry in requests/index.js — lookup() can never reach it',
        },
      ])
    })
  })

  describe('readFork against the real fork', () => {
    const result = readFork(trackedApis)

    it('has no error-level findings', () => {
      expect(result.findings.filter(f => f.level === 'error')).toEqual([])
    })

    it('reports exactly the known ElectPreferredLeaders naming inconsistency', () => {
      expect(result.findings.map(f => f.code)).toEqual(['missing-requests-entry'])
    })

    it('reads the implemented version sets', () => {
      expect(result.apis.Produce.forkVersions).toEqual([0, 1, 2, 3, 4, 5, 6, 7])
      expect(result.apis.OffsetFetch.forkVersions).toEqual([1, 2, 3, 4])
      expect(result.apis.FindCoordinator.forkVersions).toEqual([0, 1, 2])
      expect(result.apis.OffsetForLeaderEpoch.stub).toBe(true)
      expect(result.apis.ConsumerGroupHeartbeat.hasCodec).toBe(false)
    })
  })
})
