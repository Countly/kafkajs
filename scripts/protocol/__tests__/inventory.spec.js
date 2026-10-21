'use strict'

const fs = require('fs')
const os = require('os')
const path = require('path')
const { discover } = require('../schema')
const { CodecInventory, CAPS, lineOf, aliasChecker, checkInventory } = require('../inventory')

const BROKER = 'src/broker/index.js'
const API = 'Heartbeat'
const DEFINITIONS = { [API]: { versions: ['0', '1', '2', '3', '4', '5'] } }
// v0 and v1 are below the minimum of both lines, v2 is only reachable under the ceiling, v3 and v4
// are what the two lines select, and v5 is above both maxima.
const RANGES = { '4.0.0': { [API]: [2, 3] }, '4.1.0': { [API]: [2, 4] } }
const CAP = [
  { api: API, max: 2, file: BROKER, token: 'const CEILING = 2', reason: 'a test ceiling' },
]

const codecFile = (version, direction, extra = '') =>
  `src/protocol/requests/heartbeat/v${version}/${direction}.js`

/**
 * Writes a repository of tiny codecs under a temporary directory.
 * @param {Record<string, string>} [files] more files, or replacements, by repository path
 * @returns {string} its root
 */
const makeRoot = (files = {}) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'inventory-'))
  const all = { [BROKER]: 'const CEILING = 2\n', ...files }
  for (let version = 0; version <= 5; version++) {
    for (const direction of ['request', 'response']) {
      all[codecFile(version, direction)] =
        all[codecFile(version, direction)] || 'module.exports = {}\n'
    }
  }
  for (const [file, text] of Object.entries(all)) {
    fs.mkdirSync(path.dirname(path.join(root, file)), { recursive: true })
    fs.writeFileSync(path.join(root, file), text)
  }
  return root
}

const inventoryOf = (root, options = {}) =>
  new CodecInventory({
    root,
    definitions: DEFINITIONS,
    ranges: RANGES,
    caps: CAP,
    codecs: discover(root),
    ...options,
  })

const wireOf = (inventory, version, direction = 'request') =>
  inventory.rows.find((row) => row.version === version && row.direction === direction).wire

const run = (inventory) => {
  const failures = []
  const reports = []
  inventory.check({ aliasProblem: aliasChecker(inventory) }, failures, reports)
  return { failures, reports }
}

describe('scripts/protocol > the codec census', () => {
  let roots = []
  const build = (files) => {
    const root = makeRoot(files)
    roots.push(root)
    return root
  }

  afterEach(() => {
    for (const root of roots) fs.rmSync(root, { recursive: true, force: true })
    roots = []
  })

  describe('wire-negotiation reachability', () => {
    test('puts every version in exactly one class by what selectVersion returns on the lines', () => {
      const inventory = inventoryOf(build())

      expect([0, 1, 2, 3, 4, 5].map((version) => wireOf(inventory, version))).toEqual([
        'below-floor',
        'below-floor',
        'capped-only',
        'reachable',
        'reachable',
        'dead',
      ])
      expect(inventory.rows).toHaveLength(12)
    })

    test('without the ceiling the same version is dead, not capped-only', () => {
      const inventory = inventoryOf(build(), { caps: [] })

      expect(wireOf(inventory, 2)).toBe('dead')
    })

    test('a version the lines never offer is dead when it is above their maxima and below-floor when below their minima', () => {
      const inventory = inventoryOf(build(), { ranges: { '4.0.0': { [API]: [3, 4] } }, caps: [] })

      expect([0, 1, 2, 3, 4, 5].map((version) => wireOf(inventory, version))).toEqual([
        'below-floor',
        'below-floor',
        'below-floor',
        'dead',
        'reachable',
        'dead',
      ])
    })

    test('a version below the minimum of one line and not of another is dead, not below-floor', () => {
      const inventory = inventoryOf(build(), {
        ranges: { '4.0.0': { [API]: [1, 3] }, '4.1.0': { [API]: [3, 4] } },
        caps: [],
      })

      expect([0, 1, 2].map((version) => wireOf(inventory, version))).toEqual([
        'below-floor',
        'dead',
        'dead',
      ])
    })

    test('an API no line offers has dead codecs and no reachable one', () => {
      const inventory = inventoryOf(build(), { ranges: { '4.0.0': {} } })

      expect(new Set(inventory.rows.map(({ wire }) => wire))).toEqual(new Set(['dead']))
      expect(inventory.selected).toEqual([])
    })
  })

  describe('implementation-import reachability', () => {
    const importing = {
      [codecFile(4, 'response')]:
        "const { decode } = require('../v0/response')\nmodule.exports = { decode }\n",
      [codecFile(0, 'response')]: 'module.exports = { decode: () => {} }\n',
    }

    test('a below-floor file a reachable codec imports is kept; the others may be deleted', () => {
      const inventory = inventoryOf(build(importing))

      expect(
        inventory.rows.find((row) => row.file === codecFile(0, 'response')).importReachable
      ).toBe(true)
      expect(inventory.deletable.map(({ file }) => file).sort()).toEqual([
        codecFile(0, 'request'),
        codecFile(1, 'request'),
        codecFile(1, 'response'),
      ])
    })

    test('a file imported through another file is reached too', () => {
      const inventory = inventoryOf(
        build({
          ...importing,
          [codecFile(0, 'response')]: "module.exports = require('../v1/response')\n",
        })
      )

      expect(inventory.deletable.map(({ file }) => file)).not.toContain(codecFile(1, 'response'))
    })

    test('a dead file is not deletable by the rule, whether it is imported or not', () => {
      expect(inventoryOf(build()).deletable.map(({ file }) => file)).not.toContain(
        codecFile(5, 'request')
      )
    })

    test('deleting a file a reachable codec imports fails the check', () => {
      const root = build(importing)
      fs.rmSync(path.join(root, codecFile(0, 'response')))

      const { failures } = run(inventoryOf(root))

      expect(failures).toContain(
        `I4 ${codecFile(4, 'response')} requires ../v0/response, which does not exist`
      )
    })

    test('with the file present the same check has no I4', () => {
      expect(
        run(inventoryOf(build(importing))).failures.filter((failure) => failure.startsWith('I4'))
      ).toEqual([])
    })
  })

  describe('schema and golden gaps', () => {
    const schemaOf = (version, direction) =>
      `module.exports = {}\nmodule.exports.schema = { api: '${API}', version: ${version}, direction: '${direction}', fields: [] }\n`
    const withSchemas = Object.fromEntries(
      [2, 3, 4].flatMap((version) =>
        ['request', 'response'].map((direction) => [
          codecFile(version, direction),
          schemaOf(version, direction),
        ])
      )
    )
    const golden = (hasGolden) => ({
      goldensFor: (directory, version, direction) =>
        hasGolden(version, direction)
          ? [{ family: 'x/java', spec: 's', test: 't', fixtures: [{}] }]
          : [],
    })

    test('a selected pair with no schema export and no waiver is an I1 failure; a dead one is not', () => {
      const inventory = inventoryOf(
        build({ ...withSchemas, [codecFile(3, 'request')]: 'module.exports = {}\n' })
      )

      expect(run(inventory).failures).toContain(
        `I1 ${codecFile(3, 'request')} (Heartbeat v3 request, reachable) has no schema export and no waiver`
      )
      expect(inventory.schemaGaps.map(({ file }) => file)).toEqual([codecFile(3, 'request')])
    })

    test('a selected pair with no golden assertion and no waiver is an I2 failure', () => {
      const inventory = inventoryOf(build(withSchemas), {
        dispositions: golden((version) => version !== 4),
      })

      expect(inventory.goldenGaps.map(({ file }) => file)).toEqual([
        codecFile(4, 'request'),
        codecFile(4, 'response'),
      ])
      expect(run(inventory).failures.filter((failure) => failure.startsWith('I2'))).toHaveLength(2)
    })

    test('with no dispositions at all every selected pair lacks a golden', () => {
      expect(inventoryOf(build(withSchemas)).goldenGaps).toHaveLength(6)
    })

    test('an approved exclusion lifts both gaps for its pair', () => {
      const waiver = {
        api: API,
        version: 3,
        direction: 'request',
        kind: 'exclusion',
        reason: 'a test pair',
        approvedBy: '2026-10-19, protocol reviewer',
        evidence: 'a test',
      }
      const inventory = inventoryOf(
        build({ ...withSchemas, [codecFile(3, 'request')]: 'module.exports = {}\n' }),
        {
          waivers: [waiver],
          dispositions: golden((version, direction) => !(version === 3 && direction === 'request')),
        }
      )

      expect(inventory.schemaGaps).toEqual([])
      expect(inventory.goldenGaps).toEqual([])
      expect(run(inventory).failures).toEqual([])
    })

    test('the report counts every class, the gaps, the ceilings with their lines and the waivers', () => {
      const { reports } = run(inventoryOf(build(withSchemas), { dispositions: golden(() => true) }))

      expect(reports).toEqual([
        expect.stringMatching(
          /^I6 12 codec files: 4 reachable, 2 capped-only, 4 below-floor, 2 dead; 0 not selected but imported by a selected codec; 0 schema gaps, 0 golden gaps$/
        ),
        'I7 ceilings: Heartbeat v2 src/broker/index.js:1',
        'I8 waivers: 0; deletable: 4',
      ])
    })
  })

  describe('the waiver file', () => {
    const good = {
      api: API,
      version: 3,
      direction: 'request',
      kind: 'exclusion',
      reason: 'a test pair',
      approvedBy: '2026-10-19, protocol reviewer',
      evidence: 'a test',
    }
    const gapped = { [codecFile(3, 'request')]: 'module.exports = {}\n' }
    const failuresFor = (waiver, files = gapped) =>
      run(inventoryOf(build(files), { waivers: [waiver] })).failures.filter((failure) =>
        failure.startsWith('I3')
      )

    test('a complete entry for a pair with a gap has no failure', () => {
      expect(failuresFor(good)).toEqual([])
    })

    test.each(['reason', 'approvedBy', 'evidence', 'kind', 'direction', 'api'])(
      'an entry without %s fails',
      (field) => {
        expect(failuresFor({ ...good, [field]: undefined }).join('\n')).toContain(`has no ${field}`)
      }
    )

    test.each([
      ['a local path', '2026-10-19, see notes/review.md'],
      ['a ruling label', '2026-10-19, R99'],
      ['a package label', '2026-10-19, W99 reviewer'],
      ['a phase label', '2026-10-19, Phase 0 reviewer'],
      ['no date', 'protocol reviewer'],
    ])('an approvedBy with %s fails', (_, approvedBy) => {
      expect(failuresFor({ ...good, approvedBy }).join('\n')).toContain(
        'approvedBy must be a review date and role'
      )
    })

    test('an entry for a pair that is not reachable or capped-only fails', () => {
      expect(failuresFor({ ...good, version: 5 }).join('\n')).toContain(
        'is not a reachable or capped-only pair'
      )
    })

    test('an entry for a pair that has a schema and a golden is not needed', () => {
      const withSchema = `module.exports = {}\nmodule.exports.schema = { api: '${API}', version: 3, direction: 'request', fields: [] }\n`
      const inventory = inventoryOf(build({ [codecFile(3, 'request')]: withSchema }), {
        waivers: [good],
        dispositions: { goldensFor: () => [{ fixtures: [{}] }] },
      })

      expect(run(inventory).failures.join('\n')).toContain(
        'is not needed: the pair has a schema and a golden'
      )
    })

    test('an entry that appears twice fails', () => {
      const inventory = inventoryOf(build(gapped), { waivers: [good, good] })

      expect(run(inventory).failures.join('\n')).toContain('appears twice')
    })
  })

  describe('an alias waiver', () => {
    const shared = () => ({})
    const codecs = (functions) =>
      [1, 2, 3].map((version) => ({
        file: codecFile(version, 'request'),
        dir: `src/protocol/requests/heartbeat/v${version}`,
        direction: 'request',
        schema: null,
        module: functions[version],
      }))
    const alias = (aliasOf, version) => ({
      api: API,
      version,
      direction: 'request',
      kind: 'alias',
      aliasOf,
      reason: 'the same encoder',
      approvedBy: '2026-10-19, protocol reviewer',
      evidence: 'a test',
    })
    const ranges = { '4.0.0': { [API]: [0, 2] } }
    const problems = (functions, waiver) =>
      run(
        new CodecInventory({
          root: build(),
          definitions: DEFINITIONS,
          ranges,
          caps: [],
          codecs: codecs(functions),
          waivers: [waiver],
        })
      ).failures.filter((failure) => failure.startsWith('I3'))

    test('holds for the same function object and a definition that lists the same fields', () => {
      expect(problems({ 1: shared, 2: shared, 3: shared }, alias(1, 2))).toEqual([])
    })

    test('fails when the two exports are different function objects', () => {
      expect(problems({ 1: shared, 2: () => ({}), 3: shared }, alias(1, 2)).join('\n')).toContain(
        'do not export the same function object'
      )
    })

    test('fails when the two versions list different fields: v3 adds GroupInstanceId', () => {
      const wider = new CodecInventory({
        root: build(),
        definitions: DEFINITIONS,
        ranges: { '4.0.0': { [API]: [0, 3] } },
        caps: [],
        codecs: codecs({ 1: shared, 2: shared, 3: shared }),
        waivers: [alias(2, 3)],
      })

      expect(run(wider).failures.join('\n')).toContain('the definitions of v3 and v2 differ in')
    })

    test('fails without an aliasOf that names another version', () => {
      expect(
        problems({ 1: shared, 2: shared, 3: shared }, alias(undefined, 2)).join('\n')
      ).toContain('aliasOf must name another version')
    })
  })

  describe('the ceiling table', () => {
    test('finds a ceiling by the text of its line, so a moved line stays found', () => {
      const root = build({ [BROKER]: '// a\n// b\nconst CEILING = 2\n' })

      expect(lineOf(root, BROKER, 'const CEILING = 2')).toBe(3)
      expect(lineOf(root, BROKER, 'const GONE = 2')).toBeNull()
    })

    test('a ceiling whose text is gone from its file fails I5', () => {
      const { failures } = run(inventoryOf(build({ [BROKER]: 'const OTHER = 9\n' })))

      expect(failures).toContain(
        `I5 the Heartbeat ceiling v2 is not at 'const CEILING = 2' in ${BROKER}`
      )
    })
  })

  describe('the repository', () => {
    const failures = []
    const reports = []
    let inventory

    beforeAll(() => {
      inventory = checkInventory(failures, reports)
    })

    test('passes every check: no gap, no stale record, no dangling import', () => {
      expect(failures).toEqual([])
    })

    test('protocol:check as a whole exits 0 and prints the census', () => {
      const log = jest.spyOn(console, 'log').mockImplementation(() => {})
      try {
        expect(require('../index').check({ json: true })).toBe(0)

        const printed = JSON.parse(log.mock.calls[0][0])
        expect(printed.ok).toBe(true)
        expect(printed.reports.join('\n')).toMatch(/I6 \d+ codec files/)
      } finally {
        log.mockRestore()
      }
    })

    test('lists every ceiling of the broker at the line it stands on today', () => {
      expect(
        CAPS.map(({ api, max, token }) => [api, max, lineOf(inventory.root, BROKER, token) > 0])
      ).toEqual(CAPS.map(({ api, max }) => [api, max, true]))
    })

    test('puts every codec file in exactly one class', () => {
      const count = (wire) => inventory.rows.filter((row) => row.wire === wire).length

      expect(count('reachable') + count('capped-only') + count('below-floor') + count('dead')).toBe(
        inventory.rows.length
      )
      expect(inventory.rows.length).toBe(discover().length)
    })

    test.each([
      ['produce', 13, 'request', 'reachable'],
      ['produce', 12, 'response', 'reachable'],
      ['produce', 11, 'request', 'capped-only'],
      ['produce', 10, 'request', 'dead'],
      ['fetch', 12, 'response', 'capped-only'],
      ['fetch', 18, 'response', 'reachable'],
      ['syncGroup', 4, 'request', 'capped-only'],
      ['endTxn', 4, 'response', 'capped-only'],
      ['joinGroup', 8, 'request', 'reachable'],
      ['metadata', 13, 'response', 'reachable'],
    ])('%s v%i %s is %s', (directory, version, direction, wire) => {
      const row = inventory.rows.find(
        (candidate) =>
          candidate.directory === directory &&
          candidate.version === version &&
          candidate.direction === direction
      )

      expect(row.wire).toBe(wire)
    })

    test('keeps the Metadata v0 parser that Metadata v12 imports, though no line selects v0', () => {
      const row = inventory.rows.find(
        (candidate) =>
          candidate.directory === 'metadata' &&
          candidate.version === 0 &&
          candidate.direction === 'response'
      )

      expect(row.wire).not.toBe('reachable')
      expect(row.importReachable).toBe(true)
      expect(inventory.deletable).not.toContain(row)
    })

    test('names the SaslHandshake v1 pair, which has a schema and a golden since the census found it had neither', () => {
      for (const direction of ['request', 'response']) {
        const row = inventory.rows.find(
          (candidate) =>
            candidate.directory === 'saslHandshake' &&
            candidate.version === 1 &&
            candidate.direction === direction
        )

        expect(row.hasSchema).toBe(true)
        expect(inventory.goldensOf(row)).not.toEqual([])
      }
    })
  })
})
