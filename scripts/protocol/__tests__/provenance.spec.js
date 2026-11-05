'use strict'

const fs = require('fs')
const os = require('os')
const path = require('path')
const trackedApis = require('../../compat/tracked-apis.json')
const { compute, render, readSources, checkProvenance, writeProvenance } = require('../provenance')
const { duplicateKeys } = require('../duplicateKeys')

const COMMITTED = path.join(__dirname, '..', 'provenance.json')
const COMMITTED_SOURCES = path.join(__dirname, '..', 'javaVectorSources.json')
const SHA256 = /^[0-9a-f]{64}$/
const JAR_4_4_0_SHA256 = '1ab07317eb6633241392449d329dbdc5d9251e6ffc4926860f42dd966e88bdf9'
const FIXTURES = 'src/protocol/requests/deleteGroups/fixtures/java'
const DELETE_GROUPS_V3 = [
  'deleteGroupsRequest-v3-basic',
  'deleteGroupsRequest-v3-empty',
  'deleteGroupsRequest-v3-nonAscii',
  'deleteGroupsResponse-v3-error',
  'deleteGroupsResponse-v3-mixedOutcomes',
  'deleteGroupsResponse-v3-noResults',
  'deleteGroupsResponse-v3-nonAscii',
  'deleteGroupsResponse-v3-nullEmptyNonempty',
  'deleteGroupsResponse-v3-ok',
  'deleteGroupsResponse-v3-unknownTags',
].map((stem) => `${FIXTURES}/${stem}.json`)
const DELETE_GROUPS_V2 = [
  'deleteGroupsRequest-v2-basic',
  'deleteGroupsResponse-v2-error',
  'deleteGroupsResponse-v2-ok',
].map((stem) => `${FIXTURES}/${stem}.json`)

describe('scripts/protocol > provenance', () => {
  let directory
  const copyOfCommitted = (change) => {
    const provenance = JSON.parse(fs.readFileSync(COMMITTED, 'utf8'))
    change(provenance)
    const file = path.join(directory, 'provenance.json')
    fs.writeFileSync(file, render(provenance))
    return file
  }
  const failuresFor = (file) => {
    const failures = []
    checkProvenance(failures, undefined, file)
    return failures
  }

  beforeEach(() => {
    directory = fs.mkdtempSync(path.join(os.tmpdir(), 'provenance-'))
  })

  afterEach(() => {
    fs.rmSync(directory, { recursive: true, force: true })
  })

  describe('what is recorded', () => {
    const provenance = compute()

    test('one entry per tracked API that has a request slot, by its Apache message name', () => {
      const slotted = trackedApis.tracked.filter(({ apiKeysName }) => apiKeysName !== null)

      expect(Object.keys(provenance)).toEqual(slotted.map(({ kafkaName }) => kafkaName).sort())
      expect(Object.keys(provenance)).toContain('FindCoordinator')
      expect(Object.keys(provenance)).not.toContain('GroupCoordinator')
      expect(Object.keys(provenance)).toEqual([...Object.keys(provenance)].sort())
    })

    test('the Apache tag of the snapshot and the SHA-256 of both message definitions', () => {
      for (const entry of Object.values(provenance)) {
        expect(entry.kafkaTag).toBe('4.4.0')
        expect(entry.messageSha256.Request).toMatch(SHA256)
        expect(entry.messageSha256.Response).toMatch(SHA256)
      }
    })

    test('the jar of the Java vectors for an API that has some, and none for one that has none', () => {
      expect(provenance.Metadata.javaClientsJar).toBe('4.3.1')
      expect(provenance.CreateTopics.javaClientsJar).toBe('4.3.1')
      expect(provenance.SaslHandshake.javaClientsJar).toBeNull()
      expect(provenance.SaslHandshake.javaVectors).toEqual([])
    })

    test('an API whose vectors came from two jars records no single jar and each vector its own', () => {
      expect(provenance.DeleteGroups.javaClientsJar).toBeNull()
      expect(
        provenance.DeleteGroups.javaVectors.map(({ file, javaClientsJar }) => [
          file,
          javaClientsJar,
        ])
      ).toEqual(
        [
          ...DELETE_GROUPS_V2.map((file) => [file, '4.3.1']),
          ...DELETE_GROUPS_V3.map((file) => [file, '4.4.0']),
        ].sort(([a], [b]) => (a < b ? -1 : 1))
      )
    })

    test('generate writes the text the check compares with', () => {
      const file = path.join(directory, 'written.json')

      expect(writeProvenance(file)).toBe('scripts/protocol/provenance.json')
      expect(fs.readFileSync(file, 'utf8')).toBe(render(provenance))
    })

    test('the committed file is what generate writes', () => {
      expect(fs.readFileSync(COMMITTED, 'utf8')).toBe(render(provenance))
      expect(failuresFor(COMMITTED)).toEqual([])
    })
  })

  describe('the check', () => {
    test('fails for an API whose message definition changed since the file was written', () => {
      const file = copyOfCommitted((p) => {
        p.Heartbeat.messageSha256.Response = '0'.repeat(64)
      })

      expect(failuresFor(file)).toEqual([
        'P2 scripts/protocol/provenance.json differs for Heartbeat - run `npm run protocol:generate`',
      ])
    })

    test('fails for an API that is missing from the file and for one the request table lacks', () => {
      const file = copyOfCommitted((p) => {
        delete p.Heartbeat
        p.Imaginary = p.Produce
      })

      expect(failuresFor(file)).toEqual([
        'P2 scripts/protocol/provenance.json differs for Heartbeat - run `npm run protocol:generate`',
        'P2 scripts/protocol/provenance.json differs for Imaginary - run `npm run protocol:generate`',
      ])
    })

    test('fails when the jar recorded for an API is not the one its vectors name', () => {
      const file = copyOfCommitted((p) => {
        p.Metadata.javaClientsJar = '4.2.2'
      })

      expect(failuresFor(file)).toHaveLength(1)
    })

    test('fails when the file is missing, and when its text is not the text generate writes', () => {
      expect(failuresFor(path.join(directory, 'absent.json'))).toEqual([
        'P2 scripts/protocol/provenance.json is missing - run `npm run protocol:generate`',
      ])
      const file = path.join(directory, 'compact.json')
      fs.writeFileSync(file, JSON.stringify(JSON.parse(fs.readFileSync(COMMITTED, 'utf8'))))

      expect(failuresFor(file)).toEqual([
        'P2 scripts/protocol/provenance.json is not in the form `npm run protocol:generate` writes',
      ])
    })
  })

  describe('the vector source records', () => {
    const OLD_JAR = { javaClientsJar: '4.3.1', javaClientsJarSha256: null }
    const NEW_JAR = { javaClientsJar: '4.4.0', javaClientsJarSha256: JAR_4_4_0_SHA256 }
    const sha256Of = (text) => require('crypto').createHash('sha256').update(text).digest('hex')

    /**
     * A repository root holding the message definitions of the named APIs, the given Java
     * vectors, and a source record for each vector under the jar it names.
     * @param {Record<string, { directory: string, jar: object, text?: string }>} vectors by file name
     * @param {string[]} apis the Apache message names that have a definition
     * @returns {string} the root
     */
    const makeRoot = (vectors, apis = ['Heartbeat', 'Metadata', 'Fetch']) => {
      const tag = JSON.parse(
        fs.readFileSync(path.join(__dirname, '..', '..', 'compat', 'config.json'))
      ).kafkaTag
      const messages = path.join(directory, 'scripts', 'compat', 'messages', tag)
      fs.mkdirSync(messages, { recursive: true })
      for (const api of apis) {
        fs.writeFileSync(path.join(messages, `${api}Request.json`), `{"name":"${api}Request"}`)
        fs.writeFileSync(path.join(messages, `${api}Response.json`), `{"name":"${api}Response"}`)
      }

      const records = {}
      for (const [name, { directory: api, jar, text = '{"hex":"00"}' }] of Object.entries(
        vectors
      )) {
        const relative = `src/protocol/requests/${api}/fixtures/java/${name}.json`
        fs.mkdirSync(path.dirname(path.join(directory, relative)), { recursive: true })
        fs.writeFileSync(path.join(directory, relative), text)
        records[relative] = { ...jar, fixtureSha256: sha256Of(text) }
      }
      writeSources(records)
      return directory
    }
    const writeSources = (records) => {
      fs.mkdirSync(path.join(directory, 'scripts', 'protocol'), { recursive: true })
      fs.writeFileSync(
        path.join(directory, 'scripts', 'protocol', 'javaVectorSources.json'),
        `${JSON.stringify(records, null, 2)}\n`
      )
    }
    const readRecords = () =>
      JSON.parse(
        fs.readFileSync(
          path.join(directory, 'scripts', 'protocol', 'javaVectorSources.json'),
          'utf8'
        )
      )
    const fixture = (api, name) => `src/protocol/requests/${api}/fixtures/java/${name}.json`

    test('an API whose vectors all came from the 4.3.1 jar records that jar and no hash for it', () => {
      const root = makeRoot({
        'heartbeatRequest-v4-basic': { directory: 'heartbeat', jar: OLD_JAR },
      })

      expect(readSources(root).problems).toEqual([])
      expect(compute(root).Heartbeat).toMatchObject({
        javaClientsJar: '4.3.1',
        javaVectors: [
          {
            file: fixture('heartbeat', 'heartbeatRequest-v4-basic'),
            javaClientsJar: '4.3.1',
            javaClientsJarSha256: null,
            fixtureSha256: sha256Of('{"hex":"00"}'),
          },
        ],
      })
    })

    test('an API whose vectors all came from the 4.4.0 jar records that jar', () => {
      const root = makeRoot({
        'metadataRequest-v12-basic': { directory: 'metadata', jar: NEW_JAR },
      })

      expect(compute(root).Metadata.javaClientsJar).toBe('4.4.0')
    })

    test('an API with vectors from both jars records no single jar and keeps each vector sorted by file', () => {
      const root = makeRoot({
        'metadataRequest-v12-new': { directory: 'metadata', jar: NEW_JAR },
        'metadataRequest-v12-old': { directory: 'metadata', jar: OLD_JAR },
        'metadataRequest-v1-old': { directory: 'metadata', jar: OLD_JAR },
      })
      const { Metadata } = compute(root)

      expect(Metadata.javaClientsJar).toBeNull()
      expect(
        Metadata.javaVectors.map(({ file, javaClientsJar }) => [
          path.basename(file),
          javaClientsJar,
        ])
      ).toEqual([
        ['metadataRequest-v1-old.json', '4.3.1'],
        ['metadataRequest-v12-new.json', '4.4.0'],
        ['metadataRequest-v12-old.json', '4.3.1'],
      ])
    })

    test('an API with no vector records no jar and an empty vector list', () => {
      const root = makeRoot({
        'heartbeatRequest-v4-basic': { directory: 'heartbeat', jar: OLD_JAR },
      })

      expect(compute(root).Fetch).toMatchObject({ javaClientsJar: null, javaVectors: [] })
    })

    test('a global generator pin does not reattribute a vector an older jar wrote', () => {
      const root = makeRoot({
        'heartbeatRequest-v4-basic': { directory: 'heartbeat', jar: OLD_JAR },
      })
      const scripts = path.join(root, 'scripts', 'protocol', 'javaVectors')
      fs.mkdirSync(scripts, { recursive: true })
      for (const name of ['run.sh', 'runAssignors.sh']) {
        fs.writeFileSync(
          path.join(scripts, name),
          `KAFKA_CLIENTS_VERSION="\${KAFKA_CLIENTS_VERSION:-4.4.0}"\n`
        )
      }

      expect(compute(root).Heartbeat.javaClientsJar).toBe('4.3.1')
    })

    test('a 4.4.0 vector without the SHA-256 of its jar is refused, a 4.3.1 one without it is not', () => {
      const root = makeRoot({
        'heartbeatRequest-v4-basic': { directory: 'heartbeat', jar: OLD_JAR },
        'metadataRequest-v12-basic': {
          directory: 'metadata',
          jar: { javaClientsJar: '4.4.0', javaClientsJarSha256: null },
        },
      })

      expect(readSources(root).problems).toEqual([
        `source record for ${fixture('metadata', 'metadataRequest-v12-basic')} names no SHA-256 of its 4.4.0 jar`,
      ])
      expect(() => compute(root)).toThrow('names no SHA-256 of its 4.4.0 jar')
    })

    test('a malformed jar hash, no jar version and two hashes for one jar are refused', () => {
      const root = makeRoot({
        'heartbeatRequest-v4-basic': {
          directory: 'heartbeat',
          jar: { ...NEW_JAR, javaClientsJarSha256: 'abc' },
        },
        'metadataRequest-v12-a': {
          directory: 'metadata',
          jar: { ...NEW_JAR, javaClientsJarSha256: '1'.repeat(64) },
        },
        'metadataRequest-v12-b': {
          directory: 'metadata',
          jar: { ...NEW_JAR, javaClientsJarSha256: '2'.repeat(64) },
        },
        'metadataRequest-v12-c': {
          directory: 'metadata',
          jar: { javaClientsJar: 'latest', javaClientsJarSha256: null },
        },
      })

      expect(readSources(root).problems).toEqual([
        `source record for ${fixture('heartbeat', 'heartbeatRequest-v4-basic')} has a malformed jar SHA-256`,
        'source records name two SHA-256 values for the 4.4.0 jar',
        `source record for ${fixture('metadata', 'metadataRequest-v12-c')} names no jar version`,
      ])
    })

    test('a fixture changed after it was recorded is a stale record, reported by the check', () => {
      const root = makeRoot({
        'heartbeatRequest-v4-basic': { directory: 'heartbeat', jar: OLD_JAR },
      })
      fs.writeFileSync(
        path.join(root, fixture('heartbeat', 'heartbeatRequest-v4-basic')),
        '{"hex":"01"}'
      )
      const failures = []

      checkProvenance(failures, root, path.join(directory, 'none.json'))

      expect(failures).toEqual([
        `P3 source record for ${fixture('heartbeat', 'heartbeatRequest-v4-basic')} holds a stale fixture SHA-256`,
      ])
    })

    test('a vector with no record, a record with no vector and a record written twice are refused', () => {
      const root = makeRoot({
        'heartbeatRequest-v4-basic': { directory: 'heartbeat', jar: OLD_JAR },
        'heartbeatRequest-v4-other': { directory: 'heartbeat', jar: OLD_JAR },
      })
      const records = readRecords()
      const kept = fixture('heartbeat', 'heartbeatRequest-v4-basic')
      const removed = fixture('heartbeat', 'heartbeatRequest-v4-other')
      const extra = fixture('heartbeat', 'heartbeatRequest-v9-ghost')
      const body = JSON.stringify(records[kept], null, 2).replace(/\n/g, '\n  ')
      const duplicated = `{\n  "${kept}": ${body},\n  "${kept}": ${body},\n  "${extra}": ${body}\n}\n`
      fs.writeFileSync(path.join(root, 'scripts', 'protocol', 'javaVectorSources.json'), duplicated)

      expect(readSources(root).problems).toEqual([
        `duplicate source record for ${kept}`,
        `no source record for ${removed}`,
        `source record for ${extra} names no Java vector`,
      ])
    })

    describe('a record written twice', () => {
      const FILE = fixture('heartbeat', 'heartbeatRequest-v4-basic')

      /**
       * The sources file with the record of `FILE` as two members, the second under the key
       * `spelling` (which decodes to the same key), laid out with `indent`.
       */
      const writeTwice = (indent, spelling = FILE) => {
        const root = makeRoot({
          'heartbeatRequest-v4-basic': { directory: 'heartbeat', jar: OLD_JAR },
        })
        const member = JSON.stringify({ [FILE]: readRecords()[FILE] }, null, indent).slice(1, -1)
        const second = member.replace(`"${FILE}"`, `"${spelling}"`)
        fs.writeFileSync(
          path.join(root, 'scripts', 'protocol', 'javaVectorSources.json'),
          `{${member},${second}}`
        )
        return root
      }

      test.each([
        ['minified', undefined],
        ['tab-indented', '\t'],
        ['four-space indented', 4],
      ])('is found in %s JSON', (_layout, indent) => {
        expect(readSources(writeTwice(indent)).problems).toEqual([
          `duplicate source record for ${FILE}`,
        ])
      })

      test.each([
        ['an escaped slash', FILE.replaceAll('/', '\\/')],
        ['a unicode-escaped slash', FILE.replaceAll('/', '\\u002f')],
      ])('is found when the second key spells the path with %s', (_spelling, key) => {
        expect(readSources(writeTwice(2, key)).problems).toEqual([
          `duplicate source record for ${FILE}`,
        ])
      })

      test('is found inside a record, naming the record it sits in', () => {
        const root = makeRoot({
          'heartbeatRequest-v4-basic': { directory: 'heartbeat', jar: OLD_JAR },
        })
        const text = fs
          .readFileSync(path.join(root, 'scripts', 'protocol', 'javaVectorSources.json'), 'utf8')
          .replace(
            '"javaClientsJar": "4.3.1",',
            '"javaClientsJar": "4.3.1", "javaClientsJar": "4.3.1",'
          )
        fs.writeFileSync(path.join(root, 'scripts', 'protocol', 'javaVectorSources.json'), text)

        expect(readSources(root).problems).toEqual([
          `duplicate key javaClientsJar in the source record for ${FILE}`,
        ])
      })
    })

    test('a missing sources file is refused, by generate and by the check', () => {
      const root = makeRoot({
        'heartbeatRequest-v4-basic': { directory: 'heartbeat', jar: OLD_JAR },
      })
      fs.rmSync(path.join(root, 'scripts', 'protocol', 'javaVectorSources.json'))
      const failures = []

      checkProvenance(failures, root, path.join(directory, 'none.json'))

      expect(failures).toEqual(['P3 scripts/protocol/javaVectorSources.json is missing'])
      expect(() => compute(root)).toThrow('scripts/protocol/javaVectorSources.json is missing')
    })

    test('the committed records pin the retained DeleteGroups v2 vectors to 4.3.1 and the ten v3 ones to the 4.4.0 jar', () => {
      const records = JSON.parse(fs.readFileSync(COMMITTED_SOURCES, 'utf8'))

      for (const file of DELETE_GROUPS_V2) {
        expect(records[file]).toMatchObject({ javaClientsJar: '4.3.1', javaClientsJarSha256: null })
      }
      for (const file of DELETE_GROUPS_V3) {
        expect(records[file]).toMatchObject({
          javaClientsJar: '4.4.0',
          javaClientsJarSha256: JAR_4_4_0_SHA256,
        })
      }
    })

    test('every committed record names the 4.3.1 jar without a hash or the 4.4.0 jar with the verified one', () => {
      const records = Object.values(JSON.parse(fs.readFileSync(COMMITTED_SOURCES, 'utf8')))

      expect(
        records.filter(
          ({ javaClientsJar, javaClientsJarSha256 }) =>
            !(
              (javaClientsJar === '4.3.1' && javaClientsJarSha256 === null) ||
              (javaClientsJar === '4.4.0' && javaClientsJarSha256 === JAR_4_4_0_SHA256)
            )
        )
      ).toEqual([])
      expect(readSources(path.join(__dirname, '..', '..', '..')).problems).toEqual([])
    })
  })
})

describe('scripts/protocol > duplicateKeys', () => {
  test('braces, commas, colons and escaped quotes inside strings are not structure, and a repeat after them is still found', () => {
    const text = String.raw`{"k{,}:\"[":"v,{[:\"","e":"q:\\","a":1,"a":2}`

    expect(duplicateKeys(text)).toEqual([{ record: null, key: 'a' }])
  })

  test('a string equal to a key, as a value or an array item, is not a key', () => {
    expect(duplicateKeys('{"a":"a","b":["a","a"],"c":{"a":"a"}}')).toEqual([])
  })

  test('a repeat is found in an object at any depth and names the top-level record it sits in', () => {
    expect(duplicateKeys('{"r":{"x":1,"x":2},"s":{"x":1},"t":[{"y":1,"y":2}]}')).toEqual([
      { record: 'r', key: 'x' },
      { record: 't', key: 'y' },
    ])
  })

  test('a repeat of a top-level key has no record, and the same key one level down is not a repeat', () => {
    expect(duplicateKeys('{"a":1,"b":{"a":2},"a":3,"a":4}')).toEqual([
      { record: null, key: 'a' },
      { record: null, key: 'a' },
    ])
  })

  test('keys are compared decoded, whatever the escaping or the whitespace', () => {
    const text = String.raw`{ "a/b" : 1 ,
	"a\/b":2,"a\u002fb":3}`

    expect(duplicateKeys(text)).toEqual([
      { record: null, key: 'a/b' },
      { record: null, key: 'a/b' },
    ])
  })
})
