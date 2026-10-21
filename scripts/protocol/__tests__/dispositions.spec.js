'use strict'

const fs = require('fs')
const os = require('os')
const path = require('path')
const { discoverFixtures, setOf, bindingOf } = require('../fixtureFamilies')
const { FixtureDispositions, validateCaptureRoutes } = require('../dispositions')

const FIXTURES = 'src/protocol/requests/heartbeat/fixtures'
const SPEC = 'heartbeat/v4/request.spec.js'
const SPEC_TEXT =
  "const v = require('../fixtures/java/heartbeatRequest-v4-basic.json')\ntest('encodes the vector', () => {})\n"

const tree = {
  [`${FIXTURES}/java/heartbeatRequest-v4-basic.json`]: JSON.stringify({
    message: 'HeartbeatRequest',
    version: 4,
    hex: '0102',
    fields: {},
  }),
  [`${FIXTURES}/v4_response_java.json`]: JSON.stringify({ type: 'Buffer', data: [0, 0, 0, 0] }),
  [`${FIXTURES}/v4_response_kafka431.json`]: JSON.stringify({
    type: 'Buffer',
    data: [0, 0, 0, 0, 0],
  }),
  [`${FIXTURES}/v2_request.json`]: JSON.stringify({ type: 'Buffer', data: [1] }),
  [`src/protocol/requests/${SPEC}`]: SPEC_TEXT,
}

const golden = { direction: 'request', versions: [4], spec: SPEC, test: 'encodes the vector' }
const entries = () => [
  {
    family: 'heartbeat/java',
    route: 'a generator',
    evidenceKind: 'java-vector',
    disposition: 'retain',
    reason: 'a reason',
    goldens: [golden],
  },
  {
    family: 'heartbeat/javaBuffer',
    route: 'a generator',
    evidenceKind: 'java-vector',
    disposition: 'retain',
    reason: 'a reason',
  },
  {
    family: 'heartbeat/capture',
    route: 'a capture',
    evidenceKind: 'capture',
    disposition: 'retain',
    reason: 'a reason',
  },
  {
    family: 'heartbeat/hand',
    route: 'by hand',
    evidenceKind: 'hand',
    disposition: 'retain',
    reason: 'a reason',
  },
]

describe('scripts/protocol > the fixture records', () => {
  let roots = []
  const build = (files = tree) => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), 'dispositions-'))
    roots.push(root)
    for (const [file, text] of Object.entries(files)) {
      fs.mkdirSync(path.dirname(path.join(root, file)), { recursive: true })
      fs.writeFileSync(path.join(root, file), text)
    }
    return root
  }
  const dispositionsOf = (list, files) => {
    const root = build(files)
    return new FixtureDispositions({ root, fixtures: discoverFixtures(root), entries: list })
  }
  const failuresOf = (list, files) => {
    const failures = []
    dispositionsOf(list, files).validate(failures)
    return failures
  }

  afterEach(() => {
    for (const root of roots) fs.rmSync(root, { recursive: true, force: true })
    roots = []
  })

  describe('finding and binding the fixtures', () => {
    test.each([
      ['java/heartbeatRequest-v4-basic.json', 'java'],
      ['v4_response_java.json', 'javaBuffer'],
      ['v4_response_kafka431.json', 'capture'],
      ['v4_response_kafka412.json', 'capture'],
      ['v2_request.json', 'hand'],
      ['v0_request.json', 'hand'],
    ])('%s is in the %s family', (rel, set) => {
      expect(setOf(rel)).toBe(set)
    })

    test('a structured vector binds by the message and version it names, any other by its file name', () => {
      expect(bindingOf('java/x.json', { message: 'HeartbeatRequest', version: 4 })).toEqual({
        version: 4,
        direction: 'request',
      })
      expect(bindingOf('java/x.json', { message: 'HeartbeatResponse', version: 4 })).toEqual({
        version: 4,
        direction: 'response',
      })
      expect(bindingOf('v12_response_kafka412.json', {})).toEqual({
        version: 12,
        direction: 'response',
      })
      expect(bindingOf('v5_offline_replicas_response.json', {})).toEqual({
        version: 5,
        direction: 'response',
      })
      expect(bindingOf('plain_bytes.json', {})).toBeNull()
    })

    test('holds the bytes of a vector whichever way it holds them', () => {
      const root = build()
      const byFile = Object.fromEntries(
        discoverFixtures(root).map((fixture) => [fixture.rel, fixture.bytes])
      )

      expect(byFile['java/heartbeatRequest-v4-basic.json']).toEqual(Buffer.from([1, 2]))
      expect(byFile['v4_response_java.json']).toEqual(Buffer.from([0, 0, 0, 0]))
    })
  })

  describe('the dispositions', () => {
    test('a complete set has no failure', () => {
      expect(failuresOf(entries())).toEqual([])
    })

    test('a family on disk with no entry fails F1', () => {
      expect(failuresOf(entries().slice(0, 3))).toEqual([
        'F1 fixtureDispositions.json has no entry for the fixture family heartbeat/hand',
      ])
    })

    test('an entry for a family with no file fails F1, and an entry twice fails too', () => {
      const failures = failuresOf([
        ...entries(),
        { ...entries()[3], family: 'fetch/hand' },
        entries()[1],
      ])

      expect(failures).toEqual([
        'F1 fixtureDispositions.json heartbeat/javaBuffer appears twice',
        'F1 fixtureDispositions.json fetch/hand names a family with no fixture on disk',
      ])
    })

    test.each(['route', 'evidenceKind', 'disposition', 'reason'])(
      'an entry without %s fails F2',
      (field) => {
        const list = entries()
        list[1][field] = ''

        expect(failuresOf(list).join('\n')).toContain(
          `F2 fixtureDispositions.json heartbeat/javaBuffer has no ${field}`
        )
      }
    )

    test('an unknown disposition fails F2', () => {
      const list = entries()
      list[1].disposition = 'keep'

      expect(failuresOf(list).join('\n')).toContain(
        "disposition 'keep' is not one of retain, replace, remove"
      )
    })

    test('a Java family that claims to be a capture fails F2', () => {
      const list = entries()
      list[0].evidenceKind = 'capture'

      expect(failuresOf(list).join('\n')).toContain("evidenceKind 'capture' is not 'java-vector'")
    })

    test('a removed fixture names its file, and fails F5 while the file is still there', () => {
      const removed = {
        family: 'recordBatch/zstd',
        route: 'unknown',
        evidenceKind: 'hand',
        disposition: 'remove',
        reason: 'unused',
        removed: 'src/old.json',
      }

      expect(failuresOf([...entries(), removed])).toEqual([])
      expect(failuresOf([...entries(), removed], { ...tree, 'src/old.json': '{}' })).toEqual([
        "F5 fixtureDispositions.json recordBatch/zstd is dispositioned 'remove' but src/old.json is still on disk",
      ])
      expect(failuresOf([...entries(), { ...removed, removed: undefined }]).join('\n')).toContain(
        "is removed and names no file in 'removed'"
      )
    })
  })

  describe('the golden assertions', () => {
    const withGolden = (change) => {
      const list = entries()
      list[0].goldens = [{ ...golden, ...change }]
      return list
    }

    test('one that names a spec, a test of it and a fixture of its family holds', () => {
      expect(failuresOf(withGolden({}))).toEqual([])
    })

    test("a golden of one version without a spec names the spec beside that version's codec", () => {
      const list = withGolden({})
      delete list[0].goldens[0].spec

      expect(failuresOf(list)).toEqual([])
      expect(dispositionsOf(list).goldensFor('heartbeat', 4, 'request')[0].spec).toBe(SPEC)
    })

    test('a golden of several versions without a spec fails F3', () => {
      const list = withGolden({ versions: [4, 5] })
      delete list[0].goldens[0].spec

      expect(failuresOf(list).join('\n')).toContain('has several versions and names no spec')
    })

    test('a spec that does not exist fails F3', () => {
      expect(failuresOf(withGolden({ spec: 'src/none.spec.js' })).join('\n')).toContain(
        'src/none.spec.js does not exist'
      )
    })

    test('a test the spec does not have fails F3: deleting the assertion is caught', () => {
      expect(failuresOf(withGolden({ test: 'encodes the old vector' })).join('\n')).toContain(
        `has no test named 'encodes the old vector'`
      )
    })

    test('a spec that reads no fixture fails F3', () => {
      const files = {
        ...tree,
        [`src/protocol/requests/${SPEC}`]: "test('encodes the vector', () => {})\n",
      }

      expect(failuresOf(entries(), files).join('\n')).toContain(`${SPEC} reads no fixture`)
    })

    test('a version the family has no fixture for in that direction fails F3', () => {
      expect(failuresOf(withGolden({ versions: [4, 5] })).join('\n')).toContain(
        'the family has no request fixture for v5'
      )
      expect(failuresOf(withGolden({ direction: 'response' })).join('\n')).toContain(
        'the family has no response fixture for v4'
      )
    })

    test('an entry with no direction or no versions fails F3', () => {
      expect(failuresOf(withGolden({ versions: [] })).join('\n')).toContain(
        'needs a direction and a list of versions'
      )
      expect(failuresOf(withGolden({ direction: 'up' })).join('\n')).toContain(
        'needs a direction and a list of versions'
      )
    })

    test('goldensFor returns the assertions of retained families for a version and direction, with their fixtures', () => {
      const dispositions = dispositionsOf(entries())

      expect(dispositions.goldensFor('heartbeat', 4, 'request')).toEqual([
        {
          family: 'heartbeat/java',
          spec: SPEC,
          test: 'encodes the vector',
          fixtures: [expect.objectContaining({ rel: 'java/heartbeatRequest-v4-basic.json' })],
        },
      ])
      expect(dispositions.goldensFor('heartbeat', 4, 'response')).toEqual([])
      expect(dispositions.goldensFor('heartbeat', 3, 'request')).toEqual([])
      expect(dispositions.goldensFor('fetch', 4, 'request')).toEqual([])
    })

    test('a family that is not retained is evidence for nothing', () => {
      const list = entries()
      list[0].disposition = 'replace'

      expect(dispositionsOf(list).goldensFor('heartbeat', 4, 'request')).toEqual([])
    })
  })

  describe('the capture routes', () => {
    const route = (change = {}) => ({
      api: 'heartbeat',
      version: 4,
      direction: 'response',
      route: 'capture.js',
      evidenceKind: 'capture',
      disposition: 'captured',
      reason: 'recorded',
      ...change,
    })
    const captureList = [{ dir: 'heartbeat', apiVersion: 4, directions: ['response'] }]
    const failuresFor = (list, list2 = captureList) => {
      const failures = []
      validateCaptureRoutes(
        { entries: list, captureList: list2, fixtures: discoverFixtures(build()) },
        failures
      )
      return failures
    }

    test('a route for every row of the capture list, with a fixture on disk, has no failure', () => {
      expect(failuresFor([route()])).toEqual([])
    })

    test('a row of the capture list with no entry fails F4', () => {
      expect(failuresFor([])).toEqual([
        'F4 captureRoutes.json has no entry for heartbeat v4 response, which capture.js records',
      ])
    })

    test('an entry that is not a row of the list fails F4', () => {
      expect(
        failuresFor([route(), route({ version: 2, direction: 'request', decodeSide: 'walk' })])
      ).toEqual(['F4 captureRoutes.json heartbeat v2 request is not a row of capture.js'])
    })

    test('a captured message with no fixture on disk fails F4', () => {
      expect(
        failuresFor(
          [route({ version: 9 })],
          [{ dir: 'heartbeat', apiVersion: 9, directions: ['response'] }]
        )
      ).toEqual(['F4 captureRoutes.json heartbeat v9 response has no fixture on disk'])
    })

    test('a request entry without a decode-side check fails F4', () => {
      const list = [{ dir: 'heartbeat', apiVersion: 4, directions: ['request'] }]

      expect(failuresFor([route({ direction: 'request' })], list)).toEqual([
        'F4 captureRoutes.json heartbeat v4 request is a request and names no decodeSide check',
      ])
      expect(failuresFor([route({ direction: 'request', decodeSide: 'walk' })], list)).toEqual([])
    })

    test('a capture with no route is allowed with a reason, and fails without one', () => {
      const none = route({
        api: 'offsetForLeaderEpoch',
        version: 4,
        direction: 'request',
        route: 'none',
        disposition: 'no-route',
        decodeSide: 'walk',
      })

      expect(failuresFor([route(), none])).toEqual([])
      expect(failuresFor([route(), { ...none, reason: '' }])).toEqual([
        'F4 captureRoutes.json offsetForLeaderEpoch v4 request has no reason',
      ])
    })

    test('a disposition other than captured or no-route fails F4', () => {
      expect(failuresFor([route({ disposition: 'maybe' })])).toEqual(
        expect.arrayContaining([
          expect.stringContaining("disposition 'maybe' is not captured or no-route"),
        ])
      )
    })
  })
})
