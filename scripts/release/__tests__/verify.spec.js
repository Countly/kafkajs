const { execFileSync, spawnSync } = require('node:child_process')
const crypto = require('node:crypto')
const fs = require('node:fs')
const os = require('node:os')
const path = require('node:path')
const {
  verifyRelease,
  distTagOf,
  createListRuns,
  createListJobs,
  createRegistry,
  verifyOtelManifest,
  verifyMemoryEvidence,
  ReleaseVerificationError,
  main,
} = require('../verify')

const SHA = 'a'.repeat(40)
const OTHER_SHA = 'b'.repeat(40)
const VERSION = '3.0.0-rc.1'
const TAG = `v${VERSION}`
const CHANGELOG = `# Changelog\n\n## Unreleased — 3.0.0\n\n## ${VERSION} — 2026-11-20\n`
const TOKEN = 'ghs_never_print_this_token'
const REPOSITORY = 'Countly/kafkajs'
const MAIN_NAME = '@countly/kafkajs'
const OTEL_NAME = '@countly/opentelemetry-instrumentation-kafkajs'
const OTEL_VERSION = '0.1.1'
const PEER_RANGE = '>=3.0.0-0 <4'
const FINAL = { tag: 'v3.0.0', mainVersion: '3.0.0' }

const otelManifest = (overrides = {}) => ({
  name: OTEL_NAME,
  version: OTEL_VERSION,
  peerDependencies: { [MAIN_NAME]: PEER_RANGE, '@opentelemetry/api': '^1.3.0' },
  ...overrides,
})
const sriOf = (bytes) => `sha512-${crypto.createHash('sha512').update(bytes).digest('base64')}`

/** A registry lookup that answers `published` for every version and records its calls. */
const fakeLookup = (published = false) => {
  const calls = []
  const lookup = async (name, version) => {
    calls.push([name, version])
    return published
  }
  lookup.calls = calls
  return lookup
}

let nextId = 1
const run = (overrides = {}) => ({
  id: nextId++,
  event: 'push',
  head_branch: 'master',
  head_sha: SHA,
  status: 'completed',
  conclusion: 'success',
  created_at: '2026-11-10T10:00:00Z',
  ...overrides,
})
const runs = (count, overrides) => Array.from({ length: count }, () => run(overrides))
const at = (hour, overrides = {}) =>
  run({ created_at: `2026-11-10T${String(hour).padStart(2, '0')}:00:00Z`, ...overrides })

const FIELD = { event: 'event', branch: 'head_branch', head_sha: 'head_sha' }

/** A `listRuns` that filters the fixtures by the query keys, as GitHub does, and records its calls. */
const fakeListRuns = (byWorkflow) => {
  const calls = []
  const listRuns = async (workflow, query) => {
    calls.push([workflow, query])
    return (byWorkflow[workflow] ?? []).filter((r) =>
      Object.entries(query).every(([key, value]) => r[FIELD[key]] === value)
    )
  }
  listRuns.calls = calls
  return listRuns
}

const green = () => ({
  'ci.yml': [run()],
  'gate.yml': [run()],
  'weekly.yml': [run({ event: 'workflow_dispatch' })],
})

const MEMORY = 'memory-gates.yml'
const LONG_WINDOW_GROUPS = [22, 24, 26].flatMap((node) =>
  ['4.2.2', '4.3.1'].flatMap((kafka) =>
    ['unseeded', 'seeded'].map((arm) => `${node}, ${kafka}, ${arm}`)
  )
)

/** The latest-attempt jobs of a memory-gates run: a full three-replicate selection unless overridden. */
const memoryJobs = ({ replicates = [1, 2, 3], verdict = 'success', extra = [] } = {}) => [
  { name: 'select', conclusion: 'success' },
  { name: 'share-soak-controls', conclusion: 'success' },
  { name: 'share-soak-verdict', conclusion: 'success' },
  ...LONG_WINDOW_GROUPS.flatMap((group) =>
    replicates.map((r) => ({
      name: `telemetry-long-window (${group}, ${r})`,
      conclusion: 'success',
    }))
  ),
  ...[22, 24, 26].map((node) => ({
    name: `telemetry-long-window (${node}, 4.3.1, warmup-seed, 1)`,
    conclusion: 'success',
  })),
  { name: 'telemetry-long-window-verdict', conclusion: 'success' },
  { name: 'telemetry-seeded-control', conclusion: 'skipped' },
  ...extra,
  { name: 'memory-gates-verdict', conclusion: verdict },
]
const memoryRun = (overrides = {}) => run({ event: 'workflow_dispatch', ...overrides })

/** A `listJobs` over `{ [runId]: jobs }` that records the run ids it is asked for. */
const fakeListJobs = (byRun) => {
  const calls = []
  const listJobs = async (runId) => {
    calls.push(runId)
    return byRun[runId] ?? []
  }
  listJobs.calls = calls
  return listJobs
}

/** `listRuns`/`listJobs` over memory-gates runs, each paired with its jobs (a full set by default). */
const memorySeams = (...entries) => {
  const pairs = entries.map((entry) => (Array.isArray(entry) ? entry : [entry, memoryJobs()]))
  return {
    listRuns: fakeListRuns({ [MEMORY]: pairs.map(([r]) => r) }),
    listJobs: fakeListJobs(Object.fromEntries(pairs.map(([r, jobs]) => [r.id, jobs]))),
  }
}

const checkMemory = (seams) => verifyMemoryEvidence({ sha: SHA, ...seams })

const verify = (overrides = {}) =>
  verifyRelease({
    tag: TAG,
    sha: SHA,
    packageJson: { version: VERSION },
    changelog: CHANGELOG,
    isOnMaster: () => true,
    listRuns: fakeListRuns(green()),
    ...overrides,
  })

const verifyWith = (byWorkflow, overrides) =>
  verify({ listRuns: fakeListRuns({ ...green(), ...byWorkflow }), ...overrides })

const expectRule = async (promise, rule, text = /./) => {
  const error = await promise.then(
    () => null,
    (e) => e
  )
  expect(error).toBeInstanceOf(ReleaseVerificationError)
  expect(error.message).toMatch(new RegExp(`^rule ${rule}: `))
  expect(error.message).toMatch(text)
}

/** A `fetch` serving `pagesByWorkflow[workflow][page - 1]` as GitHub's run list and recording requests. */
const stubFetch = (pagesByWorkflow, { status = 200, body } = {}) => {
  const requests = []
  const fetch = async (url, init) => {
    const parsed = new URL(String(url))
    requests.push({ url: parsed, init })
    const workflow = parsed.pathname.split('/').at(-2)
    const page = Number(parsed.searchParams.get('page'))
    const list = (pagesByWorkflow[workflow] ?? [])[page - 1] ?? []
    return new Response(JSON.stringify(body ?? { total_count: list.length, workflow_runs: list }), {
      status,
    })
  }
  fetch.requests = requests
  return fetch
}

const listRunsOver = (fetch) => createListRuns({ repository: REPOSITORY, token: TOKEN, fetch })

describe('verifyRelease', () => {
  describe('rule 1: the tag and the version', () => {
    test('rejects a tag that is not v plus the package version', async () => {
      await expectRule(verify({ tag: 'v3.0.0-rc.2' }), 1)
      await expectRule(verify({ tag: VERSION }), 1)
    })

    test.each(['3.0', '3.0.0+build', 'v3.0.0'])(
      'rejects a version that is not X.Y.Z or X.Y.Z-pre: %p',
      async (version) => {
        await expectRule(verify({ tag: `v${version}`, packageJson: { version } }), 1)
      }
    )

    test('rejects a package.json without a version', async () => {
      await expectRule(verify({ tag: 'vundefined', packageJson: {} }), 1)
    })
  })

  describe('rule 2: the tagged commit', () => {
    test('rejects a commit that is not on master', async () => {
      const isOnMaster = jest.fn(() => false)
      await expectRule(verify({ isOnMaster }), 2)
      expect(isOnMaster).toHaveBeenCalledWith(SHA)
    })

    test('reports the first failing rule when several fail', async () => {
      await expectRule(verify({ tag: 'v9.9.9', isOnMaster: () => false, changelog: '' }), 1)
      await expectRule(verify({ isOnMaster: () => false, changelog: '' }), 2)
      await expectRule(verify({ changelog: '' }), 3)
    })
  })

  describe('rule 3: the CHANGELOG heading', () => {
    test('rejects a missing CHANGELOG heading', async () => {
      await expectRule(verify({ changelog: '# Changelog\n\n## Unreleased — 3.0.0\n' }), 3)
    })

    test('does not accept a longer version heading for a shorter version', async () => {
      const changelog = '# Changelog\n\n## 3.0.0-rc.1 — 2026-11-20\n\n## 3.0.0.1\n'
      await expectRule(verify({ tag: 'v3.0.0', packageJson: { version: '3.0.0' }, changelog }), 3)
    })

    test('treats the dots in a version literally', async () => {
      await expectRule(
        verify({ tag: 'v3.0.0', packageJson: { version: '3.0.0' }, changelog: '## 3x0y0\n' }),
        3
      )
    })

    test.each(['### 3.0.0', ' ## 3.0.0', '3.0.0', '# 3.0.0'])(
      'only a level-two heading counts: %p',
      async (heading) => {
        await expectRule(
          verify({ tag: 'v3.0.0', packageJson: { version: '3.0.0' }, changelog: `${heading}\n` }),
          3
        )
      }
    )

    test.each(['## 3.0.0', '## 3.0.0 — 2026-12-01', '## 3.0.0\r'])(
      'accepts the heading %p, with or without a date suffix',
      async (heading) => {
        const changelog = `# Changelog\n${heading}\n`
        await expect(
          verify({ tag: 'v3.0.0', packageJson: { version: '3.0.0' }, changelog })
        ).resolves.toEqual({ version: '3.0.0', distTag: 'latest' })
      }
    )
  })

  describe('rule 4: ci.yml and gate.yml push runs', () => {
    test.each(['ci.yml', 'gate.yml'])('rejects a missing %s run', async (workflow) => {
      await expectRule(verifyWith({ [workflow]: [] }), 4, new RegExp(workflow.replace('.', '\\.')))
    })

    test('rejects a dispatch-only success', async () => {
      await expectRule(verifyWith({ 'ci.yml': [run({ event: 'workflow_dispatch' })] }), 4)
      await expectRule(verifyWith({ 'gate.yml': [run({ event: 'pull_request' })] }), 4)
    })

    test('rejects a push run for another SHA', async () => {
      await expectRule(verifyWith({ 'ci.yml': [run({ head_sha: OTHER_SHA })] }), 4)
    })

    test('rejects a push run on another branch', async () => {
      await expectRule(verifyWith({ 'gate.yml': [run({ head_branch: 'p10/int' })] }), 4)
    })

    test.each([
      ['in_progress', null],
      ['queued', null],
    ])('rejects a pending run (%s)', async (status, conclusion) => {
      await expectRule(verifyWith({ 'ci.yml': [run({ status, conclusion })] }), 4)
    })

    test.each(['failure', 'cancelled', 'timed_out', 'skipped', 'neutral'])(
      'rejects a completed run that is %s',
      async (conclusion) => {
        await expectRule(verifyWith({ 'gate.yml': [run({ conclusion })] }), 4)
      }
    )

    test('rejects when the newest run failed even if an older one succeeded', async () => {
      const older = at(9)
      const newer = at(11, { conclusion: 'failure' })
      await expectRule(verifyWith({ 'ci.yml': [older, newer] }), 4)
      await expectRule(verifyWith({ 'ci.yml': [newer, older] }), 4)
    })

    test('picks the newest run by created_at, not by list order', async () => {
      const older = at(9, { conclusion: 'failure' })
      const newer = at(11)
      await expect(verifyWith({ 'gate.yml': [older, newer] })).resolves.toBeDefined()
      await expect(verifyWith({ 'gate.yml': [newer, older] })).resolves.toBeDefined()
    })

    test('breaks a created_at tie by the higher run id', async () => {
      const failed = run({ id: 2, conclusion: 'failure' })
      const passed = run({ id: 1 })
      await expectRule(verifyWith({ 'ci.yml': [passed, failed] }), 4)
      await expect(
        verifyWith({ 'ci.yml': [run({ id: 3, conclusion: 'failure' }), run({ id: 4 })] })
      ).resolves.toBeDefined()
    })

    test('rejects a run whose created_at cannot be read', async () => {
      await expectRule(verifyWith({ 'ci.yml': [run({ created_at: 'soon' })] }), 4, /created_at/)
    })

    test('ignores runs the server returns that do not match the request', async () => {
      const everything = [
        at(9),
        at(12, { head_sha: OTHER_SHA, conclusion: 'failure' }),
        at(12, { event: 'workflow_dispatch', conclusion: 'failure' }),
        at(12, { head_branch: 'p10/int', conclusion: 'failure' }),
      ]
      const listRuns = async (workflow) => (workflow === 'weekly.yml' ? [run()] : everything)
      await expect(verify({ listRuns })).resolves.toBeDefined()
      const onlyForeign = everything.slice(1)
      await expectRule(verify({ listRuns: async () => onlyForeign }), 4)
    })
  })

  describe('rule 5: weekly.yml', () => {
    test('rejects a missing weekly.yml run', async () => {
      await expectRule(verifyWith({ 'weekly.yml': [] }), 5, /weekly\.yml/)
    })

    test('rejects a failed weekly.yml run', async () => {
      await expectRule(verifyWith({ 'weekly.yml': [run({ conclusion: 'failure' })] }), 5)
    })

    test('rejects when weekly.yml has only failed runs', async () => {
      const failed = [at(9, { conclusion: 'failure' }), at(10, { conclusion: 'cancelled' })]
      await expectRule(verifyWith({ 'weekly.yml': failed }), 5)
    })

    test('accepts weekly.yml when any run for the SHA succeeded', async () => {
      const weekly = [at(9), at(11, { conclusion: 'failure' }), at(12, { status: 'in_progress' })]
      await expect(verifyWith({ 'weekly.yml': weekly })).resolves.toBeDefined()
    })

    test('a scheduled run counts when its SHA matches', async () => {
      await expect(
        verifyWith({ 'weekly.yml': [run({ event: 'schedule' })] })
      ).resolves.toBeDefined()
    })

    test('does not count a success for another SHA', async () => {
      const listRuns = async (workflow) =>
        workflow === 'weekly.yml' ? [run({ head_sha: OTHER_SHA })] : [run()]
      await expectRule(verify({ listRuns }), 5)
    })
  })

  test('passes with every run present and returns version and dist-tag', async () => {
    await expect(verify()).resolves.toEqual({ version: VERSION, distTag: 'next' })
    await expect(
      verify({ tag: 'v3.0.0', packageJson: { version: '3.0.0' }, changelog: '## 3.0.0\n' })
    ).resolves.toEqual({ version: '3.0.0', distTag: 'latest' })
  })

  test('asks ci.yml and gate.yml for push runs on master of the tagged SHA, and weekly.yml for the SHA only', async () => {
    const listRuns = fakeListRuns(green())
    await verify({ listRuns })
    expect(listRuns.calls).toEqual([
      ['ci.yml', { event: 'push', branch: 'master', head_sha: SHA }],
      ['gate.yml', { event: 'push', branch: 'master', head_sha: SHA }],
      ['weekly.yml', { head_sha: SHA }],
    ])
  })
})

describe('memory-gates evidence', () => {
  test('rejects a memory run on another SHA', async () => {
    await expectRule(
      checkMemory(memorySeams(memoryRun({ head_sha: OTHER_SHA }))),
      11,
      /no workflow_dispatch run of memory-gates\.yml for commit a{40}/
    )
    await expectRule(checkMemory(memorySeams(memoryRun({ event: 'pull_request' }))), 11)
  })

  test('rejects a partial memory dispatch', async () => {
    const skipped = memoryJobs({ verdict: 'skipped' })
    await expectRule(
      checkMemory(memorySeams([memoryRun(), skipped])),
      11,
      /memory-gates-verdict is skipped/
    )
    const noVerdict = memoryJobs().filter(({ name }) => name !== 'memory-gates-verdict')
    await expectRule(checkMemory(memorySeams([memoryRun(), noVerdict])), 11, /memory-gates-verdict/)
    for (const replicates of [[1], [1, 2], [1, 3], [1, 2, 3, 4]]) {
      await expectRule(
        checkMemory(memorySeams([memoryRun(), memoryJobs({ replicates })])),
        11,
        /replicates/
      )
    }
  })

  test('rejects a failed or cancelled memory run', async () => {
    for (const conclusion of ['failure', 'cancelled']) {
      await expectRule(
        checkMemory(memorySeams(memoryRun({ conclusion }))),
        11,
        new RegExp(`completed/${conclusion}`)
      )
    }
    await expectRule(
      checkMemory(memorySeams(memoryRun({ status: 'in_progress', conclusion: null }))),
      11
    )
    const cancelledJob = memoryJobs({
      extra: [{ name: 'share-peak (22, 4.2.2, enabled)', conclusion: 'cancelled' }],
    })
    await expectRule(
      checkMemory(memorySeams([memoryRun(), cancelledJob])),
      11,
      /share-peak \(22, 4\.2\.2, enabled\) is cancelled/
    )
  })

  test('accepts a full green memory run on the SHA', async () => {
    const green = memoryRun({ created_at: '2026-11-10T08:00:00Z' })
    const partial = memoryRun({ created_at: '2026-11-10T09:00:00Z' })
    const seams = memorySeams(green, [partial, memoryJobs({ verdict: 'failure' })])
    await expect(checkMemory(seams)).resolves.toBeUndefined()
    expect(seams.listRuns.calls).toEqual([[MEMORY, { event: 'workflow_dispatch', head_sha: SHA }]])
    expect(seams.listJobs.calls).toContain(green.id)
  })
})

describe('the OTel package', () => {
  const checkOtel = (overrides = {}) =>
    verifyOtelManifest({
      tag: TAG,
      mainVersion: VERSION,
      otelPackageJson: otelManifest(),
      lookup: fakeLookup(),
      ...overrides,
    })

  test('pre-pack mode checks manifests, versions and the peer range without any tarball', async () => {
    const rcLookup = fakeLookup()
    await expect(checkOtel({ lookup: rcLookup })).resolves.toEqual({ otelVersion: OTEL_VERSION })
    expect(rcLookup.calls).toEqual([])

    const finalLookup = fakeLookup()
    await expect(checkOtel({ ...FINAL, lookup: finalLookup })).resolves.toEqual({
      otelVersion: OTEL_VERSION,
    })
    expect(finalLookup.calls).toEqual([[OTEL_NAME, OTEL_VERSION]])

    for (const version of ['1.0.0', '0.1.1-rc.0', '0.1.1-alpha.9f', '0.1.1-0a', '0.1.1-x-y']) {
      await expect(checkOtel({ otelPackageJson: otelManifest({ version }) })).resolves.toEqual({
        otelVersion: version,
      })
    }
    for (const otelPackageJson of [
      otelManifest({ version: undefined }),
      otelManifest({ version: '0.1' }),
      otelManifest({ version: 'v0.1.1' }),
      otelManifest({ version: '0.1.1+build' }),
      otelManifest({ version: '01.1.1' }),
      otelManifest({ version: '0.01.1' }),
      otelManifest({ version: '0.1.1-rc..0' }),
      otelManifest({ version: '0.1.1-01' }),
      otelManifest({ version: '0.1.1-.' }),
      otelManifest({ version: '0.1.1-' }),
      otelManifest({ version: '9007199254740992.0.0' }),
      otelManifest({ name: 'kafkajs-otel' }),
    ]) {
      await expectRule(checkOtel({ otelPackageJson }), 6)
    }
  })

  test('rejects a peer range that excludes the main version', async () => {
    const withPeer = (range) =>
      otelManifest({ peerDependencies: range === undefined ? {} : { [MAIN_NAME]: range } })
    for (const [range, mainVersion] of [
      ['>=3.0.0 <4', '3.0.0-rc.1'],
      ['>=2.0.0 <4', '3.0.0-rc.1'],
      ['>=4.0.0-alpha <4', '4.0.0-beta'],
      ['>=3.0.0-0 <3.0.0-rc.1', '3.0.0-rc.1'],
      [PEER_RANGE, '4.0.0-rc.1'],
      [PEER_RANGE, '4.0.0'],
      [PEER_RANGE, '2.2.4'],
      ['>=3.1', '3.0.0'],
      ['^3.0.0', '3.0.0'],
      ['3.x', '3.0.0'],
      ['>=03.0.0 <4', '3.0.0'],
      ['>=3.0.0-rc..0 <4', '3.0.0'],
      ['>=3.0.0-01 <4', '3.0.0-1'],
      ['', '3.0.0'],
      [undefined, '3.0.0'],
    ]) {
      await expectRule(checkOtel({ mainVersion, otelPackageJson: withPeer(range) }), 7)
    }
    for (const [range, mainVersion] of [
      [PEER_RANGE, '3.0.0-rc.1'],
      [PEER_RANGE, '3.0.0-alpha.9f'],
      [PEER_RANGE, '3.0.0'],
      [PEER_RANGE, '3.9.1'],
      ['<2 || >=3.0.0', '3.0.0'],
      ['=3.0.0-rc.1', '3.0.0-rc.1'],
    ]) {
      await expect(checkOtel({ mainVersion, otelPackageJson: withPeer(range) })).resolves.toEqual({
        otelVersion: OTEL_VERSION,
      })
    }
  })

  test('rejects a v3.0.0 OTel version that is already on the registry, whatever its integrity or provenance', async () => {
    const requested = []
    const registry = (status) =>
      createRegistry({
        fetch: async (url) => {
          requested.push(String(url))
          return Response.json({ dist: { integrity: 'sha512-any' } }, { status })
        },
      })
    await expect(checkOtel({ ...FINAL, lookup: registry(404) })).resolves.toEqual({
      otelVersion: OTEL_VERSION,
    })
    await expectRule(checkOtel({ ...FINAL, lookup: registry(200) }), 8, /already on the registry/)
    await expect(checkOtel({ ...FINAL, lookup: registry(500) })).rejects.toThrow(/500/)
    expect(requested).toEqual(
      Array(3).fill(
        `https://registry.npmjs.org/@countly%2fopentelemetry-instrumentation-kafkajs/${OTEL_VERSION}`
      )
    )

    const rcLookup = fakeLookup(true)
    await expect(checkOtel({ lookup: rcLookup })).resolves.toEqual({ otelVersion: OTEL_VERSION })
    expect(rcLookup.calls).toEqual([])
  })
})

describe('distTagOf', () => {
  test.each([
    ['3.0.0-rc.1', 'next'],
    ['3.0.0-alpha.9f', 'next'],
    ['3.0.0', 'latest'],
  ])('gives %s the %s tag', (version, tag) => {
    expect(distTagOf(version)).toBe(tag)
  })

  test.each(['3.0', 'v3.0.0', '3.0.0+build', ''])('rejects %p', (version) => {
    expect(() => distTagOf(version)).toThrow(ReleaseVerificationError)
  })
})

describe('createListRuns', () => {
  const pages = (...lists) => lists

  test('an older green run on page 2 does not mask a newer failing run on page 1', async () => {
    const newestFailed = at(11, { conclusion: 'failure' })
    const fetch = stubFetch({
      'ci.yml': pages([newestFailed, ...runs(99, { conclusion: 'cancelled' })], [at(8)]),
      'gate.yml': pages([run()]),
      'weekly.yml': pages([run()]),
    })
    await expectRule(verify({ listRuns: listRunsOver(fetch) }), 4)
  })

  test('a newer failing run on page 2 is not hidden by an older green run on page 1', async () => {
    const fetch = stubFetch({
      'ci.yml': pages(
        [at(8), ...runs(99, { conclusion: 'cancelled', created_at: '2026-11-01T00:00:00Z' })],
        [at(11, { conclusion: 'failure' })]
      ),
      'gate.yml': pages([run()]),
      'weekly.yml': pages([run()]),
    })
    await expectRule(verify({ listRuns: listRunsOver(fetch) }), 4)
  })

  test('createListRuns finds a qualifying weekly.yml success that is only on page 2', async () => {
    const fetch = stubFetch({
      'ci.yml': pages([run()]),
      'gate.yml': pages([run()]),
      'weekly.yml': pages(runs(100, { conclusion: 'failure' }), [run()]),
    })
    await expect(verify({ listRuns: listRunsOver(fetch) })).resolves.toBeDefined()
    expect(fetch.requests.filter(({ url }) => url.pathname.includes('weekly.yml')).length).toBe(2)
  })

  test('createListRuns stops at the first page with fewer than 100 runs', async () => {
    const fetch = stubFetch({ 'ci.yml': pages(runs(100), runs(37), runs(100)) })
    const all = await listRunsOver(fetch)('ci.yml', { head_sha: SHA })
    expect(all).toHaveLength(137)
    expect(fetch.requests.map(({ url }) => url.searchParams.get('page'))).toEqual(['1', '2'])

    const single = stubFetch({ 'ci.yml': pages(runs(3)) })
    await listRunsOver(single)('ci.yml', { head_sha: SHA })
    expect(single.requests).toHaveLength(1)

    const exact = stubFetch({ 'ci.yml': pages(runs(100), []) })
    expect(await listRunsOver(exact)('ci.yml', { head_sha: SHA })).toHaveLength(100)
    expect(exact.requests).toHaveLength(2)
  })

  test('createListRuns throws instead of judging a truncated list when page 10 is still full', async () => {
    const full = Array.from({ length: 10 }, () => runs(100))
    const fetch = stubFetch({ 'ci.yml': full })
    const error = await listRunsOver(fetch)('ci.yml', { head_sha: SHA }).catch((e) => e)
    expect(error).toBeInstanceOf(ReleaseVerificationError)
    expect(error.message).toMatch(/truncated/)
    expect(fetch.requests).toHaveLength(10)
  })

  describe('requests', () => {
    let writes
    const spies = []
    beforeEach(() => {
      writes = []
      for (const method of ['log', 'info', 'warn', 'error', 'debug']) {
        spies.push(jest.spyOn(console, method).mockImplementation((...args) => writes.push(args)))
      }
      spies.push(
        jest.spyOn(process.stdout, 'write').mockImplementation((chunk) => {
          writes.push(chunk)
          return true
        })
      )
    })
    afterEach(() => {
      for (const spy of spies.splice(0)) spy.mockRestore()
    })

    test('createListRuns sends the bearer token and the event, branch, head_sha, per_page and page parameters, and never logs the token', async () => {
      const fetch = stubFetch({ 'ci.yml': pages(runs(1)), 'weekly.yml': pages(runs(1)) })
      const list = listRunsOver(fetch)
      await list('ci.yml', { event: 'push', branch: 'master', head_sha: SHA })
      await list('weekly.yml', { head_sha: SHA })

      const [ci, weekly] = fetch.requests
      expect(ci.url.origin).toBe('https://api.github.com')
      expect(ci.url.pathname).toBe(`/repos/${REPOSITORY}/actions/workflows/ci.yml/runs`)
      expect(Object.fromEntries(ci.url.searchParams)).toEqual({
        event: 'push',
        branch: 'master',
        head_sha: SHA,
        per_page: '100',
        page: '1',
      })
      expect(Object.fromEntries(weekly.url.searchParams)).toEqual({
        head_sha: SHA,
        per_page: '100',
        page: '1',
      })
      expect(ci.init.headers.Authorization).toBe(`Bearer ${TOKEN}`)
      expect(JSON.stringify(writes)).not.toContain(TOKEN)
    })

    test('an unsuccessful answer fails with its status and without the token', async () => {
      const fetch = stubFetch({}, { status: 403, body: { message: 'forbidden' } })
      const error = await listRunsOver(fetch)('ci.yml', { head_sha: SHA }).catch((e) => e)
      expect(error).toBeInstanceOf(ReleaseVerificationError)
      expect(error.message).toMatch(/403/)
      expect(`${error.message}${error.stack}`).not.toContain(TOKEN)
      expect(JSON.stringify(writes)).not.toContain(TOKEN)
    })
  })

  test('an answer without a workflow_runs list fails instead of reading as no runs', async () => {
    const fetch = stubFetch({}, { body: { message: 'unexpected' } })
    await expect(listRunsOver(fetch)('ci.yml', { head_sha: SHA })).rejects.toThrow(
      ReleaseVerificationError
    )
  })

  test.each(['', 'Countly', 'Countly/kafkajs/extra', 'Countly/kafka js', '../x/y'])(
    'rejects the repository name %p',
    (repository) => {
      expect(() => createListRuns({ repository, token: TOKEN, fetch: stubFetch({}) })).toThrow(
        ReleaseVerificationError
      )
    }
  )
})

describe('createListJobs', () => {
  test("createListJobs reads every page of a run's latest-attempt jobs", async () => {
    const all = Array.from({ length: 122 }, (_, i) => ({ name: `job ${i}`, conclusion: 'success' }))
    const requests = []
    const fetch = async (url, init) => {
      const parsed = new URL(String(url))
      requests.push({ url: parsed, init })
      const page = Number(parsed.searchParams.get('page'))
      const jobs = all.slice((page - 1) * 100, page * 100)
      return new Response(JSON.stringify({ total_count: all.length, jobs }), { status: 200 })
    }
    const jobs = await createListJobs({ repository: REPOSITORY, token: TOKEN, fetch })(42)
    expect(jobs).toEqual(all)
    expect(requests).toHaveLength(2)
    expect(requests[0].url.pathname).toBe(`/repos/${REPOSITORY}/actions/runs/42/jobs`)
    expect(Object.fromEntries(requests[1].url.searchParams)).toEqual({
      filter: 'latest',
      per_page: '100',
      page: '2',
    })
    expect(requests[0].init.headers.Authorization).toBe(`Bearer ${TOKEN}`)
    await expect(
      createListJobs({ repository: REPOSITORY, token: TOKEN, fetch })('42/../../x')
    ).rejects.toThrow(ReleaseVerificationError)
  })
})

describe('main', () => {
  const sandboxes = []
  const git = (cwd, ...args) =>
    execFileSync(
      'git',
      [
        '-c',
        'user.name=release-test',
        '-c',
        'user.email=t@example.com',
        '-c',
        'commit.gpgsign=false',
        ...args,
      ],
      { cwd, encoding: 'utf8' }
    ).trim()

  const sandbox = () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'release-verify-'))
    sandboxes.push(dir)
    return dir
  }

  /** A repository whose single release commit is also `origin/master`. */
  const makeRepo = ({ version = VERSION, changelog = CHANGELOG } = {}) => {
    const dir = sandbox()
    git(dir, 'init', '-q', '-b', 'master')
    fs.writeFileSync(path.join(dir, 'package.json'), JSON.stringify({ name: MAIN_NAME, version }))
    fs.writeFileSync(path.join(dir, 'CHANGELOG.md'), changelog)
    fs.mkdirSync(path.join(dir, 'otel'))
    fs.writeFileSync(path.join(dir, 'otel', 'package.json'), JSON.stringify(otelManifest()))
    git(dir, 'add', '.')
    git(dir, 'commit', '-q', '-m', 'release')
    const sha = git(dir, 'rev-parse', 'HEAD')
    git(dir, 'update-ref', 'refs/remotes/origin/master', sha)
    return { dir, sha }
  }

  const environment = ({ dir, sha }, overrides = {}) => ({
    GITHUB_REF_NAME: TAG,
    GITHUB_SHA: sha,
    GITHUB_REPOSITORY: REPOSITORY,
    GITHUB_TOKEN: TOKEN,
    GITHUB_WORKSPACE: dir,
    GITHUB_OUTPUT: path.join(sandbox(), 'output'),
    ...overrides,
  })

  /**
   * Answers GitHub and the registry: green CI runs for the queried commit, `memory(sha)` as the
   * memory-gates runs (returned unfiltered, as a server might) and `jobs` as every run's jobs.
   */
  const fakeGitHub =
    ({ memory = (sha) => [memoryRun({ head_sha: sha })], jobs = memoryJobs() } = {}) =>
    async (url) => {
      const parsed = new URL(String(url))
      const respond = (body) => new Response(JSON.stringify(body), { status: 200 })
      if (parsed.origin === 'https://registry.npmjs.org') return new Response('', { status: 404 })
      if (parsed.pathname.endsWith('/jobs')) return respond({ jobs })
      const sha = parsed.searchParams.get('head_sha')
      if (parsed.pathname.includes(`/${MEMORY}/`)) return respond({ workflow_runs: memory(sha) })
      return respond({ workflow_runs: [run({ head_sha: sha })] })
    }

  let writes
  const spies = []
  beforeEach(() => {
    writes = []
    for (const method of ['log', 'info', 'warn', 'error']) {
      spies.push(jest.spyOn(console, method).mockImplementation((...args) => writes.push(args)))
    }
    spies.push(jest.spyOn(globalThis, 'fetch').mockImplementation(fakeGitHub()))
  })
  afterEach(() => {
    for (const spy of spies.splice(0)) spy.mockRestore()
    for (const dir of sandboxes.splice(0)) fs.rmSync(dir, { recursive: true, force: true })
  })

  test('verifies the checkout and writes the version, dist-tag and commit outputs', async () => {
    const repo = makeRepo()
    const env = environment(repo)
    await main(env)
    expect(fs.readFileSync(env.GITHUB_OUTPUT, 'utf8')).toBe(
      `version=${VERSION}\ndist-tag=next\nsha=${repo.sha}\notel-version=${OTEL_VERSION}\n`
    )
    expect(fs.readdirSync(repo.dir).filter((file) => file.endsWith('.tgz'))).toEqual([])
    expect(JSON.stringify(writes)).not.toContain(TOKEN)
  })

  test('resolves an annotated tag object to the commit it points at', async () => {
    const repo = makeRepo()
    git(repo.dir, 'tag', '-a', '-m', 'release', TAG)
    const tagObject = git(repo.dir, 'rev-parse', TAG)
    expect(tagObject).not.toBe(repo.sha)
    const env = environment(repo, { GITHUB_SHA: tagObject })
    await main(env)
    expect(fs.readFileSync(env.GITHUB_OUTPUT, 'utf8')).toContain(`sha=${repo.sha}\n`)
  })

  test('rejects a tagged commit that is not on origin/master', async () => {
    const repo = makeRepo()
    fs.appendFileSync(path.join(repo.dir, 'CHANGELOG.md'), '\nmore\n')
    git(repo.dir, 'commit', '-q', '-am', 'unmerged')
    const sha = git(repo.dir, 'rev-parse', 'HEAD')
    await expectRule(main(environment({ ...repo, sha })), 2)
  })

  test('fails instead of reading a git error as "not on master" when origin/master is missing', async () => {
    const repo = makeRepo()
    git(repo.dir, 'update-ref', '-d', 'refs/remotes/origin/master')
    const error = await main(environment(repo)).catch((e) => e)
    expect(error).toBeInstanceOf(ReleaseVerificationError)
    expect(error.message).toMatch(/merge-base/)
    expect(error.message).not.toMatch(/^rule 2/)
  })

  test('rejects a checkout that is not at the commit being judged', async () => {
    const repo = makeRepo()
    fs.appendFileSync(path.join(repo.dir, 'CHANGELOG.md'), '\nmore\n')
    git(repo.dir, 'commit', '-q', '-am', 'later')
    await expect(main(environment(repo))).rejects.toThrow(/checkout is at/)
  })

  test.each([
    'GITHUB_REF_NAME',
    'GITHUB_SHA',
    'GITHUB_REPOSITORY',
    'GITHUB_TOKEN',
    'GITHUB_OUTPUT',
  ])('requires %s', async (name) => {
    const repo = makeRepo()
    await expect(main(environment(repo, { [name]: '' }))).rejects.toThrow(name)
  })

  test('rejects a GITHUB_SHA that is not a 40-hex commit id', async () => {
    const repo = makeRepo()
    await expect(main(environment(repo, { GITHUB_SHA: '--all' }))).rejects.toThrow(/GITHUB_SHA/)
  })

  test('without arguments, rejects a tagged commit with no full green memory-gates dispatch and writes nothing', async () => {
    const repo = makeRepo()
    const cases = [
      { memory: () => [] },
      { memory: () => [memoryRun({ head_sha: OTHER_SHA })] },
      { memory: (sha) => [run({ head_sha: sha, event: 'pull_request' })] },
      { memory: (sha) => [memoryRun({ head_sha: sha, conclusion: 'failure' })] },
      { memory: (sha) => [memoryRun({ head_sha: sha, conclusion: 'cancelled' })] },
      { jobs: memoryJobs({ verdict: 'skipped' }) },
      { jobs: memoryJobs({ replicates: [1, 2] }) },
    ]
    for (const github of cases) {
      globalThis.fetch.mockImplementation(fakeGitHub(github))
      const env = environment(repo)
      await expectRule(main(env, []), 11, new RegExp(`commit ${repo.sha}`))
      expect(fs.existsSync(env.GITHUB_OUTPUT)).toBe(false)
    }
  })

  test('needs memory evidence on the tagged commit itself, even when an rc tag is named', async () => {
    const repo = makeRepo()
    git(repo.dir, 'tag', TAG)
    fs.writeFileSync(
      path.join(repo.dir, 'package.json'),
      JSON.stringify({ name: MAIN_NAME, version: '3.0.0' })
    )
    fs.writeFileSync(
      path.join(repo.dir, 'CHANGELOG.md'),
      CHANGELOG.replace(`## ${VERSION} — 2026-11-20`, '## 3.0.0 — 2026-11-25')
    )
    git(repo.dir, 'commit', '-q', '-am', 'final')
    const sha = git(repo.dir, 'rev-parse', 'HEAD')
    git(repo.dir, 'update-ref', 'refs/remotes/origin/master', sha)
    const rcOnly = () => [memoryRun({ head_sha: repo.sha })]
    globalThis.fetch.mockImplementation(fakeGitHub({ memory: rcOnly }))
    const env = environment({ ...repo, sha }, { GITHUB_REF_NAME: 'v3.0.0' })
    await expectRule(main(env, ['--reuse-from', TAG]), 11, new RegExp(`commit ${sha}`))
    expect(fs.existsSync(env.GITHUB_OUTPUT)).toBe(false)
  })

  test('writes nothing when verification fails', async () => {
    const repo = makeRepo({ changelog: '# Changelog\n' })
    const env = environment(repo)
    await expectRule(main(env), 3)
    expect(fs.existsSync(env.GITHUB_OUTPUT)).toBe(false)
  })

  describe('post-pack mode', () => {
    /** Writes a gzip tarball whose package/package.json holds `manifest` and returns its path. */
    const tarball = (file, manifest) => {
      const content = sandbox()
      fs.mkdirSync(path.join(content, 'package'))
      fs.writeFileSync(path.join(content, 'package', 'package.json'), JSON.stringify(manifest))
      const target = path.join(sandbox(), file)
      execFileSync('tar', ['-czf', target, '-C', content, 'package'])
      return target
    }
    const postPack = (repo, { mainManifest, otelManifest: otel, mainSri } = {}) => {
      const mainTgz = tarball('main.tgz', mainManifest ?? { name: MAIN_NAME, version: VERSION })
      const otelTgz = tarball('otel.tgz', otel ?? otelManifest())
      const env = {
        GITHUB_REF_NAME: TAG,
        GITHUB_WORKSPACE: repo.dir,
        MAIN_SRI: mainSri ?? sriOf(fs.readFileSync(mainTgz)),
      }
      return {
        run: (args = [mainTgz, otelTgz]) => main(env, ['--post-pack', ...args]),
        mainTgz,
        otelTgz,
      }
    }

    test('post-pack mode rejects an OTel tarball whose name or version differs from its manifest', async () => {
      const repo = makeRepo()
      await expect(postPack(repo).run()).resolves.toBeUndefined()
      await expectRule(
        postPack(repo, { otelManifest: otelManifest({ version: '0.1.2' }) }).run(),
        9
      )
      await expectRule(
        postPack(repo, { otelManifest: otelManifest({ name: 'other-otel' }) }).run(),
        9
      )
      await expectRule(
        postPack(repo, { mainManifest: { name: MAIN_NAME, version: '3.0.0-rc.2' } }).run(),
        9
      )
      const swapped = postPack(repo)
      await expectRule(swapped.run([swapped.otelTgz, swapped.mainTgz]), 9)
    })

    test("post-pack mode rejects a main tarball whose SRI differs from the pack job's record", async () => {
      const repo = makeRepo()
      await expectRule(postPack(repo, { mainSri: sriOf('a different main tarball') }).run(), 10)
      await expect(postPack(repo, { mainSri: '' }).run()).rejects.toThrow(/MAIN_SRI/)
      const { run, mainTgz, otelTgz } = postPack(repo)
      await expect(run([mainTgz])).rejects.toThrow(/usage/)
      await expect(run([mainTgz, otelTgz, otelTgz])).rejects.toThrow(/usage/)
    })
  })

  test('the command line exits non-zero and names the problem without a stack', () => {
    const result = spawnSync(process.execPath, [path.join(__dirname, '..', 'verify.js')], {
      env: { PATH: process.env.PATH },
      encoding: 'utf8',
    })
    expect(result.status).toBe(1)
    expect(result.stderr).toMatch(/release verification failed: GITHUB_REF_NAME is not set/)
    expect(result.stderr).not.toMatch(/\n\s+at /)
  })
})
