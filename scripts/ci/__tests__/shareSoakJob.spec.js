const fs = require('node:fs')
const os = require('node:os')
const path = require('node:path')
const { execFileSync, spawnSync } = require('node:child_process')
const { admit, inventory, ShareSoakJob, verifyMergeParents } = require('../shareSoakJob')
const {
  PROFILES,
  childArgs,
  childEnvironment,
  digest,
  environmentIdentity,
} = require('../shareSoakContract')
const { validateJestReport } = require('../naturalExit')

/** @param {string} profile Fixed mode. @returns {object} Original absolute lifecycle. */
const lifecycle = (profile) => ({
  profile,
  origin: 1000,
  deadline: 1000 + PROFILES[profile].jobMs,
  stages: [],
  errors: [],
  project: 'owned',
})

describe('share soak CI lifecycle controls', () => {
  test.each(['short', 'full'])(
    'admits the entire %s parent at exact remaining bound and refuses one ms less',
    (profile) => {
      const state = lifecycle(profile)
      const required = PROFILES[profile].parentMs + 600000
      expect(admit(state, 601000)).toBe(required)
      expect(() => admit(state, 601001)).toThrow(/admission/)
      expect(() => admit({ ...state, origin: 1001 }, 601000)).toThrow(/admission/)
      expect(() => admit({ ...state, deadline: state.deadline + 1 }, 601000)).toThrow(/admission/)
    }
  )

  test.each(['NODE_OPTIONS', 'NODE_PATH', 'LD_PRELOAD', 'SHARE_SOAK_FULL', 'COMPOSE_FILE'])(
    'refuses %s substitution before any child is selected',
    (key) => {
      expect(() => childEnvironment('short', { [key]: 'override' })).toThrow()
    }
  )

  test('verifies real merge parents in a depth-one checkout without accepting a foreign head', () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), 'share-shallow-'))
    const source = path.join(root, 'source')
    const clone = path.join(root, 'clone')
    const git = (cwd, args) => execFileSync('git', args, { cwd, encoding: 'utf8' }).trim()
    try {
      fs.mkdirSync(source)
      git(source, ['init', '-b', 'base'])
      git(source, ['config', 'user.name', 'Fixture'])
      git(source, ['config', 'user.email', 'fixture@example.com'])
      git(source, ['-c', 'commit.gpgsign=false', 'commit', '--allow-empty', '-m', 'base'])
      const base = git(source, ['rev-parse', 'HEAD'])
      git(source, ['checkout', '-b', 'head'])
      git(source, ['-c', 'commit.gpgsign=false', 'commit', '--allow-empty', '-m', 'head'])
      const head = git(source, ['rev-parse', 'HEAD'])
      git(source, ['checkout', 'base'])
      git(source, ['-c', 'commit.gpgsign=false', 'merge', '--no-ff', 'head', '-m', 'merge'])
      git(root, ['clone', '--depth=1', `file://${source}`, clone])
      const run = (args) => git(clone, args)
      expect(run(['show', '-s', '--format=%P', 'HEAD'])).toBe('')
      expect(() =>
        verifyMergeParents({ head: { sha: head }, base: { sha: base } }, run)
      ).not.toThrow()
      expect(() =>
        verifyMergeParents({ head: { sha: base }, base: { sha: 'f'.repeat(40) } }, run)
      ).toThrow(/merge relationship/)
    } finally {
      fs.rmSync(root, { recursive: true, force: true })
    }
  })

  test('binds the fixed full CLI and exposed-GC short Jest report', () => {
    expect(childArgs('full')).toEqual([
      '--expose-gc',
      'scripts/soak/soak.js',
      '--profile',
      'share-consumer',
    ])
    expect(childArgs('short')).toContain('--outputFile=.tmp/e2e/share-soak-jest.json')
    expect(() => childArgs('other')).toThrow()
    expect(childEnvironment('full', {})).toEqual({ SHARE_SOAK_FULL: '1' })
  })

  test('rejects a skipped or forged short inventory despite exit zero', () => {
    expect(() => validateJestReport({ success: true, testResults: [] })).toThrow(/inventory/)
  })

  test('inventories actual serialized bytes deterministically and detects changed bytes', () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), 'share-seal-'))
    try {
      fs.writeFileSync(
        path.join(root, 'original.jsonl'),
        JSON.stringify({ event: 'source' }) + '\n'
      )
      fs.writeFileSync(path.join(root, 'seal.json'), 'excluded')
      const first = inventory(root)
      expect(first).toEqual([
        { path: 'original.jsonl', bytes: 19, sha256: digest('{"event":"source"}\n') },
      ])
      expect(inventory(root)).toEqual(first)
      fs.appendFileSync(path.join(root, 'original.jsonl'), '{}\n')
      expect(inventory(root)).not.toEqual(first)
      fs.symlinkSync('/tmp', path.join(root, 'shadow'))
      expect(() => inventory(root)).toThrow(/Non-regular/)
    } finally {
      fs.rmSync(root, { recursive: true, force: true })
    }
  })

  test.each(['short', 'full'])(
    'preserves %s primary and cleanup errors while attempting owned teardown',
    (profile) => {
      const state = lifecycle(profile)
      const writes = []
      const reader = jest.spyOn(fs, 'readFileSync').mockImplementation(() => JSON.stringify(state))
      const writer = jest.spyOn(fs, 'writeFileSync').mockImplementation((file, bytes) => {
        writes.push(JSON.parse(bytes))
        Object.assign(state, JSON.parse(bytes))
      })
      try {
        const job = new ShareSoakJob({ now: () => 1001 })
        expect(() =>
          job.stage(
            'parent',
            () => {
              throw new Error('primary')
            },
            PROFILES[profile].parentMs
          )
        ).toThrow('primary')
        expect(() =>
          job.stage(
            'teardown',
            () => {
              throw new Error('secondary')
            },
            120000
          )
        ).toThrow('secondary')
        expect(state.errors.map((e) => e.message)).toEqual(['primary', 'secondary'])
        expect(writes.at(-1).stages.map((s) => s.status)).toEqual(['failed', 'failed'])
      } finally {
        reader.mockRestore()
        writer.mockRestore()
      }
    }
  )

  test('does not skip cleanup when post-run reserve is exhausted', () => {
    const state = lifecycle('short')
    const reader = jest.spyOn(fs, 'readFileSync').mockReturnValue(JSON.stringify(state))
    const writer = jest.spyOn(fs, 'writeFileSync').mockImplementation(() => {})
    const cleanup = jest.fn()
    try {
      const job = new ShareSoakJob({ now: () => state.deadline - 1 })
      expect(() => job.stage('teardown', cleanup, 120000)).toThrow(/obligations/)
      expect(cleanup).toHaveBeenCalledTimes(1)
      expect(job.state.errors).toHaveLength(1)
    } finally {
      reader.mockRestore()
      writer.mockRestore()
    }
  })

  test('refuses a foreign daemon before Compose startup or destructive cleanup', () => {
    const run = jest.fn(() => 'foreign-container')
    const job = new ShareSoakJob({ run, now: () => 1000 })
    job.state = lifecycle('short')
    expect(() => job.startup()).toThrow(/Foreign/)
    expect(run).toHaveBeenCalledTimes(1)
    expect(() => job.teardown()).toThrow(/authority/)
    expect(run).toHaveBeenCalledTimes(1)
  })

  test('observes real clean subprocess exit and kills a bounded event-loop holder', () => {
    const clean = spawnSync(process.execPath, ['-e', 'setImmediate(() => {})'], { timeout: 3000 })
    expect(clean.status).toBe(0)
    expect(clean.signal).toBeNull()
    const leak = spawnSync(process.execPath, ['-e', 'setInterval(() => {}, 10)'], {
      timeout: 100,
      killSignal: 'SIGKILL',
    })
    expect(leak.error.code).toBe('ETIMEDOUT')
    expect(leak.signal).toBe('SIGKILL')
    expect(leak.status).toBeNull()
  })
})

describe('share soak CI owned lifecycle serialization', () => {
  /**
   * Supplies original-state serialization and a Docker command double to the real lifecycle.
   * The double creates and removes exactly one owned three-node cluster, never any broker.
   * @param {string} profile Fixed profile.
   * @param {string} [failure] Fixed failure injection site.
   * @returns {object} Lifecycle, operations and restore function.
   */
  const fixture = (profile, failure) => {
    const state = lifecycle(profile)
    const contract = {
      source: { commit: 'a'.repeat(40) },
      executable: process.execPath,
      environment: environmentIdentity(childEnvironment(profile, { RUNNER_NAME: 'hosted' })),
    }
    const files = new Map([
      ['.tmp/e2e/lifecycle.json', JSON.stringify(state)],
      ['.tmp/e2e/contract.json', JSON.stringify(contract)],
    ])
    const reader = jest.spyOn(fs, 'readFileSync').mockImplementation((name) => files.get(name))
    const writer = jest
      .spyOn(fs, 'writeFileSync')
      .mockImplementation((name, bytes) => files.set(name, bytes))
    const operations = []
    let exists = false
    const containers = [1, 2, 3].map((n) => ({
      Id: `id-${n}`,
      Image: 'sha-image',
      Config: {
        Image: 'apache/kafka:4.3.1',
        Hostname: `kafka${n}`,
        Labels: {
          'com.docker.compose.project': 'owned',
          'com.docker.compose.service': `kafka${n}`,
        },
      },
      State: { Running: true },
    }))
    const run = jest.fn((executable, args) => {
      operations.push(args)
      if (args[0] === 'inspect') return JSON.stringify(containers)
      if (args[0] === 'image') return JSON.stringify([{ Id: 'sha-image' }])
      if (args[0] === 'exec') return args.includes('--version') ? '4.3.1' : 'ready'
      if (args[0] === 'compose') {
        const rest = args.slice(5)
        if (rest[0] === 'up') {
          exists = true
          if (failure === 'partial startup') throw new Error('partial startup')
          return ''
        }
        if (rest[0] === 'down') {
          if (failure === 'teardown') throw new Error('teardown')
          exists = false
          return ''
        }
        if (rest[0] === 'ps') return exists ? 'id-1\nid-2\nid-3' : ''
        if (rest[0] === 'logs') {
          if (failure === 'logs') throw new Error('logs')
          return 'original logs'
        }
        return 'base config'
      }
      return ''
    })
    const job = new ShareSoakJob({
      now: () => 1001,
      run,
      env: { RUNNER_NAME: 'hosted' },
      source: () => contract.source,
    })
    return {
      job,
      files,
      run,
      operations,
      restore: () => {
        reader.mockRestore()
        writer.mockRestore()
      },
    }
  }

  test.each(['short', 'full'])(
    'runs one complete %s lifecycle without renewing the origin or clipping the parent',
    async (profile) => {
      const f = fixture(profile)
      const child = jest.fn()
      try {
        f.job.stage('startup', () => f.job.startup(), 240000)
        f.job.stage('parent', () => f.job.parent(child), PROFILES[profile].parentMs)
        await f.job.verifyStage(async () => {})
        f.job.stage('logs', () => f.job.logs(), 60000)
        f.job.stage('teardown', () => f.job.teardown(), 120000)
        f.job.stage('seal', () => {}, 60000)
        const final = JSON.parse(f.files.get('.tmp/e2e/lifecycle.json'))
        expect(final.origin).toBe(1000)
        expect(final.deadline).toBe(1000 + PROFILES[profile].jobMs)
        expect(final.errors).toEqual([])
        expect(final.stages.map((s) => s.name)).toEqual([
          'startup',
          'parent',
          'verify',
          'logs',
          'teardown',
          'seal',
        ])
        expect(child).toHaveBeenCalledTimes(1)
        expect(f.operations.filter((args) => args.includes('up'))).toHaveLength(1)
        expect(f.operations.filter((args) => args.includes('down'))).toHaveLength(1)
        expect(JSON.parse(f.files.get('.tmp/e2e/teardown.json')).remaining).toEqual({
          containers: '',
          networks: '',
          volumes: '',
        })
      } finally {
        f.restore()
      }
    }
  )

  test.each(['partial startup', 'parent', 'verify', 'logs', 'teardown', 'seal'])(
    'retains %s failure and attempts bounded owned cleanup and partial records',
    async (failure) => {
      const f = fixture('short', failure)
      try {
        try {
          f.job.stage('startup', () => f.job.startup(), 240000)
        } catch {}
        if (failure !== 'partial startup') {
          try {
            f.job.stage(
              'parent',
              () =>
                f.job.parent(() => {
                  if (failure === 'parent') throw new Error('parent')
                }),
              PROFILES.short.parentMs
            )
          } catch {}
          try {
            await f.job.verifyStage(async () => {
              if (failure === 'verify') throw new Error('verify')
            })
          } catch {}
        }
        try {
          f.job.stage('logs', () => f.job.logs(), 60000)
        } catch {}
        try {
          f.job.stage('teardown', () => f.job.teardown(), 120000)
        } catch {}
        try {
          f.job.stage(
            'seal',
            () => {
              if (failure === 'seal') throw new Error('seal')
            },
            60000
          )
        } catch {}
        const final = JSON.parse(f.files.get('.tmp/e2e/lifecycle.json'))
        expect(final.errors[0].message).toBe(failure)
        expect(f.operations.filter((args) => args.includes('down'))).toHaveLength(1)
        expect(final.stages.at(-1).name).toBe('seal')
        expect(final.errors).not.toEqual([])
      } finally {
        f.restore()
      }
    }
  )
})

describe('required memory gate composition', () => {
  test('flushes each calibration child before starting the next even after failure', () => {
    const events = []
    const spawn = jest.spyOn(require('node:child_process'), 'spawnSync').mockImplementation(() => {
      const index = events.filter((event) => event.type === 'spawn').length
      events.push({ type: 'spawn', index })
      return { status: index === 0 ? 1 : 0, stdout: JSON.stringify({ index }), stderr: '' }
    })
    const mkdir = jest.spyOn(fs, 'mkdirSync').mockImplementation(() => {})
    const write = jest.spyOn(fs, 'writeFileSync').mockImplementation((file, data, options) => {
      events.push({ type: 'write', file, data, options })
    })
    try {
      jest.isolateModules(() => {
        const { run } = require('../../soak/journalControls')
        expect(() => run('calibration-artifacts')).toThrow('Journal controls failed')
      })
      const children = events.filter((event) => event.type === 'spawn')
      expect(children).toHaveLength(35)
      for (const [index, event] of events.entries()) {
        if (event.type !== 'spawn') continue
        const json = events[index + 1]
        expect(json).toMatchObject({ type: 'write', options: { flush: true } })
        expect(json.file).toMatch(/\.json$/)
        expect(JSON.parse(json.data)).toEqual({ index: event.index })
        expect(events[index + 2]).toMatchObject({ type: 'write', options: { flush: true } })
        expect(events[index + 2].file).toMatch(/\.stderr$/)
      }
      expect(events.at(-1).file).toMatch(/summary\.json$/)
      expect(JSON.parse(events.at(-1).data).failures[0].message).toContain(
        'unsuccessful child exit'
      )
    } finally {
      spawn.mockRestore()
      mkdir.mockRestore()
      write.mockRestore()
    }
  })

  test('requires both share profiles and every relocated telemetry verdict without optional jobs', () => {
    const workflow = fs.readFileSync('.github/workflows/memory-gates.yml', 'utf8')
    const jobs = {}
    let current
    for (const line of workflow.split('\n')) {
      const name = /^ {2}([\w-]+):$/.exec(line)
      if (name) current = name[1]
      if (current) jobs[current] = (jobs[current] ?? '') + line + '\n'
    }
    const required = [
      'unit',
      'share-soak-verdict',
      'resources',
      'journal-controls',
      'retained-diagnostic',
      'telemetry-soak',
      'telemetry-seeded-control',
      'live-owner-census',
      'telemetry-verdicts',
      'telemetry-verdict-controls',
    ]
    expect(jobs.verdict).toContain('needs: [' + required.join(', ') + ']')
    expect(jobs.verdict).toContain('if: always()')
    expect(jobs.verdict).toContain("job.result !== 'success'")
    expect(jobs['share-soak']).toContain('profile: short, jobMinutes: 55, parentMinutes: 35')
    expect(jobs['share-soak']).toContain('profile: full, jobMinutes: 71, parentMinutes: 51')
    expect(jobs['share-soak']).toContain('node scripts/ci/shareSoakJob.js freeze "$SOAK_PROFILE"')
    expect(jobs['share-soak-verify']).toContain('profile: [short, full]')
    expect(jobs['share-soak-verdict']).toContain('needs: [share-soak, share-soak-verify]')
    expect(jobs.resources).toContain('node: [22, 24, 26]')
    expect(jobs.resources).toContain('    timeout-minutes: 105')
    expect(jobs.resources).toContain(
      '- name: Upload resource and calibration evidence\n' +
        '        if: always()\n' +
        '        timeout-minutes: 2\n' +
        '        uses: actions/upload-artifact@v4'
    )
    expect(jobs['telemetry-soak']).toContain('node: [22, 24, 26]')
    expect(jobs['telemetry-soak']).toContain("kafka: ['4.3.1', '4.2.2']")
    expect(jobs['telemetry-seeded-control']).toContain('node: [22, 24, 26]')
    expect(jobs['telemetry-seeded-control']).toContain("KAFKA_VERSION: '4.3.1'")
    expect(jobs['live-owner-census']).toContain('node: [22, 24, 26]')
    expect(jobs['live-owner-census']).toContain("kafka: ['4.3.1', '4.2.2']")
    expect(jobs['live-owner-census']).toContain('variant: [capture, scalar]')
    expect(jobs['live-owner-census']).toContain(
      'liveDiagnostic.js --diagnostic-only "$LIVE_CENSUS_ROOT" "$CENSUS_VARIANT"'
    )
    expect(jobs['live-owner-census']).not.toMatch(/verifyEvidence|resourceArtifact\.js/)
    expect(jobs['telemetry-verdicts']).toContain('full: true')
    expect(jobs['telemetry-verdicts']).toContain('memory-verdicts: true')
    expect(workflow).not.toContain('continue-on-error:')
    const telemetry = fs.readFileSync('.github/workflows/telemetry.yml', 'utf8')
    const start = telemetry.indexOf('- name: Require archived socket ownership')
    expect(start).toBeGreaterThanOrEqual(0)
    const memory = telemetry.slice(start, telemetry.indexOf('- name:', start + 1))
    expect(memory).toContain('always() && inputs.memory-verdicts')
    expect(memory).toContain('verifyMemoryEvidence')
    expect(telemetry).not.toContain('continue-on-error:')
  })
})
