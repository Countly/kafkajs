const { spawnSync } = require('node:child_process')
const fs = require('node:fs')
const os = require('node:os')
const path = require('node:path')
const yaml = require('js-yaml')

const WORKFLOW_FILE = path.join(__dirname, '..', '..', '..', '.github', 'workflows', 'release.yml')
const raw = fs.readFileSync(WORKFLOW_FILE, 'utf8')
const workflow = yaml.load(raw)
const { jobs } = workflow

const steps = (job) => jobs[job].steps
const allSteps = () => Object.values(jobs).flatMap((job) => job.steps)
/** A GitHub Actions expression, as it is written in the workflow file. */
const expr = (inner) => `\${{ ${inner} }}`
const needsOf = (job) => [].concat(jobs[job].needs ?? []).sort()
const runScripts = () => allSteps().flatMap((step) => (step.run ? [step.run] : []))
const stepsUsing = (action) =>
  allSteps().filter((step) => step.uses && step.uses.startsWith(`${action}@`))
const checkStep = () => steps('publish').find((step) => step.name === 'Check tarball and npm')
const publishStep = () => steps('publish').find((step) => step.name === 'Publish')
const publishCommands = () =>
  runScripts().flatMap((script) =>
    script.split('\n').filter((line) => /\b(npm|npx|pnpm|yarn)\b.*\bpublish\b/.test(line))
  )

describe('release.yml', () => {
  test('triggers only on pushed v* tags', () => {
    expect(workflow.on).toEqual({ push: { tags: ['v*'] } })
  })

  test('declares nothing at the top level but its name, trigger and jobs', () => {
    expect(Object.keys(workflow).sort()).toEqual(['jobs', 'name', 'on'])
    expect(Object.keys(jobs).sort()).toEqual(['pack', 'publish', 'verify'])
  })

  test('has no top-level permissions and each job has exactly its permissions', () => {
    expect(workflow.permissions).toBeUndefined()
    expect(jobs.verify.permissions).toEqual({ contents: 'read', actions: 'read' })
    expect(jobs.pack.permissions).toEqual({ contents: 'read' })
    expect(jobs.publish.permissions).toEqual({ contents: 'read', 'id-token': 'write' })
  })

  test('id-token write appears only on the publish job', () => {
    expect(jobs.verify.permissions['id-token']).toBeUndefined()
    expect(jobs.pack.permissions['id-token']).toBeUndefined()
    expect(raw.match(/id-token/g)).toHaveLength(1)
  })

  test('pack needs verify; publish needs verify and pack', () => {
    expect(needsOf('verify')).toEqual([])
    expect(needsOf('pack')).toEqual(['verify'])
    expect(needsOf('publish')).toEqual(['pack', 'verify'])
  })

  test('publish uses the npm-release environment and has no checkout step and no npm pack', () => {
    expect(jobs.publish.environment).toBe('npm-release')
    expect(jobs.verify.environment).toBeUndefined()
    expect(jobs.pack.environment).toBeUndefined()
    for (const step of steps('publish')) {
      expect(step.uses ?? '').not.toMatch(/actions\/checkout/)
      expect(step.run ?? '').not.toMatch(/npm\s+pack/)
    }
  })

  test('the workflow runs exactly one npm publish and it targets the downloaded tarball path', () => {
    const commands = publishCommands()
    expect(commands).toHaveLength(1)
    expect(publishStep().run.trim()).toBe(commands[0].trim())
    expect(commands[0]).toMatch(/^\s*npm publish "\.\/\$FILE"( |$)/)
    expect(commands[0]).toContain('--registry https://registry.npmjs.org')

    const download = stepsUsing('actions/download-artifact')
    expect(download).toHaveLength(1)
    expect(download[0].with.name).toBe('release-tarball')
    const directory = download[0].with.path
    expect(publishStep()['working-directory']).toBe(directory)
    expect(checkStep()['working-directory']).toBe(directory)
    expect(publishStep().env.FILE).toBe(expr(`steps.${checkStep().id}.outputs.file`))
    expect(steps('publish').indexOf(download[0])).toBeLessThan(
      steps('publish').indexOf(checkStep())
    )
    expect(steps('publish').indexOf(checkStep())).toBeLessThan(
      steps('publish').indexOf(publishStep())
    )
  })

  test('publishes with the dist-tag the verifier computed', () => {
    expect(publishStep().run).toContain('--tag "$DIST_TAG"')
    expect(publishStep().env.DIST_TAG).toBe(expr('needs.verify.outputs.dist-tag'))
  })

  test('release.yml never disables provenance or authenticates with a token', () => {
    expect(raw).not.toMatch(/provenance(=|:|\s+)\s*false/i)
    expect(raw).not.toMatch(/NPM_CONFIG_PROVENANCE|_authToken|npm\s+(login|adduser|token)/i)
  })

  test('the publish asks npm to require provenance, so a version is never published unattested', () => {
    expect(publishStep().run).toMatch(/\s--provenance(\s|$)/)
  })

  test('the publish runs no lifecycle script from the tarball while it holds the OIDC permission', () => {
    expect(publishStep().run).toMatch(/\s--ignore-scripts(\s|$)/)
  })

  test('the publish job writes the tarball sha512 to the job summary before publishing', () => {
    expect(checkStep().run).toContain('openssl dgst -sha512')
    expect(checkStep().run).toContain('GITHUB_STEP_SUMMARY')
    expect(steps('publish').indexOf(checkStep())).toBeLessThan(
      steps('publish').indexOf(publishStep())
    )
  })

  test('every uses is pinned to a 40-hex SHA with its version in a comment', () => {
    const used = allSteps().flatMap((step) => (step.uses ? [step.uses] : []))
    expect(used.length).toBeGreaterThanOrEqual(7)
    for (const uses of used) expect(uses).toMatch(/^[\w.-]+\/[\w.-]+@[0-9a-f]{40}$/)
    const lines = raw.split('\n').filter((line) => /^\s*(-\s+)?uses:/.test(line))
    expect(lines).toHaveLength(used.length)
    for (const line of lines) expect(line).toMatch(/@[0-9a-f]{40}\s+#\s+v\d+\.\d+\.\d+\s*$/)
  })

  test('names no NPM_TOKEN or NODE_AUTH_TOKEN anywhere', () => {
    expect(raw).not.toMatch(/NPM_TOKEN|NODE_AUTH_TOKEN/)
  })

  test('no run script contains a workflow expression', () => {
    expect(runScripts().length).toBeGreaterThanOrEqual(5)
    for (const script of runScripts()) expect(script).not.toMatch(/\$\{\{/)
  })

  test('no setup-node step sets registry-url', () => {
    expect(stepsUsing('actions/setup-node').length).toBeGreaterThanOrEqual(3)
    for (const step of stepsUsing('actions/setup-node')) {
      expect(Object.keys(step.with)).not.toContain('registry-url')
    }
    expect(raw).not.toContain('registry-url')
  })

  test('every setup-node step sets package-manager-cache false and no cache input', () => {
    const setups = stepsUsing('actions/setup-node')
    expect(setups.length).toBeGreaterThanOrEqual(3)
    for (const step of setups) {
      expect(step.with['package-manager-cache']).toBe(false)
      expect(Object.keys(step.with)).not.toContain('cache')
      expect(step.with['node-version']).toBe(24)
    }
  })

  test('release.yml uses no actions/cache step', () => {
    expect(stepsUsing('actions/cache')).toEqual([])
    expect(raw).not.toMatch(/(^|[\s{,])cache:/m)
  })

  test('pack installs with npm ci --ignore-scripts and nothing installs without it', () => {
    expect(steps('pack').map((step) => step.run)).toContain('npm ci --ignore-scripts')
    for (const script of runScripts()) {
      for (const line of script.split('\n').filter((l) => /\bnpm\s+(ci|install|i)\b/.test(l))) {
        expect(line).toContain('--ignore-scripts')
      }
    }
  })

  test('every checkout leaves no credentials behind', () => {
    const checkouts = stepsUsing('actions/checkout')
    expect(checkouts).toHaveLength(2)
    for (const step of checkouts) expect(step.with['persist-credentials']).toBe(false)
  })

  test('verify reads the full history and passes the token to the verifier through env only', () => {
    const [checkout] = steps('verify')
    expect(checkout.with['fetch-depth']).toBe(0)
    const verifier = steps('verify').find((step) => step.run === 'node scripts/release/verify.js')
    expect(verifier.env).toEqual({ GITHUB_TOKEN: expr('secrets.GITHUB_TOKEN') })
    expect(jobs.verify.outputs).toEqual({
      version: expr(`steps.${verifier.id}.outputs.version`),
      'dist-tag': expr(`steps.${verifier.id}.outputs.dist-tag`),
      sha: expr(`steps.${verifier.id}.outputs.sha`),
    })
  })

  test('pack builds the commit verify judged, then uploads exactly one tarball for publish', () => {
    const guard = steps('pack').find((step) => /git rev-parse HEAD/.test(step.run ?? ''))
    expect(guard.env).toEqual({ VERIFIED_SHA: expr('needs.verify.outputs.sha') })
    expect(guard.run).toContain('"$VERIFIED_SHA"')
    const install = steps('pack').find((step) => step.run === 'npm ci --ignore-scripts')
    expect(steps('pack').indexOf(guard)).toBeLessThan(steps('pack').indexOf(install))
    const packStep = steps('pack').find((step) => /npm pack/.test(step.run ?? ''))
    expect(packStep.run).toContain('npm pack --pack-destination')
    expect(packStep.run).toContain('./scripts/assertCleanTarball.sh')
    const [upload] = stepsUsing('actions/upload-artifact')
    expect(upload.with.name).toBe('release-tarball')
    expect(upload.with['if-no-files-found']).toBe('error')
    expect(stepsUsing('actions/upload-artifact')).toHaveLength(1)
    expect(steps('pack').indexOf(upload)).toBeGreaterThan(steps('pack').indexOf(packStep))
  })

  test('every job sets timeout-minutes', () => {
    for (const [name, job] of Object.entries(jobs)) {
      expect({ name, ok: Number.isInteger(job['timeout-minutes']) }).toEqual({ name, ok: true })
      expect(job['timeout-minutes']).toBeGreaterThanOrEqual(1)
      expect(job['timeout-minutes']).toBeLessThanOrEqual(360)
    }
  })
})

describe('the publish job check step', () => {
  const NAME = '@countly/kafkajs'
  const VERSION = '3.0.0-rc.1'
  const sandboxes = []

  afterEach(() => {
    for (const dir of sandboxes.splice(0)) fs.rmSync(dir, { recursive: true, force: true })
  })

  const sandbox = () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'release-check-'))
    sandboxes.push(dir)
    return dir
  }

  /** Writes `<dir>/<file>`, a gzip tarball whose package/package.json holds `manifest`. */
  const writeTarball = (dir, file, manifest) => {
    const content = sandbox()
    fs.mkdirSync(path.join(content, 'package'))
    fs.writeFileSync(path.join(content, 'package', 'package.json'), JSON.stringify(manifest))
    const result = spawnSync('tar', ['-czf', path.join(dir, file), '-C', content, 'package'])
    expect(result.status).toBe(0)
  }

  /** A fake `npm` on PATH whose `--version` prints `version`. */
  const fakeNpm = (version) => {
    const bin = sandbox()
    fs.writeFileSync(path.join(bin, 'npm'), `#!/bin/sh\necho ${version}\n`, { mode: 0o755 })
    return bin
  }

  const runCheck = ({
    files = [{ name: 'countly-kafkajs-3.0.0-rc.1.tgz' }],
    npm = '11.6.0',
    version = VERSION,
  } = {}) => {
    const dir = sandbox()
    for (const { name, manifest = { name: NAME, version: VERSION } } of files) {
      writeTarball(dir, name, manifest)
    }
    const out = sandbox()
    const outputFile = path.join(out, 'output')
    const summaryFile = path.join(out, 'summary')
    fs.writeFileSync(outputFile, '')
    fs.writeFileSync(summaryFile, '')
    const result = spawnSync('bash', ['-c', checkStep().run], {
      cwd: dir,
      encoding: 'utf8',
      env: {
        PATH: `${fakeNpm(npm)}${path.delimiter}${process.env.PATH}`,
        VERSION: version,
        GITHUB_OUTPUT: outputFile,
        GITHUB_STEP_SUMMARY: summaryFile,
      },
    })
    return {
      dir,
      status: result.status,
      stderr: result.stderr,
      output: fs.readFileSync(outputFile, 'utf8'),
      summary: fs.readFileSync(summaryFile, 'utf8'),
    }
  }

  const sha512 = (file) => {
    const result = spawnSync('openssl', ['dgst', '-sha512', '-binary', file])
    return `sha512-${result.stdout.toString('base64')}`
  }

  test('accepts one matching tarball, names it as the output and prints its sha512', () => {
    const result = runCheck()
    expect(result.stderr).toBe('')
    expect(result.status).toBe(0)
    expect(result.output).toBe('file=countly-kafkajs-3.0.0-rc.1.tgz\n')
    const file = path.join(result.dir, 'countly-kafkajs-3.0.0-rc.1.tgz')
    expect(result.summary).toContain('countly-kafkajs-3.0.0-rc.1.tgz')
    expect(result.summary).toContain(sha512(file))
  })

  test('fails when the artifact holds no tarball', () => {
    const result = runCheck({ files: [] })
    expect(result.status).not.toBe(0)
    expect(result.stderr).toMatch(/exactly one/)
    expect(result.output).toBe('')
  })

  test('fails when the artifact holds two tarballs', () => {
    const result = runCheck({ files: [{ name: 'a.tgz' }, { name: 'b.tgz' }] })
    expect(result.status).not.toBe(0)
    expect(result.stderr).toMatch(/exactly one/)
    expect(result.output).toBe('')
  })

  test.each([
    ['another package name', { name: 'kafkajs', version: VERSION }],
    ['another version', { name: NAME, version: '3.0.0-rc.2' }],
    ['a longer version', { name: NAME, version: `${VERSION}.1` }],
  ])('fails when the tarball holds %s', (_, manifest) => {
    const result = runCheck({ files: [{ name: 'x.tgz', manifest }] })
    expect(result.status).not.toBe(0)
    expect(result.output).toBe('')
    expect(result.summary).toBe('')
  })

  test('fails when the verified version is not the tarball version', () => {
    const result = runCheck({ version: '3.0.0' })
    expect(result.status).not.toBe(0)
    expect(result.output).toBe('')
  })

  test.each(['11.5.0', '11.4.9', '10.9.2', '9.0.0'])(
    'fails when npm is %s, below 11.5.1',
    (npm) => {
      const result = runCheck({ npm })
      expect(result.status).not.toBe(0)
      expect(result.stderr).toMatch(/11\.5\.1/)
      expect(result.output).toBe('')
    }
  )

  test.each(['11.5.1', '11.5.2', '11.6.0', '11.19.0', '12.0.0', '11.6.0-pre.1'])(
    'accepts npm %s, at or above 11.5.1',
    (npm) => {
      expect(runCheck({ npm }).status).toBe(0)
    }
  )
})
