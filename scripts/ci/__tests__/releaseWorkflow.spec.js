const { spawnSync } = require('node:child_process')
const crypto = require('node:crypto')
const fs = require('node:fs')
const os = require('node:os')
const path = require('node:path')
const vm = require('node:vm')
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
/** Lines that run a publish command; quoted text (an error message, a path) is not a command. */
const publishCommands = (scripts = runScripts()) =>
  scripts.flatMap((script) =>
    script
      .split('\n')
      .filter((line) =>
        /\b(npm|npx|pnpm|yarn)\b.*\bpublish\b/.test(line.replace(/"(?:[^"\\]|\\.)*"|'[^']*'/g, ''))
      )
  )
const jobRunScripts = (job) => steps(job).flatMap((step) => (step.run ? [step.run] : []))
const jobStepsUsing = (job, action) =>
  steps(job).filter((step) => step.uses && step.uses.startsWith(`${action}@`))
const stepNamed = (job, name) => steps(job).find((step) => step.name === name)
const otelPackStep = () => stepNamed('pack-otel', 'Pack, guard and smoke the OTel tarball')
const otelCheckStep = () => stepNamed('publish-otel', 'Check OTel tarball, npm and registry')
const otelPublishStep = () => stepNamed('publish-otel', 'Publish OTel')
/** Evaluates a job's `if` for a pushed ref, as GitHub compares the two strings. */
const runsFor = (job, ref) =>
  vm.runInNewContext(jobs[job].if.replace(/==/g, '==='), { github: { ref } })
const PUBLISH_JOBS = ['publish', 'publish-otel']

const OTEL_NAME = '@countly/opentelemetry-instrumentation-kafkajs'
const OTEL_VERSION = '0.1.1'
const OTEL_TGZ = 'countly-opentelemetry-instrumentation-kafkajs-0.1.1.tgz'
const MAIN_TGZ = 'countly-kafkajs-3.0.0-rc.1.tgz'
const sandboxes = []
afterEach(() => {
  for (const dir of sandboxes.splice(0)) fs.rmSync(dir, { recursive: true, force: true })
})
const sandbox = () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'release-workflow-'))
  sandboxes.push(dir)
  return dir
}
/** Writes an executable shell script. */
const script = (file, body) => fs.writeFileSync(file, `#!/bin/sh\n${body}\n`, { mode: 0o755 })

/**
 * Runs pack-otel's pack step in a sandbox checkout whose npm, node and helper scripts only record
 * their calls, with `mains` as the downloaded release-tarball artifact.
 */
const runOtelPack = ({ mains = [MAIN_TGZ] } = {}) => {
  const root = sandbox()
  const temp = sandbox()
  const bin = sandbox()
  const log = path.join(sandbox(), 'calls')
  fs.writeFileSync(log, '')
  fs.mkdirSync(path.join(root, 'otel'))
  fs.mkdirSync(path.join(root, 'scripts'))
  fs.mkdirSync(path.join(temp, 'main'))
  for (const file of mains) fs.writeFileSync(path.join(temp, 'main', file), 'main')
  const record = (name) => `echo "${name} $*" >> "${log}"`
  script(
    path.join(root, 'scripts', 'assertCleanOtelTarball.sh'),
    record('assertCleanOtelTarball.sh')
  )
  script(path.join(root, 'scripts', 'otelPackSmoke.sh'), record('otelPackSmoke.sh'))
  script(path.join(root, 'scripts', 'sri.sh'), `${record('sri.sh')}\necho sha512-otel-sri`)
  script(path.join(bin, 'node'), record('node'))
  script(
    path.join(bin, 'npm'),
    [
      `echo "npm $* in $(basename "$PWD")" >> "${log}"`,
      'while [ "$#" -gt 0 ]; do [ "$1" = --pack-destination ] && dest="$2"; shift; done',
      `touch "$dest/${OTEL_TGZ}" && echo ${OTEL_TGZ}`,
    ].join('\n')
  )
  const summary = path.join(sandbox(), 'summary')
  fs.writeFileSync(summary, '')
  const result = spawnSync('bash', ['-c', otelPackStep().run], {
    cwd: root,
    encoding: 'utf8',
    env: {
      PATH: `${bin}${path.delimiter}${process.env.PATH}`,
      RUNNER_TEMP: temp,
      GITHUB_STEP_SUMMARY: summary,
      MAIN_SRI: 'sha512-main-sri',
    },
  })
  return {
    temp,
    status: result.status,
    stderr: result.stderr,
    calls: fs.readFileSync(log, 'utf8').split('\n').filter(Boolean),
    summary: fs.readFileSync(summary, 'utf8'),
  }
}

/** Writes `<dir>/<file>`, a gzip tarball whose package/package.json holds `manifest`. */
const writeTarball = (dir, file, manifest) => {
  const content = sandbox()
  fs.mkdirSync(path.join(content, 'package'))
  fs.writeFileSync(path.join(content, 'package', 'package.json'), JSON.stringify(manifest))
  const result = spawnSync('tar', ['-czf', path.join(dir, file), '-C', content, 'package'])
  expect(result.status).toBe(0)
  return path.join(dir, file)
}

/** A fake `npm` on PATH whose `--version` prints `version`. */
const fakeNpm = (version) => {
  const bin = sandbox()
  script(path.join(bin, 'npm'), `echo ${version}`)
  return bin
}

describe('release.yml', () => {
  test('triggers only on pushed v* tags', () => {
    expect(workflow.on).toEqual({ push: { tags: ['v*'] } })
  })

  test('declares nothing at the top level but its name, trigger and jobs', () => {
    expect(Object.keys(workflow).sort()).toEqual(['jobs', 'name', 'on'])
    expect(Object.keys(jobs).sort()).toEqual([
      'pack',
      'pack-otel',
      'publish',
      'publish-otel',
      'verify',
    ])
  })

  test('has no top-level permissions and each job has exactly its permissions', () => {
    expect(workflow.permissions).toBeUndefined()
    expect(jobs.verify.permissions).toEqual({ contents: 'read', actions: 'read' })
    expect(jobs.pack.permissions).toEqual({ contents: 'read' })
    expect(jobs.publish.permissions).toEqual({ contents: 'read', 'id-token': 'write' })
    expect(jobs['pack-otel'].permissions).toEqual({ contents: 'read' })
    expect(jobs['publish-otel'].permissions).toEqual({ contents: 'read', 'id-token': 'write' })
  })

  test('id-token write is granted only to the publish jobs', () => {
    for (const [name, job] of Object.entries(jobs)) {
      const granted = job.permissions?.['id-token']
      expect({ name, granted }).toEqual({
        name,
        granted: PUBLISH_JOBS.includes(name) ? 'write' : undefined,
      })
    }
    expect(raw.match(/id-token/g)).toHaveLength(PUBLISH_JOBS.length)
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

  test('the publish job runs exactly one npm publish and it targets the downloaded tarball path', () => {
    const commands = publishCommands(jobRunScripts('publish'))
    expect(commands).toHaveLength(1)
    expect(publishStep().run.trim()).toBe(commands[0].trim())
    expect(commands[0]).toMatch(/^\s*npm publish "\.\/\$FILE"( |$)/)
    expect(commands[0]).toContain('--registry https://registry.npmjs.org')

    const download = jobStepsUsing('publish', 'actions/download-artifact')
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

  test('packs, guards and smokes both tarballs on every release tag', () => {
    for (const job of ['verify', 'pack', 'pack-otel', 'publish'])
      expect(jobs[job].if).toBeUndefined()
    const mainPack = steps('pack').find((step) => /npm pack/.test(step.run ?? ''))
    expect(mainPack.id).toBe('pack')
    expect(mainPack.run).toContain('./scripts/assertCleanTarball.sh')
    expect(mainPack.run).toMatch(/sri="\$\(\.\/scripts\/sri\.sh "\$RUNNER_TEMP"\/pack\/\*\.tgz\)"/)
    expect(mainPack.run).toContain('echo "sri=$sri" >> "$GITHUB_OUTPUT"')
    expect(jobs.pack.outputs).toEqual({ sri: expr('steps.pack.outputs.sri') })

    const lines = otelPackStep()
      .run.split('\n')
      .map((line) => line.trim())
    const at = (pattern) => lines.findIndex((line) => pattern.test(line))
    const order = [
      /cd otel && npm pack --silent --pack-destination/,
      /^\.\/scripts\/assertCleanOtelTarball\.sh "\$otel"$/,
      /^\.\/scripts\/otelPackSmoke\.sh "\$otel" "\$main"$/,
      /^node scripts\/release\/verify\.js --post-pack "\$main" "\$otel"$/,
    ].map(at)
    expect(order.every((index) => index >= 0)).toBe(true)
    expect([...order].sort((a, b) => a - b)).toEqual(order)
  })

  test('pack-otel needs verify and pack, checks the verified SHA and smokes the downloaded main tarball without repacking it', () => {
    expect(needsOf('pack-otel')).toEqual(['pack', 'verify'])
    expect(jobs['pack-otel'].environment).toBeUndefined()
    const guard = stepNamed('pack-otel', 'Check the checkout is the verified commit')
    expect(guard.run).toBe('test "$(git rev-parse HEAD)" = "$VERIFIED_SHA"')
    expect(guard.env).toEqual({ VERIFIED_SHA: expr('needs.verify.outputs.sha') })
    const install = steps('pack-otel').find((step) => step.run === 'npm ci --ignore-scripts')
    expect(install['working-directory']).toBe('otel')
    const [download] = jobStepsUsing('pack-otel', 'actions/download-artifact')
    expect(download.with).toEqual({ name: 'release-tarball', path: `${expr('runner.temp')}/main` })
    expect(otelPackStep().env).toEqual({ MAIN_SRI: expr('needs.pack.outputs.sri') })
    const order = [guard, install, download, otelPackStep()].map((step) =>
      steps('pack-otel').indexOf(step)
    )
    expect([...order].sort((a, b) => a - b)).toEqual(order)
    expect(
      jobRunScripts('pack-otel')
        .join('\n')
        .match(/npm pack/g)
    ).toHaveLength(1)

    const result = runOtelPack()
    expect(result.stderr).toBe('')
    expect(result.status).toBe(0)
    const otel = path.join(result.temp, 'otel', OTEL_TGZ)
    const main = path.join(result.temp, 'main', MAIN_TGZ)
    expect(result.calls).toEqual([
      `npm pack --silent --pack-destination ${path.join(result.temp, 'otel')} in otel`,
      `assertCleanOtelTarball.sh ${otel}`,
      `otelPackSmoke.sh ${otel} ${main}`,
      `node scripts/release/verify.js --post-pack ${main} ${otel}`,
      `sri.sh ${otel}`,
    ])
    expect(result.summary).toContain('sha512-otel-sri')

    for (const mains of [[], [MAIN_TGZ, 'countly-kafkajs-3.0.0-rc.2.tgz']]) {
      const refused = runOtelPack({ mains })
      expect(refused.status).not.toBe(0)
      expect(refused.stderr).toMatch(/exactly one main tarball/)
      expect(refused.calls).toEqual([])
    }
  })

  test('uploads the OTel tarball as an artifact on rc tags without publishing it', () => {
    const [upload] = jobStepsUsing('pack-otel', 'actions/upload-artifact')
    expect(upload.with).toEqual({
      name: 'otel-tarball',
      path: `${expr('runner.temp')}/otel/*.tgz`,
      'if-no-files-found': 'error',
    })
    expect(steps('pack-otel').indexOf(upload)).toBeGreaterThan(
      steps('pack-otel').indexOf(otelPackStep())
    )
    expect(jobs['pack-otel'].if).toBeUndefined()
    expect(publishCommands(jobRunScripts('pack-otel'))).toEqual([])
    for (const ref of ['refs/tags/v3.0.0-rc.1', 'refs/tags/v3.0.0-rc.0', 'refs/tags/v3.0.1']) {
      expect({ ref, runs: runsFor('publish-otel', ref) }).toEqual({ ref, runs: false })
    }
  })

  test('publish-otel runs only for refs/tags/v3.0.0 and needs both publish and pack-otel', () => {
    expect(jobs['publish-otel'].if).toBe("github.ref == 'refs/tags/v3.0.0'")
    expect(runsFor('publish-otel', 'refs/tags/v3.0.0')).toBe(true)
    expect(runsFor('publish-otel', 'refs/tags/v3.0.0-rc.1')).toBe(false)
    expect(needsOf('publish-otel')).toEqual(['pack-otel', 'publish', 'verify'])
    expect(jobs['publish-otel'].environment).toBe('npm-release')
  })

  test('publish-otel publishes the downloaded artifact exactly once and never repacks', () => {
    for (const step of steps('publish-otel')) {
      expect(step.run ?? '').not.toMatch(/npm\s+(pack|ci|install)|verify\.js/)
    }
    expect(jobStepsUsing('publish-otel', 'actions/checkout')).toEqual([])
    const download = jobStepsUsing('publish-otel', 'actions/download-artifact')
    expect(download).toHaveLength(1)
    expect(download[0].with.name).toBe('otel-tarball')
    const directory = download[0].with.path
    expect(otelCheckStep()['working-directory']).toBe(directory)
    expect(otelPublishStep()['working-directory']).toBe(directory)

    const commands = publishCommands(jobRunScripts('publish-otel'))
    expect(commands).toHaveLength(1)
    expect(otelPublishStep().run.trim()).toBe(
      'npm publish "./$FILE" --access public --tag latest --provenance --ignore-scripts --registry https://registry.npmjs.org'
    )
    expect(otelPublishStep().env).toEqual({
      FILE: expr(`steps.${otelCheckStep().id}.outputs.file`),
    })
    expect(otelPublishStep().if).toBeUndefined()
    expect(otelCheckStep().env).toEqual({ VERSION: expr('needs.verify.outputs.otel-version') })
    const order = [download[0], otelCheckStep(), otelPublishStep()].map((step) =>
      steps('publish-otel').indexOf(step)
    )
    expect([...order].sort((a, b) => a - b)).toEqual(order)
    expect(publishCommands()).toHaveLength(2)
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
    expect(checkouts).toHaveLength(3)
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
      'otel-version': expr(`steps.${verifier.id}.outputs.otel-version`),
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
    const [upload] = jobStepsUsing('pack', 'actions/upload-artifact')
    expect(upload.with.name).toBe('release-tarball')
    expect(upload.with['if-no-files-found']).toBe('error')
    expect(jobStepsUsing('pack', 'actions/upload-artifact')).toHaveLength(1)
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

describe('the publish-otel check step', () => {
  const VERSION_URL = `https://registry.npmjs.org/@countly%2fopentelemetry-instrumentation-kafkajs/${OTEL_VERSION}`

  /**
   * Runs publish-otel's check step on a downloaded artifact holding `manifest`, with an `npm` whose
   * `--version` prints `npm` and a `curl` that records its URL, prints `status` and exits `curlExit`.
   */
  const runOtelCheck = ({
    manifest = { name: OTEL_NAME, version: OTEL_VERSION },
    npm = '11.6.0',
    status = '404',
    curlExit = 0,
  } = {}) => {
    const dir = sandbox()
    const file = writeTarball(dir, OTEL_TGZ, manifest)
    const sri = `sha512-${crypto.createHash('sha512').update(fs.readFileSync(file)).digest('base64')}`
    const bin = fakeNpm(npm)
    const out = sandbox()
    const urls = path.join(out, 'urls')
    const outputFile = path.join(out, 'output')
    const summaryFile = path.join(out, 'summary')
    for (const file of [urls, outputFile, summaryFile]) fs.writeFileSync(file, '')
    script(
      path.join(bin, 'curl'),
      `for arg; do url="$arg"; done\necho "$url" >> "${urls}"\nprintf %s ${status}\nexit ${curlExit}`
    )
    const result = spawnSync('bash', ['-c', otelCheckStep().run], {
      cwd: dir,
      encoding: 'utf8',
      env: {
        PATH: `${bin}${path.delimiter}${process.env.PATH}`,
        VERSION: OTEL_VERSION,
        GITHUB_OUTPUT: outputFile,
        GITHUB_STEP_SUMMARY: summaryFile,
      },
    })
    return {
      sri,
      status: result.status,
      stderr: result.stderr,
      urls: fs.readFileSync(urls, 'utf8').split('\n').filter(Boolean),
      output: fs.readFileSync(outputFile, 'utf8'),
      summary: fs.readFileSync(summaryFile, 'utf8'),
    }
  }

  test('publish-otel publishes only when the registry answers 404 and stops on an existing version or a registry error', () => {
    const fresh = runOtelCheck()
    expect(fresh.stderr).toBe('')
    expect(fresh.status).toBe(0)
    expect(fresh.urls).toEqual([VERSION_URL])
    expect(fresh.output).toBe(`file=${OTEL_TGZ}\n`)
    expect(fresh.summary).toContain(fresh.sri)

    const existing = runOtelCheck({ status: '200' })
    expect(existing.status).not.toBe(0)
    expect(existing.urls).toEqual([VERSION_URL])
    expect(existing.output).toBe('')
    expect(existing.stderr).toContain(
      `::error::${OTEL_NAME}@${OTEL_VERSION} already exists on npm. Automatic recovery is disabled.`
    )
    expect(existing.summary).toContain(existing.sri)

    for (const [name, options] of Object.entries({
      'a registry error': { status: '500' },
      'a transport error': { status: '000', curlExit: 6 },
    })) {
      const result = runOtelCheck(options)
      expect({ name, failed: result.status !== 0, output: result.output }).toEqual({
        name,
        failed: true,
        output: '',
      })
    }
  })

  test('publish-otel refuses an artifact that is not the verified OTel name and version, or an npm below 11.5.1', () => {
    for (const [name, options] of Object.entries({
      'another name': { manifest: { name: '@countly/kafkajs', version: OTEL_VERSION } },
      'another version': { manifest: { name: OTEL_NAME, version: '0.1.2' } },
      'npm 11.5.0': { npm: '11.5.0' },
    })) {
      const result = runOtelCheck(options)
      expect({
        name,
        failed: result.status !== 0,
        output: result.output,
        urls: result.urls,
      }).toEqual({ name, failed: true, output: '', urls: [] })
    }
  })
})
