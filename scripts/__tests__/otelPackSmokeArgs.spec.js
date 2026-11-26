const { spawnSync } = require('child_process')
const fs = require('fs')
const os = require('os')
const path = require('path')

const ROOT = path.join(__dirname, '..', '..')
const SCRIPT = path.join(ROOT, 'scripts', 'otelPackSmoke.sh')
const OTEL_NAME = '@countly/opentelemetry-instrumentation-kafkajs'
const MAIN_NAME = '@countly/kafkajs'

// The fake npm records each call with its arguments and its project directory, and on `install`
// extracts every named tarball into node_modules and records it in package-lock.json as a file
// dependency, as npm would install it. A tarball named `kafkajs@file:<path>` is installed as the
// alias \`kafkajs\`. Installing typescript leaves a stub tsc that records its arguments and fails
// for the fixture named by FAKE_TSC_FAIL.
const FAKE_NPM = `#!/usr/bin/env node
const fs = require('fs')
const path = require('path')
const { execFileSync } = require('child_process')
const args = process.argv.slice(2)
fs.appendFileSync(process.env.FAKE_CALLS, 'npm ' + JSON.stringify({ args, cwd: process.cwd() }) + '\\n')
if (args[0] === 'install') {
  const lock = { lockfileVersion: 3, packages: {} }
  for (const arg of args.filter((arg) => arg.endsWith('.tgz'))) {
    const alias = arg.startsWith('kafkajs@file:')
    const tarball = alias ? arg.slice('kafkajs@file:'.length) : arg
    const manifest = JSON.parse(execFileSync('tar', ['-xOzf', tarball, 'package/package.json']))
    const name = alias ? 'kafkajs' : manifest.name
    const target = path.join('node_modules', name)
    fs.mkdirSync(target, { recursive: true })
    execFileSync('tar', ['-xzf', tarball, '-C', target, '--strip-components=1'])
    lock.packages['node_modules/' + name] = { version: manifest.version, resolved: 'file:' + tarball }
  }
  fs.writeFileSync('package-lock.json', JSON.stringify(lock))
  if (args.some((arg) => arg.startsWith('typescript@'))) {
    fs.mkdirSync('node_modules/.bin', { recursive: true })
    const tsc = '#!/bin/sh\\necho "tsc $*" >> "' + process.env.FAKE_CALLS + '"\\n' +
      '[ "$*" = "-p tsconfig.$FAKE_TSC_FAIL.json" ] && exit 1\\nexit 0\\n'
    fs.writeFileSync('node_modules/.bin/tsc', tsc, { mode: 0o755 })
  }
}
`

// The fake node records a smoke child and stands in for it, and answers the synchronous-hooks
// probe with FAKE_PROBE and the exit status FAKE_PROBE_STATUS; every other call runs the real node.
const FAKE_NODE = `#!/bin/bash
for arg in "$@"; do
  case "$arg" in
    otel-smoke.cjs|otel-smoke.mjs|otel-static.mjs|otel-alias.cjs|otel-alias.mjs)
      echo "node $*" >> "$FAKE_CALLS"; exit 0 ;;
    --input-type=module) printf '%s' "$FAKE_PROBE"; exit "$FAKE_PROBE_STATUS" ;;
  esac
done
exec "$REAL_NODE" "$@"
`

// One strict type fixture per declaration form, compiled apart, in the order the script runs them.
const TYPE_FIXTURES = [
  'buffer',
  'offset',
  'date',
  'timestamp-long',
  'timestamp-null',
  'timestamp-undefined',
  'null-header',
  'header-read',
  'producer-hook',
  'consumer-hook',
]
const FIXTURE_COMPILES = TYPE_FIXTURES.map((name) => `tsc -p tsconfig.${name}.json`)

describe('scripts/otelPackSmoke.sh', () => {
  let workDir
  let bin
  let calls
  let packed = 0

  beforeEach(() => {
    workDir = fs.mkdtempSync(path.join(os.tmpdir(), 'otel-pack-smoke-args-'))
    bin = path.join(workDir, 'bin')
    calls = path.join(workDir, 'calls.log')
    fs.mkdirSync(bin)
    fs.writeFileSync(calls, '')
    fs.writeFileSync(path.join(bin, 'npm'), FAKE_NPM, { mode: 0o755 })
    fs.writeFileSync(path.join(bin, 'node'), FAKE_NODE, { mode: 0o755 })
  })

  afterEach(() => {
    fs.rmSync(workDir, { recursive: true, force: true })
  })

  /**
   * Packs a package holding only its package.json.
   * @param {string} name the package name
   * @param {string} version the package version
   * @returns {string} the tarball's path
   */
  const packManifest = (name, version) => {
    const stage = path.join(workDir, `stage-${++packed}`)
    fs.mkdirSync(path.join(stage, 'package'), { recursive: true })
    fs.writeFileSync(path.join(stage, 'package', 'package.json'), JSON.stringify({ name, version }))
    const tarball = path.join(workDir, `package-${packed}.tgz`)
    expect(spawnSync('tar', ['-czf', tarball, '-C', stage, 'package/package.json']).status).toBe(0)
    return tarball
  }

  /**
   * Runs the script from workDir with the fake npm and node first on PATH.
   * @param {string[]} args script arguments
   * @param {Record<string, string>} [env] environment overrides
   * @returns {import('child_process').SpawnSyncReturns<string>} the finished process
   */
  const runWith = (args, env = {}) =>
    spawnSync('bash', [SCRIPT, ...args], {
      encoding: 'utf8',
      cwd: workDir,
      env: {
        ...process.env,
        PATH: `${bin}${path.delimiter}${process.env.PATH}`,
        FAKE_CALLS: calls,
        REAL_NODE: process.execPath,
        FAKE_PROBE: 'true',
        FAKE_PROBE_STATUS: '0',
        FAKE_TSC_FAIL: '',
        ...env,
      },
    })

  /**
   * Runs the script with the default environment.
   * @param {...string} args script arguments
   * @returns {import('child_process').SpawnSyncReturns<string>} the finished process
   */
  const run = (...args) => runWith(args)

  /** @returns {string[]} the recorded fake npm and node calls */
  const recorded = () => fs.readFileSync(calls, 'utf8').split('\n').filter(Boolean)

  /** @returns {{ args: string[], cwd: string }[]} the recorded fake npm calls */
  const npmRecorded = () =>
    recorded()
      .filter((line) => line.startsWith('npm '))
      .map((line) => JSON.parse(line.slice(4)))

  /** @returns {string[]} the recorded smoke children, as their command lines */
  const nodeRecorded = () => recorded().filter((line) => line.startsWith('node '))

  it('otelPackSmoke installs exactly the two supplied tarballs and never runs npm pack', () => {
    const otel = packManifest(OTEL_NAME, '0.1.1')
    const main = packManifest(MAIN_NAME, '3.0.0-rc.0')
    const result = run(otel, main)

    expect({ status: result.status, stderr: result.stderr }).toEqual({ status: 0, stderr: '' })
    // The project install, the TypeScript and types install, and the alias-only project install.
    expect(npmRecorded()).toHaveLength(3)
    const [install, types, aliasOnly] = npmRecorded().map(({ args }) => args)
    expect(install[0]).toBe('install')
    expect(install).toContain('--save')
    expect(install.filter((arg) => arg.endsWith('.tgz')).sort()).toEqual([main, otel].sort())
    expect(install.filter((arg) => !arg.startsWith('-') && !arg.endsWith('.tgz'))).toEqual([
      'install',
      expect.stringMatching(/^@opentelemetry\/api@\d+\.\d+\.\d+$/),
      expect.stringMatching(/^@opentelemetry\/sdk-trace@\d+\.\d+\.\d+$/),
    ])
    const rootLock = JSON.parse(fs.readFileSync(path.join(ROOT, 'package-lock.json'), 'utf8'))
    expect(types.filter((arg) => !arg.startsWith('-'))).toEqual([
      'install',
      `typescript@${rootLock.packages['node_modules/typescript'].version}`,
      expect.stringMatching(/^@types\/node@\^\d+$/),
    ])
    expect(types).toContain('--no-save')
    expect(aliasOnly.filter((arg) => arg.endsWith('.tgz')).sort()).toEqual(
      [`kafkajs@file:${main}`, otel].sort()
    )
    expect(aliasOnly).toContain('--ignore-scripts')
    const hook = '--experimental-loader=@opentelemetry/instrumentation/hook.mjs'
    expect(recorded().filter((line) => line.startsWith('node '))).toEqual([
      'node otel-smoke.cjs',
      `node ${hook} otel-smoke.mjs`,
      `node ${hook} --import ./otel-preload.mjs otel-static.mjs`,
      'node otel-alias.cjs',
      `node ${hook} otel-alias.mjs alias-esm-loader`,
      'node --import ./otel-register.mjs otel-alias.mjs alias-esm-register',
    ])
    expect(recorded().filter((line) => line.startsWith('tsc '))).toEqual(FIXTURE_COMPILES)
  })

  it('otelPackSmoke installs its two projects as siblings with one npm cache inside the temporary directory', () => {
    const result = run(packManifest(OTEL_NAME, '0.1.1'), packManifest(MAIN_NAME, '3.0.0-rc.0'))

    expect({ status: result.status, stderr: result.stderr }).toEqual({ status: 0, stderr: '' })
    const [install, types, aliasOnly] = npmRecorded()
    const work = path.dirname(install.cwd)
    expect(path.basename(install.cwd)).toBe('scoped')
    expect(types.cwd).toBe(install.cwd)
    expect(aliasOnly.cwd).toBe(path.join(work, 'alias-only'))
    for (const { args } of [install, types, aliasOnly]) {
      expect(args).toContain(`--cache=${path.join(work, 'npm-cache')}`)
    }
    expect(types.args).toContain('--no-save')
  })

  it('otelPackSmoke skips the synchronous-hooks child when the probe answers false', () => {
    const otel = packManifest(OTEL_NAME, '0.1.1')
    const main = packManifest(MAIN_NAME, '3.0.0-rc.0')
    const result = runWith([otel, main], { FAKE_PROBE: 'false' })

    expect(result.status).toBe(0)
    expect(result.stdout).toContain(
      'synchronous module hooks are not supported on this Node.js: skipped'
    )
    expect(nodeRecorded()).toHaveLength(5)
    expect(nodeRecorded()).not.toContain(
      'node --import ./otel-register.mjs otel-alias.mjs alias-esm-register'
    )
  })

  it('otelPackSmoke fails when the synchronous-hooks probe prints anything but true or false or exits nonzero', () => {
    const otel = packManifest(OTEL_NAME, '0.1.1')
    const main = packManifest(MAIN_NAME, '3.0.0-rc.0')

    for (const output of ['', 'undefined', 'maybe', 'true false']) {
      const result = runWith([otel, main], { FAKE_PROBE: output })
      expect(result.status).toBe(1)
      expect(result.stderr).toContain(
        `the synchronous-hooks probe printed '${output}', expected true or false`
      )
    }
    const failed = runWith([otel, main], { FAKE_PROBE: 'true', FAKE_PROBE_STATUS: '3' })
    expect(failed.status).toBe(3)
    expect(nodeRecorded().filter((line) => line.includes('alias-esm-register'))).toEqual([])
  })

  it('otelPackSmoke compiles every type fixture on its own and names each one that fails', () => {
    const otel = packManifest(OTEL_NAME, '0.1.1')
    const main = packManifest(MAIN_NAME, '3.0.0-rc.0')
    const result = runWith([otel, main], { FAKE_TSC_FAIL: 'offset' })

    expect(result.status).toBe(1)
    expect(result.stderr).toContain('type fixtures failed: offset')
    expect(recorded().filter((line) => line.startsWith('tsc '))).toEqual(FIXTURE_COMPILES)
    expect(nodeRecorded().filter((line) => line.includes('otel-alias'))).toEqual([])
  })

  it('otelPackSmoke rejects one argument, three arguments or swapped package names', () => {
    const otel = packManifest(OTEL_NAME, '0.1.1')
    const main = packManifest(MAIN_NAME, '3.0.0-rc.0')

    for (const args of [[otel], [otel, main, main]]) {
      const result = run(...args)
      expect(result.status).toBe(2)
      expect(result.stderr).toContain('usage')
    }
    const swapped = run(main, otel)
    expect(swapped.status).toBe(2)
    expect(swapped.stderr).toContain(`${main} holds ${MAIN_NAME}, expected ${OTEL_NAME}`)
    expect(recorded()).toEqual([])
  })

  it('otelPackSmoke removes its project directory under a relative TMPDIR', () => {
    fs.mkdirSync(path.join(workDir, 'tmp'))
    const result = runWith(
      [packManifest(OTEL_NAME, '0.1.1'), packManifest(MAIN_NAME, '3.0.0-rc.0')],
      { TMPDIR: 'tmp' }
    )

    expect({ status: result.status, stderr: result.stderr }).toEqual({ status: 0, stderr: '' })
    expect(recorded().filter((line) => line.startsWith('node '))).toHaveLength(6)
    expect(fs.readdirSync(path.join(workDir, 'tmp'))).toEqual([])
  })
})
