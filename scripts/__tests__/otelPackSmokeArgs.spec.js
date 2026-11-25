const { spawnSync } = require('child_process')
const fs = require('fs')
const os = require('os')
const path = require('path')

const ROOT = path.join(__dirname, '..', '..')
const SCRIPT = path.join(ROOT, 'scripts', 'otelPackSmoke.sh')
const OTEL_NAME = '@countly/opentelemetry-instrumentation-kafkajs'
const MAIN_NAME = '@countly/kafkajs'

// The fake npm records each call, and on `install` extracts every named tarball into node_modules
// and records it in package-lock.json as a file dependency, as npm would install it. A tarball
// named `kafkajs@file:<path>` is installed as the alias \`kafkajs\`. Installing typescript leaves a
// stub tsc that records its arguments.
const FAKE_NPM = `#!/usr/bin/env node
const fs = require('fs')
const path = require('path')
const { execFileSync } = require('child_process')
const args = process.argv.slice(2)
fs.appendFileSync(process.env.FAKE_CALLS, 'npm ' + JSON.stringify(args) + '\\n')
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
    fs.writeFileSync('node_modules/.bin/tsc', '#!/bin/sh\\necho "tsc $*" >> "' + process.env.FAKE_CALLS + '"\\n', { mode: 0o755 })
  }
}
`

// The fake node records a smoke child and stands in for it, and answers the synchronous-hooks
// probe with true; every other call runs the real node.
const FAKE_NODE = `#!/bin/bash
for arg in "$@"; do
  case "$arg" in
    otel-smoke.cjs|otel-smoke.mjs|otel-static.mjs|otel-alias.cjs|otel-alias.mjs)
      echo "node $*" >> "$FAKE_CALLS"; exit 0 ;;
    --input-type=module) echo true; exit 0 ;;
  esac
done
exec "$REAL_NODE" "$@"
`

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

  it('otelPackSmoke installs exactly the two supplied tarballs and never runs npm pack', () => {
    const otel = packManifest(OTEL_NAME, '0.1.1')
    const main = packManifest(MAIN_NAME, '3.0.0-rc.0')
    const result = run(otel, main)

    expect({ status: result.status, stderr: result.stderr }).toEqual({ status: 0, stderr: '' })
    const npmCalls = recorded()
      .filter((line) => line.startsWith('npm '))
      .map((line) => JSON.parse(line.slice(4)))
    // The project install, the TypeScript and types install, and the alias-only project install.
    expect(npmCalls).toHaveLength(3)
    const [install, types, aliasOnly] = npmCalls
    expect(install[0]).toBe('install')
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
    expect(recorded().filter((line) => line.startsWith('tsc '))).toEqual(['tsc -p .'])
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
