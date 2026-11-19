const { spawnSync } = require('child_process')
const fs = require('fs')
const os = require('os')
const path = require('path')

const SCRIPTS = path.join(__dirname, '..')

// The fake npm records each call, exits with FAKE_NPM_EXIT when set, and otherwise on `install`
// extracts every named tarball into node_modules, as npm would install it.
const FAKE_NPM = `#!/usr/bin/env node
const fs = require('fs')
const path = require('path')
const { execFileSync } = require('child_process')
const args = process.argv.slice(2)
fs.appendFileSync(process.env.FAKE_CALLS, 'npm ' + JSON.stringify(args) + '\\n')
if (process.env.FAKE_NPM_EXIT) process.exit(Number(process.env.FAKE_NPM_EXIT))
if (args[0] === 'install') {
  for (const tarball of args.filter((arg) => arg.endsWith('.tgz'))) {
    const manifest = JSON.parse(execFileSync('tar', ['-xOzf', tarball, 'package/package.json']))
    const target = path.join('node_modules', manifest.name)
    fs.mkdirSync(target, { recursive: true })
    execFileSync('tar', ['-xzf', tarball, '-C', target, '--strip-components=1'])
  }
}
`

describe('scripts/packSmoke.sh with a supplied tarball', () => {
  let workDir
  let root
  let calls
  let packed = 0

  beforeEach(() => {
    workDir = fs.mkdtempSync(path.join(os.tmpdir(), 'pack-smoke-artifact-'))
    root = path.join(workDir, 'root')
    calls = path.join(workDir, 'calls.log')
    fs.writeFileSync(calls, '')

    // A copy of the script under a fake root, whose smoke test and type checks are stubs.
    for (const dir of ['scripts', 'types', 'node_modules/.bin', 'bin']) {
      fs.mkdirSync(path.join(root, dir), { recursive: true })
    }
    fs.mkdirSync(path.join(workDir, 'pack'))
    fs.mkdirSync(path.join(workDir, 'tmp'))
    for (const script of ['packSmoke.sh', 'assertCleanTarball.sh']) {
      fs.copyFileSync(path.join(SCRIPTS, script), path.join(root, 'scripts', script))
    }
    for (const file of ['tests.node16.ts', 'tsconfig.node16.json', 'package-exports.mts']) {
      fs.writeFileSync(path.join(root, 'types', file), '')
    }
    fs.writeFileSync(
      path.join(root, 'scripts', 'packageSmoke.test.mjs'),
      [
        "import assert from 'node:assert/strict'",
        "import { test } from 'node:test'",
        "import { marker } from '@countly/kafkajs'",
        "test('the installed client is the supplied tarball', () => assert.equal(marker, 'supplied'))",
      ].join('\n')
    )
    fs.writeFileSync(
      path.join(root, 'node_modules/.bin/tsc'),
      '#!/bin/bash\necho "tsc $*" >> "$FAKE_CALLS"\n',
      { mode: 0o755 }
    )
    fs.writeFileSync(path.join(root, 'bin', 'npm'), FAKE_NPM, { mode: 0o755 })
  })

  afterEach(() => {
    fs.rmSync(workDir, { recursive: true, force: true })
  })

  /**
   * Packs a minimal client package the guard and the stub smoke test accept.
   * @param {string} [name] the package name in its package.json
   * @returns {string} the tarball's path
   */
  const packClient = (name = '@countly/kafkajs') => {
    const stage = path.join(workDir, `stage-${++packed}`)
    const files = {
      'package.json': JSON.stringify({
        name,
        version: '3.0.0-rc.0',
        main: 'index.js',
        exports: { '.': { import: './index.mjs', require: './index.js' } },
      }),
      'index.js': "module.exports = { oauthBearer: {}, marker: 'supplied' }",
      'index.mjs': "import cjs from './index.js'\nexport const { oauthBearer, marker } = cjs",
      'docs/ShareConsumer.md': '',
      'docs/ClientTelemetry.md': '',
    }
    for (const [file, content] of Object.entries(files)) {
      fs.mkdirSync(path.dirname(path.join(stage, 'package', file)), { recursive: true })
      fs.writeFileSync(path.join(stage, 'package', file), content)
    }
    const tarball = path.join(workDir, `client-${packed}.tgz`)
    const entries = Object.keys(files).map((file) => `package/${file}`)
    expect(spawnSync('tar', ['-czf', tarball, '-C', stage, ...entries]).status).toBe(0)
    return tarball
  }

  /**
   * Runs the copied script with the fake npm first on PATH and a private TMPDIR.
   * @param {string[]} args script arguments
   * @param {Record<string, string|undefined>} [env] overrides; PACK_DIR defaults to workDir/pack
   * @param {string} [cwd] the working directory
   * @returns {import('child_process').SpawnSyncReturns<string>} the finished process
   */
  const runWith = (args, env = {}, cwd = workDir) => {
    const merged = {
      ...process.env,
      PATH: `${path.join(root, 'bin')}${path.delimiter}${process.env.PATH}`,
      FAKE_CALLS: calls,
      TMPDIR: path.join(workDir, 'tmp'),
      PACK_DIR: path.join(workDir, 'pack'),
      ...env,
    }
    for (const key of Object.keys(merged)) if (merged[key] === undefined) delete merged[key]
    return spawnSync('bash', [path.join(root, 'scripts', 'packSmoke.sh'), ...args], {
      encoding: 'utf8',
      env: merged,
      cwd,
    })
  }

  /**
   * Runs the copied script with the caller-provided PACK_DIR workDir/pack.
   * @param {...string} args script arguments
   * @returns {import('child_process').SpawnSyncReturns<string>} the finished process
   */
  const run = (...args) => runWith(args)

  /** @returns {string[]} the entries left in the private TMPDIR */
  const leftInTmp = () => fs.readdirSync(path.join(workDir, 'tmp'))

  /** @returns {string[]} the recorded fake npm and tsc calls */
  const recorded = () => fs.readFileSync(calls, 'utf8').split('\n').filter(Boolean)

  it('smokes the supplied tarball and never runs npm pack', () => {
    const tarball = packClient()
    const result = run(tarball)

    expect({ status: result.status, stderr: result.stderr }).toEqual({ status: 0, stderr: '' })
    expect(result.stdout).toContain(`tarball: ${tarball}`)
    const npmCalls = recorded()
      .filter((line) => line.startsWith('npm '))
      .map((line) => JSON.parse(line.slice(4)))
    expect(npmCalls.some((args) => args.includes('pack'))).toBe(false)
    expect(npmCalls[0]).toEqual([
      'install',
      '--silent',
      '--no-audit',
      '--no-fund',
      '--ignore-scripts',
      tarball,
    ])
    expect(npmCalls).toHaveLength(2)
    expect(npmCalls[1]).toContain('@types/node@^24')
    expect(recorded().filter((line) => line.startsWith('tsc '))).toHaveLength(2)
    expect(fs.existsSync(path.join(workDir, 'pack', 'package.json'))).toBe(true)
  })

  it('installs the tarball and the Node types without lifecycle scripts', () => {
    const tarball = packClient()
    const result = run(tarball)

    expect({ status: result.status, stderr: result.stderr }).toEqual({ status: 0, stderr: '' })
    const installs = recorded()
      .filter((line) => line.startsWith('npm '))
      .map((line) => JSON.parse(line.slice(4)))
    expect(installs.map((args) => [args[0], args.includes('--ignore-scripts')])).toEqual([
      ['install', true],
      ['install', true],
    ])
  })

  it('rejects a missing tarball', () => {
    const missing = path.join(workDir, 'missing.tgz')
    const result = run(missing)

    expect(result.status).toBe(2)
    expect(result.stderr).toContain(`not a regular .tgz file: ${missing}`)
    expect(recorded()).toEqual([])
  })

  it('rejects more than one argument', () => {
    const result = run(packClient(), packClient())

    expect(result.status).toBe(2)
    expect(result.stderr).toContain('usage')
    expect(recorded()).toEqual([])
  })

  it('rejects a tarball of another package', () => {
    const tarball = packClient('kafkajs')
    const result = run(tarball)

    expect(result.status).toBe(2)
    expect(result.stderr).toContain(`${tarball} holds kafkajs, expected @countly/kafkajs`)
    expect(recorded()).toEqual([])
  })

  it('removes the project directory it created after a successful smoke', () => {
    const result = runWith([packClient()], { PACK_DIR: undefined })

    expect({ status: result.status, stderr: result.stderr }).toEqual({ status: 0, stderr: '' })
    expect(recorded().filter((line) => line.startsWith('tsc '))).toHaveLength(2)
    expect(leftInTmp()).toEqual([])
  })

  it('removes the project directory it created after a failing install', () => {
    const result = runWith([packClient()], { PACK_DIR: undefined, FAKE_NPM_EXIT: '41' })

    expect(result.status).toBe(41)
    expect(recorded()).toHaveLength(1)
    expect(leftInTmp()).toEqual([])
  })

  it('removes the project directory it created under a relative TMPDIR after a successful smoke', () => {
    const result = runWith([packClient()], { PACK_DIR: undefined, TMPDIR: 'tmp' })

    expect({ status: result.status, stderr: result.stderr }).toEqual({ status: 0, stderr: '' })
    expect(recorded().filter((line) => line.startsWith('tsc '))).toHaveLength(2)
    expect(leftInTmp()).toEqual([])
  })

  it('removes the project directory it created under a relative TMPDIR after a failing install', () => {
    const result = runWith([packClient()], {
      PACK_DIR: undefined,
      TMPDIR: 'tmp',
      FAKE_NPM_EXIT: '41',
    })

    expect(result.status).toBe(41)
    expect(recorded()).toHaveLength(1)
    expect(leftInTmp()).toEqual([])
  })

  it('creates no project directory for a rejected tarball', () => {
    const result = runWith([path.join(workDir, 'missing.tgz')], { PACK_DIR: undefined })

    expect(result.status).toBe(2)
    expect(leftInTmp()).toEqual([])
  })

  it('stops before any install or write when PACK_DIR cannot be entered', () => {
    const cwd = fs.mkdtempSync(path.join(workDir, 'cwd-'))
    const result = runWith([packClient()], { PACK_DIR: path.join(workDir, 'absent') }, cwd)

    expect(result.status).not.toBe(0)
    expect(recorded()).toEqual([])
    expect(fs.readdirSync(cwd)).toEqual([])
    expect(fs.existsSync(path.join(workDir, 'absent'))).toBe(false)
  })
})
