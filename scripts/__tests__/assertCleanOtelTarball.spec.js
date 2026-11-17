const { spawnSync } = require('child_process')
const { createHash } = require('crypto')
const fs = require('fs')
const os = require('os')
const path = require('path')

const SCRIPTS = path.join(__dirname, '..')
const OTEL = path.join(SCRIPTS, '..', 'otel')
const MANIFEST = JSON.parse(fs.readFileSync(path.join(OTEL, 'package.json'), 'utf8'))

/** @returns {Record<string, string>} the shipped OTel files below `package/` and their contents */
const completeFiles = () => {
  const files = {}
  for (const file of ['package.json', 'LICENSE', 'README.md']) {
    files[file] = fs.readFileSync(path.join(OTEL, file), 'utf8')
  }
  for (const file of fs.readdirSync(path.join(OTEL, 'src'), { recursive: true })) {
    const source = path.join(OTEL, 'src', file)
    if (fs.statSync(source).isFile()) files[`src/${file}`] = fs.readFileSync(source, 'utf8')
  }
  return files
}

let workDir
let packed = 0

beforeAll(() => {
  workDir = fs.mkdtempSync(path.join(os.tmpdir(), 'assert-clean-otel-tarball-'))
})

afterAll(() => {
  fs.rmSync(workDir, { recursive: true, force: true })
})

/**
 * Packs the given files under `package/`, file entries only, as `npm pack` lays them out.
 * @param {Record<string, string>} files contents by path below `package/`
 * @returns {string} the tarball's path
 */
const pack = (files) => {
  const stage = path.join(workDir, `stage-${++packed}`)
  for (const [file, content] of Object.entries(files)) {
    fs.mkdirSync(path.dirname(path.join(stage, 'package', file)), { recursive: true })
    fs.writeFileSync(path.join(stage, 'package', file), content)
  }
  const tarball = `${stage}.tgz`
  const entries = Object.keys(files).map((file) => `package/${file}`)
  expect(spawnSync('tar', ['-czf', tarball, '-C', stage, ...entries]).status).toBe(0)
  return tarball
}

/**
 * Runs one of the scripts through bash.
 * @param {string} script the script's file name in scripts/
 * @param {...string} args script arguments
 * @returns {import('child_process').SpawnSyncReturns<string>} the finished process
 */
const run = (script, ...args) =>
  spawnSync('bash', [path.join(SCRIPTS, script), ...args], { encoding: 'utf8' })

describe('scripts/assertCleanOtelTarball.sh', () => {
  it('passes a complete OTel tarball', () => {
    const result = run('assertCleanOtelTarball.sh', pack(completeFiles()))

    expect({ status: result.status, stderr: result.stderr }).toEqual({ status: 0, stderr: '' })
  })

  it('fails an OTel tarball with a test file', () => {
    const result = run(
      'assertCleanOtelTarball.sh',
      pack({ ...completeFiles(), 'test/kafkajs.test.js': '' })
    )

    expect(result.status).toBe(1)
    expect(result.stdout).toContain('package/test/kafkajs.test.js')
    expect(result.stderr).toContain('tarball contains test files')
  })

  it('fails an OTel tarball whose version differs from otel/package.json', () => {
    const manifest = JSON.stringify({ ...MANIFEST, version: '9.9.9' }, null, 2)
    const result = run(
      'assertCleanOtelTarball.sh',
      pack({ ...completeFiles(), 'package.json': manifest })
    )

    expect(result.status).toBe(1)
    expect(result.stderr).toContain(`version 9.9.9, expected ${MANIFEST.version}`)
  })

  it('fails an OTel tarball that contains main-package files', () => {
    const result = run(
      'assertCleanOtelTarball.sh',
      pack({ ...completeFiles(), 'index.js': '', 'types/index.d.ts': '' })
    )

    expect(result.status).toBe(1)
    expect(result.stdout).toContain('package/index.js')
    expect(result.stdout).toContain('package/types/index.d.ts')
    expect(result.stderr).toContain('tarball contains files outside the instrumentation package')
  })

  it('fails an OTel tarball whose src holds files otel/src does not have', () => {
    const files = {
      ...completeFiles(),
      'src/producer/sendMessages.js': fs.readFileSync(
        path.join(SCRIPTS, '..', 'src', 'producer', 'sendMessages.js'),
        'utf8'
      ),
      'src/accidental.js': '',
    }
    const result = run('assertCleanOtelTarball.sh', pack(files))

    expect(result.status).toBe(1)
    expect(result.stderr).toContain('tarball contains files otel/src does not have')
    expect(result.stdout).toContain('package/src/producer/sendMessages.js')
    expect(result.stdout).toContain('package/src/accidental.js')
  })

  it('fails an OTel tarball whose optionalDependencies differ from otel/package.json', () => {
    const manifest = JSON.stringify(
      { ...MANIFEST, optionalDependencies: { kafkajs: '2.2.4' } },
      null,
      2
    )
    const result = run(
      'assertCleanOtelTarball.sh',
      pack({ ...completeFiles(), 'package.json': manifest })
    )

    expect(result.status).toBe(1)
    expect(result.stderr).toContain('optionalDependencies differ from otel/package.json')
  })
})

describe('scripts/sri.sh', () => {
  it("sri.sh prints the release workflow's integrity format", () => {
    const file = path.join(workDir, 'artifact.tgz')
    const bytes = Buffer.from(Array.from({ length: 4096 }, (_, i) => (i * 31) % 256))
    fs.writeFileSync(file, bytes)
    const result = run('sri.sh', file)

    expect(result.status).toBe(0)
    expect(result.stdout).toBe(`sha512-${createHash('sha512').update(bytes).digest('base64')}\n`)
  })

  it.each(['openssl', 'base64'])('sri.sh fails and prints no SRI when %s fails', (command) => {
    const file = path.join(workDir, 'artifact.tgz')
    fs.writeFileSync(file, 'bytes')
    const bin = fs.mkdtempSync(path.join(workDir, 'bin-'))
    fs.writeFileSync(path.join(bin, command), '#!/bin/bash\necho "stub failure" >&2\nexit 37\n', {
      mode: 0o755,
    })
    const result = spawnSync('bash', [path.join(SCRIPTS, 'sri.sh'), file], {
      encoding: 'utf8',
      env: { ...process.env, PATH: `${bin}${path.delimiter}${process.env.PATH}` },
    })

    expect(result.status).toBe(37)
    expect(result.stdout).toBe('')
    expect(result.stderr).toContain('stub failure')
  })
})
