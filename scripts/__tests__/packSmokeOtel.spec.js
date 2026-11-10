const { spawnSync } = require('child_process')
const fs = require('fs')
const os = require('os')
const path = require('path')

const SCRIPT = path.join(__dirname, '..', 'assertCleanTarball.sh')

describe('scripts/assertCleanTarball.sh', () => {
  let workDir
  let packed = 0

  beforeAll(() => {
    workDir = fs.mkdtempSync(path.join(os.tmpdir(), 'assert-clean-tarball-'))
  })

  afterAll(() => {
    fs.rmSync(workDir, { recursive: true, force: true })
  })

  /**
   * Packs only the requested entries under `package/`, without implicit parent directories,
   * as `npm pack` lays out shipped files.
   * @param {string[]} files paths below `package/`; one ending in `/` is an empty directory
   * @returns {string} the tarball's path
   */
  const pack = (files) => {
    const stage = path.join(workDir, `stage-${++packed}`)
    for (const file of files) {
      const target = path.join(stage, 'package', file)
      if (file.endsWith('/')) {
        fs.mkdirSync(target, { recursive: true })
      } else {
        fs.mkdirSync(path.dirname(target), { recursive: true })
        fs.writeFileSync(target, '')
      }
    }

    const tarball = `${stage}.tgz`
    expect(
      spawnSync('tar', ['-czf', tarball, '-C', stage, ...files.map((file) => `package/${file}`)])
        .status
    ).toBe(0)
    return tarball
  }

  /**
   * Runs the guard script through bash.
   * @param {...string} args script arguments
   * @returns {import('child_process').SpawnSyncReturns<string>} the finished process
   */
  const run = (...args) => spawnSync('bash', [SCRIPT, ...args], { encoding: 'utf8' })

  it('passes a tarball that holds only shipped files', () => {
    const result = run(
      pack([
        'index.js',
        'package.json',
        'src/index.js',
        'types/index.d.ts',
        'docs/ShareConsumer.md',
      ])
    )

    expect(result.status).toBe(0)
    expect(result.stderr).toBe('')
  })

  it('fails on a file of the otel sub-package and names it', () => {
    const result = run(pack(['index.js', 'otel/src/index.js']))

    expect(result.status).toBe(1)
    expect(result.stdout).toContain('package/otel/src/index.js')
    expect(result.stderr).toContain('otel/')
  })

  it('fails on the otel directory alone', () => {
    const result = run(pack(['index.js', 'otel/']))

    expect(result.status).toBe(1)
    expect(result.stderr).toContain('otel/')
  })

  it('does not take a path that merely ends in otel for the sub-package', () => {
    const result = run(
      pack(['src/hotel/index.js', 'src/otel/index.js', 'otel.js', 'docs/ShareConsumer.md'])
    )

    expect(result.status).toBe(0)
  })

  it.each([
    ['a spec file', 'src/admin/index.spec.js'],
    ['a __tests__ directory', 'src/admin/__tests__/helpers.js'],
    ['a __snapshots__ directory', 'src/admin/__snapshots__/index.snap'],
    ['a types test file', 'types/tests.ts'],
    ['a second types test file', 'types/tests.node16.ts'],
    ['a fixtures directory', 'src/protocol/requests/fetch/fixtures/java/v4.json'],
  ])('fails on %s', (_, file) => {
    const result = run(pack(['index.js', file]))

    expect(result.status).toBe(1)
    expect(result.stdout).toContain(`package/${file}`)
    expect(result.stderr).toContain('test files')
  })

  it('fails without a tarball argument', () => {
    const result = run()

    expect(result.status).toBe(2)
    expect(result.stderr).toContain('usage')
  })

  it('fails with more than one tarball argument', () => {
    const result = run('a.tgz', 'b.tgz')

    expect(result.status).toBe(2)
    expect(result.stderr).toContain('usage')
  })

  it('fails on a tarball tar cannot read instead of passing it as empty', () => {
    const notATarball = path.join(workDir, 'not-a-tarball.tgz')
    fs.writeFileSync(notATarball, 'not gzip')

    expect(run(notATarball).status).not.toBe(0)
    expect(run(path.join(workDir, 'missing.tgz')).status).not.toBe(0)
  })
})
