const { spawnSync } = require('child_process')
const fs = require('fs')
const os = require('os')
const path = require('path')

const SCRIPT = path.join(__dirname, '..', 'assertCleanTarball.sh')
const LAYOUT = 'src/protocol/requests/describeCluster/layout.json'
const REFERENCE = 'src/protocol/recordBatch/record/v0/decoderEquality.reference.json'
const COMPLETE = [
  'package.json',
  'index.js',
  'src/protocol/requests/describeCluster/index.js',
  'docs/ShareConsumer.md',
  'docs/ClientTelemetry.md',
]

describe('scripts/assertCleanTarball.sh spec-only JSON', () => {
  let workDir
  let packed = 0

  beforeAll(() => {
    workDir = fs.mkdtempSync(path.join(os.tmpdir(), 'assert-clean-tarball-json-'))
  })

  afterAll(() => {
    fs.rmSync(workDir, { recursive: true, force: true })
  })

  /**
   * Builds a tarball laid out as `npm pack` writes it: file entries under `package/`, no directory entries.
   * @param {string[]} files paths below `package/`
   * @returns {string} the tarball's path
   */
  const pack = (files) => {
    const stage = path.join(workDir, `stage-${++packed}`)
    for (const file of files) {
      const target = path.join(stage, 'package', file)
      fs.mkdirSync(path.dirname(target), { recursive: true })
      fs.writeFileSync(target, '{}')
    }

    const tarball = `${stage}.tgz`
    const entries = files.map((file) => `package/${file}`)
    const tar = spawnSync('tar', ['-czf', tarball, '-C', stage, ...entries])
    expect(tar.status).toBe(0)
    return tarball
  }

  /**
   * Runs the guard script on one tarball.
   * @param {string} tarball the tarball's path
   * @returns {import('child_process').SpawnSyncReturns<string>} the finished process
   */
  const run = (tarball) => spawnSync('bash', [SCRIPT, tarball], { encoding: 'utf8' })

  it('fails a tarball containing a layout.json', () => {
    const result = run(pack([...COMPLETE, LAYOUT]))

    expect(result.status).toBe(1)
    expect(result.stdout).toContain(`package/${LAYOUT}`)
    expect(result.stderr).toContain('tarball contains test files')
  })

  it('fails a tarball containing a .reference.json', () => {
    const result = run(pack([...COMPLETE, REFERENCE]))

    expect(result.status).toBe(1)
    expect(result.stdout).toContain(`package/${REFERENCE}`)
    expect(result.stderr).toContain('tarball contains test files')
  })

  it('passes a complete tarball without them', () => {
    const result = run(pack(COMPLETE))

    expect(result.status).toBe(0)
    expect(result.stdout).toBe('')
    expect(result.stderr).toBe('')
  })
})
