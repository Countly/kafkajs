const fs = require('fs')
const os = require('os')
const path = require('path')
const { spawnSync } = require('child_process')

const ROOT = path.resolve(__dirname, '..', '..', '..')
const SWEEPS = path.join(ROOT, 'scripts', 'ci', 'sweeps.sh')
const BASE = '3.0.0-alpha.9b'
/** Built from parts so this file does not itself match the label sweep it feeds. A heading, so prose checks skip it. */
const PHASE = 'Phase'
const LABEL_LINE = `### ${[PHASE, '9'].join(' ')} baseline\n`

/** The files of this tree whose store reads the sweep enumerates. */
const STORE_FILES = [
  'src/consumer/index.js',
  'src/consumer/runner.js',
  'src/consumer/consumerGroup.js',
  'src/producer/eosManager/index.js',
]

/** @returns {Record<string, string>} path to text of each file the sweep's list names */
const realStoreFiles = () =>
  Object.fromEntries(
    STORE_FILES.map((name) => [name, fs.readFileSync(path.join(ROOT, name), 'utf8')])
  )

/** @returns {string[]} the `file:text` lines the sweep enumerates, read from the script itself */
const listed = () => {
  const script = fs.readFileSync(SWEEPS, 'utf8')
  const start = script.indexOf("AMBIENT_STORE=$(cat <<'LIST'\n")
  if (start === -1) return []
  const body = script.slice(script.indexOf('\n', start) + 1)
  return body.slice(0, body.indexOf('\nLIST\n')).split('\n')
}

/**
 * Commits `files` in a fresh repository, tags the commit as the sweep's baseline and runs the real
 * `sweeps.sh` there. The tree is far smaller than the real one, so the verdict line is not asserted:
 * only the named section of the report is.
 * @param {Record<string, string>} files path to text of every tracked file
 * @param {string} name the report section to return
 * @returns {string} that section of the report, empty when the sweeps wrote none
 */
const sweep = (files, name) => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'sweeps-'))
  const git = (...args) =>
    spawnSync(
      'git',
      ['-c', 'user.name=t', '-c', 'user.email=t@t', '-c', 'commit.gpgsign=false', ...args],
      {
        cwd: dir,
        encoding: 'utf8',
      }
    )
  try {
    git('init', '-q')
    // The baseline label grep must find something, or the sweeps stop before the named section.
    const tracked = { ...files, 'CHANGELOG.md': LABEL_LINE }
    for (const [file, text] of Object.entries(tracked)) {
      fs.mkdirSync(path.dirname(path.join(dir, file)), { recursive: true })
      fs.writeFileSync(path.join(dir, file), text)
    }
    git('add', '.')
    git('commit', '-q', '-m', 'baseline')
    git('tag', BASE)
    spawnSync('bash', [SWEEPS, 'out', BASE], { cwd: dir, encoding: 'utf8' })
    const report = fs.readFileSync(path.join(dir, 'out', 'sweeps.txt'), 'utf8')
    return (
      report.split('\n== ').find((part) => part.replace(/^== /, '').startsWith(`${name}:`)) || ''
    )
  } finally {
    fs.rmSync(dir, { recursive: true, force: true })
  }
}

describe('scripts/ci/sweeps.sh ambient store reads', () => {
  it('passes the store reads of this tree', () => {
    const exceptions = listed()
    const section = sweep(realStoreFiles(), 'ambient-store')

    expect(exceptions.length).toBeGreaterThan(0)
    expect(section).toContain(`ambient-store: ${exceptions.length} hit(s)\n`)
    expect(section.split('\n').slice(1).filter(Boolean).sort()).toEqual([...exceptions].sort())
  })

  it.each([
    ['a raw handler-store read', 'const batch = handlerContext.current()'],
    ['a raw callback-store read', 'if (RebalanceHooks.current()) stop()'],
    ['an ambient refusal', 'const refusal = RebalanceHooks.refusal()'],
    ['a destructured import', "const { current } = require('./handlerContext')"],
  ])('fails on %s', (_name, line) => {
    const section = sweep(
      { ...realStoreFiles(), 'src/consumer/extra.js': `${line}\n` },
      'ambient-store'
    )

    expect(section).toContain(
      `-- ambient-store: not an enumerated exception:\nsrc/consumer/extra.js:${line}`
    )
  })

  it('reports a listed read that the code no longer has', () => {
    const files = realStoreFiles()
    delete files['src/producer/eosManager/index.js']
    const section = sweep(files, 'ambient-store')
    const eosRead = listed().find((line) => line.startsWith('src/producer/eosManager/index.js:'))

    expect(eosRead).toBe(
      'src/producer/eosManager/index.js:      const identity = handlerContext.current()'
    )
    expect(section).toContain(`-- ambient-store: an exception no hit uses:\n${eosRead}`)
  })

  it('does not count a read that passes its own record', () => {
    const section = sweep(
      {
        ...realStoreFiles(),
        'src/consumer/extra.js': 'const refusal = RebalanceHooks.refusal(record)\n',
      },
      'ambient-store'
    )

    expect(section).toContain(`ambient-store: ${listed().length} hit(s)\n`)
    expect(section).not.toContain('-- ambient-store:')
  })
})
