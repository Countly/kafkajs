const fs = require('fs')
const os = require('os')
const path = require('path')
const { spawnSync } = require('child_process')

const ROOT = path.resolve(__dirname, '..', '..', '..')
const SWEEPS = path.join(ROOT, 'scripts', 'ci', 'sweeps.sh')
const BASE = '3.0.0-alpha.9b'
/** Built from parts so this file does not itself match the label sweep it feeds. */
const LABEL_LINE = `${['Phase', '9'].join(' ')} baseline\n`
const PLAN_ID = `9${'F'}-12`

/** Shipped markdown outside docs/ that the sweep also reads. */
const GOVERNANCE = [
  'README.md',
  'RELEASING.md',
  'SECURITY.md',
  'QUARANTINE.md',
  'CONTRIBUTING.md',
  'otel/README.md',
]

/** The real tree's shipped guides: the files the sweep reads, so the clean case is the repository's own text. */
const realDocs = () => {
  const files = {}
  for (const name of GOVERNANCE) files[name] = fs.readFileSync(path.join(ROOT, name), 'utf8')
  for (const name of fs.readdirSync(path.join(ROOT, 'docs')))
    if (name.endsWith('.md'))
      files[`docs/${name}`] = fs.readFileSync(path.join(ROOT, 'docs', name), 'utf8')
  return files
}

/**
 * Commits `files` in a fresh repository, tags the commit as the sweep's baseline and runs the real
 * `sweeps.sh` there. The tree is far smaller than the real one, so the verdict line is not asserted:
 * only the shipped-doc section of the report is.
 * @param {Record<string, string>} files path to text of every tracked file
 * @returns {string} the shipped-doc section of the report, empty when the sweeps wrote none
 */
const sweep = (files) => {
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
    // The baseline label grep must find something, or the sweeps stop before the shipped-doc section.
    const tracked = { 'CHANGELOG.md': LABEL_LINE, ...files }
    for (const [name, text] of Object.entries(tracked)) {
      fs.mkdirSync(path.dirname(path.join(dir, name)), { recursive: true })
      fs.writeFileSync(path.join(dir, name), text)
    }
    git('add', '.')
    git('commit', '-q', '-m', 'baseline')
    git('tag', BASE)
    spawnSync('bash', [SWEEPS, 'out', BASE], { cwd: dir, encoding: 'utf8' })
    const report = fs.readFileSync(path.join(dir, 'out', 'sweeps.txt'), 'utf8')
    return (
      report.split('\n== ').find((part) => part.replace(/^== /, '').startsWith('shipped-doc')) || ''
    )
  } finally {
    fs.rmSync(dir, { recursive: true, force: true })
  }
}

describe('scripts/ci/sweeps.sh shipped-doc identifiers', () => {
  it('passes the shipped README and guides of this tree', () => {
    const section = sweep(realDocs())

    expect(section).toContain('shipped-doc: 0 hit(s)')
    expect(section).not.toContain('-- shipped-doc:')
  })

  it.each([
    ['an acceptance id in a guide', 'docs/Testing.md', 'Measured acceptance A20 is pending.\n'],
    ['a feature id in the README', 'README.md', 'Needs F08 evidence.\n'],
    ['a benchmark phase name in a guide', 'docs/Other.md', 'Runs in P10 CI.\n'],
    ['a plan id in a guide', 'docs/Other.md', `Closed in ${PLAN_ID}.\n`],
    ['a decision id in a guide', 'docs/Other.md', 'Under D142, lanes block 3.0.0.\n'],
    ['an evidence id in a guide', 'docs/Other.md', 'The E14 direct execution contracts.\n'],
    ['a requirement id in a guide', 'docs/Other.md', 'The mandatory O17 matrix runs here.\n'],
    ['an id in the release guide', 'RELEASING.md', 'Needs D149 and A20.\n'],
    ['an id in the security policy', 'SECURITY.md', 'See F08.\n'],
    ['an id in the quarantine list', 'QUARANTINE.md', 'Waits on P10.\n'],
    ['an id in the contributing guide', 'CONTRIBUTING.md', 'Per D200.\n'],
    ['an id in the OpenTelemetry README', 'otel/README.md', 'Covered by O17.\n'],
    ['a maintainer machine in a guide', 'docs/Other.md', 'Measured on the development host.\n'],
    ['a run controller in a guide', 'docs/Other.md', 'The controller supplies the guard.\n'],
    ['a local-run rule in a guide', 'docs/Other.md', 'Local acceptance uses Node 26.\n'],
    ['a private phase process in a guide', 'docs/Other.md', 'It does not block a phase merge.\n'],
    ['a private branch prefix in a guide', 'docs/Other.md', 'Runs on p9m/ pull requests.\n'],
  ])('fails on %s', (_name, file, text) => {
    const section = sweep({ ...realDocs(), [file]: text })

    expect(section).toContain('shipped-doc: 1 hit(s)')
    expect(section).toContain(`${file}:${text.trim()}`)
    expect(section).toContain('-- shipped-doc: not an enumerated exception:')
  })

  it('does not read the generated compatibility matrix', () => {
    const section = sweep({ ...realDocs(), 'docs/KafkaCompatibility.md': 'Row A20 and F08.\n' })

    expect(section).toContain('shipped-doc: 0 hit(s)')
    expect(section).not.toContain('-- shipped-doc:')
  })

  it('does not flag longer words that only contain an id', () => {
    const section = sweep({
      ...realDocs(),
      'docs/Other.md': 'Use A200 or SF08 or P100 or D1420 or E164 or O170 or XD142.\n',
    })

    expect(section).toContain('shipped-doc: 0 hit(s)')
  })

  it('does not flag names that look like ids but are real terms', () => {
    const section = sweep({
      ...realDocs(),
      'docs/Other.md':
        'LZ4 V8 RE2 INT32 RS256 ES384 A256GCM C0 C1 H2 S1 S2 L1 L2 L3 D2 E2 O1 p99 P99.\n',
    })

    expect(section).toContain('shipped-doc: 0 hit(s)')
  })
})
