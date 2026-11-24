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
const PLAN_ID = `9${'F'}-12`
/** A short hyphenated machine name, joined at run time so this file never spells one. */
const SHORT_MACHINE = ['dev', 'machine'].join('-')

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

/** The real tree's changelog, scripts' Markdown and workflows: the other files the wording sweep reads. */
const realExtras = () => {
  const listed = spawnSync('git', ['ls-files', '--', 'scripts/*.md', '.github/workflows/*.yml'], {
    cwd: ROOT,
    encoding: 'utf8',
  })
  const files = { 'CHANGELOG.md': fs.readFileSync(path.join(ROOT, 'CHANGELOG.md'), 'utf8') }
  for (const name of listed.stdout.split('\n').filter(Boolean))
    files[name] = fs.readFileSync(path.join(ROOT, name), 'utf8')
  return files
}

/**
 * Commits `files` in a fresh repository, tags the commit as the sweep's baseline and runs the real
 * `sweeps.sh` there. The tree is far smaller than the real one, so the verdict line is not asserted:
 * only the named section of the report is.
 * @param {Record<string, string>} files path to text of every tracked file
 * @param {string} [name] the report section to return
 * @returns {string} that section of the report, empty when the sweeps wrote none
 */
const sweep = (files, name = 'shipped-doc') => {
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
    const tracked = { ...files, 'CHANGELOG.md': LABEL_LINE + (files['CHANGELOG.md'] ?? '') }
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
      report.split('\n== ').find((part) => part.replace(/^== /, '').startsWith(`${name}:`)) || ''
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

describe('scripts/ci/sweeps.sh private working-process wording', () => {
  const GUIDE = 'docs/Other.md'
  const WORKFLOW = '.github/workflows/sample.yml'
  /**
   * @param {string} file a tracked path
   * @returns {string} the report section that reads it
   */
  const sectionOf = (file) => (file === WORKFLOW ? 'workflow-comment' : 'private-wording')

  it('passes the shipped guides, changelog, script Markdown and workflow comments of this tree', () => {
    const files = { ...realDocs(), ...realExtras() }
    expect(files['CHANGELOG.md']).toContain('## Unreleased')
    expect(files['scripts/e2e/telemetry/README.md']).toContain('# Broker telemetry fixture')
    expect(files['.github/workflows/gate.yml']).toContain('name: Exit gate')

    for (const name of ['private-wording', 'changelog-phase', 'workflow-comment']) {
      const section = sweep(files, name)

      expect(section).toContain(`${name}: 0 hit(s)`)
      expect(section).not.toContain(`-- ${name}:`)
    }
  })

  it.each([
    ['a development host in a guide', GUIDE, 'Measured on the development host.\n'],
    ['a developer machine in a guide', GUIDE, 'Install it on a developer machine.\n'],
    ['hosts in the release guide', 'RELEASING.md', 'Copy it to the development hosts.\n'],
    ['a hyphenated developer host in a guide', GUIDE, 'Measured on a developer-host.\n'],
    ['a short hyphenated machine name in a guide', GUIDE, `Measured on the ${SHORT_MACHINE}.\n`],
    ['tab-separated host wording in a guide', GUIDE, 'Measured on the development\t\thost.\n'],
    ['a supplying controller in a guide', GUIDE, 'The controller supplies the guard.\n'],
    ['a test controller in a guide', GUIDE, 'The test controller starts it.\n'],
    ['a local acceptance rule in a guide', GUIDE, 'Local acceptance uses Node 26.\n'],
    ['lower-case local gates in a guide', GUIDE, 'Run the local gates first.\n'],
    ['a phase merge in a guide', GUIDE, 'It does not block a phase merge.\n'],
    ['phase-blocking text in the changelog', 'CHANGELOG.md', '- These lanes are phase-blocking.\n'],
    [
      'a reviewed guard in a script README',
      'scripts/e2e/telemetry/README.md',
      'A reviewed guard.\n',
    ],
    ['a sole guard in a guide', GUIDE, 'Run it through the sole live guard.\n'],
    ['a shared reservation in a guide', GUIDE, 'It holds the shared reservation.\n'],
    ['a CI run id in a guide', GUIDE, 'Measured by run 12345678901.\n'],
    ['a soak run id in the changelog', 'CHANGELOG.md', '- Sized from rehearsal 0abc1234def5.\n'],
    ['a one-digit branch prefix in a guide', GUIDE, 'Runs on p9z/ pull requests.\n'],
    ['a two-digit branch name in a guide', GUIDE, 'Push to p10/sample to run it.\n'],
    [
      'upper-case wording in the scripts Markdown',
      'scripts/protocol/Notes.md',
      'DEVELOPMENT HOST.\n',
    ],
    ['a run id in a workflow comment', WORKFLOW, '# Longest job: 99s (run 12345678901).\n'],
    ['a branch prefix in a workflow comment', WORKFLOW, '# A push to a p9z/ branch runs it.\n'],
  ])('fails on %s', (_name, file, text) => {
    const name = sectionOf(file)
    const section = sweep({ ...realDocs(), [file]: text }, name)

    expect(section).toContain(`${name}: 1 hit(s)`)
    expect(section).toContain(`${file}:${text.trim()}`)
    expect(section).toContain(`-- ${name}: not an enumerated exception:`)
  })

  it('does not flag fixture scopes, Kafka controllers, local prerequisites, release tags or soak phases', () => {
    const text = [
      'Scopes p9f.nonzero.scope and p9f.zero.scope.',
      'The Kafka controller elects leaders.',
      'Local runs need Docker.',
      'Tag 3.0.0-alpha.9e.',
      'Each phase ends with a disconnect.',
      'Run 12 times.',
    ].join(' ')
    const section = sweep({ ...realDocs(), [GUIDE]: `${text}\n` }, 'private-wording')

    expect(section).toContain('private-wording: 0 hit(s)')
  })

  it('ignores a branch filter and a technical phase comment in a workflow', () => {
    const yml =
      "# Each phase of the soak ends with a disconnect.\non:\n  push:\n    branches: ['p9z/**']\n"
    const section = sweep({ ...realDocs(), [WORKFLOW]: yml }, 'workflow-comment')

    expect(section).toContain('workflow-comment: 0 hit(s)')
  })

  it.each([
    ['a numbered phase in changelog prose', 'CHANGELOG.md', `- Present since ${PHASE} 6A.\n`],
    [
      'a lower-case hyphenated phase number in changelog prose',
      'CHANGELOG.md',
      '- Added in phase-7.\n',
    ],
    ['a numbered phase in a workflow comment', WORKFLOW, `# Added in ${PHASE} 10.\n`],
  ])('fails on %s', (_name, file, text) => {
    const name = file === WORKFLOW ? 'workflow-comment' : 'changelog-phase'
    const section = sweep({ ...realDocs(), [file]: text }, name)

    expect(section).toContain(`${name}: 1 hit(s)`)
    expect(section).toContain(`${file}:${text.trim()}`)
    expect(section).toContain(`-- ${name}: not an enumerated exception:`)
  })

  it('does not flag numbered changelog headings, technical phases, release tags or fixture scopes', () => {
    const changelog = [
      `### ${PHASE} 4 — flexible protocol foundation`,
      '',
      '- Each phase ends with a disconnect; a two-phase commit; tag 3.0.0-alpha.9e; scope p9f.zero.scope.',
      '',
    ].join('\n')
    const section = sweep({ ...realDocs(), 'CHANGELOG.md': changelog }, 'changelog-phase')

    expect(section).toContain('changelog-phase: 0 hit(s)')
    expect(section).not.toContain('-- changelog-phase:')
  })
})
