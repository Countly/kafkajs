const fs = require('node:fs')
const path = require('node:path')
const yaml = require('js-yaml')

const ROOT = path.join(__dirname, '..', '..', '..')
const WORKFLOWS = path.join(ROOT, '.github', 'workflows')
const GATE_MANIFEST = path.join(ROOT, 'scripts', 'ci', 'gate.json')

/** A command that packs or publishes a tarball, directly or through a repository script. */
const PACKS =
  /\bnpm (pack|publish)\b|\b(packSmoke|otelPackSmoke|otelTest)\.sh\b|\bnpm run test:(pack|otel)\b/
/** A command that installs dependencies. */
const INSTALLS = /\bnpm (ci|install|i)\b/
/** Where one shell command ends and the next begins on a line. */
const COMMAND_BREAK = /&&|\|\||[;|]/
/** The start of an install, for splitting several installs that share one command. */
const INSTALL_START = /(?=\bnpm (?:ci|install|i)\b)/
/** A workflow step that executes the command a gate manifest check names. */
const RUNS_MANIFEST_COMMAND = /\bcheck\.command\b/
/** The scripts a packing job runs that install packages themselves. */
const PACK_SCRIPTS = ['packSmoke.sh', 'otelPackSmoke.sh', 'otelTest.sh']
/** The one install that keeps lifecycle scripts: its install script compiles the optional Kerberos binding, in a prefix of its own. */
const KERBEROS_BINDING = /^npm install --prefix "\$peer" .*"kerberos@\$version"$/

/**
 * Splits shell text into lines with continued lines joined and comment lines dropped. Quoted
 * text stays: a command substitution inside quotes still runs.
 * @param {string} text a run script or shell file
 * @returns {string[]} one entry per line
 */
const commandLines = (text) =>
  text
    .replace(/\\\n/g, ' ')
    .split('\n')
    .filter((line) => !line.trim().startsWith('#'))

/** @returns {{ id: string, command: string }[]} the gate manifest checks that run a shell command */
const manifestChecks = () =>
  JSON.parse(fs.readFileSync(GATE_MANIFEST, 'utf8')).checks.filter((check) => check.command)

/**
 * A job that executes the command a gate manifest check names runs every manifest command, so its
 * lines include them all.
 * @returns {{ id: string, lines: string[] }[]} every workflow job with its command lines
 */
const workflowJobs = () => {
  const manifestLines = manifestChecks().flatMap((check) => commandLines(check.command))
  return fs
    .readdirSync(WORKFLOWS)
    .filter((file) => /\.ya?ml$/.test(file))
    .flatMap((file) => {
      const { jobs } = yaml.load(fs.readFileSync(path.join(WORKFLOWS, file), 'utf8'))
      return Object.entries(jobs).map(([name, job]) => {
        const steps = job.steps ?? []
        return {
          id: `${file}:${name}`,
          lines: [
            ...steps.flatMap((step) => (step.run ? commandLines(step.run) : [])),
            ...(steps.some((step) => RUNS_MANIFEST_COMMAND.test(step.run ?? ''))
              ? manifestLines
              : []),
          ],
        }
      })
    })
}

/** @returns {{ id: string, lines: string[] }[]} the workflow jobs that pack or publish */
const packingJobs = () => workflowJobs().filter((job) => job.lines.some((line) => PACKS.test(line)))

/**
 * Judges every install command on its own: a line is split at `&&`, `||`, `;` and `|`, then
 * where one command holds several installs, so a flag on one command never covers another.
 * @param {string[]} lines command lines
 * @returns {string[]} the install commands that do not pass --ignore-scripts
 */
const scriptedInstalls = (lines) =>
  lines
    .flatMap((line) => line.split(COMMAND_BREAK))
    .flatMap((command) => command.split(INSTALL_START))
    .filter((command) => INSTALLS.test(command) && !/(^|\s)--ignore-scripts(\s|$)/.test(command))
    .map((command) => command.trim())

describe('installs before a pack or publish', () => {
  test('the packing npm scripts are the shell scripts this check reads', () => {
    const { scripts } = JSON.parse(fs.readFileSync(path.join(ROOT, 'package.json'), 'utf8'))

    expect(scripts['test:pack']).toBe('./scripts/packSmoke.sh')
    expect(scripts['test:otel']).toBe('./scripts/otelTest.sh')
    expect(
      commandLines(fs.readFileSync(path.join(ROOT, 'scripts', 'otelTest.sh'), 'utf8'))
    ).toEqual(expect.arrayContaining([expect.stringMatching(/\bnpm pack\b/)]))
  })

  test('every workflow job that packs or publishes runs every install with --ignore-scripts', () => {
    expect(packingJobs().map((job) => job.id)).toEqual(
      expect.arrayContaining([
        'ci.yml:pack',
        'ci.yml:otel',
        'bench.yml:bench',
        'gate.yml:check',
        'release.yml:pack',
        'release.yml:pack-otel',
        'release.yml:publish',
        'release.yml:publish-otel',
      ])
    )
    expect(
      packingJobs().flatMap((job) =>
        scriptedInstalls(job.lines)
          .filter((line) => !KERBEROS_BINDING.test(line))
          .map((line) => `${job.id}: ${line}`)
      )
    ).toEqual([])
  })

  test('the gate manifest packs through its pack and OTel checks', () => {
    expect(
      manifestChecks()
        .filter((check) => PACKS.test(check.command))
        .map((check) => check.id)
    ).toEqual(expect.arrayContaining(['pack', 'otel-test', 'e2e-otel']))
  })

  test('the only install that keeps lifecycle scripts is the Kerberos binding built in its own prefix', () => {
    expect(
      packingJobs().flatMap((job) =>
        scriptedInstalls(job.lines)
          .filter((line) => KERBEROS_BINDING.test(line))
          .map((line) => `${job.id}: ${line}`)
      )
    ).toEqual([
      'gate.yml:check: npm install --prefix "$peer" --no-package-lock --no-audit --no-fund "kerberos@$version"',
    ])
  })

  test.each(PACK_SCRIPTS)('every install in scripts/%s passes --ignore-scripts', (file) => {
    const lines = commandLines(fs.readFileSync(path.join(ROOT, 'scripts', file), 'utf8'))

    expect(lines.filter((line) => INSTALLS.test(line)).length).toBeGreaterThan(0)
    expect(scriptedInstalls(lines)).toEqual([])
  })

  describe('an install that is not the only command on its line', () => {
    test.each([
      ['an unprotected install before a protected one', 'npm ci && npm install --ignore-scripts x'],
      ['an unprotected install after a protected one', 'npm install --ignore-scripts x && npm ci'],
      ['a semicolon separating the installs', 'npm ci; npm install --ignore-scripts x'],
      ['an or-list of installs', 'npm install --ignore-scripts x || npm ci'],
      ['a flag on a command that installs nothing', 'npm ci && echo --ignore-scripts'],
      ['command substitutions', 'echo "$(npm ci)" "$(npm install --ignore-scripts x)"'],
    ])('the check flags %s', (_name, line) => {
      expect(scriptedInstalls(commandLines(line))).toEqual([expect.stringContaining('npm ci')])
    })

    test('the check flags an unprotected install in a continued compound command', () => {
      const lines = commandLines('npm ci \\\n  && npm install --ignore-scripts x')

      expect(scriptedInstalls(lines)).toEqual([expect.stringContaining('npm ci')])
    })

    test('the check flags an unprotected install in a multi-line run block', () => {
      const lines = commandLines('npm ci\nnpm install --ignore-scripts x\nnpm pack')

      expect(scriptedInstalls(lines)).toEqual(['npm ci'])
    })

    test('the check accepts installs that each pass the flag', () => {
      const lines = commandLines(
        [
          'npm ci --ignore-scripts && npm install --ignore-scripts x',
          'npm ci --ignore-scripts; npm install --prefix "$RUNNER_TEMP/a" x --ignore-scripts',
          'npm pack --pack-destination "$dir" | npm install --ignore-scripts x',
        ].join('\n')
      )

      expect(scriptedInstalls(lines)).toEqual([])
    })
  })
})
