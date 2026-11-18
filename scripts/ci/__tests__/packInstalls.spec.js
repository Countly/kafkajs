const fs = require('node:fs')
const path = require('node:path')
const yaml = require('js-yaml')

const ROOT = path.join(__dirname, '..', '..', '..')
const WORKFLOWS = path.join(ROOT, '.github', 'workflows')

/** A command that packs or publishes a tarball, directly or through a repository script. */
const PACKS =
  /\bnpm (pack|publish)\b|\b(packSmoke|otelPackSmoke|otelTest)\.sh\b|\bnpm run test:(pack|otel)\b/
/** A command that installs dependencies. */
const INSTALLS = /\bnpm (ci|install|i)\b/
/** The scripts a packing job runs that install packages themselves. */
const PACK_SCRIPTS = ['packSmoke.sh', 'otelPackSmoke.sh', 'otelTest.sh']

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

/** @returns {{ id: string, lines: string[] }[]} every workflow job with its command lines */
const workflowJobs = () =>
  fs
    .readdirSync(WORKFLOWS)
    .filter((file) => /\.ya?ml$/.test(file))
    .flatMap((file) => {
      const { jobs } = yaml.load(fs.readFileSync(path.join(WORKFLOWS, file), 'utf8'))
      return Object.entries(jobs).map(([name, job]) => ({
        id: `${file}:${name}`,
        lines: (job.steps ?? []).flatMap((step) => (step.run ? commandLines(step.run) : [])),
      }))
    })

/**
 * @param {string[]} lines command lines
 * @returns {string[]} the install commands that do not pass --ignore-scripts
 */
const scriptedInstalls = (lines) =>
  lines
    .filter((line) => INSTALLS.test(line) && !/(^|\s)--ignore-scripts(\s|$)/.test(line))
    .map((line) => line.trim())

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
    const packing = workflowJobs().filter((job) => job.lines.some((line) => PACKS.test(line)))

    expect(packing.map((job) => job.id)).toEqual(
      expect.arrayContaining([
        'ci.yml:pack',
        'ci.yml:otel',
        'bench.yml:bench',
        'release.yml:pack',
        'release.yml:pack-otel',
        'release.yml:publish',
        'release.yml:publish-otel',
      ])
    )
    expect(
      packing.flatMap((job) => scriptedInstalls(job.lines).map((line) => `${job.id}: ${line}`))
    ).toEqual([])
  })

  test.each(PACK_SCRIPTS)('every install in scripts/%s passes --ignore-scripts', (file) => {
    const lines = commandLines(fs.readFileSync(path.join(ROOT, 'scripts', file), 'utf8'))

    expect(lines.filter((line) => INSTALLS.test(line)).length).toBeGreaterThan(0)
    expect(scriptedInstalls(lines)).toEqual([])
  })
})
