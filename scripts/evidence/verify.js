/**
 * Verifies live evidence against the tree and the broker a run was meant to test.
 *
 * Usage: node scripts/evidence/verify.js --broker <version> <evidence.jsonl...>
 *
 * Run it from the frozen tree while the cluster that produced the evidence is still up, with the
 * version the broker reported (`testHelpers/brokerVersion.js` has the command) and only the
 * files of that run. It works out the tree identity itself, from git, and exits 0 only when every
 * line of every file carries all the stamps `testHelpers/e2eEvidence.js` writes and agrees with
 * the verified tree, this Node and the given broker version, on the docker lane.
 */
const fs = require('fs')
const { execFileSync } = require('child_process')
const { STAMP_FIELDS } = require('../../testHelpers/e2eEvidence')

const USAGE = 'Usage: node scripts/evidence/verify.js --broker <version> <evidence.jsonl...>'

/**
 * @param {string[]} args
 * @returns {string} what `git <args>` printed
 * @throws {Error} when git fails
 */
const runGit = (args) => execFileSync('git', args, { encoding: 'utf8' })

/**
 * The identity of the tree being verified: the commit that is checked out, provided nothing
 * tracked differs from it.
 * @param {(args: string[]) => string} [git] runs git; replaced by a test
 * @returns {string} the full commit hash
 * @throws {Error} when the tracked files are changed or staged, or git gives no commit hash
 */
const verifiedTree = (git = runGit) => {
  const changed = git(['status', '--porcelain', '--untracked-files=no']).trim()
  if (changed) {
    throw new Error(`The tree is not clean, so it is not a tree anyone can name:\n${changed}`)
  }

  const commit = git(['rev-parse', 'HEAD']).trim()
  if (!/^[0-9a-f]{40}$/.test(commit)) {
    throw new Error(`git did not return a commit hash: ${JSON.stringify(commit)}`)
  }

  return commit
}

/**
 * @param {*} value
 * @returns {boolean} whether `value` is a non-null, non-array object
 */
const isPlainObject = (value) =>
  value !== null && typeof value === 'object' && !Array.isArray(value)

/**
 * What is wrong with one evidence line.
 * @param {object} line a parsed evidence line
 * @param {{ tree: string, node: string, brokerVersion: string }} verified what the run must have
 *   been against
 * @returns {string[]} one sentence per problem; empty when the line is good
 */
const problemsOf = (line, { tree, node, brokerVersion }) => {
  const problems = STAMP_FIELDS.filter((field) => !(field in line)).map(
    (field) => `has no ${field}`
  )

  for (const [field, expected] of Object.entries({ tree, node, brokerVersion })) {
    if (field in line && line[field] !== expected) {
      problems.push(
        `${field} is ${JSON.stringify(line[field])}, verified ${JSON.stringify(expected)}`
      )
    }
  }

  if ('lane' in line && line.lane !== 'docker') {
    problems.push(
      `lane is ${JSON.stringify(line.lane)}, but evidence of a broker comes from docker`
    )
  }

  for (const field of ['wireVersions', 'config']) {
    if (field in line && !isPlainObject(line[field])) {
      problems.push(`${field} is not an object`)
    }
  }

  return problems
}

/**
 * Checks every line of every file.
 * @param {string[]} files evidence files, one JSON object per line
 * @param {{ tree: string, node: string, brokerVersion: string }} verified
 * @param {(file: string) => string} [readFile] replaced by a test
 * @returns {{ lines: number, problems: string[] }} how many lines were read, and every problem
 *   found as `<file>:<line>: <what>`; a file that cannot be read, holds no line, or holds a line
 *   that is not JSON is a problem too
 */
const verifyFiles = (files, verified, readFile = (file) => fs.readFileSync(file, 'utf8')) => {
  const problems = []
  let lines = 0

  for (const file of files) {
    let text
    try {
      text = readFile(file)
    } catch (e) {
      problems.push(`${file}: cannot be read (${e.message})`)
      continue
    }

    const rows = text.split('\n')
    if (rows.every((row) => !row.trim())) {
      problems.push(`${file}: holds no evidence lines`)
    }

    rows.forEach((row, index) => {
      if (!row.trim()) {
        return
      }

      lines += 1
      let line
      try {
        line = JSON.parse(row)
      } catch {
        problems.push(`${file}:${index + 1}: is not JSON`)
        return
      }

      for (const problem of isPlainObject(line)
        ? problemsOf(line, verified)
        : ['is not an object']) {
        problems.push(`${file}:${index + 1}: ${problem}`)
      }
    })
  }

  return { lines, problems }
}

/**
 * The command line.
 * @param {string[]} argv the arguments after the script name
 * @param {object} [deps] replaced by a test
 * @param {(args: string[]) => string} [deps.git]
 * @param {(file: string) => string} [deps.readFile]
 * @param {string} [deps.node] the Node version lines must carry; this process's by default
 * @param {(text: string) => void} [deps.stdout]
 * @param {(text: string) => void} [deps.stderr]
 * @returns {number} the exit code: 0 when everything verified, 1 when something did not, 2 for a
 *   command line that cannot be run
 */
const main = (argv, deps = {}) => {
  const {
    git = runGit,
    readFile,
    node = process.version,
    stdout = console.log,
    stderr = console.error,
  } = deps
  const broker = argv.indexOf('--broker')
  const brokerVersion = broker === -1 ? undefined : argv[broker + 1]
  const files = argv.filter((arg, index) => index !== broker && index !== broker + 1)

  if (
    !brokerVersion ||
    brokerVersion.startsWith('--') ||
    files.length === 0 ||
    files.some((arg) => arg.startsWith('--'))
  ) {
    stderr(USAGE)
    return 2
  }

  let tree
  try {
    tree = verifiedTree(git)
  } catch (e) {
    stderr(e.message)
    return 1
  }

  const { lines, problems } = verifyFiles(files, { tree, node, brokerVersion }, readFile)
  if (problems.length > 0) {
    problems.forEach((problem) => stderr(problem))
    stderr(`${problems.length} problem(s) in ${lines} line(s): the evidence is not verified`)
    return 1
  }

  stdout(
    `${lines} line(s) in ${files.length} file(s) verified against ${tree}, ${node}, Kafka ${brokerVersion}`
  )
  return 0
}

if (require.main === module) {
  process.exitCode = main(process.argv.slice(2))
}

module.exports = { main, verifyFiles, verifiedTree, problemsOf }
