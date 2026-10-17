const fs = require('fs')
const os = require('os')
const path = require('path')
const { main, verifyFiles, verifiedTree, problemsOf } = require('../verify')
const { evidencePath, useEvidenceRun, writeEvidence } = require('../../../testHelpers/e2eEvidence')

const COMMIT = 'a'.repeat(40)
const VERIFIED = { tree: COMMIT, node: 'v26.7.0', brokerVersion: '4.3.1' }
const GOOD = {
  at: '2026-10-16T10:00:00.000Z',
  brokerVersion: '4.3.1',
  wireVersions: { Fetch: 18 },
  node: 'v26.7.0',
  config: { clientId: 'c' },
  tree: COMMIT,
  lane: 'docker',
  step: 'one',
}
const jsonl = (...lines) => `${lines.map((line) => JSON.stringify(line)).join('\n')}\n`

/** A git that answers `status` and `rev-parse` as given. */
const gitAnswering = ({ status = '', head = COMMIT } = {}) =>
  jest.fn((args) => (args[0] === 'status' ? status : `${head}\n`))

/** Runs the command line over in-memory files and returns what it printed and its exit code. */
const run = (argv, files, deps = {}) => {
  const out = []
  const err = []
  const code = main(argv, {
    git: gitAnswering(),
    node: 'v26.7.0',
    readFile: (file) => {
      if (!(file in files)) {
        throw new Error('ENOENT')
      }
      return files[file]
    },
    stdout: (text) => out.push(text),
    stderr: (text) => err.push(text),
    ...deps,
  })

  return { code, out, err }
}

describe('verifiedTree', () => {
  test('is the commit that is checked out, when no tracked file differs from it', () => {
    const git = gitAnswering()

    expect(verifiedTree(git)).toBe(COMMIT)
    expect(git.mock.calls).toEqual([
      [['status', '--porcelain', '--untracked-files=no']],
      [['rev-parse', 'HEAD']],
    ])
  })

  test('refuses a tree with a changed or staged tracked file, and lists it', () => {
    const git = gitAnswering({ status: ' M src/index.js\nA  src/new.js\n' })

    expect(() => verifiedTree(git)).toThrow(/not clean[^]*src\/index\.js[^]*src\/new\.js/)
  })

  test.each(['', 'HEAD', 'abc123', `${'a'.repeat(39)}g`])('refuses %p as a commit hash', (head) => {
    expect(() => verifiedTree(gitAnswering({ head }))).toThrow('did not return a commit hash')
  })
})

describe('problemsOf', () => {
  test('finds nothing wrong with a line that carries every stamp and agrees with the run', () => {
    expect(problemsOf(GOOD, VERIFIED)).toEqual([])
  })

  test.each([
    ['tree', 'b'.repeat(40)],
    ['node', 'v24.1.0'],
    ['brokerVersion', '4.1.2'],
  ])('finds a wrong %s', (field, value) => {
    expect(problemsOf({ ...GOOD, [field]: value }, VERIFIED)).toEqual([
      expect.stringContaining(`${field} is ${JSON.stringify(value)}, verified`),
    ])
  })

  test.each(['at', 'brokerVersion', 'wireVersions', 'node', 'config', 'tree', 'lane'])(
    'finds a missing %s',
    (field) => {
      const { [field]: _removed, ...line } = GOOD

      expect(problemsOf(line, VERIFIED)).toEqual([`has no ${field}`])
    }
  )

  test.each(['fake', 'chaos', 'Docker', null])('finds the forged lane %p', (lane) => {
    expect(problemsOf({ ...GOOD, lane }, VERIFIED)).toEqual([
      expect.stringContaining('evidence of a broker comes from docker'),
    ])
  })

  test.each([
    ['wireVersions', ['Fetch']],
    ['wireVersions', 'Fetch'],
    ['config', null],
  ])('finds %s that is not an object (%p)', (field, value) => {
    expect(problemsOf({ ...GOOD, [field]: value }, VERIFIED)).toEqual([`${field} is not an object`])
  })

  test('reports every problem of a line, not only the first', () => {
    const { tree: _tree, ...line } = GOOD

    expect(problemsOf({ ...line, node: 'v1.0.0', lane: 'fake' }, VERIFIED)).toHaveLength(3)
  })
})

describe('verifyFiles', () => {
  const read = (files) => (file) => files[file]

  test('counts the lines of every file and reports nothing for good ones', () => {
    const files = { 'a.jsonl': jsonl(GOOD, GOOD), 'b.jsonl': jsonl(GOOD) }

    expect(verifyFiles(['a.jsonl', 'b.jsonl'], VERIFIED, read(files))).toEqual({
      lines: 3,
      problems: [],
    })
  })

  test('names the file and the line of each problem', () => {
    const files = {
      'a.jsonl': jsonl(GOOD, { ...GOOD, tree: 'b'.repeat(40) }),
      'b.jsonl': jsonl({ ...GOOD, lane: 'fake' }),
    }

    const { problems } = verifyFiles(['a.jsonl', 'b.jsonl'], VERIFIED, read(files))

    expect(problems).toHaveLength(2)
    expect(problems[0]).toMatch(/^a\.jsonl:2: tree is /)
    expect(problems[1]).toMatch(/^b\.jsonl:1: lane is "fake"/)
  })

  test('skips blank lines, numbers lines as the file does, and flags a line that is not JSON or not an object', () => {
    const files = { 'a.jsonl': `\n${JSON.stringify(GOOD)}\n  \n{oops\n[1]\n` }

    const { lines, problems } = verifyFiles(['a.jsonl'], VERIFIED, read(files))

    expect(lines).toBe(3)
    expect(problems).toEqual(['a.jsonl:4: is not JSON', 'a.jsonl:5: is not an object'])
  })

  test('counts a file with no lines as a problem, and a file that cannot be read', () => {
    const { problems } = verifyFiles(['empty.jsonl', 'missing.jsonl'], VERIFIED, (file) => {
      if (file === 'missing.jsonl') {
        throw new Error('ENOENT: no such file')
      }
      return '\n'
    })

    expect(problems).toEqual([
      'empty.jsonl: holds no evidence lines',
      'missing.jsonl: cannot be read (ENOENT: no such file)',
    ])
  })
})

describe('main', () => {
  const argv = ['--broker', '4.3.1', 'a.jsonl']

  test('exits 0 and says what it verified when every line agrees', () => {
    const { code, out, err } = run(argv, { 'a.jsonl': jsonl(GOOD, GOOD) })

    expect(code).toBe(0)
    expect(err).toEqual([])
    expect(out).toEqual([`2 line(s) in 1 file(s) verified against ${COMMIT}, v26.7.0, Kafka 4.3.1`])
  })

  test('takes the flag before or after the files', () => {
    expect(run(['a.jsonl', '--broker', '4.3.1'], { 'a.jsonl': jsonl(GOOD) }).code).toBe(0)
  })

  test.each([
    ['a wrong tree', { ...GOOD, tree: 'b'.repeat(40) }, /tree is/],
    ['a wrong broker version', { ...GOOD, brokerVersion: '4.1.2' }, /brokerVersion is/],
    ['a wrong node', { ...GOOD, node: 'v22.0.0' }, /node is/],
    ['a missing field', { ...GOOD, config: undefined }, /has no config/],
    ['a forged lane', { ...GOOD, lane: 'fake' }, /lane is/],
  ])('exits 1 for %s', (_name, line, message) => {
    const { code, err, out } = run(argv, { 'a.jsonl': jsonl(GOOD, line) })

    expect(code).toBe(1)
    expect(out).toEqual([])
    expect(err[0]).toMatch(message)
    expect(err[err.length - 1]).toBe('1 problem(s) in 2 line(s): the evidence is not verified')
  })

  test('judges the lines against the version it was given, not the one the lines carry', () => {
    const { code } = run(['--broker', '4.1.2', 'a.jsonl'], { 'a.jsonl': jsonl(GOOD) })

    expect(code).toBe(1)
  })

  test('exits 1, and reads no file, when the tree is not clean', () => {
    const readFile = jest.fn()
    const { code, err } = run(argv, {}, { git: gitAnswering({ status: ' M x.js' }), readFile })

    expect(code).toBe(1)
    expect(err[0]).toMatch(/not clean/)
    expect(readFile).not.toHaveBeenCalled()
  })

  test('exits 1 for a file that does not exist', () => {
    const { code, err } = run(['--broker', '4.3.1', 'nope.jsonl'], {})

    expect(code).toBe(1)
    expect(err[0]).toMatch(/^nope\.jsonl: cannot be read/)
  })

  test.each([
    ['no arguments', []],
    ['no files', ['--broker', '4.3.1']],
    ['no broker', ['a.jsonl']],
    ['a broker flag with no value', ['a.jsonl', '--broker']],
    ['a broker flag followed by another flag', ['--broker', '--verbose', 'a.jsonl']],
    ['an unknown flag', ['--broker', '4.3.1', '--lane', 'a.jsonl']],
  ])('exits 2 and prints the usage for %s', (_name, args) => {
    const git = gitAnswering()
    const { code, err } = run(args, { 'a.jsonl': jsonl(GOOD) }, { git })

    expect(code).toBe(2)
    expect(err).toEqual([expect.stringMatching(/^Usage: node scripts\/evidence\/verify\.js/)])
    expect(git).not.toHaveBeenCalled()
  })
})

describe('the defaults of the command line', () => {
  test('verifyFiles reads real files when it is not given a reader', () => {
    const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'verify-spec-'))
    try {
      const file = path.join(directory, 'run.jsonl')
      fs.writeFileSync(file, jsonl(GOOD))

      expect(verifyFiles([file], VERIFIED)).toEqual({ lines: 1, problems: [] })
      expect(verifyFiles([path.join(directory, 'absent.jsonl')], VERIFIED).problems[0]).toMatch(
        /absent\.jsonl: cannot be read \(ENOENT/
      )
    } finally {
      fs.rmSync(directory, { recursive: true, force: true })
    }
  })

  test('verifiedTree asks git for the status and then the commit', () => {
    const execFileSync = jest.fn((command, args) => (args[0] === 'status' ? '' : `${COMMIT}\n`))
    let isolated
    jest.isolateModules(() => {
      jest.doMock('child_process', () => ({ execFileSync }))
      isolated = require('../verify')
    })
    jest.dontMock('child_process')

    expect(isolated.verifiedTree()).toBe(COMMIT)
    expect(execFileSync.mock.calls).toEqual([
      ['git', ['status', '--porcelain', '--untracked-files=no'], { encoding: 'utf8' }],
      ['git', ['rev-parse', 'HEAD'], { encoding: 'utf8' }],
    ])
  })

  test('main prints through the console and judges against this process’s Node', () => {
    const log = jest.spyOn(console, 'log').mockImplementation(() => {})
    const error = jest.spyOn(console, 'error').mockImplementation(() => {})
    const files = { 'a.jsonl': jsonl({ ...GOOD, node: process.version }) }
    const readFile = (file) => files[file]
    try {
      expect(main(['--broker', '4.3.1', 'a.jsonl'], { git: gitAnswering(), readFile })).toBe(0)
      expect(log).toHaveBeenCalledWith(expect.stringContaining(`${process.version}, Kafka 4.3.1`))
      expect(main(['nothing'], { git: gitAnswering() })).toBe(2)
      expect(error).toHaveBeenCalledWith(expect.stringMatching(/^Usage:/))
    } finally {
      log.mockRestore()
      error.mockRestore()
    }
  })
})

describe('the evidence the writer produces', () => {
  const name = `verify-spec-${process.pid}`

  afterEach(() => {
    useEvidenceRun()
    fs.rmSync(evidencePath(name), { force: true })
  })

  test('does not verify a line whose record tried to claim the verified run through toJSON', () => {
    const { tree, node, brokerVersion } = VERIFIED
    useEvidenceRun({ tree: 'b'.repeat(40), node, brokerVersion: '4.1.2', lane: 'fake' })
    writeEvidence(name, {
      toJSON() {
        return { ...this, tree, node, brokerVersion, lane: 'docker' }
      },
    })
    const err = []

    const code = main(['--broker', brokerVersion, evidencePath(name)], {
      git: gitAnswering(),
      node,
      stdout: () => {},
      stderr: (text) => err.push(text),
    })

    expect(code).toBe(1)
    expect(err.join('\n')).toMatch(/:1: tree is "b+", verified/)
    expect(err.join('\n')).toMatch(/:1: brokerVersion is "4\.1\.2", verified/)
    expect(err.join('\n')).toMatch(/:1: lane is "fake"/)
  })

  test('verifies a line written under the verified run', () => {
    useEvidenceRun({ ...VERIFIED, lane: 'docker' })
    writeEvidence(name, { step: 'one' })

    const out = []
    const code = main(['--broker', VERIFIED.brokerVersion, evidencePath(name)], {
      git: gitAnswering(),
      node: VERIFIED.node,
      stdout: (text) => out.push(text),
      stderr: () => {},
    })

    expect(code).toBe(0)
    expect(out).toEqual([`1 line(s) in 1 file(s) verified against ${COMMIT}, v26.7.0, Kafka 4.3.1`])
  })
})
