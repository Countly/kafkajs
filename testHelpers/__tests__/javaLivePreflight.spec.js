const fs = require('fs')
const os = require('os')
const path = require('path')
const vm = require('vm')

const ROOT = path.join(__dirname, '..', '..')
const SPECS = [
  {
    name: 'consumer',
    file: path.join(ROOT, 'src', 'consumer', '__tests__', 'javaInteropLive.spec.js'),
    gate: 'JAVA_INTEROP_E2E',
  },
  {
    name: 'producer transaction',
    file: path.join(ROOT, 'src', 'producer', '__tests__', 'javaTransactionInteropLive.spec.js'),
    gate: 'JAVA_TRANSACTION_INTEROP_E2E',
  },
]
const FAILED_TEST = 'the Java member can be built and started'

/**
 * Evaluates a live spec's top level with recording stand-ins for jest, the child process and the
 * evidence writer, so its collection-time gate can be observed without a JVM or a broker. The gate
 * variable is set on a private env object, never on `process.env`.
 * @param {{file: string, gate: string}} live
 * @param {{env?: Record<string, string>, spawnSync: Function}} options
 * @returns {{tests: {name: string, fn: Function}[], suites: string[], skipped: string[], evidence: object[], probeEnvs: object[]}}
 */
const collect = (live, { env = {}, spawnSync }) => {
  const seen = { tests: [], suites: [], skipped: [], evidence: [], probeEnvs: [] }
  const describe = (name) => seen.suites.push(name)
  describe.skip = (name) => seen.skipped.push(name)
  const stubs = {
    child_process: {
      spawn: () => {
        throw new Error('no child process may start')
      },
      spawnSync: (command, args, spawnOptions) => {
        seen.probeEnvs.push(spawnOptions.env)
        return spawnSync(command, args, spawnOptions)
      },
    },
    testHelpers: {
      writeEvidence: (...args) => {
        seen.evidence.push(args)
        throw new Error('broker discovery reached: docker compose exec')
      },
    },
  }
  const load = (id) => (id in stubs ? stubs[id] : /^[./]/.test(id) ? {} : require(id))
  const wrapped = vm.compileFunction(fs.readFileSync(live.file, 'utf8'), [
    'require',
    'module',
    'exports',
    '__dirname',
    'process',
    'test',
    'describe',
    'jest',
    'console',
  ])
  wrapped(
    load,
    { exports: {} },
    {},
    path.dirname(live.file),
    { env },
    (name, fn) => seen.tests.push({ name, fn }),
    describe,
    { setTimeout() {}, restoreAllMocks() {} },
    { warn() {}, log() {} }
  )
  return seen
}

describe.each(SPECS)('the $name Java live spec', (live) => {
  let scratch
  let realTmpdir

  beforeEach(() => {
    scratch = fs.mkdtempSync(path.join(os.tmpdir(), 'javaLivePreflight-'))
    realTmpdir = os.tmpdir
    os.tmpdir = () => scratch
  })

  afterEach(() => {
    os.tmpdir = realTmpdir
    fs.rmSync(scratch, { recursive: true, force: true })
  })

  const failingProbe = (stderr) => () => ({ status: 2, stderr })
  const gated = (extra = {}) => ({ [live.gate]: '1', ...extra })

  test('registers no test when the gate is off', () => {
    const seen = collect(live, { spawnSync: failingProbe('') })
    expect(seen.tests).toEqual([])
    expect(seen.probeEnvs).toEqual([])
  })

  test.each([
    ['javac: command not found', 'no JDK on PATH'],
    ['Exception in thread: NoClassDefFoundError', 'a class was missing from the classpath'],
    ['Missing cached Java dependency: x.jar', 'the cached jars are missing'],
    ['curl: (6) could not resolve host', 'the jars could not be downloaded'],
  ])('fails one named test for a failed preflight (%s)', (stderr, reason) => {
    let seen
    expect(() => {
      seen = collect(live, { env: gated(), spawnSync: failingProbe(stderr) })
    }).not.toThrow()
    expect(seen.tests.map((t) => t.name)).toEqual([FAILED_TEST])
    expect(() => seen.tests[0].fn()).toThrow(reason)
    expect(seen.evidence).toEqual([])
    expect(seen.skipped).toHaveLength(1)
  })

  test('hands a supplied jar cache to the probe and leaves it in place', () => {
    const cache = path.join(scratch, 'jars')
    fs.mkdirSync(cache)
    fs.writeFileSync(path.join(cache, 'kept.jar'), 'jar')
    const seen = collect(live, {
      env: gated({ JAVA_E2E_WORK_DIR: cache }),
      spawnSync: failingProbe('Missing cached Java dependency: x.jar'),
    })
    expect(seen.probeEnvs[0].JAVA_E2E_WORK_DIR).toBe(cache)
    expect(fs.existsSync(path.join(cache, 'kept.jar'))).toBe(true)
  })

  test('removes the work dir it created itself after a failed probe', () => {
    collect(live, { env: gated(), spawnSync: failingProbe('javac: command not found') })
    expect(fs.readdirSync(scratch)).toEqual([])
  })
})
