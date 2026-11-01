'use strict'

const fs = require('fs')
const os = require('os')
const path = require('path')
const { execFileSync } = require('child_process')
const { scaffold, write } = require('../scaffold')
const apiKeys = require('../../../src/protocol/requests/apiKeys')
const { messageOfKey } = require('../apiNames')

// prettier's own JS API loads plugins via a dynamic `import()`, which Jest's CJS test
// environment cannot satisfy without `--experimental-vm-modules`; the CLI (a separate Node
// process, same one `scaffold.js` itself shells out to) has no such restriction.
const PRETTIER_BIN = require.resolve('prettier/bin/prettier.cjs')
const REPO_ROOT = path.join(__dirname, '..', '..', '..')

const golden = (name) =>
  fs.readFileSync(path.join(__dirname, 'fixtures', `${name}.golden.txt`), 'utf8')

describe('scripts/protocol > scaffold', () => {
  test('emits the ApiVersions v3 request skeleton', () => {
    expect(scaffold('ApiVersions', 3, 'request')).toEqual(golden('ApiVersions-v3-request'))
  })

  test('emits the ApiVersions v3 response skeleton', () => {
    expect(scaffold('ApiVersions', 3, 'response')).toEqual(golden('ApiVersions-v3-response'))
  })

  test('a non-flexible version gets classic writers and no tag buffers', () => {
    const source = scaffold('Metadata', 6, 'request')
    // Topics is nullableVersions "1+", so the nullable writer is required (rule 6): an empty
    // topic list must keep encoding as the null array, i.e. "every topic".
    expect(source).toContain('.writeNullableArray')
    expect(source).not.toContain('writeTaggedFields')
    expect(source).toContain('version: 6,')
  })

  test('a flexible nullable array emits the null-or-empty compact form', () => {
    const source = scaffold('Metadata', 12, 'request')
    expect(source).toContain('topics.length === 0 ? null :')
    expect(source).toContain('writeTaggedFields')
  })

  test('rejects a version outside validVersions', () => {
    expect(() => scaffold('ApiVersions', 9, 'request')).toThrow(/validVersions/)
  })

  test('rejects an unknown API', () => {
    expect(() => scaffold('NotAnApi', 0, 'request')).toThrow(/NotAnApi/)
  })

  // Metadata v12's Topics struct (TopicId + Name, flexible) has just enough fields that its
  // encodeTopics chain fits Prettier's print width and gets collapsed onto one line — a
  // scaffolder that always breaks a 2+-call chain across lines would fail eslint/prettier here
  // even though it happens to pass for both golden fixtures above (their chains are longer).
  test('a scaffolded flexible codec with a short nested-struct chain is Prettier-clean', () => {
    const source = scaffold('Metadata', 12, 'request')
    const targetPath = path.join('src', 'protocol', 'requests', 'metadata', 'v12', 'request.js')

    expect(() =>
      execFileSync(process.execPath, [PRETTIER_BIN, '--check', '--stdin-filepath', targetPath], {
        input: source,
        cwd: REPO_ROOT,
        encoding: 'utf8',
      })
    ).not.toThrow()
  })

  // The Apache message is FindCoordinator; this client's key and the name its codecs report are
  // GroupCoordinator. A scaffolded request that imported the message name would get undefined.
  test.each([
    ['GroupCoordinator', 'FindCoordinator'],
    ['Produce', 'Produce'],
    ['OffsetFetch', 'OffsetFetch'],
  ])('the request of %s takes its name from apiKeys.js', (key, message) => {
    expect(messageOfKey(key)).toEqual(message)
    expect(apiKeys[key]).toEqual(expect.any(Number))

    const source = scaffold(message, 4, 'request')

    expect(source).toContain(`const { ${key}: apiKey } = require('../../apiKeys')`)
    expect(source).toContain(`apiName: '${key}',`)
    expect(source).toContain(`api: '${message}',`)
  })

  test.each([0, 1])('quota float64 fields scaffold as doubles at v%s', (version) => {
    const request = scaffold('AlterClientQuotas', version, 'request')
    const response = scaffold('DescribeClientQuotas', version, 'response')
    expect(request).toContain('writeDouble(value)')
    expect(response).toContain('decoder.readDouble()')
    expect(request).toContain('FLOAT64')
    expect(response).toContain('FLOAT64')
  })

  describe('write()', () => {
    test('never overwrites an existing file', () => {
      const tmpRoot = fs.mkdtempSync(path.join(os.tmpdir(), 'kafkajs-scaffold-'))
      try {
        const [requestFile] = write('ApiVersions', 0, tmpRoot)
        const original = fs.readFileSync(requestFile, 'utf8')

        expect(() => write('ApiVersions', 0, tmpRoot)).toThrow(/already exists/)
        expect(fs.readFileSync(requestFile, 'utf8')).toEqual(original)
      } finally {
        fs.rmSync(tmpRoot, { recursive: true, force: true })
      }
    })
  })
})
