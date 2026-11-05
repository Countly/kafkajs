#!/usr/bin/env node
'use strict'

/**
 * `npm run compat:fetch` — the only networked part of the compat tool. `compat:check`
 * reads the committed snapshot this writes and never touches the network itself, and
 * this command is not part of `npm test`.
 *
 * Refreshes the committed snapshot for the tag pinned in config.json:
 *   messages/<tag>/<Name>{Request,Response}.json  verbatim upstream bytes
 *   messages/<tag>/{Request,Response}Header.json  verbatim upstream bytes
 *   errors/<tag>.json                             extracted from Errors.java, plus a
 *                                                  mechanically derived `retriable` flag
 *
 * `--source-root <dir>` reads the same files from an unpacked Apache Kafka source release
 * instead of the network (its `gradle.properties` version must be the pinned tag), so a
 * checksum-verified tarball is the only input.
 *
 * It never edits tracked-apis.json: adopting a new API is a human decision. It
 * does report upstream *Request.json files that tracked-apis.json does not list.
 */

const fs = require('fs')
const path = require('path')

/**
 * Where upstream files are read from, by repository-relative path.
 * @typedef {object} Source
 * @property {(repoPath: string) => Promise<string>} read the file's text; rejects when absent
 * @property {(repoPath: string) => Promise<string|null>} readIfExists the file's text, or `null`
 */

const RAW = 'https://raw.githubusercontent.com/apache/kafka'
const MESSAGE_PATH = 'clients/src/main/resources/common/message'
const ERRORS_PATH = 'clients/src/main/java/org/apache/kafka/common/protocol/Errors.java'
const EXCEPTIONS_DIR = 'clients/src/main/java/org/apache/kafka/common/errors'
const CONTENTS = `https://api.github.com/repos/apache/kafka/contents/${MESSAGE_PATH}`

/**
 * `RetriableException` is Kafka's rule for the `retriable` flag: an error is retriable iff
 * its exception class transitively extends `org.apache.kafka.common.errors.RetriableException`.
 * These are the java.lang bases the chain bottoms out at when it does not — plain JDK classes
 * that live outside the apache/kafka repo, so they are recognised by name instead of fetched.
 */
const JAVA_LANG_EXCEPTION_ROOTS = new Set(['RuntimeException', 'Exception', 'Error'])
const RETRIABLE_EXCEPTION = 'RetriableException'

const STRING = '"(?:[^"\\\\]|\\\\.)*"'
const ENUM_CONSTANT = new RegExp(
  '\\b([A-Z][A-Z0-9_]*)\\(\\s*(-?\\d+)\\s*,\\s*' +
    '((?:' +
    STRING +
    '\\s*\\+\\s*)*' +
    STRING +
    '|null)\\s*,\\s*' +
    '(?:([A-Za-z0-9_.]+)::new|[^,()]*->[^,()]*)\\s*\\)',
  'g'
)

/** Java source string literal, possibly a `"a" + "b"` concatenation, to a JS string. */
const joinJavaString = (literal) => {
  if (literal === 'null') return null
  const chunks = literal.match(/"(?:[^"\\]|\\.)*"/g) || []
  return chunks.map((chunk) => JSON.parse(chunk)).join('')
}

/**
 * Every constant of the `Errors` enum, sorted by code. `exception` (the factory class name)
 * is what `resolveExceptionHierarchy` needs to derive `retriable`; it is not derivable from
 * this file alone.
 */
const parseErrorsJava = (source) => {
  const start = source.indexOf('public enum Errors {')
  if (start === -1) throw new Error('Errors.java: `public enum Errors {` not found')
  const end = source.indexOf(');', start)
  if (end === -1) throw new Error('Errors.java: end of the constant list not found')
  const body = source.slice(start, end + 2)
  const errors = []
  const seen = new Set()
  let match
  ENUM_CONSTANT.lastIndex = 0
  while ((match = ENUM_CONSTANT.exec(body)) !== null) {
    const code = Number(match[2])
    if (seen.has(code)) throw new Error(`Errors.java: duplicate error code ${code}`)
    seen.add(code)
    errors.push({
      type: match[1],
      code,
      exception: match[4] === undefined ? null : match[4],
      message: joinJavaString(match[3]),
    })
  }
  errors.sort((a, b) => a.code - b.code)
  return errors
}

const get = async (url, extraHeaders) => {
  const response = await globalThis.fetch(url, {
    headers: { 'user-agent': 'countly-kafkajs-compat', ...extraHeaders },
  })
  if (!response.ok) throw new Error(`GET ${url} -> ${response.status} ${response.statusText}`)
  return response.text()
}

/** Like `get`, but a 404 resolves to `null` instead of throwing — used for exception lookups. */
const getIfExists = async (url) => {
  const response = await globalThis.fetch(url, {
    headers: { 'user-agent': 'countly-kafkajs-compat' },
  })
  if (response.status === 404) return null
  if (!response.ok) throw new Error(`GET ${url} -> ${response.status} ${response.statusText}`)
  return response.text()
}

/**
 * @param {string} tag Kafka git tag
 * @returns {Source} the tagged tree on raw.githubusercontent.com
 */
const networkSource = (tag) => ({
  read: (repoPath) => get(`${RAW}/${tag}/${repoPath}`),
  readIfExists: (repoPath) => getIfExists(`${RAW}/${tag}/${repoPath}`),
})

/**
 * @param {string} root an unpacked Apache Kafka source release
 * @returns {Source} that tree, read from disk
 */
const localSource = (root) => ({
  read: (repoPath) => fs.promises.readFile(path.join(root, repoPath), 'utf8'),
  readIfExists: async (repoPath) => {
    try {
      return await fs.promises.readFile(path.join(root, repoPath), 'utf8')
    } catch (error) {
      if (error.code === 'ENOENT') return null
      throw error
    }
  },
})

/**
 * @param {Source} source a local source release
 * @param {string} tag the tag the snapshot is pinned to
 * @returns {Promise<void>}
 * @throws {Error} when the tree's `gradle.properties` names another version, or none
 */
const assertSourceVersion = async (source, tag) => {
  const found = /^version=(\S+)$/m.exec((await source.readIfExists('gradle.properties')) || '')
  if (found === null || found[1] !== tag) {
    throw new Error(
      `--source-root holds ${found === null ? 'no version' : found[1]}, not the pinned ${tag}`
    )
  }
}

/** The direct superclass simple name from one Java `class X extends Y` declaration, or `null`. */
const parseExtendsClause = (source) => {
  const match = source.match(/\bclass\s+\S+\s+extends\s+([A-Za-z0-9_.]+)/)
  return match === null ? null : match[1].split('.').pop()
}

/**
 * True iff `name` transitively extends `RetriableException`, per `extendsByName` (every class
 * name mapped to its direct superclass, or `null` for a class with no `extends` clause).
 * @throws {Error} if the chain revisits a name (a cycle) or leaves `extendsByName` before
 *   reaching `RetriableException` or a recognised JDK root — the caller must have resolved
 *   every class in the chain first.
 */
const resolvesToRetriable = (name, extendsByName, seen = new Set()) => {
  if (name === RETRIABLE_EXCEPTION) return true
  if (JAVA_LANG_EXCEPTION_ROOTS.has(name)) return false
  if (seen.has(name)) throw new Error(`exception hierarchy cycle detected at ${name}`)
  seen.add(name)
  if (!extendsByName.has(name)) throw new Error(`no extends information resolved for ${name}`)
  const parent = extendsByName.get(name)
  return parent === null ? false : resolvesToRetriable(parent, extendsByName, seen)
}

/**
 * Reads the source of exception class `name`, per the resolution order documented on
 * `resolveExceptionHierarchy`: the common `errors` package, then `overridePaths`.
 * @param {Source} source
 * @param {string} name
 * @param {Record<string, string>} overridePaths
 * @returns {Promise<string|null>} the Java source, or `null` if neither location has it.
 */
const fetchExceptionSource = async (source, name, overridePaths) => {
  const defaultSource = await source.readIfExists(`${EXCEPTIONS_DIR}/${name}.java`)
  if (defaultSource !== null) return defaultSource
  const overridePath = overridePaths[name]
  return overridePath === undefined ? null : source.readIfExists(overridePath)
}

/**
 * Resolves `retriable` for every name in `exceptionNames` by walking each one's `extends`
 * chain until it reaches `RetriableException` (retriable) or a JDK root (not retriable).
 *
 * Resolution order per class: (1) `clients/.../common/errors/<Name>.java`; (2) `overridePaths`,
 * for the handful of exception classes that live elsewhere in the repo. A name unresolved by
 * both is never guessed at — it is collected and reported via the `unresolved` property on
 * the thrown error, one fetch pass at a time, so `exceptionPaths.json` can be filled in.
 * @param {string} tag Kafka git tag to fetch from.
 * @param {string[]} exceptionNames distinct exception class names (`Errors.java`'s `exception` field).
 * @param {Record<string, string>} overridePaths `exceptionPaths.json`: class name -> repo-relative path.
 * @param {Source} [source] where the classes are read from; the tagged GitHub tree by default.
 * @returns {Promise<Map<string, boolean>>} exception name -> `retriable`.
 */
const resolveExceptionHierarchy = async (
  tag,
  exceptionNames,
  overridePaths,
  source = networkSource(tag)
) => {
  const extendsByName = new Map()
  const unresolved = []
  const queue = [...new Set(exceptionNames)]
  const queued = new Set(queue)

  while (queue.length > 0) {
    const name = queue.shift()
    if (name === RETRIABLE_EXCEPTION || JAVA_LANG_EXCEPTION_ROOTS.has(name)) continue

    const javaSource = await fetchExceptionSource(source, name, overridePaths)
    if (javaSource === null) {
      unresolved.push(name)
      continue
    }
    const parent = parseExtendsClause(javaSource)
    extendsByName.set(name, parent)
    if (parent !== null && !queued.has(parent)) {
      queued.add(parent)
      queue.push(parent)
    }
  }

  if (unresolved.length > 0) {
    const error = new Error(`unresolved exception classes: ${unresolved.sort().join(', ')}`)
    error.unresolved = unresolved.sort()
    throw error
  }

  const retriable = new Map()
  for (const name of exceptionNames) retriable.set(name, resolvesToRetriable(name, extendsByName))
  return retriable
}

const messageFiles = (tracked) => {
  const files = ['RequestHeader.json', 'ResponseHeader.json']
  for (const api of tracked)
    files.push(`${api.kafkaName}Request.json`, `${api.kafkaName}Response.json`)
  return files.sort()
}

/** `GITHUB_TOKEN`, when set, is sent only on the api.github.com discovery call — raw.githubusercontent.com needs no auth. */
const githubAuthHeaders = () =>
  process.env.GITHUB_TOKEN === undefined
    ? undefined
    : { Authorization: `Bearer ${process.env.GITHUB_TOKEN}` }

const discover = async (tag, files) => {
  const listing = JSON.parse(await get(`${CONTENTS}?ref=${tag}`, githubAuthHeaders()))
  const upstream = listing
    .map((entry) => entry.name)
    .filter((name) => name.endsWith('Request.json'))
  const untracked = upstream.filter((name) => files.indexOf(name) === -1).sort()
  console.log(`\nupstream request definitions at ${tag}: ${upstream.length}`)
  console.log(`not tracked by tracked-apis.json: ${untracked.length}`)
  for (const name of untracked) console.log(`  ${name.replace(/Request\.json$/, '')}`)
  console.log('\nAdopting any of these is a human decision — edit tracked-apis.json by hand.')
}

/**
 * Discovery is a nice-to-have report, not the point of the fetch: the 82-file
 * snapshot and errors/<tag>.json are already written by the time this runs. A
 * rate-limited or unreachable GitHub API must not turn a successful fetch into
 * a failed process — so a discovery failure is caught, warned about, and swallowed.
 */
const safeDiscover = async (tag, files) => {
  try {
    await discover(tag, files)
  } catch (error) {
    console.warn(`compat:fetch: discovery skipped — ${error.message}`)
  }
}

/**
 * @param {string[]} argv the command-line arguments after the script name
 * @returns {string|null} the value of `--source-root <dir>` or `--source-root=<dir>`, if given
 * @throws {Error} when the flag has no value
 */
const sourceRootOf = (argv) => {
  const at = argv.findIndex((arg) => arg === '--source-root' || arg.startsWith('--source-root='))
  if (at === -1) return null
  const value = argv[at].includes('=') ? argv[at].slice(argv[at].indexOf('=') + 1) : argv[at + 1]
  if (!value || value.startsWith('--')) throw new Error('--source-root needs a directory')
  return value
}

const main = async () => {
  const root = __dirname
  const config = JSON.parse(fs.readFileSync(path.join(root, 'config.json'), 'utf8'))
  const trackedApis = JSON.parse(fs.readFileSync(path.join(root, 'tracked-apis.json'), 'utf8'))
  const tag = config.kafkaTag
  const files = messageFiles(trackedApis.tracked)
  const sourceRoot = sourceRootOf(process.argv)
  const source = sourceRoot === null ? networkSource(tag) : localSource(sourceRoot)
  if (sourceRoot !== null) await assertSourceVersion(source, tag)

  const messagesDir = path.join(root, 'messages', tag)
  const errorsDir = path.join(root, 'errors')
  fs.mkdirSync(messagesDir, { recursive: true })
  fs.mkdirSync(errorsDir, { recursive: true })

  console.log(`reading ${files.length} message definitions from apache/kafka@${tag}`)
  for (const name of files) {
    fs.writeFileSync(path.join(messagesDir, name), await source.read(`${MESSAGE_PATH}/${name}`))
    console.log(`  ${name}`)
  }

  console.log(`reading ${ERRORS_PATH}`)
  const errors = parseErrorsJava(await source.read(ERRORS_PATH))
  if (errors.length < 100) {
    throw new Error(`Errors.java: only ${errors.length} constants parsed, the file shape changed`)
  }

  const exceptionPaths = JSON.parse(fs.readFileSync(path.join(root, 'exceptionPaths.json'), 'utf8'))
  const exceptionNames = errors.map((error) => error.exception).filter((name) => name !== null)
  console.log(`resolving the exception hierarchy for ${exceptionNames.length} classes`)
  const retriableByException = await resolveExceptionHierarchy(
    tag,
    exceptionNames,
    exceptionPaths,
    source
  )
  const errorsWithRetriable = errors.map((error) => ({
    ...error,
    retriable: error.exception === null ? false : retriableByException.get(error.exception),
  }))

  const snapshot = { kafkaTag: tag, source: ERRORS_PATH, errors: errorsWithRetriable }
  fs.writeFileSync(path.join(errorsDir, `${tag}.json`), `${JSON.stringify(snapshot, null, 2)}\n`)
  const retriableCount = errorsWithRetriable.filter((error) => error.retriable).length
  console.log(
    `  ${errors.length} error constants (${retriableCount} retriable), codes ${errors[0].code}..${
      errors[errors.length - 1].code
    }`
  )

  if (sourceRoot === null && process.argv.indexOf('--no-discover') === -1) {
    await safeDiscover(tag, files)
  }
}

module.exports = {
  networkSource,
  localSource,
  assertSourceVersion,
  sourceRootOf,
  parseErrorsJava,
  joinJavaString,
  messageFiles,
  discover,
  safeDiscover,
  parseExtendsClause,
  resolvesToRetriable,
  resolveExceptionHierarchy,
  run: main,
}

if (require.main === module) {
  main().catch((error) => {
    if (error.unresolved !== undefined) {
      console.error('\nunresolved exception classes:')
      for (const name of error.unresolved) console.error(`  ${name}`)
      console.error('\nAdd each one to scripts/compat/exceptionPaths.json with its repo path.')
    } else {
      console.error(error.message)
    }
    process.exit(1)
  })
}
