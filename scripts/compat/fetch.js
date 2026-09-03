#!/usr/bin/env node
'use strict'

/**
 * `yarn compat:fetch` — the only networked part of the compat tool.
 *
 * Refreshes the committed snapshot for the tag pinned in config.json:
 *   messages/<tag>/<Name>{Request,Response}.json  verbatim upstream bytes
 *   messages/<tag>/{Request,Response}Header.json  verbatim upstream bytes
 *   errors/<tag>.json                             extracted from Errors.java
 *
 * It never edits tracked-apis.json: adopting a new API is a human decision. It
 * does report upstream *Request.json files that tracked-apis.json does not list.
 */

const fs = require('fs')
const path = require('path')

const RAW = 'https://raw.githubusercontent.com/apache/kafka'
const MESSAGE_PATH = 'clients/src/main/resources/common/message'
const ERRORS_PATH = 'clients/src/main/java/org/apache/kafka/common/protocol/Errors.java'
const CONTENTS = `https://api.github.com/repos/apache/kafka/contents/${MESSAGE_PATH}`

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
const joinJavaString = literal => {
  if (literal === 'null') return null
  const chunks = literal.match(/"(?:[^"\\]|\\.)*"/g) || []
  return chunks.map(chunk => JSON.parse(chunk)).join('')
}

/** Every constant of the `Errors` enum, sorted by code. Retriable flags are not derivable here. */
const parseErrorsJava = source => {
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

const messageFiles = tracked => {
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
  const upstream = listing.map(entry => entry.name).filter(name => name.endsWith('Request.json'))
  const untracked = upstream.filter(name => files.indexOf(name) === -1).sort()
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

const main = async () => {
  const root = __dirname
  const config = JSON.parse(fs.readFileSync(path.join(root, 'config.json'), 'utf8'))
  const trackedApis = JSON.parse(fs.readFileSync(path.join(root, 'tracked-apis.json'), 'utf8'))
  const tag = config.kafkaTag
  const files = messageFiles(trackedApis.tracked)

  const messagesDir = path.join(root, 'messages', tag)
  const errorsDir = path.join(root, 'errors')
  fs.mkdirSync(messagesDir, { recursive: true })
  fs.mkdirSync(errorsDir, { recursive: true })

  console.log(`fetching ${files.length} message definitions from apache/kafka@${tag}`)
  for (const name of files) {
    const body = await get(`${RAW}/${tag}/${MESSAGE_PATH}/${name}`)
    fs.writeFileSync(path.join(messagesDir, name), body)
    console.log(`  ${name}`)
  }

  console.log(`fetching ${ERRORS_PATH}`)
  const errors = parseErrorsJava(await get(`${RAW}/${tag}/${ERRORS_PATH}`))
  if (errors.length < 100) {
    throw new Error(`Errors.java: only ${errors.length} constants parsed, the file shape changed`)
  }
  const snapshot = { kafkaTag: tag, source: ERRORS_PATH, errors }
  fs.writeFileSync(path.join(errorsDir, `${tag}.json`), `${JSON.stringify(snapshot, null, 2)}\n`)
  console.log(
    `  ${errors.length} error constants, codes ${errors[0].code}..${errors[errors.length - 1].code}`
  )

  if (process.argv.indexOf('--no-discover') === -1) await safeDiscover(tag, files)
}

module.exports = {
  parseErrorsJava,
  joinJavaString,
  messageFiles,
  discover,
  safeDiscover,
  run: main,
}

if (require.main === module) {
  main().catch(error => {
    console.error(error.message)
    process.exit(1)
  })
}
