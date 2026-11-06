const fs = require('node:fs/promises')
const {
  isRecord,
  text,
  optionsOf,
  failure,
  safe,
  fileVersion,
  providerOf,
} = require('./tokenCache')

/** Three base64url segments; the third may be empty, as in an unsecured JWT. */
const JWT_SHAPE = /^[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]*$/
/** The largest token file read, in bytes. */
const FILE_LIMIT = 1048576n

/**
 * @param {string} segment
 * @returns {*} the segment's JSON value, or null when it is not base64url JSON
 */
const segmentOf = (segment) => {
  try {
    return JSON.parse(Buffer.from(segment, 'base64url').toString('utf8'))
  } catch {
    return null
  }
}

/**
 * @param {string} raw the file's contents
 * @returns {string} the token: one JWT-shaped line whose header and payload are JSON objects, with
 *   one final LF or CRLF allowed and removed
 * @throws {KafkaJSSASLAuthenticationError} (terminal) on anything else; the contents never enter it
 */
const tokenOf = (raw) => {
  const value = raw.replace(/\r?\n$/, '')
  const [header, payload] = value.split('.')
  if (!JWT_SHAPE.test(value) || !isRecord(segmentOf(header)) || !isRecord(segmentOf(payload))) {
    throw failure('file-token', {}, true)
  }
  return value
}

/**
 * Builds the existing `oauthBearerProvider` callback over a token file (Java's
 * `FileJwtRetriever`). Every authentication stats the file within the retrieval budget; the file
 * is read again only when its identity changed, or when the cached token is due for refresh or
 * expired, so a rotated token is used at the next authentication and an unchanged usable one is
 * read once. A file found unreadable, empty or malformed is an error and is never cached. The
 * cached token is served only while the file still has the identity it was read under, so an older
 * token is never used after its file has changed.
 * @param {object} input see `oauthBearer.fileToken` in the type declarations
 * @returns {import('../../types').OAuthBearerTokenProvider} the callback, with `close()`
 * @throws {KafkaJSInvalidConfigurationError} naming the first invalid option, before any work
 */
const fileToken = (input) => {
  const options = optionsOf(input)
  const path = text(input.path, 'path')
  return providerOf(
    async ({ check, previous, forceRead }) => {
      let version
      let raw
      try {
        const stat = await fs.stat(path, { bigint: true })
        check()
        version = fileVersion(stat)
        if (!forceRead && previous.version === version) return previous
        if (stat.size > FILE_LIMIT) throw failure('file-token', {}, true)
        raw = await fs.readFile(path, 'utf8')
      } catch (error) {
        throw safe(error, 'file')
      }
      check()
      return { value: tokenOf(raw), version }
    },
    options,
    true
  )
}

module.exports = { fileToken }
