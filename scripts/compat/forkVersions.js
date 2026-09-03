'use strict'

/**
 * Reads what this fork actually implements, from the two sources that can disagree:
 * the `v<N>` directories under src/protocol/requests/<api>/ and the `versions` array
 * that <api>/index.js exports (which is what `lookup()` selects from). A version
 * directory `lookup()` can never pick is dead code; a listed version with no directory
 * is a crash waiting to happen — both are reported as drift (spec C3, D4).
 */

const fs = require('fs')
const path = require('path')

const REQUESTS_DIR = path.join(__dirname, '..', '..', 'src', 'protocol', 'requests')

const numeric = (a, b) => a - b

/** Directory names matching v<N> under one API directory, as sorted numbers. */
const versionDirs = (requestsDir, forkName) =>
  fs
    .readdirSync(path.join(requestsDir, forkName), { withFileTypes: true })
    .filter((entry) => entry.isDirectory() && /^v\d+$/.test(entry.name))
    .map((entry) => Number(entry.name.slice(1)))
    .sort(numeric)

/** Top-level directories that are request definitions (they export an index.js). */
const requestDirectories = (requestsDir) =>
  fs
    .readdirSync(requestsDir, { withFileTypes: true })
    .filter((entry) => entry.isDirectory())
    .map((entry) => entry.name)
    .filter((name) => fs.existsSync(path.join(requestsDir, name, 'index.js')))
    .sort()

const finding = (level, code, message) => ({ level, code, message })

/**
 * @param {{ trackedApis: object, requestsDir: string, requests: object, apiKeys: object }} input
 * @returns {{ apis: object, findings: Array<{level: string, code: string, message: string}> }}
 * Covers tracked APIs only — an API missing from tracked-apis.json is reported by the
 * unknown-directory/unknown-api-key findings above, but never introspected itself.
 */
const introspect = (input) => {
  const { trackedApis, requestsDir, requests, apiKeys } = input
  const entries = trackedApis.tracked.concat(trackedApis.untracked)
  const findings = []
  const apis = {}

  const seenKeys = new Set()
  const seenDirs = new Set()
  const seenApiKeysNames = new Set()
  for (const entry of entries) {
    if (seenKeys.has(entry.apiKey)) {
      findings.push(finding('error', 'duplicate-entry', `api key ${entry.apiKey} is listed twice`))
    }
    seenKeys.add(entry.apiKey)
    if (entry.forkName !== null) seenDirs.add(entry.forkName)
    if (entry.apiKeysName !== null) seenApiKeysNames.add(entry.apiKeysName)
  }

  for (const name of requestDirectories(requestsDir)) {
    if (!seenDirs.has(name)) {
      findings.push(
        finding(
          'error',
          'unknown-directory',
          `src/protocol/requests/${name}/ is not listed in tracked-apis.json`
        )
      )
    }
  }

  for (const name of Object.keys(apiKeys)) {
    if (!seenApiKeysNames.has(name)) {
      findings.push(
        finding(
          'error',
          'unknown-api-key',
          `apiKeys.js "${name}" is not listed in tracked-apis.json`
        )
      )
    }
  }

  for (const entry of entries) {
    if (entry.apiKeysName !== null && requests[entry.apiKeysName] === undefined) {
      findings.push(
        finding(
          'report',
          'missing-requests-entry',
          `apiKeys.js "${entry.apiKeysName}" (key ${entry.apiKey}) has no entry in requests/index.js — lookup() can never reach it`
        )
      )
    }
  }

  for (const entry of trackedApis.tracked) {
    const definition = entry.apiKeysName === null ? undefined : requests[entry.apiKeysName]
    const declared = definition === undefined ? null : definition.versions.map(Number).sort(numeric)
    const dirs = entry.forkName === null ? null : versionDirs(requestsDir, entry.forkName)

    if (dirs !== null && declared !== null && String(dirs) !== String(declared)) {
      findings.push(
        finding(
          'error',
          'dir-mismatch',
          `${entry.kafkaName}: v<N> directories [${dirs}] do not match index.js versions [${declared}]`
        )
      )
    }

    const stub = entry.forkName === null && declared !== null && declared.length === 0
    apis[entry.kafkaName] = {
      hasCodec: entry.forkName !== null || stub,
      stub,
      versionDirs: dirs,
      declaredVersions: declared,
      forkVersions: entry.forkName === null ? null : declared === null ? dirs : declared,
    }
  }

  return { apis, findings }
}

const readFork = (trackedApis) =>
  introspect({
    trackedApis,
    requestsDir: REQUESTS_DIR,
    requests: require(path.join(REQUESTS_DIR, 'index.js')).requests,
    apiKeys: require(path.join(REQUESTS_DIR, 'apiKeys.js')),
  })

module.exports = { REQUESTS_DIR, versionDirs, requestDirectories, introspect, readFork }
