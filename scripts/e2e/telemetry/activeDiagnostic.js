#!/usr/bin/env node
'use strict'

const fs = require('node:fs')
const path = require('node:path')
const crypto = require('node:crypto')
const { spawn, execFileSync } = require('node:child_process')
const { performance } = require('node:perf_hooks')
const { MEMORY_FLAGS, hostContext } = require('../../../testHelpers/processMemory')
const { MemoryOutput, KINDS, LIMITS } = require('./resourceArtifact')
const { archiveSources, verifySources } = require('./retainedDiagnostic')
const { BUDGET } = require('./retained/contract')

const ROOT = path.resolve(__dirname, '../../..')
const FLAGS = { diagnostic: true, acceptanceEligible: false, attributionComplete: false }

/**
 * Runs the unchanged resource child and flags with only diagnostic environment hooks enabled.
 * @param {string} directory fresh child directory @param {object} identity issued source/run tuple
 * @returns {Promise<object>} joined natural-exit facts and all original readings, including failures
 */
const supervise = (directory, identity) =>
  new Promise((resolve) => {
    fs.mkdirSync(directory)
    fs.writeFileSync(path.join(directory, 'identity.json'), JSON.stringify(identity))
    const output = new MemoryOutput(identity, () => performance.now())
    const before = hostContext()
    const startedAt = performance.now()
    const env = {
      ...process.env,
      RESOURCE_CHILD_ID: identity.childId,
      RESOURCE_TREE: identity.tree,
      RESOURCE_RUN_ID: identity.runId,
      ...(identity.variant === 'snapshot'
        ? { RESOURCE_ACTIVE_SNAPSHOT_DIRECTORY: directory }
        : { ACTIVE_CENSUS_DIRECTORY: directory, ACTIVE_CENSUS_VARIANT: identity.variant }),
    }
    for (const key of ['NODE_OPTIONS', 'NODE_V8_COVERAGE', 'RESOURCE_SEED']) delete env[key]
    if (identity.variant === 'snapshot')
      for (const key of ['ACTIVE_CENSUS_DIRECTORY', 'ACTIVE_CENSUS_VARIANT', 'RESOURCE_WITNESSES'])
        delete env[key]
    const child = spawn(
      process.execPath,
      [
        ...MEMORY_FLAGS,
        path.join(ROOT, 'src/telemetry/__tests__/fixtures/publicMemoryChild.js'),
        identity.kind,
      ],
      {
        env,
        stdio: ['ignore', 'pipe', 'pipe'],
      }
    )
    let failure = null
    let stderrBytes = 0
    let timedOut = false
    const limit = setTimeout(() => {
      timedOut = true
      child.kill('SIGKILL')
    }, BUDGET.child)
    child.stdout.on('data', (chunk) => {
      output.write(chunk)
      if (output.bytes <= LIMITS.outputBytes)
        fs.appendFileSync(path.join(directory, 'stdout.txt'), chunk)
      if (output.failure) child.kill('SIGKILL')
    })
    child.stderr.on('data', (chunk) => {
      stderrBytes += chunk.length
      if (stderrBytes <= 65536) fs.appendFileSync(path.join(directory, 'stderr.txt'), chunk)
      else {
        failure = 'active diagnostic stderr overflow'
        child.kill('SIGKILL')
      }
    })
    child.on('error', (error) => {
      failure = error.message
    })
    child.on('close', (code, signal) => {
      clearTimeout(limit)
      const result = {
        ...FLAGS,
        identity,
        pid: child.pid,
        code,
        signal,
        timedOut,
        stderrBytes,
        elapsedMs: performance.now() - startedAt,
        contexts: { before, after: hostContext() },
        ...output.finish(),
      }
      result.failure = failure ?? result.failure
      const manifest = result.readings?.manifest
      result.complete =
        code === 0 &&
        signal === null &&
        !timedOut &&
        !result.failure &&
        stderrBytes === 0 &&
        result.closedAfterMarkMs <= BUDGET.receipt &&
        ['childId', 'tree', 'runId', 'kind', 'node', 'v8'].every(
          (key) => manifest?.[key] === identity[key]
        ) &&
        JSON.stringify(manifest?.execArgv) === JSON.stringify(MEMORY_FLAGS)
      fs.writeFileSync(path.join(directory, 'result.json'), JSON.stringify(result))
      resolve({
        kind: identity.kind,
        variant: identity.variant,
        childId: identity.childId,
        complete: result.complete,
      })
    })
  })

/**
 * Captures four serial public arms in an isolated CI job; scalar companions run on other runners.
 * @param {string[]} argv explicit diagnostic selector, fresh directory and variant
 * @returns {Promise<void>} source archives and original scalar/paired evidence
 * @throws {Error} outside isolated CI, on source changes or incomplete natural exit
 */
const main = async ([selector, directory, variant]) => {
  if (
    selector !== '--diagnostic-only' ||
    !directory ||
    !['capture', 'scalar'].includes(variant) ||
    process.env.CI !== 'true' ||
    process.env.TELEMETRY_RESOURCES_ARTIFACT_DIR
  )
    throw new Error('Use --diagnostic-only <new-directory> <capture|scalar> in isolated CI')
  fs.mkdirSync(directory, { recursive: true })
  if (fs.readdirSync(directory).length) throw new Error('active census directory must be fresh')
  const sources = archiveSources(directory)
  verifySources(directory, sources)
  const identity = {
    schema: 1,
    ...FLAGS,
    tree: execFileSync('git', ['rev-parse', 'HEAD'], { cwd: ROOT, encoding: 'utf8' }).trim(),
    runId: process.env.GITHUB_RUN_ID,
    attempt: process.env.GITHUB_RUN_ATTEMPT,
    node: process.version,
    v8: process.versions.v8,
    flags: MEMORY_FLAGS,
    variant,
    sources,
  }
  const summary = { ...identity, results: [], complete: false }
  const save = () =>
    fs.writeFileSync(path.join(directory, 'active-summary.json'), JSON.stringify(summary))
  save()
  for (const kind of KINDS) {
    summary.results.push(
      await supervise(path.join(directory, kind), {
        ...identity,
        arm: 'public',
        kind,
        childId: crypto.randomUUID(),
      })
    )
    verifySources(directory, sources)
    save()
  }
  summary.complete =
    summary.results.length === KINDS.length && summary.results.every((row) => row.complete)
  save()
  if (!summary.complete)
    throw new Error('incomplete active diagnostic; original evidence preserved')
}

module.exports = { main, supervise }
if (require.main === module)
  main(process.argv.slice(2)).catch((error) => {
    process.stderr.write(`${error.stack}\n`)
    process.exitCode = 1
  })
