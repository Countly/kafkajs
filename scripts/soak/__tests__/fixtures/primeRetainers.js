const fs = require('node:fs')
const os = require('node:os')
const path = require('node:path')
const { spawnSync } = require('node:child_process')

const MAX_DRIVERS = 2
const MAX_PATH = 24
const MAX_INCOMING = 8
const MAX_LABEL = 160
let captured = false

/**
 * Projects one heap node or edge to bounded scalar labels; no heap objects escape the analyser.
 * @param {object} value Heap node or edge description.
 * @returns {object} Diagnostic labels and stable snapshot identity when available.
 */
const labels = (value) => ({
  ...(value.id === undefined ? {} : { id: value.id }),
  type: value.type,
  name: String(value.name).slice(0, MAX_LABEL),
})

/**
 * Finds priming drivers by their original prefix, without tagging or dereferencing live owners.
 * Paths exclude weak edges; labelled ephemeron dependencies are reported explicitly as conditional.
 * @param {string} filename Snapshot captured after the failed observation's job ended.
 * @returns {object} Bounded candidate paths, incoming owners and explicit truncation counts.
 * @throws {Error} Snapshot parsing or graph analysis failure.
 */
const summarize = (filename) => {
  const { analyseSnapshot, readSnapshot } = require('../../../e2e/telemetry/retained/heapDiff')
  const graph = analyseSnapshot(readSnapshot(filename))
  let matches = 0
  const drivers = []
  for (let v = 0; v < graph.n; v++) {
    const node = graph.describe(v)
    if (node.type !== 'object' || node.name !== 'ShareSoakDriver') continue
    if (
      !graph
        .outgoing(v)
        .some(
          ({ target, edge }) =>
            edge.type === 'property' &&
            edge.name === 'prefix' &&
            graph.describe(target).name === 'soak-share-prime'
        )
    )
      continue
    matches++
    if (drivers.length === MAX_DRIVERS) continue
    const reachable = graph.groupIndex[v] >= 0
    const route = reachable ? graph.rootPath(v) : []
    const incoming = graph.incoming(v)
    const half = MAX_PATH / 2
    const bounded =
      route.length <= MAX_PATH ? route : [...route.slice(0, half), ...route.slice(-half)]
    drivers.push({
      node: labels(node),
      reachable,
      path: bounded.map(({ node, edge }) => ({
        node: labels(node),
        edge: edge && labels(edge),
      })),
      omittedPathNodes: Math.max(0, route.length - MAX_PATH),
      conditionalPath: route.some(({ edge }) => String(edge?.name).includes('part of key')),
      incoming: incoming.slice(0, MAX_INCOMING).map(({ owner, edge }) => ({
        owner: labels(owner),
        edge: labels(edge),
      })),
      omittedIncoming: Math.max(0, incoming.length - MAX_INCOMING),
    })
  }
  return { diagnosticOnly: true, matches, omittedDrivers: matches - drivers.length, drivers }
}

/**
 * Captures at most once per suite, only for an opted-in Node 22 failed release assertion.
 * A new job clears WeakRef keep-alives. Analysis runs in a child to avoid duplicating the graph
 * in the test worker; the temporary snapshot is removed and failures never replace the assertion.
 * @param {string} stage Failed release checkpoint; no original objects may be passed here.
 * @returns {Promise<void>} Only bounded scalar diagnostic output survives.
 */
const diagnosePrimeRoots = async (stage) => {
  if (
    captured ||
    process.env.KAFKAJS_PRIMING_RETAINERS !== '1' ||
    !process.versions.node.startsWith('22.')
  )
    return
  captured = true
  let directory
  const report = (fields) =>
    process.stderr.write(
      `${JSON.stringify({ event: 'share-prime-retainers', diagnosticOnly: true, stage, ...fields })}\n`
    )
  try {
    await new Promise(setImmediate)
    directory = fs.mkdtempSync(path.join(os.tmpdir(), 'share-prime-retainers-'))
    const filename = require('node:v8').writeHeapSnapshot(
      path.join(directory, 'prime.heapsnapshot')
    )
    const child = spawnSync(process.execPath, ['--max-old-space-size=6144', __filename, filename], {
      encoding: 'utf8',
      timeout: 300000,
      maxBuffer: 65536,
    })
    if (child.error || child.status !== 0)
      throw child.error ?? new Error(`Retainer analysis failed: ${child.stderr.slice(0, 512)}`)
    report(JSON.parse(child.stdout))
  } catch (error) {
    report({ error: String(error.message).slice(0, 512) })
  } finally {
    if (directory)
      try {
        fs.rmSync(directory, { recursive: true, force: true })
      } catch (error) {
        report({ cleanupError: String(error.message).slice(0, 512) })
      }
  }
}

if (require.main === module) process.stdout.write(`${JSON.stringify(summarize(process.argv[2]))}\n`)

module.exports = { diagnosePrimeRoots }
