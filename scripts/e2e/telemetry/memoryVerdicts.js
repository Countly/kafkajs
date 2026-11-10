const assert = require('node:assert/strict')
const fs = require('fs')
const path = require('path')
const { verifyEvidence, verifyScenario, verifyQuietResources } = require('./evidence')
const { ordinaryOwnership } = require('./semantics')

/**
 * Requires the D142 resource verdicts from a functionally verified, digest-bound archive.
 * This entry point is required only by memory-gates; phase lanes retain functional validation.
 * @param {object} record stamped scenario and sidecar catalogue
 * @param {string} directory absolute evidence directory
 * @returns {void}
 * @throws {Error} for incomplete evidence, an unowned socket or a surviving owned resource
 */
const verifyMemoryScenario = (record, directory) => {
  verifyScenario(record, directory)
  const manifest = record.sidecars
  const read = (relative) => {
    assert(
      manifest.files.some((f) => f.path === relative),
      'Unlisted memory sidecar'
    )
    return JSON.parse(fs.readFileSync(path.join(directory, relative), 'utf8'))
  }
  const transcripts = Object.fromEntries(
    Object.entries(manifest.owners).map(([owner, file]) => [owner, read(file)])
  )
  const ownership = (owner, refusal) => ordinaryOwnership(transcripts[owner], { refusal })
  if (manifest.scenario === 'telemetry-shutdown') {
    for (const segment of manifest.segments) {
      const transcript = read(segment.transcript)
      assert.deepEqual(transcript.cleanup, { children: 0, timers: 0, stillOpen: [], attached: 0 })
      assert.equal(
        transcript.sockets.filter((s) => s.kind === 'telemetry').length,
        1,
        'A segment must own exactly one telemetry transport'
      )
      assert(
        transcript.sockets.every((s) => Number.isFinite(s.closedAt)),
        'A segment socket never closed'
      )
    }
    assert.deepEqual(record.results.decoderOccupancy, { running: 0, queued: 0 })
    verifyQuietResources(record.results.resources ?? {})
  } else if (manifest.scenario === 'telemetry-outcomes') {
    for (const name of Object.keys(record.results.arms))
      if (record.mode === 'base' || name === 'control')
        for (const kind of ['producer', 'consumer']) ownership(`${name}-${kind}`)
    if (record.mode === 'base') ownership('share', record.results.share)
  } else {
    assert.equal(manifest.scenario, 'telemetry-unsupported', 'Unknown telemetry memory scenario')
    for (const kind of ['producer', 'consumer', 'admin', 'share']) {
      const on = ownership(kind, kind === 'share' ? record.outcomes[kind] : undefined)
      const off = ownership(
        `${kind}-off`,
        kind === 'share' ? record.disabledOutcomes[kind] : undefined
      )
      const roles = (list) => [...new Set(list.map((r) => r.split(':').at(-1)))].sort()
      assert.deepEqual(roles(on), roles(off), 'Enabled/disabled socket ownership differs')
    }
  }
  for (const transcript of Object.values(transcripts))
    assert.deepEqual(transcript.cleanup, { children: 0, timers: 0, stillOpen: [], attached: 0 })
}

/**
 * Requires the lane's exact functional catalogue before applying every relocated memory verdict.
 * @param {string} group selected lane
 * @param {string} directory absolute evidence directory
 * @param {string[]} files stamped JSONL files
 * @returns {void} @throws {Error} for a missing scenario or failed verdict
 */
const verifyMemoryEvidence = (group, directory, files) => {
  verifyEvidence(group, directory, files)
  for (const file of files)
    for (const line of fs.readFileSync(file, 'utf8').trim().split('\n'))
      verifyMemoryScenario(JSON.parse(line), directory)
}
module.exports = { verifyMemoryScenario, verifyMemoryEvidence }
