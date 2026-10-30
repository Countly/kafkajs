const { createCoordinator } = require('../src/consumer/__tests__/fixtures/lifecycleCoordinator')

const EVENTS = ['stop', 'disconnect', 'crash', 'rejoin', 'poll-expiry']
const BOUNDARIES = [
  'before-handler',
  'in-handler',
  'in-heartbeat',
  'commit:coordinator-lookup',
  'commit:pool-connect',
  'commit:auth-encode',
  'commit:pending-queue',
  'commit:retry-backoff',
  'commit:answer-held',
  'in-join',
  'in-sync',
  'in-leave',
  'poll-expiry:leave',
  'pre-release-commit',
]
const PROTOCOLS = ['classic-eager', 'classic-cooperative', 'consumer']
const MEMBERSHIPS = ['dynamic', 'static']
const MANUAL_EVENTS = ['stop', 'disconnect', 'crash', 'reassign', 'unsubscribe', 'rebuild']
const MANUAL_COMMON_BOUNDARIES = [
  'before-handler',
  'in-handler',
  'manual:apply',
  'resolution:list-offsets',
  'position:answer-held',
  'idle-wait',
]
const MANUAL_COMMIT_BOUNDARIES = [
  'commit:coordinator-lookup',
  'commit:pool-connect',
  'commit:auth-encode',
  'commit:pending-queue',
  'commit:retry-backoff',
  'commit:answer-held',
]
const MANUAL_MEMBERSHIPS = ['grouped', 'no-group']

/**
 * The manual-assignment cells, appended after the membership cells without joining their product:
 * a manual run has no join, sync, heartbeat, poll-expiry, group-loss or consumer-target boundary,
 * and only a grouped manual run commits, so those boundaries are excluded rather than passed
 * vacuously.
 * @returns {Array<{event: string, boundary: string, protocol: 'manual', membership: 'grouped'|'no-group'}>}
 */
const manualCells = () => {
  const cells = []
  for (const event of MANUAL_EVENTS) {
    for (const boundary of MANUAL_COMMON_BOUNDARIES)
      for (const membership of MANUAL_MEMBERSHIPS)
        cells.push({ event, boundary, protocol: 'manual', membership })
    for (const boundary of MANUAL_COMMIT_BOUNDARIES)
      cells.push({ event, boundary, protocol: 'manual', membership: 'grouped' })
  }
  return cells
}

/**
 * Generates every reviewable lifecycle cell, with only the protocol's explicit exclusions.
 * @returns {Array<{event: string, boundary: string, protocol: string, membership: string}>}
 */
const lifecycleCells = () => {
  const cells = []
  for (const event of EVENTS)
    for (const boundary of BOUNDARIES)
      for (const protocol of PROTOCOLS)
        for (const membership of MEMBERSHIPS) {
          if (
            ['in-join', 'in-sync'].includes(boundary) &&
            (protocol === 'consumer' || ['stop', 'disconnect'].includes(event))
          )
            continue
          if (boundary === 'in-leave' && protocol !== 'consumer' && membership === 'static')
            continue
          if (boundary === 'poll-expiry:leave' && event !== 'poll-expiry') continue
          if (boundary === 'pre-release-commit' && event !== 'rejoin') continue
          cells.push({ event, boundary, protocol, membership })
        }
  return [...cells, ...manualCells()]
}

module.exports = { lifecycleCells, createCoordinator, EVENTS, BOUNDARIES, PROTOCOLS, MEMBERSHIPS }
