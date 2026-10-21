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
  return cells
}

module.exports = { lifecycleCells, createCoordinator, EVENTS, BOUNDARIES, PROTOCOLS, MEMBERSHIPS }
