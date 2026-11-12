const ROWS = [
  { node: 24, kafka: '4.4.0', group: 'broker' },
  { node: 24, kafka: '4.4.0', group: 'admin' },
  { node: 24, kafka: '4.4.0', group: 'producer' },
  { node: 24, kafka: '4.4.0', group: 'consumer' },
  { node: 24, kafka: '4.4.0', group: 'others' },
  { node: 24, kafka: '4.4.0', group: 'oauthbearer', compose: 'docker-compose.oauthbearer.yml' },
  { node: 22, kafka: '4.4.0', group: 'broker' },
  { node: 22, kafka: '4.4.0', group: 'others' },
  { node: 26, kafka: '4.4.0', group: 'broker' },
  { node: 26, kafka: '4.4.0', group: 'others' },
  // 55 minutes minus the relocated 35-minute soak step leaves the functional lane 20 minutes.
  { node: 24, kafka: '4.4.0', group: 'share-base', clients: '4.4.0', timeout: 20 },
  {
    node: 24,
    kafka: '4.4.0',
    group: 'share',
    compose: 'docker-compose.share.yml',
    clients: '4.4.0',
  },
  { node: 24, kafka: '4.4.0', group: 'share-limit', compose: 'docker-compose.share-limit.yml' },
]

/**
 * Select all variants of requested integration groups only on manual runs.
 * @param {string} event GitHub event name.
 * @param {string} [lanes] Optional comma-separated group names.
 * @returns {{include: object[]}} GitHub Actions matrix.
 */
const integrationPlan = (event, lanes = '') => {
  if (event !== 'workflow_dispatch' || !lanes.trim()) return { include: ROWS }
  const selected = new Set(lanes.split(',').map((lane) => lane.trim()))
  const groups = new Set(ROWS.map(({ group }) => group))
  for (const group of selected) {
    if (!groups.has(group)) throw new Error(`Unknown integration group: ${JSON.stringify(group)}`)
  }
  return { include: ROWS.filter(({ group }) => selected.has(group)) }
}

if (require.main === module) {
  console.log(
    `matrix=${JSON.stringify(integrationPlan(process.env.CI_EVENT, process.env.CI_LANES))}`
  )
}

module.exports = { integrationPlan }
