const { createHash } = require('node:crypto')

const TITLE = 'preserves share identities and resources through the short soak'
const PROFILES = Object.freeze({
  short: Object.freeze({
    lane: 'soak',
    stem: 'share-soak',
    selector: 'SHARE_SOAK_E2E',
    profile: 'share-consumer-short',
    slotMs: 32000,
    profileMs: 960000,
    totalMs: 2040000,
    parentMs: 2100000,
    jobMs: 3300000,
  }),
  full: Object.freeze({
    lane: 'soak-full',
    stem: 'share-soak-full',
    selector: 'SHARE_SOAK_FULL',
    profile: 'share-consumer',
    slotMs: 64000,
    profileMs: 1920000,
    totalMs: 3000000,
    parentMs: 3060000,
    jobMs: 4260000,
  }),
})

/** @param {string|Buffer} bytes Original bytes. @returns {string} SHA-256 digest. */
const digest = (bytes) => createHash('sha256').update(bytes).digest('hex')

/**
 * Selects a fixed command without loading the workload in the parent realm.
 * @param {'short'|'full'} profile Fixed profile.
 * @returns {string[]} Exact child arguments.
 * @throws {Error} On any other profile.
 */
const childArgs = (profile) => {
  if (!Object.hasOwn(PROFILES, profile)) throw new Error('Unknown share soak profile')
  return profile === 'full'
    ? ['--expose-gc', 'scripts/soak/soak.js', '--profile', 'share-consumer']
    : [
        '--expose-gc',
        'node_modules/jest/bin/jest.js',
        '--ci',
        '--detectOpenHandles',
        '--runInBand',
        '--runTestsByPath',
        'src/consumer/share/__tests__/shareSoakLive.spec.js',
        '--json',
        '--outputFile=.tmp/e2e/share-soak-jest.json',
      ]
}

/**
 * Refuses runtime substitution and contradictory live gates, then sets only the fixed selector.
 * @param {'short'|'full'} profile Fixed profile.
 * @param {object} env Original process environment.
 * @returns {object} Child environment with its selected live gate.
 * @throws {Error} On hooks, alternate fixtures or contradictory selectors.
 */
const childEnvironment = (profile, env) => {
  const selected = PROFILES[profile]
  if (!selected) throw new Error('Unknown share soak profile')
  for (const key of [
    'NODE_OPTIONS',
    'NODE_PATH',
    'LD_PRELOAD',
    'DYLD_INSERT_LIBRARIES',
    'JEST_WORKER_ID',
    'DOCKER_HOST',
    'DOCKER_CONTEXT',
    'COMPOSE_PROFILES',
    'KAFKAJS_BROKERS',
  ])
    if (env[key]) throw new Error(`Forbidden runtime override: ${key}`)
  for (const key of Object.keys(env))
    if (key.startsWith('SHARE_') && key !== selected.selector && env[key])
      throw new Error(`Contradictory selector: ${key}`)
  for (const [key, expected] of Object.entries({
    KAFKA_VERSION: '4.3.1',
    KAFKAJS_LANE: 'docker',
    KAFKAJS_DEBUG_PROTOCOL_BUFFERS: '1',
    COMPOSE_FILE: 'docker-compose.yml',
  }))
    if (env[key] !== undefined && env[key] !== expected)
      throw new Error(`Forbidden tuple override: ${key}`)
  if (env[selected.selector] !== undefined && env[selected.selector] !== '1')
    throw new Error('Disabled live selector')
  return { ...env, [selected.selector]: '1' }
}

/**
 * Hashes only declared acceptance-relevant environment, excluding unrelated runner credentials.
 * @param {object} env Child environment.
 * @returns {object} Canonical environment projection and digest.
 */
const environmentIdentity = (env) => {
  const values = Object.fromEntries(
    [
      'COMPOSE_FILE',
      'COMPOSE_PROJECT_NAME',
      'KAFKAJS_DEBUG_PROTOCOL_BUFFERS',
      'KAFKAJS_LANE',
      'KAFKAJS_TREE',
      'KAFKAJS_SOAK_PROFILE',
      'KAFKA_VERSION',
      'SHARE_SOAK_E2E',
      'SHARE_SOAK_FULL',
    ].map((key) => [key, env[key] ?? null])
  )
  return { values, sha256: digest(JSON.stringify(values)) }
}

module.exports = { TITLE, PROFILES, digest, childArgs, childEnvironment, environmentIdentity }
