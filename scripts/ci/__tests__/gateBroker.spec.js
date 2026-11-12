const fs = require('fs')
const path = require('path')
const { createHash } = require('crypto')

const root = path.join(__dirname, '..', '..', '..')
const groupStates = path.join('testHelpers', 'consumerGroupStates')

/**
 * @param {...string} parts path segments below the repository root
 * @returns {string} the file's text
 */
const read = (...parts) => fs.readFileSync(path.join(root, ...parts), 'utf8')

const gate = JSON.parse(read('scripts', 'ci', 'gate.json'))
const workflow = read('.github', 'workflows', 'gate.yml')

/**
 * @param {string} id a gate check id
 * @returns {object} that check
 * @throws {Error} when the manifest has no such check
 */
const check = (id) => {
  const found = gate.checks.find((entry) => entry.id === id)
  if (!found) throw new Error(`The gate manifest has no check ${id}`)
  return found
}

/**
 * @param {string} text file text
 * @returns {string[]} every distinct `kafka-storage-<version>-test*.jar` file name it carries
 */
const storageJars = (text) => [
  ...new Set(text.match(/kafka-storage-\d+\.\d+\.\d+-test(?:-fixtures)?\.jar/g) || []),
]

/** @returns {string} the digest the workflow pins for the tiered storage jar */
const tieredJarDigest = () => /^ {2}TIERED_JAR_SHA256: (\S+)$/m.exec(workflow)[1]

const FOCUSED = [
  'e2e-api-identity',
  'e2e-txn-offset-commit-v6',
  'e2e-delete-groups-v3',
  'e2e-rack-aware-producer',
  'e2e-share',
]
const CONTROLS = FOCUSED.filter((id) => id !== 'e2e-share').map((id) => `${id}-kafka-4-3`)

describe('exit gate broker', () => {
  test('the Exit gate default broker is Kafka 4.4.0', () => {
    expect(/^ {2}DEFAULT_KAFKA_VERSION: (\S+)$/m.exec(workflow)[1]).toBe('4.4.0')
  })

  test('the focused checks run on the default broker and no -kafka-4-4 id remains', () => {
    for (const id of FOCUSED) expect(check(id).kafkaVersion).toBeUndefined()
    expect(gate.checks.map(({ id }) => id).filter((id) => id.endsWith('-kafka-4-4'))).toEqual([])
  })

  test('the four old-broker controls run on Kafka 4.3.2', () => {
    expect(CONTROLS).toHaveLength(4)
    for (const id of CONTROLS) expect(check(id).kafkaVersion).toBe('4.3.2')
  })

  test('the floor checks run on Kafka 4.0.2', () => {
    const floors = gate.checks.filter(({ id }) => id.startsWith('floor-'))
    expect(floors.length).toBeGreaterThan(0)
    for (const { kafkaVersion } of floors) expect(kafkaVersion).toBe('4.0.2')
  })

  test('only the floor checks and the old-broker controls pin a broker version', () => {
    const pins = Object.fromEntries(
      gate.checks
        .filter((entry) => entry.kafkaVersion)
        .map(({ id, kafkaVersion }) => [id, kafkaVersion])
    )
    expect(pins).toEqual({
      'floor-conditional': '4.0.2',
      'floor-core': '4.0.2',
      'floor-gssapi': '4.0.2',
      'floor-oauth': '4.0.2',
      'floor-tokens': '4.0.2',
      'e2e-api-identity-kafka-4-3': '4.3.2',
      'e2e-txn-offset-commit-v6-kafka-4-3': '4.3.2',
      'e2e-delete-groups-v3-kafka-4-3': '4.3.2',
      'e2e-rack-aware-producer-kafka-4-3': '4.3.2',
    })
  })

  test('the tiered storage test-fixtures jar is the 4.4.0 jar in gate.yml and docker-compose.tiered.yml', () => {
    const jar = ['kafka-storage-4.4.0-test-fixtures.jar']
    expect(storageJars(workflow)).toEqual(jar)
    expect(storageJars(read('docker-compose.tiered.yml'))).toEqual(jar)
    expect(workflow).toContain('/kafka-storage/4.4.0/kafka-storage-4.4.0-test-fixtures.jar')
    expect(tieredJarDigest()).toMatch(/^[a-f0-9]{64}$/)
  })

  test('the admin tiered spec checks the jar and digest the gate pins', () => {
    const spec = read('src', 'admin', '__tests__', 'adminTieredLive.spec.js')
    expect(storageJars(spec)).toEqual(['kafka-storage-4.4.0-test-fixtures.jar'])
    expect(/const jarHash = '([a-f0-9]{64})'/.exec(spec)[1]).toBe(tieredJarDigest())
  })

  test('the consumer-group-state build knows Kafka 4.4.0', () => {
    const lines = /case "\$version" in ([^)]+)\)/.exec(read(groupStates, 'build.sh'))[1].split('|')
    expect(lines).toContain('4.4.0')
    const patch = fs.readFileSync(path.join(root, groupStates, '4.4.0.patch'))
    const entry = JSON.parse(read(groupStates, 'provenance.json'))['4.4.0']
    expect(entry.url).toContain('/apache/kafka/4.4.0/')
    expect(entry.sha256).toMatch(/^[a-f0-9]{64}$/)
    expect(entry.gatePatchSha256).toBe(createHash('sha256').update(patch).digest('hex'))
  })

  test('the consumer-group-state image and its state fixture default to Kafka 4.4.0', () => {
    expect(read(groupStates, 'Dockerfile')).toMatch(/^ARG KAFKA_VERSION=4\.4\.0$/m)
    expect(read(groupStates, 'compose.yml')).toMatch(
      /image: consumer-group-states:\$\{KAFKA_VERSION:-4\.4\.0\}/
    )
    const fixture = JSON.parse(
      read('src', 'consumer', '__tests__', 'fixtures', 'consumerProtocolStates.json')
    )
    expect(fixture['4.4.0']).toBeTruthy()
  })
})
