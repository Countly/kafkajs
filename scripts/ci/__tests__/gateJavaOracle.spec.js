const fs = require('fs')
const path = require('path')
const vm = require('vm')
const yaml = require('js-yaml')

const root = path.join(__dirname, '..', '..', '..')
const gate = JSON.parse(fs.readFileSync(path.join(root, 'scripts/ci/gate.json'), 'utf8'))
const workflow = yaml.load(fs.readFileSync(path.join(root, '.github/workflows/gate.yml'), 'utf8'))
const steps = workflow.jobs.check.steps
const prepare = steps.find(({ name }) => name === 'Prepare verified Java jars')

/**
 * Runs a workflow manifest reader against a supplied check, without disk writes or external tools.
 * @param {object} step the workflow step containing a Node heredoc
 * @param {object} check the manifest entry to validate
 * @returns {void}
 * @throws {Error} when the reader rejects the entry
 */
const readCheck = (step, check) => {
  const script = /^node <<'NODE'\n([\s\S]+)\nNODE\s*$/.exec(step.run)[1]
  const manifest = JSON.stringify({ version: 1, checks: [check] })
  vm.runInNewContext(script, {
    require: (name) => {
      if (name !== 'fs') throw new Error(`Unexpected module: ${name}`)
      return { readFileSync: () => manifest, writeFileSync: () => {}, appendFileSync: () => {} }
    },
    process: { env: { CHECK_ID: check.id, DEFAULT_KAFKA_VERSION: '4.4.0' } },
  })
}

describe('exit gate Java oracle', () => {
  test('the four interop checks pin Kafka clients 4.3.1', () => {
    const pins = gate.checks.filter(({ javaClientsVersion }) => javaClientsVersion !== undefined)
    expect(
      Object.fromEntries(pins.map(({ id, javaClientsVersion }) => [id, javaClientsVersion]))
    ).toEqual({
      'e2e-compression': '4.3.1',
      'e2e-java-interop': '4.3.1',
      'e2e-java-transaction-interop': '4.3.1',
      'e2e-leader-union': '4.3.1',
    })
    for (const check of pins) expect(check.needs.java).toBe(true)
  })

  test('the share check does not pin Java clients and gets Kafka clients 4.4.0', () => {
    const share = gate.checks.find(({ id }) => id === 'e2e-share')
    expect(share).toBeDefined()
    expect(share.needs.java).toBe(true)
    expect(share.javaClientsVersion).toBeUndefined()
    expect(
      share.javaClientsVersion || share.kafkaVersion || workflow.env.DEFAULT_KAFKA_VERSION
    ).toBe('4.4.0')
  })

  test('gate.yml selects javaClientsVersion before the broker version for jars and their cache', () => {
    expect(prepare.env.JAVA_CLIENTS_VERSION).toBe(
      '$' + '{{ fromJSON(steps.entry.outputs.check).javaClientsVersion }}'
    )
    expect(prepare.run).toContain(
      'KAFKA_CLIENTS_VERSION="$' + '{JAVA_CLIENTS_VERSION:-$KAFKA_VERSION}"'
    )
    expect(prepare.run).toContain('case "$KAFKA_CLIENTS_VERSION" in')
    expect(prepare.run).toContain(
      'org/apache/kafka/kafka-clients/$KAFKA_CLIENTS_VERSION/kafka-clients-$KAFKA_CLIENTS_VERSION.jar $clients_sha256'
    )
    expect(prepare.run).toContain(
      'echo "KAFKA_CLIENTS_VERSION=$KAFKA_CLIENTS_VERSION" >> "$GITHUB_ENV"'
    )
    const cache = steps.find(
      ({ uses, with: config }) =>
        uses?.startsWith('actions/cache@') && config.path.endsWith('/gate-java')
    )
    expect(cache.with.key).toContain(
      '$' + '{{ fromJSON(steps.entry.outputs.check).javaClientsVersion || env.KAFKA_VERSION }}'
    )
  })

  test('every pinned Java client version has a checksum case entry', () => {
    const versions = new Set(
      gate.checks
        .filter(({ needs }) => needs?.java)
        .map(
          ({ javaClientsVersion, kafkaVersion }) =>
            javaClientsVersion || kafkaVersion || workflow.env.DEFAULT_KAFKA_VERSION
        )
    )
    const entries = [
      ...prepare.run.matchAll(/^\s*(\d+\.\d+\.\d+)\) clients_sha256=([a-f0-9]{64}) ;;$/gm),
    ]
    const checksums = new Map(entries.map(([, version, checksum]) => [version, checksum]))
    for (const version of versions) expect(checksums.has(version)).toBe(true)
    expect(prepare.run).toMatch(/\*\).*exit 1 ;;/)
  })

  test('both manifest readers reject an unknown Java client version', () => {
    const readers = [
      workflow.jobs.plan.steps.find(({ id }) => id === 'plan'),
      steps.find(({ name }) => name === 'Read check'),
    ]
    for (const reader of readers) {
      const check = { id: 'interop', capMinutes: 10, needs: { java: true } }
      for (const javaClientsVersion of [undefined, '4.3.1', '4.4.0'])
        expect(() => readCheck(reader, { ...check, javaClientsVersion })).not.toThrow()
      for (const javaClientsVersion of ['4.9.9', 'latest', null])
        expect(() => readCheck(reader, { ...check, javaClientsVersion })).toThrow(
          'Invalid javaClientsVersion'
        )
    }
  })
})
