const assert = require('assert/strict')
const crypto = require('crypto')
const fs = require('fs')
const path = require('path')
const { execFileSync } = require('child_process')
const { performance } = require('perf_hooks')
const { selected: longWindowSelected } = require('../../soak/longWindow')
const { LongWindowReporter } = require('../../soak/longWindowReporter')

/**
 * Owns one unique Compose project on an ephemeral CI runner. Preflight must succeed before any
 * mutation; every command is bounded and teardown proves removal of all three resource kinds.
 */
class CiOwnership {
  /**
   * @param {object} [options] environment and Docker execution seam for offline controls
   * @throws {Error} outside CI, for an unsafe project name or an unregistered Compose file
   */
  constructor({ env = process.env, docker, now = () => performance.now() } = {}) {
    assert.equal(env.CI, 'true', 'Isolated ownership is CI-only')
    assert(
      /^telemetry-[a-z0-9-]+$/.test(env.COMPOSE_PROJECT_NAME),
      'Expected a unique telemetry project'
    )
    assert(['docker-compose.yml', 'docker-compose.telemetry.yml'].includes(env.COMPOSE_FILE))
    this.env = env
    this.project = env.COMPOSE_PROJECT_NAME
    this.file = env.COMPOSE_FILE
    this.directory = env.TELEMETRY_EVIDENCE_DIR
    this.receiptFile = path.resolve('.tmp/e2e/telemetry-ownership.json')
    this.now = now
    this.commandDeadline = Infinity
    this.longReporter = longWindowSelected(env) ? new LongWindowReporter({ env }).read() : null
    this.docker =
      docker ||
      ((args, { timeoutMs }) =>
        execFileSync('timeout', ['-k', '5s', `${(timeoutMs - 5000) / 1000}s`, 'docker', ...args], {
          encoding: 'utf8',
          timeout: timeoutMs,
          maxBuffer: 4 * 1024 * 1024,
          env,
        }))
  }

  /**
   * Executes within the current shared phase budget, including process kill grace.
   * @param {string[]} args Docker arguments
   * @returns {string} bounded command output
   * @throws {Error} when no command budget remains or Docker fails
   */
  run(args) {
    const timeoutMs = Math.min(35000, Math.floor(this.commandDeadline - this.now()))
    assert(timeoutMs > 5000, 'Cleanup command budget exhausted')
    if (args[0] === 'compose' && this.longReporter) {
      const index = args.indexOf(this.file)
      assert(index > 0, 'Long reporter requires the registered Compose file')
      args = [
        ...args.slice(0, index + 1),
        '-f',
        this.longReporter.compose,
        ...args.slice(index + 1),
      ]
    }
    return this.docker(args, { timeoutMs })
  }

  /**
   * @param {function(string,string[]):void} [onKind] persists partial inventories
   * @returns {object} all containers, networks and volumes with this exact Compose project label
   * @throws {Error} if any bounded Docker query fails
   */
  inventory(onKind = () => {}) {
    const filter = ['--filter', `label=com.docker.compose.project=${this.project}`]
    const list = (kind, args) => {
      const values = this.run([...args, ...filter])
        .trim()
        .split(/\s+/)
        .filter(Boolean)
        .sort()
      onKind(kind, values)
      return values
    }
    return {
      containers: list('containers', ['ps', '-aq', '--no-trunc']),
      networks: list('networks', ['network', 'ls', '-q', '--no-trunc']),
      volumes: list('volumes', ['volume', 'ls', '-q']),
    }
  }

  /** @param {object} inventory labelled project resources @returns {void} @throws {Error} for any survivor */
  empty(inventory) {
    assert.deepEqual(
      inventory,
      { containers: [], networks: [], volumes: [] },
      'Project resources remain'
    )
  }

  /** @param {object} receipt ownership journal @returns {void} persists atomically */
  save(receipt) {
    fs.mkdirSync(path.dirname(this.receiptFile), { recursive: true })
    const temp = this.receiptFile + '.new'
    fs.writeFileSync(temp, JSON.stringify(receipt, null, 2) + '\n')
    fs.renameSync(temp, this.receiptFile)
  }

  /** @returns {object} validated ownership receipt @throws {Error} without a successful preflight */
  read() {
    const receipt = JSON.parse(fs.readFileSync(this.receiptFile))
    assert.equal(receipt.project, this.project)
    assert.equal(receipt.compose, this.file)
    assert.equal(receipt.preflightPassed, true, 'No ownership authority')
    return receipt
  }

  /**
   * Proves the project is empty and no legacy-labelled broker can be adopted by readiness helpers.
   * @returns {void}
   * @throws {Error} on any conflict; no ownership receipt is created on refusal
   */
  preflight() {
    assert(!fs.existsSync(this.receiptFile), 'Ownership receipt already exists')
    const inventory = this.inventory()
    this.empty(inventory)
    assert.equal(
      this.run(['ps', '-aq', '--filter', 'label=custom.project=kafkajs']).trim(),
      '',
      'Conflicting broker labels'
    )
    const config = this.run([
      'compose',
      '-p',
      this.project,
      '-f',
      this.file,
      'config',
      '--format',
      'json',
    ])
    const parsed = JSON.parse(config)
    assert.deepEqual(Object.keys(parsed.services).sort(), ['kafka1', 'kafka2', 'kafka3'])
    if (this.file === 'docker-compose.telemetry.yml') {
      for (const service of Object.values(parsed.services)) {
        assert.equal(String(service.environment.KAFKA_TELEMETRY_MAX_BYTES), '4096')
        assert.equal(
          service.environment.KAFKA_METRIC_REPORTERS,
          'org.countly.kafkajs.telemetry.CountingTelemetryReporter'
        )
      }
    }
    fs.mkdirSync(this.directory, { recursive: true })
    assert.equal(fs.readdirSync(this.directory).length, 0, 'Run directory must be empty')
    this.save({
      project: this.project,
      compose: this.file,
      line: this.env.KAFKA_VERSION,
      configSha256: crypto.createHash('sha256').update(config).digest('hex'),
      config: parsed,
      startedAt: new Date().toISOString(),
      preflightPassed: true,
      preflight: inventory,
    })
  }

  /**
   * Journals project resources before further work, including anonymous mounted volumes that do
   * not carry Compose labels. The original receipt survives partial startup or capture failure.
   * @param {object} receipt admitted project receipt
   * @returns {object[]} inspected project containers
   * @throws {Error} on a failed inventory or inspection; recorded identities are never discarded
   */
  journal(receipt) {
    receipt.journalComplete = false
    this.save(receipt)
    try {
      const inventory = this.inventory((kind, values) => {
        receipt.owned ||= { containers: [], networks: [], volumes: [] }
        receipt.owned[kind] = [...new Set([...receipt.owned[kind], ...values])].sort()
        this.save(receipt)
      })
      const containers = inventory.containers.length
        ? JSON.parse(this.run(['inspect', ...inventory.containers]))
        : []
      const volumes = containers.flatMap((container) =>
        (container.Mounts || [])
          .filter((mount) => mount.Type === 'volume')
          .map((mount) => mount.Name)
      )
      const networks = containers.flatMap((container) =>
        Object.values(container.NetworkSettings?.Networks || {}).map((network) => network.NetworkID)
      )
      receipt.owned = Object.fromEntries(
        Object.entries(inventory).map(([kind, values]) => [
          kind,
          [
            ...new Set([
              ...(receipt.owned?.[kind] || []),
              ...values,
              ...(kind === 'volumes' ? volumes : kind === 'networks' ? networks : []),
            ]),
          ].sort(),
        ])
      )
      receipt.journalComplete = true
      this.save(receipt)
      return containers
    } catch (error) {
      receipt.journalError ||= error.message
      this.save(receipt)
      throw error
    }
  }

  /**
   * Records concrete owned container/image identities and mounted reporter hashes before tests.
   * @returns {void}
   * @throws {Error} if a service, image, line or mounted reporter differs from the selected fixture
   */
  capture() {
    const receipt = this.read()
    const containers = this.journal(receipt)
    assert.equal(containers.length, 3)
    assert.deepEqual(containers.map((c) => c.Config.Labels['com.docker.compose.service']).sort(), [
      'kafka1',
      'kafka2',
      'kafka3',
    ])
    const manifest = JSON.parse(fs.readFileSync('scripts/e2e/telemetry/fixtures/manifest.json'))
    const configured = this.file === 'docker-compose.telemetry.yml'
    if (this.longReporter) receipt.longReporter = this.longReporter
    receipt.images = []
    receipt.brokers = []
    for (const container of containers) {
      assert.equal(container.Config.Labels['com.docker.compose.project'], this.project)
      assert.equal(container.State.Running, true)
      const [image] = JSON.parse(this.run(['image', 'inspect', container.Image]))
      assert.equal(image.Id, container.Image)
      if (configured)
        assert(
          manifest.lines[receipt.line].repoDigests.some((digest) =>
            image.RepoDigests?.includes(digest)
          ),
          'Broker image digest mismatch'
        )
      const digest = this.run([
        'exec',
        container.Id,
        'sha256sum',
        '/opt/kafka/config/server.properties',
      ]).split(/\s+/)[0]
      const version = this.run([
        'exec',
        container.Id,
        '/opt/kafka/bin/kafka-broker-api-versions.sh',
        '--version',
      ]).trim()
      assert(
        version.startsWith(receipt.line + ' ') || version === receipt.line,
        'Broker version mismatch'
      )
      let reporterSha256
      if (configured) {
        reporterSha256 = this.run([
          'exec',
          container.Id,
          'sha256sum',
          '/opt/kafka/libs/counting-telemetry-reporter.jar',
        ]).split(/\s+/)[0]
        assert.equal(
          reporterSha256,
          this.longReporter?.sha256 ?? manifest.lines[receipt.line].reporterJar.sha256
        )
        if (this.longReporter) {
          const proof = JSON.parse(
            fs.readFileSync(
              path.join(
                this.directory,
                `reporter-${container.Config.Labels['com.docker.compose.service'].slice(-1)}.json`
              )
            )
          )
          assert.equal(
            proof.maxFileBytes,
            this.longReporter.sinkBytes,
            'Long reporter sink cap mismatch'
          )
        }
      }
      receipt.images.push({
        container: container.Id,
        imageId: image.Id,
        repoDigests: image.RepoDigests,
      })
      receipt.brokers.push({
        container: container.Id,
        service: container.Config.Labels['com.docker.compose.service'],
        version,
        configSha256: digest,
        reporterSha256,
      })
    }
    receipt.readyAt = new Date().toISOString()
    this.save(receipt)
    fs.writeFileSync(path.join(this.directory, 'identity.json'), JSON.stringify(receipt, null, 2))
  }

  /**
   * Removes only the project admitted by preflight; logs and errors remain in the receipt.
   * @returns {void}
   * @throws {Error} for a failed teardown or any surviving container, network or volume
   */
  stop() {
    if (!fs.existsSync(this.receiptFile)) return
    const receipt = this.read()
    const compose = ['compose', '-p', this.project, '-f', this.file]
    const start = this.now()
    let failure = receipt.journalError ? new Error(receipt.journalError) : undefined
    receipt.removalVerified = false
    this.commandDeadline = start + 25000
    try {
      this.journal(receipt)
    } catch (error) {
      failure = error
      receipt.journalError = error.message
    }
    try {
      fs.writeFileSync(
        path.join(path.dirname(this.receiptFile), 'compose.log'),
        this.run([...compose, 'logs', '--no-color'])
      )
    } catch (error) {
      receipt.logError = error.message
    }
    this.commandDeadline = start + 65000
    try {
      this.run([...compose, 'down', '-v', '--remove-orphans', '--timeout', '10'])
    } catch (error) {
      failure ||= error
      receipt.cleanupError = error.message
    }
    this.commandDeadline = start + 130000
    try {
      receipt.removalProof = this.inventory()
      this.empty(receipt.removalProof)
      const all = {
        containers: ['ps', '-aq', '--no-trunc'],
        networks: ['network', 'ls', '-q', '--no-trunc'],
        volumes: ['volume', 'ls', '-q'],
      }
      receipt.ownedRemovalProof = Object.fromEntries(
        Object.entries(all).map(([kind, args]) => {
          const remaining = new Set(this.run(args).trim().split(/\s+/).filter(Boolean))
          return [kind, (receipt.owned?.[kind] || []).filter((id) => remaining.has(id))]
        })
      )
      this.empty(receipt.ownedRemovalProof)
      assert.equal(receipt.journalComplete, true, 'Ownership journal incomplete')
      receipt.removalVerified = !failure
    } catch (error) {
      failure ||= error
      receipt.removalError = error.message
    }
    this.finalize(receipt)
    if (failure) throw failure
  }

  /**
   * Persists the final receipt without further subprocesses; twenty seconds remain before the outer kill.
   * @param {object} receipt successful or failed removal result
   * @returns {void}
   * @throws {Error} on an evidence filesystem failure
   */
  finalize(receipt) {
    receipt.stoppedAt = new Date().toISOString()
    this.save(receipt)
    const bytes = fs.readFileSync(this.receiptFile)
    fs.mkdirSync(this.directory, { recursive: true })
    const receiptPath = path.join(this.directory, 'ownership-receipt.json')
    fs.writeFileSync(receiptPath, bytes)
    const scenarios = fs
      .readdirSync(path.dirname(this.receiptFile))
      .filter((file) => file.endsWith('.jsonl'))
      .map((file) => {
        const content = fs.readFileSync(path.join(path.dirname(this.receiptFile), file))
        return {
          path: file,
          bytes: content.length,
          sha256: crypto.createHash('sha256').update(content).digest('hex'),
        }
      })
    fs.writeFileSync(
      path.join(this.directory, 'cleanup-manifest.json'),
      JSON.stringify(
        {
          project: this.project,
          receipt: {
            path: 'ownership-receipt.json',
            bytes: bytes.length,
            sha256: crypto.createHash('sha256').update(bytes).digest('hex'),
          },
          scenarios,
          segmented: this.segmentIndexes(),
        },
        null,
        2
      )
    )
  }

  /**
   * Lists the root index of every segmented case archive, so the uploaded manifest names each index
   * with its digest, format and segment count.
   * @returns {{path:string,bytes:number,sha256:string,format:string,scenario:string,segments:number}[]}
   *   one entry per `case-*` archive that holds an `index.json`, in name order
   * @throws {Error} for an index that is not valid JSON
   */
  segmentIndexes() {
    return fs
      .readdirSync(this.directory)
      .filter(
        (name) =>
          /^case-[a-z0-9-]+$/.test(name) &&
          fs.existsSync(path.join(this.directory, name, 'index.json'))
      )
      .sort()
      .map((name) => {
        const content = fs.readFileSync(path.join(this.directory, name, 'index.json'))
        const index = JSON.parse(content)
        return {
          path: `${name}/index.json`,
          bytes: content.length,
          sha256: crypto.createHash('sha256').update(content).digest('hex'),
          format: index.format,
          scenario: index.scenario,
          segments: index.segments.length,
        }
      })
  }
}

if (require.main === module) {
  const owner = new CiOwnership()
  const action = process.argv[2]
  assert(['preflight', 'capture', 'stop'].includes(action), 'Unknown ownership action')
  owner[action]()
}

module.exports = { CiOwnership }
