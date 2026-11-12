const fs = require('fs')
const path = require('path')
const pkg = require('../../package.json')

/** @param {string} file repository-relative documentation @returns {string} UTF-8 text */
const read = (file) => fs.readFileSync(path.join(__dirname, '../..', file), 'utf8')

describe('telemetry protocol documentation', () => {
  test('records the protocol and capability milestone', () => {
    const guide = read('docs/ClientTelemetry.md')
    expect(guide).toContain('## Protocol codecs')
    expect(guide).toContain('GetTelemetrySubscriptions (71)')
    expect(guide).toContain('PushTelemetry (72)')
    expect(guide).toContain('working broker exporter')
    expect(read('docs/Testing.md')).toContain('### Telemetry wire evidence')
    expect(read('CHANGELOG.md')).toContain(
      'Add independent telemetry request/response vectors and strict wire fixtures.'
    )
  })
  test.each(['test:unit', 'test:coverage:unit'])('%s registers its creator check', (lane) => {
    const tokens = [...pkg.scripts[lane].matchAll(/'([^']+)'/g)].map((match) => match[1])
    for (const file of [
      'scripts/__tests__/telemetryDocs.spec.js',
      'src/protocol/requests/getTelemetrySubscriptions/v0/request.spec.js',
      'src/protocol/requests/getTelemetrySubscriptions/v0/response.spec.js',
      'src/protocol/requests/pushTelemetry/v0/request.spec.js',
      'src/protocol/requests/pushTelemetry/v0/response.spec.js',
      'testHelpers/__tests__/wire.spec.js',
      'testHelpers/__tests__/scriptedCluster.spec.js',
    ]) {
      expect(tokens.some((pattern) => new RegExp(pattern).test(file))).toBe(true)
    }
    const literal = tokens.find((pattern) => pattern.includes('telemetryDocs'))
    expect(literal).toBe('scripts/__tests__/telemetryDocs\\.spec\\.js$')
    expect(new RegExp(literal).test('scripts/__tests__/telemetryDocsXspecXjs')).toBe(false)
  })
})

describe('numeric telemetry documentation', () => {
  test('records bounded snapshots on the public client factories', () => {
    const guide = read('docs/ClientTelemetry.md')
    expect(guide).toContain(
      'Producer, ordinary consumer, admin and ShareConsumer each expose the methods below'
    )
    expect(guide).toContain('registerMetricForSubscription')
    expect(guide).not.toContain('The collector is internal at this milestone.')
    expect(guide).toContain('257 series in total')
    expect(guide).toContain('response loss or rejection does not replay it')
    expect(guide).toContain('### Numeric snapshot registration')
    expect(guide).toContain('Changing from cumulative to delta')
    expect(read('docs/MigrationGuide-3-0-0.md')).toContain('Unregister before resetting a Sum')
    expect(read('docs/InstrumentationEvents.md')).toContain('### Private numeric observation')
    expect(read('docs/Testing.md')).toContain('### Numeric telemetry evidence')
    expect(read('CHANGELOG.md')).toContain(
      'Add bounded numeric Gauge and monotonic BigInt Sum collection and snapshot registration internally.'
    )
    expect(read('CHANGELOG.md')).toContain(
      'Verify normal physical connections and completed-request observations independently.'
    )
    const exclusions = guide
      .split('### Optional standard templates excluded in this release')[1]
      .split(/\n#{1,3} /)[0]
    expect(exclusions.match(/^\| `/gm)).toHaveLength(15)
  })
  test.each(['test:unit', 'test:coverage:unit'])(
    '%s includes every numeric creator spec',
    (lane) => {
      const tokens = [...pkg.scripts[lane].matchAll(/'([^']+)'/g)].map((match) => match[1])
      for (const file of [
        'src/telemetry/__tests__/metrics.spec.js',
        'src/telemetry/__tests__/temporality.spec.js',
        'src/telemetry/__tests__/registration.spec.js',
        'src/network/__tests__/telemetryObservation.spec.js',
      ]) {
        const literal = file.replaceAll('.', '\\.').concat('$')
        expect(tokens.some((pattern) => new RegExp(pattern).test(file))).toBe(true)
        if (!file.startsWith('src/telemetry/')) expect(tokens).toContain(literal)
        expect(new RegExp(literal).test(file)).toBe(true)
        expect(new RegExp(literal).test(file.replaceAll('.', 'X'))).toBe(false)
      }
    }
  )
})

describe('bounded telemetry payload documentation', () => {
  test('records independent protobuf bounds loss and codec policy', () => {
    const guide = read('docs/ClientTelemetry.md')
    expect(guide).toContain('## OTLP payload bounds and compression')
    expect(guide).toContain('1048576 bytes (1 MiB)')
    expect(guide).toContain('Both raw protobuf')
    expect(guide).toContain('average/maximum pairs in numeric node order')
    expect(guide).toContain('Only included identities at')
    expect(read('docs/Testing.md')).toContain('### Independent OTLP evidence')
    expect(read('scripts/protocol/javaVectors/README.md')).toContain(
      '## Independent OTLP numeric vectors'
    )
    const bullet =
      'Telemetry payloads over1000 bytes use mutually supported built-in ZSTD/GZIP in broker order; NONE is always available. Producer codec registrations are unchanged.'
    expect(read('CHANGELOG.md')).toContain(bullet)
    expect(read('docs/MigrationGuide-3-0-0.md')).toContain(bullet)
    expect(read('CHANGELOG.md')).toContain(
      'Add independently generated and decoded OTLP metric vectors.'
    )
  })
  test.each(['test:unit', 'test:coverage:unit'])(
    '%s includes every payload creator spec',
    (lane) => {
      const tokens = [...pkg.scripts[lane].matchAll(/'([^']+)'/g)].map((match) => match[1])
      for (const file of [
        'src/telemetry/__tests__/otlp.spec.js',
        'src/telemetry/__tests__/payload.spec.js',
        'src/telemetry/__tests__/compression.spec.js',
      ]) {
        const literal = file.replaceAll('.', '\\.').concat('$')
        expect(tokens.some((pattern) => new RegExp(pattern).test(file))).toBe(true)
        if (!file.startsWith('src/telemetry/')) expect(tokens).toContain(literal)
        expect(new RegExp(literal).test(file)).toBe(true)
        expect(new RegExp(literal).test(file.replaceAll('.', 'X'))).toBe(false)
      }
    }
  )
})

describe('telemetry subscription documentation', () => {
  it('publishes the numeric policy and socket cost with the transport', () => {
    const guide = read('docs/ClientTelemetry.md')
    for (const text of [
      '## Subscription transport and recovery',
      '5000 ms',
      '30000 ms',
      '65536',
      'A second 42',
      'successful Gets',
      '117 or 76',
      '16384',
      'persistent client-owned shell',
      'established-only',
      'never dials or starts authentication',
    ])
      expect(guide.toLowerCase()).toContain(text.toLowerCase())
    expect(read('docs/Configuration.md')).toContain('## Telemetry transport cost')
  })
  it('registers the five physical specs in both narrow lanes', () => {
    const scripts = pkg.scripts
    for (const file of [
      'src/telemetry/__tests__/capability.spec.js',
      'src/telemetry/__tests__/subscription.spec.js',
      'src/telemetry/__tests__/errors.spec.js',
      'src/broker/__tests__/telemetryRequests.spec.js',
      'src/cluster/__tests__/telemetryTransport.spec.js',
    ]) {
      const pattern = file.replaceAll('.', String.fromCharCode(92) + '.') + '$'
      for (const name of ['test:unit', 'test:coverage:unit']) {
        const tokens = [...scripts[name].matchAll(/'([^']+)'/g)].map((match) => match[1])
        expect(tokens.some((entry) => new RegExp(entry).test(file))).toBe(true)
        if (!file.startsWith('src/telemetry/')) expect(tokens).toContain(pattern)
      }
      expect(new RegExp(pattern).test(file.replace('.spec', 'Xspec'))).toBe(false)
    }
  })
})

describe('public telemetry documentation', () => {
  test('publishes exact options getter reasons registration disclosure and close rules', () => {
    const text = read('docs/ClientTelemetry.md')
    for (const token of [
      'default remains false',
      '1048576',
      '60000',
      '2147483647',
      '64 waiters',
      '65th',
      'KafkaJSTelemetryError',
      'group_member_id',
      '512 UTF-8',
      'dropped_attributes_count',
      'timeout0 sends no final',
      'concurrently with core draining',
      'Get cannot replenish',
      'registerMetricForSubscription',
      'unregisterMetricFromSubscription',
    ]) {
      expect(text).toContain(token)
    }
    expect(text).not.toContain('not yet attached')
  })
  test('every client guide and the migration guide publish the public telemetry section', () => {
    for (const file of [
      'docs/Producing.md',
      'docs/Consuming.md',
      'docs/Admin.md',
      'docs/ShareConsumer.md',
    ]) {
      expect(read(file)).toContain('## Broker client telemetry')
    }
    expect(read('docs/Configuration.md')).toContain('## Client telemetry options')
    expect(read('docs/MigrationGuide-3-0-0.md')).toContain('### Broker client telemetry')
  })
  test.each(['test:unit', 'test:coverage:unit'])(
    '%s includes factory files through the one telemetry wildcard',
    (lane) => {
      const tokens = [...pkg.scripts[lane].matchAll(/'([^']+)'/g)].map((m) => m[1])
      expect(tokens.filter((p) => p === 'src/telemetry/.*')).toHaveLength(1)
      expect(tokens.some((p) => p.startsWith('src/telemetry/__tests__/'))).toBe(false)
      for (const file of ['factories', 'clientInstanceId', 'options']) {
        expect(
          tokens.some((p) => new RegExp(p).test(`src/telemetry/__tests__/${file}.spec.js`))
        ).toBe(true)
      }
      expect(tokens.some((p) => new RegExp(p).test('src/errors.spec.js'))).toBe(true)
    }
  )
})

test('package smoke creates and checks the alias after the last npm install', () => {
  const script = read('scripts/packSmoke.sh')
  const alias = script.indexOf('ln -s @countly/kafkajs node_modules/kafkajs')
  expect(alias).toBeGreaterThan(script.lastIndexOf('npm install'))
  expect(script.indexOf('NODE_PATH= node --test')).toBeGreaterThan(alias)
  expect(script).toContain('require.resolve("kafkajs") !== require.resolve("@countly/kafkajs")')
  expect(script.indexOf('package-exports.mts')).toBeGreaterThan(alias)
})

describe('broker telemetry fixture documentation', () => {
  test('documents the pinned independent fixture and cleanup', () => {
    expect(read('docs/Testing.md')).toContain('### Broker telemetry fixture and export checks')
    expect(read('docs/ClientTelemetry.md')).toContain('## Broker reporter fixture')
    for (const name of ['docs/Testing.md', 'docs/ClientTelemetry.md']) {
      expect(read(name)).toContain('1000')
      expect(read(name)).toContain('CLIENT_METRICS')
      expect(read(name)).toContain('IPv4')
    }
    expect(read('scripts/e2e/telemetry/README.md')).toContain('OVERFLOW')
    const testing = read('docs/Testing.md')
    expect(testing).toContain('CI never rebuilds')
    expect(testing).toContain('two top-level tests')
    expect(testing).toContain('down -v --remove-orphans')
    expect(testing).toContain('4.4.0/4.2.2/4.1.2/4.0.2')
    expect(testing).toContain('Configured telemetry lanes check mounted reporter jars')
    expect(testing).toContain('base lanes verify broker identity without requiring a reporter jar')
    expect(testing).not.toContain('built deterministically')
  })

  test('keeps all nine script selections under the sole guard', () => {
    const names = [
      'reporter',
      'prefix',
      'custom',
      'payload',
      'shutdown',
      'unsupported',
      'java',
      'outcomes',
      'soak',
    ]
    for (const name of names) {
      const command = pkg.scripts['test:e2e:telemetry-' + name]
      expect(command).toContain('P9_CLUSTER_GUARD')
      expect(command).toContain('tv2 TELEMETRY_E2E')
      expect(command).not.toMatch(/test:kafka|forceExit/)
    }
  })

  test('documents the CI lanes and lands the tooling note without a runtime migration claim', () => {
    const testing = read('docs/Testing.md')
    expect(testing).toContain('`telemetry-reporter`')
    expect(testing).toContain('`telemetry-base`')
    expect(read('CHANGELOG.md')).toContain(
      'Add a pinned broker metrics reporter fixture with independent OTLP export and push-interval verification.'
    )
    expect(read('docs/ClientTelemetry.md')).toContain(
      'no client runtime dependency or behavior when telemetry is disabled'
    )
  })
})

describe('telemetry lifecycle documentation', () => {
  const read = (path) =>
    require('node:fs').readFileSync(require('node:path').resolve(__dirname, '../..', path), 'utf8')
  test('testing describes public identity and delayed callback schedules', () => {
    const text = read('docs/Testing.md')
    expect(text).toContain('### Telemetry lifecycle and outcome schedules')
    expect(text).toContain('request bytes and captured identities together')
    expect(text).toContain('delayed timer delivery')
    expect(text).toContain('do not prove broker export or native resource bounds')
  })
  test('guide preserves bounded best effort and original outcomes', () => {
    const text = read('docs/ClientTelemetry.md')
    expect(text).toContain('## Lifecycle and observation checks')
    expect(text).toContain('A zero close budget sends no final push.')
    expect(text).toContain('the normal push must succeed')
    expect(text).toContain(
      'do not change foreground results, rejections or handler invocation counts'
    )
  })
  test('testing documents the segmented close-and-reopen evidence', () => {
    const text = read('docs/Testing.md')
    expect(text).toContain('`segmented-1`')
    expect(text).toContain('89 in all')
    expect(text).toContain('#### Close and reopen (`telemetry-shutdown`)')
    expect(text).toContain('no checkpoint, read or await in between')
    expect(read('scripts/e2e/telemetry/README.md')).toContain('## Segmented evidence')
  })
  test('creator changelog is present', () => {
    expect(read('CHANGELOG.md')).toContain(
      'Add deterministic lifecycle, shutdown and observer-failure schedules for client telemetry.'
    )
  })
})

describe('telemetry benchmark documentation', () => {
  const fs = require('node:fs')
  const path = require('node:path')
  const read = (name) => fs.readFileSync(path.resolve(__dirname, '../..', name), 'utf8')

  test('registers the single driver and exact broker free spec', () => {
    const scripts = JSON.parse(read('package.json')).scripts
    expect(scripts['bench:telemetry']).toBe('node --expose-gc scripts/benchmarks/telemetry.js')
    const pattern = 'scripts/benchmarks/__tests__/telemetry\\.spec\\.js$'
    for (const name of ['test:unit', 'test:coverage:unit']) {
      expect(scripts[name].split(`'${pattern}'`)).toHaveLength(2)
      expect(scripts[name]).not.toContain("'scripts/benchmarks/.*'")
    }
    expect(new RegExp(pattern).test('scripts/benchmarks/__tests__/telemetry.spec.js')).toBe(true)
    expect(new RegExp(pattern).test('scripts/benchmarks/__tests__/telemetryXspecXjs')).toBe(false)
    expect(new RegExp(pattern).test('scripts/benchmarks/__tests__/telemetryLive.spec.js')).toBe(
      false
    )
  })

  test('documents actual denominators noise and independent evidence', () => {
    const text = read('docs/Testing.md')
    expect(text).toContain('### Telemetry benchmark evidence')
    expect(text).toContain('A20/A31 measured acceptance remains pending P10 CI measurements')
    expect(text).toContain('reviewed broker-guard adapter')
    expect(text).toContain('CPU or throughput noise above 3%, or p99 noise above 5%')
    expect(text).toContain('completed correlated pairs')
    expect(text).toContain('128 validated custom series plus 129 built-ins')
    expect(text).toContain('raw broker commits are independent checks')
    expect(text).toContain('no performance timing assertions')
  })

  test('retains opt in and makes no unit derived performance claim', () => {
    const text = read('docs/ClientTelemetry.md')
    expect(text).toContain('## Performance evidence')
    expect(text).toContain('A20/A31 measured acceptance is pending P10 CI measurements')
    expect(text).toContain('F08 configured-outcome evidence')
    expect(text).toContain('enableMetricsPush')
    expect(text).toContain('actual encoded byte lengths')
    expect(text).toContain(
      'No measured tuning recommendation follows from a broker-free test pass.'
    )
    expect(read('CHANGELOG.md')).toContain(
      'Measure telemetry observation, bounded serialization and end-to-end overhead with independent work and identity controls.'
    )
  })
})

describe('configured telemetry evidence documentation', () => {
  const squeeze = (file) => read(file).replace(/\s+/g, ' ')

  test('documents configured and unsupported fixtures without changing the opt-in', () => {
    const document = squeeze('docs/ClientTelemetry.md')
    expect(document).toContain('## Configured checks and Java interoperability')
    expect(document).toContain('disabled unless enableMetricsPush is true')
    expect(document).toContain('4.4.0, 4.2.2, 4.1.2 and 4.0.2 without exporter')
    expect(document).toContain('raw committed next offsets')
    expect(document).toContain('successful subscription Get alone is insufficient')
  })

  test('keeps pinned Java admin checkpoint, final policies and natural exit independently required', () => {
    const fixture = squeeze('scripts/e2e/telemetry/README.md')
    expect(fixture).toContain('a missing producer or consumer final fails the scenario')
    for (const text of [fixture, squeeze('docs/Testing.md'), squeeze('docs/ClientTelemetry.md')]) {
      expect(text).toContain(
        'Java producer and consumer each require at least two complete accepted normal exports'
      )
      expect(text).toContain(
        'The parent must independently validate that export before allowing admin close'
      )
      expect(text).toContain('Every Java role still exports exactly its connection counter')
      expect(text).toContain('All KafkaJS final/timeout/reopen requirements remain unchanged')
      expect(text).toContain('2000 ms drain')
      expect(text).toContain('2000 ms quiet')
    }
    expect(fixture).toContain('results.javaEvidenceVersion = 2')
    expect(fixture).toContain('preclose-checkpoint-v1')
    expect(fixture).toContain('required-final-v1')
    expect(fixture).toContain('natural exit within 10000 ms')
    expect(fixture).toContain('TELEMETRY_INTEROP_CACHE')
    expect(fixture).toContain('Keep the retained fixture manifest unchanged')
    const testing = squeeze('docs/Testing.md')
    expect(testing).toContain('### Telemetry scenario evidence')
    expect(testing).toContain('independent 4097-byte raw control')
    expect(testing).toContain('taints the spec run')
  })

  test('lands the interoperability tooling note with no runtime migration item', () => {
    expect(squeeze('CHANGELOG.md')).toContain(
      'Verify client telemetry updates, custom metrics, payload limits, Java interoperability, unsupported brokers and unchanged foreground outcomes.'
    )
    expect(squeeze('docs/ClientTelemetry.md')).toContain('Telemetry remains disabled')
  })
})

describe('telemetry resource documentation', () => {
  const squeeze = (file) => read(file).replace(/\s+/g, ' ')
  const testing = squeeze('docs/Testing.md')
  const telemetry = squeeze('docs/ClientTelemetry.md')

  test('keeps the exact telemetry profile and inherited runner selections', () => {
    const { scripts } = pkg
    expect(scripts['soak:telemetry']).toBe(
      'node --expose-gc scripts/soak/soak.js --profile telemetry'
    )
    expect(scripts['soak:producer-churn']).toBeDefined()
    expect(scripts['test:group:telemetry-soak']).toContain("--testNamePattern '^telemetry churn$'")
    for (const name of ['shutdown', 'soak']) {
      const command = scripts['test:e2e:telemetry-' + name]
      expect(command).toContain('P9_CLUSTER_GUARD')
      expect(command).toContain('tv2 TELEMETRY_E2E src/telemetry/__tests__/shutdownLive.spec.js')
      expect(command).not.toMatch(/test:kafka|forceExit/)
    }
  })

  test('documents per-client memory and natural exit without claiming measured passes', () => {
    expect(telemetry).toContain('## Resource bounds and natural shutdown')
    expect(testing).toContain('### Telemetry resource and churn evidence')
    for (const value of ['64 KiB/min', '1 MiB', '8 MiB', '256 KiB', '64 MiB'])
      expect(telemetry).toContain(value)
    expect(telemetry).toContain('not measured results from a unit test')
    expect(telemetry).toContain('one occupied shell')
    expect(testing).toContain('arrayBuffers is already included in external')
    expect(testing).toContain('Four one-client workers prevent memory averaging')
  })

  test('documents the control-relative memory gates and the recorded evidence', () => {
    expect(testing).toContain('resourceArtifact.js')
    expect(testing).toContain('resources-<arm>-<kind>.json')
    expect(testing).toContain('collector only resource memory for')
    expect(testing).toContain('telemetry-disabled control')
    expect(testing).toContain('/proc/self/status')
    expect(testing).toContain('bounded capture controls')
    expect(testing).not.toMatch(/absolute lifetime peak RSS at most 64 MiB/)
    expect(telemetry).toContain('refused before either selection array is built')
    expect(telemetry).toContain('64 MiB above')
    for (const document of [testing, telemetry]) {
      expect(document).toContain(
        'Δdata = Δ(heapUsed + external) − Δ(V8 code_and_metadata_size + bytecode_and_metadata_size)'
      )
      expect(document).toContain('first use M − M0 ≤ 1 MiB')
      expect(document).toContain('unseeded data below −256 KiB is inconclusive')
      expect(document).toContain('measured warm-up lifecycle')
      expect(document).toContain('schema 6')
      expect(document).toContain('96 MiB plus its declared journal bytes')
      expect(document).toContain('shadow only')
    }
    expect(testing).toContain('legacy unseeded arm is a contamination-positive control')
    expect(testing).toContain('live-flags control')
    expect(testing).toContain('23,192,560')
    expect(testing).toContain('123,855,856')
  })

  test('documents the churn method and the CI rows', () => {
    expect(testing).toContain('### Telemetry churn (`telemetry churn`)')
    expect(testing).toContain('selected by a rule over the transcript alone')
    expect(testing).toContain('nine distinct Node 24 rows, all on Kafka 4.4.0')
    expect(testing).toContain('required functional exit catalogue has 72 rows')
    expect(telemetry).toContain('35 subscription transitions')
  })

  test('retains the long-run obligation and the changelog lines', () => {
    expect(telemetry).toContain('final 24-hour idle/churn obligation')
    expect(testing).toContain('never closes the final 24-hour idle/churn gate')
    const changelog = read('CHANGELOG.md').replace(/\s+/g, ' ')
    expect(changelog).toContain(
      'Verify bounded telemetry resources, repeated reopen and natural shutdown under subscription and connection churn.'
    )
    expect(changelog).toContain('resourceArtifact.js')
    expect(changelog).toContain('parent-coordinated, segmented run')
  })
})
