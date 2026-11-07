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
    expect(testing).toContain('4.0.2, 4.1.2')
    expect(testing).toContain('only the reporter lane checks mounted reporter jars')
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
