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
