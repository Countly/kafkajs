const { describe, it } = require('node:test')
const assert = require('node:assert/strict')
const fs = require('node:fs')
const path = require('node:path')

const otelRoot = path.join(__dirname, '..')

/**
 * @param {string} file path relative to the sub-package root
 * @returns {string} the file's text
 */
const read = (file) => fs.readFileSync(path.join(otelRoot, file), 'utf8')

const manifest = JSON.parse(read('package.json'))
const lock = JSON.parse(read('package-lock.json'))

describe('otel package manifest', () => {
  it('names the package, its first version and its licence', () => {
    assert.equal(manifest.name, '@countly/opentelemetry-instrumentation-kafkajs')
    assert.equal(manifest.version, '0.1.0')
    assert.equal(manifest.license, 'Apache-2.0')
  })

  it('points at the plain CommonJS entry and its hand-written types', () => {
    assert.equal(manifest.main, 'src/index.js')
    assert.equal(manifest.types, 'src/index.d.ts')
    assert.deepEqual(manifest.files, ['src/', 'LICENSE', 'README.md'])
  })

  it('needs Node 22 or later', () => {
    assert.equal(manifest.engines.node, '>=22')
  })

  it('depends on the instrumentation base and the semantic conventions only', () => {
    assert.deepEqual(manifest.dependencies, {
      '@opentelemetry/instrumentation': '^0.223.0',
      '@opentelemetry/semantic-conventions': '^1.30.0',
    })
  })

  it('peers on the API, and optionally on the client any 3.x prerelease satisfies', () => {
    assert.deepEqual(manifest.peerDependencies, {
      '@countly/kafkajs': '>=3.0.0-0 <4',
      '@opentelemetry/api': '^1.3.0',
    })
    assert.deepEqual(manifest.peerDependenciesMeta, { '@countly/kafkajs': { optional: true } })
  })

  it('has no scripts and no publish configuration', () => {
    assert.equal(manifest.scripts, undefined)
    assert.equal(manifest.publishConfig, undefined)
  })

  it('lists the test dependencies the ported suite imports', () => {
    assert.deepEqual(manifest.devDependencies, {
      '@opentelemetry/api': '^1.3.0',
      '@opentelemetry/contrib-test-utils': '^0.70.0',
      '@opentelemetry/core': '^2.9.0',
      '@opentelemetry/sdk-metrics': '^2.9.0',
      '@opentelemetry/sdk-trace': '^2.9.0',
    })
  })
})

describe('otel licence and lockfile', () => {
  it('ships the Apache-2.0 text, not the root package licence', () => {
    assert.match(read('LICENSE'), /^\s*Apache License\s+Version 2\.0, January 2004/)
    assert.notEqual(read('LICENSE'), fs.readFileSync(path.join(otelRoot, '..', 'LICENSE'), 'utf8'))
  })

  it('records no entry for the packed client the test script installs with --no-save', () => {
    const entries = Object.entries(lock.packages)
    assert.equal(lock.lockfileVersion, 3)
    assert.deepEqual(
      entries.filter(([name]) => name.endsWith('node_modules/@countly/kafkajs')),
      []
    )
    assert.deepEqual(
      entries.filter(([, entry]) => String(entry.resolved).startsWith('file:')),
      []
    )
  })
})
