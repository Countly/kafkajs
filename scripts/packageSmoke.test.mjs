import { test } from 'node:test'
import assert from 'node:assert/strict'
import { createRequire } from 'node:module'

// Run from a temp project where @countly/kafkajs is installed from the packed tarball (scripts/packSmoke.sh).
const require = createRequire(import.meta.url)
const PKG = '@countly/kafkajs'

test('require and import resolve to the same module instance', async () => {
  const cjs = require(PKG)
  const esm = await import(PKG)
  assert.equal(esm.Kafka, cjs.Kafka)
  assert.equal(esm.default, cjs)
  assert.equal(esm.CompressionCodecs, cjs.CompressionCodecs)
})

test('the ESM wrapper re-exports every named export of index.js', async () => {
  const cjs = require(PKG)
  const esm = await import(PKG)
  const esmNames = Object.keys(esm).filter(n => n !== 'default').sort()
  assert.deepEqual(esmNames, Object.keys(cjs).sort())
})

test('the frame, record and commit error classes are exported by both entry points', async () => {
  const cjs = require(PKG)
  const esm = await import(PKG)
  for (const name of ['KafkaJSInvalidFrameError', 'KafkaJSCorruptRecordError', 'KafkaJSCommitFailedError']) {
    assert.equal(typeof cjs[name], 'function')
    assert.equal(esm[name], cjs[name])
  }
})

test('the producer buffer error classes are exported by both entry points', async () => {
  const cjs = require(PKG)
  const esm = await import(PKG)
  for (const name of ['KafkaJSBufferExhaustedError', 'KafkaJSRecordTooLargeError']) {
    assert.equal(typeof cjs[name], 'function')
    assert.equal(esm[name], cjs[name])
  }
})

test('the packed client validates maxResponseSize', () => {
  const { Kafka, KafkaJSInvalidConfigurationError } = require(PKG)
  assert.doesNotThrow(() => new Kafka({ brokers: ['localhost:9092'], maxResponseSize: 4 }))
  assert.throws(() => new Kafka({ brokers: ['localhost:9092'], maxResponseSize: 3 }), KafkaJSInvalidConfigurationError)
})

test('internal error-classification helpers stay private', () => {
  const cjs = require(PKG)
  // isRebalancing/isKafkaJSError are deliberately stripped in index.js before re-export; a helper
  // added to src/errors.js's module.exports must be stripped the same way, not leak onto the root.
  assert.equal(cjs.isStaleCoordinator, undefined)
  assert.equal(cjs.isRebalancing, undefined)
  assert.equal(cjs.isKafkaJSError, undefined)
  assert.equal(cjs.isLostMember, undefined)
  assert.equal(cjs.isRevoked, undefined)
  assert.equal(cjs.isFencedInstance, undefined)
  assert.equal(cjs.isUnknownMemberId, undefined)
})

test('deep paths under src are not exported', () => {
  assert.throws(() => require(`${PKG}/src/index`), { code: 'ERR_PACKAGE_PATH_NOT_EXPORTED' })
  assert.throws(() => require(`${PKG}/src/network/requestQueue`), { code: 'ERR_PACKAGE_PATH_NOT_EXPORTED' })
})

test('package.json stays reachable', () => {
  assert.equal(require(`${PKG}/package.json`).name, PKG)
})
