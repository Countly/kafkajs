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


test('the producer close error is exported with transmission evidence', async () => {
  const cjs = require(PKG)
  const esm = await import(PKG)
  assert.equal(esm.KafkaJSProducerClosedError, cjs.KafkaJSProducerClosedError)
  for (const outcome of ['unsent', 'unknown']) {
    assert.equal(new cjs.KafkaJSProducerClosedError('closed', { outcome }).outcome, outcome)
  }
})

test('the no-offset error is exported by both entry points and is not retriable', async () => {
  const cjs = require(PKG)
  const esm = await import(PKG)
  assert.equal(typeof cjs.KafkaJSNoOffsetError, 'function')
  assert.equal(esm.KafkaJSNoOffsetError, cjs.KafkaJSNoOffsetError)
  for (const reason of ['no-offset', 'offset-out-of-range']) {
    const error = new cjs.KafkaJSNoOffsetError('no starting position', { topic: 't', partition: 0, reason })
    assert.equal(error instanceof cjs.KafkaJSNonRetriableError, true)
    assert.equal(error.retriable, false)
    assert.equal(error.reason, reason)
  }
})

test('the awaited callback error has the same constructor in both entry points', async () => {
  const cjs = require(PKG)
  const esm = await import(PKG)
  assert.equal(typeof cjs.KafkaJSRebalanceListenerError, 'function')
  assert.equal(esm.KafkaJSRebalanceListenerError, cjs.KafkaJSRebalanceListenerError)
  const cause = new Error('external offset store')
  const error = new esm.KafkaJSRebalanceListenerError('failed', {
    hook: 'onPartitionsAssigned',
    reason: 'threw',
    cause,
    thrownValue: cause,
  })
  assert.equal(error.cause, cause)
  assert.equal(error.thrownValue, cause)
})

test('the awaited callback error is a non-retriable error that carries its hook and reason', () => {
  const { KafkaJSRebalanceListenerError, KafkaJSNonRetriableError } = require(PKG)
  const cause = new Error('hook timed out')
  const error = new KafkaJSRebalanceListenerError('failed', { hook: 'onPartitionsLost', reason: 'timeout', cause })
  assert.equal(error instanceof KafkaJSNonRetriableError, true)
  assert.equal(error.retriable, false)
  assert.equal(error.hook, 'onPartitionsLost')
  assert.equal(error.reason, 'timeout')
})

test('the packed consumer has the manual-assignment methods and validates its group identity and options', async () => {
  const { Kafka, KafkaJSNonRetriableError, logLevel } = require(PKG)
  const kafka = new Kafka({ brokers: ['localhost:9092'], logLevel: logLevel.NOTHING })
  const direct = kafka.consumer({})
  for (const name of ['assign', 'unsubscribe', 'assignment', 'subscription', 'position', 'committed', 'enforceRebalance']) {
    assert.equal(typeof direct[name], 'function', name)
  }
  assert.equal(kafka.consumer({ groupId: undefined }).assignment().length, 0)
  for (const groupId of ['', null]) {
    assert.throws(() => kafka.consumer({ groupId }), KafkaJSNonRetriableError)
  }
  assert.throws(() => kafka.consumer({ groupId: 'g', fetchSessions: 'yes' }), KafkaJSNonRetriableError)
  assert.throws(() => kafka.consumer({ groupId: 'g', rackAwareAssignment: 'yes' }), KafkaJSNonRetriableError)
  await assert.rejects(direct.subscribe({ topic: 't' }), KafkaJSNonRetriableError)
  await assert.rejects(direct.committed([]), KafkaJSNonRetriableError)
})
