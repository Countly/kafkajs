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
  assert.equal(esm.oauthBearer, cjs.oauthBearer)
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

test('OAuth helpers share their namespace across require and import', async () => {
  const cjs = require(PKG)
  const esm = await import(PKG)
  assert.equal(esm.oauthBearer, cjs.oauthBearer)
  assert.deepEqual(Object.keys(cjs.oauthBearer).sort(), ['clientCredentials', 'fileToken', 'jwtBearer', 'signAssertion'])
  for (const name of Object.keys(cjs.oauthBearer)) assert.equal(typeof cjs.oauthBearer[name], 'function')
  const provider = cjs.oauthBearer.clientCredentials({ tokenEndpointUrl: 'https://issuer.example/token', clientId: 'client', clientSecret: 'inert-secret' })
  assert.equal(typeof provider, 'function')
  assert.equal(typeof provider.close, 'function')
  assert.equal(provider.close(), undefined)
  provider.close()
  await assert.rejects(provider(), cjs.KafkaJSSASLAuthenticationError)
  assert.throws(() => require(`${PKG}/src/oauth`), { code: 'ERR_PACKAGE_PATH_NOT_EXPORTED' })
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

test('Kerberos stays an optional exact peer and deep imports stay closed', () => {
  const pkg = require(`${PKG}/package.json`)
  assert.equal(pkg.peerDependencies.kerberos, '7.0.0')
  assert.equal(pkg.peerDependenciesMeta.kerberos.optional, true)
  assert.equal(pkg.dependencies?.kerberos, undefined)
  assert.equal(pkg.optionalDependencies?.kerberos, undefined)
  assert.throws(() => require(`${PKG}/src/broker/saslAuthenticator/gssapi`), {
    code: 'ERR_PACKAGE_PATH_NOT_EXPORTED',
  })
})

test('the packed client authenticates with PLAIN and reports the absent GSSAPI peer', async () => {
  const { createServer } = await import('node:net')
  const { once } = await import('node:events')
  assert.throws(() => require.resolve('kerberos'), { code: 'MODULE_NOT_FOUND' })
  const { Kafka, KafkaJSNotImplemented } = require(PKG)
  const mechanisms = []
  const sockets = new Set()
  const server = createServer(socket => {
    sockets.add(socket)
    socket.on('close', () => sockets.delete(socket))
    let buffered = Buffer.alloc(0)
    socket.on('data', data => {
      buffered = Buffer.concat([buffered, data])
      while (buffered.length >= 4 && buffered.length >= buffered.readInt32BE(0) + 4) {
        const size = buffered.readInt32BE(0)
        const frame = buffered.subarray(4, size + 4)
        buffered = buffered.subarray(size + 4)
        const key = frame.readInt16BE(0)
        const correlation = frame.subarray(4, 8)
        let body
        if (key === 18) {
          // ErrorCode, compact ApiKeys, throttle and tags; ApiVersions keeps header v0.
          body = Buffer.from('0000040012000000040000110001000100002400020002000000000000', 'hex')
        } else if (key === 17) {
          const offset = 10 + frame.readInt16BE(8)
          mechanisms.push(frame.subarray(offset + 2, offset + 2 + frame.readInt16BE(offset)).toString())
          // No error; two classic strings: PLAIN, GSSAPI.
          body = Buffer.from('0000000000020005504c41494e0006475353415049', 'hex')
        } else if (key === 36) {
          // Flexible response header tags; code, null message, empty bytes, zero lifetime, tags.
          body = Buffer.from('0000000001000000000000000000', 'hex')
        } else {
          socket.destroy(new Error('Unexpected request in packed authentication fixture'))
          return
        }
        const length = Buffer.alloc(4)
        length.writeInt32BE(correlation.length + body.length)
        socket.write(Buffer.concat([length, correlation, body]))
      }
    })
  })
  server.listen(0, '127.0.0.1')
  await once(server, 'listening')
  const brokers = [`127.0.0.1:${server.address().port}`]
  const config = { brokers, retry: { retries: 0 }, connectionTimeout: 1000, logLevel: 0 }
  const plain = new Kafka({ ...config, sasl: { mechanism: 'plain', username: 'fixture', password: 'fixture' } }).admin()
  const gssapi = new Kafka({ ...config, sasl: { mechanism: 'gssapi' } }).admin()
  try {
    await plain.connect()
    await assert.rejects(gssapi.connect(), error => {
      assert.ok(error instanceof KafkaJSNotImplemented)
      assert.match(error.message, /npm install kerberos@7\.0\.0/)
      return true
    })
    assert.deepEqual(mechanisms, ['PLAIN', 'GSSAPI'])
  } finally {
    await Promise.allSettled([plain.disconnect(), gssapi.disconnect()])
    for (const socket of sockets) socket.destroy()
    await new Promise((resolve, reject) => server.close(error => error ? reject(error) : resolve()))
  }
})

test('share capability errors keep the unsupported API class across entry points', async () => {
  const cjs = require(PKG)
  const esm = await import(PKG)
  assert.equal(esm.KafkaJSShareGroupsNotSupportedError, cjs.KafkaJSShareGroupsNotSupportedError)
  const error = new cjs.KafkaJSShareGroupsNotSupportedError('unsupported', {
    apiKey: 78, apiName: 'ShareFetch', reason: 'api-version-range', shareVersion: 0
  })
  assert.ok(error instanceof cjs.KafkaJSServerDoesNotSupportApiKey)
  assert.equal(error.retriable, false)
  assert.equal(error.apiName, 'ShareFetch')
  assert.equal(error.reason, 'api-version-range')
})

test('the compact acknowledgement error has one constructor in both packed entry points', async () => {
  const cjs = require(PKG)
  const esm = await import(PKG)
  assert.equal(typeof cjs.KafkaJSShareAcknowledgementError, 'function')
  assert.equal(esm.KafkaJSShareAcknowledgementError, cjs.KafkaJSShareAcknowledgementError)
  const cause = new Error('lost acknowledgement answer')
  const error = new esm.KafkaJSShareAcknowledgementError('not confirmed', {
    reason: 'session-lost',
    topic: 'topic',
    partition: 0,
    ranges: [{ firstOffset: '9007199254740993', lastOffset: '9223372036854775807' }],
    cause,
  })
  assert(error instanceof cjs.KafkaJSError)
  assert.equal(error.retriable, false)
  assert.equal(error.cause, cause)
  assert.deepEqual(error.ranges, [
    { firstOffset: '9007199254740993', lastOffset: '9223372036854775807' },
  ])
})
