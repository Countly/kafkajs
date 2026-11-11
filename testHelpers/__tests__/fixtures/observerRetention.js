'use strict'

const assert = require('node:assert/strict')
const crypto = require('node:crypto')
const { EventEmitter } = require('node:events')
const fs = require('node:fs')
const path = require('node:path')
const { setImmediate: turn } = require('node:timers/promises')
const { Kafka, logLevel } = require('../../..')
const Connection = require('../../../src/network/connection')
const Generation = require('../../../src/producer/generation')
const { KafkaJSProducerClosedError } = require('../../../src/errors')
const { TelemetryWireObserver } = require('../../telemetryFixtures')

/** @param {Buffer} bytes payload @returns {string} SHA-256 */
const sha = (bytes) => crypto.createHash('sha256').update(bytes).digest('hex')

/** @returns {object} a monotonic clock whose receiver owns only a scalar */
const clock = () => ({
  tick: 0,
  now() {
    return ++this.tick
  },
})

/** @returns {EventEmitter} broker-free socket; its writes never retain the observer */
const dial = () =>
  Object.assign(new EventEmitter(), {
    write: () => true,
    setKeepAlive() {},
    remoteAddress: '127.0.0.1',
    remotePort: 19092,
  })

/** @returns {Connection} deliberately retained product object, independent of the observer */
const positive = () =>
  new Connection({
    host: 'localhost',
    port: 19092,
    logger: {
      namespace() {
        return this
      },
      debug() {},
      warn() {},
      error() {},
    },
    socketFactory: dial,
  })

/**
 * Collects in separate jobs so WeakRef keep-alives and conservative stack roots retire.
 * @returns {Promise<void>} after thirty asynchronous major collections
 */
const collect = async () => {
  for (let pass = 0; pass < 30; pass++) {
    await turn()
    await global.gc({ type: 'major', execution: 'async' })
  }
}

/** @param {TelemetryWireObserver} observer evidence owner @returns {object} all public readbacks */
const facts = (observer) => ({
  transcript: observer.readTranscript(),
  checkpoint: observer.checkpoint(),
  accepted: observer.accepted(),
  finals: observer.finals(),
  subscriptions: observer.subscriptions(),
  advertisements: observer.advertisements(),
  ownership: observer.socketOwnership(),
  telemetry: observer.telemetrySockets(),
  undecided: observer.undecided(),
  connects: observer.normalConnects(),
  requests: observer.normalRequests(),
  open: observer.openSockets(),
  cancellations: observer.cancellationKeys(),
})

/**
 * Replays exactly the parent's complete frames with no product parser or live broker.
 * @param {string} directory immutable stimulus and sink directory
 * @param {Function} decoder independent scalar decoder
 * @returns {Promise<object>} observer, socket and copied decode proofs
 */
const replay = async (directory, decoder) => {
  const observer = new TelemetryWireObserver({
    directory,
    line: '4.3.1',
    owner: 'retention-client',
    clock: clock(),
    dial,
    decoder,
  })
  const socket = observer.socketFactory({ host: 'localhost', port: 19092 })
  socket.emit('connect')
  const stimulus = JSON.parse(fs.readFileSync(path.join(directory, 'stimulus.json')))
  for (const row of stimulus) {
    socket.write(Buffer.from(row.request, 'hex'))
    socket.emit('data', Buffer.from(row.response, 'hex'))
  }
  const decoded = await Promise.all(
    observer.accepted().map((push) => observer.decodeAccepted(push, { sinkWaitMs: 0 }))
  )
  return { observer, socket, decoded }
}

/**
 * Compares actual retained blob backing capacities against the former pooled copy operation.
 * The decoder sees the stored blob itself for compression 0; it retains only weak references.
 * @param {string} directory corpus directory
 * @returns {Promise<object>} exact capacities, release and positive-control facts
 */
const backing = async (directory) => {
  const poolSize = Buffer.poolSize
  const refs = []
  const legacy = []
  const retained = await replay(directory, async (raw) => {
    refs.push(new WeakRef(raw))
    legacy.push(Buffer.from(raw))
    return { parse: 'ok', dataBytes: raw.length, dataSha256: sha(raw) }
  })
  const product = positive()
  const productRef = new WeakRef(product)
  await collect()
  const lengths = refs.map((ref) => ref.deref().length)
  const backings = refs.map((ref) => ref.deref().buffer.byteLength)
  const exactBytes = [...new Set(refs.map((ref) => ref.deref().buffer))].reduce(
    (n, b) => n + b.byteLength,
    0
  )
  const pooledBytes = [...new Set(legacy.map((blob) => blob.buffer))].reduce(
    (n, b) => n + b.byteLength,
    0
  )
  assert.deepEqual(backings, lengths, 'Retained payloads pin oversized backing stores')
  assert(pooledBytes > exactBytes, 'The pooled positive must expose its extra backing capacity')
  retained.socket.emit('close')
  const disposal = retained.observer.dispose()
  assert.equal(retained.observer.dispose(), disposal)
  await disposal
  retained.observer.assertClean()
  await collect()
  const released = refs.every((ref) => ref.deref() === undefined)
  assert(released, 'Disposal must release the actual captured blobs')
  assert.equal(productRef.deref(), product)
  return {
    lengths,
    backings,
    exactBytes,
    pooledBytes,
    poolUnchanged: Buffer.poolSize === poolSize,
    released,
    positiveAlive: productRef.deref() === product,
  }
}

/**
 * Weakly observes the real private transcript array/rows and decoder result tree. Restored before
 * collection; no observer internals are exported or replaced and no strong evidence root escapes.
 * @param {WeakRef<object>[]} transcripts weak transcript targets
 * @returns {Function} restores Array.prototype.push
 */
const watchTranscript = (transcripts) => {
  const descriptor = Object.getOwnPropertyDescriptor(Array.prototype, 'push')
  const original = descriptor.value
  const push = function (...values) {
    if (values.some((v) => v?.type === 'request' && v.owner === 'retention-client')) {
      original.call(transcripts, new WeakRef(this), ...values.map((v) => new WeakRef(v)))
    }
    return original.apply(this, values)
  }
  // eslint-disable-next-line no-extend-native -- Isolated child observes private row ownership and restores before GC.
  Object.defineProperty(Array.prototype, 'push', { ...descriptor, value: push })
  // eslint-disable-next-line no-extend-native -- Restore the exact original descriptor.
  return () => Object.defineProperty(Array.prototype, 'push', descriptor)
}

/**
 * Closes a real public producer through the observer and then releases all observer roots. The
 * product's exact cached error and Generation remain alive; its stack is never read before GC.
 * @param {string} directory corpus directory
 * @returns {Promise<object>} only public/product roots, weak evidence targets and scalar archives
 */
const releaseObserver = async (directory) => {
  const transcripts = []
  const decodedRefs = []
  const restore = watchTranscript(transcripts)
  let retained
  try {
    retained = await replay(directory, async (raw) => {
      const decoded = {
        parse: 'ok',
        dataBytes: raw.length,
        dataSha256: sha(raw),
        metrics: [{ name: 'retention-control' }],
        resources: [],
      }
      decodedRefs.push(
        ...[decoded, decoded.metrics, decoded.metrics[0], decoded.resources].map(
          (v) => new WeakRef(v)
        )
      )
      return decoded
    })
  } finally {
    restore()
  }
  assert(transcripts.length >= 2, 'The real transcript array and row must be observed')
  assert(decodedRefs.length >= 4, 'The real decoded tree must be observed')
  const client = new Kafka({ brokers: ['localhost:19092'], logLevel: logLevel.NOTHING }).producer({
    idempotent: false,
  })
  client.retainedProduct = positive()
  const productRef = new WeakRef(client.retainedProduct)
  const nativeClose = Generation.prototype.close
  let fence
  Generation.prototype.close = function (error) {
    fence = this
    return nativeClose.call(this, error)
  }
  try {
    await retained.observer.closeClient(client)
  } finally {
    Generation.prototype.close = nativeClose
  }
  assert(fence?.closed, 'The real public producer must close its lifecycle')
  const error = fence.error
  assert(error instanceof KafkaJSProducerClosedError)
  retained.socket.emit('close')
  const disposal = retained.observer.dispose()
  assert.equal(retained.observer.dispose(), disposal)
  await disposal
  retained.observer.assertHealthy()
  retained.observer.assertFunctional()
  retained.observer.assertClean()
  await retained.observer.socketsClosed()
  const before = JSON.stringify(facts(retained.observer))
  const proofs = await Promise.all(
    retained.observer.accepted().map((row) => retained.observer.decodeAccepted(row))
  )
  assert.deepEqual(proofs, retained.decoded)
  assert.equal(JSON.stringify(facts(retained.observer)), before)
  const archive = fs.readFileSync(path.join(retained.observer.directory, 'transcript.json'), 'utf8')
  assert.deepEqual(JSON.parse(archive).records, facts(retained.observer).transcript)
  const files = fs
    .readdirSync(retained.observer.directory)
    .filter((name) => name.endsWith('.bin') || name.endsWith('.json'))
    .map((name) => ({
      file: path.join(retained.observer.directory, name),
      sha256: sha(fs.readFileSync(path.join(retained.observer.directory, name))),
    }))
  await retained.observer.dispose()
  assert.equal(
    fs.readFileSync(path.join(retained.observer.directory, 'transcript.json'), 'utf8'),
    archive
  )
  return {
    client,
    fence,
    error,
    productRef,
    observer: new WeakRef(retained.observer),
    transcripts,
    decodedRefs,
    files,
  }
}

/**
 * Preserves lazy V8 stack formatting, including a custom hook that throws, throughout close/GC.
 * @param {string} directory corpus directory
 * @param {string} mode default, custom or throwing prepareStackTrace
 * @returns {Promise<object>} collection, compatibility and immutable evidence facts
 */
const closed = async (directory, mode) => {
  const descriptor = Object.getOwnPropertyDescriptor(Error, 'prepareStackTrace')
  const failure = new Error('custom stack hook failure')
  let calls = 0
  const hook = (error) => {
    calls++
    if (mode === 'throwing') throw failure
    return 'custom:' + error.message
  }
  if (mode !== 'default') Error.prepareStackTrace = hook
  try {
    const roots = await releaseObserver(directory)
    assert.equal(calls, 0, 'Close and sealing must not materialize the product error stack')
    await collect()
    const observerCollected = roots.observer.deref() === undefined
    const transcriptCollected = roots.transcripts.every((ref) => ref.deref() === undefined)
    const decodedCollected = roots.decodedRefs.every((ref) => ref.deref() === undefined)
    assert(observerCollected, 'Cached public close error retains the observer')
    assert(transcriptCollected, 'Cached public close error retains the transcript tree')
    assert(decodedCollected, 'Cached public close error retains the decoded tree')
    assert.equal(roots.fence.error, roots.error)
    assert.equal(roots.fence.guard(), roots.error)
    for (let i = 0; i < 2; i++)
      assert.throws(
        () => roots.fence.check(),
        (error) => error === roots.error
      )
    assert.equal(roots.error.outcome, 'unknown')
    assert.equal(roots.productRef.deref(), roots.client.retainedProduct)
    await roots.client.disconnect()
    assert.equal(roots.fence.error, roots.error)
    if (mode === 'throwing')
      assert.throws(
        () => roots.error.stack,
        (error) => error === failure
      )
    else if (mode === 'custom')
      assert.equal(roots.error.stack, 'custom:The producer was disconnected')
    else {
      assert.equal(typeof roots.error.stack, 'string')
      assert.match(roots.error.stack, /KafkaJSProducerClosedError: The producer was disconnected/)
      assert.match(roots.error.stack, /src[/\\]producer[/\\]index.js/)
    }
    const hookPreserved =
      mode === 'default' ? calls === 0 : Error.prepareStackTrace === hook && calls === 1
    const evidenceUnchanged = roots.files.every(
      ({ file, sha256 }) => sha(fs.readFileSync(file)) === sha256
    )
    assert(hookPreserved)
    assert(evidenceUnchanged)
    return {
      observerCollected,
      transcriptCollected,
      decodedCollected,
      positiveAlive: roots.productRef.deref() === roots.client.retainedProduct,
      fenceAlive: roots.fence.error === roots.error,
      hookPreserved,
      evidenceUnchanged,
    }
  } finally {
    if (descriptor) Object.defineProperty(Error, 'prepareStackTrace', descriptor)
    else delete Error.prepareStackTrace
  }
}

/** @returns {Promise<void>} CI-only isolated proof, never a host-side runtime check */
const run = async () => {
  assert.equal(typeof global.gc, 'function')
  const [directory, mode] = process.argv.slice(2)
  const result = mode === 'backing' ? await backing(directory) : await closed(directory, mode)
  process.stdout.write(JSON.stringify(result))
}

run().catch((error) => {
  process.stderr.write(error.message + '\n')
  process.exitCode = 1
})
