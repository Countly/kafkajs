#!/usr/bin/env node
'use strict'
/**
 * Standalone wire probe for KIP-714 (built-in modules only, no repo code, no Kafka client library).
 *
 *   node telemetryWireProbe.js apiversions <host> <port>
 *   node telemetryWireProbe.js get <host> <port> <clientId>
 *   node telemetryWireProbe.js push <host> <port> <clientId> <steps> [vectorsFile]
 *
 * `apiversions` sends one ApiVersions v4 request and prints keys 71/72 and the key count exactly as the broker sent them.
 * `get` sends one GetTelemetrySubscriptions v0 (zero instance id) and prints the answer. `push` sends GetTelemetrySubscriptions v0 (zero instance id), then one PushTelemetry v0 per step, waiting out the
 * broker push interval before each (env PROBE_PUSH_WAIT_MS replaces that wait). A step is `<vector>:<compression>[:terminating]`, compression none|gzip|zstd, the
 * vector a name in the vectors file ("empty" is the zero-length payload); the optional third part is `terminating`
 * or `badsub` (a wrong subscription id, to provoke UNKNOWN_SUBSCRIPTION_ID). Compression is done here with node:zlib, so the
 * bytes the broker decompresses were not produced by Java. One JSON object per line is printed for every event.
 */
const net = require('node:net')
const zlib = require('node:zlib')
const crypto = require('node:crypto')
const fs = require('node:fs')

const [mode, host, portArg, ...rest] = process.argv.slice(2)
const port = Number(portArg)

const uvarint = n => { const out = []; while (n > 0x7f) { out.push((n & 0x7f) | 0x80); n >>>= 7 } out.push(n); return Buffer.from(out) }
const compactStr = s => Buffer.concat([uvarint(Buffer.byteLength(s) + 1), Buffer.from(s)])
const i16 = n => { const b = Buffer.alloc(2); b.writeInt16BE(n); return b }
const i32 = n => { const b = Buffer.alloc(4); b.writeInt32BE(n); return b }

/** Request header v2 (flexible): api key, version, correlation, nullable client id (int16 length), empty tag buffer. */
const header = (key, version, correlation, clientId) => {
  const id = Buffer.from(clientId)
  return Buffer.concat([i16(key), i16(version), i32(correlation), i16(id.length), id, Buffer.from([0])])
}
const frame = (hdr, body) => Buffer.concat([i32(hdr.length + body.length), hdr, body])

class Reader {
  constructor(buf) { this.b = buf; this.o = 0 }
  i8() { const v = this.b.readInt8(this.o); this.o += 1; return v }
  i16() { const v = this.b.readInt16BE(this.o); this.o += 2; return v }
  i32() { const v = this.b.readInt32BE(this.o); this.o += 4; return v }
  uuid() { const h = this.b.toString('hex', this.o, this.o + 16); this.o += 16; return `${h.slice(0, 8)}-${h.slice(8, 12)}-${h.slice(12, 16)}-${h.slice(16, 20)}-${h.slice(20)}`; }
  uuidBytes() { const v = this.b.subarray(this.o, this.o + 16); this.o += 16; return Buffer.from(v) }
  uvarint() { let v = 0, s = 0, x; do { x = this.b[this.o++]; v |= (x & 0x7f) << s; s += 7 } while (x & 0x80); return v >>> 0 }
  cstr() { const n = this.uvarint() - 1; const v = this.b.toString('utf8', this.o, this.o + n); this.o += n; return v }
  tags() { const n = this.uvarint(); for (let i = 0; i < n; i++) { this.uvarint(); const size = this.uvarint(); this.o += size } }
  done() { return this.o === this.b.length }
}

/** One request/response round trip on a fresh connection. Resolves with the response bytes after the frame length. */
const exchange = (connection, request) => new Promise((resolve, reject) => {
  let buf = Buffer.alloc(0)
  const timer = setTimeout(() => reject(new Error('timeout')), 15000)
  connection.removeAllListeners('data')
  connection.once('close', () => { clearTimeout(timer); reject(new Error('connection closed by broker before a response')) })
  connection.on('data', d => {
    buf = Buffer.concat([buf, d])
    if (buf.length >= 4 && buf.length >= 4 + buf.readInt32BE(0)) { clearTimeout(timer); resolve(buf.subarray(4, 4 + buf.readInt32BE(0))) }
  })
  connection.write(request)
})
const connect = () => new Promise((resolve, reject) => {
  const c = net.connect({ host, port }, () => resolve(c))
  c.on('error', reject)
})
const emit = o => process.stdout.write(JSON.stringify(o) + '\n')
const sleep = ms => new Promise(r => setTimeout(r, ms))

const apiVersions = async c => {
  const body = Buffer.concat([compactStr('p9f-tf1-wire-probe'), compactStr('0'), Buffer.from([0])])
  const r = new Reader(await exchange(c, frame(header(18, 4, 1, 'p9f-tf1-wire-probe'), body)))
  const correlation = r.i32()
  const errorCode = r.i16()
  const n = r.uvarint() - 1
  const all = {}
  for (let i = 0; i < n; i++) { const k = r.i16(), min = r.i16(), max = r.i16(); r.tags(); all[k] = [min, max] }
  const throttle = r.i32()
  return { correlation, errorCode, apiKeyCount: n, throttle, all }
}

const getSubscriptions = async (c, clientId, instanceId) => {
  const body = Buffer.concat([instanceId, Buffer.from([0])])
  const r = new Reader(await exchange(c, frame(header(71, 0, 2, clientId), body)))
  const correlation = r.i32(); r.tags()
  const out = { correlation, throttleTimeMs: r.i32(), errorCode: r.i16(), clientInstanceIdBytes: r.uuidBytes() }
  out.subscriptionId = r.i32()
  const nc = r.uvarint() - 1; out.acceptedCompressionTypes = []
  for (let i = 0; i < nc; i++) out.acceptedCompressionTypes.push(r.i8())
  out.pushIntervalMs = r.i32(); out.telemetryMaxBytes = r.i32(); out.deltaTemporality = r.i8() !== 0
  const nm = r.uvarint() - 1; out.requestedMetrics = []
  for (let i = 0; i < nm; i++) out.requestedMetrics.push(r.cstr())
  r.tags()
  out.fullyConsumed = r.done()
  return out
}

const COMPRESSION = { none: [0, b => b], gzip: [1, b => zlib.gzipSync(b)], zstd: [4, b => zlib.zstdCompressSync(b)] }

const push = async (c, clientId, instanceId, subscriptionId, terminating, compression, payload) => {
  const [code, fn] = COMPRESSION[compression]
  const sent = fn(payload)
  const body = Buffer.concat([instanceId, i32(subscriptionId), Buffer.from([terminating ? 1 : 0]), Buffer.from([code]), uvarint(sent.length + 1), sent, Buffer.from([0])])
  const r = new Reader(await exchange(c, frame(header(72, 0, 3, clientId), body)))
  const correlation = r.i32(); r.tags()
  const out = { correlation, throttleTimeMs: r.i32(), errorCode: r.i16(), sentBytes: sent.length }
  r.tags(); out.fullyConsumed = r.done()
  return out
}

;(async () => {
  const c = await connect()
  try {
    if (mode === 'apiversions') {
      const v = await apiVersions(c)
      emit({ event: 'apiversions', host, port, errorCode: v.errorCode, apiKeyCount: v.apiKeyCount, key71: v.all[71] || null, key72: v.all[72] || null, key18: v.all[18] || null })
    } else if (mode === 'push' || mode === 'get') {
      const [clientId, stepsArg, vectorsFile] = rest
      const vectors = vectorsFile ? JSON.parse(fs.readFileSync(vectorsFile, 'utf8')).vectors : {}
      const g = await getSubscriptions(c, clientId, Buffer.alloc(16))
      const instanceId = g.clientInstanceIdBytes
      const idText = new Reader(instanceId).uuid()
      emit({ event: 'get', host, port, clientId, errorCode: g.errorCode, clientInstanceId: idText, subscriptionId: g.subscriptionId,
        acceptedCompressionTypes: g.acceptedCompressionTypes, pushIntervalMs: g.pushIntervalMs, telemetryMaxBytes: g.telemetryMaxBytes,
        deltaTemporality: g.deltaTemporality, requestedMetrics: g.requestedMetrics, throttleTimeMs: g.throttleTimeMs, fullyConsumed: g.fullyConsumed })
      if (g.errorCode !== 0) process.exitCode = 3
      else if (mode === 'push') for (const step of stepsArg.split(',')) {
        const [name, compression, flag] = step.split(':')
        const payload = name === 'empty' ? Buffer.alloc(0) : Buffer.from(vectors[name].hex, 'hex')
        await sleep(process.env.PROBE_PUSH_WAIT_MS ? Number(process.env.PROBE_PUSH_WAIT_MS) : g.pushIntervalMs + 150)
        const terminating = flag === 'terminating'
        const res = await push(c, clientId, instanceId, flag === 'badsub' ? g.subscriptionId + 1 : g.subscriptionId, terminating, compression, payload)
        emit({ event: 'push', host, port, clientId, step, clientInstanceId: idText, subscriptionId: g.subscriptionId, terminating, compression,
          rawBytes: payload.length, rawSha256: crypto.createHash('sha256').update(payload).digest('hex'), ...res })
        if (res.errorCode !== 0) process.exitCode = 4
      }
    } else {
      console.error('usage: apiversions <host> <port> | get <host> <port> <clientId> | push <host> <port> <clientId> <steps> [vectorsFile]'); process.exitCode = 2
    }
  } finally { c.end() }
})().catch(e => { emit({ event: 'error', message: String(e && e.message || e) }); process.exitCode = 1 })
