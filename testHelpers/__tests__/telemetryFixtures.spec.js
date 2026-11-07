const crypto = require('crypto')
const { EventEmitter } = require('events')
const fs = require('fs')
const os = require('os')
const path = require('path')
const {
  canonicalUuid,
  TelemetryExportReader,
  TelemetryWireObserver,
  withTelemetryResource,
} = require('../telemetryFixtures')

const ID = '00112233-4455-6677-8899-aabbccddeeff'
const row = (node, seq, extra = {}) => ({
  v: 1,
  node,
  seq,
  clientInstanceId: ID,
  parse: 'ok',
  dataBytes: 22,
  dataSha256: 'a'.repeat(64),
  terminating: false,
  pushIntervalMs: 1000,
  ...extra,
})

describe('independent telemetry evidence', () => {
  let directory
  let reader
  const write = (node, rows, suffix = '') =>
    fs.writeFileSync(
      path.join(directory, 'exports-' + node + '.jsonl'),
      rows.map((r) => JSON.stringify(r) + '\n').join('') + suffix
    )
  beforeEach(() => {
    directory = fs.mkdtempSync(path.join(os.tmpdir(), 'telemetry-fixture-'))
    reader = new TelemetryExportReader(directory)
  })
  afterEach(() => fs.rmSync(directory, { recursive: true, force: true }))

  test('normalizes canonical and Kafka UUID bits', () => {
    expect(canonicalUuid(ID.toUpperCase())).toBe(ID)
    expect(canonicalUuid('ABEiM0RVZneImaq7zN3u_w')).toBe(ID)
    expect(canonicalUuid('AAAAAAAAAAAAAAAAAAAAAA')).toBe('00000000-0000-0000-0000-000000000000')
    expect(() => canonicalUuid('ABEiM0RVZneImaq7zN3u_x')).toThrow('Invalid')
  })
  test('reads every node using independent sequence cursors', () => {
    write('1', [row('1', 1), row('1', 2)])
    write('2', [row('2', 1)])
    write('3', [row('3', 1)])
    expect(reader.readExports({ uuid: ID, after: { 1: 2 } }).map((r) => [r.node, r.seq])).toEqual([
      ['2', 1],
      ['3', 1],
    ])
    expect(reader.checkpoint()).toEqual({ 1: 2, 2: 1, 3: 1 })
  })
  test('matches Kafka UUIDs while preserving source identities', () => {
    write('2', [row('2', 1, { clientInstanceId: 'ABEiM0RVZneImaq7zN3u_w' })])
    expect(reader.readExports({ uuid: ID })[0]).toMatchObject({
      node: '2',
      seq: 1,
      clientInstanceId: ID,
    })
  })
  test('retries an incomplete final line without consuming it', () => {
    const next = JSON.stringify(row('1', 2))
    write('1', [row('1', 1)], next.slice(0, 12))
    expect(reader.checkpoint()).toEqual({ 1: 1, 2: 0, 3: 0 })
    write('1', [row('1', 1), row('1', 2)])
    expect(reader.readExports({ after: { 1: 1 } }).map((r) => r.seq)).toEqual([2])
  })
  test('fails overflow even on a node with no selected records', () => {
    fs.writeFileSync(path.join(directory, 'OVERFLOW-3.json'), '{}')
    expect(() => reader.readExports({ uuid: ID })).toThrow('overflow')
  })
  test('rejects corruption before filtering another UUID', () => {
    write('1', [row('1', 1, { parse: 'error:broken', clientInstanceId: 'AAAAAAAAAAAAAAAAAAAAAA' })])
    expect(() => reader.readExports({ uuid: ID })).toThrow('Invalid telemetry export')
  })
  test('rejects a complete malformed JSON line', () => {
    write('1', [], '{broken}\n')
    expect(() => reader.readExports()).toThrow(SyntaxError)
  })
  test('retains truncated detail for independent full decode', () => {
    write('1', [row('1', 1, { truncated: true })])
    expect(reader.readExports()[0].truncated).toBe(true)
  })
  test.each([
    ['wrong node', [row('2', 1)]],
    ['duplicate sequence', [row('1', 1), row('1', 1)]],
    ['gap', [row('1', 2)]],
    ['stub', [row('1', 1, { stub: true })]],
  ])('rejects %s', (_name, rows) => {
    write('1', rows)
    expect(() => reader.readExports()).toThrow()
  })
  test('rejects file and record caps', () => {
    write('1', [], 'x'.repeat(4097))
    expect(() => reader.readExports()).toThrow('record cap')
    write('1', [], 'x'.repeat(2097153))
    expect(() => reader.readExports()).toThrow('file cap')
  })
  test('rejects a reset or missing sink after a nonzero cursor', () => {
    write('1', [row('1', 1)])
    expect(() => reader.readExports({ after: { 1: 2 } })).toThrow('replaced')
    fs.unlinkSync(path.join(directory, 'exports-1.jsonl'))
    expect(() => reader.readExports({ after: { 1: 1 } })).toThrow()
  })
  test('accepts an empty fresh sink and rejects a scalar cursor', () => {
    expect(reader.readExports()).toEqual([])
    expect(() => reader.readExports({ after: 3 })).toThrow('cursor')
  })
})

describe('client metrics resource ownership', () => {
  const config = {
    name: 'fixture',
    entries: { metrics: 'org.apache.kafka.', 'interval.ms': '1000', match: 'client_id=^fixture$' },
  }
  const fixture = () => {
    let exists = false
    const result = () => ({
      resources: [{ resourceType: 16, resourceName: 'fixture', errorCode: 0 }],
    })
    const admin = {
      listConfigResources: jest.fn(async () => ({
        resources: exists ? [{ type: 16, name: 'fixture' }] : [],
      })),
      incrementalAlterConfigs: jest.fn(async ({ resources }) => {
        exists = resources[0].configEntries[0].operation === 0
        return result()
      }),
      describeConfigs: jest.fn(async () => ({
        resources: [
          {
            ...result().resources[0],
            configEntries: Object.entries(config.entries).map(([configName, configValue]) => ({
              configName,
              configValue,
            })),
          },
        ],
      })),
    }
    return admin
  }
  test('reads configuration and listing before work, then verifies absence', async () => {
    const admin = fixture()
    const work = jest.fn(async () => {
      expect(admin.describeConfigs).toHaveBeenCalledTimes(1)
      expect(admin.listConfigResources).toHaveBeenCalledTimes(2)
    })
    await withTelemetryResource(admin, config, work)
    expect(work).toHaveBeenCalledTimes(1)
    expect(admin.listConfigResources).toHaveBeenLastCalledWith({
      resourceTypes: [16],
      timeout: 3000,
    })
    expect(admin.incrementalAlterConfigs.mock.calls[1][0].resources[0].configEntries).toEqual(
      Object.keys(config.entries).map((name) => ({ name, operation: 1 }))
    )
  })
  test('preserves the exact work rejection after cleanup', async () => {
    const sentinel = new Error('original')
    const admin = fixture()
    await expect(
      withTelemetryResource(admin, config, async () => {
        throw sentinel
      })
    ).rejects.toBe(sentinel)
    expect(admin.incrementalAlterConfigs).toHaveBeenCalledTimes(2)
  })
  test('preserves a falsy work rejection after cleanup', async () => {
    const original = null
    await expect(
      withTelemetryResource(fixture(), config, async () => {
        return Promise.reject(original)
      })
    ).rejects.toBeNull()
  })
  test('retains both primary and cleanup failures in order', async () => {
    const admin = fixture()
    const first = new Error('first')
    const second = new Error('second')
    admin.incrementalAlterConfigs
      .mockImplementationOnce(async () => {
        throw first
      })
      .mockImplementationOnce(async () => {
        throw second
      })
    const error = await withTelemetryResource(admin, config, async () => {}).catch((e) => e)
    expect(error).toBeInstanceOf(AggregateError)
    expect(error.errors).toEqual([first, second])
  })
  test('refuses existing resources before any write or work', async () => {
    const admin = fixture()
    admin.listConfigResources.mockResolvedValue({ resources: [{ type: 16, name: 'fixture' }] })
    const work = jest.fn()
    await expect(withTelemetryResource(admin, config, work)).rejects.toThrow('already exists')
    expect(admin.incrementalAlterConfigs).not.toHaveBeenCalled()
    expect(work).not.toHaveBeenCalled()
  })
  test('cleans up readback mismatch without entering work', async () => {
    const admin = fixture()
    admin.describeConfigs.mockResolvedValue({
      resources: [{ resourceType: 16, resourceName: 'fixture', errorCode: 0, configEntries: [] }],
    })
    const work = jest.fn()
    await expect(withTelemetryResource(admin, config, work)).rejects.toThrow('readback')
    expect(admin.incrementalAlterConfigs).toHaveBeenCalledTimes(2)
    expect(work).not.toHaveBeenCalled()
  })
  test('fails a deletion that remains visible', async () => {
    const admin = fixture()
    admin.listConfigResources
      .mockResolvedValueOnce({ resources: [] })
      .mockResolvedValue({ resources: [{ type: 16, name: 'fixture' }] })
    await expect(withTelemetryResource(admin, config, async () => {})).rejects.toThrow('deletion')
  })
})

const i8 = (n) => Buffer.from([n & 0xff])
const i16 = (n) => {
  const bytes = Buffer.alloc(2)
  bytes.writeInt16BE(n)
  return bytes
}
const i32 = (n) => {
  const bytes = Buffer.alloc(4)
  bytes.writeInt32BE(n)
  return bytes
}
const uvar = (n) => {
  const out = []
  let rest = n
  while (rest >= 0x80) {
    out.push((rest & 0x7f) | 0x80)
    rest = Math.floor(rest / 128)
  }
  return Buffer.from([...out, rest])
}
const TAGS = Buffer.alloc(1)
const text16 = (value) => Buffer.concat([i16(value.length), Buffer.from(value)])
const frame = (...parts) => {
  const body = Buffer.concat(parts)
  return Buffer.concat([i32(body.length), body])
}
const header1 = (api, version, correlation) =>
  Buffer.concat([i16(api), i16(version), i32(correlation), text16('client')])
const header2 = (api, version, correlation) =>
  Buffer.concat([header1(api, version, correlation), TAGS])
const uuidBytes = (id) => Buffer.from(id.replace(/-/g, ''), 'hex')
const sha = (bytes) => crypto.createHash('sha256').update(bytes).digest('hex')
const get71 = (correlation, id = ID) => frame(header2(71, 0, correlation), uuidBytes(id), TAGS)
const push72 = (correlation, blob, { id = ID, terminating = false, compression = 0 } = {}) =>
  frame(
    header2(72, 0, correlation),
    uuidBytes(id),
    i32(7),
    i8(terminating ? 1 : 0),
    i8(compression),
    uvar(blob.length + 1),
    blob,
    TAGS
  )
const metadata = (correlation) => frame(header1(3, 6, correlation), i32(0))
const metadataResponse = (correlation) => frame(i32(correlation), Buffer.alloc(8))
const apiVersions = (correlation) => frame(header1(18, 2, correlation))
const apiVersionsResponse = (correlation, ranges) =>
  frame(
    i32(correlation),
    i16(0),
    i32(ranges.length),
    ...ranges.flatMap(([key, low, high]) => [i16(key), i16(low), i16(high)]),
    i32(0)
  )
const get71Response = (correlation, { interval = 1000, metrics = ['org.apache.kafka.'] } = {}) =>
  frame(
    i32(correlation),
    TAGS,
    i32(0),
    i16(0),
    uuidBytes(ID),
    i32(7),
    uvar(2),
    i8(0),
    i32(interval),
    i32(4096),
    i8(0),
    uvar(metrics.length + 1),
    ...metrics.flatMap((metric) => [uvar(metric.length + 1), Buffer.from(metric)]),
    TAGS
  )
const push72Response = (correlation, error = 0) =>
  frame(i32(correlation), TAGS, i32(0), i16(error), TAGS)
const sinkRow = (node, seq, raw, extra = {}) =>
  row(node, seq, { dataBytes: raw.length, dataSha256: sha(raw), ...extra })

const fakeSocket = (port) => {
  const socket = new EventEmitter()
  socket.written = []
  socket.originalWrite = jest.fn((chunk) => {
    socket.written.push(Buffer.from(chunk))
    return true
  })
  socket.write = socket.originalWrite
  socket.setKeepAlive = jest.fn()
  socket.remoteAddress = '127.0.0.1'
  socket.remotePort = port
  return socket
}
const stubDecoder = jest.fn(async (raw) => ({
  dataBytes: raw.length,
  dataSha256: sha(raw),
  parse: 'ok',
  resources: [],
  metrics: [],
}))

describe('telemetry wire observer', () => {
  let directory
  let observers
  const writeSink = (node, rows) =>
    fs.writeFileSync(
      path.join(directory, 'exports-' + node + '.jsonl'),
      rows.map((r) => JSON.stringify(r) + '\n').join('')
    )
  const observe = (options = {}) => {
    let tick = 0
    const dial = jest.fn(({ port }) => fakeSocket(port))
    const observer = new TelemetryWireObserver({
      directory,
      line: '4.3.1',
      clock: { now: () => ++tick },
      dial,
      decoder: stubDecoder,
      ...options,
    })
    observers.push(observer)
    return { observer, dial }
  }
  const open = (observer, port = 19092) => {
    const socket = observer.socketFactory({ host: 'localhost', port, onConnect: jest.fn() })
    socket.emit('connect')
    return socket
  }
  const send = (socket, ...frames) => socket.write(Buffer.concat(frames))
  const receive = (socket, ...frames) => socket.emit('data', Buffer.concat(frames))
  const requests = (observer) => observer.readTranscript().filter((r) => r.type === 'request')

  beforeEach(() => {
    directory = fs.mkdtempSync(path.join(os.tmpdir(), 'telemetry-observer-'))
    observers = []
    stubDecoder.mockClear()
  })
  afterEach(async () => {
    await Promise.all(observers.map((observer) => observer.dispose()))
    fs.rmSync(directory, { recursive: true, force: true })
  })

  test('accounts precisely for a held periodic response cancelled by public close', async () => {
    const { observer } = observe({ completeStream: true })
    const socket = open(observer)
    send(socket, push72(1, Buffer.alloc(0)))
    await observer.closeClient({ disconnect: async () => socket.emit('close') })
    await observer.dispose()
    observer.assertClean({ allowUnanswered: observer.cancellationKeys() })
    const transcript = JSON.parse(fs.readFileSync(path.join(observer.directory, 'transcript.json')))
    expect(transcript.cancellations).toEqual([
      expect.objectContaining({
        requestKey: requests(observer)[0].requestKey,
        apiKey: 72,
        socketId: 1,
        disposition: 'close-in-flight',
      }),
    ])
    require('../../scripts/e2e/telemetry/evidence').assertStreamAccounted({ producer: transcript })
  })

  test('rejects an unrelated unanswered request on a socket closed by the client', async () => {
    const { observer } = observe()
    const socket = open(observer)
    send(socket, metadata(1))
    await observer.closeClient({ disconnect: async () => socket.emit('close') })
    await observer.dispose()
    expect(observer.cancellationKeys()).toEqual([])
    expect(() => observer.assertClean({ allowUnanswered: observer.cancellationKeys() })).toThrow(
      'Unanswered'
    )
  })

  test('rejects a foreign fixture endpoint before dialing', () => {
    const { observer, dial } = observe()
    const onConnect = jest.fn()
    expect(() => observer.socketFactory({ host: 'localhost', port: 9092, onConnect })).toThrow(
      'refuses endpoint'
    )
    expect(() =>
      observer.socketFactory({ host: 'example.invalid', port: 19092, onConnect })
    ).toThrow('refuses endpoint')
    expect(() =>
      observer.socketFactory({ host: 'localhost', port: 19092, ssl: {}, onConnect })
    ).toThrow('plaintext')
    expect(dial).not.toHaveBeenCalled()
    const socket = observer.socketFactory({ host: 'localhost', port: 19092, onConnect })
    expect(dial).toHaveBeenCalledTimes(1)
    expect(dial).toHaveBeenCalledWith({ host: 'localhost', port: 19092, family: 4 }, onConnect)
    expect(socket.setKeepAlive).toHaveBeenCalledWith(true, 60000)
    expect(onConnect).not.toHaveBeenCalled()
    const base = observe({ mode: 'base', line: '4.1.2' })
    expect(() => base.observer.socketFactory({ host: '127.0.0.1', port: 19092 })).toThrow(
      'refuses endpoint'
    )
    expect(base.dial).not.toHaveBeenCalled()
    base.observer.socketFactory({ host: '127.0.0.1', port: 9095 })
    expect(base.dial).toHaveBeenCalledTimes(1)
  })

  test('records actual socket address and host metadata identity', () => {
    const { observer } = observe()
    for (const port of [19092, 19095, 19098]) {
      const socket = open(observer, port)
      send(socket, get71(1))
      send(socket, get71(2))
    }
    expect(
      observer
        .readTranscript()
        .filter((r) => r.type === 'connect')
        .map((r) => [r.node, r.remoteAddress, r.remotePort])
    ).toEqual([
      ['1', '127.0.0.1', 19092],
      ['2', '127.0.0.1', 19095],
      ['3', '127.0.0.1', 19098],
    ])
    expect(requests(observer).map((r) => [r.node, r.uuid, r.correlation])).toEqual([
      ['1', ID, 1],
      ['1', ID, 2],
      ['2', ID, 1],
      ['2', ID, 2],
      ['3', ID, 1],
      ['3', ID, 2],
    ])
    observer.assertHealthy()
    const wrong = observe().observer
    const substituted = wrong.socketFactory({ host: 'localhost', port: 19092 })
    substituted.remotePort = 19095
    substituted.emit('connect')
    expect(() => wrong.assertHealthy()).toThrow('expected 127.0.0.1:19092')
  })

  test('reassembles fragmented and coalesced Kafka frames', () => {
    const blob = Buffer.from('payload')
    const frames = [get71(1), push72(2, blob), metadata(3)]
    const identity = (observer) =>
      observer.readTranscript().map((r) => ({
        type: r.type,
        apiKey: r.apiKey,
        apiVersion: r.apiVersion,
        correlation: r.correlation,
        size: r.size,
        sha256: r.sha256,
        uuid: r.uuid,
        metricsBytes: r.metricsBytes,
      }))
    const control = observe().observer
    const controlSocket = open(control)
    for (const one of frames) send(controlSocket, one)
    const expected = identity(control)
    expect(expected.filter((r) => r.type === 'request').map((r) => r.apiKey)).toEqual([71, 72, 3])
    for (const cut of [1, 2, 3, 4, 5, 6, 8, 10, 12, 14, 20, 21, 22, 37]) {
      const split = observe().observer
      const socket = open(split)
      send(socket, frames[0].subarray(0, cut))
      send(socket, frames[0].subarray(cut), frames[1], frames[2])
      expect(identity(split)).toEqual(expected)
      split.assertHealthy()
    }
  })

  test('matches correlation within one captured socket', () => {
    const { observer } = observe()
    const a = open(observer, 19092)
    const b = open(observer, 19095)
    send(a, metadata(7))
    send(b, metadata(7))
    receive(a, metadataResponse(7))
    send(a, metadata(7))
    expect(new Set(requests(observer).map((r) => r.requestKey)).size).toBe(3)
    expect(requests(observer).map((r) => Boolean(r.response))).toEqual([true, false, false])
    receive(b, metadataResponse(7))
    expect(requests(observer).map((r) => Boolean(r.response))).toEqual([true, true, false])
    expect(requests(observer).map((r) => r.socketId)).toEqual([1, 2, 1])
    receive(a, metadataResponse(7))
    expect(observer.normalRequests()).toHaveLength(3)
    observer.assertHealthy()
    const duplicate = observe().observer
    const socket = open(duplicate)
    send(socket, metadata(9))
    send(socket, metadata(9))
    expect(() => duplicate.assertHealthy()).toThrow('Duplicate pending correlation 9')
    const unexpected = observe().observer
    receive(open(unexpected), metadataResponse(99))
    expect(() => unexpected.assertHealthy()).toThrow('Unexpected response correlation 99')
  })

  test('requires response success and matching exported bytes', async () => {
    const blob = Buffer.from('first-payload')
    writeSink('1', [sinkRow('1', 1, blob)])
    const { observer } = observe()
    const socket = open(observer)
    send(socket, push72(1, blob))
    receive(socket, push72Response(1, 0))
    send(socket, push72(2, Buffer.from('second')))
    receive(socket, push72Response(2, 118))
    const accepted = observer.accepted({ uuid: ID })
    expect(accepted).toHaveLength(1)
    expect(accepted[0]).toMatchObject({ version: 0, errorCode: 0, terminating: false, node: '1' })
    await expect(observer.decodeAccepted(accepted[0], { sinkWaitMs: 0 })).resolves.toMatchObject({
      node: '1',
      uuid: ID,
      rawBytes: blob.length,
      transmittedBytes: blob.length,
      rawSha256: sha(blob),
      exportIdentity: { node: '1', seq: 1 },
    })
    expect(stubDecoder).toHaveBeenCalledTimes(1)
    for (const sink of [
      ['1', sinkRow('1', 1, blob, { dataSha256: 'b'.repeat(64) })],
      ['2', sinkRow('2', 1, blob)],
    ]) {
      fs.rmSync(path.join(directory, 'exports-1.jsonl'), { force: true })
      writeSink(sink[0], [sink[1]])
      const other = observe().observer
      const peer = open(other)
      send(peer, push72(1, blob))
      receive(peer, push72Response(1, 0))
      await expect(other.decodeAccepted(other.accepted()[0], { sinkWaitMs: 0 })).rejects.toThrow(
        'No unconsumed sink record'
      )
      fs.rmSync(path.join(directory, 'exports-' + sink[0] + '.jsonl'))
    }
  })

  const javaTest = process.env.TELEMETRY_JAVA_CACHE ? test : test.skip
  javaTest('independently decodes all details behind a capped export', async () => {
    const vectors = JSON.parse(
      fs.readFileSync(
        path.join(__dirname, '../../scripts/e2e/telemetry/fixtures/golden/vectors.json'),
        'utf8'
      )
    ).vectors
    for (const name of ['zero', 'nonzero']) {
      const raw = Buffer.from(vectors[name].hex, 'hex')
      const capped = { truncated: true, resources: [], metrics: [] }
      writeSink('1', [sinkRow('1', 1, raw, capped)])
      const { observer } = observe({ decoder: undefined })
      const socket = open(observer)
      send(socket, push72(1, raw))
      receive(socket, push72Response(1, 0))
      const result = await observer.decodeAccepted(observer.accepted()[0])
      expect(result.decoded.resources).toMatchObject(vectors[name].semantic.resources)
      expect(result.decoded.metrics).toMatchObject(vectors[name].semantic.metrics)
      expect(result.decoded.metrics.length).toBeGreaterThan(capped.metrics.length)
      expect(result.decoded.metrics.length).toBe(vectors[name].semantic.metrics.length)
    }
  })

  test('excludes telemetry setup sockets from normal observations', () => {
    const { observer } = observe()
    const telemetry = open(observer, 19092)
    send(telemetry, apiVersions(1))
    receive(telemetry, apiVersionsResponse(1, [[71, 0, 0]]))
    send(telemetry, get71(2))
    const normal = open(observer, 19095)
    send(normal, apiVersions(1))
    receive(normal, apiVersionsResponse(1, [[3, 0, 6]]))
    send(normal, metadata(2))
    receive(normal, metadataResponse(2))
    expect(observer.normalConnects().map((c) => c.socketId)).toEqual([2])
    expect(observer.normalRequests().map((r) => [r.socketId, r.apiKey])).toEqual([
      [2, 18],
      [2, 3],
    ])
    expect(observer.telemetrySockets().map((s) => s.socketId)).toEqual([1])
    expect(observer.undecided()).toEqual([])
  })

  test('rejects fabricated, duplicate and missing per-kind node points', () => {
    const { verifyMetricWindows } = require('../telemetryFixtures')
    const metric = (name, points) => ({ name: 'org.apache.kafka.consumer.' + name, points })
    const pair = (value) => [{ attrs: [['node_id', 'string', '2']], asDouble: String(value) }]
    const sample = {
      requestKey: 'push',
      writeAt: 10,
      decoded: {
        metrics: [
          metric('connection.creation.total', [{ asInt: '1', attrs: [] }]),
          metric('node.request.latency.avg', pair(7)),
          metric('node.request.latency.max', pair(7)),
        ],
      },
    }
    const requests = [
      { requestKey: 'normal', node: '2', duration: 7, responseAt: 5, knownNode: true },
    ]
    const check = (samples, events = requests) =>
      verifyMetricWindows({
        kind: 'consumer',
        samples,
        requests: events,
        connects: [{ at: 1 }],
        delta: true,
      })
    expect(check([sample]).allocations).toEqual([{ node: '2', windows: [['normal']] }])
    expect(() => check([sample], [])).toThrow('node identity')
    const missing = JSON.parse(JSON.stringify(sample))
    missing.decoded.metrics.pop()
    expect(() => check([missing])).toThrow('node identity')
    const duplicate = JSON.parse(JSON.stringify(sample))
    duplicate.decoded.metrics[1].points.push(...pair(7))
    expect(() => check([duplicate])).toThrow('Duplicate')
    const omitted = JSON.parse(JSON.stringify(sample))
    omitted.decoded.metrics.splice(1)
    expect(() => check([omitted])).toThrow('node identity')
    const wrong = JSON.parse(JSON.stringify(sample))
    wrong.decoded.metrics[1].points[0].asDouble = '6'
    expect(() => check([wrong])).toThrow('arithmetic')
    const descriptor = JSON.parse(JSON.stringify(sample))
    descriptor.decoded.metrics.push(descriptor.decoded.metrics[0])
    expect(() => check([descriptor])).toThrow('Duplicate')
  })

  test('retains captured Push bytes and a failed attempt after capture and cleanup fail', async () => {
    const { runCase } = require('../../scripts/e2e/telemetry/evidence')
    const { observer } = observe()
    const primary = new Error('capture expired')
    const cleanupError = new Error('client cleanup failed')
    const written = []
    let key
    await expect(
      runCase(
        'telemetry-reporter',
        async (cleanup, publish, attempt) => {
          attempt.observers.producer = observer
          attempt.stage = 'capture'
          cleanup.defer(() => {
            throw cleanupError
          })
          cleanup.defer(() => observer.dispose())
          const socket = open(observer)
          send(socket, push72(9, Buffer.from('captured-before-failure')))
          key = observer.readTranscript().find((r) => r.apiKey === 72).requestKey
          socket.emit('close')
          throw primary
        },
        { directory, write: (name, record) => written.push({ name, record }) }
      )
    ).rejects.toMatchObject({ errors: [primary, cleanupError] })
    expect(written).toHaveLength(1)
    expect(written[0].name).toBe('telemetry-reporter-failed')
    const record = written[0].record
    expect(record.status).toBe('failed')
    expect(record.failure).toMatchObject({
      stage: 'capture',
      causes: [{ message: 'capture expired' }, { message: 'client cleanup failed' }],
    })
    const payload = record.sidecars.files.find((item) => item.path.endsWith(`/push-${key}.bin`))
    expect(fs.readFileSync(path.join(directory, payload.path))).toEqual(
      Buffer.from('captured-before-failure')
    )
    expect(record.sidecars.files.some((item) => item.path.endsWith('/transcript.json'))).toBe(true)
    expect(
      JSON.parse(fs.readFileSync(path.join(directory, record.sidecars.root, 'failed-attempt.json')))
    ).toMatchObject({ status: 'failed', failure: record.failure })
  })

  test('keeps the zero-request Get under its assigned response UUID', async () => {
    const { observer } = observe()
    const socket = open(observer)
    send(socket, get71(1, '00000000-0000-0000-0000-000000000000'))
    receive(socket, get71Response(1))
    const gets = observer.subscriptions({ uuid: ID })
    expect(gets).toHaveLength(1)
    expect(gets[0].uuid).toBe('00000000-0000-0000-0000-000000000000')
    expect(gets[0].response.uuid).toBe(ID)
    expect(observer.subscriptions({ uuid: 'aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee' })).toEqual([])
    socket.emit('close')
    await observer.dispose()
    observer.assertClean()
  })

  test('counts a proven foreground bootstrap without counting dedicated setup', async () => {
    const { observer } = observe()
    const foreground = open(observer)
    send(foreground, apiVersions(1))
    receive(foreground, apiVersionsResponse(1, [[3, 0, 6]]))
    const dedicated = open(observer, 19095)
    send(dedicated, apiVersions(1))
    receive(
      dedicated,
      apiVersionsResponse(1, [
        [71, 0, 0],
        [72, 0, 0],
      ])
    )
    observer.confirmForegroundBootstrap()
    expect(observer.normalConnects().map((c) => c.socketId)).toEqual([1])
    expect(observer.undecided().map((c) => c.socketId)).toEqual([2])
    send(dedicated, get71(2))
    receive(dedicated, get71Response(2))
    expect(() => observer.confirmForegroundBootstrap(2)).toThrow('foreground bootstrap')
    expect(observer.telemetrySockets().map((c) => c.socketId)).toEqual([2])
    expect(observer.undecided()).toEqual([])
    foreground.emit('close')
    dedicated.emit('close')
    await observer.dispose()
    observer.assertClean()
  })

  test('decodes once and reuses a result without consuming another request export', async () => {
    const blob = Buffer.from('cached-payload')
    writeSink('1', [sinkRow('1', 1, blob)])
    const { observer } = observe()
    const socket = open(observer)
    send(socket, push72(1, blob))
    receive(socket, push72Response(1))
    const [push] = observer.accepted({ uuid: ID })
    const first = await observer.decodeAccepted(push, { sinkWaitMs: 0 })
    expect(await observer.decodeAccepted(push, { sinkWaitMs: 0 })).toEqual(first)
    expect(stubDecoder).toHaveBeenCalledTimes(1)
    const decodedFile = path.join(observer.directory, `decoded-${push.requestKey}.json`)
    expect(JSON.parse(fs.readFileSync(decodedFile))).toEqual(first)
    socket.emit('close')
    await observer.dispose()
    observer.assertClean()
  })

  test('distinguishes empty selection and zero blob from missing exporter', async () => {
    const empty = observe()
    const first = open(empty.observer)
    send(first, apiVersions(1))
    receive(
      first,
      apiVersionsResponse(1, [
        [71, 0, 0],
        [72, 0, 0],
      ])
    )
    send(first, get71(2))
    receive(first, get71Response(2, { interval: 300000, metrics: [] }))
    expect(empty.observer.subscriptions({ uuid: ID })[0].response).toMatchObject({
      errorCode: 0,
      pushIntervalMs: 300000,
      requestedMetrics: [],
    })
    expect(empty.observer.accepted()).toEqual([])
    const zero = observe()
    const second = open(zero.observer)
    send(second, push72(1, Buffer.alloc(0)))
    receive(second, push72Response(1, 0))
    expect(zero.observer.accepted()).toHaveLength(1)
    expect(zero.observer.accepted()[0].metricsBytes).toBe(0)
    await expect(zero.observer.decodeAccepted(zero.observer.accepted()[0])).rejects.toThrow(
      'zero-byte'
    )
    expect(new TelemetryExportReader(directory).readExports()).toEqual([])
    const missing = observe()
    const third = open(missing.observer)
    send(third, apiVersions(1))
    receive(third, apiVersionsResponse(1, [[3, 0, 6]]))
    expect(missing.observer.advertisements()).toEqual([
      { socketId: 1, node: '1', telemetry: false, ranges: { 71: null, 72: null } },
    ])
    expect(missing.observer.telemetrySockets()).toEqual([])
    expect(missing.observer.subscriptions()).toEqual([])
    expect(missing.dial).toHaveBeenCalledTimes(1)
  })

  test('fails observer bounds and sink overflow without changing socket writes', () => {
    const run = (limits, action) => {
      const { observer } = observe({ limits })
      const socket = open(observer)
      return { observer, socket, run: (count) => action(socket, count) }
    }
    const cases = [
      ['pending', { pending: 2 }, (s, n) => send(s, metadata(n)), 2, 'pending bound'],
      ['records', { records: 3 }, (s, n) => send(s, metadata(n)), 2, 'record bound'],
      ['retained', { retained: 200 }, (s, n) => send(s, metadata(n)), 2, 'retained-byte bound'],
      ['frame', { frame: 32 }, (s) => send(s, frame(Buffer.alloc(33))), 0, 'out of range'],
    ]
    for (const [, limits, action, atLimit, message] of cases) {
      const fixture = run(limits, action)
      for (let n = 1; n <= atLimit; n++) fixture.run(n)
      fixture.observer.assertHealthy()
      fixture.run(atLimit + 1)
      expect(() => fixture.observer.assertHealthy()).toThrow(message)
    }
    const sockets = observe({ limits: { sockets: 2 } }).observer
    open(sockets, 19092)
    open(sockets, 19095)
    sockets.assertHealthy()
    const extra = open(sockets, 19098)
    expect(extra.write).toBe(extra.originalWrite)
    expect(() => sockets.assertHealthy()).toThrow('socket bound')

    const { observer } = observe({ limits: { pending: 1 } })
    const socket = open(observer)
    const callback = jest.fn()
    socket.originalWrite.mockReturnValueOnce(false)
    expect(socket.write(metadata(1), callback)).toBe(false)
    expect(socket.originalWrite).toHaveBeenCalledTimes(1)
    expect(socket.originalWrite.mock.calls[0][1]).toBe(callback)
    expect(socket.originalWrite.mock.contexts[0]).toBe(socket)
    const sentinel = new Error('write failed')
    socket.originalWrite.mockImplementationOnce(() => {
      throw sentinel
    })
    let caught
    try {
      socket.write(metadata(2))
    } catch (error) {
      caught = error
    }
    expect(caught).toBe(sentinel)
    expect(socket.originalWrite).toHaveBeenCalledTimes(2)
    expect(() => observer.assertHealthy()).toThrow('pending bound')

    fs.writeFileSync(path.join(directory, 'OVERFLOW-3.json'), '{}')
    const overflow = observe().observer
    expect(() => overflow.assertHealthy()).toThrow('overflow')
  })

  test('retains disabled client outcomes with no telemetry traffic', async () => {
    const foreground = (socket) => [socket.write(metadata(1)), socket.write(metadata(2))]
    const control = fakeSocket(19092)
    const { observer } = observe()
    const observed = open(observer)
    const expected = foreground(control)
    expect(foreground(observed)).toEqual(expected)
    receive(observed, metadataResponse(1), metadataResponse(2))
    expect(observed.written).toEqual(control.written)
    expect(observer.telemetrySockets()).toEqual([])
    expect(observer.subscriptions()).toEqual([])
    observed.emit('close')
    await observer.dispose()
    observer.assertClean()
    for (const event of ['data', 'connect', 'close']) expect(observed.listenerCount(event)).toBe(0)
    expect(observed.write).toBe(observed.originalWrite)
  })

  test('consumes each repeated export once across source nodes', async () => {
    const blob = Buffer.from('same-bytes')
    writeSink('1', [sinkRow('1', 1, blob), sinkRow('1', 2, blob)])
    writeSink('2', [sinkRow('2', 1, blob)])
    const { observer } = observe()
    const one = open(observer, 19092)
    const two = open(observer, 19095)
    for (const correlation of [1, 2]) {
      send(one, push72(correlation, blob))
      receive(one, push72Response(correlation))
    }
    send(two, push72(1, blob))
    receive(two, push72Response(1))
    const identities = []
    for (const push of observer.accepted({ uuid: ID }))
      identities.push((await observer.decodeAccepted(push, { sinkWaitMs: 0 })).exportIdentity)
    expect(identities).toHaveLength(3)
    expect(new Set(identities.map((i) => JSON.stringify(i))).size).toBe(3)
    expect(identities.map((i) => i.node + ':' + i.seq).sort()).toEqual(['1:1', '1:2', '2:1'])
    send(one, push72(3, blob))
    receive(one, push72Response(3))
    const fourth = observer.accepted({ uuid: ID }).pop()
    await expect(observer.decodeAccepted(fourth, { sinkWaitMs: 0 })).rejects.toThrow(
      'No unconsumed sink record'
    )
    expect(
      new TelemetryExportReader(directory)
        .readExports({ after: { 1: 2 } })
        .map((r) => [r.node, r.seq])
    ).toEqual([['2', 1]])
  })

  describe('decoder supervision and cleanup', () => {
    const linuxTest = process.platform === 'linux' ? test : test.skip
    let stub
    const supervised = (java) => {
      const bin = path.join(stub, 'bin')
      const script = path.join(stub, 'scripts/e2e/telemetry/run.sh')
      fs.mkdirSync(bin)
      fs.mkdirSync(path.dirname(script), { recursive: true })
      fs.copyFileSync(path.join(__dirname, '../../scripts/e2e/telemetry/run.sh'), script)
      fs.writeFileSync(
        path.join(bin, 'node'),
        '#!/bin/sh\ncat >/dev/null\necho \'{"verified":true,"line":"4.3.1"}\'\n',
        { mode: 0o755 }
      )
      fs.writeFileSync(path.join(bin, 'java'), '#!/bin/sh\n' + java, { mode: 0o755 })
      return observe({
        decoder: undefined,
        decoderScript: script,
        env: { ...process.env, PATH: bin + ':' + process.env.PATH, TELEMETRY_JAVA_CACHE: stub },
      }).observer
    }
    beforeEach(() => {
      stub = fs.mkdtempSync(path.join(os.tmpdir(), 'telemetry-stub-'))
    })
    afterEach(() => fs.rmSync(stub, { recursive: true, force: true }))

    linuxTest('returns the decoded body only after the identity line', async () => {
      const observer = supervised(
        'for last; do :; done\nprintf \'{"dataBytes":3,"dataSha256":"%s","parse":"ok"}\\n\' "$(sha256sum "$last" | cut -d" " -f1)"\n'
      )
      const raw = Buffer.from('abc')
      await expect(observer.decodeRaw(raw, { budgetMs: 5000 })).resolves.toEqual({
        dataBytes: 3,
        dataSha256: sha(raw),
        parse: 'ok',
      })
      await observer.dispose()
      observer.assertClean()
    })
    linuxTest('fails a nonzero decoder exit with its stderr', async () => {
      const observer = supervised('echo broken >&2\nexit 3\n')
      await expect(observer.decodeRaw(Buffer.from('abc'), { budgetMs: 5000 })).rejects.toThrow(
        /Decoder failed \(3\): broken/
      )
      await observer.dispose()
      observer.assertClean()
    })
    linuxTest('ends the whole process group when the budget expires', async () => {
      const pidFile = path.join(stub, 'sleeper.pid')
      const observer = supervised(`sleep 30 &\necho $! > '${pidFile}'\nwait\n`)
      await expect(observer.decodeRaw(Buffer.from('abc'), { budgetMs: 700 })).rejects.toThrow(
        /Decoder failed \(124\)/
      )
      expect(() => process.kill(Number(fs.readFileSync(pidFile, 'utf8')), 0)).toThrow(/ESRCH/)
      await observer.dispose()
      observer.assertClean()
    })
  })

  test('closes listeners and reports a still-open client socket on disposal', async () => {
    const { observer } = observe()
    const open1 = open(observer)
    const closed = open(observer, 19095)
    closed.emit('close')
    await observer.dispose()
    await observer.dispose()
    expect(() => observer.assertClean()).toThrow('Observed client socket 1 is still open')
    expect(open1.listenerCount('data')).toBe(0)
    expect(open1.write).toBe(open1.originalWrite)
    expect(closed.listenerCount('close')).toBe(0)
  })

  test('keeps a readback failure first and still deletes the resource', async () => {
    const readback = new Error('readback failed')
    const admin = {
      listConfigResources: jest.fn(async () => ({ resources: [] })),
      incrementalAlterConfigs: jest.fn(async () => ({
        resources: [{ resourceType: 16, resourceName: 'fixture', errorCode: 0 }],
      })),
      describeConfigs: jest.fn(async () => {
        throw readback
      }),
    }
    const work = jest.fn()
    await expect(
      withTelemetryResource(admin, { name: 'fixture', entries: { metrics: 'a' } }, work)
    ).rejects.toBe(readback)
    expect(work).not.toHaveBeenCalled()
    expect(admin.incrementalAlterConfigs).toHaveBeenCalledTimes(2)
  })
})

describe('isolated telemetry CI ownership', () => {
  const { CiOwnership } = require('../../scripts/e2e/telemetry/ci')
  let directory
  const environment = () => ({
    CI: 'true',
    COMPOSE_PROJECT_NAME: 'telemetry-123-1-4',
    COMPOSE_FILE: 'docker-compose.telemetry.yml',
    KAFKA_VERSION: '4.3.1',
    TELEMETRY_EVIDENCE_DIR: path.join(directory, 'sink'),
  })
  const owned = (docker) => {
    const owner = new CiOwnership({ env: environment(), docker })
    owner.receiptFile = path.join(directory, 'ownership.json')
    return owner
  }
  beforeEach(() => {
    directory = fs.mkdtempSync(path.join(os.tmpdir(), 'telemetry-ci-'))
  })
  afterEach(() => fs.rmSync(directory, { recursive: true, force: true }))
  const empty = () => ''

  test('refuses local execution and unsafe project names', () => {
    expect(() => new CiOwnership({ env: { ...environment(), CI: 'false' } })).toThrow('CI-only')
    expect(
      () => new CiOwnership({ env: { ...environment(), COMPOSE_PROJECT_NAME: 'kafkajs' } })
    ).toThrow('unique')
  })

  test.each(['ps', 'network', 'volume'])(
    'refuses a conflicting %s before any mutation',
    (resource) => {
      const docker = jest.fn((args) => (args[0] === resource ? 'foreign-resource' : ''))
      const owner = owned(docker)
      expect(() => owner.preflight()).toThrow('Project resources remain')
      expect(fs.existsSync(owner.receiptFile)).toBe(false)
      expect(
        docker.mock.calls.every(([args]) => !args.includes('down') && !args.includes('up'))
      ).toBe(true)
      owner.stop()
      expect(docker.mock.calls.every(([args]) => !args.includes('down'))).toBe(true)
    }
  )

  test('tears down only an admitted project and preserves removal proof', () => {
    const docker = jest.fn((args) =>
      args.includes('config')
        ? JSON.stringify({
            services: Object.fromEntries(
              ['kafka1', 'kafka2', 'kafka3'].map((key) => [
                key,
                {
                  environment: {
                    KAFKA_TELEMETRY_MAX_BYTES: '4096',
                    KAFKA_METRIC_REPORTERS:
                      'org.countly.kafkajs.telemetry.CountingTelemetryReporter',
                  },
                },
              ])
            ),
          })
        : ''
    )
    const owner = owned(docker)
    owner.preflight()
    owner.stop()
    expect(() =>
      require('../../scripts/e2e/telemetry/evidence').verifyCleanup(
        owner.directory,
        path.dirname(owner.receiptFile),
        owner.project
      )
    ).not.toThrow()
    expect(
      docker.mock.calls.some(
        ([args]) =>
          args.join(' ') ===
          'compose -p telemetry-123-1-4 -f docker-compose.telemetry.yml down -v --remove-orphans --timeout 10'
      )
    ).toBe(true)
    expect(JSON.parse(fs.readFileSync(owner.receiptFile))).toMatchObject({
      project: environment().COMPOSE_PROJECT_NAME,
      preflightPassed: true,
      removalVerified: true,
      removalProof: { containers: [], networks: [], volumes: [] },
      stoppedAt: expect.any(String),
      startedAt: expect.any(String),
    })
  })

  test('proves removal of journalled anonymous volumes without project labels', () => {
    const owner = owned((args) =>
      args[0] === 'volume' && !args.includes('--filter') ? 'anonymous-owned' : ''
    )
    owner.save({
      project: owner.project,
      compose: owner.file,
      preflightPassed: true,
      owned: { containers: [], networks: [], volumes: ['anonymous-owned'] },
    })
    expect(() => owner.stop()).toThrow('Project resources remain')
    const receipt = JSON.parse(fs.readFileSync(owner.receiptFile))
    expect(receipt.removalProof.volumes).toEqual([])
    expect(receipt.ownedRemovalProof.volumes).toEqual(['anonymous-owned'])
    expect(receipt.removalVerified).not.toBe(true)
  })

  test('retains partial ownership and refuses removal certification after startup inspection fails', () => {
    let removed = false
    const owner = owned((args) => {
      if (args.includes('down')) removed = true
      if (!removed && args[0] === 'ps') return 'partial-container'
      if (args[0] === 'inspect') throw new Error('startup inspection failed')
      return ''
    })
    owner.save({ project: owner.project, compose: owner.file, preflightPassed: true })
    expect(() => owner.stop()).toThrow('startup inspection failed')
    expect(removed).toBe(true)
    const receipt = JSON.parse(
      fs.readFileSync(path.join(owner.directory, 'ownership-receipt.json'))
    )
    expect(receipt.owned.containers).toEqual(['partial-container'])
    expect(receipt.journalComplete).toBe(false)
    expect(receipt.removalVerified).toBe(false)
    expect(receipt.journalError).toBe('startup inspection failed')
    expect(fs.existsSync(path.join(owner.directory, 'cleanup-manifest.json'))).toBe(true)
  })

  test('keeps a failed capture journal fatal when cleanup discovery later succeeds', () => {
    const owner = owned((args) => {
      if (args[0] === 'ps') return 'partial-container'
      if (args[0] === 'inspect') throw new Error('capture journal failed')
      return ''
    })
    owner.save({ project: owner.project, compose: owner.file, preflightPassed: true })
    expect(() => owner.capture()).toThrow('capture journal failed')
    owner.docker = empty
    expect(() => owner.stop()).toThrow('capture journal failed')
    expect(
      JSON.parse(fs.readFileSync(path.join(owner.directory, 'ownership-receipt.json')))
    ).toMatchObject({ journalError: 'capture journal failed', removalVerified: false })
  })

  test('reserves teardown and finalization time when diagnostics exhaust their shared budget', () => {
    let now = 0
    const calls = []
    const owner = new CiOwnership({
      env: environment(),
      now: () => now,
      docker: (args, { timeoutMs }) => {
        calls.push({ args, at: now, timeoutMs })
        now += timeoutMs
        return ''
      },
    })
    owner.receiptFile = path.join(directory, 'ownership.json')
    owner.save({ project: owner.project, compose: owner.file, preflightPassed: true })
    expect(() => owner.stop()).toThrow('Cleanup command budget exhausted')
    const down = calls.find(({ args }) => args.includes('down'))
    expect(down.at).toBeLessThanOrEqual(25000)
    expect(down.timeoutMs).toBeGreaterThanOrEqual(30000)
    expect(calls.some(({ args }) => args.includes('logs'))).toBe(false)
    expect(now).toBeLessThanOrEqual(130000)
    expect(
      JSON.parse(fs.readFileSync(path.join(owner.directory, 'ownership-receipt.json')))
    ).toMatchObject({ removalVerified: false, stoppedAt: expect.any(String) })
  })

  test('rejects a surviving volume and retains teardown failures', () => {
    const owner = owned(empty)
    owner.save({ project: owner.project, compose: owner.file, preflightPassed: true })
    owner.docker = (args) => {
      if (args.includes('down')) throw new Error('teardown refused')
      return args[0] === 'volume' ? 'surviving-volume' : ''
    }
    expect(() => owner.stop()).toThrow('teardown refused')
    const receipt = JSON.parse(fs.readFileSync(owner.receiptFile))
    expect(receipt.cleanupError).toBe('teardown refused')
    expect(receipt.removalProof.volumes).toEqual(['surviving-volume'])
    expect(receipt.removalVerified).not.toBe(true)
  })
})

describe('telemetry sidecar verification', () => {
  const {
    persistEvidence,
    verifyScenario,
    verifyEvidence,
  } = require('../../scripts/e2e/telemetry/evidence')
  let directory
  const kinds = ['producer', 'consumer', 'admin', 'share']
  beforeEach(() => {
    directory = fs.mkdtempSync(path.join(os.tmpdir(), 'telemetry-evidence-'))
  })
  afterEach(() => fs.rmSync(directory, { recursive: true, force: true }))
  const create = (plugin = false) => {
    const observers = {}
    const record = {
      status: 'passed',
      run: { stem: 'sample', line: '4.3.1' },
      uuids: {},
      outcomes: {},
      disabledOutcomes: {},
    }
    for (const kind of plugin ? [...kinds, ...kinds.map((k) => k + '-off')] : kinds) {
      const observer = { directory: path.join(directory, 'observer-' + kind) }
      fs.mkdirSync(observer.directory)
      observers[kind] = observer
      record.uuids[kind] = '11223344-5566-7788-99aa-bbccddeeff00'
      record.outcomes[kind.replace(/-off$/, '')] = { status: 'connected' }
      record.disabledOutcomes[kind.replace(/-off$/, '')] = { status: 'connected' }
      const records = plugin
        ? [{ type: 'connect', socketId: 1 }]
        : [
            {
              apiKey: 71,
              response: {
                uuid: record.uuids[kind],
                requestedMetrics: [],
                pushIntervalMs: 300000,
              },
            },
          ]
      fs.writeFileSync(
        path.join(observer.directory, 'transcript.json'),
        JSON.stringify({
          records,
          sockets: [{ id: 1, kind: 'normal' }],
          failures: [],
          cleanup: { children: 0, timers: 0, stillOpen: [], attached: 0 },
        })
      )
    }
    const name = plugin ? 'telemetry-reporter-no-plugin' : 'telemetry-reporter-no-resource'
    record.sidecars = persistEvidence({ name, record, directory, observers })
    return record
  }

  const configured = () => {
    const pinned = JSON.parse(
      fs.readFileSync(path.resolve('scripts/e2e/telemetry/fixtures/manifest.json'))
    )
    const digest = (bytes) => crypto.createHash('sha256').update(bytes).digest('hex')
    const record = {
      status: 'passed',
      run: {
        stem: 'positive',
        project: 'telemetry-positive',
        line: '4.3.1',
        composeSha256: pinned.composeSha256['docker-compose.telemetry.yml'],
      },
      fixture: {
        manifestSha256: digest(fs.readFileSync('scripts/e2e/telemetry/fixtures/manifest.json')),
      },
      results: { resource: { deleted: true }, window: { start: 0, deadline: 60 } },
    }
    const put = (file, value) => fs.writeFileSync(path.join(directory, file), JSON.stringify(value))
    put('identity.json', {
      project: record.run.project,
      line: '4.3.1',
      preflightPassed: true,
      preflight: { containers: [], networks: [], volumes: [] },
      readyAt: 'ready',
      startedAt: 'started',
      configSha256: 'config',
      images: [1, 2, 3].map((n) => ({
        container: String(n),
        repoDigests: pinned.lines['4.3.1'].repoDigests,
      })),
      brokers: [1, 2, 3].map(() => ({
        reporterSha256: pinned.lines['4.3.1'].reporterJar.sha256,
        configSha256: 'config',
      })),
    })
    for (const node of ['1', '2', '3'])
      put(`reporter-${node}.json`, { node, kafkaClients: '4.3.1' })
    const observers = {}
    const rows = []
    for (const [i, kind] of kinds.entries()) {
      const uuid = `00112233-4455-6677-8899-aabbccddee0${i}`
      const clientId = `positive-${kind}`
      const subdir = `observer-${kind}`
      fs.mkdirSync(path.join(directory, subdir))
      observers[kind] = { directory: path.join(directory, subdir) }
      const event = (requestKey, apiKey, writeAt, responseAt, duration) => ({
        requestKey,
        apiKey,
        apiVersion: 0,
        writeAt,
        responseAt,
        duration,
        clientId,
        node: '1',
        socketId: 1,
        correlationId: Number(requestKey.slice(1)),
        knownNode: kind === 'admin' && writeAt > 2,
      })
      const requests =
        kind === 'admin'
          ? [event('r1', 3, 1, 2, 1), event('r2', 32, 3, 4, 5), event('r3', 60, 23, 25, 7)]
          : [event('r1', 18, 1, 2, 1)]
      const wire = requests.map((r, index) => ({
        type: 'request',
        ...r,
        index: index + 1,
        correlation: r.correlationId,
        response: { responseAt: r.responseAt },
      }))
      const connects = [{ type: 'connect', socketId: 1, at: 0.5 }]
      const get = {
        apiKey: 71,
        apiVersion: 0,
        uuid,
        response: {
          uuid,
          errorCode: 0,
          pushIntervalMs: 1000,
          telemetryMaxBytes: 4096,
          requestedMetrics: ['org.apache.kafka.'],
          subscriptionId: 1,
          deltaTemporality: true,
        },
      }
      const times = kind === 'admin' ? [10, 20, 30, 40, 50] : [10, 20, 30]
      const keys = times.map((_, j) => `r${10 + j}`)
      const pushes = times.map((writeAt, j) => {
        const raw = Buffer.from(`${kind}-${j}`)
        const hash = digest(raw)
        const requestKey = keys[j]
        const metrics = [
          {
            name: `org.apache.kafka.${kind === 'share' ? 'consumer' : kind}.connection.creation.total`,
            kind: 'SUM',
            monotonic: true,
            temporality: 'AGGREGATION_TEMPORALITY_DELTA',
            points: [{ attrs: [], asInt: j === 0 ? '1' : '0' }],
          },
        ]
        if (kind === 'admin' && [0, 2].includes(j))
          for (const suffix of ['avg', 'max'])
            metrics.push({
              name: `org.apache.kafka.admin.node.request.latency.${suffix}`,
              kind: 'GAUGE',
              points: [{ attrs: [['node_id', 'string', '1']], asDouble: j === 0 ? '5' : '7' }],
            })
        const attrs = kind === 'admin' ? [] : [['client_rack', 'string', 'fixture-rack']]
        if (kind === 'consumer' || kind === 'share')
          attrs.push([
            'group_id',
            'string',
            kind === 'share' ? 'positive-share-group' : 'positive-group',
          ])
        const decoded = { dataSha256: hash, dataBytes: raw.length, resources: [{ attrs }], metrics }
        const seq = rows.length + 1
        rows.push(
          row('1', seq, {
            clientInstanceId: uuid,
            clientId,
            dataBytes: raw.length,
            dataSha256: hash,
          })
        )
        const result = {
          requestKey,
          uuid,
          node: '1',
          compressionType: 0,
          rawBytes: raw.length,
          transmittedBytes: raw.length,
          rawSha256: hash,
          transmittedSha256: hash,
          decoded,
          exportIdentity: { node: '1', seq },
        }
        fs.writeFileSync(path.join(directory, subdir, `push-${requestKey}.bin`), raw)
        fs.writeFileSync(path.join(directory, subdir, `raw-${hash}.bin`), raw)
        put(`${subdir}/decoded-${requestKey}.json`, result)
        return {
          type: 'request',
          apiKey: 72,
          apiVersion: 0,
          requestKey,
          writeAt,
          response: { errorCode: 0, responseAt: writeAt + 1 },
          uuid,
          clientId,
          node: '1',
          subscriptionId: 1,
          compressionType: 0,
          metricsBytes: raw.length,
          metricsSha256: hash,
          terminating: false,
        }
      })
      record.results[kind] = {
        uuid,
        clientId,
        requestKeys: keys,
        arithmetic: {
          requests,
          connects,
          delta: true,
          total: '1',
          expected: 1,
          sums: times.map((_, j) => (j === 0 ? '1' : '0')),
          allocations:
            kind === 'admin' ? [{ node: '1', windows: [['r2'], [], ['r3'], [], []] }] : [],
        },
      }
      if (kind === 'admin')
        record.results.control = {
          discovery: wire.slice(0, 2),
          knownAfter: 2,
          checkpoint: 2,
          controlledAt: 22,
          wire: wire.slice(2),
          requestEvents: requests.slice(2),
          requests: requests.slice(2),
          samples: ['r12', 'r13', 'r14'],
        }
      put(`${subdir}/transcript.json`, {
        records: [...connects, ...wire, get, ...pushes],
        sockets: [{ id: 1, kind: 'normal' }],
        failures: [],
        cleanup: { children: 0, timers: 0, stillOpen: [], attached: 0 },
      })
    }
    fs.writeFileSync(
      path.join(directory, 'exports-1.jsonl'),
      rows.map(JSON.stringify).join('\n') + '\n'
    )
    fs.writeFileSync(path.join(directory, 'exports-2.jsonl'), '')
    record.sidecars = persistEvidence({ name: 'telemetry-reporter', record, directory, observers })
    return record
  }
  const rewrite = (record, relative, change) => {
    const file = path.join(directory, relative)
    const value = JSON.parse(fs.readFileSync(file))
    change(value)
    const bytes = Buffer.from(JSON.stringify(value))
    fs.writeFileSync(file, bytes)
    Object.assign(
      record.sidecars.files.find((item) => item.path === relative),
      {
        bytes: bytes.length,
        sha256: crypto.createHash('sha256').update(bytes).digest('hex'),
      }
    )
  }

  test('verifies configured project evidence including complete Push and controlled RTT ledgers', () => {
    expect(() => verifyScenario(configured(), directory)).not.toThrow()
  })

  test.each([
    'omitted',
    'rejected',
    'unanswered',
    'source-node',
    'invented-control',
    'skipped-control-sample',
    'late-control',
    'missing-idle',
  ])('rejects configured evidence with %s even when sidecar hashes are refreshed', (mutation) => {
    const record = configured()
    const result = record.results.producer
    if (mutation === 'omitted') {
      result.requestKeys.splice(1, 1)
      result.arithmetic.sums.splice(1, 1)
    } else if (mutation === 'rejected' || mutation === 'unanswered') {
      rewrite(record, record.sidecars.owners.producer, (t) => {
        const push = { ...t.records.find((r) => r.apiKey === 72), requestKey: 'r99', writeAt: 15 }
        if (mutation === 'unanswered') delete push.response
        else push.response = { errorCode: 42, responseAt: 16 }
        t.records.splice(
          t.records.findIndex((r) => r.requestKey === 'r11'),
          0,
          push
        )
      })
    } else if (mutation === 'source-node') {
      rewrite(record, `${record.sidecars.root}/observer-producer/decoded-r10.json`, (d) => {
        d.exportIdentity.node = '2'
      })
      const source = JSON.parse(
        fs
          .readFileSync(path.join(directory, record.sidecars.root, 'exports-1.jsonl'), 'utf8')
          .split('\n')[0]
      )
      const relative = `${record.sidecars.root}/exports-2.jsonl`
      const bytes = Buffer.from(JSON.stringify({ ...source, node: '2' }) + '\n')
      fs.writeFileSync(path.join(directory, relative), bytes)
      Object.assign(
        record.sidecars.files.find((item) => item.path === relative),
        {
          bytes: bytes.length,
          sha256: crypto.createHash('sha256').update(bytes).digest('hex'),
        }
      )
    } else if (mutation === 'invented-control') {
      record.results.control.requests = [{ requestKey: 'invented' }]
    } else if (mutation === 'skipped-control-sample') {
      record.results.control.samples = ['r12', 'r14', 'r13']
    } else if (mutation === 'late-control') {
      record.results.control.controlledAt = 5
    } else {
      record.results.control.samples.pop()
    }
    expect(() => verifyScenario(record, directory)).toThrow()
  })

  test('verifies full immutable archives and rejects tampering or missing cleanup', () => {
    const record = create()
    expect(() => verifyScenario(record, directory)).not.toThrow()
    const file = path.join(directory, record.sidecars.owners.producer)
    const transcript = JSON.parse(fs.readFileSync(file))
    transcript.cleanup.stillOpen.push(1)
    fs.writeFileSync(file, JSON.stringify(transcript))
    expect(() => verifyScenario(record, directory)).toThrow('Sidecar')
    const item = record.sidecars.files.find(
      (entry) => entry.path === record.sidecars.owners.producer
    )
    const bytes = fs.readFileSync(file)
    item.bytes = bytes.length
    item.sha256 = require('crypto').createHash('sha256').update(bytes).digest('hex')
    expect(() => verifyScenario(record, directory)).toThrow()
  })

  test('rejects equal enabled/disabled foreground failures and missing scenarios', () => {
    const record = create(true)
    expect(() => verifyScenario(record, directory)).not.toThrow()
    record.outcomes.producer = record.disabledOutcomes.producer = { status: 'rejected' }
    expect(() => verifyScenario(record, directory)).toThrow('Ordinary baseline failed')
    expect(() => verifyEvidence('telemetry-reporter', directory, [])).toThrow(
      'Scenario catalogue mismatch'
    )
  })
})

describe('complete live telemetry archive verification', () => {
  const { persistEvidence, verifyScenario } = require('../../scripts/e2e/telemetry/evidence')
  let directory
  const digest = (bytes) => crypto.createHash('sha256').update(bytes).digest('hex')
  beforeEach(() => {
    directory = fs.mkdtempSync(path.join(os.tmpdir(), 'telemetry-complete-'))
  })
  afterEach(() => fs.rmSync(directory, { recursive: true, force: true }))

  /** @returns {object} complete three-Push finite archive with independent raw/decode/sink inputs */
  const fixture = () => {
    const owner = 'producer-candidate'
    const observer = { directory: path.join(directory, 'observer-candidate') }
    fs.mkdirSync(observer.directory)
    const rawUuid = '11223344-5566-7788-99aa-bbccddeeff00'
    const get = {
      type: 'request',
      requestKey: 'r1',
      apiKey: 71,
      apiVersion: 0,
      socketId: 1,
      node: '1',
      uuid: ID,
      response: {
        errorCode: 0,
        responseAt: 0,
        uuid: ID,
        subscriptionId: 1,
        telemetryMaxBytes: 4096,
        requestedMetrics: ['com.example.payload.'],
        acceptedCompressionTypes: [0],
        pushIntervalMs: 1000,
      },
    }
    const rows = []
    const writes = [1000, 2001, 3002]
    const pushes = writes.map((writeAt, index) => {
      const requestKey = `r${index + 2}`
      const raw = Buffer.from(`raw-payload-${index}`)
      const hash = digest(raw)
      const metric = {
        name: 'com.example.payload.0',
        kind: 'GAUGE',
        unit: '1',
        points: [
          {
            attrs: [
              ['series', 'string', '0'],
              ['pad', 'string', 'x'.repeat(128)],
            ],
            valueKind: 'AS_DOUBLE',
            asDouble: '0.0',
          },
        ],
      }
      const decoded = {
        requestKey,
        uuid: ID,
        node: '1',
        compressionType: 0,
        transmittedBytes: raw.length,
        transmittedSha256: hash,
        rawBytes: raw.length,
        rawSha256: hash,
        decoded: {
          dataSha256: hash,
          dataBytes: raw.length,
          parse: 'ok',
          truncated: false,
          metrics: [metric],
          resources: [{ attrs: [] }],
        },
        exportIdentity: { node: '1', seq: index + 1 },
      }
      fs.writeFileSync(path.join(observer.directory, `push-${requestKey}.bin`), raw)
      fs.writeFileSync(path.join(observer.directory, `raw-${hash}.bin`), raw)
      fs.writeFileSync(
        path.join(observer.directory, `decoded-${requestKey}.json`),
        JSON.stringify(decoded)
      )
      fs.writeFileSync(
        path.join(observer.directory, `decoder-${index + 1}.json`),
        JSON.stringify({
          rawSha256: hash,
          code: 0,
          signal: null,
          line: '4.3.1',
          stdout:
            JSON.stringify({ verified: true, line: '4.3.1' }) +
            '\n' +
            JSON.stringify(decoded.decoded) +
            '\n',
        })
      )
      rows.push(
        row('1', index + 1, {
          clientId: 'candidate',
          dataBytes: raw.length,
          dataSha256: hash,
          truncated: false,
        })
      )
      return {
        type: 'request',
        requestKey,
        apiKey: 72,
        apiVersion: 0,
        uuid: ID,
        clientId: 'candidate',
        socketId: 1,
        node: '1',
        subscriptionId: 1,
        terminating: false,
        compressionType: 0,
        metricsBytes: raw.length,
        metricsSha256: hash,
        writeAt,
        response: { errorCode: 0, responseAt: writeAt + 1 },
      }
    })
    fs.writeFileSync(
      path.join(observer.directory, 'transcript.json'),
      JSON.stringify({
        records: [get, ...pushes],
        sockets: [{ id: 1, kind: 'telemetry', closedAt: 4000 }],
        failures: [],
        cleanup: { children: 0, timers: 0, stillOpen: [], attached: 0 },
      })
    )
    fs.writeFileSync(
      path.join(directory, 'exports-1.jsonl'),
      rows.map(JSON.stringify).join('\n') + '\n'
    )
    const rawControl = { uuid: rawUuid, transmittedBytes: 4097, errorCode: 118 }
    const record = {
      status: 'passed',
      run: { stem: 'complete', line: '4.3.1' },
      allowed: {},
      zeroPushes: [],
      pushes: pushes.map(({ requestKey }) => ({ owner, requestKey })),
      results: {
        uuid: ID,
        rawControl,
        candidate: {
          expectedCodec: 0,
          offered: [0],
          samples: pushes.map((p) => ({
            requestKey: p.requestKey,
            rawBytes: p.metricsBytes,
            transmittedBytes: p.metricsBytes,
            compressionType: 0,
            partialPoints: [],
            pointIds: [0],
          })),
        },
      },
      oracles: {
        rawControl: {
          probeSha256: '139afdbec5034f7c1498b90306889fd918ae13097efee1ea9f435b03dcf9b1b1',
          advertisement: { code: 0 },
          vector: { vectors: { oversize: { hex: '00'.repeat(4097) } } },
          control: {
            code: 4,
            signal: null,
            lines: [
              { errorCode: 0, fullyConsumed: true, clientInstanceId: rawUuid },
              {
                errorCode: 118,
                fullyConsumed: true,
                clientInstanceId: rawUuid,
                sentBytes: 4097,
                rawSha256: digest(Buffer.alloc(4097)),
              },
            ],
          },
        },
      },
    }
    record.oracles.rawControl.advertisement.lines = [{ key71: [0, 0], key72: [0, 0] }]
    for (const child of [
      record.oracles.rawControl.advertisement,
      record.oracles.rawControl.control,
    ])
      child.stdout = child.lines.map(JSON.stringify).join('\n') + '\n'
    record.sidecars = persistEvidence({
      name: 'telemetry-payload',
      record,
      directory,
      observers: { [owner]: observer },
    })
    return record
  }
  /** @param {object} record archive @param {string} suffix JSON file suffix @param {Function} mutate change @returns {void} */
  const rewrite = (record, suffix, mutate) => {
    const item = record.sidecars.files.find((f) => f.path.endsWith(suffix))
    const file = path.join(directory, item.path)
    const json = JSON.parse(fs.readFileSync(file))
    mutate(json)
    const bytes = Buffer.from(JSON.stringify(json))
    fs.writeFileSync(file, bytes)
    item.bytes = bytes.length
    item.sha256 = digest(bytes)
  }
  test('verifies all accepted Pushes and their byte/decode/sink relationships', () => {
    expect(() => verifyScenario(fixture(), directory)).not.toThrow()
  })
  test.each([
    'omitted',
    'extra',
    'extra-successful',
    'raw-decode',
    'transmitted-raw',
    'contradicted-size',
    'contradicted-point',
    'early-push',
  ])('rejects %s with otherwise valid refreshed sidecar hashes', (change) => {
    const record = fixture()
    if (change === 'omitted') record.pushes.pop()
    else if (change === 'extra')
      record.pushes.push({ owner: 'producer-candidate', requestKey: 'r99' })
    else if (change === 'extra-successful')
      rewrite(record, '/transcript.json', (t) => {
        t.records.push({ ...t.records.find((r) => r.apiKey === 72), requestKey: 'r99' })
      })
    else if (change === 'raw-decode')
      rewrite(record, '/decoded-r3.json', (d) => {
        d.decoded.dataSha256 = 'f'.repeat(64)
      })
    else if (change === 'transmitted-raw')
      rewrite(record, '/decoded-r3.json', (d) => {
        d.rawSha256 = 'f'.repeat(64)
      })
    else if (change === 'contradicted-size') record.results.candidate.samples[1].rawBytes++
    else if (change === 'contradicted-point')
      rewrite(record, '/decoded-r3.json', (d) => {
        d.decoded.metrics[0].points[0].asDouble = '1.0'
      })
    else
      rewrite(record, '/transcript.json', (t) => {
        t.records.find((r) => r.requestKey === 'r3').writeAt = 1950
      })
    expect(() => verifyScenario(record, directory)).toThrow()
  })
})

describe('required live archive dimensions and semantics', () => {
  const { verifyScenario } = require('../../scripts/e2e/telemetry/evidence')
  const { cacheRetirement } = require('../../scripts/e2e/telemetry/cacheTrace')
  const {
    confirmedDecisions,
    heldAcquisitions,
  } = require('../../scripts/e2e/telemetry/shareEvidence')
  const Encoder = require('../../src/protocol/encoder')
  const hash = (bytes) => crypto.createHash('sha256').update(bytes).digest('hex')
  const clone = (value) => JSON.parse(JSON.stringify(value))
  const common = (kind, java = false) => ({
    name: `org.apache.kafka.${kind === 'share' ? 'consumer' : kind}.connection.creation.total`,
    kind: 'SUM',
    unit: java ? '' : '1',
    monotonic: true,
    temporality: 'AGGREGATION_TEMPORALITY_DELTA',
    res: 0,
    pointsInMessage: 1,
    points: [
      {
        attrs: [],
        valueKind: java ? 'AS_DOUBLE' : 'AS_INT',
        ...(java ? { asDouble: '1.0' } : { asInt: '1' }),
        flags: 0,
        startNs: '1',
        timeNs: '2000000',
      },
    ],
  })
  /** @param {string} role Java kind @param {number} index window @returns {object[]} literal descriptors */
  const javaMetrics = (role, index) => [
    common(role, true),
    ...['gauge.double', 'gauge.long', 'sum.total'].map((suffix) => ({
      name: `org.apache.kafka.tf1.custom.${suffix}`,
      unit: '',
      res: 0,
      pointsInMessage: 1,
      kind: suffix === 'sum.total' ? 'SUM' : 'GAUGE',
      ...(suffix === 'sum.total'
        ? { monotonic: true, temporality: 'AGGREGATION_TEMPORALITY_DELTA' }
        : {}),
      points: [
        {
          attrs: [],
          flags: 0,
          startNs: '1',
          timeNs: '2000000',
          ...(suffix === 'gauge.long'
            ? { valueKind: 'AS_INT', asInt: '4242' }
            : {
                valueKind: 'AS_DOUBLE',
                asDouble: suffix === 'sum.total' ? (index === 0 ? '7.0' : '0.0') : '42.5',
                ...(suffix === 'gauge.double' ? { asDoubleBits: '4045400000000000' } : {}),
              }),
        },
      ],
    })),
  ]
  /** Owns a finite full archive; edits regenerate all affected raw/decode/sink receipts and hashes. */
  class Archive {
    /** @param {string} scenario selected required scenario */
    constructor(scenario) {
      this.directory = fs.mkdtempSync(path.join(os.tmpdir(), 'telemetry-required-'))
      this.files = {}
      this.streams = {}
      this.rows = []
      this.next = 1
      this.record = {
        status: 'passed',
        mode: 'configured',
        run: { stem: 'required', line: '4.3.1' },
        pushes: [],
        zeroPushes: [],
        allowed: {},
        results: {},
        sidecars: { root: 'case-required', scenario, owners: {}, files: [] },
      }
      this.oracles = {}
    }

    /** @param {string} owner label @param {string} kind public kind @param {object[]} snapshots metric trees @param {string} [fault] fault mode @returns {object} stream identities */
    owner(owner, kind, snapshots = [], fault) {
      const uuid = `00112233-4455-6677-8899-${String(this.next++).padStart(12, '0')}`
      const clientId = `required-${owner}`
      const get = {
        type: 'request',
        apiKey: 71,
        apiVersion: 0,
        requestKey: 'r1',
        index: 1,
        correlation: 1,
        node: '1',
        socketId: 1,
        uuid: ID,
        clientId,
        writeAt: 0,
        response: {
          errorCode: 0,
          uuid,
          responseAt: 1,
          subscriptionId: 1,
          pushIntervalMs: 1000,
          telemetryMaxBytes: 4096,
          requestedMetrics: ['org.apache.kafka.', 'com.example.'],
        },
      }
      const transcript = {
        records: [
          {
            type: 'request',
            apiKey: kind === 'producer' ? 0 : kind === 'consumer' ? 1 : 3,
            clientId,
            socketId: 3,
          },
          ...(kind === 'consumer' ? [{ type: 'request', apiKey: 8, clientId, socketId: 3 }] : []),
          ...(snapshots.length ? [get] : []),
        ],
        sockets: [
          { id: 3, kind: 'normal', foregroundBootstrap: true, node: '1' },
          ...(snapshots.length
            ? [{ id: 1, kind: 'telemetry', closedAt: fault ? 21 : 100000 }]
            : []),
        ],
        closures: [],
        faults: [],
        cancellations: [],
        failures: [],
        cleanup: { children: 0, timers: 0, stillOpen: [], attached: 0 },
      }
      let recovery
      if (fault) {
        transcript.sockets.push({ id: 2, kind: 'telemetry', closedAt: 100000 })
        transcript.faults.push({ socketId: 1, at: 10 })
        if (fault !== 'blackhole') {
          transcript.records.push({
            type: 'request',
            requestKey: 'r2',
            apiKey: 72,
            apiVersion: 0,
            index: 2,
            clientId,
            socketId: 1,
            correlation: 2,
            writeAt: 20,
            uuid,
            metricsBytes: 1,
          })
          transcript.cancellations.push({
            requestKey: 'r2',
            apiKey: 72,
            socketId: 1,
            writeAt: 20,
            at: 10,
            end: 21,
            disposition: 'faulted-transport',
          })
        }
        transcript.records.push({
          ...clone(get),
          requestKey: 'r3',
          index: 3,
          socketId: 2,
          uuid,
          writeAt: 22,
          response: { ...get.response, responseAt: 23 },
        })
        recovery = {
          owner,
          mode: fault,
          uuid,
          injectedAt: 10,
          checkpoint: 1,
          faultedSocket: 1,
          injectedPush: fault === 'blackhole' ? null : 'r2',
          attempted: { apiKey: 72, at: 20, correlation: 2 },
          droppedCorrelation: fault === 'drop' ? 2 : null,
          replacementGet: 'r3',
          recoveredPush: 'r10',
        }
      }
      const stream = { uuid, clientId, transcript, snapshots, recovery }
      this.streams[owner] = stream
      this.record.sidecars.owners[owner] = `case-required/${owner}/transcript.json`
      for (const [index, decoded] of snapshots.entries()) {
        const requestKey = `r${(index + 1) * 10}`
        transcript.records.push({
          type: 'request',
          requestKey,
          index: (index + 1) * 10,
          apiKey: 72,
          apiVersion: 0,
          clientId,
          uuid,
          node: '1',
          socketId: fault ? 2 : 1,
          subscriptionId: 1,
          compressionType: 0,
          terminating: false,
          writeAt: 1000 + index * 1001,
          response: { errorCode: 0, responseAt: 1001 + index * 1001 },
        })
        this.record.pushes.push({ owner, requestKey })
        decoded.parse = 'ok'
        decoded.truncated = false
      }
      return stream
    }

    /** @returns {void} writes a self-consistent archive with refreshed bytes and digests */
    save() {
      const files = {}
      const rows = clone(this.rows)
      const put = (name, value) => {
        files[`case-required/${name}`] = Buffer.isBuffer(value)
          ? value
          : Buffer.from(JSON.stringify(value))
      }
      for (const [owner, stream] of Object.entries(this.streams)) {
        for (const [index, decoded] of stream.snapshots.entries()) {
          const requestKey = `r${(index + 1) * 10}`
          const request = stream.transcript.records.find((r) => r.requestKey === requestKey)
          const raw = Buffer.from(JSON.stringify(decoded))
          const digest = hash(raw)
          Object.assign(request, { metricsBytes: raw.length, metricsSha256: digest })
          const body = { ...decoded, dataBytes: raw.length, dataSha256: digest }
          const item = {
            requestKey,
            uuid: stream.uuid,
            node: '1',
            compressionType: 0,
            rawBytes: raw.length,
            rawSha256: digest,
            transmittedBytes: raw.length,
            transmittedSha256: digest,
            decoded: body,
            exportIdentity: { node: '1', seq: rows.length + 1 },
          }
          put(`${owner}/push-${requestKey}.bin`, raw)
          put(`${owner}/raw-${digest}.bin`, raw)
          put(`${owner}/decoded-${requestKey}.json`, item)
          put(`${owner}/decoder-${index}.json`, {
            code: 0,
            signal: null,
            line: '4.3.1',
            rawSha256: digest,
            stdout:
              JSON.stringify({ verified: true, line: '4.3.1' }) +
              '\n' +
              JSON.stringify(body) +
              '\n',
          })
          rows.push(
            row('1', rows.length + 1, {
              ...body,
              clientId: stream.clientId,
              clientInstanceId: stream.uuid,
              terminating: false,
            })
          )
        }
        put(`${owner}/transcript.json`, stream.transcript)
      }
      put('oracles.json', this.oracles)
      put(
        'exports-1.jsonl',
        Buffer.from(rows.map((r, i) => JSON.stringify({ ...r, seq: i + 1 })).join('\n') + '\n')
      )
      for (const [relative, bytes] of Object.entries(files)) {
        const file = path.join(this.directory, relative)
        fs.mkdirSync(path.dirname(file), { recursive: true })
        fs.writeFileSync(file, bytes)
      }
      this.record.sidecars.files = Object.entries(files).map(([path, bytes]) => ({
        path,
        bytes: bytes.length,
        sha256: hash(bytes),
      }))
    }

    /** @returns {void} full entrypoint, with no helper validator substitution */
    verify() {
      this.save()
      verifyScenario(this.record, this.directory)
    }
  }
  let archive
  afterEach(() => {
    if (archive) fs.rmSync(archive.directory, { recursive: true, force: true })
  })
  /** @returns {Archive} complete seven-client interop archive */
  const javaArchive = () => {
    const a = new Archive('telemetry-java')
    archive = a
    const r = a.record.results
    r.java = {}
    r.js = {}
    r.uuids = { java: {}, js: {} }
    for (const kind of ['producer', 'consumer', 'admin', 'share']) {
      const stream = a.owner(
        `js-${kind}`,
        kind,
        Array.from({ length: 3 }, () => ({ metrics: [common(kind)], resources: [{ attrs: [] }] }))
      )
      r.js[kind] = { uuid: stream.uuid }
      r.uuids.js[kind] = stream.uuid
    }
    for (const role of ['producer', 'consumer', 'admin']) {
      const uuid = `11223344-4455-6677-8899-${String(a.next++).padStart(12, '0')}`
      r.uuids.java[role] = uuid
      const membership = {
        membership: true,
        fromEpochMs: 0,
        epochMs: 1,
        beforeMemberId: '',
        memberId: 'member',
      }
      r.java[role] = {
        uuid,
        assignedId: `required-java-${role}`,
        closing: { closing: true, epochMs: 30 },
        closed: { closed: true, epochMs: 40 },
        exitCode: 0,
        exitSignal: null,
        closeToExitMs: 10,
        liveDescendants: [],
        stdout: JSON.stringify(membership) + '\n',
      }
      for (let index = 0; index < 3; index++)
        a.rows.push(
          row('1', a.rows.length + 1, {
            tsMs: index === 2 ? 35 : 10 + index,
            clientInstanceId: uuid,
            clientId: `required-java-${role}`,
            terminating: index === 2,
            truncated: false,
            resourceMetricsCount: 1,
            metricsTotal: 4,
            metricsKept: 4,
            pointsTotal: 4,
            resources: [
              {
                attrs:
                  role === 'consumer'
                    ? [
                        ['group_id', 'string', 'required-java-consumer-group'],
                        ['group_member_id', 'string', 'member'],
                      ]
                    : [],
                droppedAttributes: 0,
              },
            ],
            metrics: javaMetrics(role, index),
          })
        )
    }
    const fixture = path.join(__dirname, '../../scripts/e2e/telemetry')
    const cache = fs
      .readFileSync(path.join(fixture, 'interop.sha256'), 'utf8')
      .trim()
      .split('\n')
      .map((line) => {
        const [sha256, name] = line.trim().split(/\s+/)
        return { name, sha256 }
      })
    a.record.fixture = { clientSourceSha256: 'a'.repeat(64), runnerSha256: 'b'.repeat(64) }
    r.provenance = {
      sourceSha256: a.record.fixture.clientSourceSha256,
      runnerSha256: a.record.fixture.runnerSha256,
      cache,
      jdk: Object.fromEntries(
        ['producer', 'consumer', 'admin'].map((role) => [role, { jdk: '21', vendor: 'OpenJDK' }])
      ),
    }
    return a
  }
  test('verifies a complete Java archive with independent role resources and every JS export', () => {
    javaArchive().verify()
  })
  test.each(['owner', 'Java role'])(
    'rejects a deleted required %s from an otherwise complete archive',
    (dimension) => {
      const a = javaArchive()
      a.verify()
      if (dimension === 'owner') delete a.record.sidecars.owners['js-admin']
      else delete a.record.results.java.admin
      expect(() => a.verify()).toThrow(/Required.*catalogue/)
    }
  )
  test('rejects a later JS common counter change with matching raw decode sink and refreshed hashes', () => {
    const a = javaArchive()
    a.verify()
    a.streams['js-admin'].snapshots[2].metrics[0].monotonic = false
    expect(() => a.verify()).toThrow('JS common counter must be monotonic')
  })
  test.each([
    ['missing', 'Unexpected Java resource catalogue'],
    ['foreign', 'Unexpected Java resource catalogue'],
    ['inapplicable', 'Unexpected Java resource catalogue'],
    ['value', 'Unexpected Java resource catalogue'],
    ['unit', 'Unexpected Java descriptor unit'],
    ['dynamic-member', 'Unexpected Java resource catalogue'],
    ['missing-member', 'Unexpected Java resource catalogue'],
    ['duplicate', 'Duplicate Java resource key'],
  ])('rejects Java %s resource or descriptor evidence', (change, diagnostic) => {
    const a = javaArchive()
    a.verify()
    for (const r of a.rows.filter(
      (r) =>
        r.clientId ===
        (change === 'inapplicable' ? 'required-java-admin' : 'required-java-consumer')
    )) {
      if (change === 'missing') r.resources[0].attrs.shift()
      if (change === 'foreign') r.resources[0].attrs.push(['foreign', 'string', 'x'])
      if (change === 'inapplicable')
        r.resources[0].attrs.push(['transactional_id', 'string', 'foreign'])
      if (change === 'value') r.resources[0].attrs[0][2] = 'foreign-group'
      if (change === 'dynamic-member') r.resources[0].attrs[1][2] = 'foreign-member'
      if (change === 'missing-member') r.resources[0].attrs.pop()
      if (change === 'duplicate') r.resources[0].attrs.push(r.resources[0].attrs[0])
      if (change === 'unit') r.metrics[1].unit = '1'
    }
    expect(() => a.verify()).toThrow(diagnostic)
  })
  /** @returns {Archive} complete custom lifecycle archive */
  const customArchive = () => {
    const a = new Archive('telemetry-custom')
    archive = a
    const values = [
      { anchor: '1.0', negative: '-2.5', zero: '0.0', total: '9007199254740993' },
      { anchor: '1.0', negative: '-3.5', zero: '6.25', total: '9' },
      { anchor: '1.0', negative: '-3.5', zero: '6.25', total: '0' },
      { anchor: '1.0', negative: '-3.5' },
      { anchor: '1.0', negative: '-3.5', total: '1' },
    ]
    const snapshots = values.map((v, index) => ({
      resourceMetricsCount: 1,
      resources: [{ attrs: [], droppedAttributes: 0 }],
      metricsTotal: Object.keys(v).length,
      metricsKept: Object.keys(v).length,
      pointsTotal: Object.keys(v).length,
      metrics: Object.entries(v).map(([name, value]) => ({
        name: `com.example.${name}`,
        unit: '1',
        res: 0,
        pointsInMessage: 1,
        kind: name === 'total' ? 'SUM' : 'GAUGE',
        ...(name === 'total'
          ? { monotonic: true, temporality: 'AGGREGATION_TEMPORALITY_DELTA' }
          : {}),
        points: [
          {
            attrs: [['series', 'string', 'live']],
            startNs: index === 4 ? '100' : '1',
            timeNs: '200',
            ...(name === 'total'
              ? { valueKind: 'AS_INT', asInt: value }
              : { valueKind: 'AS_DOUBLE', asDouble: value }),
            ...(name === 'zero' && index === 0 ? { asDoubleBits: '0' } : {}),
            ...(name === 'negative' && index === 0 ? { asDoubleBits: 'c004000000000000' } : {}),
          },
        ],
      })),
    }))
    const stream = a.owner('producer', 'producer', snapshots)
    a.record.results = {
      uuid: stream.uuid,
      firstRequestKey: 'r10',
      update: { updateFence: 15, requestKeys: ['r20', 'r30'], states: ['updated', 'following'] },
      removal: {
        requestKey: 'r40',
        removeFence: 35,
        cursor: { 1: 3 },
        names: ['com.example.anchor', 'com.example.negative'],
      },
      readded: { requestKey: 'r50', readdFence: 45, startNs: '100', asInt: '1' },
      unregisterFence: 55,
    }
    return a
  }
  test('verifies exact custom resource metric and point cardinalities in every lifecycle window', () => {
    customArchive().verify()
  })
  test.each([
    ['metric', 'Custom metric count differs'],
    ['series', 'Custom total point count differs'],
    ['resource', 'Custom resource count differs'],
    ['point-count', 'Custom total point count differs'],
  ])(
    'rejects a later custom duplicate %s with consistent raw decode sink and hashes',
    (change, diagnostic) => {
      const a = customArchive()
      a.verify()
      const d = a.streams.producer.snapshots[2]
      if (change === 'metric') {
        d.metrics.push(clone(d.metrics[2]))
        d.metricsTotal++
        d.metricsKept++
        d.pointsTotal++
      }
      if (change === 'series') {
        d.metrics[2].points.push(clone(d.metrics[2].points[0]))
        d.metrics[2].pointsInMessage++
        d.pointsTotal++
      }
      if (change === 'resource') {
        d.resources.push(clone(d.resources[0]))
        d.resourceMetricsCount++
      }
      if (change === 'point-count') d.pointsTotal++
      expect(() => a.verify()).toThrow(diagnostic)
    }
  )
  /**
   * Builds independent acquisition and settlement exchanges, retaining both ShareFetch statuses.
   * @param {string} topicId owned or deliberately foreign UUID
   * @param {number} [releaseAt] broker confirmation time
   * @param {number} [releaseType] release or accept decision
   * @param {object} [statuses] independent acquisition/settlement status controls
   * @returns {object[]} original acquisition and acknowledgements
   * @throws {Error} for invalid primitive wire fields
   */
  const shareFrames = (topicId, releaseAt = 301, releaseType = 2, statuses = {}) => {
    const frame = (buffer) => new Encoder().writeInt32(buffer.length).writeBuffer(buffer).buffer
    const exchange = (apiKey, correlation, batches, acquired, at, responseAt) => {
      const partition = new Encoder()
        .writeInt32(0)
        .writeUVarIntArray(
          batches.map(([first, last, type]) =>
            new Encoder()
              .writeInt64(first)
              .writeInt64(last)
              .writeUVarIntArray([new Encoder().writeInt8(type)])
              .writeTaggedFields([])
          )
        )
        .writeTaggedFields([])
      const topic = new Encoder()
        .writeUuid(topicId)
        .writeUVarIntArray([partition])
        .writeTaggedFields([])
      const body = new Encoder()
        .writeUVarIntString('share-group')
        .writeUVarIntString('member')
        .writeInt32(1)
      if (apiKey === 78)
        body
          .writeInt32(100)
          .writeInt32(1)
          .writeInt32(4096)
          .writeInt32(10)
          .writeInt32(10)
          .writeInt8(0)
      body.writeBoolean(false).writeUVarIntArray([topic])
      if (apiKey === 78) body.writeUVarIntArray([])
      body.writeTaggedFields([])
      const request = frame(
        new Encoder()
          .writeInt16(apiKey)
          .writeInt16(2)
          .writeInt32(correlation)
          .writeString('holder')
          .writeTaggedFields([])
          .writeEncoder(body).buffer
      )
      const reply = require(
        apiKey === 78 ? '../wire/shareFetchV2' : '../wire/shareAcknowledgeV2'
      ).buildResponse({
        errorCode: 0,
        errorMessage: null,
        throttleTime: 0,
        acquisitionLockTimeoutMs: 15000,
        nodeEndpoints: [],
        responses: [
          {
            topicId,
            partitions: [
              {
                partitionIndex: 0,
                errorCode:
                  correlation === 1
                    ? (statuses.acquisitionFetchError ?? 0)
                    : correlation === 2
                      ? (statuses.settlementFetchError ?? 0)
                      : 0,
                errorMessage: null,
                acknowledgeErrorCode:
                  correlation === 1
                    ? (statuses.acquisitionAckError ?? 0)
                    : correlation === 2
                      ? (statuses.settlementAckError ?? 0)
                      : 0,
                acknowledgeErrorMessage: null,
                currentLeader: { leaderId: 1, leaderEpoch: 0 },
                records: '',
                acquiredRecords: acquired,
              },
            ],
          },
        ],
      })
      return {
        at,
        responseAt,
        requestHex: request.toString('hex'),
        responseHex: frame(
          new Encoder().writeInt32(correlation).writeTaggedFields([]).writeBuffer(reply).buffer
        ).toString('hex'),
      }
    }
    return [
      exchange(78, 1, [], [{ firstOffset: '0', lastOffset: '9', deliveryCount: 1 }], 100, 110),
      exchange(statuses.settlementApi ?? 79, 2, [['0', '3', 1]], [], 200, 201),
      exchange(79, 3, [['4', '9', releaseType]], [], 300, releaseAt),
    ]
  }
  /** @returns {Archive} full configured outcomes archive, including raw share confirmations */
  const outcomesArchive = () => {
    const a = new Archive('telemetry-outcomes')
    archive = a
    const r = a.record.results
    r.arms = {}
    r.faults = {}
    for (const name of ['control', 'optin', 'drop', 'blackhole', 'late']) {
      for (const kind of ['producer', 'consumer']) {
        const stream = a.owner(
          `${name}-${kind}`,
          kind,
          name === 'control' ? [] : [{ metrics: [common(kind)], resources: [{ attrs: [] }] }],
          ['drop', 'blackhole', 'late'].includes(name) ? name : undefined
        )
        if (stream.recovery) (r.faults[name] ??= []).push(stream.recovery)
      }
      const ends = { 0: '334', 1: '333', 2: '333' }
      const offsets = [0, 0, 0]
      const log = Array.from({ length: 1000 }, (_, i) => ({
        key: `required-${name}-${String(i).padStart(4, '0')}`,
        valueHex: '00',
        partition: i % 3,
        offset: String(offsets[i % 3]++),
      }))
      const digest = hash(
        Buffer.from(
          log
            .map((r) => r.key)
            .sort()
            .join('\n')
        )
      )
      r.arms[name] = {
        count: 1000,
        expectedSha256: digest,
        logSha256: digest,
        handlerSha256: digest,
        handlerEntries: 1000,
        rawEnd: ends,
        rawCommitted: ends,
        processedNext: ends,
      }
      a.oracles[name] = {
        expected: log.map(({ key, valueHex, partition }) => ({ key, valueHex, partition })),
        log,
        handled: clone(log),
        ends,
        commits: Object.entries(ends).map(([partition, offset]) => ({
          partition: Number(partition),
          offset,
        })),
      }
    }
    const stream = a.owner(
      'share',
      'share',
      Array.from({ length: 2 }, () => ({
        metrics: [common('share')],
        resources: [{ attrs: [] }],
      })),
      'drop'
    )
    const records = stream.transcript.records
    const before = records.splice(
      records.findIndex((r) => r.requestKey === 'r10'),
      1
    )[0]
    Object.assign(before, {
      index: 2,
      socketId: 1,
      writeAt: 2,
      response: { errorCode: 0, responseAt: 3 },
    })
    records.splice(
      records.findIndex((r) => r.requestKey === 'r2'),
      0,
      before
    )
    records.find((r) => r.requestKey === 'r2').index = 3
    records.find((r) => r.requestKey === 'r3').index = 4
    Object.assign(stream.recovery, { checkpoint: 2, recoveredPush: 'r20' })
    const topic = 'required-share-topic'
    const topicId = 'ffeeddcc-bbaa-9988-7766-554433221100'
    const frames = shareFrames(topicId)
    const log = Array.from({ length: 10 }, (_, i) => ({
      topic,
      partition: 0,
      offset: String(i),
      key: `share-${i}`,
      valueHex: '00',
    }))
    a.oracles.share = {
      topic,
      topicId,
      metadata: { topics: [{ name: topic, topicId }] },
      group: 'share-group',
      acquiredAt: 100,
      handlerAt: 120,
      expected: log.map(({ key, valueHex }) => ({ key, valueHex })),
      log,
      handled: clone(log),
      frames,
      held: heldAcquisitions(frames, 'share-group'),
      confirmed: confirmedDecisions(frames, 'share-group'),
      settlements: [
        { topic, partition: 0, firstOffset: '0', lastOffset: '3', type: 'accept', error: null },
        {
          topic,
          partition: 0,
          firstOffset: '4',
          lastOffset: '9',
          type: 'release',
          error: {
            name: 'KafkaJSShareAcknowledgementError',
            reason: 'membership-lost',
            retriable: false,
            topic,
            partition: 0,
          },
        },
      ],
      redelivered: log.slice(4).map((r) => ({ ...r, deliveryCount: 2, at: 400 })),
    }
    r.share = {
      recovery: stream.recovery,
      lockMs: 15000,
      redelivered: log
        .slice(4)
        .map((r) => r.key)
        .sort(),
    }
    return a
  }
  test('verifies configured ordinary fault arms and exact share topic offset handler and settlement identities', () => {
    outcomesArchive().verify()
  })
  test.each(['ordinary arm', 'fault mode'])(
    'rejects a deleted required %s and its obligations',
    (dimension) => {
      const a = outcomesArchive()
      a.verify()
      if (dimension === 'ordinary arm') {
        delete a.record.results.arms.drop
        delete a.record.results.faults.drop
      } else delete a.record.results.faults.late
      expect(() => a.verify()).toThrow(/Required.*catalogue/)
    }
  )
  test.each([
    ['missing-recovery', 'Missing configured share recovery'],
    ['unexercised-recovery', 'Invalid recovered Push'],
    ['foreign-topic', 'Broker share settlement topic differs'],
    ['handler-offset', 'Share handler identities differ'],
    ['extra-settlement', 'Public share settlement outside owned offsets'],
    ['duplicate-settlement', 'Public share settlement must occur exactly once'],
    ['lock-expired-release', 'Settlement after lock expiry'],
    ['duplicate-broker-settlement', 'Extra or duplicate broker share settlement'],
    ['foreign-acquisition', 'Share acquisition topic differs'],
    ['public-topic', 'Public share settlement topic differs'],
  ])('rejects share %s through the full archive verifier', (change, diagnostic) => {
    const a = outcomesArchive()
    a.verify()
    const inputs = a.oracles.share
    if (change === 'missing-recovery') delete a.record.results.share.recovery
    if (change === 'unexercised-recovery') a.record.results.share.recovery.recoveredPush = 'r99'
    if (change === 'foreign-topic') {
      inputs.frames.splice(1, 2, ...shareFrames(ID).slice(1))
      inputs.confirmed = confirmedDecisions(inputs.frames, inputs.group)
    }
    if (change === 'handler-offset') inputs.handled[4].offset = '5'
    if (change === 'extra-settlement')
      inputs.settlements.push({ ...inputs.settlements[0], firstOffset: '10', lastOffset: '10' })
    if (change === 'duplicate-settlement') inputs.settlements.push(clone(inputs.settlements[0]))
    if (change === 'lock-expired-release') {
      inputs.frames = shareFrames(inputs.topicId, 15100)
      inputs.confirmed = confirmedDecisions(inputs.frames, inputs.group)
    }
    if (change === 'duplicate-broker-settlement') {
      inputs.frames.push(clone(inputs.frames[1]))
      inputs.confirmed = confirmedDecisions(inputs.frames, inputs.group)
    }
    if (change === 'foreign-acquisition') {
      inputs.frames[0] = shareFrames(ID)[0]
      inputs.held = heldAcquisitions(inputs.frames, inputs.group)
    }
    if (change === 'public-topic') inputs.settlements[1].topic = 'foreign'
    expect(() => a.verify()).toThrow(diagnostic)
  })
  test.each(['replacement Get', 'pre-fault Push', 'only pre-fault exports'])(
    'rejects recovery using %s with consistent raw decode sink and hashes',
    (change) => {
      const a = outcomesArchive()
      a.verify()
      const recovery = a.record.results.share.recovery
      if (change === 'replacement Get') recovery.recoveredPush = recovery.replacementGet
      else if (change === 'pre-fault Push') recovery.recoveredPush = 'r10'
      else {
        recovery.recoveredPush = recovery.replacementGet
        a.streams.share.snapshots.pop()
        a.streams.share.transcript.records = a.streams.share.transcript.records.filter(
          (r) => r.requestKey !== 'r20'
        )
        a.record.pushes = a.record.pushes.filter(
          (p) => p.owner !== 'share' || p.requestKey !== 'r20'
        )
      }
      expect(() => a.verify()).toThrow('Invalid recovered Push')
    }
  )
  test('rejects a recovered Push substituted as the replacement Get with refreshed hashes', () => {
    const a = outcomesArchive()
    a.verify()
    a.record.results.share.recovery.replacementGet = a.record.results.share.recovery.recoveredPush
    expect(() => a.verify()).toThrow('Invalid recovery replacement Get')
  })
  test.each(['request ordinal', 'response time'])(
    'rejects a recovered Push at the replacement Get %s with refreshed hashes',
    (boundary) => {
      const a = outcomesArchive()
      a.verify()
      const { recovery } = a.record.results.share
      const records = a.streams.share.transcript.records
      const get = records.find((r) => r.requestKey === recovery.replacementGet)
      const push = records.find((r) => r.requestKey === recovery.recoveredPush)
      if (boundary === 'request ordinal') push.index = get.index
      else push.writeAt = get.response.responseAt
      expect(() => a.verify()).toThrow('Invalid recovered Push')
    }
  )
  /** @returns {Archive} all four reopen episodes, three zero-timeout owners and admin blackhole */
  const shutdownArchive = () => {
    const a = new Archive('telemetry-shutdown')
    archive = a
    const r = a.record.results
    r.episodes = {}
    r.timeout0 = {}
    r.loggerReceipts = []
    r.originalLevels = {}
    r.restoredLevels = {}
    for (const id of ['1', '2', '3']) {
      r.originalLevels[id] = r.restoredLevels[id] = {
        'org.apache.kafka.server.ClientMetricsManager': null,
        'kafka.request.logger': null,
      }
      for (let n = 0; n < 3; n++)
        r.loggerReceipts.push({
          id,
          action: ['--describe'],
          result: { code: 0, stdout: '', truncated: false },
        })
    }
    for (const kind of ['producer', 'consumer', 'admin', 'share']) {
      const stream = a.owner(`${kind}-ep`, kind, [
        { metrics: [common(kind)], resources: [{ attrs: [] }] },
      ])
      const get = stream.transcript.records.find((r) => r.apiKey === 71)
      const connectionId = '172.18.0.3:9092-172.18.0.1:54321-3'
      const trace = {
        node: '1',
        localPort: '9092',
        uuid: stream.uuid,
        since: '2026-11-07T10:00:00Z',
        completedAt: '2026-11-07T10:00:05Z',
        source: {
          sha256: 'a'.repeat(64),
          receipt: { code: 0, stdout: 'a'.repeat(64) + ' source' },
        },
        get,
        connect: {
          socketId: 1,
          node: '1',
          localAddress: '127.0.0.1',
          localPort: 54321,
          remoteAddress: '127.0.0.1',
          remotePort: 19092,
        },
        loggerReadbacks: r.loggerReceipts,
        logs: {
          1: {
            text: `TRACE Completed request:RequestHeader(apiKey=GET_TELEMETRY_SUBSCRIPTIONS, clientId=${stream.clientId}, correlationId=1) -- GetTelemetrySubscriptionsRequestData(clientInstanceId=${ID}) with response:GetTelemetrySubscriptionsResponseData(clientInstanceId=${stream.uuid}) from connection ${connectionId};totalTime:1\nTRACE Removing client connection id [${connectionId}] from the client instance cache`,
          },
          2: { text: '' },
          3: { text: '' },
        },
      }
      const cacheEvent = cacheRetirement(trace)
      a.oracles[kind] = trace
      r.episodes[kind] = {
        before: { uuid: stream.uuid, serverConnectionIdentity: cacheEvent.connectionIdentity },
        cacheEvent,
        reopen: {
          calledBeforeCacheAwait: true,
          cachedUuid: stream.uuid,
          get: { requestUuid: stream.uuid },
          finalResends: 0,
          correctiveExtraPushes: 0,
        },
        close: { getOrConnectOrAuthFrames: [] },
      }
    }
    for (const kind of ['producer', 'consumer', 'share']) {
      const stream = a.owner(`${kind}-t0`, kind, [
        { metrics: [common(kind)], resources: [{ attrs: [] }] },
      ])
      r.timeout0[kind] = { uuid: stream.uuid, finalZeroTimeoutWrites: 0 }
    }
    const blackhole = a.owner('admin-bh', 'admin', [
      { metrics: [common('admin')], resources: [{ attrs: [] }] },
    ])
    r.blackhole = { uuid: blackhole.uuid, faultedSocket: 1 }
    return a
  }
  test('verifies every shutdown and zero-timeout owner against its required episode', () => {
    shutdownArchive().verify()
  })
  test.each(['episode', 'zero-timeout', 'blackhole'])(
    'rejects a deleted required shutdown %s from the archive',
    (dimension) => {
      const a = shutdownArchive()
      a.verify()
      if (dimension === 'episode') delete a.record.results.episodes.admin
      if (dimension === 'zero-timeout') delete a.record.results.timeout0.share
      if (dimension === 'blackhole') delete a.record.results.blackhole
      expect(() => a.verify()).toThrow(/Required|Missing admin blackhole/)
    }
  )
  /** @returns {Archive} capable L6 full archive under both telemetry options */
  const unsupportedArchive = () => {
    const a = outcomesArchive()
    const corpus = a.record.results.arms.control
    const inputs = a.oracles.control
    a.streams = {}
    a.record.pushes = []
    a.record.sidecars.owners = {}
    a.record.sidecars.scenario = 'telemetry-unsupported'
    a.record.corpus = corpus
    a.record.outcomes = {}
    a.record.disabledOutcomes = {}
    a.record.results = { disabledCorpus: corpus, ownership: {} }
    a.oracles = { corpus: inputs, disabledCorpus: inputs, share: {} }
    const topic = 'required-share-work'
    const topicId = 'ffeeddcc-bbaa-9988-7766-554433221100'
    for (const kind of ['producer', 'consumer', 'admin', 'share']) {
      for (const owner of [kind, `${kind}-off`]) {
        const stream = a.owner(owner, kind)
        stream.transcript.records.push({ type: 'connect', socketId: 3 })
        a.record.outcomes[kind] = a.record.disabledOutcomes[kind] = { status: 'connected' }
      }
      a.record.results.ownership[kind] = { on: ['1:bootstrap'], off: ['1:bootstrap'] }
    }
    for (const owner of ['share', 'share-off']) {
      const frames = shareFrames(topicId, 301, 1)
      const keys = Array.from({ length: 10 }, (_, i) => `share-${i}`)
      const log = keys.map((key, i) => ({
        topic,
        partition: 0,
        offset: String(i),
        key,
        valueHex: Buffer.from(key).toString('hex'),
      }))
      a.oracles.share[owner] = {
        topic,
        topicId,
        metadata: { topics: [{ name: topic, topicId }] },
        group: 'share-group',
        keys,
        frames,
        held: heldAcquisitions(frames, 'share-group'),
        log,
        handled: clone(log),
        settlements: [
          { topic, partition: 0, firstOffset: '0', lastOffset: '9', type: 'accept', error: null },
        ],
      }
    }
    return a
  }
  test('verifies capable L6 topic and exact acquisition handler and broker identities for both options', () => {
    unsupportedArchive().verify()
  })
  test.each(['share', 'share-off'])(
    'rejects capable L6 foreign-topic offsets for %s with refreshed hashes',
    (owner) => {
      const a = unsupportedArchive()
      a.verify()
      const share = a.oracles.share[owner]
      share.frames.splice(1, 2, ...shareFrames(ID, 301, 1).slice(1))
      expect(() => a.verify()).toThrow('Capable share broker settlement identity differs')
    }
  )
  test.each(['configured share', 'share', 'share-off'])(
    'verifies independent fetch and acknowledgement statuses for %s',
    (owner) => {
      const a = owner === 'configured share' ? outcomesArchive() : unsupportedArchive()
      const share = owner === 'configured share' ? a.oracles.share : a.oracles.share[owner]
      const releaseType = owner === 'configured share' ? 2 : 1
      a.verify()
      share.frames = shareFrames(share.topicId, 301, releaseType, {
        acquisitionAckError: 7,
        settlementApi: 78,
        settlementFetchError: 7,
      })
      a.verify()
    }
  )
  test.each([
    ['configured share', 'fetch error + ack ok', 'Missing raw share acquisition'],
    ['configured share', 'fetch ok + ack error', 'Contradicted acknowledgement ledger'],
    ['share', 'fetch error + ack ok', 'Contradicted held acquisition ranges'],
    ['share', 'fetch ok + ack error', 'Incomplete capable share broker settlement'],
    ['share-off', 'fetch error + ack ok', 'Contradicted held acquisition ranges'],
    ['share-off', 'fetch ok + ack error', 'Incomplete capable share broker settlement'],
  ])('rejects %s %s with refreshed hashes', (owner, change, diagnostic) => {
    const a = owner === 'configured share' ? outcomesArchive() : unsupportedArchive()
    const share = owner === 'configured share' ? a.oracles.share : a.oracles.share[owner]
    const releaseType = owner === 'configured share' ? 2 : 1
    share.frames = shareFrames(share.topicId, 301, releaseType, { settlementApi: 78 })
    a.verify()
    share.frames = shareFrames(share.topicId, 301, releaseType, {
      settlementApi: 78,
      ...(change === 'fetch error + ack ok'
        ? { acquisitionFetchError: 7 }
        : { settlementAckError: 7 }),
    })
    expect(() => a.verify()).toThrow(diagnostic)
  })
  test('verifies the base outcomes catalogue and rejects a missing ordinary arm', () => {
    const a = outcomesArchive()
    a.record.mode = 'base'
    a.record.run.line = '4.1.2'
    a.record.results.arms = {
      control: a.record.results.arms.control,
      optin: a.record.results.arms.optin,
    }
    a.record.results.faults = {}
    a.oracles = { control: a.oracles.control, optin: a.oracles.optin }
    a.record.results.inapplicable = {
      drop: 'absent telemetry transport',
      blackhole: 'absent telemetry transport',
      late: 'absent telemetry transport',
    }
    a.record.results.share = {
      status: 'rejected',
      name: 'KafkaJSShareGroupsNotSupportedError',
      reason: 'api-version-range',
      apiKey: 78,
      shareVersion: 0,
    }
    a.streams = {}
    a.record.pushes = []
    a.record.sidecars.owners = {}
    for (const owner of [
      'control-producer',
      'control-consumer',
      'optin-producer',
      'optin-consumer',
      'share',
    ])
      a.owner(owner, owner.split('-').at(-1))
    a.verify()
    delete a.record.results.arms.optin
    expect(() => a.verify()).toThrow(/Required ordinary arm catalogue/)
  })
})

describe('telemetry observation segments', () => {
  const {
    SCHEDULES,
    TelemetrySegments,
    checkSegmentFiles,
    segmentCatalogue,
  } = require('../telemetrySegments')
  let directory
  let owners
  let dial
  const clock = (() => {
    let tick = 0
    return { now: () => ++tick }
  })()
  const shared = () => ({
    directory,
    clock,
    observer: { line: '4.3.1', dial, decoder: stubDecoder, mode: 'configured' },
  })
  const ordinary = (scenario = 'telemetry-shutdown', kind = 'producer') => {
    const segments = TelemetrySegments.ordinary(scenario, kind, shared())
    owners.push(segments)
    return segments
  }
  const open = (segments, port = 19092) => {
    const socket = segments.socketFactory({ host: 'localhost', port, onConnect: jest.fn() })
    socket.emit('connect')
    return socket
  }
  const send = (socket, ...frames) => socket.write(Buffer.concat(frames))
  const receive = (socket, ...frames) => socket.emit('data', Buffer.concat(frames))
  const fakeClient = (segments, order = []) => {
    const client = {
      sockets: [],
      disconnect: jest.fn(async function disconnect() {
        order.push(`disconnect:${segments.current().id}`)
        for (const socket of this.sockets.splice(0)) socket.emit('close')
        setImmediate(() => order.push('immediate'))
      }),
      connect: jest.fn(function connect() {
        order.push('connect')
        this.sockets.push(open(segments))
        return Promise.resolve()
      }),
    }
    client.sockets.push(open(segments))
    return client
  }
  const rotate = async (segments, client) => {
    const cycle = segments.cycle(client, { timeout: 3000 })
    await Promise.all([cycle.closed, cycle.reopened])
    return cycle
  }

  beforeEach(() => {
    directory = fs.mkdtempSync(path.join(os.tmpdir(), 'telemetry-segments-'))
    owners = []
    dial = jest.fn(({ port }) => fakeSocket(port))
    stubDecoder.mockClear()
  })
  afterEach(async () => {
    await Promise.allSettled(
      owners.flatMap((owner) => owner.undischarged().map((segment) => segment.observer?.dispose()))
    )
    fs.rmSync(directory, { recursive: true, force: true })
  })

  test('selects the next segment before the close and connects in the close continuation', async () => {
    const segments = ordinary()
    const order = []
    const client = fakeClient(segments, order)
    const first = segments.current()
    const cycle = segments.cycle(client, { timeout: 3000 })
    expect(segments.current().id).toBe('producer-ep-1')
    expect(client.connect).not.toHaveBeenCalled()
    await Promise.all([cycle.closed, cycle.reopened])
    expect(order).toEqual(['disconnect:producer-ep-1', 'connect', 'immediate'])
    const notes = segments.journal()
    expect(notes.map((n) => n.type)).toEqual(['select', 'disconnect-resolved', 'connect-called'])
    expect(notes[2].seq).toBe(notes[1].seq + 1)
    expect(first.observer.socketOwnership()).toHaveLength(1)
    expect(cycle.to.observer.socketOwnership()).toHaveLength(1)
    expect(client.disconnect).toHaveBeenCalledWith({ timeout: 3000 })
  })

  test('refuses a third undischarged segment without touching the client', async () => {
    const segments = ordinary()
    const client = fakeClient(segments)
    const first = await rotate(segments, client)
    expect(segments.undischarged().map((s) => s.id)).toEqual(['producer-ep-0', 'producer-ep-1'])
    const calls = client.disconnect.mock.calls.length
    expect(() => segments.cycle(client)).toThrow('Two undischarged observer segments exist')
    expect(client.disconnect).toHaveBeenCalledTimes(calls)
    expect(segments.current().id).toBe('producer-ep-1')
    await segments.discharge(first.from)
    expect(segments.undischarged().map((s) => s.id)).toEqual(['producer-ep-1'])
    const second = await rotate(segments, client)
    expect(second.to.id).toBe('producer-ep-2')
  })

  test('refuses disposal while a segment is active or still owns a socket', async () => {
    const segments = ordinary()
    await expect(segments.discharge(segments.current())).rejects.toThrow('is still active')
    const lingering = open(segments)
    const cycle = segments.cycle({ disconnect: async () => {}, connect: async () => {} })
    await cycle.reopened
    const dispose = jest.spyOn(cycle.from.observer, 'dispose')
    await expect(segments.discharge(cycle.from, { closeMs: 20 })).rejects.toThrow(
      'still owns 1 open sockets'
    )
    expect(dispose).not.toHaveBeenCalled()
    lingering.emit('close')
    const pack = await segments.discharge(cycle.from)
    expect(pack.id).toBe('producer-ep-0')
    expect(cycle.from.observer).toBeNull()
    expect(await segments.discharge(cycle.from)).toBe(pack)
  })

  test('holds each kind to its fixed schedule and cap', async () => {
    expect(segmentCatalogue('telemetry-shutdown')).toHaveLength(88)
    expect(segmentCatalogue('telemetry-churn')).toHaveLength(28)
    const l5 = ordinary()
    const client = fakeClient(l5)
    for (let round = 1; round < SCHEDULES['telemetry-shutdown'].perKind; round++)
      await l5.discharge((await rotate(l5, client)).from)
    expect(() => l5.cycle(client)).toThrow('Segment cap of 21')
    expect(await l5.finish(client, { timeout: 3000 })).toHaveLength(21)
    const short = ordinary()
    await expect(short.finish(fakeClient(short))).rejects.toThrow('Expected 21 segments, found 1')
    const l9 = ordinary('telemetry-churn', 'consumer')
    const churn = fakeClient(l9)
    for (let round = 1; round < 16; round++) await l9.discharge((await rotate(l9, churn)).from)
    expect(() => l9.cycle(churn)).toThrow('Segment cap of 16')
    await expect(l9.finish(churn)).rejects.toThrow('Expected 7 segments, found 16')
  })

  test('seals exactly two files holding every payload and proof', async () => {
    const segments = ordinary()
    const client = fakeClient(segments)
    const [socket] = client.sockets
    const blob = Buffer.from('payload')
    fs.writeFileSync(
      path.join(directory, 'exports-1.jsonl'),
      JSON.stringify(row('1', 1, { dataBytes: blob.length, dataSha256: sha(blob) })) + '\n'
    )
    send(socket, get71(1))
    receive(socket, get71Response(1))
    send(socket, push72(2, blob))
    receive(socket, push72Response(2))
    send(socket, push72(3, Buffer.alloc(0)))
    receive(socket, push72Response(3))
    const observerDirectory = segments.current().observer.directory
    const { from } = await rotate(segments, client)
    const pack = await segments.discharge(from)
    expect(fs.readdirSync(path.join(directory, 'segments')).sort()).toEqual([
      'producer-ep-0.bundle.jsonl',
      'producer-ep-0.transcript.json',
    ])
    expect(fs.existsSync(observerDirectory)).toBe(false)
    expect(pack.accepted).toHaveLength(1)
    expect(pack.empty).toHaveLength(1)
    const files = (name) => fs.readFileSync(path.join(directory, 'segments', name))
    const input = () => ({
      id: 'producer-ep-0',
      transcript: files('producer-ep-0.transcript.json'),
      bundle: files('producer-ep-0.bundle.jsonl').toString('utf8'),
    })
    const checked = checkSegmentFiles(input())
    expect([...checked.pushes.keys()]).toHaveLength(2)
    expect(checked.pushes.get(pack.empty[0]).bytes).toBe(0)
    expect(checked.proofs.get(pack.accepted[0]).raw.equals(blob)).toBe(true)
    const lines = input().bundle.trim().split('\n')
    const without = (type) =>
      lines.filter(
        (line) => JSON.parse(line).t !== type || JSON.parse(line).key !== pack.accepted[0]
      )
    expect(() => checkSegmentFiles({ ...input(), bundle: without('proof').join('\n') })).toThrow(
      'Missing proof'
    )
    expect(() => checkSegmentFiles({ ...input(), id: 'producer-ep-1' })).toThrow('owner differs')
    const altered = lines.map((line) => {
      const item = JSON.parse(line)
      return item.t === 'push' && item.bytes === 0
        ? JSON.stringify({ ...item, b64: 'AA==', bytes: 1 })
        : line
    })
    expect(() => checkSegmentFiles({ ...input(), bundle: altered.join('\n') })).toThrow(
      'differs from its bundle entry'
    )
  })

  test('bounds the decoder children of every observer through one gate', async () => {
    const { DecoderGate } = require('../telemetryFixtures')
    const gate = new DecoderGate(2)
    let running = 0
    let peak = 0
    const task = async (n) => {
      running++
      peak = Math.max(peak, running)
      await new Promise((resolve) => setImmediate(resolve))
      running--
      return n
    }
    const results = await Promise.all([1, 2, 3, 4, 5].map((n) => gate.run(() => task(n))))
    expect(results).toEqual([1, 2, 3, 4, 5])
    expect(peak).toBe(2)
    await expect(
      gate.run(async () => {
        throw new Error('decoder failed')
      })
    ).rejects.toThrow('decoder failed')
    expect(gate.occupancy()).toEqual({ running: 0, queued: 0 })
    expect(() => new DecoderGate(0)).toThrow('positive decoder concurrency')
    const gated = new DecoderGate(1)
    const run = jest.spyOn(gated, 'run')
    const observer = new TelemetryWireObserver({
      directory,
      line: '4.3.1',
      clock,
      dial,
      decoder: stubDecoder,
      gate: gated,
    })
    try {
      await observer.decodeRaw(Buffer.from('gated'))
      expect(run).toHaveBeenCalledTimes(1)
    } finally {
      await observer.dispose()
    }
  })

  test('consumes each sink row once across observers that share a consumed set', async () => {
    const blob = Buffer.from('shared')
    fs.writeFileSync(
      path.join(directory, 'exports-1.jsonl'),
      JSON.stringify(row('1', 1, { dataBytes: blob.length, dataSha256: sha(blob) })) + '\n'
    )
    const consumed = new Set()
    const observers = [1, 2].map(
      () =>
        new TelemetryWireObserver({
          directory,
          line: '4.3.1',
          clock,
          dial,
          decoder: stubDecoder,
          consumed,
        })
    )
    try {
      const pushes = []
      for (const observer of observers) {
        const socket = observer.socketFactory({
          host: 'localhost',
          port: 19092,
          onConnect: jest.fn(),
        })
        socket.emit('connect')
        send(socket, push72(1, blob))
        receive(socket, push72Response(1))
        pushes.push(observer.accepted()[0])
      }
      expect((await observers[0].decodeAccepted(pushes[0])).exportIdentity).toEqual({
        node: '1',
        seq: 1,
      })
      expect([...consumed]).toEqual(['1:1'])
      await expect(observers[1].decodeAccepted(pushes[1], { sinkWaitMs: 60 })).rejects.toThrow(
        'No unconsumed sink record matches'
      )
    } finally {
      await Promise.allSettled(observers.map((observer) => observer.dispose()))
    }
  })
})

describe('telemetry profile', () => {
  const scratch = []
  const { OBSERVER_LIMITS } = require('../telemetryFixtures')
  const { SCHEDULES, TelemetrySegments, segmentCatalogue } = require('../telemetrySegments')
  const { buildShutdownModel, writeShutdownArchive } = require('../segmentArchiveFixture')
  const { verifyScenario } = require('../../scripts/e2e/telemetry/evidence')
  let directory
  let owners
  let dial
  const clock = (() => {
    let tick = 0
    return { now: () => ++tick }
  })()
  const segmentsFor = (scenario, kind) => {
    const segments = TelemetrySegments.ordinary(scenario, kind, {
      directory,
      clock,
      observer: { line: '4.3.1', dial, decoder: stubDecoder, mode: 'configured' },
    })
    owners.push(segments)
    return segments
  }
  const open = (segments) => {
    const socket = segments.socketFactory({ host: 'localhost', port: 19092, onConnect: jest.fn() })
    socket.emit('connect')
    return socket
  }
  const send = (socket, ...frames) => socket.write(Buffer.concat(frames))
  const receive = (socket, ...frames) => socket.emit('data', Buffer.concat(frames))
  const client = (segments, order = []) => {
    const fake = {
      sockets: [open(segments)],
      disconnect: jest.fn(async function disconnect() {
        order.push(`disconnect:${segments.current().id}`)
        for (const socket of this.sockets.splice(0)) socket.emit('close')
        setImmediate(() => order.push('immediate'))
      }),
      connect: jest.fn(function connect() {
        order.push('connect')
        this.sockets.push(open(segments))
        return Promise.resolve()
      }),
    }
    return fake
  }
  const rotate = async (segments, fake) => {
    const cycle = segments.cycle(fake)
    await Promise.all([cycle.closed, cycle.reopened])
    return cycle
  }
  const messages = (error) => [error.message, ...(error.errors ?? []).flatMap(messages)]
  const copyTree = (from, to) => {
    for (const entry of fs.readdirSync(from, { withFileTypes: true })) {
      const source = path.join(from, entry.name)
      const target = path.join(to, entry.name)
      if (entry.isDirectory()) {
        fs.mkdirSync(target)
        copyTree(source, target)
      } else fs.copyFileSync(source, target)
    }
  }
  const healthy = (observer) => {
    observer.assertHealthy()
  }

  beforeEach(() => {
    directory = fs.mkdtempSync(path.join(os.tmpdir(), 'telemetry-profile-'))
    owners = []
    dial = jest.fn(({ port }) => fakeSocket(port))
  })
  afterEach(async () => {
    await Promise.allSettled(
      owners.flatMap((owner) => owner.undischarged().map((segment) => segment.observer?.dispose()))
    )
    for (const root of [directory, ...scratch.splice(0)])
      fs.rmSync(root, { recursive: true, force: true })
  })

  test('keeps observation segments bounded', async () => {
    expect(OBSERVER_LIMITS).toMatchObject({ sockets: 64, records: 4096, retained: 2 * 1024 * 1024 })

    // The three observer caps are exact: the last allowed item passes, the next one latches.
    const sockets = segmentsFor('telemetry-shutdown', 'producer')
    const crowded = sockets.current().observer
    const held = Array.from({ length: 64 }, () => open(sockets))
    expect(() => healthy(crowded)).not.toThrow()
    held.push(open(sockets))
    expect(() => healthy(crowded)).toThrow('Observer socket bound exceeded')
    for (const socket of held) socket.emit('close')
    const refused = await sockets.discharge(sockets.current(), { force: true }).catch((e) => e)
    expect(messages(refused)).toContain('Observer socket bound exceeded')

    const records = segmentsFor('telemetry-shutdown', 'consumer')
    const wire = open(records)
    for (let index = 1; index <= 4095; index++) {
      send(wire, metadata(index))
      receive(wire, metadataResponse(index))
    }
    expect(records.current().observer.checkpoint()).toBe(4096)
    expect(() => healthy(records.current().observer)).not.toThrow()
    send(wire, metadata(4096))
    expect(() => healthy(records.current().observer)).toThrow('Observer record bound exceeded')

    const bytes = segmentsFor('telemetry-shutdown', 'admin')
    const heavy = open(bytes)
    const big = Buffer.alloc(4096)
    for (let index = 1; index <= 504; index++) {
      send(heavy, push72(index, big))
      receive(heavy, push72Response(index, 42))
    }
    expect(() => healthy(bytes.current().observer)).not.toThrow()
    send(heavy, push72(505, big))
    expect(() => healthy(bytes.current().observer)).toThrow('Observer retained-byte bound exceeded')

    // Routing precedes the close, connect follows the close continuation, and the limits hold.
    const l9 = segmentsFor('telemetry-churn', 'producer')
    const order = []
    const fake = client(l9, order)
    const cycle = l9.cycle(fake)
    expect(l9.current().id).toBe('producer-lc-1')
    await Promise.all([cycle.closed, cycle.reopened])
    expect(order).toEqual(['disconnect:producer-lc-1', 'connect', 'immediate'])
    expect(() => l9.cycle(fake)).toThrow('Two undischarged observer segments exist')
    await expect(l9.discharge(l9.current())).rejects.toThrow('is still active')
    await l9.discharge(cycle.from)
    for (let round = 2; round < 16; round++) await l9.discharge((await rotate(l9, fake)).from)
    expect(() => l9.cycle(fake)).toThrow('Segment cap of 16')
    expect(SCHEDULES['telemetry-churn'].perKind).toBe(7)
    await expect(l9.finish(fake)).rejects.toThrow('Expected 7 segments, found 16')

    // The fixed catalogues, then a complete archive that proves them.
    const catalogue = segmentCatalogue('telemetry-shutdown')
    expect(catalogue).toHaveLength(88)
    for (const kind of ['producer', 'consumer', 'admin', 'share'])
      expect(catalogue.filter((id) => id.startsWith(`${kind}-ep-`))).toHaveLength(21)
    expect(catalogue.filter((id) => id.endsWith('-t0') || id.endsWith('-bh'))).toHaveLength(4)
    const model = buildShutdownModel()
    const archive = writeShutdownArchive(model)
    scratch.push(archive.directory)
    const verify = ({ record, directory: root }) => verifyScenario(record, root)
    expect(() => verify(archive)).not.toThrow()
    const keyed = (id) =>
      model.segments.get(id).transcript.records.filter((r) => r.type === 'request')[0].requestKey
    expect(keyed('producer-ep-3')).toBe(keyed('producer-ep-4'))
    expect(archive.record.sidecars.files.length).toBeLessThanOrEqual(512)

    const copies = []
    const copy = () => {
      const root = fs.mkdtempSync(path.join(os.tmpdir(), 'telemetry-profile-copy-'))
      copies.push(root)
      copyTree(archive.directory, root)
      return { directory: root, record: JSON.parse(JSON.stringify(archive.record)) }
    }
    const rewrite = (target, suffix, transform) => {
      const item = target.record.sidecars.files.find((f) => f.path.endsWith(suffix))
      const file = path.join(target.directory, item.path)
      const next = Buffer.from(transform(fs.readFileSync(file)))
      fs.writeFileSync(file, next)
      item.bytes = next.length
      item.sha256 = sha(next)
    }
    const json = (target, suffix, change) =>
      rewrite(target, suffix, (bytes) => JSON.stringify(change(JSON.parse(bytes)) ?? undefined))
    const edit = (target, id, part, change) => {
      const suffix = part === 'transcript' ? `${id}.transcript.json` : `${id}.bundle.jsonl`
      if (part === 'transcript')
        rewrite(target, suffix, (bytes) => {
          const value = JSON.parse(bytes)
          change(value)
          return JSON.stringify(value)
        })
      else
        rewrite(target, suffix, (bytes) =>
          change(
            bytes
              .toString('utf8')
              .split('\n')
              .filter(Boolean)
              .map((line) => JSON.parse(line))
          )
            .map((item) => JSON.stringify(item) + '\n')
            .join('')
        )
    }
    const entries = (target, change) => {
      change(target.record.sidecars.segments)
      json(target, '/index.json', (index) => {
        index.segments = target.record.sidecars.segments
        return index
      })
    }
    const accepted = (id) =>
      model.segments
        .get(id)
        .transcript.records.filter((r) => r.apiKey === 72 && r.response?.errorCode === 0)
    const cases = [
      [
        'a missing segment',
        'Required segment catalogue differs',
        (t) =>
          entries(t, (list) =>
            list.splice(
              list.findIndex((e) => e.id === 'producer-ep-7'),
              1
            )
          ),
      ],
      [
        'an extra segment',
        'Required segment catalogue differs',
        (t) =>
          entries(t, (list) => {
            list.push({ ...list.find((e) => e.id === 'producer-ep-3'), id: 'producer-ep-21' })
          }),
      ],
      [
        'a swapped segment file',
        'Segment owner differs for producer-ep-3',
        (t) => {
          const other = fs.readFileSync(
            path.join(t.directory, t.record.sidecars.root, 'segments/producer-ep-4.transcript.json')
          )
          rewrite(t, 'producer-ep-3.transcript.json', () => other)
        },
      ],
      [
        'a reused segment path',
        'Segment path does not belong to its identity',
        (t) =>
          entries(t, (list) => {
            const [a, b] = ['producer-ep-5', 'producer-ep-6'].map((id) =>
              list.find((e) => e.id === id)
            )
            b.bundle = a.bundle
          }),
      ],
      [
        'a missing proof',
        'Missing proof',
        (t) => {
          let dropped = false
          edit(t, 'producer-ep-5', 'bundle', (lines) =>
            lines.filter((item) => item.t !== 'proof' || dropped || !(dropped = true))
          )
        },
      ],
      [
        'an altered payload',
        'differs from its bundle entry',
        (t) =>
          edit(t, 'consumer-ep-2', 'bundle', (lines) =>
            lines.map((item) => (item.t === 'push' ? { ...item, b64: 'AA==' } : item))
          ),
      ],
      [
        'an extra payload',
        'payload catalogue differs from its requests',
        (t) =>
          edit(t, 'admin-ep-2', 'bundle', (lines) => [
            ...lines,
            { ...lines.find((item) => item.t === 'push'), key: 'r999' },
          ]),
      ],
      [
        'a reused sink row',
        'Reused sink record',
        (t) => {
          const taken = accepted('producer-ep-1')[0]
          const source = model.segments.get('producer-ep-1').decoded.get(taken.requestKey)
          edit(t, 'share-ep-1', 'bundle', (lines) =>
            lines.map((item) =>
              item.t === 'proof' && item.key === accepted('share-ep-1')[0].requestKey
                ? { ...item, decoded: { ...item.decoded, exportIdentity: source.exportIdentity } }
                : item
            )
          )
        },
      ],
      [
        'a truncated sink',
        'Missing sink record',
        (t) =>
          rewrite(
            t,
            'exports-1.jsonl',
            (bytes) => bytes.toString('utf8').trimEnd().split('\n').slice(0, -1).join('\n') + '\n'
          ),
      ],
      [
        'a reset sink cursor',
        'Telemetry sink sequence or node mismatch',
        (t) =>
          rewrite(
            t,
            'exports-1.jsonl',
            (bytes) =>
              bytes
                .toString('utf8')
                .trimEnd()
                .split('\n')
                .map((line, index) => (index === 2 ? line.replace('"seq":3', '"seq":1') : line))
                .join('\n') + '\n'
          ),
      ],
      [
        'a missing final',
        'Expected exactly one final at close',
        (t) => {
          const [final] = accepted('producer-ep-2').filter((r) => r.terminating)
          edit(t, 'producer-ep-2', 'transcript', (value) => {
            value.records = value.records.filter((r) => r.requestKey !== final.requestKey)
          })
          edit(t, 'producer-ep-2', 'bundle', (lines) =>
            lines.filter((item) => item.key !== final.requestKey)
          )
          t.record.pushes = t.record.pushes.filter(
            (p) => !(p.owner === 'producer-ep-2' && p.requestKey === final.requestKey)
          )
        },
      ],
      [
        'a swapped round join',
        'Round segments differ from the schedule',
        (t) => {
          t.record.results.rounds.producer[4].to = 'producer-ep-9'
        },
      ],
      [
        'a connect that was not the next public action',
        'Connect was not the next public action',
        (t) => {
          t.record.results.journal.admin[2].seq += 1
        },
      ],
      [
        'a segment selected after the close resolved',
        'The next segment was not selected before the close resolved',
        (t) => {
          const notes = t.record.results.journal.share
          ;[notes[0].seq, notes[1].seq] = [notes[1].seq, notes[0].seq]
        },
      ],
      [
        'a later cache miss in the episode',
        'Expected one exact connection retirement, found 0',
        (t) =>
          json(t, 'oracles.json', (oracles) => {
            const { chunks } = oracles.traces
            const id = /Removing client connection id \[([^\]]+)\]/.exec(
              chunks[1].logs['1'].text
            )[1]
            chunks.at(-1).logs['1'].text +=
              `\nTRACE Client connection id [${id}] is not found in the client instance cache`
            return oracles
          }),
      ],
      [
        'an overspent cleanup',
        'Cleanup exceeded its budget',
        (t) => {
          t.record.timing.cleanupMs = 20001
        },
      ],
      [
        'an overspent work deadline',
        'The work exceeded its deadline',
        (t) => {
          t.record.results.workMs = 98001
        },
      ],
      [
        'a surviving event-loop resource',
        'Quiet resources differ',
        (t) => {
          t.record.results.resources.final.activeResources = { Timeout: 3 }
        },
      ],
      [
        'an omitted resource source',
        'Required resource source catalogue differs',
        (t) => {
          delete t.record.results.resources.baseline.identities.openSockets
        },
      ],
      [
        'a missing format version',
        'Missing segmented evidence format',
        (t) => {
          delete t.record.sidecars.format
        },
      ],
      [
        'more files than the archive allows',
        'Invalid sidecar count',
        (t) => {
          t.record.sidecars.files = [
            ...t.record.sidecars.files,
            ...Array.from({ length: 400 }, () => t.record.sidecars.files[0]),
          ]
        },
      ],
    ]
    try {
      for (const [name, diagnostic, change] of cases) {
        const target = copy()
        change(target)
        let thrown
        try {
          verify(target)
        } catch (error) {
          thrown = error
        }
        expect({ name, message: thrown?.message }).toEqual({
          name,
          message: expect.stringContaining(diagnostic),
        })
      }
    } finally {
      for (const root of copies) fs.rmSync(root, { recursive: true, force: true })
    }

    // A sink that shrinks below a held cursor is refused by the reader; overflow fails the archive.
    const sink = path.join(directory, 'exports-1.jsonl')
    fs.writeFileSync(
      sink,
      [row('1', 1), row('1', 2), row('1', 3)].map((r) => JSON.stringify(r) + '\n').join('')
    )
    const reader = new TelemetryExportReader(directory)
    const cursor = reader.checkpoint()
    expect(cursor[1]).toBe(3)
    fs.writeFileSync(sink, JSON.stringify(row('1', 1)) + '\n')
    expect(() => reader.readExports({ after: cursor })).toThrow('replaced after cursor')
    const bloated = buildShutdownModel()
    bloated.padding = 17 * 1024 * 1024
    const overflow = fs.mkdtempSync(path.join(os.tmpdir(), 'telemetry-profile-overflow-'))
    scratch.push(overflow)
    expect(() => writeShutdownArchive(bloated, { directory: overflow })).toThrow(
      'Sidecar archive exceeds bound'
    )
    expect(SCHEDULES['telemetry-shutdown'].cap).toBe(21)
  }, 180000)
})
