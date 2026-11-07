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

describe('telemetry wire observer', () => {
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
