const zlib = require('zlib')
const gzip = require('../../protocol/message/compression/gzip')
const zstd = require('../../protocol/message/compression/zstd')
const TelemetryMetrics = require('../metrics')
const TelemetryPayload = require('../payload')
const { encodeMetricsData, measureMetricsData } = require('../otlp')

/** Builds an exact raw size through legal registration, with no synthetic sender metadata. */
const sample = (target = 1100) => {
  let tick = 1n
  const collector = new TelemetryMetrics({
    kind: 'producer',
    clock: { wallMs: () => 1, monotonicNs: () => tick++ },
    logger: {},
  })
  const sub = { revision: 1n, requestedMetrics: ['*'], deltaTemporality: true }
  collector.select(sub)
  for (let fill = Math.max(0, target - 260); fill <= 1024; fill++) {
    const attrs = {}
    let left = fill
    for (const key of ['a', 'b', 'c', 'd']) {
      const size = Math.min(256, left)
      attrs[key] = 'x'.repeat(size)
      left -= size
    }
    collector.register({ name: 'com.example.payload', type: 'gauge', value: -1, attributes: attrs })
    const snapshot = collector.snapshot(sub)
    const size = measureMetricsData(snapshot)
    if (size === target) return { snapshot, collector, sub }
    collector.abandon(snapshot)
    collector.unregister({ name: 'com.example.payload', attributes: attrs })
    if (size > target) break
  }
  throw new Error('No reachable raw size')
}
const opts = (acceptedCompressionTypes, cap = 4096) => ({
  acceptedCompressionTypes,
  telemetryMaxBytes: cap,
  brokerMaxBytes: cap,
  isCurrent: () => true,
})

afterEach(() => jest.restoreAllMocks())

describe('broker ordered built in compression', () => {
  test.each([
    [[], 0],
    [[99, 2, 3], 0],
    [[1, 4], 1],
    [[4, 1], 4],
    [[0, 4, 1], 4],
    [[-1, 4, 4, 1], 4],
    [[3, 1, 1, 4], 1],
  ])('preference list %j selects codec %s', async (accepted, expected) => {
    jest.replaceProperty(zstd, 'isSupported', true)
    const g = jest.spyOn(gzip, 'compress').mockResolvedValue(Buffer.from('gzip'))
    const z = jest.spyOn(zstd, 'compress').mockResolvedValue(Buffer.from('zstd'))
    const { snapshot } = sample()
    const p = await new TelemetryPayload(1n).buildPayload(snapshot, opts(accepted))
    expect(p.compressionType).toBe(expected)
    expect(g).toHaveBeenCalledTimes(expected === 1 ? 1 : 0)
    expect(z).toHaveBeenCalledTimes(expected === 4 ? 1 : 0)
    if (expected)
      expect((expected === 1 ? g : z).mock.calls[0][0].buffer).toEqual(encodeMetricsData(snapshot))
  })

  test('1000 bytes use NONE and 1001 bytes can use GZIP', async () => {
    const compress = jest.spyOn(gzip, 'compress').mockResolvedValue(Buffer.from('ok'))
    const p = await new TelemetryPayload(1n).buildPayload(sample(1000).snapshot, opts([1]))
    expect(p.rawBytes).toBe(1000)
    expect(p.compressionType).toBe(0)
    expect(compress).not.toHaveBeenCalled()
    const q = await new TelemetryPayload(1n).buildPayload(sample(1001).snapshot, opts([1]))
    expect(q.rawBytes).toBe(1001)
    expect(q.compressionType).toBe(1)
    expect(compress).toHaveBeenCalledTimes(1)
  })

  test('unavailable ZSTD falls through without invoking it', async () => {
    jest.replaceProperty(zstd, 'isSupported', false)
    const z = jest.spyOn(zstd, 'compress')
    const g = jest.spyOn(gzip, 'compress').mockResolvedValue(Buffer.from('ok'))
    expect(
      (await new TelemetryPayload(1n).buildPayload(sample().snapshot, opts([4, 4, 1])))
        .compressionType
    ).toBe(1)
    expect(z).not.toHaveBeenCalled()
    expect(g).toHaveBeenCalledTimes(1)
  })

  test('codec failure is suppressed until a new transport and duplicates do not retry it', async () => {
    jest.replaceProperty(zstd, 'isSupported', true)
    const z = jest.spyOn(zstd, 'compress').mockRejectedValue(new Error('private details'))
    jest.spyOn(gzip, 'compress').mockResolvedValue(Buffer.from('ok'))
    const builder = new TelemetryPayload(1n)
    const s = sample().snapshot
    expect((await builder.buildPayload(s, opts([4, 4, 1]))).compressionType).toBe(1)
    expect((await builder.buildPayload(s, opts([4, 1]))).compressionType).toBe(1)
    expect(z).toHaveBeenCalledTimes(1)
    builder.resetTransport(2n)
    expect((await builder.buildPayload(s, opts([4, 1]))).compressionType).toBe(1)
    expect(z).toHaveBeenCalledTimes(2)
  })

  test('failed preferred GZIP falls through to available ZSTD', async () => {
    jest.replaceProperty(zstd, 'isSupported', true)
    const g = jest.spyOn(gzip, 'compress').mockRejectedValue(new Error('codec'))
    const z = jest.spyOn(zstd, 'compress').mockResolvedValue(Buffer.from('ok'))
    const p = await new TelemetryPayload(1n).buildPayload(sample().snapshot, opts([1, 1, 4]))
    expect(p.compressionType).toBe(4)
    expect(p.codecAttempts).toBe(2)
    expect(g).toHaveBeenCalledTimes(1)
    expect(z).toHaveBeenCalledTimes(1)
  })

  test('all failed codecs return bounded NONE and do not retain the thrown value', async () => {
    jest.replaceProperty(zstd, 'isSupported', true)
    jest.spyOn(zstd, 'compress').mockRejectedValue({
      get message() {
        throw new Error('read')
      },
    })
    jest.spyOn(gzip, 'compress').mockRejectedValue(null)
    const s = sample().snapshot
    const p = await new TelemetryPayload(1n).buildPayload(s, opts([4, 1]))
    expect(p.compressionType).toBe(0)
    expect(p.metrics).toEqual(encodeMetricsData(s))
    expect(p.codecAttempts).toBe(2)
  })

  test('compressed expansion beyond the cap falls back to the fitting raw body', async () => {
    jest.spyOn(gzip, 'compress').mockResolvedValue(Buffer.alloc(1101))
    const s = sample().snapshot
    const p = await new TelemetryPayload(1n).buildPayload(s, opts([1], 1100))
    expect(p).toMatchObject({ compressionType: 0, rawBytes: 1100, transmittedBytes: 1100 })
    expect(p.metrics).toEqual(encodeMetricsData(s))
  })

  test('compressible raw-over-cap data is fitted before any codec call', async () => {
    const compress = jest.spyOn(gzip, 'compress').mockResolvedValue(Buffer.alloc(10))
    const s = sample().snapshot
    const p = await new TelemetryPayload(1n).buildPayload(s, opts([1], 1099))
    expect(p.rawBytes).toBeLessThanOrEqual(1099)
    expect(p.transmittedBytes).toBeLessThanOrEqual(1099)
    expect(p.includedIdentities).toHaveLength(1)
    expect(p.omittedIdentities).toHaveLength(1)
    expect(compress).not.toHaveBeenCalled()
  })

  test('native completion after replacement retains one job shell and cannot suppress the new codec', async () => {
    let reject
    const compress = jest
      .spyOn(gzip, 'compress')
      .mockImplementationOnce(
        () =>
          new Promise((resolve, fail) => {
            reject = fail
          })
      )
      .mockResolvedValue(Buffer.from('ok'))
    const s = sample().snapshot
    const builder = new TelemetryPayload(1n)
    const old = builder.buildPayload(s, opts([1]))
    builder.retire()
    builder.resetTransport(2n)
    expect(await builder.buildPayload(s, opts([1]))).toBeNull()
    expect(compress).toHaveBeenCalledTimes(1)
    reject(new Error('late'))
    expect(await old).toBeNull()
    expect((await builder.buildPayload(s, opts([1]))).compressionType).toBe(1)
    expect(compress).toHaveBeenCalledTimes(2)
  })

  test('stale successful completion produces no payload or collector update', async () => {
    let release
    jest.spyOn(gzip, 'compress').mockImplementationOnce(
      () =>
        new Promise((resolve) => {
          release = resolve
        })
    )
    const { snapshot, collector } = sample()
    let current = true
    const old = new TelemetryPayload(1n).buildPayload(snapshot, {
      ...opts([1]),
      isCurrent: () => current,
    })
    current = false
    release(Buffer.from('old'))
    expect(await old).toBeNull()
    expect(collector.isCurrent(snapshot)).toBe(true)
  })

  test('real GZIP is independently decompressed and its protobuf bytes match', async () => {
    const s = sample().snapshot
    const p = await new TelemetryPayload(1n).buildPayload(s, opts([1]))
    expect(p.compressionType).toBe(1)
    expect(zlib.gunzipSync(p.metrics)).toEqual(encodeMetricsData(s))
    expect(p.transmittedBytes).toBe(p.metrics.length)
    expect(p.rawBytes).toBe(1100)
  })

  test('producer registry callbacks are neither invoked nor replaced', async () => {
    const { Codecs } = require('../../protocol/message/compression')
    const previous = Codecs[1]
    const custom = jest.fn(() => {
      throw new Error('application callback')
    })
    Codecs[1] = custom
    try {
      const p = await new TelemetryPayload(1n).buildPayload(sample().snapshot, opts([1]))
      expect(p.compressionType).toBe(1)
      expect(custom).not.toHaveBeenCalled()
      expect(Codecs[1]).toBe(custom)
    } finally {
      Codecs[1] = previous
    }
  })

  test('detected native ZSTD is independently decompressed or uses the unavailable path', async () => {
    const s = sample().snapshot
    const p = await new TelemetryPayload(1n).buildPayload(s, opts([4]))
    if (zstd.isSupported) {
      expect(p.compressionType).toBe(4)
      // eslint-disable-next-line n/no-unsupported-features/node-builtins -- guarded by codec feature detection
      expect(zlib.zstdDecompressSync(p.metrics)).toEqual(encodeMetricsData(s))
    } else {
      expect(p.compressionType).toBe(0)
      expect(p.metrics).toEqual(encodeMetricsData(s))
    }
  })
})
