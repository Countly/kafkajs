const TelemetryMetrics = require('../metrics')
const { KafkaJSInvalidConfigurationError } = require('../../errors')

/** @param {object|null} logger diagnostic sink @returns {object} exact-time collector fixture */
const fixture = (logger = null) => {
  let time = 0n
  let revision = 0n
  const metrics = new TelemetryMetrics({
    kind: 'producer',
    logger,
    clock: { wallMs: () => 0, monotonicNs: () => time },
  })
  return {
    metrics,
    tick: (n) => {
      time += n
    },
    select: (deltaTemporality = true, requestedMetrics = ['*']) => {
      const s = { revision: ++revision, deltaTemporality, requestedMetrics }
      metrics.select(s)
      return s
    },
  }
}

/** @param {object} snapshot sample @returns {Array} issued point identities */
const ids = (snapshot) => snapshot.points.map((p) => p.identity)

/** @param {object} snapshot sample @returns {object} connection point */
const sum = (snapshot) => snapshot.points.find((p) => p.name.endsWith('connection.creation.total'))

describe('written metric baselines preserve exact temporality', () => {
  test('first delta is exact and unsent abandonment advances nothing', () => {
    const { metrics, tick, select } = fixture()
    const s = select()
    metrics.register({ name: 'com.test.count', type: 'sum', value: 9007199254740993n })
    tick(9n)
    const first = metrics.snapshot(s)
    expect(first.points.find((p) => p.name === 'com.test.count')).toMatchObject({
      value: 9007199254740993n,
      aggregationTemporality: 1,
      startTimeUnixNano: 0n,
      timeUnixNano: 9n,
    })
    metrics.abandon(first)
    const again = metrics.snapshot(s)
    expect(again.points.find((p) => p.name === 'com.test.count').value).toBe(9007199254740993n)
    metrics.written(again, ids(again))
    metrics.register({ name: 'com.test.count', type: 'sum', value: 9007199254740994n })
    expect(metrics.snapshot(s).points.find((p) => p.name === 'com.test.count').value).toBe(1n)
  })

  test('written response loss or rejection does not replay a delta', () => {
    const { metrics, select } = fixture()
    const s = select()
    metrics.observeConnection({})
    const first = metrics.snapshot(s)
    metrics.written(first, ids(first))
    metrics.written(first, ids(first))
    metrics.abandon(first)
    metrics.observeConnection({})
    expect(sum(metrics.snapshot(s)).value).toBe(1n)
  })

  test('omitted sums and individual gauge points retain their windows', () => {
    const { metrics, select } = fixture()
    const s = select()
    const ref = metrics.bindNode({ nodeId: 1, endpoint: 'node', endpointRevision: 1 })
    metrics.observeConnection({})
    metrics.observeRequest(ref, 8)
    const first = metrics.snapshot(s)
    metrics.observeRequest(ref, 2)
    metrics.written(
      first,
      first.points.filter((p) => p.name.endsWith('.avg')).map((p) => p.identity)
    )
    const second = metrics.snapshot(s)
    expect(second.points.map((p) => p.value)).toEqual([1n, 2, 8])
  })

  test('abandon merges frozen windows while later samples continue', () => {
    const { metrics, select } = fixture()
    const s = select()
    const ref = metrics.bindNode({ nodeId: 1, endpoint: 'node', endpointRevision: 1 })
    metrics.observeRequest(ref, 8)
    const first = metrics.snapshot(s)
    expect(() => metrics.snapshot(s)).toThrow(KafkaJSInvalidConfigurationError)
    metrics.observeRequest(ref, 2)
    metrics.abandon(first)
    metrics.abandon(first)
    expect(metrics.snapshot(s).points.map((p) => p.value)).toEqual([0n, 5, 8])
  })

  test.each(['written', 'abandon'])(
    'defers overflow diagnostics and warns once per subscription revision on %s',
    async (commit) => {
      const logger = {
        warn: jest.fn(() => {
          throw new Error('hostile diagnostic')
        }),
      }
      const { metrics, select } = fixture(logger)
      let subscription = select(true, [
        'org.apache.kafka.producer.connection.creation.total',
        'org.apache.kafka.producer.node.request.latency.avg',
      ])
      /** @param {number} nodeId fresh slot @returns {void} overflow only while settling the snapshot */
      const overflow = (nodeId) => {
        const ref = metrics.bindNode({ nodeId, endpoint: String(nodeId), endpointRevision: 1 })
        metrics.observeRequest(ref, 1)
        const sample = metrics.snapshot(subscription)
        metrics.observeRequest(ref, Number.MAX_SAFE_INTEGER)
        if (commit === 'written') metrics.written(sample, [sum(sample).identity])
        else metrics.abandon(sample)
        expect(metrics.isCurrent(sample)).toBe(false)
      }
      overflow(1)
      expect(logger.warn).not.toHaveBeenCalled()
      await Promise.resolve()
      expect(logger.warn).toHaveBeenCalledTimes(1)
      overflow(2)
      await Promise.resolve()
      expect(logger.warn).toHaveBeenCalledTimes(1)
      subscription = select(true, subscription.requestedMetrics)
      overflow(3)
      expect(logger.warn).toHaveBeenCalledTimes(1)
      await Promise.resolve()
      expect(logger.warn).toHaveBeenCalledTimes(2)
    }
  )

  test('cumulative delta cumulative switches occur at accepted boundaries', () => {
    const { metrics, select, tick } = fixture()
    let s = select(false)
    metrics.observeConnection({})
    let sample = metrics.snapshot(s)
    expect(sum(sample)).toMatchObject({
      value: 1n,
      aggregationTemporality: 2,
      startTimeUnixNano: 0n,
    })
    metrics.written(sample, ids(sample))
    tick(10n)
    s = select(true)
    metrics.observeConnection({})
    sample = metrics.snapshot(s)
    expect(sum(sample)).toMatchObject({
      value: 1n,
      aggregationTemporality: 1,
      startTimeUnixNano: 10n,
    })
    metrics.written(sample, ids(sample))
    s = select(true)
    metrics.observeConnection({})
    sample = metrics.snapshot(s)
    expect(sum(sample).value).toBe(1n)
    metrics.written(sample, ids(sample))
    s = select(false)
    expect(sum(metrics.snapshot(s))).toMatchObject({
      value: 3n,
      aggregationTemporality: 2,
      startTimeUnixNano: 0n,
    })
  })

  test('removed and re-added selection has a new revision despite equal clocks', () => {
    const { metrics, select } = fixture()
    let s = select()
    metrics.observeConnection({})
    const old = metrics.snapshot(s)
    select(true, [])
    s = select()
    metrics.written(old, ids(old))
    expect(metrics.isCurrent(old)).toBe(false)
    metrics.observeConnection({})
    const current = metrics.snapshot(s)
    expect(sum(current).identity.revision).not.toBe(sum(old).identity.revision)
    expect(sum(current).value).toBe(1n)
  })

  test('unregister then equal-valued re-registration cannot inherit an old write', () => {
    const { metrics, select } = fixture()
    const s = select()
    const metric = { name: 'com.test.count', type: 'sum', value: 9n }
    metrics.register(metric)
    const old = metrics.snapshot(s)
    metrics.unregister(metric)
    expect(metrics.isCurrent(old)).toBe(false)
    metrics.register(metric)
    metrics.written(old, ids(old))
    const current = metrics.snapshot(s)
    expect(current.points.find((p) => p.name === metric.name).value).toBe(9n)
    expect(current.points.find((p) => p.name === metric.name).identity.revision).not.toBe(
      old.points.find((p) => p.name === metric.name).identity.revision
    )
  })

  test('clock anchoring ignores wall changes and clamps monotonic reversal', () => {
    let wall = 1000
    let mono = 10n
    const metrics = new TelemetryMetrics({
      kind: 'producer',
      clock: { wallMs: () => wall, monotonicNs: () => mono },
    })
    const s = { revision: 1n, requestedMetrics: ['*'], deltaTemporality: true }
    metrics.select(s)
    wall = 1
    mono = 15n
    const first = metrics.snapshot(s)
    expect(sum(first).timeUnixNano).toBe(1000000005n)
    metrics.abandon(first)
    mono = 0n
    expect(sum(metrics.snapshot(s)).timeUnixNano).toBe(1000000005n)
  })

  test('signed int64 Sum and unsigned uint64 timestamp domains are separate', () => {
    const { metrics, tick, select } = fixture()
    const s = select()
    const max = 9223372036854775807n
    metrics.register({ name: 'com.test.boundary', type: 'sum', value: max })
    tick(18446744073709551615n)
    const sample = metrics.snapshot(s)
    expect(sample.points.find((p) => p.name === 'com.test.boundary')).toMatchObject({
      value: max,
      timeUnixNano: 18446744073709551615n,
    })
    metrics.abandon(sample)
    tick(1n)
    expect(() => metrics.snapshot(s)).toThrow(KafkaJSInvalidConfigurationError)
  })

  test('close releases transient windows but keeps application totals and descriptions', () => {
    const { metrics, select } = fixture()
    let s = select()
    metrics.register({ name: 'com.test.keep', type: 'sum', value: 7n })
    const old = metrics.snapshot(s)
    metrics.resetTransient()
    expect(metrics.isCurrent(old)).toBe(false)
    s = select()
    expect(metrics.snapshot(s).points.find((p) => p.name === 'com.test.keep').value).toBe(7n)
  })
  test('suspension retires old slots before a later selection resumes observation', () => {
    const { metrics, select } = fixture()
    select()
    const old = metrics.bindNode({ nodeId: 1, endpoint: 'same', endpointRevision: 1 })
    metrics.resetTransient()
    const s = select()
    metrics.observeRequest(old, 999)
    const current = metrics.bindNode({ nodeId: 1, endpoint: 'same', endpointRevision: 1 })
    metrics.observeRequest(current, 3)
    expect(
      metrics
        .snapshot(s)
        .points.filter((p) => p.type === 'gauge')
        .map((p) => p.value)
    ).toEqual([3, 3])
  })

  test('obsolete write cannot consume a replacement snapshot', () => {
    const { metrics, select } = fixture()
    let s = select()
    metrics.observeConnection({})
    const old = metrics.snapshot(s)
    select(true, [])
    s = select()
    metrics.observeConnection({})
    const current = metrics.snapshot(s)
    metrics.written(old, ids(old))
    expect(metrics.isCurrent(current)).toBe(true)
    metrics.written(current, ids(current))
    expect(sum(metrics.snapshot(s)).value).toBe(0n)
  })
})
