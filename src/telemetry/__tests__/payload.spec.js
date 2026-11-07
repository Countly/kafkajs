const TelemetryMetrics = require('../metrics')
const TelemetryPayload = require('../payload')
const { encodeMetricsData, measureMetricsData } = require('../otlp')

const make = () => {
  let now = 1000n
  const collector = new TelemetryMetrics({
    kind: 'producer',
    clock: { wallMs: () => 1, monotonicNs: () => now++ },
    logger: {},
  })
  const subscription = { revision: 1n, requestedMetrics: ['*'], deltaTemporality: true }
  collector.select(subscription)
  return { collector, subscription }
}
const options = (cap = 1048576) => ({
  telemetryMaxBytes: cap,
  brokerMaxBytes: cap,
  isCurrent: () => true,
})

describe('whole-point payload fitting', () => {
  test('an empty sample produces a zero blob without resource-only bytes', async () => {
    const { collector, subscription } = make()
    collector.select({ ...subscription, revision: 2n, requestedMetrics: [] })
    const s = collector.snapshot(
      { ...subscription, revision: 2n, requestedMetrics: [] },
      { group_id: 'g' }
    )
    const p = await new TelemetryPayload(1n).buildPayload(s, options(1))
    expect(p).toMatchObject({
      metrics: Buffer.alloc(0),
      compressionType: 0,
      rawBytes: 0,
      transmittedBytes: 0,
      fittingPasses: 1,
      includedIdentities: [],
    })
  })

  test('exact raw boundary retains a point and one byte less omits it intact', async () => {
    const { collector, subscription } = make()
    const s = collector.snapshot(subscription)
    const cap = measureMetricsData(s)
    const full = await new TelemetryPayload(1n).buildPayload(s, options(cap))
    expect(full.metrics).toEqual(encodeMetricsData(s))
    expect(full.includedIdentities).toEqual(s.points.map((p) => p.identity))
    const tiny = await new TelemetryPayload(1n).buildPayload(s, options(cap - 1))
    expect(tiny.metrics.length).toBe(0)
    expect(tiny.omittedIdentities).toEqual(s.points.map((p) => p.identity))
    expect(collector.isCurrent(s)).toBe(true)
  })

  test('connection sum precedes numeric node pairs and canonical custom identities', async () => {
    const { collector, subscription } = make()
    for (const id of [10, 2]) {
      const slot = collector.bindNode({ nodeId: id, endpoint: `b${id}`, endpointRevision: {} })
      collector.observeRequest(slot, id)
    }
    for (const name of ['com.example.z', 'com.example.a'])
      collector.register({ name, type: 'gauge', value: 7 })
    const s = collector.snapshot(subscription)
    const preferred = s.points.filter(
      (p) => p.type === 'sum' || p.attributes.some((a) => a.key === 'node_id' && a.value === '2')
    )
    const cap = measureMetricsData(s, preferred)
    const p = await new TelemetryPayload(1n).buildPayload(s, options(cap))
    expect(p.includedIdentities).toEqual(expect.arrayContaining(preferred.map((p) => p.identity)))
    expect(p.includedIdentities).toHaveLength(3)
    expect(p.rawBytes).toBe(cap)
    expect(p.metrics).toEqual(encodeMetricsData(s, { points: preferred }))
    expect(p.fittingPasses).toBeLessThanOrEqual(8)
    expect(p.sizeProbes).toBeLessThanOrEqual(8)
    const a = s.points.find((p) => p.name === 'com.example.a')
    const customCap = measureMetricsData(
      s,
      s.points.filter((p) => p.name !== 'com.example.z')
    )
    const q = await new TelemetryPayload(1n).buildPayload(s, options(customCap))
    expect(q.includedIdentities).toContain(a.identity)
    expect(q.omittedIdentities).toEqual([s.points.find((p) => p.name === 'com.example.z').identity])
  })

  test('fitting keeps an average and maximum together', async () => {
    const { collector, subscription } = make()
    const slot = collector.bindNode({ nodeId: 0, endpoint: 'b0', endpointRevision: {} })
    collector.observeRequest(slot, 5)
    const s = collector.snapshot(subscription)
    const withoutMax = s.points.filter((p) => !p.name.endsWith('.max'))
    const p = await new TelemetryPayload(1n).buildPayload(
      s,
      options(measureMetricsData(s, withoutMax))
    )
    expect(p.includedIdentities).toEqual([s.points.find((p) => p.type === 'sum').identity])
    expect(p.omittedIdentities).toHaveLength(2)
  })

  test('fit does not advance baselines and a reduced penalty preserves unsent samples', async () => {
    const { collector, subscription } = make()
    collector.register({ name: 'com.example.count', type: 'sum', value: 9n })
    const s = collector.snapshot(subscription)
    const encode = new TelemetryPayload(1n)
    const before = await encode.buildPayload(s, options())
    expect(collector.isCurrent(s)).toBe(true)
    const cap = measureMetricsData(
      s,
      s.points.filter((p) => p.name.startsWith('org.apache.kafka.'))
    )
    const reduced = await encode.buildPayload(s, { ...options(), penaltyMaxBytes: cap })
    expect(reduced.rawBytes).toBeLessThanOrEqual(cap)
    expect(reduced.transmittedBytes).toBeLessThanOrEqual(cap)
    expect(reduced.omittedIdentities.map((i) => i.key)).toContain(
      s.points.find((p) => p.name === 'com.example.count').identity.key
    )
    expect(before.includedIdentities).toHaveLength(2)
    collector.written(s, reduced.includedIdentities)
    expect(
      collector.snapshot(subscription).points.find((p) => p.name === 'com.example.count').value
    ).toBe(9n)
  })

  test('a smaller broker cap independently limits the body', async () => {
    const { collector, subscription } = make()
    const s = collector.snapshot(subscription)
    const p = await new TelemetryPayload(1n).buildPayload(s, { ...options(), brokerMaxBytes: 1 })
    expect(p.rawBytes).toBe(0)
    expect(p.transmittedBytes).toBe(0)
    expect(p.includedIdentities).toEqual([])
  })

  test('numeric broker 118 permits a reduced next body without replaying a written delta', async () => {
    const response = require('../../protocol/requests/pushTelemetry/v0/response')
    const { collector, subscription } = make()
    collector.register({ name: 'com.example.count', type: 'sum', value: 9n })
    const s = collector.snapshot(subscription)
    const builder = new TelemetryPayload(1n)
    const p = await builder.buildPayload(s, options())
    collector.written(s, p.includedIdentities)
    await expect(
      response.parse(await response.decode(Buffer.from('00000000007600', 'hex')))
    ).rejects.toMatchObject({ code: 118 })
    const next = collector.snapshot(subscription)
    expect(next.points.find((p) => p.name === 'com.example.count').value).toBe(0n)
    const penaltyMaxBytes = Math.max(1, Math.floor(p.rawBytes / 2))
    const q = await builder.buildPayload(next, { ...options(), penaltyMaxBytes })
    expect(q.rawBytes).toBeLessThanOrEqual(penaltyMaxBytes)
    expect(q.transmittedBytes).toBeLessThanOrEqual(penaltyMaxBytes)
    expect(collector.isCurrent(next)).toBe(true)
  })

  test('oversize UTF-8 resource omission changes count without splitting valid resources', async () => {
    const { collector, subscription } = make()
    const s = collector.snapshot(subscription, {
      client_rack: '温'.repeat(171),
      transactional_id: 'g',
    })
    expect(s.resources).toEqual([{ key: 'transactional_id', value: 'g' }])
    expect(s.droppedResourceAttributesCount).toBe(1)
    const p = await new TelemetryPayload(1n).buildPayload(s, options())
    expect(p.metrics).toEqual(encodeMetricsData(s))
    expect(p.metrics.includes(Buffer.from('温'))).toBe(false)
    collector.abandon(s)
    const rack = '温'.repeat(170) + 'ab'
    expect(Buffer.byteLength(rack)).toBe(512)
    const valid = collector.snapshot(subscription, { client_rack: rack, transactional_id: 'g' })
    expect(valid.resources).toEqual([
      { key: 'client_rack', value: rack },
      { key: 'transactional_id', value: 'g' },
    ])
    expect(valid.droppedResourceAttributesCount).toBe(0)
    expect(encodeMetricsData(valid).includes(Buffer.from(rack))).toBe(true)
  })

  test('maximum legal registration and node cardinality stay bounded under tiny caps', async () => {
    const { collector, subscription } = make()
    for (let n = 0; n < 128; n++)
      collector.register({
        name: `com.example.${String(n).padStart(3, '0')}`,
        type: 'gauge',
        value: n,
        attributes: { label: 'x'.repeat(256) },
      })
    for (let n = 0; n < 64; n++)
      collector.observeRequest(
        collector.bindNode({ nodeId: n, endpoint: `b${n}`, endpointRevision: {} }),
        n
      )
    const s = collector.snapshot(subscription)
    expect(s.points).toHaveLength(257)
    for (const cap of [1, 100, 1000, 4096]) {
      const p = await new TelemetryPayload(1n).buildPayload(s, options(cap))
      expect(p.rawBytes).toBeLessThanOrEqual(cap)
      expect(p.transmittedBytes).toBeLessThanOrEqual(cap)
      expect(p.includedIdentities.length + p.omittedIdentities.length).toBe(257)
      expect(p.fittingPasses).toBeLessThanOrEqual(8)
      expect(p.sizeProbes).toBeLessThanOrEqual(8)
    }
  })

  test.each([0, -1, 1.5, 1048577])('refuses invalid local cap %s', async (cap) => {
    const { collector, subscription } = make()
    await expect(
      new TelemetryPayload(1n).buildPayload(collector.snapshot(subscription), options(cap))
    ).rejects.toThrow(RangeError)
  })

  test('stale private authority yields no payload', async () => {
    const { collector, subscription } = make()
    expect(
      await new TelemetryPayload(1n).buildPayload(collector.snapshot(subscription), {
        ...options(),
        isCurrent: () => false,
      })
    ).toBeNull()
  })
})
