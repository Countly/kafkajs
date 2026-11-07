const TelemetryMetrics = require('../metrics')

/** @param {string} kind client kind @returns {object} collector and accepted selector fixture */
const fixture = (kind = 'producer') => {
  let mono = 0n
  const clock = { wallMs: () => 1000, monotonicNs: () => mono }
  const metrics = new TelemetryMetrics({ kind, clock })
  let revision = 0n
  return {
    metrics,
    tick: (n) => {
      mono += BigInt(n)
    },
    select: (requestedMetrics = ['*'], deltaTemporality = true) => {
      const subscription = { revision: ++revision, requestedMetrics, deltaTemporality }
      metrics.select(subscription)
      return subscription
    },
  }
}

/** @param {object} sample immutable collector output @returns {Array} literal-facing point tuples */
const tuples = (sample) => sample.points.map((p) => [p.name, p.type, p.unit, p.attributes, p.value])

describe('metric names selectors and resources', () => {
  test.each([
    ['producer', 'producer'],
    ['consumer', 'consumer'],
    ['shareConsumer', 'consumer'],
    ['admin', 'admin'],
  ])('%s has exactly the applicable three templates', (kind, prefix) => {
    const { metrics, select } = fixture(kind)
    const subscription = select()
    const ref = metrics.bindNode({ nodeId: 4, endpoint: 'one:9092', endpointRevision: 1 })
    metrics.observeConnection({})
    metrics.observeRequest(ref, 0)
    metrics.observeRequest(ref, 10)
    const sample = metrics.snapshot(subscription)
    expect(tuples(sample)).toEqual([
      [`org.apache.kafka.${prefix}.connection.creation.total`, 'sum', '1', [], 1n],
      [
        `org.apache.kafka.${prefix}.node.request.latency.avg`,
        'gauge',
        'ms',
        [{ key: 'node_id', value: '4' }],
        5,
      ],
      [
        `org.apache.kafka.${prefix}.node.request.latency.max`,
        'gauge',
        'ms',
        [{ key: 'node_id', value: '4' }],
        10,
      ],
    ])
  })

  test.each([
    [['*'], 1],
    [[''], 1],
    [['org.apache.kafka.producer.connection'], 1],
    [['org.apache.kafka.Producer'], 0],
    [['*', 'com.none.'], 0],
    [[], 0],
  ])('selects exact case-sensitive prefixes %j', (requested, count) => {
    const { metrics, select } = fixture()
    const s = select(requested)
    expect(metrics.snapshot(s).points).toHaveLength(count)
  })

  test.each([
    ['producer', ['client_rack', 'transactional_id']],
    ['consumer', ['client_rack', 'group_id', 'group_instance_id', 'group_member_id']],
    ['shareConsumer', ['client_rack', 'group_id', 'group_member_id']],
    ['admin', ['client_rack']],
  ])('copies only applicable resources for %s', (kind, expected) => {
    const { metrics, select } = fixture(kind)
    const resources = {
      client_rack: 'rack',
      group_id: 'group',
      group_instance_id: 'static',
      group_member_id: 'member',
      transactional_id: 'transaction',
      password: 'secret',
      topic: 'private',
      headers: 'private',
      principal: 'private',
      client_id: 'private',
    }
    const sample = metrics.snapshot(select(), resources)
    expect(sample.resources.map((p) => p.key)).toEqual(expected)
    expect(sample.scope.name).toBe('countly-kafkajs')
    expect(JSON.stringify(sample.resources)).not.toMatch(/secret|private/)
    expect(Object.isFrozen(sample.resources)).toBe(true)
    expect(sample.points.every((p) => Object.isFrozen(p.attributes))).toBe(true)
  })

  test('omits multibyte oversize resources intact and counts each omission', () => {
    const { metrics, select } = fixture('consumer')
    const sample = metrics.snapshot(select(), {
      client_rack: 'é'.repeat(256),
      group_id: 'g',
      group_instance_id: 'é'.repeat(257),
      group_member_id: 'm'.repeat(513),
    })
    expect(sample.resources).toEqual([
      { key: 'client_rack', value: 'é'.repeat(256) },
      { key: 'group_id', value: 'g' },
    ])
    expect(sample.droppedResourceAttributesCount).toBe(2)
  })

  test('omits group membership when no group identity applies', () => {
    const { metrics, select } = fixture('consumer')
    expect(metrics.snapshot(select(), { group_member_id: 'member' }).resources).toEqual([])
  })

  test('normal physical connections and request windows', () => {
    const { metrics, select } = fixture()
    const s = select()
    const token = {}
    metrics.observeConnection(token)
    metrics.observeConnection(token)
    metrics.observeConnection({})
    metrics.observeConnection(null)
    const unknown = metrics.bindNode({ nodeId: -1, endpoint: 'seed', endpointRevision: 1 })
    expect(unknown).toBeNull()
    metrics.observeRequest(unknown, 99)
    const sample = metrics.snapshot(s)
    expect(sample.points.map((p) => p.value)).toEqual([2n])
    metrics.written(
      sample,
      sample.points.map((p) => p.identity)
    )
    const a = metrics.bindNode({ nodeId: 1, endpoint: 'node', endpointRevision: 1 })
    expect(metrics.bindNode({ nodeId: 1, endpoint: 'node', endpointRevision: 1 })).toBe(a)
    metrics.observeRequest(a, 0)
    expect(metrics.snapshot(s).points.map((p) => p.value)).toEqual([0n, 0, 0])
  })

  test('retired node slots cannot relabel late latency', () => {
    const { metrics, select } = fixture()
    const s = select()
    const old = metrics.bindNode({ nodeId: 1, endpoint: 'same', endpointRevision: 1 })
    const current = metrics.bindNode({ nodeId: 2, endpoint: 'same', endpointRevision: 2 })
    metrics.observeRequest(old, 999)
    metrics.observeRequest(current, 7)
    metrics.observeConnection({})
    const sample = metrics.snapshot(s)
    expect(sample.points.map((p) => p.value)).toEqual([1n, 7, 7])
    expect(sample.points.flatMap((p) => p.attributes)).toEqual([
      { key: 'node_id', value: '2' },
      { key: 'node_id', value: '2' },
    ])
  })

  test('inactive nodes are evicted first and churn never exceeds sixty-four nodes', () => {
    const { metrics, select } = fixture()
    const s = select()
    const refs = Array.from({ length: 64 }, (_, nodeId) =>
      metrics.bindNode({ nodeId, endpoint: String(nodeId), endpointRevision: 1 })
    )
    metrics.releaseNode(refs[10])
    metrics.observeRequest(refs[10], 1)
    metrics.bindNode({ nodeId: 64, endpoint: '64', endpointRevision: 1 })
    metrics.observeRequest(refs[10], 100)
    for (let i = 65; i < 200; i++) {
      metrics.observeConnection({})
      metrics.observeRequest(
        metrics.bindNode({ nodeId: i, endpoint: String(i), endpointRevision: 1 }),
        1
      )
    }
    const sample = metrics.snapshot(s)
    expect(sample.points.length).toBeLessThanOrEqual(129)
    expect(sample.points.find((p) => p.type === 'sum').value).toBe(135n)
    expect(sample.points.flatMap((p) => p.attributes).some((p) => p.value === '10')).toBe(false)
  })

  test('empty and custom-only subscriptions disable RTT but retain physical counts', () => {
    const { metrics } = fixture()
    const ref = metrics.bindNode({ nodeId: 1, endpoint: 'node', endpointRevision: 1 })
    metrics.select({ revision: 1n, requestedMetrics: [], deltaTemporality: true })
    metrics.observeRequest(ref, 999)
    metrics.observeConnection({})
    const s = { revision: 2n, requestedMetrics: ['*'], deltaTemporality: true }
    metrics.select(s)
    metrics.observeRequest(ref, 3)
    expect(metrics.snapshot(s).points.map((p) => p.value)).toEqual([0n, 3, 3])
  })

  test('invalid numeric windows are omitted and the next interval restarts', () => {
    const logger = {
      warn: jest.fn(() => {
        throw new Error('logger')
      }),
    }
    const metrics = new TelemetryMetrics({ kind: 'producer', logger })
    const s = { revision: 1n, requestedMetrics: ['*'], deltaTemporality: true }
    metrics.select(s)
    const ref = metrics.bindNode({ nodeId: 1, endpoint: 'node', endpointRevision: 1 })
    metrics.observeRequest(ref, Number.MAX_SAFE_INTEGER)
    metrics.observeRequest(ref, 1)
    metrics.observeRequest(ref, Infinity)
    metrics.observeRequest(ref, -1)
    const bad = metrics.snapshot(s)
    expect(bad.points.some((p) => p.name.endsWith('.avg'))).toBe(false)
    expect(logger.warn).toHaveBeenCalledTimes(1)
    metrics.written(
      bad,
      bad.points.map((p) => p.identity)
    )
    metrics.observeRequest(ref, 2)
    expect(
      metrics
        .snapshot(s)
        .points.filter((p) => p.type === 'gauge')
        .map((p) => p.value)
    ).toEqual([2, 2])
  })
  test('maximum legal collector exposes exactly two hundred fifty-seven identities', () => {
    const { metrics, select } = fixture()
    const s = select()
    for (let nodeId = 0; nodeId < 64; nodeId++) {
      metrics.observeRequest(
        metrics.bindNode({ nodeId, endpoint: String(nodeId), endpointRevision: 1 }),
        nodeId
      )
    }
    for (let i = 0; i < 128; i++)
      metrics.register({ name: 'com.test.' + i, type: 'gauge', value: i })
    const sample = metrics.snapshot(s)
    expect(sample.points).toHaveLength(257)
    expect(new Set(sample.points.map((p) => p.identity.key)).size).toBe(257)
    expect(sample.points.filter((p) => p.type === 'sum').map((p) => p.value)).toEqual([0n])
    expect(sample.points.filter((p) => p.attributes.some((a) => a.key === 'node_id'))).toHaveLength(
      128
    )
  })
})
