const Cluster = require('../index')
const GroupRegistry = require('../groupRegistry')
const OffsetOperations = require('../../consumer/offsetManager/offsetOperations')
const ResetObligations = require('../../consumer/offsetManager/resetObligations')
const TransactionOffsets = require('../../producer/state/transactionOffsets')
const { createResourceProbe } = require('../../../testHelpers/resourceProbe')
const { newLogger } = require('testHelpers')

/** @returns {object} independent stores shared by the Clusters in one case. */
const stores = () => ({ offsets: new Map(), operations: new Map() })
/**
 * Constructs a real Cluster with network cleanup stubbed, without opening any connection.
 * @param {object} shared paired stores for this client family
 * @returns {Cluster} the participant
 */
const build = (shared) => {
  const cluster = new Cluster({
    ...shared,
    logger: newLogger(),
    brokers: ['localhost:9092'],
    retry: { retries: 0 },
  })
  cluster.brokerPool.disconnect = async () => {}
  return cluster
}
const cells = [{ topic: 'topic', partition: 0 }]

describe('Group registry lifetime', () => {
  it('retires both maps after 1000 real Cluster lifetimes and real tickets and tables', async () => {
    const shared = stores()
    const tables = []
    const ledgers = []
    const probe = createResourceProbe({
      ledgerSize: () => ledgers.reduce((n, ledger) => n + ledger.size(), 0),
      obligationSize: () => tables.reduce((n, table) => n + table.size(), 0),
    })
    const baseline = await probe.snapshot()
    for (let i = 0; i < 1000; i++) {
      const cluster = build(shared)
      const groupId = `group-${i}`
      const ledger = cluster.offsetOperations({ groupId })
      ledgers.push(ledger)
      const table = new ResetObligations()
      tables.push(table)
      table.owe('topic', 'incarnation')
      table.attachTo(ledger)
      const ticket = ledger.begin(cells, { mutation: true })
      cluster.markOffsetAsCommitted({ groupId, topic: 'topic', partition: 0, offset: '9' })
      ledger.apply(ticket, 'topic', 0)
      ledger.end(ticket)
      table.clear()
      await cluster.disconnect()
      expect(shared.offsets.size).toBe(0)
      expect(shared.operations.size).toBe(0)
    }
    expect(await probe.snapshot()).toEqual(baseline)
  })

  it('one disconnect preserves the shared peer ledger, cache and fences', async () => {
    const shared = stores()
    const a = build(shared)
    const b = build(shared)
    const ledger = a.offsetOperations({ groupId: 'group' })
    const ticket = ledger.begin(cells, { mutation: true })
    a.markOffsetAsCommitted({ groupId: 'group', topic: 'topic', partition: 0, offset: '9' })
    expect(b.offsetOperations({ groupId: 'group' })).toBe(ledger)
    const cache = b.committedOffsets({ groupId: 'group' })
    await a.disconnect()
    expect(b.committedOffsets({ groupId: 'group' })).toBe(cache)
    expect(cache.topic[0]).toBe('9')
    expect(shared.operations.get('group')).toBe(ledger)
    ledger.end(ticket)
    expect(shared.operations.get('group')).toBe(ledger)
    expect(b.committedOffsets({ groupId: 'group' })).toBe(cache)
    await b.disconnect()
    expect(shared.operations.size).toBe(0)
    expect(shared.offsets.size).toBe(0)
  })

  it('a disconnected producer ticket survives until its final end', async () => {
    const shared = stores()
    const producer = build(shared)
    const ledger = producer.offsetOperations({ groupId: 'group' })
    const ticket = ledger.begin(cells)
    await producer.disconnect()
    expect(shared.operations.get('group')).toBe(ledger)
    ledger.end(ticket)
    ledger.end(ticket)
    expect(shared.operations.size).toBe(0)
    expect(shared.offsets.size).toBe(0)
  })

  it('empty tickets also retain the registry until end', async () => {
    const shared = stores()
    const cluster = build(shared)
    const ledger = cluster.offsetOperations({ groupId: 'group' })
    const ticket = ledger.begin([])
    await cluster.disconnect()
    expect(shared.operations.get('group')).toBe(ledger)
    expect(ledger.size()).toBe(0)
    ledger.end(ticket)
    expect(shared.operations.size).toBe(0)
  })

  it('an attached table retains the registry until unlink, and reconnect acquires a new pair', async () => {
    const shared = stores()
    const cluster = build(shared)
    const old = cluster.offsetOperations({ groupId: 'group' })
    const table = new ResetObligations()
    table.attachTo(old)
    await cluster.disconnect()
    expect(shared.operations.get('group')).toBe(old)
    table.unlink()
    table.unlink()
    expect(shared.operations.size).toBe(0)
    const fresh = cluster.offsetOperations({ groupId: 'group' })
    expect(fresh).not.toBe(old)
    old.end(old.begin([]))
    expect(shared.operations.get('group')).toBe(fresh)
    await cluster.disconnect()
    expect(shared.offsets.size).toBe(0)
  })

  it('forfeited transaction permits never publish into a reacquired registry', async () => {
    const shared = stores()
    const cluster = build(shared)
    const offsets = new TransactionOffsets(newLogger())
    const ledger = cluster.offsetOperations({ groupId: 'group' })
    const call = offsets.snapshot(ledger, 'group', [
      { topic: 'topic', partitions: [{ partition: 0, offset: '99' }] },
    ])
    offsets.stage(call)
    await cluster.disconnect()
    expect(shared.operations.get('group')).toBe(ledger)
    offsets.release()
    expect(shared.operations.size).toBe(0)
    cluster.markOffsetAsCommitted({ groupId: 'group', topic: 'topic', partition: 0, offset: '2' })
    offsets.stage(call)
    offsets.publish(cluster)
    expect(cluster.committedOffsets({ groupId: 'group' }).topic[0]).toBe('2')
    await cluster.disconnect()
    expect(shared.offsets.size).toBe(0)
  })

  it('failed cluster cleanup releases only its own holder', async () => {
    const shared = stores()
    const a = build(shared)
    const b = build(shared)
    const ledger = a.offsetOperations({ groupId: 'group' })
    b.committedOffsets({ groupId: 'group' })
    a.brokerPool.disconnect = async () => {
      throw new Error('close failed')
    }
    await expect(a.disconnect()).rejects.toThrow('close failed')
    expect(shared.operations.get('group')).toBe(ledger)
    await b.disconnect()
    expect(shared.operations.size).toBe(0)
  })
})

describe('Registry identity controls', () => {
  it('adopts existing stores, ignores stale idle notifications and preserves externally replaced values', () => {
    const ledger = new OffsetOperations()
    const cache = { topic: { 0: '5' } }
    const offsets = new Map([['group', cache]])
    const operations = new Map([['group', ledger]])
    const registry = GroupRegistry.forStores(offsets, operations)
    const holder = {}
    registry.release('absent', holder)
    const entry = registry.acquire('group', holder)
    expect(entry.ledger).toBe(ledger)
    expect(entry.offsets).toBe(cache)
    const oldIdle = ledger.onIdle
    const replacementLedger = new OffsetOperations()
    const replacementCache = {}
    operations.set('group', replacementLedger)
    offsets.set('group', replacementCache)
    registry.release('group', holder)
    expect(operations.get('group')).toBe(replacementLedger)
    expect(offsets.get('group')).toBe(replacementCache)
    const fresh = registry.acquire('group', holder)
    oldIdle()
    expect(registry.entries.get('group')).toBe(fresh)
    registry.release('group', holder)
    expect(operations.size).toBe(0)
    expect(offsets.size).toBe(0)
  })
})
