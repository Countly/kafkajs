const { createCluster } = require('testHelpers')

describe('Cluster', () => {
  let groupId

  beforeEach(() => {
    groupId = 'test-group-id'
  })

  describe('if no offset map is provided', () => {
    it('should return all committed offsets by group', async () => {
      const cluster = createCluster({})
      const topic = 'test-topic'
      const partition = 0

      expect(cluster.committedOffsets({ groupId })).toEqual({})

      cluster.markOffsetAsCommitted({ groupId, topic, partition, offset: '100' })
      cluster.markOffsetAsCommitted({ groupId: 'foobar', topic, partition, offset: '999' })

      expect(cluster.committedOffsets({ groupId })).toEqual({ [topic]: { [partition]: '100' } })
      expect(cluster.committedOffsets({ groupId: 'foobar' })).toEqual({
        [topic]: { [partition]: '999' },
      })
    })
  })

  describe('if an offset map is provided', () => {
    let offsets

    beforeEach(() => {
      offsets = new Map()
    })

    it('should return all committed offsets by group', async () => {
      const cluster = createCluster({ offsets })
      const topic = 'test-topic'
      const partition = 0

      expect(cluster.committedOffsets({ groupId })).toEqual({})

      cluster.markOffsetAsCommitted({ groupId, topic, partition, offset: '100' })
      cluster.markOffsetAsCommitted({ groupId: 'foobar', topic, partition, offset: '999' })

      expect(cluster.committedOffsets({ groupId })).toEqual({ [topic]: { [partition]: '100' } })
      expect(cluster.committedOffsets({ groupId: 'foobar' })).toEqual({
        [topic]: { [partition]: '999' },
      })
    })

    it('should use the provided offsets map', async () => {
      const cluster = createCluster({ offsets })
      const topic = 'test-topic'
      const partition = 0

      cluster.markOffsetAsCommitted({ groupId, topic, partition, offset: '100' })
      cluster.markOffsetAsCommitted({ groupId: 'foobar', topic, partition, offset: '999' })

      expect(cluster.committedOffsets({ groupId })).toEqual({ [topic]: { [partition]: '100' } })
      expect(offsets.get(groupId)).toEqual({ [topic]: { [partition]: '100' } })
    })
  })

  describe('the ledger of offset operations', () => {
    it('is created once per group, on first use', () => {
      const cluster = createCluster({})

      const ledger = cluster.offsetOperations({ groupId })

      expect(ledger).toBe(cluster.offsetOperations({ groupId }))
      expect(cluster.offsetOperations({ groupId: 'foobar' })).not.toBe(ledger)
    })

    it('is kept in the store the client passed, so every cluster it builds shares it', () => {
      const operations = new Map()
      const first = createCluster({ operations })
      const second = createCluster({ operations })

      expect(first.offsetOperations({ groupId })).toBe(second.offsetOperations({ groupId }))
      expect(operations.get(groupId)).toBe(first.offsetOperations({ groupId }))
    })

    it('is not shared with a cluster that was given another store', () => {
      const first = createCluster({ operations: new Map() })
      const second = createCluster({ operations: new Map() })

      expect(first.offsetOperations({ groupId })).not.toBe(second.offsetOperations({ groupId }))
    })
  })
})
