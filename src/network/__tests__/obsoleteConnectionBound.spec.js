const { BOUND, NODES, createBoundHarness, outcome } = require('./fixtures/obsoleteConnections')
const { KafkaJSConnectionClosedError, KafkaJSConnectionError } = require('../../errors')
const metadataRequest = require('../../protocol/requests/metadata/v13/request')
const metadataResponse = require('../../protocol/requests/metadata/v13/response')

// Every case uses real Broker, ConnectionPool, Connection and RequestQueue objects.
describe('Bounded obsolete connections', () => {
  it.each(['seed', 'rebootstrap', 'refresh', 'hint'])(
    '%s recovery completes before an unanswered drain and fences old references',
    async (path) => {
      const h = await createBoundHarness({ target: path === 'seed' ? 0 : 1 })
      const { old, connection, socket, queue } = h
      try {
        const { first, second, gate } = await h.overload()
        const writes = socket.writes.length
        const socketCount = h.seam.sockets().length
        let recovered = false
        const recovering = h.replace(path).then(() => {
          recovered = true
        })
        await h.until(() => recovered)
        await recovering
        expect(socket.destroyed).toBe(false)
        expect(queue.inflight.size + queue.pending.length).toBe(2)
        await jest.advanceTimersByTimeAsync(BOUND - 1)
        expect(socket.destroyed).toBe(false)
        await jest.advanceTimersByTimeAsync(1)
        const answers = await Promise.all([first, second])
        for (const answer of answers) {
          expect(answer.error).toBeInstanceOf(KafkaJSConnectionClosedError)
          expect(answer.error.retriable).toBe(true)
        }
        expect(socket.destroyed).toBe(true)
        expect(connection.socket).toBeNull()
        expect(queue.inflight.size).toBe(0)
        expect(queue.pending).toHaveLength(0)
        for (const event of ['data', 'end', 'timeout']) expect(socket.listenerCount(event)).toBe(0)
        expect(socket.listenerCount('error')).toBe(1)
        expect(await h.queueProbe.snapshot()).toEqual(h.queueBaseline)
        await h.lateReply(gate)
        expect(socket.writes).toHaveLength(writes)
        await expect(old.connect()).rejects.toBeInstanceOf(KafkaJSConnectionError)
        await expect(old.metadata([])).rejects.toMatchObject({ retriable: true })
        expect(h.seam.sockets()).toHaveLength(socketCount)
        const current =
          path === 'seed' || path === 'rebootstrap'
            ? h.cluster.brokerPool.seedBroker
            : h.cluster.brokerPool.brokers[1]
        expect(current).toBeDefined()
        expect(current).not.toBe(old)
        await current.connect()
        await expect(current.metadata([])).resolves.toBeDefined()
        expect(socket.writes).toHaveLength(writes)
      } finally {
        await h.close()
      }
    }
  )

  it('a response before the bound finishes normally and clears the drain timer', async () => {
    const h = await createBoundHarness()
    const failures = []
    try {
      const gate = h.hold({ api: 'Metadata', node: 1 })

      const first = outcome(h.old.metadata([]))
      await gate.arrived()
      await h.replace('hint')
      await jest.advanceTimersByTimeAsync(BOUND - 1)
      gate.release()
      expect((await first).error).toBeUndefined()
      await h.until(() => h.socket.destroyed)
      expect(h.connection.socket).toBeNull()
      expect(jest.getTimerCount()).toBe(0)
    } catch (error) {
      failures.push(error)
    } finally {
      try {
        await h.close()
      } catch (error) {
        failures.push(error)
      }
    }
    if (failures.length) throw failures[0]
  })

  it('two replacements cannot erase the latest table entry and held encoders write nowhere', async () => {
    const h = await createBoundHarness()
    let release
    try {
      const { first, second } = await h.overload()
      const encoder = metadataRequest({ topics: [] })
      const encode = encoder.encode
      let entered = false
      const held = new Promise((resolve) => {
        release = resolve
      })
      encoder.encode = async () => {
        entered = true
        await held
        return encode()
      }
      const encoding = outcome(
        h.old.connectionPool.send({ request: encoder, response: metadataResponse })
      )
      await h.until(() => entered)
      const writes = h.socket.writes.length
      await h.replace('hint', NODES[2])
      const middle = h.cluster.brokerPool.brokers[1]
      await h.replace('hint', NODES[3])
      const current = h.cluster.brokerPool.brokers[1]
      expect(current).not.toBe(middle)
      await jest.advanceTimersByTimeAsync(BOUND)
      expect((await first).error).toBeInstanceOf(KafkaJSConnectionClosedError)
      expect((await second).error).toBeInstanceOf(KafkaJSConnectionClosedError)
      release()
      expect((await encoding).error).toMatchObject({ retriable: true })
      expect(h.socket.writes).toHaveLength(writes)
      expect(h.cluster.brokerPool.brokers[1]).toBe(current)
      await current.connect()
      await expect(current.metadata([])).resolves.toBeDefined()
      expect(h.socket.writes).toHaveLength(writes)
    } finally {
      release?.()
      await h.close()
    }
  })

  it('a seed and table alias keep their Broker while the replaced pool becomes permanently retired', async () => {
    const h = await createBoundHarness({ target: 0 })
    try {
      const oldPool = h.old.connectionPool
      h.cluster.brokerPool.brokers[9] = h.old
      const { first, second } = await h.overload()
      await h.replace('hint', NODES[2])
      expect(h.cluster.brokerPool.seedBroker).toBe(h.old)
      expect(h.cluster.brokerPool.brokers[0]).toBe(h.old)
      expect(h.cluster.brokerPool.brokers[9]).toBe(h.old)
      expect(h.old.connectionPool).not.toBe(oldPool)
      await jest.advanceTimersByTimeAsync(BOUND)
      expect((await first).error).toBeInstanceOf(KafkaJSConnectionClosedError)
      expect((await second).error).toBeInstanceOf(KafkaJSConnectionClosedError)
      expect(h.connection.socket).toBeNull()
      const before = h.seam.sockets().length
      await expect(oldPool.getConnection()).rejects.toMatchObject({ retriable: true })
      await expect(
        oldPool.send({ request: metadataRequest({ topics: [] }), response: metadataResponse })
      ).rejects.toMatchObject({ retriable: true })
      expect(h.seam.sockets()).toHaveLength(before)
      await h.old.connect()
      await expect(h.old.metadata([])).resolves.toBeDefined()
    } finally {
      await h.close()
    }
  })

  it('public disconnect remains reusable and builder carries the configured bound', async () => {
    const h = await createBoundHarness()
    try {
      expect(h.old.connectionPool.drainTimeout).toBe(BOUND)
      await h.old.disconnect()
      expect(h.connection.socket).toBeNull()
      const count = h.seam.sockets().length
      await h.old.connect()
      await expect(h.old.metadata([])).resolves.toBeDefined()
      expect(h.seam.sockets().length).toBeGreaterThan(count)
      expect(h.old.connectionPool.retired).not.toBe(true)
    } finally {
      await h.close()
    }
  })
})
