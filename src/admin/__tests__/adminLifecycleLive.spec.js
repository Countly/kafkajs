const net = require('net')
const { Kafka } = require('../../../index')
const { AdminLiveProbe } = require('./fixtures/liveAdmin')
const defaultSocketFactory = require('../../network/socketFactory')
const { plainTextBrokers } = require('testHelpers')
const { liveCase, caseTimeout, pollUntil } = require('../../producer/__tests__/fixtures/bounded')

const suite = process.env.ADMIN_LIFECYCLE_E2E ? describe : describe.skip
const budget = 10000

/** A local TCP proxy that holds complete Metadata replies after forwarding their actual requests. */
class MetadataProxy {
  /** @param {string} address real broker address */
  constructor(address) {
    const [host, port] = address.split(':')
    this.target = { host, port: Number(port) }
    this.sockets = []
    this.held = []
    this.frames = 0
    this.hold = false
    this.server = net.createServer((client) => {
      const upstream = net.connect(this.target)
      this.sockets.push(client, upstream)
      const metadata = new Set()
      let requests = Buffer.alloc(0)
      let replies = Buffer.alloc(0)
      client.on('data', (data) => {
        requests = Buffer.concat([requests, data])
        while (requests.length >= 4 && requests.length >= requests.readInt32BE(0) + 4) {
          const size = requests.readInt32BE(0) + 4
          const frame = requests.subarray(0, size)
          requests = requests.subarray(size)
          this.frames++
          if (frame.readInt16BE(4) === 3) metadata.add(frame.readInt32BE(8))
          upstream.write(frame)
        }
      })
      upstream.on('data', (data) => {
        replies = Buffer.concat([replies, data])
        while (replies.length >= 4 && replies.length >= replies.readInt32BE(0) + 4) {
          const size = replies.readInt32BE(0) + 4
          const frame = replies.subarray(0, size)
          replies = replies.subarray(size)
          if (this.hold && metadata.has(frame.readInt32BE(4))) this.held.push({ client, frame })
          else client.write(frame)
        }
      })
      client.on('error', () => upstream.destroy())
      upstream.on('error', () => client.destroy())
      client.on('close', () => upstream.destroy())
      upstream.on('close', () => client.destroy())
    })
  }

  /** @returns {Promise<number>} actual ephemeral listener port @throws {Error} bind failure */
  async start() {
    await new Promise((resolve, reject) => {
      this.server.once('error', reject)
      this.server.listen(0, '127.0.0.1', resolve)
    })
    return this.server.address().port
  }

  /** @returns {void} releases the old responses only to their original sockets */
  release() {
    this.hold = false
    for (const { client, frame } of this.held.splice(0)) if (!client.destroyed) client.write(frame)
  }

  /** @returns {Promise<void>} destroys both sides and closes the owned listener */
  async close() {
    this.release()
    for (const socket of this.sockets) socket.destroy()
    await new Promise((resolve) => this.server.close(resolve))
  }
}

suite('admin lifecycle on real sockets', () => {
  test(
    'closes within its deadline and reconnects only explicitly',
    liveCase(budget, async (signal) => {
      const probe = new AdminLiveProbe({ stem: 'admin-lifecycle', signal, budgetMs: budget })
      await probe.finish(async () => {
        const reserve = net.createServer()
        probe.cleanup.add(
          'ephemeral reservation',
          () => new Promise((resolve) => reserve.close(resolve))
        )
        await probe.invoke(() => new Promise((resolve) => reserve.listen(0, '127.0.0.1', resolve)))
        const port = reserve.address().port
        await probe.invoke(() => new Promise((resolve) => reserve.close(resolve)))
        const unreachable = new Kafka({
          brokers: [`127.0.0.1:${port}`],
          retry: { retries: 0 },
          connectionTimeout: 500,
          authenticationTimeout: 500,
          logCreator: () => (entry) => probe.logs.push(entry),
          socketFactory: (args) => probe.socket(args),
        }).admin()
        probe.cleanup.add('unreachable client', () => unreachable.disconnect({ timeout: 0 }))
        const started = performance.now()
        const failure = await probe
          .invoke(() => unreachable.connect())
          .then(
            () => null,
            (error) => error
          )
        const elapsed = performance.now() - started
        expect(elapsed).toBeLessThanOrEqual(750)
        let cause = failure
        while (cause && cause.name !== 'KafkaJSConnectionError') cause = cause.cause
        expect(cause?.name).toBe('KafkaJSConnectionError')
        const admin = await probe.start()
        const existingTopic = await probe.topic()
        let listenerClose
        const remove = admin.on(admin.events.CONNECT, () => {
          listenerClose = admin.disconnect()
        })
        probe.removers.push(remove)
        await probe.invoke(() => admin.disconnect())
        await probe.invoke(() => admin.connect())
        await probe.invoke(() => pollUntil(signal, 1000, () => Boolean(listenerClose)))
        await probe.invoke(() => listenerClose)
        remove()
        await probe.invoke(() => admin.connect())
        let handlerClosed = false
        await probe.invoke(() =>
          admin
            .fetchTopicMetadata({ topics: [`${existingTopic}-absent`], apiTimeout: 500 })
            .catch(async () => {
              await admin.disconnect()
              handlerClosed = true
            })
        )
        expect(handlerClosed).toBe(true)
        await probe.invoke(() => admin.disconnect())
        const proxy = new MetadataProxy(plainTextBrokers()[0])
        probe.cleanup.add('Metadata proxy', () => proxy.close())
        const proxyPort = await probe.invoke(() => proxy.start())
        const factory = defaultSocketFactory()
        const sockets = []
        const client = new Kafka({
          brokers: [`127.0.0.1:${proxyPort}`],
          retry: { retries: 0 },
          logCreator: () => (entry) => probe.logs.push(entry),
          socketFactory: (args) => {
            const socket = factory({ ...args, host: '127.0.0.1', port: proxyPort })
            sockets.push(socket)
            probe.sockets.push(socket)
            return socket
          },
        }).admin()
        probe.cleanup.add('proxied admin', () => client.disconnect({ timeout: 0 }))
        await probe.invoke(() => client.connect())
        proxy.hold = true
        let disconnects = 0
        probe.removers.push(client.on(client.events.DISCONNECT, () => disconnects++))
        const old = probe
          .invoke(() => client.listTopics())
          .then(
            () => null,
            (error) => error
          )
        await probe.invoke(() => pollUntil(signal, 1000, () => proxy.held.length > 0))
        const heldSockets = [...sockets]
        const closing = performance.now()
        await probe.invoke(() => client.disconnect({ timeout: 50 }))
        expect(performance.now() - closing).toBeLessThanOrEqual(300)
        expect(heldSockets.every((socket) => socket.destroyed)).toBe(true)
        expect((await probe.invoke(() => old))?.name).toBe('KafkaJSNonRetriableError')
        expect(disconnects).toBe(1)
        const count = sockets.length
        const frames = proxy.frames
        await expect(probe.invoke(() => client.listTopics())).rejects.toMatchObject({
          name: 'KafkaJSNonRetriableError',
        })
        expect(sockets).toHaveLength(count)
        expect(proxy.frames).toBe(frames)
        await probe.invoke(() => client.connect())
        proxy.release()
        expect(await probe.invoke(() => client.listTopics())).toContain(existingTopic)
        expect(sockets[sockets.length - 1].destroyed).toBe(false)
        expect(disconnects).toBe(1)
        for (const options of [undefined, { timeout: 50 }, { timeout: 0 }]) {
          await probe.invoke(() => client.disconnect(options))
          await probe.invoke(() => client.connect())
        }
        probe.record('bootstrap and held response closure', {
          setupMs: elapsed,
          socketCount: sockets.length,
        })
      })
    }),
    caseTimeout(budget)
  )
})
