const { Kafka } = require('../../../index')
const { createScriptedCluster, producerScript } = require('../../../testHelpers/scriptedCluster')
const { TOPIC } = require('../../consumer/__tests__/fixtures/fetchHarness')

const NODES = [{ id: 0, host: 'node0', port: 9092 }]

/**
 * A scripted cluster whose Produce frames are read for the `{"seq":n}` values they carry, in the
 * order they were written: the record order a broker would append.
 * @returns {{ cluster: object, written: number[] }}
 */
const recordingCluster = () => {
  const cluster = producerScript(createScriptedCluster({ nodes: NODES }))
  const written = []
  const socketFactory = cluster.socketFactory
  cluster.socketFactory = (options) => {
    const socket = socketFactory(options)
    const write = socket.write
    socket.write = (buffer, ...rest) => {
      const frame = Buffer.from(buffer)
      if (frame.readInt16BE(4) === 0) {
        for (const [, seq] of frame.toString('latin1').matchAll(/\{"seq":(\d+)\}/g)) {
          written.push(Number(seq))
        }
      }
      return write(buffer, ...rest)
    }
    return socket
  }
  return { cluster, written }
}

describe('Producer > concurrent sends to one partition', () => {
  it.each([
    ['after a warm-up send', true],
    ['to a topic not yet known', false],
  ])('are written in call order, %s', async (_name, warm) => {
    const { cluster, written } = recordingCluster()
    const producer = new Kafka(cluster.kafkaOptions()).producer({ idempotent: true })
    const message = (seq) => ({ value: JSON.stringify({ seq }) })
    await producer.connect()
    try {
      if (warm) await producer.send({ topic: TOPIC, messages: [message(0)] })
      let seq = 1
      await Promise.all(
        [1, 2, 3, 4, 5].map(() =>
          producer.send({ topic: TOPIC, messages: [message(seq++), message(seq++)] })
        )
      )

      expect(written).toEqual(Array.from({ length: warm ? 11 : 10 }, (_, i) => i + (warm ? 0 : 1)))
      cluster.assertClean()
    } finally {
      await producer.disconnect()
    }
  })
})
