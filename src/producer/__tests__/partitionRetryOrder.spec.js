const { Kafka } = require('../../../index')
const { createScriptedCluster, producerScript } = require('../../../testHelpers/scriptedCluster')
const { TOPIC } = require('../../consumer/__tests__/fixtures/fetchHarness')

const NODES = [{ id: 0, host: 'node0', port: 9092 }]
const METADATA_MAX_AGE = 100
const SENDS = 8

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

describe('Producer > concurrent sends to one partition across a failed metadata refresh', () => {
  it('are written in call order when the expired-metadata refresh they all wait on fails', async () => {
    const { cluster, written } = recordingCluster()
    const producer = new Kafka(cluster.kafkaOptions()).producer({
      idempotent: true,
      metadataMaxAge: METADATA_MAX_AGE,
    })
    const message = (seq) => ({ value: JSON.stringify({ seq }) })
    await producer.connect()
    try {
      await producer.send({ topic: TOPIC, messages: [message(0)] })
      await new Promise((resolve) => setTimeout(resolve, METADATA_MAX_AGE + 50))

      // The refresh of the expired metadata is held, as on a frozen broker, then its connection dies.
      const refresh = cluster.schedule.hold({ api: 'Metadata' })
      const sends = []
      for (let seq = 1; seq <= SENDS; seq++) {
        sends.push(producer.send({ topic: TOPIC, messages: [message(seq)] }))
      }
      await refresh.arrived()
      refresh.fail(new Error('connection lost while the broker was frozen'))
      await Promise.all(sends)

      expect(written).toEqual(Array.from({ length: SENDS + 1 }, (_, i) => i))
    } finally {
      await producer.disconnect()
    }
  })
})
