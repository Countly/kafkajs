const { instrument, ProducerChurn } = require('../soak')
const { STATUS } = require('../churnLedger')
const { ClosingProbe } = require('../closingProbe')
const { createScriptedCluster, producerScript } = require('../../../testHelpers/scriptedCluster')
const { assertProbe } = require('../../../testHelpers/producerProbe')
const { FakeBouncer, FakeLog, FakeReader, MemoryJournal } = require('./churnFakes')

const IDS = {
  churn: '01020304-0506-0708-090a-0b0c0d0e0f10',
  'churn-probe': '11121314-1516-1718-191a-1b1c1d1e1f20',
}
const PARTITIONS = { churn: 6, 'churn-probe': 1 }

describe('Producer churn > a real producer on a scripted cluster', () => {
  /** Closes a fake socket the way a real one ends: `end()` leads to its destruction. */
  const closing = (factory) => (args) => {
    const socket = factory(args)
    const end = socket.end.bind(socket)
    socket.end = () => {
      end()
      socket.destroy()
    }
    return socket
  }

  const metadataOf =
    (cluster) =>
    ({ topics }) => {
      const named = (topics ?? []).map(({ name }) => name).filter(Boolean)
      const names = named.length > 0 ? named : Object.keys(PARTITIONS)
      return {
        brokers: cluster.nodes.map(({ id, host, port }) => ({
          nodeId: id,
          host,
          port,
          rack: null,
        })),
        controllerId: 0,
        topics: names.map((name) => ({
          errorCode: 0,
          name,
          topicId: IDS[name],
          isInternal: false,
          topicAuthorizedOperations: -2147483648,
          partitions: Array.from({ length: PARTITIONS[name] }, (_, partitionIndex) => ({
            errorCode: 0,
            partitionIndex,
            leaderId: 0,
            leaderEpoch: 5,
            replicas: [0],
            isr: [0],
            offlineReplicas: [],
          })),
        })),
      }
    }

  test('sends at its rate, learns where each record landed, and passes the closing probe on the real buffer', async () => {
    const cluster = producerScript(
      createScriptedCluster({ nodes: [{ id: 0, host: 'node0', port: 9092 }] })
    )
    cluster.answer('Metadata', metadataOf(cluster))
    const options = cluster.kafkaOptions({ clientId: 'soak' })
    const { kafka, probe } = instrument(
      { ...options, socketFactory: closing(options.socketFactory) },
      { activeResources: false }
    )
    const admin = {
      connect: async () => {},
      disconnect: async () => {},
      createTopics: async () => true,
    }
    const log = new FakeLog()
    const reader = new FakeReader({ log })
    const closingProbe = new ClosingProbe({
      brokers: ['node0:9092'],
      clientId: 'soak',
      topic: 'churn-probe',
      capacity: 4194304,
      maxRequestSize: 8388608,
      budgetMs: 5000,
      readBackMs: 1000,
      quietMs: 2000,
      read: async () => {
        const value = Buffer.alloc(4192677)
        value.write(`churn-probe:${closingProbe.tag}:steady:`, 0, 'latin1')
        return new Map([[0, { records: [{ value, batchContext: { isControlBatch: false } }] }]])
      },
    })
    closingProbe.prepare = async () => {}
    const churn = new ProducerChurn({
      kafka: { producer: (settings) => kafka.producer(settings), admin: () => admin },
      probe,
      brokers: ['node0:9092'],
      topic: 'churn',
      record: () => {},
      phases: [{ name: 'steady', workload: 'producerSteady', ms: 300 }],
      bouncer: new FakeBouncer({ sleep: async () => {} }),
      reader,
      admissionProbe: closingProbe,
      faults: { delayWrites() {}, clear() {} },
      journal: new MemoryJournal(),
      rawLog: { endOffsets: async () => new Map(), readLog: async () => new Map() },
      settings: { rate: 200, pollMs: 5, latencySampleEvery: 1 },
    })

    try {
      await churn.prepare()
      await churn.runPhase('steady', 300)
    } finally {
      await churn.close()
    }

    const steady = churn.phaseResults.steady
    expect(steady.offered).toBeGreaterThanOrEqual(30)
    expect(steady.ack).toBe(steady.offered)
    expect(steady.unexpected).toBe(0)
    expect(steady.probeError).toBeNull()
    expect(() => assertProbe(steady.probe)).not.toThrow()
    expect(steady.probe).toMatchObject({
      instanceMatchesWorkload: true,
      calls: 1,
      bytes: 4194304,
      inEntry: true,
      reservationImmediate: true,
      acknowledged: true,
      pins: 0,
      registrations: 0,
      settlePromises: 0,
    })
    expect(steady.probe.frame.largest).toBeGreaterThan(4192677)
    expect(steady.probe.frame.largest).toBeLessThanOrEqual(8388608)
    expect(steady.afterDisconnect).toMatchObject({
      available: 4194304,
      reserved: 0,
      waitingBytes: 0,
      debts: 0,
      pins: 0,
      registrations: 0,
    })
    expect(churn.ledger.counts.ack).toBe(churn.ledger.offered)
    expect(
      churn.ledger.status.subarray(0, churn.ledger.offered).every((status) => status === STATUS.ACK)
    ).toBe(true)
    expect(churn.wire.Produce).toBeGreaterThan(0)
    expect(churn.wire.InitProducerId).toBeGreaterThan(0)
    expect(churn.unsettled).toBe(0)
    expect((await probe.snapshot()).sockets.open).toBe(0)
  }, 30000)
})
