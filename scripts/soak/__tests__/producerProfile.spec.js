const fs = require('node:fs')
const os = require('node:os')
const path = require('node:path')
const { PROFILES, WORKLOADS, main, parseArgs } = require('../soak')
const { ObservationJournal } = require('../churnLedger')
const { ClosingProbe } = require('../closingProbe')
const { CAPACITY, FakeProbe, FakeProducer, goodObservation, makeChurn } = require('./churnFakes')
const { Admission } = require('../../../src/producer/state/admission')
const Cluster = require('../../../src/cluster')
const RequestQueue = require('../../../src/network/requestQueue')
const { chargeOf } = require('../../../src/producer/state/recordSize')

describe('Soak > producer profile', () => {
  test('declares three five-minute phases without a consumer group', () => {
    expect(parseArgs(['--profile', 'producer-churn'])).toEqual({
      profile: 'producer-churn',
      phaseSeconds: null,
    })
    expect(PROFILES['producer-churn']).toEqual([
      { name: 'steady', workload: 'producerSteady', ms: 300000 },
      { name: 'outage', workload: 'producerOutage', ms: 300000 },
      { name: 'buffer-pressure', workload: 'producerPressure', ms: 300000 },
    ])
  })
})

test.each([
  ['producerSteady', 'steady'],
  ['producerOutage', 'outage'],
  ['producerPressure', 'buffer-pressure'],
])('%s joins its owned producer phase', async (workload, phase) => {
  const runPhase = jest.fn().mockResolvedValue(undefined)
  await WORKLOADS[workload]({ producerChurn: { runPhase } }, 300000)
  expect(runPhase).toHaveBeenCalledTimes(1)
  expect(runPhase).toHaveBeenCalledWith(phase, 300000)
  runPhase.mockRejectedValueOnce(new Error('phase failed'))
  await expect(WORKLOADS[workload]({ producerChurn: { runPhase } }, 300000)).rejects.toThrow(
    'phase failed'
  )
})

describe('Producer churn > a run over fake ports', () => {
  const violationsOf = async (options) => {
    const harness = makeChurn(options)
    const verdict = await harness.churn.run()
    return { ...harness, verdict }
  }

  test('with every port satisfied the run is ok and records one verdict', async () => {
    const { verdict, lines, kafka, probe, faults, bouncer, producer, reader, journal } =
      await violationsOf()

    expect(verdict.violations).toEqual([])
    expect(verdict.ok).toBe(true)
    expect(lines.filter(({ kind }) => kind === 'verdict')).toHaveLength(1)
    expect(lines.filter(({ kind }) => kind === 'resource-verdict')).toHaveLength(1)
    expect(lines.filter(({ kind }) => kind === 'phase').map(({ phase }) => phase)).toEqual([
      'steady',
      'outage',
      'buffer-pressure',
    ])
    expect(lines[0]).toMatchObject({ kind: 'config', config: { rate: 100 } })
    expect(lines.findIndex(({ kind }) => kind === 'config')).toBeLessThan(
      lines.findIndex(({ kind }) => kind === 'baseline')
    )
    expect(lines.filter(({ kind }) => kind === 'control')).toHaveLength(1)
    expect(lines.find(({ kind }) => kind === 'oracle').counts.ack).toBeGreaterThan(100)
    expect(kafka.consumer).not.toHaveBeenCalled()
    expect(probe.runs).toEqual(['steady', 'outage', 'buffer-pressure'])
    expect(probe.installs).toBe(1)
    expect(probe.restores).toBe(1)
    expect(faults.delayWrites).toHaveBeenCalledTimes(1)
    expect(faults.delayWrites).toHaveBeenCalledWith(5000)
    expect(bouncer.calls.map(({ hard, service }) => [hard, service])).toEqual([
      [false, 'kafka1'],
      [false, 'kafka2'],
      [false, 'kafka3'],
      [true, 'kafka1'],
      [true, 'kafka2'],
      [true, 'kafka3'],
    ])
    expect(bouncer.calls.every(({ topics }) => topics[0] === 'churn')).toBe(true)
    expect(producer.connects).toBe(3)
    expect(producer.disconnects).toBeGreaterThanOrEqual(3)
    expect(reader.closes).toBeGreaterThanOrEqual(3)
    expect(journal.closes).toBe(1)
    expect(verdict.controls['buffer-pressure'].pressureRecovery).toMatchObject({ attempts: 1 })
    expect(verdict.controls.outage.recoveries).toHaveLength(6)
  })

  test('an acknowledged id that is missing fails though the total of records is the same', async () => {
    const harness = makeChurn({
      plan: (id, producer) => {
        if (id === 7) {
          producer.log.append(0, Buffer.alloc(200))
          return 'lost'
        }
        return producer.delayed ? 'refuse' : 'ack'
      },
    })
    const verdict = await harness.churn.run()

    expect(verdict.ok).toBe(false)
    expect(verdict.violations[0]).toMatch(
      /^durability \(live reader\): acknowledged id 7 is missing from the log/
    )
    expect(verdict.violations.join('\n')).toMatch(/durability \(quiet read\): acknowledged id 7/)
    expect(verdict.violations.join('\n')).toMatch(/1 records carry an id that was never offered/)
    expect(harness.log.partitions.flat()).toHaveLength(verdict.counts.ack)
  })

  test("a record repeating another record's partition, pid, epoch and sequence fails, whatever its id", async () => {
    const { verdict } = await violationsOf({
      plan: (id, producer) => {
        if (id === 11) return { twin: 5 }
        return producer.delayed ? 'refuse' : 'ack'
      },
    })

    expect(verdict.ok).toBe(false)
    expect(verdict.violations[0]).toMatch(
      /durability \(live reader\): 1 records repeat a \(partition, pid, epoch, sequence\) identity/
    )
    expect(verdict.violations.some((line) => /acknowledged id/.test(line))).toBe(false)
  })

  test.each([
    ['absent', 'unknown-absent', true],
    ['present once', 'unknown-written', true],
  ])('an id of unknown outcome that is %s passes', async (_, action, ok) => {
    const { verdict } = await violationsOf({
      plan: (id, producer) => (id === 9 ? action : producer.delayed ? 'refuse' : 'ack'),
    })

    expect(verdict.violations).toEqual([])
    expect(verdict.ok).toBe(ok)
    expect(verdict.counts.unknown).toBe(1)
  })

  test('an id of unknown outcome that is read twice fails', async () => {
    const { verdict } = await violationsOf({
      plan: (id, producer) => (id === 9 ? 'unknown-twice' : producer.delayed ? 'refuse' : 'ack'),
    })

    expect(verdict.ok).toBe(false)
    expect(verdict.violations[0]).toMatch(
      /durability \(live reader\): id 9 of unknown outcome was read more than once/
    )
  })

  test('an id refused before sending that is in the log fails', async () => {
    const { verdict } = await violationsOf({
      plan: (id, producer, value) => {
        if (id === 9) {
          producer.place(id, value)
          return 'unsent'
        }
        return producer.delayed ? 'refuse' : 'ack'
      },
    })

    expect(verdict.ok).toBe(false)
    expect(verdict.violations[0]).toMatch(
      /^durability \(live reader\): id 9 was refused before sending but is in the log/
    )
  })

  test('a rejection with no outcome fails the run and names the error', async () => {
    const { verdict } = await violationsOf({
      plan: (id, producer) => (id === 9 ? 'unexpected' : producer.delayed ? 'refuse' : 'ack'),
    })

    expect(verdict.ok).toBe(false)
    expect(
      verdict.violations.some((line) =>
        /^sends: 1 rejections in steady carry no outcome, the first Error: Something else$/.test(
          line
        )
      )
    ).toBe(true)
  })

  test('a buffer that refuses nothing in the pressure phase fails', async () => {
    const { verdict } = await violationsOf({ plan: () => 'ack' })

    expect(verdict.ok).toBe(false)
    expect(verdict.violations).toContain(
      'buffer-pressure: the full buffer refused no send with KafkaJSBufferExhaustedError'
    )
  })

  test('a refusal of either reason counts as pressure', async () => {
    const { verdict, lines } = await violationsOf({
      plan: (id, producer) => (producer.delayed ? (id % 2 ? 'refuse' : 'refuse-full') : 'ack'),
    })

    expect(verdict.violations).toEqual([])
    const pressure = lines.find(
      ({ kind, phase }) => kind === 'phase' && phase === 'buffer-pressure'
    )
    expect(pressure.refusals.timeout).toBeGreaterThan(0)
    expect(pressure.refusals['queue-full']).toBeGreaterThan(0)
    expect(pressure.admitted).toBe(pressure.offered - pressure.unsent)
  })

  test('an outage phase with no outage fails', async () => {
    const harness = makeChurn()
    harness.bouncer.rollingBounce = async () => []
    const verdict = await harness.churn.run()

    expect(verdict.ok).toBe(false)
    expect(verdict.violations).toContain('outage: no broker outage was observed')
  })

  test('an outage whose restoration is not followed by an acknowledged send fails', async () => {
    const harness = makeChurn({
      settings: { recoveryMs: 1, recoveryRetryMs: 1 },
      plan: (id, producer) => {
        if (producer.delayed) return 'refuse'
        return harness.bouncer.restored.length === 2 ? 'unsent' : 'ack'
      },
    })
    const verdict = await harness.churn.run()

    expect(verdict.ok).toBe(false)
    expect(verdict.violations).toContain(
      'outage: 6 restorations, 5 followed by an acknowledged send'
    )
  })

  test('an outage phase without a hard outage fails', async () => {
    const harness = makeChurn()
    const { rollingBounce } = harness.bouncer
    harness.bouncer.rollingBounce = (options) =>
      options.hard ? Promise.resolve([]) : rollingBounce.call(harness.bouncer, options)
    const verdict = await harness.churn.run()

    expect(verdict.violations).toContain('outage: no hard outage was observed')
  })

  test('a reader whose cursor stops below the frozen end fails', async () => {
    const { verdict } = await violationsOf({ stall: true })

    expect(verdict.ok).toBe(false)
    expect(verdict.violations[0]).toMatch(
      /^durability: the final cursor 1 of partition \d is below the frozen end \d+$/
    )
  })

  test('a listener added for a new event name fails, though no other count changes', async () => {
    const state = { named: { requestQueues: {}, sockets: {} } }
    const probe = new FakeProbe({
      observe: (label) => {
        if (label === 'steady') state.named = { requestQueues: {}, sockets: { extra: 1 } }
        return goodObservation(label)
      },
    })
    const { verdict } = await violationsOf({ probe, listeners: () => state.named })

    expect(verdict.ok).toBe(false)
    expect(verdict.violations).toContain(
      "listeners after steady: 1 on sockets 'extra', 0 at the start"
    )
  })

  test('swapping one listener for another is not hidden by an equal total', async () => {
    const state = { named: { sockets: { connect: 1 } } }
    const probe = new FakeProbe({
      observe: (label) => {
        if (label === 'steady') state.named = { sockets: { error: 1 } }
        return goodObservation(label)
      },
    })
    const { verdict } = await violationsOf({ probe, listeners: () => state.named })

    expect(verdict.violations).toContain(
      "listeners after steady: 1 on sockets 'error', 0 at the start"
    )
    expect(verdict.violations).toContain(
      "listeners after steady: 0 on sockets 'connect', 1 at the start"
    )
  })

  test('an unhandled rejection after a phase fails', async () => {
    const { verdict } = await violationsOf({
      probe: new FakeProbe({
        observe: (label) => {
          if (label === 'outage')
            process.emit('unhandledRejection', new Error('lost'), Promise.resolve())
          return goodObservation(label)
        },
      }),
    })

    expect(
      verdict.violations.some((line) => /^unhandled rejections after outage: 1$/.test(line))
    ).toBe(true)
  })

  test('too few heap samples fail the run', async () => {
    const { verdict } = await violationsOf({
      soak: { sampleMs: 100000, warmupMs: 0, settleMs: 50 },
    })

    expect(verdict.ok).toBe(false)
    expect(verdict.violations).toContain('resources: 0 heap samples after warm-up, 6 are needed')
  })

  test('too few latency samples fail the run', async () => {
    const { verdict } = await violationsOf({ settings: { minLatencySamples: 100000 } })

    expect(verdict.ok).toBe(false)
    expect(
      verdict.violations.some((line) =>
        /^latency: \d+ samples, 100000 are needed, and 0 are negative$/.test(line)
      )
    ).toBe(true)
  })

  test('a closing probe that fails fails the run and is named by its phase and invariant', async () => {
    const { verdict } = await violationsOf({
      probe: new FakeProbe({
        observe: (label) => ({
          ...goodObservation(label),
          reservationImmediate: label !== 'outage',
        }),
      }),
    })

    expect(verdict.violations).toContain('admission probe (outage): Probe returned a waiter')
  })

  test('a producer that still holds a pin after its disconnect fails the run', async () => {
    const { verdict } = await violationsOf({
      probe: new FakeProbe({
        retained: () => ({ ...goodObservation(), pins: 1 }),
      }),
    })

    expect(verdict.violations).toContain(
      'after disconnect (steady): the producer still holds 1 pins'
    )
  })

  test('a run that is not given its owner refuses to start a phase', async () => {
    const unprepared = makeChurn().churn
    await expect(WORKLOADS.producerSteady({ producerChurn: unprepared }, 10)).rejects.toThrow(
      /no prepared, open producer churn/
    )
    expect(() => WORKLOADS.producerPressure({}, 10)).toThrow(TypeError)
  })
})

describe('Producer churn > the schedule', () => {
  test('offers the configured rate on a deadline schedule and reports its slip', async () => {
    const { lines } = await (async () => {
      const harness = makeChurn()
      await harness.churn.run()
      return harness
    })()
    const steady = lines.find(({ kind, phase }) => kind === 'phase' && phase === 'steady')

    expect(steady.offered).toBeGreaterThanOrEqual(100)
    expect(steady.offered).toBeLessThanOrEqual(102)
    expect(steady.backpressure).toBe(0)
    expect(steady.slip.ticks).toBeGreaterThan(10)
    expect(steady.slip.maxMs).toBeGreaterThanOrEqual(steady.slip.meanMs)
    expect(steady.offeredPerSecond).toBeCloseTo(steady.offered, 5)
  })

  test('never has more than the bound of calls unsettled, and a steady phase that cannot hold its rate fails', async () => {
    const harness = makeChurn({
      settings: { maxUnsettled: 5 },
      onSleep: ({ t }, { producer }) => {
        if (t > 20 && t < 700) producer.hold = true
        else if (t >= 700 && producer.hold) {
          producer.hold = false
          producer.release()
        }
      },
    })
    const verdict = await harness.churn.run()
    const steady = harness.lines.find(({ kind, phase }) => kind === 'phase' && phase === 'steady')

    expect(harness.producer.peak).toBeLessThanOrEqual(5)
    expect(steady.backpressure).toBeGreaterThan(0)
    expect(verdict.ok).toBe(false)
    expect(
      verdict.violations.some((line) =>
        /^steady: offered \d+ a second, target 100, \d+ ticks at the unsettled bound, slip up to \d+ ms$/.test(
          line
        )
      )
    ).toBe(true)
  })

  test('stops offering when the offered-id table is full and says so', async () => {
    const harness = makeChurn({ settings: { idSlack: 0 } })
    const verdict = await harness.churn.run()

    expect(verdict.ok).toBe(false)
    expect(verdict.violations).toContain('sends: the offered-id table filled before the run ended')
    expect(harness.churn.ledger.offered).toBe(harness.churn.ledger.capacity)
  })
})

describe('Producer churn > a phase that fails', () => {
  test('still takes the delay off, joins the reader and disconnects every client', async () => {
    const harness = makeChurn()
    const { reader, producer, faults, kafka, lines } = harness
    const poll = reader.poll.bind(reader)
    let polls = 0
    reader.poll = async () => {
      polls += 1
      if (producer.delayed) throw new Error('the reader broke')
      return poll()
    }

    await expect(harness.churn.run()).rejects.toThrow('the reader broke')

    const settled = polls
    await new Promise((resolve) => setTimeout(resolve, 30))
    expect(polls).toBe(settled)
    expect(faults.clear).toHaveBeenCalled()
    expect(producer.delayed).toBe(false)
    expect(producer.connected).toBe(false)
    expect(producer.disconnects).toBeGreaterThanOrEqual(3)
    expect(reader.closes).toBeGreaterThanOrEqual(3)
    expect(kafka.consumer).not.toHaveBeenCalled()
    expect(lines.at(-1)).toEqual({
      kind: 'verdict',
      ok: false,
      violations: ['run failed: the reader broke'],
    })
  })

  test('in the outage phase restores the node it took down and disconnects the admin', async () => {
    const harness = makeChurn()
    harness.bouncer.failAt = 2

    await expect(harness.churn.run()).rejects.toThrow('stopping kafka2 failed')

    expect(harness.bouncer.restored).toEqual(['kafka1', 'kafka2'])
    expect(harness.admin.connect).toHaveBeenCalledTimes(2)
    expect(harness.admin.disconnect).toHaveBeenCalledTimes(2)
    expect(harness.producer.connected).toBe(false)
    expect(harness.probe.restores).toBe(1)
  })

  test('is given the abort that ends the roll when the phase window closes', async () => {
    const harness = makeChurn({ phaseMs: 30 })
    const verdict = await harness.churn.run()

    expect(harness.bouncer.calls.length).toBeLessThan(6)
    expect(verdict.violations.some((line) => /^outage: /.test(line))).toBe(true)
  })

  test('a profile that names a workload nobody registered starts nothing', async () => {
    const harness = makeChurn()
    harness.churn.workloads = { ...WORKLOADS, producerPressure: undefined }

    await expect(harness.churn.run()).rejects.toThrow(
      'The phase buffer-pressure names the workload producerPressure, which is not registered'
    )

    expect(harness.kafka.producer).not.toHaveBeenCalled()
    expect(harness.lines).toEqual([])
  })
})

describe('Producer churn > main', () => {
  const withGc = async (work) => {
    const exposed = globalThis.gc
    globalThis.gc = () => {}
    try {
      return await work()
    } finally {
      globalThis.gc = exposed
    }
  }

  test('hands its owner to every phase, in order, and builds no consumer', async () => {
    const harness = makeChurn()
    const runPhase = jest.spyOn(harness.churn, 'runPhase')
    const built = []

    const verdict = await withGc(() =>
      main('producer-churn', {
        phaseSeconds: 2,
        churn: (options) => {
          built.push(options)
          return harness.churn
        },
      })
    )

    expect(built).toHaveLength(1)
    expect(built[0].phaseSeconds).toBe(2)
    expect(built[0].phases).toEqual(
      PROFILES['producer-churn'].map((phase) => ({ ...phase, ms: 2000 }))
    )
    expect(runPhase.mock.calls.map(([name]) => name)).toEqual([
      'steady',
      'outage',
      'buffer-pressure',
    ])
    expect(harness.kafka.consumer).not.toHaveBeenCalled()
    expect(verdict.ok).toBe(true)
    expect(harness.probe.restores).toBe(1)
  })

  test('closes the owner when its run fails', async () => {
    const harness = makeChurn()
    const close = jest.spyOn(harness.churn, 'close')
    harness.bouncer.failAt = 1

    await expect(
      withGc(() => main('producer-churn', { churn: () => harness.churn }))
    ).rejects.toThrow('stopping kafka1 failed')

    expect(close).toHaveBeenCalled()
  })
})

describe('Producer churn > the evidence', () => {
  let directory
  beforeAll(() => {
    directory = fs.mkdtempSync(path.join(os.tmpdir(), 'churn-journal-'))
  })
  afterAll(() => {
    fs.rmSync(directory, { recursive: true, force: true })
  })

  test("keeps the reader's journal on disk in fixed entries and takes its durability from it", async () => {
    const file = path.join(directory, 'run.journal')
    const journal = new ObservationJournal({ file, entries: 8 })
    const harness = makeChurn({ journal })
    const verdict = await harness.churn.run()

    expect(verdict.violations).toEqual([])
    expect(journal.count).toBe(harness.log.partitions.flat().length)
    expect(fs.statSync(file).size).toBe(32 * journal.count)
    expect(verdict.durability.live.counts.acknowledgedRead).toBe(verdict.counts.ack)
    expect(verdict.durability.quiet.counts.acknowledgedRead).toBe(verdict.counts.ack)
  })

  test('stamps the verdict with the wire versions the producer used and keeps every line JSON', async () => {
    const harness = makeChurn()
    const verdict = await harness.churn.run()

    expect(verdict.wireVersions).toEqual({ InitProducerId: 5, Produce: 11 })
    expect(harness.lines.map(({ kind }) => kind)).toEqual(
      expect.arrayContaining([
        'config',
        'baseline',
        'sample',
        'quiet',
        'phase',
        'phase-end',
        'oracle',
        'teardown',
        'resource-verdict',
        'verdict',
      ])
    )
    expect(harness.lines.at(-1).kind).toBe('verdict')
  })
})

describe('Producer churn > the closing probe on a real buffer', () => {
  /** A producer whose sends take their charge from a real `Admission`, in the calling turn or not. */
  class BufferedProducer extends FakeProducer {
    constructor({ sync, ...options }) {
      super(options)
      this.sync = sync
      this.admission = new Admission({ capacity: CAPACITY, maxBlockMs: 2000 })
      this.cluster = Object.assign(Object.create(Cluster.prototype), { topicPins: new Map() })
      this.probeRecords = []
      this.queue = {
        createSocketRequest: () => ({}),
        canSendSocketRequestImmediately: () => true,
        sendSocketRequest() {},
      }
    }

    send(args) {
      const { topic, messages } = args
      const charge = chargeOf(
        [{ topic, messages: [{ key: null, value: messages[0].value, timestamp: 0 }] }],
        CAPACITY
      ).total
      const enter = () => {
        const reservation = this.admission.acquire(charge, { remaining: () => 1000 })
        this.cluster.holdTopic(topic)
        return reservation
      }
      const finish = (reservation) =>
        super.send(args).finally(() => {
          Promise.resolve(reservation).then((held) => held.releaseAll())
          this.cluster.releaseTopic(topic)
        })
      return this.sync ? finish(enter()) : Promise.resolve().then(() => finish(enter()))
    }

    act(id, topic, value) {
      if (!topic.endsWith('-probe')) return super.act(id, topic, value)
      RequestQueue.prototype.push.call(this.queue, {
        entry: { apiName: 'Produce', correlationId: 1 },
        frameLength: value.length + 300,
      })
      this.probeRecords.push({ value, batchContext: { isControlBatch: false } })
      return [{ topicName: topic, partition: 0, baseOffset: '0' }]
    }
  }

  const run = async (sync) => {
    const harness = makeChurn({
      makeProducer: (parts) => new BufferedProducer({ ...parts, sync }),
      makeProbe: ({ producer }) =>
        new ClosingProbe({
          brokers: ['broker:9092'],
          clientId: 'soak',
          topic: 'churn-probe',
          capacity: CAPACITY,
          maxRequestSize: 8388608,
          budgetMs: 1000,
          read: async () => new Map([[0, { records: producer.probeRecords }]]),
          readBackMs: 200,
          quietMs: 300,
        }),
    })
    const verdict = await harness.churn.run()
    return { ...harness, verdict }
  }

  test('passes when the buffer admits the whole-capacity send in the calling turn', async () => {
    const { verdict, lines, producer } = await run(true)

    expect(verdict.violations).toEqual([])
    const observation = lines.find(({ kind }) => kind === 'quiet').probe
    expect(observation).toMatchObject({
      instanceMatchesWorkload: true,
      calls: 1,
      bytes: CAPACITY,
      inEntry: true,
      reservationImmediate: true,
      acknowledged: true,
      readBack: { found: 1 },
    })
    expect(observation.layout).toMatchObject({ valueBytes: 4192677, charge: CAPACITY })
    expect(producer.probeRecords).toHaveLength(3)
  })

  test('fails when the buffer grants the send in a later turn', async () => {
    const { verdict } = await run(false)

    expect(verdict.ok).toBe(false)
    expect(verdict.violations).toContain(
      'admission probe (steady): Probe was not admitted in its calling turn'
    )
  })
})
