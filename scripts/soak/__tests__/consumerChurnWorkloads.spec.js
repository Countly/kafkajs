const fs = require('node:fs')
const os = require('node:os')
const path = require('node:path')
const {
  CONSUMER_CHURN,
  ChurnJournal,
  GapSampler,
  PublicConsumerProbe,
  consumerChurnPlan,
} = require('../consumerChurn')
const { ChurnWorkloads, ConsumerChurnRun } = require('../consumerChurnRun')
const { FakeKafka, FakeSocket } = require('./churnClusterFake')

const MIB = 1024 * 1024

/** One short pass of each shape, in the order and under the names the plan gives them. */
const WARMUPS = [
  { name: 'warm-member-churn', workload: 'memberChurn', ms: 45000 },
  { name: 'warm-regex-topics', workload: 'regexTopics', ms: 10000 },
  { name: 'warm-pause-recreate', workload: 'pauseRecreate', ms: 10000 },
  { name: 'warm-mostly-idle', workload: 'mostlyIdle', ms: 10000 },
]

/** A clock that moves only when something sleeps; a sleep also yields to the event loop once. */
const fakeTime = () => {
  const clock = { at: 0, now: () => clock.at }
  const sleep = async (ms, signal) => {
    if (signal?.aborted) return
    clock.at += ms
    await new Promise(setImmediate)
  }
  return { clock, sleep }
}

describe('Consumer churn profile > the real workloads, journal and oracle together', () => {
  let dir
  beforeEach(() => {
    dir = fs.mkdtempSync(path.join(process.env.TMPDIR ?? os.tmpdir(), 'churn-compose-'))
  })
  afterEach(() => fs.rmSync(dir, { recursive: true, force: true }))

  /**
   * The real run over the real workloads and oracle, with fake public clients on a fake cluster.
   * `fault` may change the cluster or the clients before anything runs.
   */
  const compose = ({ phaseSeconds = 70, warmups = [], fault = () => {} } = {}) => {
    const probe = new PublicConsumerProbe({
      activeResources: () => ({}),
      heap: async () => 100 * MIB,
    })
    const kafka = new FakeKafka({ socketFactory: probe.wrap(() => new FakeSocket()) })
    const { clock, sleep } = fakeTime()
    const workloads = new ChurnWorkloads({ kafka, probe, tag: 'tg' })
    const journal = new ChurnJournal({ file: path.join(dir, 'run.journal') })
    const appended = []
    const append = journal.append.bind(journal)
    journal.append = (observation) => {
      appended.push(observation)
      append(observation)
    }
    let peakRegistrations = 0
    const track = probe.track.bind(probe)
    probe.track = (client) => {
      const remove = track(client)
      peakRegistrations = Math.max(peakRegistrations, probe.registrations.size)
      return remove
    }
    const lines = []
    let atBaseline = null
    const run = new ConsumerChurnRun({
      workloads,
      scope: workloads.groups,
      probe,
      journal,
      record: (line) => {
        lines.push(line)
        if (line.kind === 'quiet' && line.boundary === 'warm') atBaseline ??= appended.length
      },
      plan: {
        ...consumerChurnPlan(phaseSeconds),
        warmupMs: warmups.reduce((total, { ms }) => total + ms, 0),
        warmups,
      },
      clock,
      sleep,
      gaps: new GapSampler({ capacity: 64, minimum: 1 }),
      groups: CONSUMER_CHURN.groups,
      heap: async () => 100 * MIB,
      settings: { ...CONSUMER_CHURN, rate: 20 },
      progressMs: 3600000,
    })
    fault({ kafka, workloads, run })
    return {
      run,
      kafka,
      workloads,
      probe,
      appended,
      lines,
      beforeBaseline: () => appended.slice(0, atBaseline ?? 0),
      peak: () => peakRegistrations,
    }
  }

  test('every phase journals only what the oracle accepts, and every quiet point has no client open', async () => {
    const { run, appended, lines, workloads, probe } = compose()
    const verdict = await run.run()
    expect(verdict.violations).toEqual([])
    expect(verdict.ok).toBe(true)
    expect(verdict.counts.acknowledged).toBeGreaterThan(100)
    expect(verdict.counts.commits).toBeGreaterThan(0)
    const kinds = new Set(appended.map(({ kind }) => kind))
    expect(kinds).not.toContain('incarnation')
    expect(kinds).not.toContain('read-complete')
    expect(kinds).toContain('read')
    const quiets = lines.filter(({ kind }) => kind === 'quiet')
    expect(quiets).toHaveLength(15)
    for (const { snapshot } of quiets) {
      expect(snapshot.sockets.open).toBe(0)
      expect(snapshot.listeners.registrations).toBe(0)
    }
    expect(workloads.clients.size).toBe(0)
    expect(probe.registrations.size).toBe(0)
  }, 60000)

  test('every acknowledged record of every deleted incarnation was read back before its delete', async () => {
    const { run, appended } = compose()
    await run.run()
    const read = new Set()
    let checked = 0
    const id = ({ incarnation, partition, offset }) =>
      JSON.stringify([incarnation, partition, offset])
    for (const record of appended) {
      if (record.kind === 'read') read.add(id(record))
      if (record.kind !== 'delete') continue
      const produced = appended.filter(
        (other) => other.kind === 'produce' && other.incarnation === record.incarnation
      )
      for (const entry of produced) expect(read).toContain(id(entry))
      checked += produced.length
    }
    expect(checked).toBeGreaterThan(100)
  }, 60000)

  test('the groups the oracle, the gap sampler and the drain use are the ones the members join', async () => {
    const { run, appended, workloads } = compose()
    await run.run()
    const journaled = new Set(appended.filter(({ group }) => group).map(({ group }) => group))
    expect([...journaled].sort()).toEqual(
      [
        ...workloads.groups.forPhase('member-churn'),
        ...workloads.groups.forPhase('regex-topics'),
        ...workloads.groups.forPhase('pause-recreate'),
        ...workloads.groups.forPhase('mostly-idle'),
      ].sort()
    )
    for (const group of journaled) expect(group).toContain('-tg')
    for (const phase of consumerChurnPlan(1).phases)
      for (const group of workloads.groups.forPhase(phase.name))
        expect(run.gaps.series.get(`${group}|${phase.name}`).count).toBeGreaterThan(0)
  }, 60000)

  test('a recreated topic gives its members a new incarnation before their next delivery', async () => {
    const { run, appended } = compose()
    await run.run()
    const pause = appended.filter(
      ({ kind, topic }) => kind === 'acquire' && topic?.includes('-pause-')
    )
    const incarnations = new Set(pause.map(({ incarnation }) => incarnation))
    expect(incarnations.size).toBeGreaterThanOrEqual(2)
    for (const { incarnation, topic } of pause)
      expect(incarnation.startsWith(`${topic}.`)).toBe(true)
  }, 60000)

  test('the warm-up runs all four shapes under groups of their own before the baseline, and measures none', async () => {
    const { run, beforeBaseline, workloads } = compose({ warmups: WARMUPS })
    const verdict = await run.run()
    expect(verdict.violations).toEqual([])
    const warm = beforeBaseline()
    const groups = new Set(warm.filter(({ group }) => group).map(({ group }) => group))
    expect([...groups].sort()).toEqual(
      WARMUPS.flatMap(({ name }) => workloads.groups.forPhase(name)).sort()
    )
    for (const part of ['-member-', '-rx-', '-pause-', '-idle-'])
      expect(warm.some(({ kind, topic }) => kind === 'acquire' && topic?.includes(part))).toBe(true)
    const measured = new Set(workloads.groups.forPhase('member-churn'))
    for (const group of groups) expect(measured).not.toContain(group)
    expect([...run.gaps.series.keys()].some((key) => key.includes('warm'))).toBe(false)
  }, 120000)

  test('the warm-up replaces dynamic members, so a replacement is not first used in a measured phase', async () => {
    const { run, beforeBaseline } = compose({ warmups: WARMUPS })
    await run.run()
    const labels = new Set(
      beforeBaseline()
        .filter(({ kind }) => kind === 'acquire')
        .map(({ owner }) => owner.replace(/-\d+$/, ''))
    )
    expect(labels).toContain('dynamic-0-1')
    expect(labels).toContain('dynamic-1-1')
  }, 120000)

  test('retired members and their registrations leave at once, so clients do not grow with churn', async () => {
    const { run, peak, kafka } = compose({ phaseSeconds: 130 })
    await run.run()
    const members = kafka.built.filter(({ group }) => group !== undefined).length
    expect(members).toBeGreaterThan(12)
    expect(peak()).toBeLessThanOrEqual(2 * 3 + 3)
  }, 120000)

  test('a record the log reader cannot see stops the phase before its topic is deleted', async () => {
    const { run, kafka, appended } = compose({
      fault: ({ kafka: fake }) => {
        fake.cluster.hideFromReaders = (topic, partition, offset) =>
          topic.includes('-member-') && partition === 0 && offset === 3
      },
    })
    await expect(run.run()).rejects.toThrow('were not read back')
    expect(appended.filter(({ kind }) => kind === 'delete')).toEqual([])
    expect(kafka.cluster.topics.size).toBeGreaterThan(0)
  }, 120000)

  test('no client creates a topic of its own, so a recreated topic keeps its partitions', async () => {
    const { run, kafka } = compose({ phaseSeconds: 5 })
    await run.run()
    const kinds = new Set(kafka.options.map(({ kind }) => kind))
    expect([...kinds].sort()).toEqual(['consumer', 'producer'])
    for (const { options } of kafka.options) expect(options.allowAutoTopicCreation).toBe(false)
  }, 60000)

  test('a topic that already exists when the workload creates it fails the run', async () => {
    const { run, lines } = compose({
      phaseSeconds: 5,
      fault: ({ kafka }) => {
        const admin = kafka.admin.bind(kafka)
        kafka.admin = () => {
          const client = admin()
          client.createTopics = async () => false
          return client
        }
      },
    })
    await expect(run.run()).rejects.toThrow('already existed when it was created')
    expect(lines.filter(({ kind }) => kind === 'verdict')).toEqual([
      expect.objectContaining({ ok: false }),
    ])
  }, 60000)

  test('a failed disconnect fails the run, with one failing verdict and no passing one', async () => {
    const { run, lines } = compose({
      fault: ({ kafka }) => {
        const producer = kafka.producer.bind(kafka)
        kafka.producer = (...args) => {
          const client = producer(...args)
          client.disconnect = async () => {
            throw new Error('producer socket stuck')
          }
          return client
        }
      },
    })
    await expect(run.run()).rejects.toThrow('producer socket stuck')
    const verdicts = lines.filter(({ kind }) => kind === 'verdict')
    expect(verdicts).toHaveLength(1)
    expect(verdicts[0]).toMatchObject({ ok: false })
    expect(verdicts[0].violations.join()).toContain('producer socket stuck')
  }, 60000)

  test('a cleanup failure after the last phase is a failed verdict, not a passing one', async () => {
    const { run, workloads, lines } = compose({ phaseSeconds: 5 })
    workloads.close = async () => {
      throw new Error('leftover client')
    }
    await expect(run.run()).rejects.toThrow('leftover client')
    expect(lines.filter(({ kind }) => kind === 'verdict')).toEqual([
      expect.objectContaining({ ok: false, violations: ['run failed: leftover client'] }),
    ])
  }, 60000)

  test('close tries every client, forgets all of them and then throws the first failure', async () => {
    const { workloads, kafka, probe } = compose()
    const stuck = kafka.consumer({ groupId: 'a' })
    const other = kafka.consumer({ groupId: 'b' })
    stuck.disconnect = async () => {
      throw new Error('a stuck')
    }
    other.disconnect = jest.fn(async () => {})
    const removed = jest.fn()
    workloads.clients.set(stuck, removed)
    workloads.clients.set(other, removed)
    await expect(workloads.close()).rejects.toThrow('a stuck')
    expect(other.disconnect).toHaveBeenCalled()
    expect(removed).toHaveBeenCalledTimes(2)
    expect(workloads.clients.size).toBe(0)
    expect(probe.registrations.size).toBe(0)
  })

  test('setup proves the cluster answers and leaves no client open', async () => {
    const { workloads, probe } = compose()
    await workloads.setup()
    await new Promise(setImmediate)
    expect(probe.created).toBeGreaterThan(0)
    expect(probe.openSockets()).toBe(0)
    expect(workloads.clients.size).toBe(0)
  })

  test('a record fetched before its acknowledgement arrives is journaled after the acknowledgement', async () => {
    const { run, appended } = compose({
      phaseSeconds: 5,
      fault: ({ kafka }) => {
        kafka.cluster.ackLag = true
      },
    })
    const verdict = await run.run()
    expect(verdict.violations).toEqual([])
    const produced = new Set()
    for (const record of appended) {
      const id = JSON.stringify([record.incarnation, record.partition, record.offset])
      if (record.kind === 'produce') produced.add(id)
      if (record.kind === 'handler') expect(produced).toContain(id)
    }
    expect(verdict.counts.delivered).toBeGreaterThan(0)
  }, 60000)

  describe('acknowledged', () => {
    afterEach(() => jest.useRealTimers())
    const topic = (incarnation = 't.1') => ({ incarnation, acks: new Map(), waiting: new Map() })

    test('is true at once for an acknowledged record and for an incarnation that is not current', () => {
      const { workloads } = compose()
      const current = topic()
      current.acks.set('0|3', 's3')
      workloads.live.set('t', current)
      expect(workloads.acknowledged('t', 0, '3', 't.1')).toBe(true)
      expect(workloads.acknowledged('t', 0, '4', 't.0')).toBe(true)
      expect(workloads.acknowledged('gone', 0, '4', 't.1')).toBe(true)
    })

    test('waits for the acknowledgement of every waiter, and leaves nothing behind', async () => {
      const { workloads } = compose()
      const current = topic()
      workloads.live.set('t', current)
      const waits = [
        workloads.acknowledged('t', 0, '3', 't.1'),
        workloads.acknowledged('t', 0, '3', 't.1'),
      ]
      expect(current.waiting.get('0|3').size).toBe(2)
      for (const settle of [...current.waiting.get('0|3')]) settle(true)
      await expect(Promise.all(waits)).resolves.toEqual([true, true])
      expect(current.waiting.size).toBe(0)
    })

    test('gives up with false after the wait, clearing its timer and registration', async () => {
      jest.useFakeTimers()
      const { workloads } = compose()
      const current = topic()
      workloads.live.set('t', current)
      const wait = workloads.acknowledged('t', 0, '3', 't.1')
      await jest.advanceTimersByTimeAsync(10000)
      await expect(wait).resolves.toBe(false)
      expect(current.waiting.size).toBe(0)
      expect(jest.getTimerCount()).toBe(0)
    })
  })
})
