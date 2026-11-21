const crypto = require('node:crypto')
const fs = require('node:fs')
const os = require('node:os')
const path = require('node:path')
const zlib = require('node:zlib')
const { fakeTime } = require('./churnFakes')
const { parseLoadArgs } = require('../loadProfile')
const { LoadRun, parseDescribe, recheck } = require('../loadRun')

/**
 * A scripted cluster: partition logs with transaction state, sends acknowledged `ackDelayMs` after
 * they were made (on the fake clock), one consumer member that is delivered every visible record,
 * and Apache's reader and group describe over the same logs. Partition p is led by node p % 3 + 1;
 * `delayed` (a node) adds 300 ms to every send touching its partitions, and a send whose cells
 * `stalled` names is not acknowledged until it no longer does. Clients emit the instrumentation
 * events the load run measures.
 */
class FakeCluster {
  /**
   * @param {{ ackDelayMs?: number, dropLine?: (topic: string, line: string) => boolean,
   *   stalled?: (cells: string[]) => boolean }} [options]
   */
  constructor({ ackDelayMs = 0, dropLine = () => false, stalled = () => false } = {}) {
    Object.assign(this, { ackDelayMs, dropLine, stalled })
    this.logs = new Map()
    this.pending = []
    this.members = []
    this.committed = new Map()
    this.now = 0
    this.clock = null
    this.delayed = null
    this.createCalls = []
    this.readTopicCalls = 0
    this.readAllPatterns = []
    this.kafka = {
      admin: () => ({
        connect: async () => {},
        disconnect: async () => {},
        createTopics: async ({ topics }) => {
          this.createCalls.push(topics.length)
          if (this.clock) this.clock.t += 1500
          for (const { topic, numPartitions } of topics)
            for (let p = 0; p < numPartitions; p++) this.logs.set(`${topic}/${p}`, [])
        },
        fetchTopicMetadata: async ({ topics }) => ({
          topics: topics.map((name) => ({
            name,
            partitions: [...this.logs.keys()]
              .filter((cell) => cell.startsWith(`${name}/`))
              .map((cell) => {
                const partitionId = Number(cell.split('/')[1])
                return { partitionId, leader: FakeCluster.leaderOf(cell) }
              }),
          })),
        }),
        deleteTopics: async () => {},
      }),
      producer: () => this.#producer(),
      consumer: ({ groupId }) => this.#consumer(groupId),
    }
    this.reader = {
      describeGroup: async (group) => {
        const rows = [...this.logs].map(([cell, log]) => {
          const [topic, p] = cell.split('/')
          const current = this.committed.get(`${group}|${cell}`) ?? '-'
          const lag = current === '-' ? '-' : log.length - current
          return `${group} ${topic} ${p} ${current} ${log.length} ${lag} - - -`
        })
        return {
          exit: 0,
          stderr: '',
          text: [
            `Consumer group '${group}' has no active members.`,
            '',
            'GROUP TOPIC PARTITION CURRENT-OFFSET LOG-END-OFFSET LAG CONSUMER-ID HOST CLIENT-ID',
            ...rows,
          ].join('\n'),
        }
      },
      readAll: async (pattern, onLine) => {
        this.readAllPatterns.push(pattern)
        const match = new RegExp(pattern)
        let lines = 0
        for (const [cell, log] of this.logs) {
          if (!match.test(cell.split('/')[0])) continue
          for (const entry of log) {
            if (entry.state !== 'visible') continue
            onLine(
              `Partition:${cell.split('/')[1]}\tOffset:${entry.offset}\ttopic:${entry.header}\t${entry.key}`
            )
            lines++
          }
        }
        return { exit: 0, lines, processed: lines, stderr: '' }
      },
      readTopic: async (topic, onLine) => {
        this.readTopicCalls++
        let lines = 0
        for (const [cell, log] of this.logs) {
          if (!cell.startsWith(`${topic}/`)) continue
          for (const entry of log) {
            if (entry.state !== 'visible') continue
            const line = `Partition:${cell.split('/')[1]}\tOffset:${entry.offset}\t${entry.key}`
            if (this.dropLine(topic, line)) continue
            lines++
            onLine(line)
          }
        }
        return { exit: 0, lines, processed: lines, stderr: '' }
      },
    }
  }

  /** @param {string} cell `topic/partition` @returns {number} its leader */
  static leaderOf(cell) {
    return (Number(cell.split('/')[1]) % 3) + 1
  }

  /** Called on every fake sleep: acknowledges due sends and delivers visible records. */
  tick(now) {
    this.now = now
    const ready = (p) => p.at <= now && !this.stalled(p.cells)
    const due = this.pending.filter(ready)
    this.pending = this.pending.filter((p) => !ready(p))
    due.forEach((p) => p.resolve())
    for (const member of this.members) member.emit('consumer.fetch', { duration: 5 })
    for (const member of this.members.slice(0, 1)) {
      for (const [cell, log] of this.logs) {
        const key = `${member.group}|${cell}`
        let position = member.positions.get(key) ?? 0
        const messages = []
        while (position < log.length && log[position].state !== 'open') {
          if (log[position].state === 'visible') messages.push({ offset: String(position) })
          position++
        }
        member.positions.set(key, position)
        if (messages.length === 0) continue
        const [topic, partition] = cell.split('/')
        member.eachBatch({ batch: { topic, partition: Number(partition), messages } })
      }
    }
  }

  #append(topicMessages, state) {
    const metadata = []
    const touched = []
    for (const { topic, messages } of topicMessages) {
      const first = new Map()
      for (const { partition, key, headers } of messages) {
        const log = this.logs.get(`${topic}/${partition}`)
        if (!first.has(partition)) first.set(partition, log.length)
        log.push({ offset: log.length, key, state, header: headers?.topic })
        touched.push(log[log.length - 1])
      }
      for (const [partition, baseOffset] of first)
        metadata.push({ topicName: topic, partition, errorCode: 0, baseOffset: String(baseOffset) })
    }
    return { metadata, touched }
  }

  #acknowledged(metadata) {
    const cells = metadata.map((m) => `${m.topicName}/${m.partition}`)
    const slow = this.delayed && cells.some((c) => FakeCluster.leaderOf(c) === this.delayed)
    const delay = this.ackDelayMs + (slow ? 300 : 0)
    if (delay === 0 && !this.stalled(cells)) return Promise.resolve(metadata)
    return new Promise((resolve) =>
      this.pending.push({ at: this.now + delay, cells, resolve: () => resolve(metadata) })
    )
  }

  /** @returns {{ on: Function, emit: Function }} an instrumentation emitter of one client */
  static emitter() {
    const listeners = new Map()
    return {
      on: (event, listener) => {
        listeners.set(event, [...(listeners.get(event) ?? []), listener])
        return () => {}
      },
      emit: (event, payload) => (listeners.get(event) ?? []).forEach((l) => l({ payload })),
    }
  }

  #producer() {
    const end = (touched, state) => {
      const cells = new Set()
      for (const entry of touched) entry.state = state
      for (const [cell, log] of this.logs) if (touched.some((t) => log.includes(t))) cells.add(cell)
      for (const cell of cells) {
        const log = this.logs.get(cell)
        log.push({ offset: log.length, key: null, state: 'marker' })
      }
    }
    const { on, emit } = FakeCluster.emitter()
    return {
      events: { REQUEST: 'producer.network.request' },
      on,
      connect: async () => {
        emit('producer.network.request', { apiName: 'ApiVersions', size: 99 })
        emit('producer.network.request', { apiName: 'Metadata', size: 1024 })
      },
      disconnect: async () => {},
      sendBatch: ({ topicMessages }) =>
        this.#acknowledged(this.#append(topicMessages, 'visible').metadata),
      transaction: async () => {
        const touched = []
        return {
          sendBatch: ({ topicMessages }) => {
            const sent = this.#append(topicMessages, 'open')
            touched.push(...sent.touched)
            return this.#acknowledged(sent.metadata)
          },
          commit: async () => end(touched, 'visible'),
          abort: async () => end(touched, 'aborted'),
        }
      },
    }
  }

  #consumer(group) {
    const { on, emit } = FakeCluster.emitter()
    const member = { group, positions: new Map(), eachBatch: null, emit }
    return {
      events: {
        CRASH: 'consumer.crash',
        GROUP_JOIN: 'consumer.group_join',
        FETCH: 'consumer.fetch',
        REQUEST: 'consumer.network.request',
      },
      on,
      connect: async () => emit('consumer.network.request', { apiName: 'Metadata', size: 2048 }),
      subscribe: async () => {},
      run: async ({ eachBatch }) => {
        member.eachBatch = eachBatch
        this.members.push(member)
        if (this.clock) this.clock.t += 400
        const memberAssignment = {}
        for (const cell of this.logs.keys()) {
          const [topic, partition] = cell.split('/')
          ;(memberAssignment[topic] ??= []).push(Number(partition))
        }
        emit('consumer.group_join', { duration: 250, memberAssignment })
      },
      disconnect: async () => {
        for (const [key, position] of member.positions) this.committed.set(key, position)
        this.members = this.members.filter((m) => m !== member)
      },
    }
  }
}

/**
 * Time that moves 50 ms at a time while anyone sleeps, so concurrent sleepers (the offer loop and
 * a fault schedule) each wake when the clock reaches their own deadline.
 */
const steppedTime = (onStep) => {
  const clock = {
    t: 0,
    now() {
      return this.t
    },
  }
  const sleep = async (ms, signal) => {
    const target = clock.t + ms
    while (clock.t < target && !signal?.aborted) {
      clock.t = Math.min(target, clock.t + 50)
      onStep(clock)
      await new Promise(setImmediate)
    }
  }
  return { clock, sleep }
}

/**
 * A small run: 2000 records a second for 3 s over 2 topics × 3 partitions, all three kinds.
 * `args` are further flags; `tools` builds the run's fault tools and probes from its time.
 */
const runWith = async (cluster, overrides = {}, { args = [], tools, stepped = false } = {}) => {
  const out = fs.mkdtempSync(path.join(os.tmpdir(), 'load-run-'))
  const profile = {
    ...parseLoadArgs([
      '--out',
      out,
      '--rate',
      '2000',
      '--duration',
      '3',
      '--topics',
      '2',
      '--partitions',
      '3',
      '--consumers',
      '1',
      ...args,
    ]),
    ticksPerSecond: 50,
    drainQuietSeconds: 2,
    drainSeconds: 30,
    joinSeconds: 30,
    transactionMs: 200,
    abortEvery: 3,
    recordBytes: 16,
    ...overrides,
  }
  const { clock, sleep } = (stepped ? steppedTime : fakeTime)((c) => cluster.tick(c.now()))
  cluster.clock = clock
  const report = await new LoadRun({
    profile,
    kafka: cluster.kafka,
    reader: cluster.reader,
    clock,
    sleep,
    log: () => {},
    treeSha: 'abc123',
    ...tools?.({ clock, sleep }),
  }).run()
  return { report, out }
}

const findingCounts = (oracle) =>
  Object.fromEntries(
    Object.entries(oracle.findings)
      .filter(([, f]) => f.count > 0)
      .map(([k, f]) => [k, f.count])
  )

describe('load run', () => {
  const dirs = []
  afterAll(() => dirs.forEach((dir) => fs.rmSync(dir, { recursive: true, force: true })))

  it('passes a clean run of all three producer kinds and seals every file of its artifact', async () => {
    const { report, out } = await runWith(new FakeCluster())
    dirs.push(out)
    expect(report.errors).toEqual([])
    expect(findingCounts(report.oracle)).toEqual({})
    expect(report.verdict).toEqual({ ok: true, product: 'PASS', profile: 'REACHED', exitCode: 0 })
    expect(report.totals.offered).toBe(6000)
    expect(report.rate.reached).toBe(true)
    expect(report.producers.map((p) => p.kind)).toEqual(['idempotent', 'plain', 'transactional'])
    expect(report.producers[2].stats.aborted).toBeGreaterThan(0)
    expect(report.latencyMs.samples).toBe(report.producers.reduce((n, p) => n + p.stats.acked, 0))
    const header = JSON.parse(fs.readFileSync(path.join(out, 'header.json'), 'utf8'))
    expect(header.treeSha).toBe('abc123')
    const topics = JSON.parse(fs.readFileSync(path.join(out, 'profile.json'), 'utf8')).topics
    expect(Object.keys(header.files).sort()).toEqual(
      [
        'committed.json',
        'consumers.json',
        'faults.json',
        'producers.json',
        'profile.json',
        'report.json',
        ...topics.flatMap((t) => [`apache/${t}.dump.gz`, `apache/${t}.dump.gz.end`]),
        `apache/${topics[0].replace(/-0$/, '')}-group.describe`,
      ].sort()
    )
    for (const [file, { sha256 }] of Object.entries(header.files))
      expect(
        crypto
          .createHash('sha256')
          .update(fs.readFileSync(path.join(out, file)))
          .digest('hex')
      ).toBe(sha256)
    const dump = zlib.gunzipSync(fs.readFileSync(path.join(out, `apache/${topics[0]}.dump.gz`)))
    expect(dump.toString().split('\n')[0]).toMatch(/^Partition:\d\tOffset:\d+\tp\d\.0\.\d+$/)

    const replayed = await recheck(out)
    expect(replayed.findings).toEqual(report.oracle.findings)
    expect(replayed.counts).toEqual(report.oracle.counts)
    expect(replayed.counts.records).toBeGreaterThan(5000)
  })

  it('refuses to recheck an artifact whose files differ from its seal', async () => {
    const { out } = await runWith(new FakeCluster())
    dirs.push(out)
    fs.appendFileSync(path.join(out, 'producers.json'), ' ')
    await expect(recheck(out)).rejects.toThrow('producers.json differs from the seal')
  })

  it('reports a client too slow for the offered rate as a profile failure, not a product one', async () => {
    const { report, out } = await runWith(new FakeCluster({ ackDelayMs: 1500 }), {
      unsettledSeconds: 0.1,
    })
    dirs.push(out)
    expect(findingCounts(report.oracle)).toEqual({})
    expect(report.rate.reached).toBe(false)
    expect(report.rate.shortfall).toMatch(/^sent \d+ of 6000 offered/)
    expect(report.verdict).toEqual({
      ok: false,
      product: 'PASS',
      profile: 'RATE_NOT_REACHED',
      exitCode: 3,
    })
  })

  it('fails the product when the independent read misses one acknowledged record', async () => {
    let dropped = false
    const cluster = new FakeCluster({
      dropLine: (topic, line) => {
        if (dropped || !line.startsWith('Partition:0\t') || !line.endsWith('\tp0.0.0')) return false
        dropped = true
        return true
      },
    })
    const { report, out } = await runWith(cluster)
    dirs.push(out)
    expect(dropped).toBe(true)
    expect(findingCounts(report.oracle)).toEqual({ loss: 1 })
    expect(report.verdict).toMatchObject({ product: 'FAIL', exitCode: 1 })
  })
})

describe('load run fault schedules', () => {
  const dirs = []
  afterAll(() => dirs.forEach((dir) => fs.rmSync(dir, { recursive: true, force: true })))

  /** The slow-broker tools over a fake cluster: the delay slows node 2, the pause freezes `frozen`. */
  const slowTools =
    (cluster, calls = []) =>
    ({ sleep }) => ({
      netem: {
        check: async () => calls.push('check'),
        apply: async () => {
          calls.push('delay')
          cluster.delayed = 2
        },
        clear: async () => {
          calls.push('clear')
          cluster.delayed = null
        },
      },
      bouncer: {
        step: async ({ service, kind, holdMs }) => {
          calls.push(`${kind} ${service}`)
          cluster.frozen = true
          await sleep(holdMs)
          cluster.frozen = false
          calls.push(`un${kind} ${service}`)
          return { service, kind, outageMs: holdMs }
        },
      },
    })
  const slowRun = (cluster, calls) =>
    runWith(
      cluster,
      { recordBytes: 16 },
      {
        args: ['--faults', 'slow-broker', '--duration', '195', '--rate', '300'],
        tools: slowTools(cluster, calls),
        stepped: true,
      }
    )
  const onNode2 = (cells) => cells.some((c) => FakeCluster.leaderOf(c) === 2)

  it('fails at start, before creating a topic, when the fault mechanism is unavailable', async () => {
    const cluster = new FakeCluster()
    const tools = () => ({
      netem: {
        check: async () => {
          throw new Error(
            'slow-broker needs sudo, nsenter, tc and the netem qdisc: sudo: a password is required'
          )
        },
      },
      bouncer: {},
    })
    await expect(
      runWith(cluster, {}, { args: ['--faults', 'slow-broker', '--duration', '195'], tools })
    ).rejects.toThrow('slow-broker needs sudo, nsenter, tc and the netem qdisc')
    expect(cluster.createCalls).toEqual([])
  })

  it('runs slow-broker with one send lane per leader, its phases in order, and keeps the other leaders flowing', async () => {
    const calls = []
    const cluster = new FakeCluster({ stalled: (cells) => cluster.frozen && onNode2(cells) })
    const { report, out } = await slowRun(cluster, calls)
    dirs.push(out)
    expect(calls).toEqual(['check', 'delay', 'clear', 'pause kafka2', 'unpause kafka2'])
    expect(report.faults.map((f) => f.phase)).toEqual(['slow', 'frozen', 'recovery'])
    const [slow, frozen, recovery] = report.faults
    expect(slow.startMs).toBeGreaterThanOrEqual(65000)
    expect(slow.startMs).toBeLessThanOrEqual(65100)
    expect(frozen.endMs - frozen.startMs).toBe(45000)
    expect(recovery.endMs).toBeLessThanOrEqual(130100)
    expect(report.flow.verdict).toBe('KEPT_FLOWING')
    expect(report.flow.reasons).toEqual([])
    const row = (phase, leader) =>
      report.flow.table.find((r) => r.phase === phase && r.leader === leader)
    expect(row('frozen', '2').ackedRate).toBe(0)
    expect(row('frozen', '1').ackedRate).toBeGreaterThan(0.9 * row('frozen', '1').offeredRate)
    expect(row('recovery', '2').ackedRate).toBeGreaterThan(0.9 * row('recovery', '2').offeredRate)
    expect(report.memory.map((m) => m.phase)).toEqual([
      'baseline',
      'slow',
      'frozen',
      'recovery',
      'after',
    ])
    expect(report.memory.every((m) => m.peakHeapUsed > 0 && m.endRss > 0)).toBe(true)
    expect(findingCounts(report.oracle)).toEqual({})
    expect(report.verdict).toEqual({
      ok: true,
      product: 'PASS',
      profile: 'REACHED',
      flow: 'KEPT_FLOWING',
      exitCode: 0,
    })
  })

  it("reports a stalled flow, exit 4, when the freeze holds back every leader's sends", async () => {
    const cluster = new FakeCluster({ stalled: () => cluster.frozen })
    const { report, out } = await slowRun(cluster)
    dirs.push(out)
    expect(report.flow.verdict).toBe('STALLED')
    expect(report.flow.reasons).toEqual(
      expect.arrayContaining([
        expect.stringMatching(/^frozen: leader 1 stalled 4\d s \(most 20 s\)$/),
      ])
    )
    expect(findingCounts(report.oracle)).toEqual({})
    expect(report.verdict).toMatchObject({ product: 'PASS', flow: 'STALLED', exitCode: 4 })
  })

  it('reports rolling restarts cut short by the window end as restarts not run, not as a driver error', async () => {
    const steps = []
    const { report, out } = await runWith(
      new FakeCluster(),
      {},
      {
        args: ['--faults', 'rolling-restarts', '--duration', '6'],
        tools: ({ sleep }) => ({
          bouncer: {
            async rollingBounce({ hard = false, between = 0, services, signal }) {
              const outages = []
              for (const service of services ?? ['kafka1', 'kafka2', 'kafka3']) {
                if (outages.length && between) await sleep(between)
                signal?.throwIfAborted()
                steps.push(service)
                await sleep(500)
                outages.push({ service, kind: hard ? 'kill' : 'stop', outageMs: 500 })
              }
              return outages
            },
          },
        }),
        stepped: true,
      }
    )
    dirs.push(out)
    expect(steps).toEqual(['kafka1'])
    expect(report.errors).toEqual([])
    expect(report.faults.map((f) => [f.kind, f.service, f.outageMs ?? f.skipped])).toEqual([
      ['stop', 'kafka1', 500],
      ['stop', 'kafka2', 'window ended'],
      ['stop', 'kafka3', 'window ended'],
      ['kill', 'kafka1', 'window ended'],
      ['kill', 'kafka2', 'window ended'],
      ['kill', 'kafka3', 'window ended'],
    ])
    expect(report.verdict).toEqual({ ok: true, product: 'PASS', profile: 'REACHED', exitCode: 0 })
  })

  it('reports the many-topics measurements: topic creation, first assignment, rebalances, metadata requests and fetch rounds', async () => {
    const cluster = new FakeCluster()
    const bounces = []
    const { report, out } = await runWith(
      cluster,
      {},
      {
        args: ['--faults', 'many-topics', '--topics', '1000', '--partitions', '1'],
        tools: () => ({
          bouncer: {
            rollingBounce: async ({ topics }) => {
              bounces.push(topics.length)
              return [{ service: 'kafka1', kind: 'stop', outageMs: 1 }]
            },
          },
          lagProbe: { start() {}, stop() {}, maxBetween: () => 42 },
        }),
        stepped: true,
      }
    )
    dirs.push(out)
    expect(bounces).toEqual([1000])
    expect(cluster.createCalls).toEqual([250, 250, 250, 250])
    expect(report.cluster.createTopicsMs).toBe(6000)
    expect(report.cluster.firstAssignmentMs).toEqual({ c0: 400 })
    expect(report.cluster.rebalances).toEqual([
      { member: 'c0', atMs: expect.any(Number), joinMs: 250, partitions: 1000, loopDelayMaxMs: 42 },
    ])
    expect(report.cluster.metadata).toEqual({
      requests: 4,
      perMinute: [4],
      maxPerMinute: 4,
      meanBytes: 1280,
      maxBytes: 2048,
    })
    expect(report.cluster.fetchRoundMs.samples).toBeGreaterThan(10)
    expect(report.cluster.fetchRoundMs.max).toBe(5)
    expect(report.faults.map((f) => f.phase ?? `${f.kind} ${f.skipped ?? f.outageMs}`)).toEqual([
      'rolling-restart',
      'stop 1',
      'stop window ended',
      'stop window ended',
    ])

    expect(cluster.readTopicCalls).toBe(0)
    expect(cluster.readAllPatterns).toHaveLength(1)
    expect(report.oracle.reads).toEqual([
      expect.objectContaining({ topic: '*', exit: 0, lines: report.oracle.counts.records }),
    ])
    expect(findingCounts(report.oracle)).toEqual({})
    expect(report.oracle.counts.records).toBeGreaterThan(5000)
    expect(report.verdict).toEqual({ ok: true, product: 'PASS', profile: 'REACHED', exitCode: 0 })
    const replayed = await recheck(out)
    expect(replayed.findings).toEqual(report.oracle.findings)
    expect(replayed.counts).toEqual(report.oracle.counts)
  })
})

describe('load run group describe', () => {
  it('reads committed offsets and log ends, leaving out a partition with none committed', () => {
    const text = [
      "Consumer group 'g' has no active members.",
      '',
      'GROUP TOPIC PARTITION CURRENT-OFFSET LOG-END-OFFSET LAG CONSUMER-ID HOST CLIENT-ID',
      'g t 0 10 12 2 - - -',
      'g t 1 - 5 - - - -',
    ].join('\n')
    expect(parseDescribe(text, 'g')).toEqual({
      committed: { 't/0': 10 },
      logEnds: { 't/0': 12, 't/1': 5 },
    })
    expect(() => parseDescribe('nothing', 'g')).toThrow('no describe header for group g')
  })
})
