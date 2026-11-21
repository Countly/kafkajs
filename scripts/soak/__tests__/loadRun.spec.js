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
 * and Apache's reader and group describe over the same logs.
 */
class FakeCluster {
  /** @param {{ ackDelayMs?: number, dropLine?: (topic: string, line: string) => boolean }} [options] */
  constructor({ ackDelayMs = 0, dropLine = () => false } = {}) {
    Object.assign(this, { ackDelayMs, dropLine })
    this.logs = new Map()
    this.pending = []
    this.members = []
    this.committed = new Map()
    this.now = 0
    this.kafka = {
      admin: () => ({
        connect: async () => {},
        disconnect: async () => {},
        createTopics: async ({ topics }) => {
          for (const { topic, numPartitions } of topics)
            for (let p = 0; p < numPartitions; p++) this.logs.set(`${topic}/${p}`, [])
        },
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
      readTopic: async (topic, onLine) => {
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

  /** Called on every fake sleep: acknowledges due sends and delivers visible records. */
  tick(now) {
    this.now = now
    const due = this.pending.filter((p) => p.at <= now)
    this.pending = this.pending.filter((p) => p.at > now)
    due.forEach((p) => p.resolve())
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
      for (const { partition, key } of messages) {
        const log = this.logs.get(`${topic}/${partition}`)
        if (!first.has(partition)) first.set(partition, log.length)
        log.push({ offset: log.length, key, state })
        touched.push(log[log.length - 1])
      }
      for (const [partition, baseOffset] of first)
        metadata.push({ topicName: topic, partition, errorCode: 0, baseOffset: String(baseOffset) })
    }
    return { metadata, touched }
  }

  #acknowledged(value) {
    if (this.ackDelayMs === 0) return Promise.resolve(value)
    return new Promise((resolve) =>
      this.pending.push({ at: this.now + this.ackDelayMs, resolve: () => resolve(value) })
    )
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
    return {
      connect: async () => {},
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
    const member = { group, positions: new Map(), eachBatch: null }
    return {
      events: { CRASH: 'consumer.crash' },
      on: () => () => {},
      connect: async () => {},
      subscribe: async () => {},
      run: async ({ eachBatch }) => {
        member.eachBatch = eachBatch
        this.members.push(member)
      },
      disconnect: async () => {
        for (const [key, position] of member.positions) this.committed.set(key, position)
        this.members = this.members.filter((m) => m !== member)
      },
    }
  }
}

/** A small run: 2000 records a second for 3 s over 2 topics × 3 partitions, all three kinds. */
const runWith = async (cluster, overrides = {}) => {
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
  const { clock, sleep } = fakeTime((c) => cluster.tick(c.now()))
  const report = await new LoadRun({
    profile,
    kafka: cluster.kafka,
    reader: cluster.reader,
    clock,
    sleep,
    log: () => {},
    treeSha: 'abc123',
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
