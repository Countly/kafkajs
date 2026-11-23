const fs = require('node:fs')
const os = require('node:os')
const path = require('node:path')
const { spawnSync } = require('node:child_process')
const core = require('../finalSoakCore')
const fx = require('./finalSoakCoreFixture')

const { Journal, judge, checkArtifact, formatReport } = core
const dirs = []
const tmp = () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'final-soak-core-'))
  dirs.push(dir)
  return dir
}
afterAll(() => dirs.forEach((dir) => fs.rmSync(dir, { recursive: true, force: true })))

/** Judges a world: its Apache files go to disk, its records into a journal (after `edit`). */
async function check(
  world,
  { edit = (records) => records, header = (h) => h, supervisor = (s) => s, capacity } = {}
) {
  const dir = tmp()
  const files = []
  for (const [name, text] of Object.entries(world.apache)) {
    fs.mkdirSync(path.dirname(path.join(dir, 'apache', name)), { recursive: true })
    fs.writeFileSync(path.join(dir, 'apache', name), text)
    files.push({ file: `apache/${name}`, bytes: Buffer.byteLength(text), sha256: fx.sha256(text) })
  }
  fx.writeHeap(world, dir)
  for (const name of fs.readdirSync(path.join(dir, 'heap'))) {
    const bytes = fs.readFileSync(path.join(dir, 'heap', name))
    files.push({ file: `heap/${name}`, bytes: bytes.length, sha256: fx.sha256(bytes) })
  }
  const { matrix, config, mode } = world
  const journal = new Journal({ matrix, config, mode, capacity })
  edit(world.records()).forEach((r) => journal.add(r))
  const base = { ...fx.copy(world.header), files }
  return judge({
    dir,
    matrix,
    config,
    mode,
    header: header(base),
    supervisor: supervisor(fx.copy(world.supervisor)),
    journal,
    problems: [],
  })
}
const row = (report, id) => report.rows.find((r) => r.id === id)
/** Asserts a row's verdict and that one reason matches. */
const expectRow = (report, id, verdict, reason) => {
  const r = row(report, id)
  expect({ id, verdict: r.verdict }).toEqual({ id, verdict })
  if (reason) expect(r.reasons.join('\n')).toMatch(reason)
}
const without = (predicate) => (records) => records.filter((r) => !predicate(r))
const isEntry = (group, id) => (r) =>
  r.kind === 'identity' && r.stage === 'handler-enter' && r.group === group && r.id === id
const groupOf = (slot) => `g-${fx.RUN}-${slot}`
const dumpOf = (world, role) => world.apache[`${role}.dump`].split('\n').filter(Boolean)
/** @returns {object} a copy of `world` whose log layout has `role` replaced by `entries` */
const relayout = (world, role, entries) => ({
  ...world,
  layout: { ...world.layout, [role]: entries },
})
const mapRecords = (predicate, fields) => (records) =>
  records.map((r) => (predicate(r) ? { ...r, ...fields(r) } : r))
const isSample = (process, role) => (r) =>
  r.kind === 'sample' && r.process === process && r.role === role
/** Writes the world as an artifact, lets `mutate(dir)` change it and checks it from disk. */
const onDisk = async (world, mutate = () => {}) => {
  const dir = fx.writeArtifact(world, tmp())
  mutate(dir)
  return checkArtifact(dir)
}
const smoke = () => fx.healthy({ mode: 'smoke', withInventory: true })
const rehearsal = () => fx.healthy({ mode: 'rehearsal', withInventory: true })

describe('scripts/evidence/finalSoakCore', () => {
  describe('a healthy acceptance run', () => {
    it('is ACCEPTED by K1 to K7 and says it is a core verdict with the rows it does not judge', async () => {
      const report = await check(fx.healthy())
      expect(report.rows.map((r) => `${r.id}:${r.verdict}`)).toEqual(
        ['K1', 'K2', 'K3', 'K4', 'K5', 'K6', 'K7'].map((id) => `${id}:PASS`)
      )
      expect(report.overall).toBe('ACCEPTED')
      const text = formatReport(report)
      expect(text).toMatch(/CORE VERDICT: ACCEPTED/)
      expect(text).toMatch(/core checker.*K1.*K7.*NOT JUDGED/is)
    })

    it('prints every other contract row as NOT JUDGED with its CI lane and no NOT JUDGED line says PASS', async () => {
      const text = formatReport(await check(fx.healthy()))
      const notJudged = text.split('\n').filter((l) => /NOT JUDGED/.test(l) && /lane/.test(l))
      expect(notJudged.length).toBe(core.NOT_JUDGED.length)
      for (const item of core.NOT_JUDGED) {
        const line = notJudged.find((l) => l.includes(item.name))
        expect(line).toMatch(new RegExp(`lane: ${item.lane}`))
      }
      expect(text.split('\n').filter((l) => /NOT JUDGED/.test(l) && /PASS/.test(l))).toEqual([])
      expect(core.NOT_JUDGED.map((i) => i.name).join('|')).toMatch(
        /share lock lifecycle.*ownership tables.*seed offset volume.*latency.*cooperative round.*restart leave.*receipts identity.*companions.*recreate/s
      )
    })

    it('lists the seed offset volume, receipt identity, share lock and checkpoint rows as NOT JUDGED even when the supervisor receipt contradicts the run', async () => {
      const report = await check(fx.healthy(), {
        supervisor: (s) => ({ ...s, runId: 'ffffffffffff' }),
      })
      expectRow(report, 'K7', 'FAIL', /runId.*differs/)
      const names = report.notJudged.map((i) => i.name)
      for (const part of ['seed offset volume', 'receipts identity', 'share lock', 'checkpoints'])
        expect(names.some((n) => n.includes(part))).toBe(true)
      expect(report.notJudged.every((i) => i.verdict === 'NOT JUDGED')).toBe(true)
    })
  })

  describe('K1 no lost record', () => {
    it('fails a record acknowledged but absent from the dump', async () => {
      const world = fx.healthy()
      fx.redump(
        world,
        'main',
        dumpOf(world, 'main').filter((l) => !l.includes('\tc08.5\t'))
      )
      const report = await check(world)
      expectRow(report, 'K1', 'FAIL', /acknowledged.*absent.*c08\.5/)
      expect(report.overall).toBe('REJECTED')
    })

    it('fails a member of a committed transaction absent from the dump', async () => {
      const world = fx.healthy()
      fx.redump(
        world,
        'transactions',
        dumpOf(world, 'transactions').filter((l) => !l.includes('\tc09.1\t'))
      )
      expectRow(await check(world), 'K1', 'FAIL', /absent.*c09\.1/)
    })

    it('fails a committed transaction member whose send result was unknown and which never reached the log', async () => {
      const world = fx.healthy()
      world.layout.transactions[2] = { ...world.layout.transactions[2], absent: true }
      world.apache = fx.renderApache(world)
      expectRow(await check(world), 'K1', 'FAIL', /absent.*c09\.2/)
    })

    it('fails acknowledged records whose offsets lie at or beyond the log end even when the dump is not proved complete', async () => {
      const world = fx.healthy()
      world.apache = fx.renderApache(relayout(world, 'main', world.layout.main.slice(0, 9)))
      fx.redump(world, 'main', dumpOf(world, 'main').slice(0, 8))
      expectRow(await check(world), 'K1', 'FAIL', /3 acknowledged.*inside the log.*c08\.9/)
    })

    it('fails a member of an aborted transaction found in the dump', async () => {
      const world = fx.healthy()
      const aborted = `Partition:0\tOffset:5\tc09.4\t${fx.valueOf('c09.4', 'c09.0.x2')}`
      const lines = dumpOf(world, 'transactions')
      fx.redump(world, 'transactions', [...lines, aborted])
      expectRow(await check(world), 'K1', 'FAIL', /aborted.*present.*c09\.4/)
    })

    it('fails a record that is in the log twice', async () => {
      const world = fx.healthy()
      world.layout.main.splice(5, 0, { ...world.layout.main[4], duplicate: true })
      world.apache = fx.renderApache(world)
      expectRow(await check(world), 'K1', 'FAIL', /more than once.*c08\.4/)
    })

    it('fails a dump record the journal never offered', async () => {
      const world = fx.healthy()
      world.layout.main.push({ id: 'c08.99', duplicate: true })
      world.apache = fx.renderApache(world)
      expectRow(await check(world), 'K1', 'FAIL', /never offered.*c08\.99/)
    })

    it('fails an id found at an offset other than the acknowledged one', async () => {
      const world = fx.healthy()
      const edit = mapRecords(
        (r) => r.stage === 'ack' && r.id === 'c08.3',
        () => ({ offset: '30' })
      )
      expectRow(await check(world, { edit }), 'K1', 'FAIL', /offset.*c08\.3/)
    })

    it('fails a record whose bytes differ from what was offered', async () => {
      const world = fx.healthy()
      world.apache['main.dump'] = world.apache['main.dump'].replace(/(c08\.2\t[^\t\n]*?)\./, '$1x')
      expectRow(await check(world), 'K1', 'FAIL', /differ.*c08\.2/)
    })

    it.each([['-1'], [null]])(
      'accepts an acknowledged send whose offset the broker did not return (%s) when it is in the dump once',
      async (offset) => {
        const edit = mapRecords(
          (r) => r.stage === 'ack' && r.id === 'c08.3',
          () => ({ offset })
        )
        const report = await check(fx.healthy(), { edit })
        expectRow(report, 'K1', 'PASS')
        expect(report.overall).toBe('ACCEPTED')
      }
    )

    it('fails an acknowledged send with an unknown offset (-1) that is absent from a complete dump', async () => {
      const world = fx.healthy()
      world.layout.main[3] = { ...world.layout.main[3], absent: true }
      world.apache = fx.renderApache(world)
      const edit = mapRecords(
        (r) => r.stage === 'ack' && r.id === 'c08.3',
        () => ({ outcome: 'acknowledged', offset: '-1', errorType: null })
      )
      expectRow(await check(world, { edit }), 'K1', 'FAIL', /1 acknowledged.*absent.*c08\.3/)
    })

    it('is inconclusive, never ACCEPTED, when a second contradictory send result follows a lost acknowledged record', async () => {
      const world = fx.healthy()
      world.apache = fx.renderApache(relayout(world, 'main', world.layout.main.slice(0, 11)))
      const edit = (records) => {
        const ack = records.find((r) => r.stage === 'ack' && r.id === 'c08.11')
        const rejected = { outcome: 'rejected', offset: null, errorType: 'KafkaJSProtocolError' }
        return [...records, { ...ack, t: 9e7, ...rejected }]
      }
      const report = await check(world, { edit })
      expectRow(report, 'K1', 'INCONCLUSIVE', /1 ids or transactions have contradictory.*c08\.11/)
      expect(report.overall).not.toBe('ACCEPTED')
    })

    it('is inconclusive, never ACCEPTED, when a second transaction record says a lost committed transaction aborted', async () => {
      const world = fx.healthy()
      const aborted = world.layout.transactions.map((e) =>
        e.transaction === 'c09.0.x1'
          ? { ...e, invisible: true }
          : e.marker
            ? { marker: 'abort' }
            : e
      )
      world.apache = fx.renderApache(relayout(world, 'transactions', aborted))
      const edit = (records) => {
        const x1 = records.find((r) => r.stage === 'transaction' && r.transaction === 'c09.0.x1')
        const abort = { decision: 'abort', outcome: 'aborted', call: 'c09.0.x1.abort' }
        return [...records, { ...x1, t: 9e7, ...abort }]
      }
      const report = await check(world, { edit })
      expectRow(report, 'K1', 'INCONCLUSIVE', /contradictory.*c09\.0\.x1/)
      expect(row(report, 'K1').reasons.join('\n')).toMatch(/contradicted outcome/)
      expect(report.overall).not.toBe('ACCEPTED')
    })

    it('accepts a complete transactions dump when a retried marker added one more control offset before its last record', async () => {
      const world = fx.healthy()
      const [committed, aborted] = [
        world.layout.transactions.slice(0, 5),
        world.layout.transactions.slice(5),
      ]
      world.layout.transactions = [...aborted, { marker: 'abort' }, ...committed]
      world.apache = fx.renderApache(world)
      const report = await check(world)
      expectRow(report, 'K1', 'PASS')
      expect(row(report, 'K1').numbers.logEndTransactions).toBe(9)
      expect(report.overall).toBe('ACCEPTED')
    })

    it('is inconclusive when a retried marker follows the last dumped record, which a cut-off dump cannot be told apart from', async () => {
      const world = fx.healthy()
      world.layout.transactions.splice(5, 0, { marker: 'commit' })
      world.apache = fx.renderApache(world)
      const report = await check(world)
      expectRow(
        report,
        'K1',
        'INCONCLUSIVE',
        /5 offsets follow the last dumped record but the journal places 4 invisible records there/
      )
      expect(report.overall).not.toBe('ACCEPTED')
    })

    it('is inconclusive, never ACCEPTED, when a cut-off transactions dump hides a visible record after its last line', async () => {
      const world = fx.healthy()
      world.layout.transactions.splice(5, 0, { ...world.layout.transactions[1], duplicate: true })
      world.apache = fx.renderApache(world)
      fx.redump(world, 'transactions', dumpOf(world, 'transactions').slice(0, 4))
      const report = await check(world)
      expectRow(report, 'K1', 'INCONCLUSIVE', /not proved complete/)
      expect(report.overall).not.toBe('ACCEPTED')
    })

    it('does not let an aborted member acknowledged beyond the log end explain an offset after the last dumped record', async () => {
      const world = fx.healthy()
      world.layout.transactions.splice(5, 0, { ...world.layout.transactions[1], duplicate: true })
      world.apache = fx.renderApache(world)
      fx.redump(world, 'transactions', dumpOf(world, 'transactions').slice(0, 4))
      const edit = (records) => {
        const offer = records.find((r) => r.stage === 'offered' && r.id === 'c09.4')
        const ack = records.find((r) => r.stage === 'ack' && r.id === 'c09.4')
        return [
          ...records,
          { ...offer, id: 'c09.6', t: offer.t + 0.5 },
          { ...ack, id: 'c09.6', offset: '50', t: ack.t + 0.5 },
        ]
      }
      const report = await check(world, { edit })
      expectRow(
        report,
        'K1',
        'INCONCLUSIVE',
        /an acknowledged aborted record claims offset 50, at or beyond the log end/
      )
      expect(report.overall).not.toBe('ACCEPTED')
    })

    describe('every offset of the transactions log up to its end is explained by exactly one fact', () => {
      const tx = (world) => world.layout.transactions
      const ackAt = (id, offset) =>
        mapRecords(
          (r) => r.stage === 'ack' && r.id === id,
          () => ({ outcome: 'acknowledged', errorType: null, offset })
        )
      const render = (world) => {
        world.apache = fx.renderApache(world)
        return world
      }

      it('accepts a log that ends in the nontransactional closing record, leaving nothing after the last dumped record', async () => {
        const report = await check(fx.closed())
        expectRow(report, 'K1', 'PASS')
        expect(row(report, 'K1').numbers).toMatchObject({
          dumpedTransactions: 5,
          logEndTransactions: 9,
        })
        expect(report.overall).toBe('ACCEPTED')
      })

      it('accepts a retried marker before the closing record', async () => {
        const world = fx.closed()
        tx(world).splice(-1, 0, { marker: 'commit' })
        const report = await check(render(world))
        expectRow(report, 'K1', 'PASS')
        expect(row(report, 'K1').numbers.logEndTransactions).toBe(10)
      })

      it('is inconclusive, never ACCEPTED, when a dump of a closed log is cut off before the closing record', async () => {
        const world = fx.closed()
        fx.redump(world, 'transactions', dumpOf(world, 'transactions').slice(0, 4))
        const report = await check(world)
        expectRow(report, 'K1', 'INCONCLUSIVE', /not proved complete/)
        expect(report.overall).not.toBe('ACCEPTED')
      })

      it('is inconclusive, never ACCEPTED, when two acknowledged aborted records claim one offset after a cut-off dump that hides a visible record', async () => {
        const world = fx.healthy()
        tx(world).splice(
          6,
          1,
          { id: 'c09.5', transaction: 'c09.0.x2', absent: true },
          { id: 'c08.99', duplicate: true }
        )
        render(world)
        fx.redump(world, 'transactions', dumpOf(world, 'transactions').slice(0, 4))
        const report = await check(world, { edit: ackAt('c09.5', '5') })
        expectRow(report, 'K1', 'INCONCLUSIVE', /two acknowledged aborted records claim offset 5/)
        expect(report.overall).not.toBe('ACCEPTED')
      })

      it('is inconclusive when two acknowledged aborted records claim one offset even though every other offset is explained', async () => {
        const world = fx.healthy()
        tx(world).splice(
          6,
          1,
          { id: 'c09.5', transaction: 'c09.0.x2', absent: true },
          { id: 'c09.6', transaction: 'c09.0.x2', invisible: true }
        )
        const report = await check(render(world), { edit: ackAt('c09.5', '5') })
        expectRow(report, 'K1', 'INCONCLUSIVE', /two acknowledged aborted records claim offset 5/)
        expect(row(report, 'K1').reasons).toHaveLength(1)
      })

      it('is inconclusive when an acknowledged aborted record claims the offset of a dumped record', async () => {
        const report = await check(fx.closed(), { edit: ackAt('c09.4', '2') })
        expectRow(
          report,
          'K1',
          'INCONCLUSIVE',
          /an acknowledged aborted record claims offset 2, where the dump holds a visible record/
        )
        expect(row(report, 'K1').reasons).toHaveLength(1)
      })

      it('is inconclusive when an acknowledged aborted record of the main topic claims the offset of a dumped main record', async () => {
        const edit = (records) => {
          const offer = records.find((r) => r.stage === 'offered' && r.id === 'c08.6')
          const ack = records.find((r) => r.stage === 'ack' && r.id === 'c08.6')
          return [
            ...records,
            { ...offer, id: 'c08.20', transaction: 'c09.0.x2', t: 9e7 },
            { ...ack, id: 'c08.20', t: 9e7 + 1 },
          ]
        }
        const report = await check(fx.closed(), { edit })
        expectRow(
          report,
          'K1',
          'INCONCLUSIVE',
          /the main dump .*an acknowledged aborted record claims offset 6, where the dump holds a visible record/
        )
        expect(row(report, 'K1').reasons).toHaveLength(1)
      })

      it('is inconclusive when an acknowledged aborted record claims an offset at or beyond the log end', async () => {
        const edit = (records) => {
          const offer = records.find((r) => r.stage === 'offered' && r.id === 'c09.4')
          const ack = records.find((r) => r.stage === 'ack' && r.id === 'c09.4')
          return [
            ...records,
            { ...offer, id: 'c09.6', t: offer.t + 0.5 },
            { ...ack, id: 'c09.6', offset: '50', t: ack.t + 0.5 },
          ]
        }
        const report = await check(fx.closed(), { edit })
        expectRow(
          report,
          'K1',
          'INCONCLUSIVE',
          /an acknowledged aborted record claims offset 50, at or beyond the log end/
        )
        expect(row(report, 'K1').reasons).toHaveLength(1)
      })

      it('is inconclusive when a closed log holds fewer invisible offsets than its decided transactions need', async () => {
        const world = fx.closed()
        tx(world).splice(4, 1)
        const report = await check(render(world))
        expectRow(
          report,
          'K1',
          'INCONCLUSIVE',
          /5 records dumped but at least 4 offsets are invisible/
        )
        expect(row(report, 'K1').reasons).toHaveLength(1)
      })

      it('is inconclusive when the journal places more invisible records after the last dumped record than offsets follow it', async () => {
        const world = fx.healthy()
        const [first, rest] = [tx(world).slice(0, 1), tx(world).slice(1, 7)]
        world.layout.transactions = [...first, { marker: 'commit' }, ...rest]
        const report = await check(render(world))
        expectRow(
          report,
          'K1',
          'INCONCLUSIVE',
          /3 offsets follow the last dumped record but the journal places 4 invisible records there/
        )
        expect(row(report, 'K1').reasons).toHaveLength(1)
      })

      it('is inconclusive when a nontransactional record of the transactions topic was acknowledged after the last dumped record', async () => {
        const world = fx.healthy()
        world.layout.transactions = tx(world).slice(0, 5)
        const edit = (records) => {
          const offer = records.find((r) => r.stage === 'offered' && r.id === 'c09.0')
          const ack = records.find((r) => r.stage === 'ack' && r.id === 'c09.0')
          return [
            ...records,
            { ...offer, id: 'c08.12', transaction: null, t: 9e7 },
            { ...ack, id: 'c08.12', offset: '4', t: 9e7 + 1 },
          ]
        }
        const report = await check(render(world), { edit })
        expectRow(report, 'K1', 'INCONCLUSIVE', /a visible record was acknowledged at 4/)
        expect(report.overall).not.toBe('ACCEPTED')
      })

      it('is inconclusive, never ACCEPTED, when as many offsets follow a cut-off dump as the journal places there but a marker has no offset after its last record', async () => {
        const world = fx.healthy()
        const [x1, aborted] = [tx(world).slice(0, 5), tx(world).slice(5, 7)]
        world.layout.transactions = [...x1, { id: 'c08.99', duplicate: true }, ...aborted]
        render(world)
        fx.redump(world, 'transactions', dumpOf(world, 'transactions').slice(0, 4))
        const report = await check(world)
        expectRow(report, 'K1', 'INCONCLUSIVE', /a control marker .* has no free offset after/)
        expect(row(report, 'K1').reasons).toHaveLength(1)
        expect(report.overall).not.toBe('ACCEPTED')
      })
    })

    it.each([
      ['transactions', 'c09.3', 3],
      ['main', 'c08.11', 11],
    ])(
      'is inconclusive, never ACCEPTED, when a %s dump record lies beyond the log end every describe reports',
      async (role, id, offset) => {
        const world = fx.healthy()
        const moved = (r) => r.id === id && (r.stage === 'ack' || r.stage === 'handler-enter')
        const edit = mapRecords(moved, () => ({ offset: '100' }))
        const at = `Offset:${offset}\t${id}\t`
        fx.redump(
          world,
          role,
          dumpOf(world, role).map((l) =>
            l.includes(at) ? l.replace(at, `Offset:100\t${id}\t`) : l
          )
        )
        const report = await check(world, { edit })
        expectRow(report, 'K1', 'INCONCLUSIVE', /offset 100 lies at or beyond the log end/)
        expect(report.overall).not.toBe('ACCEPTED')
      }
    )

    it('is inconclusive for a manifest-bound smoke artifact whose dump record lies beyond the described log end', async () => {
      const world = smoke()
      const base = world.records
      world.records = () =>
        base().map((r) =>
          r.id === 'c09.3' && (r.stage === 'ack' || r.stage === 'handler-enter')
            ? { ...r, offset: '100' }
            : r
        )
      fx.redump(
        world,
        'transactions',
        dumpOf(world, 'transactions').map((l) =>
          l.replace('Offset:3\tc09.3\t', 'Offset:100\tc09.3\t')
        )
      )
      const report = await onDisk(world)
      expectRow(report, 'K1', 'INCONCLUSIVE', /offset 100 lies at or beyond the log end/)
      expectRow(report, 'K7', 'PASS')
    })

    it('is inconclusive when the transactions dump stops before a visible record the journal acknowledged', async () => {
      const world = fx.healthy()
      fx.redump(world, 'transactions', dumpOf(world, 'transactions').slice(0, 3))
      expectRow(await check(world), 'K1', 'INCONCLUSIVE', /acknowledged at 3.*not proved complete/)
    })

    it('is inconclusive when the log end leaves fewer invisible offsets than the journal explains', async () => {
      const world = fx.healthy()
      const early = world.layout.transactions.slice(0, -1)
      world.apache = fx.renderApache(relayout(world, 'transactions', early))
      expectRow(await check(world), 'K1', 'INCONCLUSIVE', /at least 4 offsets are invisible/)
    })

    it('is inconclusive when the dump stops before the log end, never a pass', async () => {
      const world = fx.healthy()
      fx.redump(world, 'main', dumpOf(world, 'main').slice(0, 9))
      const report = await check(world)
      expectRow(report, 'K1', 'INCONCLUSIVE', /stops at offset 8.*log ends at 12/)
      expect(row(report, 'K1').reasons.join('\n')).toMatch(/^3 acknowledged.*absent.*c08\.9/m)
      expect(report.overall).not.toBe('ACCEPTED')
    })

    it('is inconclusive when sends with unknown results are absent from the log', async () => {
      const world = fx.healthy()
      world.layout.main[7] = { ...world.layout.main[7], absent: true }
      world.apache = fx.renderApache(world)
      expectRow(await check(world), 'K1', 'INCONCLUSIVE', /1 unknown-result sends absent.*c08\.7/)
    })

    it('is inconclusive when a transaction has no confirmed outcome', async () => {
      const edit = mapRecords(
        (r) => r.stage === 'transaction' && r.decision === 'abort',
        () => ({ outcome: 'unknown' })
      )
      expectRow(await check(fx.healthy(), { edit }), 'K1', 'INCONCLUSIVE', /transaction.*unknown/)
    })

    it.each([
      [
        'an id offered twice',
        (records) => [...records, records.find((r) => r.stage === 'offered')],
        /c08\.0 is offered twice/,
      ],
      [
        'an id offered on another partition',
        mapRecords(
          (r) => r.stage === 'offered' && r.id === 'c08.2',
          (r) => ({ key: { ...r.key, partition: 1 } })
        ),
        /c08\.2 is on partition 1/,
      ],
      [
        'a send result for an id never offered',
        (records) => [
          ...records,
          { ...records.find((r) => r.stage === 'ack'), id: 'c08.77', t: 9e7 },
        ],
        /c08\.77 is acknowledged but was never offered/,
      ],
      [
        'an offset beyond 2^53',
        mapRecords(
          (r) => r.stage === 'ack' && r.id === 'c08.3',
          () => ({ offset: '9007199254740993' })
        ),
        /c08\.3 has an offset beyond 2\^53/,
      ],
    ])('leaves K1 to K6 inconclusive for a journal with %s', async (_, edit, reason) => {
      const report = await check(fx.healthy(), { edit })
      for (const id of ['K1', 'K2', 'K3', 'K5', 'K6']) expectRow(report, id, 'INCONCLUSIVE', reason)
    })
  })

  describe('ground-truth files', () => {
    const cases = [
      ['main.dump', 'K1'],
      ['transactions.dump', 'K1'],
      [`groups/${groupOf('c01')}.describe`, 'K4'],
    ]
    it.each(cases)('is inconclusive when %s is missing', async (file, id) => {
      const world = fx.healthy()
      for (const name of [file, `${file}.end`, `${file}.stderr`]) delete world.apache[name]
      const report = await check(world)
      expectRow(report, id, 'INCONCLUSIVE', new RegExp(file.replace('.', '\\.')))
      expect(report.overall).not.toBe('ACCEPTED')
    })

    it.each(cases)(
      'is inconclusive when %s was cut off before its end marker',
      async (file, id) => {
        const world = fx.healthy()
        delete world.apache[`${file}.end`]
        expectRow(await check(world), id, 'INCONCLUSIVE', /end marker/)
      }
    )

    it.each(cases)('is inconclusive when %s has a line that does not parse', async (file, id) => {
      const world = fx.healthy()
      const text = world.apache[file]
      const bad = file.endsWith('.dump') ? 'this is not a dump line\n' : 'GROUP TOPIC garbage\n'
      world.apache[file] = file.endsWith('.dump')
        ? `${text}${bad}`
        : text.replace(/\n$/, `\n${bad}`)
      world.apache[`${file}.end`] = `exit=0 lines=${world.apache[file].split('\n').length - 1}\n`
      if (file.endsWith('.dump'))
        world.apache[`${file}.stderr`] =
          `Processed a total of ${world.apache[file].split('\n').length - 1} messages\n`
      expectRow(await check(world), id, 'INCONCLUSIVE', /unparseable|does not parse/)
    })

    it.each(cases)('is inconclusive when %s exited non-zero or was killed', async (file, id) => {
      for (const status of ['1', '-1']) {
        const world = fx.healthy()
        world.apache[`${file}.end`] = world.apache[`${file}.end`].replace(
          'exit=0',
          `exit=${status}`
        )
        expectRow(await check(world), id, 'INCONCLUSIVE', new RegExp(`exit status was ${status}`))
      }
    })

    it('is inconclusive when the dump holds fewer lines than its end marker counted', async () => {
      const world = fx.healthy()
      world.apache['main.dump'] = dumpOf(world, 'main').slice(0, 6).join('\n') + '\n'
      expectRow(
        await check(world),
        'K1',
        'INCONCLUSIVE',
        /has 6 lines but its end marker counted 12/
      )
    })

    it('is inconclusive when the last dump line has no newline', async () => {
      const world = fx.healthy()
      world.apache['main.dump'] = world.apache['main.dump'].replace(/\n$/, '')
      expectRow(await check(world), 'K1', 'INCONCLUSIVE', /last line has no newline/)
    })

    it('is inconclusive when the tool did not report its own message count', async () => {
      const world = fx.healthy()
      world.apache['main.dump.stderr'] = 'Processed a total of 3 messages\n'
      expectRow(await check(world), 'K1', 'INCONCLUSIVE', /Processed a total/)
    })

    it('is inconclusive when the artifact manifest does not bind a dump file as written', async () => {
      const world = fx.healthy()
      const report = await check(world, {
        header: (h) => ({
          ...h,
          files: h.files.map((f) =>
            f.file === 'apache/main.dump' ? { ...f, sha256: 'a'.repeat(64) } : f
          ),
        }),
      })
      expectRow(report, 'K1', 'INCONCLUSIVE', /manifest/)
    })

    it('is inconclusive when the artifact manifest does not list a dump file', async () => {
      const header = (h) => ({ ...h, files: h.files.filter((f) => f.file !== 'apache/main.dump') })
      const report = await check(fx.healthy(), { header })
      expectRow(
        report,
        'K1',
        'INCONCLUSIVE',
        /apache\/main\.dump: the header manifest does not bind it/
      )
    })

    it('is inconclusive when an end marker does not parse', async () => {
      const world = fx.healthy()
      world.apache['main.dump.end'] = 'exit=0 lines=twelve\n'
      expectRow(await check(world), 'K1', 'INCONCLUSIVE', /unparseable end marker/)
    })

    it('is inconclusive for a group id that is not safe as a file name', async () => {
      const world = fx.healthy()
      world.config.clients.find((c) => c.slot === 'c02').group = 'g/../c02'
      expectRow(await check(world), 'K4', 'INCONCLUSIVE', /unsafe group id/)
    })

    it('is inconclusive when the dump is not offset ordered', async () => {
      const world = fx.healthy()
      const lines = dumpOf(world, 'main')
      fx.redump(world, 'main', [lines[1], lines[0], ...lines.slice(2)])
      expectRow(await check(world), 'K1', 'INCONCLUSIVE', /not increasing|order/)
    })

    it('is inconclusive, with a bounded read, for a dump line longer than the line cap', async () => {
      const world = fx.healthy()
      world.apache['main.dump'] += `${'x'.repeat((1 << 20) + 10)}\n`
      expectRow(await check(world), 'K1', 'INCONCLUSIVE', /longer than/)
    })

    it('is inconclusive for a describe file larger than the small-file cap', async () => {
      const world = fx.healthy()
      world.apache[`groups/${groupOf('c02')}.describe`] += ' '.repeat((1 << 20) + 10)
      expectRow(await check(world), 'K4', 'INCONCLUSIVE', /c02\.describe: larger than/)
    })
  })

  describe('K2 no lost delivery', () => {
    it('fails a visible record that never entered a handler in one group', async () => {
      const report = await check(fx.healthy(), { edit: without(isEntry(groupOf('c03'), 'c08.6')) })
      expectRow(report, 'K2', 'FAIL', /g-0123456789ab-c03 never entered.*first c08\.6/)
    })

    it('fails a visible transaction member that never entered a handler in one group', async () => {
      const report = await check(fx.healthy(), { edit: without(isEntry(groupOf('c12'), 'c09.2')) })
      expectRow(report, 'K2', 'FAIL', /g-0123456789ab-c12 never entered.*first c09\.2/)
    })

    it('counts the share group as a recipient of the main topic', async () => {
      const report = await check(fx.healthy(), { edit: without(isEntry(groupOf('c07'), 'c08.2')) })
      expectRow(report, 'K2', 'FAIL', /g-0123456789ab-c07 never entered.*first c08\.2/)
    })

    it('counts the seed group as a recipient of the main topic', async () => {
      const report = await check(fx.healthy(), { edit: without(isEntry(groupOf('c13'), 'c08.9')) })
      expectRow(report, 'K2', 'FAIL', /g-0123456789ab-c13 never entered.*first c08\.9/)
    })

    it.each([
      ['another payload digest', () => ({ payloadSha256: 'f'.repeat(64) }), /another payload/],
      ['an offset other than the log offset', () => ({ offset: '9' }), /c08\.6.*at 9/],
      ['another topic', () => ({ key: fx.key(fx.TXN) }), /c08\.6.*fs-0123456789ab-txn/],
      [
        'another partition',
        () => ({ key: { ...fx.key(fx.MAIN), partition: 1 } }),
        /c08\.6.*main\|1/,
      ],
    ])('fails a handler entry that names %s', async (_, fields, reason) => {
      const edit = mapRecords(isEntry(groupOf('c03'), 'c08.6'), fields)
      expectRow(await check(fx.healthy(), { edit }), 'K2', 'FAIL', reason)
    })

    it('fails a record every group entered at one offset that is not its log offset', async () => {
      const edit = mapRecords(
        (r) => r.stage === 'handler-enter' && r.id === 'c08.6',
        () => ({ offset: '9' })
      )
      expectRow(
        await check(fx.healthy(), { edit }),
        'K2',
        'FAIL',
        /c08\.6 entered at 9, in the log at 6/
      )
    })

    it('fails a read-committed group that entered members of an aborted transaction', async () => {
      const edit = (records) => {
        const e = records.find(isEntry(groupOf('c01'), 'c09.0'))
        return [
          ...records,
          { ...e, id: 'c09.4', offset: '5', t: 9e7, payloadSha256: fx.sha256('c09.4') },
        ]
      }
      const report = await check(fx.healthy(), { edit })
      expectRow(report, 'K2', 'FAIL', /1 records not visible.*c09\.4 in g-0123456789ab-c01/)
    })

    it('fails, never ACCEPTED, a handler entry whose payload digest shares its first eight digits with the record but differs after them', async () => {
      const edit = mapRecords(isEntry(groupOf('c03'), 'c08.3'), (r) => ({
        payloadSha256: `${r.payloadSha256.slice(0, 8)}${r.payloadSha256[8] === '0' ? '1' : '0'}${r.payloadSha256.slice(9)}`,
      }))
      const report = await check(fx.healthy(), { edit })
      expectRow(report, 'K2', 'FAIL', /c08\.3 in g-0123456789ab-c03 with another payload/)
      expect(report.overall).toBe('REJECTED')
    })

    /** The seed's control client reads only main: an entry of a transactions record is one it must not see. */
    const controlEntry = (id, offset) => (records) => {
      const e = records.find(isEntry(groupOf('c13'), 'c08.0'))
      return [
        ...records,
        { ...e, id, offset, key: fx.key(fx.TXN), t: 9e7, payloadSha256: fx.sha256(id) },
      ]
    }

    it('fails an ordinary group that does not read the transactions topic yet entered a member of an aborted transaction', async () => {
      const report = await check(fx.healthy(), { edit: controlEntry('c09.4', '5') })
      expectRow(report, 'K2', 'FAIL', /1 records not visible.*c09\.4 in g-0123456789ab-c13/)
      expect(report.overall).toBe('REJECTED')
    })

    it('fails an ordinary group that entered a visible record of a topic it does not read', async () => {
      const report = await check(fx.healthy(), { edit: controlEntry('c09.0', '0') })
      expectRow(
        report,
        'K2',
        'FAIL',
        /g-0123456789ab-c13 entered 1 records of a topic it does not read \(first c09\.0\)/
      )
      expect(report.overall).toBe('REJECTED')
    })

    it('does not ask for the delivery of a transaction member that is not visible', async () => {
      const report = await check(fx.healthy())
      expectRow(report, 'K2', 'PASS')
      expect(row(report, 'K2').numbers.visibleRecords).toBe(12 + 4)
    })

    it('is inconclusive when the dump it compares against is unusable', async () => {
      const world = fx.healthy()
      delete world.apache['main.dump.end']
      expectRow(await check(world), 'K2', 'INCONCLUSIVE', /main\.dump/)
    })
  })

  describe('K3 duplicates', () => {
    const c01 = groupOf('c01')
    const entryOf = (records, id = 'c08.4') => records.find(isEntry(c01, id))
    const boundaryOf = (records) => records.find((r) => r.kind === 'boundary' && r.group === c01)
    const repeat =
      (fields = {}) =>
      (records) => {
        const e = entryOf(records)
        return [...records, { ...e, t: e.t + 5000, handler: 'c01.0.hx', ...fields }]
      }
    /** @returns {object[]} records plus an acquire of main by another client run of c01 */
    const otherRun = (records, client = 'c01.1') => {
      const b = boundaryOf(records)
      const authority = fx.authority(client, { acquiredAt: 2 })
      return [
        ...records,
        { ...b, t: 9e7, client, run: authority.run, authority },
        { ...entryOf(records), t: 9e7 + 1, client, handler: `${client}.h1`, authority },
      ]
    }

    it('fails a repeat on the same client run with no assignment change between the entries', async () => {
      const report = await check(fx.healthy(), { edit: repeat() })
      expectRow(report, 'K3', 'FAIL', /1 same-owner repeats.*c08\.4 in g-0123456789ab-c01/)
    })

    it('passes two distinct ids that one ownership interval entered in descending offset order, each once', async () => {
      const edit = (records) => {
        const [a, b] = ['c08.4', 'c08.5'].map((id) =>
          records.findIndex(isEntry(groupOf('c01'), id))
        )
        const out = [...records]
        ;[out[a], out[b]] = [records[b], records[a]]
        return out
      }
      const report = await check(fx.healthy(), { edit })
      expectRow(report, 'K3', 'PASS')
      expect(row(report, 'K3').numbers).toMatchObject({
        sameOwnerRepeats: 0,
        repeatsAfterChange: 0,
      })
      expect(report.overall).toBe('ACCEPTED')
    })

    it('fails a repeat that an acquire record of the identical, still open ownership tries to excuse', async () => {
      const edit = (records) =>
        repeat()([...records, { ...boundaryOf(records), t: 4500, cause: 'c01.0.extra' }])
      const report = await check(fx.healthy(), { edit })
      expectRow(report, 'K3', 'FAIL', /same-owner repeats.*c08\.4/)
      expect(row(report, 'K3').numbers.redundantAcquires).toBe(1)
    })

    it('fails an entry after its run revoked the partition and never acquired it again', async () => {
      const edit = (records) => {
        const e = entryOf(records)
        const end = { ...boundaryOf(records), event: 'revoke-end', t: e.t + 4000 }
        return repeat()([...records, end])
      }
      expectRow(
        await check(fx.healthy(), { edit }),
        'K3',
        'FAIL',
        /1 entries outside an ownership.*c08\.4/
      )
    })

    it('fails a same-owner repeat even when another client run entered the record in between', async () => {
      const edit = (records) => [...otherRun(records), { ...entryOf(records), t: 9e7 + 2 }]
      const report = await check(fx.healthy(), { edit })
      expectRow(
        report,
        'K3',
        'FAIL',
        /same-owner repeats.*c08\.4 in g-0123456789ab-c01 by c01\.0\.r1/
      )
    })

    /**
     * Holds every record about c08.4 until its offer arrives: c01's own interval entered it, then
     * each client run of `clients` enters it in turn (acquiring main first when new), every
     * interval closes, and only then do the offer and its send result reach the journal.
     */
    const heldAfterClose = (clients) => (records) => {
      const late = (r) => (r.stage === 'offered' || r.stage === 'ack') && r.id === 'c08.4'
      const [b, e] = [boundaryOf(records), entryOf(records)]
      const authorities = new Map([['c01.0', fx.authority('c01.0')]])
      const extra = []
      clients.forEach((client, n) => {
        if (!authorities.has(client)) {
          const authority = fx.authority(client, { acquiredAt: 2 + n })
          authorities.set(client, authority)
          extra.push({ ...b, client, run: authority.run, authority, cause: `${client}.a` })
        }
        const authority = authorities.get(client)
        extra.push({ ...e, client, handler: `${client}.h${n}`, authority })
      })
      for (const [client, authority] of authorities)
        extra.push({ ...b, client, run: authority.run, authority, event: 'revoke-end' })
      return [...records.filter((r) => !late(r)), ...extra, ...records.filter(late)]
    }

    it('fails an interval that entered a record twice when both its entries wait for the offer until every interval has closed', async () => {
      const report = await check(fx.healthy(), { edit: heldAfterClose(['c01.1', 'c01.0']) })
      expectRow(
        report,
        'K3',
        'FAIL',
        /1 same-owner repeats.*c08\.4 in g-0123456789ab-c01 by c01\.0\.r1/
      )
      expect(row(report, 'K3').numbers).toMatchObject({
        sameOwnerRepeats: 1,
        repeatsAfterChange: 1,
      })
    })

    it('counts, without failing, entries of one record by three intervals once each that wait for the offer until every interval has closed', async () => {
      const report = await check(fx.healthy(), { edit: heldAfterClose(['c01.1', 'c01.2']) })
      expectRow(report, 'K3', 'PASS')
      expect(row(report, 'K3').numbers).toMatchObject({
        sameOwnerRepeats: 0,
        repeatsAfterChange: 2,
      })
      expect(report.overall).toBe('ACCEPTED')
    })

    it('is inconclusive with a named reason when overlapping intervals re-enter more ids than the capacity', async () => {
      const ids = Array.from({ length: 12 }, (_, n) => `c08.${n}`)
      const edit = (records) => {
        const b = boundaryOf(records)
        const runs = ['c01.1', 'c01.2'].flatMap((client, k) => {
          const authority = fx.authority(client, { acquiredAt: 2 + k })
          const t = 9e7 + k * 100
          return [
            { ...b, t, client, run: authority.run, authority },
            ...ids.map((id, n) => ({ ...entryOf(records, id), t: t + 1 + n, client, authority })),
          ]
        })
        return [...records, ...runs]
      }
      const report = await check(fx.healthy(), { edit, capacity: 18 })
      expectRow(
        report,
        'K3',
        'INCONCLUSIVE',
        /more than 18 ids re-entered by overlapping intervals/
      )
    })

    it('drops the ids an interval shared with an overlapping one once that interval closes', () => {
      const world = fx.healthy()
      const journal = new Journal({ matrix: world.matrix, config: world.config, mode: world.mode })
      const records = world.records()
      const extra = otherRun(records)
      const revoke = { ...boundaryOf(records), t: 9e7 + 5, event: 'revoke-end' }
      for (const r of [...records, ...extra.slice(records.length)]) journal.add(r)
      expect(journal.shadowedCount).toBe(1)
      journal.add(revoke)
      expect({ shadowed: journal.shadowed.size, count: journal.shadowedCount }).toEqual({
        shadowed: 0,
        count: 0,
      })
    })

    it('counts a repeat by another client run with its own acquisition, without failing it', async () => {
      const report = await check(fx.healthy(), { edit: otherRun })
      expectRow(report, 'K3', 'PASS')
      expect(row(report, 'K3').numbers.repeatsAfterChange).toBe(1)
    })

    it('counts, without failing, a repeat after the same run acquired the partition again without a revoke', async () => {
      const edit = (records) => {
        const b = boundaryOf(records)
        const authority = fx.authority('c01.0', { acquiredAt: 2 })
        return repeat({ authority })([...records, { ...b, t: 9e6, authority }])
      }
      const report = await check(fx.healthy(), { edit })
      expectRow(report, 'K3', 'PASS')
      expect(row(report, 'K3').numbers.repeatsAfterChange).toBe(1)
    })

    it('fails an entry whose batch acquisition is not the run’s open acquisition', async () => {
      const edit = (records) => {
        const authority = fx.authority('c01.0', { acquiredAt: 2 })
        return repeat()([...records, { ...boundaryOf(records), t: 9e6, authority }])
      }
      expectRow(await check(fx.healthy(), { edit }), 'K3', 'FAIL', /entries outside an ownership/)
    })

    it('counts and prints a repeat after an assignment change without failing it', async () => {
      const edit = (records) => {
        const e = entryOf(records)
        const b = boundaryOf(records)
        const end = { ...b, event: 'revoke-end', t: e.t + 1 }
        return [...records, end, { ...b, t: e.t + 2 }, { ...e, t: e.t + 3, handler: 'c01.0.hx' }]
      }
      const report = await check(fx.healthy(), { edit })
      expectRow(report, 'K3', 'PASS')
      expect(row(report, 'K3').numbers.repeatsAfterChange).toBe(1)
      expect(formatReport(report)).toMatch(/repeatsAfterChange.*1/)
    })

    it.each([
      [
        'a run no boundary opened',
        { authority: fx.authority('c01.0', { run: 'c01.0.r2' }) },
        /outside an ownership.*c01\.0\.r2/,
      ],
      ['another client', { authority: fx.authority('c01.7') }, /attributed to another client/],
      [
        'another client on the same run',
        { authority: { ...fx.authority('c01.0'), client: 'c01.9' } },
        /attributed to another client/,
      ],
      ['no authority in an ordinary group', { authority: null }, /attributed to another client/],
    ])('fails an entry whose authority names %s', async (_, fields, reason) => {
      expectRow(await check(fx.healthy(), { edit: repeat(fields) }), 'K3', 'FAIL', reason)
    })

    it('fails an entry after its run lost the partition', async () => {
      const edit = (records) => {
        const lost = { ...boundaryOf(records), event: 'lost', t: entryOf(records).t + 4000 }
        return repeat()([...records, lost])
      }
      expectRow(await check(fx.healthy(), { edit }), 'K3', 'FAIL', /1 entries outside an ownership/)
    })

    it.each([
      [
        'another client’s group',
        { client: 'c03.0', source: 'p03', authority: fx.authority('c03.0') },
      ],
      ['a run of another client', { authority: fx.authority('c01.0', { run: 'c02.0.r1' }) }],
    ])('fails an entry attributed to %s', async (_, fields) => {
      expectRow(
        await check(fx.healthy(), { edit: repeat(fields) }),
        'K3',
        'FAIL',
        /attributed to another client/
      )
    })

    it('fails a boundary emitted by a process that does not run its client', async () => {
      const edit = (records) => [...records, { ...boundaryOf(records), t: 9e7, source: 'p02' }]
      expectRow(await check(fx.healthy(), { edit }), 'K3', 'FAIL', /attributed to another client/)
    })

    it('does not judge a share group redelivery', async () => {
      const edit = (records) => {
        const e = records.find(isEntry(groupOf('c07'), 'c08.4'))
        return [...records, { ...e, t: e.t + 3, handler: 'c07.0.hx', deliveryCount: 2 }]
      }
      const report = await check(fx.healthy(), { edit })
      expectRow(report, 'K3', 'PASS')
      expect(row(report, 'K3').numbers.shareRepeats).toBe(1)
    })
  })

  describe('K4 commits', () => {
    const describe_ = (world, slot) => `groups/${groupOf(slot)}.describe`
    const edit = (world, slot, fn) => {
      const name = describe_(world, slot)
      fx.putStep(
        world.apache,
        name,
        fn(world.apache[name]),
        'Consumer group has no active members.\n'
      )
    }
    const behind = (text) => text.replace(/(\S+\s+\S+\s+0\s+)12(\s+12\s+)0/, '$111$21')

    it('fails a group whose committed offset is behind the log end', async () => {
      const world = fx.healthy()
      edit(world, 'c02', behind)
      expectRow(await check(world), 'K4', 'FAIL', /g-0123456789ab-c02 committed 11 on main.*12/)
    })

    it('does not accept client side readbacks in place of the Apache describe', async () => {
      const world = fx.healthy()
      edit(world, 'c02', behind)
      const readback = {
        t: 9000,
        kind: 'identity',
        stage: 'broker-commit',
        source: 'p02',
        client: 'c02.0',
        group: groupOf('c02'),
        commit: 'c02.0.m1',
        key: fx.key(fx.MAIN),
        next: '12',
        errorCode: 0,
      }
      const report = await check(world, { edit: (records) => [...records, readback] })
      expectRow(report, 'K4', 'FAIL', /c02 committed 11/)
    })

    it('fails a group that has no row for a topic it must commit', async () => {
      const world = fx.healthy()
      edit(world, 'c01', (text) =>
        text
          .split('\n')
          .filter((l) => !l.includes(fx.TXN))
          .join('\n')
      )
      expectRow(await check(world), 'K4', 'FAIL', /c01 has no row for transactions/)
    })

    it('fails a group that never committed on a partition', async () => {
      const world = fx.healthy()
      edit(world, 'c01', (text) => text.replace(/(\S+\s+\S+\s+0\s+)12(\s+12\s+)0/, '$1-$2-'))
      expectRow(await check(world), 'K4', 'FAIL', /c01 never committed on main/)
    })

    it('is inconclusive when a describe row has no log end', async () => {
      const world = fx.healthy()
      edit(world, 'c01', (text) => text.replace(/(\S+\s+\S+\s+0\s+12\s+)12(\s+)0/, '$1-$2-'))
      expectRow(await check(world), 'K4', 'INCONCLUSIVE', /c01 has no log end for main/)
    })

    it('is inconclusive when two describes disagree about the log end', async () => {
      const world = fx.healthy()
      edit(world, 'c01', (text) => text.replace(/(0\s+)12(\s+)12/, (_, a, b) => `${a}13${b}13`))
      const report = await check(world)
      expectRow(report, 'K4', 'INCONCLUSIVE', /disagree about the main log end/)
      expectRow(report, 'K1', 'INCONCLUSIVE', /no describe row gives the main log end/)
    })

    it('accepts the row Kafka prints for a member that has no assignment', async () => {
      const world = fx.healthy()
      const member = `${groupOf('c01')} -  -          -               -               -               c01.0-4d2a /10.0.0.7 c01.0`
      edit(world, 'c01', (text) => text.replace(/\n$/, `\n${member}\n`))
      const report = await check(world)
      expectRow(report, 'K4', 'PASS')
      expect(report.overall).toBe('ACCEPTED')
    })

    it.each([
      [
        'a LAG that disagrees with the two offsets',
        (t) => t.replace(/(0\s+12\s+12\s+)0/, '$11'),
        /LAG is not 0/,
      ],
      [
        'a second row for one partition',
        (t) => t.replace(/(\n\S+ \S+-main \S.*)\n/, '$1$1\n'),
        /second row/,
      ],
      [
        'rows of another group',
        (t) =>
          t
            .replace(/\ng-0123456789ab-c02 /g, '\ng-0123456789ab-c03 ')
            .replace('\ng-0123456789ab-c03 ', '\ng-0123456789ab-c02 '),
        /unparseable line "g-0123456789ab-c03/,
      ],
      [
        'a row of six columns',
        (t) => t.replace(/(\n\S+ \S+-main\s+0\s+12\s+12\s+0)\s.*/, '$1'),
        /unparseable line/,
      ],
      ['a non-numeric offset', (t) => t.replace(/(\S+-main\s+0\s+)12/, '$1x2'), /unparseable line/],
      [
        'a header other than Kafka’s nine columns',
        (t) => t.replace(/\s+CONSUMER-ID.*/, ''),
        /no describe header/,
      ],
    ])('is inconclusive for a describe with %s', async (_, fn, reason) => {
      const world = fx.healthy()
      edit(world, 'c02', fn)
      expectRow(await check(world), 'K4', 'INCONCLUSIVE', reason)
    })

    it('is inconclusive when a describe has more lines than its end marker counted', async () => {
      const world = fx.healthy()
      const name = describe_(world, 'c02')
      world.apache[`${name}.end`] = 'exit=0 lines=2\n'
      expectRow(await check(world), 'K4', 'INCONCLUSIVE', /other than the 2 lines/)
    })

    it('does not judge the share group, which has no offset commits', async () => {
      const world = fx.healthy()
      expect(Object.keys(world.apache).some((f) => f.includes(groupOf('c07')))).toBe(false)
      expectRow(await check(world), 'K4', 'PASS')
    })
  })

  describe('K5 heap', () => {
    const MIB = 1024 * 1024
    const quiet = (process, fn) =>
      mapRecords(isSample(process, 'quiet'), (r) => fn(Number(r.boundary.slice(1)), r))
    /** @returns {object} a healthy world whose judged bytes of `slot` follow `bytes(h)` */
    const judged = (slot, bytes, world = fx.healthy()) => {
      const base = world.heap.judged
      world.heap.judged = (s, h) => (s === slot ? bytes(h) : base(s, h))
      return world
    }
    /** @returns {object} a healthy world whose receipt of `slot` at `boundary` is changed by `fn` */
    const receipt = (slot, boundary, fn, world = fx.healthy()) => {
      world.supervisor.heapSnapshots = world.supervisor.heapSnapshots.map((r) =>
        r.slot === slot && r.boundary === boundary ? { ...r, ...fn(r) } : r
      )
      return world
    }
    const summaryOf = (dir) =>
      JSON.parse(fs.readFileSync(path.join(dir, 'heap/summary.json'), 'utf8'))
    const resummarise = (dir, fn) => {
      const summary = summaryOf(dir)
      summary.records.forEach(fn)
      fs.writeFileSync(path.join(dir, 'heap/summary.json'), JSON.stringify(summary))
    }

    it('fails a judged endpoint above the bound', async () => {
      const world = judged('p04', (h) => 9e6 + (h >= 9 ? 3 * MIB : 0))
      expectRow(await check(world), 'K5', 'FAIL', /p04 judged heap endpoint 3145728 bytes/)
    })

    it('fails a steady judged growth of 150,000 bytes an hour, which a 24-hour run rejected, on the endpoint bound scaled to the run', async () => {
      const world = judged('p04', (h) => 9e6 + h * 150000)
      const report = await check(world)
      expectRow(report, 'K5', 'FAIL', /p04 judged heap endpoint 1350000 bytes exceeds 898779/)
      expect(row(report, 'K5').reasons.join('\n')).not.toMatch(/p04 judged heap slope/)
      const bound = world.matrix.bounds['heap-endpoint'].value
      const plan = (hours) => ({ hours })
      expect(core.endpointCeiling(bound, plan(12), 'acceptance')).toBe(898779)
      expect(core.endpointCeiling(bound, plan(24), 'acceptance')).toBe(bound)
      expect(core.endpointCeiling(bound, plan(3), 'rehearsal')).toBe(bound)
    })

    it('judges a series with a missing snapshot on the unscaled endpoint bound, so the row is inconclusive and not a FAIL', async () => {
      const world = receipt(
        'p03',
        'h10',
        (r) => ({ at: r.at + 3600000 }),
        judged('p03', (h) => 9e6 + h * 120000)
      )
      const report = await check(world)
      expectRow(report, 'K5', 'INCONCLUSIVE', /p03 has 11 of 12 usable heap snapshots/)
      expect(row(report, 'K5').reasons.join('\n')).not.toMatch(/p03 judged heap endpoint/)
    })

    it('fails a judged slope above the bound even when the endpoint is clean', async () => {
      const world = judged('p05', (h) => 9e6 + (h >= 6 && h <= 8 ? 600 * MIB : 0))
      const report = await check(world)
      expectRow(report, 'K5', 'FAIL', /p05 judged heap slope/)
      expect(row(report, 'K5').reasons.join('\n')).not.toMatch(/p05.*endpoint/)
    })

    it('passes a run whose whole-process heap grows 33 MiB of excluded code and prints that growth as information', async () => {
      const world = fx.healthy()
      const edit = quiet('p04', (h, r) => ({ heapUsed: r.heapUsed + h * 3 * MIB }))
      const report = await check(world, { edit })
      expectRow(report, 'K5', 'PASS')
      expect(row(report, 'K5').numbers.p04).toEqual({ endpoint: 0, slopePerMinute: 0 })
      expect(report.information.heap.heapUsed.p04).toEqual({
        endpoint: 27 * MIB,
        slopePerMinute: Math.round((3 * MIB) / 60),
      })
      expect(formatReport(report)).toContain(
        `  whole-process heapUsed p04: endpoint ${27 * MIB} bytes, slope ${Math.round((3 * MIB) / 60)} B/min`
      )
    })

    it.each([
      [
        'a skipped receipt',
        (r) => ({ at: null, ms: null, error: 'not taken: step h10q2 late' }),
        /p03 h10 has no usable heap snapshot: not taken: step h10q2 late/,
      ],
      [
        'a failed write',
        () => ({ error: 'ENOSPC: no space left on device' }),
        /p03 h10 has no usable heap snapshot: ENOSPC/,
      ],
      [
        'a snapshot begun before the last quiet reading',
        (r) => ({ at: r.at - 20000 }),
        /p03 h10 heap snapshot at \d+ lies outside the hour's quiet window/,
      ],
      [
        'a snapshot begun after the hour',
        (r) => ({ at: r.at + 3600000 }),
        /p03 h10 heap snapshot at \d+ lies outside/,
      ],
    ])('is inconclusive, never PASS, for a boundary with %s', async (_, fn, reason) => {
      const report = await check(receipt('p03', 'h10', fn))
      expectRow(report, 'K5', 'INCONCLUSIVE', reason)
      expect(row(report, 'K5').reasons.join('\n')).toMatch(/p03 has 11 of 12 usable heap snapshots/)
      expect(report.overall).toBe('NOT ACCEPTED')
    })

    it('is inconclusive for a boundary with no summary record, or one that disagrees with the supervisor receipt', async () => {
      const missing = fx.healthy()
      missing.heap.edit = (dir) => {
        const summary = summaryOf(dir)
        summary.records = summary.records.filter((r) => !(r.slot === 'p03' && r.boundary === 'h10'))
        fs.writeFileSync(path.join(dir, 'heap/summary.json'), JSON.stringify(summary))
      }
      expectRow(await check(missing), 'K5', 'INCONCLUSIVE', /p03 h10 has no heap snapshot summary/)
      const moved = fx.healthy()
      moved.heap.edit = (dir) =>
        resummarise(dir, (r) => {
          if (r.slot === 'p03' && r.boundary === 'h10') r.at += 1
        })
      expectRow(
        await check(moved),
        'K5',
        'INCONCLUSIVE',
        /p03 h10 has a heap snapshot summary that disagrees with the supervisor receipt/
      )
    })

    it('is inconclusive for a boundary whose snapshot was written but could not be summarised, and names why', async () => {
      const world = fx.healthy()
      world.heap.edit = (dir) =>
        resummarise(dir, (r) => {
          if (r.slot === 'p03' && r.boundary === 'h10') {
            for (const key of ['judgedBytes', 'totalBytes']) delete r[key]
            r.error = 'unreadable snapshot: Malformed heap snapshot: truncated value at byte 9'
          }
        })
      expectRow(
        await check(world),
        'K5',
        'INCONCLUSIVE',
        /p03 h10 has no usable heap snapshot: unreadable snapshot: Malformed heap snapshot/
      )
    })

    it('is inconclusive without a heap summary, and fails no process on one', async () => {
      const world = judged('p04', (h) => 9e6 + (h >= 9 ? 3 * MIB : 0))
      world.heap.edit = (dir) => fs.rmSync(path.join(dir, 'heap/summary.json'))
      const report = await check(world)
      expectRow(report, 'K5', 'INCONCLUSIVE', /heap\/summary\.json: missing/)
    })

    it('re-derives the endpoints from the kept snapshot files: a summary that disagrees with its kept snapshot, or a kept snapshot that is gone, leaves the process inconclusive', async () => {
      const lied = judged('p04', (h) => 9e6 + (h >= 9 ? 3 * MIB : 0))
      lied.heap.edit = (dir) =>
        resummarise(dir, (r) => {
          if (r.slot === 'p04' && r.boundary === 'h11') r.judgedBytes = 9e6
        })
      const report = await check(lied)
      expectRow(
        report,
        'K5',
        'INCONCLUSIVE',
        /p04 h11 snapshot does not confirm its summary: heap\/p04\.h11\.heapsnapshot has judgedBytes 12145728, its summary 9000000/
      )
      expect(row(report, 'K5').numbers.p04).toBeUndefined()
      const gone = fx.healthy()
      gone.heap.edit = (dir) => fs.rmSync(path.join(dir, 'heap/p04.h00.heapsnapshot'))
      expectRow(
        await check(gone),
        'K5',
        'INCONCLUSIVE',
        /p04 h00 snapshot does not confirm its summary: heap\/p04\.h00\.heapsnapshot cannot be read/
      )
    })

    /** @returns {(dir: string) => void} an edit setting the judged bytes of `slot`'s records */
    const forge = (slot, judgedOf) => (dir) =>
      resummarise(dir, (r) => {
        const h = Number(r.boundary.slice(1))
        if (r.slot === slot && judgedOf(h) !== undefined) r.judgedBytes = judgedOf(h)
      })
    const leak = (h) => 9e6 + (h >= 9 ? 3 * MIB : 0)

    it.each([
      [
        'its files deleted',
        leak,
        (h) => (h === 9 || h === 10 ? 9e6 : undefined),
        ['h09', 'h10'],
        /p04 h09 snapshot does not confirm its summary: heap\/p04\.h09\.heapsnapshot cannot be read/,
      ],
      [
        'its files still on disk',
        leak,
        (h) => (h === 9 || h === 10 ? 9e6 : undefined),
        [],
        /p04 h09 snapshot does not confirm its summary: heap\/p04\.h09\.heapsnapshot has judgedBytes 12145728, its summary 9000000/,
      ],
      [
        'a steady leak raised at the start and lowered at the end',
        (h) => 9e6 + h * 150000,
        (h) => ({ 1: 11e6, 2: 11e6, 9: 9e6, 10: 9e6 })[h],
        [],
        /p04 h01 snapshot does not confirm its summary: heap\/p04\.h01\.heapsnapshot has judgedBytes 9150000, its summary 11000000/,
      ],
    ])(
      'never passes a run whose middle summary records lie about the endpoint inputs, %s: every snapshot is re-derived from its file',
      async (_, bytes, lie, deleted, reason) => {
        const world = judged('p04', bytes)
        world.heap.edit = (dir) => {
          forge('p04', lie)(dir)
          for (const b of deleted) fs.rmSync(path.join(dir, `heap/p04.${b}.heapsnapshot`))
        }
        const report = await check(world)
        expectRow(report, 'K5', 'INCONCLUSIVE', reason)
        expect(row(report, 'K5').numbers.p04).toBeUndefined()
      }
    )

    /** @returns {(dir: string) => void} an edit making p04's h11 record a copy of `slot`'s `boundary` record */
    const pointAt = (slot, boundary) => (dir) => {
      const summary = summaryOf(dir)
      const source = summary.records.find((r) => r.slot === slot && r.boundary === boundary)
      const i = summary.records.findIndex((r) => r.slot === 'p04' && r.boundary === 'h11')
      const { at, ms } = summary.records[i]
      summary.records[i] = { ...source, slot: 'p04', boundary: 'h11', at, ms }
      fs.writeFileSync(path.join(dir, 'heap/summary.json'), JSON.stringify(summary))
    }
    /** @returns {(dir: string) => void} an edit changing p04's h11 record by `fn` */
    const lastRecord = (fn) => (dir) =>
      resummarise(dir, (r) => {
        if (r.slot === 'p04' && r.boundary === 'h11') fn(r)
      })

    it.each([
      ['another process’s snapshot of the same boundary', pointAt('p05', 'h11'), {}],
      ['its own process’s snapshot of another boundary', pointAt('p04', 'h00'), {}],
      [
        'a size other than the one the worker recorded',
        lastRecord((r) => (r.snapshotBytes += 1)),
        {},
      ],
      ['a receipt whose size is not the file’s', () => {}, { bytes: (r) => r.bytes + 1 }],
      [
        'a receipt naming another process’s file',
        lastRecord((r) => (r.file = 'heap/p05.h11.heapsnapshot')),
        { file: () => 'heap/p05.h11.heapsnapshot' },
      ],
    ])(
      'never passes a leaking process whose last record is bound to %s',
      async (_, edit, fields) => {
        const world = judged('p04', (h) => 9e6 + (h >= 10 ? 3 * MIB : 0))
        world.heap.edit = edit
        const changed = (r) =>
          Object.fromEntries(Object.entries(fields).map(([key, value]) => [key, value(r)]))
        const supervisor = (s) => ({
          ...s,
          heapSnapshots: s.heapSnapshots.map((r) =>
            r.slot === 'p04' && r.boundary === 'h11' ? { ...r, ...changed(r) } : r
          ),
        })
        const report = await check(world, { supervisor })
        expectRow(
          report,
          'K5',
          'INCONCLUSIVE',
          /p04 h11 has a heap snapshot summary that disagrees with the supervisor receipt/
        )
        expect(row(report, 'K5').reasons.join('\n')).not.toMatch(/p04 judged heap endpoint/)
      }
    )

    it('judges the seed as the positive control of the snapshot pipeline: its judged endpoint above the breach bound is printed and lets the row pass', async () => {
      const report = await check(fx.healthy())
      expectRow(report, 'K5', 'PASS')
      expect(report.information.heap.control).toEqual({
        p13: { endpoint: 18 * MIB, slopePerMinute: Math.round((2 * MIB) / 60) },
      })
      expect(formatReport(report)).toContain(
        `  seed control p13 (judged heap, must exceed 2097152 bytes): endpoint ${18 * MIB} bytes, slope ${Math.round((2 * MIB) / 60)} B/min`
      )
    })

    it.each([
      [
        'grows nothing',
        () => 9e6,
        /seed p13 does not show its leak: judged heap endpoint 0 bytes is not above 2097152/,
      ],
      [
        'grows exactly the breach bound',
        (h) => 9e6 + (h >= 9 ? 2 * MIB : 0),
        /seed p13 does not show its leak: judged heap endpoint 2097152 bytes is not above 2097152/,
      ],
    ])('cannot pass the heap row when the seed %s', async (_, bytes, reason) => {
      const report = await check(judged('p13', bytes))
      expectRow(report, 'K5', 'INCONCLUSIVE', reason)
      expect(Object.keys(row(report, 'K5').numbers)).toHaveLength(12)
    })

    it('cannot pass the heap row when the seed took no usable snapshot', async () => {
      const failed = (r) => ({ at: null, ms: null, error: 'ENOSPC: no space left on device' })
      const world = fx.healthy()
      world.supervisor.heapSnapshots = world.supervisor.heapSnapshots.map((r) =>
        r.slot === 'p13' ? { ...r, ...failed(r) } : r
      )
      expectRow(
        await check(world),
        'K5',
        'INCONCLUSIVE',
        /seed p13 does not show its leak: p13 h00 has no usable heap snapshot: ENOSPC/
      )
    })

    it('is inconclusive, naming why, when finalize could not summarise the heap snapshots', async () => {
      const world = fx.healthy()
      world.heap.edit = (dir) => {
        fs.rmSync(path.join(dir, 'heap/summary.json'))
        fs.writeFileSync(
          path.join(dir, 'heap/summary-error.txt'),
          'EISDIR: illegal operation on a directory\n'
        )
      }
      expectRow(
        await check(world),
        'K5',
        'INCONCLUSIVE',
        /heap\/summary\.json: missing; finalize could not summarise the heap snapshots: EISDIR: illegal operation on a directory/
      )
    })

    it('is inconclusive for a boundary summarised twice', async () => {
      const world = fx.healthy()
      world.heap.edit = (dir) => {
        const summary = summaryOf(dir)
        summary.records.push(summary.records.find((r) => r.slot === 'p03' && r.boundary === 'h10'))
        fs.writeFileSync(path.join(dir, 'heap/summary.json'), JSON.stringify(summary))
      }
      expectRow(
        await check(world),
        'K5',
        'INCONCLUSIVE',
        /heap\/summary\.json: p03 h10 is summarised twice/
      )
    })

    it('is inconclusive for a summary that excludes any node type but code', async () => {
      const world = fx.healthy()
      world.heap.edit = (dir) =>
        fs.writeFileSync(
          path.join(dir, 'heap/summary.json'),
          JSON.stringify({ ...summaryOf(dir), excludedType: 'hidden' })
        )
      expectRow(
        await check(world),
        'K5',
        'INCONCLUSIVE',
        /heap\/summary\.json: not a summary that excludes only node type code/
      )
    })

    it('leaves a boundary whose quiet readings completed after its hour out of the whole-process heapUsed information line', async () => {
      const late = (h) => h === 9 || h === 10
      const edit = quiet('p04', (h, r) =>
        late(h) ? { heapUsed: r.heapUsed + 10 * MIB, completedAt: r.completedAt + 3600000 } : {}
      )
      const report = await check(fx.healthy(), { edit })
      expect(report.information.heap.heapUsed.p04.endpoint).toBe(0)
    })

    it('does not let the seed process substitute for or fail an unseeded process', async () => {
      const edit = quiet('p13', (h, r) => ({ heapUsed: r.heapUsed + h * 50 * 1024 * 1024 }))
      const report = await check(fx.healthy(), { edit })
      expectRow(report, 'K5', 'PASS')
      expect(Object.keys(row(report, 'K5').numbers)).toEqual(
        fx
          .healthy()
          .config.processes.filter((p) => p.role === 'subject')
          .map((p) => p.slot)
      )
    })

    it('prints the judged heap of the reader and the decoder as information and never judges it', async () => {
      const world = judged('p14', (h) => 9e6 + h * 3 * MIB)
      const report = await check(world)
      expectRow(report, 'K5', 'PASS')
      expect(report.information.heap.comparison).toEqual({
        p14: { endpoint: 27 * MIB, slopePerMinute: Math.round((3 * MIB) / 60) },
        p16: { endpoint: 0, slopePerMinute: 0 },
      })
      expect(formatReport(report)).toContain(
        `  judged heap of harness-only p14: endpoint ${27 * MIB} bytes`
      )
    })

    it('computes the screen from the medians of the edge boundaries and a least-squares slope, NaN without time spread', () => {
      const points = Array.from({ length: 24 }, (_, h) => ({
        bytes: 1000 + h * 100,
        minutes: h * 60,
      }))
      const result = core.heapScreen(points)
      expect(result.endpoint).toBe(2100)
      expect(result.slope).toBeCloseTo(100 / 60, 6)
      expect(core.heapScreen(points.map((p) => ({ ...p, minutes: 5 }))).slope).toBeNaN()
    })
  })

  describe('K5 on real heap snapshots', () => {
    const FIXTURE = path.join(__dirname, '../../soak/__tests__/fixtures/heapJudged.js')
    const MIB = 1024 * 1024
    // Writing and reading real snapshots is CPU work whose length follows the host load.
    const REAL_SNAPSHOT_MS = 240000
    const snapshots = {}
    beforeAll(() => {
      for (const kind of ['code', 'map', 'array', 'strings', 'seed']) {
        snapshots[kind] = tmp()
        const slot = kind === 'seed' ? 'p13' : 'p04'
        const child = spawnSync(process.execPath, [FIXTURE, snapshots[kind], slot, kind], {
          encoding: 'utf8',
        })
        expect([kind, child.status, child.stderr]).toEqual([kind, 0, ''])
      }
    }, 120000)

    /** Judges a rehearsal whose p04 snapshots are the real files of `kind`, and p13's `seed`'s. */
    const judgeReal = async (kind, seed = null) => {
      const world = rehearsal()
      world.heap.real.p04 = snapshots[kind]
      if (seed) world.heap.real.p13 = snapshots[seed]
      let summary = null
      world.heap.edit = (dir) => {
        summary = JSON.parse(fs.readFileSync(path.join(dir, 'heap/summary.json'), 'utf8'))
      }
      const report = await check(world)
      const [first, last] = ['h00', 'h02'].map((b) =>
        summary.records.find((r) => r.slot === 'p04' && r.boundary === b)
      )
      return { report, first, last }
    }

    it(
      'passes a run whose whole-process heap grows 6 MiB of excluded V8 code',
      async () => {
        const { report, first, last } = await judgeReal('code')
        expect(last.totalBytes - first.totalBytes).toBeGreaterThan(6 * MIB)
        expect(last.excluded['system / BytecodeArray']).toBeGreaterThan(
          first.excluded['system / BytecodeArray'] + MIB
        )
        expectRow(report, 'K5', 'PASS')
        expect(row(report, 'K5').numbers.p04.endpoint).toBeLessThan(64 * 1024)
      },
      REAL_SNAPSHOT_MS
    )

    it.each([
      ['a product Map', 'map', 'product'],
      ['a harness array', 'array', 'harness'],
      ['strings owned by node internals', 'strings', 'node'],
    ])(
      'fails a run whose judged bytes grow 3 MiB in %s, and attributes the growth',
      async (_, kind, owner) => {
        const { report, first, last } = await judgeReal(kind)
        expect(last.judgedBytes - first.judgedBytes).toBeGreaterThan(2.5 * MIB)
        expect(last.judgedByOwner[owner] - first.judgedByOwner[owner]).toBeGreaterThan(2 * MIB)
        expectRow(report, 'K5', 'FAIL', /p04 judged heap endpoint \d+ bytes exceeds 2097152/)
      },
      REAL_SNAPSHOT_MS
    )

    it(
      'judges what the seed retains: real snapshots of its offset map show the leak to the control',
      async () => {
        const { report } = await judgeReal('code', 'seed')
        expectRow(report, 'K5', 'PASS')
        expect(report.information.heap.control.p13.endpoint).toBeGreaterThan(2 * MIB)
      },
      REAL_SNAPSHOT_MS
    )
  })

  describe('K6 resources', () => {
    const final = (process, activeResources) =>
      mapRecords(isSample(process, 'final'), () => ({ activeResources }))

    it('fails a handle left over after the final disconnect', async () => {
      const edit = final('p02', [
        { type: 'TCPSocketWrap', count: 0 },
        { type: 'Timeout', count: 3 },
      ])
      expectRow(
        await check(fx.healthy(), { edit }),
        'K6',
        'FAIL',
        /p02 final Timeout 3 exceeds its baseline 2/
      )
    })

    it('fails a resource type that the baseline never had', async () => {
      const edit = final('p09', [{ type: 'MysteryWrap', count: 1 }])
      expectRow(
        await check(fx.healthy(), { edit }),
        'K6',
        'FAIL',
        /p09 final MysteryWrap 1 exceeds its baseline 0/
      )
    })

    it.each(['final', 'baseline'])(
      'is inconclusive when a process has no %s sample',
      async (role) => {
        const edit = without(isSample('p11', role))
        const report = await check(fx.healthy(), { edit })
        expectRow(report, 'K6', 'INCONCLUSIVE', new RegExp(`p11 has no ${role}`))
        expectRow(report, 'K7', 'FAIL', new RegExp(`p11 has no ${role} sample`))
      }
    )

    it('is inconclusive when the final sample was taken right after the baseline, before the drain', async () => {
      const edit = mapRecords(isSample('p02', 'final'), () => ({
        t: 402,
        admittedAt: 401,
        completedAt: 402,
      }))
      const report = await check(fx.healthy(), { edit })
      expectRow(
        report,
        'K6',
        'INCONCLUSIVE',
        /p02 final sample was not taken last and after the end of traffic/
      )
      expect(report.overall).not.toBe('ACCEPTED')
    })

    it('is inconclusive when another sample of the process completed after its final sample', async () => {
      const edit = mapRecords(
        (r) => isSample('p02', 'periodic')(r) && r.slot === 1440,
        (r) => ({ completedAt: r.completedAt + 200 })
      )
      expectRow(
        await check(fx.healthy(), { edit }),
        'K6',
        'INCONCLUSIVE',
        /p02 final sample was not taken last/
      )
    })

    it('is inconclusive when the end of traffic follows the final samples', async () => {
      const edit = mapRecords(
        (r) => r.kind === 'shape' && r.phase === 'end',
        (r) => ({ at: r.at + 10000 })
      )
      expectRow(
        await check(fx.healthy(), { edit }),
        'K6',
        'INCONCLUSIVE',
        /p01 final sample was not taken last/
      )
    })

    it('is inconclusive when the journal records no end of traffic', async () => {
      const edit = without((r) => r.kind === 'shape' && r.phase === 'end')
      expectRow(await check(fx.healthy(), { edit }), 'K6', 'INCONCLUSIVE', /no end of traffic/)
    })

    const trafficEnd = (fields) =>
      mapRecords(
        (r) => r.kind === 'shape' && r.execution === 'traffic' && r.phase === 'end',
        fields
      )

    it.each([
      ['a shape other than traffic', () => ({ shape: 'idle' })],
      ['a source other than the supervisor', () => ({ source: 'p01' })],
    ])(
      'is inconclusive, never ACCEPTED, when the only end of traffic comes from %s',
      async (_, fields) => {
        const report = await check(fx.healthy(), { edit: trafficEnd(fields) })
        expectRow(report, 'K6', 'INCONCLUSIVE', /no end of traffic/)
        expect(report.overall).not.toBe('ACCEPTED')
      }
    )

    it('is inconclusive, never ACCEPTED, when the end of traffic precedes the end of the run', async () => {
      const report = await check(fx.healthy(), {
        edit: trafficEnd((r) => ({ at: fx.START + 1000, t: fx.START + 1000 })),
      })
      expectRow(report, 'K6', 'INCONCLUSIVE', /end of traffic at 2000 lies outside the run end/)
      expect(report.overall).not.toBe('ACCEPTED')
    })

    describe('the end of traffic comes from the process that supervised the run', () => {
      const exitOf = (process, source) =>
        mapRecords(
          (r) => r.kind === 'process' && r.event === 'exit' && r.process === process,
          () => ({ source })
        )
      const roleOf = (world, slot, role) => {
        world.config.processes.find((p) => p.slot === slot).role = role
        return world
      }

      it.each([
        [
          'the header names the process that emitted the end of traffic',
          (world) => world,
          trafficEnd(() => ({ source: 'p14' })),
          (h) => ({ ...h, exit: { ...h.exit, supervisorProcess: 'p14' } }),
          /the header names p14 as the supervisor, but the config's collector is p15/,
        ],
        [
          'the outer observer witnessed the exit of another process',
          (world) => world,
          (records) => exitOf('p14', 'outer')(exitOf('p15', 'p14')(records)),
          (h) => h,
          /the outer observer witnessed the exit of p14, not of the config's collector p15/,
        ],
        [
          'the outer observer witnessed the exits of the supervisor and of another process',
          (world) => world,
          (records) => [
            ...records,
            {
              ...records.find((r) => r.kind === 'process' && r.event === 'exit'),
              source: 'outer',
              process: 'p14',
              t: 9e9,
            },
          ],
          (h) => h,
          /the outer observer witnessed the exit of p15, p14, not of the config's collector p15/,
        ],
        [
          'the config names another process as the collector',
          (world) => roleOf(roleOf(world, 'p15', 'reader'), 'p14', 'collector'),
          (records) => records,
          (h) => h,
          /the header names p15 as the supervisor, but the config's collector is p14/,
        ],
        [
          'the config names two collector processes',
          (world) => roleOf(world, 'p14', 'collector'),
          (records) => records,
          (h) => h,
          /the config does not name exactly one collector process/,
        ],
      ])(
        'is inconclusive and fails K7, never ACCEPTED, when %s',
        async (_, world, edit, header, reason) => {
          const report = await check(world(fx.healthy()), { edit, header })
          expectRow(report, 'K6', 'INCONCLUSIVE', /the supervisor is not attributable/)
          expectRow(report, 'K7', 'FAIL', reason)
          expect(row(report, 'K6').numbers.trafficEnd).toBeNull()
          expect(report.overall).not.toBe('ACCEPTED')
        }
      )
    })

    it('is inconclusive, never ACCEPTED, when the end of traffic follows the supervisor’s observed end', async () => {
      const world = fx.healthy()
      const late = world.header.window.observedEndAt + 1
      const report = await check(world, { edit: trafficEnd(() => ({ at: late, t: late })) })
      expectRow(
        report,
        'K6',
        'INCONCLUSIVE',
        new RegExp(`end of traffic at ${late} lies outside the run end`)
      )
      expect(report.overall).not.toBe('ACCEPTED')
    })

    it('ignores a later traffic end that the supervisor did not emit', async () => {
      const edit = (records) => {
        const end = records.find((r) => r.kind === 'shape' && r.phase === 'end')
        return [...records, { ...end, at: 9e9, t: 9e9, source: 'p14' }]
      }
      const report = await check(fx.healthy(), { edit })
      expectRow(report, 'K6', 'PASS')
      expect(report.overall).toBe('ACCEPTED')
    })

    it('is inconclusive when the baseline was completed after another sample of its process', async () => {
      const edit = mapRecords(isSample('p04', 'baseline'), () => ({ completedAt: 2e6 }))
      expectRow(
        await check(fx.healthy(), { edit }),
        'K6',
        'INCONCLUSIVE',
        /p04 baseline was completed after/
      )
    })
  })

  describe('K7 exit and completeness', () => {
    const exitOf = (process, fields) =>
      mapRecords(
        (r) => r.kind === 'process' && r.event === 'exit' && r.process === process,
        () => fields
      )
    const periodic = (process, slot, fields) =>
      mapRecords((r) => isSample(process, 'periodic')(r) && r.slot === slot, fields)

    it('fails a parent observed exit code that is not zero', async () => {
      const header = (h) => ({ ...h, exit: { ...h.exit, code: 1 } })
      expectRow(
        await check(fx.healthy(), { header }),
        'K7',
        'FAIL',
        /supervisor exited with code 1/
      )
    })

    it('fails a child that ended on a signal, even one that also reports code 0', async () => {
      for (const fields of [
        { code: null, signal: 'SIGTERM' },
        { code: 0, signal: 'SIGTERM' },
      ]) {
        const report = await check(fx.healthy(), { edit: exitOf('p06', fields) })
        expectRow(report, 'K7', 'FAIL', /p06.*SIGTERM/)
      }
    })

    it('fails a child exit by SIGKILL', async () => {
      const report = await check(fx.healthy(), {
        edit: exitOf('p07', { code: null, signal: 'SIGKILL' }),
      })
      expectRow(report, 'K7', 'FAIL', /p07.*SIGKILL/)
    })

    it.each([
      [
        'no observed exit',
        (r) => r.kind === 'process' && r.event === 'exit' && r.process === 'p12',
        /p12 has no parent-observed exit/,
      ],
      [
        'no start record',
        (r) => r.kind === 'process' && r.event === 'start' && r.process === 'p05',
        /p05 has no start record/,
      ],
    ])('fails a process with %s', async (_, predicate, reason) => {
      expectRow(await check(fx.healthy(), { edit: without(predicate) }), 'K7', 'FAIL', reason)
    })

    it('fails a process with a repeated sample', async () => {
      const edit = (records) => [
        ...records,
        records.find((r) => isSample('p05', 'periodic')(r) && r.slot === 3),
      ]
      expectRow(await check(fx.healthy(), { edit }), 'K7', 'FAIL', /p05 has 1 repeated/)
    })

    it.each([
      ['a second exit', (r) => r.kind === 'process' && r.event === 'exit' && r.process === 'p05'],
      ['a second final sample', isSample('p05', 'final')],
      ['a second quiet reading', (r) => isSample('p05', 'quiet')(r) && r.boundary === 'h03'],
    ])('fails a process with %s', async (_, predicate) => {
      const edit = (records) => [...records, records.find(predicate)]
      expectRow(await check(fx.healthy(), { edit }), 'K7', 'FAIL', /p05 has 1 repeated/)
    })

    it.each([
      ['a periodic slot', 'periodic', { slot: 5000 }],
      ['a quiet boundary', 'quiet', { boundary: 'h30' }],
      ['a quiet reading', 'quiet', { reading: 5 }],
    ])('fails a process with %s out of range', async (_, role, fields) => {
      const edit = (records) => [...records, { ...records.find(isSample('p05', role)), ...fields }]
      expectRow(
        await check(fx.healthy(), { edit }),
        'K7',
        'FAIL',
        /p05 has 1 repeated or out-of-range/
      )
    })

    it('fails a supervisor receipt that reports any failure or names another run, mode or tree', async () => {
      for (const [field, value, reason] of [
        ['aborted', 'host monitor', /aborted the run: host monitor/],
        ['incomplete', 'queue timeout', /incomplete run: queue timeout/],
        ['failures', ['stream cap'], /1 failures: stream cap/],
        ['problems', ['handler entry c01.0.h4 lacks batch authority'], /reports 1 problems/],
        ['forced', ['p07'], /forced p07/],
        ['steps', { ok: 9, late: 0, error: 1, aborted: 0 }, /1 steps failed/],
        ['steps', { ok: 9, late: 0, error: 0, aborted: 2 }, /2 aborted/],
        ['diagnostic', true, /marked diagnostic/],
        ['mode', 'smoke', /mode differs/],
        ['treeSha', 'f'.repeat(40), /treeSha differs/],
      ]) {
        const supervisor = (s) => ({ ...s, [field]: value })
        expectRow(await check(fx.healthy(), { supervisor }), 'K7', 'FAIL', reason)
      }
    })

    it('fails a run shorter than 12 hours in acceptance mode', async () => {
      const header = (h) => ({
        ...h,
        window: { ...h.window, requestedMs: 3600000, runEndAt: h.window.runStartAt + 3600000 },
      })
      expectRow(await check(fx.healthy(), { header }), 'K7', 'FAIL', /3600000.*43200000/)
    })

    it('fails a run window that is not the requested time', async () => {
      const header = (h) => ({ ...h, window: { ...h.window, runEndAt: h.window.runEndAt - 1 } })
      expectRow(
        await check(fx.healthy(), { header }),
        'K7',
        'FAIL',
        /window is not the requested time/
      )
    })

    it('fails a gap in the periodic sample grid', async () => {
      const edit = without((r) => isSample('p03', 'periodic')(r) && r.slot === 1400)
      expectRow(await check(fx.healthy(), { edit }), 'K7', 'FAIL', /p03 misses periodic slot 1400/)
    })

    it('fails a periodic sample admitted before its slot', async () => {
      const edit = periodic('p03', 700, (r) => ({ admittedAt: r.admittedAt - 60000 }))
      expectRow(await check(fx.healthy(), { edit }), 'K7', 'FAIL', /p03 slot 700.*early/)
    })

    it('fails a periodic sample admitted after its slot window', async () => {
      const edit = periodic('p03', 1400, (r) => ({ admittedAt: r.admittedAt + 6000 }))
      expectRow(
        await check(fx.healthy(), { edit }),
        'K7',
        'FAIL',
        /p03 slot 1400 was admitted late/
      )
    })

    it('fails when the stream, header, config or matrix failed validation', async () => {
      const report = await judgeWith(fx.healthy(), tmp(), {
        problems: ['line 5: seq is not the next of its source'],
      })
      expectRow(report, 'K7', 'FAIL', /seq is not the next/)
      expectRow(report, 'K1', 'INCONCLUSIVE', /not valid/)
      const many = Array.from({ length: 12 }, (_, n) => `line ${n}: bad`)
      expectRow(
        await judgeWith(fx.healthy(), tmp(), { problems: many }),
        'K7',
        'FAIL',
        /2 more invalid-evidence/
      )
    })

    it('fails when a process reported a problem diagnostic', async () => {
      const diagnostic = {
        t: 9500,
        kind: 'diagnostic',
        source: 'p01',
        message: 'problem: collector refused a record',
      }
      const report = await check(fx.healthy(), { edit: (records) => [...records, diagnostic] })
      expectRow(report, 'K7', 'FAIL', /1 problem: diagnostics/)
    })

    it('is inconclusive for delivery and duplicates when a handler call was journaled only in part', async () => {
      const diagnostic = {
        t: 9500,
        kind: 'diagnostic',
        source: 'p01',
        message: 'partial handler-enter {"id":"c08.4"}',
      }
      const report = await check(fx.healthy(), { edit: (records) => [...records, diagnostic] })
      expectRow(report, 'K2', 'INCONCLUSIVE', /journaled only in part/)
      expectRow(report, 'K3', 'INCONCLUSIVE', /journaled only in part/)
    })

    it('is inconclusive when the driver journaled a missing source', async () => {
      const diagnostic = {
        t: 9500,
        kind: 'diagnostic',
        source: 'p01',
        message: 'missing-source batch authority at c01.0.h7',
      }
      const report = await check(fx.healthy(), { edit: (records) => [...records, diagnostic] })
      expectRow(report, 'K7', 'INCONCLUSIVE', /missing-source/)
    })

    it('is inconclusive when the supervisor receipt is absent', async () => {
      const report = await check(fx.healthy(), { supervisor: () => null })
      expectRow(report, 'K7', 'INCONCLUSIVE', /supervisor\.json/)
    })
  })

  describe('stream order across processes', () => {
    const producer = (r) => r.source === 'p08'
    /** Moves the producer's records after every other record, keeping each source's own order. */
    const producerLast = (records) => [
      ...records.filter((r) => !producer(r)),
      ...records.filter(producer),
    ]
    const verdicts = (report) => report.rows.map((r) => `${r.id}:${r.verdict}`)
    const healthyVerdicts = ['K1', 'K2', 'K3', 'K4', 'K5', 'K6', 'K7'].map((id) => `${id}:PASS`)

    it('judges a handler entry that precedes its offered record in the stream when the offer arrives', async () => {
      const report = await check(fx.healthy(), { edit: producerLast })
      expect(verdicts(report)).toEqual(healthyVerdicts)
      expect(row(report, 'K2').numbers.visibleRecords).toBe(
        dumpOf(fx.healthy(), 'main').length + dumpOf(fx.healthy(), 'transactions').length
      )
    })

    it('judges a send result that precedes its offered record when the offer arrives', async () => {
      const edit = (records) => {
        const ack = records.find((r) => r.stage === 'ack' && r.id === 'c08.3')
        return [ack, ...records.filter((r) => r !== ack)]
      }
      const report = await check(fx.healthy(), { edit })
      expect(verdicts(report)).toEqual(healthyVerdicts)
    })

    it('judges a held entry against the ownership its own process had when it wrote the entry', async () => {
      const c01 = groupOf('c01')
      const edit = (records) => {
        const out = producerLast(records)
        const entries = out.filter(
          (r) => r.stage === 'handler-enter' && r.group === c01 && r.key.topic === fx.MAIN
        )
        const acquire = out.find(
          (r) => r.kind === 'boundary' && r.group === c01 && r.key.topic === fx.MAIN
        )
        out.splice(out.indexOf(entries.at(-1)) + 1, 0, {
          ...acquire,
          event: 'revoke-end',
          cause: 'c01.0.r1',
        })
        return out
      }
      const report = await check(fx.healthy(), { edit })
      expect(verdicts(report)).toEqual(healthyVerdicts)
    })

    it('fails a handler entry for an id that was never offered', async () => {
      const edit = (records) => {
        const e = records.find(isEntry(groupOf('c01'), 'c08.4'))
        return [...records, { ...e, id: 'c08.999', handler: 'c01.0.hz' }]
      }
      const report = await check(fx.healthy(), { edit })
      expectRow(
        report,
        'K2',
        'FAIL',
        /1 handler entries name an id never offered.*c08\.999 in g-0123456789ab-c01/
      )
    })

    it('fails a handler entry for an id never offered when the offered ids fill the capacity exactly', async () => {
      const offered = fx
        .healthy()
        .records()
        .filter((r) => r.stage === 'offered').length
      const edit = (records) => {
        const e = records.find(isEntry(groupOf('c01'), 'c08.4'))
        return [...records, { ...e, id: 'c08.999', handler: 'c01.0.hz' }]
      }
      const report = await check(fx.healthy(), { edit, capacity: offered })
      expectRow(report, 'K2', 'FAIL', /1 handler entries name an id never offered/)
    })

    it('is inconclusive with a named reason when more records wait for an offer than the capacity', async () => {
      const offered = fx
        .healthy()
        .records()
        .filter((r) => r.stage === 'offered').length
      const report = await check(fx.healthy(), { edit: producerLast, capacity: offered })
      for (const id of ['K1', 'K2', 'K3', 'K5', 'K6'])
        expectRow(
          report,
          id,
          'INCONCLUSIVE',
          new RegExp(`more than ${offered} records wait for an id not yet offered`)
        )
      expect(report.overall).toBe('NOT ACCEPTED')
    })

    it('gives the healthy verdict for any merge of the sources that keeps each source in its own order', async () => {
      let seed = 7
      const random = () => (seed = (seed * 48271) % 2147483647) / 2147483647
      const edit = (records) => {
        const queues = new Map()
        for (const r of records) queues.set(r.source, [...(queues.get(r.source) ?? []), r])
        const out = []
        while (queues.size > 0) {
          const sources = [...queues.keys()]
          const source = sources[Math.floor(random() * sources.length)]
          out.push(queues.get(source).shift())
          if (queues.get(source).length === 0) queues.delete(source)
        }
        return out
      }
      for (let n = 0; n < 5; n++)
        expect(verdicts(await check(fx.healthy(), { edit }))).toEqual(healthyVerdicts)
    })

    it('applies a transaction decision recorded before its members were offered', async () => {
      const edit = (records) => [
        ...records.filter((r) => r.stage === 'transaction'),
        ...records.filter((r) => r.stage !== 'transaction'),
      ]
      const report = await check(fx.healthy(), { edit })
      expect(verdicts(report)).toEqual(healthyVerdicts)
    })

    it('takes the latest end of traffic whatever the order the records arrive in', async () => {
      const edit = (records) => {
        const end = records.find((r) => r.kind === 'shape' && r.phase === 'end')
        const later = { ...end, at: 9e9, t: 9e9 }
        return [later, ...records]
      }
      expectRow(
        await check(fx.healthy(), { edit }),
        'K6',
        'INCONCLUSIVE',
        /final sample was not taken last and after the end of traffic/
      )
    })
  })

  describe('information that is not judged', () => {
    const shareOutcome = (from, to, type, reason) => ({
      t: 4000,
      kind: 'identity',
      stage: 'share-outcome',
      source: 'p07',
      client: 'c07.0',
      group: groupOf('c07'),
      key: fx.key(fx.MAIN),
      from,
      to,
      type,
      reason,
      errorType: null,
    })

    it('prints the unresolved share acknowledgements by reason and the distinct offsets they cover, and changes no row', async () => {
      const edit = (records) => [
        ...records,
        shareOutcome('0', '3', 'accept', null),
        shareOutcome('4', '5', 'release', 'lock-expired'),
        shareOutcome('5', '5', 'reject', 'poison'),
        shareOutcome('7', '7', 'release', 'lock-expired'),
      ]
      const [plain, report] = [await check(fx.healthy()), await check(fx.healthy(), { edit })]
      expect(report.rows).toEqual(plain.rows)
      expect(report.overall).toBe('ACCEPTED')
      expect(formatReport(report).split('NOT JUDGED information')[1]).toMatch(
        /^ {2}unresolved share acknowledgements: 3 \(lock-expired 2, poison 1\) covering 3 distinct offsets$/m
      )
    })

    it('prints no share acknowledgement line for a journal without share-outcome records', async () => {
      const text = formatReport(await check(fx.healthy()))
      expect(text).toMatch(/NOT JUDGED information/)
      expect(text).not.toMatch(/share acknowledgements/)
    })

    const brokerState = (event, at, extra = {}) => ({
      t: at,
      at,
      kind: 'broker-state',
      source: 'p15',
      node: 2,
      pid: 77,
      container: 'kafka2',
      event,
      members: [1, 3],
      partitions: 0,
      cause: 'h00o0',
      downMs: 2000,
      processAt: at - 3,
      processTime: '2026-11-18T16:10:00.000Z',
      ...extra,
    })
    const wave = (recordId, event, observed, paused, t = 5000) => ({
      t,
      at: t,
      kind: 'wave',
      source: 'p01',
      recordId,
      execution: 'h00w0.0',
      client: 'c01.0',
      event,
      member: 'c01.9',
      paused,
      held: 0,
      observed,
    })
    const waveKey = (cause, held, t = 5000) => ({
      t,
      at: t,
      kind: 'wave-key',
      source: 'p01',
      execution: 'h00w0.0',
      cause,
      key: fx.key(fx.MAIN),
      held,
    })
    const diagnostic = (message) => ({
      t: 5000,
      at: 5000,
      kind: 'diagnostic',
      source: 'p08',
      message,
    })
    const extras = (records) => [
      ...records,
      brokerState('down', 4000),
      brokerState('ready', 6500),
      wave('p01.w1', 'transfer', true, 2),
      waveKey('p01.w1', true),
      waveKey('p01.w1', true),
      wave('p01.w2', 'return', false, 2),
      waveKey('p01.w2', true),
      waveKey('p01.w2', false),
      diagnostic('probe send c08.77 settled after the h00r0 deadline: ok'),
      diagnostic(
        'expected skip: drain commit of fs-x|0 by c05.1: moved; its current owner covers it'
      ),
    ]

    it('keeps the verdict when probe sends settle late and drain commits are skipped as expected, and counts both', async () => {
      const report = await check(fx.healthy(), { edit: extras })
      expect(report.overall).toBe('ACCEPTED')
      expect(report.information).toMatchObject({ lateProbeSends: 1, expectedSkips: 1 })
    })

    it('prints outage instants and wave transfers with their paused and held keys as information, never as a verdict', async () => {
      const report = await check(fx.healthy(), { edit: extras })
      expect(report.information.outages).toEqual([
        { node: 2, event: 'down', at: 4000, downMs: 2000, processAt: 3997 },
        { node: 2, event: 'ready', at: 6500, downMs: 2000, processAt: 6497 },
      ])
      expect(report.information.waves).toEqual({
        transfer: 1,
        return: 1,
        unobserved: 1,
        keys: { transfer: { paused: 2, held: 2 }, return: { paused: 2, held: 1 } },
      })
      expect(report.information.incompleteWaves).toEqual([])
      const text = formatReport(report)
      const info = text.slice(text.indexOf('NOT JUDGED information'))
      expect(info).toMatch(/broker 2 down at 4000 \(configured down 2000 ms, container at 3997\)/)
      expect(info).toMatch(
        /wave transfers 1 \(2 paused keys, 2 held by the member\), returns 1 \(2 paused keys, 1 held by the member\), not observed 1/
      )
      expect(info).not.toMatch(/incomplete wave/)
      expect(info).toMatch(/probe sends settled after their step: 1/)
      expect(info).toMatch(/drain commits skipped as expected: 1/)
      expect(info).not.toMatch(/PASS|FAIL|INCONCLUSIVE/)
    })

    it('lists a wave whose counted wave-key records do not all arrive as incomplete information without changing a row', async () => {
      const edit = (records) => [
        ...extras(records),
        wave('p01.w3', 'transfer', true, 3),
        waveKey('p01.w3', true),
      ]
      const report = await check(fx.healthy(), { edit })
      expect(report.overall).toBe('ACCEPTED')
      expect(report.information.incompleteWaves).toEqual(['p01.w3 has 1 of 3 wave-key records'])
      expect(report.information.waves.keys.transfer).toEqual({ paused: 3, held: 3 })
      const text = formatReport(report)
      expect(text.slice(text.indexOf('NOT JUDGED information'))).toMatch(
        /incomplete wave: p01\.w3 has 1 of 3 wave-key records/
      )
    })

    it('rejects an artifact whose stream misses a wave-key record its wave counts, and still prints the wave as incomplete', async () => {
      const world = smoke()
      const base = world.records()
      const t = Math.max(...base.filter((r) => r.source === 'p01').map((r) => r.t))
      const cause = `p01.${base.filter((r) => r.source === 'p01').length}`
      const report = await onDisk({
        ...world,
        records: () => [...base, wave(cause, 'transfer', true, 2, t), waveKey(cause, true, t)],
      })
      expectRow(report, 'K7', 'FAIL', /misses 1 of its wave-key records/)
      expect(report.overall).toBe('REJECTED')
      expect(report.information.incompleteWaves).toEqual([`${cause} has 1 of 2 wave-key records`])
    })

    /** @returns {object[]} `n` alternating down and ready records of broker 2, one second apart */
    const outages = (n) =>
      Array.from({ length: n }, (_, k) => brokerState(k % 2 ? 'ready' : 'down', 4000 + k * 1000))

    it('keeps and prints every down and ready record of the six scheduled outages of the acceptance run', async () => {
      const report = await check(fx.healthy(), { edit: (records) => [...records, ...outages(12)] })
      expect(report.information.outages.map((o) => `${o.event}@${o.at}`)).toEqual(
        outages(12).map((o) => `${o.event}@${o.at}`)
      )
      expect(report.information.outagesNotShown).toBe(0)
      const info = formatReport(report).split('NOT JUDGED information')[1]
      expect(info.match(/^ {2}broker 2 (down|ready) at/gm)).toHaveLength(12)
      expect(info).toMatch(/broker 2 ready at 15000/)
      expect(info).not.toMatch(/not shown/)
    })

    it('prints how many down and ready records beyond the schedule it did not keep', async () => {
      const report = await check(fx.healthy(), { edit: (records) => [...records, ...outages(15)] })
      expect(report.information.outages).toHaveLength(12)
      expect(report.information.outagesNotShown).toBe(3)
      expect(formatReport(report)).toMatch(/3 more broker down or ready records not shown/)
    })

    it('leaves a broker ISR record out of the outage instants', async () => {
      const edit = (records) => [...records, brokerState('down', 4000), brokerState('isr', 5000)]
      const report = await check(fx.healthy(), { edit })
      expect(report.information.outages.map((o) => o.event)).toEqual(['down'])
      expect(formatReport(report)).not.toMatch(/broker 2 isr/)
    })

    /** The open-wave cap: every client kind running every wave of one membership execution at once, transfer and return. */
    const waveCap =
      fx.matrix.schedule.executions.find((e) => e.shape === 'membership-churn').waves *
      2 *
      Object.keys(fx.matrix.clientKinds).length

    it('keeps the verdict with as many open waves as the schedule allows at once', async () => {
      const edit = (records) => [
        ...records,
        ...Array.from({ length: waveCap }, (_, n) => wave(`p01.o${n}`, 'transfer', true, 1)),
      ]
      const report = await check(fx.healthy(), { edit })
      expect(report.overall).toBe('ACCEPTED')
      expect(report.information.incompleteWaves).toHaveLength(20)
    })

    it('is inconclusive with one named reason, and counts no key of a dropped wave, when more waves stay open than the schedule allows', async () => {
      const edit = (records) => [
        ...records,
        ...Array.from({ length: waveCap + 5 }, (_, n) => wave(`p01.o${n}`, 'transfer', true, 1)),
        waveKey(`p01.o${waveCap + 2}`, true),
      ]
      const report = await check(fx.healthy(), { edit })
      const reason = new RegExp(
        `more than ${waveCap} membership waves wait for their wave-key records`
      )
      for (const id of ['K1', 'K2', 'K3', 'K5', 'K6']) expectRow(report, id, 'INCONCLUSIVE', reason)
      expect(row(report, 'K1').reasons.filter((r) => reason.test(r))).toHaveLength(1)
      expect(report.information.waves.keys.transfer).toEqual({ paused: 0, held: 0 })
      expect(report.overall).toBe('NOT ACCEPTED')
    })

    it('counts no key and keeps the verdict for a wave-key record citing no open wave', async () => {
      const report = await check(fx.healthy(), {
        edit: (records) => [...records, waveKey('p01.none', true)],
      })
      expect(report.overall).toBe('ACCEPTED')
      expect(report.information.waves.keys.transfer).toEqual({ paused: 0, held: 0 })
      expect(report.information.waves.keys.return).toEqual({ paused: 0, held: 0 })
    })

    it('fails an acknowledged send whose offset the broker did not return when it is in the dump twice', async () => {
      const world = fx.healthy()
      const lines = dumpOf(world, 'main')
      const twice = lines.find((l) => l.includes('\tc08.3\t')).replace(/Offset:\d+/, 'Offset:900')
      fx.redump(world, 'main', [...lines, twice])
      const edit = mapRecords(
        (r) => r.stage === 'ack' && r.id === 'c08.3',
        () => ({ offset: null })
      )
      expectRow(await check(world, { edit }), 'K1', 'FAIL', /present more than once.*c08\.3/)
    })
  })

  describe('configuration', () => {
    it('leaves K1 to K6 inconclusive when the config does not name both standing topics', async () => {
      const world = fx.healthy()
      world.config.topics = { main: fx.MAIN }
      const report = await check(world)
      for (const id of ['K1', 'K2', 'K3', 'K5', 'K6'])
        expectRow(report, id, 'INCONCLUSIVE', /config\.topics does not name the topics/)
    })
  })

  describe('memory bound', () => {
    it('keeps state proportional to offered ids and owned keys, not to the number of entries', () => {
      const world = fx.healthy()
      const journal = new Journal({
        matrix: world.matrix,
        config: world.config,
        mode: 'acceptance',
        capacity: 100,
      })
      const records = world.records().filter((r) => r.kind === 'identity' || r.kind === 'boundary')
      records.forEach((r) => journal.add(r))
      const size = () => ({
        ids: journal.names.length,
        txs: journal.txs.size,
        owners: journal.owners.size,
      })
      const before = size()
      const entry = records.find((r) => r.stage === 'handler-enter' && r.authority)
      for (let i = 0; i < 20000; i++) journal.add({ ...entry, handler: `h${i}` })
      expect(size()).toEqual(before)
      expect(before.ids).toBe(12 + 6)
    })

    it.each([
      ['offered ids', 5, (records) => records, /more than 5 offered ids/],
      [
        'open ownership intervals',
        18,
        (records) => {
          const b = records.find((r) => r.kind === 'boundary')
          return [
            ...records,
            ...[2, 3, 4, 5, 6].map((n) => ({
              ...b,
              t: 9e7 + n,
              run: `${b.client}.r${n}`,
              authority: { ...b.authority, run: `${b.client}.r${n}` },
            })),
          ]
        },
        /more than 18 open ownership intervals/,
      ],
      [
        'transactions',
        18,
        (records) => {
          const x = records.find((r) => r.stage === 'transaction')
          return [
            ...records,
            ...Array.from({ length: 17 }, (_, n) => ({
              ...x,
              t: 9e7 + n,
              transaction: `c09.0.y${n}`,
            })),
          ]
        },
        /more than 18 transactions/,
      ],
    ])(
      'refuses %s beyond the capacity and leaves K1 to K6 inconclusive',
      async (_, capacity, edit, reason) => {
        const report = await check(fx.healthy(), { capacity, edit })
        for (const id of ['K1', 'K2', 'K3', 'K5', 'K6'])
          expectRow(report, id, 'INCONCLUSIVE', reason)
      }
    )
  })

  describe('a rehearsal', () => {
    it('passes every row of a healthy three-hour artifact on disk, K5 on its three boundaries, yet is NOT ACCEPTED and says rehearsal runs are diagnostic', async () => {
      const world = rehearsal()
      world.supervisor.diagnostic = true
      const report = await onDisk(world)
      expect(report.rows.map((r) => `${r.id}:${r.verdict}`)).toEqual(
        ['K1', 'K2', 'K3', 'K4', 'K5', 'K6', 'K7'].map((id) => `${id}:PASS`)
      )
      expect(Object.keys(row(report, 'K5').numbers)).toHaveLength(12)
      expect(report.overall).toBe('NOT ACCEPTED')
      expect(formatReport(report)).toMatch(
        /^CORE VERDICT: NOT ACCEPTED \(rehearsal runs are diagnostic and are never ACCEPTED\)/
      )
    })

    /** @returns {object} a rehearsal whose judged bytes of `slot` follow `bytes(h)` */
    const judgedRehearsal = (slot, bytes) => {
      const world = rehearsal()
      const base = world.heap.judged
      world.heap.judged = (s, h) => (s === slot ? bytes(h) : base(s, h))
      return world
    }

    it('judges K5 by the matrix endpoint bound on judged bytes, not scaled to its length: growth of exactly the bound passes, one byte more fails', async () => {
      const bound = fx.matrix.bounds['heap-endpoint'].value
      const grown = (bytes) => judgedRehearsal('p04', (h) => 9e6 + (h === 2 ? bytes : 0))
      const at = await check(grown(bound))
      expectRow(at, 'K5', 'PASS')
      expect(row(at, 'K5').numbers.p04.endpoint).toBe(bound)
      expectRow(await check(grown(bound + 1)), 'K5', 'FAIL', /p04 judged heap endpoint/)
    })

    it('judges K5 by the matrix slope bound per minute on judged bytes: just under it no slope reason, just over it a slope FAIL', async () => {
      const slope = fx.matrix.bounds['heap-slope'].value
      const reasons = async (minutes) =>
        row(
          await check(judgedRehearsal('p05', (h) => 9e6 + h * minutes * slope)),
          'K5'
        ).reasons.join('\n')
      expect(await reasons(59)).not.toMatch(/p05 judged heap slope/)
      expect(await reasons(61)).toMatch(/p05 judged heap slope \d+ B\/min exceeds/)
    })

    it('is inconclusive in K5 when a process has fewer than its three usable heap snapshots', async () => {
      const world = rehearsal()
      world.supervisor.heapSnapshots = world.supervisor.heapSnapshots.filter(
        (r) => !(r.slot === 'p03' && r.boundary === 'h01')
      )
      expectRow(await check(world), 'K5', 'INCONCLUSIVE', /p03 has 2 of 3 usable heap snapshots/)
    })

    it('keeps the summary and every snapshot of each process on disk, and re-derives the verdict from those sealed files', async () => {
      const world = judgedRehearsal('p04', (h) => 9e6 + (h === 2 ? 3 * 1024 * 1024 : 0))
      let dir = null
      const report = await onDisk(world, (d) => {
        dir = d
      })
      expect(fs.readdirSync(path.join(dir, 'heap')).sort()).toEqual(
        [
          'summary.json',
          ...world.config.heapSnapshots.flatMap((slot) => [
            `${slot}.h00.heapsnapshot`,
            `${slot}.h01.heapsnapshot`,
            `${slot}.h02.heapsnapshot`,
          ]),
        ].sort()
      )
      const header = JSON.parse(fs.readFileSync(path.join(dir, 'header.json'), 'utf8'))
      expect(header.files.map((f) => f.file)).toContain('heap/summary.json')
      const summary = JSON.parse(fs.readFileSync(path.join(dir, 'heap/summary.json'), 'utf8'))
      expect(summary.records).toHaveLength(world.config.heapSnapshots.length * 3)
      expectRow(report, 'K5', 'FAIL', /p04 judged heap endpoint 3145728 bytes exceeds 2097152/)
      expect(row(report, 'K5').reasons).toHaveLength(1)
    })
  })

  describe('a whole artifact on disk', () => {
    it('is checked end to end: stream validated, manifest bound, smoke named diagnostic', async () => {
      const report = await onDisk(smoke())
      expect(report.rows.map((r) => `${r.id}:${r.verdict}`)).toEqual([
        'K1:PASS',
        'K2:PASS',
        'K3:PASS',
        'K4:PASS',
        'K5:INCONCLUSIVE',
        'K6:PASS',
        'K7:PASS',
      ])
      expect(report.overall).toBe('NOT ACCEPTED')
      expect(formatReport(report)).toMatch(/smoke runs are diagnostic/)
    })

    it('fails K7 and trusts nothing from a stream that was changed after the header hashed it', async () => {
      const report = await onDisk(smoke(), (dir) => {
        const file = path.join(dir, 'stream.ndjson')
        fs.writeFileSync(file, fs.readFileSync(file, 'utf8').replace('"c08.3"', '"c08.4"'))
      })
      expectRow(report, 'K7', 'FAIL', /digest|manifest/)
      expectRow(report, 'K1', 'INCONCLUSIVE', /not valid/)
    })

    it('fails K7 for a stream whose last line has no newline', async () => {
      const report = await onDisk(smoke(), (dir) => {
        const file = path.join(dir, 'stream.ndjson')
        fs.writeFileSync(file, fs.readFileSync(file, 'utf8').replace(/\n$/, ''))
      })
      expectRow(report, 'K7', 'FAIL', /ends without a newline/)
    })

    it('fails K7 and names the checkout when the artifact ran a matrix other than the checkout’s', async () => {
      const report = await onDisk(smoke(), (dir) => {
        const file = path.join(dir, 'archive/scripts/soak/finalSoakMatrix.json')
        const m = JSON.parse(fs.readFileSync(file, 'utf8'))
        m.bounds['heap-endpoint'].value = 1e12
        fs.writeFileSync(file, JSON.stringify(m))
      })
      const checkout = path.resolve(__dirname, '../../..')
      expectRow(
        report,
        'K7',
        'FAIL',
        new RegExp(`matrix of the checkout this checker runs from \\(${checkout}\\)`)
      )
    })

    it.each(['supervisor.json', 'config.json'])(
      'fails K7 when %s changed after the header bound it',
      async (file) => {
        const report = await onDisk(smoke(), (dir) => fs.appendFileSync(path.join(dir, file), ' '))
        expectRow(
          report,
          'K7',
          'FAIL',
          new RegExp(`${file.replace('.', '\\.')}: differs from the header manifest`)
        )
      }
    )

    it.each([
      [
        'disagrees with the header exit',
        (dir) =>
          fs.writeFileSync(path.join(dir, 'outer-exit.json'), '{"status":143,"observedAt":1}'),
      ],
      ['is missing', (dir) => fs.rmSync(path.join(dir, 'outer-exit.json'))],
    ])('fails K7 when outer-exit.json %s', async (_, mutate) => {
      expectRow(
        await onDisk(smoke(), mutate),
        'K7',
        'FAIL',
        /outer-exit\.json is missing or disagrees/
      )
    })

    it('fails K7 when a manifest-bound outer-exit.json has the header exit status but another observation time', async () => {
      const world = smoke()
      world.outerExit = { observedAt: world.header.exit.observedAt + 1 }
      const report = await onDisk(world)
      expectRow(
        report,
        'K7',
        'FAIL',
        /outer-exit\.json is missing or disagrees with the header exit/
      )
      expect(row(report, 'K7').reasons.join('\n')).not.toMatch(/outer-exit\.json: differs/)
    })

    it('fails K7 when outer-exit.json changed after the header bound it, even with the same values', async () => {
      const report = await onDisk(smoke(), (dir) => {
        const file = path.join(dir, 'outer-exit.json')
        fs.writeFileSync(file, JSON.stringify(JSON.parse(fs.readFileSync(file, 'utf8')), null, 2))
      })
      expectRow(report, 'K7', 'FAIL', /outer-exit\.json: differs from the header manifest/)
    })

    it.each([['supervisor.json'], ['header.json']])(
      'fails K7 and does not read %s whole when it is larger than the side-file cap',
      async (file) => {
        const report = await onDisk(smoke(), (dir) =>
          fs.writeFileSync(path.join(dir, file), ' '.repeat((16 << 20) + 1))
        )
        expectRow(report, 'K7', 'FAIL', new RegExp(`${file}: larger than 16777216 bytes`))
        expect(report.overall).toBe('REJECTED')
      }
    )

    it('fails K7 for a stream line that is not JSON', async () => {
      const report = await onDisk(smoke(), (dir) =>
        fs.appendFileSync(path.join(dir, 'stream.ndjson'), '{not json\n')
      )
      expectRow(report, 'K7', 'FAIL', /could not be read/)
    })

    it('fails K7 when the stream file is missing', async () => {
      const report = await onDisk(smoke(), (dir) => fs.rmSync(path.join(dir, 'stream.ndjson')))
      expectRow(report, 'K7', 'FAIL', /stream file cannot be read/)
    })

    it('fails K7 for a header the merged schema refuses', async () => {
      const world = smoke()
      world.header.platform = 'darwin'
      expectRow(await onDisk(world), 'K7', 'FAIL', /header\.platform is not one of linux/)
    })

    it('rejects an artifact without a header', async () => {
      const report = await onDisk(smoke(), (dir) => fs.rmSync(path.join(dir, 'header.json')))
      expectRow(report, 'K7', 'FAIL', /header\.json or config\.json is missing/)
      expect(report.overall).toBe('REJECTED')
    })

    it('rejects a partial artifact that has a partial manifest and no header', async () => {
      const dir = tmp()
      fs.writeFileSync(path.join(dir, 'manifest.partial.json'), JSON.stringify({ partial: true }))
      const report = await checkArtifact(dir)
      expectRow(report, 'K7', 'FAIL', /partial/)
      expect(report.overall).toBe('REJECTED')
    })

    it('exits 1 for a smoke artifact, 2 for a usage error, and prints the report', async () => {
      const dir = fx.writeArtifact(smoke(), tmp())
      const lines = []
      expect(await core.main([dir], (line) => lines.push(line))).toBe(1)
      expect(lines.join('\n')).toMatch(/CORE VERDICT: NOT ACCEPTED/)
      expect(await core.main([], () => {})).toBe(2)
    })
  })
})

/** Judges a world with extra input fields (for the validator problem cases). */
async function judgeWith(world, dir, extra) {
  const journal = new Journal({ matrix: world.matrix, config: world.config, mode: world.mode })
  world.records().forEach((r) => journal.add(r))
  for (const [name, text] of Object.entries(world.apache)) {
    fs.mkdirSync(path.dirname(path.join(dir, 'apache', name)), { recursive: true })
    fs.writeFileSync(path.join(dir, 'apache', name), text)
  }
  return judge({
    dir,
    matrix: world.matrix,
    config: world.config,
    mode: world.mode,
    header: fx.copy(world.header),
    supervisor: fx.copy(world.supervisor),
    journal,
    problems: [],
    ...extra,
  })
}
