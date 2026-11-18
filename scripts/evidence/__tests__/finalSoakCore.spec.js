const fs = require('node:fs')
const os = require('node:os')
const path = require('node:path')
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
  { edit = (records) => records, header = (h) => h, supervisor = (s) => s } = {}
) {
  const dir = tmp()
  const files = []
  for (const [name, text] of Object.entries(world.apache)) {
    fs.mkdirSync(path.dirname(path.join(dir, 'apache', name)), { recursive: true })
    fs.writeFileSync(path.join(dir, 'apache', name), text)
    files.push({ file: `apache/${name}`, bytes: Buffer.byteLength(text), sha256: fx.sha256(text) })
  }
  const journal = new Journal({ matrix: world.matrix, config: world.config, mode: world.mode })
  edit(world.records()).forEach((r) => journal.add(r))
  const base = { ...fx.copy(world.header), files }
  return judge({
    dir,
    matrix: world.matrix,
    config: world.config,
    mode: world.mode,
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

describe('scripts/evidence/finalSoakCore', () => {
  describe('a healthy day', () => {
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

    it('prints every other contract row as NOT JUDGED with its CI lane and never as PASS', async () => {
      const text = formatReport(await check(fx.healthy()))
      const notJudged = text.split('\n').filter((l) => /NOT JUDGED/.test(l) && /lane/.test(l))
      expect(notJudged.length).toBe(core.NOT_JUDGED.length)
      for (const item of core.NOT_JUDGED) {
        const line = notJudged.find((l) => l.includes(item.name))
        expect(line).toMatch(new RegExp(`lane: ${item.lane}`))
        expect(line).not.toMatch(/PASS/)
      }
      expect(core.NOT_JUDGED.map((i) => i.name).join('|')).toMatch(
        /share lock lifecycle.*ownership tables.*seed sensitivity.*latency.*cooperative round.*restart leave.*receipts identity.*companions.*recreate/s
      )
    })

    it('never turns a seed start, receipt identity, share expiry or checkpoint edit into a pass', async () => {
      const world = fx.healthy()
      const report = await check(world, {
        supervisor: (s) => ({ ...s, runId: 'ffffffffffff' }),
      })
      expect(row(report, 'K7').verdict).toBe('FAIL')
      expect(formatReport(report)).toMatch(/NOT JUDGED.*seed sensitivity/s)
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
      expect(report.overall).not.toBe('ACCEPTED')
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
      const edit = (records) =>
        records.map((r) => (r.stage === 'ack' && r.id === 'c08.3' ? { ...r, offset: '30' } : r))
      expectRow(await check(world, { edit }), 'K1', 'FAIL', /offset.*c08\.3/)
    })

    it('fails a record whose bytes differ from what was offered', async () => {
      const world = fx.healthy()
      world.apache['main.dump'] = world.apache['main.dump'].replace(/(c08\.2\t[^\t\n]*?)\./, '$1x')
      expectRow(await check(world), 'K1', 'FAIL', /differ.*c08\.2/)
    })

    it('is inconclusive when the dump stops before the log end, never a pass', async () => {
      const world = fx.healthy()
      fx.redump(world, 'main', dumpOf(world, 'main').slice(0, 9))
      const report = await check(world)
      expectRow(report, 'K1', 'INCONCLUSIVE', /stops at offset 8.*log ends at 12/)
      expect(report.overall).not.toBe('ACCEPTED')
    })

    it('is inconclusive when sends with unknown results are absent from the log', async () => {
      const world = fx.healthy()
      world.layout.main[7] = { ...world.layout.main[7], absent: true }
      world.apache = fx.renderApache(world)
      expectRow(await check(world), 'K1', 'INCONCLUSIVE', /1 send.*unknown.*c08\.7/)
    })

    it('is inconclusive when a transaction has no confirmed outcome', async () => {
      const world = fx.healthy()
      const edit = (records) =>
        records.map((r) =>
          r.stage === 'transaction' && r.decision === 'abort' ? { ...r, outcome: 'unknown' } : r
        )
      expectRow(await check(world, { edit }), 'K1', 'INCONCLUSIVE', /transaction.*unknown/)
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

    it.each(cases)('is inconclusive when %s exited non-zero', async (file, id) => {
      const world = fx.healthy()
      world.apache[`${file}.end`] = world.apache[`${file}.end`].replace('exit=0', 'exit=1')
      expectRow(await check(world), id, 'INCONCLUSIVE', /exit/)
    })

    it('is inconclusive when the dump holds fewer lines than its end marker counted', async () => {
      const world = fx.healthy()
      world.apache['main.dump'] = dumpOf(world, 'main').slice(0, 6).join('\n') + '\n'
      expectRow(await check(world), 'K1', 'INCONCLUSIVE', /lines/)
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

    it('is inconclusive when the dump is not offset ordered', async () => {
      const world = fx.healthy()
      const lines = dumpOf(world, 'main')
      fx.redump(world, 'main', [lines[1], lines[0], ...lines.slice(2)])
      expectRow(await check(world), 'K1', 'INCONCLUSIVE', /not increasing|order/)
    })
  })

  describe('K2 no lost delivery', () => {
    it('fails a visible record that never entered a handler in one group', async () => {
      const world = fx.healthy()
      const report = await check(world, { edit: without(isEntry(groupOf('c03'), 'c08.6')) })
      expectRow(report, 'K2', 'FAIL', /c08\.6.*g-0123456789ab-c03|g-0123456789ab-c03.*c08\.6/)
    })

    it('counts the share group as a recipient of the main topic', async () => {
      const world = fx.healthy()
      expectRow(
        await check(world, { edit: without(isEntry(groupOf('c07'), 'c08.2')) }),
        'K2',
        'FAIL',
        /c08\.2/
      )
    })

    it('counts the seed group as a recipient of the main topic', async () => {
      const world = fx.healthy()
      expectRow(
        await check(world, { edit: without(isEntry(groupOf('c13'), 'c08.9')) }),
        'K2',
        'FAIL',
        /c08\.9/
      )
    })

    it('does not ask for the delivery of a transaction member that is not visible', async () => {
      const world = fx.healthy()
      const report = await check(world, {
        edit: without((r) => r.stage === 'handler-enter' && r.id === 'c09.4'),
      })
      expectRow(report, 'K2', 'PASS')
    })

    it('is inconclusive when the dump it compares against is unusable', async () => {
      const world = fx.healthy()
      delete world.apache['main.dump.end']
      expectRow(await check(world), 'K2', 'INCONCLUSIVE', /main\.dump/)
    })
  })

  describe('K3 duplicates', () => {
    const repeat = (id, group, slot) => (records) => {
      const first = records.find(isEntry(group, id))
      return [...records, { ...first, t: first.t + 5000, handler: `${slot}.0.hx` }]
    }
    const acquire = (slot, group) => (records) => {
      const own = records.find((r) => r.kind === 'boundary' && r.group === group)
      return { ...own, t: 4500, cause: `${slot}.0.extra` }
    }

    it('fails a repeat on the same client run with no assignment change between the entries', async () => {
      const world = fx.healthy()
      const report = await check(world, { edit: repeat('c08.4', groupOf('c01'), 'c01') })
      expectRow(report, 'K3', 'FAIL', /c08\.4.*g-0123456789ab-c01/)
    })

    it('fails a repeat that an acquire record of the identical, still open ownership tries to excuse', async () => {
      const world = fx.healthy()
      const edit = (records) =>
        repeat(
          'c08.4',
          groupOf('c01'),
          'c01'
        )([...records, acquire('c01', groupOf('c01'))(records)])
      expectRow(await check(world, { edit }), 'K3', 'FAIL', /c08\.4/)
    })

    it('counts and prints a repeat after an assignment change without failing it', async () => {
      const world = fx.healthy()
      const group = groupOf('c01')
      const edit = (records) => {
        const first = records.find(isEntry(group, 'c08.4'))
        const own = records.find((r) => r.kind === 'boundary' && r.group === group)
        const end = { ...own, event: 'revoke-end', t: first.t + 1 }
        const again = { ...own, event: 'acquire', t: first.t + 2 }
        return [...records, end, again, { ...first, t: first.t + 3, handler: 'c01.0.hx' }]
      }
      const report = await check(world, { edit })
      expectRow(report, 'K3', 'PASS')
      expect(row(report, 'K3').numbers.repeatsAfterChange).toBe(1)
      expect(formatReport(report)).toMatch(/repeatsAfterChange.*1/)
    })

    it('counts a repeat on another client run as a counted repeat, not a failure', async () => {
      const world = fx.healthy()
      const group = groupOf('c01')
      const edit = (records) => {
        const first = records.find(isEntry(group, 'c08.4'))
        return [
          ...records,
          {
            ...first,
            t: first.t + 3,
            handler: 'c01.0.hx',
            authority: { ...first.authority, run: 'r2' },
          },
        ]
      }
      const report = await check(world, { edit })
      expectRow(report, 'K3', 'PASS')
      expect(row(report, 'K3').numbers.repeatsAfterChange).toBe(1)
    })

    it('does not judge a share group redelivery', async () => {
      const world = fx.healthy()
      const edit = (records) => {
        const first = records.find(isEntry(groupOf('c07'), 'c08.4'))
        return [...records, { ...first, t: first.t + 3, handler: 'c07.0.hx', deliveryCount: 2 }]
      }
      const report = await check(world, { edit })
      expectRow(report, 'K3', 'PASS')
      expect(row(report, 'K3').numbers.shareRepeats).toBe(1)
    })
  })

  describe('K4 commits', () => {
    const behind = (world, group) => {
      const text = world.apache[`groups/${group}.describe`]
      world.apache[`groups/${group}.describe`] = text.replace(
        /(\S+\s+\S+\s+0\s+)12(\s+12)/,
        '$111$2'
      )
    }

    it('fails a group whose committed offset is behind the log end', async () => {
      const world = fx.healthy()
      behind(world, groupOf('c02'))
      expectRow(await check(world), 'K4', 'FAIL', /g-0123456789ab-c02.*11.*12/)
    })

    it('does not accept client side readbacks in place of the Apache describe', async () => {
      const world = fx.healthy()
      behind(world, groupOf('c02'))
      const edit = (records) => [
        ...records,
        {
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
        },
      ]
      expectRow(await check(world, { edit }), 'K4', 'FAIL', /c02/)
    })

    it('fails a group that has no row for a topic it must commit', async () => {
      const world = fx.healthy()
      const name = `groups/${groupOf('c01')}.describe`
      const text = world.apache[name]
        .split('\n')
        .filter((l) => !l.includes(fx.TXN))
        .join('\n')
      fx.putStep(world.apache, name, text, 'Consumer group has no active members.\n')
      expectRow(await check(world), 'K4', 'FAIL', /c01.*no row/)
    })

    it('fails a group that never committed on a partition', async () => {
      const world = fx.healthy()
      const group = groupOf('c01')
      world.apache[`groups/${group}.describe`] = world.apache[`groups/${group}.describe`].replace(
        /(\S+\s+\S+\s+0\s+)12(\s+12\s+)0/,
        '$1-$2-'
      )
      expectRow(await check(world), 'K4', 'FAIL', /c01.*committed/)
    })

    it('is inconclusive when two describes disagree about the log end', async () => {
      const world = fx.healthy()
      const group = groupOf('c01')
      world.apache[`groups/${group}.describe`] = world.apache[`groups/${group}.describe`].replace(
        /(0\s+)12(\s+)12/,
        (_, a, b) => `${a}13${b}13`
      )
      expectRow(await check(world), 'K4', 'INCONCLUSIVE', /log end/)
    })

    it('does not judge the share group, which has no offset commits', async () => {
      const world = fx.healthy()
      expect(Object.keys(world.apache).some((f) => f.includes(groupOf('c07')))).toBe(false)
      expectRow(await check(world), 'K4', 'PASS')
    })
  })

  describe('K5 heap', () => {
    const heap = (process, fn) => (records) =>
      records.map((r) =>
        r.kind === 'sample' && r.role === 'quiet' && r.process === process
          ? { ...r, heapUsed: fn(Number(r.boundary.slice(1)), r.heapUsed) }
          : r
      )

    it('fails an endpoint above the bound', async () => {
      const world = fx.healthy()
      const report = await check(world, {
        edit: heap('p04', (h, v) => (h >= 21 ? v + 3 * 1024 * 1024 : v)),
      })
      expectRow(report, 'K5', 'FAIL', /p04.*endpoint/)
    })

    it('fails a slope above the bound even when the endpoint is clean', async () => {
      const world = fx.healthy()
      const report = await check(world, {
        edit: heap('p05', (h, v) => (h >= 12 && h <= 20 ? v + 500 * 1024 * 1024 : v)),
      })
      expectRow(report, 'K5', 'FAIL', /p05.*slope/)
      expect(row(report, 'K5').reasons.join('\n')).not.toMatch(/p05.*endpoint/)
    })

    it('is inconclusive when a process has fewer than 24 quiet boundaries', async () => {
      const world = fx.healthy()
      const report = await check(world, {
        edit: without((r) => r.kind === 'sample' && r.process === 'p03' && r.boundary === 'h10'),
      })
      expectRow(report, 'K5', 'INCONCLUSIVE', /p03.*23 of 24/)
    })

    it('does not let the seed process substitute for or fail an unseeded process', async () => {
      const world = fx.healthy()
      const report = await check(world, { edit: heap('p13', (h, v) => v + h * 50 * 1024 * 1024) })
      expectRow(report, 'K5', 'PASS')
    })

    it('computes the screen from the median of each boundary and a least-squares slope', () => {
      const boundaries = Array.from({ length: 24 }, (_, h) =>
        [0, 1, 2].map((i) => ({ heapUsed: 1000 + h * 100 + i, completedAt: (h * 3600 + i) * 1000 }))
      )
      const result = core.heapScreen(boundaries, { endpointBytes: 5000, slopeBytesPerMinute: 2 })
      expect(result.endpoint).toBe(2100)
      expect(result.slope).toBeCloseTo(100 / 60, 3)
      expect(result.ok).toBe(true)
    })
  })

  describe('K6 resources', () => {
    const final = (process, resources) => (records) =>
      records.map((r) =>
        r.kind === 'sample' && r.role === 'final' && r.process === process
          ? { ...r, activeResources: resources }
          : r
      )

    it('fails a handle left over after the final disconnect', async () => {
      const world = fx.healthy()
      const report = await check(world, {
        edit: final('p02', [
          { type: 'TCPSocketWrap', count: 0 },
          { type: 'Timeout', count: 3 },
        ]),
      })
      expectRow(report, 'K6', 'FAIL', /p02.*Timeout.*3.*2/)
    })

    it('fails a resource type that the baseline never had', async () => {
      const world = fx.healthy()
      const report = await check(world, {
        edit: final('p09', [
          { type: 'TCPSocketWrap', count: 0 },
          { type: 'Timeout', count: 0 },
          { type: 'MysteryWrap', count: 1 },
        ]),
      })
      expectRow(report, 'K6', 'FAIL', /p09.*MysteryWrap/)
    })

    it('is inconclusive when a process has no final sample', async () => {
      const world = fx.healthy()
      const report = await check(world, {
        edit: without((r) => r.kind === 'sample' && r.role === 'final' && r.process === 'p11'),
      })
      expectRow(report, 'K6', 'INCONCLUSIVE', /p11.*final/)
    })
  })

  describe('K7 exit and completeness', () => {
    const exitOf = (process, fields) => (records) =>
      records.map((r) =>
        r.kind === 'process' && r.event === 'exit' && r.process === process
          ? { ...r, ...fields }
          : r
      )

    it('fails a parent observed exit code that is not zero', async () => {
      expectRow(
        await check(fx.healthy(), { header: (h) => ({ ...h, exit: { ...h.exit, code: 1 } }) }),
        'K7',
        'FAIL',
        /supervisor.*code 1/
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

    it('fails a forced exit', async () => {
      const report = await check(fx.healthy(), {
        edit: exitOf('p07', { code: null, signal: 'SIGKILL' }),
      })
      expectRow(report, 'K7', 'FAIL', /p07.*SIGKILL/)
    })

    it('fails a process with no observed exit', async () => {
      const report = await check(fx.healthy(), {
        edit: without((r) => r.kind === 'process' && r.event === 'exit' && r.process === 'p12'),
      })
      expectRow(report, 'K7', 'FAIL', /p12.*no.*exit/)
    })

    it('fails a supervisor that aborted, was incomplete or recorded collector failures', async () => {
      for (const [field, value, reason] of [
        ['aborted', 'host monitor', /abort/],
        ['incomplete', 'queue timeout', /incomplete/],
        ['failures', ['stream cap'], /failure/],
      ]) {
        expectRow(
          await check(fx.healthy(), { supervisor: (s) => ({ ...s, [field]: value }) }),
          'K7',
          'FAIL',
          reason
        )
      }
    })

    it('fails a run shorter than 24 hours in acceptance mode', async () => {
      const report = await check(fx.healthy(), {
        header: (h) => ({
          ...h,
          window: { ...h.window, requestedMs: 3600000, runEndAt: h.window.runStartAt + 3600000 },
        }),
      })
      expectRow(report, 'K7', 'FAIL', /3600000.*86400000/)
    })

    it('fails a gap in the periodic sample grid', async () => {
      const report = await check(fx.healthy(), {
        edit: without(
          (r) =>
            r.kind === 'sample' && r.role === 'periodic' && r.process === 'p03' && r.slot === 1500
        ),
      })
      expectRow(report, 'K7', 'FAIL', /p03.*slot 1500/)
    })

    it('fails a periodic sample admitted before its slot', async () => {
      const report = await check(fx.healthy(), {
        edit: (records) =>
          records.map((r) =>
            r.role === 'periodic' && r.process === 'p03' && r.slot === 700
              ? { ...r, admittedAt: r.admittedAt - 60000 }
              : r
          ),
      })
      expectRow(report, 'K7', 'FAIL', /p03.*slot 700.*early/)
    })

    it('fails when the stream, header, config or matrix failed validation', async () => {
      const dir = tmp()
      const world = fx.healthy()
      const report = await judgeWith(world, dir, {
        problems: ['line 5: seq is not the next of its source'],
      })
      expectRow(report, 'K7', 'FAIL', /seq is not the next/)
      expectRow(report, 'K1', 'INCONCLUSIVE', /not valid/)
    })

    it('is inconclusive when the driver journaled a missing source', async () => {
      const edit = (records) => [
        ...records,
        {
          t: 9500,
          kind: 'diagnostic',
          source: 'p01',
          message: 'missing-source batch authority at c01.0.h7',
        },
      ]
      expectRow(await check(fx.healthy(), { edit }), 'K7', 'INCONCLUSIVE', /missing-source/)
    })

    it('is inconclusive when the supervisor receipt is absent', async () => {
      expectRow(
        await check(fx.healthy(), { supervisor: () => null }),
        'K7',
        'INCONCLUSIVE',
        /supervisor\.json/
      )
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
      const before = journal.stateSize()
      const entry = records.find((r) => r.stage === 'handler-enter' && r.authority)
      for (let i = 0; i < 20000; i++) journal.add({ ...entry, handler: `h${i}` })
      expect(journal.stateSize()).toEqual(before)
      expect(before.ids).toBe(12 + 6)
    })

    it('refuses an offered id beyond the capacity and says K1 cannot be judged', async () => {
      const world = fx.healthy()
      const journal = new Journal({
        matrix: world.matrix,
        config: world.config,
        mode: 'acceptance',
        capacity: 5,
      })
      world.records().forEach((r) => journal.add(r))
      expect(journal.problems.join('\n')).toMatch(/more than 5 offered ids/)
    })
  })

  describe('a whole artifact on disk', () => {
    it('is checked end to end: stream validated, manifest bound, smoke named diagnostic', async () => {
      const world = fx.healthy({ mode: 'smoke', withInventory: true })
      const dir = fx.writeArtifact(world, tmp())
      const report = await checkArtifact(dir)
      expect(report.rows.map((r) => `${r.id}:${r.verdict}`)).toEqual([
        'K1:PASS',
        'K2:PASS',
        'K3:PASS',
        'K4:PASS',
        'K5:INCONCLUSIVE',
        'K6:PASS',
        'K7:PASS',
      ])
      expect(report.overall).not.toBe('ACCEPTED')
      expect(formatReport(report)).toMatch(/smoke/i)
    })

    it('fails K7 and trusts nothing from a stream that was changed after the header hashed it', async () => {
      const world = fx.healthy({ mode: 'smoke', withInventory: true })
      const dir = fx.writeArtifact(world, tmp())
      const file = path.join(dir, 'stream.ndjson')
      fs.writeFileSync(file, fs.readFileSync(file, 'utf8').replace('"c08.3"', '"c08.4"'))
      const report = await checkArtifact(dir)
      expectRow(report, 'K7', 'FAIL', /digest|manifest/)
      expectRow(report, 'K1', 'INCONCLUSIVE', /not valid/)
    })

    it('fails K7 when the artifact ran a matrix other than the reviewed one', async () => {
      const world = fx.healthy({ mode: 'smoke', withInventory: true })
      const dir = fx.writeArtifact(world, tmp())
      const file = path.join(dir, 'archive/scripts/soak/finalSoakMatrix.json')
      const m = JSON.parse(fs.readFileSync(file, 'utf8'))
      m.bounds['heap-endpoint'].value = 1e12
      fs.writeFileSync(file, JSON.stringify(m))
      expectRow(await checkArtifact(dir), 'K7', 'FAIL', /matrix/)
    })

    it('exits 0 only for ACCEPTED and prints the report', async () => {
      const world = fx.healthy({ mode: 'smoke', withInventory: true })
      const dir = fx.writeArtifact(world, tmp())
      const lines = []
      expect(await core.main([dir], (line) => lines.push(line))).toBe(1)
      expect(lines.join('\n')).toMatch(/CORE VERDICT/)
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
