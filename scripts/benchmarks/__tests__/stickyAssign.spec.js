const fs = require('fs')
const os = require('os')
const path = require('path')
const { MemberAssignment } = require('../../../src/consumer/assignerProtocol')
const sticky = require('../../../src/consumer/assigners/stickyAssigner')
const bench = require('../stickyAssign')

const REPO = path.resolve(__dirname, '../../..')
const ASSIGNERS = path.join(REPO, 'src/consumer/assigners')

/** @type {string[]} */
const trees = []

/**
 * Writes a throwaway tree whose `src/consumer/assigners` wraps this tree's assigners.
 * @param {string} body module code; `real` is this tree's assigners and `wrap(factory, around)`
 *   replaces a factory's `assign` with `around(original, group)`
 * @returns {string} the tree's directory
 */
const fakeTree = (body) => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'sticky-tree-'))
  trees.push(dir)
  fs.mkdirSync(path.join(dir, 'src/consumer/assigners'), { recursive: true })
  fs.writeFileSync(
    path.join(dir, 'src/consumer/assigners/index.js'),
    `const real = require(${JSON.stringify(ASSIGNERS)})
const wrap = (factory, around) => (deps) => {
  const assigner = factory(deps)
  const original = assigner.assign.bind(assigner)
  assigner.assign = (group) => around(original, group)
  return assigner
}
${body}`
  )
  return dir
}

/** The first `sticky` call waits 300 ms, every later one is unchanged. */
const slowFirstCall = () =>
  fakeTree(`let calls = 0
module.exports = { ...real, sticky: wrap(real.sticky, (original, group) => {
  if (calls++ === 0) { const end = Date.now() + 300; while (Date.now() < end); }
  return original(group)
}) }`)

/** `cooperative-sticky` appends a byte to the first member's buffer. */
const differentCooperative = () =>
  fakeTree(`module.exports = { ...real, cooperativeSticky: wrap(real.cooperativeSticky, async (original, group) => {
  const [first, ...rest] = await original(group)
  return [{ ...first, memberAssignment: Buffer.concat([first.memberAssignment, Buffer.from([0])]) }, ...rest]
}) }`)

/**
 * The first process to call `sticky` waits 500 ms; it leaves a marker file, so every later
 * process runs at full speed.
 */
const slowFirstProcess = () => {
  const marker = path.join(os.tmpdir(), `sticky-marker-${process.pid}-${trees.length}`)
  trees.push(marker)
  return fakeTree(`const fs = require('fs')
module.exports = { ...real, sticky: wrap(real.sticky, (original, group) => {
  if (!fs.existsSync(${JSON.stringify(marker)})) {
    fs.writeFileSync(${JSON.stringify(marker)}, '')
    const end = Date.now() + 500; while (Date.now() < end);
  }
  return original(group)
}) }`)
}

/** `sticky` never returns. */
const hangingTree = () =>
  fakeTree(`module.exports = { ...real, sticky: wrap(real.sticky, () => { for (;;) {} }) }`)

/** `sticky` throws. */
const throwingTree = () =>
  fakeTree(
    `module.exports = { ...real, sticky: wrap(real.sticky, () => { throw new Error('boom') }) }`
  )

/**
 * @param {Array<[string, {[topic: string]: number[]}]>} entries member id and assignment
 * @returns {Array<{ memberId: string, memberAssignment: Buffer }>} an assigner's result
 */
const result = (entries) =>
  entries.map(([memberId, assignment]) => ({
    memberId,
    memberAssignment: MemberAssignment.encode({ version: 3, assignment }),
  }))

afterAll(() => {
  trees.forEach((dir) => fs.rmSync(dir, { recursive: true, force: true }))
})

describe('scripts > benchmarks > stickyAssign', () => {
  describe('fixed shapes', () => {
    test('names the stag and wide shapes the curve is measured on', () => {
      expect(Object.keys(bench.FIXED)).toEqual([
        'stag-25+25x1k',
        'stag-100+100x5k',
        'stag-150+150x15k',
        'stag-500+500x30k',
        'stag-0+300x15k',
        'wide-10+10',
        'wide-20+20',
        'wide-150+150',
        'wide-0+300',
      ])
      Object.entries(bench.FIXED).forEach(([name, build]) => expect(build().name).toEqual(name))
    })

    test('names the check-only shape, and only `--check` takes it by default', () => {
      const shape = bench.CHECK_ONLY['capacity-3']()

      expect(Object.keys(bench.CHECK_ONLY)).toEqual(['capacity-3'])
      expect(shape.name).toEqual('capacity-3')
      expect(shape.members.map(({ memberId, topics }) => [memberId, topics])).toEqual([
        ['member-0001', ['topic-B']],
        ['member-0002', ['topic-A', 'topic-B']],
        ['member-0003', ['topic-A']],
      ])
      expect(shape.members[0].owned).toEqual([{ topic: 'topic-A', partitions: [0, 1] }])
      expect(bench.defaultShapes(false)).toEqual(Object.keys(bench.FIXED))
      expect(bench.defaultShapes(true)).toEqual([...Object.keys(bench.FIXED), 'capacity-3'])
    })

    test('stag: ten topics, staggered windows of five to seven, owners at generation 1', () => {
      const shape = bench.stag(25, 25, 1000)
      const topic = (i) => `topic-${String(i).padStart(4, '0')}`

      expect(Object.keys(shape.partitionsPerTopic)).toHaveLength(10)
      expect(shape.partitionsPerTopic[topic(9)]).toHaveLength(100)
      expect(shape.members).toHaveLength(50)
      expect(shape.members[0].topics).toEqual([0, 1, 2, 3, 4].map(topic))
      expect(shape.members[7].topics).toEqual([2, 3, 4, 5, 6, 7].map(topic))
      expect(shape.members[24].generationId).toEqual(1)
      // topic-0000 is subscribed by owners 0, 5, 10, 15 and 20, so it is dealt five ways.
      expect(shape.members[0].owned[0]).toEqual({
        topic: topic(0),
        partitions: Array.from({ length: 20 }, (_, i) => i * 5),
      })
      expect(shape.members[25]).toEqual({
        memberId: 'member-0025',
        groupInstanceId: null,
        topics: [0, 1, 2, 3, 4, 5].map(topic),
        owned: [],
        generationId: -1,
      })
    })

    test('the previous round gives every partition of a subscribed topic one owner that subscribes to it', () => {
      const shape = bench.stag(25, 25, 1000)
      const owners = new Map()

      for (const { memberId, topics, owned } of shape.members) {
        for (const { topic, partitions } of owned) {
          expect(topics).toContain(topic)
          partitions.forEach((p) => {
            expect(owners.has(`${topic}-${p}`)).toBe(false)
            owners.set(`${topic}-${p}`, memberId)
          })
        }
      }

      expect(owners.size).toEqual(1000)
    })

    test('wide: 100 topics of 150 partitions in narrow windows, 55 of them subscribed by 300 members', () => {
      const shape = bench.wide(10, 10)
      const subscribed = new Set(shape.members.flatMap(({ topics }) => topics))

      expect(Object.keys(shape.partitionsPerTopic)).toHaveLength(100)
      expect(shape.partitionsPerTopic['topic-0099']).toHaveLength(150)
      expect(shape.members[13].topics).toEqual([
        'topic-0013',
        'topic-0014',
        'topic-0015',
        'topic-0016',
        'topic-0017',
        'topic-0018',
        'topic-0019',
      ])
      expect(bench.wide(150, 150).members).toHaveLength(300)
      expect(new Set(bench.wide(0, 300).members.flatMap(({ topics }) => topics)).size).toEqual(55)
      expect(subscribed.size).toEqual(25)
    })
  })

  describe('random shapes', () => {
    test('a seed names the same draws', () => {
      const a = bench.seeded(7)
      const b = bench.seeded(7)
      const draws = Array.from({ length: 20 }, () => a(1000))

      expect(Array.from({ length: 20 }, () => b(1000))).toEqual(draws)
      expect(new Set(draws).size).toBeGreaterThan(15)
    })

    test('stay within 8 members and 12 topics of 1 to 16 partitions, each member subscribed', () => {
      const next = bench.seeded(1)
      const generations = new Set()
      let staticMembers = 0
      let unsubscribedOwned = 0

      for (let i = 0; i < 300; i++) {
        const shape = bench.randomShape(next, i)
        const topics = Object.keys(shape.partitionsPerTopic)

        expect(shape.name).toEqual(`random-${i}`)
        expect(shape.members.length).toBeGreaterThanOrEqual(1)
        expect(shape.members.length).toBeLessThanOrEqual(8)
        expect(topics.length).toBeLessThanOrEqual(12)
        Object.values(shape.partitionsPerTopic).forEach((p) => {
          expect(p.length).toBeGreaterThanOrEqual(1)
          expect(p.length).toBeLessThanOrEqual(16)
        })

        for (const member of shape.members) {
          expect(member.topics.length).toBeGreaterThan(0)
          generations.add(member.generationId)
          staticMembers += member.groupInstanceId === null ? 0 : 1
          unsubscribedOwned += member.owned.filter((o) => !member.topics.includes(o.topic)).length
        }
      }

      expect([...generations].sort()).toEqual([-1, 0, 1, 2])
      expect(staticMembers).toBeGreaterThan(0)
      expect(unsubscribedOwned).toBeGreaterThan(0)
    })
  })

  describe('checkInvariants', () => {
    const shape = {
      partitionsPerTopic: { a: [0, 1, 2], b: [0, 1] },
      members: [
        { memberId: 'm1', topics: ['a', 'b'] },
        { memberId: 'm2', topics: ['a', 'b'] },
        { memberId: 'm3', topics: ['b'] },
      ],
    }

    test('passes a sticky assignment of a fixed shape', async () => {
      const stag = bench.stag(25, 25, 1000)
      const assigned = await bench.assign(sticky, stag, bench.groupMembers(stag))

      expect(bench.checkInvariants(stag, assigned)).toEqual([])
    })

    test('names a partition assigned twice, one assigned to nobody and one given to a non-subscriber', () => {
      const assigned = result([
        ['m1', { a: [0, 1] }],
        ['m2', { a: [1], b: [0] }],
        ['m3', { a: [2] }],
      ])

      expect(bench.checkInvariants(shape, assigned)).toEqual([
        'a-1 assigned to m1 and m2',
        'a-2 assigned to m3, which does not subscribe to a',
        'b-1 assigned to nobody',
      ])
    })

    test('names members with the same subscription two partitions apart', () => {
      const assigned = result([
        ['m1', { a: [0, 1] }],
        ['m2', { a: [2] }],
        ['m3', { b: [0, 1] }],
      ])
      const twoApart = result([
        ['m1', { a: [0, 1] }],
        ['m2', {}],
        ['m3', { b: [0, 1] }],
      ])

      expect(bench.checkInvariants(shape, assigned)).toEqual([])
      expect(bench.checkInvariants(shape, twoApart)).toEqual([
        'a-2 assigned to nobody',
        'members subscribed to a,b hold 2, 0 partitions',
      ])
    })

    test('names a member that has no result, and counts it as holding nothing', () => {
      const twoMembers = {
        partitionsPerTopic: { a: [0, 1] },
        members: [
          { memberId: 'm1', topics: ['a'] },
          { memberId: 'm2', topics: ['a'] },
        ],
      }

      expect(bench.checkInvariants(twoMembers, result([['m1', { a: [0, 1] }]]))).toEqual([
        'm2 has no result',
        'members subscribed to a hold 2, 0 partitions',
      ])
    })

    test('names a partition outside its topic metadata', () => {
      const oneMember = {
        partitionsPerTopic: { a: [0] },
        members: [{ memberId: 'm1', topics: ['a'] }],
      }

      expect(bench.checkInvariants(oneMember, result([['m1', { a: [0, 999] }]]))).toEqual([
        'a-999 assigned to m1, but a has no partition 999',
      ])
    })

    test('names a member that has two results', () => {
      const assigned = result([
        ['m1', { a: [0, 1] }],
        ['m2', { a: [2] }],
        ['m3', { b: [0, 1] }],
        ['m3', {}],
      ])

      expect(bench.checkInvariants(shape, assigned)).toEqual(['m3 has more than one result'])
    })

    test('names a result for a member that is not in the group', () => {
      const assigned = result([
        ['m1', { a: [0, 1] }],
        ['m2', { a: [2] }],
        ['m3', { b: [0, 1] }],
        ['m9', {}],
      ])

      expect(bench.checkInvariants(shape, assigned)).toEqual(['m9 is not a member of the group'])
    })

    test('names a member outside the group that holds a partition, without throwing', () => {
      const assigned = result([
        ['m1', { a: [0, 1] }],
        ['m2', { a: [2] }],
        ['m3', { b: [0] }],
        ['m9', { b: [1] }],
      ])

      expect(bench.checkInvariants(shape, assigned)).toEqual([
        'm9 is not a member of the group',
        'b-1 assigned to nobody',
      ])
    })
  })

  describe('the differential check', () => {
    test('finds every random shape identical when the baseline is this tree', async () => {
      const summary = await bench.checkRandomShapes({ baselineTree: REPO, count: 25, seed: 3 })

      expect(summary).toEqual({
        random: 25,
        seed: 3,
        identical: 25,
        redrawn: 0,
        differences: [],
      })
    })

    test('names a shape whose buffers differ', async () => {
      const summary = await bench.checkRandomShapes({
        baselineTree: differentCooperative(),
        count: 3,
        seed: 3,
      })

      expect(summary.identical).toEqual(0)
      expect(summary.differences).toEqual(['random-0', 'random-1', 'random-2'])
    })

    test('draws a shape again when the baseline takes too long on it', async () => {
      const summary = await bench.checkRandomShapes({
        baselineTree: slowFirstCall(),
        count: 2,
        seed: 3,
        redrawMs: 150,
      })

      expect(summary).toMatchObject({ identical: 2, redrawn: 1, differences: [] })
    })

    test('compares a fixed shape run by a child process of the baseline tree', async () => {
      await expect(bench.checkFixedShape('stag-25+25x1k', REPO)).resolves.toEqual({
        shape: 'stag-25+25x1k',
        compared: true,
        identical: true,
        violations: [],
      })
      await expect(
        bench.checkFixedShape('stag-25+25x1k', differentCooperative())
      ).resolves.toMatchObject({ compared: true, identical: false })
    })

    test('compares the check-only shape without the invariants its previous round breaks', async () => {
      const shape = bench.CHECK_ONLY['capacity-3']()
      const assigned = await bench.assign(sticky, shape, bench.groupMembers(shape))

      expect(bench.checkInvariants(shape, assigned)).toEqual([
        'topic-A-0 assigned to member-0001, which does not subscribe to topic-A',
        'topic-A-1 assigned to member-0001, which does not subscribe to topic-A',
      ])
      await expect(bench.checkFixedShape('capacity-3', REPO)).resolves.toEqual({
        shape: 'capacity-3',
        compared: true,
        identical: true,
        violations: [],
      })
      await expect(
        bench.checkFixedShape('capacity-3', differentCooperative())
      ).resolves.toMatchObject({ compared: true, identical: false })
    })

    test('checks only the invariants of a fixed shape the baseline does not finish', async () => {
      await expect(bench.checkFixedShape('stag-25+25x1k', hangingTree(), 2000)).resolves.toEqual({
        shape: 'stag-25+25x1k',
        compared: false,
        identical: true,
        violations: [],
      })
    })

    test('fails when the baseline tree throws', async () => {
      await expect(bench.checkFixedShape('stag-25+25x1k', throwingTree())).rejects.toThrow(
        /worker for stag-25\+25x1k in .* failed: .*boom/s
      )
    })
  })

  describe('timing', () => {
    test('runs a warm-up and the measured runs of an arm, and records an arm over the limit as not finished', () => {
      const line = bench.timeShape('stag-25+25x1k', {
        runs: 3,
        arms: [
          { key: 'tree', dir: REPO },
          { key: 'baseline', dir: hangingTree() },
        ],
        limitMs: 2000,
      })

      expect(line.shape).toEqual('stag-25+25x1k')
      expect(line.tree.runs).toHaveLength(4)
      expect(line.baseline).toEqual('did not finish')
      expect(line.ratio).toBeUndefined()
    })

    test('reports the median without the warm-up run, and the ratio of the medians', () => {
      const line = bench.timeShape('stag-25+25x1k', {
        runs: 1,
        arms: [
          { key: 'tree', dir: REPO },
          { key: 'baseline', dir: slowFirstProcess() },
        ],
      })

      expect(line.tree.runs).toHaveLength(2)
      expect(line.baseline.runs[0]).toBeGreaterThanOrEqual(500)
      expect(line.baseline.medianMs).toBeLessThan(400)
      expect(line.ratio).toBeCloseTo(line.tree.medianMs / line.baseline.medianMs)
    })

    test('swaps the order of the arms every round', () => {
      const log = path.join(os.tmpdir(), `sticky-order-${process.pid}`)
      trees.push(log)
      const logging = (tag) =>
        fakeTree(`const fs = require('fs')
module.exports = { ...real, sticky: wrap(real.sticky, (original, group) => {
  fs.appendFileSync(${JSON.stringify(log)}, ${JSON.stringify(tag)})
  return original(group)
}) }`)

      bench.timeShape('stag-25+25x1k', {
        runs: 2,
        arms: [
          { key: 'tree', dir: logging('a') },
          { key: 'baseline', dir: logging('b') },
        ],
      })

      expect(fs.readFileSync(log, 'utf8')).toEqual('abbaab')
    })
  })

  describe('the child side', () => {
    test('prints the time of a run, or both assigners buffers', async () => {
      const printed = []
      const log = jest.spyOn(console, 'log').mockImplementation((line) => printed.push(line))

      try {
        await bench.worker('time', 'stag-25+25x1k', REPO)
        await bench.worker('bytes', 'stag-25+25x1k', REPO)
      } finally {
        log.mockRestore()
      }

      expect(Object.keys(JSON.parse(printed[0]))).toEqual(['ms'])
      const { sticky: stickyHex, cooperative } = JSON.parse(printed[1])
      expect(stickyHex).toHaveLength(50)
      expect(cooperative).toHaveLength(50)
      expect(stickyHex[0]).toMatch(/^member-0000:0003/)
    })
  })

  describe('the command line', () => {
    const run = async (argv) => {
      const lines = []
      const code = await bench.main(argv, (line) => lines.push(JSON.parse(line)))
      return { code, lines }
    }

    test.each([
      [['--shapes', 'stag-1+1']],
      [['--runs', '0']],
      [['--random', '-1', '--check', '--baseline-tree', REPO]],
      [['--seed', '1.5']],
      [['--check']],
    ])('refuses %j with exit code 2', async (argv) => {
      const { code, lines } = await run(argv)

      expect(code).toEqual(2)
      expect(lines[0]).toMatchObject({ error: 'bad arguments' })
      expect(lines[0].shapes).toEqual(bench.defaultShapes(true))
    })

    test('times the named shapes of this tree alone, a warm-up and one measured run for --runs 1', async () => {
      const { code, lines } = await run(['--runs', '1', '--shapes', 'stag-25+25x1k'])

      expect(code).toEqual(0)
      expect(lines).toHaveLength(1)
      expect(lines[0].tree.runs).toHaveLength(2)
      expect(lines[0].baseline).toBeUndefined()
    })

    test('times the named shapes against a baseline tree and reports the ratio of the medians', async () => {
      const { code, lines } = await run([
        '--runs',
        '1',
        '--shapes',
        'stag-25+25x1k',
        '--baseline-tree',
        REPO,
      ])

      expect(code).toEqual(0)
      expect(lines[0].baseline.runs).toHaveLength(2)
      expect(lines[0].ratio).toBeCloseTo(lines[0].tree.medianMs / lines[0].baseline.medianMs)
    })

    test('checks a check-only shape when it is named', async () => {
      const { code, lines } = await run([
        '--check',
        '--baseline-tree',
        REPO,
        '--shapes',
        'capacity-3',
        '--random',
        '0',
      ])

      expect(code).toEqual(0)
      expect(lines).toEqual([
        { shape: 'capacity-3', compared: true, identical: true, violations: [] },
        { random: 0, seed: 1, identical: 0, redrawn: 0, differences: [] },
      ])
    })

    test('passes a check against this tree and fails one against a different tree', async () => {
      const same = await run([
        '--check',
        '--baseline-tree',
        REPO,
        '--shapes',
        'stag-25+25x1k',
        '--random',
        '5',
      ])
      const other = await run([
        '--check',
        '--baseline-tree',
        differentCooperative(),
        '--shapes',
        'stag-25+25x1k',
        '--random',
        '0',
      ])

      expect(same.code).toEqual(0)
      expect(same.lines).toEqual([
        { shape: 'stag-25+25x1k', compared: true, identical: true, violations: [] },
        { random: 5, seed: 1, identical: 5, redrawn: 0, differences: [] },
      ])
      expect(other.code).toEqual(1)
      expect(other.lines[0].identical).toBe(false)
    })
  })
})
