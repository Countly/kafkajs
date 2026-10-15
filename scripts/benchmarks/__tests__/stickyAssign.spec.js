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

/**
 * Both assigners also give the first member they answer for partition 0 of `topic-stranger`, a
 * topic no member subscribes to, the way an older tree handed a partition back to a member that
 * had left its topic.
 * @param {string} [memberIds] a pattern: only a group with a member id it matches is affected
 * @returns {string} the tree's directory
 */
const strangerTree = (memberIds = '') =>
  fakeTree(`const stranger = (original) => async (group) => {
  if (!group.members.some(({ memberId }) => new RegExp(${JSON.stringify(memberIds)}).test(memberId))) {
    return original(group)
  }
  const [first, ...rest] = await original(group)
  const { assignment } = MemberAssignment.decode(first.memberAssignment)
  const memberAssignment = MemberAssignment.encode({ version: 3, assignment: { ...assignment, 'topic-stranger': [0] } })
  return [{ ...first, memberAssignment }, ...rest]
}
const { MemberAssignment } = require(${JSON.stringify(path.join(REPO, 'src/consumer/assignerProtocol'))})
module.exports = { ...real, sticky: wrap(real.sticky, (original, group) => stranger(original)(group)),
  cooperativeSticky: wrap(real.cooperativeSticky, (original, group) => stranger(original)(group)) }`)

/**
 * A copy of this tree's `src/` whose sticky general path always returns the assignment of the
 * unchecked balancing pass, which hands a partition back to its previous owner without asking
 * whether the owner still subscribes to its topic, as older trees did.
 * @returns {string} the tree's directory
 */
const olderTree = () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'sticky-older-'))
  trees.push(dir)
  for (const file of fs.readdirSync(path.join(REPO, 'src'), { recursive: true })) {
    const from = path.join(REPO, 'src', file)
    if (!/__tests__|fixtures|\.spec\.js$/.test(file) && fs.statSync(from).isFile()) {
      fs.mkdirSync(path.dirname(path.join(dir, 'src', file)), { recursive: true })
      fs.copyFileSync(from, path.join(dir, 'src', file))
    }
  }
  fs.copyFileSync(path.join(REPO, 'package.json'), path.join(dir, 'package.json'))

  const core = path.join(dir, 'src/consumer/assigners/stickyAssigner/stickyCore.js')
  const text = fs.readFileSync(core, 'utf8')
  const second =
    'return new GeneralAssignmentBuilder({ ...input, handBackToSubscribersOnly: true }).build()'
  if (!text.includes(second)) {
    throw new Error('the checked pass has moved; update olderTree')
  }
  fs.writeFileSync(core, text.replace(second, 'return assigned'))
  return dir
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

    test('names the check-only shapes, and only `--check` takes them by default', () => {
      const shape = bench.CHECK_ONLY['capacity-3']()
      const handBack = bench.CHECK_ONLY['hand-back-3']()

      expect(Object.keys(bench.CHECK_ONLY)).toEqual(['capacity-3', 'hand-back-3', 'fixed-owner-3'])
      expect(shape.name).toEqual('capacity-3')
      expect(shape.members.map(({ memberId, topics }) => [memberId, topics])).toEqual([
        ['member-0001', ['topic-B']],
        ['member-0002', ['topic-A', 'topic-B']],
        ['member-0003', ['topic-A']],
      ])
      expect(shape.members[0].owned).toEqual([{ topic: 'topic-A', partitions: [0, 1] }])
      expect(handBack.name).toEqual('hand-back-3')
      expect(handBack.partitionsPerTopic).toEqual({ 'topic-A': [0, 1, 2], 'topic-B': [0] })
      expect(
        handBack.members.map(({ memberId, topics, owned, generationId }) => [
          memberId,
          topics,
          owned,
          generationId,
        ])
      ).toEqual([
        ['member-0001', ['topic-B'], [{ topic: 'topic-A', partitions: [0] }], 0],
        ['member-0002', ['topic-A', 'topic-B'], [{ topic: 'topic-A', partitions: [0, 1, 2] }], 1],
        ['member-0003', ['topic-A'], [], -1],
      ])
      expect(bench.defaultShapes(false)).toEqual(Object.keys(bench.FIXED))
      expect(bench.defaultShapes(true)).toEqual([
        ...Object.keys(bench.FIXED),
        'capacity-3',
        'hand-back-3',
        'fixed-owner-3',
      ])
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

  describe('nonSubscriberGrants', () => {
    const shape = {
      partitionsPerTopic: { a: [0, 1], b: [0, 1] },
      members: [
        { memberId: 'm1', topics: ['a'] },
        { memberId: 'm2', topics: ['a', 'b'] },
      ],
    }

    test('names each partition given to a member that does not subscribe to its topic', () => {
      const assigned = result([
        ['m1', { a: [0], b: [0, 1] }],
        ['m2', { a: [1], c: [] }],
        ['m9', { b: [0] }],
      ])

      expect(bench.nonSubscriberGrants(shape, assigned)).toEqual([
        'b-0 assigned to m1, which does not subscribe to b',
        'b-1 assigned to m1, which does not subscribe to b',
        'b-0 assigned to m9, which does not subscribe to b',
      ])
    })

    test('names nothing when every partition goes to a subscriber', () => {
      const assigned = result([
        ['m1', { a: [0] }],
        ['m2', { a: [1], b: [0, 1] }],
      ])

      expect(bench.nonSubscriberGrants(shape, assigned)).toEqual([])
    })
  })

  describe('compareResults', () => {
    const shape = {
      partitionsPerTopic: { a: [0, 1], b: [0] },
      members: [
        { memberId: 'm1', topics: ['a'], owned: [] },
        { memberId: 'm2', topics: ['a', 'b'], owned: [] },
      ],
    }
    const valid = result([
      ['m1', { a: [0] }],
      ['m2', { a: [1], b: [0] }],
    ])
    const other = result([
      ['m1', { a: [1] }],
      ['m2', { a: [0], b: [0] }],
    ])
    const handedBack = result([
      ['m1', { a: [0], b: [0] }],
      ['m2', { a: [1] }],
    ])
    const withheld = result([
      ['m1', { a: [0] }],
      ['m2', { a: [1] }],
    ])
    const trees = (sticky, cooperative, cooperativeCore = cooperative) => ({
      sticky,
      cooperative,
      cooperativeCore,
    })

    test('holds a valid baseline to identical buffers', () => {
      expect(bench.compareResults(shape, trees(valid, valid), trees(valid, valid))).toEqual({
        baselineValid: true,
        identical: true,
        violations: [],
      })
      expect(bench.compareResults(shape, trees(valid, valid), trees(valid, other))).toEqual({
        baselineValid: true,
        identical: false,
        violations: [],
      })
    })

    test('finds a baseline invalid when its sticky result gives a partition to a non-subscriber', () => {
      expect(
        bench.compareResults(shape, trees(valid, valid), trees(handedBack, valid))
      ).toMatchObject({ baselineValid: false, identical: false, violations: [] })
    })

    test('finds a baseline invalid when cooperative-sticky withheld the partition it gave a non-subscriber', () => {
      expect(
        bench.compareResults(shape, trees(valid, valid), trees(valid, withheld, handedBack))
      ).toMatchObject({ baselineValid: false, violations: [] })
    })

    test('names a partition the tree under test loses before cooperative-sticky withholds', () => {
      const emptied = result([
        ['m1', {}],
        ['m2', {}],
      ])

      expect(
        bench.compareResults(shape, trees(valid, valid, emptied), trees(valid, valid))
      ).toMatchObject({
        baselineValid: true,
        identical: true,
        violations: ['a-0 assigned to nobody', 'a-1 assigned to nobody', 'b-0 assigned to nobody'],
      })
    })

    test('names a partition the tree under test gives away while another member still claims it', () => {
      const claiming = {
        ...shape,
        members: [
          { memberId: 'm1', topics: ['a'], owned: [{ topic: 'a', partitions: [0] }] },
          { memberId: 'm2', topics: ['a', 'b'], owned: [] },
        ],
      }

      expect(
        bench.compareResults(claiming, trees(valid, other, valid), trees(valid, other))
      ).toMatchObject({
        violations: ['a-0 assigned to m2 while it is still claimed by m1'],
      })
    })

    test('names the invariants the tree under test breaks, whatever the baseline', () => {
      expect(
        bench.compareResults(shape, trees(handedBack, valid, handedBack), trees(valid, valid))
      ).toEqual({
        baselineValid: true,
        identical: false,
        violations: [
          'b-0 assigned to m1, which does not subscribe to b',
          'b-0 assigned to m1, which does not subscribe to b',
        ],
      })
    })
  })

  describe('handoverViolations', () => {
    const shape = {
      partitionsPerTopic: { a: [0, 1, 2] },
      members: [
        { memberId: 'm1', topics: ['a'], owned: [{ topic: 'a', partitions: [0, 1] }] },
        { memberId: 'm2', topics: ['a'], owned: [{ topic: 'a', partitions: [1] }] },
        { memberId: 'm3', topics: ['a'], owned: [] },
      ],
    }

    test('names a partition given to a member that does not claim it while others do', () => {
      const assigned = result([
        ['m1', { a: [1] }],
        ['m2', {}],
        ['m3', { a: [0] }],
      ])

      expect(bench.handoverViolations(shape, assigned)).toEqual([
        'a-0 assigned to m3 while it is still claimed by m1',
      ])
    })

    test('accepts a partition kept by a member that claims it, even beside another claimant', () => {
      const assigned = result([
        ['m1', { a: [0, 1] }],
        ['m2', {}],
        ['m3', {}],
      ])

      expect(bench.handoverViolations(shape, assigned)).toEqual([])
      expect(
        bench.handoverViolations(
          shape,
          result([
            ['m1', {}],
            ['m2', { a: [1] }],
            ['m3', {}],
          ])
        )
      ).toEqual([])
    })

    test('accepts a partition nobody claims given to any member, and a withheld one given to nobody', () => {
      const assigned = result([
        ['m1', {}],
        ['m2', {}],
        ['m3', { a: [2] }],
      ])

      expect(bench.handoverViolations(shape, assigned)).toEqual([])
    })
  })

  describe('the differential check', () => {
    test('finds every random shape identical when the baseline is this tree', async () => {
      const summary = await bench.checkRandomShapes({ baselineTree: REPO, count: 25, seed: 3 })

      expect(summary).toEqual({
        random: 25,
        seed: 3,
        identical: 25,
        invalidBaseline: 0,
        redrawn: 0,
        differences: [],
        violations: [],
      })
    })

    test('counts a shape whose baseline gives a partition to a non-subscriber as an invalid baseline', async () => {
      const summary = await bench.checkRandomShapes({
        baselineTree: strangerTree(),
        count: 3,
        seed: 3,
      })

      expect(summary).toEqual({
        random: 3,
        seed: 3,
        identical: 0,
        invalidBaseline: 3,
        redrawn: 0,
        differences: [],
        violations: [],
      })
    })

    test('holds an older tree to identical buffers wherever it gives every partition to a subscriber', async () => {
      // random-182 at seed 1 is a shape whose older cooperative-sticky result looks valid only
      // because it withholds the partition it handed back to a member that left its topic.
      const summary = await bench.checkRandomShapes({
        baselineTree: olderTree(),
        count: 200,
        seed: 1,
      })

      expect(summary).toEqual({
        random: 200,
        seed: 1,
        identical: 178,
        invalidBaseline: 22,
        redrawn: 0,
        differences: [],
        violations: [],
      })
    })

    test('names a shape on which the tree under test breaks an invariant', async () => {
      const summary = await bench.checkRandomShapes({
        baselineTree: REPO,
        tree: strangerTree(),
        count: 2,
        seed: 3,
      })

      expect(summary).toMatchObject({
        identical: 0,
        invalidBaseline: 0,
        differences: ['random-0', 'random-1'],
        violations: ['random-0', 'random-1'],
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
        baselineValid: true,
        identical: true,
        violations: [],
      })
      await expect(
        bench.checkFixedShape('stag-25+25x1k', differentCooperative())
      ).resolves.toMatchObject({ compared: true, identical: false })
    })

    test('checks the invariants on a check-only shape, and accepts a different result where the baseline is invalid', async () => {
      await expect(bench.checkFixedShape('capacity-3', REPO)).resolves.toEqual({
        shape: 'capacity-3',
        compared: true,
        baselineValid: true,
        identical: true,
        violations: [],
      })
      await expect(bench.checkFixedShape('hand-back-3', strangerTree())).resolves.toEqual({
        shape: 'hand-back-3',
        compared: true,
        baselineValid: false,
        identical: false,
        violations: [],
      })
      await expect(
        bench.checkFixedShape('capacity-3', REPO, undefined, strangerTree())
      ).resolves.toMatchObject({
        identical: false,
        violations: [
          'topic-stranger-0 assigned to member-0001, but topic-stranger has no partition 0',
          'topic-stranger-0 assigned to member-0001, which does not subscribe to topic-stranger',
        ],
      })
    })

    test('checks only the invariants of a fixed shape the baseline does not finish', async () => {
      await expect(bench.checkFixedShape('stag-25+25x1k', hangingTree(), 2000)).resolves.toEqual({
        shape: 'stag-25+25x1k',
        compared: false,
        baselineValid: null,
        identical: null,
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
    test('prints the time of a run, or both assigners buffers and the withheld-free cooperative one', async () => {
      const printed = []
      const log = jest.spyOn(console, 'log').mockImplementation((line) => printed.push(line))

      try {
        await bench.worker('time', 'stag-25+25x1k', REPO)
        await bench.worker('bytes', 'stag-25+25x1k', REPO)
      } finally {
        log.mockRestore()
      }

      expect(Object.keys(JSON.parse(printed[0]))).toEqual(['ms'])
      const { sticky: stickyHex, cooperative, cooperativeCore } = JSON.parse(printed[1])
      expect(stickyHex).toHaveLength(50)
      expect(cooperative).toHaveLength(50)
      expect(cooperativeCore).toHaveLength(50)
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
        {
          shape: 'capacity-3',
          compared: true,
          baselineValid: true,
          identical: true,
          violations: [],
        },
        {
          random: 0,
          seed: 1,
          identical: 0,
          invalidBaseline: 0,
          redrawn: 0,
          differences: [],
          violations: [],
        },
      ])
    })

    test('passes a check whose baseline is invalid where the tree under test keeps the invariants', async () => {
      const { code, lines } = await run([
        '--check',
        '--baseline-tree',
        strangerTree(),
        '--shapes',
        'hand-back-3',
        '--random',
        '2',
      ])

      expect(code).toEqual(0)
      expect(lines[0]).toMatchObject({ baselineValid: false, identical: false, violations: [] })
      expect(lines[1]).toMatchObject({ identical: 0, invalidBaseline: 2, differences: [] })
    })

    test.each([
      ['a fixed shape', '^member-\\d{4}$', ['--shapes', 'hand-back-3', '--random', '0'], 0],
      ['a random shape', '^member-\\d+-\\d+$', ['--shapes', 'hand-back-3', '--random', '1'], 1],
    ])(
      'fails a check when the tree under test breaks an invariant on %s alone',
      async (_name, memberIds, argv, broken) => {
        const stranger = strangerTree(memberIds)
        const { code, lines } = await run([
          '--check',
          '--baseline-tree',
          stranger,
          '--tree',
          stranger,
          ...argv,
        ])

        expect(code).toEqual(1)
        expect(lines.every((line) => line.identical !== false)).toBe(true)
        expect(lines.every((line) => (line.differences || []).length === 0)).toBe(true)
        expect(lines.map(({ violations }) => violations.length > 0)).toEqual(
          lines.map((_, i) => i === broken)
        )
      }
    )

    test('times the tree named by --tree', async () => {
      const log = path.join(os.tmpdir(), `sticky-tree-flag-${process.pid}`)
      trees.push(log)
      const logging = fakeTree(`const fs = require('fs')
module.exports = { ...real, sticky: wrap(real.sticky, (original, group) => {
  fs.appendFileSync(${JSON.stringify(log)}, 't')
  return original(group)
}) }`)

      const { code } = await run(['--runs', '1', '--shapes', 'stag-25+25x1k', '--tree', logging])

      expect(code).toEqual(0)
      expect(fs.readFileSync(log, 'utf8')).toEqual('tt')
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
        {
          shape: 'stag-25+25x1k',
          compared: true,
          baselineValid: true,
          identical: true,
          violations: [],
        },
        {
          random: 5,
          seed: 1,
          identical: 5,
          invalidBaseline: 0,
          redrawn: 0,
          differences: [],
          violations: [],
        },
      ])
      expect(other.code).toEqual(1)
      expect(other.lines[0].identical).toBe(false)
    })
  })
})
