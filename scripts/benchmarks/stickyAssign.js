/**
 * Sticky assigner benchmark and differential check, pure (no broker). It times the classic
 * `sticky` assigner's general path on fixed shapes, a tree (this one by default) against a
 * baseline tree, and with `--check` compares the `MemberAssignment` buffers both trees hand out.
 *
 * Shapes (`a+b`: `a` members own a previous round at generation 1, `b` members join empty):
 * - `stag`: 10 topics; member i subscribes to `topics.slice(i % 5, i % 5 + 5 + i % 3)`.
 * - `capacity-3` and `hand-back-3`: three members, one of which names in its previous round a
 *   topic it has left; they are run by `--check` only (see `CHECK_ONLY`).
 *
 * Timing: every run is its own process, so an arm that passes the 10-minute limit is stopped and
 * recorded as `did not finish` (and not run again). Arms alternate, their order swapping each
 * round. Each arm runs `--runs` measured runs after one warm-up run that is discarded; the median
 * of the measured runs is reported, and `runs` lists every run, the warm-up first.
 *
 * `--check`: every fixed shape and `--random` seeded random shapes (at most 8 members and 12
 * topics) go through both trees' `sticky` and `cooperative-sticky` assigners (see
 * `compareResults`). Where the baseline gives every partition to a member subscribed to its topic,
 * the buffers must be identical; where it does not (an older general path handed a partition back
 * to a previous owner that had left its topic, as Java's does), they may differ. On every shape the
 * tree under test must keep the invariants: each partition of a subscribed topic assigned exactly
 * once, only to a member subscribed to its topic, and at most one partition apart among members
 * with identical subscriptions; this holds for the `cooperative-sticky` assignment before it
 * withholds transferring partitions, and after it withholds, no partition goes to a member that
 * does not claim it while another does. A fixed shape the baseline does not finish within the
 * limit is not compared; a random shape whose baseline takes over 5 s is drawn again instead.
 * Exits 1 on a difference from a valid baseline or on a violation; a bad argument exits 2.
 *
 * Usage: node scripts/benchmarks/stickyAssign.js [--runs 7] [--tree <dir>] [--baseline-tree <dir>]
 *   [--shapes <name,...>] [--check [--random 1000] [--seed 1]]
 * Prints one JSON line per shape as it finishes, then one summary line.
 */
const path = require('path')
const { spawnSync } = require('child_process')
const { MemberMetadata, MemberAssignment } = require('../../src/consumer/assignerProtocol')
const { stickyUserData } = require('../../src/consumer/assigners/stickyAssigner/stickyUserData')

const REPO = path.resolve(__dirname, '../..')
const ARM_LIMIT_MS = 10 * 60 * 1000
const REDRAW_MS = 5000

/**
 * @typedef {Object} ShapeMember
 * @property {string} memberId
 * @property {string|null} groupInstanceId
 * @property {string[]} topics
 * @property {Array<{topic: string, partitions: number[]}>} owned the previous round it reports
 * @property {number} generationId `-1` for a member that never held anything
 */

/**
 * @typedef {Object} Shape
 * @property {string} name
 * @property {{[topic: string]: number[]}} partitionsPerTopic
 * @property {ShapeMember[]} members
 */

/**
 * @param {number} count
 * @returns {number[]} `0 … count - 1`
 */
const range = (count) => Array.from({ length: count }, (_, i) => i)

/**
 * @param {string} prefix
 * @param {number} i
 * @returns {string} a name that sorts the same way as its number
 */
const padded = (prefix, i) => `${prefix}-${String(i).padStart(4, '0')}`

/**
 * Builds an `a+b` shape. The previous round is dealt topic by topic: the partitions of each topic
 * go round-robin to the first `owners` members subscribed to it, so every owned partition has one
 * owner and the input does not depend on any assigner.
 * @param {object} input
 * @param {string} input.name
 * @param {string[]} input.topics
 * @param {number} input.partitions per topic
 * @param {number} input.owners members that own the previous round, at generation 1
 * @param {number} input.joiners members that join empty
 * @param {(i: number, topics: string[]) => string[]} input.subscribe member i's topics
 * @returns {Shape}
 */
const buildShape = ({ name, topics, partitions, owners, joiners, subscribe }) => {
  const members = range(owners + joiners).map((i) => ({
    memberId: padded('member', i),
    groupInstanceId: null,
    topics: subscribe(i, topics),
    owned: [],
    generationId: i < owners ? 1 : -1,
  }))

  for (const topic of topics) {
    const holders = members.slice(0, owners).filter((member) => member.topics.includes(topic))

    if (holders.length > 0) {
      const dealt = holders.map(() => [])
      range(partitions).forEach((partition) => dealt[partition % holders.length].push(partition))
      holders.forEach((member, i) => {
        if (dealt[i].length > 0) {
          member.owned.push({ topic, partitions: dealt[i] })
        }
      })
    }
  }

  return {
    name,
    partitionsPerTopic: Object.fromEntries(topics.map((topic) => [topic, range(partitions)])),
    members,
  }
}

/**
 * @param {number} owners
 * @param {number} joiners
 * @param {number} totalPartitions over the 10 topics
 * @returns {Shape} a `stag` shape
 */
const stag = (owners, joiners, totalPartitions) =>
  buildShape({
    name: `stag-${owners}+${joiners}x${totalPartitions / 1000}k`,
    topics: range(10).map((i) => padded('topic', i)),
    partitions: totalPartitions / 10,
    owners,
    joiners,
    subscribe: (i, topics) => topics.slice(i % 5, (i % 5) + 5 + (i % 3)),
  })

/**
 * @param {number} owners
 * @param {number} joiners
 * @returns {Shape} a `wide` shape
 */
const wide = (owners, joiners) =>
  buildShape({
    name: `wide-${owners}+${joiners}`,
    topics: range(100).map((i) => padded('topic', i)),
    partitions: 150,
    owners,
    joiners,
    subscribe: (i, topics) => topics.slice(i % 50, (i % 50) + 1 + (i % 7)),
  })

/** The fixed shapes, by name. Built on demand: the largest holds 30 000 partitions. */
const FIXED = {
  'stag-25+25x1k': () => stag(25, 25, 1000),
  'stag-100+100x5k': () => stag(100, 100, 5000),
  'stag-150+150x15k': () => stag(150, 150, 15000),
  'stag-500+500x30k': () => stag(500, 500, 30000),
  'stag-0+300x15k': () => stag(0, 300, 15000),
  'wide-10+10': () => wide(10, 10),
  'wide-20+20': () => wide(20, 20),
  'wide-150+150': () => wide(150, 150),
  'wide-0+300': () => wide(0, 300),
}

/**
 * Three members and two topics. `member-0001` subscribes to `topic-B` alone, yet its previous
 * round (generation 0) names the first two partitions of `topic-A`. An older general path handed
 * them back to it, as Java's does, and then held it to the partitions `topic-B` has, so a balance
 * check that ignored a member's capacity gave a different assignment.
 * @returns {Shape} the `capacity-3` shape
 */
const capacity = () => ({
  name: 'capacity-3',
  partitionsPerTopic: { 'topic-A': range(10), 'topic-B': range(2) },
  members: [
    {
      memberId: 'member-0001',
      groupInstanceId: null,
      topics: ['topic-B'],
      owned: [{ topic: 'topic-A', partitions: [0, 1] }],
      generationId: 0,
    },
    {
      memberId: 'member-0002',
      groupInstanceId: null,
      topics: ['topic-A', 'topic-B'],
      owned: [
        { topic: 'topic-A', partitions: range(5) },
        { topic: 'topic-B', partitions: [0, 1] },
      ],
      generationId: 1,
    },
    {
      memberId: 'member-0003',
      groupInstanceId: null,
      topics: ['topic-A'],
      owned: [{ topic: 'topic-A', partitions: [5, 6, 7, 8, 9] }],
      generationId: 1,
    },
  ],
})

/**
 * The smallest shape found on which the general path once handed a partition back to a previous
 * owner that had left its topic: `member-0001` subscribes to `topic-B` alone, yet its previous
 * round (generation 0) names `topic-A` 0, which a newer round gave to `member-0002`.
 * @returns {Shape} the `hand-back-3` shape
 */
const handBack = () => ({
  name: 'hand-back-3',
  partitionsPerTopic: { 'topic-A': range(3), 'topic-B': range(1) },
  members: [
    {
      memberId: 'member-0001',
      groupInstanceId: null,
      topics: ['topic-B'],
      owned: [{ topic: 'topic-A', partitions: [0] }],
      generationId: 0,
    },
    {
      memberId: 'member-0002',
      groupInstanceId: null,
      topics: ['topic-A', 'topic-B'],
      owned: [{ topic: 'topic-A', partitions: range(3) }],
      generationId: 1,
    },
    {
      memberId: 'member-0003',
      groupInstanceId: null,
      topics: ['topic-A'],
      owned: [],
      generationId: -1,
    },
  ],
})

/**
 * Shapes only `--check` runs. They pin an assignment, so their time says nothing; their previous
 * round names a topic its member left.
 */
const CHECK_ONLY = { 'capacity-3': capacity, 'hand-back-3': handBack }

/** Every shape a name can select. */
const SHAPES = { ...FIXED, ...CHECK_ONLY }

/**
 * @param {boolean} check whether `--check` was given
 * @returns {string[]} the shapes a run takes without `--shapes`: the timed ones, and the check-only
 *   ones as well under `--check`
 */
const defaultShapes = (check) => Object.keys(check ? SHAPES : FIXED)

/**
 * A seeded pseudo-random source (mulberry32), so a `--seed` names the same shapes on every host.
 * @param {number} seed
 * @returns {(bound: number) => number} an integer in `0 … bound - 1`
 */
const seeded = (seed) => {
  let state = seed >>> 0
  return (bound) => {
    state = (state + 0x6d2b79f5) >>> 0
    let t = state
    t = Math.imul(t ^ (t >>> 15), t | 1)
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61)
    return Math.floor((((t ^ (t >>> 14)) >>> 0) / 4294967296) * bound)
  }
}

/**
 * Draws one random shape small enough for an unfixed general path to finish: 1–8 members,
 * 1–12 topics of 1–16 partitions, mixed subscriptions, static and dynamic members, and a previous
 * round with conflicting claims, stale generations and partitions of topics no longer subscribed.
 * @param {(bound: number) => number} next
 * @param {number} index used in the shape's name
 * @returns {Shape}
 */
const randomShape = (next, index) => {
  const topics = range(1 + next(12)).map((i) => padded('topic', i))
  const partitionsPerTopic = Object.fromEntries(topics.map((topic) => [topic, range(1 + next(16))]))

  const members = range(1 + next(8)).map((i) => {
    const subscribed = topics.filter(() => next(3) > 0)
    const owned = topics
      .filter((topic) => subscribed.includes(topic) || next(8) === 0)
      .map((topic) => ({
        topic,
        partitions: partitionsPerTopic[topic].filter(() => next(4) === 0),
      }))
      .filter(({ partitions }) => partitions.length > 0)

    return {
      memberId: `member-${next(1000)}-${i}`,
      groupInstanceId: next(4) === 0 ? `instance-${next(1000)}-${i}` : null,
      topics: subscribed.length > 0 ? subscribed : [topics[next(topics.length)]],
      owned,
      generationId: next(4) - 1,
    }
  })

  return { name: `random-${index}`, partitionsPerTopic, members }
}

/**
 * Encodes a shape as the group members an assigner receives. One subscription serves both
 * assigners: `sticky` reads the previous round from `userData`, `cooperative-sticky` from
 * `OwnedPartitions` and `GenerationId`.
 * @param {Shape} shape
 * @returns {Array<{ memberId: string, groupInstanceId: string|null, memberMetadata: Buffer }>}
 */
const groupMembers = ({ members }) =>
  members.map(({ memberId, groupInstanceId, topics, owned, generationId }) => ({
    memberId,
    groupInstanceId,
    memberMetadata: MemberMetadata.encode({
      version: 3,
      topics,
      userData:
        owned.length > 0 ? stickyUserData.encode({ assignment: owned, generationId }) : null,
      ownedPartitions: owned,
      generationId,
      rackId: null,
    }),
  }))

/**
 * @param {string} dir a tree holding `src/consumer/assigners`
 * @returns {{ sticky: Function, cooperativeSticky: Function }} that tree's assigner factories
 */
const loadAssigners = (dir) => require(path.resolve(dir, 'src/consumer/assigners'))

/**
 * Runs one assigner of one tree over a shape.
 * @param {Function} factory an assigner factory, `({ cluster }) => assigner`
 * @param {Shape} shape
 * @param {ReturnType<typeof groupMembers>} members the shape, encoded
 * @returns {Promise<Array<{ memberId: string, memberAssignment: Buffer }>>}
 */
const assign = (factory, shape, members) => {
  const cluster = {
    findTopicPartitionMetadata: (topic) =>
      shape.partitionsPerTopic[topic].map((partitionId) => ({ partitionId })),
  }
  const topics = [...new Set(shape.members.flatMap((member) => member.topics))]
  return factory({ cluster }).assign({ members, topics })
}

/**
 * @param {Array<{ memberId: string, memberAssignment: Buffer }>} result
 * @returns {string[]} `memberId:hex` per member, in the order the assigner returned them
 */
const toHex = (result) =>
  result.map(({ memberId, memberAssignment }) => `${memberId}:${memberAssignment.toString('hex')}`)

/**
 * Checks a `sticky` assignment against the shape it was made for. A broken invariant is one line:
 * a member without exactly one result, a result for a member outside the group, a partition
 * outside its topic's metadata, a partition assigned twice, to a member that does not subscribe to
 * its topic, or to nobody, and members with identical subscriptions more than one partition apart.
 * Every member of the shape starts at zero partitions, so a member with no result counts as
 * holding nothing. A result for a member outside the group is reported and its partitions are not
 * counted, so they also show as assigned to nobody.
 * @param {Shape} shape
 * @param {Array<{ memberId: string, memberAssignment: Buffer }>} result a `sticky` assignment
 * @returns {string[]} one line per broken invariant; empty when the assignment holds them all
 */
const checkInvariants = (shape, result) => {
  const violations = []
  const subscriptions = new Map(shape.members.map((m) => [m.memberId, new Set(m.topics)]))
  const known = new Set(
    Object.entries(shape.partitionsPerTopic).flatMap(([topic, partitions]) =>
      partitions.map((partition) => `${topic}-${partition}`)
    )
  )
  const owner = new Map()
  const sizes = new Map(shape.members.map((m) => [m.memberId, 0]))
  const answered = new Set()

  for (const { memberId, memberAssignment } of result) {
    if (!subscriptions.has(memberId)) {
      violations.push(`${memberId} is not a member of the group`)
      continue
    }
    if (answered.has(memberId)) {
      violations.push(`${memberId} has more than one result`)
    }
    answered.add(memberId)

    const { assignment } = MemberAssignment.decode(memberAssignment)

    for (const [topic, partitions] of Object.entries(assignment)) {
      for (const partition of partitions) {
        const key = `${topic}-${partition}`
        sizes.set(memberId, sizes.get(memberId) + 1)

        if (!known.has(key)) {
          violations.push(
            `${key} assigned to ${memberId}, but ${topic} has no partition ${partition}`
          )
        }
        if (owner.has(key)) {
          violations.push(`${key} assigned to ${owner.get(key)} and ${memberId}`)
        }
        owner.set(key, memberId)

        if (!subscriptions.get(memberId).has(topic)) {
          violations.push(`${key} assigned to ${memberId}, which does not subscribe to ${topic}`)
        }
      }
    }
  }

  for (const { memberId } of shape.members) {
    if (!answered.has(memberId)) {
      violations.push(`${memberId} has no result`)
    }
  }

  const subscribed = new Set(shape.members.flatMap((m) => m.topics))
  for (const topic of subscribed) {
    for (const partition of shape.partitionsPerTopic[topic]) {
      if (!owner.has(`${topic}-${partition}`)) {
        violations.push(`${topic}-${partition} assigned to nobody`)
      }
    }
  }

  const bySubscription = new Map()
  for (const { memberId, topics } of shape.members) {
    const subscription = [...new Set(topics)].sort().join(',')
    bySubscription.set(subscription, [
      ...(bySubscription.get(subscription) || []),
      sizes.get(memberId),
    ])
  }
  for (const [subscription, held] of bySubscription) {
    if (Math.max(...held) - Math.min(...held) > 1) {
      violations.push(`members subscribed to ${subscription} hold ${held.join(', ')} partitions`)
    }
  }

  return violations
}

/**
 * Names each partition a result gives to a member that does not subscribe to its topic.
 * @param {Shape} shape
 * @param {Array<{ memberId: string, memberAssignment: Buffer }>} result
 * @returns {string[]} one line per such partition, in the result's order
 */
const nonSubscriberGrants = (shape, result) => {
  const subscriptions = new Map(shape.members.map((m) => [m.memberId, m.topics]))
  const grants = []

  for (const { memberId, memberAssignment } of result) {
    const subscribed = subscriptions.get(memberId) || []

    for (const [topic, partitions] of Object.entries(
      MemberAssignment.decode(memberAssignment).assignment
    )) {
      if (!subscribed.includes(topic)) {
        partitions.forEach((partition) =>
          grants.push(
            `${topic}-${partition} assigned to ${memberId}, which does not subscribe to ${topic}`
          )
        )
      }
    }
  }

  return grants
}

/**
 * @typedef {Object} TreeResults
 * @property {Array<{ memberId: string, memberAssignment: Buffer }>} sticky
 * @property {Array<{ memberId: string, memberAssignment: Buffer }>} cooperative
 * @property {Array<{ memberId: string, memberAssignment: Buffer }>} cooperativeCore the
 *   `cooperative-sticky` assignment before it withholds transferring partitions: a withheld
 *   partition goes to that member in the next round, so a hand-back to a member that left the
 *   topic shows only here
 */

/**
 * Checks a `cooperative-sticky` result after it withholds partitions, for what that step exists
 * to guarantee: a partition is given only to a member that already claims it in its previous
 * round, or to any member when nobody claims it, so that no two members fetch it at once. It does
 * not ask for completeness or balance, which withholding gives up on purpose for a round; the
 * assignment before withholding is held to them by `checkInvariants`.
 * @param {Shape} shape
 * @param {Array<{ memberId: string, memberAssignment: Buffer }>} result a `cooperative-sticky`
 *   assignment
 * @returns {string[]} one line per partition given to a member that does not claim it while
 *   another member does
 */
const handoverViolations = (shape, result) => {
  const claimants = new Map()

  for (const { memberId, owned } of shape.members) {
    for (const { topic, partitions } of owned) {
      for (const partition of partitions) {
        const key = `${topic}-${partition}`
        claimants.set(key, (claimants.get(key) || []).concat(memberId))
      }
    }
  }

  const violations = []

  for (const { memberId, memberAssignment } of result) {
    for (const [topic, partitions] of Object.entries(
      MemberAssignment.decode(memberAssignment).assignment
    )) {
      for (const partition of partitions) {
        const key = `${topic}-${partition}`
        const holders = claimants.get(key) || []

        if (holders.length > 0 && !holders.includes(memberId)) {
          violations.push(
            `${key} assigned to ${memberId} while it is still claimed by ${holders.join(', ')}`
          )
        }
      }
    }
  }

  return violations
}

/**
 * @param {Shape} shape
 * @param {TreeResults} ours
 * @returns {string[]} the invariants the tree under test breaks: every one of `checkInvariants`
 *   on the `sticky` result and on the `cooperative-sticky` result before it withholds, and
 *   `handoverViolations` on the result it hands out
 */
const violationsOf = (shape, ours) => [
  ...checkInvariants(shape, ours.sticky),
  ...checkInvariants(shape, ours.cooperativeCore),
  ...handoverViolations(shape, ours.cooperative),
]

/**
 * The differential check's contract for one shape. The baseline is valid when its `sticky` and
 * `cooperativeCore` results give every partition to a subscriber of its topic; only then must both
 * buffers be identical. The tree under test must pass `checkInvariants` on its `sticky` and
 * `cooperativeCore` results, and `handoverViolations` on its withheld cooperative result,
 * whatever the baseline did.
 * @param {Shape} shape
 * @param {TreeResults} ours the tree under test
 * @param {TreeResults} theirs the baseline
 * @returns {{ baselineValid: boolean, identical: boolean, violations: string[] }}
 */
const compareResults = (shape, ours, theirs) => ({
  baselineValid:
    nonSubscriberGrants(shape, theirs.sticky).length === 0 &&
    nonSubscriberGrants(shape, theirs.cooperativeCore).length === 0,
  identical:
    JSON.stringify(toHex(ours.sticky)) === JSON.stringify(toHex(theirs.sticky)) &&
    JSON.stringify(toHex(ours.cooperative)) === JSON.stringify(toHex(theirs.cooperative)),
  violations: violationsOf(shape, ours),
})

/**
 * Runs one tree's `cooperative-sticky` assigner over a shape, keeping its assignment from just
 * before it withholds transferring partitions: the assigner's `withholdTransferringPartitions`,
 * which the compared trees share, is wrapped to encode what it is handed first.
 * @param {Function} factory
 * @param {Shape} shape
 * @param {ReturnType<typeof groupMembers>} members the shape, encoded
 * @returns {Promise<Pick<TreeResults, 'cooperative'|'cooperativeCore'>>}
 */
const assignCooperative = async (factory, shape, members) => {
  let cooperativeCore
  const capturing = (deps) => {
    const assigner = factory(deps)
    const withhold = assigner.withholdTransferringPartitions
    assigner.withholdTransferringPartitions = function (group, assignment, contested) {
      cooperativeCore = [...assignment].map(([memberId, byTopic]) => ({
        memberId,
        memberAssignment: MemberAssignment.encode({ version: this.version, assignment: byTopic }),
      }))
      return withhold.call(this, group, assignment, contested)
    }
    return assigner
  }
  const cooperative = await assign(capturing, shape, members)
  return { cooperative, cooperativeCore }
}

/**
 * Runs both assigners of one tree over a shape.
 * @param {string} dir
 * @param {Shape} shape
 * @returns {Promise<{ ms: number } & TreeResults>} `ms` is the `sticky` assignment's own time
 */
const assignBoth = async (dir, shape) => {
  const assigners = loadAssigners(dir)
  const members = groupMembers(shape)
  const started = performance.now()
  const sticky = await assign(assigners.sticky, shape, members)
  const ms = performance.now() - started
  return { ms, sticky, ...(await assignCooperative(assigners.cooperativeSticky, shape, members)) }
}

/**
 * @param {string[]} lines `memberId:hex` per member, as `toHex` writes them
 * @returns {Array<{ memberId: string, memberAssignment: Buffer }>}
 */
const fromHex = (lines) =>
  lines.map((line) => {
    const at = line.lastIndexOf(':')
    return { memberId: line.slice(0, at), memberAssignment: Buffer.from(line.slice(at + 1), 'hex') }
  })

/**
 * Runs a fixed shape in a child process of this script, stopped at `limitMs`.
 * @param {'time'|'bytes'} mode `time` reports the `sticky` assignment's milliseconds; `bytes`
 *   reports both assigners' buffers
 * @param {string} shapeName
 * @param {string} dir
 * @param {number} limitMs
 * @returns {object|null} what the child printed, or `null` when it did not finish in time
 */
const runChild = (mode, shapeName, dir, limitMs) => {
  const child = spawnSync(
    process.execPath,
    [__filename, '--worker', mode, '--shape', shapeName, '--tree', dir],
    { encoding: 'utf8', timeout: limitMs, maxBuffer: 1 << 30 }
  )

  if (child.error && child.error.code === 'ETIMEDOUT') {
    return null
  }
  if (child.status !== 0) {
    throw new Error(`worker for ${shapeName} in ${dir} failed: ${child.stderr}`)
  }
  return JSON.parse(child.stdout)
}

/**
 * The child side of `runChild`: prints one JSON line.
 * @param {'time'|'bytes'} mode
 * @param {string} shapeName
 * @param {string} dir
 * @returns {Promise<void>}
 */
const worker = async (mode, shapeName, dir) => {
  const shape = SHAPES[shapeName]()

  if (mode === 'time') {
    const members = groupMembers(shape)
    const started = performance.now()
    await assign(loadAssigners(dir).sticky, shape, members)
    console.log(JSON.stringify({ ms: performance.now() - started }))
    return
  }

  const { sticky, cooperative, cooperativeCore } = await assignBoth(dir, shape)
  console.log(
    JSON.stringify({
      sticky: toHex(sticky),
      cooperative: toHex(cooperative),
      cooperativeCore: toHex(cooperativeCore),
    })
  )
}

/**
 * @param {number[]} values
 * @returns {number} the middle value of the sorted list (the upper one for an even count)
 */
const median = (values) => [...values].sort((a, b) => a - b)[Math.floor(values.length / 2)]

/**
 * Times one fixed shape on each arm: every run is a child process, the arms alternate and their
 * order swaps each round, and an arm that passes the limit is not run again.
 * @param {string} shapeName
 * @param {object} options
 * @param {number} options.runs measured runs per arm, after one warm-up run that is discarded
 * @param {Array<{ key: string, dir: string }>} options.arms the trees, by the key they report under
 * @param {number} [options.limitMs]
 * @returns {object} `{ shape, <key>: { medianMs, runs } | 'did not finish', ratio? }`: `medianMs`
 *   is over the measured runs, `runs` lists every run with the warm-up first, and `ratio` is `tree`
 *   over `baseline` when both finished
 */
const timeShape = (shapeName, { runs, arms, limitMs = ARM_LIMIT_MS }) => {
  const state = arms.map(({ key, dir }) => ({ key, dir, runs: [] }))

  for (let round = 0; round <= runs; round++) {
    const order = round % 2 === 0 ? state : [...state].reverse()
    for (const arm of order) {
      if (arm.runs !== null) {
        const result = runChild('time', shapeName, arm.dir, limitMs)
        arm.runs = result === null ? null : [...arm.runs, result.ms]
      }
    }
  }

  const line = { shape: shapeName }
  for (const arm of state) {
    line[arm.key] =
      arm.runs === null
        ? 'did not finish'
        : { medianMs: median(arm.runs.slice(1)), runs: arm.runs.map(Math.round) }
  }
  if (line.tree?.medianMs && line.baseline?.medianMs) {
    line.ratio = line.tree.medianMs / line.baseline.medianMs
  }
  return line
}

/**
 * Checks one fixed shape: the tree under test in this process, the baseline in a child process.
 * @param {string} shapeName
 * @param {string} baselineTree
 * @param {number} [limitMs]
 * @param {string} [tree] the tree under test
 * @returns {Promise<{ shape: string, compared: boolean, baselineValid: boolean|null,
 *   identical: boolean|null, violations: string[] }>} `compared` is false when the baseline did
 *   not finish, and `baselineValid` and `identical` are then `null`
 */
const checkFixedShape = async (shapeName, baselineTree, limitMs = ARM_LIMIT_MS, tree = REPO) => {
  const shape = SHAPES[shapeName]()
  const ours = await assignBoth(tree, shape)
  const theirs = runChild('bytes', shapeName, baselineTree, limitMs)

  if (theirs === null) {
    return {
      shape: shapeName,
      compared: false,
      baselineValid: null,
      identical: null,
      violations: violationsOf(shape, ours),
    }
  }

  const decoded = {
    sticky: fromHex(theirs.sticky),
    cooperative: fromHex(theirs.cooperative),
    cooperativeCore: fromHex(theirs.cooperativeCore),
  }
  return { shape: shapeName, compared: true, ...compareResults(shape, ours, decoded) }
}

/**
 * Checks `count` seeded random shapes across the trees, in this process. Each shape counts once:
 * as `identical`, as a difference from a valid baseline, or as an `invalidBaseline`; `violations`
 * names the shapes on which the tree under test breaks an invariant, whatever their class.
 * @param {object} options
 * @param {string} options.baselineTree
 * @param {string} [options.tree] the tree under test
 * @param {number} options.count
 * @param {number} options.seed
 * @param {number} [options.redrawMs] a shape whose baseline arm takes longer is drawn again
 * @returns {Promise<{ random: number, seed: number, identical: number, invalidBaseline: number,
 *   redrawn: number, differences: string[], violations: string[] }>}
 */
const checkRandomShapes = async ({
  baselineTree,
  tree = REPO,
  count,
  seed,
  redrawMs = REDRAW_MS,
}) => {
  const next = seeded(seed)
  const summary = {
    random: count,
    seed,
    identical: 0,
    invalidBaseline: 0,
    redrawn: 0,
    differences: [],
    violations: [],
  }

  for (
    let index = 0;
    summary.identical + summary.invalidBaseline + summary.differences.length < count;
    index++
  ) {
    const shape = randomShape(next, index)
    const theirs = await assignBoth(baselineTree, shape)

    if (theirs.ms > redrawMs) {
      summary.redrawn++
      continue
    }

    const { baselineValid, identical, violations } = compareResults(
      shape,
      await assignBoth(tree, shape),
      theirs
    )

    if (!baselineValid) {
      summary.invalidBaseline++
    } else if (identical) {
      summary.identical++
    } else {
      summary.differences.push(shape.name)
    }
    if (violations.length > 0) {
      summary.violations.push(shape.name)
    }
  }

  return summary
}

/**
 * Reads the flag `--<name>` from `argv`.
 * @param {string[]} argv
 * @param {string} flag
 * @param {string|null} fallback returned when the flag is absent
 * @returns {string|null}
 */
const arg = (argv, flag, fallback) => {
  const i = argv.indexOf(`--${flag}`)
  return i === -1 ? fallback : argv[i + 1]
}

/**
 * The command line. Returns the exit code instead of exiting, so a caller can test it.
 * @param {string[]} argv the arguments after the script's path
 * @param {(line: string) => void} [print]
 * @returns {Promise<number>} 0, 1 when `--check` finds a difference or a violation, 2 for a bad
 *   argument
 */
const main = async (argv, print = console.log) => {
  const runs = Number(arg(argv, 'runs', '7'))
  const random = Number(arg(argv, 'random', '1000'))
  const seed = Number(arg(argv, 'seed', '1'))
  const baselineTree = arg(argv, 'baseline-tree', null)
  const tree = arg(argv, 'tree', REPO)
  const check = argv.includes('--check')
  const shapes = (arg(argv, 'shapes', null) || defaultShapes(check).join(',')).split(',')

  const unknown = shapes.filter((shape) => !SHAPES[shape])
  if (
    unknown.length > 0 ||
    !(runs >= 1) ||
    !(random >= 0) ||
    !Number.isInteger(seed) ||
    (check && !baselineTree)
  ) {
    print(
      JSON.stringify({
        error: 'bad arguments',
        unknownShapes: unknown,
        usage:
          '[--runs >=1] [--tree <dir>] [--baseline-tree <dir>] [--shapes <name,...>] [--check [--random n] [--seed n]]; --check needs --baseline-tree',
        shapes: Object.keys(SHAPES),
      })
    )
    return 2
  }

  if (!check) {
    for (const shape of shapes) {
      const arms = [{ key: 'tree', dir: tree }]
      if (baselineTree) {
        arms.push({ key: 'baseline', dir: baselineTree })
      }
      print(JSON.stringify(timeShape(shape, { runs, arms })))
    }
    return 0
  }

  let failed = false
  for (const shape of shapes) {
    const line = await checkFixedShape(shape, baselineTree, ARM_LIMIT_MS, tree)
    failed = failed || (line.baselineValid && !line.identical) || line.violations.length > 0
    print(JSON.stringify(line))
  }
  const summary = await checkRandomShapes({ baselineTree, tree, count: random, seed })
  failed = failed || summary.differences.length > 0 || summary.violations.length > 0
  print(JSON.stringify(summary))
  return failed ? 1 : 0
}

if (require.main === module) {
  const argv = process.argv.slice(2)
  const job =
    argv[0] === '--worker'
      ? worker(argv[1], arg(argv, 'shape', null), arg(argv, 'tree', null))
      : main(argv)
  job.then(
    (code = 0) => {
      process.exitCode = code
    },
    (e) => {
      console.error(e)
      process.exitCode = 1
    }
  )
}

module.exports = {
  FIXED,
  CHECK_ONLY,
  defaultShapes,
  stag,
  wide,
  seeded,
  randomShape,
  groupMembers,
  assign,
  checkInvariants,
  nonSubscriberGrants,
  handoverViolations,
  compareResults,
  checkFixedShape,
  checkRandomShapes,
  timeShape,
  worker,
  main,
}
