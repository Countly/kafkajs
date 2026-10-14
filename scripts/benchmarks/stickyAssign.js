/**
 * Sticky assigner benchmark and differential check, pure (no broker). It times the classic
 * `sticky` assigner's general path on fixed shapes, this tree against a baseline tree, and with
 * `--check` proves that both trees hand out byte-identical `MemberAssignment` buffers.
 *
 * Shapes (`a+b`: `a` members own a previous round at generation 1, `b` members join empty):
 * - `stag`: 10 topics; member i subscribes to `topics.slice(i % 5, i % 5 + 5 + i % 3)`.
 * - `capacity`: three members on which a balance check that ignores a member's capacity gives a
 *   different assignment; it is run by `--check` only (see `CHECK_ONLY`).
 *
 * Timing: every run is its own process, so an arm that passes the 10-minute limit is stopped and
 * recorded as `did not finish` (and not run again). Arms alternate, their order swapping each
 * round. Each arm runs `--runs` measured runs after one warm-up run that is discarded; the median
 * of the measured runs is reported, and `runs` lists every run, the warm-up first.
 *
 * `--check`: every fixed shape and `--random` seeded random shapes (at most 8 members and 12
 * topics) go through both trees' `sticky` and `cooperative-sticky` assigners and the buffers are
 * compared. A fixed shape the baseline does not finish within the limit is not compared; a random
 * shape whose baseline takes over 5 s is drawn again instead. This tree's `sticky` result on every
 * timed fixed shape is also checked for the invariants: each partition of a subscribed topic
 * assigned exactly once, only to a member subscribed to its topic, and at most one partition apart
 * among members with identical subscriptions. Random shapes and the check-only shapes are compared
 * only: their previous rounds may name topics a member no longer subscribes to, and the general
 * path can hand such a partition back to that member, as Java's does. Exits 1 on any difference
 * or violation; a bad argument exits 2.
 *
 * Usage: node scripts/benchmarks/stickyAssign.js [--runs 7] [--baseline-tree <dir>]
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
 * Three members and two topics on which a balance check that ignores a member's capacity gives a
 * different assignment. `member-0001` subscribes to `topic-B` alone, yet its previous round
 * (generation 0) names the first two partitions of `topic-A`, which the general path hands back to
 * it, as Java's does; it then holds as many partitions as `topic-B` has, and the balance check
 * must not count it among the members that could still take one.
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
 * Shapes only `--check` runs. They pin an assignment, so their time says nothing, and their
 * previous round names a topic its member left, so the invariants do not hold for them.
 */
const CHECK_ONLY = { 'capacity-3': capacity }

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
 * Runs both assigners of one tree over a shape.
 * @param {string} dir
 * @param {Shape} shape
 * @returns {Promise<{ ms: number, sticky: Array, cooperative: Array }>} `ms` is the `sticky`
 *   assignment's own time
 */
const assignBoth = async (dir, shape) => {
  const assigners = loadAssigners(dir)
  const members = groupMembers(shape)
  const started = performance.now()
  const sticky = await assign(assigners.sticky, shape, members)
  const ms = performance.now() - started
  const cooperative = await assign(assigners.cooperativeSticky, shape, members)
  return { ms, sticky, cooperative }
}

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

  const { sticky, cooperative } = await assignBoth(dir, shape)
  console.log(JSON.stringify({ sticky: toHex(sticky), cooperative: toHex(cooperative) }))
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
 * Compares one fixed shape across the trees and checks this tree's invariants, except on a
 * check-only shape, which is compared alone.
 * @param {string} shapeName
 * @param {string} baselineTree
 * @param {number} [limitMs]
 * @returns {Promise<{ shape: string, compared: boolean, identical: boolean, violations: string[] }>}
 *   `compared` is false when the baseline did not finish; `identical` is then vacuously true
 */
const checkFixedShape = async (shapeName, baselineTree, limitMs = ARM_LIMIT_MS) => {
  const shape = SHAPES[shapeName]()
  const ours = await assignBoth(REPO, shape)
  const theirs = runChild('bytes', shapeName, baselineTree, limitMs)
  const identical =
    theirs === null ||
    (JSON.stringify(toHex(ours.sticky)) === JSON.stringify(theirs.sticky) &&
      JSON.stringify(toHex(ours.cooperative)) === JSON.stringify(theirs.cooperative))

  return {
    shape: shapeName,
    compared: theirs !== null,
    identical,
    violations: CHECK_ONLY[shapeName] ? [] : checkInvariants(shape, ours.sticky),
  }
}

/**
 * Compares `count` seeded random shapes across the trees, in this process.
 * @param {object} options
 * @param {string} options.baselineTree
 * @param {number} options.count
 * @param {number} options.seed
 * @param {number} [options.redrawMs] a shape whose baseline arm takes longer is drawn again
 * @returns {Promise<{ random: number, seed: number, identical: number, redrawn: number,
 *   differences: string[] }>} `differences` names the shapes whose buffers differ
 */
const checkRandomShapes = async ({ baselineTree, count, seed, redrawMs = REDRAW_MS }) => {
  const next = seeded(seed)
  const summary = { random: count, seed, identical: 0, redrawn: 0, differences: [] }

  for (let index = 0; summary.identical + summary.differences.length < count; index++) {
    const shape = randomShape(next, index)
    const theirs = await assignBoth(baselineTree, shape)

    if (theirs.ms > redrawMs) {
      summary.redrawn++
      continue
    }

    const ours = await assignBoth(REPO, shape)
    const same =
      JSON.stringify(toHex(ours.sticky)) === JSON.stringify(toHex(theirs.sticky)) &&
      JSON.stringify(toHex(ours.cooperative)) === JSON.stringify(toHex(theirs.cooperative))

    if (same) {
      summary.identical++
    } else {
      summary.differences.push(shape.name)
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
          '[--runs >=1] [--baseline-tree <dir>] [--shapes <name,...>] [--check [--random n] [--seed n]]; --check needs --baseline-tree',
        shapes: Object.keys(SHAPES),
      })
    )
    return 2
  }

  if (!check) {
    for (const shape of shapes) {
      const arms = [{ key: 'tree', dir: REPO }]
      if (baselineTree) {
        arms.push({ key: 'baseline', dir: baselineTree })
      }
      print(JSON.stringify(timeShape(shape, { runs, arms })))
    }
    return 0
  }

  let failed = false
  for (const shape of shapes) {
    const line = await checkFixedShape(shape, baselineTree)
    failed = failed || !line.identical || line.violations.length > 0
    print(JSON.stringify(line))
  }
  const summary = await checkRandomShapes({ baselineTree, count: random, seed })
  failed = failed || summary.differences.length > 0
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
  checkFixedShape,
  checkRandomShapes,
  timeShape,
  worker,
  main,
}
