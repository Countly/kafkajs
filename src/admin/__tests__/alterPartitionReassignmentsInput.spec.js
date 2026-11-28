const createAdmin = require('../index')

const RETRY = { retries: 0, initialRetryTime: 1, maxRetryTime: 1 }

/**
 * An admin whose controller accepts every reassignment. Every cluster and broker call is recorded.
 * @returns {{ admin: object, cluster: object, sent: object[] }}
 */
const adminOver = () => {
  const logger = { namespace: () => logger, debug() {}, info() {}, warn() {}, error() {} }
  const sent = []
  const broker = {
    alterPartitionReassignments: jest.fn(async ({ topics }) => {
      sent.push(JSON.parse(JSON.stringify(topics)))
    }),
  }
  const cluster = {
    refreshMetadata: jest.fn(async () => {}),
    findControllerBroker: jest.fn(async () => broker),
  }
  const admin = createAdmin({ logger, retry: RETRY, cluster })
  return { admin, cluster, sent }
}

/** @returns {unknown[]} a copy of `items` with a hole at `index` */
const holeAt = (items, index) => {
  const copy = [...items]
  delete copy[index]
  return copy
}

/** @returns {unknown[]} a copy of `items` whose own index `index` is a hole the prototype fills */
const inheritedAt = (items, index, inherited) => {
  const copy = holeAt(items, index)
  Object.setPrototypeOf(copy, Object.assign(Object.create(Array.prototype), { [index]: inherited }))
  return copy
}

/** @returns {object[]} topic `t` with one partition entry */
const oneTopic = (assignment) => [{ topic: 't', partitionAssignment: [assignment] }]

/** @returns {number} how many times any method of the fake cluster was called */
const clusterCalls = (cluster) =>
  Object.values(cluster).reduce((count, method) => count + method.mock.calls.length, 0)

/**
 * Expects the call to be refused with a `KafkaJSNonRetriableError` before any cluster call.
 * @param {object[]} topics
 * @returns {Promise<void>}
 */
const expectRefused = async (topics) => {
  const { admin, cluster } = adminOver()
  await expect(admin.alterPartitionReassignments({ topics })).rejects.toMatchObject({
    name: 'KafkaJSNonRetriableError',
  })
  expect(clusterCalls(cluster)).toBe(0)
}

describe('Admin > alterPartitionReassignments > input', () => {
  test.each([
    ['NaN', NaN],
    ['a fraction', 0.5],
    ['2^31', 2 ** 31],
    ['Infinity', Infinity],
  ])('refuses the partition %s before any request', async (_, partition) => {
    await expectRefused(oneTopic({ partition, replicas: [1] }))
  })

  test.each([
    ['NaN', NaN],
    ['a fraction', 1.5],
    ['2^31', 2 ** 31],
  ])('refuses the replica %s before any request', async (_, replica) => {
    await expectRefused(oneTopic({ partition: 0, replicas: [1, replica] }))
  })

  test('refuses replicas with a hole before any request', async () => {
    await expectRefused(oneTopic({ partition: 0, replicas: holeAt([1, 0, 2], 1) }))
  })

  test('refuses replicas with a hole that their own iterator skips before any request', async () => {
    const replicas = holeAt([1, 0, 2], 1)
    replicas[Symbol.iterator] = function* () {
      yield 1
      yield 2
    }
    await expectRefused(oneTopic({ partition: 0, replicas }))
  })

  test.each([
    [
      'the topics',
      inheritedAt([null], 0, {
        topic: 't',
        partitionAssignment: [{ partition: 0, replicas: [1] }],
      }),
    ],
    [
      'the partitions',
      [
        {
          topic: 't',
          partitionAssignment: inheritedAt([null], 0, { partition: 0, replicas: [1] }),
        },
      ],
    ],
    ['the replicas', oneTopic({ partition: 0, replicas: inheritedAt([1, 0], 1, 2) })],
  ])(
    'refuses an index of %s that only the prototype fills before any request',
    async (_, topics) => {
      await expectRefused(topics)
    }
  )

  test('refuses a replica listed twice in an assignment, with a valid sibling, before any request', async () => {
    const { admin, cluster } = adminOver()

    await expect(
      admin.alterPartitionReassignments({
        topics: [
          {
            topic: 't',
            partitionAssignment: [
              { partition: 0, replicas: [1, 2] },
              { partition: 1, replicas: [2, 3, 2] },
            ],
          },
        ],
      })
    ).rejects.toMatchObject({
      name: 'KafkaJSNonRetriableError',
      message:
        'Invalid replica assignment: replica 2 is listed more than once for topic: t on partition: 1',
    })
    expect(clusterCalls(cluster)).toBe(0)
  })

  test.each([
    ['a null topic entry', [null]],
    ['an undefined topic entry', [undefined]],
    ['a hole in the topics', holeAt([null, ...oneTopic({ partition: 0, replicas: [1] })], 0)],
    ['a null partition entry', oneTopic(null)],
    [
      'a hole in the partitions',
      [
        {
          topic: 't',
          partitionAssignment: holeAt([null, { partition: 0, replicas: [1] }], 0),
        },
      ],
    ],
  ])('refuses %s with a KafkaJSNonRetriableError before any request', async (_, topics) => {
    await expectRefused(topics)
  })

  test('refuses a partition listed twice for a topic before any request', async () => {
    await expectRefused([
      {
        topic: 't',
        partitionAssignment: [
          { partition: 0, replicas: [1, 2] },
          { partition: 0, replicas: [3] },
        ],
      },
    ])
  })

  test('still refuses null replicas', async () => {
    await expectRefused(oneTopic({ partition: 0, replicas: null }))
  })

  test('sends the assignment as it was when the call started', async () => {
    const { admin, sent } = adminOver()
    const replicas = [1, 2]
    const partitionAssignment = [{ partition: 0, replicas }]
    const topics = [{ topic: 't', partitionAssignment }]

    const call = admin.alterPartitionReassignments({ topics })
    replicas[0] = 7
    partitionAssignment.push({ partition: 1, replicas: [3] })
    await call

    expect(sent).toEqual([oneTopic({ partition: 0, replicas: [1, 2] })])
  })

  test('reads the replicas getter of an entry once, when the call starts', async () => {
    const { admin, sent } = adminOver()
    let reads = 0
    let replicas = [1, 2]
    const assignment = {
      partition: 0,
      get replicas() {
        reads += 1
        return replicas
      },
    }

    const call = admin.alterPartitionReassignments({
      topics: [{ topic: 't', partitionAssignment: [assignment] }],
    })
    replicas = [3]
    await call

    expect(reads).toBe(1)
    expect(sent).toEqual([oneTopic({ partition: 0, replicas: [1, 2] })])
  })

  test('reads the topic, partitionAssignment, partition and replicas fields and each replica slot once', async () => {
    const { admin, sent } = adminOver()
    const reads = {}
    const once = (name, first, later) => ({
      enumerable: true,
      get() {
        reads[name] = (reads[name] || 0) + 1
        return reads[name] === 1 ? first : later
      },
    })
    const replicas = []
    Object.defineProperty(replicas, 0, once('replica 0', 1, 7))
    Object.defineProperty(replicas, 1, once('replica 1', 2, 7))
    const assignment = Object.defineProperties(
      {},
      { partition: once('partition', 0, 5), replicas: once('replicas', replicas, [7]) }
    )
    const topic = Object.defineProperties(
      {},
      { topic: once('topic', 't', 'u'), partitionAssignment: once('assignments', [assignment], []) }
    )

    await admin.alterPartitionReassignments({ topics: [topic] })

    expect(reads).toEqual({
      topic: 1,
      assignments: 1,
      partition: 1,
      replicas: 1,
      'replica 0': 1,
      'replica 1': 1,
    })
    expect(sent).toEqual([oneTopic({ partition: 0, replicas: [1, 2] })])
  })

  test('forwards a valid assignment and an empty replica list to the controller', async () => {
    const { admin, sent } = adminOver()

    await admin.alterPartitionReassignments({
      topics: [
        {
          topic: 't',
          partitionAssignment: [
            { partition: 0, replicas: [0, 2147483647] },
            { partition: 2147483647, replicas: [] },
          ],
        },
      ],
    })

    expect(sent).toEqual([
      [
        {
          topic: 't',
          partitionAssignment: [
            { partition: 0, replicas: [0, 2147483647] },
            { partition: 2147483647, replicas: [] },
          ],
        },
      ],
    ])
  })
})
