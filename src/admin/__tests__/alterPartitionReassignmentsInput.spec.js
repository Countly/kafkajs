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

  test('sends a valid assignment, an empty replica list included', async () => {
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
