const createAdmin = require('..')
const {
  KafkaJSInvalidConfigurationError,
  KafkaJSServerDoesNotSupportApiKey,
  KafkaJSInvariantViolation,
} = require('../../errors')
const logger = { namespace: () => logger, debug() {}, info() {}, warn() {}, error() {} }
const retry = { retries: 1, initialRetryTime: 1, factor: 0 }
const replica = (partition = 0, brokerId = 1) => ({ topic: 't', partition, brokerId })
const directory = (isFuture = false, path = '/data/a', errorCode = 0) => ({
  path,
  errorCode,
  totalBytes: '9007199254740993',
  usableBytes: '9007199254740992',
  isCordoned: false,
  topics: [
    {
      topic: 't',
      partitions: [
        { partition: 0, size: '9007199254740993', offsetLag: isFuture ? '2' : '0', isFuture },
      ],
    },
  ],
})
const fixture = (brokers, extra = {}) => {
  const cluster = {
    findBroker: jest.fn(async ({ nodeId }) => brokers.get(nodeId)),
    refreshMetadata: jest.fn(async () => {}),
    brokerPool: { metadata: { brokers: [...brokers.keys()].map((nodeId) => ({ nodeId })) } },
    ...extra,
  }
  return { cluster, admin: createAdmin({ cluster, logger, retry }) }
}
describe('log directory administration', () => {
  it('exposes three deadline-wrapped methods', () => {
    const { admin } = fixture(new Map())
    for (const name of ['describeLogDirs', 'alterReplicaLogDirs', 'describeReplicaLogDirs'])
      expect(typeof admin[name]).toBe('function')
  })
  it('sends one read per broker and keeps caller order, sizes and directory errors', async () => {
    const a = { describeLogDirs: jest.fn(async () => ({ errorCode: 0, logDirs: [directory()] })) }
    const b = {
      describeLogDirs: jest.fn(async () => ({
        errorCode: 0,
        logDirs: [directory(false, '/data/offline', 56)],
      })),
    }
    const { admin } = fixture(
      new Map([
        [1, a],
        [2, b],
      ])
    )
    const rows = await admin.describeLogDirs({
      brokers: [2, 1],
      topicPartitions: [{ topic: 't', partitions: [0] }],
    })
    expect(rows.map((r) => r.brokerId)).toEqual([2, 1])
    expect(rows[1].logDirs[0].topics[0].partitions[0].size).toBe('9007199254740993')
    expect(rows[0].logDirs[0].error).toMatchObject({ type: 'KAFKA_STORAGE_ERROR', code: 56 })
    for (const broker of [a, b]) {
      expect(broker.describeLogDirs).toHaveBeenCalledTimes(1)
      expect(broker.describeLogDirs).toHaveBeenCalledWith({
        topics: [{ topic: 't', partitions: [0] }],
      })
    }
  })
  it('defaults to every cached broker after one metadata read', async () => {
    const broker = { describeLogDirs: jest.fn(async () => ({ errorCode: 0, logDirs: [] })) }
    const { admin, cluster } = fixture(
      new Map([
        [1, broker],
        [2, broker],
      ])
    )
    expect((await admin.describeLogDirs()).map((r) => r.brokerId)).toEqual([1, 2])
    expect(cluster.refreshMetadata).toHaveBeenCalledTimes(1)
    expect(broker.describeLogDirs).toHaveBeenCalledTimes(2)
    expect(broker.describeLogDirs).toHaveBeenCalledWith({ topics: null })
  })
  it('retries only the failed broker and retains its original exhausted transport error', async () => {
    const failure = Object.assign(new Error('unavailable'), { retriable: true })
    const a = { describeLogDirs: jest.fn(async () => ({ errorCode: 0, logDirs: [directory()] })) }
    const b = {
      describeLogDirs: jest.fn(async () => {
        throw failure
      }),
    }
    const { admin } = fixture(
      new Map([
        [1, a],
        [2, b],
      ])
    )
    const rows = await admin.describeLogDirs({ brokers: [1, 2] })
    expect(a.describeLogDirs).toHaveBeenCalledTimes(1)
    expect(b.describeLogDirs).toHaveBeenCalledTimes(2)
    expect(rows[0].errorCode).toBe(0)
    expect(rows[1].errorCode).toBe(-1)
    expect(rows[1].error).toBe(failure)
  })
  it('retries top-level request timeout, keeping terminal directory errors as data', async () => {
    const broker = {
      describeLogDirs: jest
        .fn()
        .mockResolvedValueOnce({ errorCode: 7, logDirs: [] })
        .mockResolvedValueOnce({ errorCode: 0, logDirs: [directory(false, '/data/offline', 56)] }),
    }
    const { admin } = fixture(new Map([[1, broker]]))
    const rows = await admin.describeLogDirs({ brokers: [1] })
    expect(broker.describeLogDirs).toHaveBeenCalledTimes(2)
    expect(rows[0].logDirs[0].errorCode).toBe(56)
  })
  it('projects current and future locations, preserves missing replicas and source errors', async () => {
    const broker = {
      describeLogDirs: jest.fn(async () => ({
        errorCode: 0,
        logDirs: [
          directory(),
          directory(true, '/data/b'),
          { ...directory(false, '/offline', 56), topics: [] },
        ],
      })),
    }
    const { admin } = fixture(new Map([[1, broker]]))
    const rows = await admin.describeReplicaLogDirs({ replicas: [replica(), replica(1)] })
    expect(rows[0]).toMatchObject({
      currentLogDir: '/data/a',
      futureLogDir: '/data/b',
      currentOffsetLag: '0',
      futureOffsetLag: '2',
      errorCode: 56,
    })
    expect(rows[1]).toMatchObject({
      currentLogDir: null,
      futureLogDir: null,
      currentOffsetLag: null,
      futureOffsetLag: null,
      errorCode: 56,
    })
    expect(broker.describeLogDirs).toHaveBeenCalledTimes(1)
  })
  it('a missing replica without a source error is successful with null locations', async () => {
    const { admin } = fixture(
      new Map([[1, { describeLogDirs: async () => ({ errorCode: 0, logDirs: [] }) }]])
    )
    expect((await admin.describeReplicaLogDirs({ replicas: [replica()] }))[0]).toMatchObject({
      errorCode: 0,
      error: null,
      currentLogDir: null,
      futureLogDir: null,
    })
  })
  it('keeps log-dir-not-found terminal while retrying only a timed-out replica', async () => {
    const broker = {
      alterReplicaLogDirs: jest
        .fn()
        .mockResolvedValueOnce({
          topics: [
            {
              topic: 't',
              partitions: [
                { partition: 0, errorCode: 57 },
                { partition: 1, errorCode: 7 },
                { partition: 2, errorCode: 0 },
              ],
            },
          ],
        })
        .mockResolvedValueOnce({
          topics: [{ topic: 't', partitions: [{ partition: 1, errorCode: 0 }] }],
        }),
    }
    const { admin } = fixture(new Map([[1, broker]]))
    const replicas = [0, 1, 2].map((p) => ({ ...replica(p), logDir: '/data/b' }))
    const rows = await admin.alterReplicaLogDirs({ replicas })
    expect(rows.map((r) => r.errorCode)).toEqual([57, 0, 0])
    expect(rows[0].error).toMatchObject({ type: 'LOG_DIR_NOT_FOUND' })
    expect(broker.alterReplicaLogDirs.mock.calls[1][0]).toEqual({
      dirs: [{ path: '/data/b', topics: [{ topic: 't', partitions: [1] }] }],
    })
    expect(replicas).toEqual([0, 1, 2].map((p) => ({ ...replica(p), logDir: '/data/b' })))
  })
  it('rejects a missing alteration response identity instead of inferring success', async () => {
    const { admin } = fixture(new Map([[1, { alterReplicaLogDirs: async () => ({ topics: [] }) }]]))
    await expect(
      admin.alterReplicaLogDirs({ replicas: [{ ...replica(), logDir: '/data/b' }] })
    ).rejects.toBeInstanceOf(KafkaJSInvariantViolation)
  })
  it.each([
    { brokers: [] },
    { topicPartitions: [] },
    { topicPartitions: [{ topic: 't', partitions: [] }] },
  ])('empty filters do no discovery or wire %p', async (options) => {
    const { admin, cluster } = fixture(new Map())
    expect(await admin.describeLogDirs(options)).toEqual([])
    expect(cluster.findBroker).not.toHaveBeenCalled()
    expect(cluster.refreshMetadata).not.toHaveBeenCalled()
  })
  it.each([
    null,
    [],
    { brokers: null },
    { brokers: [-1] },
    { brokers: [2147483648] },
    { brokers: [1, 1] },
    { topicPartitions: [{ topic: '', partitions: [0] }] },
    { topicPartitions: [{ topic: 't', partitions: [0, 0] }] },
  ])('rejects directory input before discovery %p', async (options) => {
    const { admin, cluster } = fixture(new Map())
    await expect(admin.describeLogDirs(options)).rejects.toBeInstanceOf(
      KafkaJSInvalidConfigurationError
    )
    expect(cluster.findBroker).not.toHaveBeenCalled()
  })
  it.each(
    [
      [],
      [replica(), replica()],
      [replica(-1)],
      [replica(0, -1)],
      [{ ...replica(), logDir: 'relative' }],
      [{ ...replica(), logDir: '/bad\0path' }],
    ].map((replicas) => ({ replicas }))
  )('rejects move input before discovery %p', async ({ replicas }) => {
    const { admin, cluster } = fixture(new Map())
    await expect(admin.alterReplicaLogDirs({ replicas })).rejects.toBeInstanceOf(
      KafkaJSInvalidConfigurationError
    )
    expect(cluster.findBroker).not.toHaveBeenCalled()
  })
  it('throws a capability refusal before work with both ranges', async () => {
    const error = new KafkaJSServerDoesNotSupportApiKey('client v1-v5, broker absent', {
      apiKey: 35,
    })
    const { admin } = fixture(
      new Map([
        [
          1,
          {
            describeLogDirs: async () => {
              throw error
            },
          },
        ],
      ])
    )
    await expect(admin.describeLogDirs({ brokers: [1] })).rejects.toBe(error)
  })
})

describe('directory deadline boundary', () => {
  it.each(['describeLogDirs', 'describeReplicaLogDirs', 'alterReplicaLogDirs'])(
    'fences held %s and keeps a concurrent peer usable',
    async (method) => {
      jest.useFakeTimers()
      try {
        let rejectHeld
        const held = new Promise((resolve, reject) => {
          rejectHeld = reject
        })
        const broker = {
          describeLogDirs: jest.fn(() => held),
          alterReplicaLogDirs: jest.fn(() => held),
        }
        const { admin } = fixture(new Map([[1, broker]]))
        const options =
          method === 'describeLogDirs'
            ? { brokers: [1], apiTimeout: 20 }
            : { replicas: [{ ...replica(), logDir: '/data/b' }], apiTimeout: 20 }
        const pending = admin[method](options)
        const observed = pending.then(
          (value) => ({ value }),
          (error) => ({ error })
        )
        await jest.advanceTimersByTimeAsync(0)
        expect(
          broker[method === 'alterReplicaLogDirs' ? method : 'describeLogDirs']
        ).toHaveBeenCalledTimes(1)
        await jest.advanceTimersByTimeAsync(21)
        expect((await observed).error).toMatchObject({ name: 'KafkaJSTimeout' })
        rejectHeld(new Error('late transport failure'))
        await Promise.resolve()
        broker.describeLogDirs.mockResolvedValue({ errorCode: 0, logDirs: [] })
        expect(await admin.describeLogDirs({ brokers: [1], apiTimeout: 100 })).toMatchObject([
          { errorCode: 0 },
        ])
      } finally {
        jest.useRealTimers()
      }
    }
  )
})

describe('directory wire seam', () => {
  it('uses real negotiation, headers and independent storage pairs', async () => {
    const { Kafka } = require('../../..')
    const { createScriptedCluster } = require('../../../testHelpers/scriptedCluster')
    const scripted = createScriptedCluster({
      nodes: [{ id: 0, host: 'node0', port: 9092 }],
      profile: 'storage',
    })
    const admin = new Kafka(scripted.kafkaOptions({ retry: { retries: 0 } })).admin()
    try {
      await admin.connect()
      const rows = await admin.describeLogDirs({
        brokers: [0],
        topicPartitions: [{ topic: 't', partitions: [0] }],
      })
      expect(rows[0].error).toBeNull()
      expect(rows[0].logDirs[0]).toMatchObject({ isCordoned: true, totalBytes: '9007199254740995' })
      expect(scripted.requests().filter((r) => r.api === 'DescribeLogDirs')).toMatchObject([
        { node: 0, version: 5, fields: { topics: [{ topic: 't', partitions: [0] }] } },
      ])
      expect(
        await admin.alterReplicaLogDirs({
          replicas: [{ topic: 't', partition: 0, brokerId: 0, logDir: '/data/b' }],
        })
      ).toMatchObject([{ errorCode: 57 }])
      expect(scripted.requests().filter((r) => r.api === 'AlterReplicaLogDirs')).toMatchObject([
        {
          node: 0,
          version: 2,
          fields: { dirs: [{ path: '/data/b', topics: [{ topic: 't', partitions: [0] }] }] },
        },
      ])
    } finally {
      await admin.disconnect()
      scripted.assertClean()
    }
  })
})

describe('directory advertised capability', () => {
  it('refuses a below-floor advertised directory API without sending', async () => {
    const { Kafka } = require('../../..')
    const { createScriptedCluster } = require('../../../testHelpers/scriptedCluster')
    const scripted = createScriptedCluster({
      nodes: [{ id: 0, host: 'node0', port: 9092 }],
      profile: 'storage',
      advertise: { DescribeLogDirs: [0, 0] },
    })
    const admin = new Kafka(scripted.kafkaOptions({ retry: { retries: 0 } })).admin()
    try {
      await admin.connect()
      await expect(admin.describeLogDirs({ brokers: [0] })).rejects.toMatchObject({
        name: 'KafkaJSServerDoesNotSupportApiKey',
        apiKey: 35,
      })
      expect(scripted.requests().filter((r) => r.api === 'DescribeLogDirs')).toHaveLength(0)
    } finally {
      await admin.disconnect()
      scripted.assertClean()
    }
  })
})

describe('directory peer capability refusal', () => {
  it.each(['describeLogDirs', 'describeReplicaLogDirs', 'alterReplicaLogDirs'])(
    'keeps %s refusal call-level after a peer succeeds',
    async (method) => {
      const failure = new KafkaJSServerDoesNotSupportApiKey('missing directory API', {
        apiKey: method === 'alterReplicaLogDirs' ? 34 : 35,
      })
      let completed
      const peerDone = new Promise((resolve) => {
        completed = resolve
      })
      const good = {
        describeLogDirs: jest.fn(async () => {
          completed()
          return { errorCode: 0, logDirs: [directory()] }
        }),
        alterReplicaLogDirs: jest.fn(async () => {
          completed()
          return { topics: [{ topic: 't', partitions: [{ partition: 0, errorCode: 0 }] }] }
        }),
        disconnect: jest.fn(),
      }
      const refuse = jest.fn(async () => {
        await peerDone
        throw failure
      })
      const bad = { describeLogDirs: refuse, alterReplicaLogDirs: refuse, disconnect: jest.fn() }
      const { admin } = fixture(
        new Map([
          [1, good],
          [2, bad],
        ])
      )
      const replicas = [1, 2].map((brokerId) => ({ ...replica(0, brokerId), logDir: '/data/b' }))
      await expect(
        admin[method](method === 'describeLogDirs' ? { brokers: [1, 2] } : { replicas })
      ).rejects.toBe(failure)
      expect(
        good[method === 'alterReplicaLogDirs' ? method : 'describeLogDirs']
      ).toHaveBeenCalledTimes(1)
      expect(refuse).toHaveBeenCalledTimes(1)
      expect(good.disconnect).not.toHaveBeenCalled()
      expect(bad.disconnect).not.toHaveBeenCalled()
    }
  )
})

describe('directory response and input boundaries', () => {
  it.each(['path', 'topic', 'foreign topic', 'foreign partition'])(
    'rejects directory identity contamination: %s',
    async (kind) => {
      const dir = directory()
      const dirs = [dir]
      if (kind === 'path') dirs.push(directory())
      if (kind === 'topic') dir.topics.push({ ...dir.topics[0] })
      if (kind === 'foreign topic') dir.topics[0].topic = 'other'
      if (kind === 'foreign partition') dir.topics[0].partitions[0].partition = 1
      const { admin } = fixture(
        new Map([[1, { describeLogDirs: async () => ({ errorCode: 0, logDirs: dirs }) }]])
      )
      await expect(
        admin.describeLogDirs({ brokers: [1], topicPartitions: [{ topic: 't', partitions: [0] }] })
      ).rejects.toBeInstanceOf(KafkaJSInvariantViolation)
    }
  )
  it.each([
    [
      'current',
      false,
      {
        currentLogDir: '/data/b',
        currentOffsetLag: '7',
        futureLogDir: '/data/c',
        futureOffsetLag: '2',
      },
    ],
    [
      'future',
      true,
      {
        currentLogDir: '/data/c',
        currentOffsetLag: '0',
        futureLogDir: '/data/b',
        futureOffsetLag: '7',
      },
    ],
  ])(
    'keeps the last %s location and lag without changing other locations or brokers',
    async (location, future, expected) => {
      const first = directory(future)
      first.topics[0].partitions[0].offsetLag = '11'
      const last = directory(future, '/data/b')
      last.topics[0].partitions[0].offsetLag = '7'
      const { admin } = fixture(
        new Map([
          [
            1,
            {
              describeLogDirs: async () => ({
                errorCode: 0,
                logDirs: [first, directory(!future, '/data/c'), last],
              }),
            },
          ],
          [
            2,
            {
              describeLogDirs: async () => ({
                errorCode: 0,
                logDirs: [directory(false, '/data/peer')],
              }),
            },
          ],
        ])
      )
      expect(await admin.describeReplicaLogDirs({ replicas: [replica(0, 2), replica()] })).toEqual([
        {
          topic: 't',
          partition: 0,
          brokerId: 2,
          currentLogDir: '/data/peer',
          currentOffsetLag: '0',
          futureLogDir: null,
          futureOffsetLag: null,
          errorCode: 0,
          error: null,
          errorMessage: null,
        },
        {
          topic: 't',
          partition: 0,
          brokerId: 1,
          ...expected,
          errorCode: 0,
          error: null,
          errorMessage: null,
        },
      ])
    }
  )
  it.each([
    ['current then current', false, false, '/data/a', null, '7', null],
    ['future then future', true, true, null, '/data/a', null, '7'],
    ['current then future', false, true, null, '/data/a', null, '7'],
    ['future then current', true, false, '/data/a', null, '7', null],
  ])(
    'projects only the last partition entry in one directory: %s',
    async (
      shape,
      firstFuture,
      lastFuture,
      currentLogDir,
      futureLogDir,
      currentOffsetLag,
      futureOffsetLag
    ) => {
      const dir = directory(firstFuture)
      dir.topics[0].partitions.push({
        partition: 0,
        size: '9007199254740995',
        offsetLag: '7',
        isFuture: lastFuture,
      })
      const { admin } = fixture(
        new Map([[1, { describeLogDirs: async () => ({ errorCode: 0, logDirs: [dir] }) }]])
      )
      const raw = await admin.describeLogDirs({
        brokers: [1],
        topicPartitions: [{ topic: 't', partitions: [0] }],
      })
      expect(raw).toHaveLength(1)
      expect(raw[0].errorCode).toBe(0)
      expect(raw[0].logDirs[0].topics[0].partitions).toEqual([
        {
          partition: 0,
          size: '9007199254740993',
          offsetLag: firstFuture ? '2' : '0',
          isFuture: firstFuture,
        },
        { partition: 0, size: '9007199254740995', offsetLag: '7', isFuture: lastFuture },
      ])
      expect(await admin.describeReplicaLogDirs({ replicas: [replica()] })).toEqual([
        {
          topic: 't',
          partition: 0,
          brokerId: 1,
          currentLogDir,
          futureLogDir,
          currentOffsetLag,
          futureOffsetLag,
          errorCode: 0,
          error: null,
          errorMessage: null,
        },
      ])
    }
  )
  it('retains successful broker while a replacement route fails', async () => {
    const failure = new Error('route unavailable')
    const a = { describeLogDirs: jest.fn(async () => ({ errorCode: 0, logDirs: [] })) }
    let attempts = 0
    const { admin } = fixture(
      new Map([
        [1, a],
        [2, {}],
      ]),
      {
        findBroker: async ({ nodeId }) => {
          if (nodeId === 1) return a
          if (attempts++ === 0)
            return {
              describeLogDirs: async () => {
                throw failure
              },
            }
          throw failure
        },
      }
    )
    const rows = await admin.describeLogDirs({ brokers: [1, 2] })
    expect(rows[0].errorCode).toBe(0)
    expect(rows[1].error).toBe(failure)
    expect(a.describeLogDirs).toHaveBeenCalledTimes(1)
  })
  it.each(['describeLogDirs', 'alterReplicaLogDirs'])(
    'retains the latest request error at exhaustion: %s',
    async (method) => {
      const request = jest.fn(async () =>
        method === 'describeLogDirs'
          ? { errorCode: 7, logDirs: [] }
          : { topics: [{ topic: 't', partitions: [{ partition: 0, errorCode: 7 }] }] }
      )
      const { cluster } = fixture(new Map([[1, { [method]: request }]]))
      const admin = createAdmin({ cluster, logger, retry: { ...retry, retries: 0 } })
      const options =
        method === 'describeLogDirs'
          ? { brokers: [1] }
          : { replicas: [{ ...replica(), logDir: '/data/b' }] }
      const rows = await admin[method](options)
      expect(rows[0].errorCode).toBe(7)
      expect(rows[0].error).toMatchObject({ type: 'REQUEST_TIMED_OUT' })
    }
  )
  it.each([-1, 1.5, '0', null, 2147483648])(
    'validates complete input before discovery: %s',
    async (bad) => {
      const { admin, cluster } = fixture(new Map())
      await expect(admin.describeLogDirs({ brokers: [bad] })).rejects.toBeInstanceOf(
        KafkaJSInvalidConfigurationError
      )
      for (const method of ['alterReplicaLogDirs', 'describeReplicaLogDirs']) {
        await expect(
          admin[method]({ replicas: [{ ...replica(), brokerId: bad, logDir: '/data/b' }] })
        ).rejects.toBeInstanceOf(KafkaJSInvalidConfigurationError)
        await expect(
          admin[method]({ replicas: [{ ...replica(bad), logDir: '/data/b' }] })
        ).rejects.toBeInstanceOf(KafkaJSInvalidConfigurationError)
      }
      expect(cluster.refreshMetadata).not.toHaveBeenCalled()
      expect(cluster.findBroker).not.toHaveBeenCalled()
    }
  )
  it.each(['describeLogDirs', 'describeReplicaLogDirs', 'alterReplicaLogDirs'])(
    'fences metadata and broker lookup publication: %s',
    async (method) => {
      for (const stage of ['refreshMetadata', 'findBroker']) {
        jest.useFakeTimers()
        try {
          let rejectHeld
          const held = new Promise((resolve, reject) => {
            rejectHeld = reject
          })
          const send = jest.fn(async () => ({ errorCode: 0, logDirs: [] }))
          const { admin } = fixture(
            new Map([[1, { describeLogDirs: send, alterReplicaLogDirs: send }]]),
            { [stage]: () => held }
          )
          const options =
            method === 'describeLogDirs'
              ? { brokers: [1], apiTimeout: 20 }
              : { replicas: [{ ...replica(), logDir: '/data/b' }], apiTimeout: 20 }
          const observed = admin[method](options).catch((error) => error)
          await jest.advanceTimersByTimeAsync(21)
          expect(await observed).toMatchObject({ name: 'KafkaJSTimeout' })
          rejectHeld(new Error('late rejection'))
          await Promise.resolve()
          expect(send).not.toHaveBeenCalled()
        } finally {
          jest.useRealTimers()
        }
      }
    }
  )
})

describe('directory retry and monotonic expiry', () => {
  it.each(['describeLogDirs', 'describeReplicaLogDirs', 'alterReplicaLogDirs'])(
    'checks %s publication when timer delivery is withheld',
    async (method) => {
      let now = 100
      const clock = jest.spyOn(performance, 'now').mockImplementation(() => now)
      try {
        let release
        const held = new Promise((resolve) => {
          release = resolve
        })
        const send = jest.fn(async () => ({ errorCode: 0, logDirs: [] }))
        const { admin } = fixture(
          new Map([[1, { describeLogDirs: send, alterReplicaLogDirs: send }]]),
          { refreshMetadata: () => held }
        )
        const options =
          method === 'describeLogDirs'
            ? { brokers: [1], apiTimeout: 20 }
            : { replicas: [{ ...replica(), logDir: '/data/b' }], apiTimeout: 20 }
        const observed = admin[method](options).catch((error) => error)
        now = 121
        release()
        expect(await observed).toMatchObject({ name: 'KafkaJSTimeout' })
        expect(send).not.toHaveBeenCalled()
      } finally {
        clock.mockRestore()
      }
    }
  )
  it.each(['describeLogDirs', 'describeReplicaLogDirs', 'alterReplicaLogDirs'])(
    'retires %s retry delay before another send',
    async (method) => {
      jest.useFakeTimers()
      try {
        const send = jest.fn(async () =>
          method === 'alterReplicaLogDirs'
            ? { topics: [{ topic: 't', partitions: [{ partition: 0, errorCode: 7 }] }] }
            : { errorCode: 7, logDirs: [] }
        )
        const { cluster } = fixture(
          new Map([[1, { describeLogDirs: send, alterReplicaLogDirs: send }]])
        )
        const admin = createAdmin({
          cluster,
          logger,
          retry: { retries: 5, initialRetryTime: 1000, factor: 0 },
        })
        const options =
          method === 'describeLogDirs'
            ? { brokers: [1], apiTimeout: 20 }
            : { replicas: [{ ...replica(), logDir: '/data/b' }], apiTimeout: 20 }
        const observed = admin[method](options).catch((error) => error)
        await jest.advanceTimersByTimeAsync(21)
        const error = await observed
        expect(error).toMatchObject({ name: 'KafkaJSTimeout' })
        expect(error.message).toContain('unknown')
        await jest.advanceTimersByTimeAsync(2000)
        expect(send).toHaveBeenCalledTimes(1)
        expect(jest.getTimerCount()).toBe(0)
      } finally {
        jest.useRealTimers()
      }
    }
  )
  it.each(['describeLogDirs', 'alterReplicaLogDirs'])(
    'keeps latest transport failure after %s request timeout',
    async (method) => {
      const failure = new Error('latest transport error')
      const send = jest
        .fn()
        .mockResolvedValueOnce(
          method === 'describeLogDirs'
            ? { errorCode: 7, logDirs: [] }
            : { topics: [{ topic: 't', partitions: [{ partition: 0, errorCode: 7 }] }] }
        )
        .mockRejectedValueOnce(failure)
      const { admin } = fixture(new Map([[1, { [method]: send }]]))
      const options =
        method === 'describeLogDirs'
          ? { brokers: [1] }
          : { replicas: [{ ...replica(), logDir: '/data/b' }] }
      const rows = await admin[method](options)
      expect(rows[0].errorCode).toBe(-1)
      expect(rows[0].error).toBe(failure)
    }
  )
})

describe('directory selection validation', () => {
  it.each([null, [], 3])('rejects invalid options %s before discovery', async (options) => {
    const { admin, cluster } = fixture(new Map())
    for (const method of ['describeLogDirs', 'alterReplicaLogDirs', 'describeReplicaLogDirs'])
      await expect(admin[method](options)).rejects.toBeInstanceOf(KafkaJSInvalidConfigurationError)
    expect(cluster.refreshMetadata).not.toHaveBeenCalled()
  })
  it.each([0, 2147483647])('accepts boundary broker and partition id %s', async (id) => {
    const broker = { describeLogDirs: async () => ({ errorCode: 0, logDirs: [] }) }
    const { admin } = fixture(new Map([[id, broker]]))
    const rows = await admin.describeReplicaLogDirs({
      replicas: [{ topic: 't', partition: id, brokerId: id }],
    })
    expect(rows[0]).toMatchObject({ errorCode: 0, currentLogDir: null, futureLogDir: null })
  })
  it('retains terminal and successful moves while a peer route exhausts', async () => {
    const failure = new Error('unresolved route')
    const send = jest.fn(async () => ({
      topics: [
        {
          topic: 't',
          partitions: [
            { partition: 0, errorCode: 57 },
            { partition: 1, errorCode: 0 },
          ],
        },
      ],
    }))
    const { admin } = fixture(
      new Map([
        [1, { alterReplicaLogDirs: send }],
        [
          2,
          {
            alterReplicaLogDirs: async () => {
              throw failure
            },
          },
        ],
      ])
    )
    const rows = await admin.alterReplicaLogDirs({
      replicas: [replica(0), replica(1), replica(0, 2)].map((r) => ({ ...r, logDir: '/data/b' })),
    })
    expect(rows.map((row) => row.errorCode)).toEqual([57, 0, -1])
    expect(rows[2].error).toBe(failure)
    expect(send).toHaveBeenCalledTimes(1)
  })
})

describe('directory transport deadline composition', () => {
  it.each(['describeLogDirs', 'describeReplicaLogDirs', 'alterReplicaLogDirs'])(
    'fences %s connection setup and queued transmission',
    async (method) => {
      const Broker = require('../../broker')
      const { lookup } = require('../../protocol/requests')
      const Queue = require('../../network/requestQueue')
      for (const stage of ['setup', 'queue']) {
        for (const withheld of [false, true]) {
          jest.useFakeTimers()
          let now = 0
          const clock = jest.spyOn(performance, 'now').mockImplementation(() => now)
          const writes = jest.fn()
          const queue = new Queue({
            logger,
            requestTimeout: 500,
            enforceRequestTimeout: false,
            isConnected: () => true,
          })
          queue.maybeThrottle(100)
          let release
          const setup = new Promise((resolve) => {
            release = resolve
          })
          const connection = { isConnected: () => true }
          const pool = {
            generation: 0,
            pool: [],
            setupPolicy: { timeoutCeiling: 30000, waitCeiling: 1000 },
            getConnectionByRequest: jest.fn(() => (stage === 'setup' ? setup : connection)),
            send: jest.fn(
              (protocol, conn, { guard }) =>
                new Promise((resolve, reject) =>
                  queue.push({
                    entry: {
                      correlationId: 1,
                      apiName: protocol.request.apiName,
                      apiKey: protocol.request.apiKey,
                      apiVersion: protocol.request.apiVersion,
                      resolve,
                      reject,
                    },
                    expectResponse: true,
                    sendRequest: writes,
                    guard,
                  })
                )
            ),
            destroy: jest.fn(),
          }
          try {
            const versions = {
              34: { minVersion: 1, maxVersion: 2 },
              35: { minVersion: 1, maxVersion: 5 },
            }
            const broker = new Broker({ connectionPool: pool, logger, nodeId: 1, versions })
            broker.lookupRequest = lookup(versions)
            broker.versionsGeneration = 0
            const { admin } = fixture(new Map([[1, broker]]))
            const options =
              method === 'describeLogDirs'
                ? { brokers: [1], apiTimeout: 20 }
                : { replicas: [{ ...replica(), logDir: '/data/b' }], apiTimeout: 20 }
            const observed = admin[method](options).catch((error) => error)
            await jest.advanceTimersByTimeAsync(0)
            if (withheld) {
              now = 21
              if (stage === 'setup') release(connection)
              else {
                queue.throttledUntil = -1
                queue.checkPendingRequests()
              }
            } else {
              now = 21
              await jest.advanceTimersByTimeAsync(21)
            }

            expect(await observed).toMatchObject({ name: 'KafkaJSTimeout' })
            release(connection)
            await jest.advanceTimersByTimeAsync(200)
            expect(writes).not.toHaveBeenCalled()
            expect(pool.destroy).not.toHaveBeenCalled()
          } finally {
            queue.destroy()
            clock.mockRestore()
            jest.useRealTimers()
          }
        }
      }
    }
  )
})
