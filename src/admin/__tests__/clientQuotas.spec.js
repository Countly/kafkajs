const createAdmin = require('..')
const { Kafka } = require('../../..')
const { KafkaJSInvalidConfigurationError, KafkaJSInvariantViolation } = require('../../errors')
const { createScriptedCluster } = require('../../../testHelpers/scriptedCluster')
const logger = { namespace: () => logger, debug() {}, info() {}, warn() {}, error() {} }
const retry = { retries: 1, initialRetryTime: 1, maxRetryTime: 1, factor: 0 }
const entity = (entityName) => [{ entityType: 'user', entityName }]
const entry = (user) => ({
  entity: entity(user),
  ops: [{ key: 'producer_byte_rate', value: 1234.5 }],
})

describe('client quotas', () => {
  test.each([0, 1])(
    'quota wire v%s carries every match kind and validateOnly without follow-up',
    async (version) => {
      const scripted = createScriptedCluster({
        nodes: [{ id: 0, host: 'node0', port: 9092 }],
        profile: 'securityAdmin',
        advertise: {
          DescribeClientQuotas: [version, version],
          AlterClientQuotas: [version, version],
        },
      })
      scripted.answer('AlterClientQuotas', ({ entries }) => ({
        entries: entries.map(({ entity }, i) => ({
          entity,
          errorCode: i === 0 ? 0 : 35,
          errorMessage: i === 0 ? null : 'denied',
        })),
      }))
      const admin = new Kafka(scripted.kafkaOptions({ retry })).admin()
      try {
        await admin.connect()
        for (const [matchType, match] of [
          ['exact', 'test-user'],
          ['default', undefined],
          ['any', undefined],
        ]) {
          const rows = await admin.describeClientQuotas({
            components: [{ entityType: 'user', matchType, match }],
          })
          expect(rows[0].values.producer_byte_rate).toBe(1234.5)
        }
        const before = scripted.requests().length
        const result = await admin.alterClientQuotas({
          entries: [entry('test-user'), entry(null)],
          validateOnly: true,
        })
        expect(result.map((row) => row.errorCode)).toEqual([0, 35])
        expect(result[1].error).toMatchObject({ code: 35 })
        const requests = scripted.requests().slice(before)
        expect(requests.map(({ api }) => api)).toEqual(['AlterClientQuotas'])
        expect(requests[0].fields.validateOnly).toBe(true)
        expect(requests[0].fields.entries[0].ops[0].value).toBe(1234.5)
        expect(
          scripted
            .requests()
            .filter(({ api }) => api === 'DescribeClientQuotas')
            .map(({ fields }) => fields.components[0])
        ).toEqual([
          { entityType: 'user', matchType: 0, match: 'test-user' },
          { entityType: 'user', matchType: 1, match: null },
          { entityType: 'user', matchType: 2, match: null },
        ])
      } finally {
        await admin.disconnect()
        scripted.assertClean()
      }
    }
  )

  test('retries only the unresolved entity, correlating tuple order without resending successes', async () => {
    const attempts = []
    const broker = {
      alterClientQuotas: async ({ entries }) => {
        attempts.push(entries)
        return {
          entries: entries.map(({ entity }) => ({
            entity: [...entity].reverse(),
            errorCode: entity[0].entityName === 'b' && attempts.length === 1 ? 7 : 0,
            errorMessage: null,
          })),
        }
      },
    }
    const cluster = { findConnectedBroker: async () => broker }
    const admin = createAdmin({ cluster, logger, retry })
    const result = await admin.alterClientQuotas({ entries: [entry('a'), entry('b')] })
    expect(result.map(({ entity }) => entity[0].entityName)).toEqual(['a', 'b'])
    expect(attempts.map((rows) => rows.map(({ entity }) => entity[0].entityName))).toEqual([
      ['a', 'b'],
      ['b'],
    ])
  })

  test('exhaustion retains the last typed row and a remove has zero only on the wire', async () => {
    const broker = {
      alterClientQuotas: jest.fn(async ({ entries }) => ({
        entries: entries.map(({ entity }) => ({ entity, errorCode: 7, errorMessage: 'quota' })),
      })),
    }
    const admin = createAdmin({
      cluster: { findConnectedBroker: async () => broker },
      logger,
      retry: { retries: 0 },
    })
    const rows = await admin.alterClientQuotas({
      entries: [{ entity: entity(null), ops: [{ key: 'unknown-broker-key', remove: true }] }],
    })
    expect(rows[0]).toMatchObject({ errorCode: 7, error: { code: 7 } })
    expect(broker.alterClientQuotas.mock.calls[0][0].entries[0].ops).toEqual([
      { key: 'unknown-broker-key', remove: true, value: 0 },
    ])
  })

  test.each(['missing', 'duplicate', 'foreign'])('rejects %s result identities', async (kind) => {
    const row = {
      entity: entity(kind === 'foreign' ? 'foreign' : 'a'),
      errorCode: 0,
      errorMessage: null,
    }
    const broker = {
      alterClientQuotas: async () => ({
        entries: kind === 'missing' ? [] : kind === 'duplicate' ? [row, row] : [row],
      }),
    }
    await expect(
      createAdmin({
        cluster: { findConnectedBroker: async () => broker },
        logger,
        retry: { retries: 0 },
      }).alterClientQuotas({ entries: [entry('a')] })
    ).rejects.toBeInstanceOf(KafkaJSInvariantViolation)
  })

  test.each([
    {},
    { entries: [] },
    { entries: [{ entity: [], ops: [] }] },
    { entries: [entry('a'), entry('a')] },
    { entries: [{ entity: entity('a'), ops: [{ key: 'k', value: 1, remove: true }] }] },
    ...[NaN, Infinity, -1, '1', null].map((value) => ({
      entries: [{ entity: entity('a'), ops: [{ key: 'k', value }] }],
    })),
    {
      entries: [
        {
          entity: entity('a'),
          ops: [
            { key: 'k', value: 1 },
            { key: 'k', remove: true },
          ],
        },
      ],
    },
    { entries: [entry('a')], validateOnly: 1 },
    { entries: [{ entity: [...entity('a'), ...entity('b')], ops: [{ key: 'k', value: 1 }] }] },
    {
      entries: [
        { entity: [{ entityType: 'bad', entityName: 'a' }], ops: [{ key: 'k', value: 1 }] },
      ],
    },
    { entries: [{ entity: entity('a'), ops: [{ key: 'k', value: 1, remove: 'false' }] }] },
    { entries: [{ entity: entity('a'), ops: new Array(1) }] },
    null,
  ])('rejects an invalid mutation before discovery: %p', async (options) => {
    const findConnectedBroker = jest.fn(async () => ({
      alterClientQuotas: async ({ entries }) => ({
        entries: entries.map(({ entity }) => ({ entity, errorCode: 0, errorMessage: null })),
      }),
      describeClientQuotas: async () => ({ entries: [] }),
    }))
    const admin = createAdmin({ cluster: { findConnectedBroker }, logger, retry })
    await expect(admin.alterClientQuotas(options)).rejects.toBeInstanceOf(
      KafkaJSInvalidConfigurationError
    )
    expect(findConnectedBroker).not.toHaveBeenCalled()
  })
  test.each([
    { components: [{ entityType: 'user', matchType: 'exact' }] },
    { components: [{ entityType: 'ip', matchType: 'default', match: 'x' }] },
    { components: [{ entityType: 'user', matchType: 'any', match: null }] },
    { components: [], strict: 0 },
    { components: [{ entityType: 'bad', matchType: 'any' }] },
    {
      components: [
        { entityType: 'ip', matchType: 'any' },
        { entityType: 'ip', matchType: 'any' },
      ],
    },
    { components: new Array(1) },
  ])('rejects invalid filters before discovery: %p', async (options) => {
    const findConnectedBroker = jest.fn(async () => ({
      alterClientQuotas: async ({ entries }) => ({
        entries: entries.map(({ entity }) => ({ entity, errorCode: 0, errorMessage: null })),
      }),
      describeClientQuotas: async () => ({ entries: [] }),
    }))
    await expect(
      createAdmin({ cluster: { findConnectedBroker }, logger, retry }).describeClientQuotas(options)
    ).rejects.toBeInstanceOf(KafkaJSInvalidConfigurationError)
    expect(findConnectedBroker).not.toHaveBeenCalled()
  })
})

describe('security administration capability and retained outcomes', () => {
  const { KafkaJSConnectionError, KafkaJSServerDoesNotSupportApiKey } = require('../../errors')
  test.each(['DescribeClientQuotas', 'AlterClientQuotas'])(
    '%s refuses a range with no common version before transmission',
    async (api) => {
      const scripted = createScriptedCluster({
        nodes: [{ id: 0, host: 'node0', port: 9092 }],
        profile: 'securityAdmin',
        advertise: { [api]: [99, 99] },
      })
      const admin = new Kafka(scripted.kafkaOptions({ retry: { retries: 0 } })).admin()
      try {
        await admin.connect()
        const result =
          api === 'DescribeClientQuotas'
            ? admin.describeClientQuotas({ components: [] })
            : admin.alterClientQuotas({ entries: [entry('a')] })
        await expect(result).rejects.toBeInstanceOf(KafkaJSServerDoesNotSupportApiKey)
        expect(scripted.requests().filter((r) => r.api === api)).toEqual([])
      } finally {
        await admin.disconnect()
        scripted.assertClean()
      }
    }
  )
  test('quota route failure preserves completed entities and attaches the local error only to pending ones', async () => {
    const local = new KafkaJSConnectionError('offline')
    const broker = {
      alterClientQuotas: async () => ({
        entries: [
          { entity: entity('a'), errorCode: 0 },
          { entity: entity('b'), errorCode: 7 },
        ],
      }),
    }
    const findConnectedBroker = jest
      .fn(async () => ({
        alterClientQuotas: async ({ entries }) => ({
          entries: entries.map(({ entity }) => ({ entity, errorCode: 0, errorMessage: null })),
        }),
        describeClientQuotas: async () => ({ entries: [] }),
      }))
      .mockResolvedValueOnce(broker)
      .mockRejectedValue(local)
    const rows = await createAdmin({
      cluster: { findConnectedBroker },
      logger,
      retry,
    }).alterClientQuotas({ entries: [entry('a'), entry('b')] })
    expect(rows[0]).toMatchObject({ errorCode: 0, error: null })
    expect(rows[1].errorCode).toBe(-1)
    expect(rows[1].error).toBe(local)
  })
  test('duplicate described quota entities cannot fabricate a valid result', async () => {
    const row = { entity: entity('a'), values: [] }
    const broker = { describeClientQuotas: async () => ({ entries: [row, row] }) }
    await expect(
      createAdmin({
        cluster: { findConnectedBroker: async () => broker },
        logger,
        retry: { retries: 0 },
      }).describeClientQuotas({ components: [] })
    ).rejects.toBeInstanceOf(KafkaJSInvariantViolation)
  })
})
