const fs = require('node:fs')
const path = require('node:path')
const crypto = require('node:crypto')
const { inspect } = require('node:util')
const createAdmin = require('..')
const { Kafka, logLevel } = require('../../..')
const { SCRAM } = require('../../broker/saslAuthenticator/scram')
const {
  KafkaJSInvalidConfigurationError,
  KafkaJSTimeout,
  KafkaJSInvariantViolation,
} = require('../../errors')
const { createScriptedCluster } = require('../../../testHelpers/scriptedCluster')
const logger = { namespace: () => logger, debug() {}, info() {}, warn() {}, error() {} }
const retry = { retries: 1, initialRetryTime: 1, maxRetryTime: 1, factor: 0 }
const password = 'password-marker-only-test'
const salt = Buffer.from('salt-marker-only-test')
const change = (user = 'test-user', extra = {}) => ({
  user,
  mechanism: 'SCRAM-SHA-256',
  iterations: 4096,
  password,
  salt,
  ...extra,
})

describe('SCRAM credential administration', () => {
  afterEach(() => jest.restoreAllMocks())
  test.each(['SCRAM-SHA-256', 'SCRAM-SHA-512'])(
    'derives %s once and erases owned bytes without changing caller salt',
    async (mechanism) => {
      const bytes = []
      const retained = []
      const broker = {
        alterUserScramCredentials: jest.fn(async (fields) => {
          for (const item of fields.upsertions) {
            retained.push(item)
            bytes.push({
              salt: Buffer.from(item.salt),
              saltedPassword: Buffer.from(item.saltedPassword),
              name: item.name,
              mechanism: item.mechanism,
            })
          }
          return {
            results: [{ user: 'test-user', errorCode: 0, errorMessage: 'broker-echo-' + password }],
          }
        }),
      }
      const cluster = {
        refreshMetadata: jest.fn(async () => {}),
        findControllerBroker: jest.fn(async () => broker),
      }
      const spy = jest.spyOn(SCRAM, 'hi')
      const result = await createAdmin({ cluster, logger, retry }).alterUserScramCredentials({
        upsertions: [change('test-user', { mechanism })],
      })
      const digest = mechanism === 'SCRAM-SHA-256' ? 'sha256' : 'sha512'
      expect(bytes[0].saltedPassword).toEqual(
        crypto.pbkdf2Sync(password, salt, 4096, digest === 'sha256' ? 32 : 64, digest)
      )
      expect(bytes[0].salt).toEqual(salt)
      expect(retained[0].salt.every((byte) => byte === 0)).toBe(true)
      expect(retained[0].saltedPassword.every((byte) => byte === 0)).toBe(true)
      expect(salt.toString()).toBe('salt-marker-only-test')
      expect(spy).toHaveBeenCalledTimes(1)
      expect(result).toEqual([{ user: 'test-user', errorCode: 0, error: null, errorMessage: null }])
      expect(inspect(result, { depth: null })).not.toContain(password)
    }
  )
  test.each(['sha256', 'sha512'])('Java %s salted-password bytes match Hi', async (digest) => {
    const file = path.join(
      __dirname,
      '../../protocol/requests/alterUserScramCredentials/fixtures/java/scramHi-' + digest + '.json'
    )
    expect(fs.existsSync(file)).toBe(true)
    const vector = JSON.parse(fs.readFileSync(file, 'utf8'))
    const { DIGESTS } = require('../../broker/saslAuthenticator/scram')
    const actual = await SCRAM.hi(
      vector.password,
      Buffer.from(vector.saltHex, 'hex'),
      vector.iterations,
      digest === 'sha256' ? DIGESTS.SHA256 : DIGESTS.SHA512
    )
    expect(actual.toString('hex')).toBe(vector.hex)
  })
  test('default salt is random 32 bytes and only unresolved users relocate on code 41', async () => {
    const attempts = []
    const salts = []
    const first = {
      alterUserScramCredentials: async (fields) => {
        attempts.push(fields.upsertions.map((x) => x.name))
        salts.push(...fields.upsertions.map((x) => Buffer.from(x.salt)))
        return {
          results: [
            { user: 'a', errorCode: 0 },
            { user: 'b', errorCode: 41 },
          ],
        }
      },
    }
    const second = {
      alterUserScramCredentials: async (fields) => {
        attempts.push(fields.upsertions.map((x) => x.name))
        return { results: [{ user: 'b', errorCode: 0 }] }
      },
    }
    const cluster = {
      refreshMetadata: jest.fn(async () => {}),
      findControllerBroker: jest.fn().mockResolvedValueOnce(first).mockResolvedValueOnce(second),
    }
    const hi = jest.spyOn(SCRAM, 'hi')
    const rows = await createAdmin({ cluster, logger, retry }).alterUserScramCredentials({
      upsertions: [change('a', { salt: undefined }), change('b', { salt: undefined })],
    })
    expect(rows.map((x) => x.errorCode)).toEqual([0, 0])
    expect(attempts).toEqual([['a', 'b'], ['b']])
    expect(cluster.refreshMetadata).toHaveBeenCalledTimes(2)
    expect(cluster.findControllerBroker).toHaveBeenCalledTimes(2)
    expect(hi).toHaveBeenCalledTimes(2)
    expect(salts.map((x) => x.length)).toEqual([32, 32])
    expect(salts[0].equals(salts[1])).toBe(false)
  })
  test('several mechanisms for one user produce one result and terminal errors remain data', async () => {
    const broker = {
      alterUserScramCredentials: jest.fn(async () => ({
        results: [
          { user: 'a', errorCode: 29, errorMessage: password },
          { user: 'b', errorCode: 0 },
        ],
      })),
    }
    const cluster = { refreshMetadata: async () => {}, findControllerBroker: async () => broker }
    const rows = await createAdmin({ cluster, logger, retry }).alterUserScramCredentials({
      upsertions: [change('a'), change('a', { mechanism: 'SCRAM-SHA-512' })],
      deletions: [{ user: 'b', mechanism: 'SCRAM-SHA-256' }],
    })
    expect(rows.map((x) => x.user)).toEqual(['a', 'b'])
    expect(rows[0]).toMatchObject({ errorCode: 29, error: { code: 29 }, errorMessage: null })
    expect(broker.alterUserScramCredentials).toHaveBeenCalledTimes(1)
  })
  test.each(['missing', 'duplicate', 'foreign'])('rejects %s describe identities', async (kind) => {
    const row = { user: kind === 'foreign' ? 'other' : 'a', errorCode: 0, credentialInfos: [] }
    const broker = {
      describeUserScramCredentials: async () => ({
        results: kind === 'missing' ? [] : kind === 'duplicate' ? [row, row] : [row],
      }),
    }
    await expect(
      createAdmin({
        cluster: { findConnectedBroker: async () => broker },
        logger,
        retry: { retries: 0 },
      }).describeUserScramCredentials({ users: ['a'] })
    ).rejects.toBeInstanceOf(KafkaJSInvariantViolation)
  })
  test('empty user filters do no work; null and omitted select all', async () => {
    const broker = { describeUserScramCredentials: jest.fn(async () => ({ results: [] })) }
    const findConnectedBroker = jest.fn(async () => broker)
    const admin = createAdmin({ cluster: { findConnectedBroker }, logger })
    await expect(admin.describeUserScramCredentials({ users: [] })).resolves.toEqual([])
    expect(findConnectedBroker).not.toHaveBeenCalled()
    await admin.describeUserScramCredentials()
    await admin.describeUserScramCredentials({ users: null })
    expect(broker.describeUserScramCredentials.mock.calls.map(([fields]) => fields.users)).toEqual([
      null,
      null,
    ])
  })
  test.each([4096, 16384])(
    'accepts the iteration boundary %s and salt boundary lengths',
    async (iterations) => {
      const broker = {
        alterUserScramCredentials: async () => ({
          results: [
            { user: 'a', errorCode: 0 },
            { user: 'b', errorCode: 0 },
          ],
        }),
      }
      const admin = createAdmin({
        cluster: { refreshMetadata: async () => {}, findControllerBroker: async () => broker },
        logger,
      })
      await expect(
        admin.alterUserScramCredentials({
          upsertions: [
            change('a', { iterations, salt: Buffer.alloc(1) }),
            change('b', { iterations, salt: Buffer.alloc(1024) }),
          ],
        })
      ).resolves.toHaveLength(2)
    }
  )
  test.each([4095, 16385, NaN, Infinity, 1.1, '4096', null])(
    'rejects invalid iterations before Hi or routing: %p',
    async (iterations) => {
      const hi = jest.spyOn(SCRAM, 'hi')
      const findControllerBroker = jest.fn(async () => ({
        alterUserScramCredentials: async () => ({
          results: [
            { user: 'a', errorCode: 0 },
            { user: 'b', errorCode: 0 },
          ],
        }),
      }))
      const admin = createAdmin({
        cluster: { findControllerBroker, refreshMetadata: async () => {} },
        logger,
      })
      const error = await admin
        .alterUserScramCredentials({ upsertions: [change('a'), change('b', { iterations })] })
        .catch((e) => e)
      expect(error).toBeInstanceOf(KafkaJSInvalidConfigurationError)
      expect(error.message).not.toContain(password)
      expect(hi).not.toHaveBeenCalled()
      expect(findControllerBroker).not.toHaveBeenCalled()
    }
  )
  test.each([
    {},
    { upsertions: [] },
    { upsertions: [change('a', { password: '' })] },
    { upsertions: [change('a', { mechanism: 'PLAIN' })] },
    ...[Buffer.alloc(0), Buffer.alloc(1025), 'salt', null].map((salt) => ({
      upsertions: [change('a', { salt })],
    })),
    { upsertions: [change('a'), change('a')] },
    { upsertions: [change('a')], deletions: [{ user: 'a', mechanism: 'SCRAM-SHA-256' }] },
    { deletions: [{ user: '', mechanism: 'SCRAM-SHA-256' }] },
    { upsertions: new Array(1) },
    null,
  ])('rejects invalid credential changes without derivation: %p', async (options) => {
    const hi = jest.spyOn(SCRAM, 'hi')
    const refreshMetadata = jest.fn()
    await expect(
      createAdmin({
        cluster: {
          refreshMetadata,
          findControllerBroker: async () => ({
            alterUserScramCredentials: async () => ({ results: [{ user: 'a', errorCode: 0 }] }),
          }),
        },
        logger,
      }).alterUserScramCredentials(options)
    ).rejects.toBeInstanceOf(KafkaJSInvalidConfigurationError)
    expect(hi).not.toHaveBeenCalled()
    expect(refreshMetadata).not.toHaveBeenCalled()
  })
  test('derivation failures discard unsafe cause text', async () => {
    jest.spyOn(SCRAM, 'hi').mockRejectedValue(new Error(password + ' ' + salt.toString()))
    const error = await createAdmin({ cluster: {}, logger })
      .alterUserScramCredentials({ upsertions: [change()] })
      .catch((e) => e)
    expect(error.message).toBe('SCRAM credential derivation failed')
    expect(error.cause).toBeUndefined()
    expect(inspect(error, { depth: null })).not.toContain(password)
  })
  test('a late derivation after expiry starts no routing and erases its result', async () => {
    jest.useFakeTimers()
    let release
    const pending = new Promise((resolve) => {
      release = resolve
    })
    jest.spyOn(SCRAM, 'hi').mockReturnValue(pending)
    const refreshMetadata = jest.fn()
    const derived = Buffer.from('derived-marker-only-test')
    try {
      const admin = createAdmin({
        cluster: {
          refreshMetadata,
          findControllerBroker: async () => ({
            alterUserScramCredentials: async () => ({ results: [{ user: 'a', errorCode: 0 }] }),
          }),
        },
        logger,
      })
      const result = admin
        .alterUserScramCredentials({ upsertions: [change()], apiTimeout: 5 })
        .catch((e) => e)
      await jest.advanceTimersByTimeAsync(5)
      expect(await result).toBeInstanceOf(KafkaJSTimeout)
      release(derived)
      for (let i = 0; i < 20; i++) await Promise.resolve()
      expect(derived.every((x) => x === 0)).toBe(true)
      expect(refreshMetadata).not.toHaveBeenCalled()
      expect(jest.getTimerCount()).toBe(0)
    } finally {
      release(derived)
      jest.useRealTimers()
    }
  })
})

describe.each(['0', '1'])('SCRAM broker diagnostics buffers %s', (buffers) => {
  test('wire derivation never sends plaintext and broker echoes reach neither logs nor result diagnostics', async () => {
    const previous = process.env.KAFKAJS_DEBUG_PROTOCOL_BUFFERS
    process.env.KAFKAJS_DEBUG_PROTOCOL_BUFFERS = buffers
    const entries = []
    const marker = 'derived-marker-only-test'
    const spy = jest.spyOn(SCRAM, 'hi').mockResolvedValue(Buffer.from(marker))
    const scripted = createScriptedCluster({
      nodes: [{ id: 0, host: 'node0', port: 9092 }],
      profile: 'securityAdmin',
    })
      .answer('AlterUserScramCredentials', () => ({
        results: [
          {
            user: 'test-user',
            errorCode: 29,
            errorMessage: password + ' ' + salt.toString() + ' ' + marker,
          },
        ],
      }))
      .answer('DescribeUserScramCredentials', () => ({
        results: [
          {
            user: 'test-user',
            errorCode: 0,
            errorMessage: password,
            credentialInfos: [{ mechanism: 1, iterations: 4096 }],
          },
        ],
      }))
    const admin = new Kafka(
      scripted.kafkaOptions({
        retry: { retries: 0 },
        logLevel: logLevel.DEBUG,
        logCreator: () => (entry) => entries.push(entry),
      })
    ).admin()
    try {
      await admin.connect()
      const changed = await admin.alterUserScramCredentials({ upsertions: [change()] })
      expect(changed[0]).toMatchObject({ errorCode: 29, errorMessage: null, error: { code: 29 } })
      const rows = await admin.describeUserScramCredentials({ users: ['test-user'] })
      expect(rows[0].credentials).toEqual([{ mechanism: 'SCRAM-SHA-256', iterations: 4096 }])
      expect(rows[0].errorMessage).toBeNull()
      const wire = scripted.requests().find((r) => r.api === 'AlterUserScramCredentials').fields
        .upsertions[0]
      expect(wire.saltedPassword.toString()).toBe(marker)
      expect(wire.salt).toEqual(salt)
      expect(wire).not.toHaveProperty('password')
      const text = inspect({ entries, rows, changed }, { depth: null, maxArrayLength: null })
      for (const secret of [password, salt.toString(), marker]) expect(text).not.toContain(secret)
    } finally {
      await admin.disconnect()
      scripted.assertClean()
      spy.mockRestore()
      if (previous === undefined) delete process.env.KAFKAJS_DEBUG_PROTOCOL_BUFFERS
      else process.env.KAFKAJS_DEBUG_PROTOCOL_BUFFERS = previous
    }
  })
})

describe('SCRAM request capabilities and read retry', () => {
  const { KafkaJSServerDoesNotSupportApiKey, KafkaJSConnectionError } = require('../../errors')
  test.each(['DescribeUserScramCredentials', 'AlterUserScramCredentials'])(
    '%s rejects an unimplemented range without sending',
    async (api) => {
      const scripted = createScriptedCluster({
        nodes: [{ id: 0, host: 'node0', port: 9092 }],
        profile: 'securityAdmin',
        advertise: { [api]: [1, 1] },
      })
      const admin = new Kafka(scripted.kafkaOptions({ retry: { retries: 0 } })).admin()
      try {
        await admin.connect()
        const result =
          api === 'DescribeUserScramCredentials'
            ? admin.describeUserScramCredentials({ users: ['test-user'] })
            : admin.alterUserScramCredentials({
                deletions: [{ user: 'test-user', mechanism: 'SCRAM-SHA-256' }],
              })
        await expect(result).rejects.toBeInstanceOf(KafkaJSServerDoesNotSupportApiKey)
        expect(scripted.requests().filter((r) => r.api === api)).toEqual([])
      } finally {
        await admin.disconnect()
        scripted.assertClean()
      }
    }
  )
  test.each([undefined, ['a', 'b']])(
    'read retries only pending users, retaining caller order for %p',
    async (users) => {
      const requests = []
      const broker = {
        describeUserScramCredentials: async (fields) => {
          requests.push(fields.users)
          return {
            results:
              requests.length === 1
                ? [
                    { user: 'a', errorCode: 0, credentialInfos: [] },
                    { user: 'b', errorCode: 7, credentialInfos: [] },
                  ]
                : [{ user: 'b', errorCode: 0, credentialInfos: [] }],
          }
        },
      }
      const rows = await createAdmin({
        cluster: { findConnectedBroker: async () => broker },
        logger,
        retry,
      }).describeUserScramCredentials({ users })
      expect(rows.map((row) => row.user)).toEqual(['a', 'b'])
      expect(rows.map((row) => row.errorCode)).toEqual([0, 0])
      expect(requests[1]).toEqual([{ name: 'b' }])
      expect(requests).toHaveLength(2)
    }
  )
  test('a late routing failure preserves a completed SCRAM user', async () => {
    const local = new KafkaJSConnectionError('offline')
    const first = {
      alterUserScramCredentials: async () => ({
        results: [
          { user: 'a', errorCode: 0 },
          { user: 'b', errorCode: 7 },
        ],
      }),
    }
    const findControllerBroker = jest
      .fn(async () => ({
        alterUserScramCredentials: async () => ({
          results: [
            { user: 'a', errorCode: 0 },
            { user: 'b', errorCode: 0 },
          ],
        }),
      }))
      .mockResolvedValueOnce(first)
      .mockRejectedValue(local)
    const rows = await createAdmin({
      cluster: { refreshMetadata: async () => {}, findControllerBroker },
      logger,
      retry,
    }).alterUserScramCredentials({
      deletions: [
        { user: 'a', mechanism: 'SCRAM-SHA-256' },
        { user: 'b', mechanism: 'SCRAM-SHA-256' },
      ],
    })
    expect(rows[0]).toMatchObject({ errorCode: 0, error: null })
    expect(rows[1].errorCode).toBe(-1)
    expect(rows[1].error).toBe(local)
  })
})
