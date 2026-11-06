const fs = require('fs')
const path = require('path')
const { isDeepStrictEqual } = require('util')
const createAdmin = require('../index')
const errors = require('../../errors')
const { newLogger } = require('testHelpers')

/** Where the authentic live capture is committed; it is never written by hand. */
const FIXTURE_FILE = path.join(__dirname, 'fixtures', 'describeGroupsShareGroup.json')
const FIXTURE_PATH = 'src/admin/__tests__/fixtures/describeGroupsShareGroup.json'
const STAGES = ['member-present', 'empty']
const GROUP_ALIAS = 'share-group'
const CAPTURE_FIELDS = [
  'brokerVersion',
  'observedImageId',
  'tree',
  'node',
  'stage',
  'groupAlias',
  'wireVersions',
  'trace',
  'rawDescribeGroups',
  'publicOutcome',
]
const COORDINATOR_METHODS = new Set(['consumerGroupDescribe', 'describeGroups'])
const retry = { retries: 1, initialRetryTime: 1, maxRetryTime: 2 }

/**
 * @param {unknown} value
 * @returns {unknown} a JSON-safe copy: buffers become `{ $buffer: hex }`, errors their serialized
 *   form, and `undefined` properties stay absent
 * @throws {Error} for a value with no faithful JSON form (bigint, function, symbol)
 */
const serialize = (value) => {
  if (value === null || ['string', 'number', 'boolean'].includes(typeof value)) return value
  if (Buffer.isBuffer(value)) return { $buffer: value.toString('hex') }
  if (value instanceof Error) return serializeError(value)
  if (Array.isArray(value)) return value.map((item) => serialize(item) ?? null)
  if (typeof value === 'object') {
    const copy = {}
    for (const [key, item] of Object.entries(value)) {
      const serialized = serialize(item)
      if (serialized !== undefined) copy[key] = serialized
    }
    return copy
  }
  if (value === undefined) return undefined
  throw new Error(`A ${typeof value} has no capture form`)
}

/**
 * @param {Error} error
 * @returns {object} its name, message and every own property, the stack left out
 */
const serializeError = (error) => {
  const copy = { name: error.name, message: error.message }
  for (const key of Object.getOwnPropertyNames(error)) {
    if (['stack', 'name', 'message'].includes(key)) continue
    const serialized = serialize(error[key])
    if (serialized !== undefined) copy[key] = serialized
  }
  return copy
}

/**
 * The inverse of `serialize` for values: `{ $buffer: hex }` becomes a Buffer again.
 * @param {unknown} value a serialized capture value
 * @returns {unknown} the value with its buffers restored, as a fresh copy
 */
const revive = (value) => {
  if (Array.isArray(value)) return value.map(revive)
  if (value !== null && typeof value === 'object') {
    const keys = Object.keys(value)
    if (keys.length === 1 && keys[0] === '$buffer') return Buffer.from(value.$buffer, 'hex')
    return Object.fromEntries(keys.map((key) => [key, revive(value[key])]))
  }
  return value
}

/**
 * Rebuilds an error of the captured class with its captured fields and cause, without running its
 * constructor, so the admin sees the same `instanceof`, name, code and cause chain.
 * @param {{ name: string, message: string, cause?: object }} captured a serialized error
 * @returns {Error}
 */
const reviveError = ({ name, message, cause, ...fields }) => {
  const Type = errors[name] ?? Error
  const error = Object.create(Type.prototype)
  Object.assign(error, { name, message, stack: `${name}: ${message}` }, revive(fields))
  if (cause) error.cause = reviveError(cause)
  return error
}

/**
 * @param {{ groupIds: string[], includeAuthorizedOperations: boolean }} request what the admin sent
 * @returns {object} the two request fields a capture records, in capture form
 */
const requestOf = ({ groupIds, includeAuthorizedOperations }) =>
  serialize({ groupIds, includeAuthorizedOperations })

/**
 * Plays one capture's trace through the real admin: a stub coordinator answers each call with the
 * next recorded step, in order, after requiring the same method and request, and a method the
 * trace does not call at that position fails at once instead of inventing an answer.
 * @param {{ trace: object[] }} capture
 * @returns {Promise<{ outcome: object, raw: unknown, consumed: number, unrecorded: string[] }>} the
 *   public outcome in capture form, the unserialized result, how many steps were used, and every
 *   call the trace did not record
 */
const play = async (capture) => {
  const trace = revive(capture.trace)
  const state = { consumed: 0, unrecorded: [] }
  const answer = (method) => async (request) => {
    const step = trace[state.consumed]
    if (!step || step.method !== method) {
      state.unrecorded.push(`${method} at step ${state.consumed}`)
      throw new Error(`Unrecorded ${method} call at step ${state.consumed}`)
    }
    if (!isDeepStrictEqual(requestOf(request), capture.trace[state.consumed].request)) {
      state.unrecorded.push(`${method} with another request at step ${state.consumed}`)
      throw new Error(`Unrecorded ${method} request at step ${state.consumed}`)
    }
    state.consumed += 1
    if (step.outcome.kind === 'throw') throw reviveError(step.outcome.error)
    return step.outcome.value
  }
  const coordinator = { nodeId: 0 }
  for (const method of COORDINATOR_METHODS) coordinator[method] = answer(method)
  const cluster = {
    refreshMetadata: async () => {},
    brokerPool: { brokers: { 0: coordinator } },
    findBroker: async () => coordinator,
    findGroupCoordinator: async () => coordinator,
  }
  const admin = createAdmin({ logger: newLogger(), retry, cluster })
  let outcome
  let raw
  try {
    raw = await admin.describeGroups([GROUP_ALIAS])
    outcome = { kind: 'return', value: serialize(raw) }
  } catch (error) {
    outcome = { kind: 'throw', error: serializeError(error) }
  }

  return { outcome, raw, consumed: state.consumed, unrecorded: state.unrecorded }
}

/**
 * Replays a capture and requires the whole trace used once, no unrecorded call, the captured
 * trace left unchanged and the public outcome equal to the captured one.
 * @param {{ trace: object[], publicOutcome: object }} capture
 * @returns {Promise<{ raw: unknown }>} the unserialized public result, for freshness checks
 * @throws {Error} naming the first thing that differs
 */
const verifyReplay = async (capture) => {
  const before = JSON.stringify(capture.trace)
  const { outcome, raw, consumed, unrecorded } = await play(capture)
  if (unrecorded.length > 0) throw new Error(`Unrecorded call: ${unrecorded.join('; ')}`)
  if (consumed !== capture.trace.length) {
    throw new Error(`The trace was not used up: ${consumed} of ${capture.trace.length} steps`)
  }
  if (JSON.stringify(capture.trace) !== before) throw new Error('The captured trace changed')
  if (!isDeepStrictEqual(outcome, capture.publicOutcome)) {
    throw new Error('The public outcome differs from the capture')
  }

  return { raw }
}

describe('Admin > describeGroups on a share group > captured legacy outcomes', () => {
  if (fs.existsSync(FIXTURE_FILE)) {
    const fixture = JSON.parse(fs.readFileSync(FIXTURE_FILE, 'utf8'))
    const rows = fixture.captures.map((capture) => [capture.brokerVersion, capture.stage, capture])

    it('holds one member-present and one empty capture per observed line, in the capture schema', () => {
      expect(fixture.schema).toBe(1)
      expect(rows.length).toBeGreaterThan(0)
      const seen = new Set()
      for (const [version, stage, capture] of rows) {
        for (const field of CAPTURE_FIELDS) expect(capture).toHaveProperty(field)
        expect(version).toMatch(/^\d+\.\d+\.\d+$/)
        expect(STAGES).toContain(stage)
        expect(capture.groupAlias).toBe(GROUP_ALIAS)
        expect(capture.node).toMatch(/^v\d+\./)
        expect(capture.trace.length).toBeGreaterThan(0)
        expect(['return', 'throw']).toContain(capture.publicOutcome.kind)
        expect(capture.rawDescribeGroups.method).toBe('describeGroups')
        expect(seen.has(`${version} ${stage}`)).toBe(false)
        seen.add(`${version} ${stage}`)
      }
      for (const [version] of rows) {
        for (const stage of STAGES) expect(seen.has(`${version} ${stage}`)).toBe(true)
      }
    })

    it.each(rows)(
      'replays the captured legacy share-group outcome on Kafka %s at %s',
      async (_version, _stage, capture) => {
        const { raw } = await verifyReplay(capture)
        if (raw?.groups?.[0]) {
          raw.groups[0].members = [{ memberId: 'mutated' }]
          raw.groups.push({ groupId: 'mutated' })
        }

        await verifyReplay(capture)
      }
    )
  } else {
    it.skip(`replays the captured legacy share-group outcome (skipped: ${FIXTURE_PATH} is absent until an authentic live capture is committed)`, () => {})
  }
})

describe('Admin > describeGroups on a share group > replay controls', () => {
  /** A capture of the classic fallback after a broker without ConsumerGroupDescribe, for the controls only. */
  const controlTrace = () => [
    {
      method: 'consumerGroupDescribe',
      apiKey: 69,
      request: { groupIds: [GROUP_ALIAS], includeAuthorizedOperations: false },
      outcome: {
        kind: 'throw',
        error: {
          name: 'KafkaJSServerDoesNotSupportApiKey',
          message: 'The Kafka server does not support the requested API version',
          apiKey: 69,
          apiName: 'ConsumerGroupDescribe',
        },
      },
    },
    {
      method: 'describeGroups',
      apiKey: 15,
      apiVersion: 6,
      request: { groupIds: [GROUP_ALIAS], includeAuthorizedOperations: false },
      outcome: {
        kind: 'return',
        value: {
          groups: [
            {
              errorCode: 69,
              errorMessage: 'The group id does not exist.',
              groupId: GROUP_ALIAS,
              state: 'Dead',
              protocolType: '',
              protocol: '',
              members: [],
              authorizedOperations: -2147483648,
            },
          ],
        },
      },
    },
  ]
  /** @returns {Promise<{ trace: object[], publicOutcome: object }>} a capture the replay accepts */
  const control = async () => {
    const trace = controlTrace()
    return { trace, publicOutcome: (await play({ trace })).outcome }
  }

  it('accepts an unmodified control capture', async () => {
    const accepted = verifyReplay(await control())

    await expect(accepted).resolves.toBeDefined()
  })

  it('fails when one captured error code changes', async () => {
    const capture = await control()
    capture.trace[1].outcome.value.groups[0].errorCode = 0
    const attempt = verifyReplay(capture)

    await expect(attempt).rejects.toThrow('The public outcome differs from the capture')
  })

  it('fails when an error code is rewritten into an alias string', async () => {
    const capture = await control()
    capture.trace[1].outcome.value.groups[0].errorCode = GROUP_ALIAS
    const attempt = verifyReplay(capture)

    await expect(attempt).rejects.toThrow(
      /The public outcome differs from the capture|Unrecorded call/
    )
  })

  it('fails on an unrecorded call when a trace entry is missing', async () => {
    const capture = await control()
    capture.trace.splice(1, 1)
    const attempt = verifyReplay(capture)

    await expect(attempt).rejects.toThrow('Unrecorded call: describeGroups at step 1')
  })

  it('fails when the trace holds an entry nothing consumes', async () => {
    const capture = await control()
    capture.trace.push({ ...capture.trace[1] })
    const attempt = verifyReplay(capture)

    await expect(attempt).rejects.toThrow('The trace was not used up')
  })

  it('fails when a recorded request is not the one made', async () => {
    const capture = await control()
    capture.trace[1].request.groupIds = ['another-group']
    const attempt = verifyReplay(capture)

    await expect(attempt).rejects.toThrow('Unrecorded call: describeGroups with another request')
  })

  it('restores buffer fields byte for byte through JSON', () => {
    const bytes = Buffer.from([0, 1, 127, 128, 255])
    const written = JSON.stringify(
      serialize({ members: [{ memberAssignment: bytes }], offset: '8' })
    )
    const read = revive(JSON.parse(written))

    expect(read.members[0].memberAssignment).toEqual(bytes)
    expect(Buffer.isBuffer(read.members[0].memberAssignment)).toBe(true)
    expect(read.offset).toBe('8')
  })

  it('hands every replay its own nested arrays', async () => {
    const capture = await control()
    const { raw } = await verifyReplay(capture)
    raw.groups[0].members.push({ memberId: 'mutated' })

    await expect(verifyReplay(capture)).resolves.toBeDefined()
  })
})
