const Encoder = require('../encoder')
const { KafkaJSMemberIdRequired } = require('../../errors')

const joinGroup = {
  6: require('./joinGroup/v6/response'),
  7: require('./joinGroup/v7/response'),
  8: require('./joinGroup/v8/response'),
}
const syncGroup = { 4: require('./syncGroup/v4/response'), 5: require('./syncGroup/v5/response') }
const heartbeat = { 4: require('./heartbeat/v4/response') }
const leaveGroup = {
  4: require('./leaveGroup/v4/response'),
  5: require('./leaveGroup/v5/response'),
}

const CODECS = [
  ['JoinGroup', 6, joinGroup, ['ok']],
  ['JoinGroup', 7, joinGroup, ['ok', 'protocolType', 'nullProtocolName']],
  ['JoinGroup', 8, joinGroup, ['ok', 'protocolType', 'nullProtocolName']],
  ['SyncGroup', 4, syncGroup, ['ok']],
  ['SyncGroup', 5, syncGroup, ['ok', 'protocol']],
  ['Heartbeat', 4, heartbeat, ['ok', 'error']],
  ['LeaveGroup', 4, leaveGroup, ['ok', 'memberError']],
  ['LeaveGroup', 5, leaveGroup, ['ok', 'memberError']],
]

const vector = (api, version, caseId) => {
  const name = `${api[0].toLowerCase()}${api.slice(1)}`
  return require(`./${name}/fixtures/java/${name}Response-v${version}-${caseId}.json`)
}

/** What a decoder returns for a vector: its fields, the throttle split and the byte fields as Buffers. */
const expectedOf = ({ fields }) => {
  const convert = (value) => {
    if (Array.isArray(value)) return value.map(convert)
    if (value === null || typeof value !== 'object') return value

    return Object.fromEntries(
      Object.entries(value).map(([key, field]) => [
        key,
        ['memberMetadata', 'memberAssignment'].includes(key)
          ? Buffer.from(field, 'hex')
          : convert(field),
      ])
    )
  }

  return { ...convert(fields), throttleTime: 0, clientSideThrottleTime: fields.throttleTime }
}

/** The unsupported-version answer a broker gives: the throttle time and code 35, nothing after it. */
const UNSUPPORTED = new Encoder().writeInt32(0).writeInt16(35).buffer

describe('Protocol > Requests > the classic group response codecs', () => {
  describe.each(CODECS)('%s v%i', (api, version, codecs, cases) => {
    const { decode } = codecs[version]

    test.each(cases)('decodes the Apache 4.3.1 %s vector', async (caseId) => {
      const { hex, fields } = vector(api, version, caseId)

      expect(await decode(Buffer.from(hex, 'hex'))).toEqual(expectedOf({ fields }))
    })

    test('fails with a typed error, never a native one, on every truncation of every vector', async () => {
      for (const caseId of cases) {
        const buffer = Buffer.from(vector(api, version, caseId).hex, 'hex')

        for (let length = 0; length < buffer.length; length++) {
          await expect(decode(buffer.subarray(0, length))).rejects.toThrow(
            expect.objectContaining({ name: expect.stringMatching(/^KafkaJS/) })
          )
        }
      }
    })

    test('answers UNSUPPORTED_VERSION before it reads anything else', async () => {
      await expect(decode(UNSUPPORTED)).rejects.toMatchObject({ type: 'UNSUPPORTED_VERSION' })
    })
  })

  describe('JoinGroup', () => {
    /** A body for versions 6 and 7 built from the 4.3.1 definition, for the cases no vector covers. */
    const body = ({
      version = 6,
      errorCode = 0,
      protocolType = 'consumer',
      name = 'p',
      leader = 'l',
      members = [],
    } = {}) => {
      const encoder = new Encoder().writeInt32(25).writeInt16(errorCode).writeInt32(-1)
      if (version >= 7) encoder.writeUVarIntString(protocolType)

      return encoder
        .writeUVarIntString(name)
        .writeUVarIntString(leader)
        .writeUVarIntString('m')
        .writeUVarIntArray(members)
        .writeTaggedFields().buffer
    }
    const member = (metadata) =>
      new Encoder()
        .writeUVarIntString('x')
        .writeUVarIntString(null)
        .writeUVarIntBytes(metadata)
        .writeTaggedFields()

    test.each([6, 7, 8])(
      'v%i hands MEMBER_ID_REQUIRED to parse with the id the coordinator made',
      async (version) => {
        const data = await joinGroup[version].decode(body({ version, errorCode: 79 }))

        await expect(joinGroup[version].parse(data)).rejects.toThrow(KafkaJSMemberIdRequired)
        await expect(joinGroup[version].parse(data)).rejects.toMatchObject({ memberId: 'm' })
      }
    )

    test('v6 reports a protocol type no version-6 body carries as absent, and v7 reports null and set ones', async () => {
      const [v6, null7, set7] = await Promise.all([
        joinGroup[6].decode(body()),
        joinGroup[7].decode(body({ version: 7, protocolType: null })),
        joinGroup[7].decode(body({ version: 7, protocolType: 'connect' })),
      ])

      expect('protocolType' in v6).toBe(false)
      expect(null7.protocolType).toBeNull()
      expect(set7.protocolType).toEqual('connect')
    })

    test('keeps an empty leader and protocol name empty, and a null protocol name null', async () => {
      expect(await joinGroup[7].decode(body({ version: 7, leader: '', name: '' }))).toMatchObject({
        leaderId: '',
        groupProtocol: '',
      })
      expect(await joinGroup[7].decode(body({ version: 7, name: null }))).toMatchObject({
        groupProtocol: null,
      })
    })

    test('reads a compact null member list as empty and null metadata as an empty buffer', async () => {
      const nullMembers = new Encoder()
        .writeInt32(0)
        .writeInt16(0)
        .writeInt32(1)
        .writeUVarIntString('p')
        .writeUVarIntString('l')
        .writeUVarIntString('m')
        .writeUVarIntArray(null)
        .writeTaggedFields().buffer

      expect((await joinGroup[6].decode(nullMembers)).members).toEqual([])
      expect((await joinGroup[6].decode(body({ members: [member(null)] }))).members).toEqual([
        { memberId: 'x', groupInstanceId: null, memberMetadata: Buffer.alloc(0) },
      ])
    })

    test("v8 is v7's codec", () => {
      expect(joinGroup[8].decode).toBe(joinGroup[7].decode)
      expect(joinGroup[8].parse).toBe(joinGroup[7].parse)
    })
  })

  describe('SyncGroup', () => {
    const body = (
      version,
      { errorCode = 0, assignment = Buffer.from('ab'), type = null, name = null } = {}
    ) => {
      const encoder = new Encoder().writeInt32(25).writeInt16(errorCode)
      if (version >= 5) encoder.writeUVarIntString(type).writeUVarIntString(name)

      return encoder.writeUVarIntBytes(assignment).writeTaggedFields().buffer
    }

    test.each([4, 5])(
      'v%i hands a coordinator error to parse, and returns a good answer',
      async (version) => {
        const good = await syncGroup[version].decode(body(version))

        await expect(syncGroup[version].parse(good)).resolves.toBe(good)
        await expect(
          syncGroup[version].parse(
            await syncGroup[version].decode(body(version, { errorCode: 27 }))
          )
        ).rejects.toMatchObject({ type: 'REBALANCE_IN_PROGRESS' })
      }
    )

    test.each([4, 5])(
      'v%i reads a compact null assignment as an empty buffer, which MemberAssignment reads as none',
      async (version) => {
        expect(
          (await syncGroup[version].decode(body(version, { assignment: null }))).memberAssignment
        ).toEqual(Buffer.alloc(0))
      }
    )

    test('v5 decodes protocol fields that differ from what was sent as they are, and keeps an empty one empty', async () => {
      expect(await syncGroup[5].decode(body(5, { type: 'connect', name: '' }))).toMatchObject({
        protocolType: 'connect',
        protocolName: '',
      })
    })

    test('v4 reports no protocol fields', async () => {
      expect(Object.keys(await syncGroup[4].decode(body(4))).sort()).toEqual([
        'clientSideThrottleTime',
        'errorCode',
        'memberAssignment',
        'throttleTime',
      ])
    })
  })

  describe('Heartbeat', () => {
    test('parses a good answer and throws a coordinator error', async () => {
      const good = await heartbeat[4].decode(Buffer.from(vector('Heartbeat', 4, 'ok').hex, 'hex'))
      const bad = await heartbeat[4].decode(Buffer.from(vector('Heartbeat', 4, 'error').hex, 'hex'))

      await expect(heartbeat[4].parse(good)).resolves.toBe(good)
      await expect(heartbeat[4].parse(bad)).rejects.toMatchObject({ type: 'REBALANCE_IN_PROGRESS' })
    })
  })

  describe('LeaveGroup', () => {
    const body = (errorCode, members) =>
      new Encoder()
        .writeInt32(25)
        .writeInt16(errorCode)
        .writeUVarIntArray(members)
        .writeTaggedFields().buffer
    const memberOf = (code) =>
      new Encoder()
        .writeUVarIntString('m')
        .writeUVarIntString(null)
        .writeInt16(code)
        .writeTaggedFields()

    test('parse, the single-member form, returns a good answer and throws the first member error', async () => {
      const { parse, decode } = leaveGroup[4]
      const good = await decode(Buffer.from(vector('LeaveGroup', 4, 'ok').hex, 'hex'))

      await expect(parse(good)).resolves.toBe(good)
      await expect(
        parse(await decode(Buffer.from(vector('LeaveGroup', 4, 'memberError').hex, 'hex')))
      ).rejects.toMatchObject({ type: 'UNKNOWN_MEMBER_ID' })
    })

    test('parse throws a top-level error before any member error', async () => {
      const data = await leaveGroup[4].decode(body(16, [memberOf(25)]))

      await expect(leaveGroup[4].parse(data)).rejects.toMatchObject({
        type: 'NOT_COORDINATOR_FOR_GROUP',
      })
    })

    test("parseTopLevel, the batch form, returns every member's own code and throws only a top-level error", async () => {
      const { decode, parseTopLevel } = leaveGroup[4]
      const data = await decode(Buffer.from(vector('LeaveGroup', 4, 'memberError').hex, 'hex'))

      await expect(parseTopLevel(data)).resolves.toBe(data)
      expect(data.members.map(({ errorCode }) => errorCode)).toEqual([0, 25])
      await expect(parseTopLevel(await decode(body(15, [])))).rejects.toMatchObject({
        type: 'GROUP_COORDINATOR_NOT_AVAILABLE',
      })
    })

    test('reads a compact null member list as empty, never as null', async () => {
      expect((await leaveGroup[4].decode(body(0, null))).members).toEqual([])
    })

    test("v5 is v4's codec", () => {
      expect(leaveGroup[5].decode).toBe(leaveGroup[4].decode)
      expect(leaveGroup[5].parse).toBe(leaveGroup[4].parse)
      expect(leaveGroup[5].parseTopLevel).toBe(leaveGroup[4].parseTopLevel)
    })
  })
})
