const joinGroup = {
  6: require('./joinGroup/v6/request'),
  7: require('./joinGroup/v7/request'),
  8: require('./joinGroup/v8/request'),
}
const syncGroup = { 4: require('./syncGroup/v4/request'), 5: require('./syncGroup/v5/request') }
const heartbeat = { 4: require('./heartbeat/v4/request') }
const leaveGroup = { 4: require('./leaveGroup/v4/request'), 5: require('./leaveGroup/v5/request') }

const CODECS = [
  ['JoinGroup', 6, joinGroup, ['basic', 'staticMember']],
  ['JoinGroup', 7, joinGroup, ['basic', 'staticMember']],
  ['JoinGroup', 8, joinGroup, ['basic', 'staticMember', 'withReason']],
  ['SyncGroup', 4, syncGroup, ['assignment']],
  ['SyncGroup', 5, syncGroup, ['assignment', 'withProtocol']],
  ['Heartbeat', 4, heartbeat, ['basic']],
  ['LeaveGroup', 4, leaveGroup, ['oneMember', 'twoMembers']],
  ['LeaveGroup', 5, leaveGroup, ['oneMember', 'twoMembers', 'withReason']],
]

const vector = (api, version, caseId) =>
  require(
    `./${api[0].toLowerCase()}${api.slice(1)}/fixtures/java/${api[0].toLowerCase()}${api.slice(1)}Request-v${version}-${caseId}.json`
  )

/** A vector's builder arguments, each hex-encoded byte field turned into the Buffer a caller passes. */
const optionsOf = (value) => {
  if (Array.isArray(value)) return value.map(optionsOf)
  if (value === null || typeof value !== 'object') return value
  return Object.fromEntries(
    Object.entries(value).map(([key, field]) => [
      key,
      ['metadata', 'memberAssignment'].includes(key) ? Buffer.from(field, 'hex') : optionsOf(field),
    ])
  )
}

/** A compact string as it sits in a flexible body: the length plus one, then the bytes. */
const compact = (text) =>
  Buffer.concat([Buffer.from([text.length + 1]), Buffer.from(text)]).toString('hex')
const hexOf = async (request) => (await request.encode()).buffer.toString('hex')
const build = (codecs, version, fields) => codecs[version](optionsOf(fields))

describe('Protocol > Requests > the classic group codecs', () => {
  describe.each(CODECS)('%s v%i', (api, version, codecs, cases) => {
    test.each(cases)('encodes the Apache 4.3.1 %s vector byte for byte', async (caseId) => {
      const { hex, fields } = vector(api, version, caseId)

      expect(await hexOf(build(codecs, version, fields))).toEqual(hex)
    })
  })

  describe.each([
    ['SyncGroup', 4, syncGroup, 'assignment'],
    ['SyncGroup', 5, syncGroup, 'withProtocol'],
    ['Heartbeat', 4, heartbeat, 'basic'],
  ])('%s v%i', (api, version, codecs, caseId) => {
    test('writes a dynamic member, which has no instance id, as the null marker (00)', async () => {
      // No Apache vector has one and it is what every default consumer sends; the vector with its
      // instance id swapped for the marker is the bytes the codec must write.
      const { hex, fields } = vector(api, version, caseId)
      const expected = hex.replace(compact('test-instance'), '00')

      expect(expected).not.toEqual(hex)
      for (const groupInstanceId of [null, undefined]) {
        expect(await hexOf(build(codecs, version, { ...fields, groupInstanceId }))).toEqual(
          expected
        )
      }
    })
  })

  describe('JoinGroup', () => {
    const { fields } = vector('JoinGroup', 6, 'basic')

    test('v6 writes an omitted instance id as null and a protocol without metadata as empty bytes', async () => {
      const { groupInstanceId, ...omitted } = fields
      const bare = { ...omitted, groupProtocols: [{ name: 'a' }] }

      expect(groupInstanceId).toBeNull()
      expect(await hexOf(joinGroup[6](optionsOf(omitted)))).toEqual(
        vector('JoinGroup', 6, 'basic').hex
      )
      expect((await hexOf(joinGroup[6](bare))).endsWith('020261' + '01' + '00' + '00')).toBe(true)
    })

    test('v6 exposes its body without the final tag buffer, for v8 to extend', async () => {
      const options = optionsOf(fields)

      expect(joinGroup[6].body(options).buffer.length + 1).toEqual(
        (await joinGroup[6](options).encode()).buffer.length
      )
    })

    test("v7 sends v6's bytes, and v8 sends v7's bytes plus a null reason before the tag buffer", async () => {
      const options = optionsOf(fields)
      const v7 = await hexOf(joinGroup[7](options))

      expect(v7).toEqual(await hexOf(joinGroup[6](options)))
      expect(await hexOf(joinGroup[8](options))).toEqual(`${v7.slice(0, -2)}0000`)
    })
  })

  describe('LeaveGroup', () => {
    const { fields } = vector('LeaveGroup', 5, 'twoMembers')
    const reasons = {
      ...fields,
      members: fields.members.map((member) => ({
        ...member,
        reason: 'the consumer is being closed',
      })),
    }

    test('v4 never writes a reason, which only v5 has', async () => {
      expect(await hexOf(leaveGroup[4](reasons))).toEqual(vector('LeaveGroup', 4, 'twoMembers').hex)
    })

    test('v5 writes an absent reason as null, the same bytes as an explicit null', async () => {
      const bare = {
        ...fields,
        members: fields.members.map(({ memberId, groupInstanceId }) => ({
          memberId,
          groupInstanceId,
        })),
      }

      expect(await hexOf(leaveGroup[5](bare))).toEqual(vector('LeaveGroup', 5, 'twoMembers').hex)
    })

    test('removes a static member by its instance id alone: an empty member id, the id and a reason', async () => {
      const members = [{ memberId: '', groupInstanceId: 'test-instance', reason: 'x' }]
      const options = { groupId: 'test-group', members }

      expect(await hexOf(leaveGroup[5](options))).toEqual(
        compact('test-group') + '02' + '01' + compact('test-instance') + '02' + '78' + '00' + '00'
      )
      expect(await hexOf(leaveGroup[4](options))).toEqual(
        compact('test-group') + '02' + '01' + compact('test-instance') + '00' + '00'
      )
    })

    test('writes an absent instance id as the null marker and an empty member id as empty', async () => {
      expect(
        await hexOf(leaveGroup[4]({ groupId: 'test-group', members: [{ memberId: '' }] }))
      ).toEqual(compact('test-group') + '02' + '01' + '00' + '00' + '00')
    })
  })
})
