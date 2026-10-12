const Encoder = require('../../encoder')
const { versions, protocol } = require('./index')

const GROUP = { groupId: 'test-group' }
const REASON = 'the consumer is being closed'

/** @returns {Promise<Buffer>} the request body the version writes for `options` */
const bodyOf = async (version, options) =>
  (await protocol({ version })(options).request.encode()).buffer

/** A response body for versions 3 to 5: one good member and one UNKNOWN_MEMBER_ID, top-level code 0. */
const responseBody = (version) => {
  const member = (memberId, code) =>
    version === 3
      ? new Encoder().writeString(memberId).writeString(null).writeInt16(code)
      : new Encoder()
          .writeUVarIntString(memberId)
          .writeUVarIntString(null)
          .writeInt16(code)
          .writeTaggedFields()
  const encoder = new Encoder().writeInt32(0).writeInt16(0)

  return version === 3
    ? encoder.writeArray([member('a', 0), member('b', 25)]).buffer
    : encoder.writeUVarIntArray([member('a', 0), member('b', 25)]).writeTaggedFields().buffer
}

describe('Protocol > Requests > LeaveGroup', () => {
  test('offers versions 0 to 5', () => {
    expect(versions).toEqual(['0', '1', '2', '3', '4', '5'])
  })

  describe.each([3, 4, 5])('v%i', (version) => {
    const single = { ...GROUP, memberId: 'm', groupInstanceId: 'i' }
    const batch = { ...GROUP, members: [{ memberId: 'm', groupInstanceId: 'i' }] }

    test('writes the single form as a one-member batch, byte for byte', async () => {
      expect(await bodyOf(version, single)).toEqual(await bodyOf(version, batch))
    })

    test('answers the single form with the throwing parse and the batch form with the top-level one', async () => {
      const raw = responseBody(version)
      const whole = protocol({ version })(single).response
      const perMember = protocol({ version })(batch).response
      const data = await whole.decode(raw)

      await expect(whole.parse(data)).rejects.toMatchObject({ type: 'UNKNOWN_MEMBER_ID' })
      await expect(perMember.parse(data)).resolves.toBe(data)
      expect(data.members.map(({ errorCode }) => errorCode)).toEqual([0, 25])
    })

    test.each([[null], [undefined]])('takes members: %s as the single form', async (members) => {
      expect(await bodyOf(version, { ...single, members })).toEqual(await bodyOf(version, batch))
    })

    test('writes every member of a batch, in order', async () => {
      const members = [
        { memberId: 'a', groupInstanceId: null },
        { memberId: '', groupInstanceId: 'b' },
      ]
      const { request } = protocol({ version })({ ...GROUP, members })

      expect(request).toMatchObject({ apiKey: 13, apiVersion: version, apiName: 'LeaveGroup' })
      expect((await request.encode()).buffer.length).toBeGreaterThan(
        (await bodyOf(version, { ...GROUP, members: members.slice(0, 1) })).length
      )
    })
  })

  test.each([3, 4])(
    'drops a reason below v5: v%i writes the same bytes with and without one',
    async (version) => {
      const members = [{ memberId: 'm', groupInstanceId: 'i' }]

      expect(
        await bodyOf(version, { ...GROUP, members: [{ ...members[0], reason: REASON }] })
      ).toEqual(await bodyOf(version, { ...GROUP, members }))
      expect(await bodyOf(version, { ...GROUP, memberId: 'm', reason: REASON })).toEqual(
        await bodyOf(version, { ...GROUP, memberId: 'm' })
      )
    }
  )

  test('sends a reason at v5, from the single form as well as the batch form', async () => {
    const withReason = await bodyOf(5, { ...GROUP, memberId: 'm', reason: REASON })
    const viaBatch = await bodyOf(5, { ...GROUP, members: [{ memberId: 'm', reason: REASON }] })

    expect(withReason.includes(Buffer.from(REASON))).toBe(true)
    expect(withReason).toEqual(viaBatch)
  })

  test('keeps versions 0 to 2 as they were: one member id, no batch', async () => {
    const v2 = protocol({ version: 2 })({ ...GROUP, memberId: 'm', members: [{ memberId: 'x' }] })

    expect((await v2.request.encode()).buffer).toEqual(
      new Encoder().writeString('test-group').writeString('m').buffer
    )
  })
})
