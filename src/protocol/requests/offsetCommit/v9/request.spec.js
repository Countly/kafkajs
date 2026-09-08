const RequestV9Protocol = require('./request')
const RequestV8Protocol = require('../v8/request')
const Decoder = require('../../../decoder')

const vector = require('../fixtures/java/offsetCommitRequest-v9-basic.json')

describe('Protocol > Requests > OffsetCommit > v9', () => {
  test('encodes the vector byte-identically to the Apache 4.3.1 codec', async () => {
    const { buffer } = await RequestV9Protocol(vector.fields).encode()

    expect(buffer.toString('hex')).toEqual(vector.hex)
  })

  test('is byte-identical to v8 for the same values', async () => {
    // v9 changes only what GenerationIdOrMemberEpoch MEANS - a member epoch rather than a
    // generation id. If a byte moved, something was invented.
    const v9 = await RequestV9Protocol(vector.fields).encode()
    const v8 = await RequestV8Protocol(vector.fields).encode()

    expect(v9.buffer).toEqual(v8.buffer)
  })

  test('announces itself as version 9', async () => {
    expect(RequestV9Protocol(vector.fields).apiVersion).toEqual(9)
    expect(RequestV9Protocol(vector.fields).apiName).toEqual('OffsetCommit')
  })

  test('matches the v8 vector committed before this release', async () => {
    // The independent half of the case above: the v8 bytes here are the ones Apache's own codec
    // produced, captured in an earlier release, not the ones this repo's v8 codec produces now.
    const { buffer } = await RequestV9Protocol({
      groupId: 'test-group',
      groupGenerationId: 5,
      memberId: 'test-member',
      groupInstanceId: 'test-instance',
      topics: [{ topic: 'test-topic', partitions: [{ partition: 0, offset: '10' }] }],
    }).encode()

    expect(buffer).toEqual(Buffer.from(require('../fixtures/v8_request_java.json')))
  })

  test.each([0, -1, 7])(
    'carries member epoch %i in the generation field, at its own offset, unrewritten',
    async (epoch) => {
      // The consumer protocol passes its member epoch as `groupGenerationId`; 0 and -1 are both
      // legal epochs there. A `Buffer#includes` search would prove nothing - four zero bytes occur
      // in almost any frame - so this reads the field back positionally instead.
      const { buffer } = await RequestV9Protocol({
        ...vector.fields,
        groupGenerationId: epoch,
      }).encode()

      const decoder = new Decoder(buffer)
      expect(decoder.readUVarIntString()).toEqual('test-group')
      expect(decoder.readInt32()).toEqual(epoch)
    }
  )
})
