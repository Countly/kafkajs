const RequestV12Protocol = require('./request')
const RequestV11Protocol = require('../v11/request')
const { Types } = require('../../../message/compression')

// An explicit timestamp keeps the two record batches - and therefore their CRCs - reproducible;
// without it Date.now() makes a byte-equality comparison flaky.
const args = {
  acks: -1,
  timeout: 30000,
  compression: Types.None,
  topicData: [
    {
      topic: 'test-topic',
      partitions: [
        {
          partition: 0,
          firstSequence: 0,
          messages: [{ key: 'k', value: 'v', timestamp: 1700000000000 }],
        },
      ],
    },
  ],
}

describe('Protocol > Requests > Produce > v12', () => {
  test('is byte-identical to v11 apart from the api version', async () => {
    const v12 = await RequestV12Protocol(args).encode()
    const v11 = await RequestV11Protocol(args).encode()

    expect(v12.buffer).toEqual(v11.buffer)
    expect(RequestV12Protocol(args).apiVersion).toEqual(12)
    expect(RequestV11Protocol(args).apiVersion).toEqual(11)
  })
})
