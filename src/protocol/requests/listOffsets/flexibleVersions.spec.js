const { KafkaJSInvariantViolation, KafkaJSProtocolError } = require('../../../errors')

const VERSIONS = [10, 11]
const OK = { partition: 0, errorCode: 0, timestamp: '-1', offset: '-1', leaderEpoch: -1 }

const vector = (version) => require(`./fixtures/java/listOffsetsResponse-v${version}-storage.json`)
const wire = (version) => require(`../../../../testHelpers/wire/listOffsetsV${version}`)
const codec = (version) => require(`./v${version}/response`)

/** Resolves with the rejection reason, and fails the test when the promise resolves. */
const rejection = (promise) =>
  promise.then(
    () => {
      throw new Error('expected a rejection')
    },
    (error) => error
  )

describe.each(VERSIONS)('Protocol > Requests > ListOffsets > v%i', (version) => {
  describe('response', () => {
    test('parse resolves the decoded Apache vector as the same object', async () => {
      const data = await codec(version).decode(Buffer.from(vector(version).hex, 'hex'))

      expect(data).toEqual({
        throttleTime: 0,
        clientSideThrottleTime: 17,
        responses: [{ topic: 't', partitions: [OK] }],
      })
      await expect(codec(version).parse(data)).resolves.toBe(data)
    })

    test('parse throws the first partition error in topic then partition order', async () => {
      const body = wire(version).buildResponse({
        responses: [
          { topic: 'a', partitions: [OK] },
          {
            topic: 'b',
            partitions: [
              { ...OK, partition: 3, errorCode: 6 },
              { ...OK, partition: 4, errorCode: 1 },
            ],
          },
        ],
      })

      const error = await rejection(codec(version).decode(body).then(codec(version).parse))

      expect(error).toBeInstanceOf(KafkaJSProtocolError)
      expect({ type: error.type, code: error.code }).toEqual({
        type: 'NOT_LEADER_FOR_PARTITION',
        code: 6,
      })
    })

    test('decode rejects a byte after the final tag buffer', async () => {
      const bytes = Buffer.concat([Buffer.from(vector(version).hex, 'hex'), Buffer.from([0xff])])

      const error = await rejection(codec(version).decode(bytes))

      expect(error).toBeInstanceOf(KafkaJSInvariantViolation)
      expect(error.message).toContain('ListOffsets response has trailing bytes')
    })
  })

  describe('request', () => {
    test('a partition without timestamp or leader epoch encodes -1 for both and the default timeout', async () => {
      const request = require(`./v${version}/request`)({
        replicaId: -1,
        isolationLevel: 1,
        topics: [{ topic: 't', partitions: [{ partition: 0 }] }],
      })

      const encoded = await request.encode()

      expect(encoded.buffer.toString('hex')).toBe(
        [
          'ffffffff', // replicaId
          '01', // isolationLevel
          '02', // one topic
          '0274', // 't'
          '02', // one partition
          '00000000', // partition 0
          'ffffffff', // currentLeaderEpoch -1
          'ffffffffffffffff', // timestamp -1
          '00', // partition tag buffer
          '00', // topic tag buffer
          '00001388', // timeout 5000
          '00', // body tag buffer
        ].join('')
      )
    })
  })
})
