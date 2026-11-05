const Encoder = require('../encoder')
const { KafkaJSPartialMessageError, KafkaJSProtocolError } = require('../../errors')

const describeClientQuotas = {
  0: require('./describeClientQuotas/v0/response'),
  1: require('./describeClientQuotas/v1/response'),
}
const alterClientQuotasV0 = require('./alterClientQuotas/v0/response')
const describeDelegationTokenV1 = require('./describeDelegationToken/v1/response')

const BROKER_TEXT = 'secret-broker-text'

/** Resolves with the rejection reason, and fails the test when the promise resolves. */
const rejection = (promise) =>
  promise.then(
    () => {
      throw new Error('expected a rejection')
    },
    (error) => error
  )

/** A DescribeClientQuotas v0 body: throttle 0, `errorCode`, a message and a null entries array. */
const quotasV0 = (errorCode = 0) =>
  new Encoder().writeInt32(0).writeInt16(errorCode).writeString(BROKER_TEXT).writeInt32(-1).buffer
/** The same answer at v1, in flexible encoding. */
const quotasV1 = (errorCode = 0) =>
  new Encoder()
    .writeInt32(0)
    .writeInt16(errorCode)
    .writeUVarIntString(BROKER_TEXT)
    .writeUVarInt(0)
    .writeTaggedFields().buffer

describe('Protocol > Requests > quota and token response edges', () => {
  describe('DescribeClientQuotas', () => {
    test('v0 keeps the null entries array of the wire', async () => {
      const data = await describeClientQuotas[0].decode(quotasV0())

      expect(data.entries).toBeNull()
    })

    test('v1 reads the null entries array as an empty list', async () => {
      const data = await describeClientQuotas[1].decode(quotasV1())

      expect(data.entries).toEqual([])
    })

    test.each([
      [0, quotasV0],
      [1, quotasV1],
    ])('v%i parse rejects the top-level error without the broker text', async (version, body) => {
      const data = await describeClientQuotas[version].decode(body(31))
      expect(data.errorMessage).toBe(BROKER_TEXT)

      const error = await rejection(describeClientQuotas[version].parse(data))

      expect(error).toBeInstanceOf(KafkaJSProtocolError)
      expect({ type: error.type, code: error.code }).toEqual({
        type: 'CLUSTER_AUTHORIZATION_FAILED',
        code: 31,
      })
      expect(error.message).not.toContain(BROKER_TEXT)
    })

    test.each([-1, -2])(
      'v0 rejects an entity array of length %i inside an entry',
      async (length) => {
        const body = new Encoder()
          .writeInt32(0)
          .writeInt16(0)
          .writeString(null)
          .writeInt32(1)
          .writeInt32(length).buffer

        const error = await rejection(describeClientQuotas[0].decode(body))

        expect(error).toBeInstanceOf(KafkaJSPartialMessageError)
        expect(error.message).toContain('Invalid array length')
      }
    )

    test('v0 rejects an entries array whose length is below the null sentinel', async () => {
      const body = new Encoder().writeInt32(0).writeInt16(0).writeString(null).writeInt32(-2).buffer

      const error = await rejection(describeClientQuotas[0].decode(body))

      expect(error).toBeInstanceOf(KafkaJSPartialMessageError)
      expect(error.message).toContain('Invalid array length')
    })
  })

  describe('AlterClientQuotas v0', () => {
    test.each([-1, -2])('rejects an entries array of length %i', async (length) => {
      const body = new Encoder().writeInt32(0).writeInt32(length).buffer

      const error = await rejection(alterClientQuotasV0.decode(body))

      expect(error).toBeInstanceOf(KafkaJSPartialMessageError)
      expect(error.message).toContain('Invalid array length')
    })
  })

  describe('DescribeDelegationToken v1', () => {
    test.each([-1, -2])('rejects a tokens array of length %i', async (length) => {
      const body = new Encoder().writeInt16(0).writeInt32(length).writeInt32(0).buffer

      const error = await rejection(describeDelegationTokenV1.decode(body))

      expect(error).toBeInstanceOf(KafkaJSPartialMessageError)
      expect(error.message).toContain('Invalid array length')
    })

    test('parse rejects the top-level error', async () => {
      const body = new Encoder().writeInt16(31).writeInt32(0).writeInt32(0).buffer
      const data = await describeDelegationTokenV1.decode(body)

      const error = await rejection(describeDelegationTokenV1.parse(data))

      expect(error).toBeInstanceOf(KafkaJSProtocolError)
      expect({ type: error.type, code: error.code }).toEqual({
        type: 'CLUSTER_AUTHORIZATION_FAILED',
        code: 31,
      })
    })
  })
})
