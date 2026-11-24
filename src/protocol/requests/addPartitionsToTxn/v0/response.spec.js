const { decode, parse } = require('./response')
const { KafkaJSProtocolError, KafkaJSTransactionAbortableError } = require('../../../../errors')

/**
 * @param {number[]} codes one partition per code, in this row order
 * @returns {object} a decoded answer
 */
const answerWith = (codes) => ({
  throttleTime: 0,
  errors: [
    {
      topic: 'test-topic',
      partitionErrors: codes.map((errorCode, partition) => ({ partition, errorCode })),
    },
  ],
})

describe('Protocol > Requests > AddPartitionsToTxn > v0', () => {
  test('response', async () => {
    const data = await decode(Buffer.from(require('../fixtures/v0_response.json')))
    expect(data).toEqual({
      throttleTime: 0,
      errors: [
        {
          topic: 'test-topic-f6bab978bdca094688e3-37015-ca4f7ad4-5dcc-4bb9-9853-b1e4c4ed78a7',
          partitionErrors: [
            { errorCode: 0, partition: 1 },
            { errorCode: 0, partition: 2 },
          ],
        },
      ],
    })

    await expect(parse(data)).resolves.toBeTruthy()
  })

  test('throws KafkaJSProtocolError if there is an error on any of the partitions', async () => {
    const data = {
      throttleTime: 0,
      errors: [
        {
          topic: 'test-topic',
          partitionErrors: [
            { errorCode: 0, partition: 1 },
            { errorCode: 49, partition: 2 },
          ],
        },
      ],
    }

    await expect(parse(data)).rejects.toEqual(
      new KafkaJSProtocolError(
        'The producer attempted to use a producer id which is not currently assigned to its transactional id'
      )
    )
  })

  test('throws the authorization error for every order of [3, 29, 55]', async () => {
    const orders = [
      [3, 29, 55],
      [3, 55, 29],
      [29, 3, 55],
      [29, 55, 3],
      [55, 3, 29],
      [55, 29, 3],
    ]
    for (const codes of orders) {
      await expect(parse(answerWith(codes))).rejects.toMatchObject({
        name: 'KafkaJSProtocolError',
        code: 29,
      })
    }
  })

  test('throws the authorization error for [55, 29] and [29, 55]', async () => {
    for (const codes of [
      [55, 29],
      [29, 55],
    ]) {
      await expect(parse(answerWith(codes))).rejects.toMatchObject({
        name: 'KafkaJSProtocolError',
        code: 29,
      })
    }
  })

  test('throws UNKNOWN_TOPIC_OR_PARTITION for [55, 3] and [3, 55]', async () => {
    for (const codes of [
      [55, 3],
      [3, 55],
    ]) {
      await expect(parse(answerWith(codes))).rejects.toMatchObject({
        name: 'KafkaJSProtocolError',
        type: 'UNKNOWN_TOPIC_OR_PARTITION',
        code: 3,
        retriable: true,
      })
    }
  })

  test('an answer whose only failures are 55 throws KafkaJSTransactionAbortableError with the 55 error as cause', async () => {
    const error = await parse(answerWith([0, 55, 55])).catch((failure) => failure)

    expect(error).toBeInstanceOf(KafkaJSTransactionAbortableError)
    expect(error).toMatchObject({ category: 'abortable', retriable: false })
    expect(error.cause).toBeInstanceOf(KafkaJSProtocolError)
    expect(error.cause).toMatchObject({ type: 'OPERATION_NOT_ATTEMPTED', code: 55 })
  })

  test('throws the lowest code when two non-retriable codes other than 55 are mixed', async () => {
    for (const codes of [
      [55, 53, 29],
      [53, 29, 55],
    ]) {
      await expect(parse(answerWith(codes))).rejects.toMatchObject({
        name: 'KafkaJSProtocolError',
        code: 29,
      })
    }
  })
})
