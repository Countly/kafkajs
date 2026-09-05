const { decode, parse } = require('./response')

describe('Protocol > Requests > Produce > v8', () => {
  test('response', async () => {
    const data = await decode(Buffer.from(require('../fixtures/v8_response.json')))

    expect(data).toEqual({
      topics: [
        {
          topicName: 'test-topic',
          partitions: [
            {
              partition: 0,
              errorCode: 0,
              baseOffset: '100',
              logAppendTime: '-1',
              logStartOffset: '0',
              recordErrors: [],
              errorMessage: null,
            },
          ],
        },
      ],
      throttleTime: 0,
    })

    await expect(parse(data)).resolves.toBeTruthy()
  })

  test('a failing partition throws with the topic, partition, message and per-record detail', async () => {
    const data = await decode(Buffer.from(require('../fixtures/v8_response_record_errors.json')))

    await expect(parse(data)).rejects.toMatchObject({
      name: 'KafkaJSProtocolError',
      type: 'INVALID_RECORD',
      code: 87,
      topic: 'test-topic',
      partition: 0,
      errorMessage: 'Invalid record',
      recordErrors: [{ batchIndex: 1, errorMessage: 'record too large' }],
    })
  })

  test('the first failing partition is the one reported, as in every earlier version', async () => {
    const data = await decode(Buffer.from(require('../fixtures/v8_response_record_errors.json')))
    data.topics[0].partitions.push({
      partition: 1,
      errorCode: 6,
      baseOffset: '-1',
      logAppendTime: '-1',
      logStartOffset: '-1',
      recordErrors: [],
      errorMessage: null,
    })

    await expect(parse(data)).rejects.toMatchObject({ code: 87, partition: 0 })
  })

  test('decodes the vector the Apache kafka-clients 4.3.1 codec produced', async () => {
    const data = await decode(Buffer.from(require('../fixtures/v8_response_java.json')))

    expect(data).toEqual({
      topics: [
        {
          topicName: 'test-topic',
          partitions: [
            {
              partition: 0,
              errorCode: 0,
              baseOffset: '100',
              logAppendTime: '-1',
              logStartOffset: '0',
              recordErrors: [],
              errorMessage: null,
            },
          ],
        },
      ],
      throttleTime: 0,
    })

    await expect(parse(data)).resolves.toBeTruthy()
  })
})
