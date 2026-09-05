const responseSerializer = require('./responseSerializer')

describe('Producer > responseSerializer', () => {
  test('flattens topics/partitions into the flat RecordMetadata shape', () => {
    const response = {
      topics: [
        {
          topicName: 'topic-a',
          partitions: [
            {
              partition: 0,
              errorCode: 0,
              baseOffset: '1',
              logAppendTime: '-1',
              logStartOffset: '0',
            },
          ],
        },
      ],
    }

    expect(responseSerializer(response)).toEqual([
      {
        topicName: 'topic-a',
        partition: 0,
        errorCode: 0,
        baseOffset: '1',
        logAppendTime: '-1',
        logStartOffset: '0',
      },
    ])
  })

  test('strips the KIP-467 recordErrors/errorMessage fields from a successful (v8+) response', () => {
    // Those fields describe a failure and, per spec, live only on the thrown produce error -
    // never on a successful RecordMetadata, however a decoded v8+ partition always carries them.
    const response = {
      topics: [
        {
          topicName: 'topic-a',
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
    }

    const [metadata] = responseSerializer(response)

    expect(metadata).toEqual({
      topicName: 'topic-a',
      partition: 0,
      errorCode: 0,
      baseOffset: '100',
      logAppendTime: '-1',
      logStartOffset: '0',
    })
    expect(metadata).not.toHaveProperty('recordErrors')
    expect(metadata).not.toHaveProperty('errorMessage')
  })
})
