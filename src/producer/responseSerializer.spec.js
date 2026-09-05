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

  const ALLOWLISTED_KEYS = [
    'topicName',
    'partition',
    'errorCode',
    'baseOffset',
    'logAppendTime',
    'logStartOffset',
  ]

  test('a v9 response serializes to exactly the allowlisted RecordMetadata keys', async () => {
    const { decode } = require('../protocol/requests/produce/v9/response')
    const data = await decode(
      Buffer.from(require('../protocol/requests/produce/fixtures/v9_response.json'))
    )

    const [metadata] = responseSerializer(data)

    expect(Object.keys(metadata)).toEqual(ALLOWLISTED_KEYS)
  })

  test('a v10 response carrying the KIP-951 leader hint still serializes to exactly the allowlisted RecordMetadata keys', async () => {
    // v10_response_not_leader decodes an errored partition (errorCode 6) with currentLeader set;
    // responseSerializer only ever runs on a fully successful response in production, but the
    // allowlist must hold regardless of what the decoder attaches to the partition object, so this
    // proves currentLeader can never leak through even if it were present.
    const { decode } = require('../protocol/requests/produce/v10/response')
    const data = await decode(
      Buffer.from(require('../protocol/requests/produce/fixtures/v10_response_not_leader.json'))
    )

    const [metadata] = responseSerializer(data)

    expect(Object.keys(metadata)).toEqual(ALLOWLISTED_KEYS)
    expect(metadata).not.toHaveProperty('currentLeader')
  })
})
