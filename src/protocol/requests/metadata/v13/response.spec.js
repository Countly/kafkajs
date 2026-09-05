const { decode, parse } = require('./response')

describe('Protocol > Requests > Metadata > v13', () => {
  test('response', async () => {
    const data = await decode(Buffer.from(require('../fixtures/v13_response.json')))

    expect(data).toEqual({
      throttleTime: 0,
      clientSideThrottleTime: 0,
      errorCode: 0,
      brokers: [{ nodeId: 0, host: '127.0.0.1', port: 9092, rack: null }],
      clusterId: 'test-cluster-id',
      controllerId: 0,
      topicMetadata: [
        {
          topicErrorCode: 0,
          topic: 'test-topic',
          topicId: '01020304-0506-0708-090a-0b0c0d0e0f10',
          isInternal: false,
          topicAuthorizedOperations: -2147483648,
          partitionMetadata: [
            {
              partitionErrorCode: 0,
              partitionId: 0,
              leader: 1,
              leaderEpoch: 5,
              replicas: [1],
              isr: [1],
              offlineReplicas: [],
            },
          ],
        },
      ],
    })

    await expect(parse(data)).resolves.toBeTruthy()
  })

  test('a top-level error code is decoded unconditionally and thrown by parse', async () => {
    const data = await decode(
      Buffer.from(require('../fixtures/v13_response_rebootstrap_required.json'))
    )

    expect(data.errorCode).toEqual(129)
    await expect(parse(data)).rejects.toThrow(
      'Client metadata is stale. The client should rebootstrap to obtain new metadata'
    )
    await expect(parse(data)).rejects.toMatchObject({
      name: 'KafkaJSProtocolError',
      type: 'REBOOTSTRAP_REQUIRED',
      code: 129,
      retriable: false,
    })
  })

  test('the top-level error wins over a topic error', async () => {
    const data = await decode(
      Buffer.from(require('../fixtures/v13_response_rebootstrap_required.json'))
    )
    data.topicMetadata[0].topicErrorCode = 3 // UNKNOWN_TOPIC_OR_PARTITION

    await expect(parse(data)).rejects.toMatchObject({ type: 'REBOOTSTRAP_REQUIRED' })
  })
})
