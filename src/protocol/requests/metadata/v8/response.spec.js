const { decode, parse } = require('./response')

describe('Protocol > Requests > Metadata > v8', () => {
  test('response carries the authorized-operations bitfields', async () => {
    const data = await decode(Buffer.from(require('../fixtures/v8_response.json')))

    expect(data).toEqual({
      throttleTime: 0,
      clientSideThrottleTime: 0,
      brokers: [{ nodeId: 0, host: '127.0.0.1', port: 9092, rack: null }],
      clusterId: 'test-cluster-id',
      controllerId: 0,
      topicMetadata: [
        {
          topicErrorCode: 0,
          topic: 'test-topic',
          isInternal: false,
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
          topicAuthorizedOperations: -2147483648,
        },
      ],
      clusterAuthorizedOperations: -2147483648,
    })

    await expect(parse(data)).resolves.toBeTruthy()
  })
})
