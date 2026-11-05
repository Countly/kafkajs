const Broker = require('..')
const { lookup, requests } = require('../../protocol/requests')
const apiKeys = require('../../protocol/requests/apiKeys')
const { KafkaJSServerDoesNotSupportApiKey } = require('../../errors')

const SEND_REQUEST = Object.getOwnPropertySymbols(Broker.prototype).find((symbol) =>
  String(symbol).includes('sendRequest')
)

/**
 * A real Broker over negotiated `versions`, whose transport records each encoded request and
 * answers through the request's own response parser.
 * @param {object} versions broker ranges by api key
 * @param {object} [answer] decoded response the transport hands to the parser
 * @returns {{ broker: Broker, sent: {version: number, body: string}[] }}
 */
const brokerOver = (versions, answer = {}) => {
  const broker = Object.create(Broker.prototype)
  broker.versions = versions
  broker.lookupRequest = lookup(versions)
  broker.lookupRequestWithMax = (key, definition, maximum) =>
    lookup(versions, { [key]: maximum })(key, definition)
  const sent = []
  broker[SEND_REQUEST] = async (protocolRequest) => {
    sent.push({
      version: protocolRequest.request.apiVersion,
      body: (await protocolRequest.request.encode()).buffer.toString('hex'),
    })
    return protocolRequest.response.parse(answer)
  }
  return { broker, sent }
}

/** Returns the error the call throws, and fails the test when it does not throw. */
const thrown = (fn) => {
  try {
    fn()
  } catch (error) {
    return error
  }
  throw new Error('expected the call to throw')
}

describe('Broker > admin request capabilities', () => {
  test('a transaction request with no named option and no broker range says so', () => {
    const { broker } = brokerOver({})

    const error = thrown(() =>
      broker.transactionRequest(apiKeys.ListTransactions, requests.ListTransactions)
    )

    expect(error).toBeInstanceOf(KafkaJSServerDoesNotSupportApiKey)
    expect(error.message).toMatch(
      /^ListTransactions needs v0 or newer; client implements v0-v\d+; broker offers no range$/
    )
  })

  test('a cluster request names the generic operation when the caller names no option', () => {
    const { broker } = brokerOver({})

    const error = thrown(() =>
      broker.clusterRequest(apiKeys.ListConfigResources, requests.ListConfigResources)
    )

    expect(error).toBeInstanceOf(KafkaJSServerDoesNotSupportApiKey)
    expect(error.message).toMatch(
      /^operation requires ListConfigResources v0\+; client implements v0-v\d+, broker supports no range$/
    )
  })

  test('a cluster request with no minimum builds from any overlapping range', () => {
    const { broker } = brokerOver({
      [apiKeys.ListConfigResources]: { minVersion: 0, maxVersion: 1 },
    })

    expect(
      typeof broker.clusterRequest(apiKeys.ListConfigResources, requests.ListConfigResources)
    ).toBe('function')
  })

  test('electLeaders without options sends a preferred election of every partition at the newest version', async () => {
    const { broker, sent } = brokerOver({
      [apiKeys.ElectLeaders]: { minVersion: 0, maxVersion: 2 },
    })

    await broker.electLeaders({})

    // electionType 0, null topicPartitions, timeout 60000, tag buffer
    expect(sent).toEqual([{ version: 2, body: '00000000ea6000' }])
  })

  test('listConfigResources without options sends no resource type filter', async () => {
    const { broker, sent } = brokerOver({
      [apiKeys.ListConfigResources]: { minVersion: 0, maxVersion: 1 },
    })

    await broker.listConfigResources({})

    // empty compact resourceTypes, tag buffer
    expect(sent).toEqual([{ version: 1, body: '0100' }])
  })

  test('offsetDelete names the range a broker offers when it does not include v0', async () => {
    const { broker, sent } = brokerOver({
      [apiKeys.OffsetDelete]: { minVersion: 1, maxVersion: 2 },
    })

    const error = await broker.offsetDelete({ groupId: 'g', topics: [] }).catch((reason) => reason)

    expect(error).toBeInstanceOf(KafkaJSServerDoesNotSupportApiKey)
    expect(error.message).toBe('OffsetDelete client implements v0-v0; broker supports v1-v2')
    expect(sent).toEqual([])
  })

  test('offsetFetch in data mode accepts an answer without an error code for an all-topic read', async () => {
    const answer = {
      groupId: 'g',
      responses: [
        {
          topic: 't',
          partitions: [{ partition: 0, offset: '1', metadata: null, errorCode: 0 }],
        },
      ],
    }
    const { broker, sent } = brokerOver(
      { [apiKeys.OffsetFetch]: { minVersion: 1, maxVersion: 9 } },
      answer
    )

    const result = await broker.offsetFetch({ groupId: 'g', topics: [], returnErrors: true })

    expect(result).toBe(answer)
    expect(sent.map(({ version }) => version)).toEqual([9])
  })
})
