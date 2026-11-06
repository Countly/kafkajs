const { createConnectionPool, connectionOpts, newLogger } = require('testHelpers')
const { KafkaJSProtocolError } = require('../../errors')
const { createErrorFromCode } = require('../../protocol/error')
const { requests } = require('../../protocol/requests')
const apiKeys = require('../../protocol/requests/apiKeys')
const Broker = require('../index')

const UNSUPPORTED_VERSION_CODE = 35

describe('Broker > ApiVersions', () => {
  let broker

  beforeEach(async () => {
    broker = new Broker({
      connectionPool: createConnectionPool(connectionOpts()),
      logger: newLogger(),
    })
    await broker.connect()
  })

  afterEach(async () => {
    broker && (await broker.disconnect())
  })

  afterEach(() => jest.restoreAllMocks())

  test('request', async () => {
    await expect(broker.apiVersions()).resolves.toBeTruthy()
  })

  test('negotiates the highest implemented version the broker offers', async () => {
    jest.spyOn(requests.ApiVersions, 'protocol')

    const versions = await broker.apiVersions()

    // A broker below Kafka 4.4 answers the first v5 request with UNSUPPORTED_VERSION and its own
    // maximum, which is the one retry; a 4.4 broker accepts v5.
    const { maxVersion } = versions[apiKeys.ApiVersions]
    const asked = requests.ApiVersions.protocol.mock.calls.map(([{ version }]) => version)
    expect(asked).toEqual(maxVersion >= 5 ? [5] : [5, maxVersion])
  })

  test('retries once at the version the broker hints (KIP-511)', async () => {
    jest.spyOn(requests.ApiVersions, 'protocol').mockImplementationOnce(() => () => {
      const error = new KafkaJSProtocolError(createErrorFromCode(UNSUPPORTED_VERSION_CODE))
      error.apiVersions = [{ apiKey: apiKeys.ApiVersions, minVersion: 0, maxVersion: 1 }]
      throw error
    })

    await expect(broker.apiVersions()).resolves.toBeTruthy()
    expect(requests.ApiVersions.protocol).toHaveBeenCalledWith({ version: 5 })
    expect(requests.ApiVersions.protocol).toHaveBeenCalledWith({ version: 1 })
    expect(requests.ApiVersions.protocol).not.toHaveBeenCalledWith({ version: 2 })
  })

  test('does not retry a second time', async () => {
    jest.spyOn(requests.ApiVersions, 'protocol').mockImplementation(() => () => {
      throw new KafkaJSProtocolError(createErrorFromCode(UNSUPPORTED_VERSION_CODE))
    })

    await expect(broker.apiVersions()).rejects.toThrow('The version of API is not supported')
    expect(requests.ApiVersions.protocol).toHaveBeenCalledTimes(2)
  })
})
