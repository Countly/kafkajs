const dns = require('dns')
const { Kafka, logLevel } = require('../../../index')
const defaultSocketFactory = require('../../network/socketFactory')
const { plainTextBrokers } = require('testHelpers')

describe('Broker > bootstrap DNS (live)', () => {
  let admin
  let lookup

  afterEach(async () => {
    lookup?.mockRestore()
    await admin?.disconnect()
  })

  test('a positive readiness timeout connects with the original bootstrap hostname', async () => {
    const brokers = plainTextBrokers()
    const hosts = new Set(brokers.map((address) => address.split(':')[0]))
    const dialled = []
    const socketFactory = defaultSocketFactory()
    lookup = jest.spyOn(dns, 'lookup')
    admin = new Kafka({
      clientId: 'bootstrap-dns-live',
      brokers,
      logLevel: logLevel.NOTHING,
      bootstrapResolveTimeout: 5000,
      socketFactory: (args) => {
        dialled.push(args.host)
        return socketFactory(args)
      },
    }).admin()

    await admin.connect()
    const { topics } = await admin.fetchTopicMetadata()

    expect(Array.isArray(topics)).toBe(true)
    const [firstLookup] = lookup.mock.calls
    expect(hosts).toContain(firstLookup[0])
    expect(firstLookup[1]).toEqual({ all: true })
    expect(hosts).toContain(dialled[0])
  })
})
