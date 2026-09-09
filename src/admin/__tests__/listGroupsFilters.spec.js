const createAdmin = require('../index')
const { createErrorFromCode } = require('../../protocol/error')
const { KafkaJSServerDoesNotSupportApiKey } = require('../../errors')
const { newLogger } = require('testHelpers')

const COORDINATOR_LOAD_IN_PROGRESS = 14
const GROUP_AUTHORIZATION_FAILED = 30

const stubCluster = (listGroups) => ({
  refreshMetadata: async () => {},
  brokerPool: { brokers: { 0: {}, 1: {} } },
  findBroker: async () => ({ listGroups }),
})

describe('Admin > listGroups > filters and fields', () => {
  const retry = { retries: 1, initialRetryTime: 1, maxRetryTime: 2 }

  it('lower-cases groupType and leaves groupState wire-cased, fanned out over the brokers', async () => {
    // The broker method returns the WIRE display strings; the lower-casing is this layer's job.
    const listGroups = jest.fn(async () => ({
      errorCode: 0,
      groups: [
        { groupId: 'g', protocolType: 'consumer', groupState: 'Stable', groupType: 'Consumer' },
      ],
    }))
    const admin = createAdmin({ logger: newLogger(), retry, cluster: stubCluster(listGroups) })

    const { groups } = await admin.listGroups()

    expect(listGroups).toHaveBeenCalledTimes(2)
    expect(listGroups).toHaveBeenCalledWith({ states: [], types: [] })
    expect(groups).toHaveLength(2)
    expect(groups[0]).toEqual({
      groupId: 'g',
      protocolType: 'consumer',
      groupState: 'Stable',
      groupType: 'consumer',
    })
  })

  it.each([
    ['Classic', 'classic'],
    ['Consumer', 'consumer'],
    ['Share', 'share'],
    ['Streams', 'streams'],
    ['Unknown', 'unknown'],
  ])('maps the wire type %s to %s', async (wire, expected) => {
    const listGroups = jest.fn(async () => ({
      errorCode: 0,
      groups: [{ groupId: 'g', protocolType: '', groupState: 'Stable', groupType: wire }],
    }))
    const admin = createAdmin({ logger: newLogger(), retry, cluster: stubCluster(listGroups) })

    const { groups } = await admin.listGroups()
    expect(groups[0].groupType).toEqual(expected)
  })

  it('answers empty strings for a broker whose version carries neither field', async () => {
    // ListGroups v0-v2 has no GroupState and no GroupType, so the decoded row has neither key.
    const listGroups = jest.fn(async () => ({
      errorCode: 0,
      groups: [{ groupId: 'g', protocolType: 'consumer' }],
    }))
    const admin = createAdmin({ logger: newLogger(), retry, cluster: stubCluster(listGroups) })

    const { groups } = await admin.listGroups()

    expect(groups[0]).toEqual({
      groupId: 'g',
      protocolType: 'consumer',
      groupState: '',
      groupType: '',
    })
  })

  it('passes the lower-case filters straight through to the broker method', async () => {
    const listGroups = jest.fn(async () => ({ errorCode: 0, groups: [] }))
    const admin = createAdmin({ logger: newLogger(), retry, cluster: stubCluster(listGroups) })

    await admin.listGroups({ states: ['stable'], types: ['consumer'] })

    expect(listGroups).toHaveBeenCalledWith({ states: ['stable'], types: ['consumer'] })
  })

  it('rejects a filter that is not an array of strings', async () => {
    const admin = createAdmin({
      logger: newLogger(),
      retry,
      cluster: stubCluster(async () => ({ errorCode: 0, groups: [] })),
    })

    await expect(admin.listGroups({ states: 'stable' })).rejects.toThrow(
      'Expected states to be an array of strings'
    )
    await expect(admin.listGroups({ types: [1] })).rejects.toThrow(
      'Expected types to be an array of strings'
    )
  })

  it('lets the downgrade rule of the broker method surface unchanged, and does not retry it', async () => {
    // The version rules live in the broker method, where the negotiated version is known. The
    // admin layer adds no rule of its own, which is what this case pins: the message the broker
    // method throws is what a user sees, on the first attempt - a version the broker does not have
    // will not appear on a second one.
    const listGroups = jest.fn(async () => {
      throw new KafkaJSServerDoesNotSupportApiKey(
        'The broker only supports ListGroups v4, but we need v5 or newer to request groups by type. Requested group types: [Consumer].',
        { apiKey: 16, apiName: 'ListGroups' }
      )
    })
    const admin = createAdmin({ logger: newLogger(), retry, cluster: stubCluster(listGroups) })

    // SF-3: the retrier bails with the ORIGINAL instance rather than wrapping it, so the two
    // fields that name the unsupported request survive.
    const caught = await admin.listGroups({ types: ['consumer'] }).catch((e) => e)

    expect(caught).toBeInstanceOf(KafkaJSServerDoesNotSupportApiKey)
    expect(caught.message).toContain('we need v5 or newer to request groups by type')
    expect(caught.apiKey).toEqual(16)
    expect(caught.apiName).toEqual('ListGroups')
    expect(listGroups).toHaveBeenCalledTimes(1)
  })

  // SF-3: 2.2.4 let the raw KafkaJSProtocolError reach the caller. Every per-broker call now runs
  // inside the retrier, so what a caller catches is the retrier's own class with the broker's
  // error on `cause` - documented in the changelog's behaviour changes.
  it('rejects with the retrier class, keeping type, code and cause', async () => {
    const listGroups = jest.fn(async () => {
      throw createErrorFromCode(GROUP_AUTHORIZATION_FAILED)
    })
    const admin = createAdmin({ logger: newLogger(), retry, cluster: stubCluster(listGroups) })

    const caught = await admin.listGroups().catch((e) => e)

    expect(caught.name).toEqual('KafkaJSNonRetriableError')
    expect(caught.type).toEqual('GROUP_AUTHORIZATION_FAILED')
    expect(caught.code).toEqual(GROUP_AUTHORIZATION_FAILED)
    expect(caught.cause.name).toEqual('KafkaJSProtocolError')
  })

  it('rejects with KafkaJSNumberOfRetriesExceeded once the budget is spent', async () => {
    const listGroups = jest.fn(async () => {
      throw createErrorFromCode(COORDINATOR_LOAD_IN_PROGRESS)
    })
    const admin = createAdmin({ logger: newLogger(), retry, cluster: stubCluster(listGroups) })

    const caught = await admin.listGroups().catch((e) => e)

    expect(caught.name).toEqual('KafkaJSNumberOfRetriesExceeded')
    expect(caught.cause.code).toEqual(COORDINATOR_LOAD_IN_PROGRESS)
  })

  it('asks a broker again when its coordinator is still loading', async () => {
    let call = 0
    const listGroups = jest.fn(async () => {
      call += 1

      if (call === 1) {
        throw createErrorFromCode(COORDINATOR_LOAD_IN_PROGRESS)
      }

      return {
        errorCode: 0,
        groups: [
          {
            groupId: `g${call}`,
            protocolType: 'consumer',
            groupState: 'Stable',
            groupType: 'Consumer',
          },
        ],
      }
    })
    const admin = createAdmin({ logger: newLogger(), retry, cluster: stubCluster(listGroups) })

    const { groups } = await admin.listGroups()

    // Two brokers, one of which needed a second attempt: three calls, two rows.
    expect(listGroups).toHaveBeenCalledTimes(3)
    expect(groups.map(({ groupId }) => groupId)).toEqual(['g2', 'g3'])
  })

  it('does not retry an error the broker will answer the same way', async () => {
    const listGroups = jest.fn(async () => {
      throw createErrorFromCode(GROUP_AUTHORIZATION_FAILED)
    })
    const admin = createAdmin({ logger: newLogger(), retry, cluster: stubCluster(listGroups) })

    await expect(admin.listGroups()).rejects.toThrow('Not authorized to access group')
    expect(listGroups).toHaveBeenCalledTimes(1)
  })
})
