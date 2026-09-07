const { requests, lookup } = require('../../protocol/requests')
const apiKeys = require('../../protocol/requests/apiKeys')
const RequestQueue = require('../../network/requestQueue')
const InstrumentationEventEmitter = require('../../instrumentation/emitter')
const { newLogger } = require('testHelpers')

const versions = {
  [apiKeys.JoinGroup]: { minVersion: 0, maxVersion: 5 },
  [apiKeys.SyncGroup]: { minVersion: 0, maxVersion: 3 },
}

const joinGroupTimeout = ({ rebalanceTimeout, sessionTimeout }) =>
  lookup(versions)(apiKeys.JoinGroup, requests.JoinGroup)({
    groupId: 'g',
    sessionTimeout,
    rebalanceTimeout,
    memberId: '',
    groupProtocols: [],
  }).requestTimeout

const syncGroupTimeout = ({ rebalanceTimeout, sessionTimeout }) =>
  lookup(versions)(apiKeys.SyncGroup, requests.SyncGroup)({
    groupId: 'g',
    generationId: 1,
    memberId: 'm',
    groupAssignment: [],
    sessionTimeout,
    rebalanceTimeout,
  }).requestTimeout

describe('Consumer > derived group request timeouts', () => {
  const SESSION_TIMEOUT = 30000

  it('derives 305000 / 65000 at the defaults', () => {
    expect(joinGroupTimeout({ rebalanceTimeout: 300000, sessionTimeout: SESSION_TIMEOUT })).toEqual(
      305000
    )
    expect(syncGroupTimeout({ rebalanceTimeout: 300000, sessionTimeout: SESSION_TIMEOUT })).toEqual(
      65000
    )
  })

  it('derives 35000 / 35000 for a maxPollInterval of 30000', () => {
    expect(joinGroupTimeout({ rebalanceTimeout: 30000, sessionTimeout: SESSION_TIMEOUT })).toEqual(
      35000
    )
    expect(syncGroupTimeout({ rebalanceTimeout: 30000, sessionTimeout: SESSION_TIMEOUT })).toEqual(
      35000
    )
  })

  it('keeps the falsy fallback for rebalanceTimeout 0', () => {
    // `||`, not `??`: a configured 0 must still fall back to sessionTimeout, exactly as it did
    // before this release. `??` would produce 5000 here.
    expect(joinGroupTimeout({ rebalanceTimeout: 0, sessionTimeout: SESSION_TIMEOUT })).toEqual(
      35000
    )
    expect(syncGroupTimeout({ rebalanceTimeout: 0, sessionTimeout: SESSION_TIMEOUT })).toEqual(
      35000
    )
  })

  it('caps SyncGroup at 60000 + 5000 however large the rebalance timeout is', () => {
    expect(syncGroupTimeout({ rebalanceTimeout: 600000, sessionTimeout: SESSION_TIMEOUT })).toEqual(
      65000
    )
    expect(joinGroupTimeout({ rebalanceTimeout: 600000, sessionTimeout: SESSION_TIMEOUT })).toEqual(
      605000
    )
  })

  it('caps a MAX_SAFE_INTEGER rebalance timeout rather than overflowing', () => {
    expect(
      syncGroupTimeout({
        rebalanceTimeout: Number.MAX_SAFE_INTEGER,
        sessionTimeout: SESSION_TIMEOUT,
      })
    ).toEqual(65000)
  })

  it('is never lowered below the client default by the request queue', () => {
    // Drives the real coalescing expression, `requestQueue/index.js:118-123`'s
    // `Math.max(defaultRequestTimeout, customRequestTimeout || 0)`, through the method that owns
    // it. Asserting on the constructor argument instead would compare 30000 with 25000 and could
    // never fail.
    const queue = new RequestQueue({
      instrumentationEmitter: new InstrumentationEventEmitter(),
      maxInFlightRequests: null,
      requestTimeout: 30000,
      enforceRequestTimeout: false,
      clientId: 'timeouts',
      broker: 'localhost:9092',
      logger: newLogger(),
    })

    const socketRequestFor = (customRequestTimeout) =>
      queue.createSocketRequest({
        entry: {
          apiKey: 14,
          apiName: 'SyncGroup',
          apiVersion: 3,
          correlationId: 1,
          resolve() {},
          reject() {},
        },
        expectResponse: true,
        requestTimeout: customRequestTimeout,
        sendRequest: () => {},
      })

    // A capped SyncGroup timeout below the client default is raised to it, never shortened...
    const derived = syncGroupTimeout({ rebalanceTimeout: 20000, sessionTimeout: SESSION_TIMEOUT })
    expect(derived).toEqual(25000)
    expect(socketRequestFor(derived).requestTimeout).toEqual(30000)

    // ...and one above it is honoured as it is, which is what the 65 s default case needs.
    expect(socketRequestFor(65000).requestTimeout).toEqual(65000)
  })
})
