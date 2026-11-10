const ClientTelemetry = require('../client')
const TelemetryMetrics = require('../metrics')

const ID = '01020304-0506-0708-090a-0b0c0d0e0f10'
const PUSH_WATCHDOG_MS = 30000
const VERSIONS = { 71: { minVersion: 0, maxVersion: 0 }, 72: { minVersion: 0, maxVersion: 0 } }
const subscription = () => ({
  clientInstanceId: ID,
  subscriptionId: -7,
  acceptedCompressionTypes: [0],
  pushIntervalMs: 1000,
  telemetryMaxBytes: 1048576,
  deltaTemporality: true,
  requestedMetrics: ['*'],
})
const endpoint = (host, port, endpointRevision) => ({
  nodeId: 1,
  host,
  port,
  endpointRevision,
  versions: VERSIONS,
})

/**
 * A real collector and client over a scripted transport whose handles record what they were sent.
 * The first push is held until the test releases it.
 */
const setup = () => {
  const metrics = new TelemetryMetrics({
    kind: 'producer',
    clock: { wallMs: () => 1000, monotonicNs: () => BigInt(jest.now()) * 1000000n },
  })
  const handles = []
  let candidates = [endpoint('a', 9092, 1n)]
  let release
  const held = new Promise((resolve) => (release = resolve))
  let pushes = 0
  const transport = {
    candidates: () => candidates,
    open: jest.fn(async (candidate, { guard, onAllocated }) => {
      const handle = {
        host: candidate.host,
        retired: false,
        isConnected: () => !handle.retired,
        get: jest.fn(async (payload, authority) => {
          const error = authority.guard()
          if (error) throw error
          return subscription()
        }),
        push: jest.fn(async (payload, authority) => {
          const error = authority.guard()
          if (error) throw error
          authority.onWrite()
          return ++pushes === 1 ? held : { errorCode: 0, clientSideThrottleTime: 0 }
        }),
        retire: jest.fn(async () => {
          handle.retired = true
        }),
      }
      handles.push(handle)
      onAllocated(handle)
      const refusal = guard()
      if (refusal) throw refusal
      return handle
    }),
  }
  const client = new ClientTelemetry({
    kind: 'producer',
    metrics,
    transport,
    clock: { now: () => jest.now(), random: () => 0.5, setTimeout, clearTimeout },
    config: { enableMetricsPush: true },
  })
  return {
    client,
    transport,
    handles,
    releaseOldResponse: () => release({ errorCode: 0, clientSideThrottleTime: 0 }),
    replaceEndpoint: (next) => {
      candidates = [next]
    },
  }
}

/** Advances fake time in half-second steps until `condition` holds, at most `limit` ms. */
const until = async (condition, limit = 120000) => {
  for (let elapsed = 0; !condition() && elapsed < limit; elapsed += 500) {
    await jest.advanceTimersByTimeAsync(500)
  }
  expect(condition()).toBe(true)
}

describe('Kafka 4.4 client fix audit', () => {
  beforeEach(() => jest.useFakeTimers({ now: 0 }))
  afterEach(() => jest.useRealTimers())

  test('KAFKA-20393 endpoint changes retire stale telemetry sends', async () => {
    const { client, transport, handles, releaseOldResponse, replaceEndpoint } = setup()
    client.resume()
    await until(() => handles.length === 1 && handles[0].push.mock.calls.length === 1)
    const [oldHandle] = handles

    // The node's endpoint is replaced while the old endpoint's push is still unanswered.
    replaceEndpoint(endpoint('b', 9093, 2n))
    await jest.advanceTimersByTimeAsync(PUSH_WATCHDOG_MS)
    await until(() => handles.length === 2)
    const [, newHandle] = handles
    await until(() => newHandle.push.mock.calls.length === 1)

    expect(oldHandle.retired).toBe(true)
    expect(client.handle).toBe(newHandle)
    expect(client.candidate.endpointRevision).toBe(2n)
    const state = client.state

    releaseOldResponse()
    for (let turn = 0; turn < 20; turn++) await Promise.resolve()
    await jest.advanceTimersByTimeAsync(1)

    expect(client.handle).toBe(newHandle)
    expect(client.candidate.endpointRevision).toBe(2n)
    expect(client.state).toBe(state)
    expect(transport.open.mock.calls.map(([candidate]) => candidate.host)).toEqual(['a', 'b'])
    expect([oldHandle.get.mock.calls.length, oldHandle.push.mock.calls.length]).toEqual([1, 1])
    expect(newHandle.get).toHaveBeenCalledTimes(1)

    await client.close({ zero: true })
    expect(newHandle.retired).toBe(true)
    expect(jest.getTimerCount()).toBe(0)
  })
})
