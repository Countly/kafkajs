const { createGroupApiBroker } = require('./fixtures/groupApiHarness')
const apiKeys = require('../../protocol/requests/apiKeys')
const { requests } = require('../../protocol/requests')
const Decoder = require('../../protocol/decoder')
const { KafkaJSServerDoesNotSupportApiKey } = require('../../errors')

const DIR = '../../protocol/requests/listGroups/fixtures/java/'

// The Java generator emits vectors only for the flexible versions, so the pre-flexible answer is
// hand-built: no throttle, no error, one group carrying the same values the v3+ vectors carry.
const CLASSIC_RESPONSE = '00000000000000000001000a746573742d67726f75700008636f6e73756d6572'

const response = (version) =>
  version >= 3 ? require(`${DIR}listGroupsResponse-v${version}-groups.json`).hex : CLASSIC_RESPONSE

const build = (maxVersion) =>
  createGroupApiBroker({
    versions: { [apiKeys.ListGroups]: { minVersion: 0, maxVersion } },
    respond: () => Buffer.from(response(maxVersion), 'hex'),
  })

/**
 * Reads the filters back off a recorded flexible request body.
 * @param {Buffer} body
 * @param {number} version 3, 4 or 5
 * @returns {{ states: string[]|null, types: string[]|null }} null where the version has no field
 */
const filtersOf = (body, version) => {
  const decoder = new Decoder(body)
  const states = version >= 4 ? decoder.readUVarIntArray((d) => d.readUVarIntString()) : null
  const types = version >= 5 ? decoder.readUVarIntArray((d) => d.readUVarIntString()) : null
  decoder.readTaggedFields()
  return { states, types }
}

describe('Broker > listGroups', () => {
  let harness

  afterEach(async () => {
    if (harness) {
      await harness.pool.destroy()
      harness = null
    }
  })

  test('sends both filters as display names at v5', async () => {
    harness = build(5)

    await harness.broker.listGroups({ states: ['stable'], types: ['consumer'] })

    expect(harness.requests[0].apiVersion).toEqual(5)
    expect(filtersOf(harness.requests[0].body, 5)).toEqual({
      states: ['Stable'],
      types: ['Consumer'],
    })
  })

  test('sends two empty arrays at v5 when no filter is given', async () => {
    harness = build(5)

    await harness.broker.listGroups()

    expect(filtersOf(harness.requests[0].body, 5)).toEqual({ states: [], types: [] })
  })

  test('sends only the state filter at v4', async () => {
    harness = build(4)

    await harness.broker.listGroups({ states: ['stable'] })

    expect(harness.requests[0].apiVersion).toEqual(4)
    expect(filtersOf(harness.requests[0].body, 4)).toEqual({ states: ['Stable'], types: null })
  })

  test.each([[['classic']], [['classic', 'consumer']]])(
    'drops a %p type filter at v4, because dropping it cannot change the answer',
    async (types) => {
      // On a broker with no group types every group is classic, so the unfiltered answer IS the
      // filtered answer. The evidence is byte identity with the no-filter request, not a comment.
      harness = build(4)

      await harness.broker.listGroups({ types })
      await harness.broker.listGroups()

      expect(harness.requests).toHaveLength(2)
      expect(harness.requests[0].body).toEqual(harness.requests[1].body)
    }
  )

  test('refuses a consumer-only type filter at v4, and writes nothing', async () => {
    // Dropping this one would answer "every group in the cluster" to "which groups are consumer
    // groups" - a wrong answer rather than a degraded one, which is why Java throws here too.
    harness = build(4)

    await expect(harness.broker.listGroups({ types: ['consumer'] })).rejects.toThrow(
      'The broker only supports ListGroups v4, but we need v5 or newer to request groups by type. Requested group types: [Consumer].'
    )

    expect(harness.requests).toHaveLength(0)
  })

  test('refuses a state filter at v3, and writes nothing', async () => {
    harness = build(3)

    await expect(harness.broker.listGroups({ states: ['stable'] })).rejects.toThrow(
      'The broker only supports ListGroups v3, but we need v4 or newer to request groups by states.'
    )

    expect(harness.requests).toHaveLength(0)
  })

  test('refuses a filter value Kafka does not define, and writes nothing', async () => {
    harness = build(5)

    await expect(harness.broker.listGroups({ types: ['nope'] })).rejects.toThrow(
      'Invalid group type "nope"; accepted values are classic, consumer, share, streams, unknown'
    )

    expect(harness.requests).toHaveLength(0)
  })

  test('reports an unsupported API rather than a filter downgrade when the key is absent', async () => {
    // With nothing advertised for key 16 the negotiated version is null, and "we need v4" would be
    // the wrong diagnosis: the broker offers no ListGroups at all. That is `lookupRequest`'s error.
    harness = createGroupApiBroker({ versions: {}, respond: () => Buffer.alloc(0) })

    await expect(harness.broker.listGroups({ states: ['stable'] })).rejects.toThrow(
      KafkaJSServerDoesNotSupportApiKey
    )

    expect(harness.requests).toHaveLength(0)
  })

  test('sends the same bytes at v2 as before this change', async () => {
    // The regression guard for every existing caller: v2 is what this client negotiates today.
    harness = build(2)

    await harness.broker.listGroups()

    const expected = await requests.ListGroups.protocol({ version: 2 })().request.encode()

    expect(harness.requests[0].apiVersion).toEqual(2)
    expect(harness.requests[0].body).toEqual(expected.buffer)
  })

  test('reports one row shape at v5 and at v3', async () => {
    harness = build(5)
    const atV5 = await harness.broker.listGroups()
    await harness.pool.destroy()

    harness = build(3)
    const atV3 = await harness.broker.listGroups()

    expect(atV5.groups[0]).toEqual({
      groupId: 'test-group',
      protocolType: 'consumer',
      groupState: 'Stable',
      groupType: 'Consumer',
    })
    expect(atV3.groups[0]).toEqual({
      groupId: 'test-group',
      protocolType: 'consumer',
      groupState: '',
      groupType: '',
    })
  })
})
