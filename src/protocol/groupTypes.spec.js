const { GROUP_TYPES, GROUP_STATES, toDisplayName } = require('./groupTypes')

describe('Protocol > group types and states', () => {
  test('maps every group type Kafka 4.3 defines', () => {
    expect(GROUP_TYPES).toEqual({
      unknown: 'Unknown',
      consumer: 'Consumer',
      classic: 'Classic',
      share: 'Share',
      streams: 'Streams',
    })
  })

  test('maps every group state Kafka 4.3 defines, including the two only consumer groups reach', () => {
    expect(GROUP_STATES.assigning).toEqual('Assigning')
    expect(GROUP_STATES.reconciling).toEqual('Reconciling')
    expect(GROUP_STATES.preparingrebalance).toEqual('PreparingRebalance')
    expect(GROUP_STATES.completingrebalance).toEqual('CompletingRebalance')
    expect(Object.keys(GROUP_STATES)).toHaveLength(9)
  })

  test('rejects a value it does not know, naming the accepted ones', () => {
    expect(() => toDisplayName(GROUP_TYPES, 'nope', 'group type')).toThrow(
      'Invalid group type "nope"; accepted values are classic, consumer, share, streams, unknown'
    )
  })

  test('accepts the value in any casing, because the wire form is a display name', () => {
    expect(toDisplayName(GROUP_STATES, 'STABLE', 'group state')).toEqual('Stable')
  })
})
