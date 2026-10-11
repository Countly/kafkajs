const InstrumentationEventEmitter = require('../emitter')
const baseline = require('../../../testHelpers/compatBaseline/instrumentationEvents.json')
const { captureInstrumentationEvents } = require('../../../testHelpers/compatBaseline/capture')

const KINDS = ['producer', 'consumer', 'admin']
const COUNTS = { producer: 10, consumer: 24, admin: 8 }

describe('Instrumentation > public event baseline', () => {
  test('the committed baseline is what the tree emits and delivers to a listener', () => {
    expect(captureInstrumentationEvents()).toEqual(baseline)
  })

  test.each(KINDS)('%s: lists its public events, each prefixed with the client kind', (kind) => {
    const names = baseline[kind].map(({ publicName }) => publicName)

    expect(names).toHaveLength(COUNTS[kind])
    expect(names).toEqual([...names].sort())
    expect(names.every((name) => name.startsWith(`${kind}.`))).toBe(true)
  })

  test('forty-two distinct public names across the three clients', () => {
    const names = KINDS.flatMap((kind) => baseline[kind].map(({ publicName }) => publicName))

    expect(new Set(names).size).toEqual(42)
    expect(Object.keys(baseline.listener).sort()).toEqual([...names].sort())
  })

  test('only the network and cluster events are emitted under an internal name', () => {
    const renamed = KINDS.flatMap((kind) => baseline[kind]).filter(
      ({ publicName, internalName }) => publicName !== internalName
    )

    expect(renamed).toHaveLength(19)
    expect(renamed.every(({ internalName }) => /^(network|cluster)\./.test(internalName))).toBe(
      true
    )
  })

  test.each(Object.entries(baseline.listener))(
    '%s: a listener receives the public name as the type and exactly id, payload, timestamp, type',
    (publicName, { type, keys }) => {
      expect(type).toEqual(publicName)
      expect(keys).toEqual(['id', 'payload', 'timestamp', 'type'])
    }
  )

  test('an event carries the payload object it was emitted with, a numeric id and a timestamp', () => {
    const emitter = new InstrumentationEventEmitter()
    const payload = { probe: true }
    const received = []
    emitter.addListener('baseline.event', (event) => received.push(event))

    const before = Date.now()
    emitter.emit('baseline.event', payload)

    expect(received).toHaveLength(1)
    expect(received[0].type).toEqual('baseline.event')
    expect(received[0].payload).toBe(payload)
    expect(typeof received[0].id).toEqual('number')
    expect(received[0].timestamp).toBeGreaterThanOrEqual(before)
  })
})
