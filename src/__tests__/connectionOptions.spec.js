const { Kafka } = require('../../index')
const { KafkaJSInvalidConfigurationError } = require('../errors')
const config = { brokers: ['node:9092'], logLevel: 0 }

it.each([0, -5, null, NaN])(
  'rejects invalid legacy connection timeout %p at construction',
  (value) => {
    expect(() => new Kafka({ ...config, connectionTimeout: value })).toThrow(
      KafkaJSInvalidConfigurationError
    )
  }
)

it.each([
  'connectionTimeout',
  'connectionTimeoutMax',
  'reconnectBackoff',
  'reconnectBackoffMax',
  'authenticationTimeout',
])('validates the complete %s input matrix', (name) => {
  for (const value of [-1, 0.5, NaN, Infinity, null, 'abc', true, {}, [], 2147483648]) {
    expect(() => new Kafka({ ...config, [name]: value })).toThrow(name)
  }
  if (!name.startsWith('reconnectBackoff'))
    expect(() => new Kafka({ ...config, [name]: 0 })).toThrow(name)
  if (name !== 'connectionTimeout' && name !== 'authenticationTimeout')
    expect(() => new Kafka({ ...config, [name]: '1000' })).toThrow(name)
})

it('keeps numeric strings for the two legacy timeout fields and undefined holes', () => {
  expect(
    () =>
      new Kafka({
        ...config,
        connectionTimeout: '1000',
        authenticationTimeout: '1000',
      })
  ).not.toThrow()
  expect(
    () =>
      new Kafka({
        ...config,
        connectionTimeout: undefined,
        connectionTimeoutMax: 500,
        reconnectBackoff: 0,
      })
  ).not.toThrow()
  expect(
    () =>
      new Kafka({
        ...config,
        connectionTimeout: 1000,
        connectionTimeoutMax: 999,
      })
  ).toThrow('connectionTimeoutMax')
  expect(() => new Kafka({ ...config, reconnectBackoff: 50, reconnectBackoffMax: 49 })).toThrow(
    'reconnectBackoffMax'
  )
})

it('carries normalized options through the real client and constructed connections', async () => {
  const Connection = require('../network/connection')
  const { createScriptedCluster } = require('../../testHelpers/scriptedCluster')
  const seam = createScriptedCluster({
    nodes: [{ id: 0, host: 'node', port: 9092 }],
  })
  const capture = jest.spyOn(Connection.prototype, 'connect')
  const client = new Kafka(
    seam.kafkaOptions({
      connectionTimeout: '3000',
      connectionTimeoutMax: 6000,
      reconnectBackoff: 10,
      reconnectBackoffMax: 20,
      authenticationTimeout: '500',
    })
  )
  const admin = client.admin()
  try {
    await admin.connect()
    const connection = capture.mock.instances[0]
    expect(connection).toMatchObject({
      connectionTimeout: 3000,
      connectionTimeoutMax: 6000,
      reconnectBackoff: 10,
      reconnectBackoffMax: 20,
      authenticationTimeout: 500,
    })
    expect(connection.setupPolicy).toMatchObject({
      timeoutBase: 3000,
      timeoutCeiling: 6000,
      waitBase: 10,
      waitCeiling: 20,
    })
  } finally {
    await admin.disconnect()
    capture.mockRestore()
    seam.assertClean()
  }
})

it.each([
  'connectionTimeout',
  'connectionTimeoutMax',
  'reconnectBackoff',
  'reconnectBackoffMax',
  'authenticationTimeout',
])('undefined holes preserve implicit growth through every constructor: %s', async (name) => {
  const Connection = require('../network/connection')
  const { createScriptedCluster } = require('../../testHelpers/scriptedCluster')
  const seam = createScriptedCluster({ nodes: [{ id: 0, host: 'node', port: 9092 }] })
  const capture = jest.spyOn(Connection.prototype, 'connect')
  const admin = new Kafka({
    ...seam.kafkaOptions(),
    connectionTimeout: undefined,
    [name]: undefined,
  }).admin()
  try {
    await admin.connect()
    const connection = capture.mock.instances[0]
    expect(connection.connectionTimeout).toBe(10000)
    expect(connection.setupPolicy.timeoutCeiling).toBe(30000)
    expect(connection.setupPolicy.timeoutFor(1)).toBeGreaterThanOrEqual(16000)
    expect(connection.setupPolicy.timeoutFor(1)).toBeLessThanOrEqual(24000)
  } finally {
    await admin.disconnect()
    capture.mockRestore()
    seam.assertClean()
  }
})
