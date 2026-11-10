const apiKeys = require('../apiKeys')
const { requests, selectVersion } = require('..')
const { KafkaJSServerDoesNotSupportApiKey } = require('../../../errors')

describe('Protocol > Requests > UnregisterController', () => {
  test('registers only version zero of API key 94', () => {
    const definition = requests.UnregisterController
    expect(apiKeys.UnregisterController).toBe(94)
    expect(definition.versions).toEqual(['0'])
    expect(definition.protocol({ version: 1 })).toBeUndefined()

    const built = definition.protocol({ version: 0 })({ controllerId: 7 })
    expect(built.request).toMatchObject({ apiKey: 94, apiVersion: 0 })
    expect(built.response).toEqual(
      expect.objectContaining({ decode: expect.any(Function), parse: expect.any(Function) })
    )

    expect(selectVersion({ 94: { minVersion: 0, maxVersion: 0 } }, 94, definition)).toBe(0)
    expect(() => selectVersion({ 94: { minVersion: 1, maxVersion: 1 } }, 94, definition)).toThrow(
      KafkaJSServerDoesNotSupportApiKey
    )
  })
})
