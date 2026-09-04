const {
  REQUEST_HEADER_V1,
  REQUEST_HEADER_V2,
  RESPONSE_HEADER_V0,
  RESPONSE_HEADER_V1,
  isFlexible,
  requestHeaderVersion,
  responseHeaderVersion,
} = require('./headers')
const apiKeys = require('./requests/apiKeys')

describe('Protocol > header versions', () => {
  describe('isFlexible', () => {
    test('Metadata is flexible from v9', () => {
      expect(isFlexible(apiKeys.Metadata, 8)).toBe(false)
      expect(isFlexible(apiKeys.Metadata, 9)).toBe(true)
      expect(isFlexible(apiKeys.Metadata, 12)).toBe(true)
    })

    test('ApiVersions is flexible from v3', () => {
      expect(isFlexible(apiKeys.ApiVersions, 2)).toBe(false)
      expect(isFlexible(apiKeys.ApiVersions, 3)).toBe(true)
      expect(isFlexible(apiKeys.ApiVersions, 4)).toBe(true)
    })

    test('SaslHandshake has no flexible version at all', () => {
      expect(isFlexible(apiKeys.SaslHandshake, 0)).toBe(false)
      expect(isFlexible(apiKeys.SaslHandshake, 1)).toBe(false)
    })

    test('the partition-reassignment APIs are flexible from v0', () => {
      expect(isFlexible(apiKeys.AlterPartitionReassignments, 0)).toBe(true)
      expect(isFlexible(apiKeys.ListPartitionReassignments, 0)).toBe(true)
    })

    test('throws for an API key the generated table does not know', () => {
      expect(() => isFlexible(apiKeys.LeaderAndIsr, 0)).toThrow(/flexible-version/)
    })
  })

  describe('requestHeaderVersion', () => {
    test('v1 for classic, v2 for flexible', () => {
      expect(requestHeaderVersion(apiKeys.Metadata, 6)).toEqual(REQUEST_HEADER_V1)
      expect(requestHeaderVersion(apiKeys.Metadata, 12)).toEqual(REQUEST_HEADER_V2)
      expect(requestHeaderVersion(apiKeys.SaslHandshake, 1)).toEqual(REQUEST_HEADER_V1)
    })

    test('ApiVersions requests follow the ordinary rule', () => {
      expect(requestHeaderVersion(apiKeys.ApiVersions, 2)).toEqual(REQUEST_HEADER_V1)
      expect(requestHeaderVersion(apiKeys.ApiVersions, 4)).toEqual(REQUEST_HEADER_V2)
    })
  })

  describe('responseHeaderVersion', () => {
    test('v0 for classic, v1 for flexible', () => {
      expect(responseHeaderVersion(apiKeys.Metadata, 6)).toEqual(RESPONSE_HEADER_V0)
      expect(responseHeaderVersion(apiKeys.Metadata, 12)).toEqual(RESPONSE_HEADER_V1)
    })

    test('ApiVersions responses are always header v0, flexible body or not', () => {
      expect(responseHeaderVersion(apiKeys.ApiVersions, 0)).toEqual(RESPONSE_HEADER_V0)
      expect(responseHeaderVersion(apiKeys.ApiVersions, 3)).toEqual(RESPONSE_HEADER_V0)
      expect(responseHeaderVersion(apiKeys.ApiVersions, 4)).toEqual(RESPONSE_HEADER_V0)
    })
  })
})
