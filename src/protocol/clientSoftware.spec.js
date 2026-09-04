const { CLIENT_SOFTWARE_NAME, clientSoftwareVersion, sanitize } = require('./clientSoftware')

// The broker validates both strings against ([\.\-a-zA-Z0-9])+ (KIP-511) and answers
// INVALID_REQUEST (42) when they do not match.
const BROKER_REGEX = /^([.\-a-zA-Z0-9])+$/

describe('Protocol > client software identity (KIP-511)', () => {
  test('the name matches the broker regex', () => {
    expect(CLIENT_SOFTWARE_NAME).toEqual('countly-kafkajs')
    expect(BROKER_REGEX.test(CLIENT_SOFTWARE_NAME)).toBe(true)
  })

  test('the version comes from package.json and matches the broker regex', () => {
    const { version } = require('../../package.json')
    expect(BROKER_REGEX.test(clientSoftwareVersion())).toBe(true)
    expect(clientSoftwareVersion()).toEqual(sanitize(version))
  })

  test('strips build metadata and replaces illegal characters', () => {
    expect(sanitize('3.0.0')).toEqual('3.0.0')
    expect(sanitize('3.0.0-alpha.4')).toEqual('3.0.0-alpha.4')
    expect(sanitize('3.0.0+build.7')).toEqual('3.0.0')
    expect(sanitize('3.0.0-alpha.4+sha.abc')).toEqual('3.0.0-alpha.4')
    expect(sanitize('3.0.0 rc1')).toEqual('3.0.0-rc1')
    expect(sanitize('3.0.0/local')).toEqual('3.0.0-local')
  })

  test('never returns an empty string', () => {
    expect(sanitize('')).toEqual('unknown')
    expect(sanitize('+++')).toEqual('unknown')
  })
})
