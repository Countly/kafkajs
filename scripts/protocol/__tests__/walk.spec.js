'use strict'

const path = require('path')
const { readSpec } = require('../../compat/parse')
const { walkBody } = require('../walk')

const MESSAGES = path.join(__dirname, '..', '..', 'compat', 'messages', '4.3.1')
const REQUESTS = path.join(__dirname, '..', '..', '..', 'src', 'protocol', 'requests')

const spec = (name) => readSpec(path.join(MESSAGES, `${name}.json`))
const vector = (api, name) => require(path.join(REQUESTS, api, 'fixtures', 'java', `${name}.json`))
const bytesOf = (api, name) => Buffer.from(vector(api, name).hex, 'hex')

describe('scripts/protocol > walkBody', () => {
  describe('a body written as its version writes it', () => {
    test('walks a classic body: lengths are int16 and int32, and no structure has a tag buffer', () => {
      const request = Buffer.from('0005504c41494e', 'hex') // string "PLAIN"
      const response = Buffer.from('0000' + '00000002' + '0005504c41494e' + '0003414243', 'hex')

      expect(walkBody(spec('SaslHandshakeRequest'), 1, request)).toEqual({ ok: true })
      expect(walkBody(spec('SaslHandshakeResponse'), 1, response)).toEqual({ ok: true })
    })

    test('walks a compact body: lengths are varints of the length plus one, and a tag buffer ends it', () => {
      const body = bytesOf('heartbeat', 'heartbeatRequest-v4-basic')

      expect(walkBody(spec('HeartbeatRequest'), 4, body)).toEqual({ ok: true })
    })

    test.each([
      [
        'a struct inside an array',
        'describeTopicPartitions',
        'describeTopicPartitionsResponse-v0-elrPopulated',
        'DescribeTopicPartitionsResponse',
      ],
      [
        'a nullable struct that is present',
        'describeTopicPartitions',
        'describeTopicPartitionsRequest-v0-cursor',
        'DescribeTopicPartitionsRequest',
      ],
      [
        'a nullable struct that is null',
        'describeTopicPartitions',
        'describeTopicPartitionsRequest-v0-noCursor',
        'DescribeTopicPartitionsRequest',
      ],
      [
        'a null array and a tagged-only version',
        'createTopics',
        'createTopicsResponse-v7-topicConfigError',
        'CreateTopicsResponse',
      ],
    ])('walks %s', (_, api, name, message) => {
      const { version } = vector(api, name)

      expect(walkBody(spec(message), version, bytesOf(api, name))).toEqual({ ok: true })
    })

    test('skips the content of a tag it does not know, and accepts tags in ascending order', () => {
      const heartbeat = bytesOf('heartbeat', 'heartbeatRequest-v4-basic')
      // two tags: tag 0 with two bytes, then tag 1 with none
      const tagged = Buffer.concat([
        heartbeat.subarray(0, -1),
        Buffer.from('02' + '00' + '02' + 'abcd' + '01' + '00', 'hex'),
      ])

      expect(walkBody(spec('HeartbeatRequest'), 4, tagged)).toEqual({ ok: true })
    })
  })

  describe('a body that is not the version it is walked as', () => {
    const heartbeat = bytesOf('heartbeat', 'heartbeatRequest-v4-basic')

    test('compact bytes read as a classic version do not end where the definition ends', () => {
      expect(walkBody(spec('HeartbeatRequest'), 3, heartbeat).ok).toBe(false)
    })

    test('classic bytes read as a flexible version fail', () => {
      const classic = Buffer.from('0005504c41494e', 'hex')

      expect(walkBody(spec('SaslHandshakeRequest'), 1, classic)).toEqual({ ok: true })
      const flexible = { ...spec('SaslHandshakeRequest'), flexibleFrom: 0 }
      expect(walkBody(flexible, 1, classic).ok).toBe(false)
    })

    test('a body without its final tag buffer fails', () => {
      const result = walkBody(spec('HeartbeatRequest'), 4, heartbeat.subarray(0, -1))

      expect(result).toMatchObject({ ok: false, reason: expect.stringContaining('tag count') })
    })

    test('a byte left after the body fails, naming how many', () => {
      const result = walkBody(
        spec('HeartbeatRequest'),
        4,
        Buffer.concat([heartbeat, Buffer.from([0])])
      )

      expect(result).toEqual({
        ok: false,
        reason: expect.stringMatching(/1 byte\(s\) left after the body/),
      })
    })

    test('tags that do not ascend fail', () => {
      const descending = Buffer.concat([
        heartbeat.subarray(0, -1),
        Buffer.from('02' + '01' + '00' + '00' + '00', 'hex'),
      ])

      expect(walkBody(spec('HeartbeatRequest'), 4, descending)).toEqual({
        ok: false,
        reason: expect.stringContaining('is not above'),
      })
    })

    test('a tag larger than the bytes left fails', () => {
      const oversized = Buffer.concat([
        heartbeat.subarray(0, -1),
        Buffer.from('01' + '00' + '09' + 'ab', 'hex'),
      ])

      expect(walkBody(spec('HeartbeatRequest'), 4, oversized).ok).toBe(false)
    })

    test('a varint that never ends fails', () => {
      const endless = Buffer.from('ffffffffffff', 'hex')

      expect(walkBody(spec('HeartbeatRequest'), 4, endless)).toEqual({
        ok: false,
        reason: expect.stringContaining('varint longer than five bytes'),
      })
    })

    test('a string longer than the bytes left fails', () => {
      const long = Buffer.from('0009504c', 'hex')

      expect(walkBody(spec('SaslHandshakeRequest'), 1, long).ok).toBe(false)
    })

    test.each([
      ['heartbeat', 'heartbeatRequest-v4-basic', 'HeartbeatRequest'],
      [
        'describeTopicPartitions',
        'describeTopicPartitionsResponse-v0-elrPopulated',
        'DescribeTopicPartitionsResponse',
      ],
      ['createTopics', 'createTopicsResponse-v7-ok', 'CreateTopicsResponse'],
    ])('every proper prefix of the %s vector fails', (api, name, message) => {
      const { version } = vector(api, name)
      const body = bytesOf(api, name)

      for (let length = 0; length < body.length; length++) {
        expect({
          length,
          ok: walkBody(spec(message), version, body.subarray(0, length)).ok,
        }).toEqual({ length, ok: false })
      }
    })
  })
})
