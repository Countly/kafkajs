const { plain } = require('../../../__tests__/fixtures/fuzzResponseVectors')
const { expectRefusal } = require('../../../__tests__/fixtures/shareSpecKit')
const fs = require('fs')
const path = require('path')
const codec = require('./response')
const { KafkaJSPartialMessageError } = require('../../../../errors')

const files = [
  'shareGroupHeartbeatResponse-v1-assigned.json',
  'shareGroupHeartbeatResponse-v1-unchanged.json',
  'shareGroupHeartbeatResponse-v1-emptyAssignment.json',
  'shareGroupHeartbeatResponse-v1-error.json',
  'shareGroupHeartbeatResponse-v1-unknownTags.json',
]

describe('independent Java response bytes', () => {
  test.each(files)('decodes %s to its literal fields', async (name) => {
    const vector = JSON.parse(
      fs.readFileSync(path.join(__dirname, '../fixtures/java', name), 'utf8')
    )
    const decoded = await codec.decode(Buffer.from(vector.hex, 'hex'))
    expect(decoded).toMatchObject({
      throttleTime: 0,
      clientSideThrottleTime: vector.fields.throttleTime,
    })
    expect(plain(decoded)).toEqual(vector.fields)
    expect(await codec.parse(decoded)).toBe(decoded)
  })
  test.each(files)('rejects every truncated envelope of %s', async (name) => {
    const vector = JSON.parse(
      fs.readFileSync(path.join(__dirname, '../fixtures/java', name), 'utf8')
    )
    const bytes = Buffer.from(vector.hex, 'hex')
    for (let length = 0; length < bytes.length; length++) {
      await expect(codec.decode(bytes.subarray(0, length))).rejects.toBeInstanceOf(
        KafkaJSPartialMessageError
      )
    }
  })
  test.each(files)('rejects a trailing byte after %s', async (name) => {
    const vector = JSON.parse(
      fs.readFileSync(path.join(__dirname, '../fixtures/java', name), 'utf8')
    )
    await expectRefusal(
      codec.decode(Buffer.concat([Buffer.from(vector.hex, 'hex'), Buffer.from([0])])),
      KafkaJSPartialMessageError,
      'Trailing share response bytes'
    )
  })
})

test.each([-2, 0, 2])('refuses invalid assignment presence %s', async (marker) => {
  const source = require('../fixtures/java/shareGroupHeartbeatResponse-v1-assigned.json')
  const bytes = Buffer.from(source.hex, 'hex')
  bytes.writeInt8(marker, 28)
  await expectRefusal(
    codec.decode(bytes),
    KafkaJSPartialMessageError,
    'Invalid nullable structure marker'
  )
})
