const { generateMemberId } = require('../groupProtocol/memberId')

const BASE64URL = /^[A-Za-z0-9_-]{22}$/

// The two ids the generator refuses to hand out, derived here from the bytes rather than imported,
// so a change to the module's own constants is a failing test rather than a silent agreement.
// @see https://github.com/apache/kafka/blob/4.3/clients/src/main/java/org/apache/kafka/common/Uuid.java
const RESERVED = [`${'A'.repeat(22)}`, `${'A'.repeat(21)}Q`]

/**
 * A scripted byte source: hands out the queued buffers in order, then 16 fixed non-reserved bytes.
 * @param {Buffer[]} queue
 * @returns {(size: number) => Buffer}
 */
const scriptedBytes = (queue) => {
  const calls = []
  const fn = (size) => {
    calls.push(size)
    return queue.length ? queue.shift() : Buffer.alloc(16, 0x7f)
  }
  fn.calls = calls
  return fn
}

const zeroBytes = () => Buffer.alloc(16)
const oneBytes = () => {
  const buffer = Buffer.alloc(16)
  buffer[15] = 1
  return buffer
}
/** Leading six bits 111110 => '-' in the base64url alphabet. */
const dashLeadingBytes = () => {
  const buffer = Buffer.alloc(16, 0x11)
  buffer[0] = 0xf8
  return buffer
}

describe('Consumer > groupProtocol > memberId', () => {
  it('is 22 characters of the base64url alphabet, unpadded', () => {
    for (let i = 0; i < 200; i++) {
      const id = generateMemberId()
      expect(id).toMatch(BASE64URL)
      expect(id).not.toContain('=')
      expect(id).not.toContain('+')
      expect(id).not.toContain('/')
    }
  })

  it('pins the two reserved encodings', () => {
    expect(zeroBytes().toString('base64url')).toEqual(RESERVED[0])
    expect(oneBytes().toString('base64url')).toEqual(RESERVED[1])
    expect(RESERVED[0]).toEqual('A'.repeat(22))
    expect(RESERVED[1]).toEqual(`${'A'.repeat(21)}Q`)
  })

  it('regenerates on the all-zero id, the ...0001 id and a leading dash, in that order', () => {
    const randomBytes = scriptedBytes([zeroBytes(), oneBytes(), dashLeadingBytes()])

    const id = generateMemberId({ randomBytes })

    expect(randomBytes.calls).toEqual([16, 16, 16, 16])
    expect(RESERVED).not.toContain(id)
    expect(id.startsWith('-')).toBe(false)
    expect(id).toMatch(BASE64URL)
    expect(id).toEqual(Buffer.alloc(16, 0x7f).toString('base64url'))
  })

  it('draws each reserved value again on its own, not only in sequence', () => {
    for (const reserved of [zeroBytes(), oneBytes(), dashLeadingBytes()]) {
      const randomBytes = scriptedBytes([reserved])
      expect(generateMemberId({ randomBytes })).toEqual(
        Buffer.alloc(16, 0x7f).toString('base64url')
      )
      expect(randomBytes.calls).toEqual([16, 16])
    }
  })

  it('asks for exactly 16 bytes and returns the first acceptable value', () => {
    const randomBytes = scriptedBytes([])

    expect(generateMemberId({ randomBytes })).toEqual(Buffer.alloc(16, 0x7f).toString('base64url'))
    expect(randomBytes.calls).toEqual([16])
  })

  it('produces distinct ids', () => {
    const ids = new Set()
    for (let i = 0; i < 1000; i++) ids.add(generateMemberId())
    expect(ids.size).toEqual(1000)
  })
})
