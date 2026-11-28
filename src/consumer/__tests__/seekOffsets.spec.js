const SeekOffsets = require('../seekOffsets')

describe('Consumer > SeekOffsets', () => {
  test('has answers false on an empty map without building a key', () => {
    const seekOffsets = new SeekOffsets()
    const getKey = jest.spyOn(seekOffsets, 'getKey')

    expect(seekOffsets.has('topic', 0)).toBe(false)
    expect(getKey).not.toHaveBeenCalled()
  })

  test('has finds a pending seek and misses every other partition', () => {
    const seekOffsets = new SeekOffsets()
    seekOffsets.set('topic', 1, '5')

    expect(seekOffsets.has('topic', 1)).toBe(true)
    expect(seekOffsets.has('topic', 0)).toBe(false)
    expect(seekOffsets.has('other', 1)).toBe(false)
  })

  test('has answers false again once the last pending seek is popped', () => {
    const seekOffsets = new SeekOffsets()
    seekOffsets.set('topic', 0, '5')

    expect(seekOffsets.pop('topic', 0)).toEqual({ topic: 'topic', partition: 0, offset: '5' })
    expect(seekOffsets.size).toBe(0)
    expect(seekOffsets.has('topic', 0)).toBe(false)
  })
})
