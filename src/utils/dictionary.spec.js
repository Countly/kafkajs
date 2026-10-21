const dictionary = require('./dictionary')

const NAMES = ['__proto__', 'constructor', 'toString', 'hasOwnProperty', 'valueOf']

describe('Utils > dictionary', () => {
  test('has no prototype', () => {
    expect(Object.getPrototypeOf(dictionary())).toBeNull()
    expect(Object.keys(dictionary())).toEqual([])
  })

  test.each(NAMES)('%s is stored and read as an own entry, and nothing else changes', (name) => {
    const before = Object.getOwnPropertyDescriptors(Object.prototype)
    const table = dictionary()

    expect(table[name]).toBeUndefined()
    table[name] = { partitions: [0] }

    expect(Object.keys(table)).toEqual([name])
    expect(Object.hasOwn(table, name)).toBe(true)
    expect(table[name]).toEqual({ partitions: [0] })
    expect(Object.getPrototypeOf(table)).toBeNull()
    expect(Object.getOwnPropertyDescriptors(Object.prototype)).toEqual(before)
  })

  test('holds the entries it is given, in order, a later key replacing an earlier one', () => {
    const table = dictionary([
      ['__proto__', 1],
      ['b', 2],
      ['__proto__', 3],
    ])

    expect(Object.entries(table)).toEqual([
      ['__proto__', 3],
      ['b', 2],
    ])
  })

  test('a copy made with Object.assign keeps every name as an own entry', () => {
    const source = dictionary(NAMES.map((name, index) => [name, index]))
    const copy = Object.assign(dictionary(), source, { constructor: 9 })

    expect(Object.entries(copy)).toEqual([
      ['__proto__', 0],
      ['constructor', 9],
      ['toString', 2],
      ['hasOwnProperty', 3],
      ['valueOf', 4],
    ])
    expect(Object.getPrototypeOf(copy)).toBeNull()
  })
})
