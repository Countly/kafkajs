const groupBy = require('./groupBy')

describe('Utils > groupBy', () => {
  it('group items by the function return', async () => {
    const input = [1, 2, 3, 4]
    const groupFn = (item) => (item % 2 === 0 ? 'even' : 'odd')
    const output = new Map([
      ['even', [2, 4]],
      ['odd', [1, 3]],
    ])

    await expect(groupBy(input, groupFn)).resolves.toEqual(output)
  })

  it('works with async functions', async () => {
    const input = [1, 2, 3, 4]
    const groupFn = async (item) => (item % 2 === 0 ? 'even' : 'odd')
    const output = new Map([
      ['even', [2, 4]],
      ['odd', [1, 3]],
    ])

    await expect(groupBy(input, groupFn)).resolves.toEqual(output)
  })

  it('keeps the groups in order of first appearance and the items in input order', async () => {
    const input = [5, 2, 3, 8, 1, 4]
    const groupFn = (item) => (item % 2 === 0 ? 'even' : 'odd')

    const output = await groupBy(input, groupFn)

    expect(Array.from(output)).toEqual([
      ['odd', [5, 3, 1]],
      ['even', [2, 8, 4]],
    ])
  })

  it('answers an empty map for no items', async () => {
    await expect(groupBy([], (item) => item)).resolves.toEqual(new Map())
  })

  it('works with objects as group keys', async () => {
    const even = {}
    const odd = {}

    const input = [1, 2, 3, 4]
    const groupFn = async (item) => (item % 2 === 0 ? even : odd)
    const output = new Map([
      [even, [2, 4]],
      [odd, [1, 3]],
    ])

    await expect(groupBy(input, groupFn)).resolves.toEqual(output)
  })
})
