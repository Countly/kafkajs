const arrayDiff = require('./arrayDiff')

describe('Utils > arrayDiff', () => {
  it('returns the elements in A that are not in B', () => {
    const a = [1, 2, 3, 4]
    const b = [2, 3, 4]
    expect(arrayDiff(a, b)).toEqual([1])
  })

  it('takes null and undefined in consideration', () => {
    const a = [1, 2, 3, 4, null, undefined]
    const b = [2, 3, 4, 5]
    expect(arrayDiff(a, b)).toEqual([1, null, undefined])
  })

  it('returns empty if A is empty', () => {
    const b = [2, 3, 4, 5]
    expect(arrayDiff([], b)).toEqual([])
  })

  it('only takes A in consideration', () => {
    const a = [1, 2, 3]
    const b = [1, 2, 3, 4, 5, 6, 7, 8, 9]
    expect(arrayDiff(a, b)).toEqual([])
  })
})

describe('equivalence with the indexOf implementation', () => {
  // The reference is the original indexOf body, kept here so the Set rewrite is provably behavior-
  // preserving. Set#has uses SameValueZero and indexOf uses strict equality; they differ only
  // for NaN, which never appears in a topic name, partition id or broker id.
  const referenceDiff = (a, b) => {
    const result = []
    for (let i = 0; i < a.length; i++) {
      if (b.indexOf(a[i]) === -1) {
        result.push(a[i])
      }
    }
    return result
  }

  const randomCorpus = () => {
    const pick = () => {
      const r = Math.random()
      if (r < 0.4) return `topic-${Math.floor(Math.random() * 20)}`
      if (r < 0.8) return Math.floor(Math.random() * 20)
      return Math.random() < 0.5 ? null : undefined
    }
    const size = Math.floor(Math.random() * 40)
    return Array.from({ length: size }, pick)
  }

  it('matches the reference on 500 randomized inputs, duplicates and mixed types included', () => {
    for (let i = 0; i < 500; i++) {
      const a = randomCorpus()
      const b = randomCorpus()
      expect(arrayDiff(a, b)).toEqual(referenceDiff(a, b))
    }
  })

  it('preserves duplicates and order', () => {
    expect(arrayDiff([1, 1, 2], [2])).toEqual([1, 1])
    expect(arrayDiff(['a', 'a'], [])).toEqual(['a', 'a'])
    expect(arrayDiff(['c', 'a', 'b'], ['a'])).toEqual(['c', 'b'])
  })

  it('returns an empty array when everything is present in b', () => {
    expect(arrayDiff(['a', 'b'], ['b', 'a'])).toEqual([])
  })
})
