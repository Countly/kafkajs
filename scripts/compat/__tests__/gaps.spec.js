const { classify, formatVersions } = require('../gaps')

const api = (overrides) =>
  Object.assign(
    {
      validVersions: { min: 0, max: 3 },
      forkVersions: [0, 1],
      hasCodec: true,
      stub: false,
      planned: 'done',
    },
    overrides
  )

describe('scripts/compat/gaps', () => {
  describe('formatVersions', () => {
    it.each([
      [[0, 1, 2, 3, 4, 5, 6, 7], '0-7'],
      [[1, 2, 3, 5], '1-3, 5'],
      [[0], '0'],
      [[5, 3, 4], '3-5'],
      [[], 'none'],
      [null, 'none'],
    ])('formats %p as %p', (versions, expected) => {
      expect(formatVersions(versions)).toBe(expected)
    })
  })

  describe('classify', () => {
    const cases = [
      {
        name: 'current when the fork max equals the Kafka max',
        input: api({ validVersions: { min: 0, max: 1 }, forkVersions: [0, 1] }),
        expected: { gap: 'current', onFloor: false, belowFloor: [] },
      },
      {
        name: 'behind:N counts from the fork max, ignoring holes',
        input: api({ validVersions: { min: 1, max: 10 }, forkVersions: [1, 2, 3, 5] }),
        expected: { gap: 'behind:5', onFloor: false, belowFloor: [] },
      },
      {
        name: 'below-floor versions are listed alongside behind:N (Produce today)',
        input: api({ validVersions: { min: 3, max: 13 }, forkVersions: [0, 1, 2, 3, 4, 5, 6, 7] }),
        expected: { gap: 'behind:6', onFloor: false, belowFloor: [0, 1, 2] },
      },
      {
        name: 'on-floor and below-floor can both apply (DeleteTopics today)',
        input: api({ validVersions: { min: 1, max: 6 }, forkVersions: [0, 1] }),
        expected: { gap: 'behind:5', onFloor: true, belowFloor: [0] },
      },
      {
        name: 'on-floor without below-floor',
        input: api({ validVersions: { min: 2, max: 7 }, forkVersions: [2] }),
        expected: { gap: 'behind:5', onFloor: true, belowFloor: [] },
      },
      {
        name: 'a zero floor is never reported as on-floor',
        input: api({ validVersions: { min: 0, max: 4 }, forkVersions: [0] }),
        expected: { gap: 'behind:4', onFloor: false, belowFloor: [] },
      },
      {
        name: 'stub wins over every version comparison',
        input: api({ validVersions: { min: 2, max: 4 }, forkVersions: null, stub: true }),
        expected: { gap: 'stub', onFloor: false, belowFloor: [] },
      },
      {
        name: 'missing when planned but not implemented',
        input: api({ forkVersions: null, hasCodec: false, planned: 'phase-6' }),
        expected: { gap: 'missing', onFloor: false, belowFloor: [] },
      },
      {
        name: 'not-planned when deliberately unimplemented',
        input: api({ forkVersions: null, hasCodec: false, planned: 'no' }),
        expected: { gap: 'not-planned', onFloor: false, belowFloor: [] },
      },
      {
        name: 'ahead:N when the fork implements a version Kafka does not define',
        input: api({ validVersions: { min: 0, max: 1 }, forkVersions: [0, 1, 99] }),
        expected: { gap: 'ahead:98', onFloor: false, belowFloor: [] },
      },
      {
        name: 'unknown when a codec directory exports an empty versions array',
        input: api({ validVersions: { min: 0, max: 3 }, forkVersions: [] }),
        expected: { gap: 'unknown', onFloor: false, belowFloor: [] },
      },
    ]

    for (const testCase of cases) {
      it(testCase.name, () => {
        expect(classify(testCase.input)).toEqual(testCase.expected)
      })
    }

    // Spec B3 as amended: `gap` carries five values; `on-floor` and `below-floor` are
    // reported as the separate onFloor/belowFloor fields, not as gap values. `ahead` and
    // `unknown` are sentinels for input that must never reach the committed output.
    it('produces the five documented gap values and both sentinels, and nothing else', () => {
      const produced = new Set(cases.map((c) => c.expected.gap.replace(/:\d+$/, '')))
      expect([...produced].sort()).toEqual([
        'ahead',
        'behind',
        'current',
        'missing',
        'not-planned',
        'stub',
        'unknown',
      ])
    })

    it('never returns on-floor or below-floor as a gap value', () => {
      for (const testCase of cases) {
        expect(['on-floor', 'below-floor']).not.toContain(testCase.expected.gap)
      }
    })
  })
})
