const {
  parseArgs,
  generate,
  expansionControl,
  verify,
  controls,
  DELIVERY_CORRUPTIONS,
  observe,
  failedGates,
} = require('../shareRecordWalk')
const { ShareRecordWalk } = require('../../../src/consumer/share/shareRecordWalk')
const failure = (name, message) => expect.objectContaining({ name, message })
const span = (first, last) => ({ firstOffset: String(first), lastOffset: String(last) })
const width = (gaps) =>
  gaps.reduce((sum, gap) => sum + BigInt(gap.lastOffset) - BigInt(gap.firstOffset) + 1n, 0n)
const USAGE = 'Expected --offsets 100000|1000000 --control 0|1'
const INVALID = 'Invalid offsets or control value'
describe('deterministic internal walk benchmark controls', () => {
  test.each([
    [[], INVALID],
    [['--offsets'], USAGE],
    [['--offsets', '100000'], INVALID],
    [['--offsets', '10000', '--control', '0'], INVALID],
    [['--offsets', '100000', '--control', '2'], INVALID],
    [['--offsets', '100000', '--control', '0', '--offsets', '100000'], USAGE],
    [['--unknown', '1', '--offsets', '100000', '--control', '0'], USAGE],
  ])('rejects invalid CLI %p', (args, message) =>
    expect(() => parseArgs(args)).toThrow(failure('Error', message))
  )
  test.each([100000, 1000000])('admits declared size %i without running a benchmark', (offsets) =>
    expect(parseArgs(['--offsets', String(offsets), '--control', '1'])).toEqual({
      offsets,
      control: 1,
    })
  )
  test('independent generator verifies both algorithms and rejects skipped/fabricated work', () => {
    const input = generate(1000)
    expect(input.acquiredRecords).toEqual([
      { firstOffset: '0', lastOffset: '499', deliveryCount: 1 },
      { firstOffset: '503', lastOffset: '1002', deliveryCount: 32767 },
    ])
    expect(input.delivered).toBe(500)
    expect(input.gaps).toBe(500)
    expect(() => verify(ShareRecordWalk.walk(input), input)).not.toThrow()
    expect(() => verify(expansionControl(input), input)).not.toThrow()
    expect(controls(input)).toEqual({ validArms: 2, rejectedControls: 13 })
  })
  test.each([
    ['the walk', ShareRecordWalk.walk],
    ['the expansion control', expansionControl],
  ])('%s pairs the 1 and 32767 boundary counts with the right records', (_label, arm) => {
    const input = generate(1000)
    const { records, deliveryCounts } = arm(input).deliver
    expect(records).toHaveLength(500)
    expect(deliveryCounts).toEqual(
      Uint16Array.from([...Array(250).fill(1), ...Array(250).fill(32767)])
    )
    records.forEach((record, i) => expect(record).toBe(input.messages[i]))
  })
  test.each(DELIVERY_CORRUPTIONS.map(({ label, message, corrupt }) => [label, message, corrupt]))(
    'rejects a corrupt delivery output: %s',
    (_label, message, corrupt) => {
      const input = generate(1000)
      const baseline = ShareRecordWalk.walk(input)
      expect(() => verify(baseline, input)).not.toThrow()
      expect(() => verify(corrupt(baseline, input), input)).toThrow(
        failure('AssertionError', message)
      )
    }
  )
  test.each([
    [
      'the first gap repeated in place of the second',
      (gaps) => [gaps[0], gaps[0]],
      'gaps must ascend without overlap',
    ],
    [
      'the second gap replaced by an equal-width interval past the acquired span',
      (gaps) => [gaps[0], span(1003, 1252)],
      'gap lies outside the acquired span',
    ],
    [
      'the second gap replaced by an equal-width interval over unacquired offsets',
      (gaps) => [gaps[0], span(500, 749)],
      'gap crosses an unacquired offset',
    ],
    [
      'the first gap replaced by an equal-width interval over delivered offsets',
      (gaps) => [span(0, 249), gaps[1]],
      'gap covers a delivered offset',
    ],
  ])('rejects a gap corruption that keeps the total: %s', (_label, corrupt, message) => {
    const input = generate(1000)
    const output = ShareRecordWalk.walk(input)
    expect(output.gaps).toEqual([span(250, 499), span(753, 1002)])
    const gaps = corrupt(output.gaps)
    expect(width(gaps)).toBe(BigInt(input.gaps))
    expect(() => verify({ ...output, gaps }, input)).toThrow(failure('AssertionError', message))
  })
})

describe('benchmark memory reporting and gates', () => {
  const usage = (heapUsed, arrayBuffers) => ({ heapUsed, arrayBuffers })
  beforeEach(() => {
    delete global.gc
  })
  afterEach(() => {
    jest.restoreAllMocks()
    delete global.gc
  })
  test('requires an explicit garbage collector', () => {
    expect(() => observe(generate(1000), ShareRecordWalk.walk)).toThrow(
      failure('Error', 'Run the internal-API benchmark with node --expose-gc')
    )
  })
  test('reports heap and array-buffer deltas separately, clamped at zero, and their total', () => {
    global.gc = jest.fn()
    jest
      .spyOn(process, 'memoryUsage')
      .mockReturnValueOnce(usage(1000, 500))
      .mockReturnValueOnce(usage(400, 900))
    const result = observe(generate(1000), ShareRecordWalk.walk)
    expect(global.gc).toHaveBeenCalledTimes(2)
    expect(result.retainedHeap).toBe(0)
    expect(result.retainedBuffers).toBe(400)
    expect(result.retainedTotal).toBe(400)
    expect(result.deliveries).toBe(500)
    expect(result.bytesPerDelivery).toBe(400 / 500)
  })
  test('sums two positive deltas into the total', () => {
    global.gc = jest.fn()
    jest
      .spyOn(process, 'memoryUsage')
      .mockReturnValueOnce(usage(1000, 500))
      .mockReturnValueOnce(usage(1700, 800))
    const result = observe(generate(1000), expansionControl)
    expect([result.retainedHeap, result.retainedBuffers, result.retainedTotal]).toEqual([
      700, 300, 1000,
    ])
    expect(result.bytesPerDelivery).toBe(1000 / 500)
  })
  const MIB = 1024 * 1024
  const medians = (walk, control) => ({
    medians: {
      walk: { nsPerOffset: 1, retainedHeap: 0, retainedTotal: 0, ...walk },
      control: { nsPerOffset: 1, retainedHeap: 100 * MIB, retainedTotal: 100 * MIB, ...control },
    },
  })
  test('passes when time, heap and total are within every bound', () => {
    const ok = medians({ retainedHeap: 5 * MIB, retainedTotal: 6 * MIB }, {})
    expect(failedGates([ok, ok], 1000000)).toEqual([])
    expect(failedGates([ok], 100000)).toEqual([])
  })
  test('fails the total ratio even when heap alone is within bounds', () => {
    const offHeap = medians({ retainedHeap: MIB, retainedTotal: 20 * MIB }, {})
    expect(failedGates([offHeap], 100000)).toEqual(['walk total exceeds control ratio'])
  })
  test('fails the ten MiB total ceiling only at one million offsets', () => {
    const large = medians(
      { retainedHeap: 5 * MIB, retainedTotal: 10 * MIB + 1 },
      { retainedHeap: 1000 * MIB, retainedTotal: 1000 * MIB }
    )
    expect(failedGates([large], 1000000)).toEqual(['walk total exceeds ten MiB'])
    expect(failedGates([large], 100000)).toEqual([])
  })
  test('fails the heap ceiling and time bound for each session', () => {
    const slow = medians(
      { nsPerOffset: 1.5001, retainedHeap: 10 * MIB + 1, retainedTotal: 10 * MIB + 1 },
      { retainedHeap: 1000 * MIB, retainedTotal: 1000 * MIB }
    )
    expect(failedGates([slow, slow], 1000000)).toEqual([
      'walk time exceeds control bound',
      'walk heap exceeds ten MiB',
      'walk total exceeds ten MiB',
      'walk time exceeds control bound',
      'walk heap exceeds ten MiB',
      'walk total exceeds ten MiB',
    ])
  })
})
