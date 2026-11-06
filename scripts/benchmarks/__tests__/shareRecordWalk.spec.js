const { parseArgs, generate, expansionControl, verify, controls } = require('../shareRecordWalk')
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
      { firstOffset: '503', lastOffset: '1002', deliveryCount: 1 },
    ])
    expect(input.delivered).toBe(500)
    expect(input.gaps).toBe(500)
    expect(() => verify(ShareRecordWalk.walk(input), input)).not.toThrow()
    expect(() => verify(expansionControl(input), input)).not.toThrow()
    expect(controls(input)).toEqual({ validArms: 2, rejectedControls: 4 })
  })
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
