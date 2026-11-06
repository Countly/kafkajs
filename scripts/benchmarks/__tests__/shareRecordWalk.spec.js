const { parseArgs, generate, expansionControl, verify, controls } = require('../shareRecordWalk')
const { ShareRecordWalk } = require('../../../src/consumer/share/shareRecordWalk')
describe('deterministic internal walk benchmark controls', () => {
  test.each(
    [
      [],
      ['--offsets', '100000'],
      ['--offsets', '10000', '--control', '0'],
      ['--offsets', '100000', '--control', '2'],
      ['--offsets', '100000', '--control', '0', '--offsets', '100000'],
      ['--unknown', '1', '--offsets', '100000', '--control', '0'],
    ].map((args) => [args])
  )('rejects invalid CLI %p', (args) => expect(() => parseArgs(args)).toThrow())
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
  test('rejects a duplicate or unacquired gap despite a plausible total', () => {
    const input = generate(500)
    const output = ShareRecordWalk.walk(input)
    expect(() => verify({ ...output, gaps: output.gaps.concat(output.gaps[0]) }, input)).toThrow()
    expect(() =>
      verify(
        { ...output, gaps: [{ firstOffset: '501', lastOffset: '501' }, ...output.gaps.slice(1)] },
        input
      )
    ).toThrow()
  })
})
