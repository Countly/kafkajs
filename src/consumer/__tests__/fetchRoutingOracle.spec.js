const { MOVE_WAIT_MS, ROUND_ALLOWANCE_MS, moveProblems } = require('./fixtures/fetchRoutingOracle')

const busy = {
  kind: 'busy',
  hinted: true,
  newNode: 2,
  hint: { leaderId: 2 },
  anyHint: true,
  refreshed: true,
  priorFetches: 1,
  pollWriteAt: 9000,
  routeAt: 10000,
  pollDoneAt: 10500,
  nextAt: 10600,
}
const idle = { ...busy, kind: 'idle', priorFetches: 0, pollWriteAt: null, pollDoneAt: null }

describe('Consumer > Fetch routing oracle (offline controls)', () => {
  it('passes a long poll that is outstanding at the route', () => {
    expect(moveProblems(busy)).toEqual([])
  })

  it('fails a long poll that completed before the move', () => {
    const done = { ...busy, routeAt: 10000, pollDoneAt: 9900, nextAt: 10100 }
    expect(moveProblems(done)).toContain('the long poll was not outstanding at the route')
  })

  it('fails a poll answered at the route instant', () => {
    expect(moveProblems({ ...busy, pollDoneAt: 10000, nextAt: 10100 })).not.toEqual([])
  })

  it('fails a poll written after the route', () => {
    expect(moveProblems({ ...busy, pollWriteAt: 10001 })).not.toEqual([])
  })

  it('fails a busy move with no prior Fetch, never an idle pass', () => {
    const none = { ...busy, priorFetches: 0, pollWriteAt: null, pollDoneAt: null, nextAt: 10100 }
    expect(moveProblems(none)).toContain('the new node had no long poll outstanding')
  })

  it('fails a late route', () => {
    const late = { ...busy, nextAt: busy.nextAt + MOVE_WAIT_MS + 2 * ROUND_ALLOWANCE_MS }
    expect(moveProblems(late)).not.toEqual([])
  })

  it('requires the hint when hinted and none when not', () => {
    expect(moveProblems({ ...busy, hint: null })).not.toEqual([])
    const unhinted = { ...busy, hinted: false, hint: null, anyHint: false }
    expect(moveProblems(unhinted)).toEqual([])
    expect(moveProblems({ ...unhinted, anyHint: true })).not.toEqual([])
    expect(moveProblems({ ...unhinted, refreshed: false })).not.toEqual([])
  })

  it('keeps the idle bounds', () => {
    expect(moveProblems(idle)).toEqual([])
    expect(moveProblems({ ...idle, priorFetches: 1 })).not.toEqual([])
    expect(moveProblems({ ...idle, nextAt: idle.routeAt + ROUND_ALLOWANCE_MS + 1 })).not.toEqual([])
  })
})
