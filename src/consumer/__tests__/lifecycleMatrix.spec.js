const { lifecycleCells } = require('../../../testHelpers/lifecycle')
const committed = require('../../../testHelpers/lifecycleCells.json')
const { runLifecycleCell } = require('./fixtures/lifecycleCase')
const { runManualLifecycleCell } = require('./fixtures/manualLifecycleCase')

describe('Consumer lifecycle matrix', () => {
  test('cell catalogue is exactly the committed reviewed list', () => {
    expect(lifecycleCells()).toEqual(committed)
    expect(committed).toHaveLength(434)
    expect(new Set(committed.map((cell) => JSON.stringify(cell))).size).toBe(434)
    expect(committed.filter((cell) => cell.protocol === 'manual')).toHaveLength(108)
  })
  describe.each(committed)('$event at $boundary on $protocol/$membership', (cell) => {
    beforeEach(() => jest.useFakeTimers({ doNotFake: ['setImmediate'] }))
    afterEach(() => {
      jest.useRealTimers()
      jest.restoreAllMocks()
    })
    test('no commit after loss, owed rejoin and settled teardown baseline', async () => {
      await (cell.protocol === 'manual' ? runManualLifecycleCell(cell) : runLifecycleCell(cell))
    })
  })
})
