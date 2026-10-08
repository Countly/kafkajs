const { errorCodes, createErrorFromCode } = require('../error')
const captured = require('../../../testHelpers/compatBaseline/errorRetriable.json')

describe('Protocol > error table > the retriable column, against the captured baseline', () => {
  test('every captured code keeps its type and its retriable flag', () => {
    // A code added later is not a changed value, so only the captured rows are compared.
    const live = new Map(errorCodes.map((row) => [row.code, row]))
    const drifted = captured.filter(({ code, type, retriable }) => {
      const row = live.get(code)
      return row === undefined || row.type !== type || row.retriable !== retriable
    })

    expect(drifted).toEqual([])
  })

  test('an error built from each captured code reports the captured retriable flag', () => {
    const drifted = captured.filter(
      ({ code, retriable }) => createErrorFromCode(code).retriable !== retriable
    )

    expect(drifted).toEqual([])
  })
})
