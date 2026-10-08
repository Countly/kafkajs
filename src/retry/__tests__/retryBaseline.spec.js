const defaults = require('../defaults')
const { RETRY_CASES, captureRetryTimeline } = require('../../../testHelpers/compatBaseline/capture')
const captured = require('../../../testHelpers/compatBaseline/retryTimeline.json')

describe('Retry > the timeline, against the captured baseline', () => {
  test.each(RETRY_CASES)('$case: the same attempts, delays and outcome', async (testCase) => {
    expect(await captureRetryTimeline(testCase)).toEqual(
      captured.find((entry) => entry.case === testCase.case)
    )
  })

  test('the production defaults the baseline recorded are still the defaults', () => {
    // Jest runs the retrier on its test defaults, so the `{}` case above cannot see these.
    expect(defaults).toEqual(
      captured.find((entry) => entry.case === 'productionDefaultsExhausted').config
    )
  })
})
