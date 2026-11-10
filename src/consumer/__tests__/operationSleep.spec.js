const { ConsumerOperation } = require('../groupProtocol/interface')

describe('ConsumerOperation sleep', () => {
  afterEach(() => jest.restoreAllMocks())

  test('schedules a remaining time that already passed as an immediate timer, never a negative one', async () => {
    const timer = jest.spyOn(global, 'setTimeout')
    const operation = new ConsumerOperation()
    await operation.sleep(-0.0013)
    const delays = timer.mock.calls.map(([, delay]) => delay)
    expect(delays).toContain(0)
    expect(delays.every((delay) => delay >= 0)).toBe(true)
  })

  test('keeps a positive delay unchanged', async () => {
    const timer = jest.spyOn(global, 'setTimeout')
    const operation = new ConsumerOperation()
    await operation.sleep(5)
    expect(timer.mock.calls.map(([, delay]) => delay)).toContain(5)
  })
})
