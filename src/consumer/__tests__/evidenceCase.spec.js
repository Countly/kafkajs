const { CleanupStack } = require('../../producer/__tests__/fixtures/liveProbe')
const { runWithEvidence } = require('./fixtures/evidenceCase')

describe('Consumer > runWithEvidence', () => {
  it('keeps the failure of a failed body as the thrown error and attaches the failed evidence write to it', async () => {
    const bodyFailure = new Error('body failed')
    const writeFailure = new Error('disk full')
    const order = []
    const cleanup = new CleanupStack()
    cleanup.add('closes', () => order.push('cleanup'))
    const write = jest.fn(async () => {
      order.push('write')
      return writeFailure
    })

    await expect(
      runWithEvidence(
        cleanup,
        write,
        async () => {
          throw bodyFailure
        },
        5000
      )
    ).rejects.toBe(bodyFailure)

    expect(bodyFailure.evidenceError).toBe(writeFailure)
    expect(write).toHaveBeenCalledTimes(1)
    expect(write).toHaveBeenCalledWith(bodyFailure)
    expect(order).toEqual(['write', 'cleanup'])
  })
})
