const { SetupBenchmark } = require('../connectSetup')

it('counts actual socket establishments and negotiations outside cleanup time', async () => {
  const result = await new SetupBenchmark({ episodes: 2 }).run()
  expect(result).toMatchObject({
    episodes: 2,
    establishments: 2,
    negotiations: 2,
    sockets: 0,
    timers: 0,
  })
  expect(result.usPerSetup).toBeGreaterThan(0)
  expect(result.loadBefore).toHaveLength(3)
  expect(result.loadAfter).toHaveLength(3)
})

it('refuses healthy no-op connects as a setup denominator', async () => {
  await expect(new SetupBenchmark({ episodes: 2, reuse: true }).run()).rejects.toThrow(
    'distinct client establishments'
  )
})

it('declares the production denominator and the positive-control mode', () => {
  const normal = new SetupBenchmark()
  const control = new SetupBenchmark({ control: true })
  expect(normal.episodes).toBe(2000)
  expect(control.control).toBe(true)
})
