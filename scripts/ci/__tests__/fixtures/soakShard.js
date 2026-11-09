const fs = require('fs')

describe('shard fixture', () => {
  let ready = false
  beforeAll(() => {
    ready = true
  })
  afterAll(() => {
    expect(ready).toBe(true)
  })
  afterEach(() => {
    fs.appendFileSync(process.env.SOAK_SHARD_LOG, 'afterEach\n')
  })

  test.each([0, 1, 2, 3, 4, 5])('case %i', (index) => {
    expect(ready).toBe(true)
    fs.appendFileSync(process.env.SOAK_SHARD_LOG, `${index}\n`)
  })
  describe('nested', () => {
    test('case 6', () => {
      expect(ready).toBe(true)
      fs.appendFileSync(process.env.SOAK_SHARD_LOG, '6\n')
    })
  })
  test.skip('must remain skipped', () => {
    throw new Error('A shard enabled a skipped test')
  })
  test.todo('must remain todo')
})
