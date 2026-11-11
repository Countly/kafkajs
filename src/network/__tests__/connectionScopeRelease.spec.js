const path = require('node:path')
const { spawnSync } = require('node:child_process')

describe('Network > Connection > request scope release', () => {
  it.each([
    ['consumer', 'settled'],
    ['consumer', 'closed'],
    ['consumer', 'retired'],
    ['admin', 'settled'],
    ['admin', 'closed'],
    ['admin', 'retired'],
    ['discovery', 'settled'],
    ['discovery', 'closed'],
    ['discovery', 'retired'],
  ])('releases the %s scope of a %s request while the connection stays alive', (kind, mode) => {
    const child = spawnSync(
      process.execPath,
      ['--expose-gc', path.join(__dirname, 'fixtures/requestScopeChild.js'), mode, kind],
      { encoding: 'utf8', timeout: 15000 }
    )
    expect(child.error).toBeUndefined()
    expect(child.stderr).toBe('')
    expect(child.status).toBe(0)
    expect(JSON.parse(child.stdout)).toEqual({
      markerCollected: true,
      connected: mode === 'settled',
      queueAlive: true,
      positiveAlive: true,
    })
  })
})
