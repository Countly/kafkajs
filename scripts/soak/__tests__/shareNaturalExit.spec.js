const path = require('path')
const { execFile } = require('child_process')

const ROOT = path.join(__dirname, '..', '..', '..')
const PROBE = path.join(__dirname, '..', 'shareNaturalExit.js')
const BOUND_MS = 60000
const OUTCOMES = ['success', 'zero', 'forced', 'failed', 'crash', 'restart']

/**
 * @returns {Promise<{error: Error|null, stdout: string, stderr: string}>} The probe run as a
 *   foreground child with real timers, killed at a hard bound so a hang fails instead of stalling.
 */
const runProbe = () =>
  new Promise((resolve) => {
    execFile(
      process.execPath,
      [PROBE],
      {
        timeout: BOUND_MS,
        killSignal: 'SIGKILL',
        cwd: ROOT,
        env: { ...process.env, NODE_PATH: ROOT },
      },
      (error, stdout, stderr) => resolve({ error, stdout, stderr })
    )
  })

describe('share consumer natural exit', () => {
  test(
    'leaves no timer, immediate or open socket after any close outcome and exits on its own',
    async () => {
      const { error, stdout, stderr } = await runProbe()

      expect(stderr).toBe('')
      expect(error).toBeNull()
      const lines = stdout.trim().split('\n')
      expect(lines).toHaveLength(1)
      expect(JSON.parse(lines[0])).toEqual({
        event: 'share-natural-exit',
        outcomes: OUTCOMES.map((outcome) => ({
          outcome,
          leaked: {},
          openSockets: 0,
          ...(outcome === 'restart' && {
            restart: { joins: 2, crashes: 1, deliveries: 1, confirmed: 1, restart: true },
          }),
        })),
      })
    },
    BOUND_MS + 15000
  )
})
