const path = require('path')

const ROOT = path.resolve(__dirname, '..', '..')

describe('the compose file the test helpers drive', () => {
  const env = { ...process.env }
  let execFileSync

  /** leaderElection loaded fresh, with `docker` replaced by a recorder. */
  const load = () => {
    execFileSync = jest.fn(() => Buffer.from(''))
    let leaderElection
    jest.isolateModules(() => {
      jest.doMock('child_process', () => ({ execFileSync }))
      leaderElection = require('../leaderElection')
    })
    return leaderElection
  }

  afterEach(() => {
    jest.dontMock('child_process')
    process.env = { ...env }
  })

  test.each([
    ['the root docker-compose.yml by default', undefined, path.join(ROOT, 'docker-compose.yml')],
    [
      'a relative COMPOSE_FILE, from the repository root',
      'docker-compose.acl.yml',
      path.join(ROOT, 'docker-compose.acl.yml'),
    ],
    ['an absolute COMPOSE_FILE, as it is', '/srv/other/compose.yml', '/srv/other/compose.yml'],
  ])('is %s', (_name, composeFile, expected) => {
    if (composeFile === undefined) {
      delete process.env.COMPOSE_FILE
    } else {
      process.env.COMPOSE_FILE = composeFile
    }

    load().execInKafka1(['true'])

    expect(execFileSync.mock.calls[0][1]).toEqual([
      'compose',
      '-f',
      expected,
      'exec',
      '-T',
      'kafka1',
      'true',
    ])
  })

  test('is the same wherever the process was started from', () => {
    delete process.env.COMPOSE_FILE
    const here = process.cwd()
    try {
      process.chdir(path.join(ROOT, 'src'))
      load().compose('ps')
    } finally {
      process.chdir(here)
    }

    expect(execFileSync.mock.calls[0][1].slice(0, 3)).toEqual([
      'compose',
      '-f',
      path.join(ROOT, 'docker-compose.yml'),
    ])
  })
})
