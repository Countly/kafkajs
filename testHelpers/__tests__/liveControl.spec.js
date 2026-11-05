const fs = require('fs')
const os = require('os')
const path = require('path')
const { AssignmentGate } = require('../consumerGroupStates/gate')

describe('standalone live controls', () => {
  const env = { ...process.env }
  let execFile
  let helpers

  beforeEach(() => {
    delete process.env.P9_CLUSTER_GUARD
    delete process.env.P9_CLUSTER_MANIFEST
    process.env.COMPOSE_FILE = 'docker-compose.racks.yml'
    execFile = jest.fn((command, args, options, callback) => callback(null, 'ok'))
    jest.isolateModules(() => {
      jest.doMock('child_process', () => ({ execFile }))
      helpers = require('../liveControl')
    })
  })

  afterEach(() => {
    jest.dontMock('child_process')
    process.env = { ...env }
  })

  test.each([
    ['stop', 20000],
    ['start', 15000],
  ])('%s uses bounded standalone Compose', async (operation, budget) => {
    const signal = new AbortController().signal
    await helpers.createBrokerRun({ stop: 20000, start: 15000 })(operation, 'kafka3', { signal })
    expect(execFile.mock.calls[0].slice(0, 3)).toEqual([
      'docker',
      [
        'compose',
        '-f',
        path.resolve(__dirname, '../../docker-compose.racks.yml'),
        operation,
        'kafka3',
      ],
      { signal, timeout: budget, killSignal: 'SIGKILL' },
    ])
  })

  test.each([
    ['stop', 20000],
    ['start', 15000],
  ])('%s preserves guarded argv and options', async (operation, budget) => {
    process.env.P9_CLUSTER_GUARD = 'guard.js'
    process.env.P9_CLUSTER_MANIFEST = 'manifest.json'
    const signal = new AbortController().signal
    await helpers.createBrokerRun({ stop: 20000, start: 15000 })(operation, 'kafka3', { signal })
    expect(execFile.mock.calls[0].slice(0, 3)).toEqual([
      'timeout',
      [`${budget / 1000}s`, 'node', 'guard.js', 'control', 'manifest.json', operation, 'kafka3'],
      { signal },
    ])
  })

  test('propagates a failed standalone broker start', async () => {
    const error = new Error('Broker start failed')
    execFile.mockImplementationOnce((command, args, options, callback) => callback(error))
    await expect(helpers.createBrokerRun({ start: 15000 })('start', 'kafka3')).rejects.toThrow(
      'Broker start failed'
    )
  })

  test.each([
    [
      'standalone',
      (operation) => [
        'docker',
        [
          'compose',
          '-f',
          path.resolve(__dirname, '../../docker-compose.racks.yml'),
          operation,
          'kafka2',
        ],
      ],
    ],
    [
      'guarded',
      (operation) => [
        'timeout',
        ['15s', 'node', 'guard.js', 'control', 'manifest.json', operation, 'kafka2'],
      ],
      { P9_CLUSTER_GUARD: 'guard.js', P9_CLUSTER_MANIFEST: 'manifest.json' },
    ],
  ])('%s resume unpauses a broker before it starts it', async (mode, argv, guard) => {
    Object.assign(process.env, guard)
    const resume = helpers.createBrokerResume(
      helpers.createBrokerRun({ unpause: 15000, start: 15000 })
    )

    await expect(resume('kafka2')).resolves.toBeUndefined()

    expect(execFile.mock.calls.map(([command, args]) => [command, args])).toEqual([
      argv('unpause'),
      argv('start'),
    ])
  })

  test('resume still starts a broker whose unpause failed because it was not paused', async () => {
    execFile.mockImplementationOnce((command, args, options, callback) =>
      callback(new Error('Container is not paused'))
    )
    const resume = helpers.createBrokerResume(
      helpers.createBrokerRun({ unpause: 15000, start: 15000 })
    )

    await expect(resume('kafka2')).resolves.toBeUndefined()

    expect(execFile.mock.calls.map(([, args]) => args.slice(-2))).toEqual([
      ['unpause', 'kafka2'],
      ['start', 'kafka2'],
    ])
  })

  test('resume rejects with the failure of the start that follows its unpause', async () => {
    execFile
      .mockImplementationOnce((command, args, options, callback) => callback(null, 'ok'))
      .mockImplementationOnce((command, args, options, callback) =>
        callback(new Error('Broker start failed'))
      )
    const resume = helpers.createBrokerResume(
      helpers.createBrokerRun({ unpause: 15000, start: 15000 })
    )

    await expect(resume('kafka2')).rejects.toThrow('Broker start failed')

    expect(execFile.mock.calls.map(([, args]) => args.slice(-2))).toEqual([
      ['unpause', 'kafka2'],
      ['start', 'kafka2'],
    ])
  })

  test('resume refuses a runner with no unpause budget before it starts anything', async () => {
    const resume = helpers.createBrokerResume(helpers.createBrokerRun({ start: 15000 }))

    await expect(resume('kafka2')).rejects.toThrow('No guard budget for unpause')

    expect(execFile).not.toHaveBeenCalled()
  })

  test.each(['P9_CLUSTER_GUARD', 'P9_CLUSTER_MANIFEST'])(
    'refuses an incomplete environment with only %s',
    (variable) => {
      process.env[variable] = 'present'
      expect(() => helpers.createBrokerRun({ stop: 20000 })('stop', 'kafka3')).toThrow(
        'Incomplete cluster ownership environment'
      )
      expect(() => helpers.gateCommand('arm', 'group', 'nonce')).toThrow(
        'Incomplete cluster ownership environment'
      )
      expect(execFile).not.toHaveBeenCalled()
    }
  )

  test('refuses operations without a budget on either path', () => {
    const run = helpers.createBrokerRun({ stop: 20000 })
    expect(() => run('kill', 'kafka3')).toThrow('No guard budget for kill')
    process.env.P9_CLUSTER_GUARD = 'guard.js'
    process.env.P9_CLUSTER_MANIFEST = 'manifest.json'
    expect(() => run('kill', 'kafka3')).toThrow('No guard budget for kill')
    expect(execFile).not.toHaveBeenCalled()
  })

  test.each(['arm', 'release', 'read', 'retire'])('%s selects the state controller', (action) => {
    expect(helpers.gateCommand(action, 'group', 'nonce')).toEqual([
      require.resolve('../consumerGroupStates/gate'),
      action,
      'group',
      'nonce',
    ])
    process.env.P9_CLUSTER_GUARD = 'guard.js'
    process.env.P9_CLUSTER_MANIFEST = 'manifest.json'
    expect(helpers.gateCommand(action, 'group', 'nonce')).toEqual([
      'guard.js',
      'gate',
      'manifest.json',
      action,
      'group',
      'nonce',
    ])
  })
})

describe('standalone assignment fixture', () => {
  let directory
  let gate

  beforeEach(() => {
    directory = fs.mkdtempSync(path.join(os.tmpdir(), 'assignment-gate-'))
    gate = new AssignmentGate(directory)
  })

  afterEach(() => fs.rmSync(directory, { recursive: true, force: true }))

  test('arms, releases, filters complete events and retires the shared gate', () => {
    fs.writeFileSync(path.join(directory, 'release'), 'old')
    expect(gate.run('arm', 'group', 'nonce')).toEqual([])
    expect(fs.readFileSync(path.join(directory, 'arm'), 'utf8')).toBe('group\nnonce\n')
    expect(fs.existsSync(path.join(directory, 'release'))).toBe(false)
    const event = { groupId: 'group', nonce: 'nonce', status: 'arrived' }
    fs.writeFileSync(
      path.join(directory, 'events.jsonl'),
      [event, { ...event, nonce: 'old' }].map(JSON.stringify).join('\n') + '\n{"partial":'
    )
    expect(gate.run('read', 'group', 'nonce')).toEqual([event])
    expect(gate.run('release', 'group', 'nonce')).toEqual([event])
    expect(fs.readFileSync(path.join(directory, 'release'), 'utf8')).toBe('nonce\n')
    expect(gate.run('retire', 'group', 'nonce')).toEqual([event])
    expect(fs.readdirSync(directory)).toEqual(['events.jsonl'])
  })

  test.each([
    ['an unsupported action', 'unknown', 'group', 'nonce'],
    ['a multiline group', 'arm', 'group\ninjected', 'nonce'],
    ['a path in the nonce', 'arm', 'group', '../nonce'],
  ])('refuses invalid operands with %s', (label, ...args) => {
    expect(() => gate.run(...args)).toThrow('Invalid assignment gate operation')
    expect(fs.readdirSync(directory)).toEqual([])
  })
})
