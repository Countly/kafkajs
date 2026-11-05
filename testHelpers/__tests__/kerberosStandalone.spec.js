const fs = require('node:fs')
const os = require('node:os')
const path = require('node:path')
const vm = require('node:vm')
const { StandaloneKdc, parseTgt, parseCommitted } = require('../kerberos/standalone')
const KdcFixture = require('../kerberos')

const HOUR = 3600000
const START = Date.UTC(2026, 10, 5, 14, 0, 0)

/**
 * @param {number} ms UTC milliseconds
 * @param {boolean} [fullYear] four-digit year, as MIT `klist` prints on some builds
 * @returns {string} `MM/DD/YY HH:MM:SS`
 */
const stamp = (ms, fullYear = false) => {
  const date = new Date(ms)
  const two = (value) => String(value).padStart(2, '0')
  const year = fullYear ? date.getUTCFullYear() : String(date.getUTCFullYear()).slice(2)
  return `${two(date.getUTCMonth() + 1)}/${two(date.getUTCDate())}/${year} ${two(date.getUTCHours())}:${two(date.getUTCMinutes())}:${two(date.getUTCSeconds())}`
}

/**
 * @param {number} issued start in UTC milliseconds
 * @param {number} expires end in UTC milliseconds
 * @param {string} [principal] default principal
 * @returns {string} `klist` output
 */
const klist = (issued, expires, principal = 'client@KAFKAJS.TEST') =>
  `Ticket cache: FILE:/tmp/x.ccache\nDefault principal: ${principal}\n\nValid starting     Expires            Service principal\n${stamp(issued)}  ${stamp(expires)}  krbtgt/KAFKAJS.TEST@KAFKAJS.TEST\n`

/**
 * A fake `docker` whose answers are scripted by the command run inside a service.
 * @param {{lifetimeMs: number, issuedAt: number, listing?: string, statusValue?: number, outputs?: Record<string, string>}} script
 * @returns {{calls: string[][], controller: StandaloneKdc, clock: {time: number, slept: number[]}, directory: string}}
 */
const fixture = ({ lifetimeMs, issuedAt, statusValue = 1, outputs = {} }) => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'kdc-'))
  const clock = { time: issuedAt, slept: [] }
  const calls = []
  const run = (argv) => {
    calls.push(argv)
    if (argv.includes('klist')) return klist(issuedAt, issuedAt + lifetimeMs)
    if (argv[3] === 'cp') {
      fs.writeFileSync(argv[argv.length - 1], 'cache')
      return ''
    }
    const tool = argv.find((arg) => arg.startsWith('/opt/kafka/bin/'))
    return tool ? outputs[path.basename(tool)] : ''
  }
  const status = (argv) => {
    calls.push(argv)
    return statusValue
  }
  const controller = new StandaloneKdc({
    directory,
    run,
    status,
    now: () => clock.time,
    sleep: (ms) => {
      clock.slept.push(ms)
      clock.time += ms
    },
  })
  return { calls, controller, clock, directory }
}

describe('standalone KDC fixture', () => {
  const directories = []
  const make = (script) => {
    const made = fixture(script)
    directories.push(made.directory)
    return made
  }
  afterEach(() => {
    for (const directory of directories.splice(0))
      fs.rmSync(directory, { recursive: true, force: true })
  })

  describe('parseTgt', () => {
    test('reads the same instants from two-digit and four-digit years', () => {
      const text = (full) =>
        `Default principal: client@KAFKAJS.TEST\n\nValid starting     Expires            Service principal\n${stamp(START, full)}  ${stamp(START + HOUR, full)}  krbtgt/KAFKAJS.TEST@KAFKAJS.TEST\n`
      expect(parseTgt(text(false))).toEqual({ issuedAtMs: START, expiresAtMs: START + HOUR })
      expect(parseTgt(text(true))).toEqual({ issuedAtMs: START, expiresAtMs: START + HOUR })
    })

    test('refuses a cache of another principal', () => {
      expect(() => parseTgt(klist(START, START + HOUR, 'other@KAFKAJS.TEST'))).toThrow(
        'no krbtgt row for the client principal'
      )
    })
  })

  describe('ticket', () => {
    test('issues the named lifetime from the keytab and copies only the cache, mode 0600', () => {
      const { controller, calls, directory } = make({ lifetimeMs: HOUR, issuedAt: START })
      const record = controller.control('gssapi-ticket', ['fresh'])
      const kinit = calls.find((argv) => argv.includes('kinit'))
      expect(kinit.slice(kinit.indexOf('kinit'))).toEqual([
        'kinit',
        '-k',
        '-t',
        '/state/client.keytab',
        '-l',
        '3600s',
        '-c',
        'FILE:/tmp/kafkajs-gssapi-fresh.ccache',
        'client@KAFKAJS.TEST',
      ])
      expect(record).toEqual({
        cache: path.join(directory, 'fresh.ccache'),
        config: path.join(directory, 'krb5.conf'),
        principal: 'client@KAFKAJS.TEST',
        issuedAtMs: START,
        expiresAtMs: START + HOUR,
      })
      expect(fs.statSync(record.cache).mode & 0o777).toBe(0o600)
      expect(fs.readFileSync(record.config, 'utf8')).toContain('kdc = localhost:88')
      expect(fs.existsSync(`${record.cache}.pending`)).toBe(false)
    })

    test('copies a client config that keeps the broker host name exactly as given', () => {
      const { controller } = make({ lifetimeMs: HOUR, issuedAt: START })
      const config = fs.readFileSync(controller.ticket('fresh').config, 'utf8')
      expect(config).toMatch(/^ dns_canonicalize_hostname = false$/m)
      expect(config).toMatch(/^ qualify_shortname = ""$/m)
    })

    test('refuses a ticket whose lifetime is not the requested one, before copying anything', () => {
      const { controller, calls } = make({ lifetimeMs: HOUR / 2, issuedAt: START })
      expect(() => controller.ticket('fresh')).toThrow('Ticket lifetime or clock')
      expect(calls.some((argv) => argv[3] === 'cp')).toBe(false)
    })

    test('refuses a ticket issued far from the runner clock', () => {
      const { controller, clock } = make({ lifetimeMs: HOUR, issuedAt: START })
      clock.time = START + 600000
      expect(() => controller.ticket('fresh')).toThrow('Ticket lifetime or clock')
    })
  })

  describe('expired', () => {
    test('waits past the end time, then needs klist to report no valid ticket', () => {
      const { controller, clock } = make({ lifetimeMs: 60000, issuedAt: START })
      const issued = controller.ticket('expired')
      const observed = controller.control('gssapi-expired', ['expired'])
      expect(clock.slept.reduce((sum, ms) => sum + ms, 0)).toBe(61000)
      expect(observed).toEqual({
        cache: issued.cache,
        observedAtMs: issued.expiresAtMs + 1000,
        klistStatus: 1,
      })
    })

    test('refuses a ticket that klist still judges valid', () => {
      const { controller } = make({ lifetimeMs: 60000, issuedAt: START, statusValue: 0 })
      controller.ticket('expired')
      expect(() => controller.expired('expired')).toThrow('Expiry not observed: klist -s status 0')
    })
  })

  describe('log', () => {
    const groups =
      'GROUP TOPIC PARTITION CURRENT-OFFSET LOG-END-OFFSET LAG CONSUMER-ID HOST CLIENT-ID\ng t 0 2 2 0 - - -\n'

    test('reads the records up to the log end and the committed offset', () => {
      const { controller } = make({
        lifetimeMs: HOUR,
        issuedAt: START,
        outputs: {
          'kafka-consumer-groups.sh': groups,
          'kafka-console-consumer.sh': 'Offset:0\trecord-0\nOffset:1\trecord-1\n',
        },
      })
      expect(controller.control('gssapi-log', ['t', 'g'])).toEqual({
        log: [
          { offset: '0', value: 'record-0' },
          { offset: '1', value: 'record-1' },
        ],
        committedOffset: '2',
      })
    })

    test('refuses a read that returns fewer records than the log end', () => {
      const { controller } = make({
        lifetimeMs: HOUR,
        issuedAt: START,
        outputs: {
          'kafka-consumer-groups.sh': groups,
          'kafka-console-consumer.sh': 'Offset:0\trecord-0\n',
        },
      })
      expect(() => controller.log('t', 'g')).toThrow('Read 1 of 2 records')
    })

    test('parseCommitted refuses a description without the topic row', () => {
      expect(() => parseCommitted(groups, 'other')).toThrow('No partition 0 row')
    })
  })

  test('control refuses an unknown operation and an option-shaped name', () => {
    const { controller } = make({ lifetimeMs: HOUR, issuedAt: START })
    expect(() => controller.control('gssapi-other', [])).toThrow('Invalid KDC control')
    expect(() => controller.control('gssapi-log', ['--topic', 'g'])).toThrow('Invalid KDC control')
    expect(() => controller.control('gssapi-ticket', ['forever'])).toThrow('Invalid KDC control')
  })
})

describe('KdcFixture ownership', () => {
  const keep = { guard: process.env.P9_CLUSTER_GUARD, manifest: process.env.P9_CLUSTER_MANIFEST }
  afterEach(() => {
    for (const [name, value] of [
      ['P9_CLUSTER_GUARD', keep.guard],
      ['P9_CLUSTER_MANIFEST', keep.manifest],
    ]) {
      if (value === undefined) delete process.env[name]
      else process.env[name] = value
    }
  })

  test('runs standalone, with captures beside its caches, when neither guard variable is set', () => {
    delete process.env.P9_CLUSTER_GUARD
    delete process.env.P9_CLUSTER_MANIFEST
    const kdc = new KdcFixture()
    expect(kdc.standalone).toBeInstanceOf(StandaloneKdc)
    expect(kdc.record.runDir).toBe(kdc.standalone.directory)
    kdc.standalone.control = jest.fn(() => ({ ok: true }))
    expect(kdc.ticket('fresh')).toEqual({ ok: true })
    expect(kdc.standalone.control).toHaveBeenCalledWith('gssapi-ticket', ['fresh'])
  })

  test.each(['P9_CLUSTER_GUARD', 'P9_CLUSTER_MANIFEST'])('refuses %s alone', (name) => {
    delete process.env.P9_CLUSTER_GUARD
    delete process.env.P9_CLUSTER_MANIFEST
    process.env[name] = 'only-one'
    expect(() => new KdcFixture()).toThrow('Incomplete cluster ownership environment')
  })
})

const GUARD_VARIABLES = ['P9_CLUSTER_GUARD', 'P9_CLUSTER_MANIFEST']

/** Registers hooks that give each test the environment of a standalone fixture, then restore it. */
const withoutGuard = () => {
  const keep = GUARD_VARIABLES.map((name) => process.env[name])
  beforeEach(() => {
    for (const name of GUARD_VARIABLES) delete process.env[name]
  })
  afterEach(() => {
    GUARD_VARIABLES.forEach((name, index) => {
      if (keep[index] === undefined) delete process.env[name]
      else process.env[name] = keep[index]
    })
  })
}

describe('KdcFixture credential selection', () => {
  withoutGuard()
  const NAMES = ['KRB5_CONFIG', 'KRB5CCNAME', 'KRB5_CLIENT_KTNAME']
  // The real environment, which the native library reads; a spec's own `process.env` is a copy.
  const native = vm.runInThisContext('process.env')
  const keep = {}
  beforeEach(() => {
    for (const name of NAMES) keep[name] = native[name]
  })
  afterEach(() => {
    for (const name of NAMES) {
      if (keep[name] === undefined) delete native[name]
      else native[name] = keep[name]
    }
  })

  test('selects the Kerberos config and cache in the real environment and drops a client keytab', () => {
    native.KRB5_CLIENT_KTNAME = '/stale/client.keytab'
    new KdcFixture().select({ config: '/run/krb5.conf', cache: '/run/fresh.ccache' })
    expect(native.KRB5_CONFIG).toBe('/run/krb5.conf')
    expect(native.KRB5CCNAME).toBe('/run/fresh.ccache')
    expect(native.KRB5_CLIENT_KTNAME).toBeUndefined()
  })

  test('replaces an earlier selection with the next one', () => {
    const kdc = new KdcFixture()
    kdc.select({ config: '/run/krb5.conf', cache: '/run/fresh.ccache' })
    kdc.select({ config: '/run/krb5.conf', cache: '/run/expired.ccache' })
    expect(native.KRB5CCNAME).toBe('/run/expired.ccache')
  })
})

describe('KdcFixture observation', () => {
  withoutGuard()
  let write
  beforeEach(() => {
    write = jest.spyOn(process.stderr, 'write').mockImplementation(() => true)
  })
  afterEach(() => write.mockRestore())

  test('prints a native initialization failure and rejects with it unchanged', async () => {
    const failure = new Error('No Kerberos credentials available')
    const peer = { initializeClient: jest.fn().mockRejectedValue(failure) }
    const kdc = new KdcFixture()
    const restore = kdc.observe(peer)
    await expect(peer.initializeClient('kafka@localhost', { principal: 'p' })).rejects.toBe(failure)
    restore()
    expect(write).toHaveBeenCalledExactlyOnceWith(
      'kerberos initializeClient failed: No Kerberos credentials available\n'
    )
    expect(kdc.events).toEqual([])
  })

  test('prints a native exchange failure and records no event for the failed call', async () => {
    const failure = new Error('Ticket expired')
    const client = { step: jest.fn().mockRejectedValue(failure), contextComplete: false }
    const peer = { initializeClient: jest.fn().mockResolvedValue(client) }
    const kdc = new KdcFixture()
    const restore = kdc.observe(peer)
    const observed = await peer.initializeClient('kafka@localhost', { principal: 'p' })
    await expect(observed.step('')).rejects.toBe(failure)
    restore()
    expect(write).toHaveBeenCalledExactlyOnceWith('kerberos step failed: Ticket expired\n')
    expect(kdc.events.map(({ method }) => method)).toEqual(['initialize'])
  })

  test('records a successful exchange and prints nothing', async () => {
    const client = { step: jest.fn().mockResolvedValue('token'), contextComplete: true }
    const peer = { initializeClient: jest.fn().mockResolvedValue(client) }
    const kdc = new KdcFixture()
    const restore = kdc.observe(peer)
    const observed = await peer.initializeClient('kafka@localhost', { principal: 'p' })
    await expect(observed.step('')).resolves.toBe('token')
    restore()
    expect(write).not.toHaveBeenCalled()
    expect(kdc.events).toEqual([
      { context: 0, method: 'initialize', service: 'kafka@localhost', principal: 'p' },
      { context: 0, method: 'step', args: [''], result: 'token', complete: true },
    ])
  })
})
