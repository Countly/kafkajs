const fs = require('fs')
const {
  STAMP_FIELDS,
  evidencePath,
  evidenceLines,
  writeEvidence,
  useEvidenceRun,
  wireVersionsOf,
  sanitiseConfig,
} = require('../e2eEvidence')

const RUN = {
  brokerVersion: '4.3.1',
  node: 'v26.7.0',
  tree: '0123456789abcdef0123456789abcdef01234567',
  lane: 'docker',
}

describe('e2eEvidence', () => {
  const name = `e2e-evidence-spec-${process.pid}`

  afterEach(() => {
    useEvidenceRun()
    fs.rmSync(evidencePath(name), { force: true })
  })

  test('STAMP_FIELDS lists what a verifier requires, and cannot change', () => {
    expect(STAMP_FIELDS).toEqual([
      'at',
      'brokerVersion',
      'wireVersions',
      'node',
      'config',
      'tree',
      'lane',
    ])
    expect(Object.isFrozen(STAMP_FIELDS)).toBe(true)
  })

  describe('useEvidenceRun', () => {
    test('accepts a complete context, with or without wireVersions and config', () => {
      expect(() => useEvidenceRun(RUN)).not.toThrow()
      expect(() =>
        useEvidenceRun({ ...RUN, wireVersions: { Fetch: 18 }, config: { clientId: 'c' } })
      ).not.toThrow()
    })

    test.each([
      ['brokerVersion', undefined],
      ['brokerVersion', ''],
      ['brokerVersion', '4.3'],
      ['brokerVersion', 'v4.3.1'],
      ['brokerVersion', 431],
      ['node', undefined],
      ['node', '26.7.0'],
      ['tree', undefined],
      ['tree', ''],
      ['tree', 12],
      ['lane', undefined],
      ['lane', 'chaos'],
      ['wireVersions', []],
      ['wireVersions', 'Fetch 18'],
      ['config', null],
      ['config', ['a']],
    ])('rejects a context whose %s is %p, and names the field', (field, value) => {
      expect(() => useEvidenceRun({ ...RUN, [field]: value })).toThrow(
        new RegExp(`no valid ${field}`)
      )
    })

    test.each([[null], [{}], ['docker']])('rejects %p instead of a context', (context) => {
      expect(() => useEvidenceRun(context)).toThrow('Evidence run context has no valid')
    })

    test('keeps the previous run when the new context is rejected', () => {
      useEvidenceRun(RUN)

      expect(() => useEvidenceRun({ ...RUN, tree: '' })).toThrow()
      writeEvidence(name, { step: 'a' })

      expect(evidenceLines(name)[0].tree).toBe(RUN.tree)
    })
  })

  describe('writeEvidence', () => {
    test('stamps every line with the time and the identity of the run', () => {
      useEvidenceRun({ ...RUN, wireVersions: { Fetch: 18 }, config: { clientId: 'c' } })

      writeEvidence(name, { step: 'one' })

      expect(evidenceLines(name)).toEqual([
        {
          at: expect.any(String),
          step: 'one',
          wireVersions: { Fetch: 18 },
          config: { clientId: 'c' },
          ...RUN,
        },
      ])
      expect(Object.keys(evidenceLines(name)[0])).toEqual(expect.arrayContaining(STAMP_FIELDS))
    })

    test('appends one line per record', () => {
      useEvidenceRun(RUN)

      writeEvidence(name, { step: 'one' })
      writeEvidence(name, { step: 'two' })

      expect(evidenceLines(name).map(({ step }) => step)).toEqual(['one', 'two'])
      expect(fs.readFileSync(evidencePath(name), 'utf8').split('\n')).toHaveLength(3)
    })

    test('defaults wireVersions and config to empty objects', () => {
      useEvidenceRun(RUN)

      writeEvidence(name, {})

      expect(evidenceLines(name)[0]).toMatchObject({ wireVersions: {}, config: {} })
    })

    test('lets a record carry its own wireVersions and config over the run’s', () => {
      useEvidenceRun({ ...RUN, wireVersions: { Fetch: 18 }, config: { clientId: 'run' } })

      writeEvidence(name, { wireVersions: { Produce: 13 }, config: { clientId: 'record' } })

      expect(evidenceLines(name)[0]).toMatchObject({
        wireVersions: { Produce: 13 },
        config: { clientId: 'record' },
      })
    })

    test('does not let a record claim another tree, node, broker or lane', () => {
      useEvidenceRun(RUN)

      writeEvidence(name, {
        tree: 'forged',
        node: 'v1.0.0',
        brokerVersion: '9.9.9',
        lane: 'fake',
        step: 'forgery',
      })

      expect(evidenceLines(name)[0]).toMatchObject({ ...RUN, step: 'forgery' })
    })

    test('lets the record’s own at win, as it always did', () => {
      useEvidenceRun(RUN)

      writeEvidence(name, { at: 'yesterday' })

      expect(evidenceLines(name)[0].at).toBe('yesterday')
    })

    test('leaves credentials out of the config it writes', () => {
      useEvidenceRun(RUN)

      writeEvidence(name, {
        config: {
          clientId: 'c',
          sasl: { mechanism: 'plain', username: 'u', password: 'p@ss' },
          ssl: { servername: 'localhost', ca: ['-----BEGIN CERTIFICATE-----'], key: 'k' },
        },
      })

      expect(JSON.stringify(evidenceLines(name)[0])).not.toMatch(/p@ss|BEGIN CERTIFICATE|"k"/)
      expect(evidenceLines(name)[0].config.sasl).toEqual({
        mechanism: 'plain',
        username: 'u',
        password: '[redacted]',
      })
    })

    test('leaves every credential of an AWS IAM config out of the file, and keeps the rest', () => {
      useEvidenceRun(RUN)
      const sasl = {
        mechanism: 'aws',
        authorizationIdentity: 'AIDAIOSFODNN7EXAMPLE',
        accessKeyId: 'AKIAIOSFODNN7EXAMPLE',
        secretAccessKey: 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
        sessionToken: 'FwoGZXIvYXdzEXAMPLETOKEN',
      }

      writeEvidence(name, { config: { clientId: 'c', ssl: true, sasl } })

      const text = fs.readFileSync(evidencePath(name), 'utf8')
      for (const credential of Object.values(sasl).filter((value) => value !== 'aws')) {
        expect(text).not.toContain(credential)
      }
      expect(evidenceLines(name)[0].config).toEqual({
        clientId: 'c',
        ssl: true,
        sasl: {
          mechanism: 'aws',
          authorizationIdentity: '[redacted]',
          accessKeyId: '[redacted]',
          secretAccessKey: '[redacted]',
          sessionToken: '[redacted]',
        },
      })
    })

    describe('a record that carries a toJSON', () => {
      const FORGED = { tree: 'forged', node: 'v1.0.0', brokerVersion: '9.9.9', lane: 'fake' }

      test.each([
        [
          'replaces the line',
          function replace() {
            return { ...this, ...FORGED, config: { password: 'TOP_SECRET' } }
          },
        ],
        [
          'changes the line in place',
          function mutate() {
            Object.assign(this, FORGED)
            this.config = { password: 'TOP_SECRET' }
            return this
          },
        ],
        [
          'puts a password back',
          function restore() {
            return { ...this, config: { password: 'TOP_SECRET' } }
          },
        ],
      ])('is never run, so a toJSON that %s changes nothing', (_name, toJSON) => {
        useEvidenceRun(RUN)
        const hook = jest.fn(toJSON)

        writeEvidence(name, { step: 'forgery', config: { password: 'TOP_SECRET' }, toJSON: hook })

        expect(hook).not.toHaveBeenCalled()
        expect(fs.readFileSync(evidencePath(name), 'utf8')).not.toContain('TOP_SECRET')
        expect(evidenceLines(name)).toEqual([
          {
            at: expect.any(String),
            step: 'forgery',
            wireVersions: {},
            config: { password: '[redacted]' },
            ...RUN,
          },
        ])
      })
    })

    test('rejects an invalid name and writes nothing', () => {
      useEvidenceRun(RUN)

      expect(() => writeEvidence('../escape', { step: 'x' })).toThrow(
        'Evidence name must be lower-case kebab-case'
      )
    })
  })

  describe('evidenceLines', () => {
    test('reads nothing from a scenario that wrote nothing', () => {
      expect(evidenceLines(name)).toEqual([])
    })

    test('rejects an invalid name', () => {
      expect(() => evidenceLines('Upper')).toThrow('Evidence name must be lower-case kebab-case')
    })
  })
})

describe('wireVersionsOf', () => {
  test('is the highest version each API was sent at', () => {
    expect(
      wireVersionsOf([
        { apiName: 'Metadata', apiVersion: 12 },
        { apiName: 'CreateTopics', apiVersion: 7 },
        { apiName: 'Metadata', apiVersion: 13 },
        { apiName: 'Metadata', apiVersion: 11 },
      ])
    ).toEqual({ Metadata: 13, CreateTopics: 7 })
  })

  test('is empty for a client that sent nothing, and keeps a version of 0', () => {
    expect(wireVersionsOf([])).toEqual({})
    expect(wireVersionsOf([{ apiName: 'ApiVersions', apiVersion: 0 }])).toEqual({ ApiVersions: 0 })
  })
})

describe('sanitiseConfig', () => {
  test('keeps what is not a credential, at any depth, and does not change its input', () => {
    const config = { a: { b: [1, { c: 'x' }] }, n: null, t: true }
    const copy = JSON.stringify(config)

    expect(sanitiseConfig(config)).toEqual(config)
    expect(JSON.stringify(config)).toBe(copy)
  })

  test.each([
    'password',
    'sslPassphrase',
    'clientSecret',
    'accessToken',
    'credentials',
    'authorization',
    'privateKey',
    'apiKey',
    'accessKeyId',
    'accesskeyid',
    'ACCESSKEYID',
    'secretAccessKey',
    'sessionToken',
    'authorizationIdentity',
    'ca',
    'cert',
    'key',
    'pfx',
  ])('redacts the value under %s', (key) => {
    expect(sanitiseConfig({ [key]: 'value' })).toEqual({ [key]: '[redacted]' })
  })

  test('redacts a whole AWS IAM config, whichever of its credentials the client was given', () => {
    expect(
      sanitiseConfig({
        sasl: {
          mechanism: 'aws',
          authorizationIdentity: 'AIDAIOSFODNN7EXAMPLE',
          accessKeyId: 'AKIAIOSFODNN7EXAMPLE',
          secretAccessKey: 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
          sessionToken: 'FwoGZXIvYXdzEXAMPLETOKEN',
        },
      })
    ).toEqual({
      sasl: {
        mechanism: 'aws',
        authorizationIdentity: '[redacted]',
        accessKeyId: '[redacted]',
        secretAccessKey: '[redacted]',
        sessionToken: '[redacted]',
      },
    })
  })

  test.each(['clientId', 'brokers', 'keepAlive', 'mechanism', 'caching'])(
    'keeps the value under %s',
    (key) => {
      expect(sanitiseConfig({ [key]: 'value' })).toEqual({ [key]: 'value' })
    }
  )

  test('replaces functions, binary data and bigints by markers or strings', () => {
    expect(
      sanitiseConfig({
        provider: () => 'token',
        raw: Buffer.from('x'),
        view: new Uint8Array(2),
        big: 12n,
      })
    ).toEqual({ provider: '[function]', raw: '[binary]', view: '[binary]', big: '12' })
  })

  test('marks a cycle, but copies an object that is only shared', () => {
    const shared = { x: 1 }
    const cyclic = { shared, again: shared }
    cyclic.self = cyclic

    expect(sanitiseConfig(cyclic)).toEqual({
      shared: { x: 1 },
      again: { x: 1 },
      self: '[circular]',
    })
  })
})

describe('the run of a live scenario', () => {
  const name = `e2e-evidence-spec-live-${process.pid}`
  const env = { ...process.env }

  /** The module loaded fresh, with the broker-version reader replaced. */
  const load = (readBrokerVersion) => {
    let evidence
    jest.isolateModules(() => {
      jest.doMock('../brokerVersion', () => ({ readBrokerVersion }))
      evidence = require('../e2eEvidence')
    })
    return evidence
  }

  afterEach(() => {
    jest.dontMock('../brokerVersion')
    process.env = { ...env }
    fs.rmSync(load(() => '4.3.1').evidencePath(name), { force: true })
  })

  test('is read from the broker, this Node, KAFKAJS_TREE and the lane, once for any number of lines', () => {
    process.env.KAFKA_VERSION = '4.1.2'
    process.env.KAFKAJS_TREE = 'abc123'
    process.env.KAFKAJS_LANE = 'docker'
    const readBrokerVersion = jest.fn(() => '4.1.2')
    const evidence = load(readBrokerVersion)

    evidence.writeEvidence(name, { step: 'one' })
    evidence.writeEvidence(name, { step: 'two' })

    expect(readBrokerVersion).toHaveBeenCalledTimes(1)
    expect(evidence.evidenceLines(name)).toEqual([
      expect.objectContaining({
        brokerVersion: '4.1.2',
        node: process.version,
        tree: 'abc123',
        lane: 'docker',
      }),
      expect.objectContaining({ brokerVersion: '4.1.2', tree: 'abc123' }),
    ])
  })

  test('has the tree "unfrozen" when nobody froze one, and the fake lane outside test:kafka', () => {
    delete process.env.KAFKAJS_TREE
    delete process.env.KAFKAJS_LANE
    const evidence = load(() => '4.3.1')

    evidence.writeEvidence(name, {})

    expect(evidence.evidenceLines(name)[0]).toMatchObject({ tree: 'unfrozen', lane: 'fake' })
  })

  test('is refused, and nothing is written, when the broker is not the version the lane asked for', () => {
    process.env.KAFKA_VERSION = '4.3.1'
    const evidence = load(() => '4.1.2')

    expect(() => evidence.writeEvidence(name, {})).toThrow(
      'The broker reports 4.1.2 but KAFKA_VERSION=4.3.1'
    )
    expect(evidence.evidenceLines(name)).toEqual([])
  })

  test('is not compared with anything when the lane declares no version', () => {
    delete process.env.KAFKA_VERSION
    const evidence = load(() => '4.2.2')

    evidence.writeEvidence(name, {})

    expect(evidence.evidenceLines(name)[0].brokerVersion).toBe('4.2.2')
  })

  test('is refused, and nothing is written, when the broker cannot be asked', () => {
    const evidence = load(() => {
      throw new Error('no such service: kafka1')
    })

    expect(() => evidence.writeEvidence(name, {})).toThrow('no such service: kafka1')
    expect(evidence.evidenceLines(name)).toEqual([])
  })

  test('asks again after a failed attempt, and then remembers the answer', () => {
    delete process.env.KAFKA_VERSION
    const readBrokerVersion = jest
      .fn()
      .mockImplementationOnce(() => {
        throw new Error('not up yet')
      })
      .mockReturnValue('4.3.1')
    const evidence = load(readBrokerVersion)

    expect(() => evidence.writeEvidence(name, {})).toThrow('not up yet')
    evidence.writeEvidence(name, {})
    evidence.writeEvidence(name, {})

    expect(readBrokerVersion).toHaveBeenCalledTimes(2)
  })

  test('gives way to a context a test installs, and comes back when it is cleared', () => {
    delete process.env.KAFKA_VERSION
    const readBrokerVersion = jest.fn(() => '4.3.1')
    const evidence = load(readBrokerVersion)

    evidence.useEvidenceRun({ ...RUN, brokerVersion: '4.0.2', tree: 'installed' })
    evidence.writeEvidence(name, { step: 'installed' })
    evidence.useEvidenceRun()
    evidence.writeEvidence(name, { step: 'discovered' })

    expect(readBrokerVersion).toHaveBeenCalledTimes(1)
    expect(evidence.evidenceLines(name).map(({ brokerVersion }) => brokerVersion)).toEqual([
      '4.0.2',
      '4.3.1',
    ])
  })
})
