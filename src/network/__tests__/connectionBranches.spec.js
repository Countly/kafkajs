const { EventEmitter } = require('events')
const Connection = require('../connection')
const { KafkaJSConnectionError } = require('../../errors')
const {
  newLogger,
  createFakeSocketFactory,
  readRequestHeader,
  respondWithFrame,
} = require('testHelpers')
const Encoder = require('../../protocol/encoder')
const apiKeys = require('../../protocol/requests/apiKeys')
const { responseHeaderVersion } = require('../../protocol/headers')
const { createLogger, LEVELS } = require('../../loggers')

/**
 * A socket factory that records every socket it creates and leaves each one's `onConnect` to the
 * case: `socket.onConnect()` completes that attempt.
 * @returns {{ factory: Function, sockets: EventEmitter[] }}
 */
const createFactory = () => {
  const sockets = []
  const factory = ({ onConnect }) => {
    const socket = new EventEmitter()
    socket.write = () => true
    socket.end = () => {}
    socket.destroy = () => {}
    socket.unref = () => {}
    socket.onConnect = onConnect
    sockets.push(socket)
    return socket
  }
  return { factory, sockets }
}

const build = (socketFactory, options = {}) =>
  new Connection({
    host: '127.0.0.1',
    port: 9092,
    logger: newLogger(),
    socketFactory,
    clientId: 'test-client',
    connectionTimeout: 200,
    requestTimeout: 200,
    enforceRequestTimeout: false,
    ...options,
  })

describe('Connection > events from an attempt that is no longer current', () => {
  it('ignores a connect that completes after a disconnect and a new attempt replaced its attempt', async () => {
    const { factory, sockets } = createFactory()
    const connection = build(factory)
    const first = connection.connect()
    const firstError = first.catch((e) => e)
    await connection.disconnect()
    const second = connection.connect()

    sockets[0].onConnect()

    expect(connection.isConnected()).toBe(false)
    expect(connection.generation).toBe(0)
    sockets[1].onConnect()
    await expect(second).resolves.toBe(true)
    expect((await firstError).message).toMatch('Connection closed while connecting')
    expect(connection.generation).toBe(1)
    await connection.disconnect()
  })

  describe('after a socket the broker already destroyed was replaced', () => {
    let connection
    let stale

    beforeEach(async () => {
      const { factory, sockets } = createFactory()
      connection = build(factory)
      const connecting = connection.connect()
      sockets[0].onConnect()
      await connecting
      // a closed socket is left alone by disconnect(), listeners included
      stale = sockets[0]
      stale.destroyed = true
      await connection.disconnect()
      const reconnecting = connection.connect()
      sockets[1].onConnect()
      await reconnecting
      jest.spyOn(connection, 'processData')
      jest.spyOn(connection, 'rejectRequests')
    })

    afterEach(async () => {
      await connection.disconnect()
    })

    it('ignores its data', () => {
      stale.emit('data', Buffer.alloc(4))

      expect(connection.processData).not.toHaveBeenCalled()
    })

    it.each([
      ['end', []],
      ['error', [new Error('ECONNRESET')]],
      ['timeout', []],
    ])('ignores its %s, and the new socket stays connected', (event, args) => {
      stale.emit(event, ...args)

      expect(connection.rejectRequests).not.toHaveBeenCalled()
      expect(connection.isConnected()).toBe(true)
    })
  })

  it('rejects a connect that the socket ended before it connected', async () => {
    const { factory, sockets } = createFactory()
    const connection = build(factory)
    const connecting = connection.connect()
    const outcome = connecting.catch((e) => e)
    jest.spyOn(connection, 'rejectRequests')

    sockets[0].emit('end')

    expect(await outcome).toBeInstanceOf(KafkaJSConnectionError)
    expect(connection.isConnected()).toBe(false)
    // there is no request to reject before the connection was ever up
    expect(connection.rejectRequests).not.toHaveBeenCalled()
  })
})

describe('Connection > reporting an authentication failure', () => {
  it('emits the event when no handler was given', () => {
    const instrumentationEmitter = { emit: jest.fn() }
    const connection = build(createFactory().factory, { instrumentationEmitter })

    connection.reportAuthenticationFailure('PLAIN', 'denied')

    expect(instrumentationEmitter.emit).toHaveBeenCalledTimes(1)
    expect(instrumentationEmitter.emit).toHaveBeenCalledWith('network.authentication_failure', {
      broker: '127.0.0.1:9092',
      mechanism: 'PLAIN',
      errorMessage: 'denied',
    })
  })

  it('calls the handler when there is no emitter', () => {
    const onAuthenticationFailure = jest.fn()
    const connection = build(createFactory().factory, { onAuthenticationFailure })

    connection.reportAuthenticationFailure('PLAIN', 'denied')

    expect(onAuthenticationFailure).toHaveBeenCalledWith('denied')
  })
})

describe('Connection > DEBUG response logs', () => {
  const FIRST_FLAG = 'KAFKAJS_DEBUG_PROTOCOL_BUFFERS'
  const EXTENDED_FLAG = 'KAFKAJS_DEBUG_EXTENDED_PROTOCOL_BUFFERS'
  const NEITHER = { first: false, extended: false }
  const FIRST_ONLY = { first: true, extended: false }
  const EXTENDED_ONLY = { first: false, extended: true }
  const BOTH = { first: true, extended: true }
  const DATA_MARKERS = [
    'marker-key-3f1a',
    'marker-value-3f1a',
    'marker-header-3f1a',
    'marker-config-3f1a',
    'marker-message-3f1a',
  ]
  const BYTES_MARKER = 'marker-bytes-3f1a'
  const PARSE_ERROR = 'could not parse the response'
  const FILTERED_BUFFER = { type: 'Buffer', data: '[filtered]' }
  const CONFIG_APIS = ['DescribeConfigs', 'CreateTopics', 'AlterConfigs', 'IncrementalAlterConfigs']
  const VERSIONS = {
    Fetch: 11,
    ShareFetch: 2,
    DescribeConfigs: 4,
    CreateTopics: 5,
    AlterConfigs: 2,
    IncrementalAlterConfigs: 1,
    SaslAuthenticate: 2,
    Metadata: 8,
  }
  const decodedData = {
    records: [
      {
        key: Buffer.from(DATA_MARKERS[0]),
        value: Buffer.from(DATA_MARKERS[1]),
        headers: { h: Buffer.from(DATA_MARKERS[2]) },
      },
    ],
    configEntries: [{ configName: 'a.config', configValue: DATA_MARKERS[3] }],
    errorMessage: DATA_MARKERS[4],
  }
  let saved
  let captured = []

  beforeEach(() => {
    saved = { first: process.env[FIRST_FLAG], extended: process.env[EXTENDED_FLAG] }
    jest.useFakeTimers()
  })

  afterEach(() => {
    jest.useRealTimers()
    for (const [name, value] of [
      [FIRST_FLAG, saved.first],
      [EXTENDED_FLAG, saved.extended],
    ]) {
      if (value === undefined) delete process.env[name]
      else process.env[name] = value
    }
  })

  /** @param {string} name @param {boolean} on */
  const setFlag = (name, on) => {
    if (on) process.env[name] = '1'
    else delete process.env[name]
  }

  /**
   * Every string a value exposes: own properties at any depth, and each Buffer as latin1, utf8,
   * base64 and hex text.
   * @param {*} value
   * @param {Set<object>} [seen]
   * @returns {string}
   */
  const textOf = (value, seen = new Set()) => {
    if (value === null || value === undefined) return ''
    if (Buffer.isBuffer(value)) {
      return ['latin1', 'utf8', 'base64', 'hex'].map((form) => value.toString(form)).join('\n')
    }
    if (typeof value !== 'object') return String(value)
    if (seen.has(value)) return ''
    seen.add(value)
    return Object.getOwnPropertyNames(value)
      .map((name) => textOf(value[name], seen))
      .join('\n')
  }

  /**
   * @param {object} log a logged entry's fields
   * @param {string[]} markers
   * @returns {string[]} the markers the entry shows, as text, base64 or hex
   */
  const leaks = (log, markers) => {
    const text = textOf(log)
    return markers.filter((marker) =>
      [marker, Buffer.from(marker).toString('base64'), Buffer.from(marker).toString('hex')].some(
        (form) => text.includes(form)
      )
    )
  }

  /**
   * @param {string[]} markers
   * @returns {string[]} the markers any entry logged by the last `responseLog` call shows
   */
  const leaksAnywhere = (markers) => leaks(captured, markers)

  /**
   * Sends one request of `apiName` over a fake socket, answers it with a frame holding
   * `BYTES_MARKER`, and returns the DEBUG log fields of the response. Every entry the connection
   * logged is kept for `leaksAnywhere`. The flags are set before the connection is built, as it
   * reads them in its constructor.
   * @param {string} apiName
   * @param {{ first: boolean, extended: boolean }} flags
   * @param {boolean} fails whether parsing the response throws
   * @returns {Promise<object>} the fields of the one DEBUG entry of that response
   */
  const responseLog = async (apiName, flags, fails) => {
    setFlag(FIRST_FLAG, flags.first)
    setFlag(EXTENDED_FLAG, flags.extended)
    const entries = []
    captured = entries
    const logger = createLogger({
      level: LEVELS.DEBUG,
      logCreator: () => (entry) => entries.push(entry),
    })
    const fake = createFakeSocketFactory()
    const connection = build(fake.factory, { logger })
    const apiKey = apiKeys[apiName]
    const apiVersion = VERSIONS[apiName]
    const protocol = {
      request: { apiKey, apiName, apiVersion, encode: async () => new Encoder() },
      response: {
        decode: async () => ({ clientSideThrottleTime: 0 }),
        parse: async () => {
          if (fails) throw new Error(PARSE_ERROR)
          return decodedData
        },
      },
    }

    try {
      const connecting = connection.connect()
      await jest.advanceTimersByTimeAsync(0)
      await connecting
      const outcome = connection.send(protocol).then(
        (data) => ({ data }),
        (error) => ({ error })
      )
      await jest.advanceTimersByTimeAsync(0)
      expect(fake.state.writes).toHaveLength(1)
      respondWithFrame(
        fake.state,
        readRequestHeader(fake.state.writes[0]).correlationId,
        responseHeaderVersion(apiKey, apiVersion),
        Buffer.from(BYTES_MARKER)
      )
      const result = await outcome
      expect(result.error?.message).toBe(fails ? PARSE_ERROR : undefined)
    } finally {
      await connection.disconnect({ drainTimeout: 0 })
      await jest.advanceTimersByTimeAsync(0)
    }

    const logged = entries.filter(
      ({ label, log }) => label === 'DEBUG' && log.message.startsWith(`Response ${apiName}(`)
    )
    expect(logged).toHaveLength(1)
    return logged[0].log
  }

  it('filters a ShareFetch response at DEBUG unless both buffer flags are set', async () => {
    for (const flags of [NEITHER, FIRST_ONLY, EXTENDED_ONLY]) {
      const log = await responseLog('ShareFetch', flags, false)

      expect({ flags, data: log.data }).toEqual({ flags, data: '[filtered]' })
      expect({ flags, leaks: leaks(log, DATA_MARKERS) }).toEqual({ flags, leaks: [] })
      expect({ flags, leaks: leaksAnywhere(DATA_MARKERS) }).toEqual({ flags, leaks: [] })
    }
    expect((await responseLog('ShareFetch', BOTH, false)).data).toBe(decodedData)
  })

  it.each(['Fetch', 'ShareFetch'])(
    'logs no raw bytes of a failed %s response with only the first flag',
    async (apiName) => {
      const log = await responseLog(apiName, FIRST_ONLY, true)

      expect(log.error).toBe(PARSE_ERROR)
      expect(log.payload).toEqual(FILTERED_BUFFER)
      expect(leaks(log, [BYTES_MARKER])).toEqual([])
    }
  )

  it('never logs config response data, whatever the flags', async () => {
    for (const apiName of CONFIG_APIS) {
      for (const flags of [NEITHER, FIRST_ONLY, EXTENDED_ONLY, BOTH]) {
        const where = { apiName, flags }
        const success = await responseLog(apiName, flags, false)
        const successLeaks = leaksAnywhere(DATA_MARKERS)
        const failure = await responseLog(apiName, flags, true)
        const failureLeaks = leaksAnywhere([BYTES_MARKER])

        expect({ ...where, data: success.data }).toEqual({ ...where, data: '[filtered]' })
        expect({ ...where, leaks: leaks(success, DATA_MARKERS) }).toEqual({ ...where, leaks: [] })
        expect({ ...where, leaks: successLeaks }).toEqual({ ...where, leaks: [] })
        expect({ ...where, error: failure.error, payload: failure.payload }).toEqual({
          ...where,
          error: PARSE_ERROR,
          payload: '[filtered]',
        })
        expect({ ...where, leaks: leaks(failure, [BYTES_MARKER]) }).toEqual({ ...where, leaks: [] })
        expect({ ...where, leaks: failureLeaks }).toEqual({ ...where, leaks: [] })
      }
    }
  })

  it('still logs a Fetch response with both flags', async () => {
    const success = await responseLog('Fetch', BOTH, false)
    const failure = await responseLog('Fetch', BOTH, true)

    expect(success.data).toBe(decodedData)
    expect(Buffer.isBuffer(failure.payload)).toBe(true)
    expect(failure.payload.toString('latin1')).toContain(BYTES_MARKER)
  })

  it('keeps a SaslAuthenticate response filtered with both flags', async () => {
    const success = await responseLog('SaslAuthenticate', BOTH, false)
    const failure = await responseLog('SaslAuthenticate', BOTH, true)

    expect(success.data).toBe('[filtered]')
    expect(failure.error).toBe('Sensitive response failed')
    expect(failure.payload).toBe('[filtered]')
    expect(leaks(failure, [BYTES_MARKER])).toEqual([])
  })

  it('logs an ordinary response in full, and its raw bytes on failure with the first flag', async () => {
    expect((await responseLog('Metadata', NEITHER, false)).data).toBe(decodedData)
    expect((await responseLog('Metadata', NEITHER, true)).payload).toEqual(FILTERED_BUFFER)

    const failure = await responseLog('Metadata', FIRST_ONLY, true)

    expect(Buffer.isBuffer(failure.payload)).toBe(true)
    expect(failure.payload.toString('latin1')).toContain(BYTES_MARKER)
  })
})
