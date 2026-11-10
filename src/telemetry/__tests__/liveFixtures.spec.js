const { unexpectedJavaStderr } = require('./fixtures/javaStderr')
const {
  connectionIdentity,
  cacheRetirement,
  retirementsOf,
} = require('../../../scripts/e2e/telemetry/cacheTrace')
const fs = require('fs')
const os = require('os')
const path = require('path')
const { execFileSync } = require('child_process')
const { performance } = require('perf_hooks')
// eslint-disable-next-line n/no-extraneous-require -- YAML parser is pinned by the ESLint/Jest toolchain
const yaml = require('js-yaml')
const { TelemetrySegments } = require('../../../testHelpers/telemetrySegments')
const { requestTraceArgs, assertRequestLogConfig } = require('./fixtures/requestTrace')

describe('Java telemetry source-launcher diagnostics', () => {
  test('classifies known legacy source-launcher and SLF4J notices', () => {
    expect(
      unexpectedJavaStderr(
        'Note: /owned/JavaTelemetryClient.java uses or overrides a deprecated API.\n' +
          'Note: Recompile with -Xlint:deprecation for details.\n' +
          'SLF4J: Failed to load class "org.slf4j.impl.StaticLoggerBinder".\n' +
          'SLF4J: Defaulting to no-operation (NOP) logger implementation\n' +
          'SLF4J: See http://www.slf4j.org/codes.html#StaticLoggerBinder for further details.\n'
      )
    ).toEqual([])
  })

  test.each([
    'Exception in thread "main" java.lang.NoClassDefFoundError: missing class',
    'error: cannot find symbol',
    'Note: /foreign/OtherClient.java uses or overrides a deprecated API.',
    'java.lang.IllegalStateException: deprecated API failed',
    'JavaTelemetryClient.java:199: warning: [deprecation] close(Duration) in KafkaConsumer has been deprecated',
  ])('retains an unexpected diagnostic: %s', (line) => {
    expect(unexpectedJavaStderr(`${line}\n`)).toEqual([line])
  })
})

describe('broker request appender capture', () => {
  test('reads Gets from the request file in a half-open window, including DEBUG records', () => {
    const args = requestTraceArgs('kafka1', '2026-11-08T19:10:27.000Z', '2026-11-08T19:10:28.000Z')
    expect(args.slice(0, 5)).toEqual(['exec', '-T', 'kafka1', 'env', 'LC_ALL=C'])
    expect(args.at(-1)).toBe('/opt/kafka/logs/kafka-request.log')
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'request-log-'))
    try {
      const get =
        'DEBUG Completed request:{"requestHeader":{"requestApiKeyName":"GET_TELEMETRY_SUBSCRIPTIONS"}} (kafka.request.logger)'
      const lines = [
        `[2026-11-08 19:10:26,999] ${get}`,
        `[2026-11-08 19:10:27,000] ${get}`,
        '[2026-11-08 19:10:27,010] DEBUG Completed request:{"requestHeader":{"requestApiKeyName":"METADATA"}}',
        `[2026-11-08 19:10:27,999] ${get}`,
        `[2026-11-08 19:10:28,000] ${get}`,
      ]
      const file = path.join(dir, 'kafka-request.log')
      fs.writeFileSync(file, lines.join('\n') + '\n')
      expect(execFileSync('bash', [...args.slice(6, -1), file], { encoding: 'utf8' })).toBe(
        `${lines[1]}\n${lines[3]}\n`
      )
    } finally {
      fs.rmSync(dir, { recursive: true, force: true })
    }
  })

  test.each([false, true])(
    'reads a rollover window exactly once (rotate during read: %s)',
    (race) => {
      const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'request-rollover-'))
      try {
        const args = requestTraceArgs(
          'kafka1',
          '2026-11-08T19:59:59.000Z',
          '2026-11-08T20:00:01.000Z'
        )
        const file = path.join(dir, 'kafka-request.log')
        const get = (stamp, id) =>
          `[2026-11-08 ${stamp}] DEBUG Completed request:GET_TELEMETRY_SUBSCRIPTIONS ${id}`
        const before = get('19:59:59,999', 'before')
        const after = get('20:00:00,000', 'after')
        const duplicate = get('19:59:59,500', 'genuine-duplicate')
        const old = `${get('19:59:58,999', 'outside')}\n${before}\n${duplicate}\n${duplicate}\n`
        const active = `${after}\n${get('20:00:01,000', 'outside')}\n`
        fs.writeFileSync(file, race ? old : active)
        if (!race) fs.writeFileSync(`${file}.2026-11-08-19`, old)
        fs.writeFileSync(`${file}.2026-11-08-18`, get('19:59:59,100', 'irrelevant-file') + '\n')
        fs.writeFileSync(`${file}.next`, active)
        if (race) {
          // Replace the active path after its first identity snapshot, before the reader commits it.
          fs.writeFileSync(
            path.join(dir, 'stat'),
            `#!/bin/bash
if [[ "\${!#}" == "${file}" ]]; then
  if [[ -f "${file}.seen" && -f "${file}.next" ]]; then
    mv "${file}" "${file}.2026-11-08-19"
    mv "${file}.next" "${file}"
  fi
  touch "${file}.seen"
fi
exec /usr/bin/stat "$@"
`,
            { mode: 0o700 }
          )
        }
        const text = execFileSync('bash', [...args.slice(6, -1), file], {
          encoding: 'utf8',
          env: { ...process.env, PATH: `${dir}:${process.env.PATH}` },
        })
        expect(text.trim().split('\n').sort()).toEqual([before, duplicate, duplicate, after].sort())
      } finally {
        fs.rmSync(dir, { recursive: true, force: true })
      }
    }
  )

  test('fails instead of silently returning an empty trace when the active file is absent', () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'missing-request-log-'))
    try {
      const args = requestTraceArgs(
        'kafka1',
        '2026-11-08T19:10:27.000Z',
        '2026-11-08T19:10:28.000Z'
      )
      expect(() =>
        execFileSync('bash', [...args.slice(6, -1), path.join(dir, 'kafka-request.log')], {
          stdio: 'pipe',
        })
      ).toThrow()
    } finally {
      fs.rmSync(dir, { recursive: true, force: true })
    }
  })

  test('fails for a missing or changed request-appender route', () => {
    const config = `Configuration:
  Appenders:
    RollingFile:
      - name: RequestAppender
        fileName: "\${sys:kafka.logs.dir}/kafka-request.log"
        filePattern: "\${sys:kafka.logs.dir}/kafka-request.log.%d{yyyy-MM-dd-HH}"
        TimeBasedTriggeringPolicy:
          modulate: true
          interval: 1
  Loggers:
    Logger:
      - name: kafka.request.logger
        level: WARN
        additivity: false
        AppenderRef:
          ref: RequestAppender
`
    expect(() => assertRequestLogConfig(yaml.load(config))).not.toThrow()
    expect(() =>
      assertRequestLogConfig(yaml.load(config.replace('kafka-request.log', 'other.log')))
    ).toThrow()
    expect(() =>
      assertRequestLogConfig(yaml.load(config.replace('ref: RequestAppender', 'ref: STDOUT')))
    ).toThrow()
    expect(() =>
      assertRequestLogConfig(yaml.load(config.replace('yyyy-MM-dd-HH', 'yyyy-MM-dd')))
    ).toThrow()
    expect(() =>
      assertRequestLogConfig(yaml.load(config.replace('interval: 1', 'interval: 2')))
    ).toThrow()
  })

  test('binds the Docker-rewritten YAML route regardless of field order and scalar quoting', () => {
    const config = `Configuration:
  Appenders:
    RollingFile:
      - fileName: "\${sys:kafka.logs.dir}/kafka-request.log"
        TimeBasedTriggeringPolicy:
          interval: 1
          modulate: true
        filePattern: "\${sys:kafka.logs.dir}/kafka-request.log.%d{yyyy-MM-dd-HH}"
        name: "RequestAppender"
  Loggers:
    Logger:
      - AppenderRef:
          ref: "RequestAppender"
        additivity: false
        level: "WARN"
        name: "kafka.request.logger"
`
    expect(() => assertRequestLogConfig(yaml.load(config))).not.toThrow()
  })
})

describe('incomplete shutdown schedule diagnostics', () => {
  test('keeps the primary failure as the incomplete schedule cause after sealing partial evidence', async () => {
    const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'failed-segments-'))
    const segments = TelemetrySegments.ordinary('telemetry-shutdown', 'producer', {
      directory,
      clock: { now: () => performance.now() },
      observer: { line: '4.3.1', mode: 'configured' },
    })
    const client = { disconnect: jest.fn(async () => {}) }
    const cause = new Error('Missing or ambiguous Get connection trace')
    try {
      await expect(segments.finish(client, { timeout: 3000 }, { cause })).rejects.toMatchObject({
        cause,
        message: expect.stringContaining(
          'Expected 21 segments, found 1 after scenario failure: Missing or ambiguous Get connection trace'
        ),
      })
      expect(segments.sealed()).toHaveLength(1)
      expect(client.disconnect).toHaveBeenCalledTimes(1)
      await expect(segments.finish(client)).rejects.toThrow('Expected 21 segments, found 1')
    } finally {
      await segments.current().observer?.dispose()
      fs.rmSync(directory, { recursive: true, force: true })
    }
  })
})

describe('Kafka JSON connection traces', () => {
  const uuid = '00112233-4455-6677-8899-aabbccddeeff'
  const connection = '172.18.0.4:9092-172.18.0.1:54321-9-8'
  const input = () => {
    const request = {
      requestHeader: {
        requestApiKey: 71,
        requestApiVersion: 0,
        requestApiKeyName: 'GET_TELEMETRY_SUBSCRIPTIONS',
        clientId: 'candidate',
        correlationId: 2,
      },
      request: { clientInstanceId: 'AAAAAAAAAAAAAAAAAAAAAA' },
      response: { errorCode: 0, clientInstanceId: 'ABEiM0RVZneImaq7zN3u_w' },
      connection,
    }
    return {
      request,
      options: {
        node: '1',
        localPort: '9092',
        uuid,
        since: '2026-11-08T19:10:27.000Z',
        completedAt: '2026-11-08T19:10:29.000Z',
        source: { sha256: 'a'.repeat(64) },
        get: {
          socketId: 2,
          node: '1',
          requestKey: 'r7',
          clientId: 'candidate',
          correlation: 2,
          apiVersion: 0,
          uuid: '00000000-0000-0000-0000-000000000000',
          response: { errorCode: 0, uuid },
        },
        connect: { socketId: 2, node: '1', localAddress: '127.0.0.1', localPort: 54321 },
        logs: { 1: { text: '' }, 2: { text: '' }, 3: { text: '' } },
      },
    }
  }
  const line = (request) =>
    `[2026-11-08 19:10:27,123] DEBUG Completed request:${JSON.stringify(request)} (kafka.request.logger)`

  test('joins JSON request/response UUIDs and preserves processor and connection indices', () => {
    const { request, options } = input()
    options.logs[1].text = `${line(request)}\nTRACE Removing client connection id [${connection}] from the client instance cache`
    expect(connectionIdentity(options)).toMatchObject({ connectionId: connection, uuid })
    expect(cacheRetirement(options)).toMatchObject({ connectionId: connection })
    expect(retirementsOf(options.logs[1].text)).toEqual([
      expect.objectContaining({
        connectionId: connection,
        processor: '9',
        index: '8',
      }),
    ])
  })

  test('keeps the index of a five-component legacy connection ID', () => {
    const legacy = '172.18.0.4:9092-172.18.0.1:54321-8'
    expect(
      retirementsOf(
        `TRACE Removing client connection id [${legacy}] from the client instance cache`
      )
    ).toEqual([expect.objectContaining({ connectionId: legacy, processor: undefined, index: '8' })])
  })

  test.each([
    ['client id', (r) => (r.requestHeader.clientId = 'another')],
    ['correlation', (r) => r.requestHeader.correlationId++],
    ['version', (r) => r.requestHeader.requestApiVersion++],
    ['request UUID', (r) => (r.request.clientInstanceId = uuid)],
    ['response UUID', (r) => (r.response.clientInstanceId = 'AAAAAAAAAAAAAAAAAAAAAA')],
    ['response error', (r) => (r.response.errorCode = 42)],
    ['listener', (r) => (r.connection = connection.replace(':9092-', ':9095-'))],
  ])('rejects a mismatched %s', (_, change) => {
    const { request, options } = input()
    change(request)
    options.logs[1].text = line(request)
    expect(() => connectionIdentity(options)).toThrow()
  })

  test('rejects duplicate Gets and a later cache miss for the exact connection', () => {
    const { request, options } = input()
    options.logs[1].text = `${line(request)}\n${line(request)}`
    expect(() => connectionIdentity(options)).toThrow('Missing or ambiguous Get connection trace')
    options.logs[1].text = `${line(request)}\nTRACE Removing client connection id [${connection}] from the client instance cache\nTRACE Client connection id [${connection}] is not found in the client instance cache`
    expect(() => cacheRetirement(options)).toThrow('Expected one exact connection retirement')
  })
})
