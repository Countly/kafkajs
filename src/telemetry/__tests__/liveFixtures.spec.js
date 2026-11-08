const { unexpectedJavaStderr } = require('./fixtures/javaStderr')

describe('Java telemetry source-launcher diagnostics', () => {
  test('classifies both lines of the intentional classic-consumer deprecation notice', () => {
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
  ])('retains an unexpected diagnostic: %s', (line) => {
    expect(unexpectedJavaStderr(`${line}\n`)).toEqual([line])
  })
})
