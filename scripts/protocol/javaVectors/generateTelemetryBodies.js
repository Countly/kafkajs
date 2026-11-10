const fs = require('fs')
const path = require('path')
const { encodeMetricsData } = require('../../../src/telemetry/otlp')

/**
 * Writes actual production bodies for independent Java verification, without deriving expected bytes.
 * Restores integer domains and preserves JSON negative zero from the literal vector inputs.
 * @param {string} directory destination for the eight binary bodies
 * @returns {void}
 * @throws {Error} for invalid arguments, fixture inputs, encoding or filesystem failures
 */
const generate = (directory) => {
  if (!directory) throw new Error('Expected an output directory')
  fs.mkdirSync(directory, { recursive: true })
  const fixtures = path.join(__dirname, '../../../src/telemetry/__tests__/fixtures/otlp')
  for (const name of [
    'cumulative',
    'empty',
    'multibyte',
    'negative-zero',
    'nested',
    'signed-max',
    'signed-min',
    'zero',
  ]) {
    const { fields } = JSON.parse(fs.readFileSync(path.join(fixtures, `${name}.json`), 'utf8'))
    const snapshot = {
      ...fields,
      points: fields.points.map((point) => ({
        ...point,
        identity: { ...point.identity, revision: BigInt(point.identity.revision) },
        value:
          point.type === 'sum' || point.valueKind === 'int' ? BigInt(point.value) : point.value,
        timeUnixNano: BigInt(point.timeUnixNano),
        startTimeUnixNano:
          point.startTimeUnixNano === undefined ? undefined : BigInt(point.startTimeUnixNano),
      })),
    }
    fs.writeFileSync(path.join(directory, `${name}.bin`), encodeMetricsData(snapshot))
  }
}

generate(process.argv[2])
