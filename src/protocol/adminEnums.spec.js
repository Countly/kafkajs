const path = require('path')
const { execFileSync } = require('child_process')
const { pathToFileURL } = require('url')
const kafkajs = require('../../index')
const ConfigResourceTypes = require('./configResourceTypes')
const ConfigSource = require('./configSource')
const AlterConfigOpTypes = require('./alterConfigOpTypes')
const ConfigTypes = require('./configTypes')

/**
 * @returns {string[]} the names `index.mjs` exports, read by importing it in a child process, so
 *   the check holds for whatever form the file's exports take
 */
const esmExportNames = () => {
  const entry = pathToFileURL(path.join(__dirname, '..', '..', 'index.mjs')).href
  const script = `import * as entry from '${entry}'; console.log(JSON.stringify(Object.keys(entry)))`
  const output = execFileSync(process.execPath, ['--input-type=module', '-e', script], {
    encoding: 'utf8',
  })

  return JSON.parse(output).filter((name) => name !== 'default')
}

describe('Protocol > admin enums', () => {
  test('ConfigResourceTypes keeps its values and gains CLIENT_METRICS and GROUP', () => {
    expect(ConfigResourceTypes).toEqual({
      UNKNOWN: 0,
      TOPIC: 2,
      BROKER: 4,
      BROKER_LOGGER: 8,
      CLIENT_METRICS: 16,
      GROUP: 32,
    })
  })

  test('ConfigSource keeps its values and gains CLIENT_METRICS_CONFIG and GROUP_CONFIG', () => {
    expect(ConfigSource).toEqual({
      UNKNOWN: 0,
      TOPIC_CONFIG: 1,
      DYNAMIC_BROKER_CONFIG: 2,
      DYNAMIC_DEFAULT_BROKER_CONFIG: 3,
      STATIC_BROKER_CONFIG: 4,
      DEFAULT_CONFIG: 5,
      DYNAMIC_BROKER_LOGGER_CONFIG: 6,
      CLIENT_METRICS_CONFIG: 7,
      GROUP_CONFIG: 8,
    })
  })

  test('AlterConfigOpTypes is the frozen AlterConfigOp.OpType', () => {
    expect(AlterConfigOpTypes).toEqual({ SET: 0, DELETE: 1, APPEND: 2, SUBTRACT: 3 })
    expect(Object.isFrozen(AlterConfigOpTypes)).toBe(true)
  })

  test('ConfigTypes is the frozen ConfigEntry.ConfigType, in the order of its ids', () => {
    expect(ConfigTypes).toEqual({
      UNKNOWN: 0,
      BOOLEAN: 1,
      STRING: 2,
      INT: 3,
      SHORT: 4,
      LONG: 5,
      DOUBLE: 6,
      LIST: 7,
      CLASS: 8,
      PASSWORD: 9,
    })
    expect(Object.isFrozen(ConfigTypes)).toBe(true)
  })

  test('the entry point exports the four enums', () => {
    expect(kafkajs.ConfigResourceTypes).toBe(ConfigResourceTypes)
    expect(kafkajs.ConfigSource).toBe(ConfigSource)
    expect(kafkajs.AlterConfigOpTypes).toBe(AlterConfigOpTypes)
    expect(kafkajs.ConfigTypes).toBe(ConfigTypes)
  })

  test('the ESM entry point exports every name of the CommonJS one, and no other', () => {
    expect(esmExportNames().sort()).toEqual(Object.keys(kafkajs).sort())
  })
})
