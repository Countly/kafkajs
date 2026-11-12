const fs = require('fs')
const path = require('path')

const root = path.join(__dirname, '..', '..', '..')
const DEFAULT = '4.4.0'

/**
 * @param {...string} parts path segments below the repository root
 * @returns {string} the file's text
 */
const read = (...parts) => fs.readFileSync(path.join(root, ...parts), 'utf8')

/**
 * @param {string} text file text
 * @returns {string[]} every `${KAFKA_VERSION:-<default>}` default in it, in order
 */
const defaultsIn = (text) => [...text.matchAll(/\$\{KAFKA_VERSION:-([^}]+)\}/g)].map((m) => m[1])

describe('default test broker', () => {
  const composeFiles = fs
    .readdirSync(root)
    .filter((name) => /^docker-compose.*\.yml$/.test(name))
    .sort()

  test.each(composeFiles)('%s starts Kafka 4.4.0 unless KAFKA_VERSION says otherwise', (name) => {
    const text = read(name)
    const images = [...text.matchAll(/image: apache\/kafka:(\S+)/g)].map((m) => m[1])

    expect(images.length).toBeGreaterThan(0)
    for (const image of images) expect(image).toBe(`\${KAFKA_VERSION:-${DEFAULT}}`)
  })

  test('the cluster startup script announces Kafka 4.4.0 as the default', () => {
    expect(defaultsIn(read('scripts', 'dockerComposeUp.sh'))).toEqual([DEFAULT])
  })

  test('the jest setup defaults KAFKA_VERSION to Kafka 4.4.0', () => {
    expect(read('testHelpers', 'setup.js')).toContain(`process.env.KAFKA_VERSION ||= '${DEFAULT}'`)
  })

  test('the ApiVersions probe falls back to Kafka 4.4.0', () => {
    expect(read('scripts', 'e2e', 'apiVersionsProbe.js')).toContain(
      `process.env.KAFKA_VERSION || '${DEFAULT}'`
    )
  })

  test('no package.json script defaults the broker to another version', () => {
    const { scripts } = JSON.parse(read('package.json'))
    const defaults = Object.entries(scripts).flatMap(([name, command]) =>
      defaultsIn(command).map((version) => [name, version])
    )

    expect(defaults.length).toBeGreaterThan(0)
    expect(defaults.filter(([, version]) => version !== DEFAULT)).toEqual([])
  })
})
