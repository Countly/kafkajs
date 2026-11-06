const { createErrorFromCode } = require('../../protocol/error')
const errors = require('../../errors')
const fs = require('node:fs')
const path = require('node:path')

describe('broker error messages', () => {
  it.each(['broker detail', '', null, undefined])(
    'retains create-topic detail %p separately',
    async (errorMessage) => {
      const parse = require('../../protocol/requests/createTopics/v2/response').parse
      const frozen = createErrorFromCode(36)
      const error = await parse({
        topicErrors: [{ topic: 't', errorCode: 36, errorMessage }],
      }).catch((e) => e)
      const child = error.errors[0]
      expect(error.message).toBe('Topic creation errors')
      expect(child).toBeInstanceOf(errors.KafkaJSCreateTopicError)
      expect(child.message).toBe(frozen.message)
      expect(child).toMatchObject({
        topic: 't',
        code: 36,
        type: frozen.type,
        retriable: frozen.retriable,
        errorMessage: errorMessage ?? null,
      })
    }
  )
  it.each(['broker detail', '', null, undefined])(
    'retains alter-config detail %p separately',
    async (errorMessage) => {
      const parse = require('../../protocol/requests/alterConfigs/v0/response').parse
      const frozen = createErrorFromCode(40)
      const error = await parse({
        resources: [{ resourceType: 2, resourceName: 't', errorCode: 40, errorMessage }],
      }).catch((e) => e)
      expect(error).toBeInstanceOf(errors.KafkaJSProtocolError)
      expect(error.message).toBe(frozen.message)
      expect(error).toMatchObject({
        code: 40,
        type: frozen.type,
        retriable: frozen.retriable,
        errorMessage: errorMessage ?? null,
      })
    }
  )
  it('declares the runtime error inheritance without changing runtime bases', () => {
    const declarations = fs.readFileSync(path.join(__dirname, '../../../types/index.d.ts'), 'utf8')
    const classes = [
      'KafkaJSPartialMessageError',
      'KafkaJSSASLAuthenticationError',
      'KafkaJSGroupCoordinatorNotFound',
      'KafkaJSNotImplemented',
      'KafkaJSTimeout',
      'KafkaJSUnsupportedMagicByteInMessageSet',
      'KafkaJSLockTimeout',
    ]
    for (const name of classes) {
      const base = name === 'KafkaJSLockTimeout' ? 'KafkaJSTimeout' : 'KafkaJSNonRetriableError'
      expect(Object.getPrototypeOf(errors[name].prototype)).toBe(errors[base].prototype)
      expect(declarations.match(new RegExp(`export class ${name} extends (\\w+)`))[1]).toBe(base)
    }
  })
})
