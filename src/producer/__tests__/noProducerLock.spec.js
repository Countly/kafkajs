jest.mock('../../utils/lock')

const fs = require('fs')
const path = require('path')
const Lock = require('../../utils/lock')
const createProducer = require('../index')
const createEosManager = require('../eosManager')
const { createFakeCluster } = require('./fixtures/fakeCluster')
const { KafkaJSLockTimeout } = require('../../errors')
const { newLogger } = require('testHelpers')

const SOURCE_ROOT = path.join(__dirname, '..', '..')
/** Built from parts, so this file does not name them itself. */
const LOCK_METHODS = ['acquire', 'release'].map((verb) => `${verb}BrokerLock`)

/**
 * Every JavaScript file under `directory`, tests included.
 * @param {string} directory
 * @returns {string[]}
 */
const javascriptFiles = (directory) =>
  fs
    .readdirSync(directory, { recursive: true })
    .filter((file) => file.endsWith('.js'))
    .map((file) => path.join(directory, file))

describe('Producer > no per-broker lock', () => {
  test('no file under src/ names the lock methods the eosManager used to have', () => {
    const naming = javascriptFiles(SOURCE_ROOT).filter((file) => {
      const source = fs.readFileSync(file, 'utf8')
      return LOCK_METHODS.some((method) => source.includes(method))
    })

    expect(naming).toEqual([])
  })

  test.each([false, true])(
    'an eosManager (transactional: %p) has no lock method',
    (transactional) => {
      const eosManager = createEosManager({
        logger: newLogger(),
        cluster: createFakeCluster(),
        transactional,
        transactionalId: transactional ? 'transactional-a' : undefined,
      })

      for (const method of LOCK_METHODS) {
        expect(eosManager).not.toHaveProperty(method)
      }
    }
  )

  test('idempotent and transactional sends construct no Lock and emit no LOCK_TIMEOUT', async () => {
    const producer = createProducer({
      cluster: createFakeCluster({ partitions: 2 }),
      logger: newLogger(),
      transactionalId: 'transactional-a',
    })
    const timeouts = []
    producer.on(producer.events.LOCK_TIMEOUT, (event) => timeouts.push(event))
    await producer.connect()

    await producer.send({ topic: 'topic-a', messages: [{ key: 'a', value: '1' }] })
    const transaction = await producer.transaction()
    await transaction.send({ topic: 'topic-a', messages: [{ key: 'b', value: '2' }] })
    await transaction.commit()

    expect(Lock).not.toHaveBeenCalled()
    expect(timeouts).toEqual([])
    expect(producer.events.LOCK_TIMEOUT).toEqual('producer.network.lock_timeout')
    expect(KafkaJSLockTimeout).toBeInstanceOf(Function)
  })
})
