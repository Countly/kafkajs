const { Kafka } = require('../../../index')
const { createScriptedCluster, groupScript } = require('../../../testHelpers/scriptedCluster')
const { lanes } = require('../../../testHelpers/lane')
const { plainTextBrokers, secureRandom } = require('testHelpers')
const { writeEvidence } = require('../../../testHelpers/e2eEvidence')
const { TOPIC } = require('./fixtures/fetchHarness')

/**
 * Starts the already subscribed consumer and observes the public terminal metadata failure.
 * @param {import('../../../types').Consumer} consumer the connected, subscribed consumer
 * @returns {Promise<Error>} the original unknown-topic cause reported by CRASH
 * @throws {Error} if no crash arrives within the bounded observation window
 */
const startAfterDeletion = async (consumer) => {
  let timer
  let remove
  const crashed = new Promise((resolve) => {
    remove = consumer.on(consumer.events.CRASH, resolve)
  })
  const deadline = new Promise((resolve, reject) => {
    timer = setTimeout(
      () => reject(new Error('Post-deletion consumer crash did not arrive')),
      15000
    )
  })
  try {
    const [, { payload }] = await Promise.all([
      consumer.run({ eachMessage: async () => {} }),
      Promise.race([crashed, deadline]),
    ])
    expect(payload.restart).toBe(false)
    let error = payload.error
    while (error.cause) error = error.cause
    expect(error).toMatchObject({ type: 'UNKNOWN_TOPIC_OR_PARTITION' })
    return error
  } finally {
    clearTimeout(timer)
    remove()
  }
}

/** No log records or handler deliveries; a disabled named metadata lookup must not recreate. */
describe.each(lanes())('Auto topic creation on %s', (lane) => {
  const fake = lane === 'fake' ? test : test.skip
  fake.each([undefined, true, false])(
    'Metadata carries allowAutoTopicCreation=%s',
    async (flag) => {
      const scripted = groupScript(
        createScriptedCluster({ nodes: [{ id: 0, host: 'node', port: 9092 }] }),
        { batches: 0 }
      )
      const consumer = new Kafka(scripted.kafkaOptions()).consumer({
        groupId: 'auto-create',
        allowAutoTopicCreation: flag,
      })
      try {
        await consumer.connect()
        await consumer.subscribe({ topic: TOPIC })
        const metadata = scripted.requests().filter((request) => request.api === 'Metadata')
        expect(metadata.length).toBeGreaterThan(0)
        expect(
          metadata.every((request) => request.fields.allowAutoTopicCreation === (flag ?? true))
        ).toBe(true)
      } finally {
        try {
          await consumer.disconnect()
        } finally {
          scripted.assertClean()
        }
      }
    }
  )

  fake('a later run sends disabled Metadata and reports a deleted subscribed topic', async () => {
    const scripted = groupScript(
      createScriptedCluster({ nodes: [{ id: 0, host: 'node', port: 9092 }] }),
      { batches: 0 }
    )
    const consumer = new Kafka(scripted.kafkaOptions({ retry: { retries: 0 } })).consumer({
      groupId: 'deleted-topic',
      allowAutoTopicCreation: false,
      metadataMaxAge: 0,
      retry: { retries: 0, restartOnFailure: async () => false },
    })
    try {
      await consumer.connect()
      await consumer.subscribe({ topic: TOPIC })
      const before = scripted.requests().filter(({ api }) => api === 'Metadata').length
      scripted.answer('Metadata', () => ({
        topics: [{ name: TOPIC, errorCode: 3, partitions: [] }],
      }))
      await startAfterDeletion(consumer)
      const later = scripted
        .requests()
        .filter(({ api }) => api === 'Metadata')
        .slice(before)
      expect(later.length).toBeGreaterThan(0)
      expect(later.every(({ fields }) => fields.allowAutoTopicCreation === false)).toBe(true)
      expect(later.some(({ fields }) => fields.topics.some(({ name }) => name === TOPIC))).toBe(
        true
      )
    } finally {
      try {
        await consumer.disconnect()
      } finally {
        scripted.assertClean()
      }
    }
  })

  const live = lane === 'docker' && process.env.AUTO_TOPIC_CREATION_E2E === '1' ? test : test.skip
  live('a deleted subscribed topic stays absent when auto creation is disabled', async () => {
    const name = `e2e-p9a-auto-topic-${secureRandom()}`
    const kafka = new Kafka({ brokers: plainTextBrokers(), retry: { retries: 0 } })
    const admin = kafka.admin()
    const consumer = kafka.consumer({
      groupId: `${name}-group`,
      allowAutoTopicCreation: false,
      metadataMaxAge: 0,
      retry: { retries: 0, restartOnFailure: async () => false },
    })
    try {
      await admin.connect()
      await admin.createTopics({
        topics: [{ topic: name, numPartitions: 1, replicationFactor: 1 }],
      })
      await consumer.connect()
      await consumer.subscribe({ topic: name })
      await admin.deleteTopics({ topics: [name] })
      for (let attempt = 0; attempt < 100; attempt++) {
        if (!(await admin.listTopics()).includes(name)) break
        await new Promise((resolve) => setTimeout(resolve, 50))
      }
      expect(await admin.listTopics()).not.toContain(name)
      await startAfterDeletion(consumer)
      for (let attempt = 0; attempt < 20; attempt++) {
        expect(await admin.listTopics()).not.toContain(name)
        await new Promise((resolve) => setTimeout(resolve, 50))
      }
      writeEvidence('auto-topic-creation', {
        topic: name,
        allowAutoTopicCreation: false,
        recreated: false,
      })
    } finally {
      await consumer.disconnect()
      await admin.deleteTopics({ topics: [name] }).catch(() => {})
      await admin.disconnect()
    }
  })
})
