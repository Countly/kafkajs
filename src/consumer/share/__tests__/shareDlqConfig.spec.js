const fs = require('fs')
const path = require('path')
const { Kafka } = require('../../../../index')
const { SHARE_CONSUMER_FIELDS, validateShareConsumerOptions } = require('..')
const {
  configureShareDlq,
  resetShareDlqClusterConfigs,
} = require('../../../../testHelpers/shareKafka44')

// Literal wire values (ConfigResource.Type, DescribeConfigs ConfigSource, AlterConfigOp.OpType).
const TOPIC_RESOURCE = 2
const BROKER_RESOURCE = 4
const GROUP_RESOURCE = 32
const TOPIC_CONFIG = 1
const DYNAMIC_DEFAULT_BROKER_CONFIG = 3
const GROUP_CONFIG = 8
const SET = 0
const DELETE = 1

const GROUP = 'dlq-config-group'
const SOURCE_TOPIC = 'dlq-config-source'
const DLQ_TOPIC = 'dlq.dlq-config-destination'

const SOURCE_BY_RESOURCE = {
  [TOPIC_RESOURCE]: TOPIC_CONFIG,
  [BROKER_RESOURCE]: DYNAMIC_DEFAULT_BROKER_CONFIG,
  [GROUP_RESOURCE]: GROUP_CONFIG,
}

/**
 * An admin double that applies each alteration to an in-memory table and reads it back with the
 * config source the broker gives that resource type.
 * @param {number} [errorCode=0] the code every alteration answers
 * @returns {object} `admin` plus the `alterations` it received, in order
 */
const fakeAdmin = (errorCode = 0) => {
  const table = new Map()
  const alterations = []
  const admin = {
    incrementalAlterConfigs: jest.fn(async ({ resources }) => {
      alterations.push(...resources)
      for (const { type, name, configEntries } of resources) {
        for (const entry of configEntries) {
          const key = `${type}/${name}/${entry.name}`
          if (entry.operation === SET) table.set(key, entry.value)
          else table.delete(key)
        }
      }
      return {
        resources: resources.map(({ type, name }) => ({
          errorCode,
          errorMessage: errorCode ? 'The config is invalid' : null,
          resourceType: type,
          resourceName: name,
        })),
      }
    }),
    describeConfigs: jest.fn(async ({ resources }) => ({
      resources: resources.map(({ type, name, configNames }) => ({
        errorCode: 0,
        errorMessage: null,
        resourceType: type,
        resourceName: name,
        configEntries: configNames
          .filter((configName) => table.has(`${type}/${name}/${configName}`))
          .map((configName) => ({
            configName,
            configValue: table.get(`${type}/${name}/${configName}`),
            configSource: SOURCE_BY_RESOURCE[type],
          })),
      })),
    })),
  }
  return { admin, alterations }
}

const entry = (name, value) => ({ name, operation: SET, value })

describe('Share DLQ configuration', () => {
  test('sets group destination and copy flag through existing config APIs', async () => {
    for (const copyRecord of [false, true]) {
      const { admin, alterations } = fakeAdmin()
      await configureShareDlq({
        admin,
        groupId: GROUP,
        sourceTopic: SOURCE_TOPIC,
        dlqTopic: DLQ_TOPIC,
        copyRecord,
      })
      expect(alterations.filter(({ type }) => type === GROUP_RESOURCE)).toEqual([
        {
          type: GROUP_RESOURCE,
          name: GROUP,
          configEntries: [
            entry('errors.deadletterqueue.topic.name', DLQ_TOPIC),
            entry('errors.deadletterqueue.copy.record.enable', String(copyRecord)),
          ],
        },
      ])
    }
  })

  test('enables a manually created DLQ topic', async () => {
    const { admin, alterations } = fakeAdmin()
    await configureShareDlq({
      admin,
      groupId: GROUP,
      sourceTopic: SOURCE_TOPIC,
      dlqTopic: DLQ_TOPIC,
      copyRecord: false,
    })
    expect(alterations.filter(({ type }) => type !== GROUP_RESOURCE)).toEqual([
      {
        type: BROKER_RESOURCE,
        name: '',
        configEntries: [
          entry('errors.deadletterqueue.topic.name.prefix', 'dlq.'),
          entry('errors.deadletterqueue.auto.create.topics.enable', 'false'),
        ],
      },
      {
        type: TOPIC_RESOURCE,
        name: DLQ_TOPIC,
        configEntries: [entry('errors.deadletterqueue.group.enable', 'true')],
      },
    ])

    const reset = fakeAdmin()
    await resetShareDlqClusterConfigs(reset.admin)
    expect(reset.alterations).toEqual([
      {
        type: BROKER_RESOURCE,
        name: '',
        configEntries: [
          { name: 'errors.deadletterqueue.topic.name.prefix', operation: DELETE },
          { name: 'errors.deadletterqueue.auto.create.topics.enable', operation: DELETE },
        ],
      },
    ])
  })

  test('refuses a destination equal to the source and surfaces a broker refusal', async () => {
    const same = fakeAdmin()
    await expect(
      configureShareDlq({
        admin: same.admin,
        groupId: GROUP,
        sourceTopic: SOURCE_TOPIC,
        dlqTopic: SOURCE_TOPIC,
        copyRecord: true,
      })
    ).rejects.toThrow(`Topic ${SOURCE_TOPIC} cannot be its own dead-letter destination`)
    expect(same.admin.incrementalAlterConfigs).not.toHaveBeenCalled()

    const refused = fakeAdmin(40)
    await expect(
      configureShareDlq({
        admin: refused.admin,
        groupId: GROUP,
        sourceTopic: SOURCE_TOPIC,
        dlqTopic: DLQ_TOPIC,
        copyRecord: true,
      })
    ).rejects.toThrow('error 40 The config is invalid')
    expect(refused.alterations).toHaveLength(1)
    await expect(resetShareDlqClusterConfigs(refused.admin)).rejects.toThrow('refused: error 40')
  })

  test('does not add a client DLQ option', () => {
    const named = /dead.?letter|dlq/i
    expect(SHARE_CONSUMER_FIELDS.filter((name) => named.test(name))).toEqual([])
    expect(
      validateShareConsumerOptions({ groupId: GROUP, deadLetterQueue: 'q', dlq: true })
    ).toEqual(validateShareConsumerOptions({ groupId: GROUP }))
    const kafka = new Kafka({ brokers: ['localhost:9092'] })
    for (const client of [kafka.admin(), kafka.shareConsumer({ groupId: GROUP })]) {
      expect(Object.keys(client).filter((name) => named.test(name))).toEqual([])
    }

    const types = fs.readFileSync(path.join(__dirname, '../../../../types/index.d.ts'), 'utf8')
    expect(types.split('\n').filter((line) => named.test(line))).toEqual([])
  })
})
