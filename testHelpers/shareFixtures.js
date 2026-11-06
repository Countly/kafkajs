const sleep = require('../src/utils/sleep')
const CONFIG_RESOURCE_TYPES = require('../src/protocol/configResourceTypes')
const CONFIG_SOURCE = require('../src/protocol/configSource')
const ALTER_CONFIG_OP_TYPES = require('../src/protocol/alterConfigOpTypes')
const helpers = require('./index')

/** The topics of the share-group scenarios start with this, so leftovers on a cluster are found. */
const SHARE_TOPIC_PREFIX = 'e2e-p9e-'

/** A scenario name: lower-case kebab-case, so it can never change the topic's shape. */
const SCENARIO_NAME = /^[a-z0-9]+(?:-[a-z0-9]+)*$/

/**
 * A fresh topic name for a share-group scenario, `e2e-p9e-<scenario>-<random>`.
 * @param {string} scenario e.g. `delivery`
 * @returns {string}
 * @throws {Error} when `scenario` is not lower-case kebab-case
 */
const shareTopicName = (scenario) => {
  if (typeof scenario !== 'string' || !SCENARIO_NAME.test(scenario)) {
    throw new Error(
      `A share scenario name is lower-case kebab-case, got ${JSON.stringify(scenario)}`
    )
  }

  return `${SHARE_TOPIC_PREFIX}${scenario}-${helpers.secureRandom()}`
}

/**
 * Creates a fresh topic for a share-group scenario through `createTopic`, which returns once every
 * broker serves the topic with a leader for each partition.
 * @param {{ scenario: string, partitions?: number, replicas?: number }} options
 * @returns {Promise<string>} the name of the topic it created
 * @throws {Error} when the scenario name is invalid (before anything is sent), or what
 *   `createTopic` throws
 */
const createShareTopic = async ({ scenario, partitions = 1, replicas = 1 }) => {
  const topic = shareTopicName(scenario)
  await helpers.createTopic({ topic, partitions, replicas })
  return topic
}

/**
 * Which of the wanted configs one `describeConfigs` answer does not show for the resource yet. A
 * value counts only on an entry of that very resource whose source is `GROUP_CONFIG`: a default or
 * a broker-wide value that happens to be equal does not prove the change applied.
 * @param {object} admin a connected admin client
 * @param {{ type: number, name: string }} resource the GROUP resource
 * @param {[string, string][]} wanted config names and their values
 * @returns {Promise<string[]>} `name=value` of each config not read back
 */
const unreadConfigs = async (admin, resource, wanted) => {
  const { resources } = await admin.describeConfigs({
    resources: [{ ...resource, configNames: wanted.map(([name]) => name) }],
  })
  const read = resources.find(
    ({ resourceType, resourceName }) =>
      resourceType === resource.type && resourceName === resource.name
  )
  const entries = new Map((read?.configEntries ?? []).map((entry) => [entry.configName, entry]))

  return wanted
    .filter(([name, value]) => {
      const entry = entries.get(name)
      return !(
        entry &&
        entry.configValue === value &&
        entry.configSource === CONFIG_SOURCE.GROUP_CONFIG
      )
    })
    .map(([name, value]) => `${name}=${value}`)
}

/**
 * Sets configs on a share group's GROUP resource and resolves only once one `describeConfigs`
 * answer reads every value back as a group config. A member that joins before a change applies
 * starts from the old values (`share.auto.offset.reset` is `latest` until the change is read back),
 * so a scenario calls this before its first member joins.
 *
 * It sets exactly the keys it is given, with one `incrementalAlterConfigs` SET each, so a scenario
 * never inherits a lock or isolation level it did not ask for. A broker that refuses the change (a
 * lock below the broker minimum, an unknown value) fails the call at once with the broker's code
 * and message; nothing is read back then. Reads are repeated every `interval` until `timeout`.
 * @param {object} admin a connected admin client (`kafka.admin()`)
 * @param {string} group the share group's id
 * @param {Record<string, string|number|boolean>} kv config names and values, e.g.
 *   `{ 'share.auto.offset.reset': 'earliest', 'share.record.lock.duration.ms': 15000 }`
 * @param {{ timeout?: number, interval?: number }} [options] how long to keep reading back, in ms
 *   (default 10000), and the pause between two reads (default 250)
 * @returns {Promise<void>}
 * @throws {Error} when `kv` names no config; when the broker refuses the change; when a value is
 *   still not read back at the deadline, naming every such `name=value`
 */
const setShareGroupConfigs = async (admin, group, kv, { timeout = 10000, interval = 250 } = {}) => {
  const wanted = Object.entries(kv).map(([name, value]) => [name, String(value)])
  if (wanted.length === 0) {
    throw new Error(`No config to set on group ${group}`)
  }

  const resource = { type: CONFIG_RESOURCE_TYPES.GROUP, name: group }
  const configEntries = wanted.map(([name, value]) => ({
    name,
    operation: ALTER_CONFIG_OP_TYPES.SET,
    value,
  }))
  const { resources } = await admin.incrementalAlterConfigs({
    resources: [{ ...resource, configEntries }],
  })
  const [result] = resources
  if (result.errorCode !== 0) {
    throw new Error(
      `Group ${group} refused ${configEntries.map(({ name }) => name).join(', ')}: error ${
        result.errorCode
      } ${result.errorMessage}`
    )
  }

  const deadline = Date.now() + timeout
  for (;;) {
    const unread = await unreadConfigs(admin, resource, wanted)
    if (unread.length === 0) {
      return
    }

    if (Date.now() >= deadline) {
      throw new Error(`Group ${group} did not read back ${unread.join(', ')} within ${timeout} ms`)
    }

    await sleep(interval)
  }
}

module.exports = { SHARE_TOPIC_PREFIX, shareTopicName, createShareTopic, setShareGroupConfigs }
