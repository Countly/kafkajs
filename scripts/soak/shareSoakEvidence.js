const { shareRecordIdentity } = require('../../testHelpers/shareFixtures')

/**
 * Adds call-through primitive archival to an existing owned driver. No client, realm, baseline,
 * deadline or workload is created. Original arguments, results and failures remain unchanged.
 */
class ShareSoakEvidence {
  /**
   * @param {object} driver Original real or scripted cluster adapter.
   * @param {Function} record Scope-bound serializing evidence sink.
   */
  constructor(driver, record) {
    Object.assign(this, { driver, record, cycle: null, kind: 'cycle' })
  }

  /**
   * Observes an original method through a local proxy, preserving the original receiver.
   * @param {object} target Original admin client.
   * @param {object} methods Call-through wrappers keyed by method.
   * @returns {object} Local observing view; the original client remains the resource owner.
   */
  proxy(target, methods) {
    return new Proxy(target, {
      get: (object, key) =>
        methods[key]
          ? (...args) => methods[key](object[key].bind(object), args)
          : Reflect.get(object, key),
    })
  }

  /**
   * Installs per-instance observations before any driver effect. Reinstallation is refused.
   * @returns {object} The same original driver.
   * @throws {Error} On repeated observation or serialization failure.
   */
  install() {
    const { driver, record } = this
    if (driver.primitiveEvidence) throw new Error('Original driver already observed')
    driver.primitiveEvidence = this
    const configuration = driver.readConfiguration.bind(driver)
    driver.readConfiguration = async (admin, brokers, scope) => {
      const original = await configuration(admin, brokers, scope)
      const topicConfig = await driver.ports.bounded.bounded(scope, Infinity, () =>
        admin.describeConfigs({
          resources: [
            {
              type: 2,
              name: driver.topic,
              configNames: ['min.insync.replicas', 'retention.ms', 'cleanup.policy'],
            },
          ],
          includeSynonyms: true,
        })
      )
      const topicMetadata = await driver.ports.bounded.bounded(scope, Infinity, () =>
        admin.fetchTopicMetadata({ topics: [driver.topic] })
      )
      return { ...original, topicConfig, topicMetadata }
    }
    const cycle = driver.cycle.bind(driver)
    driver.cycle = async (plan, ...args) => {
      this.cycle = plan.cycle
      this.kind = 'cycle'
      const observation = await cycle(plan, ...args)
      record('soak-original-cycle', { cycle: plan.cycle, observation })
      return observation
    }
    const final = driver.final.bind(driver)
    driver.final = async (...args) => {
      this.kind = 'final'
      const observation = await final(...args)
      record('soak-original-final', { observation })
      return observation
    }
    const produce = driver.produce.bind(driver)
    driver.produce = async (plan, ...args) => {
      const original = driver.producer
      driver.producer = this.proxy(original, {
        send: async (send, [options]) => {
          const sent = await send(options)
          record('soak-producer', {
            cycle: plan.cycle,
            topic: options.topic,
            partition: options.messages[0].partition,
            messages: options.messages.map((m) => ({
              partition: m.partition,
              key: m.key.toString('hex'),
              value: m.value.toString('hex'),
              timestamp: m.timestamp,
            })),
            sent,
          })
          return sent
        },
      })
      try {
        return await produce(plan, ...args)
      } finally {
        driver.producer = original
      }
    }
    const readLogs = driver.readLogs.bind(driver)
    driver.readLogs = (plan, log) => {
      const records = [...log].flatMap(([partition, row]) =>
        row.records.map((record) => ({
          topic: driver.topic,
          partition,
          offset: String(record.offset),
          key: record.key?.toString('hex') ?? null,
          value: record.value?.toString('hex') ?? null,
        }))
      )
      record('soak-raw-log', {
        cycle: plan.cycle,
        kind: this.kind,
        records,
        identities: records.map((r) =>
          shareRecordIdentity({
            ...r,
            key: r.key === null ? null : Buffer.from(r.key, 'hex'),
            value: r.value === null ? null : Buffer.from(r.value, 'hex'),
          })
        ),
        ends: Object.fromEntries(
          [...log].map(([partition, r]) => [
            partition,
            { end: r.endOffset, count: r.records.length },
          ])
        ),
      })
      return readLogs(plan, log)
    }
    const awaitMembers = driver.awaitMembers.bind(driver)
    driver.awaitMembers = async (scope, members) => {
      const original = driver.admin
      const reads = []
      driver.admin = this.proxy(original, {
        describeShareGroups: async (describe, args) => {
          const response = await describe(...args)
          reads.push(response)
          return response
        },
      })
      try {
        const result = await awaitMembers(scope, members)
        record('soak-membership', {
          cycle: this.cycle,
          members: members.map((m) => ({ memberId: m.memberId, assignment: m.assignment })),
          described: reads.at(-1)?.groups?.[0]?.members,
          reads,
        })
        return result
      } finally {
        driver.admin = original
      }
    }
    const fixtures = driver.ports.fixtures
    driver.ports = {
      ...driver.ports,
      fixtures: {
        ...fixtures,
        resetShareGroupConfigs: async (admin, group, names, options) => {
          let resources
          let read
          const proxy = this.proxy(admin, {
            incrementalAlterConfigs: async (alter, args) => {
              const result = await alter(...args)
              resources = result.resources
              return result
            },
            describeConfigs: async (describe, args) => {
              const result = await describe(...args)
              read = result
              return result
            },
          })
          try {
            return await fixtures.resetShareGroupConfigs(proxy, group, names, options)
          } finally {
            record('soak-cleanup', { step: 'config-reset', group, names, resources, read })
          }
        },
        assertShareCleanup: (admin, groups, topics, baseline, probe, options) =>
          fixtures.assertShareCleanup(admin, groups, topics, baseline, probe, {
            ...options,
            record: (step, fields) => {
              options?.record?.(step, fields)
              record('soak-cleanup', { step, ...fields })
            },
          }),
      },
    }
    return driver
  }
}

/**
 * Archives original driver observations in the owner's current scope.
 * @param {object} driver Original driver, registered before preparation.
 * @param {Function} record Scope-bound serializing sink.
 * @returns {object} The same observed driver.
 * @throws {Error} On repeated installation.
 */
const observeShareSoakDriver = (driver, record) => new ShareSoakEvidence(driver, record).install()
module.exports = { ShareSoakEvidence, observeShareSoakDriver }
