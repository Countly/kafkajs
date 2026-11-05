const { randomBytes } = require('crypto')
const { AdminLiveProbe } = require('./fixtures/liveAdmin')
const { QuotaOracle, setProduceQuota } = require('./fixtures/liveOracles')
const { saslBrokers, saslConnectionOpts } = require('testHelpers')
const { liveCase, caseTimeout, delay } = require('../../producer/__tests__/fixtures/bounded')

const suite = process.env.ADMIN_QUOTA_E2E ? describe : describe.skip
const budget = 180000
const entity = [
  { entityType: 'user', entityName: 'live-quota' },
  { entityType: 'client-id', entityName: 'live-quota' },
]

suite('admin producer quotas', () => {
  test(
    'applies a produce quota and recovers after removal',
    liveCase(budget, async (signal) => {
      const probe = new AdminLiveProbe({ stem: 'quota-throttle', signal, budgetMs: budget })
      await probe.finish(async () => {
        const admin = await probe.start()
        const user = 'live-quota'
        const password = randomBytes(32).toString('base64')
        probe.credentials.push(Buffer.from(password))
        probe.effects.add('remove SCRAM principal', () =>
          admin.alterUserScramCredentials({ deletions: [{ user, mechanism: 'SCRAM-SHA-256' }] })
        )
        probe.items(
          await probe.invoke(() =>
            admin.alterUserScramCredentials({
              upsertions: [{ user, mechanism: 'SCRAM-SHA-256', password, iterations: 4096 }],
            })
          )
        )
        const removeQuota = () =>
          admin.alterClientQuotas({
            entries: [{ entity, ops: [{ key: 'producer_byte_rate', remove: true }] }],
          })
        probe.effects.add('remove produce quota', removeQuota)
        const topic = await probe.topic({ replicaAssignment: [{ partition: 0, replicas: [1, 2] }] })
        const initialMetadata = await probe.invoke(() =>
          admin.fetchTopicMetadata({ topics: [topic] })
        )
        const leaderId = initialMetadata.topics[0].partitions[0].leader
        const services = []
        for (const service of ['kafka1', 'kafka2', 'kafka3']) {
          if (
            Number((await probe.host(service, ['printenv', 'KAFKA_NODE_ID'])).trim()) === leaderId
          )
            services.push(service)
        }
        expect(services).toHaveLength(1)
        const service = services[0]
        const { ssl } = saslConnectionOpts()
        const producer = await probe.client(
          'producer',
          { idempotent: true, bufferMemory: 134217728, maxInFlightRequests: 1 },
          {
            clientId: 'live-quota',
            brokers: saslBrokers(),
            ssl,
            sasl: { mechanism: 'scram-sha-256', username: user, password },
          }
        )
        const pending = new Set()
        const sends = []
        const ackedIds = []
        probe.effects.add('join offered sends after quota removal', async () => {
          probe.items(await removeQuota())
          await probe.invoke(() => Promise.all(sends))
          if (pending.size || failedCount) throw new Error('Unsettled or failed offered sends')
        })
        const failedIds = []
        const failureCodes = []
        let failedCount = 0
        let sequence = 0
        let activeWindow
        let throttleMs = 0
        probe.removers.push(
          producer.on(producer.events.REQUEST, ({ payload }) => {
            throttleMs += payload.throttleTime || 0
          })
        )
        const offer = (scope) => {
          scope.throwIfAborted()
          const id = `quota-${sequence++}`
          const value = Buffer.alloc(1024)
          value.write(id)
          const send = producer
            .send({
              topic,
              acks: -1,
              compression: 0,
              messages: [{ partition: 0, key: null, value }],
            })
            .then(
              () => {
                ackedIds.push(id)
                const at = performance.now()
                if (activeWindow && at >= activeWindow.start && at < activeWindow.end)
                  activeWindow.payloadBytes += 1024
              },
              (error) => {
                failedIds.push(id)
                while (error?.cause) error = error.cause
                failureCodes.push(String(error?.code ?? error?.name ?? 'unknown'))
                failedCount++
              }
            )
            .finally(() => pending.delete(send))
          pending.add(send)
          sends.push(send)
        }
        const schedule = async (scope, durationMs, measure) => {
          const start = performance.now()
          const window = {
            start,
            end: start + durationMs,
            durationMs,
            payloadBytes: 0,
            offeredBytes: 0,
            maxPending: 0,
          }
          activeWindow = measure ? window : null
          let offered = 0
          while (performance.now() < window.end) {
            const target = Math.floor(
              (Math.min(durationMs, performance.now() - start) * 1024) / 1000
            )
            while (offered < target) {
              offer(scope)
              offered++
            }
            window.maxPending = Math.max(window.maxPending, pending.size)
            if (pending.size > 131072) throw new Error('Offered workload exceeded pending cap')
            await delay(scope, Math.min(5, Math.max(0, window.end - performance.now())))
          }
          const target = (durationMs * 1024) / 1000
          while (offered < target) {
            offer(scope)
            offered++
          }
          window.offeredBytes = offered * 1024
          activeWindow = null
          return window
        }
        const readQuota = () =>
          probe.invoke(() =>
            admin.describeClientQuotas({
              components: entity.map(({ entityType, entityName }) => ({
                entityType,
                matchType: 'exact',
                match: entityName,
              })),
              strict: true,
            })
          )
        probe.items(await probe.invoke(removeQuota))
        expect(await readQuota()).toEqual([])
        probe.items(
          await probe.invoke(() =>
            admin.alterClientQuotas({
              validateOnly: true,
              entries: [{ entity, ops: [{ key: 'producer_byte_rate', value: 65536 }] }],
            })
          )
        )
        expect(await readQuota()).toEqual([])
        const objectName = `kafka.server:type=BrokerTopicMetrics,name=BytesInPerSec,topic=${topic}`
        const meter = async () => {
          const metadata = await probe.invoke(() => admin.fetchTopicMetadata({ topics: [topic] }))
          expect(metadata.topics[0].partitions[0].leader).toBe(leaderId)
          const raw = await probe.host(service, [
            '/opt/kafka/bin/kafka-run-class.sh',
            'org.apache.kafka.tools.JmxTool',
            '--jmx-url',
            'service:jmx:rmi:///jndi/rmi://localhost:9999/jmxrmi',
            '--object-name',
            objectName,
            '--attributes',
            'Count',
            '--one-time',
            'true',
          ])
          probe.receipt('quota-jmx.jsonl', {
            service,
            objectName,
            attribute: 'Count',
            raw,
            at: performance.now(),
          })
          const lines = raw.trim().split('\n')
          if (lines.length !== 2 || !lines[0].includes('Count') || lines[0].includes('Rate'))
            throw new Error('Missing unambiguous cumulative JMX Count')
          const fields = lines[1].split(',').map((field) => field.replaceAll('"', ''))
          const count = Number(fields[fields.length - 1])
          if (!Number.isSafeInteger(count) || count < 0) throw new Error('Invalid broker Count')
          const at = performance.now()
          probe.receipt('quota-jmx.jsonl', {
            service,
            objectName,
            attribute: 'Count',
            raw,
            count,
            at,
          })
          return { count, at }
        }
        /**
         * The leader creates a topic's own meter on its first append, so before the first produce
         * the JMX tool fails with "No matched attributes"; this waits for the meter to exist.
         * @returns {Promise<{ count: number, at: number }>} the first reading of the meter
         * @throws {Error} any failure of the read other than the meter not existing yet
         */
        const awaitMeter = async () => {
          let reading
          await probe.poll(async () => {
            try {
              reading = await meter()
            } catch (error) {
              if (!error.message.includes('No matched attributes')) throw error
              return false
            }
            return true
          }, 1000)
          return reading
        }
        const sendOne = () =>
          probe.invoke(() =>
            producer.send({
              topic,
              acks: -1,
              messages: [{ value: Buffer.alloc(1024, 'x'), partition: 0 }],
            })
          )
        expect(await sendOne()).toHaveLength(1)
        const quietBefore = await awaitMeter()
        expect(quietBefore.count).toBeGreaterThan(0)
        const quietAfter = await meter()
        expect(quietAfter.count).toBe(quietBefore.count)
        expect(await sendOne()).toHaveLength(1)
        const positiveAfter = await meter()
        expect(positiveAfter.count).toBeGreaterThan(quietAfter.count)
        await probe.invoke(removeQuota)
        await probe.invoke((scope) => schedule(scope, 10000, false))
        const control = await probe.invoke((scope) => schedule(scope, 30000, true))
        probe.items(await probe.invoke(() => setProduceQuota(admin)))
        await probe.poll(async () => {
          const rows = await readQuota()
          return rows.length === 1 && rows[0].values.producer_byte_rate === 65536
        })
        await probe.invoke((scope) => schedule(scope, 10000, false))
        const before = await meter()
        const quota = await probe.invoke((scope) => schedule(scope, 30000, true))
        const after = await meter()
        expect(before.at).toBeLessThanOrEqual(quota.start)
        expect(after.at).toBeGreaterThanOrEqual(quota.end)
        expect(after.count).toBeGreaterThanOrEqual(before.count)
        probe.items(await probe.invoke(removeQuota))
        await probe.poll(async () => (await readQuota()).length === 0)
        await probe.invoke((scope) => schedule(scope, 10000, false))
        const recovery = await probe.invoke((scope) => schedule(scope, 20000, true))
        await probe.invoke(() => Promise.all(sends))
        expect(pending.size).toBe(0)
        const physical = await probe.physical(topic)
        const physicalIds = physical
          .get(0)
          .records.map(({ value }) => value.toString().replace(/\0+$/, ''))
          .filter((id) => id.startsWith('quota-'))
        const sample = {
          control,
          quota,
          recovery,
          brokerBytes: after.count - before.count,
          brokerElapsedMs: after.at - before.at,
          controlPending: control.maxPending,
          quotaPending: quota.maxPending,
          throttleMs,
          failedCount,
          ackedIds,
          physicalIds,
        }
        probe.record('offered send outcomes', {
          attemptedCount: sends.length,
          ackedCount: ackedIds.length,
          failedCount,
          failedIds,
          failureCode: failureCodes,
        })
        QuotaOracle.verify(sample)
        probe.record('matched produce quota and recovery', {
          brokerBytes: sample.brokerBytes,
          brokerElapsedMs: Math.round(sample.brokerElapsedMs),
          leaderBroker: leaderId,
          attemptedCount: sends.length,
          controlOfferedBytes: control.offeredBytes,
          quotaOfferedBytes: quota.offeredBytes,
          recoveryOfferedBytes: recovery.offeredBytes,
          controlPendingCount: control.maxPending,
          quotaPendingCount: quota.maxPending,
          throttleMs,
          ackedCount: ackedIds.length,
          failedCount,
          controlBytes: control.payloadBytes,
          quotaBytes: quota.payloadBytes,
          recoveryBytes: recovery.payloadBytes,
        })
      })
    }),
    caseTimeout(budget)
  )
})
