const { createHash } = require('crypto')
const { isDeepStrictEqual } = require('util')
const { AdminLiveProbe } = require('./fixtures/liveAdmin')
const { ConfigResourceTypes, ConfigSource } = require('../../../index')
const { liveCase, caseTimeout } = require('../../producer/__tests__/fixtures/bounded')

const suite = process.env.ADMIN_LOGDIRS_E2E ? describe : describe.skip
const budget = 300000
const key = 'replica.alter.log.dirs.io.max.bytes.per.second'
const directories = ['/tmp/kraft-logs-a', '/tmp/kraft-logs-b']

/** @param {Map<number, object>} log raw partition data @returns {Array<object>} complete data corpus */
const corpus = (log) =>
  [...log].flatMap(([partition, { records }]) =>
    records.map(({ offset, value }) => ({ partition, offset, value: value.toString('hex') }))
  )

suite('admin replica directories', () => {
  test(
    'moves a replica between configured directories without changing its log',
    liveCase(budget, async (signal) => {
      const probe = new AdminLiveProbe({ stem: 'admin-logdirs', signal, budgetMs: budget })
      await probe.finish(async () => {
        for (const service of ['kafka1', 'kafka2', 'kafka3']) {
          const formatted = []
          for (const directory of directories) {
            expect(
              (await probe.host(service, ['test', '-s', `${directory}/meta.properties`])).trim()
            ).toBe('')
            formatted.push(directory)
          }
          expect(formatted.sort()).toEqual([...directories].sort())
        }
        const admin = await probe.start()
        const topic = await probe.topic({ replicaAssignment: [{ partition: 0, replicas: [1, 2] }] })
        const producer = await probe.client('producer', {
          idempotent: true,
          maxInFlightRequests: 1,
        })
        for (let base = 0; base < 16384; base += 128) {
          await probe.invoke(() =>
            producer.send({
              topic,
              acks: -1,
              messages: Array.from({ length: 128 }, (_, index) => {
                const value = Buffer.alloc(1024)
                value.write(`record-${base + index}`)
                return { partition: 0, value }
              }),
            })
          )
        }
        const before = corpus(await probe.physical(topic))
        expect(before).toHaveLength(16384)
        const replica = { topic, partition: 0, brokerId: 1 }
        const locate = async () => {
          const rows = await probe.invoke(() =>
            admin.describeReplicaLogDirs({ replicas: [replica] })
          )
          probe.items(rows)
          expect(rows).toHaveLength(1)
          return rows[0]
        }
        const initial = await locate()
        const source = initial.currentLogDir
        expect(directories).toContain(source)
        expect(initial.futureLogDir).toBeNull()
        const destination = directories.find((directory) => directory !== source)
        expect(destination).not.toBe(source)
        const resource = { type: ConfigResourceTypes.BROKER, name: '1' }
        /**
         * The key is dynamic-only, so the broker lists it only while it is set.
         * @returns {Promise<{ value: string, source: number }|null>} null when no entry is listed
         */
        const readThrottle = async () => {
          const result = await probe.invoke(() =>
            admin.describeConfigs({ resources: [{ ...resource, configNames: [key] }] })
          )
          probe.items(result.resources)
          const entry = result.resources[0].configEntries.find(
            ({ configName }) => configName === key
          )
          return entry ? { value: entry.configValue, source: entry.configSource } : null
        }
        /**
         * Waits for the broker to apply a change, which reaches it from the metadata log.
         * @param {{ value: string, source: number }|null} expected the entry to list, or null
         * @returns {Promise<void>}
         * @throws {Error} naming the key, the expected entry and the last one read
         */
        const awaitThrottle = async (expected) => {
          let seen
          await probe
            .poll(async () => {
              seen = await readThrottle()
              return isDeepStrictEqual(seen, expected)
            })
            .catch((error) => {
              throw new Error(
                `Broker 1 did not list ${key} as ${JSON.stringify(expected)}; last read ${JSON.stringify(seen)}`,
                { cause: error }
              )
            })
        }
        const original = await readThrottle()
        const setThrottle = (value) =>
          admin.incrementalAlterConfigs({
            resources: [
              {
                ...resource,
                configEntries: [{ name: key, value, operation: value === null ? 1 : 0 }],
              },
            ],
          })
        const restoreThrottle = async () => {
          const dynamic = original?.source === ConfigSource.DYNAMIC_BROKER_CONFIG
          probe.items((await setThrottle(dynamic ? original.value : null)).resources)
          await awaitThrottle(original)
        }
        probe.effects.add('restore replica movement throttle', restoreThrottle)
        probe.items((await probe.invoke(() => setThrottle('262144'))).resources)
        await awaitThrottle({ value: '262144', source: ConfigSource.DYNAMIC_BROKER_CONFIG })
        const moves = await probe.invoke(() =>
          admin.alterReplicaLogDirs({ replicas: [{ ...replica, logDir: destination }] })
        )
        probe.items(moves)
        let future
        await probe.poll(async () => {
          future = await locate()
          return future.currentLogDir === source && future.futureLogDir === destination
        }, 100)
        expect(future.currentLogDir).toBe(source)
        expect(future.futureLogDir).toBe(destination)
        const dirs = await probe.invoke(() => admin.describeLogDirs({ brokers: [1] }))
        const version = probe.requests
          .filter(({ apiName }) => apiName === 'DescribeLogDirs')
          .at(-1).apiVersion
        probe.items(dirs)
        for (const { logDirs } of dirs) {
          probe.items(logDirs)
          for (const dir of logDirs) {
            if (version >= 4) {
              expect(dir.totalBytes).toMatch(/^[0-9]+$/)
              expect(dir.usableBytes).toMatch(/^[0-9]+$/)
              expect(BigInt(dir.totalBytes)).toBeGreaterThan(0n)
              expect(BigInt(dir.usableBytes)).toBeLessThanOrEqual(BigInt(dir.totalBytes))
            } else {
              expect(dir.totalBytes).toBeNull()
              expect(dir.usableBytes).toBeNull()
            }
            if (version >= 5) expect(typeof dir.isCordoned).toBe('boolean')
            else expect(dir.isCordoned).toBeNull()
            for (const entry of dir.topics.filter((entry) => entry.topic === topic))
              for (const partition of entry.partitions) {
                expect(partition.size).toMatch(/^[0-9]+$/)
                expect(BigInt(partition.size)).toBeGreaterThan(0n)
                expect(partition.offsetLag).toMatch(/^[0-9]+$/)
              }
          }
        }
        const futureCopy = dirs
          .flatMap(({ logDirs }) => logDirs)
          .find(({ path }) => path === destination)
        expect(
          futureCopy.topics.find((entry) => entry.topic === topic).partitions[0].isFuture
        ).toBe(true)
        let final
        await probe.poll(async () => {
          final = await locate()
          return final.currentLogDir === destination && final.futureLogDir === null
        }, 100)
        const finalDirs = await probe.invoke(() => admin.describeLogDirs({ brokers: [1] }))
        const copies = finalDirs
          .flatMap(({ logDirs }) => logDirs)
          .flatMap(({ path: logDir, topics }) =>
            topics
              .filter((entry) => entry.topic === topic)
              .flatMap(({ partitions }) =>
                partitions.map((partition) => ({ logDir, ...partition }))
              )
          )
        expect(copies.some(({ logDir }) => logDir === source)).toBe(false)
        expect(copies.find(({ logDir }) => logDir === destination).isFuture).toBe(false)
        expect(corpus(await probe.physical(topic))).toEqual(before)
        await restoreThrottle()
        probe.record('replica movement and complete corpus', {
          source,
          destination,
          directoryVersion: version,
          throttleOriginalPresent: original !== null,
          futureObserved: true,
          currentChanged: true,
          sourceAbsent: true,
          throttleRestored: true,
          corpusDigest: createHash('sha256').update(JSON.stringify(before)).digest('hex'),
          recordCount: before.length,
        })
      })
    }),
    caseTimeout(budget)
  )
})
