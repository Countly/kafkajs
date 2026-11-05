const { createHash } = require('crypto')
const { AdminLiveProbe } = require('./fixtures/liveAdmin')
const { StorageOracle, decodeSegment } = require('./fixtures/liveOracles')
const { liveCase, caseTimeout } = require('../../producer/__tests__/fixtures/bounded')

const suite = process.env.ADMIN_TIERED_E2E ? describe : describe.skip
const budget = 540000
const jarHash = 'f3236936854c13815ffaa7f35a3f2d6861c125616cba5395b538b8680dded886'

suite('admin configured remote storage', () => {
  test(
    'reads retained records after remote upload and local eviction',
    liveCase(budget, async (signal) => {
      const probe = new AdminLiveProbe({ stem: 'tiered-offsets', signal, budgetMs: budget })
      await probe.finish(async () => {
        for (const service of ['kafka1', 'kafka2', 'kafka3']) {
          const hash = await probe.host(service, [
            'sha256sum',
            '/opt/kafka/plugins/kafka-storage-4.3.1-test.jar',
          ])
          expect(hash.split(/\s+/)[0]).toBe(jarHash)
        }
        const admin = await probe.start()
        const topic = await probe.topic({
          replicaAssignment: [{ partition: 0, replicas: [1, 2] }],
          configEntries: [
            { name: 'remote.storage.enable', value: 'true' },
            { name: 'segment.bytes', value: '1048576' },
            { name: 'local.retention.bytes', value: '1048576' },
            { name: 'segment.ms', value: '1000' },
          ],
        })
        const config = await probe.invoke(() =>
          admin.describeConfigs({
            resources: [
              {
                type: 2,
                name: topic,
                configNames: [
                  'remote.storage.enable',
                  'segment.bytes',
                  'local.retention.bytes',
                  'retention.ms',
                  'segment.ms',
                ],
              },
            ],
          })
        )
        probe.items(config.resources)
        for (const [name, value] of Object.entries({
          'remote.storage.enable': 'true',
          'segment.bytes': '1048576',
          'local.retention.bytes': '1048576',
          'retention.ms': '-1',
          'segment.ms': '1000',
        })) {
          expect(config.resources[0].configEntries).toContainEqual(
            expect.objectContaining({ configName: name, configValue: value })
          )
        }
        const producer = await probe.client('producer', {
          idempotent: true,
          maxInFlightRequests: 1,
        })
        const expectedIds = []
        for (let base = 0; base < 16512; base += 128) {
          const messages = Array.from({ length: 128 }, (_, index) => {
            const id = `tiered-${base + index}`
            expectedIds.push(id)
            const value = Buffer.alloc(1024)
            value.write(id)
            return { partition: 0, value }
          })
          await probe.invoke(() => producer.send({ topic, acks: -1, messages }))
        }
        const bounds = (await probe.invoke(() => admin.fetchTopicOffsets(topic)))[0]
        const uuid = probe.topics.find(({ name }) => name === topic).topicId
        const root = '/var/lib/kafka/data/remote'
        const compactUuid = Buffer.from(uuid.replaceAll('-', ''), 'hex').toString('base64url')
        const partitionDirectories = [`${topic}-0-${uuid}`, `${topic}-0-${compactUuid}`]
        const localRoot = `/tmp/kraft-combined-logs/${topic}-0`
        // The broker image has BusyBox `find`, which has no -printf; `stat` prints each size.
        const files = async (directory) =>
          (
            await probe.host('kafka1', [
              'find',
              directory,
              '-type',
              'f',
              '-name',
              '*.log',
              '-exec',
              'stat',
              '-c',
              '%s %n',
              '{}',
              ';',
            ])
          )
            .trim()
            .split('\n')
            .filter(Boolean)
            .map((line) => {
              const space = line.indexOf(' ')
              return { path: line.slice(space + 1), size: Number(line.slice(0, space)) }
            })
        let snapshot
        await probe.poll(async () => {
          const remote = (await files(root))
            .sort((a, b) => a.path.localeCompare(b.path))
            .filter(
              ({ path, size }) =>
                size > 0 &&
                path.split('/').some((component) => partitionDirectories.includes(component))
            )
          const local = (await files(localRoot))
            .sort((a, b) => a.path.localeCompare(b.path))
            .filter(({ size }) => size > 0)
          if (!remote.length || !local.length) return false
          const localFirst = String(
            BigInt(local.map(({ path }) => path.split('/').pop().split('.')[0]).sort()[0])
          )
          if (BigInt(localFirst) <= BigInt(bounds.low)) return false
          const decoded = []
          for (const file of remote) {
            const encoded = await probe.host('kafka1', ['base64', '-w', '0', file.path])
            const bytes = Buffer.from(encoded, 'base64')
            if (bytes.length !== file.size) throw new Error('Remote snapshot changed during read')
            decoded.push(...(await decodeSegment(bytes)))
          }
          decoded.sort((a, b) => Number(BigInt(a.offset) - BigInt(b.offset)))
          const unique = [...new Map(decoded.map((record) => [record.offset, record])).values()]
          for (let index = 1; index < unique.length; index++) {
            expect(BigInt(unique[index].offset)).toBe(BigInt(unique[index - 1].offset) + 1n)
          }
          if (!unique.length) return false
          expect(unique[0].offset).toBe(bounds.low)
          const remoteLast = unique[unique.length - 1].offset
          const pending = []
          for (const file of local) {
            const base = BigInt(file.path.split('/').pop().split('.')[0])
            if (base <= BigInt(remoteLast)) continue
            const encoded = await probe.host('kafka1', ['base64', '-w', '0', file.path])
            const data = await decodeSegment(Buffer.from(encoded, 'base64'))
            if (data.length) pending.push(data[0].offset)
          }
          const again = (await files(root))
            .sort((a, b) => a.path.localeCompare(b.path))
            .filter(
              ({ path, size }) =>
                size > 0 &&
                path.split('/').some((component) => partitionDirectories.includes(component))
            )
          if (JSON.stringify(remote) !== JSON.stringify(again)) return false
          const localAgain = (await files(localRoot))
            .sort((a, b) => a.path.localeCompare(b.path))
            .filter(({ size }) => size > 0)
          if (JSON.stringify(local) !== JSON.stringify(localAgain)) return false
          snapshot = {
            localFirst,
            remoteNames: remote.map(({ path }) => path),
            remoteSizes: remote.map(({ size }) => size),
            localNames: local.map(({ path }) => path),
            localSizes: local.map(({ size }) => size),
            remoteLast,
            pendingFirst: pending.sort((a, b) => Number(BigInt(a) - BigInt(b)))[0] || '-1',
            remoteSegments: remote.length,
            remoteBytes: remote.reduce((sum, { size }) => sum + size, 0),
          }
          return true
        })
        const named = async (target, name) =>
          (await probe.invoke(() => admin.fetchTopicOffsetsByTimestamp(target, name)))[0].offset
        const earliestLocal = await named(topic, 'earliestLocal')
        const latestTiered = await named(topic, 'latestTiered')
        const earliestPendingUpload = await named(topic, 'earliestPendingUpload')
        const listOffsetsVersion = probe.requests
          .filter(({ apiName }) => apiName === 'ListOffsets')
          .at(-1).apiVersion
        expect(listOffsetsVersion).toBeGreaterThanOrEqual(11)
        const plainTopic = await probe.topic({ name: 'plain' })
        const plainBounds = (await probe.invoke(() => admin.fetchTopicOffsets(plainTopic)))[0]
        const plain = {
          logStart: plainBounds.low,
          earliestLocal: await named(plainTopic, 'earliestLocal'),
          latestTiered: await named(plainTopic, 'latestTiered'),
          earliestPendingUpload: await named(plainTopic, 'earliestPendingUpload'),
        }
        const consumer = await probe.client('consumer', {
          groupId: `${topic}-read`,
          readUncommitted: true,
        })
        await probe.invoke(() => consumer.subscribe({ topic, fromBeginning: true }))
        const retainedIds = []
        const lastRetained = BigInt(bounds.high) - 1n
        let reachedHigh = false
        await probe.invoke(() =>
          consumer.run({
            autoCommit: false,
            eachBatchAutoResolve: false,
            eachBatch: async ({ batch, resolveOffset, pause }) => {
              for (const { offset, value } of batch.messages) {
                if (BigInt(offset) >= BigInt(bounds.low) && BigInt(offset) <= lastRetained)
                  retainedIds.push(value.toString().replace(/\0+$/, ''))
                resolveOffset(offset)
                if (BigInt(offset) >= lastRetained) {
                  pause()
                  reachedHigh = true
                  return
                }
              }
            },
          })
        )
        await probe.poll(async () => reachedHigh)
        await probe.invoke(() => consumer.stop())
        expect(retainedIds).toEqual(expectedIds)
        const physical = await probe.physical(topic)
        expect(
          physical.get(0).records.map(({ value }) => value.toString().replace(/\0+$/, ''))
        ).toEqual(expectedIds)
        const sample = {
          ...snapshot,
          logStart: bounds.low,
          high: bounds.high,
          earliestLocal,
          latestTiered,
          earliestPendingUpload,
          hasPendingUploadSpec: true,
          producedBytes: expectedIds.length * 1024,
          expectedIds,
          retainedIds,
          plain,
        }
        StorageOracle.verify(sample)
        probe.record('independent remote segments and retained fetch', {
          topicId: uuid,
          remoteCount: snapshot.remoteSegments,
          remoteSegmentIds: snapshot.remoteNames,
          remoteSegmentBytes: snapshot.remoteSizes,
          localSegmentIds: snapshot.localNames,
          localSegmentBytes: snapshot.localSizes,
          listOffsetsVersion,
          retainedLow: bounds.low,
          observedHigh: bounds.high,
          earliestLocalOffset: earliestLocal,
          latestTieredOffset: latestTiered,
          earliestPendingUploadOffset: earliestPendingUpload,
          plainLow: plain.logStart,
          plainLocalOffset: plain.earliestLocal,
          plainTieredOffset: plain.latestTiered,
          plainPendingOffset: plain.earliestPendingUpload,
          retainedCount: retainedIds.length,
          remoteBytes: snapshot.remoteBytes,
          localFirst: snapshot.localFirst,
          remoteLast: snapshot.remoteLast,
          pendingFirst: snapshot.pendingFirst,
          corpusDigest: createHash('sha256').update(JSON.stringify(retainedIds)).digest('hex'),
        })
      })
    }),
    caseTimeout(budget)
  )
})
