const { EventEmitter } = require('events')
const { reply } = require('./soakReplies')
const Encoder = require('../../../../src/protocol/encoder')
const { responseHeaderVersion, requestHeaderVersion } = require('../../../../src/protocol/headers')

/** Scripted external I/O boundary; requests use product encoders and the original passive tap. */
class SoakTransport {
  /** @param {object} tap Original observer. @param {string} [endpoint] Scripted broker address. */
  constructor(tap, endpoint = 'node0:9092') {
    this.tap = tap
    this.endpoint = endpoint
    this.correlation = 0
    this.sockets = new Set()
  }

  /**
   * Writes one encoded operation and matches its scripted response before returning I/O data.
   * @param {object} socket Original scripted stream.
   * @param {object} request Product request descriptor.
   * @param {number} [errorCode=0] Negotiation outcome for ApiVersions.
   * @param {object} [values] Literal upstream fields from the scripted external result.
   * @returns {Promise<void>} Complete observed exchange.
   */
  async exchange(socket, request, errorCode = 0, values = {}) {
    const correlation = ++this.correlation
    const header = new Encoder()
      .writeInt16(request.apiKey)
      .writeInt16(request.apiVersion)
      .writeInt32(correlation)
      .writeString('soak-script')
    if (requestHeaderVersion(request.apiKey, request.apiVersion) === 2) header.writeTaggedFields()
    header.writeEncoder(await request.encode())
    socket.write(new Encoder().writeInt32(header.size()).writeEncoder(header).buffer)
    const response = new Encoder().writeInt32(correlation)
    if (request.apiKey === 18) {
      if (errorCode)
        response.writeInt16(errorCode).writeInt32(1).writeInt16(18).writeInt16(0).writeInt16(4)
      else
        response.writeBuffer(
          require('../../../../testHelpers/wire/apiVersionsV4').buildResponse({
            apiKeys: [
              [0, 9],
              [1, 12],
              [2, 7],
              [3, 13],
              [10, 6],
              [16, 5],
              [18, 4],
              [19, 7],
              [20, 6],
              [22, 4],
              [32, 4],
              [42, 2],
              [44, 1],
              [60, 2],
              [76, 1],
              [77, 1],
              [78, 2],
              [79, 2],
              [90, 1],
            ].map(([apiKey, version]) => ({ apiKey, minVersion: version, maxVersion: version })),
            finalizedFeatures: [{ name: 'share.version', minVersionLevel: 1, maxVersionLevel: 1 }],
          })
        )
    } else {
      if (responseHeaderVersion(request.apiKey, request.apiVersion) === 1)
        response.writeTaggedFields()
      const api = Object.entries(require('../../../../src/protocol/requests/apiKeys')).find(
        ([, key]) => key === request.apiKey
      )[0]
      response.writeBuffer(reply(api, request.apiVersion, values))
    }
    socket.emit('data', new Encoder().writeInt32(response.size()).writeEncoder(response).buffer)
  }

  /**
   * Creates a fresh external client, negotiating on its own observed stream.
   * @param {string} role Client role.
   * @param {object} client Scripted public I/O operations.
   * @param {(name: string, args: any[]) => object|undefined} descriptor Concrete request selection.
   * @returns {object} Observed facade with original I/O results and errors.
   */
  client(role, client, descriptor) {
    let socket
    const close = () => {
      if (!socket) return
      socket.destroyed = true
      socket.emit('close', false)
      socket.removeAllListeners()
      this.sockets.delete(socket)
      socket = null
    }
    const observed = Object.fromEntries(
      Object.entries(client).map(([name, work]) => [
        name,
        typeof work !== 'function' || ['on'].includes(name)
          ? work
          : async (...args) => {
              if (!socket) {
                socket = Object.assign(new EventEmitter(), { destroyed: false, write: () => true })
                this.sockets.add(socket)
                this.tap.wrap(socket, this.endpoint)
                for (const version of [5, 4])
                  await this.exchange(
                    socket,
                    require(`../../../../src/protocol/requests/apiVersions/v${version}/request`)(
                      {}
                    ),
                    version === 5 ? 35 : 0
                  )
              }
              try {
                const request = descriptor(name, args)
                const result = await work(...args)
                if (request)
                  await this.exchange(socket, request, 0, this.resultFields(name, args, result))
                return result
              } finally {
                if (name === 'disconnect' || role === 'raw') close()
              }
            },
      ])
    )
    return this.tap.client(role, observed)
  }

  /**
   * Projects the external operation's actual result into upstream response field names.
   * @param {string} name Public operation.
   * @param {any[]} args Original call arguments.
   * @param {*} result Scripted broker result.
   * @returns {object} Literal upstream fields; failed operations never reach this method.
   */
  resultFields(name, [options], result) {
    if (name === 'describeCluster')
      return {
        Brokers: result.brokers.map((b) => ({ BrokerId: b.nodeId, Host: b.host, Port: b.port })),
      }
    if (name === 'fetchTopicMetadata' || name === 'listTopics')
      return {
        Topics: (name === 'listTopics'
          ? result.map((name) => ({ name, partitions: [] }))
          : result.topics
        ).map((t) => ({
          Name: t.name,
          TopicId: t.topicId,
          Partitions: t.partitions.map((p) => ({
            PartitionIndex: p.partitionId,
            LeaderId: p.leader,
            ReplicaNodes: p.replicas,
          })),
        })),
      }
    if (name === 'listGroups') return { Groups: result.groups.map((g) => ({ GroupId: g.groupId })) }
    if (name === 'describeConfigs')
      return {
        Results: result.resources.map((r) => ({
          ErrorCode: r.errorCode,
          ResourceType: r.resourceType,
          ResourceName: r.resourceName,
          Configs: r.configEntries.map((e) => ({
            Name: e.configName,
            Value: e.configValue,
            ConfigSource: e.configSource,
          })),
        })),
      }
    if (name === 'incrementalAlterConfigs')
      return {
        Responses: options.resources.map((r, i) => ({
          ErrorCode: result.resources[i].errorCode,
          ResourceType: r.type,
          ResourceName: r.name,
        })),
      }
    if (name === 'send')
      return {
        Responses: [
          {
            Name: options.topic,
            PartitionResponses: result.map((r) => ({
              Index: r.partition,
              ErrorCode: r.errorCode,
              BaseOffset: r.baseOffset,
            })),
          },
        ],
      }
    if (name === 'deleteGroups') return { Results: options.map((GroupId) => ({ GroupId })) }
    if (name === 'createTopics')
      return {
        Topics: options.topics.map((t) => ({
          Name: t.topic,
          NumPartitions: 3,
          ReplicationFactor: 3,
        })),
      }
    if (name === 'deleteTopics') return { Responses: options.topics.map((Name) => ({ Name })) }
    if (name === 'fetchTopicOffsets')
      return {
        Topics: [
          {
            Name: options,
            Partitions: result.map((r) => ({
              PartitionIndex: r.partition,
              Offset: r.high,
              LeaderEpoch: 1,
            })),
          },
        ],
      }
    if (name === 'listShareGroupOffsets')
      return {
        Groups: [
          {
            GroupId: options.groupId,
            Topics: result.map((t) => ({
              TopicName: t.topic,
              TopicId: t.topicId,
              Partitions: t.partitions.map((p) => ({
                PartitionIndex: p.partition,
                StartOffset: p.startOffset,
                Lag: p.lag,
                LeaderEpoch: p.leaderEpoch,
                ErrorCode: p.errorCode,
              })),
            })),
          },
        ],
      }
    if (name === 'describeShareGroups')
      return {
        Groups: result.groups.map((g) => ({
          ErrorCode: g.errorCode,
          GroupId: g.groupId,
          GroupState: g.state,
          Members: g.members.map((m) => ({
            MemberId: m.memberId,
            Assignment: {
              TopicPartitions: m.assignment.map((t) => ({
                TopicName: t.topic,
                Partitions: t.partitions,
              })),
            },
          })),
        })),
      }
    return {}
  }

  /**
   * Encodes the public admin operation at the scripted boundary's selected versions.
   * @param {string} name Public operation.
   * @param {any[]} args Original arguments.
   * @returns {object|undefined} Product encoder for operations that send requests.
   */
  adminRequest(name, [options]) {
    const calls = {
      createTopics: ['createTopics/v7', { timeout: 5000, ...options }],
      deleteTopics: ['deleteTopics/v6', options],
      listTopics: ['metadata/v13', { topics: [] }],
      fetchTopicMetadata: ['metadata/v13', { topics: [], ...options }],
      describeCluster: ['describeCluster/v2', options],
      listGroups: ['listGroups/v5', options],
      deleteGroups: ['deleteGroups/v2', options],
      describeShareGroups: ['shareGroupDescribe/v1', { groupIds: options }],
      listShareGroupOffsets: [
        'describeShareGroupOffsets/v1',
        {
          groups: [
            {
              groupId: options?.groupId,
              topics:
                options?.topics?.map((t) => ({ topicName: t.topic, partitions: t.partitions })) ??
                null,
            },
          ],
        },
      ],
      fetchTopicOffsets: [
        'listOffsets/v7',
        {
          replicaId: -1,
          isolationLevel: 1,
          topics: [
            {
              topic: options,
              partitions: [0, 1, 2].map((partition) => ({ partition, timestamp: '-1' })),
            },
          ],
        },
      ],
      incrementalAlterConfigs: ['incrementalAlterConfigs/v1', options],
      describeConfigs: ['describeConfigs/v4', options],
    }
    const call = calls[name]
    return call && require(`../../../../src/protocol/requests/${call[0]}/request`)(call[1])
  }

  /**
   * Wraps the scripted admin boundary without substituting an inventory verdict.
   * @param {object} client Original admin I/O double.
   * @returns {object} Observed admin facade.
   */
  admin(client) {
    return this.client('admin', client, this.adminRequest.bind(this))
  }

  /**
   * Encodes each actual production batch and idempotent producer initialization.
   * @param {object} client Original producer I/O double.
   * @returns {object} Observed producer facade.
   */
  producer(client) {
    return this.client('producer', client, (name, [options]) => {
      if (name === 'connect')
        return require('../../../../src/protocol/requests/initProducerId/v4/request')({
          transactionTimeout: 30000,
        })
      if (name === 'send')
        return require('../../../../src/protocol/requests/produce/v9/request')({
          acks: options.acks,
          timeout: 3000,
          topicData: [
            {
              topic: options.topic,
              partitions: [
                { partition: options.messages[0].partition, messages: options.messages },
              ],
            },
          ],
        })
    })
  }

  /**
   * Emits a raw Fetch when the driver performs its independent raw-log read.
   * @param {Function} read Original raw corpus reader.
   * @returns {Function} Observed reader with unchanged data.
   */
  reader(read) {
    return this.client('raw', { readLog: read }, (_name, [options]) =>
      require('../../../../src/protocol/requests/fetch/v12/request')({
        replicaId: -1,
        maxWaitTime: 0,
        minBytes: 1,
        maxBytes: 1048576,
        topics: [
          {
            topic: options.topic,
            partitions: [0, 1, 2].map((partition) => ({
              partition,
              fetchOffset: '0',
              maxBytes: 1048576,
            })),
          },
        ],
      })
    ).readLog
  }

  /** @returns {void} Releases every remaining scripted boundary stream on failure. */
  close() {
    for (const socket of this.sockets) {
      socket.destroyed = true
      socket.emit('close', false)
      socket.removeAllListeners()
    }
    this.sockets.clear()
  }
}

module.exports = { SoakTransport }
