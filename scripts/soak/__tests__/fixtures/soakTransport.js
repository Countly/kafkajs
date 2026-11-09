const { EventEmitter } = require('events')
const Encoder = require('../../../../src/protocol/encoder')
const { requestHeaderVersion } = require('../../../../src/protocol/headers')

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
   * @returns {Promise<void>} Complete observed exchange.
   */
  async exchange(socket, request, errorCode = 0) {
    const correlation = ++this.correlation
    const header = new Encoder()
      .writeInt16(request.apiKey)
      .writeInt16(request.apiVersion)
      .writeInt32(correlation)
      .writeString('soak-script')
    if (requestHeaderVersion(request.apiKey, request.apiVersion) === 2) header.writeTaggedFields()
    header.writeEncoder(await request.encode())
    socket.write(new Encoder().writeInt32(header.size()).writeEncoder(header).buffer)
    const response = new Encoder().writeInt32(correlation).writeInt16(errorCode)
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
                if (request) await this.exchange(socket, request)
                return await work(...args)
              } finally {
                if (name === 'disconnect' || role === 'raw') close()
              }
            },
      ])
    )
    return this.tap.client(role, observed)
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
    return this.client('raw', { readLog: read }, () =>
      require('../../../../src/protocol/requests/fetch/v12/request')({
        replicaId: -1,
        maxWaitTime: 0,
        minBytes: 1,
        maxBytes: 1048576,
        topics: [],
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
