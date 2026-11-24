const apiKeys = require('./apiKeys')
const { currentMutation } = require('../../admin/deadline')
const { MUTATING_APIS, isMutation, owned } = require('../../admin/unknownOutcome')
const { ADMIN_MUTATION } = require('../../broker/privateOptions')
const { KafkaJSServerDoesNotSupportApiKey, KafkaJSNotImplemented } = require('../../errors')

/**
 * @typedef {(options?: Object) => { request: any, response: any, logResponseErrors?: boolean }} Request
 */

/**
 * @typedef {Object} RequestDefinitions
 * @property {string[]} versions
 * @property {({ version: number }) => Request} protocol
 */

/**
 * @typedef {(apiKey: number, definitions: RequestDefinitions) => Request} Lookup
 */

/** @type {RequestDefinitions} */
const noImplementedRequestDefinitions = {
  versions: [],
  /**
   * Rejects protocol construction for every unimplemented request.
   * @returns {never}
   * @throws {KafkaJSNotImplemented} Always, with a diagnostic message.
   */
  protocol: () => {
    throw new KafkaJSNotImplemented('The requested API is not implemented')
  },
}

/**
 * @type {{[apiName: string]: RequestDefinitions}}
 */
const requests = {
  Produce: require('./produce'),
  Fetch: require('./fetch'),
  ListOffsets: require('./listOffsets'),
  Metadata: require('./metadata'),
  LeaderAndIsr: noImplementedRequestDefinitions,
  StopReplica: noImplementedRequestDefinitions,
  UpdateMetadata: noImplementedRequestDefinitions,
  ControlledShutdown: noImplementedRequestDefinitions,
  OffsetCommit: require('./offsetCommit'),
  OffsetFetch: require('./offsetFetch'),
  GroupCoordinator: require('./findCoordinator'),
  JoinGroup: require('./joinGroup'),
  Heartbeat: require('./heartbeat'),
  LeaveGroup: require('./leaveGroup'),
  SyncGroup: require('./syncGroup'),
  DescribeGroups: require('./describeGroups'),
  ListGroups: require('./listGroups'),
  SaslHandshake: require('./saslHandshake'),
  ApiVersions: require('./apiVersions'),
  CreateTopics: require('./createTopics'),
  DeleteTopics: require('./deleteTopics'),
  DeleteRecords: require('./deleteRecords'),
  InitProducerId: require('./initProducerId'),
  OffsetForLeaderEpoch: require('./offsetForLeaderEpoch'),
  AddPartitionsToTxn: require('./addPartitionsToTxn'),
  AddOffsetsToTxn: require('./addOffsetsToTxn'),
  EndTxn: require('./endTxn'),
  WriteTxnMarkers: require('./writeTxnMarkers'),
  TxnOffsetCommit: require('./txnOffsetCommit'),
  DescribeAcls: require('./describeAcls'),
  CreateAcls: require('./createAcls'),
  DeleteAcls: require('./deleteAcls'),
  DescribeConfigs: require('./describeConfigs'),
  AlterConfigs: require('./alterConfigs'),
  AlterReplicaLogDirs: require('./alterReplicaLogDirs'),
  DescribeLogDirs: require('./describeLogDirs'),
  SaslAuthenticate: require('./saslAuthenticate'),
  CreatePartitions: require('./createPartitions'),
  CreateDelegationToken: require('./createDelegationToken'),
  RenewDelegationToken: require('./renewDelegationToken'),
  ExpireDelegationToken: require('./expireDelegationToken'),
  DescribeDelegationToken: require('./describeDelegationToken'),
  DeleteGroups: require('./deleteGroups'),
  ElectLeaders: require('./electLeaders'),
  IncrementalAlterConfigs: require('./incrementalAlterConfigs'),
  AlterPartitionReassignments: require('./alterPartitionReassignments'),
  ListPartitionReassignments: require('./listPartitionReassignments'),
  OffsetDelete: require('./offsetDelete'),
  DescribeClientQuotas: require('./describeClientQuotas'),
  AlterClientQuotas: require('./alterClientQuotas'),
  DescribeUserScramCredentials: require('./describeUserScramCredentials'),
  AlterUserScramCredentials: require('./alterUserScramCredentials'),
  UpdateFeatures: require('./updateFeatures'),
  DescribeCluster: require('./describeCluster'),
  DescribeProducers: require('./describeProducers'),
  DescribeTransactions: require('./describeTransactions'),
  ListTransactions: require('./listTransactions'),
  ConsumerGroupHeartbeat: require('./consumerGroupHeartbeat'),
  ConsumerGroupDescribe: require('./consumerGroupDescribe'),
  GetTelemetrySubscriptions: require('./getTelemetrySubscriptions'),
  PushTelemetry: require('./pushTelemetry'),
  ListConfigResources: require('./listConfigResources'),
  DescribeTopicPartitions: require('./describeTopicPartitions'),
  ShareGroupHeartbeat: require('./shareGroupHeartbeat'),
  ShareGroupDescribe: require('./shareGroupDescribe'),
  ShareFetch: require('./shareFetch'),
  ShareAcknowledge: require('./shareAcknowledge'),
  DescribeShareGroupOffsets: require('./describeShareGroupOffsets'),
  AlterShareGroupOffsets: require('./alterShareGroupOffsets'),
  DeleteShareGroupOffsets: require('./deleteShareGroupOffsets'),
  UnregisterController: require('./unregisterController'),
}

const names = Object.keys(apiKeys)
const keys = Object.values(apiKeys)
const findApiName = (apiKey) => names[keys.indexOf(apiKey)]

/**
 * Chooses the version this client will use for one API on one connection: the highest implemented
 * version inside the broker's `[minVersion, maxVersion]` range (a missing `minVersion` is 0),
 * subject to an optional internal ceiling.
 *
 * Extracted from `lookup()` so a caller can ask *which* version would be selected without
 * building a request — `definition.protocol({ version })` returns a builder in every API in this
 * repo, so `apiVersion` is only observable on a built request. There is one selection rule, and
 * this is it; `lookup()` calls it.
 *
 * @param {import("../../../types").ApiVersions} versions the broker's advertised ranges
 * @param {number} apiKey
 * @param {RequestDefinitions} definition
 * @param {{ [apiKey: number]: number }} [maxVersions] internal per-API ceiling
 * @returns {number} the selected version
 * @throws {KafkaJSServerDoesNotSupportApiKey} when the broker does not advertise the API, no
 *   implemented version lies inside the broker range, or the cap excludes every implemented version
 */
const selectVersion = (versions, apiKey, definition, maxVersions = {}) => {
  const version = versions[apiKey]
  const cap = maxVersions[apiKey]
  const availableVersions = definition.versions
    .map(Number)
    .filter((available) => cap === undefined || available <= cap)
  const apiName = findApiName(apiKey)

  if (!version || version.maxVersion == null) {
    throw new KafkaJSServerDoesNotSupportApiKey(
      `The Kafka server does not support the requested API version`,
      { apiKey, apiName }
    )
  }

  if (availableVersions.length === 0) {
    throw new KafkaJSServerDoesNotSupportApiKey(
      `No implemented version of ${apiName} is at or below the internal cap v${cap}`,
      { apiKey, apiName }
    )
  }

  const brokerMinVersion = version.minVersion == null ? 0 : version.minVersion
  // By membership, not by envelope: a set with a gap can straddle the broker range without
  // having a member inside it. Kafka 4.0 raised the floor for most APIs (KIP-896/KIP-724).
  const candidates = availableVersions.filter(
    (available) => available >= brokerMinVersion && available <= version.maxVersion
  )

  if (candidates.length === 0) {
    throw new KafkaJSServerDoesNotSupportApiKey(
      `The Kafka server does not support any version of ${apiName} implemented by this client: ` +
        `client implements v${Math.min(...availableVersions)}-v${Math.max(...availableVersions)}, ` +
        `broker supports v${brokerMinVersion}-v${version.maxVersion}`,
      { apiKey, apiName }
    )
  }

  return Math.max(...candidates)
}

/**
 * Builds the per-API version selector for one connection.
 *
 * `maxVersions` is an internal cap — `{ [apiKey]: version }` — used by tests and by per-request
 * downgrades. It is never surfaced as a user-facing option: the client always negotiates the
 * highest mutually supported version otherwise.
 *
 * @param {import("../../../types").ApiVersions} versions the broker's advertised ranges
 * @param {{ [apiKey: number]: number }} [maxVersions] internal per-API ceiling
 * @returns {Lookup}
 * @throws {KafkaJSServerDoesNotSupportApiKey} from the returned selector, via `selectVersion`
 */
const lookup =
  (versions, maxVersions = {}) =>
  (apiKey, definition) =>
    definition.protocol({ version: selectVersion(versions, apiKey, definition, maxVersions) })

/** Wraps only mutation definitions so ordinary request lookups keep their existing path.
 * @param {number} apiKey
 * @param {RequestDefinitions} definition
 * @returns {RequestDefinitions} a definition with an admin-aware builder
 */
const mutationDefinition = (apiKey, definition) => ({
  ...definition,
  protocol(options) {
    const build = definition.protocol(options)
    const shared = apiKey === apiKeys.InitProducerId || apiKey === apiKeys.LeaveGroup
    return (args, ownership) => {
      if ((shared && ownership !== ADMIN_MUTATION) ||
          !currentMutation() || !isMutation(apiKey, options.version, args)) {
        return build(args)
      }
      const input = owned(args)
      const built = build(input)
      built.args = input
      return built
    }
  },
})

for (const [name, definition] of Object.entries(requests)) {
  const apiKey = apiKeys[name]
  if (MUTATING_APIS.has(apiKey)) requests[name] = mutationDefinition(apiKey, definition)
}

module.exports = {
  requests,
  lookup,
  selectVersion,
}
