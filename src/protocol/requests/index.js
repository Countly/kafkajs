const apiKeys = require('./apiKeys')
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
  protocol: () => {
    throw new KafkaJSNotImplemented()
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
  AlterReplicaLogDirs: noImplementedRequestDefinitions,
  DescribeLogDirs: noImplementedRequestDefinitions,
  SaslAuthenticate: require('./saslAuthenticate'),
  CreatePartitions: require('./createPartitions'),
  CreateDelegationToken: noImplementedRequestDefinitions,
  RenewDelegationToken: noImplementedRequestDefinitions,
  ExpireDelegationToken: noImplementedRequestDefinitions,
  DescribeDelegationToken: noImplementedRequestDefinitions,
  DeleteGroups: require('./deleteGroups'),
  ElectLeaders: noImplementedRequestDefinitions,
  IncrementalAlterConfigs: require('./incrementalAlterConfigs'),
  AlterPartitionReassignments: require('./alterPartitionReassignments'),
  ListPartitionReassignments: require('./listPartitionReassignments'),
  OffsetDelete: noImplementedRequestDefinitions,
  DescribeClientQuotas: noImplementedRequestDefinitions,
  AlterClientQuotas: noImplementedRequestDefinitions,
  DescribeUserScramCredentials: noImplementedRequestDefinitions,
  AlterUserScramCredentials: noImplementedRequestDefinitions,
  UpdateFeatures: noImplementedRequestDefinitions,
  DescribeCluster: noImplementedRequestDefinitions,
  DescribeProducers: require('./describeProducers'),
  DescribeTransactions: require('./describeTransactions'),
  ListTransactions: require('./listTransactions'),
  ConsumerGroupHeartbeat: require('./consumerGroupHeartbeat'),
  ConsumerGroupDescribe: require('./consumerGroupDescribe'),
  ListConfigResources: noImplementedRequestDefinitions,
  DescribeTopicPartitions: require('./describeTopicPartitions'),
}

const names = Object.keys(apiKeys)
const keys = Object.values(apiKeys)
const findApiName = (apiKey) => names[keys.indexOf(apiKey)]

/**
 * Chooses the version this client will use for one API on one connection: the highest version
 * both sides implement, subject to an optional internal ceiling.
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
 * @throws {KafkaJSServerDoesNotSupportApiKey} when the broker does not advertise the API, the two
 *   version ranges do not overlap, or the cap excludes every implemented version
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

  const bestImplementedVersion = Math.max(...availableVersions)
  const lowestImplementedVersion = Math.min(...availableVersions)
  const brokerMinVersion = version.minVersion == null ? 0 : version.minVersion

  // The client and broker version ranges must overlap. Kafka 4.0 raised the floor for most
  // APIs (KIP-896/KIP-724) and future majors will raise it again; without this check we would
  // send a version the broker rejects, or select a version we do not implement.
  if (brokerMinVersion > bestImplementedVersion || version.maxVersion < lowestImplementedVersion) {
    throw new KafkaJSServerDoesNotSupportApiKey(
      `The Kafka server does not support any version of ${apiName} implemented by this client: ` +
        `client implements v${lowestImplementedVersion}-v${bestImplementedVersion}, ` +
        `broker supports v${brokerMinVersion}-v${version.maxVersion}`,
      { apiKey, apiName }
    )
  }

  // By membership, not by `Math.min`: the highest version this client actually implements at or
  // below the broker's maximum. An implemented set with a hole would otherwise select a version
  // with no codec, and `definition.protocol({ version })` would return undefined.
  return Math.max(...availableVersions.filter((available) => available <= version.maxVersion))
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

module.exports = {
  requests,
  lookup,
  selectVersion,
}
