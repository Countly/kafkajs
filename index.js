const Kafka = require('./src')
const oauthBearer = require('./src/oauth')
const PartitionAssigners = require('./src/consumer/assigners')
const AssignerProtocol = require('./src/consumer/assignerProtocol')
const Partitioners = require('./src/producer/partitioners')
const Compression = require('./src/protocol/message/compression')
const ConfigResourceTypes = require('./src/protocol/configResourceTypes')
const ConfigSource = require('./src/protocol/configSource')
const AlterConfigOpTypes = require('./src/protocol/alterConfigOpTypes')
const ConfigTypes = require('./src/protocol/configTypes')
const AclResourceTypes = require('./src/protocol/aclResourceTypes')
const AclOperationTypes = require('./src/protocol/aclOperationTypes')
const AclPermissionTypes = require('./src/protocol/aclPermissionTypes')
const ResourcePatternTypes = require('./src/protocol/resourcePatternTypes')
const {
  isRebalancing,
  isStaleCoordinator,
  isKafkaJSError,
  isLostMember,
  isRevoked,
  isFencedInstance,
  isUnknownMemberId,
  isPollIntervalExceeded,
  isFatalMembership,
  CONSUMER_PROTOCOL_UNSUPPORTED,
  COORDINATOR_LOAD_IN_PROGRESS_CODE,
  ...errors
} = require('./src/errors')
const { LEVELS } = require('./src/loggers')

module.exports = {
  Kafka,
  oauthBearer,
  PartitionAssigners,
  AssignerProtocol,
  Partitioners,
  logLevel: LEVELS,
  CompressionTypes: Compression.Types,
  CompressionCodecs: Compression.Codecs,
  ConfigResourceTypes,
  AclResourceTypes,
  AclOperationTypes,
  AclPermissionTypes,
  ResourcePatternTypes,
  ConfigSource,
  AlterConfigOpTypes,
  ConfigTypes,
  ...errors,
}
