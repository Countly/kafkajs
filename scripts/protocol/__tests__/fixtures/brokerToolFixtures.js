const crypto = require('crypto')
const fs = require('fs')
const path = require('path')

/** Hand-written broker tool output: nothing here is produced by the code under test. */

/** @param {string|Buffer} data @returns {string} the SHA-256 of `data`, hex */
const sha256 = (data) => crypto.createHash('sha256').update(data).digest('hex')

/**
 * One broker block as `kafka-broker-api-versions.sh` prints it.
 * @param {number} id the broker id
 * @param {string[]} entries the API lines of the block
 * @returns {string}
 */
const brokerBlock = (id, entries) =>
  `kafka${id}:29092 (id: ${id} rack: null isFenced: false) -> (\n\t${entries.join(',\n\t')}\n)\n`

/** The API lines every well-formed block carries, in the tool's spelling. */
const API_ENTRIES = [
  'Produce(0): 3 to 13 [usable: 13]',
  'Fetch(1): 4 to 18 [usable: 18]',
  'LeaderAndIsr(4): UNSUPPORTED',
  'SaslHandshake(17): 0 to 1 [usable: 1]',
  'ApiVersions(18): 0 to 5 [usable: 5]',
  'ShareGroupHeartbeat(76): 1 [usable: 1]',
  'AddRaftVoter(80): UNSUPPORTED',
  'UNKNOWN(95): 0 to 2',
]

/** The numeric ranges those lines state; UNSUPPORTED entries state none. */
const API_RANGES = {
  0: [3, 13],
  1: [4, 18],
  17: [0, 1],
  18: [0, 5],
  76: [1, 1],
  95: [0, 2],
}

/** @type {string} three agreeing broker blocks */
const API_VERSIONS_TEXT = [1, 2, 3].map((id) => brokerBlock(id, API_ENTRIES)).join('')

/**
 * One `kafka-features.sh describe` row, padded the way the tool pads it.
 * @param {string} name
 * @param {string|number} min
 * @param {string|number} max
 * @param {string|number} level
 * @param {string} epoch
 * @returns {string}
 */
const featureRow = (name, min, max, level, epoch) =>
  `Feature: ${name.padEnd(40)}  SupportedMinVersion: ${String(min).padEnd(15)}  ` +
  `SupportedMaxVersion: ${String(max).padEnd(15)}  FinalizedVersionLevel: ${String(level).padEnd(15)}  ` +
  `Epoch: ${epoch}`

/**
 * @param {string} [epoch] what every row prints as its epoch
 * @returns {string} the tool's output for the wire features below
 */
const featuresText = (epoch = '41') =>
  [
    featureRow('group.version', 0, 1, 1, epoch),
    featureRow('kraft.version', 0, 1, 0, epoch),
    featureRow('metadata.version', '3.3-IV3', '4.4-IV2', '4.4-IV2', epoch),
    featureRow('share.version', 0, 2, 2, epoch),
    featureRow('transaction.version', 0, 2, 2, epoch),
  ].join('\n') + '\n'

/** @returns {object} the object `wireFeatures.js` prints for the tool outputs above */
const wireObject = () => ({
  correlation: 1,
  errorCode: 0,
  apiKeyCount: 6,
  throttle: 0,
  apiRanges: structuredClone(API_RANGES),
  shareApiRanges: { 76: [1, 1] },
  supportedFeatures: [
    { name: 'group.version', min: 0, max: 1 },
    { name: 'kraft.version', min: 0, max: 1 },
    { name: 'metadata.version', min: 7, max: 32 },
    { name: 'share.version', min: 0, max: 2 },
    { name: 'transaction.version', min: 0, max: 2 },
  ],
  finalizedFeaturesEpoch: '41',
  finalizedFeatures: [
    { name: 'group.version', maxVersionLevel: 1, minVersionLevel: 1 },
    { name: 'transaction.version', maxVersionLevel: 2, minVersionLevel: 2 },
    { name: 'share.version', maxVersionLevel: 2, minVersionLevel: 2 },
    { name: 'metadata.version', maxVersionLevel: 32, minVersionLevel: 32 },
  ],
  tags: [0, 1, 2],
})

/** The three raw capture files, by name. */
const RAW = {
  apiVersions: 'broker-api-versions.txt',
  features: 'kafka-features-describe.txt',
  wire: 'wire-apiversions-v4.jsonl',
}

/**
 * A complete projector input whose provenance digests match the raw text it carries.
 * @param {object} [options]
 * @param {string} [options.apiVersions] raw `kafka-broker-api-versions.sh` text
 * @param {string} [options.features] raw `kafka-features.sh describe` text
 * @param {object} [options.wire] the object the wire line carries
 * @returns {{ apiVersionsText: string, wireFeaturesLine: string, featuresText: string, provenance: object }}
 */
const projectorInput = ({
  apiVersions = API_VERSIONS_TEXT,
  features = featuresText(),
  wire = wireObject(),
} = {}) => {
  const wireFeaturesLine = `${JSON.stringify(wire)}\n`
  return {
    apiVersionsText: apiVersions,
    featuresText: features,
    wireFeaturesLine,
    provenance: {
      source: 'Independent broker tool captures (Kafka 4.4.0 default cluster)',
      brokerVersion: '4.4.0',
      brokerCommitId: '0123456789abcdef',
      imageDigest: `apache/kafka@sha256:${'a'.repeat(64)}`,
      candidateSha: 'b'.repeat(40),
      featureState: 'share.version upgraded to 2 before every capture',
      captures: [
        {
          file: RAW.apiVersions,
          sha256: sha256(apiVersions),
          capturedOn: '2026-11-12T10:00:01Z',
          howProduced: 'kafka-broker-api-versions.sh --bootstrap-server kafka1:29092',
        },
        {
          file: RAW.features,
          sha256: sha256(features),
          capturedOn: '2026-11-12T10:00:03Z',
          howProduced: 'kafka-features.sh --bootstrap-server kafka1:29092 describe',
        },
        {
          file: RAW.wire,
          sha256: sha256(wireFeaturesLine),
          capturedOn: '2026-11-12T10:00:04Z',
          howProduced: 'node wireFeatures.js localhost 9092',
        },
      ],
    },
  }
}

/** The broker's startup log lines the capture job keeps as the identity of the broker. */
const IDENTITY_LOG =
  'kafka1  | [2026-11-12 10:00:00,001] INFO Kafka version: 4.4.0 (org.apache.kafka.common.utils.AppInfoParser)\n' +
  'kafka1  | [2026-11-12 10:00:00,001] INFO Kafka commitId: 0123456789abcdef (org.apache.kafka.common.utils.AppInfoParser)\n'

/**
 * Writes the files one capture job uploads, with the digests a job would have recorded.
 * @param {string} dir an existing directory
 * @param {object} [options]
 * @param {string} [options.identity] the broker identity log
 * @param {object} [options.wire] the wire feature response
 * @returns {void}
 */
const writeArtifact = (dir, { identity = IDENTITY_LOG, wire } = {}) => {
  const {
    apiVersionsText,
    featuresText: features,
    wireFeaturesLine,
    provenance,
  } = projectorInput({ wire })
  const files = {
    [RAW.apiVersions]: apiVersionsText,
    [RAW.features]: features,
    [RAW.wire]: wireFeaturesLine,
    'broker-identity.txt': identity,
    'image-digest.txt': `${provenance.imageDigest}\n`,
    'capture-meta.json': JSON.stringify({
      source: provenance.source,
      candidateSha: provenance.candidateSha,
      featureState: provenance.featureState,
      captures: provenance.captures.map(({ file, capturedOn, howProduced }) => ({
        file,
        capturedOn,
        howProduced,
      })),
    }),
  }
  for (const [file, text] of Object.entries(files)) fs.writeFileSync(path.join(dir, file), text)
  fs.writeFileSync(
    path.join(dir, 'SHA256SUMS'),
    Object.entries(files)
      .map(([file, text]) => `${sha256(text)}  ${file}`)
      .join('\n') + '\n'
  )
}

module.exports = {
  sha256,
  writeArtifact,
  IDENTITY_LOG,
  brokerBlock,
  API_ENTRIES,
  API_RANGES,
  API_VERSIONS_TEXT,
  featureRow,
  featuresText,
  wireObject,
  RAW,
  projectorInput,
}
