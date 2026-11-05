const fs = require('fs')
const path = require('path')
const apiKeys = require('../../requests/apiKeys')
const flexibleVersions = require('../../flexibleVersions')
const { keyOfMessage, messageOfDirectory } = require('../../../../scripts/protocol/apiNames')

const REQUESTS = path.join(__dirname, '..', '..', 'requests')

/** The Fetch response vectors have their own corpus, with the batches and the abort rules. */
const FETCH = 'fetch'

/** A topic of the offset vectors, by the id the vectors carry for it. */
const TOPIC_NAMES = new Map([['01020304-0506-0708-090a-0b0c0d0e0f10', 'test-topic']])

/**
 * What a decoded response is, in the terms of a vector's control: plain data, with the numbers
 * the wire carries as 64 bits as decimal strings and bytes as hex, and the one throttle time a
 * response has, which the decoders hold under `clientSideThrottleTime`, under `throttleTime`.
 * @param {*} value what a response codec's `decode` returned
 * @returns {*} the same, as plain data
 */
const plain = (value) => {
  if (value === null || value === undefined || typeof value !== 'object') {
    return typeof value === 'bigint' ? value.toString() : value
  }
  if (Buffer.isBuffer(value)) return value.toString('hex')
  if (value.constructor && value.constructor.name === 'Long') return value.toString()
  if (Array.isArray(value)) return value.map(plain)
  if (value instanceof Map)
    return Object.fromEntries([...value].map(([key, item]) => [key, plain(item)]))

  const copy = {}
  for (const [key, item] of Object.entries(value)) {
    if (typeof item !== 'function' && item !== undefined) copy[key] = plain(item)
  }
  if ('clientSideThrottleTime' in copy) {
    copy.throttleTime = copy.clientSideThrottleTime
    delete copy.clientSideThrottleTime
  }
  return copy
}

/**
 * Where two values differ, if they do: every field of `expected` is in `actual` with the same
 * value, an array has the same length, and `actual` may hold more fields than `expected` names.
 * @param {*} expected the control
 * @param {*} actual a decoded value, as {@link plain} returns it
 * @param {string} [at] the path so far
 * @returns {string|null} the first difference, as `path: expected X, got Y`, or null
 */
const differences = (expected, actual, at = '') => {
  if (expected === null || typeof expected !== 'object') {
    return expected === actual
      ? null
      : `${at || 'value'}: expected ${JSON.stringify(expected)}, got ${JSON.stringify(actual)}`
  }
  if (Array.isArray(expected)) {
    if (!Array.isArray(actual) || actual.length !== expected.length) {
      return `${at}: expected a list of ${expected.length}, got ${Array.isArray(actual) ? `${actual.length}` : JSON.stringify(actual)}`
    }
    for (let i = 0; i < expected.length; i++) {
      const found = differences(expected[i], actual[i], `${at}[${i}]`)
      if (found) return found
    }
    return null
  }
  if (actual === null || typeof actual !== 'object' || Array.isArray(actual)) {
    return `${at}: expected an object, got ${JSON.stringify(actual)}`
  }
  for (const key of Object.keys(expected)) {
    if (!(key in actual)) return `${at}.${key}: missing`
    const found = differences(expected[key], actual[key], `${at}.${key}`)
    if (found) return found
  }
  return null
}

const byTopic = (a, b) => a.topic.localeCompare(b.topic)

/** The triples of a list of ranges, which is how a vector states 70 of them in a line. */
const triples = (ranges) =>
  ranges.map(({ apiKey, minVersion, maxVersion }) => [apiKey, minVersion, maxVersion])

/**
 * Projections a vector can ask for by name, when the whole value would be longer than the bytes:
 * a metadata response of a cluster with 50 partitions in one topic states the partitions as a count.
 * @type {Record<string, Function>}
 */
const NAMED = {
  topicsSummary: (value) => ({
    ...value,
    topicMetadata: value.topicMetadata.map(({ topic, topicId, partitionMetadata }) => [
      topic,
      topicId,
      partitionMetadata.length,
    ]),
  }),
}

/**
 * The few APIs whose decoded value is not the vector's fields once {@link plain} has run, and how.
 * `decoded` turns a plain decoded value into the vector's terms; `fields` turns a structured
 * vector's fields into the terms a decoder can hold.
 * @type {Record<string, { decoded?: Function, fields?: Function, options?: Function }>}
 */
const PROJECTIONS = {
  alterPartitionReassignments: {
    fields: ({ errorMessage, ...fields }, version) =>
      version === 1
        ? {
            ...fields,
            responses: fields.responses.map((topic) => ({
              ...topic,
              partitions: topic.partitions.map(({ errorMessage, ...partition }) => partition),
            })),
          }
        : fields,
  },
  createDelegationToken: {
    fields: ({ hmac, ...fields }) => ({ ...fields, hmac: hmac.$bytes }),
  },
  // the two keys that describe the unknown tagged field every struct of one vector carries are
  // not values a decoder returns
  deleteGroups: {
    fields: ({ unknownTag, unknownTagBytes, ...fields }) => fields,
  },
  describeDelegationToken: {
    fields: (fields) => ({
      ...fields,
      tokens: fields.tokens.map(({ hmac, ...token }) => ({ ...token, hmac: hmac.$bytes })),
    }),
  },
  // a broker and a partition are the values of their fields, in order: a broker is [nodeId, host,
  // port, rack]; a partition is [partitionErrorCode, partitionId, leader, leaderEpoch (v7 and
  // later), replicas, isr, offlineReplicas (v5 and later)]
  metadata: {
    decoded: (value) => ({
      ...value,
      brokers: value.brokers.map((broker) => Object.values(broker)),
      topicMetadata: value.topicMetadata.map((topic) => ({
        ...topic,
        partitionMetadata: topic.partitionMetadata.map((partition) => Object.values(partition)),
      })),
    }),
  },
  // every API a broker offers is a triple of numbers; a vector names its list `apiKeys`, and the
  // two keys that describe an unknown tagged field are not values a decoder returns
  apiVersions: {
    decoded: (value) =>
      value.apiVersions ? { ...value, apiVersions: triples(value.apiVersions) } : value,
    fields: ({ apiKeys, unknownTag, unknownTagBytes, ...fields }) => ({
      ...fields,
      apiVersions: triples(apiKeys),
    }),
  },
  // the decoder sorts topics by name, the wire lists them as sent
  deleteTopics: {
    fields: (fields) => ({ ...fields, topicErrors: [...fields.topicErrors].sort(byTopic) }),
  },
  // from v9 the response reads `responses` where the vectors say `topics`, and from v10 a topic is
  // named from the request's map and was sent as an id
  offsetCommit: {
    options: (version) => (version >= 10 ? { topicNames: TOPIC_NAMES } : undefined),
    decoded: (value, version) =>
      version >= 9
        ? { throttleTime: value.throttleTime, topics: topicsOf(value.responses, version) }
        : value,
  },
  offsetFetch: {
    options: (version) => (version >= 10 ? { topicNames: TOPIC_NAMES } : undefined),
    decoded: (value, version) =>
      version >= 9
        ? {
            throttleTime: value.throttleTime,
            errorCode: value.errorCode,
            topics: topicsOf(value.responses, version, ({ committedLeaderEpoch, ...rest }) => ({
              ...rest,
              leaderEpoch: committedLeaderEpoch,
            })),
          }
        : value,
    // the group id is in the body and not in what the decoder returns, from v9; v8's decoder keeps
    // it and flattens its one group. A body that names no group or several is refused.
    fields: (fields, version) => {
      if (version >= 8 && fields.groups && fields.groups.length !== 1) {
        return { error: 'KafkaJSInvariantViolation' }
      }
      const { groupId, ...rest } = fields
      if (version >= 9) return rest
      if (version === 7 && fields.topics)
        return {
          throttleTime: fields.throttleTime,
          errorCode: fields.errorCode,
          responses: fields.topics.map(({ topic, partitions }) => ({
            topic,
            partitions: partitions.map(({ leaderEpoch, ...partition }) => ({
              ...partition,
              committedLeaderEpoch: leaderEpoch,
            })),
          })),
        }
      if (version < 8) return fields
      return {
        throttleTime: fields.throttleTime,
        groupId,
        errorCode: fields.errorCode,
        responses: fields.topics.map(({ topic, partitions }) => ({
          topic,
          partitions: partitions.map(({ leaderEpoch, ...partition }) => ({
            ...partition,
            committedLeaderEpoch: leaderEpoch,
          })),
        })),
      }
    },
  },
}

/**
 * @param {{ topic: string, partitions: object[] }[]} responses the topics of a decoded response
 * @param {number} version
 * @param {(partition: object) => object} [partition] a change to each partition
 * @returns {object[]} the topics as the vectors state them: by name below v10, by the id the name
 *   was looked up from above
 */
function topicsOf(responses, version, partition = (found) => found) {
  const idOf = Object.fromEntries([...TOPIC_NAMES].map(([id, name]) => [name, id]))
  return responses.map(({ topic, partitions }) => ({
    ...(version >= 10 ? { topicId: idOf[topic] } : { topic }),
    partitions: partitions.map(partition),
  }))
}

/**
 * @typedef {object} ResponseVector
 * @property {string} name `<api>/<file>`
 * @property {string} directory the API's directory under `requests/`
 * @property {number} version
 * @property {'structured'|'bytes'} kind a Java vector with its fields, or bytes with a literal control
 * @property {Buffer} body
 * @property {object} control the values the unmutated body decodes to, or `{ error }` naming the
 *   class of the typed error it is decoded to
 * @property {string} [projection] the name of the projection the control is stated in
 * @property {string} codec absolute path of the response codec
 * @property {object} [request] what a codec that is built from its request is given
 */

const listFiles = (directory) =>
  fs
    .readdirSync(directory, { withFileTypes: true })
    .flatMap((entry) =>
      entry.isDirectory()
        ? listFiles(path.join(directory, entry.name)).map((rel) => path.join(entry.name, rel))
        : [entry.name]
    )
    .sort()

/** What a response codec that checks the answer against what was asked is built from. */
const REQUEST = { topics: [{ topic: 'test-topic', partitions: [{ partition: 0, offset: '1' }] }] }

/**
 * @param {string} directory the API's directory under `requests/`
 * @param {number} version
 * @returns {{ directory: string, version: number, codec: string, request?: object }} what stands for
 *   a vector of that API and version where there is no vector: a corpus case, a self-test
 */
const targetOf = (directory, version) => ({
  directory,
  version,
  codec: path.join(REQUESTS, directory, `v${version}`, 'response'),
  request: directory === 'deleteRecords' ? REQUEST : undefined,
})

let memo = null

/**
 * Every response vector of every API but Fetch, found on disk, with its control: a structured
 * vector's own fields, or the literal written for it in `fuzzResponseControls.json`, where a vector
 * whose body is read as the same values as another's names that vector instead of repeating them.
 * @returns {{ vectors: ResponseVector[], missing: string[], unused: string[], excluded: string[] }}
 *   `missing` are response fixtures with no control and no exclusion, `unused` controls that name
 *   no fixture, and `excluded` the fixtures set aside with their reason
 */
const loadVectors = () => {
  if (memo) return memo

  const book = require('../../fixtures/fuzzResponseControls.json')
  const vectors = []
  const missing = []
  const seen = new Set()
  for (const directory of fs.readdirSync(REQUESTS).sort()) {
    const fixtures = path.join(REQUESTS, directory, 'fixtures')
    if (directory === FETCH || !fs.existsSync(fixtures)) continue

    for (const rel of listFiles(fixtures).filter(
      (file) => /response/i.test(file) && file.endsWith('.json')
    )) {
      const name = `${directory}/${rel.split(path.sep).join('/')}`
      const json = JSON.parse(fs.readFileSync(path.join(fixtures, rel), 'utf8'))
      seen.add(name)
      if (book.exclusions[name]) continue

      const structured = typeof json.hex === 'string'
      const version = Number.isInteger(json.version)
        ? json.version
        : Number((/^v(\d+)_/.exec(path.basename(rel)) || [])[1])
      const written = book.controls[name]
      const found = typeof written === 'string' ? book.controls[written] : written
      const entry = found && found.project ? found.control : found
      if (!structured && !entry) {
        missing.push(name)
        continue
      }

      const hooks = PROJECTIONS[directory] || {}
      vectors.push({
        name,
        directory,
        version,
        kind: structured ? 'structured' : 'bytes',
        body: structured ? Buffer.from(json.hex, 'hex') : Buffer.from(json.data),
        control: structured
          ? hooks.fields
            ? hooks.fields(json.fields, version)
            : json.fields
          : entry,
        projection: found && found.project,
        ...targetOf(directory, version),
      })
    }
  }

  const unused = Object.keys(book.controls).filter((name) => !seen.has(name))
  memo = { vectors, missing, unused, excluded: Object.keys(book.exclusions) }
  return memo
}

/**
 * @param {ResponseVector} vector
 * @returns {{ decode: Function, parse: Function }} the codec of the vector's version; a codec that
 *   is built from the request it answers is given a request that matches the vector
 */
const codecOf = ({ codec, request }) => {
  const found = require(codec)
  return typeof found === 'function' ? found(request) : found
}

/**
 * @param {ResponseVector} vector
 * @returns {object|undefined} the options the vector's decoder is given
 */
const optionsOf = ({ directory, version }) => {
  const hooks = PROJECTIONS[directory]
  if (hooks && hooks.options) return hooks.options(version)
  return directory === 'produce' && version === 13 ? { topicNames: TOPIC_NAMES } : undefined
}

/**
 * What a decoded response is, in the terms of the vector's control.
 * @param {ResponseVector} vector
 * @param {*} decoded
 * @returns {*}
 */
const project = ({ directory, version, projection }, decoded) => {
  const value = plain(decoded)
  const hooks = PROJECTIONS[directory]
  const projected = hooks && hooks.decoded ? hooks.decoded(value, version) : value
  return projection ? NAMED[projection](projected) : projected
}

/**
 * The frame a broker sends for the vector, as the wire is written from the definition: the size,
 * the correlation id, a tag buffer when the response header is flexible (the version is flexible,
 * ApiVersions' header being the standing exception), then the body.
 * @param {ResponseVector} vector
 * @param {number} correlationId
 * @returns {Buffer}
 */
const frameOf = (vector, correlationId = 0) => {
  const key = keyOfMessage(messageOfDirectory(vector.directory))
  const first = flexibleVersions[apiKeys[key]]
  const flexibleHeader = key !== 'ApiVersions' && first !== null && vector.version >= first
  const header = Buffer.alloc(flexibleHeader ? 5 : 4)
  header.writeInt32BE(correlationId)
  const payload = Buffer.concat([header, vector.body])
  const size = Buffer.alloc(4)
  size.writeInt32BE(payload.length)
  return Buffer.concat([size, payload])
}

module.exports = {
  loadVectors,
  targetOf,
  codecOf,
  optionsOf,
  project,
  plain,
  differences,
  frameOf,
  TOPIC_NAMES,
}
