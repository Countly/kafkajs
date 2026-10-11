const fs = require('fs')
const path = require('path')

const REQUESTS = path.join(__dirname, '..', '..', '..', 'src', 'protocol', 'requests')
const README = fs.readFileSync(path.join(__dirname, '..', 'javaVectors', 'README.md'), 'utf8')

/**
 * @param {string} message lower-camel Apache message name
 * @param {number[]} versions
 * @param {string[]} caseIds
 * @returns {string[]} vector file stems
 */
const stems = (message, versions, caseIds) =>
  versions.flatMap((version) => caseIds.map((caseId) => `${message}-v${version}-${caseId}`))

const ACL_BASIC = ['basic']
const ACL_V3 = ['basic', 'userResource']

/** Every vector the generator writes for the admin APIs, by fixtures directory. */
const MANIFEST = {
  incrementalAlterConfigs: [
    ...stems(
      'incrementalAlterConfigsRequest',
      [0, 1],
      ['set', 'append', 'subtract', 'delete', 'twoResources', 'validateOnly']
    ),
    ...stems('incrementalAlterConfigsResponse', [0, 1], ['ok', 'error']),
  ],
  describeConfigs: [
    ...stems(
      'describeConfigsRequest',
      [3, 4],
      ['keys', 'allKeys', 'includeDocumentation', 'noSynonyms']
    ),
    ...stems('describeConfigsResponse', [3, 4], ['entries']),
  ],
  alterConfigs: [
    ...stems('alterConfigsRequest', [2], ['basic', 'validateOnly']),
    ...stems('alterConfigsResponse', [2], ['ok', 'error']),
  ],
  createTopics: [
    ...stems('createTopicsRequest', [4, 5, 6, 7], ['defaults', 'explicit', 'assigned']),
    ...stems('createTopicsResponse', [4, 5, 6, 7], ['ok', 'error']),
    ...stems('createTopicsResponse', [5, 6, 7], ['topicConfigError']),
  ],
  deleteTopics: [
    ...stems('deleteTopicsRequest', [2, 3, 4, 5], ['names']),
    ...stems('deleteTopicsRequest', [6], ['byName']),
    ...stems('deleteTopicsResponse', [2, 3, 4, 5, 6], ['ok', 'error']),
    ...stems('deleteTopicsResponse', [3, 4, 5, 6], ['deletionDisabled']),
  ],
  createPartitions: [
    ...stems(
      'createPartitionsRequest',
      [2, 3],
      ['noAssignments', 'explicitEmptyAssignments', 'assignments']
    ),
    ...stems('createPartitionsResponse', [2, 3], ['ok', 'error']),
  ],
  deleteRecords: [
    ...stems('deleteRecordsRequest', [2], ['basic']),
    ...stems('deleteRecordsResponse', [2], ['ok', 'error']),
  ],
  deleteGroups: [
    ...stems('deleteGroupsRequest', [2], ['basic']),
    ...stems('deleteGroupsResponse', [2], ['ok', 'error']),
  ],
  describeAcls: [
    ...stems('describeAclsRequest', [2], [...ACL_BASIC, 'anyFilter']),
    ...stems('describeAclsRequest', [3], [...ACL_V3, 'anyFilter']),
    ...stems('describeAclsResponse', [2], ACL_BASIC),
    ...stems('describeAclsResponse', [3], ACL_V3),
  ],
  createAcls: [
    ...stems('createAclsRequest', [2], ACL_BASIC),
    ...stems('createAclsRequest', [3], ACL_V3),
    ...stems('createAclsResponse', [2, 3], ['ok', 'error']),
  ],
  deleteAcls: [
    ...stems('deleteAclsRequest', [2], [...ACL_BASIC, 'anyFilter']),
    ...stems('deleteAclsRequest', [3], [...ACL_V3, 'anyFilter']),
    ...stems('deleteAclsResponse', [2], ACL_BASIC),
    ...stems('deleteAclsResponse', [3], ACL_V3),
  ],
  describeTopicPartitions: [
    ...stems('describeTopicPartitionsRequest', [0], ['noCursor', 'cursor']),
    ...stems(
      'describeTopicPartitionsResponse',
      [0],
      ['elrNull', 'elrEmpty', 'elrPopulated', 'nextCursor', 'noNextCursor', 'unauthorizedZeroId']
    ),
  ],
}

const ENTRIES = Object.entries(MANIFEST).flatMap(([api, names]) =>
  names.map((name) => ({ api, name }))
)

/** `fields` carry these as decimal strings, which are not on the wire as text. */
const INT64_KEYS = new Set(['offset', 'lowWatermark'])
const UUID = /^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$/

/**
 * @param {string} api fixtures directory name
 * @param {string} name vector file stem
 * @returns {object} one vector document
 */
const load = (api, name) =>
  JSON.parse(fs.readFileSync(path.join(REQUESTS, api, 'fixtures', 'java', `${name}.json`), 'utf8'))

/**
 * @param {string} api
 * @param {string} name
 * @returns {Buffer} one vector's bytes
 */
const bytes = (api, name) => Buffer.from(load(api, name).hex, 'hex')

/**
 * @param {Buffer} a
 * @param {Buffer} b same length as `a`
 * @returns {number[]} the offsets at which the two differ
 */
const differences = (a, b) => {
  expect(a.length).toEqual(b.length)
  return [...a.keys()].filter((offset) => a[offset] !== b[offset])
}

/**
 * @param {*} value a `fields` subtree
 * @param {string} [key] the key it sits under
 * @returns {string[]} every text value in it as the hex of its UTF-8 bytes, which must appear in
 *   the vector's bytes; integers and uuids are left out
 */
const wireHex = (value, key = '') => {
  if (typeof value === 'string') {
    return INT64_KEYS.has(key) || UUID.test(value) ? [] : [Buffer.from(value).toString('hex')]
  }
  if (Array.isArray(value)) {
    return value.flatMap((item) => wireHex(item, key))
  }
  if (value !== null && typeof value === 'object') {
    return Object.entries(value).flatMap(([k, v]) => wireHex(v, k))
  }
  return []
}

/**
 * Wire pieces, so an expected body reads as the protocol and not as a copied hex string.
 *
 * @param {number} value non-negative
 * @param {number} size bytes, big-endian
 * @returns {string} the INT8 / INT16 / INT32 as hex
 */
const int = (value, size) => value.toString(16).padStart(size * 2, '0')

/** @param {string} text @returns {string} a STRING: INT16 length, then the UTF-8 bytes */
const str = (text) => int(text.length, 2) + Buffer.from(text).toString('hex')

/** @param {string} text @returns {string} a COMPACT_STRING: UVARINT length + 1, then the bytes */
const compact = (text) => int(text.length + 1, 1) + Buffer.from(text).toString('hex')

/** @param {number} elements @returns {string} a COMPACT_ARRAY's one-byte length: count + 1 */
const count = (elements) => int(elements + 1, 1)

const TAGS = '00' // an empty tag buffer ends every flexible struct
const TOPIC = 'test-topic'
const OTHER = 'other-topic'
const THROTTLE = int(25, 4) // ThrottleTimeMs
const TIMEOUT = int(5000, 4)
const NULL_COMPACT = '00' // a compact null string, bytes or array
const NULL_CLASSIC_STRING = 'ffff'
const NULL_CLASSIC_ARRAY = 'ffffffff'
const ZERO_UUID = '00'.repeat(16)
const TOPIC_UUID = '0102030405060708090a0b0c0d0e0f10'
const NO_AUTHORIZED_OPERATIONS = '80000000' // -2147483648

/**
 * A CreateTopics response whose only error is TopicConfigErrorCode 29. It travels in tag 0 (tag,
 * size 2, the code) after the topic struct; at 0 it is not written.
 *
 * @param {string} topicId the TopicId as hex, which v7 puts between Name and ErrorCode; empty below
 * @returns {string} the body as hex
 */
const topicConfigErrorResponse = (topicId) =>
  THROTTLE +
  count(1) +
  compact(TOPIC) +
  topicId +
  int(0, 2) +
  NULL_COMPACT +
  int(3, 4) +
  int(2, 2) +
  NULL_COMPACT +
  (int(1, 1) + int(0, 1) + int(2, 1) + int(29, 2)) +
  TAGS

/**
 * A DescribeTopicPartitions response with one topic and one partition, whose two ELR arrays are
 * the only part that varies; the cursor is null.
 *
 * @param {string} eligibleLeaderReplicas the nullable compact int array, as hex
 * @param {string} lastKnownElr the nullable compact int array, as hex
 * @returns {string} the body as hex
 */
const describeTopicPartitionsResponse = (eligibleLeaderReplicas, lastKnownElr) =>
  THROTTLE +
  count(1) +
  (int(0, 2) +
    compact(TOPIC) +
    TOPIC_UUID +
    int(0, 1) +
    count(1) +
    (int(0, 2) +
      int(0, 4) +
      int(1, 4) +
      int(5, 4) +
      (count(3) + int(1, 4) + int(2, 4) + int(3, 4)) +
      (count(2) + int(1, 4) + int(2, 4)) +
      eligibleLeaderReplicas +
      lastKnownElr +
      (count(1) + int(3, 4)) +
      TAGS) +
    NO_AUTHORIZED_OPERATIONS +
    TAGS) +
  'ff' +
  TAGS

/** Full expected bodies of the short vectors, as `[api, file stem, hex]`. */
const PINNED = [
  // IncrementalAlterConfigs: a DELETE writes a null Value, ffff when classic and 00 when compact.
  [
    'incrementalAlterConfigs',
    'incrementalAlterConfigsRequest-v0-delete',
    int(1, 4) +
      int(2, 1) +
      str(TOPIC) +
      int(1, 4) +
      str('retention.ms') +
      int(1, 1) +
      NULL_CLASSIC_STRING +
      int(0, 1),
  ],
  [
    'incrementalAlterConfigs',
    'incrementalAlterConfigsRequest-v1-delete',
    count(1) +
      int(2, 1) +
      compact(TOPIC) +
      count(1) +
      (compact('retention.ms') + int(1, 1) + NULL_COMPACT + TAGS) +
      TAGS +
      int(0, 1) +
      TAGS,
  ],
  [
    'incrementalAlterConfigs',
    'incrementalAlterConfigsResponse-v1-ok',
    THROTTLE + count(1) + (int(0, 2) + NULL_COMPACT + int(2, 1) + compact(TOPIC) + TAGS) + TAGS,
  ],
  // The operation byte: APPEND is 2 and SUBTRACT is 3, each with its value.
  [
    'incrementalAlterConfigs',
    'incrementalAlterConfigsRequest-v0-append',
    int(1, 4) +
      int(2, 1) +
      str(TOPIC) +
      int(1, 4) +
      str('cleanup.policy') +
      int(2, 1) +
      str('compact') +
      int(0, 1),
  ],
  [
    'incrementalAlterConfigs',
    'incrementalAlterConfigsRequest-v0-subtract',
    int(1, 4) +
      int(2, 1) +
      str(TOPIC) +
      int(1, 4) +
      str('cleanup.policy') +
      int(3, 1) +
      str('delete') +
      int(0, 1),
  ],
  [
    'incrementalAlterConfigs',
    'incrementalAlterConfigsRequest-v1-append',
    count(1) +
      int(2, 1) +
      compact(TOPIC) +
      count(1) +
      (compact('cleanup.policy') + int(2, 1) + compact('compact') + TAGS) +
      TAGS +
      int(0, 1) +
      TAGS,
  ],
  [
    'incrementalAlterConfigs',
    'incrementalAlterConfigsRequest-v1-subtract',
    count(1) +
      int(2, 1) +
      compact(TOPIC) +
      count(1) +
      (compact('cleanup.policy') + int(3, 1) + compact('delete') + TAGS) +
      TAGS +
      int(0, 1) +
      TAGS,
  ],
  // DescribeConfigs: the key list is a string array when named and the null array when not.
  [
    'describeConfigs',
    'describeConfigsRequest-v3-keys',
    int(1, 4) +
      int(2, 1) +
      str(TOPIC) +
      (int(2, 4) + str('cleanup.policy') + str('retention.ms')) +
      int(1, 1) +
      int(0, 1),
  ],
  [
    'describeConfigs',
    'describeConfigsRequest-v3-allKeys',
    int(1, 4) + int(2, 1) + str(TOPIC) + NULL_CLASSIC_ARRAY + int(1, 1) + int(0, 1),
  ],
  [
    'describeConfigs',
    'describeConfigsRequest-v4-keys',
    count(1) +
      int(2, 1) +
      compact(TOPIC) +
      (count(2) + compact('cleanup.policy') + compact('retention.ms')) +
      TAGS +
      int(1, 1) +
      int(0, 1) +
      TAGS,
  ],
  [
    'describeConfigs',
    'describeConfigsRequest-v4-allKeys',
    count(1) + int(2, 1) + compact(TOPIC) + NULL_COMPACT + TAGS + int(1, 1) + int(0, 1) + TAGS,
  ],
  // CreateTopics: v4 is classic, v5 flexible; the -1/-1 defaults and empty lists are in the body.
  [
    'createTopics',
    'createTopicsRequest-v4-defaults',
    int(1, 4) + str(TOPIC) + 'ffffffff' + 'ffff' + int(0, 4) + int(0, 4) + TIMEOUT + int(0, 1),
  ],
  [
    'createTopics',
    'createTopicsRequest-v5-defaults',
    count(1) +
      compact(TOPIC) +
      'ffffffff' +
      'ffff' +
      count(0) +
      count(0) +
      TAGS +
      TIMEOUT +
      int(0, 1) +
      TAGS,
  ],
  // The error answer: v7 puts a zero TopicId between Name and ErrorCode; v5 keeps NumPartitions,
  // ReplicationFactor and the null Configs after the message.
  [
    'createTopics',
    'createTopicsResponse-v7-error',
    THROTTLE +
      count(1) +
      compact(TOPIC) +
      ZERO_UUID +
      int(36, 2) +
      compact("Topic 'test-topic' already exists.") +
      'ffffffff' +
      'ffff' +
      NULL_COMPACT +
      TAGS +
      TAGS,
  ],
  // TopicConfigErrorCode 29 travels in tag 0 at v5 to v7; v7 also carries the TopicId after the Name.
  ['createTopics', 'createTopicsResponse-v5-topicConfigError', topicConfigErrorResponse('')],
  ['createTopics', 'createTopicsResponse-v6-topicConfigError', topicConfigErrorResponse('')],
  [
    'createTopics',
    'createTopicsResponse-v7-topicConfigError',
    topicConfigErrorResponse(TOPIC_UUID),
  ],
  // DeleteTopics: names at v2, a name with the zero TopicId at v6.
  ['deleteTopics', 'deleteTopicsRequest-v2-names', int(2, 4) + str(TOPIC) + str(OTHER) + TIMEOUT],
  [
    'deleteTopics',
    'deleteTopicsRequest-v6-byName',
    count(2) +
      (compact(TOPIC) + ZERO_UUID + TAGS) +
      (compact(OTHER) + ZERO_UUID + TAGS) +
      TIMEOUT +
      TAGS,
  ],
  [
    'deleteTopics',
    'deleteTopicsResponse-v6-error',
    THROTTLE +
      count(1) +
      compact(TOPIC) +
      ZERO_UUID +
      int(3, 2) +
      compact('This server does not host this topic-partition.') +
      TAGS +
      TAGS,
  ],
  // CreatePartitions: no assignments is the compact null, 00; an empty list would be 01.
  [
    'createPartitions',
    'createPartitionsRequest-v2-noAssignments',
    count(1) + compact(TOPIC) + int(3, 4) + NULL_COMPACT + TAGS + TIMEOUT + int(0, 1) + TAGS,
  ],
  [
    'createPartitions',
    'createPartitionsRequest-v2-explicitEmptyAssignments',
    count(1) + compact(TOPIC) + int(3, 4) + count(0) + TAGS + TIMEOUT + int(0, 1) + TAGS,
  ],
  [
    'createPartitions',
    'createPartitionsRequest-v3-assignments',
    count(1) +
      compact(TOPIC) +
      int(3, 4) +
      (count(2) +
        (count(2) + int(1, 4) + int(2, 4) + TAGS) +
        (count(2) + int(2, 4) + int(3, 4) + TAGS)) +
      TAGS +
      TIMEOUT +
      int(0, 1) +
      TAGS,
  ],
  // DeleteRecords / DeleteGroups.
  [
    'deleteRecords',
    'deleteRecordsRequest-v2-basic',
    count(1) +
      compact(TOPIC) +
      (count(2) + (int(0, 4) + int(5, 8) + TAGS) + (int(2, 4) + 'ffffffffffffffff' + TAGS)) +
      TAGS +
      TIMEOUT +
      TAGS,
  ],
  [
    'deleteGroups',
    'deleteGroupsRequest-v2-basic',
    count(2) + compact('test-group') + compact('other-group') + TAGS,
  ],
  // ACLs: a USER resource is resource type 7.
  [
    'createAcls',
    'createAclsRequest-v3-userResource',
    count(1) +
      int(7, 1) +
      compact('User:bob') +
      int(3, 1) +
      compact('User:alice') +
      compact('*') +
      int(3, 1) +
      int(3, 1) +
      TAGS +
      TAGS,
  ],
  [
    'describeAcls',
    'describeAclsRequest-v3-userResource',
    int(7, 1) +
      compact('User:bob') +
      int(3, 1) +
      compact('User:alice') +
      compact('*') +
      int(3, 1) +
      int(3, 1) +
      TAGS,
  ],
  [
    'deleteAcls',
    'deleteAclsRequest-v3-userResource',
    count(1) +
      int(7, 1) +
      compact('User:bob') +
      int(3, 1) +
      compact('User:alice') +
      compact('*') +
      int(3, 1) +
      int(3, 1) +
      TAGS +
      TAGS,
  ],
  [
    'describeAcls',
    'describeAclsRequest-v3-anyFilter',
    int(1, 1) +
      NULL_COMPACT +
      int(1, 1) +
      NULL_COMPACT +
      NULL_COMPACT +
      int(1, 1) +
      int(1, 1) +
      TAGS,
  ],
  // DescribeTopicPartitions: a null cursor is the marker byte ff, a present one 01 and the struct.
  [
    'describeTopicPartitions',
    'describeTopicPartitionsRequest-v0-noCursor',
    count(2) + (compact(OTHER) + TAGS) + (compact(TOPIC) + TAGS) + int(2000, 4) + 'ff' + TAGS,
  ],
  [
    'describeTopicPartitions',
    'describeTopicPartitionsRequest-v0-cursor',
    count(1) +
      (compact(TOPIC) + TAGS) +
      int(1, 4) +
      (int(1, 1) + compact(TOPIC) + int(2, 4) + TAGS) +
      TAGS,
  ],
  [
    'describeTopicPartitions',
    'describeTopicPartitionsResponse-v0-unauthorizedZeroId',
    THROTTLE +
      count(1) +
      (int(29, 2) +
        compact(TOPIC) +
        ZERO_UUID +
        int(0, 1) +
        count(0) +
        NO_AUTHORIZED_OPERATIONS +
        TAGS) +
      'ff' +
      TAGS,
  ],
  // The ELR arrays are nullable: 00 is null and any other length byte is the count plus one.
  [
    'describeTopicPartitions',
    'describeTopicPartitionsResponse-v0-elrNull',
    describeTopicPartitionsResponse(NULL_COMPACT, NULL_COMPACT),
  ],
  [
    'describeTopicPartitions',
    'describeTopicPartitionsResponse-v0-elrEmpty',
    describeTopicPartitionsResponse(count(0), count(0)),
  ],
  [
    'describeTopicPartitions',
    'describeTopicPartitionsResponse-v0-elrPopulated',
    describeTopicPartitionsResponse(count(1) + int(1, 4), count(1) + int(2, 4)),
  ],
]

/** The three DescribeTopicPartitions responses that differ in the ELR arrays, and what they write. */
const ELR_CASES = [
  ['elrNull', NULL_COMPACT],
  ['elrEmpty', count(0)],
]

describe('Java message vectors > admin APIs', () => {
  test('the manifest names one hundred and nineteen vectors', () => {
    expect(ENTRIES).toHaveLength(119)
  })

  test.each(ENTRIES)('$name is on disk and is what its name says', ({ api, name }) => {
    const vector = load(api, name)
    const [, message, version, caseId] = name.match(/^(\w+)-v(\d+)-(\w+)$/)

    expect(vector.message.charAt(0).toLowerCase() + vector.message.slice(1)).toEqual(message)
    expect(vector.version).toEqual(Number(version))
    expect(vector.case).toEqual(caseId)
    expect(vector.hex).toMatch(/^(?:[0-9a-f]{2})+$/)
    expect(vector.fields).toEqual(expect.any(Object))
  })

  test.each(ENTRIES)('$name: every text value in fields is on the wire', ({ api, name }) => {
    const { hex, fields } = load(api, name)

    for (const wire of wireHex(fields)) {
      expect(hex).toContain(wire)
    }
  })

  test.each(ENTRIES.filter(({ name }) => /Response-/.test(name)))(
    '$name: ThrottleTimeMs is 25, so a decoder that does not split it fails',
    ({ api, name }) => {
      expect(load(api, name).hex.startsWith(THROTTLE)).toBe(true)
      expect(load(api, name).fields.throttleTime).toEqual(25)
    }
  )

  test.each(Object.keys(MANIFEST))(
    '%s: the fixtures directory holds exactly the manifest',
    (api) => {
      const onDisk = fs.readdirSync(path.join(REQUESTS, api, 'fixtures', 'java'))

      expect(onDisk.sort()).toEqual(MANIFEST[api].map((name) => `${name}.json`).sort())
    }
  )

  test.each(Object.keys(MANIFEST))('%s: the README table lists its versions and vectors', (api) => {
    const versions = [...new Set(MANIFEST[api].map((name) => Number(name.match(/-v(\d+)-/)[1])))]
    // the first table of this README has two columns, the one for these entry points four
    const row = README.split('\n').find(
      (line) =>
        line.startsWith(`| ${api[0].toUpperCase()}${api.slice(1)} |`) &&
        line.split('|').length === 6
    )
    const [, , , listed, vectors] = row.split('|').map((cell) => cell.trim())
    expect(listed.split(', ').map(Number)).toEqual(expect.arrayContaining(versions))
    expect(Number(vectors)).toEqual(MANIFEST[api].length)
  })

  test.each(PINNED)('%s/%s: the bytes are the protocol', (api, name, expected) => {
    expect(load(api, name).hex).toEqual(expected)
  })

  test.each([0, 1])(
    'IncrementalAlterConfigs v%i: DELETE and SET differ in the operation and the value',
    (version) => {
      const set = bytes('incrementalAlterConfigs', `incrementalAlterConfigsRequest-v${version}-set`)
      const del = bytes(
        'incrementalAlterConfigs',
        `incrementalAlterConfigsRequest-v${version}-delete`
      )

      expect(set.length).toBeGreaterThan(del.length)
      expect(
        load('incrementalAlterConfigs', `incrementalAlterConfigsRequest-v${version}-delete`).fields
      ).toMatchObject({ resources: [{ configEntries: [{ operation: 1, value: null }] }] })
    }
  )

  test('IncrementalAlterConfigs: v1 is v0 in flexible framing, so no case has the same bytes', () => {
    for (const caseId of ['set', 'append', 'subtract', 'delete', 'twoResources', 'validateOnly']) {
      const [v0, v1] = [0, 1].map(
        (version) =>
          load('incrementalAlterConfigs', `incrementalAlterConfigsRequest-v${version}-${caseId}`)
            .hex
      )

      expect(v1).not.toEqual(v0)
    }
  })

  test.each([3, 4])(
    'DescribeConfigs v%i: IncludeDocumentation and IncludeSynonyms are one boolean each',
    (version) => {
      const all = bytes('describeConfigs', `describeConfigsRequest-v${version}-allKeys`)
      const docs = bytes(
        'describeConfigs',
        `describeConfigsRequest-v${version}-includeDocumentation`
      )
      const noSynonyms = bytes('describeConfigs', `describeConfigsRequest-v${version}-noSynonyms`)
      const tail = version === 4 ? 2 : 1 // the request's tag buffer follows a flexible body

      expect(differences(all, docs)).toEqual([all.length - tail])
      expect(differences(all, noSynonyms)).toEqual([all.length - tail - 1])
    }
  )

  test.each([2, 3])(
    'CreatePartitions v%i: a null list and an empty list differ in the one length byte',
    (version) => {
      const none = bytes('createPartitions', `createPartitionsRequest-v${version}-noAssignments`)
      const empty = bytes(
        'createPartitions',
        `createPartitionsRequest-v${version}-explicitEmptyAssignments`
      )

      expect(differences(none, empty)).toHaveLength(1)
      expect([none[differences(none, empty)[0]], empty[differences(none, empty)[0]]]).toEqual([
        0, 1,
      ])
    }
  )

  test('CreateTopics: v5, v6 and v7 send the same request, which is not the classic v4 one', () => {
    for (const caseId of ['defaults', 'explicit', 'assigned']) {
      const [v4, v5, v6, v7] = [4, 5, 6, 7].map(
        (version) => load('createTopics', `createTopicsRequest-v${version}-${caseId}`).hex
      )

      expect([v6, v7]).toEqual([v5, v5])
      expect(v4).not.toEqual(v5)
    }
  })

  test('CreateTopics: v6 answers in v5 bytes and v7 inserts the TopicId after the Name', () => {
    const v5 = bytes('createTopics', 'createTopicsResponse-v5-ok')
    const v6 = bytes('createTopics', 'createTopicsResponse-v6-ok')
    const v7 = bytes('createTopics', 'createTopicsResponse-v7-ok')
    const nameEnd = 4 + 1 + 1 + TOPIC.length

    expect(v6).toEqual(v5)
    expect(v7).toHaveLength(v5.length + 16)
    expect(v7.subarray(nameEnd, nameEnd + 16).toString('hex')).toEqual(TOPIC_UUID)
    expect(Buffer.concat([v7.subarray(0, nameEnd), v7.subarray(nameEnd + 16)])).toEqual(v5)
  })

  test('DeleteTopics: v2 and v3 send one body, v4 and v5 another, and v6 changes the shape', () => {
    const hex = (version, caseId) =>
      load('deleteTopics', `deleteTopicsRequest-v${version}-${caseId}`).hex

    expect(hex(3, 'names')).toEqual(hex(2, 'names'))
    expect(hex(5, 'names')).toEqual(hex(4, 'names'))
    expect(hex(4, 'names')).not.toEqual(hex(3, 'names'))
    expect(hex(6, 'byName')).not.toEqual(hex(5, 'names'))
  })

  test('CreatePartitions, DeleteRecords and the ACL APIs: v3 sends the bytes v2 does', () => {
    for (const [api, message, caseIds] of [
      [
        'createPartitions',
        'createPartitionsRequest',
        ['noAssignments', 'explicitEmptyAssignments', 'assignments'],
      ],
      ['createPartitions', 'createPartitionsResponse', ['ok', 'error']],
      ['describeAcls', 'describeAclsRequest', ['basic', 'anyFilter']],
      ['describeAcls', 'describeAclsResponse', ['basic']],
      ['createAcls', 'createAclsRequest', ['basic']],
      ['createAcls', 'createAclsResponse', ['ok', 'error']],
      ['deleteAcls', 'deleteAclsRequest', ['basic', 'anyFilter']],
      ['deleteAcls', 'deleteAclsResponse', ['basic']],
    ]) {
      for (const caseId of caseIds) {
        expect(load(api, `${message}-v3-${caseId}`).hex).toEqual(
          load(api, `${message}-v2-${caseId}`).hex
        )
      }
    }
  })

  test.each(['describeAclsRequest', 'createAclsRequest', 'deleteAclsRequest'])(
    '%s v3: the USER resource is resource type 7 where the topic ACL has 2',
    (message) => {
      const api = message.replace('Request', '')
      const topic = load(api, `${message}-v3-basic`).fields
      const user = load(api, `${message}-v3-userResource`).fields
      const resourceType = (fields) =>
        (fields.creations || fields.filters || [fields])[0].resourceType

      expect([resourceType(topic), resourceType(user)]).toEqual([2, 7])
    }
  )

  test('DescribeTopicPartitions: a null and an empty ELR list differ in two length bytes', () => {
    const [[, nullList], [, emptyList]] = ELR_CASES
    const withNull = bytes('describeTopicPartitions', 'describeTopicPartitionsResponse-v0-elrNull')
    const withEmpty = bytes(
      'describeTopicPartitions',
      'describeTopicPartitionsResponse-v0-elrEmpty'
    )
    const populated = bytes(
      'describeTopicPartitions',
      'describeTopicPartitionsResponse-v0-elrPopulated'
    )
    const offsets = differences(withNull, withEmpty)

    expect(populated).not.toEqual(withEmpty)

    expect(offsets).toHaveLength(2)
    expect(offsets.map((offset) => withNull[offset].toString(16).padStart(2, '0'))).toEqual([
      nullList,
      nullList,
    ])
    expect(offsets.map((offset) => withEmpty[offset].toString(16).padStart(2, '0'))).toEqual([
      emptyList,
      emptyList,
    ])
  })

  test('DescribeTopicPartitions: a response with a next cursor ends with the marker 01, one without with ff', () => {
    const tail = (caseId) =>
      bytes('describeTopicPartitions', `describeTopicPartitionsResponse-v0-${caseId}`).subarray(-2)
    const nextCursor = bytes(
      'describeTopicPartitions',
      'describeTopicPartitionsResponse-v0-nextCursor'
    )
    const struct = Buffer.from(compact(TOPIC) + int(1, 4) + TAGS + TAGS, 'hex')

    expect(tail('noNextCursor').toString('hex')).toEqual('ff00')
    expect(nextCursor.subarray(-struct.length - 1).toString('hex')).toEqual(
      '01' + struct.toString('hex')
    )
  })
})
