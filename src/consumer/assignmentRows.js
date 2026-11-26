const Decoder = require('../protocol/decoder')

const REPEATED_MESSAGE =
  'Consumer group assignment named a topic or partition more than once, using each once'

/**
 * Reads the rows of a member assignment as they stand on the wire. A leader's assigner may write a
 * topic in more than one row and a partition more than once in a row.
 * @param {Buffer} buffer
 * @returns {{ version: number, rows: Array<{ topic: string, partitions: number[] }>,
 *   userData: Buffer|null }|null} null when the buffer carries no version header
 */
const readRows = (buffer) => {
  const decoder = new Decoder(buffer)
  if (!decoder.canReadInt16()) {
    return null
  }

  return {
    version: decoder.readInt16(),
    rows: decoder.readArray((d) => ({
      topic: d.readString(),
      partitions: d.readArray((p) => p.readInt32()),
    })),
    userData: decoder.readBytes(),
  }
}

/**
 * Joins the rows of each topic, in row order, and keeps every partition as written.
 * @param {Array<{ topic: string, partitions: number[] }>} rows
 * @returns {Object<string, number[]>} partitions by topic
 */
const unionRows = (rows) => {
  const byTopic = new Map()
  for (const { topic, partitions } of rows) {
    const known = byTopic.get(topic)
    byTopic.set(topic, known ? known.concat(partitions) : partitions)
  }

  return Object.fromEntries(byTopic)
}

/**
 * Joins the rows of each topic and keeps each partition once, at its first place.
 * @param {Array<{ topic: string, partitions: number[] }>} rows
 * @returns {{ assignment: Object<string, number[]>,
 *   repeatedPartitions: Array<{ topic: string, partitions: number[] }>,
 *   repeatedTopics: string[] }} the partitions by topic, the partitions met again, and the topics
 *   written in more than one row
 */
const collapseRows = (rows) => {
  const unique = new Map()
  const repeated = new Map()
  const repeatedTopics = new Set()

  for (const { topic, partitions } of rows) {
    let kept = unique.get(topic)
    if (kept) {
      repeatedTopics.add(topic)
    } else {
      kept = new Set()
      unique.set(topic, kept)
    }

    for (const partition of partitions) {
      if (!kept.has(partition)) {
        kept.add(partition)
        continue
      }
      if (!repeated.has(topic)) repeated.set(topic, new Set())
      repeated.get(topic).add(partition)
    }
  }

  return {
    assignment: Object.fromEntries([...unique].map(([topic, kept]) => [topic, [...kept]])),
    repeatedPartitions: [...repeated].map(([topic, partitions]) => ({
      topic,
      partitions: [...partitions],
    })),
    repeatedTopics: [...repeatedTopics],
  }
}

/**
 * Decodes the assignment a classic member was handed, for applying it. Each topic is kept once with
 * the partitions of all its rows, each partition once, at its first place, so the member never
 * offset-fetches or fetches a partition twice. Java reads the same assignment into a set. When
 * anything was repeated, logs one warning for the assignment; nothing is thrown, because the
 * collapsed assignment is a valid one. The cost is one pass over the assignment per rebalance.
 *
 * @param {Buffer} buffer the SyncGroup answer's member assignment
 * @param {{ warn: (message: string, extra: object) => void }} logger
 * @param {{ groupId: string, memberId: string, generationId: number, groupProtocol: string }} context
 *   what the warning names; `groupProtocol` is the assignor the coordinator chose
 * @returns {{ version: number, assignment: Object<string, number[]>, userData: Buffer|null }|null}
 *   null when the buffer carries no version header
 */
const decodeApplied = (buffer, logger, context) => {
  const read = readRows(buffer)
  if (read === null) {
    return null
  }

  const { assignment, repeatedPartitions, repeatedTopics } = collapseRows(read.rows)
  if (repeatedPartitions.length > 0 || repeatedTopics.length > 0) {
    logger.warn(REPEATED_MESSAGE, { ...context, repeatedPartitions, repeatedTopics })
  }

  return { version: read.version, assignment, userData: read.userData }
}

module.exports = { readRows, unionRows, decodeApplied }
