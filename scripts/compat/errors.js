'use strict'

/**
 * Error-table comparison (spec F1). The fork's src/protocol/error.js is compared with the
 * committed Errors.java snapshot for the pinned tag:
 *
 *   missingInFork   defined upstream, absent here            -> report only (Phase 4 work)
 *   unknownToKafka  defined here, not by the pinned Kafka    -> failure
 *   mismatches      same code, different type name           -> failure unless aliased
 *   aliased         a known historical KafkaJS name          -> report only
 *
 * Retriable flags are neither checked nor generated: RetriableException inheritance is not
 * visible in Errors.java (spec A5/F2).
 */

const fs = require('fs')
const path = require('path')

/** Kafka's NONE(0) is the success sentinel, not an error, and the fork rightly has no entry. */
const SUCCESS_CODE = 0

const readSnapshot = (tag, root) =>
  JSON.parse(fs.readFileSync(path.join(root || __dirname, 'errors', `${tag}.json`), 'utf8'))

/**
 * @param {{ snapshot: {errors: Array}, forkErrorCodes: Array, aliases: object }} input
 */
const compareErrorTables = (input) => {
  const aliases = input.aliases || {}
  const kafka = new Map()
  for (const error of input.snapshot.errors) {
    if (error.code !== SUCCESS_CODE) kafka.set(error.code, error.type)
  }
  const fork = new Map(input.forkErrorCodes.map((error) => [error.code, error.type]))

  const missingInFork = []
  for (const [code, type] of kafka) {
    if (!fork.has(code)) missingInFork.push({ code, type })
  }

  const unknownToKafka = []
  const mismatches = []
  const aliased = []
  for (const [code, type] of fork) {
    if (!kafka.has(code)) {
      unknownToKafka.push({ code, type })
      continue
    }
    const kafkaType = kafka.get(code)
    if (kafkaType === type) continue
    const alias = aliases[type]
    if (alias !== undefined && alias.kafkaType === kafkaType) {
      aliased.push({ code, forkType: type, kafkaType, reason: alias.reason })
    } else {
      mismatches.push({ code, forkType: type, kafkaType })
    }
  }

  const staleAliases = []
  for (const forkType of Object.keys(aliases)) {
    if (!aliased.some((entry) => entry.forkType === forkType)) {
      staleAliases.push({ forkType, kafkaType: aliases[forkType].kafkaType })
    }
  }

  const byCode = (a, b) => a.code - b.code
  /** Plain ASCII comparator: locale-dependent sorting is forbidden in the generator. */
  const byForkType = (a, b) => (a.forkType < b.forkType ? -1 : a.forkType > b.forkType ? 1 : 0)
  return {
    counts: { kafka: kafka.size, fork: fork.size },
    missingInFork: missingInFork.sort(byCode),
    unknownToKafka: unknownToKafka.sort(byCode),
    mismatches: mismatches.sort(byCode),
    aliased: aliased.sort(byCode),
    staleAliases: staleAliases.sort(byForkType),
  }
}

module.exports = { SUCCESS_CODE, readSnapshot, compareErrorTables }
