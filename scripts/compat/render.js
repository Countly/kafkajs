'use strict'

/**
 * Markdown renderer (spec B1). Pure and deterministic: same model in, byte-identical
 * document out. No Date, no locale-sensitive sorting, no environment reads — a single
 * unstable byte here would turn `compat:check` into a false-positive machine (B4).
 */

const { formatVersions } = require('./gaps')

/** Table cells must never break the table: escape pipes, flatten newlines. */
const cell = (value) => {
  const text = value === null || value === undefined ? '' : String(value)
  return text.replace(/\|/g, '\\|').replace(/\r?\n/g, ' ')
}

const row = (cells) => `| ${cells.map(cell).join(' | ')} |`

const range = (bounds) =>
  bounds === null ? '—' : bounds[0] === bounds[1] ? `${bounds[0]}` : `${bounds[0]}-${bounds[1]}`

const gapCell = (api) => {
  const flags = []
  if (api.onFloor) flags.push('on-floor')
  if (api.belowFloor.length > 0) flags.push(`below-floor ${formatVersions(api.belowFloor)}`)
  return flags.length === 0 ? api.gap : `${api.gap} (${flags.join(', ')})`
}

const section = (lines, title) => {
  lines.push('')
  lines.push(`## ${title}`)
  lines.push('')
}

const renderSummary = (lines, model) => {
  section(lines, 'Summary')
  lines.push(
    row(['API', 'Key', `Kafka ${model.kafkaTag}`, 'Flexible from', 'Fork', 'Gap', 'Notes'])
  )
  lines.push('|---|--:|---|---|---|---|---|')
  for (const api of model.apis) {
    lines.push(
      row([
        api.kafkaName,
        api.apiKey,
        range(api.validVersions),
        api.flexibleFrom === null ? 'never' : `v${api.flexibleFrom}+`,
        api.forkVersions === null ? '—' : formatVersions(api.forkVersions),
        gapCell(api),
        api.notes,
      ])
    )
  }
}

const renderFloorWatch = (lines, model) => {
  const sitters = model.apis.filter((api) => api.onFloor)
  section(lines, 'Floor watch')
  lines.push(
    'These APIs implement exactly the minimum version the pinned broker still accepts. The next KIP-896 floor raise breaks them with no warning.'
  )
  lines.push('')
  if (sitters.length === 0) {
    lines.push('None.')
    return
  }
  lines.push(row(['API', 'Key', 'Fork max', `Kafka ${model.kafkaTag} minimum`]))
  lines.push('|---|--:|--:|--:|')
  for (const api of sitters) {
    lines.push(
      row([
        api.kafkaName,
        api.apiKey,
        `v${Math.max.apply(null, api.forkVersions)}`,
        `v${api.validVersions[0]}`,
      ])
    )
  }
}

const renderWaivers = (lines, model) => {
  section(lines, 'Below-floor waivers')
  lines.push(
    'Versions implemented here that the pinned broker rejects outright: dead code, and dangerous while version negotiation ignores the broker minimum. Each is waived until the phase named below deletes it; an unwaived below-floor version fails `yarn compat:check`.'
  )
  lines.push('')
  if (model.waivers.length === 0) {
    lines.push('None — the waiver list is empty.')
    return
  }
  lines.push(row(['API', 'Waived versions', `Kafka ${model.kafkaTag} minimum`, 'Reason', 'Until']))
  lines.push('|---|---|--:|---|---|')
  for (const waiver of model.waivers) {
    lines.push(
      row([
        waiver.api,
        formatVersions(waiver.versions),
        waiver.kafkaMin === null ? '—' : `v${waiver.kafkaMin}`,
        waiver.reason,
        waiver.until,
      ])
    )
  }
}

const renderCrossCutting = (lines, model) => {
  section(lines, 'Cross-cutting gaps')
  lines.push(
    'Not per-API, and not derivable from the message definitions. Each row names the phase that closes it, so this table doubles as a burn-down.'
  )
  lines.push('')
  lines.push(row(['Gap', 'Where', 'Impact', 'Owner']))
  lines.push('|---|---|---|---|')
  for (const gap of model.crossCutting) {
    lines.push(row([gap.gap, gap.where, gap.impact, gap.owner]))
  }
}

const renderErrors = (lines, model) => {
  const errors = model.errors
  section(lines, 'Error table')
  lines.push(
    `\`src/protocol/error.js\` defines ${errors.counts.fork} codes; Kafka ${model.kafkaTag} defines ${errors.counts.kafka} (excluding \`NONE\`, which is the success sentinel). Retriable flags are hand-set here and are **not** checked: \`RetriableException\` inheritance is not visible in \`Errors.java\`, so this table says nothing about whether a retriable flag is right.`
  )
  lines.push('')
  lines.push(
    `- Defined upstream, missing here: **${errors.missingInFork.length}** (reported, not a failure).`
  )
  lines.push(
    `- Defined here, unknown to Kafka ${model.kafkaTag}: **${errors.unknownToKafka.length}** (a failure).`
  )
  lines.push(
    `- Type-name mismatches on a shared code: **${errors.mismatches.length}** (a failure), plus **${errors.aliased.length}** accepted historical aliases.`
  )
  lines.push('')
  if (errors.missingInFork.length > 0) {
    lines.push('Missing codes:')
    lines.push('')
    lines.push(row(['Code', 'Kafka type']))
    lines.push('|--:|---|')
    for (const error of errors.missingInFork) lines.push(row([error.code, error.type]))
    lines.push('')
  }
  if (errors.aliased.length > 0) {
    lines.push('Accepted aliases (the KafkaJS name is public API and is deliberately kept):')
    lines.push('')
    lines.push(row(['Code', 'This client', `Kafka ${model.kafkaTag}`, 'Why']))
    lines.push('|--:|---|---|---|')
    for (const alias of errors.aliased) {
      lines.push(row([alias.code, alias.forkType, alias.kafkaType, alias.reason]))
    }
  }
}

const renderUntracked = (lines, model) => {
  section(lines, 'Untracked API keys')
  lines.push(
    'Keys this client declares but deliberately does not track in the matrix: they are broker-internal, ZooKeeper-era, or not planned.'
  )
  lines.push('')
  lines.push(row(['API', 'Key', 'apiKeys.js name', 'Why untracked']))
  lines.push('|---|--:|---|---|')
  for (const api of model.untracked) {
    lines.push(row([api.kafkaName, api.apiKey, api.apiKeysName, api.reason]))
  }
}

const render = (model) => {
  const lines = []
  lines.push('# Kafka protocol compatibility')
  lines.push('')
  lines.push(
    `Generated from the committed Apache Kafka **${model.kafkaTag}** message definitions in \`scripts/compat/messages/${model.kafkaTag}/\`. **Do not edit by hand** — run \`yarn compat:generate\`; \`yarn compat:check\` fails when this file and the generator disagree.`
  )
  lines.push('')
  lines.push(
    'Gap classes: `current` (fork max equals the Kafka max) · `behind:N` · `stub` (the key is known but unimplemented) · `missing` (planned, no codec) · `not-planned`. `on-floor` and `below-floor` are reported alongside the class, not instead of it.'
  )
  renderSummary(lines, model)
  renderFloorWatch(lines, model)
  renderWaivers(lines, model)
  renderCrossCutting(lines, model)
  renderErrors(lines, model)
  renderUntracked(lines, model)
  lines.push('')
  return lines.join('\n')
}

module.exports = { render, cell, row, range, gapCell }
