/**
 * Generates operator mutants for declared target files, deterministically for a given tree.
 *
 * Each source line yields at most one mutant per operator, in this order:
 * - `early-return`: a function head ending in `{` gains ` return`, so the body is skipped;
 * - `boundary`: the first comparison swaps `<`↔`<=` or `>`↔`>=`;
 * - `negated-condition`: a one-line `if (…)`/`while (…)` condition becomes `!(…)`;
 * - `removed-call`: a statement that is only a call becomes an empty line;
 * - `constant`: the first `true`/`false` flips, or an integer `0`→`1`, `1`→`0`, `n`→`n+1`.
 * Operators read the line with strings and comments masked, so text inside them is never mutated.
 * Every mutant keeps the file's line count.
 *
 * A mutant's witnesses are the specs that must notice it; the first is the record's `spec`, and the
 * record lists all of them in `witnesses` when there are several (the runner kills the mutant when any
 * one fails). A target's default witness is `<dir>/<name>.spec.js` or `<dir>/__tests__/<name>.spec.js`
 * when listed in the area, otherwise the listed spec in the closest directory (list order breaks ties).
 * The area's `witnesses` table refines that per target: a spec or a list of specs replaces the default
 * for the whole file; a map from method name to specs names the guards of those methods, which run in
 * their listed order with the default appended when not listed, and every other method keeps the default alone. A method is the module-level
 * function or class method that holds the mutated line, so the table survives line moves. Every spec the
 * area names must exist, every file a table names must be a generated target and every method it names
 * must hold a mutated line, or generation fails. An area with `methodsOnly` generates, for a target with
 * method rules, only the lines of the methods those rules name; its other targets stay whole.
 *
 * Usage: node scripts/mutants/generate.js [--root <dir>] [--out <file>]   prints or writes `{ records }`
 */
const fs = require('fs')
const path = require('path')

const OFFSET_TESTS = 'src/consumer/offsetManager/__tests__'
const SHARE_TESTS = 'src/consumer/share/__tests__'
const PRODUCER_TESTS = 'src/producer/__tests__'
const PROTOCOL = 'src/protocol'
const REQUESTS = 'src/protocol/requests'
const SYNC_GROUP = 'src/protocol/requests/syncGroup'
const SCHEMA_SPEC = 'scripts/protocol/__tests__/schema.spec.js'
const CLUSTER_TESTS = 'src/cluster/__tests__'
const NETWORK_TESTS = 'src/network/__tests__'
const CONSUMER_TESTS = 'src/consumer/__tests__'
const COMPRESSION = 'src/protocol/message/compression'
const RECORD_BATCH = 'src/protocol/recordBatch/v0'
const AGED_METADATA_SPEC = `${PRODUCER_TESTS}/agedMetadata.spec.js`

/**
 * The risk areas, their broker-free default witness specs, and the `witnesses` table that names, per
 * target, the specs that guard it where the default does not reach (see the file's description).
 * `aged-metadata` is `methodsOnly`: the rest of the cluster and broker pool is not its subject.
 */
const AREAS = [
  {
    name: 'transactions',
    targets: ['src/producer/eosManager/', 'src/producer/state/'],
    specs: [
      'src/producer/eosManager/index.spec.js',
      'src/producer/state/__tests__/producerStateManager.spec.js',
      'src/producer/state/__tests__/accumulator.spec.js',
      'src/producer/state/__tests__/admission.spec.js',
      'src/producer/state/__tests__/errorClassifier.spec.js',
      'src/producer/state/__tests__/joinOrder.spec.js',
      'src/producer/state/__tests__/partitionSequencer.spec.js',
      'src/producer/state/__tests__/producerIdAndEpoch.spec.js',
      'src/producer/state/__tests__/recordSize.spec.js',
      'src/producer/state/__tests__/transactionOffsets.spec.js',
      'src/producer/state/__tests__/transactionState.spec.js',
      'src/producer/state/__tests__/transactionVersion.spec.js',
    ],
    witnesses: {
      'src/producer/eosManager/index.js': {
        createEosManager: [
          'src/producer/eosManager/index.spec.js',
          `${PRODUCER_TESTS}/sendOffsetsPublication.spec.js`,
        ],
        hasPartitions: [
          `${PRODUCER_TESTS}/sendOffsets.spec.js`,
          `${PRODUCER_TESTS}/sendOffsetsPublication.spec.js`,
        ],
      },
      'src/producer/state/accumulator.js': {
        halvesOf: ['src/producer/state/__tests__/subdivision.spec.js'],
      },
      'src/producer/state/partitionSequencer.js': {
        restoreUnsent: [`${PRODUCER_TESTS}/splitSend.spec.js`],
      },
      'src/producer/state/recordSize.js': {
        EMPTY_RECORD: [`${REQUESTS}/produce/produceFraming.spec.js`],
        recordsBytes: [`${REQUESTS}/produce/produceFraming.spec.js`],
      },
    },
  },
  {
    name: 'offsets',
    targets: ['src/cluster/committedOffsetRevisions.js', 'src/consumer/offsetManager/'],
    specs: [
      'src/cluster/__tests__/committedOffsetRevisions.spec.js',
      'src/consumer/offsetManager/__tests__/commitOffsets.spec.js',
      'src/consumer/offsetManager/__tests__/initializeConsumerOffsets.spec.js',
      'src/consumer/offsetManager/__tests__/isInvalidOffset.spec.js',
      'src/consumer/offsetManager/__tests__/offsetOperations.spec.js',
      'src/consumer/offsetManager/__tests__/resetObligations.spec.js',
    ],
    witnesses: {
      'src/cluster/committedOffsetRevisions.js': {
        forCache: [`${OFFSET_TESTS}/privateResetPositions.spec.js`],
        peek: ['src/cluster/__tests__/committedOffsetRetirement.spec.js'],
      },
      'src/consumer/offsetManager/index.js': {
        commitWithOwnership: ['src/consumer/__tests__/lostMeansNoCommit.spec.js'],
        holdResetRefusals: ['src/consumer/__tests__/offsetResetHalt.spec.js'],
        reposition: ['src/consumer/__tests__/logTruncation.spec.js'],
        seek: [`${OFFSET_TESTS}/groupInstanceId.spec.js`],
        resetByDuration: [`${OFFSET_TESTS}/resetSchedules.spec.js`],
        commitAndClear: [`${OFFSET_TESTS}/resetSchedules.spec.js`],
        resolveOffsets: [
          `${OFFSET_TESTS}/localOffsets.spec.js`,
          `${OFFSET_TESTS}/resetSchedules.spec.js`,
        ],
        applyResolution: [
          `${OFFSET_TESTS}/initializeConsumerOffsets.spec.js`,
          `${OFFSET_TESTS}/resetSchedules.spec.js`,
        ],
        seedConsumedEpochs: [`${OFFSET_TESTS}/resolveOffsetsEpoch.spec.js`],
      },
      'src/consumer/offsetManager/offsetOperations.js': {
        bind: ['src/cluster/__tests__/groupRegistry.spec.js'],
        end: ['src/cluster/__tests__/groupRegistry.spec.js'],
        notifyIdle: ['src/cluster/__tests__/groupRegistry.spec.js'],
      },
      'src/consumer/offsetManager/resetObligations.js': {
        claimSubscription: ['src/cluster/__tests__/committedOffsetRetirement.spec.js'],
        clear: [
          'src/cluster/__tests__/groupRegistry.spec.js',
          `${OFFSET_TESTS}/resetSchedules.spec.js`,
        ],
        retain: ['src/cluster/__tests__/committedOffsetRetirement.spec.js'],
      },
    },
  },
  {
    name: 'group-timing',
    targets: ['src/consumer/groupTimeouts.js', 'src/protocol/requests/syncGroup/'],
    specs: [
      'src/consumer/__tests__/groupTimeouts.spec.js',
      'src/protocol/requests/syncGroup/index.spec.js',
      'src/protocol/requests/syncGroup/v0/request.spec.js',
      'src/protocol/requests/syncGroup/v0/response.spec.js',
      'src/protocol/requests/syncGroup/v1/request.spec.js',
      'src/protocol/requests/syncGroup/v1/response.spec.js',
      'src/protocol/requests/syncGroup/v2/request.spec.js',
      'src/protocol/requests/syncGroup/v2/response.spec.js',
      'src/protocol/requests/syncGroup/v3/request.spec.js',
      'src/protocol/requests/syncGroup/v3/response.spec.js',
      'src/protocol/requests/groupRequests.spec.js',
      'src/protocol/requests/groupResponses.spec.js',
    ],
    witnesses: {
      'src/consumer/groupTimeouts.js': {
        resolveGroupTimeouts: ['src/consumer/__tests__/consumerDefaults.spec.js'],
      },
      [`${SYNC_GROUP}/index.js`]: [
        `${SYNC_GROUP}/index.spec.js`,
        'scripts/compat/__tests__/forkVersions.spec.js',
      ],
      [`${SYNC_GROUP}/v0/request.js`]: [
        `${REQUESTS}/index.spec.js`,
        `${SYNC_GROUP}/v0/request.spec.js`,
      ],
      [`${SYNC_GROUP}/v1/request.js`]: [
        `${REQUESTS}/index.spec.js`,
        `${SYNC_GROUP}/v1/request.spec.js`,
      ],
      [`${SYNC_GROUP}/v2/request.js`]: [
        `${REQUESTS}/index.spec.js`,
        `${SYNC_GROUP}/v2/request.spec.js`,
      ],
      [`${SYNC_GROUP}/v3/request.js`]: [
        `${REQUESTS}/index.spec.js`,
        `${SYNC_GROUP}/v3/request.spec.js`,
      ],
      [`${SYNC_GROUP}/v4/request.js`]: [
        `${REQUESTS}/groupRequests.spec.js`,
        `${REQUESTS}/index.spec.js`,
        SCHEMA_SPEC,
      ],
      [`${SYNC_GROUP}/v5/request.js`]: [
        `${REQUESTS}/groupRequests.spec.js`,
        `${REQUESTS}/index.spec.js`,
        SCHEMA_SPEC,
      ],
      [`${SYNC_GROUP}/v4/response.js`]: [`${REQUESTS}/groupResponses.spec.js`, SCHEMA_SPEC],
      [`${SYNC_GROUP}/v5/response.js`]: [`${REQUESTS}/groupResponses.spec.js`, SCHEMA_SPEC],
    },
  },
  {
    name: 'share',
    targets: ['src/consumer/share/shareRunner.js', 'src/consumer/share/shareRun.js'],
    specs: [`${SHARE_TESTS}/shareConsumerClose.spec.js`],
    witnesses: {
      'src/consumer/share/shareRun.js': {
        afterFetch: [`${SHARE_TESTS}/shareConsumerSubscription.spec.js`],
        assignmentChanged: [
          `${SHARE_TESTS}/shareConsumerEvents.spec.js`,
          `${SHARE_TESTS}/shareConsumerEventMatrix.spec.js`,
        ],
        captureCloseCoordinator: [`${SHARE_TESTS}/shareConsumerCoordinatorClose.spec.js`],
        close: [
          `${SHARE_TESTS}/shareConsumerShutdown.spec.js`,
          `${SHARE_TESTS}/shareConsumerCloseRevocation.spec.js`,
        ],
        dispatchBatch: [`${SHARE_TESTS}/shareConsumerModes.spec.js`],
        escalate: [`${SHARE_TESTS}/shareConsumerShutdown.spec.js`],
        freeze: [
          `${SHARE_TESTS}/shareConsumerDialRelease.spec.js`,
          `${SHARE_TESTS}/shareConsumerEvents.spec.js`,
        ],
        leave: [`${SHARE_TESTS}/shareConsumerCoordinatorClose.spec.js`],
        onMove: [`${SHARE_TESTS}/shareSessionFaults.spec.js`],
        publishCommitted: [
          `${SHARE_TESTS}/shareConsumerConnect.spec.js`,
          `${SHARE_TESTS}/shareConsumerEvents.spec.js`,
        ],
        releaseFrom: [`${SHARE_TESTS}/shareConsumerModes.spec.js`],
        retireLookups: [`${SHARE_TESTS}/shareConsumerCoordinatorClose.spec.js`],
      },
      'src/consumer/share/shareRunner.js': {
        connect: [`${SHARE_TESTS}/shareConsumerConnect.spec.js`],
        disconnect: ['src/telemetry/__tests__/close.spec.js'],
        parseDisconnectTimeout: [`${SHARE_TESTS}/shareConsumerOptions.spec.js`],
        readTelemetryResources: ['src/telemetry/__tests__/factories.spec.js'],
        run: [`${SHARE_TESTS}/shareConsumerConnect.spec.js`],
        superviseCrash: [
          `${SHARE_TESTS}/shareConsumerConnect.spec.js`,
          `${SHARE_TESTS}/shareConsumerModes.spec.js`,
        ],
      },
    },
  },
  {
    name: 'protocol',
    targets: [
      'src/protocol/decoder.js',
      'src/protocol/encoder.js',
      'src/protocol/requests/index.js',
    ],
    specs: [
      'src/protocol/decoder.spec.js',
      'src/protocol/encoder.spec.js',
      'src/protocol/requests/index.spec.js',
    ],
    witnesses: {
      'src/protocol/decoder.js': {
        assertCount: [`${PROTOCOL}/decoderCountBounds.spec.js`],
        readArray: [`${PROTOCOL}/decoderCountBounds.spec.js`],
        readInt32: [`${PROTOCOL}/decoder.fuzz.spec.js`],
        readInt64: [`${PROTOCOL}/decoder.spec.js`, 'src/__tests__/equivalentApis.spec.js'],
        readTaggedFields: [`${PROTOCOL}/taggedFields.spec.js`],
        readUVarInt: [`${PROTOCOL}/encoder.spec.js`],
        readUVarIntBytes: [`${PROTOCOL}/compactPrimitives.spec.js`, `${PROTOCOL}/encoder.spec.js`],
        readUuid: [`${PROTOCOL}/uuid.spec.js`],
        readVarIntArrayBytesBounded: [`${PROTOCOL}/decoderHeaderArray.spec.js`],
      },
      'src/protocol/encoder.js': {
        writeInt8: [`${PROTOCOL}/compactPrimitives.spec.js`],
        writeNullableArray: [`${REQUESTS}/metadata/v1/request.spec.js`],
        writeUuid: [`${PROTOCOL}/uuid.spec.js`],
        writeUVarIntBytes: [`${PROTOCOL}/compactPrimitives.spec.js`],
        writeVarIntBytes: [
          `${PROTOCOL}/recordBatch/record/v0/decoderEquality.spec.js`,
          `${REQUESTS}/produce/produceFraming.spec.js`,
        ],
      },
    },
  },
  {
    name: 'producer-send',
    targets: [
      'src/producer/index.js',
      'src/producer/messageProducer.js',
      'src/producer/sendMessages.js',
    ],
    specs: ['src/producer/sendMessages.spec.js', `${PRODUCER_TESTS}/messageProducer.spec.js`],
    witnesses: {
      'src/producer/index.js': [
        `${PRODUCER_TESTS}/drainLoop.spec.js`,
        `${PRODUCER_TESTS}/deliveryBudget.spec.js`,
        `${PRODUCER_TESTS}/closeContract.spec.js`,
        `${PRODUCER_TESTS}/producerOptions.spec.js`,
      ],
      'src/producer/messageProducer.js': [
        `${PRODUCER_TESTS}/messageProducer.spec.js`,
        `${PRODUCER_TESTS}/partitionerSeam.spec.js`,
        `${PRODUCER_TESTS}/deliveryBudget.spec.js`,
      ],
      'src/producer/sendMessages.js': [
        'src/producer/sendMessages.spec.js',
        `${PRODUCER_TESTS}/drainLoop.spec.js`,
        `${PRODUCER_TESTS}/splitSend.spec.js`,
        `${PRODUCER_TESTS}/leaderHint.spec.js`,
      ],
    },
  },
  {
    name: 'network',
    targets: ['src/network/'],
    specs: [
      `${NETWORK_TESTS}/connectionLifecycle.spec.js`,
      'src/network/connectionPool.spec.js',
      `${NETWORK_TESTS}/connectionSetup.spec.js`,
      'src/network/requestQueue/index.spec.js',
      'src/network/requestQueue/socketRequest.spec.js',
    ],
    witnesses: {
      'src/network/connection.js': [
        `${NETWORK_TESTS}/connectionLifecycle.spec.js`,
        `${NETWORK_TESTS}/connectionSendOrder.spec.js`,
        `${NETWORK_TESTS}/connectionBranches.spec.js`,
        `${NETWORK_TESTS}/requestTimeoutClose.spec.js`,
      ],
      'src/network/connectionFailure.js': [
        `${NETWORK_TESTS}/connectionLifecycle.spec.js`,
        `${NETWORK_TESTS}/thrownValues.spec.js`,
      ],
      'src/network/connectionPool.js': [
        'src/network/connectionPool.spec.js',
        `${NETWORK_TESTS}/idleReaping.spec.js`,
        `${NETWORK_TESTS}/obsoleteConnectionBound.spec.js`,
        `${NETWORK_TESTS}/shareConnectionPool.spec.js`,
      ],
      'src/network/connectionSetup.js': [
        `${NETWORK_TESTS}/connectionSetup.spec.js`,
        `${NETWORK_TESTS}/connectionSetupEdges.spec.js`,
        `${NETWORK_TESTS}/connectionSetupPolicy.spec.js`,
      ],
      'src/network/describeThrown.js': [`${NETWORK_TESTS}/thrownValues.spec.js`],
      'src/network/frameBuffer.js': [
        `${NETWORK_TESTS}/frameValidation.spec.js`,
        `${NETWORK_TESTS}/flexibleFraming.spec.js`,
      ],
      'src/network/requestQueue/index.js': [
        'src/network/requestQueue/index.spec.js',
        'src/network/requestQueue/ownership.spec.js',
        'src/network/requestQueue/idleScheduling.spec.js',
        'src/network/requestQueue/scopeRelease.spec.js',
      ],
      'src/network/requestQueue/socketRequest.js': [
        'src/network/requestQueue/socketRequest.spec.js',
        `${NETWORK_TESTS}/telemetryObservation.spec.js`,
      ],
      'src/network/socket.js': [
        `${NETWORK_TESTS}/disconnectClosedSocket.spec.js`,
        `${NETWORK_TESTS}/connectionLifecycle.spec.js`,
      ],
      'src/network/socketFactory.js': [
        `${NETWORK_TESTS}/tlsNegative.spec.js`,
        `${NETWORK_TESTS}/dnsAddresses.spec.js`,
      ],
    },
  },
  {
    name: 'fetch-loop',
    targets: [
      'src/consumer/batch.js',
      'src/consumer/fetchManager.js',
      'src/consumer/fetcher.js',
      'src/consumer/runner.js',
    ],
    specs: [
      `${CONSUMER_TESTS}/batch.spec.js`,
      'src/consumer/fetchManager.spec.js',
      'src/consumer/fetcher.spec.js',
      `${CONSUMER_TESTS}/runner.spec.js`,
    ],
    witnesses: {
      'src/consumer/fetchManager.js': [
        'src/consumer/fetchManager.spec.js',
        `${CONSUMER_TESTS}/fetchManagerReconcile.spec.js`,
        `${CONSUMER_TESTS}/fetchManagerStop.spec.js`,
        `${CONSUMER_TESTS}/fetchManagerErrorIsolation.spec.js`,
      ],
      'src/consumer/fetcher.js': [
        'src/consumer/fetcher.spec.js',
        `${CONSUMER_TESTS}/fetcherPhases.spec.js`,
        `${CONSUMER_TESTS}/fetcherRecreations.spec.js`,
      ],
      'src/consumer/runner.js': [
        `${CONSUMER_TESTS}/runner.spec.js`,
        `${CONSUMER_TESTS}/crash.spec.js`,
        `${CONSUMER_TESTS}/boundedClose.spec.js`,
      ],
    },
  },
  {
    name: 'compression',
    targets: [`${COMPRESSION}/`, `${RECORD_BATCH}/decoder.js`, `${RECORD_BATCH}/index.js`],
    specs: [
      `${COMPRESSION}/compressionLevels.spec.js`,
      `${COMPRESSION}/lz4.spec.js`,
      `${COMPRESSION}/snappy.spec.js`,
      `${COMPRESSION}/zstd.spec.js`,
    ],
    witnesses: {
      [`${COMPRESSION}/index.js`]: [
        `${COMPRESSION}/compressionLevels.spec.js`,
        `${COMPRESSION}/customCodec.spec.js`,
        `${COMPRESSION}/codecBombs.spec.js`,
        `${COMPRESSION}/decompressionBound.spec.js`,
      ],
      [`${RECORD_BATCH}/decoder.js`]: [
        `${COMPRESSION}/decompressionBound.spec.js`,
        `${RECORD_BATCH}/compressedCount.spec.js`,
        `${RECORD_BATCH}/recordBounds.spec.js`,
        `${RECORD_BATCH}/crcValidation.spec.js`,
      ],
      [`${RECORD_BATCH}/index.js`]: [
        `${COMPRESSION}/compressionLevels.spec.js`,
        `${RECORD_BATCH}/compressedCount.spec.js`,
        `${RECORD_BATCH}/crcValidation.spec.js`,
      ],
    },
  },
  {
    name: 'aged-metadata',
    targets: ['src/cluster/brokerPool.js', 'src/cluster/index.js'],
    specs: [AGED_METADATA_SPEC],
    methodsOnly: true,
    witnesses: {
      'src/cluster/brokerPool.js': {
        disconnect: [AGED_METADATA_SPEC, `${CLUSTER_TESTS}/disconnectDrains.spec.js`],
        metadataState: [AGED_METADATA_SPEC, `${CLUSTER_TESTS}/consumerTopicIsolation.spec.js`],
        refreshMetadata: [AGED_METADATA_SPEC],
        refreshMetadataIfNecessary: [
          AGED_METADATA_SPEC,
          `${CLUSTER_TESTS}/consumerTopicIsolation.spec.js`,
        ],
        refreshMetadataUnshared: [
          AGED_METADATA_SPEC,
          `${CLUSTER_TESTS}/metadataCacheWiring.spec.js`,
          `${CLUSTER_TESTS}/obsoleteDrain.spec.js`,
          `${CLUSTER_TESTS}/consumerTopicIsolation.spec.js`,
        ],
      },
      'src/cluster/index.js': {
        refreshMetadataIfNecessary: [AGED_METADATA_SPEC],
        serveFromCache: [AGED_METADATA_SPEC],
      },
    },
  },
  {
    name: 'cluster',
    targets: ['src/cluster/bootstrapResolution.js'],
    specs: ['src/cluster/__tests__/bootstrapResolution.spec.js'],
  },
  {
    name: 'admin',
    targets: ['src/admin/deadline.js'],
    specs: ['src/admin/__tests__/apiDeadline.spec.js'],
    witnesses: {
      'src/admin/deadline.js': {
        check: ['src/admin/__tests__/deadlineScopes.spec.js'],
        expired: ['src/admin/__tests__/deadlineScopes.spec.js'],
      },
    },
  },
]

const BOUNDARY = { '<': '<=', '<=': '<', '>': '>=', '>=': '>' }
const KEYWORD_HEAD = /^(?:\}\s*)?(?:if|else|for|while|switch|catch|do|try|finally|return|class)\b/
const CALL_STATEMENT = /^(?:await\s+)?[\w$#]+(?:\??\.[\w$#]+)*\s*\(.*\)$/
const CLASS_HEAD = /^(?:module\.exports\s*=\s*)?class\b/
const SYMBOL_HEAD = /^ {2}(?:(?:async|static|get|set)\s+|\*)*([\w$#]+)\s*[(=]/
const NOT_A_CALL = /^(?:return|throw|new|typeof|delete|void|if|for|while|switch)\b/

/**
 * Masks string, template and regex literals with `_` and comments with spaces, keeping every
 * character's position. Block comments and templates may span lines.
 * @param {string} text a whole source file
 * @returns {string} the masked file, the same length
 */
const mask = (text) => {
  let out = ''
  let i = 0
  let code = ''
  while (i < text.length) {
    const c = text[i]
    const next = text[i + 1]
    let end = -1
    let fill = '_'
    if (c === '/' && next === '/') {
      end = text.indexOf('\n', i)
      if (end === -1) end = text.length
      fill = ' '
    } else if (c === '/' && next === '*') {
      end = text.indexOf('*/', i + 2)
      end = end === -1 ? text.length : end + 2
      fill = ' '
    } else if (c === "'" || c === '"' || c === '`') {
      end = i + 1
      const lineEnd = c === '`' ? null : '\n'
      while (end < text.length && text[end] !== c && text[end] !== lineEnd)
        end += text[end] === '\\' ? 2 : 1
      end = Math.min(end + 1, text.length)
    } else if (c === '/' && /(?:^|[(,=:[!&|?{};]|\breturn)\s*$/.test(code)) {
      end = i + 1
      let inClass = false
      while (end < text.length && text[end] !== '\n' && (inClass || text[end] !== '/')) {
        if (text[end] === '\\') end++
        else if (text[end] === '[') inClass = true
        else if (text[end] === ']') inClass = false
        end++
      }
      end = Math.min(end + 1, text.length)
    }
    if (end === -1) {
      out += c
      code = c === '\n' ? '' : code + c
      i++
    } else {
      out += text.slice(i, end).replace(/[^\n]/g, fill)
      code = text.slice(i, end).includes('\n') ? '' : code + ' '
      i = end
    }
  }
  return out
}

/**
 * @param {string} masked a masked line
 * @param {number} open the index of an opening parenthesis
 * @returns {number} the index of its closing parenthesis on the line, or -1
 */
const closingParen = (masked, open) => {
  let depth = 0
  for (let i = open; i < masked.length; i++) {
    if (masked[i] === '(') depth++
    else if (masked[i] === ')' && --depth === 0) return i
  }
  return -1
}

/**
 * Lists the mutants of one line.
 * @param {string} line the source line
 * @param {string} masked the same line, masked
 * @param {string} previous the previous non-blank masked line, trimmed
 * @param {string} following the next non-blank masked line, trimmed
 * @returns {{ operator: string, mutated: string }[]} in operator order
 */
const lineMutants = (line, masked, previous, following) => {
  const out = []
  const body = masked.trim()
  if (body === '') return out
  const at = (index, length, text) => line.slice(0, index) + text + line.slice(index + length)

  const isHead =
    body.endsWith('{') &&
    !KEYWORD_HEAD.test(body) &&
    /(?:\bfunction\b.*\)|\)\s*=>|^(?:async\s+)?(?:static\s+)?(?:get\s+|set\s+)?\*?[\w$#]+\s*\(.*\))\s*\{$/.test(
      body
    )
  if (isHead) out.push({ operator: 'early-return', mutated: `${line.trimEnd()} return` })

  const comparison = /(?<![<>=!-])(<=|>=|<|>)(?![<>=])/.exec(masked)
  if (comparison)
    out.push({
      operator: 'boundary',
      mutated: at(comparison.index, comparison[1].length, BOUNDARY[comparison[1]]),
    })

  const condition = /\b(?:if|while)\s*\(/.exec(masked)
  if (condition) {
    const open = condition.index + condition[0].length - 1
    const close = closingParen(masked, open)
    if (close !== -1)
      out.push({
        operator: 'negated-condition',
        mutated: `${line.slice(0, open + 1)}!(${line.slice(open + 1, close)})${line.slice(close)}`,
      })
  }

  const balanced = (body.match(/\(/g) || []).length === (body.match(/\)/g) || []).length
  const continues = /(?:[=([,+\-*/?:&|.>]|\breturn)$/.test(previous) || /^\??\./.test(following)
  const bareCondition = /^(?:\}\s*)?(?:(?:else\s+)?if|for|while)\b.*\)$|^(?:\}\s*)?else$/.test(
    previous
  )
  if (
    CALL_STATEMENT.test(body) &&
    !NOT_A_CALL.test(body) &&
    balanced &&
    !continues &&
    !bareCondition
  )
    out.push({ operator: 'removed-call', mutated: '' })

  const constant = /\b(?:true|false)\b|(?<![\w$.])\d+(?![\w$.])/.exec(masked)
  if (constant) {
    const value = constant[0]
    const swapped =
      value === 'true'
        ? 'false'
        : value === 'false'
          ? 'true'
          : value === '0'
            ? '1'
            : value === '1'
              ? '0'
              : String(BigInt(value) + 1n)
    out.push({ operator: 'constant', mutated: at(constant.index, value.length, swapped) })
  }
  return out.filter((m) => m.mutated !== line)
}

/**
 * @param {string} root
 * @param {string} target a file or a directory (trailing `/`) relative to `root`
 * @returns {string[]} the source files it names, sorted; specs, `__tests__` and fixtures excluded
 */
const expand = (root, target) => {
  if (!target.endsWith('/')) return [target]
  return fs
    .readdirSync(path.join(root, target), { recursive: true })
    .map((f) => path.posix.join(target, f.split(path.sep).join('/')))
    .filter(
      (f) =>
        f.endsWith('.js') &&
        !f.endsWith('.spec.js') &&
        !/(^|\/)(__tests__|fixtures)\//.test(f) &&
        fs.statSync(path.join(root, f)).isFile()
    )
    .sort()
}

/**
 * Names what holds a line: the module-level function, class or constant declaration at column 0, or,
 * inside a class, the nearest method head two spaces in. Closures inside them belong to them.
 * @param {string[]} masked the masked lines of a file
 * @param {number} index the zero-based line
 * @returns {string} the name, or an empty string for any other module-level statement
 */
const enclosingSymbol = (masked, index) => {
  let method = ''
  for (let i = index; i >= 0; i--) {
    const line = masked[i]
    const opens = /(?:[{(]|=>.*)$/.test(line.trimEnd())
    if (CLASS_HEAD.test(line)) return method
    if (/^\S/.test(line)) {
      const declared = /^(?:async\s+)?(?:function\*?|const|let|var)\s+([\w$#]+)/.exec(line)
      if (declared) return declared[1]
      // `_` opens a masked line inside a multi-line template literal; a closer that opens a body continues a head
      if (!(/^[})\]]/.test(line) && opens) && !line.startsWith('_')) return ''
    } else {
      const head = SYMBOL_HEAD.exec(line)
      if (head && opens && !KEYWORD_HEAD.test(line.trim())) method = method || head[1]
    }
  }
  return method
}

/**
 * Picks a target's default witness from the area's specs.
 * @param {string} target
 * @param {{ specs: string[] }} area
 * @returns {string}
 */
const witnessFor = (target, { specs }) => {
  const dir = path.posix.dirname(target)
  const name = path.posix.basename(target, '.js')
  const exact = specs.find(
    (s) => s === `${dir}/${name}.spec.js` || s === `${dir}/__tests__/${name}.spec.js`
  )
  if (exact) return exact
  const shared = (s) => {
    const a = dir.split('/')
    const b = path.posix.dirname(s).split('/')
    let n = 0
    while (n < a.length && a[n] === b[n]) n++
    return n
  }
  return specs.reduce((best, s) => (shared(s) > shared(best) ? s : best), specs[0])
}

/**
 * @param {{ witnesses?: object }} area
 * @param {string} target
 * @returns {boolean} whether the area names guard specs per method for the target
 */
const hasMethodRules = (area, target) => {
  const entry = area.witnesses?.[target]
  return typeof entry === 'object' && !Array.isArray(entry)
}

/**
 * Picks a mutant's witnesses from the area: a whole-file entry of the `witnesses` table as it stands;
 * otherwise the guards the table names for the method in their listed order, then the target's default
 * unless they list it already.
 * @param {string} target
 * @param {string} method the method that holds the mutated line ('' when not looked up)
 * @param {{ specs: string[], witnesses?: Record<string, string|string[]|Record<string, string[]>> }} area
 * @returns {string[]} one or more specs, the first being the record's `spec`
 */
const witnessesFor = (target, method, area) => {
  const entry = area.witnesses?.[target]
  if (typeof entry === 'string') return [entry]
  if (Array.isArray(entry)) return [...entry]
  const guards = entry && Object.hasOwn(entry, method) ? entry[method] : []
  return [...new Set([...guards, witnessFor(target, area)])]
}

/**
 * @param {{ specs: string[], witnesses?: object }} area
 * @returns {string[]} every spec the area names, in its default list and in its `witnesses` table
 */
const specsNamed = ({ specs, witnesses = {} }) => [
  ...specs,
  ...Object.values(witnesses).flatMap((entry) =>
    typeof entry === 'object' && !Array.isArray(entry) ? Object.values(entry).flat() : entry
  ),
]

/**
 * Generates the mutants of every area's targets.
 * @param {string} root the tree
 * @param {{ name: string, targets: string[], specs: string[], witnesses?: object, methodsOnly?: boolean }[]} [areas]
 * @returns {object[]} runner records, ordered by area, file and line
 * @throws {Error} when an area names a witness spec that is not in the tree, a rule names a file that
 *   is not a generated target, or a method rule matches no method of its target
 */
const generate = (root, areas = AREAS) => {
  const records = []
  for (const area of areas) {
    for (const spec of specsNamed(area))
      if (!fs.existsSync(path.join(root, spec)))
        throw new Error(`mutants: the witness spec ${spec} of area ${area.name} does not exist`)
    const targets = area.targets.flatMap((t) => expand(root, t))
    for (const ruled of Object.keys(area.witnesses || {}))
      if (!targets.includes(ruled))
        throw new Error(
          `mutants: the witness rule for ${ruled} of area ${area.name} names no generated target`
        )
    for (const target of targets) {
      const text = fs.readFileSync(path.join(root, target), 'utf8')
      const lines = text.split('\n')
      const masked = mask(text).split('\n')
      const byMethod = hasMethodRules(area, target)
      const symbols = new Set()
      const neighbour = (from, step) => {
        for (let i = from + step; i >= 0 && i < masked.length; i += step)
          if (masked[i].trim() !== '') return masked[i].trim()
        return ''
      }
      lines.forEach((line, i) => {
        const mutants = lineMutants(line, masked[i], neighbour(i, -1), neighbour(i, 1))
        if (mutants.length === 0) return
        const method = byMethod ? enclosingSymbol(masked, i) : ''
        if (area.methodsOnly && byMethod && !Object.hasOwn(area.witnesses[target], method)) return
        symbols.add(method)
        const witnesses = witnessesFor(target, method, area)
        for (const m of mutants) {
          records.push({
            id: `${area.name}/${target}:${i + 1}:${m.operator}`,
            area: area.name,
            target,
            line: i + 1,
            operator: m.operator,
            original: line,
            mutated: m.mutated,
            spec: witnesses[0],
            ...(witnesses.length > 1 && { witnesses }),
            title: '',
            commandClass: 'unit',
          })
        }
      })
      if (byMethod)
        for (const key of Object.keys(area.witnesses[target]))
          if (!symbols.has(key))
            throw new Error(`mutants: the witness rule ${key} of ${target} matches no method`)
    }
  }
  return records
}

module.exports = { generate, AREAS }

if (require.main === module) {
  const args = process.argv.slice(2)
  const option = (name) => {
    const at = args.indexOf(name)
    return at === -1 ? undefined : args[at + 1]
  }
  const text = JSON.stringify({ records: generate(path.resolve(option('--root') || '.')) }, null, 2)
  const out = option('--out')
  if (out) fs.writeFileSync(out, text + '\n')
  else process.stdout.write(text + '\n')
}
