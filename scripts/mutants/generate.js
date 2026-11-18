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
 * The witness is one spec from the area's list: the area's explicit `witnesses` entry for the target,
 * else `<dir>/<name>.spec.js` or `<dir>/__tests__/<name>.spec.js` when listed, otherwise the listed spec
 * in the closest directory (list order breaks ties).
 *
 * Usage: node scripts/mutants/generate.js [--root <dir>] [--out <file>]   prints or writes `{ records }`
 */
const fs = require('fs')
const path = require('path')

/** The risk areas, their broker-free witness specs and the targets whose witness is named explicitly. */
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
      'src/producer/state/__tests__/partitionSequencer.spec.js',
      'src/producer/state/__tests__/producerIdAndEpoch.spec.js',
      'src/producer/state/__tests__/recordSize.spec.js',
      'src/producer/state/__tests__/transactionOffsets.spec.js',
      'src/producer/state/__tests__/transactionState.spec.js',
      'src/producer/state/__tests__/transactionVersion.spec.js',
    ],
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
      'src/protocol/requests/syncGroup/v4/request.js':
        'src/protocol/requests/groupRequests.spec.js',
      'src/protocol/requests/syncGroup/v5/request.js':
        'src/protocol/requests/groupRequests.spec.js',
      'src/protocol/requests/syncGroup/v4/response.js':
        'src/protocol/requests/groupResponses.spec.js',
      'src/protocol/requests/syncGroup/v5/response.js':
        'src/protocol/requests/groupResponses.spec.js',
    },
  },
  {
    name: 'share',
    targets: ['src/consumer/share/shareRunner.js', 'src/consumer/share/shareRun.js'],
    specs: ['src/consumer/share/__tests__/shareConsumerClose.spec.js'],
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
  },
]

const BOUNDARY = { '<': '<=', '<=': '<', '>': '>=', '>=': '>' }
const KEYWORD_HEAD = /^(?:\}\s*)?(?:if|else|for|while|switch|catch|do|try|finally|return|class)\b/
const CALL_STATEMENT = /^(?:await\s+)?[\w$#]+(?:\??\.[\w$#]+)*\s*\(.*\)$/
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
 * Picks a target's witness from the area's specs.
 * @param {string} target
 * @param {{ specs: string[], witnesses?: Record<string, string> }} area
 * @returns {string}
 */
const witnessFor = (target, { specs, witnesses = {} }) => {
  if (witnesses[target]) return witnesses[target]
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
 * Generates the mutants of every area's targets.
 * @param {string} root the tree
 * @param {{ name: string, targets: string[], specs: string[], witnesses?: Record<string, string> }[]} [areas]
 * @returns {object[]} runner records, ordered by area, file and line
 */
const generate = (root, areas = AREAS) => {
  const records = []
  for (const area of areas) {
    for (const target of area.targets.flatMap((t) => expand(root, t))) {
      const text = fs.readFileSync(path.join(root, target), 'utf8')
      const lines = text.split('\n')
      const masked = mask(text).split('\n')
      const spec = witnessFor(target, area)
      const neighbour = (from, step) => {
        for (let i = from + step; i >= 0 && i < masked.length; i += step)
          if (masked[i].trim() !== '') return masked[i].trim()
        return ''
      }
      lines.forEach((line, i) => {
        for (const m of lineMutants(line, masked[i], neighbour(i, -1), neighbour(i, 1))) {
          records.push({
            id: `${area.name}/${target}:${i + 1}:${m.operator}`,
            area: area.name,
            target,
            line: i + 1,
            operator: m.operator,
            original: line,
            mutated: m.mutated,
            spec,
            title: '',
            commandClass: 'unit',
          })
        }
      })
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
