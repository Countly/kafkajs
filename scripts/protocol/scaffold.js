'use strict'

/**
 * Emits codec skeletons from the committed Apache message-definition snapshot
 * (`scripts/compat/messages/<tag>/`). A skeleton has the field list in wire order for one
 * concrete version, the version's classic-vs-compact writers/readers already chosen, nested
 * struct encoders/decoders, a tag-buffer call at every struct end when the version is
 * flexible, a JSDoc block documenting the wire shape, and the `schema` export the validator
 * (`npm run protocol:check`) reads.
 *
 * It never overwrites a file: a hand-finished codec is a one-way trip. The engineer who runs
 * `scaffold` completes the semantic mapping afterwards (option names, defaults, which fields
 * the client actually sends, `parse()` error handling).
 *
 * Zero dependencies beyond `scripts/compat/parse`, CommonJS, Node >= 22.
 */

const fs = require('fs')
const path = require('path')
const { execFileSync } = require('child_process')
const { readSpec, parseVersionRange } = require('../compat/parse')
const { kafkaTag } = require('./table')
const { keyOfMessage } = require('./apiNames')

const COMPAT_ROOT = path.join(__dirname, '..', 'compat')
const REPO_ROOT = path.join(__dirname, '..', '..')
const PROTOCOL_URL = 'https://kafka.apache.org/43/design/protocol'
const PRETTIER_BIN = require.resolve('prettier/bin/prettier.cjs')

const SCALAR_INT = /^int(8|16|32|64)$/

/**
 * Runs the project's own Prettier (its committed `.prettierrc`, invoked as a real
 * subprocess) over emitted source instead of hand-reproducing its layout rules. Prettier
 * collapses a short method chain onto one line regardless of how many calls it holds, so an
 * emitter that always breaks a 2+-call chain across lines is wrong for some field
 * combinations even when it is right for others — formatting through Prettier itself makes
 * every emitted file correct by construction, and free of drift if the project's Prettier
 * config ever changes.
 * @param {string} source
 * @param {string} targetPath the path Prettier resolves its parser/config relative to
 * @returns {string} the Prettier-formatted source
 * @throws {Error} if `source` is not valid JavaScript (surfaces Prettier's own parse error)
 */
const formatWithPrettier = (source, targetPath) =>
  execFileSync(process.execPath, [PRETTIER_BIN, '--stdin-filepath', targetPath], {
    input: source,
    cwd: REPO_ROOT,
    encoding: 'utf8',
  })

/** @param {string} name @returns {string} `name` with its first character lowercased */
const camel = (name) => name.charAt(0).toLowerCase() + name.slice(1)

/** @param {string} fieldPath dotted field path, e.g. `Topics.Partitions` @returns {string} last segment */
const lastSegment = (fieldPath) => fieldPath.slice(fieldPath.lastIndexOf('.') + 1)

/** @param {string} fieldPath @returns {string} the path with its dots removed, for helper names */
const pathNoDots = (fieldPath) => fieldPath.replace(/\./g, '')

/**
 * @param {{min: number, max: number|null}|null} range
 * @param {number} version
 * @returns {boolean} whether `version` falls inside the closed (or open-ended) range
 */
const rangeContains = (range, version) =>
  range !== null && version >= range.min && (range.max === null || version <= range.max)

/**
 * Rebuilds the field tree that `scripts/compat/parse#flattenFields` flattened, using each
 * row's dotted path to recover nesting depth. Fields arrive in DFS pre-order, so a single
 * pass with a depth-indexed stack is enough.
 * @param {{path: string}[]} fields
 * @returns {{row: object, children: object[], isArray: boolean}[]} top-level nodes
 */
const buildForest = (fields) => {
  const roots = []
  const stack = []
  for (const row of fields) {
    const depth = row.path.split('.').length - 1
    const node = { row, children: [], isArray: row.type.startsWith('[]') }
    while (stack.length > depth) stack.pop()
    if (stack.length === 0) {
      roots.push(node)
    } else {
      stack[stack.length - 1].children.push(node)
    }
    stack.push(node)
  }
  return roots
}

/**
 * Parses one message-definition file for one API version and renders a request or response
 * codec skeleton for it. One instance renders exactly one file: parse the spec, resolve the
 * fields that survive at this version, then emit.
 */
class CodecScaffolder {
  /**
   * @param {string} apiName Apache message name without `Request`/`Response`, e.g. `ApiVersions`
   * @param {number} version the concrete API version to render
   * @param {'request'|'response'} direction
   * @throws {Error} when the message definition is missing or `version` is outside `validVersions`
   */
  constructor(apiName, version, direction) {
    if (direction !== 'request' && direction !== 'response') {
      throw new Error(`scaffold: direction must be 'request' or 'response', got ${direction}`)
    }

    this.apiName = apiName
    this.version = version
    this.direction = direction
    this.tag = kafkaTag()
    this.suffix = direction === 'request' ? 'Request' : 'Response'
    this.spec = this.readMessageSpec()

    if (!rangeContains(this.spec.validVersions, version)) {
      throw new Error(
        `${apiName} ${direction} version ${version} is outside validVersions ` +
          `${JSON.stringify(this.spec.validVersions)}`
      )
    }

    this.flexible = this.spec.flexibleFrom !== null && version >= this.spec.flexibleFrom
    this.fields = this.spec.fields.filter((row) =>
      rangeContains(parseVersionRange(row.versions), version)
    )
    this.forest = buildForest(this.fields)
  }

  /** @returns {object} the parsed message spec (see `scripts/compat/parse#parseSpec`) */
  readMessageSpec() {
    const file = path.join(COMPAT_ROOT, 'messages', this.tag, `${this.apiName}${this.suffix}.json`)
    if (!fs.existsSync(file)) {
      throw new Error(`No message definition for ${this.apiName} (looked for ${file})`)
    }
    return readSpec(file)
  }

  /**
   * @param {object} row a field row (from `spec.fields`)
   * @returns {boolean} whether the field is nullable at this instance's version
   */
  isNullable(row) {
    return rangeContains(parseVersionRange(row.nullableVersions), this.version)
  }

  /**
   * @returns {string} the full skeleton source text, formatted through the project's Prettier
   */
  render() {
    const source = this.direction === 'request' ? this.renderRequest() : this.renderResponse()
    const targetPath = path.join(
      'src',
      'protocol',
      'requests',
      camel(this.apiName),
      `v${this.version}`,
      `${this.direction}.js`
    )
    return formatWithPrettier(source, targetPath)
  }

  // --- JSDoc -----------------------------------------------------------------------------

  /**
   * @param {string} type a JSON scalar type (never an array type)
   * @param {boolean} compact
   * @returns {string} the Apache protocol wire-type name, e.g. `COMPACT_STRING`
   */
  scalarWireLabel(type, compact) {
    if (type === 'bool') return 'BOOLEAN'
    if (SCALAR_INT.test(type)) return type.toUpperCase()
    if (type === 'uuid') return 'UUID'
    if (type === 'string') return compact ? 'COMPACT_STRING' : 'STRING'
    if (type === 'bytes' || type === 'records') return compact ? 'COMPACT_BYTES' : 'BYTES'
    return type.toUpperCase()
  }

  /**
   * @param {{row: object, children: object[], isArray: boolean}} node
   * @returns {string} the wire-type label shown in the JSDoc for this field
   */
  wireType(node) {
    const compact = this.flexible
    if (!node.isArray) return this.scalarWireLabel(node.row.type, compact)

    const base = compact ? 'COMPACT_ARRAY' : 'ARRAY'
    if (node.children.length === 0) {
      return `${base} of ${this.scalarWireLabel(node.row.type.slice(2), compact)}`
    }
    return base
  }

  /**
   * Renders one struct's JSDoc lines: each non-tagged field (recursing into its own children),
   * then a `TAG_BUFFER` line when the version is flexible, then the struct's own tagged fields
   * — one line each, never recursed into, since this client neither sends nor decodes them.
   * @param {object[]} nodes direct children of the struct being documented
   * @param {number} level nesting depth, for indentation (two spaces per level)
   * @returns {string[]} doc lines, without the leading ` * `
   */
  docLines(nodes, level) {
    const indent = '  '.repeat(level)
    const nonTagged = nodes.filter((node) => node.row.tag === null)
    const tagged = nodes.filter((node) => node.row.tag !== null)
    const lines = []

    for (const node of nonTagged) {
      lines.push(
        `${indent}${lastSegment(node.row.path)} => ${this.wireType(node)} (${node.row.versions})`
      )
      if (node.children.length > 0) {
        lines.push(...this.docLines(node.children, level + 1))
      }
    }

    if (this.flexible) {
      lines.push(`${indent}TAG_BUFFER`)
    }

    const tagIndent = `${indent}  `
    for (const node of tagged) {
      lines.push(
        `${tagIndent}tag ${node.row.tag} ${lastSegment(node.row.path)} => ${this.wireType(node)} ` +
          `(${node.row.versions})`
      )
    }

    return lines
  }

  /** @returns {string} the full `/** ... *\/` JSDoc block, unterminated by a trailing newline */
  renderDoc() {
    const flexSuffix = this.flexible ? ' — flexible (KIP-482)' : ''
    const title = `${this.apiName}${this.suffix} (Version: ${this.version})${flexSuffix}`
    const body = this.docLines(this.forest, 0)

    return [
      '/**',
      ` * ${title}`,
      ' *',
      ...body.map((line) => ` * ${line}`),
      ' *',
      ` * @see ${PROTOCOL_URL}`,
      ` * @see clients/src/main/resources/common/message/${this.apiName}${this.suffix}.json ` +
        `(Apache Kafka ${this.tag})`,
      ' */',
    ].join('\n')
  }

  // --- schema ------------------------------------------------------------------------------

  /** @returns {string} the `module.exports.schema = { ... }` block */
  renderSchema() {
    const fieldLines = this.fields.map((row) => {
      const parts = [`name: '${row.path}'`, `type: '${row.type}'`, `versions: '${row.versions}'`]
      if (this.isNullable(row)) parts.push('nullable: true')
      if (row.tag !== null) parts.push(`tag: ${row.tag}`)
      return `    { ${parts.join(', ')} },`
    })

    return [
      'module.exports.schema = {',
      `  api: '${this.apiName}',`,
      `  version: ${this.version},`,
      `  direction: '${this.direction}',`,
      '  fields: [',
      ...fieldLines,
      '  ],',
      '}',
    ].join('\n')
  }

  // --- shared field-level writer/reader mapping ---------------------------------------------

  /**
   * @param {string} type a JSON scalar type (never an array type)
   * @param {string} valueExpr the JS expression holding the field's value
   * @returns {string} an `Encoder` method call, e.g. `writeInt32(foo)`
   * @throws {Error} for a type this scaffolder does not know how to encode
   */
  scalarWriterCall(type, valueExpr) {
    const compact = this.flexible
    if (type === 'bool') return `writeBoolean(${valueExpr})`
    const int = SCALAR_INT.exec(type)
    if (int) return `writeInt${int[1]}(${valueExpr})`
    if (type === 'uuid') return `writeUuid(${valueExpr})`
    if (type === 'string') {
      return compact ? `writeUVarIntString(${valueExpr})` : `writeString(${valueExpr})`
    }
    if (type === 'bytes' || type === 'records') {
      return compact ? `writeUVarIntBytes(${valueExpr})` : `writeBytes(${valueExpr})`
    }
    throw new Error(`scaffold: unsupported field type '${type}'`)
  }

  /**
   * @param {{row: object, isArray: boolean}} node
   * @param {string} valueExpr the JS expression holding the field's value
   * @returns {string} an `Encoder` method call for the whole field, array or scalar
   */
  buildWriterCall(node, valueExpr) {
    if (!node.isArray) return this.scalarWriterCall(node.row.type, valueExpr)

    const compact = this.flexible
    const helperName = `encode${pathNoDots(node.row.path)}`
    const mapExpr = `${valueExpr}.map(${helperName})`
    const nullable = this.isNullable(node.row)

    if (compact) {
      return nullable
        ? `writeUVarIntArray(${valueExpr} == null || ${valueExpr}.length === 0 ? null : ${mapExpr})`
        : `writeUVarIntArray(${mapExpr})`
    }
    return nullable ? `writeNullableArray(${mapExpr})` : `writeArray(${mapExpr})`
  }

  /**
   * @param {string} type a JSON scalar type (never an array type)
   * @returns {string} a `Decoder` method call, e.g. `decoder.readInt32()`
   * @throws {Error} for a type this scaffolder does not know how to decode
   */
  scalarReaderExpr(type) {
    const compact = this.flexible
    if (type === 'bool') return 'decoder.readBoolean()'
    const int = SCALAR_INT.exec(type)
    if (int) return `decoder.readInt${int[1]}()`
    if (type === 'uuid') return 'decoder.readUuid()'
    if (type === 'string') {
      return compact ? 'decoder.readUVarIntString()' : 'decoder.readString()'
    }
    if (type === 'bytes' || type === 'records') {
      return compact ? 'decoder.readUVarIntBytes()' : 'decoder.readBytes()'
    }
    throw new Error(`scaffold: unsupported field type '${type}'`)
  }

  /**
   * @param {{row: object, isArray: boolean}} node
   * @returns {string} a `Decoder` read expression for the whole field, array or scalar
   */
  buildReaderExpr(node) {
    if (!node.isArray) return this.scalarReaderExpr(node.row.type)

    const compact = this.flexible
    const helperName = `decode${pathNoDots(node.row.path)}`
    if (!compact) return `decoder.readArray(${helperName})`

    const base = `decoder.readUVarIntArray(${helperName})`
    return this.isNullable(node.row) ? `${base} || []` : base
  }

  /**
   * Collects, in pre-order, every array-typed field that needs its own `encode`/`decode`
   * helper — tagged fields are skipped entirely, since this client neither sends nor decodes
   * their content.
   * @param {object[]} nodes
   * @param {object[]} [out]
   * @returns {object[]} array nodes, outermost first
   */
  collectArrayNodes(nodes, out = []) {
    for (const node of nodes) {
      if (node.row.tag !== null) continue
      if (node.isArray) out.push(node)
      if (node.children.length > 0) this.collectArrayNodes(node.children, out)
    }
    return out
  }

  // --- request -----------------------------------------------------------------------------

  /** @returns {string} the encode chain's method calls, each prefixed with `.` */
  renderEncodeCalls() {
    const calls = this.forest
      .filter((node) => node.row.tag === null)
      .map((node) => `.${this.buildWriterCall(node, camel(lastSegment(node.row.path)))}`)
    if (this.flexible) calls.push('.writeTaggedFields()')
    return calls
  }

  /** @returns {string} the `module.exports = (...) => ({ ... })` block */
  renderMainExport() {
    const nonTagged = this.forest.filter((node) => node.row.tag === null)
    const names = nonTagged.map((node) => camel(lastSegment(node.row.path)))
    const paramsExpr = names.length > 0 ? `{ ${names.join(', ')} }` : ''
    const calls = this.renderEncodeCalls()

    let bodyLines
    if (calls.length === 0) {
      bodyLines = ['    return new Encoder()']
    } else if (calls.length === 1) {
      bodyLines = [`    return new Encoder()${calls[0]}`]
    } else {
      bodyLines = ['    return new Encoder()', ...calls.map((call) => `      ${call}`)]
    }

    return [
      `module.exports = (${paramsExpr}) => ({`,
      '  apiKey,',
      `  apiVersion: ${this.version},`,
      `  apiName: '${keyOfMessage(this.apiName)}',`,
      '  encode: async () => {',
      ...bodyLines,
      '  },',
      '})',
    ].join('\n')
  }

  /**
   * @param {{row: object, children: object[]}} node an array-typed field
   * @returns {string} the `const encodeFoo = ... => { ... }` helper for that field
   */
  renderEncodeHelper(node) {
    const helperName = `encode${pathNoDots(node.row.path)}`

    if (node.children.length === 0) {
      const paramName = camel(lastSegment(node.row.path))
      const call = this.scalarWriterCall(node.row.type.slice(2), paramName)
      return [
        `const ${helperName} = (${paramName}) => {`,
        `  return new Encoder().${call}`,
        '}',
      ].join('\n')
    }

    const children = node.children.filter((child) => child.row.tag === null)
    const names = children.map((child) => camel(lastSegment(child.row.path)))
    const params = names.length > 0 ? `{ ${names.join(', ')} }` : ''
    const calls = children.map(
      (child) => `.${this.buildWriterCall(child, camel(lastSegment(child.row.path)))}`
    )
    if (this.flexible) calls.push('.writeTaggedFields()')

    const bodyLines =
      calls.length === 1
        ? [`  return new Encoder()${calls[0]}`]
        : ['  return new Encoder()', ...calls.map((call) => `    ${call}`)]

    return [`const ${helperName} = (${params}) => {`, ...bodyLines, '}'].join('\n')
  }

  /** @returns {string} the full request codec source */
  renderRequest() {
    const header = [
      "const Encoder = require('../../../encoder')",
      `const { ${keyOfMessage(this.apiName)}: apiKey } = require('../../apiKeys')`,
    ].join('\n')

    const doc = this.renderDoc()
    const bodyParts = [
      this.renderMainExport(),
      ...this.collectArrayNodes(this.forest).map((node) => this.renderEncodeHelper(node)),
      this.renderSchema(),
    ]

    return assembleFile(header, doc, bodyParts)
  }

  // --- response ----------------------------------------------------------------------------

  /** @returns {string} the `const decode = async (rawData) => { ... }` function */
  renderDecodeFn() {
    const nonTagged = this.forest.filter((node) => node.row.tag === null)
    const lines = ['  const decoder = new Decoder(rawData)']
    const names = []

    for (const node of nonTagged) {
      const name = camel(lastSegment(node.row.path))
      names.push(name)
      lines.push(`  const ${name} = ${this.buildReaderExpr(node)}`)
    }

    if (this.flexible) {
      lines.push('  const taggedFields = decoder.readTaggedFields()')
      names.push('taggedFields')
    }

    return [
      'const decode = async (rawData) => {',
      ...lines,
      '',
      '  return {',
      ...names.map((name) => `    ${name},`),
      '  }',
      '}',
    ].join('\n')
  }

  /**
   * @param {{row: object, children: object[]}} node an array-typed field
   * @returns {string} the `const decodeFoo = (decoder) => { ... }` helper for that field
   */
  renderDecodeHelper(node) {
    const helperName = `decode${pathNoDots(node.row.path)}`

    if (node.children.length === 0) {
      const expr = this.scalarReaderExpr(node.row.type.slice(2))
      return [`const ${helperName} = (decoder) => {`, `  return ${expr}`, '}'].join('\n')
    }

    const varName = camel(lastSegment(node.row.path))
    const children = node.children.filter((child) => child.row.tag === null)
    const propLines = children.map(
      (child) => `    ${camel(lastSegment(child.row.path))}: ${this.buildReaderExpr(child)},`
    )

    const lines = [
      `const ${helperName} = (decoder) => {`,
      `  const ${varName} = {`,
      ...propLines,
      '  }',
    ]
    if (this.flexible) {
      lines.push('', '  decoder.readTaggedFields()')
    }
    lines.push(`  return ${varName}`, '}')

    return lines.join('\n')
  }

  /** @returns {string} the full response codec source */
  renderResponse() {
    const header = "const Decoder = require('../../../decoder')"
    const doc = this.renderDoc()
    const parseFn = ['const parse = async (data) => {', '  return data', '}'].join('\n')
    const exportsBlock = ['module.exports = {', '  decode,', '  parse,', '}'].join('\n')

    const bodyParts = [
      ...this.collectArrayNodes(this.forest).map((node) => this.renderDecodeHelper(node)),
      this.renderDecodeFn(),
      parseFn,
      exportsBlock,
      this.renderSchema(),
    ]

    return assembleFile(header, doc, bodyParts)
  }
}

/**
 * Joins the requires, the JSDoc block (attached with no blank line to whatever follows it —
 * standard JSDoc placement) and the remaining sections (each separated by one blank line).
 * @param {string} header the `require(...)` lines
 * @param {string} doc the JSDoc block
 * @param {string[]} bodyParts sections after the doc, in order; must be non-empty
 * @returns {string} the full file text, ending in exactly one newline
 */
const assembleFile = (header, doc, bodyParts) => {
  const sections = [header, `${doc}\n${bodyParts[0]}`, ...bodyParts.slice(1)]
  return `${sections.join('\n\n')}\n`
}

/**
 * @param {string} apiName Apache message name without `Request`/`Response`, e.g. `ApiVersions`
 * @param {number} version the concrete API version to render
 * @param {'request'|'response'} direction
 * @returns {string} the codec skeleton source text
 * @throws {Error} when the message definition is missing or `version` is outside `validVersions`
 */
const scaffold = (apiName, version, direction) =>
  new CodecScaffolder(apiName, version, direction).render()

/**
 * Writes `<repoRoot>/src/protocol/requests/<api>/v<version>/{request,response}.js`, creating
 * the directory as needed. Never overwrites an existing file.
 * @param {string} apiName Apache message name without `Request`/`Response`, e.g. `ApiVersions`
 * @param {number} version the concrete API version to render
 * @param {string} [repoRoot] defaults to this repository; a test may point it at a temp dir
 * @returns {string[]} the two file paths written, request then response
 * @throws {Error} when either target file already exists, or `scaffold` itself throws
 */
const write = (apiName, version, repoRoot = REPO_ROOT) => {
  const outDir = path.join(repoRoot, 'src', 'protocol', 'requests', camel(apiName), `v${version}`)
  const targets = ['request', 'response'].map((direction) => ({
    direction,
    file: path.join(outDir, `${direction}.js`),
  }))

  for (const { file } of targets) {
    if (fs.existsSync(file)) {
      throw new Error(`${file} already exists — scaffold never overwrites a hand-finished codec`)
    }
  }

  fs.mkdirSync(outDir, { recursive: true })
  return targets.map(({ direction, file }) => {
    fs.writeFileSync(file, scaffold(apiName, version, direction))
    return file
  })
}

module.exports = { scaffold, write, CodecScaffolder }
