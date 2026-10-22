const fs = require('fs')
const path = require('path')
const ts = require('typescript')
const kafkajs = require('../../index')

const typesFile = path.join(__dirname, '..', '..', 'types', 'index.d.ts')

/** @returns {ts.SourceFile} `types/index.d.ts`, parsed with the compiler's own parser */
const parseTypes = () =>
  ts.createSourceFile(typesFile, fs.readFileSync(typesFile, 'utf8'), ts.ScriptTarget.Latest)

/**
 * Reads the enums `types/index.d.ts` declares.
 * @returns {Record<string, Record<string, number|string>>} member values by enum name
 * @throws {Error} for an initializer that is not a plain non-negative number or a string literal
 */
const declaredEnums = () => {
  const enums = {}
  for (const node of parseTypes().statements.filter(ts.isEnumDeclaration)) {
    let next = 0
    enums[node.name.text] = Object.fromEntries(
      node.members.map(({ name, initializer }) => {
        if (initializer && ts.isStringLiteral(initializer)) return [name.text, initializer.text]
        if (initializer && !ts.isNumericLiteral(initializer)) {
          throw new Error(
            `${node.name.text}.${name.text} has an initializer this check cannot read`
          )
        }
        if (initializer) next = Number(initializer.text)
        return [name.text, next++]
      })
    )
  }
  return enums
}

/**
 * Reads the names `types/index.d.ts` exports as values: a class, an enum, a function or a variable.
 * @returns {Set<string>}
 */
const declaredValueNames = () => {
  const names = new Set()
  const isExported = (node) =>
    node.modifiers?.some(({ kind }) => kind === ts.SyntaxKind.ExportKeyword)
  for (const node of parseTypes().statements.filter(isExported)) {
    if (ts.isVariableStatement(node)) {
      node.declarationList.declarations.forEach(({ name }) => names.add(name.text))
    } else if (
      ts.isClassDeclaration(node) ||
      ts.isEnumDeclaration(node) ||
      ts.isFunctionDeclaration(node)
    ) {
      names.add(node.name.text)
    }
  }
  return names
}

describe('types/index.d.ts enums', () => {
  const declared = declaredEnums()
  const isConstants = (value) =>
    value &&
    typeof value === 'object' &&
    Object.values(value).length > 0 &&
    Object.values(value).every((member) => typeof member === 'number')
  const constants = Object.entries(kafkajs)
    .filter(([, value]) => isConstants(value))
    .map(([name]) => name)

  it('declares an enum for every constant object the package exports', () => {
    expect(constants).toEqual(
      expect.arrayContaining(['AclResourceTypes', 'AlterConfigOpTypes', 'ConfigTypes'])
    )
    expect(constants.filter((name) => !(name in declared))).toEqual([])
  })

  it('exports every enum the types declare', () => {
    expect(Object.keys(declared).filter((name) => !(name in kafkajs))).toEqual([])
  })

  it.each(constants)('declares %s with the values it has at run time', (name) => {
    expect(declared[name]).toEqual({ ...kafkajs[name] })
  })
})

describe('types/index.d.ts exports', () => {
  it('declares every name the package root exports', () => {
    const exported = Object.keys(kafkajs)
    const declared = declaredValueNames()

    expect(exported).toEqual(expect.arrayContaining(['Kafka', 'KafkaJSError', 'logLevel']))
    expect(exported.filter((name) => !declared.has(name))).toEqual([])
  })
})
