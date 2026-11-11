const fs = require('fs')
const net = require('net')
const path = require('path')
const { isBuiltin } = require('module')
const { Linter } = require('eslint')
const { projectBrokerTools } = require('../captureBrokerTools')
const { capture } = require('../wireFeatures')
const {
  API_RANGES,
  API_ENTRIES,
  brokerBlock,
  featuresText,
  featureRow,
  wireObject,
  RAW,
  projectorInput,
} = require('./fixtures/brokerToolFixtures')

const WIRE_SCRIPT = path.join(__dirname, '..', 'wireFeatures.js')

/**
 * Rejects dependency specifiers outside Node's built-ins without executing the source.
 * @param {string} source JavaScript to scan
 * @returns {void}
 * @throws {Error} for a client dependency or a non-literal specifier
 */
const assertBuiltinDependencies = (source) => {
  const messages = new Linter().verify(source, {
    languageOptions: { ecmaVersion: 2023, sourceType: 'module' },
    plugins: {
      capture: {
        rules: {
          builtins: {
            create(context) {
              const check = (node, specifier) => {
                if (
                  specifier?.type !== 'Literal' ||
                  typeof specifier.value !== 'string' ||
                  !isBuiltin(specifier.value)
                ) {
                  context.report({
                    node,
                    message: 'Expected only literal Node built-in dependencies',
                  })
                }
              }
              return {
                CallExpression(node) {
                  if (node.callee.type === 'Identifier' && node.callee.name === 'require') {
                    check(node, node.arguments.length === 1 ? node.arguments[0] : undefined)
                  }
                },
                ImportDeclaration: (node) => check(node, node.source),
                ImportExpression: (node) => check(node, node.source),
                ExportNamedDeclaration: (node) => node.source && check(node, node.source),
                ExportAllDeclaration: (node) => check(node, node.source),
              }
            },
          },
        },
      },
    },
    rules: { 'capture/builtins': 'error' },
  })
  if (messages.length) throw new Error(messages.map(({ message }) => message).join('\n'))
}

/**
 * @param {object} wire a wire object
 * @param {string} field the field to take out
 * @returns {object} a copy of `wire` without `field`
 */
const without = (wire, field) => {
  const copy = { ...wire }
  delete copy[field]
  return copy
}

describe('Independent broker tool captures', () => {
  test('projects supported numeric ranges from every agreeing broker block', () => {
    const { apiRanges, provenance } = projectBrokerTools(projectorInput())

    expect(apiRanges).toEqual(API_RANGES)
    expect(Object.keys(apiRanges)).toEqual(['0', '1', '17', '18', '76', '95'])
    expect(apiRanges).not.toHaveProperty('4')
    expect(apiRanges).not.toHaveProperty('80')
    expect(provenance).toEqual(projectorInput().provenance)
    expect(provenance.captures.map(({ file }) => file)).toEqual([
      RAW.apiVersions,
      RAW.features,
      RAW.wire,
    ])
  })

  test('rejects disagreeing broker blocks and malformed ranges', () => {
    const blocks = (...entries) => entries.map((lines, i) => brokerBlock(i + 1, lines)).join('')
    const refused = (apiVersions) => () => projectBrokerTools(projectorInput({ apiVersions }))
    const produce = (replacement) =>
      API_ENTRIES.map((line) => (line.startsWith('Produce(0)') ? replacement : line))
    const thirdBlock = API_ENTRIES.map((line) =>
      line.replace('4 to 18 [usable: 18]', '4 to 17 [usable: 17]')
    )

    expect(refused(blocks(API_ENTRIES, API_ENTRIES, thirdBlock))).toThrow(/disagree.*kafka3/)

    for (const [entries, reason] of [
      [produce('Produce(0): 9 to 3 [usable: 3]'), /minimum above/],
      [produce('Produce(0): 3 to x'), /malformed/],
      [produce('Produce(0): 0 to 40000'), /out of range/],
      [[...API_ENTRIES, 'Produce(0): 3 to 13 [usable: 13]'], /twice/],
      [[...API_ENTRIES, 'not an api line'], /malformed/],
    ]) {
      expect(refused(blocks(entries))).toThrow(reason)
    }

    expect(
      refused('kafka1:29092 (id: 1 rack: null isFenced: false) -> ERROR: timed out\n')
    ).toThrow(/ERROR/)
    expect(refused('')).toThrow(/no broker block/)
  })

  test('projects finalized feature levels without inventing absent fields', () => {
    const full = projectBrokerTools(projectorInput())
    const expected = without(wireObject(), 'apiRanges')

    expect(full.features).toEqual(expected)
    expect(Object.keys(full.features)).toEqual([
      'correlation',
      'errorCode',
      'apiKeyCount',
      'throttle',
      'shareApiRanges',
      'supportedFeatures',
      'finalizedFeaturesEpoch',
      'finalizedFeatures',
      'tags',
    ])

    const bare = projectBrokerTools(
      projectorInput({ wire: without(wireObject(), 'tags') })
    ).features
    expect(bare).not.toHaveProperty('tags')
    expect(bare).not.toHaveProperty('zkMigrationReady')
    expect(bare.finalizedFeatures).toEqual(wireObject().finalizedFeatures)

    const taken = projectBrokerTools(
      projectorInput({ wire: without(wireObject(), 'finalizedFeaturesEpoch') })
    ).features
    expect(taken.finalizedFeaturesEpoch).toBe('41')

    const real = fs.readFileSync
    const spy = jest
      .spyOn(fs, 'readFileSync')
      .mockImplementation((file, ...rest) =>
        String(file).endsWith(path.join('captures', '4.4.0', 'apiVersions.json'))
          ? JSON.stringify(projectBrokerTools(projectorInput()))
          : real(file, ...rest)
      )
    try {
      const { readBrokerCapture } = require('../../../testHelpers/brokerCapture')
      expect(readBrokerCapture('4.4.0')).toEqual(projectBrokerTools(projectorInput()))
    } finally {
      spy.mockRestore()
    }
  })

  test('accepts an advancing finalized-features epoch and projects the wire epoch', () => {
    for (const [toolEpoch, wireEpoch] of [
      ['183', '184'],
      ['9', '10'],
      ['9007199254740992', '9007199254740993'],
    ]) {
      const { features } = projectBrokerTools(
        projectorInput({
          features: featuresText(toolEpoch),
          wire: { ...wireObject(), finalizedFeaturesEpoch: wireEpoch },
        })
      )

      expect(features.finalizedFeaturesEpoch).toBe(wireEpoch)
    }
  })

  test('rejects a finalized-features epoch that moves backwards from tool to wire', () => {
    for (const [toolEpoch, wireEpoch] of [
      ['185', '184'],
      ['9007199254740993', '9007199254740992'],
    ]) {
      expect(() =>
        projectBrokerTools(
          projectorInput({
            features: featuresText(toolEpoch),
            wire: { ...wireObject(), finalizedFeaturesEpoch: wireEpoch },
          })
        )
      ).toThrow(
        new RegExp(`finalized-features epoch disagree: tool ${toolEpoch}, wire ${wireEpoch}`)
      )
    }
  })

  test('accepts equal finalized-features epochs', () => {
    const { features } = projectBrokerTools(
      projectorInput({
        features: featuresText('184'),
        wire: { ...wireObject(), finalizedFeaturesEpoch: '184' },
      })
    )

    expect(features.finalizedFeaturesEpoch).toBe('184')
  })

  test('rejects epoch comparisons without timestamps proving the tool was captured first', () => {
    for (const toolEpoch of ['183', '184']) {
      for (const capturedOn of ['2026-11-12T10:00:04Z', '2026-11-12T10:00:05Z']) {
        const input = projectorInput({
          features: featuresText(toolEpoch),
          wire: { ...wireObject(), finalizedFeaturesEpoch: '184' },
        })
        input.provenance.captures.find(({ file }) => file === RAW.features).capturedOn = capturedOn

        expect(() => projectBrokerTools(input)).toThrow(/capture order.*tool.*before.*wire/)
      }
    }
  })

  test('rejects missing provenance and a raw digest mismatch', () => {
    const input = projectorInput()
    expect(() => projectBrokerTools({ ...input, provenance: undefined })).toThrow(/provenance/)

    expect(() =>
      projectBrokerTools({ ...input, provenance: without(input.provenance, 'brokerCommitId') })
    ).toThrow(/brokerCommitId/)
    expect(() =>
      projectBrokerTools({
        ...input,
        provenance: { ...input.provenance, captures: input.provenance.captures.slice(0, 2) },
      })
    ).toThrow(/all three raw/)

    for (const [field, file] of [
      ['apiVersionsText', RAW.apiVersions],
      ['featuresText', RAW.features],
      ['wireFeaturesLine', RAW.wire],
    ]) {
      expect(() => projectBrokerTools({ ...input, [field]: `${input[field]} ` })).toThrow(
        new RegExp(`digest mismatch.*${file.replace('.', '\\.')}`)
      )
    }
  })

  test('both capture tools load only literal Node built-in dependencies', () => {
    for (const file of [WIRE_SCRIPT, path.join(__dirname, '..', 'captureBrokerTools.js')]) {
      expect(() => assertBuiltinDependencies(fs.readFileSync(file, 'utf8'))).not.toThrow()
    }
    expect(() =>
      assertBuiltinDependencies(`
        const fs = require ('fs')
        import path from 'node:path'
        import 'node:crypto'
        import('node:net')
      `)
    ).not.toThrow()
  })

  test('dependency scan rejects client code and non-literal specifiers', () => {
    for (const source of [
      "require('../../src/protocol/decoder')",
      "import '../../src/protocol/decoder.js'",
      "require ('../../src/protocol/decoder')",
      'require (specifier)',
      'import(specifier)',
      "import Decoder from '../../src/protocol/decoder.js'",
      "import('../../src/protocol/decoder.js')",
      "require('node:' + specifier)",
    ]) {
      expect(() => assertBuiltinDependencies(source)).toThrow(/Node built-in dependencies/)
    }
  })

  test('rejects a wire feature level that disagrees with the feature tool', () => {
    const features = (level, max = 1) =>
      [
        featureRow('group.version', 0, max, level, '41'),
        featureRow('share.version', 0, 2, 2, '41'),
      ].join('\n') + '\n'

    expect(() => projectBrokerTools(projectorInput({ features: features(0) }))).toThrow(
      /group\.version.*finalized level 0.*wire.*1/
    )
    expect(() => projectBrokerTools(projectorInput({ features: features(1, 2) }))).toThrow(
      /group\.version.*supported.*disagree/
    )
    expect(() =>
      projectBrokerTools(
        projectorInput({ features: `${featureRow('x.version', 0, 1, 0, '41')}\n` })
      )
    ).toThrow(/x\.version.*not supported on the wire/)
  })

  test('rejects a capture whose required feature envelope field is missing from every source', () => {
    for (const field of [
      'correlation',
      'errorCode',
      'apiKeyCount',
      'throttle',
      'shareApiRanges',
      'supportedFeatures',
      'finalizedFeatures',
    ]) {
      expect(() =>
        projectBrokerTools(projectorInput({ wire: without(wireObject(), field) }))
      ).toThrow(new RegExp(`${field}.*every source`))
    }

    expect(() =>
      projectBrokerTools(
        projectorInput({
          wire: without(wireObject(), 'finalizedFeaturesEpoch'),
          features: featuresText('-'),
        })
      )
    ).toThrow(/finalizedFeaturesEpoch.*every source/)
  })
})

describe('Independent broker tool captures wire script', () => {
  const name = '776972652d6665617475726573'
  const feature = '73686172652e76657273696f6e'
  /** ApiVersions v4: header v2, correlation 1, client id and software name "wire-features". */
  const REQUEST = `00000029${'0012'}${'0004'}${'00000001'}${'000d'}${name}${'00'}${'0e'}${name}${'0230'}${'00'}`
  /** A response header v0 and an error-free body: two APIs and the three feature tags. */
  const BODY =
    `${'00000001'}${'0000'}${'03'}${'0012'}${'0000'}${'0004'}${'00'}${'004c'}${'0001'}${'0001'}${'00'}` +
    `${'00000000'}${'03'}` +
    `${'00'}${'14'}${'02'}${'0e'}${feature}${'0000'}${'0002'}${'00'}` +
    `${'01'}${'08'}${'0000000000000029'}` +
    `${'02'}${'14'}${'02'}${'0e'}${feature}${'0002'}${'0002'}${'00'}`
  const frame = (hex) =>
    Buffer.from(`${(hex.length / 2).toString(16).padStart(8, '0')}${hex}`, 'hex')

  /**
   * Serves one connection with `reply`, recording the first bytes the script wrote.
   * @param {Buffer} reply
   * @returns {Promise<{ port: number, received: Promise<Buffer>, close: () => void }>}
   */
  const serve = (reply) =>
    new Promise((listening) => {
      let seen
      const received = new Promise((resolve) => {
        seen = resolve
      })
      const server = net.createServer((socket) => {
        socket.once('data', (data) => {
          seen(data)
          socket.end(reply)
        })
      })
      server.listen(0, '127.0.0.1', () =>
        listening({ port: server.address().port, received, close: () => server.close() })
      )
    })

  test('frames ApiVersions v4 and reads the answer', async () => {
    const server = await serve(frame(BODY))
    try {
      const line = await capture({ host: '127.0.0.1', port: server.port })

      expect((await server.received).toString('hex')).toBe(REQUEST)
      expect(line).toEqual({
        correlation: 1,
        errorCode: 0,
        apiKeyCount: 2,
        throttle: 0,
        apiRanges: { 18: [0, 4], 76: [1, 1] },
        shareApiRanges: { 76: [1, 1] },
        supportedFeatures: [{ name: 'share.version', min: 0, max: 2 }],
        finalizedFeaturesEpoch: '41',
        finalizedFeatures: [{ name: 'share.version', maxVersionLevel: 2, minVersionLevel: 2 }],
        tags: [0, 1, 2],
      })
      expect(Object.keys(line)).toEqual([
        'correlation',
        'errorCode',
        'apiKeyCount',
        'throttle',
        'apiRanges',
        'shareApiRanges',
        'supportedFeatures',
        'finalizedFeaturesEpoch',
        'finalizedFeatures',
        'tags',
      ])
    } finally {
      server.close()
    }
  })

  test('refuses an error answer, a short frame and an oversized frame', async () => {
    const refusals = [
      [frame(`${'00000001'}${'0023'}${'01'}${'00000000'}${'00'}`), /error code 35/],
      [Buffer.from('0000001000000001', 'hex'), /closed before the frame was complete/],
      [Buffer.from('7fffffff', 'hex'), /frame size/],
    ]
    for (const [reply, reason] of refusals) {
      const server = await serve(reply)
      try {
        await expect(
          capture({ host: '127.0.0.1', port: server.port, timeoutMs: 2000 })
        ).rejects.toThrow(reason)
      } finally {
        server.close()
      }
    }
  })
})
