const path = require('path')
const { spawnSync } = require('child_process')
const Decoder = require('../../../src/protocol/decoder')
const { ARMS, OPTIONS, parseArgs, prepare } = require('../arrayDecode')

const SCRIPT = path.join(__dirname, '..', 'arrayDecode.js')

/**
 * Runs `run` with `Decoder.prototype[reader]` wrapped, so the widest array that reader returns
 * while it runs is known. Both sync and async readers are covered.
 * @param {string} reader a `Decoder` method name
 * @param {() => Promise<*>} run
 * @returns {Promise<{ calls: number, widest: number }>}
 */
const watchReader = async (reader, run) => {
  const original = Decoder.prototype[reader]
  let calls = 0
  let widest = 0
  const note = (array) => {
    widest = Math.max(widest, array ? array.length : 0)
    return array
  }
  Decoder.prototype[reader] = function wrapped(...args) {
    calls++
    const out = original.apply(this, args)
    return typeof out?.then === 'function' ? out.then(note) : note(out)
  }

  try {
    await run()
  } finally {
    Decoder.prototype[reader] = original
  }

  return { calls, widest }
}

describe('Benchmarks > arrayDecode', () => {
  it('has one arm per Decoder array reader', () => {
    expect(Object.keys(ARMS)).toEqual([
      'readArray',
      'readVarIntArray',
      'readUVarIntArray',
      'readArrayAsync',
      'readUVarIntArrayAsync',
    ])
  })

  test.each(Object.keys(ARMS))(
    'the %s arm decodes a real response whose widest array goes through that reader',
    async (name) => {
      const { raw, arm } = await prepare(name)

      const { calls, widest } = await watchReader(name, () => arm.decode(raw, OPTIONS))

      expect(calls).toBeGreaterThan(0)
      expect(widest).toBe(arm.widest)
      expect(arm.widest).toBeGreaterThanOrEqual(1000)
      expect(arm.elements).toBeGreaterThanOrEqual(2000)
      expect(arm.count(await arm.decode(raw, OPTIONS))).toBe(arm.elements)
    }
  )

  it('refuses an arm whose response does not decode to the claimed shape', async () => {
    const original = ARMS.readArray.elements
    ARMS.readArray.elements = original + 1
    try {
      await expect(prepare('readArray')).rejects.toThrow(/decoded 2000 elements, expected 2001/)
    } finally {
      ARMS.readArray.elements = original
    }
  })

  it('parses arm and iteration arguments strictly', () => {
    expect(parseArgs(['--arm', 'readArray'])).toEqual({ arm: 'readArray', iterations: 300 })
    expect(parseArgs(['--arm', 'readVarIntArray', '--iterations', '50'])).toEqual({
      arm: 'readVarIntArray',
      iterations: 50,
    })

    for (const argv of [
      [],
      ['--iterations', '50'],
      ['--arm', 'readNothing'],
      ['--arm', 'toString'],
      ['--arm', 'readArray', '--iterations', '3'],
      ['--arm', 'readArray', '--iterations', 'many'],
      ['--arm', 'readArray', '--unknown', '1'],
    ]) {
      expect(() => parseArgs(argv)).toThrow(/usage/)
    }
  })

  it('prints one JSON line of numbers for an arm', () => {
    const run = spawnSync(process.execPath, [SCRIPT, '--arm', 'readArray', '--iterations', '5'], {
      encoding: 'utf8',
    })

    expect(run.status).toBe(0)
    expect(JSON.parse(run.stdout)).toMatchObject({
      available: 1,
      arm: 'readArray',
      iterations: 5,
      elements: 2000,
    })
    expect(JSON.parse(run.stdout).nsPerOp).toBeGreaterThan(0)
  })
})
