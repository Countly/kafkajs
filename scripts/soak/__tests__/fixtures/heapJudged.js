'use strict'
/**
 * Writes three real heap snapshots of this process, `<dir>/<slot>.h00`, `.h01` and `.h02`, and
 * between the last two grows one thing:
 *
 *   code     about 7 MiB of bytecode and feedback, by calling functions whose source was loaded
 *            before the first snapshot
 *   map      3 MiB of entries in the `topics` Map of a product `MetadataCache`
 *   array    3 MiB of objects in an array of this harness file
 *   strings  3 MiB of distinct strings appended to a `URLSearchParams` (node internals)
 *   seed     3 MiB of entries in a Map shaped as the soak seed's retained offset map
 *            (`<partition>:<offset>` to the offset string, `finalSoakWorkers.js`)
 *
 *   node scripts/soak/__tests__/fixtures/heapJudged.js <dir> <slot> <case>
 */
const fs = require('node:fs')
const path = require('node:path')
const v8 = require('node:v8')
const vm = require('node:vm')
const MetadataCache = require('../../../../src/cluster/metadataCache')

const [dir, slot, kind] = process.argv.slice(2)
const MIB = 1024 * 1024
const FUNCTIONS = 800

/** @param {number} i @returns {string} the source of one function with a long body */
const body = (i) => {
  let s = `function f${i}(x){`
  for (let k = 0; k < 60; k++) s += `x=(x*${k + 3}+${i})|0;if(x>${k * 7})x-=${k};`
  return `${s}return x}`
}

/** One retained element of the harness array. */
class HarnessMarker {
  /** @param {number} i */
  constructor(i) {
    this.index = i
    this.label = `marker-${i}`.padEnd(48, '.')
  }
}

const kept = { functions: [], cache: null, harness: [], params: null, retained: new Map() }
if (kind === 'code')
  kept.functions = vm.runInThisContext(`[${Array.from({ length: FUNCTIONS }, (_, i) => body(i))}]`)
if (kind === 'map') kept.cache = new MetadataCache({ logger: { namespace: () => ({}) } })
if (kind === 'strings') kept.params = new URLSearchParams()

/** Grows the chosen thing once. */
const grow = {
  code: () => {
    let x = 0
    for (let round = 0; round < 20; round++) for (const f of kept.functions) x = f(x + round)
    return x
  },
  map: () => {
    for (let i = 0; kept.cache.topics.size * 360 < 3 * MIB; i++)
      kept.cache.topics.set(`topic-${i}`.padEnd(64, '-'), {
        topicId: `id-${i}`.padEnd(40, '0'),
        seenAt: i,
        decodedReleased: false,
        partitions: null,
      })
  },
  array: () => {
    for (let i = 0; kept.harness.length * 270 < 3 * MIB; i++)
      kept.harness.push(new HarnessMarker(i))
  },
  seed: () => {
    for (let offset = 0; kept.retained.size * 100 < 3 * MIB; offset++) {
      const message = { offset: String(offset) }
      kept.retained.set(`${offset % 12}:${message.offset}`, String(message.offset))
    }
  },
  strings: () => {
    for (let i = 0; i * 340 < 3 * MIB; i++)
      kept.params.append(`k${i}`, `value-${i}`.padEnd(100, 'v'))
  },
}

const write = (boundary) => v8.writeHeapSnapshot(path.join(dir, `${slot}.${boundary}.heapsnapshot`))
fs.mkdirSync(dir, { recursive: true })
write('h00')
write('h01')
grow[kind]()
write('h02')
