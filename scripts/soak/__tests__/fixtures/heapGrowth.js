'use strict'
/**
 * Writes `<dir>/heap/p03.h00.heapsnapshot`, then retains a known array of `GrowthMarker`
 * instances through a closure of this file and writes `<dir>/heap/p03.h01.heapsnapshot`.
 *
 *   node scripts/soak/__tests__/fixtures/heapGrowth.js <dir> <markers>
 */
const fs = require('node:fs')
const path = require('node:path')
const v8 = require('node:v8')

/** One retained element of the known array. */
class GrowthMarker {
  /** @param {number} index */
  constructor(index) {
    this.index = index
    this.label = `marker-${index}`
  }
}

const kept = []
/** @param {number} n @returns {void} appends `n` markers to the known array */
globalThis.retainMarkers = (n) => {
  for (let i = 0; i < n; i++) kept.push(new GrowthMarker(i))
}

const dir = path.join(process.argv[2], 'heap')
fs.mkdirSync(dir, { recursive: true })
v8.writeHeapSnapshot(path.join(dir, 'p03.h00.heapsnapshot'))
globalThis.retainMarkers(Number(process.argv[3]))
v8.writeHeapSnapshot(path.join(dir, 'p03.h01.heapsnapshot'))
