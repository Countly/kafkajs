const v8 = require('node:v8')

/**
 * Reads diagnostic-only counters synchronously after an original control reading. Does not collect,
 * settle, mark or alter any judged quantity. Heap/space/code/memory sizes are bytes; maxRSS is the
 * raw process.resourceUsage() counter in KiB, separate from the gate's peak sources.
 * @returns {object} V8 space capacity/use/availability/physical bytes, heap capacity/allocator
 *   counters, all code and process-memory counters, and raw maxRSS
 */
module.exports = function controlDiagnostics() {
  const heapSpaces = v8.getHeapSpaceStatistics()
  const heap = v8.getHeapStatistics()
  return {
    heapSpaces,
    heap: {
      total_heap_size: heap.total_heap_size,
      total_physical_size: heap.total_physical_size,
      malloced_memory: heap.malloced_memory,
      peak_malloced_memory: heap.peak_malloced_memory,
      external_memory: heap.external_memory,
    },
    code: v8.getHeapCodeStatistics(),
    memory: process.memoryUsage(),
    maxRSS: process.resourceUsage().maxRSS,
  }
}
