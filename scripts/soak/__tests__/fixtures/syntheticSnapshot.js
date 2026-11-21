'use strict'
/**
 * Writes a minimal V8 heap snapshot file in the real file format: a synthetic root with one
 * element edge to each given node, so every node is reachable.
 */
const fs = require('node:fs')
const path = require('node:path')

/** V8's node types, in V8's order, plus one type V8 does not have. */
const NODE_TYPES = [
  'hidden',
  'array',
  'string',
  'object',
  'code',
  'closure',
  'regexp',
  'number',
  'native',
  'synthetic',
  'concatenated string',
  'sliced string',
  'symbol',
  'bigint',
  'object shape',
  'brand new type',
]

/**
 * @param {string} file the snapshot path
 * @param {{ type: string, name: string, bytes: number }[]} nodes the nodes below the root
 * @returns {string} the file
 */
function writeSyntheticSnapshot(file, nodes) {
  const strings = ['', ...new Set(nodes.map((n) => n.name))]
  const all = [{ type: 'synthetic', name: '', bytes: 0 }, ...nodes]
  const flat = all.flatMap((n, i) => [
    NODE_TYPES.indexOf(n.type),
    strings.indexOf(n.name),
    i * 2 + 1,
    n.bytes,
    i === 0 ? nodes.length : 0,
    0,
  ])
  const edges = nodes.flatMap((_, i) => [1, i + 1, (i + 1) * 6])
  const snapshot = {
    meta: {
      node_fields: ['type', 'name', 'id', 'self_size', 'edge_count', 'detachedness'],
      node_types: [NODE_TYPES, 'string', 'number', 'number', 'number', 'number'],
      edge_fields: ['type', 'name_or_index', 'to_node'],
      edge_types: [
        ['context', 'element', 'property', 'internal', 'hidden', 'shortcut', 'weak'],
        'string_or_number',
        'node',
      ],
    },
    node_count: all.length,
    edge_count: nodes.length,
  }
  fs.mkdirSync(path.dirname(file), { recursive: true })
  fs.writeFileSync(file, JSON.stringify({ snapshot, nodes: flat, edges, strings }))
  return file
}

module.exports = { writeSyntheticSnapshot, NODE_TYPES }
