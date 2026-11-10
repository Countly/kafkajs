'use strict'

/**
 * Inventories distinct graph backing stores, their ArrayBuffers, aliased views and named paths.
 * Unreachable/conditional owners remain explicit; wrapper death alone proves no native release.
 * @param {object} snapshot analysed graph with ownership labels and optional semantic liveness
 * @returns {object[]} one row per stable store ID; self bytes are counted once
 */
const backingInventory = ({ graph, labels, semantic }) => {
  const stores = new Set()
  const byId = new Map()
  for (let v = 0; v < graph.n; v++) {
    const node = graph.describe(v)
    byId.set(node.id, v)
    if (graph.groupIndex[v] < 0) continue
    if (/JSArrayBufferData/.test(node.name)) stores.add(v)
    for (const { target, edge } of graph.outgoing(v))
      if (edge.name === 'backing_store') stores.add(target)
  }
  return [...stores].map((v) => {
    const node = graph.describe(v)
    const wrappers = graph.incoming(v).filter((entry) => entry.edge.name === 'backing_store')
    const aliases = new Map()
    for (const wrapper of wrappers)
      for (const entry of graph.incoming(byId.get(wrapper.owner.id)))
        if (/^(Buffer|Uint8Array|DataView)$/.test(entry.owner.name))
          aliases.set(entry.owner.id, {
            ...entry.owner,
            edge: entry.edge,
            rootPath: graph.rootPath(byId.get(entry.owner.id)),
          })
    return {
      ...node,
      category: labels[v],
      semanticLive: semantic ? semantic.reachableIds.has(node.id) : null,
      rootPath: graph.rootPath(v),
      wrappers,
      aliases: [...aliases.values()],
    }
  })
}

/**
 * Compares actual old/new stores without adding overlapping retained subtrees or view lengths.
 * @param {object[]} before earlier inventory @param {object[]} after later inventory
 * @returns {object} additions, removals, surviving IDs/aliases and additive self-byte changes
 */
const backingComparison = (before, after) => {
  const old = new Map(before.map((row) => [row.id, row]))
  const next = new Map(after.map((row) => [row.id, row]))
  return {
    added: after.filter((row) => !old.has(row.id)),
    removed: before.filter((row) => !next.has(row.id)),
    surviving: after
      .filter((row) => old.has(row.id))
      .map((row) => ({
        id: row.id,
        before: old.get(row.id),
        after: row,
        resized: row.selfBytes - old.get(row.id).selfBytes,
      })),
    delta:
      after.reduce((sum, row) => sum + row.selfBytes, 0) -
      before.reduce((sum, row) => sum + row.selfBytes, 0),
  }
}

module.exports = { backingInventory, backingComparison }
