#!/usr/bin/env node
'use strict'

const { ownershipIndex } = require('./heapDiff')

/**
 * Reconstructs labelled V8 ephemerons as AND(table, key) -> value rules. Ordinary
 * reachability excludes both copies of the labelled pair edge and all weak edges.
 * Removing a dependency and recomputing the least fixed point tests conditional
 * retention; an activated edge alone is NOT an ordinary dominator relation.
 */
const ephemeronLiveness = (raw, excludedIds = []) => {
  const { nodes, edges, strings, snapshot } = raw
  const meta = snapshot.meta
  const nf = meta.node_fields.length
  const ef = meta.edge_fields.length
  const field = (fields, name) => fields.indexOf(name)
  const idAt = field(meta.node_fields, 'id')
  const countAt = field(meta.node_fields, 'edge_count')
  const nodeNameAt = field(meta.node_fields, 'name')
  const typeAt = field(meta.edge_fields, 'type')
  const nameAt = field(meta.edge_fields, 'name_or_index')
  const toAt = field(meta.edge_fields, 'to_node')
  const edgeTypes = meta.edge_types[typeAt]
  const n = nodes.length / nf
  const byId = new Map(Array.from({ length: n }, (_, v) => [nodes[v * nf + idAt], v]))
  const ordinary = Array.from({ length: n }, () => [])
  const pairs = new Map()
  let e = 0
  for (let v = 0; v < n; v++) {
    for (let j = 0; j < nodes[v * nf + countAt]; j++, e += ef) {
      const type = edgeTypes[edges[e + typeAt]]
      const target = edges[e + toAt] / nf
      const name = ['element', 'hidden'].includes(type) ? '' : strings[edges[e + nameAt]]
      const match =
        /part of key \(.+ @(\d+)\) -> value \(.+ @(\d+)\) pair in WeakMap \(table @(\d+)\)/.exec(
          name
        )
      if (match) {
        const [keyId, valueId, tableId] = match.slice(1).map(Number)
        if (
          !byId.has(keyId) ||
          !byId.has(valueId) ||
          !byId.has(tableId) ||
          byId.get(valueId) !== target ||
          ![keyId, tableId].includes(nodes[v * nf + idAt])
        )
          throw new Error('Invalid labelled ephemeron dependency')
        // Deleted slots use V8's Hole sentinel, not a JavaScript WeakMap key.
        if (strings[nodes[byId.get(keyId) * nf + nodeNameAt]] !== 'system / Hole')
          pairs.set(`${tableId}/${keyId}/${valueId}`, { tableId, keyId, valueId })
      } else if (typeof name === 'string' && name.includes('part of key')) {
        throw new Error('Unrecognized ephemeron label')
      } else if (type !== 'weak') ordinary[v].push(target)
    }
  }
  const excluded = new Set(excludedIds.map((id) => byId.get(id)))
  const reached = new Set()
  const queue = []
  const add = (v) => {
    if (!reached.has(v) && !excluded.has(v)) {
      reached.add(v)
      queue.push(v)
    }
  }
  const drain = () => {
    for (let cursor = 0; cursor < queue.length; cursor++)
      for (const target of ordinary[queue[cursor]]) add(target)
    queue.length = 0
  }
  add((snapshot.root_index ?? 0) / nf)
  drain()
  const ordinaryIds = new Set([...reached].map((v) => nodes[v * nf + idAt]))
  let changed
  do {
    const oldSize = reached.size
    for (const { tableId, keyId, valueId } of pairs.values())
      if (reached.has(byId.get(tableId)) && reached.has(byId.get(keyId))) add(byId.get(valueId))
    drain()
    changed = reached.size !== oldSize
  } while (changed)
  const reachableIds = new Set([...reached].map((v) => nodes[v * nf + idAt]))
  return {
    ordinaryIds,
    reachableIds,
    pairs: [...pairs.values()].map((pair) => ({
      ...pair,
      active: reachableIds.has(pair.tableId) && reachableIds.has(pair.keyId),
      independentlyReachableValue: ordinaryIds.has(pair.valueId),
    })),
  }
}

/**
 * Identifies only the observer's row array, element storage, rows, kind strings and WeakRefs.
 * Strong additions to a row or wrapper remain unclassified, even when the history dominates
 * them. Weak target edges never transfer the observer label to a production object.
 * @param {object} graph analysed heap graph
 * @returns {Set<number>} node indices whose self bytes belong to weak-observation history
 */
const weakHistoryNodes = (graph) => {
  const nodes = new Set()
  for (let v = 0; v < graph.n; v++) {
    const owner = graph.describe(v)
    if (owner.type !== 'object' || owner.name !== 'WeakOwners') continue
    for (const { target: rows, edge } of graph.outgoing(v)) {
      if (edge.type !== 'property' || edge.name !== 'rows') continue
      nodes.add(rows)
      const add = (target) => {
        if (graph.dominates(rows, target)) nodes.add(target)
      }
      for (const { target, edge } of graph.outgoing(rows)) {
        if (edge.type === 'internal' && edge.name === 'elements') add(target)
        if (!(edge.type === 'element' || (edge.type === 'property' && /^\d+$/.test(edge.name))))
          continue
        if (graph.describe(target).name !== 'Object') continue
        add(target)
        for (const child of graph.outgoing(target)) {
          if (child.edge.type !== 'property') continue
          if (child.edge.name === 'weak' && graph.describe(child.target).name === 'WeakRef')
            add(child.target)
          if (child.edge.name !== 'kind') continue
          const pending = [child.target]
          const seen = new Set()
          while (pending.length) {
            const string = pending.pop()
            if (seen.has(string) || !/string/.test(graph.describe(string).type)) continue
            seen.add(string)
            add(string)
            for (const link of graph.outgoing(string)) pending.push(link.target)
          }
        }
      }
    }
  }
  return nodes
}

/**
 * Refines ONLY the former unclassified bucket. Each rule labels a node's self
 * bytes. Named property regions require domination, and ties are left shared.
 * Runtime labels never exempt arbitrary application descendants of code nodes.
 */
const refineOwnership = (graph) => {
  const original = ownershipIndex(graph)
  const weakHistory = weakHistoryNodes(graph)
  const anchors = new Map()
  const sharedCorpus = new Set()
  const internedStrings = new Set()
  const add = (v, label) => {
    const previous = anchors.get(v)
    anchors.set(v, previous && previous !== label ? 'residual:shared-property-target' : label)
  }
  for (let v = 0; v < graph.n; v++) {
    if (graph.groupIndex[v] < 0) continue
    const owner = graph.describe(v)
    if (owner.name === '(Internalized strings)')
      for (const { target } of graph.outgoing(v)) internedStrings.add(target)
    for (const { target, edge } of graph.outgoing(v)) {
      if (
        edge.type === 'property' &&
        owner.type === 'object' &&
        /^(ShareSoak|ShareSoakDriver|Soak|ShareFetchTap|ShareStateObserver|ShareTimerObserver|ShareCycleOracle)$/.test(
          owner.name
        ) &&
        edge.name !== '__proto__'
      )
        add(target, `harness:${owner.name}.${edge.name}`)
      if (
        ['context', 'property'].includes(edge.type) &&
        /^(realpathCache|_pathCache|directoryToParentPackageJSONPathCache|packageJSONCache|pendingUnhandledRejections|allocPool|allocBuffer)$/.test(
          edge.name
        )
      )
        add(target, `node:${edge.name}`)
      if (
        edge.type === 'internal' &&
        edge.name === 'table' &&
        /^(WeakMap|WeakSet)$/.test(owner.name)
      )
        add(target, 'runtime:weak-collection-capacity')
      if (edge.name === 'transition_info' && /AllocationSite/.test(owner.name))
        add(target, 'runtime:allocation-literal-template')
      if (edge.type === 'property' && edge.name === 'prototype' && owner.type === 'closure')
        add(target, 'runtime:prototype-definition')
      if (owner.name === 'ShareSoakDriver' && edge.name === 'acknowledged')
        for (const entry of graph.outgoing(target))
          if (/string/.test(graph.describe(entry.target).type)) sharedCorpus.add(entry.target)
    }
  }
  const inherited = new Array(graph.n)
  const labels = new Array(graph.n)
  for (let i = 1; i <= graph.visited; i++) {
    const v = graph.vertex[i]
    const node = graph.describe(v)
    const parent = graph.idom[v]
    inherited[v] = anchors.get(v) ?? inherited[parent]
    if (original.categories[v] !== 'unclassified') {
      labels[v] = original.categories[v]
      continue
    }
    let label = 'residual:unresolved'
    if ((node.type === 'native' || node.type === 'synthetic') && /^Node \/ /.test(node.name))
      label = 'node:native-storage'
    else if (node.type === 'object shape') label = 'runtime:object-shapes'
    else if (node.name === 'system / WeakArrayList') label = 'runtime:weak-lists-and-dependent-code'
    else if (
      /^system \/ (ProtectedFixedArray|SharedFunctionInfoWrapper|WeakFixedArray)$/.test(node.name)
    )
      label = 'runtime:deoptimization-and-weak-storage'
    else if (sharedCorpus.has(v)) label = 'harness:shared-acknowledged-identities'
    else if (
      inherited[v] &&
      !['runtime:prototype-definition', 'runtime:weak-collection-capacity'].includes(inherited[v])
    )
      label = inherited[v]
    else if (anchors.has(v)) label = anchors.get(v)
    // Only strings/numbers and array metadata dominated by compiler metadata qualify.
    // Generic objects/contexts below code remain unresolved unless anchored above.
    else if (/string/.test(node.type) || ['number', 'array'].includes(node.type)) {
      for (let d = parent; d !== -1; d = graph.idom[d]) {
        const owner = graph.describe(d)
        if (owner.type === 'code') {
          label = 'runtime:compiler-metadata-children'
          break
        }
        if (!/string/.test(owner.type)) break
      }
      if (/string/.test(node.type) && internedStrings.has(v)) label = 'runtime:internalized-strings'
    }
    if (node.type === 'object' && node.name === 'Module') label = 'node:commonjs-module-records'
    if (weakHistory.has(v)) label = 'harness:weak-observation-history'
    labels[v] = label
  }
  return { labels, original: original.categories }
}

/** Stable-ID additive ledger, including releases, resizing and label transfers. */
const selfLedger = (before, after, beforeLabels, afterLabels) => {
  const rows = {}
  const row = (label) =>
    (rows[label] ??= { before: 0, after: 0, added: 0, removed: 0, resized: 0, transfer: 0 })
  const ids = new Map()
  for (let v = 0; v < before.n; v++)
    if (before.groupIndex[v] >= 0) {
      const node = before.describe(v)
      ids.set(node.id, v)
      row(beforeLabels[v]).before += node.selfBytes
    }
  const seen = new Set()
  for (let v = 0; v < after.n; v++)
    if (after.groupIndex[v] >= 0) {
      const node = after.describe(v)
      const label = afterLabels[v]
      const entry = row(label)
      entry.after += node.selfBytes
      const old = ids.get(node.id)
      seen.add(node.id)
      if (old === undefined) entry.added += node.selfBytes
      else if (beforeLabels[old] === label)
        entry.resized += node.selfBytes - before.describe(old).selfBytes
      else {
        row(beforeLabels[old]).transfer -= before.describe(old).selfBytes
        entry.transfer += node.selfBytes
      }
    }
  for (const [id, v] of ids)
    if (!seen.has(id)) row(beforeLabels[v]).removed += before.describe(v).selfBytes
  for (const entry of Object.values(rows)) {
    entry.delta = entry.after - entry.before
    if (entry.delta !== entry.added - entry.removed + entry.resized + entry.transfer)
      throw new Error('Self-byte reconciliation failed')
  }
  return rows
}

module.exports = { ephemeronLiveness, refineOwnership, selfLedger }
