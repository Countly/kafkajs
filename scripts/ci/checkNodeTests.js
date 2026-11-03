#!/usr/bin/env node
'use strict'

const fs = require('fs')
const path = require('path')

/** Parses explicit TAP reporter streams, keeping each invocation separate. */
class NodeTestReport {
  /**
   * @param {string} text Concatenated Node TAP reporter output.
   * @throws {Error} If a stream is incomplete or has malformed test records.
   */
  constructor(text) {
    this.blocks = []
    let block
    let stack = []
    let result
    for (const line of text.split('\n')) {
      if (line === 'TAP version 13') {
        block = { names: [], results: [], counts: {}, plan: null }
        this.blocks.push(block)
        stack = []
        result = null
      }
      if (!block) {
        if (line.trim()) throw new Error('output before TAP header')
        continue
      }
      const start = /^( *)# Subtest: (.+)$/.exec(line)
      if (start) {
        const depth = start[1].length
        stack = stack.filter((entry) => entry.depth < depth)
        stack.push({ depth, name: start[2] })
      }
      const end = /^( *)(not ok|ok) (\d+) - (.*?)(?: # (SKIP|TODO)\b.*)?$/.exec(line)
      if (end) {
        const depth = end[1].length
        const current = stack.find((entry) => entry.depth === depth)
        if (!current || current.name !== end[4]) throw new Error('unmatched TAP test result')
        result = {
          name: stack
            .filter((entry) => entry.depth <= depth)
            .map((entry) => entry.name)
            .join(' > '),
          ok: end[2] === 'ok' && !end[5],
          failed: end[2] !== 'ok' || end[5] === 'TODO',
          depth,
          type: null,
        }
        block.results.push(result)
        if (result.ok) block.names.push(result.name)
        stack = stack.filter((entry) => entry.depth < depth)
      }
      const type = /^ +type: '(test|suite)'$/.exec(line)
      if (type && result) result.type = type[1]
      const plan = /^1\.\.(\d+)$/.exec(line)
      if (plan) {
        if (block.plan !== null) throw new Error('duplicate TAP plan')
        block.plan = Number(plan[1])
      }
      const count = /^# (tests|pass|fail|cancelled|skipped|todo) (\d+)$/.exec(line)
      if (count) {
        if (stack.length) throw new Error('unfinished TAP test')
        if (count[1] in block.counts) throw new Error('duplicate TAP summary')
        block.counts[count[1]] = Number(count[2])
      }
      if (/^# duration_ms /.test(line)) block.complete = true
      if (/^Bail out!/i.test(line)) throw new Error('TAP bailed out')
    }
    for (const item of this.blocks) {
      if (
        !item.complete ||
        Object.keys(item.counts).length !== 6 ||
        item.plan !== item.results.filter((entry) => entry.depth === 0).length ||
        item.results.some((entry) => !entry.type)
      )
        throw new Error('incomplete TAP block')
    }
  }

  /**
   * Enforces completed runs, skip policy, and exact qualified live identities per block.
   * @param {object} contract Manifest nodeTest contract with blocks, skipped and optional liveNames/liveTests.
   * @returns {string[]} Contract violations; an empty list means pass.
   */
  check(contract) {
    const problems = []
    if (
      !Number.isInteger(contract.blocks) ||
      contract.blocks < 1 ||
      !['none', 'allowed'].includes(contract.skipped)
    )
      return ['invalid nodeTest contract']
    if (this.blocks.length !== contract.blocks)
      problems.push(`expected ${contract.blocks} blocks, found ${this.blocks.length}`)
    this.blocks.forEach((block, i) => {
      const { tests, pass, fail, cancelled, skipped, todo } = block.counts
      if (
        block.results.some((entry) => entry.failed) ||
        tests < 1 ||
        pass < 1 ||
        fail ||
        cancelled ||
        todo ||
        pass + skipped !== tests ||
        (contract.skipped === 'none' && (skipped || block.results.some((entry) => !entry.ok)))
      ) {
        problems.push(`block ${i + 1}: failed, skipped, empty or inconsistent tests`)
      }
      const expected = contract.liveNames?.[i] || []
      if (expected.length) {
        const roots = expected.filter(
          (name) => !expected.some((parent) => name.startsWith(`${parent} > `))
        )
        const live = block.results.filter((entry) =>
          roots.some((root) => entry.name === root || entry.name.startsWith(`${root} > `))
        )
        const names = live
          .filter((entry) => entry.ok)
          .map((entry) => entry.name)
          .sort()
        if (
          JSON.stringify(names) !== JSON.stringify([...expected].sort()) ||
          live.filter((entry) => entry.ok && entry.type === 'test').length !==
            contract.liveTests?.[i]
        ) {
          problems.push(`block ${i + 1}: live test names or count differ`)
        }
      } else if (contract.liveTests?.[i]) problems.push(`block ${i + 1}: missing live names`)
    })
    if (
      (contract.liveNames?.length || 0) > this.blocks.length ||
      (contract.liveTests?.length || 0) > this.blocks.length
    )
      problems.push('missing live block')
    return problems
  }
}

/**
 * Verifies a report against one manifest entry. Missing reports and contracts fail closed.
 * @param {string[]} argv Report path, check id, optional manifest path.
 * @param {{out?: Function, err?: Function}} io Summary and diagnostic writers.
 * @returns {number} Zero on success, one on a contract or read failure.
 */
const run = (argv, { out = console.log, err = console.error } = {}) => {
  const [file, id, manifest = path.join(__dirname, 'gate.json')] = argv
  try {
    if (!file || !id)
      throw new Error('usage: checkNodeTests.js <tap-report> <check-id> [gate.json]')
    const contract = JSON.parse(fs.readFileSync(manifest, 'utf8')).checks.find(
      (entry) => entry.id === id
    )?.nodeTest
    if (!contract) throw new Error('missing nodeTest contract')
    const report = new NodeTestReport(fs.readFileSync(file, 'utf8'))
    const problems = report.check(contract)
    if (problems.length) throw new Error(problems.join('; '))
    out(`${id}: ok, ${report.blocks.length} Node test blocks verified`)
    return 0
  } catch (error) {
    err(`${id || 'node-test'}: ${error.message}`)
    return 1
  }
}

module.exports = { NodeTestReport, run }
if (require.main === module) process.exitCode = run(process.argv.slice(2))
