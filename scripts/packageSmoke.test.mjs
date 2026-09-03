import { test } from 'node:test'
import assert from 'node:assert/strict'
import { createRequire } from 'node:module'

// Run from a temp project where @countly/kafkajs is installed from the packed tarball (scripts/packSmoke.sh).
const require = createRequire(import.meta.url)
const PKG = '@countly/kafkajs'

test('require and import resolve to the same module instance', async () => {
  const cjs = require(PKG)
  const esm = await import(PKG)
  assert.equal(esm.Kafka, cjs.Kafka)
  assert.equal(esm.default, cjs)
  assert.equal(esm.CompressionCodecs, cjs.CompressionCodecs)
})

test('the ESM wrapper re-exports every named export of index.js', async () => {
  const cjs = require(PKG)
  const esm = await import(PKG)
  const esmNames = Object.keys(esm).filter(n => n !== 'default').sort()
  assert.deepEqual(esmNames, Object.keys(cjs).sort())
})

test('deep paths under src are not exported', () => {
  assert.throws(() => require(`${PKG}/src/index`), { code: 'ERR_PACKAGE_PATH_NOT_EXPORTED' })
  assert.throws(() => require(`${PKG}/src/network/requestQueue`), { code: 'ERR_PACKAGE_PATH_NOT_EXPORTED' })
})

test('package.json stays reachable', () => {
  assert.equal(require(`${PKG}/package.json`).name, PKG)
})
