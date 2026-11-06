'use strict'

const fs = require('fs')
const { kafkaTag, collect, render, OUTPUT } = require('../table')
const trackedApis = require('../../compat/tracked-apis.json')

describe('scripts/protocol > flexible-versions table', () => {
  const tag = kafkaTag()
  const rows = collect(tag)

  test('covers every API in the committed snapshot', () => {
    expect(rows.length).toBe(trackedApis.tracked.length)
    expect(rows[0]).toEqual({ apiKey: 0, apiName: 'Produce', flexibleFrom: 9 })
    expect(rows.find((row) => row.apiKey === 17)).toEqual({
      apiKey: 17,
      apiName: 'SaslHandshake',
      flexibleFrom: null,
    })
    expect(rows.find((row) => row.apiKey === 18).flexibleFrom).toBe(3)
    expect(rows.find((row) => row.apiKey === 3).flexibleFrom).toBe(9)
    expect(rows.find((row) => row.apiKey === 45).flexibleFrom).toBe(0)
  })

  test('the committed file is byte-identical to a fresh render', () => {
    expect(fs.readFileSync(OUTPUT, 'utf8')).toEqual(render(tag, rows))
  })

  test('the committed file is what src/ actually loads', () => {
    const table = require('../../../src/protocol/flexibleVersions')
    for (const row of rows) {
      expect(table[row.apiKey]).toEqual(row.flexibleFrom)
    }
    expect(Object.keys(table).length).toBe(rows.length)
  })
})
