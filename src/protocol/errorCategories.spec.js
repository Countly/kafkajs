const { categoryOf } = require('./errorCategories')
const { createErrorFromCode, errorCodes } = require('./error')
const { KafkaJSProtocolError } = require('../errors')
const { parse } = require('./requests/saslAuthenticate/v0/response')

const RETRIABLE_CATEGORIES = ['retriable', 'refreshRetriable']

describe('categoryOf', () => {
  test.each(errorCodes.map(({ code, type, retriable }) => [code, type, retriable]))(
    'the error for %i %s carries the category the table gives it (retriable: %p)',
    (code, _type, retriable) => {
      const error = createErrorFromCode(code)

      expect(error.category).toEqual(categoryOf(code, { retriable }))
      expect(RETRIABLE_CATEGORIES.includes(error.category)).toBe(retriable)
    }
  )

  test('a row that names a surface is skipped until the surface is known', () => {
    expect(categoryOf(87, { retriable: false })).toEqual('applicationRecoverable')
    expect(categoryOf(87, { retriable: false, surface: 'produce' })).toEqual('invalidConfiguration')
    expect(categoryOf(87, { retriable: false, surface: 'txn' })).toEqual('applicationRecoverable')
  })

  test.each([10, 32])(
    'a rejected record, code %i, is invalidConfiguration on Produce only',
    (code) => {
      expect(categoryOf(code, { retriable: false, surface: 'produce' })).toEqual(
        'invalidConfiguration'
      )
      expect(
        categoryOf(code, { retriable: false, surface: 'produce', transactional: true })
      ).toEqual('invalidConfiguration')
      expect(categoryOf(code, { retriable: false })).toEqual('applicationRecoverable')
      expect(categoryOf(code, { retriable: false, surface: 'txn' })).toEqual(
        'applicationRecoverable'
      )
    }
  )

  test('rows are tried in order, so one code can answer differently on each surface', () => {
    expect(categoryOf(48, { retriable: false, surface: 'produce' })).toEqual('abortable')
    expect(categoryOf(48, { retriable: false, surface: 'txn' })).toEqual('applicationRecoverable')
    expect(categoryOf(22, { retriable: false, surface: 'offsets' })).toEqual('abortable')
    expect(categoryOf(22, { retriable: false, surface: 'produce' })).toEqual(
      'applicationRecoverable'
    )
  })

  test.each([45, 59])(
    'code %i on Produce is abortable in a transaction, not outside one',
    (code) => {
      const produce = { retriable: false, surface: 'produce' }

      expect(categoryOf(code, { ...produce, transactional: true })).toEqual('abortable')
      expect(categoryOf(code, { ...produce, transactional: false })).toEqual(
        'applicationRecoverable'
      )
      expect(categoryOf(code, { retriable: false })).toEqual('applicationRecoverable')
    }
  )

  test.each(['produce', 'txn', 'offsets'])(
    'GROUP_AUTHORIZATION_FAILED is invalidConfiguration on %s, and before the surface is known',
    (surface) => {
      expect(categoryOf(30, { retriable: false, surface })).toEqual('invalidConfiguration')
      expect(categoryOf(30, { retriable: false })).toEqual('invalidConfiguration')
    }
  )

  test('UNKNOWN_PRODUCER_ID on the transaction requests is abortable in a transaction, not outside one', () => {
    const txn = { retriable: false, surface: 'txn' }

    expect(categoryOf(59, { ...txn, transactional: true })).toEqual('abortable')
    expect(categoryOf(59, { ...txn, transactional: false })).toEqual('applicationRecoverable')
    expect(categoryOf(59, { retriable: false, surface: 'offsets', transactional: true })).toEqual(
      'applicationRecoverable'
    )
  })

  test('never answers a category that contradicts the retriable flag it is given', () => {
    // 6 is refreshRetriable in the table and 31 invalidConfiguration; a flag that disagrees decides.
    expect(categoryOf(6, { retriable: false })).toEqual('applicationRecoverable')
    expect(categoryOf(31, { retriable: true })).toEqual('retriable')
  })

  test('a code no row names takes the category its retriable flag implies', () => {
    expect(categoryOf(9999, { retriable: false })).toEqual('applicationRecoverable')
    expect(categoryOf(41, { retriable: true })).toEqual('retriable')
  })
})

describe('a protocol error carries its category from construction', () => {
  test('the SASL authentication failure its parser throws is invalidConfiguration', async () => {
    const error = await parse({
      errorCode: 58,
      errorMessage: 'Invalid username or password',
      authBytes: Buffer.alloc(0),
    }).catch((e) => e)

    expect(error).toBeInstanceOf(KafkaJSProtocolError)
    expect(error).toMatchObject({
      type: 'SASL_AUTHENTICATION_FAILED',
      code: 58,
      retriable: false,
      message: 'SASL Authentication failed',
      category: 'invalidConfiguration',
    })
  })

  test('a row without a numeric code takes the category its retriable flag implies', () => {
    const row = { type: 'CUSTOM', retriable: true, message: 'from outside the table' }

    expect(new KafkaJSProtocolError(row).category).toEqual('retriable')
    expect(
      new KafkaJSProtocolError({ ...row, code: 'E_CUSTOM', retriable: false }).category
    ).toEqual('applicationRecoverable')
  })

  test('an error rebuilt from the fields of another keeps its category', () => {
    const rebuilt = new KafkaJSProtocolError({
      ...createErrorFromCode(120),
      message: 'TRANSACTION_ABORTABLE: with the broker message',
    })

    expect(rebuilt.category).toEqual('abortable')
  })
})
