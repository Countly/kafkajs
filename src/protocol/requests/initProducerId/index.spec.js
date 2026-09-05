const initProducerId = require('./index')
const requestV0 = require('./v0/request')
const requestV1 = require('./v1/request')
const requestV2 = require('./v2/request')
const requestV3 = require('./v3/request')
const requestV4 = require('./v4/request')
const requestV5 = require('./v5/request')

jest.mock('./v0/request')
jest.mock('./v1/request')
jest.mock('./v2/request')
jest.mock('./v3/request')
jest.mock('./v4/request')
jest.mock('./v5/request')

describe('Protocol > Requests > InitProducerId', () => {
  ;[requestV0, requestV1, requestV2, requestV3, requestV4, requestV5].forEach(
    (request, version) => {
      describe(`version v${version}`, () => {
        test('provides a default timeout of 5000', async () => {
          initProducerId.protocol({ version })({ transactionalId: 'foo' })
          expect(request).toHaveBeenCalledWith({ transactionalId: 'foo', transactionTimeout: 5000 })
        })
      })
    }
  )
})
