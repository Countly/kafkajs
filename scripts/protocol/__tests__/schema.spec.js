'use strict'

const { parseSpec } = require('../../compat/parse')
const { expectedFields, compare, discover, fieldsFromSpec } = require('../schema')

describe('scripts/protocol > schema validation', () => {
  test('resolves the expected fields of ApiVersionsResponse v0', () => {
    expect(expectedFields('ApiVersions', 'response', 0)).toEqual([
      { name: 'ErrorCode', type: 'int16', versions: '0+' },
      { name: 'ApiKeys', type: '[]ApiVersion', versions: '0+' },
      { name: 'ApiKeys.ApiKey', type: 'int16', versions: '0+' },
      { name: 'ApiKeys.MinVersion', type: 'int16', versions: '0+' },
      { name: 'ApiKeys.MaxVersion', type: 'int16', versions: '0+' },
    ])
  })

  test('resolves nullability and tags at the requested version', () => {
    const fields = expectedFields('Metadata', 'request', 12)
    expect(fields[0]).toEqual({
      name: 'Topics',
      type: '[]MetadataRequestTopic',
      versions: '0+',
      nullable: true,
    })
    expect(fields.find((f) => f.name === 'Topics.Name')).toEqual({
      name: 'Topics.Name',
      type: 'string',
      versions: '0+',
      nullable: true,
    })
    expect(fields.map((f) => f.name)).not.toContain('IncludeClusterAuthorizedOperations')
  })

  test('drops fields whose range closed before this version', () => {
    expect(expectedFields('Metadata', 'request', 9).map((f) => f.name)).toContain(
      'IncludeClusterAuthorizedOperations'
    )
  })

  test('compare passes an identical list', () => {
    const expected = expectedFields('ApiVersions', 'response', 0)
    const codec = {
      file: 'x/response.js',
      schema: { api: 'ApiVersions', direction: 'response', version: 0, fields: expected },
    }
    expect(compare(codec, expected)).toEqual([])
  })

  test('compare reports a reordered field', () => {
    const expected = expectedFields('ApiVersions', 'response', 0)
    const reordered = [expected[1], expected[0], ...expected.slice(2)]
    const codec = {
      file: 'x/response.js',
      schema: { api: 'ApiVersions', direction: 'response', version: 0, fields: reordered },
    }
    const failures = compare(codec, expected)
    expect(failures.length).toBeGreaterThan(0)
    expect(failures[0]).toMatch(/^S1 /)
    expect(failures[0]).toMatch(/ErrorCode/)
  })

  test('compare reports a wrong type, a wrong version range, and a missing tag', () => {
    const expected = expectedFields('ApiVersions', 'response', 3)
    const mutate = (index, patch) => {
      const fields = expected.map((f) => ({ ...f }))
      Object.assign(fields[index], patch)
      return {
        file: 'x/response.js',
        schema: { api: 'ApiVersions', direction: 'response', version: 3, fields },
      }
    }
    expect(compare(mutate(0, { type: 'int32' }), expected)[0]).toMatch(/"type":"int32"/)
    expect(compare(mutate(5, { versions: '0+' }), expected)[0]).toMatch(/"versions":"0\+"/)
    const untagged = expected.map((f) => ({ ...f }))
    delete untagged[6].tag
    expect(
      compare(
        {
          file: 'x/response.js',
          schema: { api: 'ApiVersions', direction: 'response', version: 3, fields: untagged },
        },
        expected
      )[0]
    ).toMatch(/tag/)
  })

  describe('the versions:null tagged-only fallback (brief "Check algorithm" step 5)', () => {
    // Same shape as scripts/compat/__tests__/fixtures/SyntheticRequest.json, passed straight to
    // parseSpec (it accepts an already-parsed object, not just a JSON string): one plain field,
    // and one declared only as tagged, whose own `versions` resolves to `null` and is alive only
    // through `taggedVersions`.
    const spec = parseSpec({
      apiKey: 9999,
      type: 'request',
      name: 'SyntheticTaggedOnlyRequest',
      validVersions: '0-3',
      flexibleVersions: '2+',
      fields: [
        { name: 'Regular', type: 'int32', versions: '0+' },
        { name: 'TaggedOnly', type: 'string', versions: null, tag: 0, taggedVersions: '2+' },
      ],
    })

    test('keeps the tagged-only field, reporting versions from taggedVersions', () => {
      // Without the fallback, rangeContains(parseVersionRange(null), 2) is false and this field
      // would be dropped silently even though taggedVersions "2+" is alive at v2.
      expect(fieldsFromSpec(spec, 2)).toEqual([
        { name: 'Regular', type: 'int32', versions: '0+' },
        { name: 'TaggedOnly', type: 'string', versions: '2+', tag: 0 },
      ])
    })

    test('range-tests it by taggedVersions, not by its own null versions', () => {
      expect(fieldsFromSpec(spec, 1).map((f) => f.name)).toEqual(['Regular'])
    })
  })

  test('discovers every committed codec and reports the unvalidated ones', () => {
    const codecs = discover()
    expect(codecs.length).toBeGreaterThan(100)
    const validated = codecs.filter((codec) => codec.schema != null)
    // Grows as codecs gain a schema export; keep it sorted.
    expect(validated.map((codec) => codec.file).sort()).toEqual([
      'src/protocol/requests/alterPartitionReassignments/v0/request.js',
      'src/protocol/requests/alterPartitionReassignments/v0/response.js',
      'src/protocol/requests/apiVersions/v0/response.js',
      'src/protocol/requests/apiVersions/v3/request.js',
      'src/protocol/requests/apiVersions/v3/response.js',
      'src/protocol/requests/apiVersions/v4/request.js',
      'src/protocol/requests/apiVersions/v4/response.js',
      'src/protocol/requests/consumerGroupDescribe/v0/request.js',
      'src/protocol/requests/consumerGroupDescribe/v0/response.js',
      'src/protocol/requests/consumerGroupDescribe/v1/request.js',
      'src/protocol/requests/consumerGroupDescribe/v1/response.js',
      'src/protocol/requests/consumerGroupHeartbeat/v0/request.js',
      'src/protocol/requests/consumerGroupHeartbeat/v0/response.js',
      'src/protocol/requests/consumerGroupHeartbeat/v1/request.js',
      'src/protocol/requests/consumerGroupHeartbeat/v1/response.js',
      'src/protocol/requests/fetch/v12/request.js',
      'src/protocol/requests/fetch/v12/response.js',
      'src/protocol/requests/fetch/v13/request.js',
      'src/protocol/requests/fetch/v13/response.js',
      'src/protocol/requests/fetch/v14/request.js',
      'src/protocol/requests/fetch/v14/response.js',
      'src/protocol/requests/fetch/v15/request.js',
      'src/protocol/requests/fetch/v15/response.js',
      'src/protocol/requests/fetch/v16/request.js',
      'src/protocol/requests/fetch/v16/response.js',
      'src/protocol/requests/fetch/v17/request.js',
      'src/protocol/requests/fetch/v17/response.js',
      'src/protocol/requests/fetch/v18/request.js',
      'src/protocol/requests/fetch/v18/response.js',
      'src/protocol/requests/findCoordinator/v3/request.js',
      'src/protocol/requests/findCoordinator/v3/response.js',
      'src/protocol/requests/findCoordinator/v4/request.js',
      'src/protocol/requests/findCoordinator/v4/response.js',
      'src/protocol/requests/findCoordinator/v5/request.js',
      'src/protocol/requests/findCoordinator/v5/response.js',
      'src/protocol/requests/findCoordinator/v6/request.js',
      'src/protocol/requests/findCoordinator/v6/response.js',
      'src/protocol/requests/initProducerId/v2/request.js',
      'src/protocol/requests/initProducerId/v2/response.js',
      'src/protocol/requests/initProducerId/v3/request.js',
      'src/protocol/requests/initProducerId/v3/response.js',
      'src/protocol/requests/initProducerId/v4/request.js',
      'src/protocol/requests/initProducerId/v4/response.js',
      'src/protocol/requests/initProducerId/v5/request.js',
      'src/protocol/requests/initProducerId/v5/response.js',
      'src/protocol/requests/listOffsets/v4/request.js',
      'src/protocol/requests/listOffsets/v4/response.js',
      'src/protocol/requests/listOffsets/v5/request.js',
      'src/protocol/requests/listOffsets/v5/response.js',
      'src/protocol/requests/listOffsets/v6/request.js',
      'src/protocol/requests/listOffsets/v6/response.js',
      'src/protocol/requests/listOffsets/v7/request.js',
      'src/protocol/requests/listOffsets/v7/response.js',
      'src/protocol/requests/listOffsets/v8/request.js',
      'src/protocol/requests/listOffsets/v8/response.js',
      'src/protocol/requests/listOffsets/v9/request.js',
      'src/protocol/requests/listOffsets/v9/response.js',
      'src/protocol/requests/listPartitionReassignments/v0/request.js',
      'src/protocol/requests/listPartitionReassignments/v0/response.js',
      'src/protocol/requests/metadata/v0/request.js',
      'src/protocol/requests/metadata/v10/request.js',
      'src/protocol/requests/metadata/v10/response.js',
      'src/protocol/requests/metadata/v11/request.js',
      'src/protocol/requests/metadata/v11/response.js',
      'src/protocol/requests/metadata/v12/request.js',
      'src/protocol/requests/metadata/v12/response.js',
      'src/protocol/requests/metadata/v13/request.js',
      'src/protocol/requests/metadata/v13/response.js',
      'src/protocol/requests/metadata/v7/request.js',
      'src/protocol/requests/metadata/v7/response.js',
      'src/protocol/requests/metadata/v8/request.js',
      'src/protocol/requests/metadata/v8/response.js',
      'src/protocol/requests/metadata/v9/request.js',
      'src/protocol/requests/metadata/v9/response.js',
      'src/protocol/requests/offsetCommit/v6/request.js',
      'src/protocol/requests/offsetCommit/v6/response.js',
      'src/protocol/requests/offsetCommit/v7/request.js',
      'src/protocol/requests/offsetCommit/v7/response.js',
      'src/protocol/requests/offsetCommit/v8/request.js',
      'src/protocol/requests/offsetCommit/v8/response.js',
      'src/protocol/requests/offsetFetch/v5/request.js',
      'src/protocol/requests/offsetFetch/v5/response.js',
      'src/protocol/requests/offsetFetch/v6/request.js',
      'src/protocol/requests/offsetFetch/v6/response.js',
      'src/protocol/requests/offsetFetch/v7/request.js',
      'src/protocol/requests/offsetFetch/v7/response.js',
      'src/protocol/requests/offsetFetch/v8/request.js',
      'src/protocol/requests/offsetFetch/v8/response.js',
      'src/protocol/requests/offsetForLeaderEpoch/v3/request.js',
      'src/protocol/requests/offsetForLeaderEpoch/v3/response.js',
      'src/protocol/requests/offsetForLeaderEpoch/v4/request.js',
      'src/protocol/requests/offsetForLeaderEpoch/v4/response.js',
      'src/protocol/requests/produce/v10/request.js',
      'src/protocol/requests/produce/v10/response.js',
      'src/protocol/requests/produce/v11/request.js',
      'src/protocol/requests/produce/v11/response.js',
      'src/protocol/requests/produce/v12/request.js',
      'src/protocol/requests/produce/v12/response.js',
      'src/protocol/requests/produce/v13/request.js',
      'src/protocol/requests/produce/v13/response.js',
      'src/protocol/requests/produce/v8/request.js',
      'src/protocol/requests/produce/v8/response.js',
      'src/protocol/requests/produce/v9/request.js',
      'src/protocol/requests/produce/v9/response.js',
      'src/protocol/requests/saslAuthenticate/v2/request.js',
      'src/protocol/requests/saslAuthenticate/v2/response.js',
    ])
  })
})
