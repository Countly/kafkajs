const offsetDelete = require('./offsetDelete')

const encodedHex = async (api, version, options) =>
  (await require(`./${api}/v${version}/request`)(options).encode()).buffer.toString('hex')

describe('Protocol > Requests > cluster admin request defaults', () => {
  test.each([
    ['describeCluster', 0, '0000'], // includeClusterAuthorizedOperations=false, tags
    ['describeCluster', 1, '000100'], // + endpointType=1
    ['describeCluster', 2, '00010000'], // + includeFencedBrokers=false
    ['electLeaders', 0, '000000000000ea60'], // no partitions, timeout 60000
    ['electLeaders', 1, '00000000000000ea60'], // electionType=0 first
    ['electLeaders', 2, '00010000ea6000'], // compact empty array, tags
    ['updateFeatures', 0, '0000ea600100'], // timeout 60000, no updates, tags
    ['updateFeatures', 1, '0000ea60010000'], // + validateOnly=false
    ['updateFeatures', 2, '0000ea60010000'],
    ['listConfigResources', 1, '0100'], // no resource types, tags
    ['listTransactions', 0, '010100'], // no states, no producer ids, tags
    ['listTransactions', 1, '0101ffffffffffffffff00'], // + durationFilter=-1
    ['listTransactions', 2, '0101ffffffffffffffff0000'], // + null transactionalIdPattern
  ])('%s v%i encodes its defaults when called without options', async (api, version, hex) => {
    expect(await encodedHex(api, version, undefined)).toBe(hex)
  })

  test.each([
    ['electLeaders', 0, 'ffffffff0000ea60'],
    ['electLeaders', 1, '00ffffffff0000ea60'],
    ['electLeaders', 2, '00000000ea6000'],
  ])(
    '%s v%i encodes null topicPartitions as the null array that elects every partition',
    async (api, version, hex) => {
      expect(await encodedHex(api, version, { topicPartitions: null })).toBe(hex)
    }
  )

  test.each([
    [1, 'ffffffff'],
    [2, '0000'],
    [3, '0000'],
    [4, '0000'],
    [5, '0000'],
  ])(
    'describeLogDirs v%i encodes a missing topic list as the null array for every log dir',
    async (version, hex) => {
      expect(await encodedHex('describeLogDirs', version, undefined)).toBe(hex)
    }
  )

  test.each([
    ['omitted', {}],
    ['null', { users: null }],
  ])(
    'describeUserScramCredentials v0 encodes %s users as the null array for every user',
    async (_label, options) => {
      expect(await encodedHex('describeUserScramCredentials', 0, options)).toBe('0000')
    }
  )

  test('offsetDelete selects only v0 and names the unsupported version', () => {
    expect(typeof offsetDelete.protocol({ version: '0' })).toBe('function')
    expect(() => offsetDelete.protocol({ version: 1 })).toThrow('Unsupported OffsetDelete version')
  })
})
