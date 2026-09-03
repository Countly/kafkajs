const path = require('path')
const {
  stripComments,
  readSpec,
  parseVersionRange,
  parseValidVersions,
  parseFlexibleVersions,
  expandVersions,
} = require('../parse')

const fixture = name => path.join(__dirname, 'fixtures', `${name}.json`)

describe('scripts/compat/parse', () => {
  describe('stripComments', () => {
    it('removes // comments outside string literals', () => {
      expect(stripComments('{ "a": 1 } // trailing')).toBe('{ "a": 1 } \n')
    })

    it('keeps // inside a string literal', () => {
      expect(stripComments('{ "a": "http://x" }')).toBe('{ "a": "http://x" }')
    })

    it('keeps // after an escaped quote inside a string literal', () => {
      expect(stripComments('{ "a": "say \\" // still string" }')).toBe(
        '{ "a": "say \\" // still string" }'
      )
    })
  })

  describe('version ranges', () => {
    it('parses "N-M", "N", "N+" and "none"', () => {
      expect(parseVersionRange('0-2')).toEqual({ min: 0, max: 2 })
      expect(parseVersionRange('4')).toEqual({ min: 4, max: 4 })
      expect(parseVersionRange('12+')).toEqual({ min: 12, max: null })
      expect(parseVersionRange('none')).toBeNull()
    })

    it('throws on an unparsable range', () => {
      expect(() => parseVersionRange('1..3')).toThrow('unparsable version range')
    })

    it('requires validVersions to be closed', () => {
      expect(parseValidVersions('3-13')).toEqual({ min: 3, max: 13 })
      expect(parseValidVersions('0')).toEqual({ min: 0, max: 0 })
      expect(() => parseValidVersions('3+')).toThrow('must be a closed range')
    })

    it('reduces flexibleVersions to the first flexible version', () => {
      expect(parseFlexibleVersions('9+')).toBe(9)
      expect(parseFlexibleVersions('none')).toBeNull()
    })

    it('expands a closed range', () => {
      expect(expandVersions({ min: 3, max: 6 })).toEqual([3, 4, 5, 6])
      expect(expandVersions(null)).toEqual([])
    })
  })

  describe('parseSpec', () => {
    const spec = readSpec(fixture('SyntheticRequest'))

    it('reads the top-level keys', () => {
      expect(spec.name).toBe('SyntheticRequest')
      expect(spec.type).toBe('request')
      expect(spec.apiKey).toBe(4242)
      expect(spec.validVersions).toEqual({ min: 0, max: 2 })
      expect(spec.flexibleFrom).toBe(2)
      expect(spec.deprecatedVersions).toBe('0')
      expect(spec.latestVersionUnstable).toBe(true)
    })

    it('keeps "//" and escaped quotes inside about strings', () => {
      const url = spec.fields.find(f => f.path === 'Url')
      expect(url.about).toBe('A doc string containing // not a comment and an escaped " quote.')
    })

    it('flattens inline nested struct fields with a dotted path', () => {
      const paths = spec.fields.map(f => f.path)
      expect(paths).toContain('Topics.Name')
      expect(paths).toContain('Topics.Partitions')
      const topics = spec.fields.find(f => f.path === 'Topics')
      expect(topics.nullableVersions).toBe('1+')
    })

    it('resolves commonStructs references', () => {
      const paths = spec.fields.map(f => f.path)
      expect(paths).toContain('Groups.GroupId')
      expect(paths).toContain('Groups.MemberEpoch')
    })

    it('reads tagged fields and defaults versions to taggedVersions', () => {
      const clusterId = spec.fields.find(f => f.path === 'ClusterId')
      expect(clusterId.tag).toBe(0)
      expect(clusterId.taggedVersions).toBe('2+')
      expect(clusterId.versions).toBe('2+')
    })

    it('emits fields in wire order', () => {
      expect(spec.fields.map(f => f.path)).toEqual([
        'Url',
        'Groups',
        'Groups.GroupId',
        'Groups.MemberEpoch',
        'Topics',
        'Topics.Name',
        'Topics.Partitions',
        'ClusterId',
      ])
    })
  })

  describe('never-flexible single-version message', () => {
    const spec = readSpec(fixture('NeverFlexibleRequest'))

    it('has a single valid version and no flexible version', () => {
      expect(spec.validVersions).toEqual({ min: 0, max: 0 })
      expect(spec.flexibleFrom).toBeNull()
      expect(spec.latestVersionUnstable).toBe(false)
    })
  })

  describe('the real snapshot', () => {
    const dir = path.join(__dirname, '..', 'messages', '4.3.1')

    it('parses ApiVersionsRequest.json', () => {
      const spec = readSpec(path.join(dir, 'ApiVersionsRequest.json'))
      expect(spec.apiKey).toBe(18)
      expect(spec.validVersions).toEqual({ min: 0, max: 4 })
      expect(spec.flexibleFrom).toBe(3)
      expect(spec.fields.map(f => f.path)).toEqual(['ClientSoftwareName', 'ClientSoftwareVersion'])
    })

    it('parses RequestHeader.json (a header file with no apiKey)', () => {
      const spec = readSpec(path.join(dir, 'RequestHeader.json'))
      expect(spec.type).toBe('header')
      expect(spec.apiKey).toBeNull()
      expect(spec.validVersions).toEqual({ min: 1, max: 2 })
      expect(spec.flexibleFrom).toBe(2)
    })
  })
})
