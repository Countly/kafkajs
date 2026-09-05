const fs = require('fs')
const path = require('path')
const {
  parseErrorsJava,
  messageFiles,
  discover,
  safeDiscover,
  parseExtendsClause,
  resolvesToRetriable,
  resolveExceptionHierarchy,
} = require('../fetch')
const trackedApis = require('../tracked-apis.json')

const readFixture = (name) => fs.readFileSync(path.join(__dirname, 'fixtures', name), 'utf8')

describe('scripts/compat/fetch', () => {
  describe('parseErrorsJava', () => {
    const errors = parseErrorsJava(readFixture('ErrorsSample.java'))

    it('reads every constant shape, sorted by code', () => {
      expect(errors.map((e) => e.code)).toEqual([-1, 0, 1, 6, 129])
    })

    it('reads a lambda-factory constant with a null message', () => {
      expect(errors.find((e) => e.code === 0)).toEqual({
        type: 'NONE',
        code: 0,
        exception: null,
        message: null,
      })
    })

    it('joins a message split across concatenated literals', () => {
      const notLeader = errors.find((e) => e.code === 6)
      expect(notLeader.type).toBe('NOT_LEADER_OR_FOLLOWER')
      expect(notLeader.exception).toBe('NotLeaderOrFollowerException')
      expect(notLeader.message).toBe(
        'For requests intended only for the leader, this error indicates that the broker is not the current leader. ' +
          'For requests intended for any replica, this error indicates that the broker is not a replica of the topic partition.'
      )
    })

    it('stops at the end of the constant list', () => {
      expect(errors.map((e) => e.type)).not.toContain('CLASS_TO_ERROR')
    })

    it('rejects a file whose enum declaration is missing', () => {
      expect(() => parseErrorsJava('class Foo {}')).toThrow('public enum Errors {')
    })
  })

  describe('messageFiles', () => {
    const files = messageFiles(trackedApis.tracked)

    it('asks for a request and a response per tracked API, plus both headers', () => {
      expect(files).toHaveLength(trackedApis.tracked.length * 2 + 2)
      expect(files).toContain('RequestHeader.json')
      expect(files).toContain('ResponseHeader.json')
      expect(files).toContain('ProduceRequest.json')
      expect(files).toContain('ProduceResponse.json')
    })

    it('matches the committed snapshot exactly', () => {
      const snapshot = fs.readdirSync(path.join(__dirname, '..', 'messages', '4.3.1')).sort()
      expect(files).toEqual(snapshot)
    })
  })

  describe('parseExtendsClause', () => {
    it('reads the direct superclass from a class file with a license header and Javadoc', () => {
      const superclass = parseExtendsClause(readFixture('ExceptionClassSample.java'))
      expect(superclass).toBe('InvalidMetadataException')
    })

    it('returns null when the source declares no `extends` clause', () => {
      expect(parseExtendsClause('public class Foo {\n}\n')).toBeNull()
    })

    it('strips a fully-qualified superclass down to its simple name', () => {
      const source = 'public class Foo extends org.apache.kafka.common.KafkaException {\n}\n'
      expect(parseExtendsClause(source)).toBe('KafkaException')
    })
  })

  describe('resolvesToRetriable', () => {
    it('is true once the chain reaches RetriableException', () => {
      const extendsByName = new Map([
        ['UnknownTopicIdException', 'InvalidMetadataException'],
        ['InvalidMetadataException', 'RetriableException'],
      ])
      expect(resolvesToRetriable('UnknownTopicIdException', extendsByName)).toBe(true)
    })

    it('is false once the chain reaches a JDK exception root', () => {
      const extendsByName = new Map([
        ['ProducerFencedException', 'ApiException'],
        ['ApiException', 'KafkaException'],
        ['KafkaException', 'RuntimeException'],
      ])
      expect(resolvesToRetriable('ProducerFencedException', extendsByName)).toBe(false)
    })

    it('throws on a cycle instead of looping forever', () => {
      const extendsByName = new Map([
        ['A', 'B'],
        ['B', 'A'],
      ])
      expect(() => resolvesToRetriable('A', extendsByName)).toThrow('cycle')
    })

    it('throws when the chain leaves the resolved set', () => {
      expect(() => resolvesToRetriable('Unresolved', new Map())).toThrow(
        'no extends information resolved for Unresolved'
      )
    })
  })

  describe('resolveExceptionHierarchy (injected fetch, no network)', () => {
    const originalFetch = globalThis.fetch

    afterEach(() => {
      globalThis.fetch = originalFetch
    })

    /** A minimal fake source tree: url -> { ok, status, text }. */
    const fakeFetch = (sources) => (url) => {
      const entry = sources[url]
      if (entry === undefined) return Promise.resolve({ ok: false, status: 404 })
      return Promise.resolve({ ok: true, status: 200, text: () => Promise.resolve(entry) })
    }

    it('walks the chain across the default errors package', async () => {
      const tag = 'test-tag'
      const dir = 'clients/src/main/java/org/apache/kafka/common/errors'
      globalThis.fetch = jest.fn(
        fakeFetch({
          [`https://raw.githubusercontent.com/apache/kafka/${tag}/${dir}/UnknownTopicIdException.java`]:
            'public class UnknownTopicIdException extends InvalidMetadataException {\n}\n',
          [`https://raw.githubusercontent.com/apache/kafka/${tag}/${dir}/InvalidMetadataException.java`]:
            'public class InvalidMetadataException extends RetriableException {\n}\n',
        })
      )

      const result = await resolveExceptionHierarchy(tag, ['UnknownTopicIdException'], {})
      expect(result.get('UnknownTopicIdException')).toBe(true)
    })

    it('falls back to the override map when the default location 404s', async () => {
      const tag = 'test-tag'
      globalThis.fetch = jest.fn(
        fakeFetch({
          'https://raw.githubusercontent.com/apache/kafka/test-tag/clients/src/main/java/org/apache/kafka/common/KafkaException.java':
            'public class KafkaException extends RuntimeException {\n}\n',
        })
      )

      const overridePaths = {
        KafkaException: 'clients/src/main/java/org/apache/kafka/common/KafkaException.java',
      }
      const result = await resolveExceptionHierarchy(tag, ['KafkaException'], overridePaths)
      expect(result.get('KafkaException')).toBe(false)
    })

    it('reports every unresolved class instead of guessing', async () => {
      globalThis.fetch = jest.fn(fakeFetch({}))

      expect.assertions(2)
      try {
        await resolveExceptionHierarchy('test-tag', ['MysteryException'], {})
      } catch (error) {
        expect(error.message).toContain('MysteryException')
        expect(error.unresolved).toEqual(['MysteryException'])
      }
    })
  })

  describe('discover / safeDiscover (injected fetch, no network)', () => {
    const originalFetch = globalThis.fetch
    const originalToken = process.env.GITHUB_TOKEN
    let warnSpy
    let logSpy

    beforeEach(() => {
      warnSpy = jest.spyOn(console, 'warn').mockImplementation(() => {})
      logSpy = jest.spyOn(console, 'log').mockImplementation(() => {})
    })

    afterEach(() => {
      globalThis.fetch = originalFetch
      if (originalToken === undefined) delete process.env.GITHUB_TOKEN
      else process.env.GITHUB_TOKEN = originalToken
      warnSpy.mockRestore()
      logSpy.mockRestore()
    })

    it('safeDiscover warns and resolves (does not throw/exit) when the GitHub API rate-limits', async () => {
      globalThis.fetch = jest.fn().mockResolvedValue({
        ok: false,
        status: 403,
        statusText: 'rate limit exceeded',
      })

      await expect(safeDiscover('4.3.1', ['RequestHeader.json'])).resolves.toBeUndefined()

      expect(warnSpy).toHaveBeenCalledTimes(1)
      expect(warnSpy.mock.calls[0][0]).toContain('compat:fetch: discovery skipped —')
      expect(warnSpy.mock.calls[0][0]).toContain('403')
    })

    it('discover sends an Authorization header for the GitHub API call when GITHUB_TOKEN is set', async () => {
      process.env.GITHUB_TOKEN = 'test-token'
      const fetchMock = jest.fn().mockResolvedValue({
        ok: true,
        text: () => Promise.resolve('[]'),
      })
      globalThis.fetch = fetchMock

      await discover('4.3.1', ['RequestHeader.json'])

      expect(fetchMock).toHaveBeenCalledTimes(1)
      const [url, options] = fetchMock.mock.calls[0]
      expect(url).toContain('api.github.com')
      expect(options.headers.Authorization).toBe('Bearer test-token')
    })

    it('discover omits the Authorization header when GITHUB_TOKEN is unset', async () => {
      delete process.env.GITHUB_TOKEN
      const fetchMock = jest.fn().mockResolvedValue({
        ok: true,
        text: () => Promise.resolve('[]'),
      })
      globalThis.fetch = fetchMock

      await discover('4.3.1', ['RequestHeader.json'])

      const [, options] = fetchMock.mock.calls[0]
      expect(options.headers.Authorization).toBeUndefined()
    })
  })
})
