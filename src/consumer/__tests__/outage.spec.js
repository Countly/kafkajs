const Outage = require('../outage')
const { isConnectionLoss } = require('../../network/connectionFailure')
const {
  KafkaJSConnectionError,
  KafkaJSConnectionClosedError,
  KafkaJSRequestTimeoutError,
  KafkaJSInvalidFrameError,
  KafkaJSNumberOfRetriesExceeded,
  KafkaJSError,
} = require('../../errors')
const { createErrorFromCode } = require('../../protocol/error')

const refused = () =>
  new KafkaJSConnectionError('Connection error: connect ECONNREFUSED', { code: 'ECONNREFUSED' })
const exhausted = (cause) =>
  new KafkaJSNumberOfRetriesExceeded(cause, { retryCount: 5, retryTime: 100 })

describe('Network > isConnectionLoss', () => {
  it.each([
    ['a refused connection', refused()],
    ['a reset connection', new KafkaJSConnectionError('read ECONNRESET', { code: 'ECONNRESET' })],
    ['a closed connection', new KafkaJSConnectionClosedError('Closed connection', {})],
    ['a timed-out request', new KafkaJSRequestTimeoutError('Request Fetch timed out')],
    ['a retrier that gave up on a refused connection', exhausted(refused())],
  ])('is true for %s', (_name, error) => {
    expect(isConnectionLoss(error)).toBe(true)
  })

  it.each([
    ['an invalid frame', new KafkaJSInvalidFrameError('Unreadable response frame', {})],
    ['a retrier that gave up on something else', exhausted(new KafkaJSError('boom'))],
    ['a broker error', createErrorFromCode(27)],
    ['a plain error', new Error('ECONNRESET')],
    ['a thrown string', 'KafkaJSConnectionError'],
    ['nothing', undefined],
    [
      'a retrier whose cause cannot be read',
      Object.defineProperty({ name: 'KafkaJSNumberOfRetriesExceeded' }, 'cause', {
        get() {
          throw new Error('hostile')
        },
      }),
    ],
    [
      'an error whose name cannot be read',
      Object.defineProperty({}, 'name', {
        get() {
          throw new Error('hostile')
        },
      }),
    ],
  ])('is false for %s', (_name, error) => {
    expect(isConnectionLoss(error)).toBe(false)
  })
})

describe('Consumer > Outage', () => {
  it('rides out a lost connection and a moved or unavailable coordinator', () => {
    for (const error of [refused(), createErrorFromCode(15), createErrorFromCode(16)]) {
      expect(new Outage(1000).absorbs(error, 0)).toBe(true)
    }
  })

  it('rides out a coordinator that is still loading the group', () => {
    expect(new Outage(1000).absorbs(createErrorFromCode(14), 0)).toBe(true)
  })

  it('does not ride out any other error, and starts no outage for one', () => {
    const outage = new Outage(1000)

    for (const error of [createErrorFromCode(27), new Error('handler'), null, undefined]) {
      expect(outage.absorbs(error, 0)).toBe(false)
    }
    expect(outage.absorbs(refused(), 5000)).toBe(true)
    expect(outage.absorbs(refused(), 5999)).toBe(true)
    expect(outage.absorbs(refused(), 6000)).toBe(false)
  })

  it('stops riding once the outage has lasted the limit, counted from its first error', () => {
    const outage = new Outage(1000)

    expect(outage.absorbs(refused(), 100)).toBe(true)
    expect(outage.absorbs(refused(), 1099)).toBe(true)
    expect(outage.absorbs(refused(), 1100)).toBe(false)
    expect(outage.absorbs(refused(), 1101)).toBe(false)
  })

  it('keeps its deadline through slow attempts: only a success ends an outage', () => {
    const outage = new Outage(1000)

    expect(outage.absorbs(refused(), 0)).toBe(true)
    expect(outage.absorbs(refused(), 1000)).toBe(false)
    expect(outage.absorbs(refused(), 2000)).toBe(false)
    expect(outage.absorbs(refused(), 3000)).toBe(false)
  })

  it('starts a new outage after the path succeeds', () => {
    const outage = new Outage(1000)

    expect(outage.absorbs(refused(), 0)).toBe(true)
    expect(outage.absorbs(refused(), 500)).toBe(true)
    outage.end()
    expect(outage.absorbs(refused(), 900)).toBe(true)
    expect(outage.absorbs(refused(), 1800)).toBe(true)
    expect(outage.absorbs(refused(), 1900)).toBe(false)
  })
})
