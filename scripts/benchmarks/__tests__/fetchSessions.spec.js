const { FetchObservation, verifyCycle, control, main } = require('../fetchSessions')
const { EventEmitter } = require('events')

test('full-cycle bytes are summed over three brokers with the declared partition shape', () => {
  const result = control()
  expect(result.shape).toEqual([334, 333, 333])
  expect(result.requests).toBe(3)
  expect(result.responses).toBe(3)
  expect(result.requestBytes).toBe(24042)
  expect(result.bytes).toBe(24066)
  expect(result.frames.filter((f) => f.direction === 'request').every((f) => f.bytes < 20000)).toBe(
    true
  )
})
test('idle incremental size and cycle membership have independent controls', () => {
  const frames = [0, 1, 2].map((nodeId) => ({
    direction: 'request',
    nodeId,
    bytes: 128,
  }))
  expect(verifyCycle(frames, { full: false })).toBe(384)
  expect(() =>
    verifyCycle([...frames.slice(0, 2), { ...frames[2], bytes: 129 }], {
      full: false,
    })
  ).toThrow('above 128')
  expect(() => verifyCycle(frames.slice(0, 2), { full: false })).toThrow('Incomplete')
  expect(() => verifyCycle(frames, { full: true })).toThrow('below 20000')
})
test('socket observation preserves write return, callback and arguments and restores on close', () => {
  const observation = new FetchObservation()
  const socket = new EventEmitter()
  const callback = () => {}
  socket.write = jest.fn(() => false)
  const original = socket.write
  const wrapped = observation.socketFactory(
    () => socket,
    () => 0
  )({ host: 'local', port: 1 })
  const request = Buffer.alloc(14)
  request.writeInt32BE(10)
  request.writeInt16BE(1, 4)
  request.writeInt32BE(8, 8)
  expect(wrapped.write(request, callback)).toBe(false)
  expect(original).toHaveBeenCalledWith(request, callback)
  const response = Buffer.alloc(8)
  response.writeInt32BE(4)
  response.writeInt32BE(8, 4)
  wrapped.emit('data', response)
  expect(observation.result()).toMatchObject({
    bytes: 22,
    requests: 1,
    responses: 1,
  })
  wrapped.emit('close')
  expect(wrapped.write).toBe(original)
  expect(wrapped.listenerCount('data')).toBe(0)
})
test('an idle observation without a socket counts no logical fetch as a frame', () => {
  expect(new FetchObservation().result()).toMatchObject({
    bytes: 0,
    requests: 0,
    responses: 0,
    frames: [],
  })
})

const header = (id) => {
  const request = Buffer.alloc(14)
  request.writeInt32BE(10)
  request.writeInt16BE(1, 4)
  request.writeInt32BE(id, 8)
  return request
}
const reply = (id) => {
  const response = Buffer.alloc(8)
  response.writeInt32BE(4)
  response.writeInt32BE(id, 4)
  return response
}
const factoryFor = (observation, write = () => true) =>
  observation.socketFactory(
    () => {
      const socket = new EventEmitter()
      socket.write = jest.fn(write)
      return socket
    },
    () => 0
  )

test('each connection a factory returns is its own socket, so reconnects reuse IDs safely', () => {
  const observation = new FetchObservation()
  const factory = factoryFor(observation)
  const first = factory({})
  const second = factory({})
  first.write(header(7))
  second.write(header(7))
  second.emit('data', reply(7))
  first.emit('data', reply(7))
  expect(observation.result()).toMatchObject({ requests: 2, responses: 2, bytes: 44 })
  expect(new Set(observation.result().frames.map((frame) => frame.connection)).size).toBe(2)
})

test('a failing write passes its error through and leaves the observation invalid', () => {
  const observation = new FetchObservation()
  const failure = new Error('EPIPE')
  const socket = factoryFor(observation, () => {
    throw failure
  })({})
  expect(() => socket.write(header(9), 'utf8')).toThrow(failure)
  expect(() => observation.result()).toThrow('Missing')
})

test('closing a socket that never carried traffic still removes its listeners', () => {
  const observation = new FetchObservation()
  const socket = factoryFor(observation)({})
  const wrapped = socket.write
  expect(socket.listenerCount('data')).toBe(1)
  socket.emit('close')
  expect(socket.listenerCount('data')).toBe(0)
  expect(socket.listenerCount('close')).toBe(0)
  expect(socket.write).not.toBe(wrapped)
  expect(jest.isMockFunction(socket.write)).toBe(true)
})

test('only the control entry point is available', () => {
  expect(() => main([])).toThrow('Only --control')
  expect(() => main(['--partitions', '1000'])).toThrow('Only --control')
  const write = jest.spyOn(process.stdout, 'write').mockImplementation(() => true)
  try {
    main(['--control'])
    expect(JSON.parse(write.mock.calls[0][0]).bytes).toBe(24066)
  } finally {
    write.mockRestore()
  }
})
