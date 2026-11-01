const RequestQueueStamp = require('../requestQueueStamp')

test('the request fixture traverses preparation and fulfillment for each pair', async () => {
  const gc = globalThis.gc
  globalThis.gc = () => {}
  try {
    const result = await new RequestQueueStamp({ pairs: 20 }).run()
    expect(result).toMatchObject({
      pairs: 20,
      prepared: 20,
      written: 20,
      fulfilled: 20,
      extraClocks: 0,
    })
    expect(result.nsPerPair).toBeGreaterThan(0)
  } finally {
    globalThis.gc = gc
  }
})
test('the positive control counts exactly ten extra clock calls per completed pair', async () => {
  const gc = globalThis.gc
  globalThis.gc = () => {}
  try {
    expect(await new RequestQueueStamp({ pairs: 20, control: 1 }).run()).toMatchObject({
      extraClocks: 200,
    })
  } finally {
    globalThis.gc = gc
  }
})
test('a no-op send cannot satisfy actual work assertions', async () => {
  const gc = globalThis.gc
  globalThis.gc = () => {}
  const driver = new RequestQueueStamp({ pairs: 1 })
  driver.connection.send = async () => {}
  try {
    await expect(driver.run()).rejects.toThrow()
  } finally {
    globalThis.gc = gc
  }
})
test('a completion omitted from the real queue cannot satisfy actual work assertions', async () => {
  const gc = globalThis.gc
  globalThis.gc = () => {}
  const driver = new RequestQueueStamp({ pairs: 1 })
  const send = driver.connection.send.bind(driver.connection)
  driver.connection.send = async (protocol) => {
    await send(protocol)
    driver.fulfilled = 0
  }
  try {
    await expect(driver.run()).rejects.toThrow()
  } finally {
    globalThis.gc = gc
  }
})
test.each([0, -1, 1.5, NaN])('refuses an invalid pair count %#', (pairs) => {
  expect(() => new RequestQueueStamp({ pairs })).toThrow()
})

test('an omitted preparation count cannot satisfy actual work assertions', async () => {
  const gc = globalThis.gc
  globalThis.gc = () => {}
  const driver = new RequestQueueStamp({ pairs: 1 })
  Object.defineProperty(driver, 'prepared', { get: () => 0, set: () => {} })
  try {
    await expect(driver.run()).rejects.toThrow()
  } finally {
    globalThis.gc = gc
  }
})
