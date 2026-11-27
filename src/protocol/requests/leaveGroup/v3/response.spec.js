const { decode, parse } = require('./response')

describe('Protocol > Requests > LeaveGroup > v3', () => {
  test('response', async () => {
    const data = await decode(Buffer.from(require('../fixtures/v3_response.json')))
    expect(data).toEqual({
      throttleTime: 0,
      clientSideThrottleTime: 0,
      errorCode: 0,
      members: [
        {
          memberId: 'test-42962f68-e801-4cd8-b359-2d862ebb4d05',
          groupInstanceId: null,
          errorCode: 0,
        },
      ],
    })
    await expect(parse(data)).resolves.toBeTruthy()
  })

  test('a top-level error beside a member error rejects with the top-level error and leaves no unhandled rejection', async () => {
    const unhandled = []
    const onUnhandled = (reason) => unhandled.push(reason)
    process.on('unhandledRejection', onUnhandled)
    try {
      await expect(
        parse({
          throttleTime: 0,
          clientSideThrottleTime: 0,
          errorCode: 15,
          members: [{ memberId: 'member-1', groupInstanceId: null, errorCode: 25 }],
        })
      ).rejects.toMatchObject({ code: 15 })
      await new Promise((resolve) => setImmediate(resolve))
      await new Promise((resolve) => setImmediate(resolve))
    } finally {
      process.off('unhandledRejection', onUnhandled)
    }

    expect(unhandled).toEqual([])
  })
})
