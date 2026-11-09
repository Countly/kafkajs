const { TelemetryWorkerProcess, WORKER_ENV } = require('../../telemetryChurn')

/** Exercises terminal IPC backpressure without constructing a client or running a soak. */
const publish = async () => {
  const worker = new TelemetryWorkerProcess({ kind: process.env[WORKER_ENV], soak: {} })
  await worker.send({ type: 'hello' })
  await worker.send({ type: 'progress', row: { kind: 'verdict', ok: false } })
  await worker.send(
    { type: process.argv[2], facts: { ledger: 'x'.repeat(2 * 1024 * 1024) } },
    { flush: true }
  )
  process.disconnect()
  process.exitCode = 1
}

publish().catch((error) => {
  console.error(error)
  process.exitCode = 1
  if (process.connected) process.disconnect()
})
