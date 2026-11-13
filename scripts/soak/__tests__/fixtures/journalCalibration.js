'use strict'

const { FixedJournal } = require('../../../../testHelpers/fixedJournal')
const { slope } = require('../../bounds')
const { TELEMETRY } = require('../../soak')

/** Fixed-size scalar observations for a real-time, slope-and-endpoint-only calibration. */
class RetainedCalibration {
  /**
   * @param {object} options scalar observation sources
   * @param {object} options.memory the resource arms' ProcessMemory observer
   * @param {() => number} options.now monotonic milliseconds
   * @param {number} options.origin real phase origin in the same clock domain
   * @param {() => object} options.inventory observer WeakRefs, maps and buffer geometry
   * @param {() => object|null} [options.journals] workload storage at each boundary
   * @returns {RetainedCalibration} fully touched storage, reserved before M
   */
  constructor({ memory, now, origin, inventory, journals = () => null }) {
    Object.assign(this, { memory, now, origin, inventory, journals })
    this.rows = new FixedJournal({
      name: 'real-time calibration',
      capacity: TELEMETRY.sampleAt.length + 2,
      bytes: (TELEMETRY.sampleAt.length + 2) * 65536,
    })
    this.geometry = null
  }

  /**
   * @param {string} checkpoint M, sample label or closed
   * @param {number|null} [scheduledAtMs] original phase-relative admission grid time
   * @returns {Promise<object>} actual times, full settlement facts and observer inventory
   * @throws {Error} on observer growth, archive overflow or failed settlement
   */
  async take(checkpoint, scheduledAtMs = null) {
    const observer = { ...this.inventory() }
    const geometry = JSON.stringify(observer)
    if (this.geometry === null) this.geometry = geometry
    if (geometry !== this.geometry) throw new Error('observer storage changed')
    const admittedAt = this.now()
    const admittedAtMs = admittedAt - this.origin
    const memory = await this.memory.settled()
    const completedAt = this.now()
    const completedAtMs = completedAt - this.origin
    const row = {
      checkpoint,
      scheduledAtMs,
      origin: this.origin,
      admittedAt,
      completedAt,
      admittedAtMs,
      completedAtMs,
      observer,
      snapshot: {
        memory: { ...memory, retainedBytes: memory.retained },
        journals: this.journals(),
      },
    }
    this.rows.push(row)
    return row
  }

  /**
   * Uses the live limits and ordinary least squares without invoking identity, RSS or closed gates.
   * No unrelated full-verdict violations are manufactured by absent evidence.
   * @param {object} input real observations or deterministic evaluator vectors
   * @param {object[]} input.samples ordered post-settlement samples
   * @param {number} input.measurementMs actual span after the original warm-up
   * @returns {object} complete component verdict, including both measured values
   * @throws {Error} on an incomplete duration, grid, span or memory observation
   */
  static evaluate({ samples, measurementMs }) {
    if (
      measurementMs < TELEMETRY.measureMs ||
      !Number.isFinite(measurementMs) ||
      samples.length !== TELEMETRY.sampleAt.length
    )
      throw new Error('calibration duration or samples')
    let previous = -Infinity
    const points = samples.map(({ admittedAtMs, completedAtMs, snapshot }, index) => {
      const retained = snapshot?.memory?.retainedBytes
      if (
        !Number.isFinite(admittedAtMs) ||
        !Number.isFinite(completedAtMs) ||
        admittedAtMs < TELEMETRY.sampleAt[index] ||
        admittedAtMs <= previous ||
        completedAtMs < admittedAtMs ||
        completedAtMs > TELEMETRY.warmupMs + TELEMETRY.measureMs ||
        !Number.isSafeInteger(retained) ||
        retained < 0
      )
        throw new Error('invalid calibration sample')
      previous = admittedAtMs
      return [completedAtMs / 60000, retained]
    })
    if (
      samples[0].admittedAtMs > TELEMETRY.warmupMs + TELEMETRY.sampleMs ||
      samples.at(-1).admittedAtMs - samples[0].admittedAtMs < TELEMETRY.minSpanMs
    )
      throw new Error('invalid calibration span')
    const measured = {
      samples: samples.length,
      slopeBytesPerMinute: slope(points),
      endpointBytes: points.at(-1)[1] - points[0][1],
    }
    const violations = []
    if (measured.slopeBytesPerMinute > TELEMETRY.slopeBytesPerMinute)
      violations.push('retained slope')
    if (measured.endpointBytes > TELEMETRY.endpointBytes) violations.push('retained endpoint')
    return { scope: 'slope-and-endpoint-only', ok: violations.length === 0, violations, measured }
  }
}

module.exports = { RetainedCalibration }
