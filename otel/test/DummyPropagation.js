/*
 * Copyright The OpenTelemetry Authors, Aspecto
 * SPDX-License-Identifier: Apache-2.0
 */

const { TraceFlags, trace } = require('@opentelemetry/api')

/** A propagator that writes the trace and span ids to two plain headers. */
class DummyPropagation {
  static TRACE_CONTEXT_KEY = 'x-dummy-trace-id'
  static SPAN_CONTEXT_KEY = 'x-dummy-span-id'

  /**
   * @param {import('@opentelemetry/api').Context} context
   * @param {unknown} carrier
   * @param {import('@opentelemetry/api').TextMapGetter} getter
   * @returns {import('@opentelemetry/api').Context} the context with the carrier's span context,
   *   or the context unchanged when the carrier has none
   */
  extract(context, carrier, getter) {
    const extractedSpanContext = {
      traceId: getter.get(carrier, DummyPropagation.TRACE_CONTEXT_KEY),
      spanId: getter.get(carrier, DummyPropagation.SPAN_CONTEXT_KEY),
      traceFlags: TraceFlags.SAMPLED,
    }

    if (!extractedSpanContext.traceId || !extractedSpanContext.spanId) return context

    return trace.setSpanContext(context, extractedSpanContext)
  }

  /**
   * @param {import('@opentelemetry/api').Context} context
   * @param {unknown} carrier
   * @param {import('@opentelemetry/api').TextMapSetter} setter
   * @returns {void}
   */
  inject(context, carrier, setter) {
    const spanContext = trace.getSpanContext(context)
    if (!spanContext) return

    setter.set(carrier, DummyPropagation.TRACE_CONTEXT_KEY, spanContext.traceId)
    setter.set(carrier, DummyPropagation.SPAN_CONTEXT_KEY, spanContext.spanId)
  }

  /** @returns {string[]} */
  fields() {
    return [DummyPropagation.TRACE_CONTEXT_KEY, DummyPropagation.SPAN_CONTEXT_KEY]
  }
}

module.exports = { DummyPropagation }
