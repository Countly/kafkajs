/*
 * Copyright The OpenTelemetry Authors, Aspecto
 * SPDX-License-Identifier: Apache-2.0
 */

/**
 * The same as OpenTelemetry's `defaultTextMapGetter`, but it also reads a header whose value is a
 * Buffer, adding `toString()` so a string is always returned, and matches a key that differs only
 * in case.
 *
 * @type {import('@opentelemetry/api').TextMapGetter}
 */
const bufferTextMapGetter = {
  /**
   * @param {Record<string, unknown> | undefined} carrier the message headers
   * @param {string} key
   * @returns {string | undefined}
   */
  get(carrier, key) {
    if (!carrier) {
      return undefined
    }

    const keys = Object.keys(carrier)

    for (const carrierKey of keys) {
      if (carrierKey === key || carrierKey.toLowerCase() === key) {
        return carrier[carrierKey]?.toString()
      }
    }

    return undefined
  },

  /**
   * @param {Record<string, unknown> | undefined} carrier
   * @returns {string[]}
   */
  keys(carrier) {
    return carrier ? Object.keys(carrier) : []
  },
}

module.exports = { bufferTextMapGetter }
