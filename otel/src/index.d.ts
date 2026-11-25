/*
 * Copyright The OpenTelemetry Authors, Aspecto
 * SPDX-License-Identifier: Apache-2.0
 */

/// <reference types="node" />

import type { Span } from '@opentelemetry/api'
import {
  InstrumentationBase,
  InstrumentationConfig,
  InstrumentationNodeModuleDefinition,
} from '@opentelemetry/instrumentation'

/** The message a hook receives: a produced message, or a consumed one. */
export interface KafkajsMessage {
  key?: Buffer | string | null
  value: Buffer | string | null
  partition?: number
  /** Set on a consumed message. */
  offset?: string
  headers?: Record<string, Buffer | string | null | (Buffer | string | null)[] | undefined>
  /**
   * A consumed message's is a string; a producer hook sees what the application passed: a number,
   * a numeric string, a `Date`, a `bigint`, a `Long` (an object with a `bigint` `value`), or
   * `null` or `undefined` for none.
   */
  timestamp?: string | number | Date | bigint | { value: bigint } | null | undefined
}

export interface MessageInfo<T = KafkajsMessage> {
  topic: string
  message: T
}

export interface KafkaProducerCustomAttributeFunction<T = KafkajsMessage> {
  (span: Span, info: MessageInfo<T>): void
}

export interface KafkaConsumerCustomAttributeFunction<T = KafkajsMessage> {
  (span: Span, info: MessageInfo<T>): void
}

export interface KafkaJsInstrumentationConfig extends InstrumentationConfig {
  /** hook for adding custom attributes before producer message is sent */
  producerHook?: KafkaProducerCustomAttributeFunction | undefined

  /** hook for adding custom attributes before consumer message is processed */
  consumerHook?: KafkaConsumerCustomAttributeFunction | undefined
}

/** OpenTelemetry instrumentation for `@countly/kafkajs` 3.x. */
export declare class KafkaJsInstrumentation extends InstrumentationBase<KafkaJsInstrumentationConfig> {
  constructor(config?: KafkaJsInstrumentationConfig)
  protected init(): InstrumentationNodeModuleDefinition[]
}
