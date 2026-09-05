/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 */
package org.apache.kafka.common.errors;

/**
 * Trimmed shape fixture for scripts/compat/__tests__/fetch.spec.js: a license header,
 * package declaration, Javadoc and a single class declaration with a superclass,
 * matching the real files under clients/src/main/java/org/apache/kafka/common/errors/.
 */
public class UnknownTopicIdException extends InvalidMetadataException {

    public UnknownTopicIdException(String message) {
        super(message);
    }
}
