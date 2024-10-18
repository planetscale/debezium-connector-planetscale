/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess;

/**
 * Used to signal that Debezium should not continue consuming the streaming event source after encountering a COPY_COMPLETED VEvent from the VStream and when snapshot.mode=initial_only.
 */
public class VStreamCopyCompletedEventException extends RuntimeException {
}
