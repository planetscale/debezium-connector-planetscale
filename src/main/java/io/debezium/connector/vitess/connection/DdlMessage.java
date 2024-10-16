/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess.connection;

import java.time.Instant;
import java.util.List;

import io.debezium.schema.SchemaChangeEvent;

/** Whether this message represents a DDL event. We don't have the DDL statement here because we don't need it.*/
public class DdlMessage implements ReplicationMessage {

    private final String transactionId;
    private final Instant commitTime;
    private final Operation operation;
    private final String ddlStatement;
    private final String tableName;

    private final SchemaChangeEvent.SchemaChangeEventType schemaChangeType;

    public DdlMessage(
                      String transactionId,
                      Instant commitTime,
                      String tableName,
                      String ddlStatement,
                      SchemaChangeEvent.SchemaChangeEventType schemaChangeType) {
        this.transactionId = transactionId;
        this.commitTime = commitTime;
        this.operation = Operation.DDL;
        this.ddlStatement = ddlStatement;
        this.tableName = tableName;
        this.schemaChangeType = schemaChangeType;
    }

    @Override
    public String getDDL() {
        return ddlStatement;
    }

    @Override
    public SchemaChangeEvent.SchemaChangeEventType getSchemaChangeType() {
        return schemaChangeType;
    }

    @Override
    public Operation getOperation() {
        return operation;
    }

    @Override
    public Instant getCommitTime() {
        return commitTime;
    }

    @Override
    public String getTransactionId() {
        return transactionId;
    }

    @Override
    public String getTable() {
        return tableName;
    }

    @Override
    public String getShard() {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<Column> getOldTupleList() {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<Column> getNewTupleList() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isTransactionalMessage() {
        return false;
    }

    @Override
    public String toString() {
        return "DdlMessage{"
                + "transactionId='"
                + transactionId
                + '\''
                + ", commitTime="
                + commitTime
                + ", operation="
                + operation
                + '}';
    }
}
