/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess;

import java.util.Objects;
import java.util.function.Predicate;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.vitess.connection.ReplicationMessage;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.schema.SchemaChangeEvent;

/**
 * Used by {@link EventDispatcher} to get the {@link SourceRecord} {@link Struct} and pass it to a
 * {@link Receiver}, which in turn enqueue the {@link SourceRecord} to {@link SchemaChangeEvent}.
 */
public class VitessDDLEmitter implements io.debezium.pipeline.spi.SchemaChangeEventEmitter {
    private static final Logger LOGGER = LoggerFactory.getLogger(VitessDDLEmitter.class);

    private final ReplicationMessage message;

    private final VitessOffsetContext offsetContext;

    private final VitessPartition partition;

    private final SchemaChangeEvent.SchemaChangeEventType eventType;
    private final TableId tableId;

    private final Table table;

    private final Predicate<String> ddlFilter;

    public VitessDDLEmitter(
                            VitessPartition partition,
                            VitessOffsetContext offsetContext,
                            Predicate<String> ddlFilter,
                            VitessDatabaseSchema schema,
                            ReplicationMessage message) {
        this.partition = partition;
        this.offsetContext = offsetContext;
        this.eventType = message.getSchemaChangeType();
        this.message = message;
        this.ddlFilter = ddlFilter;

        this.tableId = VitessDatabaseSchema.parse(message.getTable());
        Objects.requireNonNull(this.tableId);

        this.table = schema.tableFor(tableId);
        Objects.requireNonNull(table);
    }

    @Override
    public void emitSchemaChangeEvent(Receiver receiver) throws InterruptedException {
        SchemaChangeEvent event = getSchemaChangeEvent();
        if (event != null) {
            LOGGER.info("emitting schema change event {}", event);
            receiver.schemaChangeEvent(event);
        }
    }

    public SchemaChangeEvent getSchemaChangeEvent() {
        if (ddlFilter != null && ddlFilter.test(message.getDDL())) {
            LOGGER.debug("DDL '{}' was filtered out of processing", message.getDDL());
            return null;
        }

        final SchemaChangeEvent event = SchemaChangeEvent.of(
                eventType,
                partition,
                offsetContext,
                tableId.schema(),
                tableId.schema(),
                message.getDDL(),
                table,
                false);

        LOGGER.info("emitting schema change event {}", event);
        return event;
    }

}
