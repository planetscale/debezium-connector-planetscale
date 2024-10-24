/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.SnapshotRecord;
import io.debezium.connector.vitess.VitessConnectorConfig.SnapshotMode;
import io.debezium.connector.vitess.connection.ReplicationConnection;
import io.debezium.connector.vitess.connection.ReplicationMessage;
import io.debezium.connector.vitess.connection.ReplicationMessageProcessor;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.util.Clock;
import io.debezium.util.DelayStrategy;
import io.grpc.Status;

/**
 * Read events from source and dispatch each event using {@link EventDispatcher}
 * to the {@link
 * io.debezium.pipeline.source.spi.ChangeEventSource}. It runs in the
 * change-event-source-coordinator thread only.
 */
public class VitessStreamingChangeEventSource
        implements StreamingChangeEventSource<VitessPartition, VitessOffsetContext> {
    private static final Logger LOGGER = LoggerFactory.getLogger(VitessStreamingChangeEventSource.class);

    private final EventDispatcher<VitessPartition, TableId> dispatcher;
    private final ErrorHandler errorHandler;
    private final Clock clock;
    private final VitessDatabaseSchema schema;
    private final VitessConnectorConfig connectorConfig;
    private final ReplicationConnection replicationConnection;
    private final DelayStrategy pauseNoMessage;

    public VitessStreamingChangeEventSource(
                                            EventDispatcher<VitessPartition, TableId> dispatcher,
                                            ErrorHandler errorHandler,
                                            Clock clock,
                                            VitessDatabaseSchema schema,
                                            VitessConnectorConfig connectorConfig,
                                            ReplicationConnection replicationConnection) {
        this.dispatcher = dispatcher;
        this.errorHandler = errorHandler;
        this.clock = clock;
        this.schema = schema;
        this.connectorConfig = connectorConfig;
        this.replicationConnection = replicationConnection;
        this.pauseNoMessage = DelayStrategy.constant(connectorConfig.getPollInterval());

        LOGGER.info("VitessStreamingChangeEventSource is created");
    }

    @Override
    public void execute(ChangeEventSourceContext context, VitessPartition partition, VitessOffsetContext offsetContext) {
        if (offsetContext == null) {
            boolean snapshot = connectorConfig.getSnapshotMode() != SnapshotMode.NEVER;
            offsetContext = VitessOffsetContext.initialContext(snapshot, connectorConfig, clock);
        }
        else {
            // XXX(maxenglander): this is an ugly hack to ensure that records
            // produced after VStream copy are not marked as snapshot records.
            //
            // This is not usually necessary, but one circumstance where it is
            // needed is when snapshot mode is INITIAL, and the connector is
            // stopped immediately after the VStream copy is completed.
            offsetContext.markSnapshotRecord(SnapshotRecord.FALSE);
        }

        try {
            AtomicReference<Throwable> error = new AtomicReference<>();
            replicationConnection.startStreaming(
                    offsetContext, newReplicationMessageProcessor(partition, offsetContext), error);

            while (context.isRunning() && error.get() == null) {
                pauseNoMessage.sleepWhen(true);
            }
            if (error.get() != null) {
                LOGGER.error("Error during streaming", error.get());
                throw error.get();
            }
        }
        catch (Throwable e) {
            Status s = Status.fromThrowable(e);
            if (s.getCode() == Status.Code.CANCELLED && s.getCause() instanceof VStreamCopyCompletedEventException) {
                LOGGER.info("VStream stopped after COPY_COMPLETED event");
            }
            else {
                errorHandler.setProducerThrowable(e);
            }
        }
        finally {
            try {
                // closing the connection should also disconnect the VStream gRPC channel
                replicationConnection.close();
            }
            catch (Exception e) {
                LOGGER.error("Failed to close replicationConnection", e);
            }
        }
    }

    private ReplicationMessageProcessor newReplicationMessageProcessor(VitessPartition partition,
                                                                       VitessOffsetContext offsetContext) {
        return (message, newVgtid, isLastRowOfTransaction) -> {
            if (message.isTransactionalMessage()) {
                // Tx BEGIN/END event
                offsetContext.rotateVgtid(newVgtid, message.getCommitTime());
                if (message.getOperation() == ReplicationMessage.Operation.BEGIN) {
                    // send to transaction topic
                    dispatcher.dispatchTransactionStartedEvent(partition, message.getTransactionId(), offsetContext,
                            message.getCommitTime());
                }
                else if (message.getOperation() == ReplicationMessage.Operation.COMMIT) {
                    // send to transaction topic
                    dispatcher.dispatchTransactionCommittedEvent(partition, offsetContext, message.getCommitTime());
                }
                return;
            }
            else if (message.getOperation() == ReplicationMessage.Operation.OTHER) {
                offsetContext.rotateVgtid(newVgtid, message.getCommitTime());
            }
            else if (message.getOperation() == ReplicationMessage.Operation.DDL) {
                // DDL event
                offsetContext.rotateVgtid(newVgtid, message.getCommitTime());

                TableId tableId = VitessDatabaseSchema.parse(message.getTable());
                Objects.requireNonNull(tableId);
                LOGGER.info("found Table ID '{}', in DDL statement '{}'", tableId, message.getDDL());

                Table table = schema.tableFor(tableId);
                if (table == null) {
                    LOGGER.info("Table '{}' not found in schema, skipping DDL event", tableId);
                    return;
                }

                offsetContext.event(tableId, message.getCommitTime());
                offsetContext.setShard("-");
                LOGGER.info("calling dispatchSchemaChangeEvent for tableId {}", tableId);
                dispatcher.dispatchSchemaChangeEvent(
                        partition,
                        offsetContext,
                        tableId,
                        new VitessDDLEmitter(
                                partition, offsetContext, connectorConfig.ddlFilter(), schema, message));
            }
            else {
                // DML event
                TableId tableId = VitessDatabaseSchema.parse(message.getTable());
                Objects.requireNonNull(tableId);

                offsetContext.event(tableId, message.getCommitTime());
                offsetContext.setShard(message.getShard());
                if (isLastRowOfTransaction) {
                    // Right before processing the last row, reset the previous offset to the new
                    // vgtid so the last row has the new vgtid as offset.
                    offsetContext.resetVgtid(newVgtid, message.getCommitTime());
                }

                dispatcher.dispatchDataChangeEvent(
                        partition,
                        tableId,
                        new VitessChangeRecordEmitter(
                                partition, offsetContext, clock, connectorConfig, schema, message));
            }
        };
    }
}
