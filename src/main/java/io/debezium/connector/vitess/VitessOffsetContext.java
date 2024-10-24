/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.SnapshotRecord;
import io.debezium.connector.vitess.connection.VitessReplicationConnection;
import io.debezium.pipeline.CommonOffsetContext;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.txmetadata.TransactionContext;
import io.debezium.relational.TableId;
import io.debezium.spi.schema.DataCollectionId;
import io.debezium.util.Clock;

/**
 * Current offset of the connector. There is only one instance per connector
 * setup. We need to
 * update the offset by calling the APIs provided by this class, every time we
 * process a new
 * ReplicationMessage.
 */
public class VitessOffsetContext extends CommonOffsetContext<SourceInfo> {
    private static final Logger LOGGER = LoggerFactory.getLogger(VitessOffsetContext.class);
    private static final String SNAPSHOT_COMPLETED_KEY = "snapshot_completed";

    private boolean snapshotCompleted;
    private final Schema sourceInfoSchema;
    private final TransactionContext transactionContext;

    public VitessOffsetContext(
                               boolean snapshot,
                               boolean snapshotCompleted,
                               VitessConnectorConfig connectorConfig,
                               Vgtid initialVgtid,
                               Instant time,
                               TransactionContext transactionContext) {
        super(new SourceInfo(connectorConfig));
        this.snapshotCompleted = snapshotCompleted;
        if (this.snapshotCompleted) {
            postSnapshotCompletion();
        }
        else {
            sourceInfo.setSnapshot(snapshot ? SnapshotRecord.TRUE : SnapshotRecord.FALSE);
        }
        this.sourceInfo.resetVgtid(initialVgtid, time);
        this.sourceInfoSchema = sourceInfo.schema();
        this.transactionContext = transactionContext;
    }

    /** Initialize VitessOffsetContext if no previous offset exists */
    public static VitessOffsetContext initialContext(
                                                     boolean snapshot, VitessConnectorConfig connectorConfig, Clock clock) {
        LOGGER.info("No previous offset exists. Use default VGTID.");
        final Vgtid defaultVgtid = VitessReplicationConnection.defaultVgtid(connectorConfig);
        return new VitessOffsetContext(
                snapshot, false, connectorConfig, defaultVgtid, clock.currentTimeAsInstant(), new TransactionContext());
    }

    /**
     * Rotate current and restart vgtid. Only rotate wen necessary.
     */
    public void rotateVgtid(Vgtid newVgtid, Instant commitTime) {
        sourceInfo.rotateVgtid(newVgtid, commitTime);
    }

    public void resetVgtid(Vgtid newVgtid, Instant commitTime) {
        sourceInfo.resetVgtid(newVgtid, commitTime);
    }

    public Vgtid getRestartVgtid() {
        return sourceInfo.getRestartVgtid();
    }

    public void setShard(String shard) {
        sourceInfo.setShard(shard);
    }

    /**
     * Calculate and return the offset that will be used to create the
     * {@link SourceRecord}.
     *
     * @return
     */
    @Override
    public Map<String, ?> getOffset() {
        Map<String, Object> result = new HashMap<>();
        if (sourceInfo.getRestartVgtid() != null) {
            result.put(SourceInfo.VGTID_KEY, sourceInfo.getRestartVgtid().toString());
        }
        if (sourceInfo.isSnapshot()) {
            if (!snapshotCompleted) {
                result.put(SourceInfo.SNAPSHOT_KEY, true);
            }
        }
        // put OFFSET_TRANSACTION_ID
        return transactionContext.store(result);
    }

    @Override
    public Schema getSourceInfoSchema() {
        return sourceInfoSchema;
    }

    @Override
    public boolean isSnapshotRunning() {
        return sourceInfo.isSnapshot() && !snapshotCompleted;
    }

    @Override
    public void preSnapshotStart() {
        sourceInfo.setSnapshot(SnapshotRecord.TRUE);
        snapshotCompleted = false;
    }

    @Override
    public void preSnapshotCompletion() {
        snapshotCompleted = true;
    }

    @Override
    public void event(DataCollectionId collectionId, Instant timestamp) {
        sourceInfo.setTimestamp(timestamp);
        sourceInfo.setTableId((TableId) collectionId);
    }

    @Override
    public TransactionContext getTransactionContext() {
        return transactionContext;
    }

    @Override
    public String toString() {
        return "VitessOffsetContext{"
                + "sourceInfo="
                + sourceInfo
                + '}';
    }

    public static class Loader implements OffsetContext.Loader<VitessOffsetContext> {

        private final VitessConnectorConfig connectorConfig;

        public Loader(VitessConnectorConfig connectorConfig) {
            this.connectorConfig = connectorConfig;
        }

        @Override
        public VitessOffsetContext load(Map<String, ?> offset) {
            final boolean snapshot = Boolean.TRUE.equals(offset.get(SourceInfo.SNAPSHOT_KEY))
                    || "true".equals(offset.get(SourceInfo.SNAPSHOT_KEY));
            final boolean snapshotCompleted = Boolean.TRUE.equals(offset.get(SNAPSHOT_COMPLETED_KEY))
                    || "true".equals(offset.get(SNAPSHOT_COMPLETED_KEY));
            final String vgtid = (String) offset.get(SourceInfo.VGTID_KEY);
            return new VitessOffsetContext(
                    snapshot,
                    snapshotCompleted,
                    connectorConfig,
                    Vgtid.of(vgtid),
                    null,
                    TransactionContext.load(offset));
        }
    }
}
