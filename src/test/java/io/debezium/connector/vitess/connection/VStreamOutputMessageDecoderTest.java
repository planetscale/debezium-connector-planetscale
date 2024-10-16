/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess.connection;

import static junit.framework.Assert.assertEquals;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.fail;

import java.util.function.Predicate;

import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.vitess.AnonymousValue;
import io.debezium.connector.vitess.TestHelper;
import io.debezium.connector.vitess.Vgtid;
import io.debezium.connector.vitess.VgtidTest;
import io.debezium.connector.vitess.VitessConnectorConfig;
import io.debezium.connector.vitess.VitessDatabaseSchema;
import io.debezium.doc.FixFor;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.schema.DefaultTopicNamingStrategy;
import io.debezium.schema.SchemaChangeEvent;
import io.debezium.schema.SchemaNameAdjuster;
import io.debezium.spi.topic.TopicNamingStrategy;
import io.vitess.proto.Query;

import binlogdata.Binlogdata;

public class VStreamOutputMessageDecoderTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(VStreamOutputMessageDecoderTest.class);

    private VitessConnectorConfig connectorConfig;
    private VitessDatabaseSchema schema;
    private VStreamOutputMessageDecoder decoder;

    @Before
    public void before() {
        connectorConfig = new VitessConnectorConfig(TestHelper.defaultConfig().build());
        schema = new VitessDatabaseSchema(
                connectorConfig,
                SchemaNameAdjuster.create(),
                (TopicNamingStrategy) DefaultTopicNamingStrategy.create(connectorConfig));
        decoder = new VStreamOutputMessageDecoder(schema, connectorConfig.ddlFilter());
    }

    @Test
    public void shouldProcessBeginEvent() throws Exception {
        // setup fixture
        Binlogdata.VEvent event = Binlogdata.VEvent.newBuilder()
                .setType(Binlogdata.VEventType.BEGIN)
                .setTimestamp(AnonymousValue.getLong())
                .build();
        Vgtid newVgtid = Vgtid.of(VgtidTest.VGTID_JSON);

        // exercise SUT
        final boolean[] processed = { false };
        decoder.processMessage(
                event,
                (message, vgtid, isLastRowEventOfTransaction) -> {
                    // verify outcome
                    assertThat(message).isNotNull();
                    assertThat(message).isInstanceOf(TransactionalMessage.class);
                    assertThat(message.getOperation()).isEqualTo(ReplicationMessage.Operation.BEGIN);
                    assertThat(message.getTransactionId()).isEqualTo(newVgtid.toString());
                    assertThat(vgtid).isEqualTo(newVgtid);
                    processed[0] = true;
                },
                newVgtid,
                false,
                false,
                null);
        assertThat(processed[0]).isTrue();
    }

    @Test
    @FixFor("DBZ-4667")
    public void shouldNotProcessBeginEventIfNoVgtid() throws Exception {
        // setup fixture
        Binlogdata.VEvent event = Binlogdata.VEvent.newBuilder()
                .setType(Binlogdata.VEventType.BEGIN)
                .setTimestamp(AnonymousValue.getLong())
                .build();

        // exercise SUT
        final boolean[] processed = { false };
        decoder.processMessage(
                event,
                (message, vgtid, isLastRowEventOfTransaction) -> {
                    // verify outcome
                    assertThat(message).isNotNull();
                    assertThat(message).isInstanceOf(TransactionalMessage.class);
                    assertThat(message.getOperation()).isEqualTo(ReplicationMessage.Operation.BEGIN);
                    processed[0] = true;
                },
                null,
                false,
                false,
                null);
        assertThat(processed[0]).isFalse();
    }

    @Test
    public void shouldProcessCommitEvent() throws Exception {
        // setup fixture
        Binlogdata.VEvent event = Binlogdata.VEvent.newBuilder()
                .setType(Binlogdata.VEventType.COMMIT)
                .setTimestamp(AnonymousValue.getLong())
                .build();
        Vgtid newVgtid = Vgtid.of(VgtidTest.VGTID_JSON);
        decoder.setTransactionId(newVgtid.toString());

        // exercise SUT
        final boolean[] processed = { false };
        decoder.processMessage(
                event,
                (message, vgtid, isLastRowEventOfTransaction) -> {
                    // verify outcome
                    assertThat(message).isNotNull();
                    assertThat(message).isInstanceOf(TransactionalMessage.class);
                    assertThat(message.getOperation()).isEqualTo(ReplicationMessage.Operation.COMMIT);
                    assertThat(message.getTransactionId()).isEqualTo(newVgtid.toString());
                    assertThat(vgtid).isEqualTo(newVgtid);
                    processed[0] = true;
                },
                newVgtid,
                false,
                false,
                null);
        assertThat(processed[0]).isTrue();
    }

    @Test
    @FixFor("DBZ-4667")
    public void shouldNotProcessCommitEventIfNoVgtid() throws Exception {
        // setup fixture
        Binlogdata.VEvent event = Binlogdata.VEvent.newBuilder()
                .setType(Binlogdata.VEventType.COMMIT)
                .setTimestamp(AnonymousValue.getLong())
                .build();

        // exercise SUT
        final boolean[] processed = { false };
        decoder.processMessage(
                event,
                (message, vgtid, isLastRowEventOfTransaction) -> {
                    // verify outcome
                    assertThat(message).isNotNull();
                    assertThat(message).isInstanceOf(TransactionalMessage.class);
                    assertThat(message.getOperation()).isEqualTo(ReplicationMessage.Operation.COMMIT);
                    processed[0] = true;
                },
                null,
                false,
                false,
                null);
        assertThat(processed[0]).isFalse();
    }

    @Test
    public void shouldProcessAlterDdlEvent() throws Exception {
        // setup fixture
        Binlogdata.VEvent event = Binlogdata.VEvent.newBuilder()
                .setType(Binlogdata.VEventType.DDL)
                .setKeyspace("customers")
                .setShard("-")
                .setTimestamp(AnonymousValue.getLong())
                .setStatement("ALTER TABLE foo ADD bar INT default 10")
                .build();

        // exercise SUT
        final boolean[] processed = { false };
        decoder.processMessage(
                event,
                (message, vgtid, isLastRowEventOfTransaction) -> {
                    // verify outcome
                    assertThat(message).isNotNull();
                    assertThat(message).isInstanceOf(DdlMessage.class);
                    assertThat(message.getOperation()).isEqualTo(ReplicationMessage.Operation.DDL);
                    assertThat(message.getTable()).isEqualTo("-.customers.foo");
                    assertThat(message.getDDL()).isEqualTo("ALTER TABLE foo ADD bar INT default 10");
                    assertThat(message.getSchemaChangeType()).isEqualTo(SchemaChangeEvent.SchemaChangeEventType.ALTER);
                    processed[0] = true;
                },
                null,
                false,
                false,
                null);
        assertThat(processed[0]).isTrue();
    }

    @Test
    public void shouldSkipIgnoredDdl() throws Exception {
        Predicate<String> ignoreAlters = (ddl) -> ddl.toUpperCase().contains("ALTER");
        VStreamOutputMessageDecoder ignoredDDLDecoder = new VStreamOutputMessageDecoder(schema, ignoreAlters);
        // setup fixture
        Binlogdata.VEvent event = Binlogdata.VEvent.newBuilder()
                .setType(Binlogdata.VEventType.DDL)
                .setKeyspace("customers")
                .setShard("-")
                .setTimestamp(AnonymousValue.getLong())
                .setStatement("ALTER TABLE foo ADD bar INT default 10")
                .build();

        ignoredDDLDecoder.processMessage(
                event,
                (message, vgtid, isLastRowEventOfTransaction) -> {
                    fail("should not reach here");
                },
                null,
                false,
                false,
                null);
    }

    @Test
    public void shouldHandleTableNamesWithKeyspace() throws Exception {
        // setup fixture
        Binlogdata.VEvent event = Binlogdata.VEvent.newBuilder()
                .setType(Binlogdata.VEventType.DDL)
                .setKeyspace("customers")
                .setShard("-")
                .setTimestamp(AnonymousValue.getLong())
                .setStatement("ALTER TABLE customers.foo ADD bar INT default 10")
                .build();

        // exercise SUT
        final boolean[] processed = { false };
        decoder.processMessage(
                event,
                (message, vgtid, isLastRowEventOfTransaction) -> {
                    // verify outcome
                    assertThat(message).isNotNull();
                    assertThat(message).isInstanceOf(DdlMessage.class);
                    assertThat(message.getOperation()).isEqualTo(ReplicationMessage.Operation.DDL);
                    assertThat(message.getTable()).isEqualTo("-.customers.foo");
                    assertThat(message.getDDL()).isEqualTo("ALTER TABLE customers.foo ADD bar INT default 10");
                    assertThat(message.getSchemaChangeType()).isEqualTo(SchemaChangeEvent.SchemaChangeEventType.ALTER);
                    processed[0] = true;
                },
                null,
                false,
                false,
                null);
        assertThat(processed[0]).isTrue();
    }

    @Test
    public void shouldProcessDropDdlEvent() throws Exception {
        // setup fixture
        Binlogdata.VEvent event = Binlogdata.VEvent.newBuilder()
                .setType(Binlogdata.VEventType.DDL)
                .setKeyspace("customers")
                .setShard("-")
                .setTimestamp(AnonymousValue.getLong())
                .setStatement("DROP TABLE foo")
                .build();

        // exercise SUT
        final boolean[] processed = { false };
        decoder.processMessage(
                event,
                (message, vgtid, isLastRowEventOfTransaction) -> {
                    // verify outcome
                    assertThat(message).isNotNull();
                    assertThat(message).isInstanceOf(DdlMessage.class);
                    assertThat(message.getOperation()).isEqualTo(ReplicationMessage.Operation.DDL);
                    assertThat(message.getTable()).isEqualTo("-.customers.foo");
                    assertThat(message.getDDL()).isEqualTo("DROP TABLE foo");
                    assertThat(message.getSchemaChangeType()).isEqualTo(SchemaChangeEvent.SchemaChangeEventType.DROP);
                    processed[0] = true;
                },
                null,
                false,
                false,
                null);
        assertThat(processed[0]).isTrue();
    }

    @Test
    public void shouldSupportTruncateTableEvent() throws Exception {
        // setup fixture
        Binlogdata.VEvent event = Binlogdata.VEvent.newBuilder()
                .setType(Binlogdata.VEventType.DDL)
                .setKeyspace("customers")
                .setShard("-")
                .setTimestamp(AnonymousValue.getLong())
                .setStatement("TRUNCATE TABLE foo")
                .build();

        // exercise SUT
        final boolean[] processed = { false };
        decoder.processMessage(
                event,
                (message, vgtid, isLastRowEventOfTransaction) -> {
                    // verify outcome
                    assertThat(message).isNotNull();
                    assertThat(message).isInstanceOf(VStreamOutputReplicationMessage.class);
                    assertThat(message.getOperation()).isEqualTo(ReplicationMessage.Operation.TRUNCATE);
                    assertThat(message.getTable()).isEqualTo(VitessDatabaseSchema.buildTableId("-", "customers", "foo").toDoubleQuotedString());
                    processed[0] = true;
                },
                null,
                false,
                false,
                null);
        assertThat(processed[0]).isTrue();
    }

    @Test
    public void shouldSkipInvalidDDLEvent() throws Exception {
        // setup fixture
        Binlogdata.VEvent event = Binlogdata.VEvent.newBuilder()
                .setType(Binlogdata.VEventType.DDL)
                .setKeyspace("customers")
                .setShard("-")
                .setTimestamp(AnonymousValue.getLong())
                .setStatement("Hello World")
                .build();

        Exception exception = assertThrows(InterruptedException.class, () -> {
            decoder.processMessage(
                    event,
                    (message, vgtid, isLastRowEventOfTransaction) -> {
                        fail("should not reach here");
                    },
                    null,
                    false,
                    false,
                    null);
        });
        assertEquals("Unable to parse DDL 'Hello World', skipping DDL", exception.getMessage());
    }

    @Test
    public void shouldProcessCreateDdlEvent() throws Exception {
        String stmt = "CREATE TABLE pet (name VARCHAR(20), owner VARCHAR(20),\n" +
                "       species VARCHAR(20), sex CHAR(1), birth DATE, death DATE);";
        // setup fixture
        Binlogdata.VEvent event = Binlogdata.VEvent.newBuilder()
                .setType(Binlogdata.VEventType.DDL)
                .setKeyspace("customers")
                .setShard("-")
                .setTimestamp(AnonymousValue.getLong())
                .setStatement(stmt)
                .build();

        // exercise SUT
        final boolean[] processed = { false };
        decoder.processMessage(
                event,
                (message, vgtid, isLastRowEventOfTransaction) -> {
                    // verify outcome
                    assertThat(message).isNotNull();
                    assertThat(message).isInstanceOf(DdlMessage.class);
                    assertThat(message.getOperation()).isEqualTo(ReplicationMessage.Operation.DDL);
                    assertThat(message.getTable()).isEqualTo("-.customers.pet");
                    assertThat(message.getDDL()).isEqualTo(stmt);
                    assertThat(message.getSchemaChangeType()).isEqualTo(SchemaChangeEvent.SchemaChangeEventType.CREATE);
                    processed[0] = true;
                },
                null,
                false,
                false,
                null);
        assertThat(processed[0]).isTrue();
    }

    @Test
    public void shouldProcessOtherEvent() throws Exception {
        // setup fixture
        Binlogdata.VEvent event = Binlogdata.VEvent.newBuilder()
                .setType(Binlogdata.VEventType.OTHER)
                .setTimestamp(AnonymousValue.getLong())
                .build();

        // exercise SUT
        final boolean[] processed = { false };
        decoder.processMessage(
                event,
                (message, vgtid, isLastRowEventOfTransaction) -> {
                    // verify outcome
                    assertThat(message).isNotNull();
                    assertThat(message).isInstanceOf(OtherMessage.class);
                    assertThat(message.getOperation()).isEqualTo(ReplicationMessage.Operation.OTHER);
                    processed[0] = true;
                },
                null,
                false,
                false,
                null);
        assertThat(processed[0]).isTrue();
    }

    @Test
    public void shouldProcessFieldEvent() throws Exception {
        // exercise SUT
        decoder.processMessage(TestHelper.defaultFieldEvent(), null, null, false, false, null);
        Table table = schema.tableFor(TestHelper.defaultTableId());

        // verify outcome
        assertThat(table).isNotNull();
        assertThat(table.id().schema()).isEqualTo(TestHelper.TEST_UNSHARDED_KEYSPACE);
        assertThat(table.id().table()).isEqualTo(TestHelper.TEST_TABLE);
        assertThat(table.columns().size()).isEqualTo(TestHelper.defaultNumOfColumns());
        for (Query.Field field : TestHelper.defaultFields()) {
            assertThat(table.columnWithName(field.getName())).isNotNull();
        }
    }

    @Test
    public void shouldHandleAddColumnPerShard() throws Exception {
        String shard1 = "-80";
        String shard2 = "80-";
        // exercise SUT
        decoder.processMessage(TestHelper.newFieldEvent(TestHelper.columnValuesSubset(), shard1, TestHelper.TEST_SHARDED_KEYSPACE), null, null, false, false, null);
        decoder.processMessage(TestHelper.newFieldEvent(TestHelper.columnValuesSubset(), shard2, TestHelper.TEST_SHARDED_KEYSPACE), null, null, false, false, null);
        Table table = schema.tableFor(new TableId(shard1, TestHelper.TEST_SHARDED_KEYSPACE, TestHelper.TEST_TABLE));

        // verify outcome
        assertThat(table).isNotNull();
        assertThat(table.id().schema()).isEqualTo(TestHelper.TEST_SHARDED_KEYSPACE);
        assertThat(table.id().table()).isEqualTo(TestHelper.TEST_TABLE);
        assertThat(table.columns().size()).isEqualTo(TestHelper.columnSubsetNumOfColumns());
        for (Query.Field field : TestHelper.fieldsSubset()) {
            assertThat(table.columnWithName(field.getName())).isNotNull();
        }

        decoder.processMessage(
                TestHelper.insertEvent(TestHelper.columnValuesSubset(), shard1, TestHelper.TEST_SHARDED_KEYSPACE),
                (message, vgtid, isLastRowEventOfTransaction) -> {
                    // verify outcome
                    assertThat(message).isNotNull();
                    assertThat(message).isInstanceOf(VStreamOutputReplicationMessage.class);
                    assertThat(message.getOperation()).isEqualTo(ReplicationMessage.Operation.INSERT);
                    assertThat(message.getOldTupleList()).isNull();
                    assertThat(message.getNewTupleList().size()).isEqualTo(TestHelper.columnSubsetNumOfColumns());
                },
                null, false, false, null);

        decoder.processMessage(
                TestHelper.insertEvent(TestHelper.columnValuesSubset(), shard2, TestHelper.TEST_SHARDED_KEYSPACE),
                (message, vgtid, isLastRowEventOfTransaction) -> {
                    // verify outcome
                    assertThat(message).isNotNull();
                    assertThat(message).isInstanceOf(VStreamOutputReplicationMessage.class);
                    assertThat(message.getOperation()).isEqualTo(ReplicationMessage.Operation.INSERT);
                    assertThat(message.getOldTupleList()).isNull();
                    assertThat(message.getNewTupleList().size()).isEqualTo(TestHelper.columnSubsetNumOfColumns());
                },
                null, false, false, null);

        // update schema for shard 2
        decoder.processMessage(TestHelper.newFieldEvent(TestHelper.defaultColumnValues(), shard2, TestHelper.TEST_SHARDED_KEYSPACE), null, null, false, false, null);
        Table tableAfterSchemaChange = schema.tableFor(new TableId(shard2, TestHelper.TEST_SHARDED_KEYSPACE, TestHelper.TEST_TABLE));

        // verify outcome
        assertThat(tableAfterSchemaChange).isNotNull();
        assertThat(tableAfterSchemaChange.id().schema()).isEqualTo(TestHelper.TEST_SHARDED_KEYSPACE);
        assertThat(tableAfterSchemaChange.id().table()).isEqualTo(TestHelper.TEST_TABLE);
        assertThat(tableAfterSchemaChange.columns().size()).isEqualTo(TestHelper.defaultNumOfColumns());
        for (Query.Field field : TestHelper.defaultFields()) {
            assertThat(tableAfterSchemaChange.columnWithName(field.getName())).isNotNull();
        }

        // shard 2 has been updated with new schema, so should handle values that match the new schema
        decoder.processMessage(
                TestHelper.insertEvent(TestHelper.defaultColumnValues(), shard2, TestHelper.TEST_SHARDED_KEYSPACE),
                (message, vgtid, isLastRowEventOfTransaction) -> {
                    // verify outcome
                    assertThat(message).isNotNull();
                    assertThat(message).isInstanceOf(VStreamOutputReplicationMessage.class);
                    assertThat(message.getOperation()).isEqualTo(ReplicationMessage.Operation.INSERT);
                    assertThat(message.getOldTupleList()).isNull();
                    assertThat(message.getNewTupleList().size()).isEqualTo(TestHelper.defaultNumOfColumns());
                },
                null, false, false, null);

        // shard 1 has not been updated with new schema so it should still be able to handle values with the old schema
        decoder.processMessage(
                TestHelper.insertEvent(TestHelper.columnValuesSubset(), shard1, TestHelper.TEST_SHARDED_KEYSPACE),
                (message, vgtid, isLastRowEventOfTransaction) -> {
                    // verify outcome
                    assertThat(message).isNotNull();
                    assertThat(message).isInstanceOf(VStreamOutputReplicationMessage.class);
                    assertThat(message.getOperation()).isEqualTo(ReplicationMessage.Operation.INSERT);
                    assertThat(message.getOldTupleList()).isNull();
                    assertThat(message.getNewTupleList().size()).isEqualTo(TestHelper.columnSubsetNumOfColumns());
                },
                null, false, false, null);
    }

    @Test
    public void shouldHandleRemoveColumnPerShard() throws Exception {
        String shard1 = "-80";
        String shard2 = "80-";
        // exercise SUT
        decoder.processMessage(TestHelper.defaultFieldEvent(shard1, TestHelper.TEST_SHARDED_KEYSPACE), null, null, false, false, null);
        decoder.processMessage(TestHelper.defaultFieldEvent(shard2, TestHelper.TEST_SHARDED_KEYSPACE), null, null, false, false, null);
        Table table = schema.tableFor(new TableId(shard1, TestHelper.TEST_SHARDED_KEYSPACE, TestHelper.TEST_TABLE));

        // verify outcome
        assertThat(table).isNotNull();
        assertThat(table.id().schema()).isEqualTo(TestHelper.TEST_SHARDED_KEYSPACE);
        assertThat(table.id().table()).isEqualTo(TestHelper.TEST_TABLE);
        assertThat(table.columns().size()).isEqualTo(TestHelper.defaultNumOfColumns());
        for (Query.Field field : TestHelper.defaultFields()) {
            assertThat(table.columnWithName(field.getName())).isNotNull();
        }

        decoder.processMessage(
                TestHelper.insertEvent(TestHelper.defaultColumnValues(), shard1, TestHelper.TEST_SHARDED_KEYSPACE),
                (message, vgtid, isLastRowEventOfTransaction) -> {
                    // verify outcome
                    assertThat(message).isNotNull();
                    assertThat(message).isInstanceOf(VStreamOutputReplicationMessage.class);
                    assertThat(message.getOperation()).isEqualTo(ReplicationMessage.Operation.INSERT);
                    assertThat(message.getOldTupleList()).isNull();
                    assertThat(message.getNewTupleList().size()).isEqualTo(TestHelper.defaultNumOfColumns());
                },
                null, false, false, null);

        decoder.processMessage(
                TestHelper.insertEvent(TestHelper.defaultColumnValues(), shard2, TestHelper.TEST_SHARDED_KEYSPACE),
                (message, vgtid, isLastRowEventOfTransaction) -> {
                    // verify outcome
                    assertThat(message).isNotNull();
                    assertThat(message).isInstanceOf(VStreamOutputReplicationMessage.class);
                    assertThat(message.getOperation()).isEqualTo(ReplicationMessage.Operation.INSERT);
                    assertThat(message.getOldTupleList()).isNull();
                    assertThat(message.getNewTupleList().size()).isEqualTo(TestHelper.defaultNumOfColumns());
                },
                null, false, false, null);

        // update schema for shard 2
        decoder.processMessage(TestHelper.newFieldEvent(TestHelper.columnValuesSubset(), shard2, TestHelper.TEST_SHARDED_KEYSPACE), null, null, false, false, null);
        Table tableAfterSchemaChange = schema.tableFor(new TableId(shard2, TestHelper.TEST_SHARDED_KEYSPACE, TestHelper.TEST_TABLE));

        // verify outcome
        assertThat(tableAfterSchemaChange).isNotNull();
        assertThat(tableAfterSchemaChange.id().schema()).isEqualTo(TestHelper.TEST_SHARDED_KEYSPACE);
        assertThat(tableAfterSchemaChange.id().table()).isEqualTo(TestHelper.TEST_TABLE);
        assertThat(tableAfterSchemaChange.columns().size()).isEqualTo(TestHelper.columnSubsetNumOfColumns());
        for (Query.Field field : TestHelper.fieldsSubset()) {
            assertThat(tableAfterSchemaChange.columnWithName(field.getName())).isNotNull();
        }

        // shard 2 has been updated with new schema, so should handle values that match the new schema
        decoder.processMessage(
                TestHelper.insertEvent(TestHelper.columnValuesSubset(), shard2, TestHelper.TEST_SHARDED_KEYSPACE),
                (message, vgtid, isLastRowEventOfTransaction) -> {
                    // verify outcome
                    assertThat(message).isNotNull();
                    assertThat(message).isInstanceOf(VStreamOutputReplicationMessage.class);
                    assertThat(message.getOperation()).isEqualTo(ReplicationMessage.Operation.INSERT);
                    assertThat(message.getOldTupleList()).isNull();
                    assertThat(message.getNewTupleList().size()).isEqualTo(TestHelper.columnSubsetNumOfColumns());
                },
                null, false, false, null);

        // shard 1 has not been updated with new schema so it should still be able to handle values with the old schema
        decoder.processMessage(
                TestHelper.insertEvent(TestHelper.defaultColumnValues(), shard1, TestHelper.TEST_SHARDED_KEYSPACE),
                (message, vgtid, isLastRowEventOfTransaction) -> {
                    // verify outcome
                    assertThat(message).isNotNull();
                    assertThat(message).isInstanceOf(VStreamOutputReplicationMessage.class);
                    assertThat(message.getOperation()).isEqualTo(ReplicationMessage.Operation.INSERT);
                    assertThat(message.getOldTupleList()).isNull();
                    assertThat(message.getNewTupleList().size()).isEqualTo(TestHelper.defaultNumOfColumns());
                },
                null, false, false, null);
    }

    @Test
    public void shouldThrowExceptionWithDetailedMessageOnRowSchemaMismatch() throws Exception {
        // exercise SUT
        decoder.processMessage(TestHelper.defaultFieldEvent(), null, null, false, false, null);
        Table table = schema.tableFor(TestHelper.defaultTableId());

        // verify outcome
        assertThat(table).isNotNull();
        assertThat(table.id().schema()).isEqualTo(TestHelper.TEST_UNSHARDED_KEYSPACE);
        assertThat(table.id().table()).isEqualTo(TestHelper.TEST_TABLE);
        assertThat(table.columns().size()).isEqualTo(TestHelper.defaultNumOfColumns());
        for (Query.Field field : TestHelper.defaultFields()) {
            assertThat(table.columnWithName(field.getName())).isNotNull();
        }

        assertThatThrownBy(() -> {
            decoder.processMessage(TestHelper.insertEvent(TestHelper.columnValuesSubset()), null, null, false, false, null);
        }).isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("bool_col")
                .hasMessageContaining("long_col");
    }

    @Test
    public void shouldProcessInsertEvent() throws Exception {
        // setup fixture
        decoder.processMessage(TestHelper.defaultFieldEvent(), null, null, false, false, null);
        schema.tableFor(TestHelper.defaultTableId());

        // exercise SUT
        final boolean[] processed = { false };
        decoder.processMessage(
                TestHelper.defaultInsertEvent(),
                (message, vgtid, isLastRowEventOfTransaction) -> {
                    // verify outcome
                    assertThat(message).isNotNull();
                    assertThat(message).isInstanceOf(VStreamOutputReplicationMessage.class);
                    assertThat(message.getOperation()).isEqualTo(ReplicationMessage.Operation.INSERT);
                    assertThat(message.getOldTupleList()).isNull();
                    assertThat(message.getShard()).isEqualTo(TestHelper.TEST_SHARD);
                    assertThat(message.getNewTupleList().size()).isEqualTo(TestHelper.defaultNumOfColumns());
                    processed[0] = true;
                },
                null, false, false, null);
        assertThat(processed[0]).isTrue();
    }

    @Test
    public void shouldProcessDeleteEvent() throws Exception {
        // setup fixture
        decoder.processMessage(TestHelper.defaultFieldEvent(), null, null, false, false, null);
        schema.tableFor(TestHelper.defaultTableId());

        // exercise SUT
        final boolean[] processed = { false };
        decoder.processMessage(
                TestHelper.defaultDeleteEvent(),
                (message, vgtid, isLastRowEventOfTransaction) -> {
                    // verify outcome
                    assertThat(message).isNotNull();
                    assertThat(message).isInstanceOf(VStreamOutputReplicationMessage.class);
                    assertThat(message.getOperation()).isEqualTo(ReplicationMessage.Operation.DELETE);
                    assertThat(message.getNewTupleList()).isNull();
                    assertThat(message.getOldTupleList().size()).isEqualTo(TestHelper.defaultNumOfColumns());
                    processed[0] = true;
                },
                null,
                false,
                false,
                null);
        assertThat(processed[0]).isTrue();
    }

    @Test
    public void shouldProcessUpdateEvent() throws Exception {
        // setup fixture
        decoder.processMessage(TestHelper.defaultFieldEvent(), null, null, false, false, null);
        schema.tableFor(TestHelper.defaultTableId());

        // exercise SUT
        final boolean[] processed = { false };
        decoder.processMessage(
                TestHelper.defaultUpdateEvent(),
                (message, vgtid, isLastRowEventOfTransaction) -> {
                    // verify outcome
                    assertThat(message).isNotNull();
                    assertThat(message).isInstanceOf(VStreamOutputReplicationMessage.class);
                    assertThat(message.getOperation()).isEqualTo(ReplicationMessage.Operation.UPDATE);
                    assertThat(message.getOldTupleList().size()).isEqualTo(TestHelper.defaultNumOfColumns());
                    assertThat(message.getNewTupleList().size()).isEqualTo(TestHelper.defaultNumOfColumns());
                    processed[0] = true;
                },
                null,
                false,
                false,
                null);
        assertThat(processed[0]).isTrue();
    }

}
