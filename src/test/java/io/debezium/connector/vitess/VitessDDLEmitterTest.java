/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.function.Predicate;

import org.junit.BeforeClass;
import org.junit.Test;

import io.debezium.connector.vitess.connection.DdlMessage;
import io.debezium.connector.vitess.connection.ReplicationMessage;
import io.debezium.connector.vitess.connection.VStreamOutputMessageDecoder;
import io.debezium.schema.DefaultTopicNamingStrategy;
import io.debezium.schema.SchemaChangeEvent;
import io.debezium.schema.SchemaNameAdjuster;
import io.debezium.spi.topic.TopicNamingStrategy;

public class VitessDDLEmitterTest {
    private static VitessConnectorConfig connectorConfig;
    private static VitessDatabaseSchema schema;
    private static VStreamOutputMessageDecoder decoder;

    @BeforeClass
    public static void beforeClass() throws Exception {
        connectorConfig = new VitessConnectorConfig(TestHelper.defaultConfig().build());
        schema = new VitessDatabaseSchema(
                connectorConfig,
                SchemaNameAdjuster.create(),
                (TopicNamingStrategy) DefaultTopicNamingStrategy.create(connectorConfig));
        decoder = new VStreamOutputMessageDecoder(schema, connectorConfig.ddlFilter());
        // initialize schema by FIELD event
        decoder.processMessage(TestHelper.defaultFieldEvent(), null, null, false);
    }

    @Test
    public void shouldFilterDDLMessages() {
        Predicate<String> skipAlters = (ddl) -> ddl.toUpperCase().contains("ALTER");
        // setup fixture
        ReplicationMessage message = new DdlMessage(
                AnonymousValue.getString(),
                AnonymousValue.getInstant(),
                TestHelper.defaultTableId().toDoubleQuotedString(),
                "ALTER TABLE customers ADD COLUMN enabled_at DATETIME ",
                SchemaChangeEvent.SchemaChangeEventType.ALTER);
        // exercise SUT
        VitessDDLEmitter emitter = new VitessDDLEmitter(initializePartition(),
                null,
                skipAlters,
                schema,
                message);
        assertThat(emitter.getSchemaChangeEvent()).isNull();
    }

    private VitessPartition initializePartition() {
        return new VitessPartition("test", null);
    }
}
