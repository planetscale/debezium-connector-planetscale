/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess.connection

import io.debezium.config.Configuration
import io.debezium.connector.vitess.VitessConnectorConfig
import kotlin.test.Test
import kotlin.test.assertEquals

internal class VitessReplicationConnectionTest {
  // quoteIdentifier is a non-static method on VitessReplicationConnection (mirroring
  // JdbcConnection#quoteIdentifier); construction is lightweight and does not open a channel.
  private fun connection(): VitessReplicationConnection =
    VitessReplicationConnection(VitessConnectorConfig(Configuration.create().build()), null)

  @Test fun quoteIdentifier_wrapsPlainNameInBackticks() {
    assertEquals("`my_keyspace`", connection().quoteIdentifier("my_keyspace"))
  }

  @Test fun quoteIdentifier_supportsKeyspaceWithHyphen() {
    // Hyphens make the bare identifier invalid in MySQL/Vitess SQL
    // (e.g. SHOW TABLES FROM <keyspace>), so the name must be backticked.
    assertEquals("`keyspace-with-hyphens`", connection().quoteIdentifier("keyspace-with-hyphens"))
  }

  @Test fun quoteIdentifier_supportsTableWithHyphen() {
    // VStream filter SQL ("select * from <table>") needs the same quoting so
    // hyphenated tables in tableIncludeList don't break rule construction.
    assertEquals("`table-with-hyphens`", connection().quoteIdentifier("table-with-hyphens"))
  }

  @Test fun quoteIdentifier_supportsTableWithEmbeddedBacktick() {
    // MySQL escapes an embedded backtick by doubling it, so `weird`name`
    // becomes `weird``name` once wrapped.
    assertEquals("`weird``name`", connection().quoteIdentifier("weird`name"))
  }
}
