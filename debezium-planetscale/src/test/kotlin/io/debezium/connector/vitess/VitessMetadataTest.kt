/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess

import kotlin.test.Test
import kotlin.test.assertEquals

class VitessMetadataTest {
  @Test fun quoteIdentifier_wrapsPlainNameInBackticks() {
    assertEquals("`my_keyspace`", VitessMetadata.quoteIdentifier("my_keyspace"))
  }

  @Test fun quoteIdentifier_supportsKeyspaceWithHyphen() {
    // Hyphens make the bare identifier invalid in MySQL/Vitess SQL
    // (e.g. SHOW TABLES FROM <keyspace>), so the name must be backticked.
    assertEquals("`keyspace-with-hyphens`", VitessMetadata.quoteIdentifier("keyspace-with-hyphens"))
  }

  @Test fun quoteIdentifier_supportsTableWithHyphen() {
    // VStream filter SQL ("select * from <table>") needs the same quoting so
    // hyphenated tables in tableIncludeList don't break rule construction.
    assertEquals("`table-with-hyphens`", VitessMetadata.quoteIdentifier("table-with-hyphens"))
  }

  @Test fun quoteIdentifier_supportsTableWithEmbeddedBacktick() {
    // MySQL escapes an embedded backtick by doubling it, so `weird`name`
    // becomes `weird``name` once wrapped.
    assertEquals("`weird``name`", VitessMetadata.quoteIdentifier("weird`name"))
  }

  @Test fun escapeStringLiteral_escapesSingleQuotesAndBackslashes() {
    assertEquals("plain", VitessMetadata.escapeStringLiteral("plain"))
    assertEquals("with-dash", VitessMetadata.escapeStringLiteral("with-dash"))
    assertEquals("o\\'brien", VitessMetadata.escapeStringLiteral("o'brien"))
    assertEquals("back\\\\slash", VitessMetadata.escapeStringLiteral("back\\slash"))
  }
}
