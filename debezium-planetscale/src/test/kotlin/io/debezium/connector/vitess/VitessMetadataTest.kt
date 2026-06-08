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

  @Test fun escapeLikePattern_escapesUnderscoreAndPercent() {
    // `_` matches any single char in LIKE; `%` matches any sequence. Without
    // escaping, `ehsan_test_airbyte` over-matches `ehsan-test-airbyte` etc.
    assertEquals("plain", VitessMetadata.escapeLikePattern("plain"))
    assertEquals("with-dash", VitessMetadata.escapeLikePattern("with-dash"))
    assertEquals("ehsan\\_test\\_airbyte", VitessMetadata.escapeLikePattern("ehsan_test_airbyte"))
    assertEquals("fifty\\%off", VitessMetadata.escapeLikePattern("fifty%off"))
  }

  @Test fun escapeLikePattern_escapesBackslashFirst() {
    // Backslash is the LIKE escape char, so it must be doubled before _ and %
    // are escaped (otherwise the backslashes we add would themselves be split).
    assertEquals("back\\\\slash", VitessMetadata.escapeLikePattern("back\\slash"))
  }

  @Test fun escapeLikePattern_composesWithEscapeStringLiteral() {
    // Real call site is escapeStringLiteral(escapeLikePattern(value)). The LIKE
    // backslash gets doubled once for the LIKE engine, then again for the SQL
    // string literal -- so `ehsan_test` ends up as `ehsan\\_test` in the SQL.
    val composed = VitessMetadata.escapeStringLiteral(VitessMetadata.escapeLikePattern("ehsan_test"))
    assertEquals("ehsan\\\\_test", composed)
  }
}
