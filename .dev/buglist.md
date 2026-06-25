# Bug List: Debezium PlanetScale Connector

## Active Bugs

### 1. `validateInheritEpoch` always returns 0 — config validation silently skipped
- **File:** `VitessConnectorConfig.java:715`
- **Priority:** High
- **Details:** The method reports a validation problem via `problems.accept()` but always `return 0`, so the connector accepts invalid configs where `inherit.epoch=true` without the required `VitessOrderedTransactionMetadataFactory`. Compare with `validateTimePrecisionMode` (line 510) which correctly returns `1`.

### 2. `validateInheritEpoch` NPE when `factory` is null
- **File:** `VitessConnectorConfig.java:712`
- **Priority:** High
- **Details:** `config.getString(TRANSACTION_METADATA_FACTORY)` can return `null` if unconfigured, then `factory.equals(...)` throws NPE. Fix: reverse to `VitessOrderedTransactionMetadataFactory.class.getName().equals(factory)`.

### 3. Schema/value type mismatch for unknown datatypes
- **File:** `VitessColumnValue.java:90` vs `VitessValueConverter.java:162-164`
- **Priority:** High
- **Details:** When `includeUnknownDatatypes` is true, the schema declares the field as `SchemaBuilder.bytes()` but `asDefault()` returns `asString()` (a `String`). This type mismatch will cause a Kafka Connect serialization error at runtime.

### 4. `stringToTimestamp` misses zero-day dates
- **File:** `VitessValueConverter.java:465`
- **Priority:** Medium
- **Details:** The regex `^\d{4}-00-00.*$` only catches zero-month dates, but MySQL also allows `2024-01-00` (valid month, zero day), which will throw `IllegalArgumentException` from `Timestamp.valueOf()`. The companion method `stringToLocalDate()` (line 437) correctly checks for both zero month and zero day.

### 5. NPE in `close()` when channel was never created
- **File:** `VitessReplicationConnection.java:408-410`
- **Priority:** Medium
- **Details:** `managedChannel` is initialized as `new AtomicReference<>()` (null). If `close()` is called before any connection is established (startup failure, error recovery), `managedChannel.get().shutdownNow()` throws NPE. Needs a null guard.

### 6. ManagedChannel leak on repeated `execute()` calls
- **File:** `VitessReplicationConnection.java:69-70,80-81,98-99`
- **Priority:** Medium
- **Details:** Each call to `execute()` or `startStreaming()` creates a new `ManagedChannel`, but `compareAndSet(null, channel)` only stores the first one. Subsequent channels are used for the gRPC call but never closed. The `close()` method only shuts down the originally stored channel.

### 7. `tlsCredentialSafe` only catches `RuntimeException`
- **File:** `TlsUtils.kt:205-210`
- **Priority:** Medium
- **Details:** Keystore/crypto operations throw checked exceptions (`KeyStoreException`, `IOException`, `CertificateException`, etc.). In Kotlin these propagate uncaught through the `catch (err: RuntimeException)` block, crashing the caller despite the "Safe" suffix contract. Should catch `Exception`.

### 8. Password `CharArray` copy never zeroed after use
- **File:** `TlsUtils.kt:113-119,126-132`
- **Priority:** Medium
- **Details:** The `PasswordHolder.consume()` method copies the password and zeroes the original, but the copy (used for `store.load()` and `kmf.init()`) is never zeroed. The password remains in heap memory indefinitely, defeating the secure password handling pattern.

### 9. Missing `build.version` resource for MySQL `Module`
- **File:** `Module.java:7` + `build.gradle.kts:156`
- **Priority:** Medium
- **Details:** The patched `Module.java` for `io.debezium.connector.mysql` loads `build.version` from a relocated path, but the `debeziumClasses` task only copies `*.class` files (no resources), and no `build.version` exists in `src/main/resources/` for the MySQL package. `Module.version()` will return `null`.

### 10. Trust store loaded before checking if TLS is configured
- **File:** `TlsUtils.kt:184-200`
- **Priority:** Low
- **Details:** The trust store file is loaded before the `when` block checks if any certificate is configured. If a user sets only `TLS_TRUST_FILE` without a certificate, they get a file-not-found error instead of a clean `null` return.

### 11. String `==` comparison instead of `.equals()`
- **File:** `VitessReplicationConnection.java:466,483-484`
- **Priority:** Low
- **Details:** Uses `==` to compare strings against `Vgtid.EMPTY_GTID` / `Vgtid.CURRENT_GTID`. In practice this works because `getVgtid()` returns the constant references directly, but it's fragile. The `validateVgtids` method at line 668 correctly uses `.equals()`.

---

## Dormant Bugs (dead ByteBuddy transform code)

The entire `transforms/` module's ByteBuddy delegation path is dead code. The patched Java `VitessReplicationConnection` replaces the upstream class wholesale, and the patched `newChannel()` is `private` (unmatchable by ByteBuddy's default matchers). If this code were ever activated, it would fail in multiple cascading ways:

### D1. `builder.apply {}` discards transform results
- **File:** `VitessPluginHooks.kt:32-36` and `VitessManagedChannel.kt:18-21`
- **Details:** Kotlin's `.apply {}` returns the original receiver. `method(...).intercept(...)` returns a new builder (ByteBuddy builders are immutable), which is discarded. No transforms are ever applied. Fix: use `.fold()` or reassign.

### D2. `PlanetscaleManagedChannel.config` is `lateinit` but never initialized
- **File:** `PlanetscaleManagedChannel.kt:28,37`
- **Details:** The assignment `this.config = config` is commented out. Any access throws `UninitializedPropertyAccessException`.

### D3. `PlanetscaleConstants.HOST` includes URL scheme
- **File:** `PlanetscaleConstants.kt:10`
- **Details:** `"https://connect.psdb.cloud"` includes the `https://` prefix, but `ManagedChannelBuilder.forAddress()` expects a bare hostname. Would cause DNS resolution failure on the fallback path.

### D4. `AbstractTransform.apply()` calls `it.make()` prematurely
- **File:** `AbstractTransform.kt:30`
- **Details:** `.also { it.make() }` materializes bytecode then discards the result. Wasteful and could produce confusing errors.
