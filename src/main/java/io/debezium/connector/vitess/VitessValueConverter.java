/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess;

import java.math.BigDecimal;
import java.util.List;

import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

import io.debezium.config.CommonConnectorConfig.BinaryHandlingMode;
import io.debezium.connector.mysql.MySqlValueConverters;
import io.debezium.data.Json;
import io.debezium.jdbc.TemporalPrecisionMode;
import io.debezium.relational.Column;
import io.debezium.relational.RelationalChangeRecordEmitter;
import io.debezium.relational.ValueConverter;
import io.vitess.proto.Query;

/** Used by {@link RelationalChangeRecordEmitter} to convert Java value to Connect value */
public class VitessValueConverter extends MySqlValueConverters {
    private static final BigDecimal BIGINT_MAX_VALUE = new BigDecimal("18446744073709551615");
    private static final BigDecimal BIGINT_CORRECTION = BIGINT_MAX_VALUE.add(BigDecimal.ONE);

    private final boolean includeUnknownDatatypes;
    private final VitessConnectorConfig.BigIntUnsignedHandlingMode bigIntUnsignedHandlingMode;

    public VitessValueConverter(
                                DecimalMode decimalMode,
                                TemporalPrecisionMode temporalPrecisionMode,
                                BinaryHandlingMode binaryMode,
                                boolean includeUnknownDatatypes,
                                VitessConnectorConfig.BigIntUnsignedHandlingMode bigIntUnsignedHandlingMode) {
        super(
                decimalMode,
                temporalPrecisionMode,
                null/* bigint unsigned mode */,
                binaryMode,
                x -> x,
                null);
        this.includeUnknownDatatypes = includeUnknownDatatypes;
        this.bigIntUnsignedHandlingMode = bigIntUnsignedHandlingMode;
    }

    // Get Kafka connect schema from Debezium column.
    @Override
    public SchemaBuilder schemaBuilder(Column column) {
        String typeName = column.typeName().toUpperCase();
        if (matches(typeName, Query.Type.JSON.name())) {
            return Json.builder();
        }
        if (matches(typeName, Query.Type.ENUM.name())) {
            return io.debezium.data.Enum.builder(column.enumValues());
        }
        if (matches(typeName, Query.Type.SET.name())) {
            return io.debezium.data.EnumSet.builder(column.enumValues());
        }

        if (matches(typeName, Query.Type.UINT64.name())) {
            switch (bigIntUnsignedHandlingMode) {
                case LONG:
                    return SchemaBuilder.int64();
                case STRING:
                    return SchemaBuilder.string();
                case PRECISE:
                    // In order to capture unsigned INT 64-bit data source, org.apache.kafka.connect.data.Decimal:Byte will be required to safely capture all valid values with scale of 0
                    // Source: https://kafka.apache.org/0102/javadoc/org/apache/kafka/connect/data/Schema.Type.html
                    return Decimal.builder(0);
                default:
                    throw new IllegalArgumentException("Unknown bigIntUnsignedHandlingMode: " + bigIntUnsignedHandlingMode);
            }
        }

        final SchemaBuilder jdbcSchemaBuilder = super.schemaBuilder(column);
        if (jdbcSchemaBuilder == null) {
            return includeUnknownDatatypes ? SchemaBuilder.bytes() : null;
        }
        else {
            return jdbcSchemaBuilder;
        }
    }

    // Convert Java value to Kafka Connect value.
    @Override
    public ValueConverter converter(Column column, Field fieldDefn) {
        String typeName = column.typeName().toUpperCase();
        if (matches(typeName, Query.Type.ENUM.name())) {
            return (data) -> convertEnumToString(column.enumValues(), column, fieldDefn, data);
        }
        if (matches(typeName, Query.Type.SET.name())) {
            return (data) -> convertSetToString(column.enumValues(), column, fieldDefn, data);
        }

        if (matches(typeName, Query.Type.UINT64.name())) {
            switch (bigIntUnsignedHandlingMode) {
                case LONG:
                    return (data) -> convertBigInt(column, fieldDefn, data);
                case STRING:
                    return (data) -> convertString(column, fieldDefn, data);
                case PRECISE:
                    // Convert BIGINT UNSIGNED internally from SIGNED to UNSIGNED based on the boundary settings
                    return (data) -> convertUnsignedBigint(column, fieldDefn, data);
                default:
                    throw new IllegalArgumentException("Unknown bigIntUnsignedHandlingMode: " + bigIntUnsignedHandlingMode);
            }
        }

        final ValueConverter jdbcConverter = super.converter(column, fieldDefn);
        if (jdbcConverter == null) {
            return includeUnknownDatatypes
                    ? data -> convertBinary(column, fieldDefn, data, binaryMode)
                    : null;
        }
        else {
            return jdbcConverter;
        }
    }

    /**
     * Convert original value insertion of type 'BIGINT' into the correct BIGINT UNSIGNED representation
     * Note: Unsigned BIGINT (64-bit) is represented in 'BigDecimal' data type. Reference: https://kafka.apache.org/0102/javadoc/org/apache/kafka/connect/data/Schema.Type.html
     *
     * @param originalNumber {@link BigDecimal} the original insertion value
     * @return {@link BigDecimal} the correct representation of the original insertion value
     */
    protected static BigDecimal convertUnsignedBigint(BigDecimal originalNumber) {
        if (originalNumber.compareTo(BigDecimal.ZERO) == -1) {
            return originalNumber.add(BIGINT_CORRECTION);
        }
        else {
            return originalNumber;
        }
    }

    /**
     * Convert the a value representing a Unsigned BIGINT value to the correct Unsigned INT representation.
     *
     * @param column the column in which the value appears
     * @param fieldDefn the field definition for the SourceRecord's {@link Schema}; never null
     * @param data the data; may be null
     *
     * @return the converted value, or null if the conversion could not be made and the column allows nulls
     *
     * @throws IllegalArgumentException if the value could not be converted but the column does not allow nulls
     */
    protected Object convertUnsignedBigint(Column column, Field fieldDefn, Object data) {
        return convertValue(column, fieldDefn, data, 0L, (r) -> {
            if (data instanceof BigDecimal) {
                r.deliver(convertUnsignedBigint((BigDecimal) data));
            }
            else if (data instanceof Number) {
                r.deliver(convertUnsignedBigint(new BigDecimal(((Number) data).toString())));
            }
            else if (data instanceof String) {
                r.deliver(convertUnsignedBigint(new BigDecimal((String) data)));
            }
            else {
                r.deliver(convertNumeric(column, fieldDefn, data));
            }
        });
    }

    /**
     * Determine if the uppercase form of a column's type exactly matches or begins with the specified prefix.
     * Note that this logic works when the column's {@link Column#typeName() type} contains the type name followed by parentheses.
     *
     * @param upperCaseTypeName the upper case form of the column's {@link Column#typeName() type name}
     * @param upperCaseMatch the upper case form of the expected type or prefix of the type; may not be null
     * @return {@code true} if the type matches the specified type, or {@code false} otherwise
     */
    protected boolean matches(String upperCaseTypeName, String upperCaseMatch) {
        if (upperCaseTypeName == null) {
            return false;
        }
        return upperCaseMatch.equals(upperCaseTypeName) || upperCaseTypeName.startsWith(upperCaseMatch + "(");
    }

    /**
     * Converts a value object for a MySQL {@code ENUM}, which is represented in the binlog events as an integer value containing
     * the index of the enum option.
     *
     * @param options the characters that appear in the same order as defined in the column; may not be null
     * @param column the column definition describing the {@code data} value; never null
     * @param fieldDefn the field definition; never null
     * @param data the data object to be converted into an {@code ENUM} literal String value
     * @return the converted value, or empty string if the conversion could not be made
     */
    protected Object convertEnumToString(List<String> options, Column column, Field fieldDefn, Object data) {
        return convertValue(column, fieldDefn, data, "", (r) -> {
            if (options != null) {
                // The binlog will contain an int with the 1-based index of the option in the enum value ...
                int value = ((Integer) data).intValue();
                if (value == 0) {
                    // an invalid value was specified, which corresponds to the empty string '' and an index of 0
                    r.deliver("");
                }
                else {
                    int index = value - 1; // 'options' is 0-based
                    if (index < options.size() && index >= 0) {
                        r.deliver(options.get(index));
                    }
                    else {
                        r.deliver("");
                    }
                }
            }
            else {
                r.deliver("");
            }
        });
    }

    /**
     * Converts a value object for a MySQL {@code SET}, which is represented in the binlog events contain a long number in which
     * every bit corresponds to a different option.
     *
     * @param options the characters that appear in the same order as defined in the column; may not be null
     * @param column the column definition describing the {@code data} value; never null
     * @param fieldDefn the field definition; never null
     * @param data the data object to be converted into an {@code SET} literal String value
     * @return the converted value, or empty string if the conversion could not be made
     */
    protected Object convertSetToString(List<String> options, Column column, Field fieldDefn, Object data) {
        return convertValue(column, fieldDefn, data, "", (r) -> {
            // The binlog will contain a long with the indexes of the options in the set value ...
            long indexes = ((Long) data).longValue();
            r.deliver(convertSetValue(column, indexes, options));
        });
    }

    protected String convertSetValue(Column column, long indexes, List<String> options) {
        StringBuilder sb = new StringBuilder();
        int index = 0;
        boolean first = true;
        int optionLen = options.size();
        while (indexes != 0L) {
            if (indexes % 2L != 0) {
                if (first) {
                    first = false;
                }
                else {
                    sb.append(',');
                }
                if (index < optionLen) {
                    sb.append(options.get(index));
                }
                else {
                    logger.warn("Found unexpected index '{}' on column {}", index, column);
                }
            }
            ++index;
            indexes = indexes >>> 1;
        }
        return sb.toString();
    }
}
