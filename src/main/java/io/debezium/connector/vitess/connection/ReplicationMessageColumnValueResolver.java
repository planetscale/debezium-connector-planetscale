/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess.connection;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;

import io.debezium.connector.vitess.VitessType;

/** Resolve raw column value to Java value */
public class ReplicationMessageColumnValueResolver {

    public static Object resolveValue(
                                      VitessType vitessType, ReplicationMessage.ColumnValue<byte[]> value, boolean includeUnknownDatatypes) {
        if (value.isNull()) {
            return null;
        }

        switch (vitessType.getJdbcId()) {
            case Types.SMALLINT:
                return value.asShort();
            case Types.INTEGER:
                if (vitessType.isEnum()) {
                    return vitessType.getEnumOrdinal(value.asString());
                }
                return value.asInteger();
            case Types.BIGINT:
                if (vitessType.isEnum()) {
                    return vitessType.getSetNumeral(value.asString());
                }
                return value.asLong();
            case Types.BLOB:
            case Types.BINARY:
                return value.asBytes();
            case Types.DATE:
                return Date.valueOf(value.asString());
            case Types.TIME:
                return Time.valueOf(value.asString());
            case Types.TIMESTAMP:
                return Timestamp.valueOf(value.asString());
            case Types.TIMESTAMP_WITH_TIMEZONE:
                return value.asString();
            case Types.VARCHAR:
                return value.asString();
            case Types.FLOAT:
                return value.asFloat();
            case Types.DOUBLE:
                return value.asDouble();
            default:
                break;
        }

        return value.asDefault(vitessType, includeUnknownDatatypes);
    }
}
