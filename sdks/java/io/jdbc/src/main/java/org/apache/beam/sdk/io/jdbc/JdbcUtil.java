/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.io.jdbc;

import com.google.common.collect.ImmutableMap;
import org.apache.beam.sdk.schemas.Schema;

import java.sql.Date;
import java.sql.JDBCType;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class JdbcUtil {

    public static String generateStatement(String tableName, List<Schema.Field> fields) {

        String fieldNames = IntStream.range(0, fields.size()).mapToObj((index) -> {
            return fields.get(index).getName();
        }).collect(Collectors.joining(", "));

        String valuePlaceholder = IntStream.range(0, fields.size()).mapToObj((index) -> {
            return "?";
        }).collect(Collectors.joining(", "));

        return String.format("INSERT INTO %s(%s) VALUES(%s)", tableName, fieldNames, valuePlaceholder);
    }

    private static Map<Schema.TypeName, JdbcIO.PreparedStatementSetCaller> TYPE_NAME_PS_SET_CALLER_MAP = new EnumMap<Schema.TypeName, JdbcIO.PreparedStatementSetCaller>(
            ImmutableMap.<Schema.TypeName, JdbcIO.PreparedStatementSetCaller>builder()
                    .put(Schema.TypeName.BYTE, (element, ps, i) -> ps.setByte(i + 1, element.getByte(i)))
                    .put(Schema.TypeName.INT16, (element, ps, i) -> ps.setInt(i + 1, element.getInt16(i)))
                    .put(Schema.TypeName.INT64, (element, ps, i) -> ps.setLong(i + 1, element.getInt64(i)))
                    .put(Schema.TypeName.DECIMAL, (element, ps, i) -> ps.setBigDecimal(i + 1, element.getDecimal(i)))
                    .put(Schema.TypeName.FLOAT, (element, ps, i) -> ps.setFloat(i + 1, element.getFloat(i)))
                    .put(Schema.TypeName.DOUBLE, (element, ps, i) -> ps.setDouble(i + 1, element.getDouble(i)))
                    .put(Schema.TypeName.DATETIME, (element, ps, i) -> ps.setTimestamp(i + 1, new Timestamp(element.getDateTime(i).getMillis())))
                    .put(Schema.TypeName.BOOLEAN, (element, ps, i) -> ps.setBoolean(i + 1, element.getBoolean(i)))
                    .put(Schema.TypeName.BYTES, (element, ps, i) -> ps.setBytes(i + 1, element.getBytes(i)))
                    .put(Schema.TypeName.INT32, (element, ps, i) -> ps.setInt(i + 1, element.getInt32(i)))
                    .put(Schema.TypeName.STRING, (element, ps, i) -> ps.setString(i + 1, element.getString(i)))
                    .build());

    public static JdbcIO.PreparedStatementSetCaller getPreparedStatementSetCaller(Schema.FieldType fieldType) {
        switch (fieldType.getTypeName()) {
            case ARRAY:
                return (element, ps, i) -> {
                    ps.setArray(i + 1, ps.getConnection()
                            .createArrayOf(fieldType.getCollectionElementType().getTypeName().name(), element.getArray(i).toArray()));
                };
            case LOGICAL_TYPE: {
                String logicalTypeName = fieldType.getLogicalType().getIdentifier();
                JDBCType jdbcType = JDBCType.valueOf(logicalTypeName);
                switch (jdbcType) {
                    case DATE:
                        return (element, ps, i) -> {
                            ps.setDate(i + 1, new Date(element.getDateTime(i).getMillis()));
                        };
                    case TIME:
                        return (element, ps, i) -> {
                            ps.setTime(i + 1, new Time(element.getDateTime(i).getMillis()));
                        };
                    case TIMESTAMP_WITH_TIMEZONE:
                        return (element, ps, i) -> {
                            ps.setTimestamp(i + 1, new Timestamp(element.getDateTime(i).getMillis()));
                        };
                    default:
                        return getPreparedStatementSetCaller(fieldType.getLogicalType().getBaseType());
                }
            }
            default: {
                if (TYPE_NAME_PS_SET_CALLER_MAP.containsKey(fieldType.getTypeName())) {
                    return TYPE_NAME_PS_SET_CALLER_MAP.get(fieldType.getTypeName());
                } else {
                    throw new RuntimeException(fieldType.getTypeName().name() + " in schema is not supported while writing. Please provide statement and preparedStatementSetter");
                }
            }
        }
    }

}
