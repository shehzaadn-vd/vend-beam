package org.apache.beam.sdk.io.jdbc;

import com.google.common.collect.ImmutableMap;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;

import java.io.Serializable;
import java.sql.Date;
import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.EnumMap;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class JdbcUtil {

    /**
     * Interface implemented by functions that sets prepared statement data
     */
    @FunctionalInterface
    interface PreparedStatementSetCaller extends Serializable {
        void set(Row element, PreparedStatement preparedStatement, int index) throws SQLException;
    }

    // PreparedStatementSetCaller for primitive schema types (excluding arrays, structs and logical types).
    private static final EnumMap<Schema.TypeName, PreparedStatementSetCaller>
            PREPARED_STATEMENT_DATA_SETTER = new EnumMap<>(
                    ImmutableMap.<Schema.TypeName, PreparedStatementSetCaller>builder()
                            .put(Schema.TypeName.BYTE, (element, ps, index) -> ps.setByte(index + 1, element.getByte(index)))
                            .put(Schema.TypeName.INT16, (element, ps, index) -> ps.setInt(index + 1, element.getInt16(index)))
                            .put(Schema.TypeName.INT32, (element, ps, index) -> ps.setInt(index + 1, element.getInt32(index)))
                            .put(Schema.TypeName.INT64, (element, ps, index) -> ps.setLong(index + 1, element.getInt64(index)))
                            .put(Schema.TypeName.DECIMAL, (element, ps, index) -> ps.setBigDecimal(index + 1, element.getDecimal(index)))
                            .put(Schema.TypeName.FLOAT, (element, ps, index) -> ps.setFloat(index + 1, element.getFloat(index)))
                            .put(Schema.TypeName.DOUBLE, (element, ps, index) -> ps.setDouble(index + 1, element.getDouble(index)))
                            .put(Schema.TypeName.STRING, (element, ps, index) -> ps.setString(index + 1, element.getString(index)))
                            .put(Schema.TypeName.DATETIME, (element, ps, index) -> ps.setTimestamp(index + 1, new Timestamp(element.getDateTime(index).getMillis())))
                            .put(Schema.TypeName.BOOLEAN, (element, ps, index) -> ps.setBoolean(index + 1, element.getBoolean(index)))
                            .put(Schema.TypeName.BYTES, (element, ps, index) -> ps.setBytes(index + 1, element.getBytes(index)))
                            .build());

    public static void mapDataAccordingToSchema(Row element, PreparedStatement preparedStatement, Schema.FieldType fieldType, int index) {
        try {
            switch (fieldType.getTypeName()) {
                case ARRAY:
                    preparedStatement.setArray(index + 1,
                            preparedStatement.getConnection().createArrayOf(fieldType.getCollectionElementType().getTypeName().name(), element.getArray(index).toArray()));
                    break;
                case LOGICAL_TYPE: {
                    String logicalTypeName = fieldType.getLogicalType().getIdentifier();
                    JDBCType jdbcType = JDBCType.valueOf(logicalTypeName);
                    switch (jdbcType) {
                        case DATE:
                            preparedStatement.setDate(index + 1, new Date(element.getDateTime(index).getMillis()));
                            break;
                        case TIME:
                            preparedStatement.setTime(index + 1, new Time(element.getDateTime(index).getMillis()));
                            break;
                        case TIMESTAMP_WITH_TIMEZONE:
                            preparedStatement.setTimestamp(index + 1, new Timestamp(element.getDateTime(index).getMillis()));
                            break;
                        default: // nothing
                    }
                    break;
                }
                default: {
                    if (PREPARED_STATEMENT_DATA_SETTER.containsKey(fieldType.getTypeName())) {
                        PREPARED_STATEMENT_DATA_SETTER.get(fieldType.getTypeName()).set(element, preparedStatement, index);
                    } else {
                        throw new RuntimeException(fieldType.getTypeName().name() + " in schema is not supported while writing. Please provide statement and preparedStatementSetter");
                    }
                }
            }
        }
        catch (SQLException | NullPointerException e) {
            throw new RuntimeException("Error while setting data to preparedStatement", e);
        }
    }

    public static String generateStatement(String tableName, List<Schema.Field> fields) {

        String fieldNames = IntStream.range(0, fields.size()).mapToObj((index) -> {
            return fields.get(index).getName();
        }).collect(Collectors.joining(", "));

        String values = IntStream.range(0, fields.size()).mapToObj((index) -> {
            return "?";
        }).collect(Collectors.joining(","));

        return "INSERT INTO "+tableName+" ("+fieldNames+") VALUES("+values+")";
    }
}
