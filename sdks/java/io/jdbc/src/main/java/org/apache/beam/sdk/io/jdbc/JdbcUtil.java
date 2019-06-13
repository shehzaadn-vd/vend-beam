package org.apache.beam.sdk.io.jdbc;

import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;

import java.sql.Date;
import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.List;
import java.util.StringJoiner;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class JdbcUtil {

    public static void mapDataAccordingToSchema(Row element, PreparedStatement preparedStatement, Schema.FieldType fieldType, int index) {
        try {
            switch (fieldType.getTypeName()) {
                case BYTE: // One-byte signed integer.
                    preparedStatement.setByte(index + 1, element.getByte(index)); // index+1 is because preparedStatement indices start with 1
                    break;
                case INT16: // two-byte signed integer.
                    preparedStatement.setInt(index + 1, element.getInt16(index));
                    break;
                case INT32: // four-byte signed integer.
                    preparedStatement.setInt(index + 1, element.getInt32(index));
                    break;
                case INT64: // eight-byte signed integer.
                    preparedStatement.setLong(index + 1, element.getInt64(index));
                    break;
                case DECIMAL: // Arbitrary-precision decimal number
                    preparedStatement.setBigDecimal(index + 1, element.getDecimal(index));
                    break;
                case FLOAT:
                    preparedStatement.setFloat(index + 1, element.getFloat(index));
                    break;
                case DOUBLE:
                    preparedStatement.setDouble(index + 1, element.getDouble(index));
                    break;
                case STRING: // String.
                    preparedStatement.setString(index + 1, element.getString(index));
                    break;
                case DATETIME: // Date and time.
                    preparedStatement.setTimestamp(index + 1, new Timestamp(element.getDateTime(index).getMillis()));
                    break;
                case BOOLEAN: // Boolean.
                    preparedStatement.setBoolean(index + 1, element.getBoolean(index));
                    break;
                case BYTES: // Byte array.
                    preparedStatement.setBytes(index + 1, element.getBytes(index));
                    break;
                case ARRAY:
                    preparedStatement.setArray(index + 1,
                            preparedStatement.getConnection().createArrayOf(fieldType.getCollectionElementType().getTypeName().name(), element.getArray(index).toArray()));
                    break;
                case MAP:
                    throw new RuntimeException("Map in schema is not supported while writing. Please provide statement and preparedStatementSetter");
                case ROW: // The field is itself a nested row.
                    throw new RuntimeException("Row in schema is not supported while writing. Please provide statement and preparedStatementSetter");
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
                        default:
                            break;
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
