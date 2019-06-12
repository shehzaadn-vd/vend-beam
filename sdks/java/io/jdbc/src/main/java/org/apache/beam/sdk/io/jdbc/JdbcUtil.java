package org.apache.beam.sdk.io.jdbc;

import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;

public class JdbcUtil {
    //fixme: rename method
    public static void setFieldPreparedStatement(Row element, PreparedStatement preparedStatement, Schema.FieldType fieldType, int index) {
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
                    //TODO: WIP
//          preparedStatement.setArray(index + 1, element.getArray(index));
                    break;
//          throw new RuntimeException("Arrays in schema are not a supported while writing. Please provide statement and preparedStatementSetter");
                case MAP:
                    throw new RuntimeException("Maps in schema are not a supported while writing. Please provide statement and preparedStatementSetter");
                case ROW: // The field is itself a nested row.
                    //TODO: WIP
                case LOGICAL_TYPE:
                    //TODO: WIP
            }
        }
        catch (SQLException | NullPointerException e) {
            throw new RuntimeException("Error while setting data to preparedStatement", e);
        }
    }
}
