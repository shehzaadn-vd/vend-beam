package org.apache.beam.sdk.io.jdbc;

import org.apache.beam.sdk.schemas.LogicalTypes;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.joda.time.Instant;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;

public class JdbcUtils {

    /**
     * Get the Beam {@link FieldType} from a {@link ResultSetMetaData ColumnType}.
     *
     * <p>Supports JDBC Types</p>
     *
     * @param type of {@link ResultSetMetaData ColumnType}
     * @return Corresponding Beam {@link FieldType}
     */
    private static FieldType fromFieldType(int type) {
        switch (type) {
            case Types.VARCHAR:
            case Types.CHAR:
            case Types.LONGNVARCHAR:
                return FieldType.STRING;
            case Types.BIT:
            case Types.BOOLEAN:
                return FieldType.BOOLEAN;
            case Types.NUMERIC:
                return FieldType.DECIMAL;
            case Types.TINYINT:
                return FieldType.BYTE;
            case Types.SMALLINT:
                return FieldType.INT16;
            case Types.INTEGER:
                return FieldType.INT32;
            case Types.BIGINT:
                return FieldType.INT64;
            case Types.FLOAT:
                return FieldType.FLOAT;
            case Types.REAL:
            case Types.DOUBLE:
                return FieldType.DOUBLE;
            case Types.VARBINARY:
            case Types.BINARY:
            case Types.BLOB:
                return FieldType.BYTES;
            case Types.DATE:
                return FieldType.logicalType(
                        new LogicalTypes.PassThroughLogicalType<Instant>(
                                "SqlDateType", "", FieldType.DATETIME) {});
            case Types.TIME:
                return FieldType.logicalType(
                        new LogicalTypes.PassThroughLogicalType<Instant>(
                                "SqlTimeType", "", FieldType.DATETIME) {});
            case Types.TIMESTAMP:
                return FieldType.logicalType(
                        new LogicalTypes.PassThroughLogicalType<Instant>(
                                "SqlTimestampWithLocalTzType", "", FieldType.DATETIME) {});
            default:
                throw new UnsupportedOperationException( "Conversion or sql type to Beam type is unsupported");
        }
    }

    private static Schema fromResultSetMetaData(ResultSetMetaData metaData) throws SQLException {
        Schema.Builder schemaBuilder = Schema.builder();
        for (int i = 1; i <= metaData.getColumnCount(); i++) {
            FieldType fieldType = fromFieldType(metaData.getColumnType(i));
            Field field = Field.of(metaData.getColumnName(i), fieldType).withNullable(metaData.isNullable(i) != ResultSetMetaData.columnNoNulls);
            schemaBuilder.addField(field);
        }
        return schemaBuilder.build();
    }

    /** Convert a JDBC {@link ResultSet} to a Beam {@link Schema}. */
    public static Schema fromResultSetToSchema(ResultSet resultSet) throws SQLException {
        return fromResultSetMetaData(resultSet.getMetaData());
    }

}
