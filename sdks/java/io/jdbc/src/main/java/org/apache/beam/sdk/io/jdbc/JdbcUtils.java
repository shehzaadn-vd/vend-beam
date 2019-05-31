package org.apache.beam.sdk.io.jdbc;

import com.google.common.collect.ImmutableMap;
import org.apache.beam.sdk.schemas.LogicalTypes;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Instant;

import java.sql.JDBCType;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.sql.ResultSet;

public class JdbcUtils {

    /*
     * Mapping of JDBC types to Beam Schema types
     * TODO: Add support for BLOB and CLOB types
     */
    private static final ImmutableMap<JDBCType, FieldType> JDBC_TO_BEAM_TYPE_MAPPING =
            ImmutableMap.<JDBCType, FieldType>builder()
                    .put(JDBCType.BIGINT, Schema.FieldType.INT64)
                    .put(JDBCType.BINARY, Schema.FieldType.BYTES)
                    .put(JDBCType.BIT, Schema.FieldType.BOOLEAN)
                    .put(JDBCType.BOOLEAN, Schema.FieldType.BOOLEAN)
                    .put(JDBCType.CHAR, Schema.FieldType.STRING)
                    .put(JDBCType.DECIMAL, Schema.FieldType.DECIMAL)
                    .put(JDBCType.DOUBLE, Schema.FieldType.DOUBLE)
                    .put(JDBCType.FLOAT, Schema.FieldType.FLOAT)
                    .put(JDBCType.INTEGER, Schema.FieldType.INT32)
                    .put(JDBCType.LONGVARBINARY, Schema.FieldType.BYTES)
                    .put(JDBCType.LONGVARCHAR, Schema.FieldType.STRING)
                    .put(JDBCType.NUMERIC, Schema.FieldType.DECIMAL)
                    .put(JDBCType.REAL, Schema.FieldType.FLOAT)
                    .put(JDBCType.SMALLINT, Schema.FieldType.INT16)
                    .put(JDBCType.TIMESTAMP, Schema.FieldType.DATETIME)
                    .put(JDBCType.TINYINT, Schema.FieldType.INT16)
                    .put(JDBCType.VARBINARY, Schema.FieldType.BYTES)
                    .put(JDBCType.VARCHAR, Schema.FieldType.STRING)
                    .build();

    /**
     * Get the Beam {@link FieldType} from a {@link java.sql.ResultSetMetaData ColumnType}.
     *
     * <p>Supports JDBC Types</p>
     *
     * @param columnType of {@link java.sql.ResultSetMetaData ColumnType}
     * @return Corresponding Beam {@link FieldType}
     */
    private static FieldType toFieldType(int columnType, String columnTypeName) {
        JDBCType jdbcType = JDBCType.valueOf(columnType);
        switch (jdbcType) {
            case ARRAY:
                if (!JDBC_TO_BEAM_TYPE_MAPPING.containsKey(jdbcType))
                    throw new UnsupportedOperationException("Array of type " + jdbcType.name() + " is not supported");
                return JDBC_TO_BEAM_TYPE_MAPPING.get(jdbcType);
            case DATE:
                return Schema.FieldType.logicalType(
                        new LogicalTypes.PassThroughLogicalType<Instant>(
                                "SqlDateType", "", Schema.FieldType.DATETIME) {});
            case TIMESTAMP_WITH_TIMEZONE:
                return Schema.FieldType.logicalType(
                        new LogicalTypes.PassThroughLogicalType<Instant>(
                                "SqlTimestampWithLocalTzType", "", Schema.FieldType.DATETIME) {});
            default:
                if (!JDBC_TO_BEAM_TYPE_MAPPING.containsKey(jdbcType)) {
                    throw new UnsupportedOperationException("Representing fields of type [" + jdbcType.name() + "] in Beam schemas is not supported");
                }
                return JDBC_TO_BEAM_TYPE_MAPPING.get(jdbcType);
        }
    }

    public static Schema fromResultSetMetaDataToSchema(ResultSetMetaData metaData) throws SQLException {
        Schema.Builder schemaBuilder = Schema.builder();
        for (int i = 1; i <= metaData.getColumnCount(); i++) {
            FieldType fieldType = toFieldType(metaData.getColumnType(i), metaData.getColumnTypeName(i));
            Field field = Field.of(metaData.getColumnLabel(i), fieldType).withNullable(metaData.isNullable(i) != ResultSetMetaData.columnNoNulls);
            schemaBuilder.addField(field);
        }
        return schemaBuilder.build();
    }

    /**
     * Tries to convert a JDBC {@link ResultSet} row into a Beam {@link Row}.
     */
    public static Row toBeamRow(Schema rowSchema, ResultSet resultSet) {
        return rowSchema.getFields().stream()
            .map(field -> {
                try {
                    return toBeamRowFieldValue(field, resultSet.getObject(field.getName()));
                } catch (SQLException e) {
                   throw new RuntimeException("Error while accessing the column value from database.", e);
                }
            }).collect(Row.toRow(rowSchema));
    }

    private static Object toBeamRowFieldValue(Schema.Field field, Object jdbcRowValue) {
        if (jdbcRowValue == null) {
            if (field.getType().getNullable())
                return null;
            else
                throw new IllegalArgumentException("Received null value for non-nullable field " + field.getName());
        }
        return jdbcRowValue;
    }
}
