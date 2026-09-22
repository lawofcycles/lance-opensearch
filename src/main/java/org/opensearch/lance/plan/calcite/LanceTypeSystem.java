/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.plan.calcite;

import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystemImpl;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * Maps the Arrow schema of a Lance table to Calcite's type system.
 *
 * <p>The class doubles as the {@code RelDataTypeSystem} the planner's type
 * factories must be built with: Calcite's default system caps datetime
 * precision at milliseconds, while Arrow timestamps carry up to nanoseconds,
 * so {@link #getMaxPrecision} widens the timestamp cap to 9. A factory built
 * with the default system would silently clamp a microsecond or nanosecond
 * column to precision 3.
 *
 * <p>Unsigned integers map to the next wider signed type; unsigned 64-bit
 * needs {@code DECIMAL(20, 0)} because no wider signed integer exists.
 * A fixed size list of single precision floats is the Arrow shape of a
 * Lance vector column and maps to an array of {@code REAL}; the fixed
 * length is not represented in the Calcite type yet and is left to the
 * planner phases that cost vector scans. A nullable Arrow struct maps to a
 * nullable {@code ROW} whose fields all become nullable, because Calcite's
 * type factory propagates a record's nullability into its fields.
 */
public final class LanceTypeSystem extends RelDataTypeSystemImpl {

    /** Singleton; the class carries no state. */
    public static final LanceTypeSystem INSTANCE = new LanceTypeSystem();

    /** Precision of an unsigned 64-bit integer rendered as a decimal: ceil(log10(2^64)). */
    private static final int UINT64_DECIMAL_PRECISION = 20;

    private LanceTypeSystem() {}

    @Override
    public int getMaxPrecision(SqlTypeName typeName) {
        switch (typeName) {
            case TIMESTAMP:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return 9;
            default:
                return super.getMaxPrecision(typeName);
        }
    }

    @Override
    public int getMaxNumericPrecision() {
        // Calcite's default of 19 would clamp the DECIMAL(20, 0) that
        // represents an unsigned 64-bit integer.
        return UINT64_DECIMAL_PRECISION;
    }

    /**
     * Maps every field of an Arrow schema, preserving field order and names.
     */
    public static RelDataType rowType(Schema schema, RelDataTypeFactory factory) {
        RelDataTypeFactory.Builder builder = factory.builder();
        for (Field field : schema.getFields()) {
            builder.add(field.getName(), toRelDataType(field, factory));
        }
        return builder.build();
    }

    /**
     * Maps one Arrow field to the Calcite type described in the class
     * comment, carrying the field's nullability onto the Calcite type.
     *
     * @throws UnsupportedOperationException naming the Arrow type when the
     *     field's type has no mapping
     */
    public static RelDataType toRelDataType(Field field, RelDataTypeFactory factory) {
        return factory.createTypeWithNullability(map(field, factory), field.isNullable());
    }

    private static RelDataType map(Field field, RelDataTypeFactory factory) {
        ArrowType arrowType = field.getType();
        if (arrowType instanceof ArrowType.Bool) {
            return factory.createSqlType(SqlTypeName.BOOLEAN);
        }
        if (arrowType instanceof ArrowType.Int) {
            return mapInt(field, (ArrowType.Int) arrowType, factory);
        }
        if (arrowType instanceof ArrowType.FloatingPoint) {
            FloatingPointPrecision precision = ((ArrowType.FloatingPoint) arrowType).getPrecision();
            return factory.createSqlType(precision == FloatingPointPrecision.DOUBLE ? SqlTypeName.DOUBLE : SqlTypeName.REAL);
        }
        if (arrowType instanceof ArrowType.Utf8 || arrowType instanceof ArrowType.LargeUtf8) {
            return factory.createSqlType(SqlTypeName.VARCHAR);
        }
        if (arrowType instanceof ArrowType.Binary || arrowType instanceof ArrowType.LargeBinary) {
            return factory.createSqlType(SqlTypeName.VARBINARY);
        }
        if (arrowType instanceof ArrowType.Date) {
            return factory.createSqlType(SqlTypeName.DATE);
        }
        if (arrowType instanceof ArrowType.Timestamp) {
            return mapTimestamp(field, (ArrowType.Timestamp) arrowType, factory);
        }
        if (arrowType instanceof ArrowType.List) {
            return factory.createArrayType(toRelDataType(onlyChild(field), factory), -1);
        }
        if (arrowType instanceof ArrowType.FixedSizeList) {
            Field child = onlyChild(field);
            ArrowType childType = child.getType();
            if (childType instanceof ArrowType.FloatingPoint
                && ((ArrowType.FloatingPoint) childType).getPrecision() == FloatingPointPrecision.SINGLE) {
                return factory.createArrayType(toRelDataType(child, factory), -1);
            }
            throw unsupported(field);
        }
        if (arrowType instanceof ArrowType.Struct) {
            RelDataTypeFactory.Builder builder = factory.builder();
            for (Field child : field.getChildren()) {
                builder.add(child.getName(), toRelDataType(child, factory));
            }
            return builder.build();
        }
        throw unsupported(field);
    }

    private static RelDataType mapInt(Field field, ArrowType.Int intType, RelDataTypeFactory factory) {
        if (intType.getIsSigned()) {
            switch (intType.getBitWidth()) {
                case 8:
                    return factory.createSqlType(SqlTypeName.TINYINT);
                case 16:
                    return factory.createSqlType(SqlTypeName.SMALLINT);
                case 32:
                    return factory.createSqlType(SqlTypeName.INTEGER);
                case 64:
                    return factory.createSqlType(SqlTypeName.BIGINT);
                default:
                    throw unsupported(field);
            }
        }
        switch (intType.getBitWidth()) {
            case 8:
                return factory.createSqlType(SqlTypeName.SMALLINT);
            case 16:
                return factory.createSqlType(SqlTypeName.INTEGER);
            case 32:
                return factory.createSqlType(SqlTypeName.BIGINT);
            case 64:
                return factory.createSqlType(SqlTypeName.DECIMAL, UINT64_DECIMAL_PRECISION, 0);
            default:
                throw unsupported(field);
        }
    }

    private static RelDataType mapTimestamp(Field field, ArrowType.Timestamp timestamp, RelDataTypeFactory factory) {
        final int precision;
        switch (timestamp.getUnit()) {
            case SECOND:
                precision = 0;
                break;
            case MILLISECOND:
                precision = 3;
                break;
            case MICROSECOND:
                precision = 6;
                break;
            case NANOSECOND:
                precision = 9;
                break;
            default:
                throw unsupported(field);
        }
        SqlTypeName typeName = timestamp.getTimezone() == null ? SqlTypeName.TIMESTAMP : SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE;
        return factory.createSqlType(typeName, precision);
    }

    private static Field onlyChild(Field field) {
        if (field.getChildren().size() != 1) {
            throw unsupported(field);
        }
        return field.getChildren().get(0);
    }

    private static UnsupportedOperationException unsupported(Field field) {
        return new UnsupportedOperationException("Unsupported Arrow type [" + field.getType() + "] on field [" + field.getName() + "]");
    }
}
