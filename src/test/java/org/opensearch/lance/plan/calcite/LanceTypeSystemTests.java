/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.plan.calcite;

import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

/**
 * Pins every Arrow to Calcite mapping of {@link LanceTypeSystem}. The type
 * factory is built with {@link LanceTypeSystem#INSTANCE} because Calcite's
 * default type system clamps timestamp precision to 3 and decimal precision
 * to 19, which would silently rewrite the microsecond / nanosecond and
 * unsigned 64-bit mappings under test.
 */
public class LanceTypeSystemTests extends OpenSearchTestCase {

    private final SqlTypeFactoryImpl factory = new SqlTypeFactoryImpl(LanceTypeSystem.INSTANCE);

    private RelDataType map(ArrowType arrowType, boolean nullable) {
        return LanceTypeSystem.toRelDataType(field("f", arrowType, nullable), factory);
    }

    private static Field field(String name, ArrowType arrowType, boolean nullable) {
        return new Field(name, new FieldType(nullable, arrowType, null), null);
    }

    public void testBool() {
        RelDataType type = map(new ArrowType.Bool(), true);
        assertEquals(SqlTypeName.BOOLEAN, type.getSqlTypeName());
        assertTrue(type.isNullable());
    }

    public void testInt8Signed() {
        RelDataType type = map(new ArrowType.Int(8, true), false);
        assertEquals(SqlTypeName.TINYINT, type.getSqlTypeName());
        assertFalse(type.isNullable());
    }

    public void testInt16Signed() {
        assertEquals(SqlTypeName.SMALLINT, map(new ArrowType.Int(16, true), true).getSqlTypeName());
    }

    public void testInt32Signed() {
        assertEquals(SqlTypeName.INTEGER, map(new ArrowType.Int(32, true), true).getSqlTypeName());
    }

    public void testInt64Signed() {
        assertEquals(SqlTypeName.BIGINT, map(new ArrowType.Int(64, true), true).getSqlTypeName());
    }

    public void testInt8Unsigned() {
        assertEquals(SqlTypeName.SMALLINT, map(new ArrowType.Int(8, false), true).getSqlTypeName());
    }

    public void testInt16Unsigned() {
        assertEquals(SqlTypeName.INTEGER, map(new ArrowType.Int(16, false), true).getSqlTypeName());
    }

    public void testInt32Unsigned() {
        assertEquals(SqlTypeName.BIGINT, map(new ArrowType.Int(32, false), true).getSqlTypeName());
    }

    public void testInt64Unsigned() {
        RelDataType type = map(new ArrowType.Int(64, false), true);
        assertEquals(SqlTypeName.DECIMAL, type.getSqlTypeName());
        assertEquals(20, type.getPrecision());
        assertEquals(0, type.getScale());
    }

    public void testFloatSingle() {
        assertEquals(SqlTypeName.REAL, map(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE), true).getSqlTypeName());
    }

    public void testFloatDouble() {
        assertEquals(SqlTypeName.DOUBLE, map(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE), true).getSqlTypeName());
    }

    public void testFloatHalf() {
        assertEquals(SqlTypeName.REAL, map(new ArrowType.FloatingPoint(FloatingPointPrecision.HALF), true).getSqlTypeName());
    }

    public void testUtf8() {
        assertEquals(SqlTypeName.VARCHAR, map(new ArrowType.Utf8(), true).getSqlTypeName());
    }

    public void testLargeUtf8() {
        assertEquals(SqlTypeName.VARCHAR, map(new ArrowType.LargeUtf8(), true).getSqlTypeName());
    }

    public void testBinary() {
        assertEquals(SqlTypeName.VARBINARY, map(new ArrowType.Binary(), true).getSqlTypeName());
    }

    public void testLargeBinary() {
        assertEquals(SqlTypeName.VARBINARY, map(new ArrowType.LargeBinary(), true).getSqlTypeName());
    }

    public void testDateDay() {
        assertEquals(SqlTypeName.DATE, map(new ArrowType.Date(DateUnit.DAY), true).getSqlTypeName());
    }

    public void testDateMillisecond() {
        assertEquals(SqlTypeName.DATE, map(new ArrowType.Date(DateUnit.MILLISECOND), true).getSqlTypeName());
    }

    public void testTimestampWithoutZoneByUnit() {
        assertTimestamp(TimeUnit.SECOND, 0);
        assertTimestamp(TimeUnit.MILLISECOND, 3);
        assertTimestamp(TimeUnit.MICROSECOND, 6);
        assertTimestamp(TimeUnit.NANOSECOND, 9);
    }

    private void assertTimestamp(TimeUnit unit, int expectedPrecision) {
        RelDataType type = map(new ArrowType.Timestamp(unit, null), true);
        assertEquals(SqlTypeName.TIMESTAMP, type.getSqlTypeName());
        assertEquals(expectedPrecision, type.getPrecision());
    }

    public void testTimestampWithZoneByUnit() {
        assertZonedTimestamp(TimeUnit.SECOND, 0);
        assertZonedTimestamp(TimeUnit.MILLISECOND, 3);
        assertZonedTimestamp(TimeUnit.MICROSECOND, 6);
        assertZonedTimestamp(TimeUnit.NANOSECOND, 9);
    }

    private void assertZonedTimestamp(TimeUnit unit, int expectedPrecision) {
        RelDataType type = map(new ArrowType.Timestamp(unit, "UTC"), true);
        assertEquals(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE, type.getSqlTypeName());
        assertEquals(expectedPrecision, type.getPrecision());
    }

    public void testListOfUtf8() {
        Field list = new Field(
            "tags",
            new FieldType(true, new ArrowType.List(), null),
            List.of(field("item", new ArrowType.Utf8(), false))
        );
        RelDataType type = LanceTypeSystem.toRelDataType(list, factory);
        assertEquals(SqlTypeName.ARRAY, type.getSqlTypeName());
        assertTrue(type.isNullable());
        assertEquals(SqlTypeName.VARCHAR, type.getComponentType().getSqlTypeName());
        assertFalse(type.getComponentType().isNullable());
    }

    public void testListOfOtherTypeMapsComponentRecursively() {
        Field list = new Field(
            "readings",
            new FieldType(true, new ArrowType.List(), null),
            List.of(field("item", new ArrowType.Int(64, true), true))
        );
        RelDataType type = LanceTypeSystem.toRelDataType(list, factory);
        assertEquals(SqlTypeName.ARRAY, type.getSqlTypeName());
        assertEquals(SqlTypeName.BIGINT, type.getComponentType().getSqlTypeName());
        assertTrue(type.getComponentType().isNullable());
    }

    public void testFixedSizeListOfFloat32() {
        Field vector = new Field(
            "embedding",
            new FieldType(true, new ArrowType.FixedSizeList(384), null),
            List.of(field("item", new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE), false))
        );
        RelDataType type = LanceTypeSystem.toRelDataType(vector, factory);
        assertEquals(SqlTypeName.ARRAY, type.getSqlTypeName());
        assertEquals(SqlTypeName.REAL, type.getComponentType().getSqlTypeName());
    }

    public void testFixedSizeListOfNonFloat32Throws() {
        Field vector = new Field(
            "codes",
            new FieldType(true, new ArrowType.FixedSizeList(8), null),
            List.of(field("item", new ArrowType.Int(32, true), false))
        );
        UnsupportedOperationException e = expectThrows(
            UnsupportedOperationException.class,
            () -> LanceTypeSystem.toRelDataType(vector, factory)
        );
        assertTrue(e.getMessage(), e.getMessage().contains("FixedSizeList"));
        assertTrue(e.getMessage(), e.getMessage().contains("codes"));
    }

    public void testStruct() {
        Field struct = new Field(
            "location",
            new FieldType(false, new ArrowType.Struct(), null),
            List.of(
                field("lat", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE), false),
                field("name", new ArrowType.Utf8(), true)
            )
        );
        RelDataType type = LanceTypeSystem.toRelDataType(struct, factory);
        assertEquals(SqlTypeName.ROW, type.getSqlTypeName());
        assertFalse(type.isNullable());
        List<RelDataTypeField> fields = type.getFieldList();
        assertEquals(2, fields.size());
        assertEquals("lat", fields.get(0).getName());
        assertEquals(SqlTypeName.DOUBLE, fields.get(0).getType().getSqlTypeName());
        assertFalse(fields.get(0).getType().isNullable());
        assertEquals("name", fields.get(1).getName());
        assertEquals(SqlTypeName.VARCHAR, fields.get(1).getType().getSqlTypeName());
        assertTrue(fields.get(1).getType().isNullable());
    }

    public void testNullableStructMakesChildrenNullable() {
        // Calcite's type factory propagates a record's nullability into its
        // fields (RelDataTypeFactoryImpl.copyRecordType), so a nullable
        // Arrow struct cannot keep a non-nullable child. Pin that here so a
        // future Calcite upgrade that changes the semantics is caught.
        Field struct = new Field(
            "location",
            new FieldType(true, new ArrowType.Struct(), null),
            List.of(field("lat", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE), false))
        );
        RelDataType type = LanceTypeSystem.toRelDataType(struct, factory);
        assertEquals(SqlTypeName.ROW, type.getSqlTypeName());
        assertTrue(type.isNullable());
        assertTrue(type.getFieldList().get(0).getType().isNullable());
    }

    public void testUnsupportedTypeThrowsWithTypeNamed() {
        UnsupportedOperationException e = expectThrows(
            UnsupportedOperationException.class,
            () -> map(new ArrowType.Decimal(38, 10, 128), true)
        );
        assertTrue(e.getMessage(), e.getMessage().contains("Decimal"));
        assertTrue(e.getMessage(), e.getMessage().contains("f"));
    }

    public void testRowTypePreservesFieldOrderAndNames() {
        Schema schema = new Schema(
            List.of(
                field("id", new ArrowType.Int(64, true), false),
                field("title", new ArrowType.Utf8(), true),
                field("price", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE), true),
                field("created_at", new ArrowType.Timestamp(TimeUnit.MICROSECOND, null), true),
                field("in_stock", new ArrowType.Bool(), true)
            )
        );
        RelDataType row = LanceTypeSystem.rowType(schema, factory);
        assertEquals(SqlTypeName.ROW, row.getSqlTypeName());
        List<RelDataTypeField> fields = row.getFieldList();
        assertEquals(5, fields.size());
        assertEquals(List.of("id", "title", "price", "created_at", "in_stock"), row.getFieldNames());
        assertEquals(SqlTypeName.BIGINT, fields.get(0).getType().getSqlTypeName());
        assertFalse(fields.get(0).getType().isNullable());
        assertEquals(SqlTypeName.VARCHAR, fields.get(1).getType().getSqlTypeName());
        assertEquals(SqlTypeName.DOUBLE, fields.get(2).getType().getSqlTypeName());
        assertEquals(SqlTypeName.TIMESTAMP, fields.get(3).getType().getSqlTypeName());
        assertEquals(6, fields.get(3).getType().getPrecision());
        assertEquals(SqlTypeName.BOOLEAN, fields.get(4).getType().getSqlTypeName());
    }
}
