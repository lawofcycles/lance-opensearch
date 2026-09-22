/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.plan.calcite;

import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.schema.Schema.TableType;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The table's row type must be exactly what {@link LanceTypeSystem} maps
 * from the Arrow schema, and the statistic must read the row count supplier
 * on every call instead of caching a value.
 */
public class LanceTableTests extends OpenSearchTestCase {

    private static final Schema FIVE_COLUMNS = new Schema(
        List.of(
            field("id", new ArrowType.Int(32, true), false),
            field("body", new ArrowType.Utf8(), true),
            field("rating", new ArrowType.Int(64, true), true),
            field("price", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE), true),
            field("created", new ArrowType.Timestamp(TimeUnit.MICROSECOND, "UTC"), true)
        )
    );

    private static Field field(String name, ArrowType arrowType, boolean nullable) {
        return new Field(name, new FieldType(nullable, arrowType, null), null);
    }

    public void testRowTypeMatchesTypeSystem() {
        LanceTable table = new LanceTable("idx", FIVE_COLUMNS, () -> 0L);
        SqlTypeFactoryImpl factory = new SqlTypeFactoryImpl(LanceTypeSystem.INSTANCE);
        RelDataType fromTable = table.getRowType(factory);
        RelDataType fromTypeSystem = LanceTypeSystem.rowType(FIVE_COLUMNS, factory);
        assertEquals(fromTypeSystem, fromTable);
        assertEquals(List.of("id", "body", "rating", "price", "created"), fromTable.getFieldNames());
    }

    public void testStatisticReadsSupplier() {
        AtomicLong rows = new AtomicLong(512);
        LanceTable table = new LanceTable("idx", FIVE_COLUMNS, rows::get);
        assertEquals(512.0, table.getStatistic().getRowCount(), 0.0);
        rows.set(1024);
        assertEquals(1024.0, table.getStatistic().getRowCount(), 0.0);
    }

    public void testJdbcTableType() {
        LanceTable table = new LanceTable("idx", FIVE_COLUMNS, () -> 0L);
        assertEquals(TableType.TABLE, table.getJdbcTableType());
    }
}
