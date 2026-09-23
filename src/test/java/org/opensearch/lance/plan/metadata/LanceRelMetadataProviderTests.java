/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.plan.metadata;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;
import org.lance.index.IndexType;
import org.opensearch.lance.plan.calcite.LancePlannerFactory;
import org.opensearch.lance.plan.calcite.LanceSchema;
import org.opensearch.lance.plan.calcite.LanceTable;
import org.opensearch.lance.plan.metadata.ColumnStatistics.IndexSummary;
import org.opensearch.lance.plan.rel.LanceTableScan;
import org.opensearch.test.OpenSearchTestCase;

/**
 * {@link LanceRelMetadataProvider} through a cluster's metadata query:
 * the scan's row count comes from the table statistics, the distinct
 * row count of a bitmap indexed column from its {@code num_bitmaps},
 * a column without such an index keeps Calcite's default (unknown), and
 * a table built with a row count alone keeps the row count path.
 */
public class LanceRelMetadataProviderTests extends OpenSearchTestCase {

    private static final Schema SCHEMA = new Schema(
        List.of(
            new Field("id", new FieldType(false, new ArrowType.Int(32, true), null), null),
            new Field("category", FieldType.nullable(new ArrowType.Utf8()), null),
            new Field("rating", FieldType.nullable(new ArrowType.Int(32, true)), null)
        )
    );

    private static IndexSummary index(String name, IndexType type, OptionalLong distinct) {
        return new IndexSummary(
            name,
            Optional.of(type),
            2,
            2,
            OptionalLong.of(1024L),
            OptionalLong.of(1000L),
            OptionalLong.of(0L),
            distinct,
            true
        );
    }

    private static TableStatistics statistics(long rows) {
        return new TableStatistics(
            rows,
            0L,
            List.of(new TableStatistics.FragmentStats(0, rows / 2, 1), new TableStatistics.FragmentStats(1, rows - rows / 2, 1)),
            Map.of(
                "category",
                new ColumnStatistics("category", List.of(index("category_bitmap", IndexType.BITMAP, OptionalLong.of(4L)))),
                "rating",
                new ColumnStatistics("rating", List.of(index("rating_btree", IndexType.BTREE, OptionalLong.empty())))
            ),
            7L,
            Instant.EPOCH
        );
    }

    private static RelBuilder builder(LanceTable table) {
        LancePlannerFactory factory = new LancePlannerFactory(1L << 30, 1L << 30);
        return factory.relBuilder(new LanceSchema(Map.of("t", table)));
    }

    private static LanceTableScan scan(LanceTable table) {
        return (LanceTableScan) builder(table).scan(LancePlannerFactory.SCHEMA_NAME, "t").build();
    }

    public void testRowCountComesFromTheTableStatistics() {
        TableStatistics stats = statistics(1000L);
        LanceTableScan scan = scan(new LanceTable("t", SCHEMA, () -> 1L, () -> stats));
        RelMetadataQuery mq = scan.getCluster().getMetadataQuery();
        assertEquals(1000.0, mq.getRowCount(scan), 0.0);
        assertEquals("the table statistic agrees", 1000.0, scan.getTable().getRowCount(), 0.0);

        RexBuilder rex = scan.getCluster().getRexBuilder();
        RexNode condition = rex.makeCall(SqlStdOperatorTable.IS_NOT_NULL, rex.makeInputRef(scan, 2));
        LanceTableScan filtered = scan.withPushedFilter(condition, "rating IS NOT NULL");
        double filteredRows = mq.getRowCount(filtered);
        assertTrue("a pushed filter scales the count down: " + filteredRows, filteredRows < 1000.0 && filteredRows > 0.0);
        assertEquals(filtered.estimateRowCount(mq), filteredRows, 0.0);
    }

    public void testRowCountWithoutStatisticsKeepsTheSupplier() {
        LanceTableScan scan = scan(new LanceTable("t", SCHEMA, () -> 512L));
        RelMetadataQuery mq = scan.getCluster().getMetadataQuery();
        assertEquals(512.0, mq.getRowCount(scan), 0.0);
        assertNull("no statistics, no cardinality", mq.getDistinctRowCount(scan, ImmutableBitSet.of(1), null));
    }

    public void testDistinctRowCountOfABitmapIndexedColumn() {
        TableStatistics stats = statistics(1000L);
        LanceTableScan scan = scan(new LanceTable("t", SCHEMA, () -> 1L, () -> stats));
        RelMetadataQuery mq = scan.getCluster().getMetadataQuery();

        assertEquals(4.0, mq.getDistinctRowCount(scan, ImmutableBitSet.of(1), null), 0.0);
        assertNull("a BTree reports no cardinality", mq.getDistinctRowCount(scan, ImmutableBitSet.of(2), null));
        assertNull("a column without an index has none", mq.getDistinctRowCount(scan, ImmutableBitSet.of(0), null));
        assertNull("one unknown key column makes the group unknown", mq.getDistinctRowCount(scan, ImmutableBitSet.of(1, 2), null));

        RexBuilder rex = scan.getCluster().getRexBuilder();
        RexNode predicate = rex.makeCall(SqlStdOperatorTable.IS_NOT_NULL, rex.makeInputRef(scan, 2));
        Double withPredicate = mq.getDistinctRowCount(scan, ImmutableBitSet.of(1), predicate);
        assertNotNull(withPredicate);
        assertTrue("a predicate never raises the estimate: " + withPredicate, withPredicate <= 4.0 && withPredicate > 0.0);
    }

    public void testDistinctRowCountIsCappedByTheRowCount() {
        TableStatistics stats = statistics(3L);
        LanceTableScan scan = scan(new LanceTable("t", SCHEMA, () -> 1L, () -> stats));
        RelMetadataQuery mq = scan.getCluster().getMetadataQuery();
        assertEquals(
            "four bitmaps over three rows: three distinct at most",
            3.0,
            mq.getDistinctRowCount(scan, ImmutableBitSet.of(1), null),
            0.0
        );
    }

    public void testAggregateRowCountReadsTheDistinctEstimate() {
        TableStatistics stats = statistics(1000L);
        RelBuilder builder = builder(new LanceTable("t", SCHEMA, () -> 1L, () -> stats));
        RelNode aggregate = builder.scan(LancePlannerFactory.SCHEMA_NAME, "t")
            .aggregate(builder.groupKey(1), builder.count(false, "n"))
            .build();
        RelMetadataQuery mq = aggregate.getCluster().getMetadataQuery();
        assertEquals("one row per bitmap", 4.0, mq.getRowCount(aggregate), 0.0);
    }

    public void testProviderIsOneInstancePerNode() {
        LancePlannerFactory factory = new LancePlannerFactory(1L << 30, 1L << 30);
        assertSame(LanceRelMetadataProvider.INSTANCE, factory.newCluster().getMetadataProvider());
        assertSame(LanceRelMetadataProvider.INSTANCE, factory.newCluster().getMetadataProvider());
    }
}
