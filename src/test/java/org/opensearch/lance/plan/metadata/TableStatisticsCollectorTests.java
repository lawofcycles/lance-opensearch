/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.plan.metadata;

import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;

import org.apache.arrow.memory.RootAllocator;
import org.lance.Dataset;
import org.lance.index.Index;
import org.lance.index.IndexOptions;
import org.lance.index.IndexParams;
import org.lance.index.IndexType;
import org.lance.index.scalar.ScalarIndexParams;
import org.lance.index.scalar.ZoneStats;
import org.opensearch.lance.LanceRegistry;
import org.opensearch.lance.LanceTableFactory;
import org.opensearch.lance.StorageOptions;
import org.opensearch.lance.plan.metadata.ColumnStatistics.IndexSummary;
import org.opensearch.lance.plan.metadata.TableStatisticsCollector.ParsedStatistics;
import org.opensearch.test.OpenSearchTestCase;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

/**
 * {@link TableStatisticsCollector} against a small table carrying one
 * index of each kind (BTree, bitmap, inverted, IVF_PQ, zone map): the
 * fragment figures, the per column index summaries from the manifest,
 * the figures read from Lance's index statistics for the bitmap and the
 * vector index alone, the lazily read zone map, and the deleted rows and the index
 * coverage after a delete and a partial index build.
 */
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class TableStatisticsCollectorTests extends OpenSearchTestCase {

    private static final int FRAGMENTS = 2;
    private static final int ROWS_PER_FRAGMENT = 200;

    private String uri;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        Path scratchDir = createTempDir();
        uri = LanceTableFactory.writeIndexedFixtureTable(scratchDir, "stats-" + getTestName(), FRAGMENTS, ROWS_PER_FRAGMENT);
        try (
            RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
            Dataset dataset = Dataset.open().allocator(allocator).uri(uri).build()
        ) {
            dataset.createIndex(
                IndexOptions.builder(
                    Collections.singletonList("id"),
                    IndexType.ZONEMAP,
                    IndexParams.builder().setScalarIndexParams(ScalarIndexParams.create("zonemap", "{\"rows_per_zone\":100}")).build()
                ).withIndexName("id_zonemap").build()
            );
        }
    }

    private TableStatistics collect() {
        try (Dataset dataset = LanceRegistry.openDataset(uri, StorageOptions.empty())) {
            return TableStatisticsCollector.collect(dataset);
        }
    }

    private static IndexSummary onlyIndex(TableStatistics statistics, String column) {
        Optional<ColumnStatistics> stats = statistics.column(column);
        assertTrue("column " + column + " carries statistics: " + statistics.columns().keySet(), stats.isPresent());
        assertEquals("one index on " + column + ": " + stats.get().indexes(), 1, stats.get().indexes().size());
        return stats.get().indexes().get(0);
    }

    public void testCollectsFragmentsAndIndexesOfTheFixtureTable() {
        TableStatistics statistics = collect();

        assertEquals(FRAGMENTS * ROWS_PER_FRAGMENT, statistics.rowCount());
        assertEquals(0L, statistics.deletedRows());
        assertEquals(FRAGMENTS, statistics.fragments().size());
        for (int i = 0; i < FRAGMENTS; i++) {
            TableStatistics.FragmentStats fragment = statistics.fragments().get(i);
            assertEquals(i, fragment.id());
            assertEquals(ROWS_PER_FRAGMENT, fragment.rows());
            assertEquals(1, fragment.dataFiles());
        }
        assertTrue(statistics.datasetVersion() > 0L);
        assertNotNull(statistics.collectedAt());
        assertEquals("one column per index", Set.of("rating", "category", "body", "embedding", "id"), statistics.columns().keySet());

        IndexSummary btree = onlyIndex(statistics, "rating");
        assertEquals("rating_btree", btree.name());
        assertEquals(Optional.of(IndexType.BTREE), btree.type());
        assertEquals(FRAGMENTS, btree.coveredFragments());
        assertEquals(FRAGMENTS, btree.totalFragments());
        assertTrue(btree.coversAllFragments());
        assertTrue("the BTree's statistics carry its bounds, so they are read", btree.statisticsAvailable());
        assertEquals(OptionalLong.of(FRAGMENTS * ROWS_PER_FRAGMENT), btree.indexedRows());
        assertEquals(OptionalLong.of(0L), btree.unindexedRows());
        assertEquals("a BTree reports no cardinality", OptionalLong.empty(), btree.distinctCount());
        // rating is (i * 37) % 1000 with every fifth row null. Lance sorts
        // nulls first when it trains the BTree, so the first page starts
        // with a null and the statistics report a null smallest value
        // next to the largest, 999: a column with nulls yields no range.
        assertEquals("no range without both bounds", OptionalLong.empty(), btree.integerRange());
        assertEquals(OptionalLong.empty(), statistics.column("rating").get().distinctCount());
        assertEquals(OptionalLong.empty(), statistics.column("rating").get().distinctUpperBound());
        assertTrue("the manifest records the index size", btree.sizeBytes().isPresent() && btree.sizeBytes().getAsLong() > 0L);

        IndexSummary bitmap = onlyIndex(statistics, "category");
        assertEquals("category_bitmap", bitmap.name());
        assertEquals(Optional.of(IndexType.BITMAP), bitmap.type());
        assertTrue("the bitmap's statistics carry the distinct count, so they are read", bitmap.statisticsAvailable());
        assertEquals(OptionalLong.of(FRAGMENTS * ROWS_PER_FRAGMENT), bitmap.indexedRows());
        assertEquals(OptionalLong.of(0L), bitmap.unindexedRows());
        // c0, c1, c2 plus the null bitmap (every fourth row is null).
        assertEquals(OptionalLong.of(4L), bitmap.distinctCount());
        assertEquals("a bitmap has no integer range", OptionalLong.empty(), bitmap.integerRange());
        assertEquals(OptionalLong.of(4L), statistics.column("category").get().distinctCount());
        assertEquals(OptionalLong.of(4L), statistics.column("category").get().distinctUpperBound());
        assertTrue(statistics.column("category").get().hasIndex(IndexType.BITMAP));
        assertFalse(statistics.column("category").get().hasIndex(IndexType.BTREE));

        IndexSummary inverted = onlyIndex(statistics, "body");
        assertEquals("body_fts", inverted.name());
        assertEquals(Optional.of(IndexType.INVERTED), inverted.type());
        assertFalse("an inverted index's statistics are not read", inverted.statisticsAvailable());
        assertEquals(OptionalLong.empty(), inverted.indexedRows());
        assertEquals(OptionalLong.empty(), inverted.distinctCount());
        assertTrue(inverted.coversAllFragments());
        assertTrue(inverted.sizeBytes().isPresent() && inverted.sizeBytes().getAsLong() > 0L);

        IndexSummary vector = onlyIndex(statistics, "embedding");
        assertEquals("embedding_ivf", vector.name());
        assertEquals("the statistics name the concrete vector type", Optional.of(IndexType.IVF_PQ), vector.type());
        assertTrue("a vector index's statistics carry the partition count, so they are read", vector.statisticsAvailable());
        assertEquals(OptionalLong.of(FRAGMENTS * ROWS_PER_FRAGMENT), vector.indexedRows());
        assertEquals(OptionalLong.empty(), vector.distinctCount());
        assertEquals("the fixture trains one IVF partition", OptionalLong.of(1L), vector.partitions());
        assertEquals("a scalar index has no partitions", OptionalLong.empty(), btree.partitions());
        assertEquals(OptionalLong.empty(), inverted.partitions());
        assertTrue(vector.sizeBytes().isPresent() && vector.sizeBytes().getAsLong() > 0L);

        IndexSummary zoneMap = onlyIndex(statistics, "id");
        assertEquals("id_zonemap", zoneMap.name());
        assertEquals(Optional.of(IndexType.ZONEMAP), zoneMap.type());
        assertFalse("a zone map's statistics are not read; its zones are read lazily", zoneMap.statisticsAvailable());
        assertEquals(OptionalLong.empty(), zoneMap.distinctCount());
    }

    public void testZoneMapIsReadLazilyAndMemoised() {
        TableStatistics statistics = collect();
        ColumnStatistics id = statistics.column("id").get();
        assertEquals("not read at collection", Optional.empty(), id.zoneMapIfRead());
        try (Dataset dataset = LanceRegistry.openDataset(uri, StorageOptions.empty())) {
            List<ZoneStats> zones = id.zoneMap(dataset);
            // 100 rows per zone over two fragments of 200 rows.
            assertEquals(4, zones.size());
            long coveredRows = 0L;
            for (ZoneStats zone : zones) {
                coveredRows += zone.getZoneLength();
                assertEquals(0L, zone.getNullCount());
                assertTrue(zone.getFragmentId() == 0 || zone.getFragmentId() == 1);
            }
            assertEquals(FRAGMENTS * ROWS_PER_FRAGMENT, coveredRows);
            assertSame("memoised", zones, id.zoneMap(dataset));
            assertEquals(Optional.of(zones), id.zoneMapIfRead());

            ColumnStatistics rating = statistics.column("rating").get();
            assertTrue("no zone map index on rating", rating.zoneMap(dataset).isEmpty());
        }
    }

    public void testReadZoneMapsReadsTheNamedZoneMappedColumnsOnly() {
        TableStatistics statistics = collect();
        try (Dataset dataset = LanceRegistry.openDataset(uri, StorageOptions.empty())) {
            // rating carries a BTree, not a zone map; naming it reads nothing.
            statistics.readZoneMaps(dataset, Set.of("rating", "body"));
            assertEquals(Optional.empty(), statistics.column("id").get().zoneMapIfRead());
            assertEquals(Optional.empty(), statistics.column("rating").get().zoneMapIfRead());
            // An empty field set reads nothing either.
            statistics.readZoneMaps(dataset, Set.of());
            assertEquals(Optional.empty(), statistics.column("id").get().zoneMapIfRead());
            // A dotted path below the column names it.
            statistics.readZoneMaps(dataset, Set.of("id.raw"));
            assertEquals(4, statistics.column("id").get().zoneMapIfRead().orElseThrow().size());
        }
        assertTrue(TableStatistics.namesColumn(Set.of("meta.region"), "meta"));
        assertTrue(TableStatistics.namesColumn(Set.of("meta.region"), "meta.region"));
        assertFalse(TableStatistics.namesColumn(Set.of("metadata"), "meta"));
        assertFalse(TableStatistics.namesColumn(Set.of("meta"), "meta.region"));
    }

    public void testDeletedAndUnindexedRowsAfterDeleteAndPartialIndex() throws Exception {
        // A fresh three fragment table: an inverted index over body
        // covers every fragment, a BTree over rating is built for the
        // first two fragments only, then ten rows are deleted.
        String partialUri = LanceTableFactory.writeHintFixtureTable(createTempDir(), "partial-" + getTestName(), 3, ROWS_PER_FRAGMENT);
        long baseVersion;
        try (
            RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
            Dataset dataset = Dataset.open().allocator(allocator).uri(partialUri).build()
        ) {
            baseVersion = dataset.version();
            IndexOptions options = IndexOptions.builder(
                Collections.singletonList("rating"),
                IndexType.BTREE,
                IndexParams.builder().setScalarIndexParams(ScalarIndexParams.create("btree")).build()
            ).withIndexName("rating_btree").withFragmentIds(List.of(0, 1)).build();
            Index segment = dataset.createIndex(options);
            dataset.commitExistingIndexSegments("rating_btree", "rating", Collections.singletonList(segment));
            dataset.delete("id < 10");
        }

        TableStatistics statistics;
        try (Dataset dataset = LanceRegistry.openDataset(partialUri, StorageOptions.empty())) {
            statistics = TableStatisticsCollector.collect(dataset);
        }
        assertTrue(statistics.datasetVersion() > baseVersion);
        assertEquals(3 * ROWS_PER_FRAGMENT - 10, statistics.rowCount());
        assertEquals(10L, statistics.deletedRows());
        assertEquals(3, statistics.fragments().size());
        assertEquals(ROWS_PER_FRAGMENT - 10, statistics.fragments().get(0).rows());
        assertEquals(ROWS_PER_FRAGMENT, statistics.fragments().get(2).rows());

        IndexSummary btree = onlyIndex(statistics, "rating");
        assertEquals(2, btree.coveredFragments());
        assertEquals(3, btree.totalFragments());
        assertFalse(btree.coversAllFragments());
        assertEquals(
            "coverage comes from the manifest, the row figures from the statistics",
            OptionalLong.of(ROWS_PER_FRAGMENT),
            btree.unindexedRows()
        );
        assertTrue(btree.indexedRows().isPresent());
        assertEquals("rating holds nulls, so no smallest value and no range", OptionalLong.empty(), btree.integerRange());

        IndexSummary inverted = onlyIndex(statistics, "body");
        assertTrue(inverted.coversAllFragments());
        assertEquals(3, inverted.coveredFragments());
    }

    public void testStatisticsWithoutTheExpectedKeysLeaveTheFiguresEmpty() {
        ParsedStatistics none = TableStatisticsCollector.readStatistics(null, Optional.of(IndexType.BITMAP));
        assertEquals(Optional.of(IndexType.BITMAP), none.type());
        assertEquals(OptionalLong.empty(), none.indexedRows());
        assertEquals(OptionalLong.empty(), none.unindexedRows());
        assertEquals(OptionalLong.empty(), none.distinctCount());

        ParsedStatistics partial = TableStatisticsCollector.readStatistics(
            Map.of("num_indexed_rows", 12, "num_unindexed_rows", "not a number", "indices", List.of(Map.of("num_pages", 3))),
            Optional.of(IndexType.BITMAP)
        );
        assertEquals(OptionalLong.of(12L), partial.indexedRows());
        assertEquals(OptionalLong.empty(), partial.unindexedRows());
        assertEquals("no num_bitmaps entry", OptionalLong.empty(), partial.distinctCount());

        ParsedStatistics deltas = TableStatisticsCollector.readStatistics(
            Map.of("num_indexed_rows", 7L, "num_unindexed_rows", 0, "indices", List.of(Map.of("num_bitmaps", 3), Map.of("num_bitmaps", 2))),
            Optional.of(IndexType.BITMAP)
        );
        assertEquals("num_bitmaps summed over the deltas", OptionalLong.of(5L), deltas.distinctCount());

        ParsedStatistics btree = TableStatisticsCollector.readStatistics(
            Map.of("num_indexed_rows", 7L, "indices", List.of(Map.of("num_bitmaps", 3))),
            Optional.of(IndexType.BTREE)
        );
        assertEquals("only a bitmap index yields a cardinality", OptionalLong.empty(), btree.distinctCount());
        assertEquals("no bounds, no range", OptionalLong.empty(), btree.integerRange());

        ParsedStatistics unknownType = TableStatisticsCollector.readStatistics(Map.of("num_indexed_rows", 1), Optional.empty());
        assertEquals(Optional.empty(), unknownType.type());
        assertEquals(OptionalLong.of(1L), unknownType.indexedRows());
        assertEquals(OptionalLong.empty(), unknownType.distinctCount());

        // The manifest says VECTOR for every vector index; the
        // statistics name the concrete type.
        ParsedStatistics vector = TableStatisticsCollector.readStatistics(Map.of("index_type", "IVF_PQ"), Optional.of(IndexType.VECTOR));
        assertEquals(Optional.of(IndexType.IVF_PQ), vector.type());
        assertEquals(
            Optional.of(IndexType.IVF_HNSW_SQ),
            TableStatisticsCollector.readStatistics(Map.of("index_type", "IVF_HNSW_SQ"), Optional.of(IndexType.VECTOR)).type()
        );
        assertEquals(
            Optional.of(IndexType.BLOOM_FILTER),
            TableStatisticsCollector.readStatistics(Map.of("index_type", "BloomFilter"), Optional.empty()).type()
        );
        assertEquals(
            "a string naming no type keeps the declared one",
            Optional.of(IndexType.VECTOR),
            TableStatisticsCollector.readStatistics(Map.of("index_type", "N/A"), Optional.of(IndexType.VECTOR)).type()
        );
        assertEquals(
            "a bitmap named by the statistics yields its cardinality",
            OptionalLong.of(2L),
            TableStatisticsCollector.readStatistics(
                Map.of("index_type", "Bitmap", "indices", List.of(Map.of("num_bitmaps", 2))),
                Optional.empty()
            ).distinctCount()
        );
    }

    public void testIntegerRangeOfABTreeOverAColumnWithoutNulls() throws Exception {
        // id runs 0 to 299 without a null over three fragments of a
        // hundred rows, so the BTree reports both bounds and the range is
        // the row count; rating holds nulls and reports no smallest value.
        String rangeUri = LanceTableFactory.writeHintFixtureTable(createTempDir(), "range-" + getTestName(), 3, 100);
        try (
            RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
            Dataset dataset = Dataset.open().allocator(allocator).uri(rangeUri).build()
        ) {
            for (String column : List.of("id", "rating")) {
                dataset.createIndex(
                    IndexOptions.builder(
                        Collections.singletonList(column),
                        IndexType.BTREE,
                        IndexParams.builder().setScalarIndexParams(ScalarIndexParams.create("btree")).build()
                    ).withIndexName(column + "_btree").build()
                );
            }
        }
        TableStatistics statistics;
        try (Dataset dataset = LanceRegistry.openDataset(rangeUri, StorageOptions.empty())) {
            statistics = TableStatisticsCollector.collect(dataset);
        }
        IndexSummary id = onlyIndex(statistics, "id");
        assertTrue(id.statisticsAvailable());
        assertEquals(OptionalLong.of(300L), id.integerRange());
        assertEquals(OptionalLong.empty(), id.distinctCount());
        assertEquals(OptionalLong.of(300L), statistics.column("id").get().distinctUpperBound());
        assertEquals(
            "the admission estimate keeps to measured counts",
            OptionalLong.empty(),
            statistics.column("id").get().distinctCount()
        );
        IndexSummary rating = onlyIndex(statistics, "rating");
        assertTrue(rating.statisticsAvailable());
        assertEquals(OptionalLong.empty(), rating.integerRange());
    }

    public void testIntegerRangeIsReadFromTheBoundsOfAnIntegerColumnsBTree() {
        // Lance prints the bounds as the values' decimal text.
        Map<String, Object> rating = Map.of("index_type", "BTree", "indices", List.of(Map.of("min", "1", "max", "5", "num_pages", 3)));
        ParsedStatistics integer = TableStatisticsCollector.readStatistics(rating, Optional.of(IndexType.BTREE), true);
        assertEquals(Optional.of(IndexType.BTREE), integer.type());
        assertEquals("max - min + 1", OptionalLong.of(5L), integer.integerRange());
        assertEquals("the range is not a cardinality", OptionalLong.empty(), integer.distinctCount());
        assertEquals(
            "a string or float column's bounds enclose no finite set of values",
            OptionalLong.empty(),
            TableStatisticsCollector.readStatistics(rating, Optional.of(IndexType.BTREE), false).integerRange()
        );
        assertEquals(
            "the two argument form reads no range",
            OptionalLong.empty(),
            TableStatisticsCollector.readStatistics(rating, Optional.of(IndexType.BTREE)).integerRange()
        );
        // Several deltas: the smallest min and the largest max.
        ParsedStatistics deltas = TableStatisticsCollector.readStatistics(
            Map.of("index_type", "BTree", "indices", List.of(Map.of("min", "-3", "max", "4"), Map.of("min", "0", "max", "10"))),
            Optional.of(IndexType.BTREE),
            true
        );
        assertEquals(OptionalLong.of(14L), deltas.integerRange());
        // A delta without a bound (all its pages null) leaves the range empty.
        ParsedStatistics unbounded = TableStatisticsCollector.readStatistics(
            Map.of("index_type", "BTree", "indices", List.of(Map.of("min", "0", "max", "10"), Map.of("num_pages", 0))),
            Optional.of(IndexType.BTREE),
            true
        );
        assertEquals(OptionalLong.empty(), unbounded.integerRange());
        // A column with nulls: Lance sorts nulls first and prints the
        // first page's null smallest value as NULL.
        assertEquals(
            OptionalLong.empty(),
            TableStatisticsCollector.readStatistics(
                Map.of("index_type", "BTree", "indices", List.of(Map.of("min", "NULL", "max", "999", "num_pages", 1))),
                Optional.of(IndexType.BTREE),
                true
            ).integerRange()
        );
        // Bounds the JSON reader typed as numbers count when whole.
        assertEquals(
            OptionalLong.of(101L),
            TableStatisticsCollector.readStatistics(
                Map.of("index_type", "BTree", "indices", List.of(Map.of("min", 0, "max", 100L))),
                Optional.of(IndexType.BTREE),
                true
            ).integerRange()
        );
        assertEquals(
            "a fraction is not an integer bound",
            OptionalLong.empty(),
            TableStatisticsCollector.readStatistics(
                Map.of("index_type", "BTree", "indices", List.of(Map.of("min", "0.5", "max", "9.5"))),
                Optional.of(IndexType.BTREE),
                true
            ).integerRange()
        );
        assertEquals(
            "a date text is not an integer bound",
            OptionalLong.empty(),
            TableStatisticsCollector.readStatistics(
                Map.of("index_type", "BTree", "indices", List.of(Map.of("min", "2024-01-01T00:00:00", "max", "2024-12-31T00:00:00"))),
                Optional.of(IndexType.BTREE),
                true
            ).integerRange()
        );
        assertEquals(
            "a range that overflows a long is left empty",
            OptionalLong.empty(),
            TableStatisticsCollector.readStatistics(
                Map.of(
                    "index_type",
                    "BTree",
                    "indices",
                    List.of(Map.of("min", Long.toString(Long.MIN_VALUE), "max", Long.toString(Long.MAX_VALUE)))
                ),
                Optional.of(IndexType.BTREE),
                true
            ).integerRange()
        );
        assertEquals(
            "a bitmap's bounds are not read",
            OptionalLong.empty(),
            TableStatisticsCollector.readStatistics(
                Map.of("index_type", "Bitmap", "indices", List.of(Map.of("min", "1", "max", "5", "num_bitmaps", 5))),
                Optional.of(IndexType.BITMAP),
                true
            ).integerRange()
        );
        assertTrue(TableStatisticsCollector.readsStatistics(Optional.of(IndexType.BTREE)));
        assertFalse(TableStatisticsCollector.readsStatistics(Optional.of(IndexType.INVERTED)));
    }

    public void testDistinctUpperBoundFoldsTheIntegerRangeAndDistinctCountKeepsTheBitmapAlone() {
        IndexSummary btree = new IndexSummary(
            "rating_btree",
            Optional.of(IndexType.BTREE),
            1,
            1,
            OptionalLong.empty(),
            OptionalLong.empty(),
            OptionalLong.empty(),
            OptionalLong.empty(),
            OptionalLong.empty(),
            OptionalLong.of(5L),
            true
        );
        IndexSummary bitmap = new IndexSummary(
            "rating_bitmap",
            Optional.of(IndexType.BITMAP),
            1,
            1,
            OptionalLong.empty(),
            OptionalLong.empty(),
            OptionalLong.empty(),
            OptionalLong.of(3L),
            true
        );
        ColumnStatistics btreeOnly = new ColumnStatistics("rating", List.of(btree));
        assertEquals(OptionalLong.empty(), btreeOnly.distinctCount());
        assertEquals(OptionalLong.of(5L), btreeOnly.distinctUpperBound());
        ColumnStatistics both = new ColumnStatistics("rating", List.of(btree, bitmap));
        assertEquals("the bitmap's count is the estimate", OptionalLong.of(3L), both.distinctCount());
        assertEquals("the smaller of the count and the range", OptionalLong.of(3L), both.distinctUpperBound());
        IndexSummary wideBitmap = new IndexSummary(
            "rating_bitmap",
            Optional.of(IndexType.BITMAP),
            1,
            1,
            OptionalLong.empty(),
            OptionalLong.empty(),
            OptionalLong.empty(),
            OptionalLong.of(9L),
            true
        );
        assertEquals(OptionalLong.of(5L), new ColumnStatistics("rating", List.of(wideBitmap, btree)).distinctUpperBound());
    }

    public void testPartitionCountIsTheSmallestNumPartitionsOfAVectorIndexsDeltas() {
        // One delta: its count.
        ParsedStatistics one = TableStatisticsCollector.readStatistics(
            Map.of("index_type", "IVF_PQ", "indices", List.of(Map.of("num_partitions", 1024, "partitions", List.of()))),
            Optional.of(IndexType.VECTOR)
        );
        assertEquals(Optional.of(IndexType.IVF_PQ), one.type());
        assertEquals(OptionalLong.of(1024L), one.partitions());
        // Several deltas: a nearest scan probes nprobes partitions of
        // each, so the smallest count bounds the probed share.
        ParsedStatistics deltas = TableStatisticsCollector.readStatistics(
            Map.of("index_type", "IVF_HNSW_SQ", "indices", List.of(Map.of("num_partitions", 4096), Map.of("num_partitions", 256))),
            Optional.of(IndexType.VECTOR)
        );
        assertEquals(OptionalLong.of(256L), deltas.partitions());
        // No count, a non numeric one, or a zero: empty.
        assertEquals(
            OptionalLong.empty(),
            TableStatisticsCollector.readStatistics(Map.of("index_type", "IVF_PQ", "indices", List.of(Map.of())), Optional.empty())
                .partitions()
        );
        assertEquals(
            OptionalLong.empty(),
            TableStatisticsCollector.readStatistics(
                Map.of("index_type", "IVF_PQ", "indices", List.of(Map.of("num_partitions", "many"))),
                Optional.empty()
            ).partitions()
        );
        assertEquals(
            OptionalLong.empty(),
            TableStatisticsCollector.readStatistics(
                Map.of("index_type", "IVF_PQ", "indices", List.of(Map.of("num_partitions", 0))),
                Optional.empty()
            ).partitions()
        );
        // A scalar index's deltas are not read for a partition count.
        assertEquals(
            OptionalLong.empty(),
            TableStatisticsCollector.readStatistics(
                Map.of("index_type", "BTree", "indices", List.of(Map.of("num_partitions", 8))),
                Optional.empty()
            ).partitions()
        );
    }
}
