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
 * fragment figures, the per column index summaries, the figures read
 * from Lance's index statistics, the lazily read zone map, and the
 * deleted and unindexed rows after a delete and an append.
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
        assertTrue(btree.statisticsAvailable());
        assertEquals(OptionalLong.of(FRAGMENTS * ROWS_PER_FRAGMENT), btree.indexedRows());
        assertEquals(OptionalLong.of(0L), btree.unindexedRows());
        assertEquals("a BTree reports no cardinality", OptionalLong.empty(), btree.distinctCount());
        assertTrue("the manifest records the index size", btree.sizeBytes().isPresent() && btree.sizeBytes().getAsLong() > 0L);

        IndexSummary bitmap = onlyIndex(statistics, "category");
        assertEquals("category_bitmap", bitmap.name());
        assertEquals(Optional.of(IndexType.BITMAP), bitmap.type());
        assertTrue(bitmap.statisticsAvailable());
        assertEquals(OptionalLong.of(FRAGMENTS * ROWS_PER_FRAGMENT), bitmap.indexedRows());
        // c0, c1, c2 plus the null bitmap (every fourth row is null).
        assertEquals(OptionalLong.of(4L), bitmap.distinctCount());
        assertEquals(OptionalLong.of(4L), statistics.column("category").get().distinctCount());
        assertTrue(statistics.column("category").get().hasIndex(IndexType.BITMAP));
        assertFalse(statistics.column("category").get().hasIndex(IndexType.BTREE));

        IndexSummary inverted = onlyIndex(statistics, "body");
        assertEquals("body_fts", inverted.name());
        assertEquals(Optional.of(IndexType.INVERTED), inverted.type());
        assertTrue(inverted.statisticsAvailable());
        assertEquals(OptionalLong.of(FRAGMENTS * ROWS_PER_FRAGMENT), inverted.indexedRows());
        assertEquals(OptionalLong.empty(), inverted.distinctCount());

        IndexSummary vector = onlyIndex(statistics, "embedding");
        assertEquals("embedding_ivf", vector.name());
        assertEquals(Optional.of(IndexType.IVF_PQ), vector.type());
        assertTrue(vector.statisticsAvailable());
        assertEquals(OptionalLong.of(FRAGMENTS * ROWS_PER_FRAGMENT), vector.indexedRows());
        assertEquals(OptionalLong.empty(), vector.distinctCount());

        IndexSummary zoneMap = onlyIndex(statistics, "id");
        assertEquals("id_zonemap", zoneMap.name());
        assertEquals(Optional.of(IndexType.ZONEMAP), zoneMap.type());
        assertTrue(zoneMap.statisticsAvailable());
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
        assertEquals(OptionalLong.of(2 * ROWS_PER_FRAGMENT - 10), btree.indexedRows());
        assertEquals(OptionalLong.of(ROWS_PER_FRAGMENT), btree.unindexedRows());

        IndexSummary inverted = onlyIndex(statistics, "body");
        assertTrue(inverted.coversAllFragments());
        assertEquals(OptionalLong.of(0L), inverted.unindexedRows());
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
}
