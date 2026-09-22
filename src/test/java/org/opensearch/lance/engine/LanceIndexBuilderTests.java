/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.engine;

import java.nio.file.Path;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;
import org.apache.arrow.memory.RootAllocator;
import org.lance.Dataset;
import org.lance.index.IndexCriteria;
import org.lance.index.IndexDescription;
import org.opensearch.lance.LanceOverrides;
import org.opensearch.lance.LanceTableFactory;
import org.opensearch.test.OpenSearchTestCase;

/**
 * The builder's index type selection against a real Lance table: a
 * preference from the {@code indexes} clause picks the scalar or vector
 * type (and its params) instead of the fixed BTree / IVF_PQ, {@code none}
 * skips the column, an absent preference keeps the defaults, and the
 * per-type training minimums gate the vector build.
 */
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class LanceIndexBuilderTests extends OpenSearchTestCase {

    public void testScalarPreferencesPickTheTypeAndParams() throws Exception {
        Path scratch = createTempDir();
        String uri = LanceTableFactory.writeHintFixtureTable(scratch, "scalar-prefs", 2, 10);
        Map<String, LanceOverrides.IndexPreference> preferences = LanceOverrides.parseIndexesClause(
            Map.of(
                "rating",
                Map.of("scalar", "zonemap", "params", Map.of("rows_per_zone", 8)),
                "category",
                Map.of("scalar", "bitmap"),
                "id",
                Map.of("scalar", "bloomfilter", "params", Map.of("number_of_items", 1000, "probability", 0.01)),
                "flag",
                Map.of("scalar", "none")
            )
        );
        try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE); Dataset dataset = open(allocator, uri)) {
            LanceIndexBuilder.BuildResult result = LanceIndexBuilder.ensureScalarIndexes(
                dataset,
                Set.of("id", "rating", "category", "flag"),
                Long.MAX_VALUE,
                Optional.empty(),
                preferences
            );
            assertEquals("failures: " + result.failed(), 0, result.failed().size());
            assertTrue(result.built().contains(new LanceIndexBuilder.Built("rating", "ZONEMAP")));
            assertTrue(result.built().contains(new LanceIndexBuilder.Built("category", "BITMAP")));
            assertTrue(result.built().contains(new LanceIndexBuilder.Built("id", "BLOOM_FILTER")));
            assertEquals(List.of(new LanceIndexBuilder.Skipped("flag", "index type none")), result.skipped());

            assertEquals("ZONEMAP", describedType(dataset, "rating"));
            assertEquals("BITMAP", describedType(dataset, "category"));
            assertEquals("BLOOMFILTER", describedType(dataset, "id"));
            assertTrue("flag must carry no index", dataset.describeIndices(forColumn("flag")).isEmpty());
        }
    }

    public void testAbsentPreferenceKeepsTheBTreeDefault() throws Exception {
        Path scratch = createTempDir();
        String uri = LanceTableFactory.writeHintFixtureTable(scratch, "scalar-default", 1, 10);
        try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE); Dataset dataset = open(allocator, uri)) {
            LanceIndexBuilder.BuildResult result = LanceIndexBuilder.ensureScalarIndexes(
                dataset,
                Set.of("rating"),
                Long.MAX_VALUE,
                Optional.empty(),
                Map.of()
            );
            assertEquals("failures: " + result.failed(), 0, result.failed().size());
            assertEquals(List.of(new LanceIndexBuilder.Built("rating", "BTREE")), result.built());
            assertEquals("BTREE", describedType(dataset, "rating"));
        }
    }

    public void testVectorPreferencePicksIvfFlatBelowThePqMinimum() throws Exception {
        Path scratch = createTempDir();
        // 20 rows: under the 256-row PQ training minimum, enough for
        // IVF_FLAT with one partition.
        String uri = LanceTableFactory.writeHintFixtureTable(scratch, "vector-flat", 2, 10);
        Map<String, LanceOverrides.IndexPreference> preferences = LanceOverrides.parseIndexesClause(
            Map.of("embedding", Map.of("vector", "ivf_flat"))
        );
        try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE); Dataset dataset = open(allocator, uri)) {
            LanceIndexBuilder.BuildResult result = LanceIndexBuilder.ensureVectorIndexes(
                dataset,
                Set.of("embedding"),
                Long.MAX_VALUE,
                Optional.empty(),
                preferences
            );
            assertEquals("failures: " + result.failed(), 0, result.failed().size());
            assertEquals(List.of(new LanceIndexBuilder.Built("embedding", "IVF_FLAT")), result.built());
            assertEquals("IVFFLAT", describedType(dataset, "embedding"));
        }
    }

    public void testVectorDefaultSkipsBelowThePqTrainingMinimum() throws Exception {
        Path scratch = createTempDir();
        String uri = LanceTableFactory.writeHintFixtureTable(scratch, "vector-small", 1, 10);
        try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE); Dataset dataset = open(allocator, uri)) {
            LanceIndexBuilder.BuildResult result = LanceIndexBuilder.ensureVectorIndexes(
                dataset,
                Set.of("embedding"),
                Long.MAX_VALUE,
                Optional.empty(),
                Map.of()
            );
            assertTrue(result.built().isEmpty());
            assertEquals(1, result.skipped().size());
            LanceIndexBuilder.Skipped skipped = result.skipped().get(0);
            assertEquals("embedding", skipped.column());
            assertTrue(skipped.reason(), skipped.reason().contains("ivf_pq training minimum of 256"));
        }
    }

    public void testVectorNonePreferenceSkips() throws Exception {
        Path scratch = createTempDir();
        String uri = LanceTableFactory.writeHintFixtureTable(scratch, "vector-none", 1, 10);
        Map<String, LanceOverrides.IndexPreference> preferences = LanceOverrides.parseIndexesClause(
            Map.of("embedding", Map.of("vector", "none"))
        );
        try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE); Dataset dataset = open(allocator, uri)) {
            LanceIndexBuilder.BuildResult result = LanceIndexBuilder.ensureVectorIndexes(
                dataset,
                Set.of("embedding"),
                Long.MAX_VALUE,
                Optional.empty(),
                preferences
            );
            assertEquals(List.of(new LanceIndexBuilder.Skipped("embedding", "index type none")), result.skipped());
            assertTrue(dataset.describeIndices(forColumn("embedding")).isEmpty());
        }
    }

    public void testVectorTrainingMinimums() {
        // PQ-trained types need 2^num_bits rows; every IVF type needs at
        // least num_partitions rows for k-means.
        assertEquals(256L, LanceIndexBuilder.vectorTrainingMinimum("ivf_pq", Map.of()));
        assertEquals(16L, LanceIndexBuilder.vectorTrainingMinimum("ivf_pq", Map.<String, Number>of("num_bits", 4)));
        assertEquals(512L, LanceIndexBuilder.vectorTrainingMinimum("ivf_pq", Map.<String, Number>of("num_partitions", 512)));
        assertEquals(256L, LanceIndexBuilder.vectorTrainingMinimum("ivf_hnsw_pq", Map.of()));
        assertEquals(1L, LanceIndexBuilder.vectorTrainingMinimum("ivf_flat", Map.of()));
        assertEquals(32L, LanceIndexBuilder.vectorTrainingMinimum("ivf_flat", Map.<String, Number>of("num_partitions", 32)));
        assertEquals(1L, LanceIndexBuilder.vectorTrainingMinimum("ivf_hnsw_sq", Map.of()));
        assertEquals(1L, LanceIndexBuilder.vectorTrainingMinimum("ivf_sq", Map.of()));
        assertEquals(1L, LanceIndexBuilder.vectorTrainingMinimum("ivf_rq", Map.of()));
    }

    private static Dataset open(RootAllocator allocator, String uri) {
        return Dataset.open().allocator(allocator).uri(uri).build();
    }

    private static IndexCriteria forColumn(String column) {
        return new IndexCriteria.Builder().forColumn(column).build();
    }

    /**
     * The one index type Lance reports for {@code column}, normalised to
     * upper case without underscores so the Rust display form
     * ({@code ZoneMap}, {@code IVF_FLAT}) compares stably.
     */
    private static String describedType(Dataset dataset, String column) {
        List<IndexDescription> descriptions = dataset.describeIndices(forColumn(column));
        assertEquals("expected exactly one index on " + column + ": " + descriptions, 1, descriptions.size());
        return descriptions.get(0).getIndexType().replace("_", "").toUpperCase(Locale.ROOT);
    }
}
