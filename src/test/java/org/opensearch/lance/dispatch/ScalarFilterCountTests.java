/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.lance.dispatch;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

import java.nio.file.Path;
import java.util.List;

import org.lance.Dataset;
import org.opensearch.lance.LanceRegistry;
import org.opensearch.lance.LanceTableFactory;
import org.opensearch.lance.StorageOptions;
import org.opensearch.lance.plan.execute.PlanExecutor;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.test.OpenSearchTestCase;

/**
 * The scalar filter count of an executor, exact and under a
 * {@code track_total_hits} bound, over the whole table and over a
 * proper subset of its fragments.
 *
 * <p>Fixture: {@link LanceTableFactory#writeMultiFragmentTable} with
 * twelve rows in three fragments of four. Row {@code i} has
 * {@code id = i} and sits in fragment {@code i / 4}, so
 * {@code id >= 4} matches eight rows, none in fragment 0 and four in
 * each of fragments 1 and 2.
 */
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class ScalarFilterCountTests extends OpenSearchTestCase {

    private static final String FILTER = "id >= 4";
    private static final int MATCHES = 8;
    private static final List<Integer> ALL = List.of(0, 1, 2);
    private static final List<Integer> NODE_A = List.of(0);
    private static final List<Integer> NODE_B = List.of(1, 2);

    private String uri;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        Path scratchDir = createTempDir();
        uri = LanceTableFactory.writeMultiFragmentTable(scratchDir, "scalar-count-" + getTestName(), 12, 4);
    }

    /**
     * Run {@code countScalarFilter} and check which path answered: an
     * accurate request must go through {@code LanceScanner.countRows()}
     * and never a limited scan, a bounded request the other way round.
     * Both counters are read before and after so a batch loop that
     * returns the right number still fails the exact case.
     */
    private static PlanExecutor.MatchedCount count(Dataset dataset, String filter, List<Integer> fragmentIds, int upTo) throws Exception {
        long nativeBefore = PlanExecutor.NATIVE_SCALAR_COUNTS.get();
        long boundedBefore = PlanExecutor.BOUNDED_SCALAR_COUNT_SCANS.get();
        PlanExecutor.MatchedCount result = PlanExecutor.countScalarFilter(dataset, filter, fragmentIds, upTo);
        long nativeCalls = PlanExecutor.NATIVE_SCALAR_COUNTS.get() - nativeBefore;
        long boundedScans = PlanExecutor.BOUNDED_SCALAR_COUNT_SCANS.get() - boundedBefore;
        boolean accurate = upTo == SearchContext.TRACK_TOTAL_HITS_ACCURATE;
        assertEquals("native countRows calls for upTo " + upTo, accurate ? 1L : 0L, nativeCalls);
        assertEquals("limited count scans for upTo " + upTo, accurate ? 0L : 1L, boundedScans);
        return result;
    }

    private static PlanExecutor.MatchedCount count(Dataset dataset, List<Integer> fragmentIds, int upTo) throws Exception {
        return count(dataset, FILTER, fragmentIds, upTo);
    }

    private static void assertCount(long value, boolean lowerBound, PlanExecutor.MatchedCount actual) {
        assertEquals("value", value, actual.value());
        assertEquals("lowerBound", lowerBound, actual.lowerBound());
    }

    public void testExactCountMatchesDatasetCountRows() throws Exception {
        try (Dataset dataset = LanceRegistry.openDataset(uri, StorageOptions.empty())) {
            long expected = dataset.countRows(FILTER);
            assertEquals(MATCHES, expected);
            assertCount(expected, false, count(dataset, ALL, SearchContext.TRACK_TOTAL_HITS_ACCURATE));
            // A subset executor counts only its own fragments, and the
            // subsets add up to the whole.
            PlanExecutor.MatchedCount a = count(dataset, NODE_A, SearchContext.TRACK_TOTAL_HITS_ACCURATE);
            PlanExecutor.MatchedCount b = count(dataset, NODE_B, SearchContext.TRACK_TOTAL_HITS_ACCURATE);
            assertCount(0L, false, a);
            assertCount(8L, false, b);
            assertCount(4L, false, count(dataset, List.of(1), SearchContext.TRACK_TOTAL_HITS_ACCURATE));
            assertEquals(expected, a.value() + b.value());
        }
    }

    public void testBoundBelowMatchesIsALowerBound() throws Exception {
        try (Dataset dataset = LanceRegistry.openDataset(uri, StorageOptions.empty())) {
            // Eight matches against a bound of five: the scan fills
            // its limit of six and reports a lower bound whose value
            // is the limit.
            assertCount(6L, true, count(dataset, ALL, 5));
            assertCount(6L, true, count(dataset, NODE_B, 5));
            // Bound one below the matches: the limit equals the match
            // count, so the scan fills and the count is still a lower
            // bound (the coordinator answers gte with the bound).
            assertCount(8L, true, count(dataset, ALL, 7));
            // A single fragment with four matches under a bound of
            // three.
            assertCount(4L, true, count(dataset, List.of(1), 3));
            // An executor with no matches comes back short of any
            // limit and is exact at zero.
            assertCount(0L, false, count(dataset, NODE_A, 5));
        }
    }

    public void testBoundAtOrAboveMatchesIsExact() throws Exception {
        try (Dataset dataset = LanceRegistry.openDataset(uri, StorageOptions.empty())) {
            // Bound equal to the matches: limit nine, eight rows come
            // back, exact.
            assertCount(8L, false, count(dataset, ALL, MATCHES));
            assertCount(8L, false, count(dataset, ALL, 20));
            assertCount(8L, false, count(dataset, ALL, SearchContext.DEFAULT_TRACK_TOTAL_HITS_UP_TO));
            assertCount(4L, false, count(dataset, List.of(2), 4));
            assertCount(0L, false, count(dataset, NODE_A, 20));
        }
    }

    public void testFilterWithoutMatchesIsExactZero() throws Exception {
        try (Dataset dataset = LanceRegistry.openDataset(uri, StorageOptions.empty())) {
            assertCount(0L, false, count(dataset, "id > 100", ALL, SearchContext.TRACK_TOTAL_HITS_ACCURATE));
            assertCount(0L, false, count(dataset, "id > 100", ALL, 5));
        }
    }
}
