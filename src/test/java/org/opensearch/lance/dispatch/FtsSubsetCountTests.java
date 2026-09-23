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
import org.opensearch.lance.query.LanceFtsQuery;
import org.opensearch.lance.plan.execute.PlanExecutor;
import org.opensearch.test.OpenSearchTestCase;

/**
 * The FTS count of an executor that holds a proper subset of the
 * table's fragments. Two executors, one over fragment 0 and one over
 * fragments 1 and 2, stand in for a three node fan out; the sum of
 * their counts is what the coordinator compares with the
 * {@code track_total_hits} bound.
 *
 * <p>Fixture: {@link LanceTableFactory#writeInterleavedTable} with
 * three fragments of four rows. Row {@code i} sits in fragment
 * {@code i % 3}; {@code lance} matches all twelve rows with a score
 * that grows with {@code i}, so the top {@code n} rows are the ids
 * {@code 11, 10, ...} and every executor sees the same {@code n}.
 */
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class FtsSubsetCountTests extends OpenSearchTestCase {

    private static final int TOTAL = 12;
    private static final List<Integer> NODE_A = List.of(0);
    private static final List<Integer> NODE_B = List.of(1, 2);

    private String uri;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        Path scratchDir = createTempDir();
        uri = LanceTableFactory.writeInterleavedTable(scratchDir, "count-" + getTestName(), 3, 4);
    }

    private static PlanExecutor.FtsHitCount count(Dataset dataset, List<Integer> fragmentIds, long limit) throws Exception {
        return PlanExecutor.countFtsHitsDirectly(dataset, new LanceFtsQuery("body", "lance"), fragmentIds, limit);
    }

    private static void assertCount(long scanned, long own, PlanExecutor.FtsHitCount actual) {
        assertEquals("scanned", scanned, actual.scanned());
        assertEquals("own", own, actual.own());
    }

    public void testBoundedCountsSumToMinOfTotalAndBound() throws Exception {
        try (Dataset dataset = LanceRegistry.openDataset(uri, StorageOptions.empty())) {
            // total < upTo + 1: every match is seen, the executors
            // split the twelve rows 4 / 8, and the scan came back
            // short of its limit on both.
            assertCount(TOTAL, 4L, count(dataset, NODE_A, 21L));
            assertCount(TOTAL, 8L, count(dataset, NODE_B, 21L));
            // total == upTo + 1: the limit is reached exactly at the
            // last match; the sum still equals the total, and the
            // scan filled its limit like it does on a whole table
            // executor (whose countRows returns the same 12).
            assertCount(12L, 4L, count(dataset, NODE_A, 12L));
            assertCount(12L, 8L, count(dataset, NODE_B, 12L));
            // total > upTo + 1: the whole-table top 5 is ids 11..7, of
            // which id 9 sits in fragment 0 and 11, 10, 8, 7 in
            // fragments 1 and 2. Each executor's share is below the
            // limit, but scanned is the limit on both, which is what
            // the executor compares with the bound.
            PlanExecutor.FtsHitCount a = count(dataset, NODE_A, 5L);
            PlanExecutor.FtsHitCount b = count(dataset, NODE_B, 5L);
            assertCount(5L, 1L, a);
            assertCount(5L, 4L, b);
            assertEquals(Math.min(TOTAL, 5L), a.own() + b.own());
        }
    }

    public void testExactCountProbesAndFallsBackWhenTheProbeFills() throws Exception {
        try (Dataset dataset = LanceRegistry.openDataset(uri, StorageOptions.empty())) {
            // Default probe limit: twelve matches come back short of
            // it, the executors count their own rows.
            assertCount(TOTAL, 4L, count(dataset, NODE_A, 0L));
            assertCount(TOTAL, 8L, count(dataset, NODE_B, 0L));

            int before = LanceFtsQuery.subsetProbeLimit();
            LanceFtsQuery.setSubsetProbeLimit(5);
            try {
                // Twelve matches fill a probe of five, so the count
                // comes from the restricted scan and is still exact;
                // that scan returns the executor's rows only.
                assertCount(4L, 4L, count(dataset, NODE_A, 0L));
                assertCount(8L, 8L, count(dataset, NODE_B, 0L));
            } finally {
                LanceFtsQuery.setSubsetProbeLimit(before);
            }
        }
    }

    public void testFullCoverageCountIsUnchanged() throws Exception {
        try (Dataset dataset = LanceRegistry.openDataset(uri, StorageOptions.empty())) {
            assertCount(TOTAL, TOTAL, count(dataset, null, 0L));
            assertCount(TOTAL, TOTAL, count(dataset, List.of(0, 1, 2), 0L));
            assertCount(5L, 5L, count(dataset, List.of(0, 1, 2), 5L));
        }
    }
}
