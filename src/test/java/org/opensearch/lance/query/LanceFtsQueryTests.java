/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.query;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.lucene.search.QueryVisitor;
import org.lance.Dataset;
import org.lance.Fragment;
import org.lance.ipc.FullTextQuery;
import org.lance.ipc.LanceScanner;
import org.lance.ipc.ScanOptions;
import org.opensearch.lance.LanceRegistry;
import org.opensearch.lance.LanceTableFactory;
import org.opensearch.lance.StorageOptions;
import org.opensearch.test.OpenSearchTestCase;

// The fragment coverage tests open a real Lance dataset; Lance JNI
// spins up native worker threads that outlive the test method.
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class LanceFtsQueryTests extends OpenSearchTestCase {

    public void testEqualsAndHashCode() {
        LanceFtsQuery a = new LanceFtsQuery("body", "camera");
        LanceFtsQuery b = new LanceFtsQuery("body", "camera");
        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());

        assertNotEquals(a, new LanceFtsQuery("title", "camera"));
        assertNotEquals(a, new LanceFtsQuery("body", "phone"));
    }

    public void testToStringIsHumanReadable() {
        LanceFtsQuery query = new LanceFtsQuery("body", "camera");
        String text = query.toString("body");
        assertTrue("toString should mention the column, saw: " + text, text.contains("body"));
        assertTrue("toString should mention the query text, saw: " + text, text.contains("camera"));
    }

    public void testVisitorReceivesLeaf() {
        LanceFtsQuery query = new LanceFtsQuery("body", "camera");
        AtomicInteger leafCalls = new AtomicInteger();
        query.visit(new QueryVisitor() {
            @Override
            public void visitLeaf(org.apache.lucene.search.Query q) {
                leafCalls.incrementAndGet();
                assertSame(query, q);
            }
        });
        assertEquals("expected exactly one visitLeaf call", 1, leafCalls.get());
    }

    public void testPhraseQueryDiffersFromMatchQuery() {
        // phrase / non-phrase are distinct queries even when column and
        // text agree, because Lance routes them to different FullTextQuery
        // constructors. Making equals reflect that keeps query cache keys
        // and _explain output honest.
        LanceFtsQuery match = new LanceFtsQuery("body", "quick brown");
        LanceFtsQuery phrase = new LanceFtsQuery("body", "quick brown", true, 0);
        assertNotEquals(match, phrase);
        assertNotEquals(match.hashCode(), phrase.hashCode());
        String phraseText = phrase.toString("body");
        assertTrue("phrase toString should mention phrase, saw: " + phraseText, phraseText.contains("phrase"));
    }

    public void testPhraseSlopParticipatesInEqualsAndHashCode() {
        LanceFtsQuery slop0 = new LanceFtsQuery("body", "quick brown", true, 0);
        LanceFtsQuery slop2 = new LanceFtsQuery("body", "quick brown", true, 2);
        assertNotEquals(slop0, slop2);
        // Slop is clamped to a non-negative int; negative values collapse to 0.
        LanceFtsQuery slopNegative = new LanceFtsQuery("body", "quick brown", true, -5);
        assertEquals(slop0, slopNegative);
    }

    public void testOperatorParticipatesInEqualsAndHashCode() {
        LanceFtsQuery orQuery = new LanceFtsQuery("body", "quick brown");
        LanceFtsQuery andQuery = new LanceFtsQuery("body", "quick brown", FullTextQuery.Operator.AND);
        assertNotEquals(orQuery, andQuery);
        assertNotEquals(orQuery.hashCode(), andQuery.hashCode());
        String andText = andQuery.toString("body");
        assertTrue("AND toString should mention the operator, saw: " + andText, andText.contains("AND"));
    }

    public void testNullOperatorFallsBackToOr() {
        // The constructor tolerates a null operator so callers do not have
        // to guard when threading through code paths where OR is the
        // default. Equality with the explicit-OR form documents that.
        LanceFtsQuery a = new LanceFtsQuery("body", "camera", false, 0, null);
        LanceFtsQuery b = new LanceFtsQuery("body", "camera", false, 0, FullTextQuery.Operator.OR);
        assertEquals(a, b);
    }

    public void testDirectFullTextQueryConstructorAndColumnsCollection() {
        // A LanceFtsQuery built from a MatchQuery with fuzziness must not
        // compare equal to one without it, because the parameter change
        // reaches Lance and can change the result set. Documenting this
        // through equality keeps _explain output honest for callers who
        // rely on canonical DSL representations.
        FullTextQuery plain = FullTextQuery.match("hello", "body");
        FullTextQuery fuzzy = FullTextQuery.match("hello", "body", 1f, Optional.of(1), 50, FullTextQuery.Operator.OR, 0);
        LanceFtsQuery plainQuery = new LanceFtsQuery(plain, Set.of("body"));
        LanceFtsQuery fuzzyQuery = new LanceFtsQuery(fuzzy, Set.of("body"));
        assertNotEquals(plainQuery, fuzzyQuery);
    }

    public void testCollectColumnsWalksBooleanTree() {
        FullTextQuery bodyMatch = FullTextQuery.match("hello", "body");
        FullTextQuery titleMatch = FullTextQuery.match("world", "title");
        FullTextQuery combined = FullTextQuery.booleanQuery(
            List.of(
                new FullTextQuery.BooleanClause(FullTextQuery.Occur.MUST, bodyMatch),
                new FullTextQuery.BooleanClause(FullTextQuery.Occur.SHOULD, titleMatch)
            )
        );
        Set<String> columns = LanceFtsQuery.collectColumns(combined);
        assertEquals(Set.of("body", "title"), columns);
    }

    public void testCollectColumnsWalksBoostTree() {
        FullTextQuery pos = FullTextQuery.match("hello", "body");
        FullTextQuery neg = FullTextQuery.match("stale", "title");
        FullTextQuery boosted = FullTextQuery.boost(pos, neg, 0.5f);
        Set<String> columns = LanceFtsQuery.collectColumns(boosted);
        assertEquals(Set.of("body", "title"), columns);
    }

    public void testCoversAllFragmentsWhenExecutorHoldsEveryFragment() throws Exception {
        // 12 rows, 4 per file: fragments 0, 1, 2. An executor that
        // holds exactly that set covers the dataset, and the scan
        // builder must stay free of a fragmentIds restriction.
        Path scratchDir = createTempDir();
        String uri = LanceTableFactory.writeMultiFragmentTable(scratchDir, "coverage-all", 12, 4);
        try (Dataset dataset = LanceRegistry.openDataset(uri, StorageOptions.empty())) {
            List<Integer> all = fragmentIdsOf(dataset);
            assertEquals("fixture must produce three fragments", 3, all.size());

            assertTrue(LanceFtsQuery.coversAllFragments(all, dataset));
            // Order and duplicates do not matter: coverage is a set check.
            List<Integer> shuffled = new ArrayList<>(all);
            Collections.reverse(shuffled);
            shuffled.add(all.get(0));
            assertTrue(LanceFtsQuery.coversAllFragments(shuffled, dataset));

            ScanOptions options = LanceFtsQuery.restrictToFragmentsUnlessAll(new ScanOptions.Builder(), all, dataset).build();
            assertFalse("full coverage must not set fragmentIds", options.getFragmentIds().isPresent());
        }
    }

    public void testCoversAllFragmentsIsFalseForProperSubset() throws Exception {
        Path scratchDir = createTempDir();
        String uri = LanceTableFactory.writeMultiFragmentTable(scratchDir, "coverage-subset", 12, 4);
        try (Dataset dataset = LanceRegistry.openDataset(uri, StorageOptions.empty())) {
            List<Integer> all = fragmentIdsOf(dataset);
            assertEquals(3, all.size());

            // Drop one fragment: the remaining two have the same size
            // as a two-node round-robin slice and must be reported as
            // a subset regardless of which fragment is missing.
            for (int missing = 0; missing < all.size(); missing++) {
                List<Integer> subset = new ArrayList<>(all);
                subset.remove(missing);
                assertFalse("subset missing fragment " + all.get(missing), LanceFtsQuery.coversAllFragments(subset, dataset));

                ScanOptions options = LanceFtsQuery.restrictToFragmentsUnlessAll(new ScanOptions.Builder(), subset, dataset).build();
                assertEquals("subset must be passed through as fragmentIds", Optional.of(subset), options.getFragmentIds());
            }
            assertFalse("empty executor set never covers a non-empty dataset", LanceFtsQuery.coversAllFragments(List.of(), dataset));
        }
    }

    public void testCoversAllFragmentsIgnoresIdsUnknownToDataset() throws Exception {
        // A superset that carries ids the manifest does not know (for
        // example a fragment id the coordinator saw before a
        // compaction rewrote the table) still covers every fragment
        // the dataset has, so the scan can run unrestricted. A size
        // comparison alone would call this a mismatch.
        Path scratchDir = createTempDir();
        String uri = LanceTableFactory.writeMultiFragmentTable(scratchDir, "coverage-superset", 12, 4);
        try (Dataset dataset = LanceRegistry.openDataset(uri, StorageOptions.empty())) {
            List<Integer> all = fragmentIdsOf(dataset);
            assertEquals(3, all.size());

            List<Integer> superset = new ArrayList<>(all);
            superset.add(1_000);
            superset.add(1_001);
            assertTrue(LanceFtsQuery.coversAllFragments(superset, dataset));
            ScanOptions options = LanceFtsQuery.restrictToFragmentsUnlessAll(new ScanOptions.Builder(), superset, dataset).build();
            assertFalse(options.getFragmentIds().isPresent());

            // Same size as the dataset but one real id swapped for an
            // unknown one: not a cover, because a real fragment is
            // missing. This is the case a size comparison gets wrong.
            List<Integer> sameSizeButMissingOne = new ArrayList<>(all.subList(1, all.size()));
            sameSizeButMissingOne.add(1_000);
            assertEquals(all.size(), sameSizeButMissingOne.size());
            assertFalse(LanceFtsQuery.coversAllFragments(sameSizeButMissingOne, dataset));
        }
    }

    public void testFtsScanReturnsOnlySubsetRowsWithRestrictionAndAllRowsWithout() throws Exception {
        // Drive Lance with the same ScanOptions shape ensureShardScan
        // builds (fullTextQuery + row address + limit) through both
        // branches of restrictToFragmentsUnlessAll. "hello" sits in
        // the even rows, two per fragment of four rows.
        Path scratchDir = createTempDir();
        String uri = LanceTableFactory.writeMultiFragmentTable(scratchDir, "coverage-scan", 12, 4);
        try (Dataset dataset = LanceRegistry.openDataset(uri, StorageOptions.empty())) {
            List<Integer> all = fragmentIdsOf(dataset);
            assertEquals(3, all.size());

            Map<Integer, Integer> unrestricted = hitsByFragment(dataset, all);
            assertEquals("two hello rows per fragment", Map.of(all.get(0), 2, all.get(1), 2, all.get(2), 2), unrestricted);

            List<Integer> subset = List.of(all.get(0), all.get(2));
            Map<Integer, Integer> restricted = hitsByFragment(dataset, subset);
            assertEquals("restricted scan must not return rows of the excluded fragment", Map.of(all.get(0), 2, all.get(2), 2), restricted);
        }
    }

    private static Map<Integer, Integer> hitsByFragment(Dataset dataset, List<Integer> executorFragmentIds) throws Exception {
        ScanOptions options = LanceFtsQuery.restrictToFragmentsUnlessAll(new ScanOptions.Builder(), executorFragmentIds, dataset)
            .fullTextQuery(FullTextQuery.match("hello", "body"))
            .withRowAddress(true)
            .limit(100L)
            .build();
        Map<Integer, Integer> counts = new HashMap<>();
        try (LanceScanner scanner = dataset.newScan(options); ArrowReader reader = scanner.scanBatches()) {
            while (reader.loadNextBatch()) {
                VectorSchemaRoot root = reader.getVectorSchemaRoot();
                UInt8Vector rowAddr = (UInt8Vector) root.getVector("_rowaddr");
                for (int i = 0; i < root.getRowCount(); i++) {
                    int fragmentId = (int) (rowAddr.get(i) >>> 32);
                    counts.merge(fragmentId, 1, Integer::sum);
                }
            }
        }
        return counts;
    }

    private static List<Integer> fragmentIdsOf(Dataset dataset) {
        List<Integer> ids = new ArrayList<>();
        for (Fragment fragment : dataset.getFragments()) {
            ids.add(fragment.getId());
        }
        return ids;
    }
}
